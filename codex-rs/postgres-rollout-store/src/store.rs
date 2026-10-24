//! Canonical, append-only rollout lines.
//!
//! A thread's lines occupy dense positions starting at zero. Appending is a compare-and-extend on
//! the next position under the thread row lock: the caller names the position it expects to
//! write at, so a batch whose commit result was lost can be retried and recognized, never
//! duplicated, and a writer that fell behind is told instead of interleaving.

use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::require_storage_open;
use codex_protocol::ThreadId;
use sqlx::Acquire;
use sqlx::PgConnection;
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;

const QUERY_TIMEOUT: Duration = Duration::from_secs(60);

/// Largest number of lines returned by one read, bounding memory for every caller.
pub const MAX_READ_LINES: usize = 1_000;

/// Failures that never expose connection details.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RolloutStoreError {
    /// A caller value cannot be represented by the PostgreSQL storage format.
    #[error("invalid rollout storage request: {0}")]
    InvalidRequest(&'static str),
    /// PostgreSQL could not be reached or the operation did not complete.
    #[error("rollout storage is unavailable: {0}")]
    Unavailable(String),
    /// The thread row does not exist, so it cannot hold lines.
    #[error("thread {0} does not exist in rollout storage")]
    MissingThread(ThreadId),
    /// The append position or content disagrees with what is stored.
    #[error("rollout append conflict: expected position {expected}, stored next position {stored}")]
    Conflict { expected: u64, stored: u64 },
    /// A stored position was unexpectedly missing or malformed.
    #[error("stored rollout lines are inconsistent: {0}")]
    Corrupt(String),
}

fn database(error: sqlx::Error) -> RolloutStoreError {
    match error {
        sqlx::Error::Database(database) => RolloutStoreError::Unavailable(format!(
            "database error {}",
            database.code().as_deref().unwrap_or("unknown")
        )),
        _ => RolloutStoreError::Unavailable("the database operation failed".to_string()),
    }
}

fn pool(error: PoolError) -> RolloutStoreError {
    RolloutStoreError::Unavailable(format!("PostgreSQL is unavailable ({error:?})"))
}

/// One stored line and the position it holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredRolloutLine {
    pub position: u64,
    pub ordinal: Option<u64>,
    /// The serialized rollout line, byte for byte as it was appended.
    pub line: String,
}

/// Fixed-namespace canonical rollout adapter. Construction does not activate PostgreSQL.
#[derive(Clone)]
pub struct PostgresRolloutStore {
    pool: Arc<PostgresPool>,
}

impl PostgresRolloutStore {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }

    /// Run one operation in a transaction, bounded by the query timeout.
    async fn run<T, F>(&self, operation: F) -> Result<T, RolloutStoreError>
    where
        F: for<'c> FnOnce(
            &'c mut PgConnection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, RolloutStoreError>> + Send + 'c>,
        >,
    {
        let mut connection = self.pool.acquire().await.map_err(pool)?;
        timeout(QUERY_TIMEOUT, async {
            let mut tx = connection.begin().await.map_err(database)?;
            let value = operation(&mut tx).await?;
            tx.commit().await.map_err(database)?;
            Ok(value)
        })
        .await
        .map_err(|_| RolloutStoreError::Unavailable("the operation timed out".to_string()))?
    }

    /// The position the next appended line will take.
    pub async fn next_position(&self, thread_id: ThreadId) -> Result<u64, RolloutStoreError> {
        self.run(move |connection| {
            Box::pin(async move {
                let exists: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM threads WHERE id = $1::uuid)")
                        .bind(thread_id.to_string())
                        .fetch_one(&mut *connection)
                        .await
                        .map_err(database)?;
                if !exists {
                    return Err(RolloutStoreError::MissingThread(thread_id));
                }
                next_position_in(connection, thread_id).await
            })
        })
        .await
    }

    /// Append `lines` at `expected_position`, returning the next position afterwards.
    ///
    /// When the stored next position is already past `expected_position` and the lines stored
    /// there equal this batch, an earlier attempt committed, so the call succeeds without
    /// writing again. Any other disagreement is a [`RolloutStoreError::Conflict`].
    pub async fn append(
        &self,
        thread_id: ThreadId,
        expected_position: u64,
        lines: Vec<(Option<u64>, String)>,
    ) -> Result<u64, RolloutStoreError> {
        validate_append_request(expected_position, &lines)?;
        self.run(move |connection| {
            Box::pin(
                async move { append_in(connection, thread_id, expected_position, &lines).await },
            )
        })
        .await
    }

    /// Read up to `limit` lines starting at `from_position`, in position order.
    pub async fn read(
        &self,
        thread_id: ThreadId,
        from_position: u64,
        limit: usize,
    ) -> Result<Vec<StoredRolloutLine>, RolloutStoreError> {
        checked_request_position(from_position)?;
        let limit = limit.min(MAX_READ_LINES);
        self.run(move |connection| {
            Box::pin(async move { read_in(connection, thread_id, from_position, limit).await })
        })
        .await
    }

    /// Read every line, paging so no single query is unbounded.
    pub async fn read_all(
        &self,
        thread_id: ThreadId,
    ) -> Result<Vec<StoredRolloutLine>, RolloutStoreError> {
        let mut all = Vec::new();
        loop {
            let from = all
                .last()
                .map_or(0, |line: &StoredRolloutLine| line.position + 1);
            let page = self.read(thread_id, from, MAX_READ_LINES).await?;
            let done = page.len() < MAX_READ_LINES;
            all.extend(page);
            if done {
                return Ok(all);
            }
        }
    }

    /// Keep positions below `keep` and delete the rest, for reverting a thread.
    pub async fn truncate(&self, thread_id: ThreadId, keep: u64) -> Result<u64, RolloutStoreError> {
        let keep = checked_request_position(keep)?;
        self.run(move |connection| {
            Box::pin(async move {
                lock_thread(connection, thread_id).await?;
                Ok(sqlx::query(
                    "DELETE FROM thread_rollout_lines \
                     WHERE thread_id = $1::uuid AND position >= $2",
                )
                .bind(thread_id.to_string())
                .bind(keep)
                .execute(&mut *connection)
                .await
                .map_err(database)?
                .rows_affected())
            })
        })
        .await
    }

    /// Copy the first `count` lines of `source` into the empty `destination`, for forking.
    pub async fn copy_prefix(
        &self,
        source: ThreadId,
        destination: ThreadId,
        count: u64,
    ) -> Result<(), RolloutStoreError> {
        let count_signed = checked_request_position(count)?;
        self.run(move |connection| {
            Box::pin(async move {
                lock_thread(connection, destination).await?;
                let existing = next_position_in(connection, destination).await?;
                if existing != 0 {
                    return Err(RolloutStoreError::Conflict {
                        expected: 0,
                        stored: existing,
                    });
                }
                let copied = sqlx::query(
                    "INSERT INTO thread_rollout_lines \
                     (thread_id, position, ordinal, line) \
                     SELECT $1::uuid, position, ordinal, line \
                     FROM thread_rollout_lines \
                     WHERE thread_id = $2::uuid AND position < $3",
                )
                .bind(destination.to_string())
                .bind(source.to_string())
                .bind(count_signed)
                .execute(&mut *connection)
                .await
                .map_err(database)?
                .rows_affected();
                if copied != count {
                    return Err(RolloutStoreError::Corrupt(format!(
                        "the source holds {copied} of the {count} requested lines"
                    )));
                }
                Ok(())
            })
        })
        .await
    }
}

/// Append inside a caller-owned transaction. Validate the complete request before any SQL.
/// The next position must remain representable as PostgreSQL BIGINT.
pub async fn append_in(
    connection: &mut PgConnection,
    thread_id: ThreadId,
    expected_position: u64,
    lines: &[(Option<u64>, String)],
) -> Result<u64, RolloutStoreError> {
    let end = validate_append_request(expected_position, lines)?;
    let count = end - expected_position;
    lock_thread(connection, thread_id).await?;
    let stored = next_position_in(connection, thread_id).await?;
    if stored > expected_position {
        if stored >= end {
            let existing = read_in(connection, thread_id, expected_position, lines.len()).await?;
            let same = existing.len() == lines.len()
                && existing.iter().zip(lines).all(|(stored, (ordinal, line))| {
                    stored.ordinal == *ordinal && stored.line == *line
                });
            if same && count > 0 {
                return Ok(end);
            }
        }
        return Err(RolloutStoreError::Conflict {
            expected: expected_position,
            stored,
        });
    }
    if stored < expected_position {
        return Err(RolloutStoreError::Conflict {
            expected: expected_position,
            stored,
        });
    }
    for (offset, (ordinal, line)) in lines.iter().enumerate() {
        let offset = u64::try_from(offset)
            .map_err(|_| RolloutStoreError::InvalidRequest("batch position overflow"))?;
        let position = checked_request_position(
            expected_position
                .checked_add(offset)
                .ok_or(RolloutStoreError::InvalidRequest("batch position overflow"))?,
        )?;
        sqlx::query(
            "INSERT INTO thread_rollout_lines \
             (thread_id, position, ordinal, line) VALUES ($1::uuid, $2, $3, $4)",
        )
        .bind(thread_id.to_string())
        .bind(position)
        .bind(checked_write_ordinal(*ordinal)?)
        .bind(line)
        .execute(&mut *connection)
        .await
        .map_err(database)?;
    }
    Ok(end)
}

/// Lock the thread row, which also proves the thread exists. Every write takes this lock
/// first, so it is also where writes are refused while a migration holds the store.
async fn lock_thread(
    connection: &mut PgConnection,
    thread_id: ThreadId,
) -> Result<(), RolloutStoreError> {
    require_storage_open(connection)
        .await
        .map_err(|error| RolloutStoreError::Unavailable(error.to_string()))?;
    sqlx::query_scalar::<_, i32>("SELECT 1 FROM threads WHERE id = $1::uuid FOR UPDATE")
        .bind(thread_id.to_string())
        .fetch_optional(connection)
        .await
        .map_err(database)?
        .map(|_| ())
        .ok_or(RolloutStoreError::MissingThread(thread_id))
}

async fn next_position_in(
    connection: &mut PgConnection,
    thread_id: ThreadId,
) -> Result<u64, RolloutStoreError> {
    let next: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(position) FROM thread_rollout_lines WHERE thread_id = $1::uuid",
    )
    .bind(thread_id.to_string())
    .fetch_one(connection)
    .await
    .map_err(database)?;
    next.map_or(Ok(0), |last| {
        u64::try_from(last)
            .ok()
            .and_then(|last| last.checked_add(1))
            .ok_or_else(|| RolloutStoreError::Corrupt("position out of range".to_string()))
    })
}

fn checked_write_ordinal(ordinal: Option<u64>) -> Result<Option<i64>, RolloutStoreError> {
    ordinal
        .map(i64::try_from)
        .transpose()
        .map_err(|_| RolloutStoreError::InvalidRequest("ordinal exceeds PostgreSQL BIGINT"))
}

fn checked_request_position(position: u64) -> Result<i64, RolloutStoreError> {
    i64::try_from(position)
        .map_err(|_| RolloutStoreError::InvalidRequest("position exceeds PostgreSQL BIGINT"))
}

fn validate_append_request(
    expected: u64,
    lines: &[(Option<u64>, String)],
) -> Result<u64, RolloutStoreError> {
    checked_request_position(expected)?;
    let count = u64::try_from(lines.len())
        .map_err(|_| RolloutStoreError::InvalidRequest("batch length overflow"))?;
    let end = expected
        .checked_add(count)
        .ok_or(RolloutStoreError::InvalidRequest("batch position overflow"))?;
    checked_request_position(end)?;
    for (ordinal, _) in lines {
        checked_write_ordinal(*ordinal)?;
    }
    Ok(end)
}

fn checked_read_line(
    expected: u64,
    position: i64,
    ordinal: Option<i64>,
    line: String,
) -> Result<StoredRolloutLine, RolloutStoreError> {
    let position = u64::try_from(position)
        .map_err(|_| RolloutStoreError::Corrupt("negative position".to_string()))?;
    if position != expected {
        return Err(RolloutStoreError::Corrupt(
            "rollout page contains a position gap".to_string(),
        ));
    }
    let ordinal = ordinal
        .map(u64::try_from)
        .transpose()
        .map_err(|_| RolloutStoreError::Corrupt("negative ordinal".to_string()))?;
    Ok(StoredRolloutLine {
        position,
        ordinal,
        line,
    })
}

async fn read_in(
    connection: &mut PgConnection,
    thread_id: ThreadId,
    from_position: u64,
    limit: usize,
) -> Result<Vec<StoredRolloutLine>, RolloutStoreError> {
    let from_signed = checked_request_position(from_position)?;
    let rows =
        sqlx::query(
            "SELECT position, ordinal, line FROM thread_rollout_lines \
         WHERE thread_id = $1::uuid AND position >= $2 ORDER BY position LIMIT $3",
        )
        .bind(thread_id.to_string())
        .bind(from_signed)
        .bind(i64::try_from(limit).map_err(|_| {
            RolloutStoreError::InvalidRequest("read limit exceeds PostgreSQL BIGINT")
        })?)
        .fetch_all(connection)
        .await
        .map_err(database)?;
    rows.iter()
        .enumerate()
        .map(|(offset, row)| {
            let position: i64 = row.try_get("position").map_err(database)?;
            let ordinal: Option<i64> = row.try_get("ordinal").map_err(database)?;
            let offset = u64::try_from(offset)
                .map_err(|_| RolloutStoreError::Corrupt("page offset overflow".to_string()))?;
            let expected = from_position
                .checked_add(offset)
                .ok_or_else(|| RolloutStoreError::Corrupt("page position overflow".to_string()))?;
            checked_read_line(
                expected,
                position,
                ordinal,
                row.try_get("line").map_err(database)?,
            )
        })
        .collect()
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "store_contract_tests.rs"]
mod contract_tests;
