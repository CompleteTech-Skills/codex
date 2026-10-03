//! Canonical, append-only rollout lines.
//!
//! A thread's lines occupy dense positions starting at zero. Appending is a compare-and-extend on
//! the next position under the thread row lock: the caller names the position it expects to
//! write at, so a batch whose commit result was lost can be retried and recognized, never
//! duplicated, and a writer that fell behind is told instead of interleaving.

use codex_postgres_runtime::BoundWrite;
use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
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

    async fn write<T, F>(&self, operation: F) -> Result<T, RolloutStoreError>
    where
        F: for<'c> FnOnce(
            &'c mut PgConnection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, RolloutStoreError>> + Send + 'c>,
        >,
    {
        timeout(QUERY_TIMEOUT, async {
            let mut admitted = self
                .pool
                .begin_selected_write()
                .await
                .map_err(|error| RolloutStoreError::Unavailable(error.to_string()))?;
            let value = operation(admitted.connection()).await?;
            admitted.commit().await.map_err(database)?;
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
        timeout(QUERY_TIMEOUT, async {
            let mut admitted = self
                .pool
                .begin_selected_write()
                .await
                .map_err(|error| RolloutStoreError::Unavailable(error.to_string()))?;
            let next = append_in(&mut admitted, thread_id, expected_position, &lines).await?;
            admitted.commit().await.map_err(database)?;
            Ok(next)
        })
        .await
        .map_err(|_| RolloutStoreError::Unavailable("the operation timed out".to_string()))?
    }

    /// Read up to `limit` lines starting at `from_position`, in position order.
    pub async fn read(
        &self,
        thread_id: ThreadId,
        from_position: u64,
        limit: usize,
    ) -> Result<Vec<StoredRolloutLine>, RolloutStoreError> {
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
        self.write(move |connection| {
            Box::pin(async move {
                lock_thread(connection, thread_id).await?;
                Ok(sqlx::query(
                    "DELETE FROM thread_rollout_lines \
                     WHERE thread_id = $1::uuid AND position >= $2",
                )
                .bind(thread_id.to_string())
                .bind(i64::try_from(keep).unwrap_or(i64::MAX))
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
        self.write(move |connection| {
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
                .bind(i64::try_from(count).unwrap_or(i64::MAX))
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

/// Compare-and-extend inside the caller's transaction; see [`PostgresRolloutStore::append`].
pub async fn append_in(
    admitted: &mut BoundWrite<'_>,
    thread_id: ThreadId,
    expected_position: u64,
    lines: &[(Option<u64>, String)],
) -> Result<u64, RolloutStoreError> {
    let connection = admitted.connection();
    let count = u64::try_from(lines.len())
        .map_err(|_| RolloutStoreError::Corrupt("batch is too large".to_string()))?;
    lock_thread(connection, thread_id).await?;
    let stored = next_position_in(connection, thread_id).await?;
    if stored > expected_position {
        let end = expected_position
            .checked_add(count)
            .ok_or(RolloutStoreError::Conflict {
                expected: expected_position,
                stored,
            })?;
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
        let position = i64::try_from(expected_position + offset as u64)
            .map_err(|_| RolloutStoreError::Corrupt("position overflow".to_string()))?;
        sqlx::query(
            "INSERT INTO thread_rollout_lines              (thread_id, position, ordinal, line) VALUES ($1::uuid, $2, $3, $4)",
        )
        .bind(thread_id.to_string())
        .bind(position)
        .bind(ordinal.map(|ordinal| ordinal as i64))
        .bind(line)
        .execute(&mut *connection)
        .await
        .map_err(database)?;
    }
    Ok(expected_position + count)
}

/// Lock the thread row, which also proves the thread exists. Every write takes this lock
/// first, after its caller's original transaction has taken the activation admission lock.
async fn lock_thread(
    connection: &mut PgConnection,
    thread_id: ThreadId,
) -> Result<(), RolloutStoreError> {
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

async fn read_in(
    connection: &mut PgConnection,
    thread_id: ThreadId,
    from_position: u64,
    limit: usize,
) -> Result<Vec<StoredRolloutLine>, RolloutStoreError> {
    let rows = sqlx::query(
        "SELECT position, ordinal, line FROM thread_rollout_lines \
         WHERE thread_id = $1::uuid AND position >= $2 ORDER BY position LIMIT $3",
    )
    .bind(thread_id.to_string())
    .bind(i64::try_from(from_position).unwrap_or(i64::MAX))
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(connection)
    .await
    .map_err(database)?;
    rows.iter()
        .map(|row| {
            let position: i64 = row.try_get("position").map_err(database)?;
            let ordinal: Option<i64> = row.try_get("ordinal").map_err(database)?;
            Ok(StoredRolloutLine {
                position: u64::try_from(position)
                    .map_err(|_| RolloutStoreError::Corrupt("negative position".to_string()))?,
                ordinal: ordinal.map(|ordinal| ordinal as u64),
                line: row.try_get("line").map_err(database)?,
            })
        })
        .collect()
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
