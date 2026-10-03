use chrono::Utc;
use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
use codex_protocol::ThreadId;
use codex_state::QueuedUserSubmissionRecord;
use codex_thread_store::MAX_QUEUE_ITEMS;
use codex_thread_store::QueueStore;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreFuture;
use sqlx::Acquire;
use sqlx::PgConnection;
use sqlx::Row;
use sqlx::postgres::PgRow;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Fixed-namespace queue adapter. Construction does not activate PostgreSQL.
///
/// Every write first takes the lock on the single change-counter row. That serializes queue
/// writers, which keeps the per-thread item limit and queue order exact, and it makes change
/// versions become visible in commit order: a reader that has seen version `n` has seen every
/// change numbered `n` or lower, so a late commit can never be skipped.
#[derive(Clone)]
pub struct PostgresQueueStore {
    pool: Arc<PostgresPool>,
}

impl PostgresQueueStore {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }

    /// Remove every queued item for a thread and record the change, for thread deletion.
    pub async fn delete_thread_queue(&self, thread_id: ThreadId) -> Result<bool, ThreadStoreError> {
        self.write(|connection| {
            Box::pin(async move { Ok(delete_thread_queue_in(connection, thread_id).await?) })
        })
        .await
    }

    /// Run one queue write in a transaction, bounded by the query timeout.
    async fn write<T, F>(&self, operation: F) -> Result<T, ThreadStoreError>
    where
        F: for<'c> FnOnce(
            &'c mut PgConnection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<T, WriteError>> + Send + 'c>,
        >,
    {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        timeout(QUERY_TIMEOUT, async {
            let mut transaction = connection.begin().await?;
            let admitted = async {
                self.pool.prepare_selected_write(&mut transaction).await?;
                self.pool.require_bound_storage_open(&mut transaction).await
            }
            .await;
            if let Err(error) = admitted {
                transaction.rollback().await?;
                return Ok(Err(unavailable(&error.to_string())));
            }
            match operation(&mut transaction).await {
                Ok(value) => {
                    transaction.commit().await?;
                    Ok(Ok(value))
                }
                Err(WriteError::Rejected(error)) => {
                    transaction.rollback().await?;
                    Ok(Err(error))
                }
                Err(WriteError::Database(error)) => Err(error),
            }
        })
        .await
        .map_err(|_| unavailable("queue operation timed out"))?
        .map_err(|error: sqlx::Error| classify_sqlx(&error))?
    }
}

/// Remove a thread's queued items and record the change. The caller owns the transaction, which
/// also covers deleting the thread row so a watcher still sees the queue disappear.
pub async fn delete_thread_queue_in(
    connection: &mut PgConnection,
    thread_id: ThreadId,
) -> Result<bool, sqlx::Error> {
    let version = next_version(connection).await?;
    let deleted = sqlx::query("DELETE FROM queued_items WHERE thread_id = $1::uuid")
        .bind(thread_id.to_string())
        .execute(&mut *connection)
        .await?
        .rows_affected()
        > 0;
    if deleted {
        record_revision(connection, thread_id, version).await?;
    }
    Ok(deleted)
}

enum WriteError {
    /// A request the store rejects; the transaction is rolled back and the caller sees the error.
    Rejected(ThreadStoreError),
    Database(sqlx::Error),
}

impl From<sqlx::Error> for WriteError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}

/// Take the counter-row lock and return the version this transaction will publish.
async fn next_version(connection: &mut PgConnection) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "UPDATE queue_change_counter SET version = version + 1 \
         WHERE singleton RETURNING version",
    )
    .fetch_one(connection)
    .await
}

async fn record_revision(
    connection: &mut PgConnection,
    thread_id: ThreadId,
    version: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO queued_thread_revisions (thread_id, revision) \
         VALUES ($1::uuid, $2) \
         ON CONFLICT (thread_id) DO UPDATE SET revision = excluded.revision",
    )
    .bind(thread_id.to_string())
    .bind(version)
    .execute(connection)
    .await?;
    Ok(())
}

impl QueueStore for PostgresQueueStore {
    fn change_version(&self) -> ThreadStoreFuture<'_, i64> {
        Box::pin(async move {
            let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
            timeout(
                QUERY_TIMEOUT,
                sqlx::query_scalar("SELECT version FROM queue_change_counter")
                    .fetch_one(&mut *connection),
            )
            .await
            .map_err(|_| unavailable("queue operation timed out"))?
            .map_err(|error| classify_sqlx(&error))
        })
    }

    fn changes_since<'a>(
        &'a self,
        revision: i64,
        thread_ids: &'a [ThreadId],
    ) -> ThreadStoreFuture<'a, Vec<(ThreadId, i64)>> {
        Box::pin(async move {
            if thread_ids.is_empty() {
                return Ok(Vec::new());
            }
            let ids: Vec<String> = thread_ids.iter().map(ToString::to_string).collect();
            let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
            let rows = timeout(
                QUERY_TIMEOUT,
                sqlx::query(
                    "SELECT thread_id::text AS thread_id, revision \
                     FROM queued_thread_revisions \
                     WHERE revision > $1 AND thread_id = ANY($2::uuid[]) \
                     ORDER BY revision",
                )
                .bind(revision)
                .bind(ids)
                .fetch_all(&mut *connection),
            )
            .await
            .map_err(|_| unavailable("queue operation timed out"))?
            .map_err(|error| classify_sqlx(&error))?;
            rows.into_iter()
                .map(|row| {
                    let thread_id: String = row
                        .try_get("thread_id")
                        .map_err(|error| classify_sqlx(&error))?;
                    let revision: i64 = row
                        .try_get("revision")
                        .map_err(|error| classify_sqlx(&error))?;
                    let thread_id = ThreadId::try_from(thread_id)
                        .map_err(|_| unavailable("invalid thread id in queue revision"))?;
                    Ok((thread_id, revision))
                })
                .collect()
        })
    }

    fn enqueue(
        &self,
        thread_id: ThreadId,
        payload: String,
    ) -> ThreadStoreFuture<'_, QueuedUserSubmissionRecord> {
        Box::pin(async move {
            self.write(|connection| {
                Box::pin(async move {
                    let version = next_version(connection).await?;
                    let queued: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM queued_items WHERE thread_id = $1::uuid",
                    )
                    .bind(thread_id.to_string())
                    .fetch_one(&mut *connection)
                    .await?;
                    if queued >= i64::try_from(MAX_QUEUE_ITEMS).unwrap_or(i64::MAX) {
                        return Err(WriteError::Rejected(ThreadStoreError::InvalidRequest {
                            message: format!(
                                "queue cannot contain more than {MAX_QUEUE_ITEMS} submissions"
                            ),
                        }));
                    }
                    let now_ms = Utc::now().timestamp_millis();
                    let row = sqlx::query(
                        "INSERT INTO queued_items \
                         (id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms) \
                         VALUES ($1, $2::uuid, $3, \
                           COALESCE((SELECT MAX(queue_order) FROM queued_items \
                                     WHERE thread_id = $2::uuid), -1) + 1, $4, $4) \
                         RETURNING id, thread_id::text AS thread_id, payload_json",
                    )
                    .bind(Uuid::now_v7().to_string())
                    .bind(thread_id.to_string())
                    .bind(&payload)
                    .bind(now_ms)
                    .fetch_one(&mut *connection)
                    .await
                    .map_err(|error| missing_thread(&error, thread_id))?;
                    record_revision(connection, thread_id, version).await?;
                    record_from_row(&row).map_err(WriteError::Rejected)
                })
            })
            .await
        })
    }

    fn list_page(
        &self,
        thread_id: ThreadId,
        offset: usize,
        limit: usize,
    ) -> ThreadStoreFuture<'_, Vec<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);
            let offset = i64::try_from(offset).unwrap_or(i64::MAX);
            let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
            let rows = timeout(
                QUERY_TIMEOUT,
                sqlx::query(
                    "SELECT id, thread_id::text AS thread_id, payload_json \
                     FROM queued_items WHERE thread_id = $1::uuid \
                     ORDER BY queue_order LIMIT $2 OFFSET $3",
                )
                .bind(thread_id.to_string())
                .bind(limit)
                .bind(offset)
                .fetch_all(&mut *connection),
            )
            .await
            .map_err(|_| unavailable("queue operation timed out"))?
            .map_err(|error| classify_sqlx(&error))?;
            rows.iter().map(record_from_row).collect()
        })
    }

    fn update(
        &self,
        thread_id: ThreadId,
        item_id: String,
        payload: String,
    ) -> ThreadStoreFuture<'_, Option<QueuedUserSubmissionRecord>> {
        Box::pin(async move {
            self.write(|connection| {
                Box::pin(async move {
                    let version = next_version(connection).await?;
                    let row = sqlx::query(
                        "UPDATE queued_items \
                         SET payload_json = $1, updated_at_ms = $2 \
                         WHERE thread_id = $3::uuid AND id = $4 \
                         RETURNING id, thread_id::text AS thread_id, payload_json",
                    )
                    .bind(&payload)
                    .bind(Utc::now().timestamp_millis())
                    .bind(thread_id.to_string())
                    .bind(&item_id)
                    .fetch_optional(&mut *connection)
                    .await?;
                    let Some(row) = row else {
                        return Ok(None);
                    };
                    record_revision(connection, thread_id, version).await?;
                    record_from_row(&row)
                        .map(Some)
                        .map_err(WriteError::Rejected)
                })
            })
            .await
        })
    }

    fn delete(&self, thread_id: ThreadId, item_id: String) -> ThreadStoreFuture<'_, bool> {
        Box::pin(async move {
            self.write(|connection| {
                Box::pin(async move {
                    let version = next_version(connection).await?;
                    let deleted = sqlx::query(
                        "DELETE FROM queued_items \
                         WHERE thread_id = $1::uuid AND id = $2",
                    )
                    .bind(thread_id.to_string())
                    .bind(&item_id)
                    .execute(&mut *connection)
                    .await?
                    .rows_affected()
                        > 0;
                    if deleted {
                        record_revision(connection, thread_id, version).await?;
                    }
                    Ok(deleted)
                })
            })
            .await
        })
    }

    fn reorder(&self, thread_id: ThreadId, item_ids: Vec<String>) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.write(|connection| {
                Box::pin(async move {
                    let version = next_version(connection).await?;
                    let rows: Vec<(String, i64)> = sqlx::query_as(
                        "SELECT id, queue_order FROM queued_items \
                         WHERE thread_id = $1::uuid ORDER BY queue_order FOR UPDATE",
                    )
                    .bind(thread_id.to_string())
                    .fetch_all(&mut *connection)
                    .await?;
                    let mut expected: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
                    let mut requested: Vec<&str> = item_ids.iter().map(String::as_str).collect();
                    expected.sort_unstable();
                    requested.sort_unstable();
                    if expected != requested {
                        return Err(WriteError::Rejected(ThreadStoreError::InvalidRequest {
                            message:
                                "queue reorder must include every queued submission exactly once"
                                    .to_string(),
                        }));
                    }
                    let now_ms = Utc::now().timestamp_millis();
                    // New positions start above every existing one, so the unique order index
                    // never sees two rows with the same position.
                    let base = rows.last().map_or(-1, |(_, order)| *order);
                    for (index, item_id) in item_ids.iter().enumerate() {
                        sqlx::query(
                            "UPDATE queued_items \
                             SET queue_order = $1, updated_at_ms = $2 \
                             WHERE thread_id = $3::uuid AND id = $4",
                        )
                        .bind(base + i64::try_from(index).unwrap_or(i64::MAX) + 1)
                        .bind(now_ms)
                        .bind(thread_id.to_string())
                        .bind(item_id)
                        .execute(&mut *connection)
                        .await?;
                    }
                    if !item_ids.is_empty() {
                        record_revision(connection, thread_id, version).await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }
}

fn record_from_row(row: &PgRow) -> Result<QueuedUserSubmissionRecord, ThreadStoreError> {
    let invalid = |_| unavailable("invalid queued submission record");
    let thread_id: String = row.try_get("thread_id").map_err(invalid)?;
    Ok(QueuedUserSubmissionRecord {
        id: row.try_get("id").map_err(invalid)?,
        thread_id: ThreadId::try_from(thread_id)
            .map_err(|_| unavailable("invalid queued submission thread id"))?,
        payload: row.try_get("payload_json").map_err(invalid)?,
    })
}

fn missing_thread(error: &sqlx::Error, thread_id: ThreadId) -> WriteError {
    match error {
        sqlx::Error::Database(database) if database.code().as_deref() == Some("23503") => {
            WriteError::Rejected(ThreadStoreError::ThreadNotFound { thread_id })
        }
        other => WriteError::Database(clone_error(other)),
    }
}

/// `sqlx::Error` is not `Clone`; only its classification matters after this point.
fn clone_error(error: &sqlx::Error) -> sqlx::Error {
    match error {
        sqlx::Error::Database(database) => sqlx::Error::Protocol(
            database
                .code()
                .map(|code| format!("SQLSTATE {code}"))
                .unwrap_or_else(|| "database error".to_string()),
        ),
        _ => sqlx::Error::Protocol("database error".to_string()),
    }
}

fn unavailable(message: &str) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("queue storage failed: {message}"),
    }
}

fn classify_pool(error: PoolError) -> ThreadStoreError {
    match error {
        PoolError::Timeout => unavailable("connection timed out"),
        PoolError::InvalidSettings
        | PoolError::Authentication
        | PoolError::Tls
        | PoolError::Unavailable
        | PoolError::Closed
        | PoolError::UnsupportedServer => unavailable("storage unavailable"),
    }
}

fn classify_sqlx(error: &sqlx::Error) -> ThreadStoreError {
    match error {
        sqlx::Error::Database(database) => match database.code().as_deref() {
            Some("23503") => unavailable("thread has no PostgreSQL thread row"),
            _ => unavailable("database operation failed"),
        },
        _ => unavailable("database operation failed"),
    }
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod tests;
