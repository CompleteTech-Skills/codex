use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use codex_postgres_runtime::PostgresPool;
use codex_state::LogEntry;
use codex_state::LogQuery;
use codex_state::LogRow;
use codex_state::LogStoreFuture;
use codex_state::RuntimeLogStore;
use sqlx::Acquire;
use sqlx::PgConnection;
use sqlx::Postgres;
use sqlx::QueryBuilder;
use sqlx::Row;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Retained log content per partition, matching the SQLite store.
const LOG_PARTITION_SIZE_LIMIT_BYTES: i64 = 10 * 1024 * 1024;
const LOG_PARTITION_ROW_LIMIT: i64 = 1_000;
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Fixed-namespace runtime log adapter. Construction does not activate PostgreSQL.
///
/// Each batch takes the lock on the single id-counter row, allocates a contiguous id range and
/// prunes the affected partitions before it commits. Ids therefore become visible in commit
/// order, so a reader that polls for ids above a known one cannot skip a row that commits late.
#[derive(Clone)]
pub struct PostgresLogStore {
    pool: Arc<PostgresPool>,
}

impl PostgresLogStore {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }

    async fn insert(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let count = i64::try_from(entries.len()).context("log batch is too large")?;
        let mut connection = self.pool.acquire().await?;
        timeout(QUERY_TIMEOUT, async {
            let mut tx = connection.begin().await?;
            self.pool.prepare_selected_write(&mut tx).await?;
            self.pool.require_bound_storage_open(&mut tx).await?;
            let last_id: i64 = sqlx::query_scalar(
                "UPDATE log_id_counter SET last_id = last_id + $1 \
                 WHERE singleton RETURNING last_id",
            )
            .bind(count)
            .fetch_one(&mut *tx)
            .await?;
            let first_id = last_id - count + 1;
            let mut builder = QueryBuilder::<Postgres>::new(
                "INSERT INTO logs (id, ts, ts_nanos, level, target, \
                 feedback_log_body, thread_id, process_uuid, module_path, file, line, \
                 estimated_bytes) ",
            );
            let mut id = first_id;
            builder.push_values(entries, |mut row, entry| {
                row.push_bind(id)
                    .push_bind(entry.ts)
                    .push_bind(entry.ts_nanos)
                    .push_bind(&entry.level)
                    .push_bind(&entry.target)
                    .push_bind(entry.feedback_log_body.as_ref().or(entry.message.as_ref()))
                    .push_bind(&entry.thread_id)
                    .push_bind(&entry.process_uuid)
                    .push_bind(&entry.module_path)
                    .push_bind(&entry.file)
                    .push_bind(entry.line)
                    .push_bind(estimated_bytes(entry));
                id += 1;
            });
            builder.build().execute(&mut *tx).await?;
            prune_after_insert(entries, &mut tx).await?;
            tx.commit().await?;
            anyhow::Ok(())
        })
        .await
        .context("log insert timed out")?
    }

    async fn query(&self, query: &LogQuery) -> Result<Vec<LogRow>> {
        let mut builder = QueryBuilder::<Postgres>::new(
            "SELECT id, ts, ts_nanos, level, target, feedback_log_body AS message, thread_id, \
             process_uuid, file, line FROM logs WHERE TRUE",
        );
        push_log_filters(&mut builder, query);
        builder.push(if query.descending {
            " ORDER BY id DESC"
        } else {
            " ORDER BY id ASC"
        });
        if let Some(limit) = query.limit {
            builder
                .push(" LIMIT ")
                .push_bind(i64::try_from(limit).unwrap_or(i64::MAX));
        }
        let mut connection = self.pool.acquire().await?;
        let rows = timeout(QUERY_TIMEOUT, builder.build().fetch_all(&mut *connection))
            .await
            .context("log query timed out")??;
        rows.iter()
            .map(|row| {
                Ok(LogRow {
                    id: row.try_get("id")?,
                    ts: row.try_get("ts")?,
                    ts_nanos: row.try_get("ts_nanos")?,
                    level: row.try_get("level")?,
                    target: row.try_get("target")?,
                    message: row.try_get("message")?,
                    thread_id: row.try_get("thread_id")?,
                    process_uuid: row.try_get("process_uuid")?,
                    file: row.try_get("file")?,
                    line: row.try_get("line")?,
                })
            })
            .collect()
    }

    async fn feedback_logs(&self, thread_ids: &[&str]) -> Result<Vec<u8>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }
        let requested: Vec<String> = thread_ids.iter().map(ToString::to_string).collect();
        let max_bytes = usize::try_from(LOG_PARTITION_SIZE_LIMIT_BYTES).unwrap_or(usize::MAX);
        // Bound the fetched rows in SQL first so over-retained partitions do not load every
        // row, then apply the exact whole-line byte cap after formatting.
        let mut connection = self.pool.acquire().await?;
        let rows = timeout(
            QUERY_TIMEOUT,
            sqlx::query(
                r#"
WITH requested_threads(thread_id) AS (
    SELECT DISTINCT unnest($1::text[])
),
latest_processes AS (
    SELECT (
        SELECT process_uuid
        FROM logs
        WHERE logs.thread_id = requested_threads.thread_id AND process_uuid IS NOT NULL
        ORDER BY ts DESC, ts_nanos DESC, id DESC
        LIMIT 1
    ) AS process_uuid
    FROM requested_threads
),
feedback_logs AS (
    SELECT ts, ts_nanos, level, feedback_log_body, estimated_bytes, id
    FROM logs
    WHERE feedback_log_body IS NOT NULL AND (
        thread_id IN (SELECT thread_id FROM requested_threads)
        OR (
            thread_id IS NULL
            AND process_uuid IN (
                SELECT process_uuid FROM latest_processes WHERE process_uuid IS NOT NULL
            )
        )
    )
),
bounded_feedback_logs AS (
    SELECT
        ts,
        ts_nanos,
        level,
        feedback_log_body,
        id,
        SUM(estimated_bytes) OVER (
            ORDER BY ts DESC, ts_nanos DESC, id DESC
        ) AS cumulative_estimated_bytes
    FROM feedback_logs
)
SELECT ts, ts_nanos, level, feedback_log_body
FROM bounded_feedback_logs
WHERE cumulative_estimated_bytes <= $2
ORDER BY ts DESC, ts_nanos DESC, id DESC
"#,
            )
            .bind(requested)
            .bind(LOG_PARTITION_SIZE_LIMIT_BYTES)
            .fetch_all(&mut *connection),
        )
        .await
        .context("feedback log query timed out")??;

        let mut lines = Vec::new();
        let mut total_bytes = 0usize;
        for row in rows {
            let line = format_feedback_log_line(
                row.try_get("ts")?,
                row.try_get("ts_nanos")?,
                &row.try_get::<String, _>("level")?,
                &row.try_get::<String, _>("feedback_log_body")?,
            );
            if total_bytes.saturating_add(line.len()) > max_bytes {
                break;
            }
            total_bytes += line.len();
            lines.push(line);
        }
        let mut ordered_bytes = Vec::with_capacity(total_bytes);
        for line in lines.into_iter().rev() {
            ordered_bytes.extend_from_slice(line.as_bytes());
        }
        Ok(ordered_bytes)
    }

    async fn max_id(&self, query: &LogQuery) -> Result<i64> {
        let mut builder =
            QueryBuilder::<Postgres>::new("SELECT MAX(id) AS max_id FROM logs WHERE TRUE");
        push_log_filters(&mut builder, query);
        let mut connection = self.pool.acquire().await?;
        let row = timeout(QUERY_TIMEOUT, builder.build().fetch_one(&mut *connection))
            .await
            .context("log max id query timed out")??;
        Ok(row.try_get::<Option<i64>, _>("max_id")?.unwrap_or(0))
    }
}

impl RuntimeLogStore for PostgresLogStore {
    fn insert_logs<'a>(&'a self, entries: &'a [LogEntry]) -> LogStoreFuture<'a, ()> {
        Box::pin(self.insert(entries))
    }

    fn query_logs<'a>(&'a self, query: &'a LogQuery) -> LogStoreFuture<'a, Vec<LogRow>> {
        Box::pin(self.query(query))
    }

    fn query_feedback_logs_for_threads<'a>(
        &'a self,
        thread_ids: &'a [&str],
    ) -> LogStoreFuture<'a, Vec<u8>> {
        Box::pin(self.feedback_logs(thread_ids))
    }

    fn max_log_id<'a>(&'a self, query: &'a LogQuery) -> LogStoreFuture<'a, i64> {
        Box::pin(self.max_id(query))
    }
}

/// Keep this equal to the SQLite store's size estimate so both retain the same rows.
fn estimated_bytes(entry: &LogEntry) -> i64 {
    let body = entry.feedback_log_body.as_ref().or(entry.message.as_ref());
    body.map_or(0, String::len) as i64
        + entry.level.len() as i64
        + entry.target.len() as i64
        + entry.module_path.as_ref().map_or(0, String::len) as i64
        + entry.file.as_ref().map_or(0, String::len) as i64
}

/// Enforce the retained-content caps for every partition touched by a batch.
///
/// Thread logs are capped per thread. Threadless logs are capped per process, and rows with no
/// process form their own partition. Pruning runs in the insert's transaction so readers never
/// observe inserted-but-unpruned rows.
async fn prune_after_insert(entries: &[LogEntry], tx: &mut PgConnection) -> Result<()> {
    let thread_ids: Vec<&str> = entries
        .iter()
        .filter_map(|entry| entry.thread_id.as_deref())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if !thread_ids.is_empty() {
        sqlx::query(PRUNE_THREADS)
            .bind(thread_ids)
            .bind(LOG_PARTITION_SIZE_LIMIT_BYTES)
            .bind(LOG_PARTITION_ROW_LIMIT)
            .execute(&mut *tx)
            .await?;
    }
    let process_uuids: Vec<&str> = entries
        .iter()
        .filter(|entry| entry.thread_id.is_none())
        .filter_map(|entry| entry.process_uuid.as_deref())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if !process_uuids.is_empty() {
        sqlx::query(PRUNE_PROCESSES)
            .bind(process_uuids)
            .bind(LOG_PARTITION_SIZE_LIMIT_BYTES)
            .bind(LOG_PARTITION_ROW_LIMIT)
            .execute(&mut *tx)
            .await?;
    }
    if entries
        .iter()
        .any(|entry| entry.thread_id.is_none() && entry.process_uuid.is_none())
    {
        sqlx::query(PRUNE_NULL_PROCESS)
            .bind(LOG_PARTITION_SIZE_LIMIT_BYTES)
            .bind(LOG_PARTITION_ROW_LIMIT)
            .execute(&mut *tx)
            .await?;
    }
    Ok(())
}

const PRUNE_THREADS: &str = r#"
DELETE FROM logs
WHERE id IN (
    SELECT id FROM (
        SELECT
            id,
            SUM(estimated_bytes) OVER w AS cumulative_bytes,
            ROW_NUMBER() OVER w AS row_number
        FROM logs
        WHERE thread_id = ANY($1::text[])
        WINDOW w AS (PARTITION BY thread_id ORDER BY ts DESC, ts_nanos DESC, id DESC)
    ) ranked
    WHERE cumulative_bytes > $2 OR row_number > $3
)
"#;

const PRUNE_PROCESSES: &str = r#"
DELETE FROM logs
WHERE id IN (
    SELECT id FROM (
        SELECT
            id,
            SUM(estimated_bytes) OVER w AS cumulative_bytes,
            ROW_NUMBER() OVER w AS row_number
        FROM logs
        WHERE thread_id IS NULL AND process_uuid = ANY($1::text[])
        WINDOW w AS (PARTITION BY process_uuid ORDER BY ts DESC, ts_nanos DESC, id DESC)
    ) ranked
    WHERE cumulative_bytes > $2 OR row_number > $3
)
"#;

const PRUNE_NULL_PROCESS: &str = r#"
DELETE FROM logs
WHERE id IN (
    SELECT id FROM (
        SELECT
            id,
            SUM(estimated_bytes) OVER w AS cumulative_bytes,
            ROW_NUMBER() OVER w AS row_number
        FROM logs
        WHERE thread_id IS NULL AND process_uuid IS NULL
        WINDOW w AS (ORDER BY ts DESC, ts_nanos DESC, id DESC)
    ) ranked
    WHERE cumulative_bytes > $1 OR row_number > $2
)
"#;

fn format_feedback_log_line(
    ts: i64,
    ts_nanos: i64,
    level: &str,
    feedback_log_body: &str,
) -> String {
    let nanos = u32::try_from(ts_nanos).unwrap_or(0);
    let timestamp = match DateTime::<Utc>::from_timestamp(ts, nanos) {
        Some(dt) => dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
        None => format!("{ts}.{ts_nanos:09}Z"),
    };
    let mut line = format!("{timestamp} {level:>5} {feedback_log_body}");
    if !line.ends_with('\n') {
        line.push('\n');
    }
    line
}

fn push_log_filters(builder: &mut QueryBuilder<Postgres>, query: &LogQuery) {
    if !query.levels_upper.is_empty() {
        builder.push(" AND UPPER(level) IN (");
        {
            let mut separated = builder.separated(", ");
            for level_upper in &query.levels_upper {
                separated.push_bind(level_upper.as_str());
            }
        }
        builder.push(")");
    }
    if let Some(from_ts) = query.from_ts {
        builder.push(" AND ts >= ").push_bind(from_ts);
    }
    if let Some(to_ts) = query.to_ts {
        builder.push(" AND ts <= ").push_bind(to_ts);
    }
    push_like_filters(builder, "module_path", &query.module_like);
    push_like_filters(builder, "file", &query.file_like);
    if !query.thread_ids.is_empty() || query.include_threadless {
        builder.push(" AND (");
        let mut needs_or = false;
        for thread_id in &query.thread_ids {
            if needs_or {
                builder.push(" OR ");
            }
            builder.push("thread_id = ").push_bind(thread_id.as_str());
            needs_or = true;
        }
        if query.include_threadless {
            if needs_or {
                builder.push(" OR ");
            }
            builder.push("thread_id IS NULL");
        }
        builder.push(")");
    }
    if let Some(after_id) = query.after_id {
        builder.push(" AND id > ").push_bind(after_id);
    }
    if let Some(search) = query.search.as_ref() {
        builder.push(" AND strpos(COALESCE(feedback_log_body, ''), ");
        builder.push_bind(search.as_str());
        builder.push(") > 0");
    }
}

/// SQLite `LIKE` ignores ASCII case, so the PostgreSQL store uses `ILIKE` to select the same rows.
fn push_like_filters(builder: &mut QueryBuilder<Postgres>, column: &str, filters: &[String]) {
    if filters.is_empty() {
        return;
    }
    builder.push(" AND (");
    for (idx, filter) in filters.iter().enumerate() {
        if idx > 0 {
            builder.push(" OR ");
        }
        builder
            .push(column)
            .push(" ILIKE '%' || ")
            .push_bind(filter.as_str())
            .push(" || '%'");
    }
    builder.push(")");
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod tests;
