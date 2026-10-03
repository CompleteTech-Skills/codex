use anyhow::Result;
use anyhow::anyhow;
use chrono::Duration;
use chrono::Utc;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_thread_rows::epoch_seconds_to_datetime;
use codex_postgres_thread_rows::thread_columns;
use codex_postgres_thread_rows::thread_metadata_from_row;
use codex_protocol::ThreadId;
use codex_state::MemoryStoreFuture;
use codex_state::Phase2JobClaimOutcome;
use codex_state::RuntimeMemoryStore;
use codex_state::Stage1JobClaim;
use codex_state::Stage1JobClaimOutcome;
use codex_state::Stage1Output;
use codex_state::Stage1StartupClaimParams;
use sqlx::Acquire;
use sqlx::PgConnection;
use sqlx::Row;
use sqlx::postgres::PgRow;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::time::timeout;
use uuid::Uuid;

const JOB_KIND_MEMORY_STAGE1: &str = "memory_stage1";
const JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL: &str = "memory_consolidate_global";
const MEMORY_CONSOLIDATION_JOB_KEY: &str = "global";
const PHASE2_SUCCESS_COOLDOWN_SECONDS: i64 = 6 * 60 * 60;
const DEFAULT_RETRY_REMAINING: i64 = 3;
const QUERY_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// Columns of a memory output joined with the thread fields it reports.
macro_rules! output_columns {
    () => {
        concat!(
            "so.thread_id::text AS thread_id, so.source_updated_at, so.raw_memory, ",
            "so.rollout_summary, so.rollout_slug, so.generated_at, ",
            "threads.origin_rollout_path, threads.origin_cwd, threads.git_branch"
        )
    };
}

type OperationFuture<'c, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>;

/// Fixed-namespace generated-memory adapter. Construction does not activate PostgreSQL.
///
/// Every mutation first locks the single consolidation-progress row. That serializes memory
/// work the way the SQLite store's single writer does, so the running-job limit holds and
/// transactions that touch several tables cannot deadlock each other.
#[derive(Clone)]
pub struct PostgresMemoryStore {
    pool: Arc<PostgresPool>,
}

impl PostgresMemoryStore {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }

    /// Run one operation in a transaction that holds the memory lock, bounded by a timeout.
    async fn run<T, F>(&self, operation: F) -> Result<T>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> OperationFuture<'c, T>,
    {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| anyhow!("PostgreSQL memory storage is unavailable: {error:?}"))?;
        timeout(QUERY_TIMEOUT, async {
            let mut tx = connection.begin().await?;
            self.pool.prepare_selected_write(&mut tx).await?;
            self.pool.require_bound_storage_open(&mut tx).await?;
            lock_memory_in(&mut tx).await?;
            let value = operation(&mut tx).await?;
            tx.commit().await?;
            anyhow::Ok(value)
        })
        .await
        .map_err(|_| anyhow!("PostgreSQL memory operation timed out"))?
    }

    async fn read_rows(
        &self,
        query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> Result<Vec<PgRow>> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| anyhow!("PostgreSQL memory storage is unavailable: {error:?}"))?;
        timeout(QUERY_TIMEOUT, query.fetch_all(&mut *connection))
            .await
            .map_err(|_| anyhow!("PostgreSQL memory query timed out"))?
            .map_err(Into::into)
    }

    async fn source_needs_update(
        &self,
        thread_id: ThreadId,
        source_updated_at: i64,
    ) -> Result<bool> {
        let rows = self
            .read_rows(
                sqlx::query(
                    "SELECT (SELECT source_updated_at FROM memory_stage1_outputs \
                             WHERE thread_id = $1::uuid) AS output_source_updated_at, \
                            (SELECT last_success_watermark FROM memory_jobs \
                             WHERE kind = $2 AND job_key = $1) AS last_success_watermark",
                )
                .bind(thread_id.to_string())
                .bind(JOB_KIND_MEMORY_STAGE1),
            )
            .await?;
        let row = &rows[0];
        let output: Option<i64> = row.try_get("output_source_updated_at")?;
        let watermark: Option<i64> = row.try_get("last_success_watermark")?;
        Ok(output.is_none_or(|existing| existing < source_updated_at)
            && watermark.is_none_or(|existing| existing < source_updated_at))
    }
}

fn stage1_output_from_row(row: &PgRow) -> Result<Stage1Output> {
    let thread_id: String = row.try_get("thread_id")?;
    Ok(Stage1Output {
        thread_id: ThreadId::try_from(thread_id)?,
        rollout_path: PathBuf::from(row.try_get::<String, _>("origin_rollout_path")?),
        source_updated_at: epoch_seconds_to_datetime(row.try_get("source_updated_at")?)?,
        raw_memory: row.try_get("raw_memory")?,
        rollout_summary: row.try_get("rollout_summary")?,
        rollout_slug: row.try_get("rollout_slug")?,
        cwd: PathBuf::from(row.try_get::<String, _>("origin_cwd")?),
        git_branch: row.try_get("git_branch")?,
        generated_at: epoch_seconds_to_datetime(row.try_get("generated_at")?)?,
    })
}

/// Take the memory lock that serializes memory work. Callers that combine memory changes with
/// other tables lock it first, so every transaction acquires locks in the same order.
pub async fn lock_memory_in(connection: &mut PgConnection) -> Result<()> {
    sqlx::query("SELECT 1 FROM memory_consolidation_progress WHERE singleton FOR UPDATE")
        .execute(connection)
        .await?;
    Ok(())
}

/// Delete one thread generated memory, queueing consolidation when it was part of the last
/// successful baseline. The caller owns the transaction and must hold the memory lock.
pub async fn delete_thread_memory_in(
    connection: &mut PgConnection,
    thread_id: ThreadId,
) -> Result<()> {
    let now = Utc::now().timestamp();
    let thread_id = thread_id.to_string();
    let was_selected = sqlx::query_scalar::<_, i64>(
        "SELECT selected_for_phase2 FROM memory_stage1_outputs WHERE thread_id = $1::uuid",
    )
    .bind(&thread_id)
    .fetch_optional(&mut *connection)
    .await?
    .is_some_and(|selected| selected != 0);
    let deleted = sqlx::query("DELETE FROM memory_stage1_outputs WHERE thread_id = $1::uuid")
        .bind(&thread_id)
        .execute(&mut *connection)
        .await?
        .rows_affected();
    sqlx::query("DELETE FROM memory_jobs WHERE kind = $1 AND job_key = $2")
        .bind(JOB_KIND_MEMORY_STAGE1)
        .bind(&thread_id)
        .execute(&mut *connection)
        .await?;
    if deleted > 0 && was_selected {
        enqueue_global_consolidation_in(connection, now).await?;
    }
    Ok(())
}

/// Enqueue or advance the global consolidation job. The job stays running when it already is,
/// pending and errored jobs become pending, and the watermark only moves forward.
async fn enqueue_global_consolidation_in(
    connection: &mut PgConnection,
    input_watermark: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO memory_jobs (kind, job_key, status, worker_id, \
         ownership_token, started_at, finished_at, lease_until, retry_at, retry_remaining, \
         last_error, input_watermark, last_success_watermark) \
         VALUES ($1, $2, 'pending', NULL, NULL, NULL, NULL, NULL, NULL, $3, NULL, $4, 0) \
         ON CONFLICT (kind, job_key) DO UPDATE SET \
           status = CASE WHEN memory_jobs.status = 'running' THEN 'running' ELSE 'pending' END, \
           retry_at = CASE WHEN memory_jobs.status = 'running' THEN memory_jobs.retry_at \
                           ELSE NULL END, \
           retry_remaining = GREATEST(memory_jobs.retry_remaining, excluded.retry_remaining), \
           input_watermark = CASE \
             WHEN excluded.input_watermark > COALESCE(memory_jobs.input_watermark, 0) \
               THEN excluded.input_watermark \
             ELSE COALESCE(memory_jobs.input_watermark, 0) + 1 END",
    )
    .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
    .bind(MEMORY_CONSOLIDATION_JOB_KEY)
    .bind(DEFAULT_RETRY_REMAINING)
    .bind(input_watermark)
    .execute(connection)
    .await?;
    Ok(())
}

impl RuntimeMemoryStore for PostgresMemoryStore {
    fn clear_memory_data(&self) -> MemoryStoreFuture<'_, ()> {
        Box::pin(self.run(|connection| {
            Box::pin(async move {
                sqlx::query("UPDATE memory_consolidation_progress SET max_thread_count = 0")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("DELETE FROM memory_stage1_outputs")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("DELETE FROM memory_jobs WHERE kind = $1 OR kind = $2")
                    .bind(JOB_KIND_MEMORY_STAGE1)
                    .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        }))
    }

    fn delete_thread_memory(&self, thread_id: ThreadId) -> MemoryStoreFuture<'_, ()> {
        Box::pin(
            self.run(move |connection| Box::pin(delete_thread_memory_in(connection, thread_id))),
        )
    }

    fn record_stage1_output_usage<'a>(
        &'a self,
        thread_ids: &'a [ThreadId],
    ) -> MemoryStoreFuture<'a, usize> {
        Box::pin(async move {
            if thread_ids.is_empty() {
                return Ok(0);
            }
            let ids: Vec<String> = thread_ids.iter().map(ToString::to_string).collect();
            self.run(move |connection| {
                Box::pin(async move {
                    let now = Utc::now().timestamp();
                    let mut updated = 0;
                    for thread_id in &ids {
                        updated += sqlx::query(
                            "UPDATE memory_stage1_outputs \
                             SET usage_count = COALESCE(usage_count, 0) + 1, last_usage = $1 \
                             WHERE thread_id = $2::uuid",
                        )
                        .bind(now)
                        .bind(thread_id)
                        .execute(&mut *connection)
                        .await?
                        .rows_affected() as usize;
                    }
                    Ok(updated)
                })
            })
            .await
        })
    }

    fn claim_stage1_jobs_for_startup<'a>(
        &'a self,
        current_thread_id: ThreadId,
        params: Stage1StartupClaimParams<'a>,
    ) -> MemoryStoreFuture<'a, Vec<Stage1JobClaim>> {
        Box::pin(async move {
            let Stage1StartupClaimParams {
                scan_limit,
                max_claimed,
                max_age_days,
                min_rollout_idle_hours,
                allowed_sources,
                lease_seconds,
            } = params;
            if scan_limit == 0 || max_claimed == 0 {
                return Ok(Vec::new());
            }
            let max_age_cutoff =
                (Utc::now() - Duration::days(max_age_days.max(0))).timestamp_millis();
            let idle_cutoff =
                (Utc::now() - Duration::hours(min_rollout_idle_hours.max(0))).timestamp_millis();
            let rows = self
                .read_rows(
                    sqlx::query(concat!(
                        "SELECT ",
                        thread_columns!(),
                        " FROM threads \
                         WHERE threads.archived_at_s IS NULL \
                           AND COALESCE(threads.preview, '') <> '' \
                           AND (cardinality($1::text[]) = 0 OR threads.source = ANY($1::text[])) \
                           AND threads.memory_mode = 'enabled' \
                           AND threads.id <> $2::uuid \
                           AND threads.updated_at_ms >= $3 \
                           AND threads.updated_at_ms <= $4 \
                         ORDER BY threads.updated_at_ms DESC LIMIT $5"
                    ))
                    .bind(allowed_sources.to_vec())
                    .bind(current_thread_id.to_string())
                    .bind(max_age_cutoff)
                    .bind(idle_cutoff)
                    .bind(i64::try_from(scan_limit).unwrap_or(i64::MAX)),
                )
                .await?;

            let mut claimed = Vec::new();
            for row in &rows {
                if claimed.len() >= max_claimed {
                    break;
                }
                let thread = thread_metadata_from_row(row)?;
                let source_updated_at = thread.updated_at.timestamp();
                if !self
                    .source_needs_update(thread.id, source_updated_at)
                    .await?
                {
                    continue;
                }
                if let Stage1JobClaimOutcome::Claimed { ownership_token } = self
                    .try_claim_stage1_job(
                        thread.id,
                        current_thread_id,
                        source_updated_at,
                        lease_seconds,
                        max_claimed,
                    )
                    .await?
                {
                    claimed.push(Stage1JobClaim {
                        thread,
                        ownership_token,
                    });
                }
            }
            Ok(claimed)
        })
    }

    fn list_stage1_outputs_for_global(&self, n: usize) -> MemoryStoreFuture<'_, Vec<Stage1Output>> {
        Box::pin(async move {
            if n == 0 {
                return Ok(Vec::new());
            }
            let rows = self
                .read_rows(
                    sqlx::query(concat!(
                        "SELECT ",
                        output_columns!(),
                        " FROM memory_stage1_outputs AS so \
                         JOIN threads \
                           ON threads.id = so.thread_id AND threads.memory_mode = 'enabled' \
                         WHERE length(trim(so.raw_memory)) > 0 \
                            OR length(trim(so.rollout_summary)) > 0 \
                         ORDER BY so.source_updated_at DESC, so.thread_id DESC LIMIT $1"
                    ))
                    .bind(i64::try_from(n).unwrap_or(i64::MAX)),
                )
                .await?;
            rows.iter().map(stage1_output_from_row).collect()
        })
    }

    fn prune_stage1_outputs_for_retention(
        &self,
        max_unused_days: i64,
        limit: usize,
    ) -> MemoryStoreFuture<'_, usize> {
        Box::pin(async move {
            if limit == 0 {
                return Ok(0);
            }
            let cutoff = (Utc::now() - Duration::days(max_unused_days.max(0))).timestamp();
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);
            self.run(move |connection| {
                Box::pin(async move {
                    Ok(sqlx::query(
                        "DELETE FROM memory_stage1_outputs WHERE thread_id IN ( \
                           SELECT thread_id FROM memory_stage1_outputs \
                           WHERE selected_for_phase2 = 0 \
                             AND COALESCE(last_usage, source_updated_at) < $1 \
                           ORDER BY COALESCE(last_usage, source_updated_at) ASC, \
                                    source_updated_at ASC, thread_id ASC \
                           LIMIT $2)",
                    )
                    .bind(cutoff)
                    .bind(limit)
                    .execute(&mut *connection)
                    .await?
                    .rows_affected() as usize)
                })
            })
            .await
        })
    }

    fn get_phase2_input_selection(
        &self,
        n: usize,
        max_unused_days: i64,
    ) -> MemoryStoreFuture<'_, Vec<Stage1Output>> {
        Box::pin(async move {
            if n == 0 {
                return Ok(Vec::new());
            }
            let cutoff = (Utc::now() - Duration::days(max_unused_days.max(0))).timestamp();
            let rows = self
                .read_rows(
                    sqlx::query(concat!(
                        "SELECT ",
                        output_columns!(),
                        " FROM memory_stage1_outputs AS so \
                         JOIN threads \
                           ON threads.id = so.thread_id AND threads.memory_mode = 'enabled' \
                         WHERE (length(trim(so.raw_memory)) > 0 \
                                OR length(trim(so.rollout_summary)) > 0) \
                           AND ((so.last_usage IS NOT NULL AND so.last_usage >= $1) \
                                OR (so.last_usage IS NULL AND so.source_updated_at >= $1)) \
                         ORDER BY COALESCE(so.usage_count, 0) DESC, \
                                  COALESCE(so.last_usage, so.source_updated_at) DESC, \
                                  so.source_updated_at DESC, so.thread_id DESC \
                         LIMIT $2"
                    ))
                    .bind(cutoff)
                    .bind(i64::try_from(n).unwrap_or(i64::MAX)),
                )
                .await?;
            let mut selected = rows
                .iter()
                .map(stage1_output_from_row)
                .collect::<Result<Vec<_>>>()?;
            selected.sort_by_key(|entry| entry.thread_id.to_string());
            Ok(selected)
        })
    }

    fn mark_thread_memory_mode_polluted(&self, thread_id: ThreadId) -> MemoryStoreFuture<'_, bool> {
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                let thread_id = thread_id.to_string();
                let selected = sqlx::query_scalar::<_, i64>(
                    "SELECT selected_for_phase2 FROM memory_stage1_outputs \
                     WHERE thread_id = $1::uuid",
                )
                .bind(&thread_id)
                .fetch_optional(&mut *connection)
                .await?
                .unwrap_or(0);
                let changed = sqlx::query(
                    "UPDATE threads SET memory_mode = 'polluted' \
                     WHERE id = $1::uuid AND memory_mode <> 'polluted'",
                )
                .bind(&thread_id)
                .execute(&mut *connection)
                .await?
                .rows_affected();
                if selected != 0 {
                    enqueue_global_consolidation_in(connection, now).await?;
                }
                Ok(changed > 0)
            })
        }))
    }

    fn try_claim_stage1_job(
        &self,
        thread_id: ThreadId,
        worker_id: ThreadId,
        source_updated_at: i64,
        lease_seconds: i64,
        max_running_jobs: usize,
    ) -> MemoryStoreFuture<'_, Stage1JobClaimOutcome> {
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                let lease_until = now.saturating_add(lease_seconds.max(0));
                let ownership_token = Uuid::new_v4().to_string();
                let thread_id = thread_id.to_string();
                let status = sqlx::query(
                    "SELECT (SELECT source_updated_at FROM memory_stage1_outputs \
                             WHERE thread_id = $1::uuid) AS output_source_updated_at, \
                            (SELECT last_success_watermark FROM memory_jobs \
                             WHERE kind = $2 AND job_key = $1) AS last_success_watermark",
                )
                .bind(&thread_id)
                .bind(JOB_KIND_MEMORY_STAGE1)
                .fetch_one(&mut *connection)
                .await?;
                let output: Option<i64> = status.try_get("output_source_updated_at")?;
                let watermark: Option<i64> = status.try_get("last_success_watermark")?;
                if output.is_some_and(|existing| existing >= source_updated_at)
                    || watermark.is_some_and(|existing| existing >= source_updated_at)
                {
                    return Ok(Stage1JobClaimOutcome::SkippedUpToDate);
                }

                let claimed = sqlx::query(
                    "INSERT INTO memory_jobs (kind, job_key, status, worker_id, \
                     ownership_token, started_at, finished_at, lease_until, retry_at, \
                     retry_remaining, last_error, input_watermark, last_success_watermark) \
                     SELECT $1, $2, 'running', $3, $4, $5::bigint, NULL, $6::bigint, NULL, \
                            $7::bigint, NULL, $8::bigint, NULL \
                     WHERE (SELECT COUNT(*) FROM memory_jobs \
                            WHERE kind = $1 AND status = 'running' \
                              AND lease_until IS NOT NULL AND lease_until > $5) < $9 \
                     ON CONFLICT (kind, job_key) DO UPDATE SET \
                       status = 'running', \
                       worker_id = excluded.worker_id, \
                       ownership_token = excluded.ownership_token, \
                       started_at = excluded.started_at, \
                       finished_at = NULL, \
                       lease_until = excluded.lease_until, \
                       retry_at = NULL, \
                       retry_remaining = CASE \
                         WHEN excluded.input_watermark > COALESCE(memory_jobs.input_watermark, -1) \
                           THEN excluded.retry_remaining \
                         ELSE memory_jobs.retry_remaining END, \
                       last_error = NULL, \
                       input_watermark = excluded.input_watermark \
                     WHERE (memory_jobs.status <> 'running' OR memory_jobs.lease_until IS NULL \
                            OR memory_jobs.lease_until <= excluded.started_at) \
                       AND (memory_jobs.retry_at IS NULL \
                            OR memory_jobs.retry_at <= excluded.started_at \
                            OR excluded.input_watermark > COALESCE(memory_jobs.input_watermark, -1)) \
                       AND (memory_jobs.retry_remaining > 0 \
                            OR excluded.input_watermark > COALESCE(memory_jobs.input_watermark, -1)) \
                       AND (SELECT COUNT(*) FROM memory_jobs AS running_jobs \
                            WHERE running_jobs.kind = excluded.kind \
                              AND running_jobs.status = 'running' \
                              AND running_jobs.lease_until IS NOT NULL \
                              AND running_jobs.lease_until > excluded.started_at \
                              AND running_jobs.job_key <> excluded.job_key) < $9",
                )
                .bind(JOB_KIND_MEMORY_STAGE1)
                .bind(&thread_id)
                .bind(worker_id.to_string())
                .bind(&ownership_token)
                .bind(now)
                .bind(lease_until)
                .bind(DEFAULT_RETRY_REMAINING)
                .bind(source_updated_at)
                .bind(i64::try_from(max_running_jobs).unwrap_or(i64::MAX))
                .execute(&mut *connection)
                .await?
                .rows_affected();
                if claimed > 0 {
                    return Ok(Stage1JobClaimOutcome::Claimed { ownership_token });
                }

                let existing = sqlx::query(
                    "SELECT status, lease_until, retry_at, retry_remaining \
                     FROM memory_jobs WHERE kind = $1 AND job_key = $2",
                )
                .bind(JOB_KIND_MEMORY_STAGE1)
                .bind(&thread_id)
                .fetch_optional(&mut *connection)
                .await?;
                let Some(existing) = existing else {
                    return Ok(Stage1JobClaimOutcome::SkippedRunning);
                };
                let retry_at: Option<i64> = existing.try_get("retry_at")?;
                if existing.try_get::<i64, _>("retry_remaining")? <= 0 {
                    return Ok(Stage1JobClaimOutcome::SkippedRetryExhausted);
                }
                if retry_at.is_some_and(|retry_at| retry_at > now) {
                    return Ok(Stage1JobClaimOutcome::SkippedRetryBackoff);
                }
                Ok(Stage1JobClaimOutcome::SkippedRunning)
            })
        }))
    }

    fn mark_stage1_job_succeeded<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
        source_updated_at: i64,
        raw_memory: &'a str,
        rollout_summary: &'a str,
        rollout_slug: Option<&'a str>,
    ) -> MemoryStoreFuture<'a, bool> {
        let ownership_token = ownership_token.to_string();
        let raw_memory = raw_memory.to_string();
        let rollout_summary = rollout_summary.to_string();
        let rollout_slug = rollout_slug.map(str::to_string);
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                let thread_id = thread_id.to_string();
                let owned = sqlx::query(
                    "UPDATE memory_jobs SET status = 'done', finished_at = $1, \
                     lease_until = NULL, last_error = NULL, \
                     last_success_watermark = input_watermark \
                     WHERE kind = $2 AND job_key = $3 \
                       AND status = 'running' AND ownership_token = $4",
                )
                .bind(now)
                .bind(JOB_KIND_MEMORY_STAGE1)
                .bind(&thread_id)
                .bind(ownership_token.as_str())
                .execute(&mut *connection)
                .await?
                .rows_affected();
                if owned == 0 {
                    return Ok(false);
                }
                sqlx::query(
                    "INSERT INTO memory_stage1_outputs (thread_id, \
                     source_updated_at, raw_memory, rollout_summary, rollout_slug, generated_at) \
                     VALUES ($1::uuid, $2, $3, $4, $5, $6) \
                     ON CONFLICT (thread_id) DO UPDATE SET \
                       source_updated_at = excluded.source_updated_at, \
                       raw_memory = excluded.raw_memory, \
                       rollout_summary = excluded.rollout_summary, \
                       rollout_slug = excluded.rollout_slug, \
                       generated_at = excluded.generated_at \
                     WHERE excluded.source_updated_at >= memory_stage1_outputs.source_updated_at",
                )
                .bind(&thread_id)
                .bind(source_updated_at)
                .bind(raw_memory.as_str())
                .bind(rollout_summary.as_str())
                .bind(rollout_slug.as_deref())
                .bind(now)
                .execute(&mut *connection)
                .await?;
                enqueue_global_consolidation_in(connection, source_updated_at).await?;
                Ok(true)
            })
        }))
    }

    fn mark_stage1_job_succeeded_no_output<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
    ) -> MemoryStoreFuture<'a, bool> {
        let ownership_token = ownership_token.to_string();
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                let thread_id = thread_id.to_string();
                let owned = sqlx::query(
                    "UPDATE memory_jobs SET status = 'done', finished_at = $1, \
                     lease_until = NULL, last_error = NULL, \
                     last_success_watermark = input_watermark \
                     WHERE kind = $2 AND job_key = $3 \
                       AND status = 'running' AND ownership_token = $4",
                )
                .bind(now)
                .bind(JOB_KIND_MEMORY_STAGE1)
                .bind(&thread_id)
                .bind(ownership_token.as_str())
                .execute(&mut *connection)
                .await?
                .rows_affected();
                if owned == 0 {
                    return Ok(false);
                }
                let source_updated_at: i64 = sqlx::query_scalar(
                    "SELECT input_watermark FROM memory_jobs \
                     WHERE kind = $1 AND job_key = $2 AND ownership_token = $3",
                )
                .bind(JOB_KIND_MEMORY_STAGE1)
                .bind(&thread_id)
                .bind(ownership_token.as_str())
                .fetch_one(&mut *connection)
                .await?;
                let deleted =
                    sqlx::query("DELETE FROM memory_stage1_outputs WHERE thread_id = $1::uuid")
                        .bind(&thread_id)
                        .execute(&mut *connection)
                        .await?
                        .rows_affected();
                if deleted > 0 {
                    enqueue_global_consolidation_in(connection, source_updated_at).await?;
                }
                Ok(true)
            })
        }))
    }

    fn mark_stage1_job_failed<'a>(
        &'a self,
        thread_id: ThreadId,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> MemoryStoreFuture<'a, bool> {
        let ownership_token = ownership_token.to_string();
        let failure_reason = failure_reason.to_string();
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                let retry_at = now.saturating_add(retry_delay_seconds.max(0));
                Ok(sqlx::query(
                    "UPDATE memory_jobs SET status = 'error', finished_at = $1, \
                     lease_until = NULL, retry_at = $2, retry_remaining = retry_remaining - 1, \
                     last_error = $3 \
                     WHERE kind = $4 AND job_key = $5 \
                       AND status = 'running' AND ownership_token = $6",
                )
                .bind(now)
                .bind(retry_at)
                .bind(failure_reason.as_str())
                .bind(JOB_KIND_MEMORY_STAGE1)
                .bind(thread_id.to_string())
                .bind(ownership_token.as_str())
                .execute(&mut *connection)
                .await?
                .rows_affected()
                    > 0)
            })
        }))
    }

    fn enqueue_global_consolidation(&self, input_watermark: i64) -> MemoryStoreFuture<'_, ()> {
        Box::pin(self.run(move |connection| {
            Box::pin(enqueue_global_consolidation_in(connection, input_watermark))
        }))
    }

    fn try_claim_global_phase2_job(
        &self,
        worker_id: ThreadId,
        lease_seconds: i64,
    ) -> MemoryStoreFuture<'_, Phase2JobClaimOutcome> {
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                let lease_until = now.saturating_add(lease_seconds.max(0));
                let cooldown_cutoff = now.saturating_sub(PHASE2_SUCCESS_COOLDOWN_SECONDS);
                let ownership_token = Uuid::new_v4().to_string();
                let worker_id = worker_id.to_string();
                let existing = sqlx::query(
                    "SELECT status, lease_until, retry_at, input_watermark, finished_at, \
                     last_error FROM memory_jobs WHERE kind = $1 AND job_key = $2",
                )
                .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
                .bind(MEMORY_CONSOLIDATION_JOB_KEY)
                .fetch_optional(&mut *connection)
                .await?;
                let Some(existing) = existing else {
                    sqlx::query(
                        "INSERT INTO memory_jobs (kind, job_key, status, worker_id, \
                         ownership_token, started_at, finished_at, lease_until, retry_at, \
                         retry_remaining, last_error, input_watermark, last_success_watermark) \
                         VALUES ($1, $2, 'running', $3, $4, $5, NULL, $6, NULL, $7, NULL, 0, 0)",
                    )
                    .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
                    .bind(MEMORY_CONSOLIDATION_JOB_KEY)
                    .bind(&worker_id)
                    .bind(&ownership_token)
                    .bind(now)
                    .bind(lease_until)
                    .bind(DEFAULT_RETRY_REMAINING)
                    .execute(&mut *connection)
                    .await?;
                    return Ok(Phase2JobClaimOutcome::Claimed {
                        ownership_token,
                        input_watermark: 0,
                    });
                };

                let input_watermark: Option<i64> = existing.try_get("input_watermark")?;
                let status: String = existing.try_get("status")?;
                let existing_lease_until: Option<i64> = existing.try_get("lease_until")?;
                let retry_at: Option<i64> = existing.try_get("retry_at")?;
                let finished_at: Option<i64> = existing.try_get("finished_at")?;
                let last_error: Option<String> = existing.try_get("last_error")?;
                if retry_at.is_some_and(|retry_at| retry_at > now) {
                    return Ok(Phase2JobClaimOutcome::SkippedRetryUnavailable);
                }
                if status == "running"
                    && existing_lease_until.is_some_and(|lease_until| lease_until > now)
                {
                    return Ok(Phase2JobClaimOutcome::SkippedRunning);
                }
                if last_error.is_none()
                    && finished_at.is_some_and(|finished_at| finished_at > cooldown_cutoff)
                {
                    return Ok(Phase2JobClaimOutcome::SkippedCooldown);
                }

                let claimed = sqlx::query(
                    "UPDATE memory_jobs SET status = 'running', worker_id = $1, \
                     ownership_token = $2, started_at = $3, finished_at = NULL, \
                     lease_until = $4, retry_at = NULL, last_error = NULL \
                     WHERE kind = $5 AND job_key = $6 \
                       AND (status <> 'running' OR lease_until IS NULL OR lease_until <= $3) \
                       AND (retry_at IS NULL OR retry_at <= $3) \
                       AND (last_error IS NOT NULL OR finished_at IS NULL OR finished_at <= $7)",
                )
                .bind(&worker_id)
                .bind(&ownership_token)
                .bind(now)
                .bind(lease_until)
                .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
                .bind(MEMORY_CONSOLIDATION_JOB_KEY)
                .bind(cooldown_cutoff)
                .execute(&mut *connection)
                .await?
                .rows_affected();
                Ok(if claimed == 0 {
                    Phase2JobClaimOutcome::SkippedRunning
                } else {
                    Phase2JobClaimOutcome::Claimed {
                        ownership_token,
                        input_watermark: input_watermark.unwrap_or(0),
                    }
                })
            })
        }))
    }

    fn heartbeat_global_phase2_job<'a>(
        &'a self,
        ownership_token: &'a str,
        lease_seconds: i64,
    ) -> MemoryStoreFuture<'a, bool> {
        let ownership_token = ownership_token.to_string();
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let lease_until = Utc::now().timestamp().saturating_add(lease_seconds.max(0));
                Ok(sqlx::query(
                    "UPDATE memory_jobs SET lease_until = $1 \
                     WHERE kind = $2 AND job_key = $3 \
                       AND status = 'running' AND ownership_token = $4",
                )
                .bind(lease_until)
                .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
                .bind(MEMORY_CONSOLIDATION_JOB_KEY)
                .bind(ownership_token.as_str())
                .execute(&mut *connection)
                .await?
                .rows_affected()
                    > 0)
            })
        }))
    }

    fn mark_global_phase2_job_succeeded<'a>(
        &'a self,
        ownership_token: &'a str,
        completed_watermark: i64,
        selected_outputs: &'a [Stage1Output],
    ) -> MemoryStoreFuture<'a, bool> {
        let ownership_token = ownership_token.to_string();
        let selected: Vec<(i64, String)> = selected_outputs
            .iter()
            .map(|output| {
                (
                    output.source_updated_at.timestamp(),
                    output.thread_id.to_string(),
                )
            })
            .collect();
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                let owned = sqlx::query(
                    "UPDATE memory_jobs SET status = 'done', finished_at = $1, \
                     lease_until = NULL, last_error = NULL, \
                     last_success_watermark = GREATEST(COALESCE(last_success_watermark, 0), $2) \
                     WHERE kind = $3 AND job_key = $4 \
                       AND status = 'running' AND ownership_token = $5",
                )
                .bind(now)
                .bind(completed_watermark)
                .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
                .bind(MEMORY_CONSOLIDATION_JOB_KEY)
                .bind(ownership_token.as_str())
                .execute(&mut *connection)
                .await?
                .rows_affected();
                if owned == 0 {
                    return Ok(false);
                }
                sqlx::query(
                    "UPDATE memory_stage1_outputs SET selected_for_phase2 = 0, \
                     selected_for_phase2_source_updated_at = NULL \
                     WHERE selected_for_phase2 <> 0 \
                        OR selected_for_phase2_source_updated_at IS NOT NULL",
                )
                .execute(&mut *connection)
                .await?;
                for (source_updated_at, thread_id) in &selected {
                    sqlx::query(
                        "UPDATE memory_stage1_outputs SET selected_for_phase2 = 1, \
                         selected_for_phase2_source_updated_at = $1 \
                         WHERE thread_id = $2::uuid AND source_updated_at = $1",
                    )
                    .bind(source_updated_at)
                    .bind(thread_id)
                    .execute(&mut *connection)
                    .await?;
                }
                sqlx::query(
                    "UPDATE memory_consolidation_progress \
                     SET max_thread_count = GREATEST(max_thread_count, $1)",
                )
                .bind(i64::try_from(selected.len())?)
                .execute(&mut *connection)
                .await?;
                Ok(true)
            })
        }))
    }

    fn mark_global_phase2_job_failed<'a>(
        &'a self,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> MemoryStoreFuture<'a, bool> {
        let ownership_token = ownership_token.to_string();
        let failure_reason = failure_reason.to_string();
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                Ok(sqlx::query(
                    "UPDATE memory_jobs SET status = 'error', finished_at = $1, \
                     lease_until = NULL, retry_at = $2, \
                     retry_remaining = GREATEST(retry_remaining - 1, 0), last_error = $3 \
                     WHERE kind = $4 AND job_key = $5 \
                       AND status = 'running' AND ownership_token = $6",
                )
                .bind(now)
                .bind(now.saturating_add(retry_delay_seconds.max(0)))
                .bind(failure_reason.as_str())
                .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
                .bind(MEMORY_CONSOLIDATION_JOB_KEY)
                .bind(ownership_token.as_str())
                .execute(&mut *connection)
                .await?
                .rows_affected()
                    > 0)
            })
        }))
    }

    fn mark_global_phase2_job_failed_if_unowned<'a>(
        &'a self,
        ownership_token: &'a str,
        failure_reason: &'a str,
        retry_delay_seconds: i64,
    ) -> MemoryStoreFuture<'a, bool> {
        let ownership_token = ownership_token.to_string();
        let failure_reason = failure_reason.to_string();
        Box::pin(self.run(move |connection| {
            Box::pin(async move {
                let now = Utc::now().timestamp();
                Ok(sqlx::query(
                    "UPDATE memory_jobs SET status = 'error', finished_at = $1, \
                     lease_until = NULL, retry_at = $2, \
                     retry_remaining = GREATEST(retry_remaining - 1, 0), last_error = $3 \
                     WHERE kind = $4 AND job_key = $5 AND status = 'running' \
                       AND (ownership_token = $6 OR ownership_token IS NULL)",
                )
                .bind(now)
                .bind(now.saturating_add(retry_delay_seconds.max(0)))
                .bind(failure_reason.as_str())
                .bind(JOB_KIND_MEMORY_CONSOLIDATE_GLOBAL)
                .bind(MEMORY_CONSOLIDATION_JOB_KEY)
                .bind(ownership_token.as_str())
                .execute(&mut *connection)
                .await?
                .rows_affected()
                    > 0)
            })
        }))
    }

    fn max_consolidated_thread_count(&self) -> MemoryStoreFuture<'_, u32> {
        Box::pin(async move {
            let rows = self
                .read_rows(sqlx::query(
                    "SELECT max_thread_count FROM memory_consolidation_progress \
                     WHERE singleton",
                ))
                .await?;
            Ok(u32::try_from(
                rows[0].try_get::<i64, _>("max_thread_count")?,
            )?)
        })
    }
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod tests;
