use chrono::DateTime;
use chrono::Utc;
use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
use codex_protocol::ThreadId;
use codex_state::GoalAccountingMode;
use codex_state::GoalAccountingOutcome;
use codex_state::GoalStoreFuture;
use codex_state::GoalUpdate;
use codex_state::ThreadGoal;
use codex_state::ThreadGoalStatus;
use codex_state::ThreadGoalStore;
use sqlx::Acquire;
use sqlx::AssertSqlSafe;
use sqlx::Row;
use sqlx::postgres::PgRow;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

const GOAL_COLUMNS: &str = "thread_id::text AS thread_id, goal_id, objective, status, \
     token_budget, tokens_used, time_used_seconds, created_at_ms, updated_at_ms";

/// Redacted failure from goal persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GoalStoreError {
    #[error("PostgreSQL goal operation timed out")]
    Timeout,
    #[error("PostgreSQL goal storage unavailable")]
    Unavailable,
    #[error("thread has no PostgreSQL thread row")]
    ThreadNotFound,
    #[error("invalid PostgreSQL goal record")]
    InvalidRecord,
}

/// Fixed-namespace goal adapter. Construction does not activate PostgreSQL.
#[derive(Clone)]
pub struct PostgresGoalStore {
    pool: Arc<PostgresPool>,
}

impl PostgresGoalStore {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }

    async fn get(&self, thread_id: ThreadId) -> Result<Option<ThreadGoal>, GoalStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query(AssertSqlSafe(format!(
                "SELECT {GOAL_COLUMNS} FROM thread_goals WHERE thread_id = $1::uuid"
            )))
            .bind(thread_id.to_string())
            .fetch_optional(&mut *connection),
        )
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))?;
        row.map(goal_from_row).transpose()
    }

    async fn replace_snapshot(&self, goal: &ThreadGoal) -> Result<(), GoalStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        timeout(QUERY_TIMEOUT, async {
            let mut transaction = connection.begin().await?;
            self.pool
                .prepare_selected_write(&mut transaction)
                .await
                .map_err(|_| sqlx::Error::PoolClosed)?;
            self.pool
                .require_bound_storage_open(&mut transaction)
                .await
                .map_err(|_| sqlx::Error::PoolClosed)?;
            sqlx::query(
                "INSERT INTO thread_goals \
                 (thread_id, goal_id, objective, status, token_budget, tokens_used, \
                  time_used_seconds, created_at_ms, updated_at_ms) \
                 VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9) \
                 ON CONFLICT (thread_id) DO UPDATE SET \
                 goal_id = excluded.goal_id, objective = excluded.objective, \
                 status = excluded.status, token_budget = excluded.token_budget, \
                 tokens_used = excluded.tokens_used, \
                 time_used_seconds = excluded.time_used_seconds, \
                 created_at_ms = excluded.created_at_ms, updated_at_ms = excluded.updated_at_ms",
            )
            .bind(goal.thread_id.to_string())
            .bind(&goal.goal_id)
            .bind(&goal.objective)
            .bind(goal.status.as_str())
            .bind(goal.token_budget)
            .bind(goal.tokens_used)
            .bind(goal.time_used_seconds)
            .bind(goal.created_at.timestamp_millis())
            .bind(goal.updated_at.timestamp_millis())
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "INSERT INTO thread_goal_continuation_deferrals (thread_id) \
                 VALUES ($1::uuid) ON CONFLICT (thread_id) DO NOTHING",
            )
            .bind(goal.thread_id.to_string())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await
        })
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))
    }

    async fn has_deferral(&self, thread_id: ThreadId) -> Result<bool, GoalStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        timeout(
            QUERY_TIMEOUT,
            sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM thread_goal_continuation_deferrals \
                 WHERE thread_id = $1::uuid)",
            )
            .bind(thread_id.to_string())
            .fetch_one(&mut *connection),
        )
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))
    }

    async fn clear_deferral(&self, thread_id: ThreadId) -> Result<(), GoalStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let mut tx = begin_write(&self.pool, &mut connection).await?;
        timeout(
            QUERY_TIMEOUT,
            sqlx::query(
                "DELETE FROM thread_goal_continuation_deferrals \
                 WHERE thread_id = $1::uuid",
            )
            .bind(thread_id.to_string())
            .execute(&mut *tx),
        )
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))?;
        tx.commit().await.map_err(|error| classify_sqlx(&error))?;
        Ok(())
    }

    async fn upsert(
        &self,
        thread_id: ThreadId,
        objective: &str,
        status: ThreadGoalStatus,
        token_budget: Option<i64>,
        only_replace_complete: bool,
    ) -> Result<Option<ThreadGoal>, GoalStoreError> {
        let now_ms = Utc::now().timestamp_millis();
        let status = status_after_budget_limit(status, /*tokens_used*/ 0, token_budget);
        let guard = if only_replace_complete {
            " WHERE thread_goals.status = 'complete'"
        } else {
            ""
        };
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let mut tx = begin_write(&self.pool, &mut connection).await?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO thread_goals \
                 (thread_id, goal_id, objective, status, token_budget, tokens_used, \
                  time_used_seconds, created_at_ms, updated_at_ms) \
                 VALUES ($1::uuid, $2, $3, $4, $5, 0, 0, $6, $6) \
                 ON CONFLICT (thread_id) DO UPDATE SET \
                 goal_id = excluded.goal_id, objective = excluded.objective, \
                 status = excluded.status, token_budget = excluded.token_budget, \
                 tokens_used = 0, time_used_seconds = 0, \
                 created_at_ms = excluded.created_at_ms, \
                 updated_at_ms = excluded.updated_at_ms{guard} \
                 RETURNING {GOAL_COLUMNS}"
            )))
            .bind(thread_id.to_string())
            .bind(Uuid::new_v4().to_string())
            .bind(objective)
            .bind(status.as_str())
            .bind(token_budget)
            .bind(now_ms)
            .fetch_optional(&mut *tx),
        )
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))?;
        tx.commit().await.map_err(|error| classify_sqlx(&error))?;
        row.map(goal_from_row).transpose()
    }

    async fn update(
        &self,
        thread_id: ThreadId,
        update: GoalUpdate,
    ) -> Result<Option<ThreadGoal>, GoalStoreError> {
        let GoalUpdate {
            objective,
            status,
            token_budget,
            expected_goal_id,
        } = update;
        let now_ms = Utc::now().timestamp_millis();
        let thread = thread_id.to_string();
        // Each variant mirrors the SQLite store's statement and binds exactly the parameters it
        // references, because PostgreSQL cannot infer the type of an unreferenced parameter.
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let mut tx = begin_write(&self.pool, &mut connection).await?;
        let row = timeout(QUERY_TIMEOUT, async {
            match (status, token_budget) {
                (Some(status), Some(token_budget)) => {
                    sqlx::query(AssertSqlSafe(format!(
                        "UPDATE thread_goals SET                          objective = COALESCE($1::text, objective),                          status = CASE                            WHEN status = 'budget_limited' AND $2::text IN ('paused', 'blocked')                              THEN status                            WHEN $2::text = 'active' AND $3::bigint IS NOT NULL                              AND tokens_used >= $3::bigint THEN 'budget_limited'                            ELSE $2::text END,                          token_budget = $3::bigint, updated_at_ms = $4                          WHERE thread_id = $5::uuid AND ($6::text IS NULL OR goal_id = $6::text)                          RETURNING {GOAL_COLUMNS}"
                    )))
                    .bind(objective.as_deref())
                    .bind(status.as_str())
                    .bind(token_budget)
                    .bind(now_ms)
                    .bind(&thread)
                    .bind(expected_goal_id.as_deref())
                    .fetch_optional(&mut *tx)
                    .await
                }
                (Some(status), None) => {
                    sqlx::query(AssertSqlSafe(format!(
                        "UPDATE thread_goals SET                          objective = COALESCE($1::text, objective),                          status = CASE                            WHEN status = 'budget_limited' AND $2::text IN ('paused', 'blocked')                              THEN status                            WHEN $2::text = 'active' AND token_budget IS NOT NULL                              AND tokens_used >= token_budget THEN 'budget_limited'                            ELSE $2::text END,                          updated_at_ms = $3                          WHERE thread_id = $4::uuid AND ($5::text IS NULL OR goal_id = $5::text)                          RETURNING {GOAL_COLUMNS}"
                    )))
                    .bind(objective.as_deref())
                    .bind(status.as_str())
                    .bind(now_ms)
                    .bind(&thread)
                    .bind(expected_goal_id.as_deref())
                    .fetch_optional(&mut *tx)
                    .await
                }
                (None, Some(token_budget)) => {
                    sqlx::query(AssertSqlSafe(format!(
                        "UPDATE thread_goals SET                          objective = COALESCE($1::text, objective),                          token_budget = $2::bigint,                          status = CASE                            WHEN status = 'active' AND $2::bigint IS NOT NULL                              AND tokens_used >= $2::bigint THEN 'budget_limited'                            ELSE status END,                          updated_at_ms = $3                          WHERE thread_id = $4::uuid AND ($5::text IS NULL OR goal_id = $5::text)                          RETURNING {GOAL_COLUMNS}"
                    )))
                    .bind(objective.as_deref())
                    .bind(token_budget)
                    .bind(now_ms)
                    .bind(&thread)
                    .bind(expected_goal_id.as_deref())
                    .fetch_optional(&mut *tx)
                    .await
                }
                (None, None) => match objective.as_deref() {
                    Some(objective) => {
                        sqlx::query(AssertSqlSafe(format!(
                            "UPDATE thread_goals SET objective = $1::text,                              updated_at_ms = $2                              WHERE thread_id = $3::uuid AND ($4::text IS NULL OR goal_id = $4::text)                              RETURNING {GOAL_COLUMNS}"
                        )))
                        .bind(objective)
                        .bind(now_ms)
                        .bind(&thread)
                        .bind(expected_goal_id.as_deref())
                        .fetch_optional(&mut *tx)
                        .await
                    }
                    None => {
                        // A no-op update only reads the goal and applies the expected-ID check.
                        sqlx::query(AssertSqlSafe(format!(
                            "SELECT {GOAL_COLUMNS} FROM thread_goals                              WHERE thread_id = $1::uuid AND ($2::text IS NULL OR goal_id = $2::text)"
                        )))
                        .bind(&thread)
                        .bind(expected_goal_id.as_deref())
                        .fetch_optional(&mut *tx)
                        .await
                    }
                },
            }
        })
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))?;
        tx.commit().await.map_err(|error| classify_sqlx(&error))?;
        row.map(goal_from_row).transpose()
    }

    async fn update_active_status(
        &self,
        thread_id: ThreadId,
        status: ThreadGoalStatus,
    ) -> Result<Option<ThreadGoal>, GoalStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let mut tx = begin_write(&self.pool, &mut connection).await?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE thread_goals SET status = $1, updated_at_ms = $2 \
                 WHERE thread_id = $3::uuid AND (status = 'active' \
                   OR ($1::text = 'usage_limited' AND status = 'budget_limited')) \
                 RETURNING {GOAL_COLUMNS}"
            )))
            .bind(status.as_str())
            .bind(Utc::now().timestamp_millis())
            .bind(thread_id.to_string())
            .fetch_optional(&mut *tx),
        )
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))?;
        tx.commit().await.map_err(|error| classify_sqlx(&error))?;
        row.map(goal_from_row).transpose()
    }

    async fn delete(&self, thread_id: ThreadId) -> Result<Option<ThreadGoal>, GoalStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let mut tx = begin_write(&self.pool, &mut connection).await?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM thread_goals WHERE thread_id = $1::uuid \
                 RETURNING {GOAL_COLUMNS}"
            )))
            .bind(thread_id.to_string())
            .fetch_optional(&mut *tx),
        )
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))?;
        tx.commit().await.map_err(|error| classify_sqlx(&error))?;
        row.map(goal_from_row).transpose()
    }

    async fn account_usage(
        &self,
        thread_id: ThreadId,
        time_delta_seconds: i64,
        token_delta: i64,
        mode: GoalAccountingMode,
        expected_goal_id: Option<&str>,
    ) -> Result<GoalAccountingOutcome, GoalStoreError> {
        let time_delta_seconds = time_delta_seconds.max(0);
        let token_delta = token_delta.max(0);
        if time_delta_seconds == 0 && token_delta == 0 {
            return Ok(GoalAccountingOutcome::Unchanged(self.get(thread_id).await?));
        }
        let active_or_stopped =
            "status IN ('active', 'paused', 'blocked', 'usage_limited', 'budget_limited')";
        let status_filter = match mode {
            GoalAccountingMode::ActiveStatusOnly => "status = 'active'",
            GoalAccountingMode::ActiveOnly => "status IN ('active', 'budget_limited')",
            GoalAccountingMode::ActiveOrComplete => {
                "status IN ('active', 'budget_limited', 'complete')"
            }
            GoalAccountingMode::ActiveOrStopped => active_or_stopped,
        };
        let budget_limit_filter = match mode {
            GoalAccountingMode::ActiveStatusOnly
            | GoalAccountingMode::ActiveOnly
            | GoalAccountingMode::ActiveOrComplete => "status = 'active'",
            GoalAccountingMode::ActiveOrStopped => active_or_stopped,
        };
        let sql = format!(
            "UPDATE thread_goals SET \
             time_used_seconds = time_used_seconds + $1, \
             tokens_used = tokens_used + $2, \
             status = CASE WHEN {budget_limit_filter} AND token_budget IS NOT NULL \
                 AND tokens_used + $2 >= token_budget THEN 'budget_limited' ELSE status END, \
             updated_at_ms = $3 \
             WHERE thread_id = $4::uuid AND {status_filter} \
               AND ($5::text IS NULL OR goal_id = $5::text) \
             RETURNING {GOAL_COLUMNS}"
        );
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let mut tx = begin_write(&self.pool, &mut connection).await?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query(AssertSqlSafe(sql))
                .bind(time_delta_seconds)
                .bind(token_delta)
                .bind(Utc::now().timestamp_millis())
                .bind(thread_id.to_string())
                .bind(expected_goal_id)
                .fetch_optional(&mut *tx),
        )
        .await
        .map_err(|_| GoalStoreError::Timeout)?
        .map_err(|error| classify_sqlx(&error))?;
        tx.commit().await.map_err(|error| classify_sqlx(&error))?;
        match row {
            Some(row) => Ok(GoalAccountingOutcome::Updated(goal_from_row(row)?)),
            None => Ok(GoalAccountingOutcome::Unchanged(self.get(thread_id).await?)),
        }
    }
}

impl ThreadGoalStore for PostgresGoalStore {
    fn get_thread_goal(&self, thread_id: ThreadId) -> GoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move { Ok(self.get(thread_id).await?) })
    }

    fn replace_thread_goal_snapshot<'a>(&'a self, goal: &'a ThreadGoal) -> GoalStoreFuture<'a, ()> {
        Box::pin(async move { Ok(self.replace_snapshot(goal).await?) })
    }

    fn has_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> GoalStoreFuture<'_, bool> {
        Box::pin(async move { Ok(self.has_deferral(thread_id).await?) })
    }

    fn clear_thread_goal_continuation_deferral(
        &self,
        thread_id: ThreadId,
    ) -> GoalStoreFuture<'_, ()> {
        Box::pin(async move { Ok(self.clear_deferral(thread_id).await?) })
    }

    fn replace_thread_goal<'a>(
        &'a self,
        thread_id: ThreadId,
        objective: &'a str,
        status: ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> GoalStoreFuture<'a, ThreadGoal> {
        Box::pin(async move {
            self.upsert(
                thread_id,
                objective,
                status,
                token_budget,
                /*only_replace_complete*/ false,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("PostgreSQL goal replacement returned no row"))
        })
    }

    fn insert_thread_goal<'a>(
        &'a self,
        thread_id: ThreadId,
        objective: &'a str,
        status: ThreadGoalStatus,
        token_budget: Option<i64>,
    ) -> GoalStoreFuture<'a, Option<ThreadGoal>> {
        Box::pin(async move {
            Ok(self
                .upsert(
                    thread_id,
                    objective,
                    status,
                    token_budget,
                    /*only_replace_complete*/ true,
                )
                .await?)
        })
    }

    fn update_thread_goal(
        &self,
        thread_id: ThreadId,
        update: GoalUpdate,
    ) -> GoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move { Ok(self.update(thread_id, update).await?) })
    }

    fn pause_active_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> GoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move {
            Ok(self
                .update_active_status(thread_id, ThreadGoalStatus::Paused)
                .await?)
        })
    }

    fn usage_limit_active_thread_goal(
        &self,
        thread_id: ThreadId,
    ) -> GoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move {
            Ok(self
                .update_active_status(thread_id, ThreadGoalStatus::UsageLimited)
                .await?)
        })
    }

    fn delete_thread_goal(&self, thread_id: ThreadId) -> GoalStoreFuture<'_, Option<ThreadGoal>> {
        Box::pin(async move { Ok(self.delete(thread_id).await?) })
    }

    fn account_thread_goal_usage<'a>(
        &'a self,
        thread_id: ThreadId,
        time_delta_seconds: i64,
        token_delta: i64,
        mode: GoalAccountingMode,
        expected_goal_id: Option<&'a str>,
    ) -> GoalStoreFuture<'a, GoalAccountingOutcome> {
        Box::pin(async move {
            Ok(self
                .account_usage(
                    thread_id,
                    time_delta_seconds,
                    token_delta,
                    mode,
                    expected_goal_id,
                )
                .await?)
        })
    }
}

fn status_after_budget_limit(
    status: ThreadGoalStatus,
    tokens_used: i64,
    token_budget: Option<i64>,
) -> ThreadGoalStatus {
    if status == ThreadGoalStatus::Active
        && token_budget.is_some_and(|budget| tokens_used >= budget)
    {
        ThreadGoalStatus::BudgetLimited
    } else {
        status
    }
}

fn goal_from_row(row: PgRow) -> Result<ThreadGoal, GoalStoreError> {
    let invalid = |_| GoalStoreError::InvalidRecord;
    let thread_id: String = row.try_get("thread_id").map_err(invalid)?;
    let status: String = row.try_get("status").map_err(invalid)?;
    let created_at_ms: i64 = row.try_get("created_at_ms").map_err(invalid)?;
    let updated_at_ms: i64 = row.try_get("updated_at_ms").map_err(invalid)?;
    Ok(ThreadGoal {
        thread_id: ThreadId::try_from(thread_id).map_err(|_| GoalStoreError::InvalidRecord)?,
        goal_id: row.try_get("goal_id").map_err(invalid)?,
        objective: row.try_get("objective").map_err(invalid)?,
        status: ThreadGoalStatus::try_from(status.as_str())
            .map_err(|_| GoalStoreError::InvalidRecord)?,
        token_budget: row.try_get("token_budget").map_err(invalid)?,
        tokens_used: row.try_get("tokens_used").map_err(invalid)?,
        time_used_seconds: row.try_get("time_used_seconds").map_err(invalid)?,
        created_at: DateTime::from_timestamp_millis(created_at_ms)
            .ok_or(GoalStoreError::InvalidRecord)?,
        updated_at: DateTime::from_timestamp_millis(updated_at_ms)
            .ok_or(GoalStoreError::InvalidRecord)?,
    })
}

/// Begin a write transaction that is refused while a migration holds the store.
async fn begin_write<'c>(
    pool: &PostgresPool,
    connection: &'c mut sqlx::PgConnection,
) -> Result<sqlx::Transaction<'c, sqlx::Postgres>, GoalStoreError> {
    let mut tx = connection
        .begin()
        .await
        .map_err(|error| classify_sqlx(&error))?;
    pool.prepare_selected_write(&mut tx)
        .await
        .map_err(|_| GoalStoreError::Unavailable)?;
    pool.require_bound_storage_open(&mut tx)
        .await
        .map_err(|_| GoalStoreError::Unavailable)?;
    Ok(tx)
}

fn classify_pool(error: PoolError) -> GoalStoreError {
    match error {
        PoolError::Timeout => GoalStoreError::Timeout,
        PoolError::InvalidSettings
        | PoolError::Authentication
        | PoolError::Tls
        | PoolError::Unavailable
        | PoolError::Closed
        | PoolError::UnsupportedServer => GoalStoreError::Unavailable,
    }
}

fn classify_sqlx(error: &sqlx::Error) -> GoalStoreError {
    match error {
        sqlx::Error::Database(error) if error.code().as_deref() == Some("23503") => {
            GoalStoreError::ThreadNotFound
        }
        _ => GoalStoreError::Unavailable,
    }
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod tests;
