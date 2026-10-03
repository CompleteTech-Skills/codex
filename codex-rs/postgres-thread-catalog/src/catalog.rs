//! Thread metadata persistence in the fixed PostgreSQL namespace.
//!
//! Operations mirror the SQLite state runtime's observable behavior. Host paths are stored as
//! recorded origins, and timestamps that order listings come from the shared marks. Writers
//! that touch several stores take locks in one order: memory, queue, timestamp marks, then
//! rows, so combined transactions cannot deadlock.

use crate::timestamps::lock_marks;
use anyhow::Result;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use codex_postgres_memory_store::delete_thread_memory_in;
use codex_postgres_memory_store::lock_memory_in;
use codex_postgres_queue_store::delete_thread_queue_in;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_thread_rows::thread_columns;
use codex_postgres_thread_rows::thread_metadata_from_row;
use codex_protocol::SanitizedGitUrl;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_state::ThreadMetadata;
use serde::Serialize;
use sqlx::Acquire;
use sqlx::PgConnection;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) type BoxOperation<'c, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>;

/// Fixed-namespace thread catalog adapter. Construction does not activate PostgreSQL.
#[derive(Clone)]
pub struct PostgresThreadCatalog {
    pool: Arc<PostgresPool>,
}

/// Every insertable thread column, in bind order.
macro_rules! insert_thread {
    () => {
        concat!(
            "INSERT INTO threads (id, origin_rollout_path, ",
            "created_at_ms, updated_at_ms, recency_at_ms, source, originator, creator_user_id, ",
            "creator_account_id, history_mode, thread_source, agent_nickname, agent_role, ",
            "agent_path, model_provider, model, reasoning_effort, origin_cwd, cli_version, ",
            "title, name, preview, sandbox_policy, approval_mode, tokens_used, ",
            "first_user_message, archived_at_s, thread_section_id, section_position, ",
            "section_entered_at_ms, git_sha, git_branch, git_origin_url, memory_mode, ",
            "project_id, daybreak_enabled) VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, ",
            "$10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, ",
            "$26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36)"
        )
    };
}

impl PostgresThreadCatalog {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }

    pub(crate) fn pool(&self) -> &PostgresPool {
        &self.pool
    }

    /// Run one catalog write in a transaction, bounded by the query timeout.
    pub(crate) async fn write<T, F>(&self, operation: F) -> Result<T>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxOperation<'c, T>,
    {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| anyhow!("PostgreSQL thread storage is unavailable: {error:?}"))?;
        timeout(QUERY_TIMEOUT, async {
            let mut tx = connection.begin().await?;
            self.pool.prepare_selected_write(&mut tx).await?;
            self.pool.require_bound_storage_open(&mut tx).await?;
            let value = operation(&mut tx).await?;
            tx.commit().await?;
            anyhow::Ok(value)
        })
        .await
        .map_err(|_| anyhow!("PostgreSQL thread operation timed out"))?
    }

    /// Run reads against one snapshot, like a deferred SQLite transaction.
    pub(crate) async fn read<T, F>(&self, operation: F) -> Result<T>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxOperation<'c, T>,
    {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| anyhow!("PostgreSQL thread storage is unavailable: {error:?}"))?;
        timeout(QUERY_TIMEOUT, async {
            let mut tx = connection.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *tx)
                .await?;
            let value = operation(&mut tx).await?;
            tx.commit().await?;
            anyhow::Ok(value)
        })
        .await
        .map_err(|_| anyhow!("PostgreSQL thread query timed out"))?
    }

    pub async fn get_thread(&self, id: ThreadId) -> Result<Option<ThreadMetadata>> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| anyhow!("PostgreSQL thread storage is unavailable: {error:?}"))?;
        timeout(QUERY_TIMEOUT, get_thread_in(&mut connection, id, false))
            .await
            .map_err(|_| anyhow!("PostgreSQL thread query timed out"))?
    }

    pub async fn get_thread_memory_mode(&self, id: ThreadId) -> Result<Option<String>> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| anyhow!("PostgreSQL thread storage is unavailable: {error:?}"))?;
        Ok(
            sqlx::query_scalar("SELECT memory_mode FROM threads WHERE id = $1::uuid")
                .bind(id.to_string())
                .fetch_optional(&mut *connection)
                .await?,
        )
    }

    /// Insert or replace thread metadata directly.
    pub async fn upsert_thread(&self, metadata: &ThreadMetadata) -> Result<()> {
        let metadata = metadata.clone();
        self.write(move |connection| {
            Box::pin(async move {
                upsert_in(connection, &metadata, /*memory_mode*/ "enabled").await
            })
        })
        .await
    }

    pub async fn insert_thread_if_absent(&self, metadata: &ThreadMetadata) -> Result<bool> {
        let metadata = metadata.clone();
        self.write(move |connection| {
            Box::pin(async move {
                let mut marks = lock_marks(connection).await?;
                let updated_at = marks.allocate_updated_at(metadata.updated_at);
                let recency_at = marks.allocate_recency_at(metadata.recency_at);
                marks.save(connection).await?;
                let inserted = bind_thread(
                    sqlx::query(concat!(insert_thread!(), " ON CONFLICT (id) DO NOTHING")),
                    &metadata,
                    updated_at,
                    recency_at,
                    "enabled",
                )
                .execute(&mut *connection)
                .await?
                .rows_affected()
                    > 0;
                insert_spawn_edge_from_source(connection, metadata.id, &metadata.source).await?;
                Ok(inserted)
            })
        })
        .await
    }

    /// Set the user preference without changing rollout-derived thread metadata.
    pub async fn set_thread_daybreak_enabled(
        &self,
        thread_id: ThreadId,
        daybreak_enabled: bool,
    ) -> Result<bool> {
        self.update_one(
            "UPDATE threads SET daybreak_enabled = $1 WHERE id = $2::uuid",
            move |query| query.bind(daybreak_enabled).bind(thread_id.to_string()),
        )
        .await
    }

    pub async fn set_thread_memory_mode(
        &self,
        thread_id: ThreadId,
        memory_mode: &str,
    ) -> Result<bool> {
        let memory_mode = memory_mode.to_string();
        self.update_one(
            "UPDATE threads SET memory_mode = $1 WHERE id = $2::uuid",
            move |query| query.bind(memory_mode).bind(thread_id.to_string()),
        )
        .await
    }

    pub async fn update_thread_title(&self, thread_id: ThreadId, title: &str) -> Result<bool> {
        let title = title.to_string();
        self.update_one(
            "UPDATE threads SET title = $1 WHERE id = $2::uuid",
            move |query| query.bind(title).bind(thread_id.to_string()),
        )
        .await
    }

    pub async fn update_thread_name(
        &self,
        thread_id: ThreadId,
        name: Option<&str>,
    ) -> Result<bool> {
        let name = name.map(str::to_string);
        self.update_one(
            "UPDATE threads SET name = $1 WHERE id = $2::uuid",
            move |query| query.bind(name).bind(thread_id.to_string()),
        )
        .await
    }

    /// Permanently promote a thread, preserving its canonical name or a legacy-visible fallback.
    pub async fn mark_thread_paginated(
        &self,
        thread_id: ThreadId,
        legacy_name: Option<&str>,
    ) -> Result<bool> {
        let legacy_name = legacy_name.map(str::to_string);
        self.update_one(
            "UPDATE threads SET history_mode = 'paginated', name = CASE \
               WHEN name IS NULL OR trim(name) = '' THEN $1 \
               WHEN history_mode = 'legacy' \
                 AND source = '{\"subagent\":{\"other\":\"guardian\"}}' \
                 AND name = $2 THEN COALESCE($1, name) \
               ELSE name END \
             WHERE id = $3::uuid",
            move |query| {
                query
                    .bind(legacy_name)
                    .bind(codex_state::GUARDIAN_THREAD_TITLE)
                    .bind(thread_id.to_string())
            },
        )
        .await
    }

    pub async fn set_thread_preview_if_empty(
        &self,
        thread_id: ThreadId,
        preview: &str,
    ) -> Result<bool> {
        let preview = preview.trim().to_string();
        if preview.is_empty() {
            return Ok(false);
        }
        self.update_one(
            "UPDATE threads SET preview = $1 \
             WHERE id = $2::uuid AND COALESCE(preview, '') = ''",
            move |query| query.bind(preview).bind(thread_id.to_string()),
        )
        .await
    }

    pub async fn touch_thread_updated_at(
        &self,
        thread_id: ThreadId,
        updated_at: DateTime<Utc>,
    ) -> Result<bool> {
        self.write(move |connection| {
            Box::pin(async move {
                let mut marks = lock_marks(connection).await?;
                let allocated = marks.allocate_updated_at(updated_at);
                marks.save(connection).await?;
                Ok(
                    sqlx::query("UPDATE threads SET updated_at_ms = $1 WHERE id = $2::uuid")
                        .bind(allocated)
                        .bind(thread_id.to_string())
                        .execute(&mut *connection)
                        .await?
                        .rows_affected()
                        > 0,
                )
            })
        })
        .await
    }

    pub async fn touch_thread_recency_at(
        &self,
        thread_id: ThreadId,
        recency_at: DateTime<Utc>,
    ) -> Result<bool> {
        self.write(move |connection| {
            Box::pin(async move {
                let mut marks = lock_marks(connection).await?;
                let allocated = marks.allocate_recency_at(recency_at);
                marks.save(connection).await?;
                Ok(sqlx::query(
                    "UPDATE threads \
                     SET recency_at_ms = GREATEST($1, recency_at_ms + 1) WHERE id = $2::uuid",
                )
                .bind(allocated)
                .bind(thread_id.to_string())
                .execute(&mut *connection)
                .await?
                .rows_affected()
                    > 0)
            })
        })
        .await
    }

    pub async fn update_thread_git_info(
        &self,
        thread_id: ThreadId,
        git_sha: Option<Option<&str>>,
        git_branch: Option<Option<&str>>,
        git_origin_url: Option<Option<&SanitizedGitUrl>>,
    ) -> Result<bool> {
        let (set_sha, git_sha) = (git_sha.is_some(), git_sha.flatten().map(str::to_string));
        let (set_branch, git_branch) = (
            git_branch.is_some(),
            git_branch.flatten().map(str::to_string),
        );
        let (set_url, git_origin_url) = (
            git_origin_url.is_some(),
            git_origin_url
                .flatten()
                .map(|url| SanitizedGitUrl::as_str(url).to_string()),
        );
        self.update_one(
            "UPDATE threads SET \
               git_sha = CASE WHEN $1 THEN $2 ELSE git_sha END, \
               git_branch = CASE WHEN $3 THEN $4 ELSE git_branch END, \
               git_origin_url = CASE WHEN $5 THEN $6 ELSE git_origin_url END \
             WHERE id = $7::uuid",
            move |query| {
                query
                    .bind(set_sha)
                    .bind(git_sha)
                    .bind(set_branch)
                    .bind(git_branch)
                    .bind(set_url)
                    .bind(git_origin_url)
                    .bind(thread_id.to_string())
            },
        )
        .await
    }

    pub async fn find_rollout_path_by_id(
        &self,
        id: ThreadId,
        archived_only: Option<bool>,
    ) -> Result<Option<PathBuf>> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|error| anyhow!("PostgreSQL thread storage is unavailable: {error:?}"))?;
        let path: Option<String> = sqlx::query_scalar(
            "SELECT origin_rollout_path FROM threads WHERE id = $1::uuid \
             AND ($2::boolean IS NULL OR ($2 = (archived_at_s IS NOT NULL)))",
        )
        .bind(id.to_string())
        .bind(archived_only)
        .fetch_optional(&mut *connection)
        .await?;
        Ok(path.map(PathBuf::from))
    }

    /// Swap one thread's recorded rollout path only when it still matches the expected path.
    pub async fn replace_rollout_path_if_current(
        &self,
        id: ThreadId,
        expected: &Path,
        replacement: &Path,
    ) -> Result<bool> {
        let (expected, replacement) = (
            expected.display().to_string(),
            replacement.display().to_string(),
        );
        Ok(self
            .update_count(
                "UPDATE threads SET origin_rollout_path = $1 \
                 WHERE id = $2::uuid AND origin_rollout_path = $3",
                move |query| query.bind(replacement).bind(id.to_string()).bind(expected),
            )
            .await?
            == 1)
    }

    /// Mark a thread archived at the given time with its archived rollout location.
    pub async fn mark_archived(
        &self,
        thread_id: ThreadId,
        rollout_path: &Path,
        archived_at: DateTime<Utc>,
    ) -> Result<()> {
        let rollout_path = rollout_path.to_path_buf();
        self.write(move |connection| {
            Box::pin(async move {
                let Some(mut metadata) = get_thread_in(connection, thread_id, true).await? else {
                    return Ok(());
                };
                metadata.archived_at = Some(archived_at);
                metadata.rollout_path = rollout_path;
                upsert_in(connection, &metadata, "enabled").await
            })
        })
        .await
    }

    pub async fn mark_unarchived(&self, thread_id: ThreadId, rollout_path: &Path) -> Result<()> {
        let rollout_path = rollout_path.to_path_buf();
        self.write(move |connection| {
            Box::pin(async move {
                let Some(mut metadata) = get_thread_in(connection, thread_id, true).await? else {
                    return Ok(());
                };
                metadata.archived_at = None;
                metadata.rollout_path = rollout_path;
                upsert_in(connection, &metadata, "enabled").await
            })
        })
        .await
    }

    /// Delete a thread and all associated state by id.
    pub async fn delete_thread(&self, thread_id: ThreadId) -> Result<u64> {
        self.delete_threads_strict(&[thread_id]).await
    }

    /// Delete a set of threads and all associated state in one transaction.
    ///
    /// Spawn edges and thread rows go last. Goals cascade with the thread row, while logs,
    /// generated memory and queued messages are removed explicitly so their consumers observe
    /// the same changes as a SQLite deletion.
    pub async fn delete_threads_strict(&self, thread_ids: &[ThreadId]) -> Result<u64> {
        if thread_ids.is_empty() {
            return Ok(0);
        }
        let thread_ids = thread_ids.to_vec();
        self.write(move |connection| {
            Box::pin(async move {
                lock_memory_in(connection).await?;
                for thread_id in &thread_ids {
                    let id = thread_id.to_string();
                    sqlx::query("DELETE FROM logs WHERE thread_id = $1")
                        .bind(&id)
                        .execute(&mut *connection)
                        .await?;
                    delete_thread_queue_in(connection, *thread_id).await?;
                    delete_thread_memory_in(connection, *thread_id).await?;
                }
                let mut rows_affected = 0;
                for thread_id in &thread_ids {
                    let id = thread_id.to_string();
                    sqlx::query(
                        "DELETE FROM thread_spawn_edges \
                         WHERE parent_thread_id = $1::uuid OR child_thread_id = $1::uuid",
                    )
                    .bind(&id)
                    .execute(&mut *connection)
                    .await?;
                    rows_affected += sqlx::query("DELETE FROM threads WHERE id = $1::uuid")
                        .bind(&id)
                        .execute(&mut *connection)
                        .await?
                        .rows_affected();
                }
                Ok(rows_affected)
            })
        })
        .await
    }

    async fn update_one(
        &self,
        sql: &'static str,
        bind: impl FnOnce(PgQuery<'static>) -> PgQuery<'static> + Send + 'static,
    ) -> Result<bool> {
        Ok(self.update_count(sql, bind).await? > 0)
    }

    async fn update_count(
        &self,
        sql: &'static str,
        bind: impl FnOnce(PgQuery<'static>) -> PgQuery<'static> + Send + 'static,
    ) -> Result<u64> {
        self.write(move |connection| {
            Box::pin(async move {
                Ok(bind(sqlx::query(sql))
                    .execute(&mut *connection)
                    .await?
                    .rows_affected())
            })
        })
        .await
    }
}

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

/// Read one thread, optionally locking its row for a read-modify-write.
pub async fn get_thread_in(
    connection: &mut PgConnection,
    id: ThreadId,
    lock: bool,
) -> Result<Option<ThreadMetadata>> {
    let sql = if lock {
        concat!(
            "SELECT ",
            thread_columns!(),
            " FROM threads WHERE threads.id = $1::uuid FOR UPDATE OF threads"
        )
    } else {
        concat!(
            "SELECT ",
            thread_columns!(),
            " FROM threads WHERE threads.id = $1::uuid"
        )
    };
    sqlx::query(sql)
        .bind(id.to_string())
        .fetch_optional(connection)
        .await?
        .map(|row| thread_metadata_from_row(&row))
        .transpose()
}

/// Insert or replace thread metadata inside the caller's transaction, so a thread row and the
/// data that references it can commit together.
pub async fn upsert_thread_in(
    connection: &mut PgConnection,
    metadata: &ThreadMetadata,
    memory_mode: &str,
) -> Result<()> {
    upsert_in(connection, metadata, memory_mode).await
}

/// Insert or replace thread metadata. Daybreak, project and section choices are insert-only;
/// explicit changes use their own setters.
async fn upsert_in(
    connection: &mut PgConnection,
    metadata: &ThreadMetadata,
    memory_mode: &str,
) -> Result<()> {
    let mut marks = lock_marks(connection).await?;
    let updated_at = marks.allocate_updated_at(metadata.updated_at);
    let insert_recency_at = marks.allocate_recency_at(metadata.recency_at);
    marks.save(connection).await?;
    bind_thread(
        sqlx::query(concat!(
            insert_thread!(),
            " ON CONFLICT (id) DO UPDATE SET \
             origin_rollout_path = excluded.origin_rollout_path, \
             created_at_ms = excluded.created_at_ms, \
             updated_at_ms = excluded.updated_at_ms, \
             recency_at_ms = threads.recency_at_ms, \
             source = excluded.source, \
             originator = COALESCE(threads.originator, excluded.originator), \
             creator_user_id = COALESCE(threads.creator_user_id, excluded.creator_user_id), \
             creator_account_id = COALESCE(threads.creator_account_id, excluded.creator_account_id), \
             history_mode = CASE WHEN threads.history_mode = 'paginated' \
               THEN threads.history_mode ELSE excluded.history_mode END, \
             thread_source = excluded.thread_source, \
             agent_nickname = excluded.agent_nickname, \
             agent_role = excluded.agent_role, \
             agent_path = excluded.agent_path, \
             model_provider = excluded.model_provider, \
             model = excluded.model, \
             reasoning_effort = excluded.reasoning_effort, \
             origin_cwd = excluded.origin_cwd, \
             cli_version = excluded.cli_version, \
             title = excluded.title, \
             preview = COALESCE(NULLIF(excluded.preview, ''), threads.preview), \
             sandbox_policy = excluded.sandbox_policy, \
             approval_mode = excluded.approval_mode, \
             tokens_used = excluded.tokens_used, \
             first_user_message = excluded.first_user_message, \
             archived_at_s = excluded.archived_at_s, \
             git_sha = COALESCE(threads.git_sha, excluded.git_sha), \
             git_branch = COALESCE(threads.git_branch, excluded.git_branch), \
             git_origin_url = COALESCE(threads.git_origin_url, excluded.git_origin_url)"
        )),
        metadata,
        updated_at,
        insert_recency_at,
        memory_mode,
    )
    .execute(&mut *connection)
    .await?;
    insert_spawn_edge_from_source(connection, metadata.id, &metadata.source).await
}

fn bind_thread<'q>(
    query: PgQuery<'q>,
    metadata: &ThreadMetadata,
    updated_at_ms: i64,
    recency_at_ms: i64,
    memory_mode: &str,
) -> PgQuery<'q> {
    let preview = metadata
        .preview
        .as_deref()
        .or(metadata.first_user_message.as_deref())
        .unwrap_or_default()
        .to_string();
    query
        .bind(metadata.id.to_string())
        .bind(metadata.rollout_path.display().to_string())
        .bind(metadata.created_at.timestamp_millis())
        .bind(updated_at_ms)
        .bind(recency_at_ms)
        .bind(metadata.source.clone())
        .bind(metadata.originator.clone())
        .bind(metadata.creator_user_id.clone())
        .bind(metadata.creator_account_id.clone())
        .bind(metadata.history_mode.as_str().to_string())
        .bind(
            metadata
                .thread_source
                .as_ref()
                .map(|source| source.as_str().to_string()),
        )
        .bind(metadata.agent_nickname.clone())
        .bind(metadata.agent_role.clone())
        .bind(metadata.agent_path.clone())
        .bind(metadata.model_provider.clone())
        .bind(metadata.model.clone())
        .bind(metadata.reasoning_effort.as_ref().map(enum_to_string))
        .bind(metadata.cwd.display().to_string())
        .bind(metadata.cli_version.clone())
        .bind(metadata.title.clone())
        .bind(metadata.name.clone())
        .bind(preview)
        .bind(metadata.sandbox_policy.clone())
        .bind(metadata.approval_mode.clone())
        .bind(metadata.tokens_used)
        .bind(metadata.first_user_message.clone().unwrap_or_default())
        .bind(metadata.archived_at.map(|at| at.timestamp()))
        .bind(metadata.section.as_ref().map(|section| section.id.clone()))
        .bind(metadata.section_position)
        .bind(metadata.section_entered_at.map(|at| at.timestamp_millis()))
        .bind(metadata.git_sha.clone())
        .bind(metadata.git_branch.clone())
        .bind(
            metadata
                .git_origin_url
                .as_ref()
                .map(|url| SanitizedGitUrl::as_str(url).to_string()),
        )
        .bind(memory_mode.to_string())
        .bind(metadata.project_id.clone())
        .bind(metadata.daybreak_enabled)
}

fn enum_to_string<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(text)) => text,
        Ok(other) => other.to_string(),
        Err(_) => String::new(),
    }
}

/// A thread spawned by another records the directional edge once, from its source.
async fn insert_spawn_edge_from_source(
    connection: &mut PgConnection,
    child_thread_id: ThreadId,
    source: &str,
) -> Result<()> {
    let parsed = serde_json::from_str(source).or_else(|_| {
        serde_json::from_value::<SessionSource>(serde_json::Value::String(source.to_string()))
    });
    let Some(parent_thread_id) = parsed.ok().and_then(|source| source.parent_thread_id()) else {
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status) \
         VALUES ($1::uuid, $2::uuid, 'open') ON CONFLICT (child_thread_id) DO NOTHING",
    )
    .bind(parent_thread_id.to_string())
    .bind(child_thread_id.to_string())
    .execute(connection)
    .await?;
    Ok(())
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
