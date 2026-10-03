use codex_postgres_runtime::PostgresPool;
use codex_protocol::ThreadId;
use sqlx::Acquire;
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use crate::AgentGraphStore;
use crate::AgentGraphStoreError;
use crate::AgentGraphStoreFuture;
use crate::AgentGraphStoreResult;
use crate::ThreadSpawnEdgeStatus;

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Fixed-namespace PostgreSQL graph adapter. Construction does not select a storage backend.
#[derive(Clone)]
pub struct PostgresAgentGraphStore {
    pool: Arc<PostgresPool>,
}

impl PostgresAgentGraphStore {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }
}

impl AgentGraphStore for PostgresAgentGraphStore {
    fn upsert_thread_spawn_edge(
        &self,
        parent_thread_id: ThreadId,
        child_thread_id: ThreadId,
        status: ThreadSpawnEdgeStatus,
    ) -> AgentGraphStoreFuture<'_, ()> {
        Box::pin(async move {
            let mut connection = self.pool.acquire().await.map_err(|_| internal_error())?;
            timeout(QUERY_TIMEOUT, async {
                let mut tx = connection.begin().await.map_err(|_| internal_error())?;
                self.pool
                    .prepare_selected_write(&mut tx)
                    .await
                    .map_err(|_| internal_error())?;
                self.pool
                    .require_bound_storage_open(&mut tx)
                    .await
                    .map_err(|_| internal_error())?;
                sqlx::query(
                    "INSERT INTO thread_spawn_edges \
                     (parent_thread_id, child_thread_id, status) VALUES ($1::uuid, $2::uuid, $3) \
                     ON CONFLICT (child_thread_id) DO UPDATE SET \
                     parent_thread_id = excluded.parent_thread_id, status = excluded.status",
                )
                .bind(parent_thread_id.to_string())
                .bind(child_thread_id.to_string())
                .bind(status_name(status))
                .execute(&mut *tx)
                .await
                .map_err(|_| internal_error())?;
                tx.commit().await.map_err(|_| internal_error())
            })
            .await
            .map_err(|_| internal_error())?
            .map_err(|_| internal_error())?;
            Ok(())
        })
    }

    fn set_thread_spawn_edge_status(
        &self,
        child_thread_id: ThreadId,
        status: ThreadSpawnEdgeStatus,
    ) -> AgentGraphStoreFuture<'_, ()> {
        Box::pin(async move {
            let mut connection = self.pool.acquire().await.map_err(|_| internal_error())?;
            timeout(QUERY_TIMEOUT, async {
                let mut tx = connection.begin().await.map_err(|_| internal_error())?;
                self.pool
                    .prepare_selected_write(&mut tx)
                    .await
                    .map_err(|_| internal_error())?;
                self.pool
                    .require_bound_storage_open(&mut tx)
                    .await
                    .map_err(|_| internal_error())?;
                sqlx::query(
                    "UPDATE thread_spawn_edges SET status = $2 \
                     WHERE child_thread_id = $1::uuid",
                )
                .bind(child_thread_id.to_string())
                .bind(status_name(status))
                .execute(&mut *tx)
                .await
                .map_err(|_| internal_error())?;
                tx.commit().await.map_err(|_| internal_error())
            })
            .await
            .map_err(|_| internal_error())?
            .map_err(|_| internal_error())?;
            Ok(())
        })
    }

    fn list_thread_spawn_children(
        &self,
        parent_thread_id: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move {
            let mut connection = self.pool.acquire().await.map_err(|_| internal_error())?;
            let rows = timeout(
                QUERY_TIMEOUT,
                sqlx::query(
                    "SELECT child_thread_id::text AS child_thread_id \
                     FROM thread_spawn_edges \
                     WHERE parent_thread_id = $1::uuid \
                     AND ($2::text IS NULL OR status = $2) \
                     ORDER BY child_thread_id",
                )
                .bind(parent_thread_id.to_string())
                .bind(status_filter.map(status_name))
                .fetch_all(&mut *connection),
            )
            .await
            .map_err(|_| internal_error())?
            .map_err(|_| internal_error())?;
            rows.into_iter().map(read_thread_id).collect()
        })
    }

    fn list_thread_spawn_descendants(
        &self,
        root_thread_id: ThreadId,
        status_filter: Option<ThreadSpawnEdgeStatus>,
    ) -> AgentGraphStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move {
            let mut connection = self.pool.acquire().await.map_err(|_| internal_error())?;
            let rows = timeout(
                QUERY_TIMEOUT,
                sqlx::query(
                    "WITH RECURSIVE subtree(child_thread_id, depth, path, cycle) AS ( \
                     SELECT child_thread_id, 1, ARRAY[$1::uuid, child_thread_id], \
                            child_thread_id = $1::uuid \
                     FROM thread_spawn_edges \
                     WHERE parent_thread_id = $1::uuid \
                       AND ($2::text IS NULL OR status = $2) \
                     UNION ALL \
                     SELECT edge.child_thread_id, subtree.depth + 1, \
                            subtree.path || edge.child_thread_id, \
                            edge.child_thread_id = ANY(subtree.path) \
                     FROM thread_spawn_edges AS edge \
                     JOIN subtree ON edge.parent_thread_id = subtree.child_thread_id \
                     WHERE NOT subtree.cycle AND ($2::text IS NULL OR edge.status = $2) \
                     ) \
                     SELECT child_thread_id::text AS child_thread_id, cycle \
                     FROM subtree ORDER BY depth, child_thread_id",
                )
                .bind(root_thread_id.to_string())
                .bind(status_filter.map(status_name))
                .fetch_all(&mut *connection),
            )
            .await
            .map_err(|_| internal_error())?
            .map_err(|_| internal_error())?;
            if rows
                .iter()
                .any(|row| row.try_get::<bool, _>("cycle").unwrap_or(true))
            {
                return Err(internal_error());
            }
            rows.into_iter().map(read_thread_id).collect()
        })
    }
}

fn status_name(status: ThreadSpawnEdgeStatus) -> &'static str {
    match status {
        ThreadSpawnEdgeStatus::Open => "open",
        ThreadSpawnEdgeStatus::Closed => "closed",
    }
}

fn read_thread_id(row: sqlx::postgres::PgRow) -> AgentGraphStoreResult<ThreadId> {
    let id: String = row
        .try_get("child_thread_id")
        .map_err(|_| internal_error())?;
    ThreadId::try_from(id.as_str()).map_err(|_| internal_error())
}

fn internal_error() -> AgentGraphStoreError {
    AgentGraphStoreError::Internal {
        message: "PostgreSQL graph operation failed".to_string(),
    }
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod tests;
