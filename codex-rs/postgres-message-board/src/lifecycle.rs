//! Permanent board deletion, serialized with writes across all handles in the namespace.
//!
//! Keep only a root ID tombstone so delayed writes cannot resurrect deleted data.

use crate::board::LOCK_WRITERS;
use crate::board::PostgresAgentMessageBoard;
use crate::board::pool_error;
use crate::board::storage;
use crate::board::storage_message;
use codex_postgres_runtime::PostgresPool;
use codex_protocol::SessionId;
use codex_protocol::error::Result;
use sqlx::Acquire;

impl PostgresAgentMessageBoard {
    /// Permanently removes boards owned by these roots, including their posts and subscriptions.
    /// A child ID does not match its parent board. Unload and archive must not call this.
    /// Safe to retry and independent of whether the feature is currently enabled.
    pub async fn delete_boards(pool: &PostgresPool, roots: &[SessionId]) -> Result<()> {
        if roots.is_empty() {
            return Ok(());
        }
        let mut connection = pool.acquire().await.map_err(pool_error)?;
        let mut tx = connection.begin().await.map_err(storage)?;
        pool.prepare_selected_write(&mut tx)
            .await
            .map_err(|error| storage_message(&error.to_string()))?;
        pool.require_bound_storage_open(&mut tx)
            .await
            .map_err(|error| storage_message(&error.to_string()))?;
        sqlx::query(LOCK_WRITERS)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        for root in roots {
            sqlx::query(
                "INSERT INTO agent_board_deleted (board) VALUES ($1) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(root.to_string())
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
            for statement in [
                "DELETE FROM agent_board_subscriptions WHERE board = $1",
                "DELETE FROM agent_board_opt_outs WHERE board = $1",
                "DELETE FROM agent_board_posts WHERE board = $1",
                "DELETE FROM agent_board_channels WHERE board = $1",
            ] {
                sqlx::query(statement)
                    .bind(root.to_string())
                    .execute(&mut *tx)
                    .await
                    .map_err(storage)?;
            }
        }
        tx.commit().await.map_err(storage)?;
        Ok(())
    }
}
