//! A cheap preview of what a migration would move.

use crate::source::SourceDatabase;
use crate::source::SqliteSource;
use anyhow::Result;
use serde::Deserialize;
use serde::Serialize;
use sqlx::SqlitePool;

/// Row and file counts of a home, read without copying anything.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct SourceEstimate {
    pub threads: u64,
    pub sections: u64,
    pub projects: u64,
    pub attachments: u64,
    pub queued_items: u64,
    pub goals: u64,
    pub logs: u64,
    pub memory_outputs: u64,
    pub board_posts: u64,
    /// Rollout files that exist, and their total size in bytes.
    pub rollout_files: u64,
    pub rollout_bytes: u64,
}

async fn count(pool: &Option<SqlitePool>, statement: &'static str) -> Result<u64> {
    let Some(pool) = pool else { return Ok(0) };
    let rows: i64 = sqlx::query_scalar(statement).fetch_one(pool).await?;
    Ok(u64::try_from(rows).unwrap_or(0))
}

/// Count what the source holds. Missing databases count as empty, like a fresh home.
pub async fn estimate_source(source: &SqliteSource) -> Result<SourceEstimate> {
    let state = source.pool(SourceDatabase::State).await?;
    let goals = source.pool(SourceDatabase::Goals).await?;
    let queue = source.pool(SourceDatabase::Queue).await?;
    let logs = source.pool(SourceDatabase::Logs).await?;
    let memories = source.pool(SourceDatabase::Memories).await?;
    let board = source.pool(SourceDatabase::Board).await?;
    estimate_pools(source, [state, goals, queue, logs, memories, board]).await
}
/// Preview using SDK originals already retained by the external source owner.
pub async fn estimate_retained_source(
    source: &crate::source::RetainedSqliteSource,
) -> Result<SourceEstimate> {
    let mut pools = std::array::from_fn::<_, 6, _>(|_| None);
    for (index, database) in [
        SourceDatabase::State,
        SourceDatabase::Goals,
        SourceDatabase::Queue,
        SourceDatabase::Logs,
        SourceDatabase::Memories,
        SourceDatabase::Board,
    ]
    .into_iter()
    .enumerate()
    {
        pools[index] = source.pool(database).await?;
    }
    match estimate_pools(source.source(), pools).await {
        Ok(estimate) => Ok(estimate),
        Err(cause) => {
            source.retain_query_failure(cause);
            anyhow::bail!("retained source estimate query failed");
        }
    }
}
async fn estimate_pools(
    source: &SqliteSource,
    pools: [Option<SqlitePool>; 6],
) -> Result<SourceEstimate> {
    let [state, goals, queue, logs, memories, board] = pools;
    let mut estimate = SourceEstimate {
        threads: count(&state, "SELECT COUNT(*) FROM threads").await?,
        sections: count(&state, "SELECT COUNT(*) FROM thread_sections").await?,
        projects: count(&state, "SELECT COUNT(*) FROM projects").await?,
        attachments: count(&state, "SELECT COUNT(*) FROM thread_attachments").await?,
        queued_items: count(&queue, "SELECT COUNT(*) FROM queued_items").await?,
        goals: count(&goals, "SELECT COUNT(*) FROM thread_goals").await?,
        logs: count(&logs, "SELECT COUNT(*) FROM logs").await?,
        memory_outputs: count(&memories, "SELECT COUNT(*) FROM stage1_outputs").await?,
        board_posts: count(&board, "SELECT COUNT(*) FROM posts").await?,
        ..SourceEstimate::default()
    };
    if let Some(state) = &state {
        let paths: Vec<String> = sqlx::query_scalar("SELECT rollout_path FROM threads")
            .fetch_all(state)
            .await?;
        for recorded in paths {
            if let Ok(metadata) = tokio::fs::metadata(source.resolve(&recorded)).await
                && metadata.is_file()
            {
                estimate.rollout_files += 1;
                estimate.rollout_bytes += metadata.len();
            }
        }
    }
    Ok(estimate)
}
