use chrono::Utc;
use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
use codex_state::ExternalAgentConfigImportDetailsRecord;
use codex_state::ExternalAgentConfigImportFailureRecord;
use codex_state::ExternalAgentConfigImportHistoryRecord;
use codex_state::ExternalAgentConfigImportSuccessRecord;
use sqlx::Acquire;
use sqlx::Row;
use sqlx::postgres::PgRow;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Redacted failure from import record persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ImportStoreError {
    #[error("PostgreSQL import record operation timed out")]
    Timeout,
    #[error("PostgreSQL import record storage unavailable")]
    Unavailable,
    #[error("invalid PostgreSQL import record")]
    InvalidRecord,
}

/// Fixed-namespace import record adapter. Construction does not activate PostgreSQL.
#[derive(Clone)]
pub struct PostgresExternalAgentImportStore {
    pool: Arc<PostgresPool>,
}

impl PostgresExternalAgentImportStore {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }

    pub async fn record_external_agent_config_import_completed(
        &self,
        import_id: &str,
        provider_id: Option<&str>,
        successes: &[ExternalAgentConfigImportSuccessRecord],
        failures: &[ExternalAgentConfigImportFailureRecord],
    ) -> Result<(), ImportStoreError> {
        let successes =
            serde_json::to_string(successes).map_err(|_| ImportStoreError::InvalidRecord)?;
        let failures =
            serde_json::to_string(failures).map_err(|_| ImportStoreError::InvalidRecord)?;
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        timeout(QUERY_TIMEOUT, async {
            let mut tx = connection
                .begin()
                .await
                .map_err(|_| ImportStoreError::Unavailable)?;
            self.pool
                .prepare_selected_write(&mut tx)
                .await
                .map_err(|_| ImportStoreError::Unavailable)?;
            self.pool
                .require_bound_storage_open(&mut tx)
                .await
                .map_err(|_| ImportStoreError::Unavailable)?;
            sqlx::query(
                "INSERT INTO external_agent_config_imports \
                 (import_id, provider_id, completed_at_ms, successes, failures) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (import_id) DO UPDATE SET \
                 provider_id = excluded.provider_id, \
                 completed_at_ms = excluded.completed_at_ms, \
                 successes = excluded.successes, failures = excluded.failures",
            )
            .bind(import_id)
            .bind(provider_id)
            .bind(Utc::now().timestamp_millis())
            .bind(successes)
            .bind(failures)
            .execute(&mut *tx)
            .await
            .map_err(|_| ImportStoreError::Unavailable)?;
            tx.commit().await.map_err(|_| ImportStoreError::Unavailable)
        })
        .await
        .map_err(|_| ImportStoreError::Timeout)?
        .map_err(|_| ImportStoreError::Unavailable)?;
        Ok(())
    }

    pub async fn external_agent_config_import_details_record(
        &self,
        import_id: &str,
    ) -> Result<Option<ExternalAgentConfigImportDetailsRecord>, ImportStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query(
                "SELECT successes, failures FROM external_agent_config_imports \
                 WHERE import_id = $1",
            )
            .bind(import_id)
            .fetch_optional(&mut *connection),
        )
        .await
        .map_err(|_| ImportStoreError::Timeout)?
        .map_err(|_| ImportStoreError::Unavailable)?;
        row.map(|row| {
            let successes: String = row
                .try_get("successes")
                .map_err(|_| ImportStoreError::InvalidRecord)?;
            let failures: String = row
                .try_get("failures")
                .map_err(|_| ImportStoreError::InvalidRecord)?;
            Ok(ExternalAgentConfigImportDetailsRecord {
                successes: serde_json::from_str(&successes)
                    .map_err(|_| ImportStoreError::InvalidRecord)?,
                failures: serde_json::from_str(&failures)
                    .map_err(|_| ImportStoreError::InvalidRecord)?,
            })
        })
        .transpose()
    }

    pub async fn external_agent_config_import_history_records(
        &self,
    ) -> Result<Vec<ExternalAgentConfigImportHistoryRecord>, ImportStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let rows = timeout(
            QUERY_TIMEOUT,
            sqlx::query(
                "SELECT import_id, provider_id, completed_at_ms, successes, failures \
                 FROM external_agent_config_imports \
                 ORDER BY completed_at_ms DESC, import_id ASC",
            )
            .fetch_all(&mut *connection),
        )
        .await
        .map_err(|_| ImportStoreError::Timeout)?
        .map_err(|_| ImportStoreError::Unavailable)?;
        rows.into_iter().map(read_history).collect()
    }
}

fn classify_pool(error: PoolError) -> ImportStoreError {
    match error {
        PoolError::Timeout => ImportStoreError::Timeout,
        PoolError::InvalidSettings
        | PoolError::Authentication
        | PoolError::Tls
        | PoolError::Unavailable
        | PoolError::Closed
        | PoolError::UnsupportedServer => ImportStoreError::Unavailable,
    }
}

fn read_history(row: PgRow) -> Result<ExternalAgentConfigImportHistoryRecord, ImportStoreError> {
    let import_id = row
        .try_get("import_id")
        .map_err(|_| ImportStoreError::InvalidRecord)?;
    let provider_id = row
        .try_get("provider_id")
        .map_err(|_| ImportStoreError::InvalidRecord)?;
    let completed_at_ms = row
        .try_get("completed_at_ms")
        .map_err(|_| ImportStoreError::InvalidRecord)?;
    let successes: String = row
        .try_get("successes")
        .map_err(|_| ImportStoreError::InvalidRecord)?;
    let failures: String = row
        .try_get("failures")
        .map_err(|_| ImportStoreError::InvalidRecord)?;
    Ok(ExternalAgentConfigImportHistoryRecord {
        import_id,
        provider_id,
        completed_at_ms,
        successes: serde_json::from_str(&successes).map_err(|_| ImportStoreError::InvalidRecord)?,
        failures: serde_json::from_str(&failures).map_err(|_| ImportStoreError::InvalidRecord)?,
    })
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod tests;
