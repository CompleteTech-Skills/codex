mod assignment;
pub use assignment::SectionPlacement;
mod assignment_clear;
mod reads;

use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::TransactionError;
use codex_state::PINNED_THREAD_SECTION_ID;
use codex_state::ThreadSection;
use codex_state::ThreadSectionAppearance;
use sqlx::AssertSqlSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const QUERY_TIMEOUT: Duration = Duration::from_secs(/*secs*/ 30);

/// Explicit appearance update, including the difference between retaining and clearing it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppearanceChange {
    Keep,
    Clear,
    Set(ThreadSectionAppearance),
}

/// Redacted section mutation failure. Unknown outcomes need durable readback, never blind retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SectionStoreError {
    #[error("PostgreSQL section operation timed out")]
    Timeout,
    #[error("PostgreSQL section storage unavailable")]
    Unavailable,
    #[error("invalid PostgreSQL section data")]
    InvalidSection,
    #[error("built-in pinned section cannot be changed")]
    Protected,
    #[error("PostgreSQL section transaction serialization conflict")]
    SerializationConflict,
    #[error("PostgreSQL section transaction deadlocked")]
    Deadlock,
    #[error("PostgreSQL section mutation commit outcome unknown")]
    CommitOutcomeUnknown,
}

/// Namespace selected by a trusted host, without granting activation or schema authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SectionNamespace {
    Default,
    Named(NamedNamespace),
}

/// Unwired section mutations. Requires a qualified v5+ namespace and cooperating writers.
/// Production backend selection and writer fencing remain outside this stage.
#[derive(Clone)]
pub struct PostgresThreadSectionStore {
    pool: Arc<PostgresPool>,
    schema: String,
}

impl PostgresThreadSectionStore {
    pub fn new(pool: Arc<PostgresPool>, namespace: SectionNamespace) -> Self {
        let schema = match namespace {
            SectionNamespace::Default => "codex_storage".to_owned(),
            SectionNamespace::Named(namespace) => namespace.schema_name().to_owned(),
        };
        Self { pool, schema }
    }

    pub async fn create_thread_section(
        &self,
        name: &str,
        appearance: Option<ThreadSectionAppearance>,
    ) -> Result<ThreadSection, SectionStoreError> {
        let section = ThreadSection {
            id: Uuid::now_v7().to_string(),
            name: name.to_string(),
            appearance,
        };
        let appearance = section
            .appearance
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| SectionStoreError::InvalidSection)?;
        let schema = &self.schema;
        let mut transaction = self
            .pool
            .begin_serializable()
            .await
            .map_err(classify_transaction)?;
        timeout(
            QUERY_TIMEOUT,
            sqlx::query(AssertSqlSafe(format!(
                "INSERT INTO \"{schema}\".thread_sections (id, name, appearance) VALUES ($1, $2, $3)"
            )))
            .bind(&section.id)
            .bind(&section.name)
            .bind(appearance)
            .execute(transaction.connection()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|error| classify_transaction(TransactionError::classify_statement(&error)))?;
        transaction.commit().await.map_err(classify_transaction)?;
        Ok(section)
    }

    pub async fn rename_thread_section(
        &self,
        id: &str,
        name: &str,
        change: AppearanceChange,
    ) -> Result<Option<ThreadSection>, SectionStoreError> {
        if id == PINNED_THREAD_SECTION_ID {
            return Err(SectionStoreError::Protected);
        }
        let replace_appearance = !matches!(change, AppearanceChange::Keep);
        let appearance = match change {
            AppearanceChange::Keep | AppearanceChange::Clear => None,
            AppearanceChange::Set(appearance) => Some(
                serde_json::to_string(&appearance)
                    .map_err(|_| SectionStoreError::InvalidSection)?,
            ),
        };
        let schema = &self.schema;
        let mut transaction = self
            .pool
            .begin_serializable()
            .await
            .map_err(classify_transaction)?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query_as::<_, (String, String, Option<String>)>(AssertSqlSafe(format!(
                "UPDATE \"{schema}\".thread_sections SET name = $1, \
                 appearance = CASE WHEN $2 THEN $3 ELSE appearance END \
                 WHERE id = $4 RETURNING id, name, appearance"
            )))
            .bind(name)
            .bind(replace_appearance)
            .bind(appearance)
            .bind(id)
            .fetch_optional(transaction.connection()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|error| classify_transaction(TransactionError::classify_statement(&error)))?;
        let section = row.map(read_section).transpose()?;
        transaction.commit().await.map_err(classify_transaction)?;
        Ok(section)
    }

    pub async fn delete_thread_section(&self, id: &str) -> Result<bool, SectionStoreError> {
        if id == PINNED_THREAD_SECTION_ID {
            return Err(SectionStoreError::Protected);
        }
        let schema = &self.schema;
        let mut transaction = self
            .pool
            .begin_serializable()
            .await
            .map_err(classify_transaction)?;
        let exists = timeout(
            QUERY_TIMEOUT,
            sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
                "SELECT id FROM \"{schema}\".thread_sections WHERE id = $1 FOR UPDATE"
            )))
            .bind(id)
            .fetch_optional(transaction.connection()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|error| classify_transaction(TransactionError::classify_statement(&error)))?
        .is_some();
        if !exists {
            transaction.rollback().await.map_err(classify_transaction)?;
            return Ok(false);
        }
        timeout(
            QUERY_TIMEOUT,
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE \"{schema}\".threads SET thread_section_id = NULL, \
                 section_position = NULL, section_entered_at_ms = NULL WHERE thread_section_id = $1"
            )))
            .bind(id)
            .execute(transaction.connection()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|error| classify_transaction(TransactionError::classify_statement(&error)))?;
        timeout(
            QUERY_TIMEOUT,
            sqlx::query(AssertSqlSafe(format!(
                "DELETE FROM \"{schema}\".thread_sections WHERE id = $1"
            )))
            .bind(id)
            .execute(transaction.connection()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|error| classify_transaction(TransactionError::classify_statement(&error)))?;
        transaction.commit().await.map_err(classify_transaction)?;
        Ok(true)
    }
}

fn read_section(
    (id, name, appearance): (String, String, Option<String>),
) -> Result<ThreadSection, SectionStoreError> {
    Ok(ThreadSection {
        id,
        name,
        appearance: appearance
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(|_| SectionStoreError::InvalidSection)?,
    })
}

fn classify_transaction(error: TransactionError) -> SectionStoreError {
    match error {
        TransactionError::Timeout => SectionStoreError::Timeout,
        TransactionError::SerializationConflict => SectionStoreError::SerializationConflict,
        TransactionError::Deadlock => SectionStoreError::Deadlock,
        TransactionError::CommitOutcomeUnknown => SectionStoreError::CommitOutcomeUnknown,
        TransactionError::Privilege
        | TransactionError::Rejected
        | TransactionError::Unavailable => SectionStoreError::Unavailable,
    }
}

#[cfg(test)]
#[path = "postgres_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "section_read_tests.rs"]
mod read_tests;

#[cfg(test)]
#[path = "section_assignment_clear_tests.rs"]
mod assignment_clear_tests;

#[cfg(test)]
#[path = "section_assignment_tests.rs"]
mod assignment_tests;

#[cfg(test)]
#[path = "section_assignment_concurrency_tests.rs"]
mod assignment_concurrency_tests;
