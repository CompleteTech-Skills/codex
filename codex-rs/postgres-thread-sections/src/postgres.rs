use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::TransactionError;
use codex_state::PINNED_THREAD_SECTION_ID;
use codex_state::ThreadSection;
use codex_state::ThreadSectionAppearance;
use codex_state::ThreadSectionsPage;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Explicit appearance update, including the difference between retaining and clearing it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppearanceChange {
    Keep,
    Clear,
    Set(ThreadSectionAppearance),
}

/// Redacted section catalog failure. A commit-unknown deletion needs readback.
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
    #[error("PostgreSQL section deletion commit outcome unknown")]
    CommitOutcomeUnknown,
}

/// Fixed-namespace catalog adapter. Construction does not activate PostgreSQL.
#[derive(Clone)]
pub struct PostgresThreadSectionStore {
    pool: Arc<PostgresPool>,
}

impl PostgresThreadSectionStore {
    pub fn new(pool: Arc<PostgresPool>) -> Self {
        Self { pool }
    }

    pub async fn get_thread_section(
        &self,
        id: &str,
    ) -> Result<Option<ThreadSection>, SectionStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query_as::<_, (String, String, Option<String>)>(
                "SELECT id, name, appearance FROM codex_storage.thread_sections WHERE id = $1",
            )
            .bind(id)
            .fetch_optional(&mut *connection),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|_| SectionStoreError::Unavailable)?;
        row.map(read_section).transpose()
    }

    pub async fn list_thread_sections(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ThreadSectionsPage, SectionStoreError> {
        let page_size = limit.max(1);
        let fetch_limit = i64::try_from(page_size.saturating_add(1))
            .map_err(|_| SectionStoreError::InvalidSection)?;
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let rows = timeout(
            QUERY_TIMEOUT,
            sqlx::query_as::<_, (String, String, Option<String>)>(
                "SELECT id, name, appearance FROM codex_storage.thread_sections \
                 WHERE ($1::text IS NULL OR id > $1::text COLLATE \"C\") \
                 ORDER BY id COLLATE \"C\" LIMIT $2",
            )
            .bind(cursor)
            .bind(fetch_limit)
            .fetch_all(&mut *connection),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|_| SectionStoreError::Unavailable)?;
        let mut sections = rows
            .into_iter()
            .map(read_section)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = if sections.len() > page_size {
            sections.pop();
            sections.last().map(|section| section.id.clone())
        } else {
            None
        };
        Ok(ThreadSectionsPage {
            sections,
            next_cursor,
        })
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
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        timeout(
            QUERY_TIMEOUT,
            sqlx::query(
                "INSERT INTO codex_storage.thread_sections (id, name, appearance) VALUES ($1, $2, $3)",
            )
            .bind(&section.id)
            .bind(&section.name)
            .bind(appearance)
            .execute(&mut *connection),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|_| SectionStoreError::Unavailable)?;
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
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query_as::<_, (String, String, Option<String>)>(
                "UPDATE codex_storage.thread_sections SET name = $1, \
                 appearance = CASE WHEN $2 THEN $3 ELSE appearance END \
                 WHERE id = $4 RETURNING id, name, appearance",
            )
            .bind(name)
            .bind(replace_appearance)
            .bind(appearance)
            .bind(id)
            .fetch_optional(&mut *connection),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|_| SectionStoreError::Unavailable)?;
        row.map(read_section).transpose()
    }

    pub async fn delete_thread_section(&self, id: &str) -> Result<bool, SectionStoreError> {
        if id == PINNED_THREAD_SECTION_ID {
            return Err(SectionStoreError::Protected);
        }
        let mut transaction = self
            .pool
            .begin_serializable()
            .await
            .map_err(classify_transaction)?;
        let exists = timeout(
            QUERY_TIMEOUT,
            sqlx::query_scalar::<_, String>(
                "SELECT id FROM codex_storage.thread_sections WHERE id = $1 FOR UPDATE",
            )
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
            sqlx::query(
                "UPDATE codex_storage.threads SET thread_section_id = NULL, \
                 section_position = NULL, section_entered_at_ms = NULL WHERE thread_section_id = $1",
            )
            .bind(id)
            .execute(transaction.connection()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(|error| classify_transaction(TransactionError::classify_statement(&error)))?;
        timeout(
            QUERY_TIMEOUT,
            sqlx::query("DELETE FROM codex_storage.thread_sections WHERE id = $1")
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

fn classify_pool(error: PoolError) -> SectionStoreError {
    match error {
        PoolError::Timeout => SectionStoreError::Timeout,
        PoolError::InvalidSettings
        | PoolError::Authentication
        | PoolError::Tls
        | PoolError::Unavailable
        | PoolError::Closed
        | PoolError::UnsupportedServer => SectionStoreError::Unavailable,
    }
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
