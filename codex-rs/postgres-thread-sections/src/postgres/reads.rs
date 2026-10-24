use super::*;
use codex_postgres_runtime::PoolError;
use codex_state::ThreadSectionsPage;

impl PostgresThreadSectionStore {
    pub async fn get_thread_section(
        &self,
        id: &str,
    ) -> Result<Option<ThreadSection>, SectionStoreError> {
        let mut connection = self.pool.acquire().await.map_err(classify_pool)?;
        let row = timeout(
            QUERY_TIMEOUT,
            sqlx::query_as::<_, (String, String, Option<String>)>(sqlx::AssertSqlSafe(format!(
                "SELECT id, name, appearance FROM \"{}\".thread_sections WHERE id = $1",
                self.schema
            )))
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
            sqlx::query_as::<_, (String, String, Option<String>)>(sqlx::AssertSqlSafe(format!(
                "SELECT id, name, appearance FROM \"{}\".thread_sections WHERE ($1::text IS NULL OR id > $1::text COLLATE \"C\") ORDER BY id COLLATE \"C\" LIMIT $2", self.schema
            )))
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
