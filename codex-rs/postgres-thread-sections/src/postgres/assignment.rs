use super::*;
use codex_postgres_runtime::PostgresTransaction;
use codex_protocol::ThreadId;

/// Position a thread at the end of a section or before an existing member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SectionPlacement {
    Append,
    Before(ThreadId),
}

impl PostgresThreadSectionStore {
    /// Atomically assign/reorder a retained thread, preserving entry time within its section.
    /// Missing threads return false; invalid destinations or before-members refuse changes.
    pub async fn assign_thread_to_section(
        &self,
        thread_id: ThreadId,
        section: &str,
        placement: SectionPlacement,
    ) -> Result<bool, SectionStoreError> {
        let schema = &self.schema;
        let id = thread_id.to_string();
        let mut transaction = self
            .pool
            .begin_serializable()
            .await
            .map_err(classify_transaction)?;
        let current = timeout(
            QUERY_TIMEOUT,
            sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(format!(
                "SELECT thread_section_id FROM \"{schema}\".threads WHERE id=$1::uuid FOR UPDATE"
            )))
            .bind(&id)
            .fetch_optional(transaction.connection()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(statement)?;
        let Some(current) = current else {
            transaction.rollback().await.map_err(classify_transaction)?;
            return Ok(false);
        };
        let exists = timeout(
            QUERY_TIMEOUT,
            sqlx::query_scalar::<_, String>(AssertSqlSafe(format!(
                "SELECT id FROM \"{schema}\".thread_sections WHERE id=$1 FOR UPDATE"
            )))
            .bind(section)
            .fetch_optional(transaction.connection()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)?
        .map_err(statement)?
        .is_some();
        if !exists || matches!(placement, SectionPlacement::Before(before) if before == thread_id) {
            return Err(SectionStoreError::InvalidSection);
        }
        let before = match placement {
            SectionPlacement::Append => None,
            SectionPlacement::Before(before) => Some(before.to_string()),
        };
        let position = timeout(
            QUERY_TIMEOUT,
            move_position(&mut transaction, schema, section, &id, before.as_deref()),
        )
        .await
        .map_err(|_| SectionStoreError::Timeout)??;
        // Same-section reorder changes only rank. Moving across sections records this host's entry time.
        let entered = chrono::Utc::now().timestamp_millis();
        timeout(QUERY_TIMEOUT, sqlx::query(AssertSqlSafe(format!(
            "UPDATE \"{schema}\".threads SET thread_section_id=$1, section_position=$2, section_entered_at_ms=CASE WHEN $3 THEN section_entered_at_ms ELSE $4 END WHERE id=$5::uuid"
        ))).bind(section).bind(position).bind(current.as_deref()==Some(section)).bind(entered).bind(&id)
            .execute(transaction.connection())).await.map_err(|_| SectionStoreError::Timeout)?.map_err(statement)?;
        transaction.commit().await.map_err(classify_transaction)?;
        Ok(true)
    }
}

fn statement(error: sqlx::Error) -> SectionStoreError {
    classify_transaction(TransactionError::classify_statement(&error))
}

async fn move_position(
    transaction: &mut PostgresTransaction,
    schema: &str,
    section: &str,
    id: &str,
    before: Option<&str>,
) -> Result<i64, SectionStoreError> {
    const GAP: i64 = 1_000_000;
    for attempt in 0..2 {
        let position = if let Some(before) = before {
            let upper = sqlx::query_scalar::<_, Option<i64>>(AssertSqlSafe(format!(
                "SELECT section_position FROM \"{schema}\".threads WHERE id=$1::uuid AND thread_section_id=$2"
            ))).bind(before).bind(section).fetch_optional(transaction.connection()).await.map_err(statement)?
                .flatten().ok_or(SectionStoreError::InvalidSection)?;
            let lower = sqlx::query_scalar::<_, Option<i64>>(AssertSqlSafe(format!(
                "SELECT MAX(section_position) FROM \"{schema}\".threads WHERE thread_section_id=$1 AND section_position<$2 AND id<>$3::uuid"
            ))).bind(section).bind(upper).bind(id).fetch_one(transaction.connection()).await.map_err(statement)?;
            match lower {
                Some(lower) if i128::from(upper) - i128::from(lower) > 1 => Some(
                    i64::try_from(i128::from(lower) + (i128::from(upper) - i128::from(lower)) / 2)
                        .map_err(|_| SectionStoreError::InvalidSection)?,
                ),
                Some(_) => None,
                None if upper > 1 => Some(upper / 2),
                None => None,
            }
        } else {
            sqlx::query_scalar::<_, Option<i64>>(AssertSqlSafe(format!(
                "SELECT MAX(section_position) FROM \"{schema}\".threads WHERE thread_section_id=$1 AND id<>$2::uuid"
            ))).bind(section).bind(id).fetch_one(transaction.connection()).await.map_err(statement)?
                .unwrap_or_default().checked_add(GAP)
        };
        if let Some(position) = position {
            return Ok(position);
        }
        if attempt == 0 {
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE \"{schema}\".threads SET section_position=ranked.position FROM (SELECT id,ROW_NUMBER() OVER (ORDER BY section_position ASC NULLS FIRST,id::text COLLATE \"C\" ASC)*$1::bigint AS position FROM \"{schema}\".threads WHERE thread_section_id=$2 AND id<>$3::uuid) ranked WHERE threads.id=ranked.id"
            ))).bind(GAP).bind(section).bind(id).execute(transaction.connection()).await.map_err(statement)?;
        }
    }
    Err(SectionStoreError::InvalidSection)
}
