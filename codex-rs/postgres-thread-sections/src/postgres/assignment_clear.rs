use super::*;
use codex_protocol::ThreadId;

impl PostgresThreadSectionStore {
    /// Clear complete section assignment metadata while retaining the thread.
    /// A missing thread returns false; an already unassigned thread returns true.
    pub async fn clear_thread_section_assignment(
        &self,
        thread_id: ThreadId,
    ) -> Result<bool, SectionStoreError> {
        let schema = &self.schema;
        let mut transaction = self
            .pool
            .begin_serializable()
            .await
            .map_err(classify_transaction)?;
        let changed = timeout(QUERY_TIMEOUT, sqlx::query(AssertSqlSafe(format!(
            "UPDATE \"{schema}\".threads SET thread_section_id=NULL, section_position=NULL, section_entered_at_ms=NULL WHERE id=$1::uuid"
        ))).bind(thread_id.to_string()).execute(transaction.connection()))
            .await.map_err(|_| SectionStoreError::Timeout)?
            .map_err(|error| classify_transaction(TransactionError::classify_statement(&error)))?
            .rows_affected();
        transaction.commit().await.map_err(classify_transaction)?;
        Ok(changed != 0)
    }
}
