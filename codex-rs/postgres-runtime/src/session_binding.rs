//! Immutable activation identity observed on the pool's original selected namespace.
use crate::ActivationGateError;
use crate::PostgresPool;
use sqlx::Acquire;
use sqlx::PgConnection;
use sqlx::Row;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ActivationBinding {
    dataset: Option<String>,
    generation: i64,
}

/// An actual admitted transaction begun internally on its original immutable bound pool.
/// It cannot be constructed from a bare connection, tuple, or successful earlier readback.
pub struct BoundWrite<'a> {
    transaction: sqlx::Transaction<'a, sqlx::Postgres>,
    _original_pool: &'a PostgresPool,
    acknowledged: Option<(
        &'a mut Option<OriginalTransactionAcknowledgment>,
        Arc<String>,
    )>,
}

impl BoundWrite<'_> {
    /// Use only data statements; transaction control remains with the original owner.
    pub fn connection(&mut self) -> &mut PgConnection {
        &mut self.transaction
    }

    pub async fn commit(self) -> Result<(), sqlx::Error> {
        self.finish_original(true).await
    }

    pub async fn rollback(self) -> Result<(), sqlx::Error> {
        self.finish_original(false).await
    }
    async fn finish_original(mut self, requested_commit: bool) -> Result<(), sqlx::Error> {
        let Some((slot, identity)) = self.acknowledged else {
            return if requested_commit {
                self.transaction.commit().await
            } else {
                self.transaction.rollback().await
            };
        };
        // COMMIT Ok can hide the server's ROLLBACK command tag. Resolve the
        // pre-DML xid on the SAME still-borrowed original physical connection.
        sqlx::query(if requested_commit {
            "COMMIT"
        } else {
            "ROLLBACK"
        })
        .execute(&mut *self.transaction)
        .await?;
        let status: Option<String> =
            sqlx::query_scalar("SELECT pg_catalog.pg_xact_status($1::pg_catalog.xid8)")
                .bind(identity.as_str())
                .fetch_one(&mut *self.transaction)
                .await?;
        let committed = matches!(
            original_outcome(status.as_deref())?,
            OriginalTransactionOutcome::Committed
        );
        // Reserve/capture identity before DML; recording the actual outcome here
        // allocates nothing and precedes later bookkeeping or pool return.
        *slot = Some(OriginalTransactionAcknowledgment {
            identity,
            committed,
        });
        // Resolve SQLx's owned depth bookkeeping. Idle ROLLBACK cannot undo xid;
        // its errors remain original-session uncertainty, not physical closure.
        self.transaction.rollback().await?;
        if committed != requested_commit {
            return Err(sqlx::Error::Protocol(
                "original transaction ended with a different outcome".to_string(),
            ));
        }
        Ok(())
    }
}

async fn observe(
    pool: &PostgresPool,
    connection: &mut PgConnection,
) -> Result<(String, ActivationBinding), ActivationGateError> {
    // One shared lock protects both the state decision and the retained identity.
    let row = pool
        .retain_runtime_query(
            sqlx::query(
                "SELECT state, dataset_id::uuid::text AS dataset_id, generation \
         FROM storage_activation WHERE singleton FOR SHARE",
            )
            .fetch_one(connection),
        )
        .await
        .map_err(|_| ActivationGateError::Unavailable)?;
    let state: String = pool
        .retain_runtime_query(async { row.try_get("state") })
        .await
        .map_err(|_| ActivationGateError::Unavailable)?;
    let binding = ActivationBinding {
        dataset: pool
            .retain_runtime_query(async { row.try_get("dataset_id") })
            .await
            .map_err(|_| ActivationGateError::Unavailable)?,
        generation: pool
            .retain_runtime_query(async { row.try_get("generation") })
            .await
            .map_err(|_| ActivationGateError::Unavailable)?,
    };
    if binding.generation < 0 || !matches!(state.as_str(), "open" | "migrating" | "retired") {
        return Err(ActivationGateError::Unavailable);
    }
    Ok((state, binding))
}

impl PostgresPool {
    /// Begin only from this pool, then admit once before any data access. A foreign
    /// transaction cannot be supplied even if its namespace and scalar tuple match.
    pub async fn begin_selected_write(&self) -> Result<BoundWrite<'_>, ActivationGateError> {
        let mut transaction = tokio::time::timeout(self.acquire_timeout, self.pool.begin())
            .await
            .map_err(|_| ActivationGateError::Unavailable)?
            .map_err(|_| ActivationGateError::Unavailable)?;
        let admission = async {
            self.prepare_selected_write(&mut transaction).await?;
            self.require_bound_storage_open(&mut transaction).await
        }
        .await;
        if let Err(error) = admission {
            transaction
                .rollback()
                .await
                .map_err(|_| ActivationGateError::Unavailable)?;
            return Err(error);
        }
        Ok(BoundWrite {
            transaction,
            _original_pool: self,
            acknowledged: None,
        })
    }
    /// Capture once from actual SQL. A retained pool can never adopt a newer activation.
    /// The namespace must originate from `connect_in_namespace`, not ambient role settings.
    /// Capture is an observation, including migrating/retired snapshots for status display;
    /// it never authorizes writes. The bound write gate separately requires an open state.
    pub async fn bind_current_activation(&self) -> Result<(), ActivationGateError> {
        if self.activation_binding.get().is_some() {
            return Err(ActivationGateError::Unavailable);
        }
        let mut connection = self
            .acquire()
            .await
            .map_err(|_| ActivationGateError::Unavailable)?;
        let mut tx = self
            .retain_runtime_query(connection.begin())
            .await
            .map_err(|_| ActivationGateError::Unavailable)?;
        self.prepare_selected_write(&mut tx).await?;
        let (_, observed) = observe(self, &mut tx).await?;
        // Commit first: publication never claims a successful transaction that failed to end.
        self.retain_runtime_query(tx.commit())
            .await
            .map_err(|_| ActivationGateError::Unavailable)?;
        self.activation_binding
            .set(Arc::new(observed))
            .map_err(|_| ActivationGateError::Unavailable)
    }

    /// Projection of the original observed identity; never accepts a caller-provided tuple.
    pub fn bound_activation_generation(&self) -> Option<i64> {
        self.activation_binding
            .get()
            .map(|binding| binding.generation)
    }

    /// First statements after BEGIN for a selected bound writer. This is an explicit
    /// per-transaction profile; it does not modify global engine or role settings.
    pub async fn prepare_selected_write(
        &self,
        connection: &mut PgConnection,
    ) -> Result<(), ActivationGateError> {
        self.retain_runtime_query(
            sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").execute(&mut *connection),
        )
        .await
        .map_err(|_| ActivationGateError::Unavailable)?;
        self.prepare_selected_namespace(connection).await
    }

    pub(crate) async fn prepare_selected_namespace(
        &self,
        connection: &mut PgConnection,
    ) -> Result<(), ActivationGateError> {
        let path = self
            .selected_search_path
            .as_ref()
            .ok_or(ActivationGateError::Unavailable)?;
        self.retain_runtime_query(
            sqlx::query("SELECT set_config('search_path', $1, true)")
                .bind(path)
                .execute(connection),
        )
        .await
        .map_err(|_| ActivationGateError::Unavailable)?;
        Ok(())
    }

    /// Check a selected writer using its original pool binding and one activation lock.
    /// Call after `prepare_selected_write`, before any data change in this transaction.
    /// An unbound pool is refused, so missing factory wiring cannot silently bypass identity.
    pub async fn require_bound_storage_open(
        &self,
        connection: &mut PgConnection,
    ) -> Result<(), ActivationGateError> {
        let expected = self
            .activation_binding
            .get()
            .ok_or(ActivationGateError::Unavailable)?;
        let (state, actual) = observe(self, connection).await?;
        match state.as_str() {
            "migrating" => Err(ActivationGateError::Migrating),
            "retired" => Err(ActivationGateError::Retired),
            "open" if actual == **expected => Ok(()),
            "open" => Err(ActivationGateError::GenerationChanged),
            _ => Err(ActivationGateError::Unavailable),
        }
    }

    /// Cleanup can release the original lease while its own activation is closed.
    /// It cannot adopt a replacement dataset/generation or authorize new data writes.
    pub(crate) async fn require_bound_storage_cleanup(
        &self,
        connection: &mut PgConnection,
    ) -> Result<(), ActivationGateError> {
        let expected = self
            .activation_binding
            .get()
            .ok_or(ActivationGateError::Unavailable)?;
        let (_, actual) = observe(self, connection).await?;
        if actual == **expected {
            Ok(())
        } else {
            Err(ActivationGateError::GenerationChanged)
        }
    }
}

/// Owns the actual original connection outside its borrowed SQLx transaction.
/// Failed commit/rollback cannot destroy or return that connection to the pool.
/// Host must retain this frame across cancellation; it is not store-wide drain proof.
pub struct RetainedWriteSession {
    original_pool: Arc<PostgresPool>,
    connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    original_transaction: Option<Arc<String>>,
    acknowledged: Option<OriginalTransactionAcknowledgment>,
}

/// Available original admission causes, not a terminal or rollback capability.
pub enum RetainedWriteAdmissionError {
    Begin(sqlx::Error),
    Stamp {
        original: sqlx::Error,
        rollback: Option<sqlx::Error>,
    },
    Gate {
        original: ActivationGateError,
        rollback: Option<sqlx::Error>,
    },
}
impl std::fmt::Debug for RetainedWriteAdmissionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RetainedWriteAdmissionError([retained causes])")
    }
}
impl std::fmt::Display for RetainedWriteAdmissionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("original selected write admission failed")
    }
}
impl std::error::Error for RetainedWriteAdmissionError {}

/// Acknowledges ROLLBACK on this exact original connection only.
/// Does not determine earlier uncertain COMMIT, authorize writes or generation
/// adoption, retire storage, or establish any other session's physical closure.
pub struct OriginalSessionRollback<'a> {
    original: &'a RetainedWriteSession,
}
impl OriginalSessionRollback<'_> {
    pub fn original_pool(&self) -> &Arc<PostgresPool> {
        &self.original.original_pool
    }
}
impl PostgresPool {
    /// Only this genuine pool produces the owner; no supplied connection/tuple.
    pub async fn acquire_retained_write(
        self: &Arc<Self>,
    ) -> Result<RetainedWriteSession, crate::PoolError> {
        let connection = self.acquire().await?;
        Ok(RetainedWriteSession {
            original_pool: Arc::clone(self),
            connection,
            original_transaction: None,
            acknowledged: None,
        })
    }
}
impl RetainedWriteSession {
    pub(crate) fn into_original_hard_close(
        self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), sqlx::Error>> + Send>> {
        use sqlx::Connection;
        // Ownership transfer only; actual future completion is separately owned.
        let original = self.connection.detach();
        Box::pin(original.close_hard())
    }
    // SDK-private bootstrap consumer uses the SAME physical container, without
    // calling selected-write begin or claiming Runtime activation admission.
    pub(crate) fn bootstrap_connection(&mut self) -> &mut PgConnection {
        &mut self.connection
    }

    pub fn original_pool(&self) -> &Arc<PostgresPool> {
        &self.original_pool
    }

    /// Borrow from this original connection; first statements admit selected RC,
    /// namespace, then the one original-generation activation FOR SHARE.
    pub async fn begin(&mut self) -> Result<BoundWrite<'_>, RetainedWriteAdmissionError> {
        if self.original_transaction.is_some() {
            return Err(RetainedWriteAdmissionError::Begin(sqlx::Error::Protocol(
                "original transaction identity is still retained".to_string(),
            )));
        }
        let mut transaction = self
            .connection
            .begin()
            .await
            .map_err(RetainedWriteAdmissionError::Begin)?;
        let admission = async {
            self.original_pool
                .prepare_selected_write(&mut transaction)
                .await?;
            self.original_pool
                .require_bound_storage_open(&mut transaction)
                .await
        }
        .await;
        if let Err(original) = admission {
            let rollback = transaction.rollback().await.err();
            return Err(RetainedWriteAdmissionError::Gate { original, rollback });
        }
        // Capture from the actual selected transaction before application mutation.
        let stamp = sqlx::query_scalar::<_, String>("SELECT pg_catalog.pg_current_xact_id()::text")
            .fetch_one(&mut *transaction)
            .await;
        match stamp {
            Ok(stamp) => self.original_transaction = Some(Arc::new(stamp)),
            Err(original) => {
                let rollback = transaction.rollback().await.err();
                return Err(RetainedWriteAdmissionError::Stamp { original, rollback });
            }
        }
        Ok(BoundWrite {
            transaction,
            _original_pool: &self.original_pool,
            acknowledged: Some((
                &mut self.acknowledged,
                Arc::clone(self.original_transaction.as_ref().ok_or_else(|| {
                    RetainedWriteAdmissionError::Begin(sqlx::Error::Protocol(
                        "original transaction stamp missing".to_string(),
                    ))
                })?),
            )),
        })
    }

    /// Reconcile this retained connection, including queued SQLx rollback.
    /// Earlier error or another pool roundtrip cannot produce this borrowed token.
    /// Error/cancellation leaves the original connection in this owning frame.
    pub async fn rollback_original(&mut self) -> Result<OriginalSessionRollback<'_>, sqlx::Error> {
        sqlx::query("ROLLBACK")
            .execute(&mut *self.connection)
            .await?;
        Ok(OriginalSessionRollback { original: self })
    }
}

/// Opaque, runtime-produced proof of an actual ACK for the pre-DML original xid.
#[derive(Clone)]
pub struct OriginalTransactionAcknowledgment {
    identity: Arc<String>,
    committed: bool,
}
impl OriginalTransactionAcknowledgment {
    pub fn original_xid(&self) -> &str {
        self.identity.as_str()
    }
    pub fn committed(&self) -> bool {
        self.committed
    }
    pub fn shares_original_transaction(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}
impl RetainedWriteSession {
    pub fn original_acknowledgment(&self) -> Option<OriginalTransactionAcknowledgment> {
        self.acknowledged.as_ref().cloned()
    }
}

/// Status of the original transaction, observed on its original physical connection.
/// This grants no current-generation write, retirement, or all-session authority.
pub enum OriginalTransactionOutcome {
    Committed,
    Aborted,
}
impl RetainedWriteSession {
    /// First reconcile queued rollback, then ask about the captured xid8. Idle alone
    /// never resolves an earlier uncertain COMMIT. NULL, in-progress, unknown status
    /// or protocol error refuses while retaining the original connection and stamp.
    pub async fn observe_original_transaction(
        &mut self,
    ) -> Result<OriginalTransactionOutcome, sqlx::Error> {
        let stamp = self.original_transaction.as_ref().ok_or_else(|| {
            sqlx::Error::Protocol("original transaction identity unavailable".to_string())
        })?;
        sqlx::query("ROLLBACK")
            .execute(&mut *self.connection)
            .await?;
        let status: Option<String> =
            sqlx::query_scalar("SELECT pg_catalog.pg_xact_status($1::pg_catalog.xid8)")
                .bind(stamp.as_str())
                .fetch_one(&mut *self.connection)
                .await?;
        original_outcome(status.as_deref())
    }
}

fn original_outcome(status: Option<&str>) -> Result<OriginalTransactionOutcome, sqlx::Error> {
    match status {
        Some("committed") => Ok(OriginalTransactionOutcome::Committed),
        Some("aborted") => Ok(OriginalTransactionOutcome::Aborted),
        _ => Err(sqlx::Error::Protocol(
            "original transaction outcome remains unavailable".to_string(),
        )),
    }
}
#[cfg(test)]
mod original_outcome_tests {
    use super::*;
    #[test]
    fn unavailable_or_in_progress_history_cannot_resolve_original_commit() {
        for status in [
            None,
            Some("in progress"),
            Some("unknown"),
            Some(""),
            Some("COMMITTED"),
        ] {
            assert!(original_outcome(status).is_err());
        }
        assert!(matches!(
            original_outcome(Some("committed")),
            Ok(OriginalTransactionOutcome::Committed)
        ));
        assert!(matches!(
            original_outcome(Some("aborted")),
            Ok(OriginalTransactionOutcome::Aborted)
        ));
    }
}
