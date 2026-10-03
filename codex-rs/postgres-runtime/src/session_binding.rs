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
}

impl BoundWrite<'_> {
    /// Use only data statements; transaction control remains with the original owner.
    pub fn connection(&mut self) -> &mut PgConnection {
        &mut self.transaction
    }

    pub async fn commit(self) -> Result<(), sqlx::Error> {
        self.transaction.commit().await
    }

    pub async fn rollback(self) -> Result<(), sqlx::Error> {
        self.transaction.rollback().await
    }
}

async fn observe(
    connection: &mut PgConnection,
) -> Result<(String, ActivationBinding), ActivationGateError> {
    // One shared lock protects both the state decision and the retained identity.
    let row = sqlx::query(
        "SELECT state, dataset_id::uuid::text AS dataset_id, generation \
         FROM storage_activation WHERE singleton FOR SHARE",
    )
    .fetch_one(connection)
    .await
    .map_err(|_| ActivationGateError::Unavailable)?;
    let state: String = row
        .try_get("state")
        .map_err(|_| ActivationGateError::Unavailable)?;
    let binding = ActivationBinding {
        dataset: row
            .try_get("dataset_id")
            .map_err(|_| ActivationGateError::Unavailable)?,
        generation: row
            .try_get("generation")
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
        let mut tx = connection
            .begin()
            .await
            .map_err(|_| ActivationGateError::Unavailable)?;
        self.prepare_selected_write(&mut tx).await?;
        let (_, observed) = observe(&mut tx).await?;
        // Commit first: publication never claims a successful transaction that failed to end.
        tx.commit()
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
        sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *connection)
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
        sqlx::query("SELECT set_config('search_path', $1, true)")
            .bind(path)
            .execute(connection)
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
        let (state, actual) = observe(connection).await?;
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
        let (_, actual) = observe(connection).await?;
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
}

/// Available original admission causes, not a terminal or rollback capability.
pub enum RetainedWriteAdmissionError {
    Begin(sqlx::Error),
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
        })
    }
}
impl RetainedWriteSession {
    pub fn original_pool(&self) -> &Arc<PostgresPool> {
        &self.original_pool
    }

    /// Borrow from this original connection; first statements admit selected RC,
    /// namespace, then the one original-generation activation FOR SHARE.
    pub async fn begin(&mut self) -> Result<BoundWrite<'_>, RetainedWriteAdmissionError> {
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
        Ok(BoundWrite {
            transaction,
            _original_pool: &self.original_pool,
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
