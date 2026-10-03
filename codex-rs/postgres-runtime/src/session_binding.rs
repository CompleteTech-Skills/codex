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
