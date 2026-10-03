use crate::RemoteStorageError;
use codex_agent_graph_store::PostgresAgentGraphStore;
use codex_agent_message_board_extension::MessageBoardHost;
use codex_postgres_goal_store::PostgresGoalStore;
use codex_postgres_import_store::PostgresExternalAgentImportStore;
use codex_postgres_log_store::PostgresLogStore;
use codex_postgres_memory_store::PostgresMemoryStore;
use codex_postgres_message_board::PostgresAgentMessageBoard;
use codex_postgres_queue_store::PostgresQueueStore;
use codex_postgres_runtime::ActivationGateError;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::ThreadOwnershipNamespace;
use codex_postgres_runtime::check_runtime_schema;
use codex_postgres_thread_catalog::PostgresThreadCatalog;
use codex_postgres_thread_store::PostgresThreadStore;
use codex_protocol::SessionId;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::RemotePostgresProfile;
use sqlx::Acquire;
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;

/// The schema every client without an explicit named namespace uses.
const DEFAULT_SCHEMA: &str = "codex_storage";
const DEFAULT_RUNTIME_LOGIN: &str = "codex_runtime";
const DEFAULT_MIGRATOR_LOGIN: &str = "codex_migrator";

/// Whether the dataset accepts writes, and which activation it is at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageActivation {
    /// True while a migration holds the dataset and every store refuses writes.
    pub migrating: bool,
    /// True once the dataset was handed back to local storage; it never accepts writes again.
    pub retired: bool,
    /// Increases each time a verified migration is activated.
    pub generation: i64,
    /// The dataset the last activation published; `None` before the first one.
    pub dataset_id: Option<uuid::Uuid>,
}

/// Which of the dataset's two logins a connection uses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoginRole {
    /// Reads and writes data through the stores.
    Runtime,
    /// Creates and upgrades tables; used only by the explicit initialize and upgrade actions.
    Migrator,
}

/// Connection settings and the namespace for one login of the profile's dataset.
///
/// The runtime login uses the profile's `credential`. The migrator login uses its
/// `migrator_credential`, and a profile without one cannot initialize or upgrade a dataset.
pub fn connection_settings(
    profile: &RemotePostgresProfile,
    resolver: &HostCredentialResolver<'_>,
    role: LoginRole,
) -> Result<(ConnectionSettings, Option<NamedNamespace>), RemoteStorageError> {
    let namespace = if profile.namespace() == DEFAULT_SCHEMA {
        None
    } else {
        Some(
            NamedNamespace::new(profile.namespace())
                .map_err(|_| RemoteStorageError::UnsupportedNamespace)?,
        )
    };
    let ca_certificate = profile
        .ca_certificate()
        .ok_or(RemoteStorageError::CaCertificateRequired)?;
    let source = match role {
        LoginRole::Runtime => profile.credential(),
        LoginRole::Migrator => profile
            .migrator_credential()
            .ok_or(RemoteStorageError::MigratorCredentialMissing)?,
    };
    let credential = resolver
        .resolve(source)
        .map_err(RemoteStorageError::Credential)?;
    let username = match (role, namespace.as_ref()) {
        (LoginRole::Runtime, None) => DEFAULT_RUNTIME_LOGIN,
        (LoginRole::Migrator, None) => DEFAULT_MIGRATOR_LOGIN,
        (LoginRole::Runtime, Some(namespace)) => namespace.runtime_login(),
        (LoginRole::Migrator, Some(namespace)) => namespace.migrator_login(),
    };
    let settings = ConnectionSettings {
        host: profile.endpoint().to_owned(),
        port: profile.port(),
        database: profile.database().to_owned(),
        username: username.to_owned(),
        password: credential.into_zeroizing().into(),
        ca_certificate: ca_certificate.to_path_buf(),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(u64::from(profile.connect_timeout_seconds())),
            acquire_timeout: Duration::from_secs(u64::from(profile.pool_acquire_timeout_seconds())),
            max_connections: u32::from(profile.max_connections()),
        },
    };
    Ok((settings, namespace))
}

/// A verified connection to one remote dataset.
#[derive(Clone)]
pub struct RemoteStorage {
    pool: Arc<PostgresPool>,
    namespace: Option<NamedNamespace>,
    connected_generation: i64,
}

impl std::fmt::Debug for RemoteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RemoteStorage([redacted])")
    }
}

impl RemoteStorage {
    /// Connect with the runtime login and verify the dataset without changing it.
    pub async fn connect(
        profile: &RemotePostgresProfile,
        resolver: &HostCredentialResolver<'_>,
    ) -> Result<Self, RemoteStorageError> {
        let (settings, namespace) = connection_settings(profile, resolver, LoginRole::Runtime)?;
        let pool = Arc::new(
            PostgresPool::connect_in_namespace(settings, namespace.as_ref())
                .await
                .map_err(RemoteStorageError::Connection)?,
        );
        check_runtime_schema(&pool)
            .await
            .map_err(RemoteStorageError::Schema)?;
        pool.bind_current_activation()
            .await
            .map_err(activation_error)?;
        let connected_generation = pool.bound_activation_generation().ok_or_else(|| {
            RemoteStorageError::Schema(codex_postgres_runtime::RuntimeSchemaError::Unavailable)
        })?;
        Ok(Self {
            pool,
            namespace,
            connected_generation,
        })
    }

    /// The activation state as the dataset reports it right now.
    pub async fn activation(&self) -> Result<StorageActivation, RemoteStorageError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(RemoteStorageError::Connection)?;
        let row = sqlx::query(
            "SELECT state, generation, dataset_id FROM storage_activation WHERE singleton",
        )
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| {
            RemoteStorageError::Schema(codex_postgres_runtime::RuntimeSchemaError::Unavailable)
        })?;
        let state: String = row.try_get("state").map_err(|_| {
            RemoteStorageError::Schema(codex_postgres_runtime::RuntimeSchemaError::Invalid)
        })?;
        let generation: i64 = row.try_get("generation").map_err(|_| {
            RemoteStorageError::Schema(codex_postgres_runtime::RuntimeSchemaError::Invalid)
        })?;
        let dataset: Option<String> = row.try_get("dataset_id").map_err(|_| {
            RemoteStorageError::Schema(codex_postgres_runtime::RuntimeSchemaError::Invalid)
        })?;
        Ok(StorageActivation {
            migrating: state == "migrating",
            retired: state == "retired",
            generation,
            dataset_id: dataset.and_then(|value| uuid::Uuid::parse_str(&value).ok()),
        })
    }

    /// The pool every store of this handle shares, for hosts that run storage operations.
    pub fn pool(&self) -> &Arc<PostgresPool> {
        &self.pool
    }

    /// The named namespace this handle addresses; `None` is the default schema.
    pub fn namespace(&self) -> Option<&NamedNamespace> {
        self.namespace.as_ref()
    }

    /// The generation this connection was opened against.
    pub fn connected_generation(&self) -> i64 {
        self.connected_generation
    }

    /// Fail unless the dataset is open and still at the generation this connection saw. A host
    /// calls this before it resumes work after a reconnect, so a client never keeps writing a
    /// dataset that was re-activated underneath it.
    pub async fn require_current_generation(&self) -> Result<(), RemoteStorageError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(RemoteStorageError::Connection)?;
        let mut tx = connection.begin().await.map_err(|_| {
            RemoteStorageError::Schema(codex_postgres_runtime::RuntimeSchemaError::Unavailable)
        })?;
        let observed = async {
            self.pool.prepare_selected_write(&mut tx).await?;
            self.pool.require_bound_storage_open(&mut tx).await
        }
        .await;
        // Refusals also finish the original read transaction before reporting the result.
        tx.rollback().await.map_err(|_| {
            RemoteStorageError::Schema(codex_postgres_runtime::RuntimeSchemaError::Unavailable)
        })?;
        observed.map_err(activation_error)
    }

    fn ownership_namespace(&self) -> ThreadOwnershipNamespace {
        self.namespace.clone().map_or(
            ThreadOwnershipNamespace::Default,
            ThreadOwnershipNamespace::Named,
        )
    }

    pub fn thread_catalog(&self) -> PostgresThreadCatalog {
        PostgresThreadCatalog::new(self.pool.clone())
    }

    pub fn thread_store(
        &self,
        default_model_provider_id: impl Into<String>,
    ) -> Arc<PostgresThreadStore> {
        Arc::new(
            PostgresThreadStore::new(self.pool.clone(), default_model_provider_id)
                .with_namespace(self.ownership_namespace()),
        )
    }

    pub fn queue_store(&self) -> Arc<PostgresQueueStore> {
        Arc::new(PostgresQueueStore::new(self.pool.clone()))
    }

    pub fn goal_store(&self) -> Arc<PostgresGoalStore> {
        Arc::new(PostgresGoalStore::new(self.pool.clone()))
    }

    pub fn log_store(&self) -> Arc<PostgresLogStore> {
        Arc::new(PostgresLogStore::new(self.pool.clone()))
    }

    pub fn memory_store(&self) -> Arc<PostgresMemoryStore> {
        Arc::new(PostgresMemoryStore::new(self.pool.clone()))
    }

    pub fn agent_graph_store(&self) -> Arc<PostgresAgentGraphStore> {
        Arc::new(PostgresAgentGraphStore::new(self.pool.clone()))
    }

    pub fn external_import_store(&self) -> Arc<PostgresExternalAgentImportStore> {
        Arc::new(PostgresExternalAgentImportStore::new(self.pool.clone()))
    }

    pub fn message_board(
        &self,
        identity: SessionId,
        host: Arc<dyn MessageBoardHost>,
    ) -> PostgresAgentMessageBoard {
        PostgresAgentMessageBoard::new(self.pool.clone(), identity, host)
    }

    /// Close every connection. Stores built from this handle fail afterwards.
    pub async fn close(&self) {
        let _ = self.pool.close().await;
    }
}

fn activation_error(error: ActivationGateError) -> RemoteStorageError {
    match error {
        ActivationGateError::Migrating => RemoteStorageError::Migrating,
        ActivationGateError::Retired => RemoteStorageError::Retired,
        ActivationGateError::GenerationChanged => RemoteStorageError::GenerationChanged,
        ActivationGateError::Unavailable => {
            RemoteStorageError::Schema(codex_postgres_runtime::RuntimeSchemaError::Unavailable)
        }
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
