//! Selects the storage backend for a process before any session machinery starts.
//!
//! A home whose authority records say PostgreSQL is authoritative must run against PostgreSQL or
//! not at all. [`prepare_storage`] connects, proves the store is the dataset this home published,
//! and installs the remote stores in core. It reports failures instead of falling back to local
//! files, because a quiet fallback would create a second writable history.

use codex_core::RemoteBackend;
use codex_core::config::Config;
use codex_keyring_store::DefaultKeyringStore;
use codex_keyring_store::KeyringStore;
use codex_remote_storage::RemoteStorage;
use codex_remote_storage::RemoteStorageError;
use codex_storage_authority::AuthorityState;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::LocalAuthority;
use codex_storage_authority::StorageCandidateProfile;
use thiserror::Error;

/// Why a process could not start against its authoritative storage. Diagnostics carry no
/// profile values, credentials or paths.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RemoteBackendError {
    #[error("the storage authority records are unusable")]
    Authority,
    #[error("a storage cutover was interrupted; recover it before starting")]
    CutoverInProgress,
    #[error("remote storage is authoritative but no remote storage profile is configured")]
    NoProfile,
    #[error("the configured remote storage profile cannot be read")]
    InvalidProfile,
    #[error("remote storage is unavailable: {0}")]
    Storage(RemoteStorageError),
    #[error("the remote store is a different dataset or generation than this home published")]
    DatasetMismatch,
    #[error("remote storage was already installed in this process")]
    AlreadyInstalled,
}

impl RemoteBackendError {
    /// Whether starting again later can succeed without anyone changing configuration.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Storage(error) if error.is_retryable())
    }
}

/// Connect and install the remote stores when this home's authority is PostgreSQL.
///
/// Returns `Ok(false)` for homes that stay local, and `Ok(true)` once the remote stores are
/// installed. Call it once, before building a thread manager or opening any local database.
pub async fn prepare_storage(config: &Config) -> Result<bool, RemoteBackendError> {
    prepare_storage_with(config, &DefaultKeyringStore).await
}

/// [`prepare_storage`] with an explicit credential store, for hosts that keep their own.
pub async fn prepare_storage_with(
    config: &Config,
    keyring: &dyn KeyringStore,
) -> Result<bool, RemoteBackendError> {
    let authority = match codex_storage_authority::authority_state(&config.codex_home)
        .map_err(|_| RemoteBackendError::Authority)?
    {
        AuthorityState::Unmanaged | AuthorityState::Local(_) => return Ok(false),
        AuthorityState::CutoverInProgress(_) => return Err(RemoteBackendError::CutoverInProgress),
        AuthorityState::Remote(authority) => authority,
    };
    let storage = connect(config, &authority, keyring).await?;
    let provider = config.model_provider_id.clone();
    codex_core::install_remote_backend(RemoteBackend {
        original_pool: std::sync::Arc::clone(storage.pool()),
        thread_store: storage.thread_store(provider),
        queue_store: storage.queue_store(),
        agent_graph_store: storage.agent_graph_store(),
    })
    .map_err(|_| RemoteBackendError::AlreadyInstalled)?;
    Ok(true)
}

async fn connect(
    config: &Config,
    authority: &LocalAuthority,
    keyring: &dyn KeyringStore,
) -> Result<RemoteStorage, RemoteBackendError> {
    let candidate = config
        .config_layer_stack
        .storage_candidate()
        .map_err(|_| RemoteBackendError::InvalidProfile)?;
    let Some(StorageCandidateProfile::RemotePostgres(profile)) = candidate else {
        return Err(RemoteBackendError::NoProfile);
    };
    let resolver = HostCredentialResolver::new(keyring);
    let storage = RemoteStorage::connect(&profile, &resolver)
        .await
        .map_err(RemoteBackendError::Storage)?;
    let activation = storage
        .activation()
        .await
        .map_err(RemoteBackendError::Storage)?;
    if activation.retired {
        storage.close().await;
        return Err(RemoteBackendError::Storage(RemoteStorageError::Retired));
    }
    if activation.migrating {
        return Err(RemoteBackendError::Storage(RemoteStorageError::Migrating));
    }
    let published = i64::try_from(authority.identity.generation)
        .map_err(|_| RemoteBackendError::DatasetMismatch)?;
    if activation.dataset_id != Some(authority.identity.dataset_id)
        || activation.generation != published
    {
        storage.close().await;
        return Err(RemoteBackendError::DatasetMismatch);
    }
    Ok(storage)
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
