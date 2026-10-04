use crate::BlockerCode;
use crate::Observer;
use crate::StorageError;
use crate::journal::Journal;
use crate::journal::OperationRecord;
use crate::plan::PlanInputs;
use crate::plan::StoragePlan;
use crate::types::AuthorityLabel;
use crate::types::BackendName;
use crate::types::CheckStage;
use crate::types::ConnectionReport;
use crate::types::PlanAction;
use crate::types::RemoteSummary;
use crate::types::StorageStatus;
use codex_keyring_store::KeyringStore;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_postgres_runtime::bootstrap_named_namespace;
use codex_remote_storage::LoginRole;
use codex_remote_storage::RemoteStorage;
use codex_remote_storage::RemoteStorageError;
use codex_remote_storage::connection_settings;
use codex_state::SqliteConfig;
use codex_storage_authority::AuthorityState;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::RemotePostgresProfile;
use codex_storage_migration::SqliteSource;
use codex_storage_migration::estimate_source;
use codex_storage_migration::target_is_empty;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

/// What the host supplies: where the home is, which profile is saved, and where credentials live.
pub struct StorageServiceInputs {
    pub codex_home: PathBuf,
    pub sqlite: SqliteConfig,
    /// The saved proposal, read from a trusted configuration layer by the host.
    pub candidate: Option<RemotePostgresProfile>,
    pub default_model_provider_id: String,
    /// Names the machine that owns this home, so a client can tell whose storage it controls.
    pub host_label: String,
    pub keyring: Arc<dyn KeyringStore>,
}

/// The one implementation behind the CLI, the API and the TUI.
pub struct StorageService {
    pub(crate) inputs: Arc<StorageServiceInputs>,
    pub(crate) journal: Arc<Journal>,
    running: Arc<std::sync::Mutex<std::collections::HashSet<Uuid>>>,
    pub(crate) migration_startups: Arc<crate::MigrationStartupRegistry>,
    pub(crate) original_operations: Arc<crate::service_operations::Operations>,
    pub(crate) operation_resources: Option<Arc<crate::operation_resources::Resources>>,
}

/// Held while this process drives an operation, so a repeated request cannot drive it twice.
pub(crate) struct RunGuard<'a> {
    service: &'a StorageService,
    operation_id: Uuid,
}

impl Drop for RunGuard<'_> {
    fn drop(&mut self) {
        self.service
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.operation_id);
    }
}

impl StorageService {
    pub fn new(inputs: StorageServiceInputs) -> Self {
        let journal = Journal::new(&inputs.codex_home);
        Self {
            inputs: Arc::new(inputs),
            journal: Arc::new(journal),
            running: Arc::new(std::sync::Mutex::default()),
            original_operations: Arc::default(),
            operation_resources: None,
            migration_startups: Arc::new(crate::MigrationStartupRegistry::default()),
        }
    }

    /// Report every durable change to an operation record to `observer`, so a front end can push
    /// progress instead of being polled.
    pub fn observed_by(self, observer: Observer) -> Self {
        self.journal.set_observer(observer);
        self
    }
    // Same original state owners; no new controller, namespace, or pool selection.
    pub(crate) fn operation_context(&self) -> Self {
        Self {
            inputs: Arc::clone(&self.inputs),
            journal: Arc::clone(&self.journal),
            running: Arc::clone(&self.running),
            migration_startups: Arc::clone(&self.migration_startups),
            original_operations: Arc::clone(&self.original_operations),
            operation_resources: self.operation_resources.as_ref().map(Arc::clone),
        }
    }

    /// Claim the right to drive an operation; `None` means this process already is.
    pub(crate) fn claim(&self, operation_id: Uuid) -> Option<RunGuard<'_>> {
        let inserted = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(operation_id);
        inserted.then_some(RunGuard {
            service: self,
            operation_id,
        })
    }

    /// The machine whose storage this service controls.
    pub fn host_label(&self) -> &str {
        &self.inputs.host_label
    }

    /// The active backend and what blocks changing it. Only a request that asks to probe reaches
    /// the network.
    pub async fn status(&self, probe_remote: bool) -> StorageStatus {
        let state = codex_storage_authority::authority_state(&self.inputs.codex_home);
        let (authority, active_backend, generation, dataset, ever) = match &state {
            Ok(AuthorityState::Unmanaged) => (
                AuthorityLabel::Unmanaged,
                BackendName::LocalSqlite,
                None,
                None,
                false,
            ),
            Ok(AuthorityState::Local(local)) => (
                AuthorityLabel::Local,
                BackendName::LocalSqlite,
                Some(local.identity.generation),
                Some(local.identity.dataset_id),
                local.marker.remote_ever_activated,
            ),
            Ok(AuthorityState::Remote(local)) => (
                AuthorityLabel::Remote,
                BackendName::RemotePostgres,
                Some(local.identity.generation),
                Some(local.identity.dataset_id),
                true,
            ),
            Ok(AuthorityState::CutoverInProgress(intent)) => (
                AuthorityLabel::CutoverInProgress,
                BackendName::LocalSqlite,
                Some(intent.from_generation),
                Some(intent.dataset_id),
                false,
            ),
            Err(_) => (
                AuthorityLabel::Invalid,
                BackendName::LocalSqlite,
                None,
                None,
                false,
            ),
        };
        let mut blockers = Vec::new();
        match authority {
            AuthorityLabel::CutoverInProgress => blockers.push(BlockerCode::CutoverInProgress),
            AuthorityLabel::Invalid => blockers.push(BlockerCode::AuthorityInvalid),
            AuthorityLabel::Unmanaged | AuthorityLabel::Local | AuthorityLabel::Remote => {}
        }
        let remote = if probe_remote && self.inputs.candidate.is_some() {
            let report = self.check_connection().await;
            let matches = match (&state, report.dataset_id) {
                (Ok(AuthorityState::Remote(local)), Some(remote_dataset)) => {
                    Some(local.identity.dataset_id == remote_dataset)
                }
                _ => None,
            };
            if authority == AuthorityLabel::Remote {
                if let Some(code) = report.blocker {
                    blockers.push(code);
                } else if matches == Some(false) {
                    blockers.push(BlockerCode::DatasetMismatch);
                } else {
                    match report.dataset_state.as_deref() {
                        Some("retired") => blockers.push(BlockerCode::DatasetRetired),
                        Some("migrating") => blockers.push(BlockerCode::DatasetMigrating),
                        _ => {}
                    }
                }
            }
            Some(RemoteSummary {
                reachable: report.stage >= CheckStage::Dataset,
                state: report.dataset_state,
                generation: report.generation,
                dataset_id: report.dataset_id,
                dataset_matches: matches,
                blocker: report.blocker,
            })
        } else {
            None
        };
        StorageStatus {
            active_backend,
            authority,
            local_generation: generation,
            dataset_id: dataset,
            remote_ever_activated: ever,
            candidate_configured: self.inputs.candidate.is_some(),
            remote,
            blockers,
        }
    }

    /// Test the saved profile with the runtime login. Nothing is created or changed.
    pub async fn check_connection(&self) -> ConnectionReport {
        let mut report = ConnectionReport {
            stage: CheckStage::Profile,
            blocker: None,
            schema_format: None,
            dataset_state: None,
            dataset_id: None,
            generation: None,
            empty: None,
        };
        let Some(profile) = &self.inputs.candidate else {
            report.blocker = Some(BlockerCode::NoCandidateProfile);
            return report;
        };
        let resolver = HostCredentialResolver::new(self.inputs.keyring.as_ref());
        let storage = match RemoteStorage::connect(profile, &resolver).await {
            Ok(storage) => storage,
            Err(error) => {
                report.stage = match error {
                    RemoteStorageError::UnsupportedNamespace
                    | RemoteStorageError::CaCertificateRequired
                    | RemoteStorageError::MigratorCredentialMissing => CheckStage::Profile,
                    RemoteStorageError::Credential(_) => CheckStage::Credential,
                    RemoteStorageError::Connection(_) => CheckStage::Connect,
                    RemoteStorageError::Schema(_) => CheckStage::Schema,
                    RemoteStorageError::Migrating
                    | RemoteStorageError::Retired
                    | RemoteStorageError::GenerationChanged => CheckStage::Dataset,
                };
                report.blocker = Some(error.into());
                return report;
            }
        };
        report.schema_format = Some(codex_postgres_runtime::client_schema_format());
        report.stage = CheckStage::Dataset;
        match storage.activation().await {
            Ok(activation) => {
                report.generation = Some(activation.generation);
                report.dataset_id = activation.dataset_id;
                report.dataset_state = Some(
                    if activation.retired {
                        "retired"
                    } else if activation.migrating {
                        "migrating"
                    } else {
                        "open"
                    }
                    .to_string(),
                );
            }
            Err(error) => {
                report.blocker = Some(error.into());
                storage.close().await;
                return report;
            }
        }
        finish_target_inspection(&mut report, target_is_empty(storage.pool()).await);
        storage.close().await;
        report
    }

    /// Create or upgrade the dataset's tables with the schema-owner credential. This is the only
    /// action that reads that credential, and it never touches existing history.
    pub async fn initialize_schema(&self) -> Result<i32, StorageError> {
        let profile = self
            .inputs
            .candidate
            .as_ref()
            .ok_or(BlockerCode::NoCandidateProfile)?;
        let resolver = HostCredentialResolver::new(self.inputs.keyring.as_ref());
        let (settings, namespace) = connection_settings(profile, &resolver, LoginRole::Migrator)
            .map_err(|error| StorageError(error.into()))?;
        let pool = PostgresPool::connect(settings)
            .await
            .map_err(|error| StorageError(RemoteStorageError::Connection(error).into()))?;
        let result = match &namespace {
            Some(namespace) => bootstrap_named_namespace(&pool, namespace).await,
            None => bootstrap_codex_storage(&pool).await,
        };
        let _ = pool.close().await;
        result.map_err(|_| StorageError(BlockerCode::SchemaInvalid))?;
        Ok(codex_postgres_runtime::client_schema_format())
    }

    /// Describe what an action would do and what blocks it, without changing anything.
    pub async fn plan(&self, action: PlanAction) -> Result<StoragePlan, StorageError> {
        let inputs = PlanInputs {
            authority: codex_storage_authority::authority_state(&self.inputs.codex_home),
            connection: self.check_connection().await,
            estimate: if action == PlanAction::Migrate {
                let source = SqliteSource::new(self.inputs.sqlite.clone());
                estimate_source(&source)
                    .await
                    .map_err(|_| StorageError(BlockerCode::SourceUnreadable))?
                    .into()
            } else {
                None
            },
            sqlite_home_matches: self.inputs.sqlite.home() == self.inputs.codex_home.as_path(),
        };
        Ok(StoragePlan::build(
            action,
            &self.inputs.host_label,
            self.inputs.candidate.as_ref(),
            inputs,
        ))
    }

    /// One operation by id.
    pub fn operation(&self, operation_id: Uuid) -> Result<OperationRecord, StorageError> {
        self.journal
            .read(operation_id)
            .map_err(|_| StorageError(BlockerCode::Internal))?
            .ok_or(StorageError(BlockerCode::OperationNotFound))
    }

    /// One operation with the copy progress the destination records while it is running. A
    /// reconnecting client sees where the work is, not only where it last saved.
    pub async fn operation_progress(
        &self,
        operation_id: Uuid,
    ) -> Result<OperationRecord, StorageError> {
        let mut record = self.operation(operation_id)?;
        let copying = matches!(
            record.state,
            crate::journal::OperationState::Copying | crate::journal::OperationState::Verifying
        );
        if !copying {
            return Ok(record);
        }
        let Ok(storage) = self.connect().await else {
            return Ok(record);
        };
        // Progress is read-only and keeps the runtime credential.
        let migrator = codex_storage_migration::Migrator::new(
            SqliteSource::new(self.inputs.sqlite.clone()),
            Arc::clone(storage.pool()),
        );
        if let Ok(state) = migrator.activation_state().await
            && let Some(run_id) = record.run_id.or(state.run_id)
            && let Ok(rows) = migrator.progress(run_id).await
        {
            record.copied = rows;
        }
        storage.close().await;
        Ok(record)
    }

    /// Every recorded operation, oldest first.
    pub fn operations(&self) -> Vec<OperationRecord> {
        self.journal.list()
    }
}

pub(super) fn finish_target_inspection(
    report: &mut ConnectionReport,
    result: Result<bool, codex_storage_migration::MigrationError>,
) {
    match result {
        Ok(empty) => {
            report.empty = Some(empty);
            report.stage = CheckStage::Ready;
        }
        Err(error) => {
            report.empty = None;
            report.stage = CheckStage::Dataset;
            report.blocker = Some(error.into());
        }
    }
}
