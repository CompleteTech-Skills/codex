//! Starting, committing, recovering and cancelling a migration to PostgreSQL.
//!
//! An operation copies and verifies in `start_migration`, then waits in `Ready` until
//! `activate` makes the verified copy authoritative. Both steps are idempotent on the operation
//! id, so a lost response and a repeated request never produce a second copy or a second
//! generation.

use crate::BlockerCode;
use crate::PlanAction;
use crate::StorageError;
use crate::journal::OperationRecord;
use crate::journal::OperationState;
use crate::service::StorageService;
use codex_remote_storage::RemoteStorage;
use codex_storage_authority::ActiveBackend;
use codex_storage_authority::AuthorityState;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::adopt_quiesced_home;
use codex_storage_authority::attach_remote_dataset;
use codex_storage_authority::read_cutover;
use codex_storage_migration::Cutover;
use codex_storage_migration::MigrationError;
use codex_storage_migration::Migrator;
use codex_storage_migration::RecoveryOutcome;
use codex_storage_migration::SqliteSource;
use codex_storage_migration::read_plan;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

/// What the operator promised before work that needs a quiet home.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Confirmation {
    /// Every Codex process that writes this home has been stopped.
    pub writers_stopped: bool,
}

/// How recovery settled the operations that were in flight.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RecoveryReport {
    pub outcome: RecoveryKind,
    pub operations: Vec<OperationRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryKind {
    /// Nothing was in progress.
    Idle,
    /// The destination had already published, so the home was moved to match it.
    RolledForward,
    /// The destination had not published, so the home stayed local and the copy waits.
    RolledBack,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn internal<E>(_: E) -> StorageError {
    StorageError(BlockerCode::Internal)
}

impl StorageService {
    pub(crate) async fn connect(&self) -> Result<std::sync::Arc<RemoteStorage>, StorageError> {
        if self.inputs.candidate.is_none() {
            return Err(StorageError(BlockerCode::NoCandidateProfile));
        }
        self.connect_original_remote()
            .await
            .map_err(|error| StorageError(error.into()))
    }
    pub(crate) async fn connect_original_remote(
        &self,
    ) -> Result<std::sync::Arc<RemoteStorage>, codex_remote_storage::RemoteStorageError> {
        let profile = self.inputs.candidate.as_ref().ok_or(
            codex_remote_storage::RemoteStorageError::Connection(
                codex_postgres_runtime::PoolError::InvalidSettings,
            ),
        )?;
        let resolver = HostCredentialResolver::new(self.inputs.keyring.as_ref());
        let reserved = self
            .operation_resources
            .as_ref()
            .map(|resources| resources.reserve_runtime())
            .transpose()
            .map_err(|_| {
                codex_remote_storage::RemoteStorageError::Connection(
                    codex_postgres_runtime::PoolError::Unavailable,
                )
            })?;
        let pool_slot = self
            .operation_resources
            .as_ref()
            .map(|resources| resources.reserve_runtime_pool())
            .transpose()
            .map_err(|_| {
                codex_remote_storage::RemoteStorageError::Connection(
                    codex_postgres_runtime::PoolError::Unavailable,
                )
            })?;
        let result = if let Some(pool_slot) = pool_slot.as_ref() {
            let mut capture = |pool| {
                pool_slot
                    .capture_original_arc(pool)
                    .map_err(|_| codex_postgres_runtime::PoolError::Unavailable)
            };
            RemoteStorage::connect_with_retained_sdk(profile, &resolver, &mut capture).await
        } else {
            RemoteStorage::connect(profile, &resolver).await
        };
        let storage = match result {
            Ok(storage) => storage,
            Err(original) => {
                if let Some(slot) = reserved {
                    slot.record_returned_failure(std::sync::Arc::new(original));
                }
                if let Some(slot) = pool_slot {
                    slot.record_returned_failure(std::sync::Arc::new(original));
                }
                return Err(original);
            }
        };
        match reserved {
            Some(original) => original.install(storage).map_err(|owning| {
                // Original is already retained in its pre-dispatch resource slot.
                let _ = (&owning.original_value, owning.original_slots());
                codex_remote_storage::RemoteStorageError::Connection(
                    codex_postgres_runtime::PoolError::Unavailable,
                )
            }),
            None => Ok(std::sync::Arc::new(storage)),
        }
    }

    pub(crate) async fn migrator(&self, storage: &RemoteStorage) -> Result<Migrator, StorageError> {
        Ok(Migrator::new(
            SqliteSource::new(self.inputs.sqlite.clone()),
            self.migration_pool_or_close(storage).await?,
        ))
    }

    pub(crate) fn save(
        &self,
        record: &mut OperationRecord,
        state: OperationState,
    ) -> Result<(), StorageError> {
        record.state = state;
        record.updated_at_ms = now_ms();
        self.journal.update(record).map_err(internal)
    }

    pub(crate) fn fail(&self, record: &mut OperationRecord, code: BlockerCode) -> StorageError {
        record.blocker = Some(code);
        let _ = self.save(record, OperationState::Failed);
        StorageError(code)
    }

    /// Copy this home's history into the empty destination and verify it. The operation then
    /// waits in `Ready`; nothing is authoritative until [`StorageService::activate`].
    pub async fn start_migration(
        &self,
        operation_id: Uuid,
        plan_id: Uuid,
        confirmation: Confirmation,
    ) -> Result<OperationRecord, StorageError> {
        let (record, created) = self
            .prepare_migration(operation_id, plan_id, confirmation)
            .await?;
        if created {
            self.run_migration(record).await
        } else {
            Ok(record)
        }
    }

    /// Validate a migration request and record it, without copying anything. Returns the
    /// record and whether this call created it; a repeated request gets the existing record.
    pub async fn prepare_migration(
        &self,
        operation_id: Uuid,
        plan_id: Uuid,
        confirmation: Confirmation,
    ) -> Result<(OperationRecord, bool), StorageError> {
        self.original_observation(
            crate::service_operations::Kind::PrepareMigration,
            Some(operation_id),
            move |service| async move {
                service
                    .prepare_migration_original(operation_id, plan_id, confirmation)
                    .await
            },
        )
        .await
    }
    pub(crate) async fn prepare_migration_original(
        &self,
        operation_id: Uuid,
        plan_id: Uuid,
        confirmation: Confirmation,
    ) -> Result<(OperationRecord, bool), StorageError> {
        if let Some(existing) = self.journal.read(operation_id).map_err(internal)? {
            // A repeated request answers with what is already there.
            return Ok((existing, false));
        }
        if !confirmation.writers_stopped {
            return Err(StorageError(BlockerCode::NotConfirmed));
        }
        let plan = self.plan(PlanAction::Migrate).await?;
        if plan.plan_id != plan_id {
            return Err(StorageError(BlockerCode::StalePlan));
        }
        if let Some(blocker) = plan.blockers.first() {
            return Err(StorageError(*blocker));
        }
        let record = OperationRecord {
            operation_id,
            action: PlanAction::Migrate,
            plan_digest: plan.digest,
            state: OperationState::Planned,
            run_id: None,
            return_source: None,
            created_at_ms: now_ms(),
            updated_at_ms: now_ms(),
            blocker: None,
            copied: Vec::new(),
        };
        self.journal.create(&record).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StorageError(BlockerCode::OperationConflict)
            } else {
                internal(error)
            }
        })?;
        Ok((record, true))
    }

    /// Join a dataset that already exists. Nothing is copied or merged: this home's own history
    /// stays untouched and stops being authoritative. The operator names the dataset they mean,
    /// so a second client can never join a namespace it did not intend to.
    pub async fn attach_dataset(
        &self,
        operation_id: Uuid,
        plan_id: Uuid,
        dataset_id: Uuid,
        confirmation: Confirmation,
    ) -> Result<OperationRecord, StorageError> {
        self.original_observation(
            crate::service_operations::Kind::Attach,
            Some(operation_id),
            move |service| async move {
                service
                    .attach_dataset_original(operation_id, plan_id, dataset_id, confirmation)
                    .await
            },
        )
        .await
    }
    pub(crate) async fn attach_dataset_original(
        &self,
        operation_id: Uuid,
        plan_id: Uuid,
        dataset_id: Uuid,
        confirmation: Confirmation,
    ) -> Result<OperationRecord, StorageError> {
        if let Some(existing) = self.journal.read(operation_id).map_err(internal)? {
            return Ok(existing);
        }
        if !confirmation.writers_stopped {
            return Err(StorageError(BlockerCode::NotConfirmed));
        }
        let plan = self.plan(PlanAction::Attach).await?;
        if plan.plan_id != plan_id {
            return Err(StorageError(BlockerCode::StalePlan));
        }
        if let Some(blocker) = plan.blockers.first() {
            return Err(StorageError(*blocker));
        }
        if plan.connection.dataset_id != Some(dataset_id) {
            return Err(StorageError(BlockerCode::DatasetMismatch));
        }
        let mut record = OperationRecord {
            operation_id,
            action: PlanAction::Attach,
            plan_digest: plan.digest,
            state: OperationState::Planned,
            run_id: None,
            return_source: None,
            created_at_ms: now_ms(),
            updated_at_ms: now_ms(),
            blocker: None,
            copied: Vec::new(),
        };
        self.journal.create(&record).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                StorageError(BlockerCode::OperationConflict)
            } else {
                internal(error)
            }
        })?;
        let storage = match self.connect().await {
            Ok(storage) => storage,
            Err(error) => return Err(self.fail(&mut record, error.0)),
        };
        self.save(&mut record, OperationState::Committing)?;
        let activation = storage.activation().await;
        storage.close().await;
        let generation = match activation {
            Ok(activation)
                if !activation.migrating
                    && !activation.retired
                    && activation.dataset_id == Some(dataset_id) =>
            {
                u64::try_from(activation.generation).unwrap_or(0)
            }
            Ok(_) => return Err(self.fail(&mut record, BlockerCode::DatasetMismatch)),
            Err(error) => return Err(self.fail(&mut record, BlockerCode::from(error))),
        };
        if attach_remote_dataset(&self.inputs.codex_home, dataset_id, generation).is_err() {
            return Err(self.fail(&mut record, BlockerCode::AuthorityInvalid));
        }
        self.save(&mut record, OperationState::Active)?;
        Ok(record)
    }

    /// Copy and verify a prepared migration. Does nothing if this process already drives it.
    pub(crate) async fn run_migration_original(
        &self,
        mut record: OperationRecord,
    ) -> Result<OperationRecord, StorageError> {
        let Some(_claim) = self.claim(record.operation_id) else {
            return Ok(record);
        };
        self.copy_and_verify(&mut record).await?;
        Ok(record)
    }

    async fn copy_and_verify(&self, record: &mut OperationRecord) -> Result<(), StorageError> {
        // The authority records are what the later cutover rewrites, so they must exist first.
        if let Err(error) = adopt_quiesced_home(&self.inputs.codex_home) {
            let _ = error;
            return Err(self.fail(record, BlockerCode::AuthorityInvalid));
        }
        let storage = match self.connect().await {
            Ok(storage) => storage,
            Err(error) => return Err(self.fail(record, error.0)),
        };
        if let Err(error) = self.save(record, OperationState::Copying) {
            return Err(error);
        }
        let migrator = match self.migrator(&storage).await {
            Ok(migrator) => migrator,
            Err(original) => return Err(self.fail(record, original.0)),
        };
        let summary = match migrator.import().await {
            Ok(summary) => summary,
            Err(error) => {
                return Err(self.fail(record, error.into()));
            }
        };
        record.run_id = Some(summary.run_id);
        record.copied = summary
            .domains
            .iter()
            .map(|(domain, rows)| (domain.name().to_string(), *rows))
            .collect();
        self.save(record, OperationState::Verifying)?;
        if let Err(error) = migrator.verify(summary.run_id).await {
            return Err(self.fail(record, error.into()));
        }
        self.save(record, OperationState::Ready)
    }

    /// Make a verified copy authoritative. Repeating the call after it succeeded returns the
    /// same record.
    pub(crate) async fn activate_original(
        &self,
        operation_id: Uuid,
    ) -> Result<OperationRecord, StorageError> {
        let mut record = self.operation(operation_id)?;
        match record.state {
            OperationState::Active => return Ok(record),
            OperationState::Ready | OperationState::Committing => {}
            OperationState::Planned
            | OperationState::Copying
            | OperationState::Verifying
            | OperationState::Failed
            | OperationState::Cancelled => {
                return Err(StorageError(BlockerCode::OperationConflict));
            }
        }
        if record.action == PlanAction::Return {
            return self.activate_return(record).await;
        }
        let run_id = record
            .run_id
            .ok_or(StorageError(BlockerCode::OperationConflict))?;
        // What stays behind is recorded before the home stops being authoritative.
        codex_storage_migration::write_source_manifest(&self.inputs.codex_home, run_id)
            .map_err(internal)?;
        let storage = self.connect().await?;
        if let Err(error) = self.save(&mut record, OperationState::Committing) {
            return Err(error);
        }
        let migrator = match self.migrator(&storage).await {
            Ok(migrator) => migrator,
            Err(original) => {
                // Preserve an earlier cutover/intent; admission refusal proves no
                // new mutation, not that an old Committing operation rolled back.
                record.blocker = Some(original.0);
                self.save(&mut record, OperationState::Committing)?;
                return Err(original);
            }
        };
        let cutover = Cutover::new(self.inputs.codex_home.clone(), migrator);
        // An interrupted earlier attempt left an intent behind; settle it before starting.
        let result = match cutover.recover().await {
            Ok(RecoveryOutcome::RolledForward { .. }) => Ok(()),
            Ok(_) => cutover.execute(run_id).await.map(|_| ()),
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => {
                self.save(&mut record, OperationState::Active)?;
                Ok(record)
            }
            Err(codex_storage_migration::CutoverError::Uncertain) => {
                record.blocker = Some(BlockerCode::CutoverInProgress);
                self.save(&mut record, OperationState::Committing)?;
                Err(StorageError(BlockerCode::CutoverInProgress))
            }
            Err(error) => Err(self.fail(&mut record, error.into())),
        }
    }

    /// Settle an interrupted cutover from the evidence on both sides and bring the records in
    /// line with the outcome.
    pub(crate) async fn recover_original(&self) -> Result<RecoveryReport, StorageError> {
        let records = self.journal.list_checked().map_err(internal)?;
        let returning = read_cutover(&self.inputs.codex_home)
            .map_err(internal)?
            .is_some_and(|intent| intent.target == ActiveBackend::Local)
            || read_plan(&self.inputs.codex_home)
                .map_err(internal)?
                .is_some()
            || records.iter().any(|record| {
                record.action == PlanAction::Return
                    && record.run_id.is_some()
                    && matches!(
                        record.state,
                        OperationState::Copying
                            | OperationState::Verifying
                            | OperationState::Failed
                            | OperationState::Committing
                    )
            });
        if returning {
            return self.recover_return().await;
        }
        let storage = self.connect().await?;
        let cutover = Cutover::new(
            self.inputs.codex_home.clone(),
            self.migrator(&storage).await?,
        );
        let outcome = cutover.recover().await;
        let outcome = outcome.map_err(|error| StorageError(error.into()))?;
        let mut touched = Vec::new();
        for mut record in self
            .journal
            .list_checked()
            .map_err(internal)?
            .into_iter()
            .filter(|record| record.state == OperationState::Committing)
        {
            match outcome {
                RecoveryOutcome::RolledForward { .. } => {
                    record.blocker = None;
                    self.save(&mut record, OperationState::Active)?;
                }
                RecoveryOutcome::RolledBack => {
                    record.blocker = None;
                    self.save(&mut record, OperationState::Ready)?;
                }
                RecoveryOutcome::Idle => continue,
            }
            touched.push(record);
        }
        Ok(RecoveryReport {
            outcome: match outcome {
                RecoveryOutcome::Idle => RecoveryKind::Idle,
                RecoveryOutcome::RolledForward { .. } => RecoveryKind::RolledForward,
                RecoveryOutcome::RolledBack => RecoveryKind::RolledBack,
            },
            operations: touched,
        })
    }

    /// Cancel an operation that has not been activated. The home stays local and the partial
    /// copy stays closed in the destination, where the same source can resume it.
    pub(crate) async fn cancel_original(
        &self,
        operation_id: Uuid,
    ) -> Result<OperationRecord, StorageError> {
        let mut record = self.operation(operation_id)?;
        match record.state {
            OperationState::Cancelled => return Ok(record),
            OperationState::Active => return Err(StorageError(BlockerCode::AlreadyRemote)),
            OperationState::Planned
            | OperationState::Copying
            | OperationState::Verifying
            | OperationState::Ready
            | OperationState::Committing
            | OperationState::Failed => {}
        }
        if record.action == PlanAction::Return {
            return self.cancel_return(record).await;
        }
        let storage = self.connect().await?;
        let migrator = self.migrator(&storage).await?;
        let cutover = Cutover::new(
            self.inputs.codex_home.clone(),
            self.migrator(&storage).await?,
        );
        let aborted = cutover.abort().await;
        if let Err(error) = aborted {
            return Err(StorageError(error.into()));
        }
        let activation = migrator
            .activation_state()
            .await
            .map_err(|error| StorageError(<MigrationError as Into<BlockerCode>>::into(error)))?;
        if let Some(run_id) = record.run_id.or(activation.run_id)
            && activation.migrating
            && activation.run_id == Some(run_id)
        {
            migrator
                .abandon(run_id)
                .await
                .map_err(|error| StorageError(error.into()))?;
        }
        record.blocker = None;
        self.save(&mut record, OperationState::Cancelled)?;
        Ok(record)
    }

    /// Whether the home is currently remote, as the records say.
    pub fn is_remote(&self) -> bool {
        matches!(
            codex_storage_authority::authority_state(&self.inputs.codex_home),
            Ok(AuthorityState::Remote(_))
        )
    }
}

impl StorageService {
    pub async fn run_migration(
        &self,
        record: OperationRecord,
    ) -> Result<OperationRecord, StorageError> {
        match self
            .original_operation(
                crate::service_operations::Kind::Forward,
                Some(record.operation_id),
                Some(record),
            )
            .await?
        {
            crate::service_operations::Output::Record(record) => Ok(*record),
            _ => Err(StorageError(BlockerCode::Internal)),
        }
    }
    pub async fn activate(&self, operation_id: Uuid) -> Result<OperationRecord, StorageError> {
        match self
            .original_operation(
                crate::service_operations::Kind::Activate,
                Some(operation_id),
                None,
            )
            .await?
        {
            crate::service_operations::Output::Record(record) => Ok(*record),
            _ => Err(StorageError(BlockerCode::Internal)),
        }
    }
    pub async fn recover(&self) -> Result<RecoveryReport, StorageError> {
        match self
            .original_operation(crate::service_operations::Kind::Recover, None, None)
            .await?
        {
            crate::service_operations::Output::Recovery(report) => Ok(report),
            _ => Err(StorageError(BlockerCode::Internal)),
        }
    }
    pub async fn cancel(&self, operation_id: Uuid) -> Result<OperationRecord, StorageError> {
        match self
            .original_operation(
                crate::service_operations::Kind::Cancel,
                Some(operation_id),
                None,
            )
            .await?
        {
            crate::service_operations::Output::Record(record) => Ok(*record),
            _ => Err(StorageError(BlockerCode::Internal)),
        }
    }
}
