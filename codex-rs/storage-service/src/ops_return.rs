//! Handing authority back from PostgreSQL to this home's local files.
//!
//! The dataset is exported into a staging directory inside the home while it is closed to every
//! writer, verified, and waits in `Ready`. Activating swaps the staged files in, keeps what they
//! replace in a verified backup, and retires the dataset. Nothing is deleted.

use crate::BlockerCode;
use crate::PlanAction;
use crate::StorageError;
use crate::journal::OperationRecord;
use crate::journal::OperationState;
use crate::journal::ReturnSource;
use crate::ops::Confirmation;
use crate::ops::RecoveryKind;
use crate::ops::RecoveryReport;
use crate::service::StorageService;
use codex_state::SqliteConfig;
use codex_storage_authority::read_cutover;
use codex_storage_migration::ActivationTarget;
use codex_storage_migration::Migrator;
use codex_storage_migration::RecoveryOutcome;
use codex_storage_migration::ReturnCutover;
use codex_storage_migration::SqliteSource;
use codex_storage_migration::SqliteTarget;
use codex_storage_migration::read_plan;
use codex_utils_absolute_path::AbsolutePathBuf;
use sha2::Digest;
use std::path::PathBuf;
use uuid::Uuid;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn internal<E>(_: E) -> StorageError {
    StorageError(BlockerCode::Internal)
}

impl StorageService {
    fn check_return_intent(&self, record: &OperationRecord) -> Result<(), StorageError> {
        let source = self.return_source(record)?;
        if let Some(intent) = read_cutover(&self.inputs.codex_home).map_err(internal)?
            && (Some(intent.run_id) != record.run_id
                || intent.dataset_id != source.dataset_id
                || i64::try_from(intent.from_generation).ok() != Some(source.generation)
                || i64::try_from(intent.to_generation).ok() != source.generation.checked_add(1)
                || intent.target != codex_storage_authority::ActiveBackend::Local)
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        if let Some(plan) = read_plan(&self.inputs.codex_home).map_err(internal)?
            && (Some(plan.run_id) != record.run_id
                || plan.staged_home != self.staged_home(record.operation_id))
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        Ok(())
    }
    fn return_source(&self, record: &OperationRecord) -> Result<ActivationTarget, StorageError> {
        let source = record
            .return_source
            .as_ref()
            .ok_or(StorageError(BlockerCode::OperationConflict))?;
        let profile = self
            .inputs
            .candidate
            .as_ref()
            .ok_or(StorageError(BlockerCode::NoCandidateProfile))?;
        let destination = format!(
            "{}:{}/{}/{}",
            profile.endpoint(),
            profile.port(),
            profile.database(),
            profile.namespace()
        );
        if source.destination != destination {
            return Err(StorageError(BlockerCode::DatasetMismatch));
        }
        let local_matches = match codex_storage_authority::authority_state(&self.inputs.codex_home)
        {
            Ok(codex_storage_authority::AuthorityState::Remote(local)) => {
                local.identity.dataset_id == source.dataset_id
                    && i64::try_from(local.identity.generation).ok() == Some(source.generation)
            }
            Ok(codex_storage_authority::AuthorityState::CutoverInProgress(intent)) => {
                Some(intent.run_id) == record.run_id
                    && intent.dataset_id == source.dataset_id
                    && i64::try_from(intent.from_generation).ok() == Some(source.generation)
                    && intent.target == codex_storage_authority::ActiveBackend::Local
            }
            Ok(codex_storage_authority::AuthorityState::Local(local)) => {
                matches!(
                    record.state,
                    OperationState::Committing | OperationState::Failed
                ) && local.identity.dataset_id == source.dataset_id
                    && i64::try_from(local.identity.generation).ok()
                        == source.generation.checked_add(1)
            }
            _ => false,
        };
        if !local_matches {
            return Err(StorageError(BlockerCode::DatasetMismatch));
        }
        Ok(ActivationTarget {
            dataset_id: source.dataset_id,
            generation: source.generation,
        })
    }
    pub(crate) fn staged_home(&self, operation_id: Uuid) -> PathBuf {
        self.inputs
            .codex_home
            .join("storage-staging")
            .join(operation_id.to_string())
    }

    fn staged_config(&self, staged_home: &std::path::Path) -> Result<SqliteConfig, StorageError> {
        AbsolutePathBuf::from_absolute_path_checked(staged_home)
            .map(SqliteConfig::from_sqlite_home)
            .map_err(internal)
    }

    fn staged_source(&self, config: SqliteConfig) -> SqliteSource {
        SqliteSource::new(config).relocated_from(self.inputs.codex_home.clone())
    }

    /// Export the authoritative PostgreSQL dataset into a staged home and verify it. The
    /// operation then waits in `Ready`; nothing local changes until the return is activated.
    pub async fn start_return(
        &self,
        operation_id: Uuid,
        plan_id: Uuid,
        confirmation: Confirmation,
    ) -> Result<OperationRecord, StorageError> {
        let (record, created) = self
            .prepare_return(operation_id, plan_id, confirmation)
            .await?;
        if created {
            self.run_return(record).await
        } else {
            Ok(record)
        }
    }

    /// Validate a return request and record it, without exporting anything.
    pub async fn prepare_return(
        &self,
        operation_id: Uuid,
        plan_id: Uuid,
        confirmation: Confirmation,
    ) -> Result<(OperationRecord, bool), StorageError> {
        if let Some(existing) = self.journal.read(operation_id).map_err(internal)? {
            if existing.action != PlanAction::Return {
                return Err(StorageError(BlockerCode::OperationConflict));
            }
            let expected_plan = uuid::Uuid::from_slice(
                &sha2::Sha256::digest(existing.plan_digest.as_bytes())[..16],
            )
            .map_err(internal)?;
            if !confirmation.writers_stopped || expected_plan != plan_id {
                return Err(StorageError(BlockerCode::NotConfirmed));
            }
            let retry = existing.run_id.is_some()
                && matches!(
                    existing.state,
                    OperationState::Planned
                        | OperationState::Copying
                        | OperationState::Verifying
                        | OperationState::Failed
                );
            return Ok((existing, retry));
        }
        if !confirmation.writers_stopped {
            return Err(StorageError(BlockerCode::NotConfirmed));
        }
        let plan = self.plan(PlanAction::Return).await?;
        if plan.plan_id != plan_id {
            return Err(StorageError(BlockerCode::StalePlan));
        }
        if let Some(blocker) = plan.blockers.first() {
            return Err(StorageError(*blocker));
        }
        let record = OperationRecord {
            operation_id,
            action: PlanAction::Return,
            plan_digest: plan.digest,
            state: OperationState::Planned,
            // Persist the owner before the remote transaction can fence writers. A lost
            // export acknowledgement must not lose the only handle capable of recovery.
            run_id: Some(Uuid::now_v7()),
            return_source: Some(ReturnSource {
                dataset_id: plan
                    .connection
                    .dataset_id
                    .ok_or(StorageError(BlockerCode::DatasetMismatch))?,
                generation: plan
                    .connection
                    .generation
                    .ok_or(StorageError(BlockerCode::DatasetMismatch))?,
                destination: plan.destination,
            }),
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

    /// Export and verify a prepared return. Does nothing if this process already drives it.
    pub async fn run_return(
        &self,
        mut record: OperationRecord,
    ) -> Result<OperationRecord, StorageError> {
        let Some(_claim) = self.claim(record.operation_id) else {
            return Ok(record);
        };
        let _owner = self
            .journal
            .claim_return(record.operation_id)
            .map_err(|_| StorageError(BlockerCode::OperationConflict))?;
        record = self.operation(record.operation_id)?;
        if record.action != PlanAction::Return
            || !matches!(
                record.state,
                OperationState::Planned
                    | OperationState::Copying
                    | OperationState::Verifying
                    | OperationState::Failed
            )
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        self.export_and_verify(&mut record).await?;
        Ok(record)
    }

    async fn export_and_verify(&self, record: &mut OperationRecord) -> Result<(), StorageError> {
        let run_id = record
            .run_id
            .ok_or(StorageError(BlockerCode::OperationConflict))?;
        let source = self.return_source(record)?;
        let staged_home = self.staged_home(record.operation_id);
        std::fs::create_dir_all(&staged_home).map_err(internal)?;
        let config = self.staged_config(&staged_home)?;
        self.save(record, OperationState::Copying)?;
        let storage = match self.connect().await {
            Ok(storage) => storage,
            Err(error) => return Err(self.fail(record, error.0)),
        };
        let target = match SqliteTarget::create(
            config.clone(),
            self.inputs.codex_home.clone(),
            &self.inputs.default_model_provider_id,
        )
        .await
        {
            Ok(target) => target,
            Err(_) => {
                storage.close().await;
                return Err(self.fail(record, BlockerCode::StagingFailed));
            }
        };
        let exporter = Migrator::new(self.staged_source(config), storage.pool().clone());
        let summary = match exporter.export_owned(&target, run_id, source).await {
            Ok(summary) => summary,
            Err(error) => {
                target.close().await;
                storage.close().await;
                return Err(self.fail(record, error.into()));
            }
        };
        record.run_id = Some(summary.run_id);
        record.copied = summary
            .domains
            .iter()
            .map(|(domain, rows)| (domain.name().to_string(), *rows))
            .collect();
        if let Err(error) = self.save(record, OperationState::Verifying) {
            target.close().await;
            storage.close().await;
            return Err(error);
        }
        let verified = exporter.verify(summary.run_id).await;
        target.close().await;
        storage.close().await;
        if let Err(error) = verified {
            return Err(self.fail(record, error.into()));
        }
        self.save(record, OperationState::Ready)
    }

    pub(crate) async fn activate_return(
        &self,
        mut record: OperationRecord,
    ) -> Result<OperationRecord, StorageError> {
        let run_id = record
            .run_id
            .ok_or(StorageError(BlockerCode::OperationConflict))?;
        let Some(_claim) = self.claim(record.operation_id) else {
            return Err(StorageError(BlockerCode::OperationConflict));
        };
        let _owner = self
            .journal
            .claim_return(record.operation_id)
            .map_err(|_| StorageError(BlockerCode::OperationConflict))?;
        record = self.operation(record.operation_id)?;
        let completed_failure = record.state == OperationState::Failed
            && matches!(
                codex_storage_authority::authority_state(&self.inputs.codex_home),
                Ok(codex_storage_authority::AuthorityState::Local(_))
            );
        if record.run_id != Some(run_id)
            || (!matches!(
                record.state,
                OperationState::Ready | OperationState::Committing
            ) && !completed_failure)
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        let source = self.return_source(&record)?;
        self.check_return_intent(&record)?;
        let staged_home = self.staged_home(record.operation_id);
        let config = self.staged_config(&staged_home)?;
        let storage = self.connect().await?;
        let migrator = Migrator::new(self.staged_source(config), storage.pool().clone());
        if let Ok(codex_storage_authority::AuthorityState::Local(local)) =
            codex_storage_authority::authority_state(&self.inputs.codex_home)
        {
            let remote = migrator.activation_state().await;
            let completed = remote.is_ok_and(|remote| {
                remote.retired
                    && remote.run_id == Some(run_id)
                    && remote.dataset_id == Some(source.dataset_id)
                    && source.generation.checked_add(1) == Some(remote.generation)
                    && local.identity.dataset_id == source.dataset_id
                    && i64::try_from(local.identity.generation).ok() == Some(remote.generation)
            });
            if !completed {
                storage.close().await;
                return Err(StorageError(BlockerCode::OperationConflict));
            }
            // A leftover owned plan after authority publication can now be discarded.
            let returning =
                ReturnCutover::new(self.inputs.codex_home.clone(), staged_home, migrator);
            let recovered = returning.recover().await;
            storage.close().await;
            recovered.map_err(|error| StorageError(error.into()))?;
            self.save(&mut record, OperationState::Active)?;
            return Ok(record);
        }
        if let Err(error) = self.save(&mut record, OperationState::Committing) {
            storage.close().await;
            return Err(error);
        }
        let returning = ReturnCutover::new(self.inputs.codex_home.clone(), staged_home, migrator);
        // An interrupted earlier attempt left an intent behind; settle it before starting.
        let result = match returning.recover().await {
            Ok(RecoveryOutcome::RolledForward { .. }) => Ok(()),
            Ok(RecoveryOutcome::RolledBack) => {
                storage.close().await;
                record.blocker = None;
                self.save(&mut record, OperationState::Cancelled)?;
                return Ok(record);
            }
            Ok(RecoveryOutcome::Idle) => returning.execute(run_id).await.map(|_| ()),
            Err(error) => Err(error),
        };
        storage.close().await;
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

    /// Settle an interrupted return from the evidence on both sides.
    pub(crate) async fn recover_return(&self) -> Result<RecoveryReport, StorageError> {
        let plan = read_plan(&self.inputs.codex_home).map_err(internal)?;
        let Some(plan) = plan else {
            let mut operations = Vec::new();
            for record in self.journal.list().into_iter().filter(|record| {
                record.action == PlanAction::Return
                    && record.run_id.is_some()
                    && matches!(
                        record.state,
                        OperationState::Copying
                            | OperationState::Verifying
                            | OperationState::Failed
                            | OperationState::Committing
                    )
            }) {
                let completed_failure = record.state == OperationState::Failed
                    && matches!(
                        codex_storage_authority::authority_state(&self.inputs.codex_home),
                        Ok(codex_storage_authority::AuthorityState::Local(_))
                    );
                operations.push(
                    if record.state == OperationState::Committing || completed_failure {
                        self.activate_return(record).await?
                    } else {
                        self.cancel_return(record).await?
                    },
                );
            }
            return Ok(RecoveryReport {
                outcome: if operations
                    .iter()
                    .any(|record| record.state == OperationState::Active)
                {
                    RecoveryKind::RolledForward
                } else if operations.is_empty() {
                    RecoveryKind::Idle
                } else {
                    RecoveryKind::RolledBack
                },
                operations,
            });
        };
        let owners: Vec<_> = self
            .journal
            .list()
            .into_iter()
            .filter(|record| {
                record.action == PlanAction::Return
                    && record.run_id == Some(plan.run_id)
                    && matches!(
                        record.state,
                        OperationState::Committing | OperationState::Failed
                    )
                    && self.staged_home(record.operation_id) == plan.staged_home
            })
            .collect();
        if owners.len() != 1 {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        self.return_source(&owners[0])?;
        if matches!(
            codex_storage_authority::authority_state(&self.inputs.codex_home),
            Ok(codex_storage_authority::AuthorityState::Local(_))
        ) {
            let completed = self.activate_return(owners[0].clone()).await?;
            return Ok(RecoveryReport {
                outcome: RecoveryKind::RolledForward,
                operations: vec![completed],
            });
        }
        let Some(_claim) = self.claim(owners[0].operation_id) else {
            return Err(StorageError(BlockerCode::OperationConflict));
        };
        let _owner = self
            .journal
            .claim_return(owners[0].operation_id)
            .map_err(|_| StorageError(BlockerCode::OperationConflict))?;
        let owner = self.operation(owners[0].operation_id)?;
        if owner != owners[0] {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        self.check_return_intent(&owner)?;
        let config = self.staged_config(&plan.staged_home)?;
        let storage = self.connect().await?;
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            plan.staged_home,
            Migrator::new(self.staged_source(config), storage.pool().clone()),
        );
        let outcome = returning.recover().await;
        storage.close().await;
        let outcome = outcome.map_err(|error| StorageError(error.into()))?;
        let mut touched = Vec::new();
        for mut record in [owner] {
            match outcome {
                RecoveryOutcome::RolledForward { .. } => {
                    record.blocker = None;
                    self.save(&mut record, OperationState::Active)?;
                }
                // The dataset was reopened, so a new return needs a new export.
                RecoveryOutcome::RolledBack => {
                    record.blocker = None;
                    self.save(&mut record, OperationState::Cancelled)?;
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

    /// Cancel a return that has not retired the dataset. The dataset is reopened unchanged.
    pub(crate) async fn cancel_return(
        &self,
        mut record: OperationRecord,
    ) -> Result<OperationRecord, StorageError> {
        let Some(_claim) = self.claim(record.operation_id) else {
            return Err(StorageError(BlockerCode::OperationConflict));
        };
        let _owner = self
            .journal
            .claim_return(record.operation_id)
            .map_err(|_| StorageError(BlockerCode::OperationConflict))?;
        record = self.operation(record.operation_id)?;
        if record.action != PlanAction::Return
            || matches!(
                record.state,
                OperationState::Active | OperationState::Cancelled
            )
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        let run_id = record
            .run_id
            .ok_or(StorageError(BlockerCode::OperationConflict))?;
        let source = self.return_source(&record)?;
        let staged_home = self.staged_home(record.operation_id);
        let config = self.staged_config(&staged_home)?;
        let storage = self.connect().await?;
        let migrator = Migrator::new(self.staged_source(config.clone()), storage.pool().clone());
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            staged_home,
            Migrator::new(self.staged_source(config), storage.pool().clone()),
        );
        let result = async {
            let intent = read_cutover(&self.inputs.codex_home).map_err(internal)?;
            if let Some(intent) = &intent
                && (intent.run_id != run_id
                    || intent.target != codex_storage_authority::ActiveBackend::Local)
            {
                return Err(StorageError(BlockerCode::OperationConflict));
            }
            if intent.is_some() {
                self.check_return_intent(&record)?;
                returning
                    .abort()
                    .await
                    .map_err(|error| StorageError(error.into()))?;
            } else {
                migrator
                    .abandon_export_owned(run_id, source)
                    .await
                    .map_err(|error| StorageError(error.into()))?;
            }
            Ok(())
        }
        .await;
        storage.close().await;
        result?;
        record.blocker = None;
        self.save(&mut record, OperationState::Cancelled)?;
        Ok(record)
    }
}
