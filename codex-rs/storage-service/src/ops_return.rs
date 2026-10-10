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
use codex_storage_authority::ActiveBackend;
use codex_storage_authority::read_cutover;
use codex_storage_migration::ActivationTarget;
use codex_storage_migration::CutoverError;
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
    fn current_return(&self, record: &OperationRecord) -> Result<(), StorageError> {
        if self.operation(record.operation_id)? != *record || record.action != PlanAction::Return {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        self.return_markers(record)?;
        Ok(())
    }

    fn check_return_intent(&self, record: &OperationRecord) -> Result<(), StorageError> {
        self.return_markers(record)?;
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

    /// Whether this run can only be settled by recovery, never re-exported or failed: its
    /// markers remain (unreadable ones are assumed its own) or Remote authority is unproven.
    fn requires_recovery(&self, record: &OperationRecord) -> bool {
        self.return_markers(record).unwrap_or(true)
            || !matches!(
                codex_storage_authority::authority_state(&self.inputs.codex_home),
                Ok(codex_storage_authority::AuthorityState::Remote(_))
            )
    }

    /// Whether any return marker exists; markers of another run are a conflict.
    fn return_markers(&self, record: &OperationRecord) -> Result<bool, StorageError> {
        let home = &self.inputs.codex_home;
        let intent = read_cutover(home).map_err(internal)?;
        let plan = read_plan(home).map_err(internal)?;
        if intent
            .as_ref()
            .is_some_and(|i| Some(i.run_id) != record.run_id || i.target != ActiveBackend::Local)
            || plan.as_ref().is_some_and(|p| {
                Some(p.run_id) != record.run_id
                    || p.staged_home != self.staged_home(record.operation_id)
            })
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        Ok(intent.is_some() || plan.is_some())
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
                )
                && !self.requires_recovery(&existing);
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
            return self.operation(record.operation_id);
        };
        let _owner = match self.journal.claim_return(record.operation_id) {
            Ok(owner) => owner,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return self.operation(record.operation_id);
            }
            Err(_) => return Err(StorageError(BlockerCode::OperationConflict)),
        };
        self.current_return(&record)?;
        if !matches!(
            record.state,
            OperationState::Planned
                | OperationState::Copying
                | OperationState::Verifying
                | OperationState::Failed
        ) || self.requires_recovery(&record)
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
        let staged_home = self.staged_home(record.operation_id);
        if record.return_source.is_none() {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        let preflight = self.return_source(record).and_then(|source| {
            let config = self.staged_config(&staged_home)?;
            std::fs::create_dir_all(&staged_home)
                .map_err(|_| StorageError(BlockerCode::StagingFailed))?;
            Ok((source, config))
        });
        let (source, config) = match preflight {
            Ok(ready) => ready,
            Err(error) => {
                record.blocker = Some(error.0);
                self.save(record, OperationState::Failed)?;
                return Err(error);
            }
        };
        record.blocker = None;
        self.save(record, OperationState::Copying)?;
        let storage = match self.connect().await {
            Ok(storage) => storage,
            Err(error) => {
                self.current_return(record)?;
                return Err(self.fail(record, error.0));
            }
        };
        if let Err(error) = self.current_return(record) {
            storage.close().await;
            return Err(error);
        }
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
                self.current_return(record)?;
                return Err(self.fail(record, BlockerCode::StagingFailed));
            }
        };
        let exporter = Migrator::new(self.staged_source(config), storage.pool().clone());
        let summary = match exporter.export_owned(&target, run_id, source).await {
            Ok(summary) => summary,
            Err(error) => {
                target.close().await;
                storage.close().await;
                self.current_return(record)?;
                return Err(self.fail(record, error.into()));
            }
        };
        if let Err(error) = self.current_return(record) {
            target.close().await;
            storage.close().await;
            return Err(error);
        }
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
        self.current_return(record)?;
        if let Err(error) = verified {
            return Err(self.fail(record, error.into()));
        }
        self.save(record, OperationState::Ready)
    }

    pub(crate) async fn activate_return(
        &self,
        mut record: OperationRecord,
    ) -> Result<OperationRecord, StorageError> {
        let _claim = self
            .claim(record.operation_id)
            .ok_or(StorageError(BlockerCode::OperationConflict))?;
        let _owner = self
            .journal
            .claim_return(record.operation_id)
            .map_err(|_| StorageError(BlockerCode::OperationConflict))?;
        self.current_return(&record)?;
        if !matches!(
            record.state,
            OperationState::Ready | OperationState::Committing
        ) {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        self.check_return_intent(&record)?;
        let run_id = record
            .run_id
            .ok_or(StorageError(BlockerCode::OperationConflict))?;
        let staged_home = self.staged_home(record.operation_id);
        let config = self.staged_config(&staged_home)?;
        let storage = self.connect().await?;
        if let Err(error) = self
            .current_return(&record)
            .and_then(|_| self.check_return_intent(&record))
            .and_then(|_| self.save(&mut record, OperationState::Committing))
        {
            storage.close().await;
            return Err(error);
        }
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            staged_home,
            Migrator::new(self.staged_source(config), storage.pool().clone()),
        );
        // An interrupted earlier attempt left an intent behind; settle it before starting.
        let result = match returning.recover().await {
            Ok(RecoveryOutcome::RolledForward { .. }) => Ok(()),
            // Nothing left to settle: only the journal is behind if the exact run already retired.
            Ok(_) => match returning.completed(run_id).await {
                Ok(None) => returning.execute(run_id).await.map(|_| ()),
                done => done.map(|_| ()),
            },
            Err(error) => Err(error),
        };
        storage.close().await;
        self.current_return(&record)?;
        match result {
            Ok(()) => {
                self.save(&mut record, OperationState::Active)?;
                Ok(record)
            }
            // With markers left the dataset may be retired: only recovery settles it, never Failed.
            Err(error)
                if !matches!(error, CutoverError::Uncertain)
                    && !self.requires_recovery(&record) =>
            {
                Err(self.fail(&mut record, error.into()))
            }
            Err(_) => {
                record.blocker = Some(BlockerCode::CutoverInProgress);
                self.save(&mut record, OperationState::Committing)?;
                Err(StorageError(BlockerCode::CutoverInProgress))
            }
        }
    }

    /// Settle an interrupted return from the evidence on both sides.
    pub(crate) async fn recover_return(&self) -> Result<RecoveryReport, StorageError> {
        let plan = read_plan(&self.inputs.codex_home).map_err(internal)?;
        let Some(plan) = plan else {
            if read_cutover(&self.inputs.codex_home)
                .map_err(internal)?
                .is_some()
            {
                return Err(StorageError(BlockerCode::OperationConflict));
            }
            return Ok(RecoveryReport {
                outcome: RecoveryKind::Idle,
                operations: Vec::new(),
            });
        };
        let (plan_run, staged) = (plan.run_id, plan.staged_home.clone());
        let owners: Vec<_> = self
            .journal
            .list()
            .into_iter()
            .filter(|record| {
                record.action == PlanAction::Return
                    && matches!(
                        record.state,
                        OperationState::Committing | OperationState::Failed
                    )
                    && record.run_id == Some(plan_run)
                    && self.staged_home(record.operation_id) == staged
            })
            .collect();
        let [record] = owners.as_slice() else {
            return Err(StorageError(BlockerCode::OperationConflict));
        };
        let mut record = record.clone();
        let Some(_claim) = self.claim(record.operation_id) else {
            return Err(StorageError(BlockerCode::OperationConflict));
        };
        let _owner = self
            .journal
            .claim_return(record.operation_id)
            .map_err(|_| StorageError(BlockerCode::OperationConflict))?;
        self.current_return(&record)?;
        self.check_return_intent(&record)?;
        if read_plan(&self.inputs.codex_home)
            .map_err(internal)?
            .as_ref()
            != Some(&plan)
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        let config = self.staged_config(&staged)?;
        let storage = self.connect().await?;
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            plan.staged_home.clone(),
            Migrator::new(self.staged_source(config), storage.pool().clone()),
        );
        let outcome = async {
            self.current_return(&record)?;
            self.check_return_intent(&record)?;
            if read_plan(&self.inputs.codex_home)
                .map_err(internal)?
                .as_ref()
                != Some(&plan)
            {
                return Err(StorageError(BlockerCode::OperationConflict));
            }
            returning
                .recover()
                .await
                .map_err(|error| StorageError(error.into()))
        }
        .await;
        storage.close().await;
        self.current_return(&record)?;
        let outcome = outcome?;
        let mut touched = Vec::new();
        match outcome {
            RecoveryOutcome::RolledForward { .. } => {
                record.blocker = None;
                self.save(&mut record, OperationState::Active)?;
                touched.push(record);
            }
            // The dataset was reopened, so a new return needs a new export.
            RecoveryOutcome::RolledBack => {
                record.blocker = None;
                self.save(&mut record, OperationState::Cancelled)?;
                touched.push(record);
            }
            RecoveryOutcome::Idle => {}
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

    /// Cancel a return. Before retirement the dataset is reopened unchanged; afterwards the
    /// return can only finish, so the completed handoff is settled as `Active`, never `Cancelled`.
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
        self.current_return(&record)?;
        if matches!(
            record.state,
            OperationState::Active | OperationState::Cancelled
        ) {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        let run_id = record
            .run_id
            .ok_or(StorageError(BlockerCode::OperationConflict))?;
        let staged_home = self.staged_home(record.operation_id);
        if record.return_source.is_none() {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        // Copying is durable before any SQL fence; an untouched Planned owner needs no SQL.
        if record.state == OperationState::Planned {
            if !record.copied.is_empty()
                || record.blocker.is_some()
                || self.requires_recovery(&record)
                || !matches!(std::fs::symlink_metadata(&staged_home),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                return Err(StorageError(BlockerCode::OperationConflict));
            }
            self.save(&mut record, OperationState::Cancelled)?;
            return Ok(record);
        }
        self.check_return_intent(&record)?;
        let source = self.return_source(&record)?;
        let config = self.staged_config(&staged_home)?;
        let storage = self.connect().await?;
        let refuse = |error: CutoverError| StorageError(error.into());
        let migrator = Migrator::new(self.staged_source(config.clone()), storage.pool().clone());
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            staged_home,
            Migrator::new(self.staged_source(config), storage.pool().clone()),
        );
        let result = async {
            self.current_return(&record)?;
            self.check_return_intent(&record)?;
            let markers = self.return_markers(&record)?;
            let finished = if markers {
                let outcome = returning.abort().await.map_err(refuse)?;
                matches!(outcome, RecoveryOutcome::RolledForward { .. })
            } else {
                returning.completed(run_id).await.map_err(refuse)?.is_some()
            };
            if !finished {
                // A run that was never retired only read the dataset, so abandoning it reopens it.
                migrator
                    .abandon_export_owned(run_id, source)
                    .await
                    .map_err(|error| StorageError(error.into()))?;
            }
            Ok::<bool, StorageError>(finished)
        }
        .await;
        storage.close().await;
        self.current_return(&record)?;
        let finished = match result {
            Ok(finished) => finished,
            Err(_) if self.requires_recovery(&record) => {
                record.blocker = Some(BlockerCode::CutoverInProgress);
                self.save(&mut record, OperationState::Committing)?;
                return Err(StorageError(BlockerCode::CutoverInProgress));
            }
            Err(error) => return Err(error),
        };
        record.blocker = None;
        let state = if finished {
            OperationState::Active
        } else {
            OperationState::Cancelled
        };
        self.save(&mut record, state)?;
        Ok(record)
    }
}
