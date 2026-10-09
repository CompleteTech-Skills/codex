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
use crate::ops::Confirmation;
use crate::ops::RecoveryKind;
use crate::ops::RecoveryReport;
use crate::service::StorageService;
use codex_state::SqliteConfig;
use codex_storage_authority::read_cutover;
use codex_storage_migration::CutoverError;
use codex_storage_migration::Migrator;
use codex_storage_migration::RecoveryOutcome;
use codex_storage_migration::ReturnCutover;
use codex_storage_migration::SqliteSource;
use codex_storage_migration::SqliteTarget;
use codex_storage_migration::read_plan;
use codex_utils_absolute_path::AbsolutePathBuf;
use std::path::PathBuf;
use uuid::Uuid;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn internal<E>(_: E) -> StorageError {
    StorageError(BlockerCode::Internal)
}

impl StorageService {
    /// Whether this run can only be settled by recovery, never re-exported or failed: its
    /// markers remain (unreadable ones are assumed its own) or the home is already Local.
    fn requires_recovery(&self, run_id: Uuid) -> bool {
        self.return_markers(Some(run_id)).unwrap_or(true)
            || matches!(
                codex_storage_authority::authority_state(&self.inputs.codex_home),
                Ok(codex_storage_authority::AuthorityState::Local(_))
            )
    }

    /// Whether any return marker exists; markers of another run are a conflict.
    fn return_markers(&self, run_id: Option<Uuid>) -> Result<bool, StorageError> {
        let home = &self.inputs.codex_home;
        let intent = read_cutover(home).map_err(internal)?.map(|i| i.run_id);
        let plan = read_plan(home).map_err(internal)?.map(|p| p.run_id);
        if [intent, plan]
            .into_iter()
            .flatten()
            .any(|owner| Some(owner) != run_id)
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
            return Ok((existing, false));
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
            run_id: None,
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
        if record.run_id.is_some_and(|run| self.requires_recovery(run)) {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        self.export_and_verify(&mut record).await?;
        Ok(record)
    }

    async fn export_and_verify(&self, record: &mut OperationRecord) -> Result<(), StorageError> {
        let staged_home = self.staged_home(record.operation_id);
        std::fs::create_dir_all(&staged_home).map_err(internal)?;
        let config = self.staged_config(&staged_home)?;
        let storage = match self.connect().await {
            Ok(storage) => storage,
            Err(error) => return Err(self.fail(record, error.0)),
        };
        if let Err(error) = self.save(record, OperationState::Copying) {
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
                return Err(self.fail(record, BlockerCode::StagingFailed));
            }
        };
        let exporter = Migrator::new(self.staged_source(config), storage.pool().clone());
        let summary = match exporter.export(&target).await {
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
        let staged_home = self.staged_home(record.operation_id);
        let config = self.staged_config(&staged_home)?;
        let storage = self.connect().await?;
        if let Err(error) = self.save(&mut record, OperationState::Committing) {
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
        match result {
            Ok(()) => {
                self.save(&mut record, OperationState::Active)?;
                Ok(record)
            }
            // With markers left the dataset may be retired: only recovery settles it, never Failed.
            Err(error)
                if !matches!(error, CutoverError::Uncertain) && !self.requires_recovery(run_id) =>
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
            return Ok(RecoveryReport {
                outcome: RecoveryKind::Idle,
                operations: Vec::new(),
            });
        };
        let (plan_run, staged) = (plan.run_id, plan.staged_home.clone());
        let config = self.staged_config(&staged)?;
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
        for mut record in self.journal.list().into_iter().filter(|record| {
            record.action == PlanAction::Return
                && matches!(
                    record.state,
                    OperationState::Committing | OperationState::Failed
                )
                && record.run_id == Some(plan_run)
                && self.staged_home(record.operation_id) == staged
        }) {
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

    /// Cancel a return. Before retirement the dataset is reopened unchanged; afterwards the
    /// return can only finish, so the completed handoff is settled as `Active`, never `Cancelled`.
    pub(crate) async fn cancel_return(
        &self,
        mut record: OperationRecord,
    ) -> Result<OperationRecord, StorageError> {
        let staged_home = self.staged_home(record.operation_id);
        let config = self.staged_config(&staged_home)?;
        let markers = self.return_markers(record.run_id)?;
        let storage = self.connect().await?;
        let refuse = |error: CutoverError| StorageError(error.into());
        let migrator = Migrator::new(self.staged_source(config.clone()), storage.pool().clone());
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            staged_home,
            Migrator::new(self.staged_source(config), storage.pool().clone()),
        );
        let result = async {
            let finished = if markers {
                let outcome = returning.abort().await.map_err(refuse)?;
                matches!(outcome, RecoveryOutcome::RolledForward { .. })
            } else if let Some(run_id) = record.run_id {
                returning.completed(run_id).await.map_err(refuse)?.is_some()
            } else {
                false
            };
            if let Some(run_id) = record.run_id.filter(|_| !finished) {
                // A run that was never retired only read the dataset, so abandoning it reopens it.
                let state = migrator
                    .activation_state()
                    .await
                    .map_err(|error| StorageError(error.into()))?;
                if state.migrating && state.run_id == Some(run_id) {
                    migrator
                        .abandon(run_id)
                        .await
                        .map_err(|error| StorageError(error.into()))?;
                }
            }
            Ok::<bool, StorageError>(finished)
        }
        .await;
        storage.close().await;
        let finished = result?;
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
