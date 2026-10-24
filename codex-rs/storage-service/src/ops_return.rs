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
use codex_storage_authority::ActiveBackend;
use codex_storage_authority::AuthorityState;
use codex_storage_authority::authority_state;
use codex_storage_authority::read_cutover;
use codex_storage_migration::ActivationTarget;
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

/// Check the run-bound backup left by a completed return without following replaced paths.
fn completed_return_backup_matches(
    home: &std::path::Path,
    staged_home: &std::path::Path,
    run_id: Uuid,
) -> Result<bool, StorageError> {
    let backup_root = home.join("storage-backups");
    let backup_dir = backup_root.join(format!("{run_id}-before-return"));
    for directory in [&backup_root, &backup_dir] {
        let metadata = match std::fs::symlink_metadata(directory) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(internal(error)),
        };
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Ok(false);
        }
    }
    let manifest = backup_dir.join("manifest.json");
    let metadata = match std::fs::symlink_metadata(&manifest) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(internal(error)),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let plan: codex_storage_migration::InstallPlan =
        serde_json::from_slice(&std::fs::read(manifest).map_err(internal)?).map_err(internal)?;
    if plan.run_id != run_id || plan.staged_home != staged_home || plan.backup_dir != backup_dir {
        return Ok(false);
    }
    let mut names = std::collections::HashSet::new();
    for unit in &plan.units {
        if unit.name.is_empty()
            || unit.name == "."
            || unit.name == ".."
            || unit
                .name
                .chars()
                .any(|character| matches!(character, '/' | '\\' | ':'))
            || !names.insert(unit.name.as_str())
        {
            return Ok(false);
        }
    }
    codex_storage_migration::verify_backup(&plan).map_err(internal)
}

impl StorageService {
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

    /// `None` means the activation is not retired; a retired but mismatched owner is `Some(false)`.
    async fn retired_return_owner_matches(
        &self,
        operation_id: Uuid,
        run_id: Uuid,
        dataset_id: Uuid,
        source_generation: u64,
        target_generation: u64,
    ) -> Result<Option<bool>, StorageError> {
        let staged_home = self.staged_home(operation_id);
        let config = self.staged_config(&staged_home)?;
        let fingerprint = self
            .staged_source(config)
            .fingerprint()
            .await
            .map_err(internal)?;
        let storage = self.connect().await?;
        let result: Result<Option<bool>, StorageError> = async {
            let mut connection = storage
                .pool()
                .acquire()
                .await
                .map_err(|_| StorageError(BlockerCode::ConnectionFailed))?;
            let row = sqlx::query_as::<
                _,
                (
                    String,
                    Option<Uuid>,
                    i64,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                ),
            >(
                "SELECT activation.state, activation.run_id, activation.generation, \
                        activation.dataset_id, migration_run.direction, \
                        migration_run.source_fingerprint, migration_run.state \
                 FROM storage_activation AS activation \
                 LEFT JOIN storage_migration_runs AS migration_run \
                   ON migration_run.run_id = activation.run_id \
                 WHERE activation.singleton",
            )
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| StorageError(BlockerCode::ConnectionFailed))?;
            let Some((state, held_run, generation, dataset, direction, source, run_state)) = row
            else {
                return Ok(None);
            };
            if state != "retired" {
                return Ok(None);
            }
            let source_generation = i64::try_from(source_generation).ok();
            let target_generation = i64::try_from(target_generation).ok();
            Ok(Some(
                held_run == Some(run_id)
                    && source_generation.and_then(|value| value.checked_add(1))
                        == target_generation
                    && target_generation == Some(generation)
                    && dataset.as_deref() == Some(dataset_id.to_string().as_str())
                    && direction.as_deref() == Some("export")
                    && source.as_deref() == Some(fingerprint.as_str())
                    && run_state.as_deref() == Some("activated"),
            ))
        }
        .await;
        storage.close().await;
        result
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
        self.export_and_verify(&mut record).await?;
        Ok(record)
    }

    async fn export_and_verify(&self, record: &mut OperationRecord) -> Result<(), StorageError> {
        let storage = match self.connect().await {
            Ok(storage) => storage,
            Err(error) => return Err(self.fail(record, error.0)),
        };
        self.save(record, OperationState::Copying)?;
        let staged_home = self.staged_home(record.operation_id);
        std::fs::create_dir_all(&staged_home).map_err(internal)?;
        let config = self.staged_config(&staged_home)?;
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
        self.save(record, OperationState::Verifying)?;
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
        let storage = self.connect().await?;
        self.save(&mut record, OperationState::Committing)?;
        let staged_home = self.staged_home(record.operation_id);
        let config = self.staged_config(&staged_home)?;
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            staged_home,
            Migrator::new(self.staged_source(config), storage.pool().clone()),
        );
        // An interrupted earlier attempt left an intent behind; settle it before starting.
        let result = match returning.recover().await {
            Ok(RecoveryOutcome::RolledForward { .. }) => Ok(()),
            Ok(_) => returning.execute(run_id).await.map(|_| ()),
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
        let intent = read_cutover(&self.inputs.codex_home).map_err(internal)?;
        let plan = read_plan(&self.inputs.codex_home).map_err(internal)?;
        let run_id = intent
            .as_ref()
            .map(|intent| intent.run_id)
            .or_else(|| plan.as_ref().map(|plan| plan.run_id));
        let Some(run_id) = run_id else {
            return Ok(RecoveryReport {
                outcome: RecoveryKind::Idle,
                operations: Vec::new(),
            });
        };
        if intent
            .as_ref()
            .zip(plan.as_ref())
            .is_some_and(|(intent, plan)| intent.run_id != plan.run_id)
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        let mut matching_records =
            self.journal.list().into_iter().filter(|record| {
                record.action == PlanAction::Return && record.run_id == Some(run_id)
            });
        let Some(operation) = matching_records.next() else {
            return Err(StorageError(BlockerCode::OperationConflict));
        };
        let operation_state = operation.state;
        // A failed owner with an exact intent may still need the engine's unretired rollback
        // path, which is valid without an install plan. A retired intent without its plan is
        // refused by the engine; plan-only recovery remains a separate service stage.
        let failed_with_intent = operation_state == OperationState::Failed && intent.is_some();
        if matching_records.next().is_some()
            || (operation_state != OperationState::Committing && !failed_with_intent)
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        let staged_home = self.staged_home(operation.operation_id);
        if plan
            .as_ref()
            .is_some_and(|plan| plan.staged_home != staged_home)
        {
            return Err(StorageError(BlockerCode::OperationConflict));
        }
        let storage = self.connect().await?;
        let config = self.staged_config(&staged_home)?;
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            staged_home,
            Migrator::new(self.staged_source(config), storage.pool().clone()),
        );
        let outcome = returning.recover().await;
        storage.close().await;
        let outcome = outcome.map_err(|error| StorageError(error.into()))?;
        let mut touched = Vec::new();
        for mut record in self.journal.list().into_iter().filter(|record| {
            record.operation_id == operation.operation_id
                && record.run_id == Some(run_id)
                && record.action == PlanAction::Return
                && record.state == operation_state
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

    /// Cancel a return that has not retired the dataset. The dataset is reopened unchanged.
    pub(crate) async fn cancel_return(
        &self,
        mut record: OperationRecord,
    ) -> Result<OperationRecord, StorageError> {
        let staged_home = self.staged_home(record.operation_id);
        let intent = read_cutover(&self.inputs.codex_home).map_err(internal)?;
        let plan = read_plan(&self.inputs.codex_home).map_err(internal)?;
        if intent.is_some() || plan.is_some() {
            let run_id = intent
                .as_ref()
                .map(|intent| intent.run_id)
                .or_else(|| plan.as_ref().map(|plan| plan.run_id));
            let Some(run_id) = run_id else {
                return Err(StorageError(BlockerCode::OperationConflict));
            };
            let unique_owner = self
                .journal
                .list()
                .into_iter()
                .filter(|candidate| {
                    candidate.action == PlanAction::Return && candidate.run_id == Some(run_id)
                })
                .collect::<Vec<_>>();
            let current = self.journal.read(record.operation_id).map_err(internal)?;
            if record.action != PlanAction::Return
                || !matches!(
                    record.state,
                    OperationState::Committing | OperationState::Failed
                )
                || record.run_id != Some(run_id)
                || current.as_ref() != Some(&record)
                || unique_owner.len() != 1
                || unique_owner[0].operation_id != record.operation_id
                || unique_owner[0].state != record.state
                || intent
                    .as_ref()
                    .zip(plan.as_ref())
                    .is_some_and(|(intent, plan)| intent.run_id != plan.run_id)
                || plan
                    .as_ref()
                    .is_some_and(|plan| plan.staged_home != staged_home)
            {
                return Err(StorageError(BlockerCode::OperationConflict));
            }
        }

        // If cutover markers are absent but this return is still unsettled, distinguish the
        // exact unretired Remote export from the exact retired Local return before changing its
        // journal state. This is deliberately scoped to its durable owner; generic markerless
        // journal reconciliation is a separate service stage.
        if intent.is_none()
            && plan.is_none()
            && matches!(
                record.state,
                OperationState::Committing | OperationState::Failed
            )
            && let Some(run_id) = record.run_id
        {
            let current = self.journal.read(record.operation_id).map_err(internal)?;
            let owners = self
                .journal
                .list()
                .into_iter()
                .filter(|candidate| {
                    candidate.action == PlanAction::Return && candidate.run_id == Some(run_id)
                })
                .collect::<Vec<_>>();
            if record.action != PlanAction::Return
                || current.as_ref() != Some(&record)
                || owners.len() != 1
                || owners[0].operation_id != record.operation_id
                || owners[0].state != record.state
            {
                return Err(StorageError(BlockerCode::OperationConflict));
            }

            let staged_home = self.staged_home(record.operation_id);
            let authority = authority_state(&self.inputs.codex_home).map_err(internal)?;
            let local = match authority {
                AuthorityState::Remote(remote) => {
                    let generation = i64::try_from(remote.identity.generation)
                        .map_err(|_| StorageError(BlockerCode::OperationConflict))?;
                    if remote.marker.active_backend != ActiveBackend::Remote
                        || !remote.marker.remote_ever_activated
                        || read_cutover(&self.inputs.codex_home)
                            .map_err(internal)?
                            .is_some()
                        || read_plan(&self.inputs.codex_home)
                            .map_err(internal)?
                            .is_some()
                        || self
                            .journal
                            .read(record.operation_id)
                            .map_err(internal)?
                            .as_ref()
                            != Some(&record)
                    {
                        return Err(StorageError(BlockerCode::OperationConflict));
                    }
                    let owners = self
                        .journal
                        .list()
                        .into_iter()
                        .filter(|candidate| {
                            candidate.action == PlanAction::Return
                                && candidate.run_id == Some(run_id)
                        })
                        .collect::<Vec<_>>();
                    if owners.len() != 1
                        || owners[0].operation_id != record.operation_id
                        || owners[0].state != record.state
                    {
                        return Err(StorageError(BlockerCode::OperationConflict));
                    }

                    let storage = self.connect().await?;
                    let config = self.staged_config(&staged_home)?;
                    let migrator =
                        Migrator::new(self.staged_source(config), storage.pool().clone());
                    let result = migrator
                        .abandon_existing_export_owned(
                            run_id,
                            ActivationTarget {
                                dataset_id: remote.identity.dataset_id,
                                generation,
                            },
                        )
                        .await;
                    storage.close().await;
                    result.map_err(|error| StorageError(error.into()))?;

                    if authority_state(&self.inputs.codex_home).map_err(internal)?
                        != AuthorityState::Remote(remote)
                        || read_cutover(&self.inputs.codex_home)
                            .map_err(internal)?
                            .is_some()
                        || read_plan(&self.inputs.codex_home)
                            .map_err(internal)?
                            .is_some()
                        || self
                            .journal
                            .read(record.operation_id)
                            .map_err(internal)?
                            .as_ref()
                            != Some(&record)
                    {
                        return Err(StorageError(BlockerCode::OperationConflict));
                    }
                    let owners = self
                        .journal
                        .list()
                        .into_iter()
                        .filter(|candidate| {
                            candidate.action == PlanAction::Return
                                && candidate.run_id == Some(run_id)
                        })
                        .collect::<Vec<_>>();
                    if owners.len() != 1
                        || owners[0].operation_id != record.operation_id
                        || owners[0].state != record.state
                    {
                        return Err(StorageError(BlockerCode::OperationConflict));
                    }
                    record.blocker = None;
                    self.save(&mut record, OperationState::Cancelled)?;
                    return Ok(record);
                }
                AuthorityState::Local(local) => local,
                _ => return Err(StorageError(BlockerCode::OperationConflict)),
            };
            let source_generation = local
                .identity
                .generation
                .checked_sub(1)
                .ok_or(StorageError(BlockerCode::OperationConflict))?;
            let remote_retired = self
                .retired_return_owner_matches(
                    record.operation_id,
                    run_id,
                    local.identity.dataset_id,
                    source_generation,
                    local.identity.generation,
                )
                .await?;
            if let Some(matches) = remote_retired {
                let current_local =
                    match authority_state(&self.inputs.codex_home).map_err(internal)? {
                        AuthorityState::Local(local) => local,
                        _ => return Err(StorageError(BlockerCode::OperationConflict)),
                    };
                if !matches
                    || current_local != local
                    || !completed_return_backup_matches(
                        &self.inputs.codex_home,
                        &staged_home,
                        run_id,
                    )?
                {
                    return Err(StorageError(BlockerCode::OperationConflict));
                }
                let owners = self
                    .journal
                    .list()
                    .into_iter()
                    .filter(|candidate| {
                        candidate.action == PlanAction::Return && candidate.run_id == Some(run_id)
                    })
                    .collect::<Vec<_>>();
                if read_cutover(&self.inputs.codex_home)
                    .map_err(internal)?
                    .is_some()
                    || read_plan(&self.inputs.codex_home)
                        .map_err(internal)?
                        .is_some()
                    || self
                        .journal
                        .read(record.operation_id)
                        .map_err(internal)?
                        .as_ref()
                        != Some(&record)
                    || owners.len() != 1
                    || owners[0].operation_id != record.operation_id
                    || owners[0].state != record.state
                {
                    return Err(StorageError(BlockerCode::OperationConflict));
                }
                let final_local =
                    match authority_state(&self.inputs.codex_home).map_err(internal)? {
                        AuthorityState::Local(local) => local,
                        _ => return Err(StorageError(BlockerCode::OperationConflict)),
                    };
                if final_local != local {
                    return Err(StorageError(BlockerCode::OperationConflict));
                }
                record.blocker = None;
                self.save(&mut record, OperationState::Active)?;
                return Err(StorageError(BlockerCode::AlreadyRemote));
            }
        }

        let storage = self.connect().await?;
        let config = self.staged_config(&staged_home)?;
        let migrator = Migrator::new(self.staged_source(config), storage.pool().clone());
        let returning = ReturnCutover::new(
            self.inputs.codex_home.clone(),
            staged_home,
            Migrator::new(
                self.staged_source(self.staged_config(&self.staged_home(record.operation_id))?),
                storage.pool().clone(),
            ),
        );
        if (intent.is_some() || plan.is_some())
            && let Err(error) = returning.abort().await {
                let settlement = if matches!(
                    &error,
                    codex_storage_migration::CutoverError::AlreadyActivated
                ) {
                    record.blocker = None;
                    self.save(&mut record, OperationState::Active)
                } else {
                    Ok(())
                };
                storage.close().await;
                settlement?;
                return Err(StorageError(error.into()));
            }
        if let Some(run_id) = record.run_id {
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
        storage.close().await;
        record.blocker = None;
        self.save(&mut record, OperationState::Cancelled)?;
        Ok(record)
    }
}
