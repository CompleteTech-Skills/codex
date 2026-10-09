//! Handing authority back from PostgreSQL to local files.
//!
//! The dataset is exported into a staged home while it is closed to writers, verified, and then
//! swapped in: the intent is recorded, PostgreSQL is retired, the staged files replace the live
//! ones with everything they replace kept in a backup, and the authority records flip last.
//! [`ReturnCutover::recover`] settles an interruption from the evidence on both sides. Nothing
//! is deleted, and the retired dataset stays in PostgreSQL.

use crate::ActivationTarget;
use crate::CutoverError;
use crate::MigrationError;
use crate::Migrator;
use crate::RecoveryOutcome;
use crate::install;
use codex_storage_authority::ActiveBackend;
use codex_storage_authority::AuthorityState;
use codex_storage_authority::CutoverIntent;
use codex_storage_authority::LocalAuthority;
use codex_storage_authority::abandon_cutover;
use codex_storage_authority::authority_state;
use codex_storage_authority::begin_cutover;
use codex_storage_authority::complete_cutover;
use codex_storage_authority::load_authority;
use codex_storage_authority::read_cutover;
use std::path::PathBuf;
use uuid::Uuid;

fn io_error(error: std::io::Error) -> CutoverError {
    CutoverError::Authority(error.to_string())
}

fn authority(error: codex_storage_authority::AuthorityError) -> CutoverError {
    CutoverError::Authority(error.to_string())
}

/// The return of one home from one retired dataset.
pub struct ReturnCutover {
    home: PathBuf,
    staged_home: PathBuf,
    migrator: Migrator,
}

impl ReturnCutover {
    /// `migrator` must be the export migrator whose source is the staged home.
    pub fn new(home: PathBuf, staged_home: PathBuf, migrator: Migrator) -> Self {
        Self {
            home,
            staged_home,
            migrator,
        }
    }

    fn matching_plan(&self, run_id: Uuid) -> Result<install::InstallPlan, CutoverError> {
        let plan = install::read_plan(&self.home)
            .map_err(io_error)?
            .ok_or_else(|| CutoverError::Authority("the install plan is missing".to_string()))?;
        install::validate_plan_identity(&self.home, &self.staged_home, run_id, &plan)
            .map_err(|_| CutoverError::Conflict)?;
        Ok(plan)
    }

    fn ensure_current_intent(
        &self,
        expected: Option<&CutoverIntent>,
    ) -> Result<(), MigrationError> {
        let current =
            read_cutover(&self.home).map_err(|error| MigrationError::Staging(error.to_string()))?;
        if current.as_ref() != expected {
            return Err(MigrationError::TargetBusy);
        }
        Ok(())
    }

    fn ensure_current_plan(
        &self,
        expected: Option<&install::InstallPlan>,
    ) -> Result<(), MigrationError> {
        let current = install::read_plan(&self.home)
            .map_err(|error| MigrationError::Staging(error.to_string()))?;
        if current.as_ref() != expected {
            return Err(MigrationError::TargetBusy);
        }
        if let Some(plan) = current.as_ref() {
            install::validate_plan_identity(&self.home, &self.staged_home, plan.run_id, plan)
                .map_err(|error| MigrationError::Staging(error.to_string()))?;
        }
        Ok(())
    }

    fn install_plan(&self, plan: &install::InstallPlan) -> Result<(), CutoverError> {
        install::validate_plan_identity(&self.home, &self.staged_home, plan.run_id, plan)
            .map_err(|_| CutoverError::Conflict)?;
        install::install(&self.home, plan).map_err(io_error)
    }

    /// Record the intent and the swap plan. Nothing else changes yet.
    pub fn prepare(&self, run_id: Uuid) -> Result<CutoverIntent, CutoverError> {
        let intent = begin_cutover(&self.home, run_id, ActiveBackend::Local).map_err(authority)?;
        if let Err(error) = install::plan_install(&self.home, &self.staged_home, run_id) {
            abandon_cutover(&self.home, &intent).map_err(authority)?;
            return Err(io_error(error));
        }
        Ok(intent)
    }

    /// Close the dataset to every writer for good. Safe to repeat.
    pub async fn publish(&self, intent: &CutoverIntent) -> Result<(), CutoverError> {
        let generation = i64::try_from(intent.to_generation).map_err(|_| CutoverError::Conflict)?;
        self.migrator
            .retire(
                intent.run_id,
                ActivationTarget {
                    dataset_id: intent.dataset_id,
                    generation,
                },
            )
            .await?;
        Ok(())
    }

    /// Replace the live files with the staged ones, keeping what they replace. Safe to repeat.
    pub fn install(&self) -> Result<(), CutoverError> {
        let intent = read_cutover(&self.home).map_err(authority)?;
        let Some(intent) = intent else {
            return Err(CutoverError::Conflict);
        };
        if intent.target != ActiveBackend::Local {
            return Err(CutoverError::Conflict);
        }
        let plan = self.matching_plan(intent.run_id)?;
        self.install_plan(&plan)
    }

    /// Flip the authority records to local, then forget the plan. Safe to repeat.
    pub fn finish(&self, intent: &CutoverIntent) -> Result<LocalAuthority, CutoverError> {
        if intent.target != ActiveBackend::Local {
            return Err(CutoverError::Conflict);
        }
        let current = read_cutover(&self.home).map_err(authority)?;
        if current.is_none() {
            let current_plan = install::read_plan(&self.home).map_err(io_error)?;
            let moved = load_authority(&self.home).map_err(authority)?;
            if moved.marker.active_backend != ActiveBackend::Local
                || moved.identity.dataset_id != intent.dataset_id
                || moved.identity.generation != intent.to_generation
                || moved.identity.format_version != intent.format_version
                || intent.from_generation.checked_add(1) != Some(intent.to_generation)
            {
                return Err(CutoverError::Conflict);
            }
            let plan = install::read_backup_plan(&self.home, intent.run_id).map_err(io_error)?;
            install::validate_plan_identity(&self.home, &self.staged_home, intent.run_id, &plan)
                .map_err(|_| CutoverError::Conflict)?;
            if current_plan
                .as_ref()
                .is_some_and(|current| current != &plan)
            {
                return Err(CutoverError::Conflict);
            }
            if !install::verify_backup(&plan).map_err(io_error)? {
                return Err(CutoverError::Conflict);
            }
            self.ensure_current_intent(None)?;
            self.ensure_current_plan(current_plan.as_ref())?;
            if current_plan.is_some() {
                install::discard_plan(&self.home).map_err(io_error)?;
            }
            return Ok(moved);
        }
        if current.as_ref() != Some(intent) {
            return Err(CutoverError::Conflict);
        }
        let plan = self.matching_plan(intent.run_id)?;
        let backup_plan = install::read_backup_plan(&self.home, intent.run_id).map_err(io_error)?;
        install::validate_plan_identity(&self.home, &self.staged_home, intent.run_id, &backup_plan)
            .map_err(|_| CutoverError::Conflict)?;
        if backup_plan != plan || !install::verify_backup(&backup_plan).map_err(io_error)? {
            return Err(CutoverError::Conflict);
        }
        let moved = complete_cutover(&self.home, intent).map_err(authority)?;
        self.ensure_current_plan(Some(&plan))?;
        install::discard_plan(&self.home).map_err(io_error)?;
        Ok(moved)
    }

    /// Marker-less completion: `None` before retirement, otherwise exact owned backup proof.
    pub async fn completed(&self, run_id: Uuid) -> Result<Option<u64>, CutoverError> {
        let remote = self.migrator.activation_state().await?;
        if !remote.retired {
            return Ok(None);
        }
        if read_cutover(&self.home).map_err(authority)?.is_some()
            || install::read_plan(&self.home).map_err(io_error)?.is_some()
        {
            return Err(CutoverError::Conflict);
        }
        let AuthorityState::Local(local) = authority_state(&self.home).map_err(authority)? else {
            return Err(CutoverError::Conflict);
        };
        self.installed(&remote, run_id, &local, None).map(Some)
    }

    /// Require exact SQL/Local identity, owned backup integrity and any leftover plan equality.
    fn installed(
        &self,
        remote: &crate::ActivationState,
        run_id: Uuid,
        local: &LocalAuthority,
        leftover: Option<&install::InstallPlan>,
    ) -> Result<u64, CutoverError> {
        if !remote.retired
            || remote.run_id != Some(run_id)
            || remote.dataset_id != Some(local.identity.dataset_id)
            || u64::try_from(remote.generation).ok() != Some(local.identity.generation)
        {
            return Err(CutoverError::Conflict);
        }
        let backup = install::read_backup_plan(&self.home, run_id).map_err(io_error)?;
        if leftover.is_some_and(|plan| plan != &backup)
            || install::validate_plan_identity(&self.home, &self.staged_home, run_id, &backup)
                .is_err()
            || !install::verify_backup(&backup).map_err(io_error)?
        {
            return Err(CutoverError::Conflict);
        }
        Ok(local.identity.generation)
    }

    /// Run the whole protocol for a verified export.
    pub async fn execute(&self, run_id: Uuid) -> Result<LocalAuthority, CutoverError> {
        let intent = self.prepare(run_id)?;
        let plan = self.matching_plan(run_id)?;
        match self.publish(&intent).await {
            Ok(()) => {}
            Err(CutoverError::Destination(
                refusal @ (MigrationError::NotVerified
                | MigrationError::TargetBusy
                | MigrationError::GenerationNotAdvancing),
            )) => {
                self.ensure_current_intent(Some(&intent))?;
                self.ensure_current_plan(Some(&plan))?;
                abandon_cutover(&self.home, &intent).map_err(authority)?;
                self.ensure_current_intent(None)?;
                self.ensure_current_plan(Some(&plan))?;
                install::discard_plan(&self.home).map_err(io_error)?;
                return Err(CutoverError::Destination(refusal));
            }
            Err(_) => return Err(CutoverError::Uncertain),
        }
        self.install()?;
        self.finish(&intent)
    }

    /// Settle an interrupted return from the evidence on both sides.
    pub async fn recover(&self) -> Result<RecoveryOutcome, CutoverError> {
        let Some(intent) = read_cutover(&self.home).map_err(authority)? else {
            // Without an owner-bound service result, retain any leftover evidence.
            if install::read_plan(&self.home).map_err(io_error)?.is_some() {
                return Err(CutoverError::Conflict);
            }
            return Ok(RecoveryOutcome::Idle);
        };
        if intent.target != ActiveBackend::Local {
            return Err(CutoverError::Conflict);
        }
        let plan = self.matching_plan(intent.run_id)?;
        let remote = self.migrator.activation_state().await?;
        let to_generation =
            i64::try_from(intent.to_generation).map_err(|_| CutoverError::Conflict)?;
        if remote.retired
            && remote.run_id == Some(intent.run_id)
            && remote.generation == to_generation
            && remote.dataset_id == Some(intent.dataset_id)
        {
            self.ensure_current_intent(Some(&intent))?;
            self.ensure_current_plan(Some(&plan))?;
            self.install()?;
            let moved = self.finish(&intent)?;
            return Ok(RecoveryOutcome::RolledForward {
                generation: moved.identity.generation,
            });
        }
        let from_generation =
            i64::try_from(intent.from_generation).map_err(|_| CutoverError::Conflict)?;
        if remote.retired
            || remote.generation != from_generation
            || remote.run_id != Some(intent.run_id)
            || remote.dataset_id != Some(intent.dataset_id)
        {
            return Err(CutoverError::Conflict);
        }
        // The export only read the dataset while it was closed, so hand it back unchanged.
        self.migrator
            .abandon_export_after(
                intent.run_id,
                ActivationTarget {
                    dataset_id: intent.dataset_id,
                    generation: from_generation,
                },
                false,
                || {
                    self.ensure_current_intent(Some(&intent))?;
                    self.ensure_current_plan(Some(&plan))?;
                    abandon_cutover(&self.home, &intent)
                        .map_err(|error| MigrationError::Staging(error.to_string()))?;
                    self.ensure_current_intent(None)?;
                    self.ensure_current_plan(Some(&plan))?;
                    install::discard_plan(&self.home)
                        .map_err(|error| MigrationError::Staging(error.to_string()))
                },
            )
            .await?;
        Ok(RecoveryOutcome::RolledBack)
    }

    /// Cancel before the dataset was retired. Afterwards the return can only finish.
    pub async fn abort(&self) -> Result<RecoveryOutcome, CutoverError> {
        let Some(intent) = read_cutover(&self.home).map_err(authority)? else {
            return Ok(RecoveryOutcome::Idle);
        };
        match self.recover().await? {
            RecoveryOutcome::RolledForward { .. } => Err(CutoverError::AlreadyActivated),
            outcome @ (RecoveryOutcome::RolledBack | RecoveryOutcome::Idle) => {
                let _ = intent;
                Ok(outcome)
            }
        }
    }
}
