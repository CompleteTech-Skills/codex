//! The resumable migration engine.
//!
//! A run claims the store through the activation row, writes each domain in key order with a
//! checkpoint committed in the same transaction as every batch, and then verifies by comparing
//! the source and target digest of every domain. Replaying a batch is harmless because every
//! write is an upsert, so a crash or a lost commit result only costs a repeat.

use crate::attachments::Attachments;
use crate::attachments::SpawnEdges;
use crate::board::Channels;
use crate::board::DeletedBoards;
use crate::board::OptOuts;
use crate::board::Posts;
use crate::board::Subscriptions;
use crate::digest::DigestBuilder;
use crate::digest::DomainDigest;
use crate::domain::Domain;
use crate::domain::DomainOps;
use crate::external_imports::ExternalImports;
use crate::goals::Goals;
use crate::logs::Logs;
use crate::memory::MemoryJobs;
use crate::memory::MemoryProgress;
use crate::memory::Stage1Outputs;
use crate::projects::ProjectKeys;
use crate::projects::Projects;
use crate::queue::QueueRevisions;
use crate::queue::QueuedItems;
use crate::rollouts::Rollouts;
use crate::sections::Sections;
use crate::source::SqliteSource;
use crate::sqlite_target::SqliteTarget;
use crate::threads::Threads;
use codex_postgres_runtime::PostgresPool;
use sqlx::Acquire;
use sqlx::PgConnection;
use sqlx::Row;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

const DEFAULT_BATCH: usize = 500;

/// True when the destination holds history a migration would have to merge with.
const EMPTY_TARGET_SQL: &str = "SELECT EXISTS (SELECT 1 FROM threads) \
     OR EXISTS (SELECT 1 FROM projects) \
     OR EXISTS (SELECT 1 FROM queued_items) \
     OR EXISTS (SELECT 1 FROM logs) \
     OR EXISTS (SELECT 1 FROM agent_board_posts) \
     OR EXISTS (SELECT 1 FROM memory_stage1_outputs) \
     OR EXISTS (SELECT 1 FROM thread_goals)";

/// Whether the destination is free of history, so a migration could write into it.
pub async fn target_is_empty(pool: &PostgresPool) -> Result<bool, MigrationError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|error| MigrationError::Target(format!("{error:?}")))?;
    let occupied: bool = sqlx::query_scalar(EMPTY_TARGET_SQL)
        .fetch_one(&mut *connection)
        .await
        .map_err(target)?;
    Ok(!occupied)
}

/// Run one expression for the domain's operations without repeating the list of domains.
macro_rules! with_domain {
    ($domain:expr, $ops:ident => $body:expr) => {
        match $domain {
            Domain::Sections => {
                type $ops = Sections;
                $body
            }
            Domain::Projects => {
                type $ops = Projects;
                $body
            }
            Domain::ProjectKeys => {
                type $ops = ProjectKeys;
                $body
            }
            Domain::Threads => {
                type $ops = Threads;
                $body
            }
            Domain::Attachments => {
                type $ops = Attachments;
                $body
            }
            Domain::SpawnEdges => {
                type $ops = SpawnEdges;
                $body
            }
            Domain::Rollouts => {
                type $ops = Rollouts;
                $body
            }
            Domain::Goals => {
                type $ops = Goals;
                $body
            }
            Domain::QueuedItems => {
                type $ops = QueuedItems;
                $body
            }
            Domain::QueueRevisions => {
                type $ops = QueueRevisions;
                $body
            }
            Domain::Logs => {
                type $ops = Logs;
                $body
            }
            Domain::MemoryOutputs => {
                type $ops = Stage1Outputs;
                $body
            }
            Domain::MemoryJobs => {
                type $ops = MemoryJobs;
                $body
            }
            Domain::MemoryProgress => {
                type $ops = MemoryProgress;
                $body
            }
            Domain::BoardDeleted => {
                type $ops = DeletedBoards;
                $body
            }
            Domain::BoardChannels => {
                type $ops = Channels;
                $body
            }
            Domain::BoardPosts => {
                type $ops = Posts;
                $body
            }
            Domain::BoardSubscriptions => {
                type $ops = Subscriptions;
                $body
            }
            Domain::BoardOptOuts => {
                type $ops = OptOuts;
                $body
            }
            Domain::ExternalImports => {
                type $ops = ExternalImports;
                $body
            }
        }
    };
}

/// Failures never carry connection details or row contents.
#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("the source could not be read: {0}")]
    Source(String),
    #[error("the target rejected the migration: {0}")]
    Target(String),
    #[error("another migration holds the target store")]
    TargetBusy,
    #[error("{domain} differs between the source and the target")]
    Mismatch { domain: &'static str },
    #[error("the run stopped after its batch limit and can be resumed")]
    Interrupted,
    #[error("the target already holds data, and a migration never merges into existing history")]
    TargetNotEmpty,
    #[error("the run has not been verified, so it cannot be activated")]
    NotVerified,
    #[error("the new generation must be higher than the store's current generation")]
    GenerationNotAdvancing,
    #[error("the store has never been activated, so there is no dataset to export")]
    NotActivated,
    #[error("the staged home could not be written: {0}")]
    Staging(String),
}

/// The dataset identity and generation a verified import is published as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActivationTarget {
    pub dataset_id: Uuid,
    pub generation: i64,
}

/// What the store's activation row currently says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivationState {
    /// True while a run holds the store and ordinary writers are refused.
    pub migrating: bool,
    /// True once the store was handed back to local storage.
    pub retired: bool,
    /// The run that holds the store, while one does.
    pub run_id: Option<Uuid>,
    pub generation: i64,
    /// The dataset the last activation published; `None` before the first one.
    pub dataset_id: Option<Uuid>,
}

fn target(error: impl std::fmt::Display) -> MigrationError {
    MigrationError::Target(error.to_string())
}

fn source(error: impl std::fmt::Display) -> MigrationError {
    MigrationError::Source(error.to_string())
}

/// What a run moved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSummary {
    pub run_id: Uuid,
    pub resumed: bool,
    pub domains: Vec<(Domain, u64)>,
}

/// Source and target digests per domain, equal when verification passes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationReport {
    pub run_id: Uuid,
    pub domains: Vec<(Domain, DomainDigest)>,
}

/// Moves a SQLite home into a PostgreSQL store.
pub struct Migrator {
    source: SqliteSource,
    target: Arc<PostgresPool>,
    batch_size: usize,
    batch_limit: Option<usize>,
}

impl Migrator {
    pub fn new(source: SqliteSource, target: Arc<PostgresPool>) -> Self {
        Self {
            source,
            target,
            batch_size: DEFAULT_BATCH,
            batch_limit: None,
        }
    }

    /// Smaller batches checkpoint more often; tests use them to exercise resumption.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Stop after this many committed batches. The run keeps its checkpoints, so importing
    /// again continues where this one stopped.
    pub fn with_batch_limit(mut self, batch_limit: usize) -> Self {
        self.batch_limit = Some(batch_limit);
        self
    }

    /// Claim the store and write every domain, resuming a run that was interrupted.
    pub async fn import(&self) -> Result<RunSummary, MigrationError> {
        let fingerprint = self.source.fingerprint().await.map_err(source)?;
        let (run_id, resumed) = self.begin(&fingerprint, "import", None, None).await?;
        let mut domains = Vec::new();
        let mut batches = 0;
        for domain in Domain::ALL {
            let moved = with_domain!(
                *domain,
                Ops => self.import_domain::<Ops>(run_id, &mut batches).await?
            );
            domains.push((*domain, moved));
        }
        Ok(RunSummary {
            run_id,
            resumed,
            domains,
        })
    }

    /// Write the activated PostgreSQL dataset into a staged SQLite home, resuming a run that was
    /// interrupted. The store stays closed to writers for the whole run, so the copy is a
    /// consistent snapshot. This migrator's source must be the staged home, because
    /// verification reads it back.
    pub async fn export(&self, staged: &SqliteTarget) -> Result<RunSummary, MigrationError> {
        self.export_inner(staged, None, None).await
    }

    /// Export only under the identity already persisted by the owning operation. A retry
    /// cannot adopt another operation's run, even if its staged-home fingerprint matches.
    pub async fn export_owned(
        &self,
        staged: &SqliteTarget,
        run_id: Uuid,
        source: ActivationTarget,
    ) -> Result<RunSummary, MigrationError> {
        self.export_inner(staged, Some(run_id), Some(source)).await
    }

    async fn export_inner(
        &self,
        staged: &SqliteTarget,
        owned_run: Option<Uuid>,
        owned_source: Option<ActivationTarget>,
    ) -> Result<RunSummary, MigrationError> {
        let fingerprint = self.source.fingerprint().await.map_err(source)?;
        let (run_id, resumed) = self
            .begin(&fingerprint, "export", owned_run, owned_source)
            .await?;
        let mut domains = Vec::new();
        let mut batches = 0;
        for domain in Domain::ALL {
            let moved = with_domain!(
                *domain,
                Ops => self.export_domain::<Ops>(run_id, staged, &mut batches).await?
            );
            domains.push((*domain, moved));
        }
        Ok(RunSummary {
            run_id,
            resumed,
            domains,
        })
    }

    /// Prove the target holds exactly what the source holds, then mark the run verified.
    pub async fn verify(&self, run_id: Uuid) -> Result<VerificationReport, MigrationError> {
        let mut domains = Vec::new();
        for domain in Domain::ALL {
            let (source_digest, target_digest) =
                with_domain!(*domain, Ops => self.digests::<Ops>().await?);
            if source_digest != target_digest {
                return Err(MigrationError::Mismatch {
                    domain: domain.name(),
                });
            }
            self.record_digest(run_id, *domain, &target_digest).await?;
            domains.push((*domain, target_digest));
        }
        self.set_run_state(run_id, "verified").await?;
        Ok(VerificationReport { run_id, domains })
    }

    /// Rows copied so far in a run, by domain, as the run's checkpoints record them.
    pub async fn progress(&self, run_id: Uuid) -> Result<Vec<(String, u64)>, MigrationError> {
        let mut connection = self.connection().await?;
        let rows = sqlx::query(
            "SELECT domain, row_count FROM storage_migration_domains \
             WHERE run_id = $1 ORDER BY domain",
        )
        .bind(run_id)
        .fetch_all(&mut *connection)
        .await
        .map_err(target)?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("domain").map_err(target)?,
                    row.try_get::<i64, _>("row_count").map_err(target)? as u64,
                ))
            })
            .collect()
    }

    /// The activation row as the store reports it right now.
    pub async fn activation_state(&self) -> Result<ActivationState, MigrationError> {
        let mut connection = self.connection().await?;
        let row = sqlx::query(
            "SELECT state, run_id, generation, dataset_id FROM storage_activation WHERE singleton",
        )
        .fetch_one(&mut *connection)
        .await
        .map_err(target)?;
        let state: String = row.try_get("state").map_err(target)?;
        let dataset: Option<String> = row.try_get("dataset_id").map_err(target)?;
        Ok(ActivationState {
            migrating: state == "migrating",
            retired: state == "retired",
            run_id: row.try_get("run_id").map_err(target)?,
            generation: row.try_get("generation").map_err(target)?,
            dataset_id: dataset.and_then(|value| Uuid::parse_str(&value).ok()),
        })
    }

    /// Give up on a run before it is activated. The store stays closed to writers, because the
    /// partial copy must never look like a finished one, and the same source can resume later.
    pub async fn abandon(&self, run_id: Uuid) -> Result<(), MigrationError> {
        let mut connection = self.connection().await?;
        let mut tx = connection.begin().await.map_err(target)?;
        let held: Option<Uuid> =
            sqlx::query_scalar("SELECT run_id FROM storage_activation WHERE singleton FOR UPDATE")
                .fetch_one(&mut *tx)
                .await
                .map_err(target)?;
        if held != Some(run_id) {
            return Err(MigrationError::TargetBusy);
        }
        let now = chrono::Utc::now().timestamp_millis();
        sqlx::query(
            "UPDATE storage_migration_runs SET state = 'abandoned', updated_at_ms = $2 \
             WHERE run_id = $1 AND state IN ('running', 'verified')",
        )
        .bind(run_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(target)?;
        // An export only read the dataset while it was closed, so cancelling it hands the
        // dataset back unchanged. An abandoned import leaves a partial copy, which stays closed.
        sqlx::query(
            "UPDATE storage_activation SET state = 'open', updated_at_ms = $2 \
             WHERE singleton AND state = 'migrating' AND EXISTS ( \
                 SELECT 1 FROM storage_migration_runs WHERE run_id = $1 AND direction = 'export')",
        )
        .bind(run_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(target)?;
        tx.commit().await.map_err(target)?;
        Ok(())
    }

    /// Reopen only this staged home's explicitly owned export. Foreign runs and imports
    /// remain fenced; a missing run is a no-op only for an open dataset with no held run.
    pub async fn abandon_export_owned(
        &self,
        run_id: Uuid,
        source: ActivationTarget,
    ) -> Result<(), MigrationError> {
        self.abandon_export_after(run_id, source, true, || Ok(()))
            .await
    }

    /// Abandon the exact export before removing its local recovery markers. Cancellation leaves
    /// the already-authoritative remote dataset in place, so its durable SQL transition must
    /// precede local cleanup.
    pub(crate) async fn abandon_export_after(
        &self,
        run_id: Uuid,
        expected: ActivationTarget,
        allow_unstarted: bool,
        cleanup: impl FnOnce() -> Result<(), MigrationError>,
    ) -> Result<(), MigrationError> {
        let fingerprint = self.source.fingerprint().await.map_err(source)?;
        let mut connection = self.connection().await?;
        let mut tx = connection.begin().await.map_err(target)?;
        let activation = sqlx::query(
            "SELECT state, run_id, generation, dataset_id FROM storage_activation WHERE singleton FOR UPDATE",
        ).fetch_one(&mut *tx).await.map_err(target)?;
        let state: String = activation.try_get("state").map_err(target)?;
        let held: Option<Uuid> = activation.try_get("run_id").map_err(target)?;
        let generation: i64 = activation.try_get("generation").map_err(target)?;
        let dataset: Option<String> = activation.try_get("dataset_id").map_err(target)?;
        if generation != expected.generation
            || dataset.as_deref() != Some(expected.dataset_id.to_string().as_str())
        {
            return Err(MigrationError::TargetBusy);
        }
        let run = sqlx::query(
            "SELECT direction, source_fingerprint, state FROM storage_migration_runs WHERE run_id = $1 FOR UPDATE",
        ).bind(run_id).fetch_optional(&mut *tx).await.map_err(target)?;
        let Some(run) = run else {
            if state == "open" && allow_unstarted && held.is_none() {
                return Ok(());
            }
            return Err(MigrationError::TargetBusy);
        };
        if run.try_get::<String, _>("direction").map_err(target)? != "export"
            || run
                .try_get::<String, _>("source_fingerprint")
                .map_err(target)?
                != fingerprint
            || held != Some(run_id)
        {
            return Err(MigrationError::TargetBusy);
        }
        let run_state: String = run.try_get("state").map_err(target)?;
        if state == "open" && run_state == "abandoned" {
            cleanup()?;
            tx.commit().await.map_err(target)?;
            return Ok(());
        }
        if state != "migrating" || !matches!(run_state.as_str(), "running" | "verified") {
            return Err(MigrationError::TargetBusy);
        }
        let now = chrono::Utc::now().timestamp_millis();
        let abandoned = sqlx::query("UPDATE storage_migration_runs SET state = 'abandoned', updated_at_ms = $2 WHERE run_id = $1")
            .bind(run_id).bind(now).execute(&mut *tx).await.map_err(target)?;
        if abandoned.rows_affected() != 1 {
            return Err(MigrationError::TargetBusy);
        }
        let reopened = sqlx::query(
            "UPDATE storage_activation SET state = 'open', updated_at_ms = $1 WHERE singleton",
        )
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(target)?;
        if reopened.rows_affected() != 1 {
            return Err(MigrationError::TargetBusy);
        }
        tx.commit().await.map_err(target)?;

        // The SQL transition is now durable. Re-lock and revalidate before touching local
        // evidence; another export may have claimed the open dataset after the first commit.
        let mut tx = connection.begin().await.map_err(target)?;
        let activation = sqlx::query(
            "SELECT state, run_id, generation, dataset_id FROM storage_activation WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(target)?;
        let state: String = activation.try_get("state").map_err(target)?;
        let held: Option<Uuid> = activation.try_get("run_id").map_err(target)?;
        let generation: i64 = activation.try_get("generation").map_err(target)?;
        let dataset: Option<String> = activation.try_get("dataset_id").map_err(target)?;
        if state != "open"
            || held != Some(run_id)
            || generation != expected.generation
            || dataset.as_deref() != Some(expected.dataset_id.to_string().as_str())
        {
            return Err(MigrationError::TargetBusy);
        }
        let run = sqlx::query(
            "SELECT direction, source_fingerprint, state FROM storage_migration_runs WHERE run_id = $1 FOR UPDATE",
        )
        .bind(run_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(target)?
        .ok_or(MigrationError::TargetBusy)?;
        if run.try_get::<String, _>("direction").map_err(target)? != "export"
            || run
                .try_get::<String, _>("source_fingerprint")
                .map_err(target)?
                != fingerprint
            || run.try_get::<String, _>("state").map_err(target)? != "abandoned"
        {
            return Err(MigrationError::TargetBusy);
        }
        cleanup()?;
        tx.commit().await.map_err(target)?;
        Ok(())
    }

    /// Hand the store back to local storage: after a verified export, close it to every writer for
    /// good. Repeating a call that already succeeded returns the same generation.
    pub async fn retire(
        &self,
        run_id: Uuid,
        publish: ActivationTarget,
    ) -> Result<i64, MigrationError> {
        let mut connection = self.connection().await?;
        let mut tx = connection.begin().await.map_err(target)?;
        let row = sqlx::query(
            "SELECT state, run_id, generation, dataset_id FROM storage_activation \
             WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(target)?;
        let state: String = row.try_get("state").map_err(target)?;
        let held: Option<Uuid> = row.try_get("run_id").map_err(target)?;
        let generation: i64 = row.try_get("generation").map_err(target)?;
        let dataset: Option<String> = row.try_get("dataset_id").map_err(target)?;
        if state == "retired"
            && generation == publish.generation
            && held == Some(run_id)
            && dataset.as_deref() == Some(publish.dataset_id.to_string().as_str())
        {
            return Ok(generation);
        }
        if state != "migrating" || held != Some(run_id) {
            return Err(MigrationError::TargetBusy);
        }
        let run =
            sqlx::query("SELECT state, direction FROM storage_migration_runs WHERE run_id = $1")
                .bind(run_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(target)?;
        let Some(run) = run else {
            return Err(MigrationError::NotVerified);
        };
        let run_state: String = run.try_get("state").map_err(target)?;
        let direction: String = run.try_get("direction").map_err(target)?;
        if run_state != "verified" || direction != "export" {
            return Err(MigrationError::NotVerified);
        }
        if dataset.as_deref() != Some(publish.dataset_id.to_string().as_str()) {
            return Err(MigrationError::TargetBusy);
        }
        if publish.generation <= generation {
            return Err(MigrationError::GenerationNotAdvancing);
        }
        let now = chrono::Utc::now().timestamp_millis();
        sqlx::query(
            "UPDATE storage_migration_runs SET state = 'activated', updated_at_ms = $2 \
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(target)?;
        sqlx::query(
            "UPDATE storage_activation SET state = 'retired', generation = $1, \
             activated_at_ms = $2, updated_at_ms = $2 WHERE singleton",
        )
        .bind(publish.generation)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(target)?;
        tx.commit().await.map_err(target)?;
        Ok(publish.generation)
    }

    /// Publish a verified import as the store's content and reopen the store for writers.
    ///
    /// Returns the new generation. Only the run that holds the store and passed verification
    /// can activate it, so a partial or unverified copy never becomes writable. Repeating a
    /// call that already succeeded returns the same generation, which makes a lost
    /// acknowledgement harmless.
    pub async fn activate(
        &self,
        run_id: Uuid,
        publish: ActivationTarget,
    ) -> Result<i64, MigrationError> {
        let mut connection = self.connection().await?;
        let mut tx = connection.begin().await.map_err(target)?;
        let row = sqlx::query(
            "SELECT state, run_id, generation, dataset_id FROM storage_activation \
             WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(target)?;
        let state: String = row.try_get("state").map_err(target)?;
        let held: Option<Uuid> = row.try_get("run_id").map_err(target)?;
        let generation: i64 = row.try_get("generation").map_err(target)?;
        let dataset: Option<String> = row.try_get("dataset_id").map_err(target)?;
        if state == "open"
            && generation == publish.generation
            && dataset.as_deref() == Some(publish.dataset_id.to_string().as_str())
        {
            let activated: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM storage_migration_runs \
                 WHERE run_id = $1 AND state = 'activated')",
            )
            .bind(run_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(target)?;
            return if activated {
                Ok(generation)
            } else {
                Err(MigrationError::TargetBusy)
            };
        }
        if state != "migrating" || held != Some(run_id) {
            return Err(MigrationError::TargetBusy);
        }
        let run_state: Option<String> =
            sqlx::query_scalar("SELECT state FROM storage_migration_runs WHERE run_id = $1")
                .bind(run_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(target)?;
        if run_state.as_deref() != Some("verified") {
            return Err(MigrationError::NotVerified);
        }
        if publish.generation <= generation {
            return Err(MigrationError::GenerationNotAdvancing);
        }
        let now = chrono::Utc::now().timestamp_millis();
        sqlx::query(
            "UPDATE storage_migration_runs SET state = 'activated', updated_at_ms = $2 \
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(target)?;
        sqlx::query(
            "UPDATE storage_activation SET state = 'open', generation = $1, dataset_id = $2, \
             activated_at_ms = $3, updated_at_ms = $3 WHERE singleton",
        )
        .bind(publish.generation)
        .bind(publish.dataset_id.to_string())
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(target)?;
        tx.commit().await.map_err(target)?;
        Ok(publish.generation)
    }

    async fn connection(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, MigrationError> {
        self.target
            .acquire()
            .await
            .map_err(|error| MigrationError::Target(format!("{error:?}")))
    }

    /// Take the store: only one run may hold it, and an interrupted run of the same source
    /// resumes instead of starting over.
    async fn begin(
        &self,
        fingerprint: &str,
        direction: &str,
        owned_run: Option<Uuid>,
        owned_source: Option<ActivationTarget>,
    ) -> Result<(Uuid, bool), MigrationError> {
        let mut connection = self.connection().await?;
        let mut tx = connection.begin().await.map_err(target)?;
        let row = sqlx::query(
            "SELECT state, run_id, dataset_id, generation FROM storage_activation WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(target)?;
        let state: String = row.try_get("state").map_err(target)?;
        let held: Option<Uuid> = row.try_get("run_id").map_err(target)?;
        let dataset: Option<String> = row.try_get("dataset_id").map_err(target)?;
        if let Some(source) = owned_source {
            let generation: i64 = row.try_get("generation").map_err(target)?;
            if generation != source.generation
                || dataset.as_deref() != Some(source.dataset_id.to_string().as_str())
            {
                return Err(MigrationError::TargetBusy);
            }
        }
        let now = chrono::Utc::now().timestamp_millis();
        let outcome = if state == "migrating" {
            let run_id = held.ok_or(MigrationError::TargetBusy)?;
            if owned_run.is_some_and(|owned| owned != run_id) {
                return Err(MigrationError::TargetBusy);
            }
            let existing = sqlx::query(
                "SELECT source_fingerprint, state, direction FROM storage_migration_runs \
                 WHERE run_id = $1",
            )
            .bind(run_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(target)?;
            let resumable = existing.is_some_and(|run| {
                run.try_get::<String, _>("source_fingerprint")
                    .ok()
                    .as_deref()
                    == Some(fingerprint)
                    && run.try_get::<String, _>("direction").ok().as_deref() == Some(direction)
                    && matches!(
                        run.try_get::<String, _>("state").ok().as_deref(),
                        Some("running" | "abandoned")
                    )
                    || (owned_run == Some(run_id)
                        && run
                            .try_get::<String, _>("source_fingerprint")
                            .ok()
                            .as_deref()
                            == Some(fingerprint)
                        && run.try_get::<String, _>("direction").ok().as_deref() == Some(direction)
                        && run.try_get::<String, _>("state").ok().as_deref() == Some("verified"))
            });
            if !resumable {
                return Err(MigrationError::TargetBusy);
            }
            sqlx::query(
                "UPDATE storage_migration_runs SET state = 'running', updated_at_ms = $2                  WHERE run_id = $1 AND state = 'abandoned'",
            )
            .bind(run_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(target)?;
            (run_id, true)
        } else {
            if direction == "export" {
                // Only an activated dataset has history to hand back.
                if state != "open" || dataset.is_none() {
                    return Err(MigrationError::NotActivated);
                }
            }
            let occupied: bool = direction == "import"
                && sqlx::query_scalar(EMPTY_TARGET_SQL)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(target)?;
            if occupied {
                return Err(MigrationError::TargetNotEmpty);
            }
            let run_id = owned_run.unwrap_or_else(Uuid::now_v7);
            sqlx::query(
                "INSERT INTO storage_migration_runs \
                 (run_id, direction, source_fingerprint, state, started_at_ms, updated_at_ms) \
                 VALUES ($1, $2, $3, 'running', $4, $4)",
            )
            .bind(run_id)
            .bind(direction)
            .bind(fingerprint)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(target)?;
            sqlx::query(
                "UPDATE storage_activation \
                 SET state = 'migrating', run_id = $1, updated_at_ms = $2 WHERE singleton",
            )
            .bind(run_id)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(target)?;
            (run_id, false)
        };
        tx.commit().await.map_err(target)?;
        Ok(outcome)
    }

    async fn set_run_state(&self, run_id: Uuid, state: &str) -> Result<(), MigrationError> {
        let mut connection = self.connection().await?;
        sqlx::query(
            "UPDATE storage_migration_runs SET state = $2, updated_at_ms = $3 \
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(state)
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(&mut *connection)
        .await
        .map_err(target)?;
        Ok(())
    }

    async fn record_digest(
        &self,
        run_id: Uuid,
        domain: Domain,
        digest: &DomainDigest,
    ) -> Result<(), MigrationError> {
        let mut connection = self.connection().await?;
        sqlx::query(
            "INSERT INTO storage_migration_domains \
             (run_id, domain, done, row_count, digest) VALUES ($1, $2, TRUE, $3, $4) \
             ON CONFLICT (run_id, domain) DO UPDATE SET done = TRUE, row_count = $3, digest = $4",
        )
        .bind(run_id)
        .bind(domain.name())
        .bind(i64::try_from(digest.count).unwrap_or(i64::MAX))
        .bind(&digest.digest)
        .execute(&mut *connection)
        .await
        .map_err(target)?;
        Ok(())
    }

    /// Write one domain in key order, committing a checkpoint with every batch.
    async fn import_domain<D: DomainOps>(
        &self,
        run_id: Uuid,
        batches: &mut usize,
    ) -> Result<u64, MigrationError> {
        let (mut cursor, done, mut moved) = {
            let mut connection = self.connection().await?;
            let row = sqlx::query(
                "SELECT resume_cursor, done, row_count \
                 FROM storage_migration_domains WHERE run_id = $1 AND domain = $2",
            )
            .bind(run_id)
            .bind(D::DOMAIN.name())
            .fetch_optional(&mut *connection)
            .await
            .map_err(target)?;
            match row {
                Some(row) => (
                    row.try_get::<Option<String>, _>("resume_cursor")
                        .map_err(target)?,
                    row.try_get::<bool, _>("done").map_err(target)?,
                    row.try_get::<i64, _>("row_count").map_err(target)? as u64,
                ),
                None => (None, false, 0),
            }
        };
        if done {
            return Ok(moved);
        }
        loop {
            let records = D::export(&self.source, cursor.as_deref(), self.batch_size)
                .await
                .map_err(source)?;
            let last = records.last().map(D::key);
            let mut connection = self.connection().await?;
            let mut tx = connection.begin().await.map_err(target)?;
            D::import(&mut tx, &records).await.map_err(target)?;
            moved += records.len() as u64;
            let finished = records.len() < self.batch_size;
            checkpoint(
                &mut tx,
                run_id,
                D::DOMAIN,
                last.as_deref().or(cursor.as_deref()),
                finished,
                moved,
            )
            .await?;
            tx.commit().await.map_err(target)?;
            *batches += 1;
            if finished {
                return Ok(moved);
            }
            if self.batch_limit.is_some_and(|limit| *batches >= limit) {
                return Err(MigrationError::Interrupted);
            }
            cursor = last;
        }
    }

    /// Copy one domain from PostgreSQL into the staged home in key order, committing a
    /// checkpoint after every page.
    async fn export_domain<D: DomainOps>(
        &self,
        run_id: Uuid,
        staged: &SqliteTarget,
        batches: &mut usize,
    ) -> Result<u64, MigrationError> {
        let (mut cursor, done, mut moved) = self.load_checkpoint::<D>(run_id).await?;
        if done {
            return Ok(moved);
        }
        loop {
            let records = {
                let mut connection = self.connection().await?;
                D::read_back(&mut connection, cursor.as_deref(), self.batch_size)
                    .await
                    .map_err(target)?
            };
            let last = records.last().map(D::key);
            D::write_sqlite(staged, &records)
                .await
                .map_err(|error| MigrationError::Staging(error.to_string()))?;
            moved += records.len() as u64;
            let finished = records.len() < self.batch_size;
            let mut connection = self.connection().await?;
            let mut tx = connection.begin().await.map_err(target)?;
            checkpoint(
                &mut tx,
                run_id,
                D::DOMAIN,
                last.as_deref().or(cursor.as_deref()),
                finished,
                moved,
            )
            .await?;
            tx.commit().await.map_err(target)?;
            *batches += 1;
            if finished {
                return Ok(moved);
            }
            if self.batch_limit.is_some_and(|limit| *batches >= limit) {
                return Err(MigrationError::Interrupted);
            }
            cursor = last;
        }
    }

    /// The saved position of a domain in this run: cursor, whether it finished, rows so far.
    async fn load_checkpoint<D: DomainOps>(
        &self,
        run_id: Uuid,
    ) -> Result<(Option<String>, bool, u64), MigrationError> {
        let mut connection = self.connection().await?;
        let row = sqlx::query(
            "SELECT resume_cursor, done, row_count \
             FROM storage_migration_domains WHERE run_id = $1 AND domain = $2",
        )
        .bind(run_id)
        .bind(D::DOMAIN.name())
        .fetch_optional(&mut *connection)
        .await
        .map_err(target)?;
        Ok(match row {
            Some(row) => (
                row.try_get::<Option<String>, _>("resume_cursor")
                    .map_err(target)?,
                row.try_get::<bool, _>("done").map_err(target)?,
                row.try_get::<i64, _>("row_count").map_err(target)? as u64,
            ),
            None => (None, false, 0),
        })
    }

    /// The source digest and the digest of what the target now holds, for one domain.
    async fn digests<D: DomainOps>(&self) -> Result<(DomainDigest, DomainDigest), MigrationError> {
        let mut from_source = DigestBuilder::new();
        let mut cursor: Option<String> = None;
        loop {
            let records = D::export(&self.source, cursor.as_deref(), self.batch_size)
                .await
                .map_err(source)?;
            for record in &records {
                from_source.add(record).map_err(source)?;
            }
            let Some(last) = records.last() else { break };
            cursor = Some(D::key(last));
            if records.len() < self.batch_size {
                break;
            }
        }
        let mut from_target = DigestBuilder::new();
        let mut connection = self.connection().await?;
        let mut cursor: Option<String> = None;
        loop {
            let records = D::read_back(&mut connection, cursor.as_deref(), self.batch_size)
                .await
                .map_err(target)?;
            for record in &records {
                from_target.add(record).map_err(target)?;
            }
            let Some(last) = records.last() else { break };
            cursor = Some(D::key(last));
            if records.len() < self.batch_size {
                break;
            }
        }
        Ok((from_source.finish(), from_target.finish()))
    }
}

async fn checkpoint(
    connection: &mut PgConnection,
    run_id: Uuid,
    domain: Domain,
    cursor: Option<&str>,
    done: bool,
    moved: u64,
) -> Result<(), MigrationError> {
    sqlx::query(
        "INSERT INTO storage_migration_domains \
         (run_id, domain, resume_cursor, done, row_count) VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (run_id, domain) DO UPDATE \
         SET resume_cursor = $3, done = $4, row_count = $5",
    )
    .bind(run_id)
    .bind(domain.name())
    .bind(cursor)
    .bind(done)
    .bind(i64::try_from(moved).unwrap_or(i64::MAX))
    .execute(connection)
    .await
    .map_err(target)?;
    Ok(())
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "gate_tests.rs"]
mod gate_tests;

#[cfg(test)]
#[path = "cutover_tests.rs"]
mod cutover_tests;

#[cfg(test)]
#[path = "export_tests.rs"]
mod export_tests;

#[cfg(test)]
#[path = "return_tests.rs"]
mod return_tests;
