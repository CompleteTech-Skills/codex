//! Durable operation records, so a restart or a reconnecting client can find an operation again
//! and a repeated request never starts a second copy of it.

use crate::BlockerCode;
use crate::PlanAction;
use serde::Deserialize;
use serde::Serialize;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use uuid::Uuid;

const DIRECTORY: &str = "storage-operations";

/// Where an operation is in its life.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Planned,
    Copying,
    Verifying,
    /// The copy is verified and waiting for the explicit step that makes it authoritative.
    Ready,
    Committing,
    Active,
    Failed,
    Cancelled,
}

impl OperationState {
    /// Whether the operation can still change without a new request.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Active | Self::Failed | Self::Cancelled)
    }
}

/// One operation as it is stored and reported.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReturnSource {
    pub dataset_id: Uuid,
    pub generation: i64,
    /// Credential-free endpoint/database/namespace selected by the confirmed plan.
    pub destination: String,
}

/// One operation as it is stored and reported.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct OperationRecord {
    pub operation_id: Uuid,
    pub action: PlanAction,
    /// The plan the operator confirmed; a changed world makes it stale.
    pub plan_digest: String,
    pub state: OperationState,
    /// The migration run in the destination, once one exists.
    pub run_id: Option<Uuid>,
    /// Old journals without source ownership cannot safely resume or cancel an export.
    #[serde(default)]
    pub return_source: Option<ReturnSource>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub blocker: Option<BlockerCode>,
    /// Rows copied so far, by domain name.
    pub copied: Vec<(String, u64)>,
}

/// The operation records of one home.
pub(crate) struct Journal {
    directory: PathBuf,
    observer: Option<Observer>,
}

/// Called with every record the journal durably writes.
pub type Observer = std::sync::Arc<dyn Fn(&OperationRecord) + Send + Sync>;

impl Journal {
    /// Cooperative cross-process exclusion for an operation's export and cancellation.
    /// The persistent lock file is never removed; closing the handle releases ownership.
    pub(crate) fn claim_return(&self, operation_id: Uuid) -> io::Result<std::fs::File> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.path(operation_id).with_extension("lock"))?;
        file.try_lock().map_err(io::Error::other)?;
        Ok(file)
    }

    pub(crate) fn new(codex_home: &std::path::Path) -> Self {
        Self {
            directory: codex_home.join(DIRECTORY),
            observer: None,
        }
    }

    pub(crate) fn observed_by(mut self, observer: Observer) -> Self {
        self.observer = Some(observer);
        self
    }

    fn notify(&self, record: &OperationRecord) {
        if let Some(observer) = &self.observer {
            observer(record);
        }
    }

    fn path(&self, operation_id: Uuid) -> PathBuf {
        self.directory.join(format!("{operation_id}.json"))
    }

    /// Record a new operation; fails if the id is already taken.
    pub(crate) fn create(&self, record: &OperationRecord) -> io::Result<()> {
        std::fs::create_dir_all(&self.directory)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.path(record.operation_id))?;
        serde_json::to_writer(&mut file, record).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        self.notify(record);
        Ok(())
    }

    /// Replace a record atomically.
    pub(crate) fn update(&self, record: &OperationRecord) -> io::Result<()> {
        let path = self.path(record.operation_id);
        let temporary = path.with_extension("json.tmp");
        let mut file = std::fs::File::create(&temporary)?;
        serde_json::to_writer(&mut file, record).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(temporary, path)?;
        self.notify(record);
        Ok(())
    }

    pub(crate) fn read(&self, operation_id: Uuid) -> io::Result<Option<OperationRecord>> {
        match std::fs::read(self.path(operation_id)) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "operation record")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Every readable record, oldest first. Unreadable files are skipped, not trusted.
    pub(crate) fn list(&self) -> Vec<OperationRecord> {
        let Ok(entries) = std::fs::read_dir(&self.directory) else {
            return Vec::new();
        };
        let mut records: Vec<OperationRecord> = entries
            .flatten()
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
            .filter_map(|entry| std::fs::read(entry.path()).ok())
            .filter_map(|bytes| serde_json::from_slice(&bytes).ok())
            .collect();
        records.sort_by_key(|record| (record.created_at_ms, record.operation_id));
        records
    }
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;
