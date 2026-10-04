//! Durable operation records, so a restart or a reconnecting client can find an operation again
//! and a repeated request never starts a second copy of it.

use crate::BlockerCode;
use crate::PlanAction;
use serde::Deserialize;
use serde::Serialize;
use std::io;
#[cfg(any(not(unix), test))]
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
    observer: std::sync::Mutex<Option<Observer>>,
    #[cfg(unix)]
    namespace: std::sync::Mutex<Option<std::sync::Arc<namespace::Namespace>>>,
    #[cfg(unix)]
    claims:
        std::sync::Mutex<std::collections::HashMap<Uuid, std::sync::Weak<owned_claim::ClaimInner>>>,
    #[cfg(unix)]
    claim_scratch: std::sync::Arc<owned_claim::ScratchAdmission>,
    #[cfg(all(test, unix))]
    sync_probe: Option<std::sync::Arc<std::sync::Mutex<SyncProbe>>>,
}

/// Called with every record the journal durably writes.
pub type Observer = std::sync::Arc<dyn Fn(&OperationRecord) + Send + Sync>;

impl Journal {
    /// Cooperative cross-process exclusion for an operation's export and cancellation.
    /// The persistent lock file is never removed; closing the handle releases ownership.
    pub(crate) fn claim_return(&self, operation_id: Uuid) -> io::Result<ReturnClaim> {
        #[cfg(unix)]
        {
            self.claim_owned(operation_id)
        }
        #[cfg(not(unix))]
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.path(operation_id).with_extension("lock"))?;
            file.try_lock().map_err(io::Error::other)?;
            Ok(file)
        }
    }
    pub(crate) fn new(codex_home: &std::path::Path) -> Self {
        Self {
            directory: codex_home.join(DIRECTORY),
            observer: std::sync::Mutex::new(None),
            #[cfg(unix)]
            namespace: std::sync::Mutex::new(None),
            #[cfg(unix)]
            claims: std::sync::Mutex::new(std::collections::HashMap::new()),
            #[cfg(unix)]
            claim_scratch: std::sync::Arc::new(owned_claim::ScratchAdmission::default()),
            #[cfg(all(test, unix))]
            sync_probe: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn observed_by(self, observer: Observer) -> Self {
        self.set_observer(observer);
        self
    }
    pub(crate) fn set_observer(&self, observer: Observer) {
        *self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observer);
    }

    fn notify(&self, record: &OperationRecord) {
        let observer = self
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(observer) = observer {
            observer(record);
        }
    }

    #[cfg(any(not(unix), test))]
    fn path(&self, operation_id: Uuid) -> PathBuf {
        self.directory.join(format!("{operation_id}.json"))
    }

    /// Record a new operation; fails if the id is already taken.
    pub(crate) fn create(&self, record: &OperationRecord) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.create_owned(record)
        }
        #[cfg(not(unix))]
        {
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
    }

    /// Replace a record atomically.
    pub(crate) fn update(&self, record: &OperationRecord) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.update_owned(record)
        }
        #[cfg(not(unix))]
        {
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
    }
    pub(crate) fn read(&self, operation_id: Uuid) -> io::Result<Option<OperationRecord>> {
        #[cfg(unix)]
        {
            self.read_owned(operation_id)
        }
        #[cfg(not(unix))]
        {
            self.read_lexical(operation_id)
        }
    }
    pub(crate) fn list_checked(&self) -> io::Result<Vec<OperationRecord>> {
        #[cfg(unix)]
        {
            self.list_owned()
        }
        #[cfg(not(unix))]
        {
            self.list_checked_lexical()
        }
    }
    pub(crate) fn list(&self) -> Vec<OperationRecord> {
        #[cfg(unix)]
        {
            self.list_owned().unwrap_or_default()
        }
        #[cfg(not(unix))]
        {
            self.list_lexical()
        }
    }
    #[cfg(not(unix))]
    fn read_lexical(&self, operation_id: Uuid) -> io::Result<Option<OperationRecord>> {
        if !self.ordinary_directory()? {
            return Ok(None);
        }
        match read_record(&self.path(operation_id), operation_id) {
            Ok((record, _)) => Ok(Some(record)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
    #[cfg(not(unix))]
    fn ordinary_directory(&self) -> io::Result<bool> {
        match std::fs::symlink_metadata(&self.directory) {
            Ok(metadata) if metadata.is_dir() && !is_redirected(&metadata) => Ok(true),
            Ok(_) => Err(invalid_record("operation journal is not ordinary")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Recovery must distinguish an absent journal from unreadable ownership evidence.
    #[cfg(not(unix))]
    fn list_checked_lexical(&self) -> io::Result<Vec<OperationRecord>> {
        if !self.ordinary_directory()? {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        let mut owners = std::collections::HashSet::new();
        let mut bytes = 0_usize;
        let mut entries = 0_usize;
        for entry in std::fs::read_dir(&self.directory)? {
            let entry = entry?;
            entries += 1;
            if entries > RECORD_COUNT_LIMIT {
                return Err(invalid_record("journal entry limit"));
            }
            let path = entry.path();
            if !path
                .extension()
                .and_then(|extension| extension.to_str())
                .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
            {
                continue;
            }
            let metadata = std::fs::symlink_metadata(&path)?;
            if !metadata.file_type().is_file() || is_redirected(&metadata) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "operation record is not regular",
                ));
            }
            let operation_id = path
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| Uuid::parse_str(name).ok())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "operation record filename")
                })?;
            // Readers and writers use this exact spelling. Accepting aliases on a
            // case-sensitive directory could make an update create a second owner.
            if path.file_name().and_then(|name| name.to_str())
                != Some(format!("{operation_id}.json").as_str())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "operation record filename",
                ));
            }
            let (record, size) =
                read_record_bounded(&path, operation_id, JOURNAL_BYTE_LIMIT - bytes)?;
            bytes += size;
            if bytes > JOURNAL_BYTE_LIMIT {
                return Err(invalid_record("journal byte limit"));
            }
            if record.operation_id != operation_id || !owners.insert(operation_id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "operation record identity",
                ));
            }
            records.push(record);
        }
        records.sort_by_key(|record| (record.created_at_ms, record.operation_id));
        Ok(records)
    }

    /// Best-effort listing for display only. Never use this to decide recovery is idle.
    #[cfg(not(unix))]
    fn list_lexical(&self) -> Vec<OperationRecord> {
        if !matches!(self.ordinary_directory(), Ok(true)) {
            return Vec::new();
        }
        let Ok(entries) = std::fs::read_dir(&self.directory) else {
            return Vec::new();
        };
        let mut records = Vec::new();
        let mut bytes = 0_usize;
        for entry in entries.take(RECORD_COUNT_LIMIT).flatten() {
            let path = entry.path();
            let Some(id) = path
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| Uuid::parse_str(name).ok())
            else {
                continue;
            };
            if let Ok((record, size)) = read_record_bounded(&path, id, JOURNAL_BYTE_LIMIT - bytes) {
                bytes += size;
                if bytes > JOURNAL_BYTE_LIMIT {
                    break;
                }
                records.push(record);
            }
        }
        records.sort_by_key(|record| (record.created_at_ms, record.operation_id));
        records
    }
}

#[cfg(windows)]
fn is_redirected(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    // FILE_ATTRIBUTE_REPARSE_POINT includes junctions that still report directory type.
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(unix)]
impl Journal {
    fn sync(
        &self,
        file: &std::fs::File,
        path: &std::path::Path,
        phase: &'static str,
    ) -> io::Result<()> {
        #[cfg(test)]
        self.crash_boundary(phase, false)?;
        #[cfg(test)]
        if let Some(probe) = &self.sync_probe {
            let mut probe = probe
                .lock()
                .map_err(|_| io::Error::other("sync probe poisoned"))?;
            probe.events.push((phase, path.to_path_buf()));
            if probe.fail == Some(phase) {
                probe.fail = None;
                return Err(io::Error::other("injected sync refusal"));
            }
        }
        #[cfg(not(test))]
        let _ = (path, phase);
        file.sync_all()?;
        #[cfg(test)]
        self.crash_boundary(phase, true)?;
        Ok(())
    }
    #[cfg(test)]
    fn crash_boundary(&self, phase: &'static str, after: bool) -> io::Result<()> {
        if let Some(probe) = &self.sync_probe {
            let probe = probe
                .lock()
                .map_err(|_| io::Error::other("sync probe poisoned"))?;
            if let Some((expected, expected_after, witness)) = &probe.crash
                && *expected == phase
                && *expected_after == after
            {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(witness)?;
                write!(file, "{phase}:{after}")?;
                file.sync_all()?;
                // Process-crash control: no stack unwinding or observer notification.
                std::process::exit(87);
            }
        }
        Ok(())
    }
}
#[cfg(all(test, unix))]
#[derive(Default)]
struct SyncProbe {
    fail: Option<&'static str>,
    events: Vec<(&'static str, PathBuf)>,
    crash: Option<(&'static str, bool, PathBuf)>,
}

#[cfg(all(not(windows), any(not(unix), test)))]
fn is_redirected(metadata: &std::fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;

#[cfg(all(test, unix))]
#[path = "journal_durability_tests.rs"]
mod durability_tests;

#[cfg(all(test, unix))]
#[path = "journal_crash_tests.rs"]
mod crash_tests;

const RECORD_BYTE_LIMIT: usize = 1024 * 1024;
const RECORD_COUNT_LIMIT: usize = 4096;
const JOURNAL_BYTE_LIMIT: usize = 16 * 1024 * 1024;
fn invalid_record(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
#[cfg(any(not(unix), test))]
fn read_record(path: &std::path::Path, expected: Uuid) -> io::Result<(OperationRecord, usize)> {
    if path.file_name().and_then(|name| name.to_str()) != Some(format!("{expected}.json").as_str())
    {
        return Err(invalid_record("operation record filename"));
    }
    let file = open_record(path)?;
    read_open_record(path, expected, file, RECORD_BYTE_LIMIT)
}
#[cfg(any(not(unix), test))]
fn read_record_bounded(
    path: &std::path::Path,
    expected: Uuid,
    remaining: usize,
) -> io::Result<(OperationRecord, usize)> {
    if path.file_name().and_then(|name| name.to_str()) != Some(format!("{expected}.json").as_str())
    {
        return Err(invalid_record("operation record filename"));
    }
    let file = open_record(path)?;
    read_open_record(path, expected, file, remaining.min(RECORD_BYTE_LIMIT))
}
#[cfg(any(not(unix), test))]
fn open_record(path: &std::path::Path) -> io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Atomic final-component refusal: neither symlink nor replaced FIFO may block/read.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x00200000).share_mode(1);
    }
    options.open(path)
}
#[cfg(any(not(unix), test))]
fn read_open_record(
    path: &std::path::Path,
    expected: Uuid,
    mut file: std::fs::File,
    limit: usize,
) -> io::Result<(OperationRecord, usize)> {
    use std::io::Read;
    let before = file.metadata()?;
    if !before.is_file() || is_redirected(&before) || before.len() > limit as u64 {
        return Err(invalid_record("operation record type or byte limit"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.nlink() != 1 {
            return Err(invalid_record("operation record hardlink"));
        }
    }
    let mut bytes = Vec::new();
    (&mut file).take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit || bytes.len() as u64 != before.len() {
        return Err(invalid_record("operation record byte limit or changed"));
    }
    let after = file.metadata()?;
    let named = std::fs::symlink_metadata(path)?;
    if !named.is_file()
        || is_redirected(&named)
        || before.len() != after.len()
        || before.modified()? != after.modified()?
    {
        return Err(invalid_record("operation record changed"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if after.nlink() != 1
            || named.nlink() != 1
            || before.dev() != named.dev()
            || before.ino() != named.ino()
        {
            return Err(invalid_record("operation record identity changed"));
        }
    }
    let record: OperationRecord =
        serde_json::from_slice(&bytes).map_err(|_| invalid_record("operation record"))?;
    if record.operation_id != expected {
        return Err(invalid_record("operation record identity"));
    }
    Ok((record, bytes.len()))
}
#[cfg(test)]
#[path = "journal_read_tests.rs"]
mod bounded_read_tests;

#[cfg(unix)]
#[path = "journal_namespace.rs"]
mod namespace;
#[cfg(unix)]
#[path = "journal_owned_writes.rs"]
mod owned_writes;

#[cfg(unix)]
#[path = "journal_owned_reads.rs"]
mod owned_reads;

#[cfg(unix)]
#[path = "journal_owned_claim.rs"]
mod owned_claim;
#[cfg(unix)]
pub(crate) use owned_claim::ReturnClaim;
#[cfg(not(unix))]
pub(crate) type ReturnClaim = std::fs::File;
