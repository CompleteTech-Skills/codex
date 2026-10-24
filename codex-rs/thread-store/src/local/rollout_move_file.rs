//! Inactive no-clobber rollout publication primitive. Activated by the lifecycle stage.

use std::fs::FileTimes;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;

use super::rollout_move_identity::RolloutFileIdentity;
use super::rollout_move_identity::rollout_file_digest;
use super::rollout_move_identity::rollout_file_identity;
use super::rollout_move_identity::rollout_file_identity_from_handle;
#[path = "rollout_move_receipt_publication.rs"]
mod receipt_publication;
pub(super) fn touch_modified_time(path: &Path) -> std::io::Result<()> {
    let times = FileTimes::new().set_modified(SystemTime::now());
    OpenOptions::new().append(true).open(path)?.set_times(times)
}

pub(super) fn move_rollout_noclobber_retained_bound(
    source: &Path,
    destination: &Path,
    codex_home: &Path,
    expected_source_id: RolloutFileIdentity,
    expected_source_digest: [u8; 32],
) -> io::Result<()> {
    let binding = SourceBinding::Journaled {
        identity: expected_source_id,
        digest: expected_source_digest,
    };
    binding.verify(source)?;
    move_rollout_with_hooks(
        source,
        destination,
        codex_home,
        binding,
        || Ok(()),
        || binding.verify(source),
        sync_parent_directory,
    )
}

#[derive(Clone, Copy)]
enum SourceBinding {
    #[cfg(test)]
    Unbound,
    Journaled {
        identity: RolloutFileIdentity,
        digest: [u8; 32],
    },
}

impl SourceBinding {
    fn verify(self, source: &Path) -> io::Result<()> {
        match self {
            #[cfg(test)]
            Self::Unbound => Ok(()),
            Self::Journaled { identity, digest } => {
                if rollout_file_identity(source)? != identity
                    || rollout_file_digest(source)? != digest
                {
                    return Err(io::Error::other(
                        "rollout source differs from journaled revision",
                    ));
                }
                Ok(())
            }
        }
    }
}

fn move_rollout_with_hooks(
    source: &Path,
    destination: &Path,
    codex_home: &Path,
    binding: SourceBinding,
    before_staging: impl FnOnce() -> io::Result<()>,
    before_quarantine: impl FnOnce() -> io::Result<()>,
    sync_intent_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let canonical_sessions =
        std::fs::canonicalize(codex_home.join(codex_rollout::SESSIONS_SUBDIR))?;
    let canonical_archived =
        std::fs::canonicalize(codex_home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR))?;
    let canonical_source = std::fs::canonicalize(source)?;
    let canonical_destination_parent = std::fs::canonicalize(
        destination
            .parent()
            .ok_or_else(|| std::io::Error::other("rollout destination has no parent"))?,
    )?;
    let destination_name = destination
        .file_name()
        .ok_or_else(|| std::io::Error::other("rollout destination has no filename"))?;
    let within_collections = (canonical_source.starts_with(&canonical_sessions)
        && canonical_destination_parent.starts_with(&canonical_archived))
        || (canonical_source.starts_with(&canonical_archived)
            && canonical_destination_parent.starts_with(&canonical_sessions));
    if !within_collections || !std::fs::symlink_metadata(source)?.file_type().is_file() {
        return Err(std::io::Error::other(
            "rollout move is outside its collection or is not a file",
        ));
    }

    let canonical_destination = canonical_destination_parent.join(destination_name);
    let intent_path = rollout_move_intent_path(&canonical_destination);
    let source_metadata = std::fs::metadata(source)?;
    let source_id = rollout_file_identity(source)?;
    let source_digest = rollout_file_digest(source)?;
    binding.verify(source)?;
    before_staging()?;
    binding.verify(source)?;
    let source_after_hook = std::fs::metadata(source)?;
    if source_after_hook.len() != source_metadata.len()
        || source_after_hook.modified()? != source_metadata.modified()?
        || rollout_file_identity(source)? != source_id
        || rollout_file_digest(source)? != source_digest
    {
        return Err(io::Error::other("rollout source changed before staging"));
    }
    let intent = match std::fs::symlink_metadata(&intent_path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file() {
                return Err(io::Error::other("rollout move has a conflicting intent"));
            }
            let intent = read_rollout_move_intent(&intent_path)?;
            if intent.source != canonical_source
                || intent.destination != canonical_destination
                || intent.source_len != source_metadata.len()
                || intent.source_modified != source_metadata.modified()?
                || intent.source_id != source_id
                || intent.source_digest != source_digest
            {
                return Err(io::Error::other("rollout move has a conflicting intent"));
            }
            intent
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            match std::fs::symlink_metadata(&canonical_destination) {
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Ok(_) => return Err(io::Error::from(io::ErrorKind::AlreadyExists)),
                Err(err) => return Err(err),
            }
            // Give the published file an identity distinct from the source before the
            // no-clobber publication. The durable intent can then distinguish our file
            // from even an unrelated hard link to the source after a crash.
            let intent = allocate_resources(&canonical_source, &canonical_destination)?;
            // Publication errors can follow a durable receipt. Retain its resources.
            write_rollout_move_intent(&intent_path, &intent, sync_intent_parent)?;
            // The receipt must be durable before any rollout bytes enter the stage.
            // A crash before this point can leave only an empty, unowned tempfile.
            intent
        }
        Err(err) => return Err(err),
    };
    if intent.stage_path.parent() != Some(canonical_destination_parent.as_path())
        || !intent
            .stage_path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(".codex-rollout-stage-"))
    {
        return Err(io::Error::other("rollout move has an invalid stage path"));
    }
    quarantine_directory(&intent)?;
    match std::fs::symlink_metadata(&canonical_destination) {
        Ok(_) => {
            if rollout_file_identity(&canonical_destination)? != intent.stage_id
                || rollout_file_digest(&canonical_destination)? != intent.stage_digest
            {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            prepare_stage(source, &intent, binding)?;
            binding.verify(source)?;
            if rollout_file_identity(&intent.stage_path)? != intent.stage_id
                || rollout_file_digest(&intent.stage_path)? != intent.stage_digest
            {
                return Err(io::Error::other("rollout move stage identity changed"));
            }
            let stage = tempfile::TempPath::try_from_path(&intent.stage_path)?;
            if let Err(err) = stage.persist_noclobber(&canonical_destination) {
                let kind = err.error.kind();
                let _ = err.path.keep();
                return Err(io::Error::from(kind));
            }
            if rollout_file_identity(&canonical_destination)? != intent.stage_id
                || rollout_file_digest(&canonical_destination)? != intent.stage_digest
            {
                return Err(io::Error::other(
                    "rollout move destination identity changed",
                ));
            }
        }
        Err(err) => return Err(err),
    }
    sync_parent_directory(&canonical_destination)?;
    if rollout_file_identity(source)? != intent.source_id
        || rollout_file_digest(source)? != intent.source_digest
    {
        return Err(io::Error::other("rollout source changed before quarantine"));
    }
    before_quarantine()?;
    // Rename into our own same-directory quarantine before deciding what to delete. A
    // replacement at the source pathname can then be detected without deleting it.
    std::fs::rename(source, &intent.quarantine_path)?;
    sync_parent_directory(source)?;
    sync_parent_directory(&intent.quarantine_path)?;
    finish_quarantined_source(&intent)?;
    Ok(())
}

fn sync_parent_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("missing parent"))?,
    )?
    .sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn prepare_stage(
    source: &Path,
    intent: &RolloutMoveIntent,
    binding: SourceBinding,
) -> io::Result<()> {
    binding.verify(source)?;
    let mut stage = std::fs::File::open(&intent.stage_path)?;
    if rollout_file_identity(&intent.stage_path)? != intent.stage_id
        || rollout_file_identity_from_handle(&stage)? != intent.stage_id
    {
        return Err(io::Error::other("rollout move stage identity changed"));
    }
    let stage_metadata = stage.metadata()?;
    if !stage_has_single_link(&stage)? || stage_metadata.len() > intent.source_len {
        return Err(io::Error::other(
            "rollout move stage is not an exclusive partial copy",
        ));
    }
    let mut input = validate_stage_prefix(source, &mut stage, stage_metadata.len())?;
    if stage_metadata.len() == intent.source_len
        && rollout_file_digest(&intent.stage_path)? == intent.stage_digest
        && stage_metadata.modified()? == intent.source_modified
        && stage_permissions_match(&stage_metadata, &std::fs::metadata(source)?)
    {
        return Ok(());
    }
    let mut writable_stage = OpenOptions::new().append(true).open(&intent.stage_path)?;
    if rollout_file_identity_from_handle(&writable_stage)? != intent.stage_id {
        return Err(io::Error::other("rollout move stage identity changed"));
    }
    if stage_metadata.len() < intent.source_len {
        io::copy(&mut input, &mut writable_stage)?;
    }
    let source_metadata = std::fs::metadata(source)?;
    if source_metadata.len() != intent.source_len
        || source_metadata.modified()? != intent.source_modified
        || rollout_file_identity(source)? != intent.source_id
        || rollout_file_digest(source)? != intent.source_digest
        || rollout_file_identity_from_handle(&writable_stage)? != intent.stage_id
        || rollout_file_digest(&intent.stage_path)? != intent.stage_digest
    {
        return Err(io::Error::other(
            "rollout source or stage changed while preparing move",
        ));
    }
    writable_stage.set_times(FileTimes::new().set_modified(intent.source_modified))?;
    writable_stage.set_permissions(source_metadata.permissions())?;
    writable_stage.sync_all()?;
    sync_parent_directory(&intent.stage_path)
}

fn validate_stage_prefix(
    source: &Path,
    stage: &mut std::fs::File,
    stage_len: u64,
) -> io::Result<std::fs::File> {
    let mut input = std::fs::File::open(source)?;
    let mut remaining = stage_len;
    let mut source_buf = [0; 8192];
    let mut stage_buf = [0; 8192];
    while remaining > 0 {
        let amount = remaining.min(source_buf.len() as u64) as usize;
        input.read_exact(&mut source_buf[..amount])?;
        stage.read_exact(&mut stage_buf[..amount])?;
        if source_buf[..amount] != stage_buf[..amount] {
            return Err(io::Error::other(
                "rollout move stage differs from source prefix",
            ));
        }
        remaining -= amount as u64;
    }
    Ok(input)
}

#[cfg(unix)]
fn stage_permissions_match(stage: &std::fs::Metadata, source: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;

    stage.permissions().mode() == source.permissions().mode()
}

#[cfg(windows)]
fn stage_permissions_match(stage: &std::fs::Metadata, source: &std::fs::Metadata) -> bool {
    stage.permissions().readonly() == source.permissions().readonly()
}

#[cfg(unix)]
fn stage_has_single_link(file: &std::fs::File) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;

    Ok(file.metadata()?.nlink() == 1)
}

#[cfg(windows)]
fn stage_has_single_link(file: &std::fs::File) -> io::Result<bool> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::FILE_STANDARD_INFO;
    use windows_sys::Win32::Storage::FileSystem::FileStandardInfo;
    use windows_sys::Win32::Storage::FileSystem::GetFileInformationByHandleEx;

    let mut info: FILE_STANDARD_INFO = unsafe { std::mem::zeroed() };
    let result = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle() as _,
            FileStandardInfo,
            (&mut info as *mut FILE_STANDARD_INFO).cast(),
            std::mem::size_of::<FILE_STANDARD_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(info.NumberOfLinks == 1)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct RolloutMoveIntent {
    #[serde(with = "super::rollout_move_path_json")]
    source: PathBuf,
    #[serde(with = "super::rollout_move_path_json")]
    destination: PathBuf,
    source_len: u64,
    source_modified: SystemTime,
    source_id: RolloutFileIdentity,
    source_digest: [u8; 32],
    #[serde(with = "super::rollout_move_path_json")]
    stage_path: PathBuf,
    stage_id: RolloutFileIdentity,
    stage_digest: [u8; 32],
    #[serde(with = "super::rollout_move_path_json")]
    quarantine_path: PathBuf,
    #[serde(default)]
    quarantine_id: Option<RolloutFileIdentity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_publication: Option<receipt_publication::ReceiptPublication>,
}

fn quarantine_directory(intent: &RolloutMoveIntent) -> io::Result<&Path> {
    let directory = intent
        .quarantine_path
        .parent()
        .ok_or_else(|| io::Error::other("rollout quarantine has no parent"))?;
    if directory.parent() != intent.source.parent()
        || intent.quarantine_path.file_name() != Some(std::ffi::OsStr::new("quarantined-source"))
        || !directory.file_name().is_some_and(|name| {
            name.to_string_lossy()
                .starts_with(".codex-rollout-quarantine-")
        })
    {
        return Err(io::Error::other(
            "rollout move has an invalid quarantine path",
        ));
    }
    if let Some(expected) = intent.quarantine_id {
        match super::rollout_move_identity::rollout_directory_identity(directory) {
            Ok(actual) if actual == expected => {}
            Ok(_) => return Err(io::Error::other("quarantine directory was replaced")),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(directory)
}

fn finish_quarantined_source(intent: &RolloutMoveIntent) -> io::Result<()> {
    let directory = quarantine_directory(intent)?;
    match std::fs::symlink_metadata(&intent.quarantine_path) {
        Ok(metadata) => {
            if !metadata.file_type().is_file()
                || rollout_file_identity(&intent.quarantine_path)? != intent.source_id
                || rollout_file_digest(&intent.quarantine_path)? != intent.source_digest
            {
                return Err(io::Error::other(
                    "quarantined rollout source identity or contents changed",
                ));
            }
            match std::fs::symlink_metadata(&intent.source) {
                Ok(_) => {
                    return Err(io::Error::other(
                        "rollout source pathname was replaced during move",
                    ));
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            #[cfg(windows)]
            if metadata.permissions().readonly() {
                let mut permissions = metadata.permissions();
                permissions.set_readonly(false);
                std::fs::set_permissions(&intent.quarantine_path, permissions)?;
            }
            std::fs::remove_file(&intent.quarantine_path)?;
            sync_parent_directory(&intent.quarantine_path)?;
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    match std::fs::remove_dir(directory) {
        Ok(()) => sync_parent_directory(directory),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

fn rollout_move_intent_path(destination: &Path) -> PathBuf {
    let mut name = destination.as_os_str().to_owned();
    name.push(".codex-move-intent");
    PathBuf::from(name)
}

fn write_rollout_move_intent(
    path: &Path,
    intent: &RolloutMoveIntent,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    receipt_publication::write_with_sync(path, intent, sync_parent)
}

fn read_rollout_move_intent(path: &Path) -> io::Result<RolloutMoveIntent> {
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() > 4096 {
        return Err(io::Error::other("rollout move intent is too large"));
    }
    let mut contents = String::new();
    file.take(4097).read_to_string(&mut contents)?;
    if contents.len() > 4096 {
        return Err(io::Error::other("rollout move intent is too large"));
    }
    if !contents.ends_with('\n') {
        return Err(io::Error::other("rollout move intent is incomplete"));
    }
    let mut lines = contents.lines();
    let intent: RolloutMoveIntent = serde_json::from_str(
        lines
            .next()
            .ok_or_else(|| io::Error::other("empty intent"))?,
    )
    .map_err(io::Error::other)?;
    if lines.next().is_some() {
        return Err(io::Error::other("rollout move intent has extra records"));
    }
    receipt_publication::validate(path, &intent)?;
    Ok(intent)
}

pub(super) fn clear_rollout_move_intent(destination: &Path) -> io::Result<()> {
    let parent = std::fs::canonicalize(
        destination
            .parent()
            .ok_or_else(|| io::Error::other("rollout destination has no parent"))?,
    )?;
    let name = destination
        .file_name()
        .ok_or_else(|| io::Error::other("rollout destination has no filename"))?;
    let intent_path = rollout_move_intent_path(&parent.join(name));
    let intent = read_rollout_move_intent(&intent_path)?;
    quarantine_directory(&intent)?;
    if intent.stage_path.parent() != Some(parent.as_path())
        || !intent
            .stage_path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(".codex-rollout-stage-"))
    {
        return Err(io::Error::other("rollout move has an invalid stage path"));
    }
    let stage_exists = match std::fs::symlink_metadata(&intent.stage_path) {
        Ok(_) => true,
        Err(err) if err.kind() == io::ErrorKind::NotFound => false,
        Err(err) => return Err(err),
    };
    let quarantine_exists = match std::fs::symlink_metadata(&intent.quarantine_path) {
        Ok(_) => true,
        Err(err) if err.kind() == io::ErrorKind::NotFound => false,
        Err(err) => return Err(err),
    };
    if stage_exists && quarantine_exists {
        return Err(io::Error::other(
            "rollout stage and quarantined source coexist",
        ));
    }
    receipt_publication::cleanup(&intent_path, &intent)?;
    finish_quarantined_source(&intent)?;
    match std::fs::symlink_metadata(&intent.stage_path) {
        Ok(_) => {
            let mut stage = std::fs::File::open(&intent.stage_path)?;
            if rollout_file_identity(&intent.stage_path)? != intent.stage_id
                || rollout_file_identity_from_handle(&stage)? != intent.stage_id
            {
                return Err(io::Error::other("rollout move stage identity changed"));
            }
            if rollout_file_digest(&intent.stage_path)? != intent.stage_digest {
                if !stage_has_single_link(&stage)? {
                    return Err(io::Error::other(
                        "rollout move partial stage has other links",
                    ));
                }
                match std::fs::symlink_metadata(&intent.destination) {
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                    Ok(_) => {
                        return Err(io::Error::other(
                            "rollout destination exists during partial-stage cleanup",
                        ));
                    }
                    Err(err) => return Err(err),
                }
                if stage.metadata()?.len() > intent.source_len
                    || rollout_file_identity(&intent.source)? != intent.source_id
                    || rollout_file_digest(&intent.source)? != intent.source_digest
                {
                    return Err(io::Error::other("rollout move stage contents changed"));
                }
                let stage_len = stage.metadata()?.len();
                validate_stage_prefix(&intent.source, &mut stage, stage_len)?;
            }
            drop(stage);
            std::fs::remove_file(&intent.stage_path)?;
            sync_parent_directory(&intent.stage_path)?;
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    std::fs::remove_file(&intent_path)?;
    sync_parent_directory(&intent_path)
}

/// Verify a completed move whose source was unlinked before its caller committed metadata.
pub(super) fn verify_published_rollout_move(
    source: &Path,
    destination: &Path,
    codex_home: &Path,
) -> io::Result<()> {
    let canonical_sessions =
        std::fs::canonicalize(codex_home.join(codex_rollout::SESSIONS_SUBDIR))?;
    let canonical_archived =
        std::fs::canonicalize(codex_home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR))?;
    let source_parent = std::fs::canonicalize(
        source
            .parent()
            .ok_or_else(|| io::Error::other("rollout source has no parent"))?,
    )?;
    let destination_parent = std::fs::canonicalize(
        destination
            .parent()
            .ok_or_else(|| io::Error::other("rollout destination has no parent"))?,
    )?;
    let source_name = source
        .file_name()
        .ok_or_else(|| io::Error::other("rollout source has no filename"))?;
    let destination_name = destination
        .file_name()
        .ok_or_else(|| io::Error::other("rollout destination has no filename"))?;
    let within_collections = (source_parent.starts_with(&canonical_sessions)
        && destination_parent.starts_with(&canonical_archived))
        || (source_parent.starts_with(&canonical_archived)
            && destination_parent.starts_with(&canonical_sessions));
    if !within_collections || source_name != destination_name {
        return Err(io::Error::other("rollout move is outside its collection"));
    }
    match std::fs::symlink_metadata(source) {
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Ok(_) => return Err(io::Error::other("rollout move source still exists")),
        Err(err) => return Err(err),
    }
    let canonical_destination = destination_parent.join(destination_name);
    let intent = read_rollout_move_intent(&rollout_move_intent_path(&canonical_destination))?;
    if intent.source != source_parent.join(source_name)
        || intent.destination != canonical_destination
        || intent.stage_id != rollout_file_identity(&canonical_destination)?
        || intent.stage_digest != rollout_file_digest(&canonical_destination)?
    {
        return Err(io::Error::other(
            "rollout move published identity does not match",
        ));
    }
    finish_quarantined_source(&intent)
}

/// Check ownership without requiring source unlink, for an interrupted publication.
pub(super) fn published_rollout_move_owned(
    source: &Path,
    destination: &Path,
    expected_source_id: RolloutFileIdentity,
    expected_source_digest: [u8; 32],
) -> io::Result<bool> {
    let source_parent = std::fs::canonicalize(
        source
            .parent()
            .ok_or_else(|| io::Error::other("rollout source has no parent"))?,
    )?;
    let destination_parent = std::fs::canonicalize(
        destination
            .parent()
            .ok_or_else(|| io::Error::other("rollout destination has no parent"))?,
    )?;
    let source_name = source
        .file_name()
        .ok_or_else(|| io::Error::other("rollout source has no filename"))?;
    let destination_name = destination
        .file_name()
        .ok_or_else(|| io::Error::other("rollout destination has no filename"))?;
    let destination = destination_parent.join(destination_name);
    let intent = match read_rollout_move_intent(&rollout_move_intent_path(&destination)) {
        Ok(intent) => intent,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    if intent.source != source_parent.join(source_name) || intent.destination != destination {
        return Ok(false);
    }
    if intent.source_id != expected_source_id || intent.source_digest != expected_source_digest {
        return Err(io::Error::other(
            "published move belongs to a different source revision",
        ));
    }
    match std::fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(rollout_file_identity(&destination)?
            == intent.stage_id
            && rollout_file_digest(&destination)? == intent.stage_digest),
        Ok(_) => Ok(false),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
#[path = "rollout_move_file_tests.rs"]
pub(super) mod tests;


fn allocate_resources(source: &Path, destination: &Path) -> io::Result<RolloutMoveIntent> {
    let destination_parent = destination.parent().ok_or_else(|| io::Error::other("missing destination parent"))?;
    let source_metadata = std::fs::metadata(source)?;
    let source_id = rollout_file_identity(source)?;
    let source_digest = rollout_file_digest(source)?;
    let stage = tempfile::Builder::new()
        .prefix(".codex-rollout-stage-")
        .tempfile_in(&destination_parent)?;
    let stage_path = stage.path().to_path_buf();
    let stage_id = rollout_file_identity(&stage_path)?;
    let (stage_file, stage_path) = stage.keep().map_err(|err| err.error)?;
    drop(stage_file);
    sync_parent_directory(&stage_path)?;
    let quarantine_dir = tempfile::Builder::new()
        .prefix(".codex-rollout-quarantine-")
        .tempdir_in(
            source
        .parent()
        .ok_or_else(|| io::Error::other("rollout source has no parent"))?,
        )?
        .keep();
    sync_parent_directory(&quarantine_dir)?;
    // Recursive rollout discovery recognizes production filenames even inside hidden
    // directories. Keep a quarantined source out of those scans after a crash.
    let quarantine_path = quarantine_dir.join("quarantined-source");
    let intent = RolloutMoveIntent {
        source: source.to_path_buf(),
        destination: destination.to_path_buf(),
        source_len: source_metadata.len(),
        source_modified: source_metadata.modified()?,
        source_id,
        source_digest,
        stage_path,
        stage_id,
        stage_digest: source_digest,
        quarantine_path,
        quarantine_id: Some(super::rollout_move_identity::rollout_directory_identity(&quarantine_dir)?),
    receipt_publication: None,
    };
    Ok(intent)
}

pub(super) fn prepare_rollout_resources(source: &Path, destination: &Path) -> io::Result<RolloutMoveIntent> {
    let source = std::fs::canonicalize(source)?;
    let destination_parent = std::fs::canonicalize(destination.parent().ok_or_else(|| io::Error::other("missing destination parent"))?)?;
    let destination = destination_parent.join(destination.file_name().ok_or_else(|| io::Error::other("missing destination name"))?);
    let mut intent = allocate_resources(&source, &destination)?;
    let (publication, file) = receipt_publication::prepare(&intent.stage_path)?;
    drop(file);
    intent.receipt_publication = Some(publication);
    Ok(intent)
}

pub(super) fn publish_prepared_resources(intent: &RolloutMoveIntent, source: &Path, destination: &Path,
    source_id: RolloutFileIdentity, source_digest: [u8; 32]) -> io::Result<()> {
    let source = std::fs::canonicalize(source)?;
    let destination_parent = std::fs::canonicalize(destination.parent().ok_or_else(|| io::Error::other("missing destination parent"))?)?;
    let destination = destination_parent.join(destination.file_name().ok_or_else(|| io::Error::other("missing destination name"))?);
    if intent.source != source || intent.destination != destination
        || intent.source_id != source_id || intent.source_digest != source_digest
        || intent.stage_path.parent() != Some(destination_parent.as_path())
        || !intent.stage_path.file_name().is_some_and(|name| name.to_string_lossy().starts_with(".codex-rollout-stage-"))
    { return Err(io::Error::other("prepared rollout does not match its journal pair")); }

    let path = rollout_move_intent_path(&intent.destination);
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {
            let current = read_rollout_move_intent(&path)?;
            if current != *intent { return Err(io::Error::other("prepared rollout receipt changed")); }
            receipt_publication::validate(&path, &current)?;
            return Ok(());
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    if rollout_file_identity(&intent.source)? != intent.source_id
        || rollout_file_digest(&intent.source)? != intent.source_digest
        || rollout_file_identity(&intent.stage_path)? != intent.stage_id
        || std::fs::metadata(&intent.stage_path)?.len() != 0
    { return Err(io::Error::other("prepared rollout resources changed")); }
    let directory = quarantine_directory(intent)?;
    if intent.quarantine_id != Some(super::rollout_move_identity::rollout_directory_identity(directory)?) {
        return Err(io::Error::other("prepared quarantine identity changed"));
    }
    receipt_publication::verify_prepared(intent)?;
    write_rollout_move_intent(&path, intent, sync_parent_directory)
}


pub(super) fn discard_prepared_resources(intent: &RolloutMoveIntent) -> io::Result<()> {
    let publication = intent.receipt_publication.as_ref().ok_or_else(|| io::Error::other("preparation has no receipt ownership"))?;
    // Validate every surviving resource before deleting any of them.
    match std::fs::symlink_metadata(&intent.stage_path) {
        Ok(metadata) if metadata.file_type().is_file() && metadata.len() == 0
            && rollout_file_identity(&intent.stage_path)? == intent.stage_id => {}
        Ok(_) => return Err(io::Error::other("prepared stage was replaced or filled")),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    match receipt_publication::verify_empty(publication, &intent.stage_path) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let directory = quarantine_directory(intent)?;
    match std::fs::read_dir(directory) {
        Ok(mut entries) => {
            if entries.next().is_some() {
                return Err(io::Error::other("prepared quarantine is not empty"));
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    receipt_publication::remove_prepared(publication, &intent.stage_path)?;
    match std::fs::remove_file(&intent.stage_path) {
        Ok(()) => sync_parent_directory(&intent.stage_path)?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    match std::fs::remove_dir(directory) {
        Ok(()) => sync_parent_directory(directory),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}
