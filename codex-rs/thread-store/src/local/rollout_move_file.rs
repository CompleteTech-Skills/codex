//! Inactive no-clobber rollout publication primitive. Activated by the lifecycle stage.

use std::fs::FileTimes;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use serde::Deserialize;
use serde::Serialize;

use super::rollout_move_identity::RolloutFileIdentity;
use super::rollout_move_identity::rollout_file_digest;
use super::rollout_move_identity::rollout_file_identity;
pub(super) fn touch_modified_time(path: &Path) -> std::io::Result<()> {
    let times = FileTimes::new().set_modified(SystemTime::now());
    OpenOptions::new().append(true).open(path)?.set_times(times)
}

#[cfg(test)]
pub(super) fn move_rollout_noclobber(
    source: &Path,
    destination: &Path,
    codex_home: &Path,
) -> std::io::Result<()> {
    move_rollout_noclobber_retained(source, destination, codex_home)?;
    clear_rollout_move_intent(destination)
}

#[cfg(test)]
pub(super) fn move_rollout_noclobber_retained(
    source: &Path,
    destination: &Path,
    codex_home: &Path,
) -> std::io::Result<()> {
    move_rollout_with_before_quarantine(source, destination, codex_home, || Ok(()))
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
    )
}

#[cfg(test)]
fn move_rollout_with_before_quarantine(
    source: &Path,
    destination: &Path,
    codex_home: &Path,
    before_quarantine: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    move_rollout_with_hooks(
        source,
        destination,
        codex_home,
        SourceBinding::Unbound,
        || Ok(()),
        before_quarantine,
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
            let mut stage = tempfile::Builder::new()
                .prefix(".codex-rollout-stage-")
                .tempfile_in(&canonical_destination_parent)?;
            let mut input = std::fs::File::open(source)?;
            io::copy(&mut input, &mut stage)?;
            stage
                .as_file()
                .set_times(FileTimes::new().set_modified(source_metadata.modified()?))?;
            stage.as_file().sync_all()?;
            let source_after_copy = std::fs::metadata(source)?;
            if source_after_copy.len() != source_metadata.len()
                || source_after_copy.modified()? != source_metadata.modified()?
                || rollout_file_identity(source)? != source_id
            {
                return Err(io::Error::other(
                    "rollout source changed while preparing move",
                ));
            }
            let stage_path = stage.path().to_path_buf();
            let stage_id = rollout_file_identity(&stage_path)?;
            let stage_digest = rollout_file_digest(&stage_path)?;
            if stage_digest != source_digest {
                return Err(io::Error::other(
                    "rollout source changed while preparing move",
                ));
            }
            binding.verify(source)?;
            let (stage_file, stage_path) = stage.keep().map_err(|err| err.error)?;
            drop(stage_file);
            sync_parent_directory(&stage_path)?;
            let quarantine_dir = tempfile::Builder::new()
                .prefix(".codex-rollout-quarantine-")
                .tempdir_in(
                    canonical_source
                        .parent()
                        .ok_or_else(|| io::Error::other("rollout source has no parent"))?,
                )?
                .keep();
            sync_parent_directory(&quarantine_dir)?;
            let quarantine_path = quarantine_dir.join(
                canonical_source
                    .file_name()
                    .ok_or_else(|| io::Error::other("rollout source has no filename"))?,
            );
            let intent = RolloutMoveIntent {
                source: canonical_source,
                destination: canonical_destination.clone(),
                source_len: source_metadata.len(),
                source_modified: source_metadata.modified()?,
                source_id,
                source_digest,
                stage_path,
                stage_id,
                stage_digest,
                quarantine_path,
            };
            if let Err(err) = write_rollout_move_intent(&intent_path, &intent) {
                let _ = std::fs::remove_file(&intent.stage_path);
                let _ = std::fs::remove_dir(quarantine_dir);
                return Err(err);
            }
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

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct RolloutMoveIntent {
    source: PathBuf,
    destination: PathBuf,
    source_len: u64,
    source_modified: SystemTime,
    source_id: RolloutFileIdentity,
    source_digest: [u8; 32],
    stage_path: PathBuf,
    stage_id: RolloutFileIdentity,
    stage_digest: [u8; 32],
    quarantine_path: PathBuf,
}

fn quarantine_directory(intent: &RolloutMoveIntent) -> io::Result<&Path> {
    let directory = intent
        .quarantine_path
        .parent()
        .ok_or_else(|| io::Error::other("rollout quarantine has no parent"))?;
    if directory.parent() != intent.source.parent()
        || intent.quarantine_path.file_name() != intent.source.file_name()
        || !directory.file_name().is_some_and(|name| {
            name.to_string_lossy()
                .starts_with(".codex-rollout-quarantine-")
        })
    {
        return Err(io::Error::other(
            "rollout move has an invalid quarantine path",
        ));
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
            if std::fs::symlink_metadata(&intent.source).is_ok() {
                return Err(io::Error::other(
                    "rollout source pathname was replaced during move",
                ));
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

fn write_rollout_move_intent(path: &Path, intent: &RolloutMoveIntent) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("missing intent parent"))?;
    let mut file = tempfile::Builder::new()
        .prefix(".codex-move-intent-")
        .tempfile_in(parent)?;
    serde_json::to_writer(&mut file, intent).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.as_file().sync_all()?;
    file.persist_noclobber(path).map_err(|err| err.error)?;
    sync_parent_directory(path)
}

fn read_rollout_move_intent(path: &Path) -> io::Result<RolloutMoveIntent> {
    let mut file = std::fs::File::open(path)?;
    if file.metadata()?.len() > 4096 {
        return Err(io::Error::other("rollout move intent is too large"));
    }
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    if !contents.ends_with('\n') {
        return Err(io::Error::other("rollout move intent is incomplete"));
    }
    let mut lines = contents.lines();
    let intent = serde_json::from_str(
        lines
            .next()
            .ok_or_else(|| io::Error::other("empty intent"))?,
    )
    .map_err(io::Error::other)?;
    if lines.next().is_some() {
        return Err(io::Error::other("rollout move intent has extra records"));
    }
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
    finish_quarantined_source(&intent)?;
    match std::fs::symlink_metadata(&intent.stage_path) {
        Ok(_) => {
            if rollout_file_identity(&intent.stage_path)? != intent.stage_id
                || rollout_file_digest(&intent.stage_path)? != intent.stage_digest
            {
                return Err(io::Error::other("rollout move stage identity changed"));
            }
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

/// Recover the source revision from an owned publication when upgrading a
/// path-only move journal after its source has already been unlinked.
pub(super) fn published_rollout_source_binding(
    source: &Path,
    destination: &Path,
) -> io::Result<(RolloutFileIdentity, [u8; 32])> {
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
    let intent = read_rollout_move_intent(&rollout_move_intent_path(&destination))?;
    if intent.source != source_parent.join(source_name)
        || intent.destination != destination
        || intent.stage_id != rollout_file_identity(&destination)?
        || intent.stage_digest != rollout_file_digest(&destination)?
        || intent.stage_digest != intent.source_digest
    {
        return Err(io::Error::other(
            "legacy rollout publication does not match its receipt",
        ));
    }
    Ok((intent.source_id, intent.source_digest))
}

#[cfg(test)]
#[path = "rollout_move_file_tests.rs"]
mod tests;
