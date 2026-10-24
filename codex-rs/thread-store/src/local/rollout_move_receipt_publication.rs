//! Cleanup authority for aliases left by no-clobber receipt publication.
//! Pre-publication files without a canonical receipt remain unowned and retained.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use super::RolloutMoveIntent;
use super::rollout_file_identity;
use super::rollout_file_identity_from_handle;
use super::sync_parent_directory;
use crate::local::rollout_move_identity::RolloutFileIdentity;

const MAX_BYTES: u64 = 4096;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct ReceiptPublication {
    #[serde(with = "crate::local::rollout_move_path_json")]
    path: PathBuf,
    identity: RolloutFileIdentity,
}

fn staging_path(stage: &Path) -> PathBuf {
    let mut name = stage.as_os_str().to_owned();
    name.push(".move-intent");
    PathBuf::from(name)
}

#[cfg(test)]
fn write(path: &Path, intent: &RolloutMoveIntent) -> io::Result<()> {
    write_with_sync(path, intent, sync_parent_directory)
}

pub(super) fn write_with_sync(
    path: &Path,
    intent: &RolloutMoveIntent,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    write_with_publication_and_sync(path, intent, |temporary, canonical| {
        let temporary = tempfile::TempPath::try_from_path(temporary)?;
        if let Err(error) = temporary.persist_noclobber(canonical) {
            let kind = error.error.kind();
            let _ = error.path.keep();
            return Err(io::Error::from(kind));
        }
        Ok(())
    }, sync_parent)
}

#[cfg(test)]
fn write_with_publication(
    path: &Path,
    intent: &RolloutMoveIntent,
    publish: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    write_with_publication_and_sync(path, intent, publish, sync_parent_directory)
}

fn write_with_publication_and_sync(
    path: &Path,
    intent: &RolloutMoveIntent,
    publish: impl FnOnce(&Path, &Path) -> io::Result<()>,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let mut recorded = intent.clone();
    let (temporary, mut file) = if let Some(publication) = &intent.receipt_publication {
        let file = open_bound(publication, &intent.stage_path)?;
        (publication.path.clone(), file)
    } else {
        let (publication, file) = prepare(&intent.stage_path)?;
        let path = publication.path.clone();
        recorded.receipt_publication = Some(publication);
        (path, file)
    };
    let mut bytes = serde_json::to_vec(&recorded).map_err(io::Error::other)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAX_BYTES {
        // No receipt bytes were written. Keep the empty resource pending journal ownership.
        return Err(io::Error::new(io::ErrorKind::InvalidData, "rollout move intent is too large"));
    }
    let publication = recorded.receipt_publication.as_ref().ok_or_else(|| io::Error::other("receipt has no ownership"))?;
    let written = verify_prefix(&mut file, publication, &bytes)?;
    file.write_all(&bytes[written..])?;
    file.sync_all()?;
    if verify_prefix(&mut file, publication, &bytes)? != bytes.len() {
        return Err(io::Error::other("receipt write remained incomplete"));
    }
    drop(file);
    publish(&temporary, path)?;
    sync_parent(path)
}

fn read_owned(path: &Path, identity: RolloutFileIdentity) -> io::Result<Vec<u8>> {
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(io::Error::other("move receipt is not a regular file"));
    }
    let mut file = File::open(path)?;
    let before = file.metadata()?;
    if rollout_file_identity_from_handle(&file)? != identity
        || rollout_file_identity(path)? != identity
        || before.len() > MAX_BYTES
    {
        return Err(io::Error::other("move receipt identity or size changed"));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file).take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if bytes.len() as u64 > MAX_BYTES
        || bytes.len() as u64 != before.len()
        || before.len() != after.len()
        || before.modified()? != after.modified()?
        || rollout_file_identity(path)? != identity
    {
        return Err(io::Error::other("move receipt changed while reading"));
    }
    Ok(bytes)
}

pub(super) fn validate(path: &Path, intent: &RolloutMoveIntent) -> io::Result<()> {
    let Some(publication) = &intent.receipt_publication else {
        return Ok(());
    };
    if publication.path != staging_path(&intent.stage_path)
        || intent.stage_path.parent() != path.parent()
        || !intent.stage_path.file_name().is_some_and(|name| name.to_string_lossy().starts_with(".codex-rollout-stage-"))
        || super::rollout_move_intent_path(&intent.destination) != path
    {
        return Err(io::Error::other("move receipt publication path is invalid"));
    }
    let canonical = read_owned(path, publication.identity)?;
    let current: RolloutMoveIntent = serde_json::from_slice(&canonical).map_err(io::Error::other)?;
    if &current != intent {
        return Err(io::Error::other("move receipt changed after it was decoded"));
    }
    match std::fs::symlink_metadata(&publication.path) {
        Ok(_) => {
            if read_owned(&publication.path, publication.identity)? != canonical {
                return Err(io::Error::other("move receipt publication contents changed"));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

pub(super) fn cleanup(path: &Path, intent: &RolloutMoveIntent) -> io::Result<()> {
    cleanup_with_sync(path, intent, sync_parent_directory)
}

fn cleanup_with_sync(
    path: &Path,
    intent: &RolloutMoveIntent,
    sync: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    validate(path, intent)?;
    let Some(publication) = &intent.receipt_publication else {
        return Ok(());
    };
    match std::fs::remove_file(&publication.path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    // A retry must also sync an already absent alias before retiring canonical authority.
    sync(&publication.path)
}

#[cfg(test)]
#[path = "rollout_move_receipt_publication_tests.rs"]
mod tests;


pub(super) fn prepare(stage: &Path) -> io::Result<(ReceiptPublication, File)> {
    let path = staging_path(stage);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&path)?;
    let publication = ReceiptPublication { path, identity: rollout_file_identity_from_handle(&file)? };
    file.sync_all()?;
    sync_parent_directory(&publication.path)?;
    Ok((publication, file))
}

pub(super) fn verify_empty(publication: &ReceiptPublication, stage: &Path) -> io::Result<()> {
    drop(open_empty(publication, stage)?);
    Ok(())
}

fn open_empty(publication: &ReceiptPublication, stage: &Path) -> io::Result<File> {
    let file = open_bound(publication, stage)?;
    if file.metadata()?.len() != 0 {
        return Err(io::Error::other("prepared receipt alias is not empty"));
    }
    Ok(file)
}

fn open_bound(publication: &ReceiptPublication, stage: &Path) -> io::Result<File> {
    if publication.path != staging_path(stage)
        || !std::fs::symlink_metadata(&publication.path)?.file_type().is_file()
        || rollout_file_identity(&publication.path)? != publication.identity
    {
        return Err(io::Error::other("prepared receipt alias was replaced"));
    }
    let file = OpenOptions::new().read(true).write(true).open(&publication.path)?;
    if rollout_file_identity_from_handle(&file)? != publication.identity || file.metadata()?.len() > MAX_BYTES {
        return Err(io::Error::other("prepared receipt alias is not the owned empty file"));
    }
    Ok(file)
}


pub(super) fn remove_prepared(publication: &ReceiptPublication, stage: &Path) -> io::Result<()> {
    match verify_empty(publication, stage) {
        Ok(()) => {
            std::fs::remove_file(&publication.path)?;
            sync_parent_directory(&publication.path)
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}


fn verify_prefix(file: &mut File, publication: &ReceiptPublication, expected: &[u8]) -> io::Result<usize> {
    let before = file.metadata()?;
    if expected.len() as u64 > MAX_BYTES || before.len() > MAX_BYTES
        || rollout_file_identity_from_handle(file)? != publication.identity
        || rollout_file_identity(&publication.path)? != publication.identity
    { return Err(io::Error::other("prepared receipt identity or bounds changed")); }
    file.rewind()?;
    let mut existing = Vec::new();
    Read::by_ref(file).take(MAX_BYTES + 1).read_to_end(&mut existing)?;
    let after = file.metadata()?;
    if !expected.starts_with(&existing) || existing.len() as u64 != before.len()
        || before.len() != after.len() || before.modified()? != after.modified()?
        || rollout_file_identity(&publication.path)? != publication.identity
    { return Err(io::Error::other("prepared receipt is not an owned prefix of its journaled content")); }
    Ok(existing.len())
}

pub(super) fn verify_prepared(intent: &RolloutMoveIntent) -> io::Result<()> {
    let publication = intent.receipt_publication.as_ref().ok_or_else(|| io::Error::other("receipt has no ownership"))?;
    let mut file = open_bound(publication, &intent.stage_path)?;
    let mut bytes = serde_json::to_vec(intent).map_err(io::Error::other)?;
    bytes.push(b'\n');
    verify_prefix(&mut file, publication, &bytes)?;
    Ok(())
}
