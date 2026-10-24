//! Durable exact-path claim for an archive/unarchive transaction.
//! Exact canonical claims avoid unrecorded random journal publication aliases.
//! Incomplete claims are retained, never interpreted as a completed move plan.

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;

use super::MoveDirection;
use crate::local::rollout_move_identity::RolloutFileIdentity;
use crate::local::rollout_move_identity::rollout_file_identity;
use crate::local::rollout_move_identity::rollout_file_identity_from_handle;

const MAGIC: &[u8] = b"CODEX-MOVE-SEED/1";
const MAX_HEADER_BYTES: usize = 1_048_576 + 1024;
const MAX_PLAN_BYTES: usize = 1_048_576;
const MAX_SEED_BYTES: usize = MAX_HEADER_BYTES + MAX_PLAN_BYTES + 256;

#[derive(Deserialize, Serialize)]
struct Header {
    thread_id: ThreadId,
    direction: MoveDirection,
    identity: RolloutFileIdentity,
    #[serde(default)]
    plan: Option<serde_json::Value>,
}

pub(super) enum SeedRecord {
    Preparing,
    Prepared(Vec<u8>),
}

pub(super) struct SeedClaim {
    path: PathBuf,
    header: Header,
    file: File,
    header_len: u64,
    framed_header: Vec<u8>,
}

pub(super) fn claim_path(home: &Path, thread_id: ThreadId) -> PathBuf {
    home.join("rollout_move_transactions").join(format!("{thread_id}.seed"))
}

fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(path.parent().ok_or_else(|| io::Error::other("seed has no parent"))?)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
pub(super) fn create(home: &Path, thread_id: ThreadId, direction: MoveDirection) -> io::Result<SeedClaim> {
    create_claim(home, thread_id, direction, None)
}

pub(super) fn create_with_plan(home: &Path, thread_id: ThreadId, direction: MoveDirection, plan: &[u8]) -> io::Result<SeedClaim> {
    validate_plan(plan, thread_id, direction)?;
    create_claim(home, thread_id, direction, Some(serde_json::from_slice(plan).map_err(io::Error::other)?))
}

fn create_claim(home: &Path, thread_id: ThreadId, direction: MoveDirection, plan: Option<serde_json::Value>) -> io::Result<SeedClaim> {
    let canonical_home = std::fs::canonicalize(home)?;
    let path = claim_path(&canonical_home, thread_id);
    let parent = path.parent().ok_or_else(|| io::Error::other("seed has no parent"))?;
    std::fs::create_dir_all(parent)?;
    sync_parent(parent)?;
    if !std::fs::canonicalize(parent)?.starts_with(&canonical_home) {
        return Err(io::Error::other("seed collection escapes Codex home"));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    let header = Header { thread_id, direction, identity: rollout_file_identity_from_handle(&file)?, plan };
    let bytes = serde_json::to_vec(&header).map_err(io::Error::other)?;
    if bytes.len() > MAX_HEADER_BYTES {
        return Err(io::Error::other("seed header is too large"));
    }
    let mut framed_header = Vec::new();
    framed_header.extend_from_slice(MAGIC);
    framed_header.push(b'\n');
    framed_header.extend_from_slice(&bytes);
    framed_header.push(b'\n');
    framed_header.extend_from_slice(blake3::hash(&bytes).to_hex().as_bytes());
    framed_header.push(b'\n');
    file.write_all(&framed_header)?;
    file.sync_all()?;
    sync_parent(&path)?;
    let header_len = file.metadata()?.len();
    Ok(SeedClaim { path, header, file, header_len, framed_header })
}

impl SeedClaim {
    pub(super) fn complete(&mut self, plan: &[u8]) -> io::Result<()> {
        validate_plan(plan, self.header.thread_id, self.header.direction)?;
        if let Some(expected) = &self.header.plan
            && serde_json::from_slice::<serde_json::Value>(plan).map_err(io::Error::other)? != *expected
        {
            return Err(io::Error::other("seed completion differs from its durable preparation"));
        }
        if self.file.metadata()?.len() != self.header_len
            || rollout_file_identity(&self.path)? != self.header.identity
            || rollout_file_identity_from_handle(&self.file)? != self.header.identity
        {
            return Err(io::Error::other("seed identity or phase changed"));
        }
        let original_header = self.framed_header.clone();
        self.verify_contents(&original_header)?;
        self.file.write_all(plan)?;
        self.file.write_all(b"\n")?;
        self.file.write_all(blake3::hash(plan).to_hex().as_bytes())?;
        self.file.write_all(b"\nCOMPLETE\n")?;
        // Caller must not move/quarantine bytes or mutate SQLite before this returns success.
        self.file.sync_all()?;
        sync_parent(&self.path)?;
        let mut completed = self.framed_header.clone();
        completed.extend_from_slice(plan);
        completed.push(b'\n');
        completed.extend_from_slice(blake3::hash(plan).to_hex().as_bytes());
        completed.extend_from_slice(b"\nCOMPLETE\n");
        self.verify_contents(&completed)
    }

    fn verify_contents(&mut self, expected: &[u8]) -> io::Result<()> {
        let before = self.file.metadata()?;
        if before.len() != expected.len() as u64
            || rollout_file_identity(&self.path)? != self.header.identity
            || rollout_file_identity_from_handle(&self.file)? != self.header.identity
        {
            return Err(io::Error::other("seed claim identity or size changed"));
        }
        self.file.rewind()?;
        let mut actual = Vec::new();
        Read::by_ref(&mut self.file).take(MAX_SEED_BYTES as u64 + 1).read_to_end(&mut actual)?;
        let after = self.file.metadata()?;
        if actual != expected || before.len() != after.len()
            || before.modified()? != after.modified()?
            || rollout_file_identity(&self.path)? != self.header.identity
        {
            return Err(io::Error::other("seed claim contents changed"));
        }
        Ok(())
    }
}

fn validate_plan(plan: &[u8], thread_id: ThreadId, direction: MoveDirection) -> io::Result<()> {
    if plan.len() > MAX_PLAN_BYTES || plan.contains(&b'\n') {
        return Err(io::Error::other("seed plan exceeds its frame bounds"));
    }
    let value: serde_json::Value = serde_json::from_slice(plan).map_err(io::Error::other)?;
    if value.get("thread_id") != Some(&serde_json::to_value(thread_id).map_err(io::Error::other)?)
        || value.get("direction") != Some(&serde_json::to_value(direction).map_err(io::Error::other)?)
    {
        return Err(io::Error::other("seed plan belongs to another operation"));
    }
    Ok(())
}

pub(super) fn read(home: &Path, thread_id: ThreadId) -> io::Result<SeedRecord> {
    let canonical_home = std::fs::canonicalize(home)?;
    let path = claim_path(&canonical_home, thread_id);
    let parent = path.parent().ok_or_else(|| io::Error::other("seed has no parent"))?;
    if !std::fs::canonicalize(parent)?.starts_with(&canonical_home)
        || !std::fs::symlink_metadata(&path)?.file_type().is_file()
    {
        return Err(io::Error::other("seed path is outside its collection or is not a file"));
    }
    let mut file = File::open(&path)?;
    let before = file.metadata()?;
    if before.len() > MAX_SEED_BYTES as u64 {
        return Err(io::Error::other("seed is too large"));
    }
    let identity = rollout_file_identity_from_handle(&file)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file).take(MAX_SEED_BYTES as u64 + 1).read_to_end(&mut bytes)?;
    let after = file.metadata()?;
    if bytes.len() > MAX_SEED_BYTES || bytes.len() as u64 != before.len()
        || before.len() != after.len() || before.modified()? != after.modified()?
        || rollout_file_identity(&path)? != identity
    {
        return Err(io::Error::other("seed changed while reading"));
    }
    let mut lines = bytes.split(|byte| *byte == b'\n');
    if lines.next() != Some(MAGIC) {
        return Err(io::Error::other("unsupported or incomplete seed header"));
    }
    let header_bytes = lines.next().ok_or_else(|| io::Error::other("incomplete seed header"))?;
    let header_digest = lines.next().ok_or_else(|| io::Error::other("incomplete seed header"))?;
    if header_bytes.len() > MAX_HEADER_BYTES || blake3::hash(header_bytes).to_hex().as_bytes() != header_digest {
        return Err(io::Error::other("seed header integrity failed"));
    }
    let header: Header = serde_json::from_slice(header_bytes).map_err(io::Error::other)?;
    if header.thread_id != thread_id || header.identity != identity {
        return Err(io::Error::other("seed header does not own this canonical claim"));
    }
    let tail = lines.collect::<Vec<_>>();
    if tail == [b"".as_slice()] || !bytes.ends_with(b"\nCOMPLETE\n") {
        // No destructive action or automatic cleanup may follow this result.
        return Ok(SeedRecord::Preparing);
    }
    if tail.len() != 4 || tail[2] != b"COMPLETE" || !tail[3].is_empty()
        || blake3::hash(tail[0]).to_hex().as_bytes() != tail[1]
    {
        return Err(io::Error::other("seed plan integrity failed"));
    }
    validate_plan(tail[0], thread_id, header.direction)?;
    if let Some(expected) = header.plan
        && serde_json::from_slice::<serde_json::Value>(tail[0]).map_err(io::Error::other)? != expected
    {
        return Err(io::Error::other("seed plan differs from its durable preparation"));
    }
    Ok(SeedRecord::Prepared(tail[0].to_vec()))
}

#[cfg(test)]
#[path = "rollout_move_seed_tests.rs"]
mod tests;


pub(super) fn recover_preparing(home: &Path, thread_id: ThreadId) -> io::Result<Vec<u8>> {
    if !matches!(read(home, thread_id)?, SeedRecord::Preparing) {
        return Err(io::Error::other("seed is not preparing"));
    }
    let path = claim_path(&std::fs::canonicalize(home)?, thread_id);
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    let mut bytes = Vec::new();
    Read::by_ref(&mut file).take(MAX_SEED_BYTES as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > MAX_SEED_BYTES { return Err(io::Error::other("seed is too large")); }
    let mut lines = bytes.split(|byte| *byte == b'\n');
    if lines.next() != Some(MAGIC) { return Err(io::Error::other("incomplete seed header")); }
    let header_bytes = lines.next().ok_or_else(|| io::Error::other("incomplete seed header"))?;
    let digest = lines.next().ok_or_else(|| io::Error::other("incomplete seed header"))?;
    if header_bytes.len() > MAX_HEADER_BYTES || blake3::hash(header_bytes).to_hex().as_bytes() != digest {
        return Err(io::Error::other("seed header integrity failed"));
    }
    let header: Header = serde_json::from_slice(header_bytes).map_err(io::Error::other)?;
    if header.thread_id != thread_id || rollout_file_identity_from_handle(&file)? != header.identity {
        return Err(io::Error::other("seed recovery does not own this claim"));
    }
    let plan = serde_json::to_vec(header.plan.as_ref().ok_or_else(|| io::Error::other("seed has no durable preparation"))?)
        .map_err(io::Error::other)?;
    validate_plan(&plan, thread_id, header.direction)?;
    let header_len = (MAGIC.len() + header_bytes.len() + digest.len() + 3) as u64;
    let framed_header = bytes.get(..header_len as usize)
        .ok_or_else(|| io::Error::other("seed header delimiter is incomplete"))?.to_vec();
    let mut claim = SeedClaim { path, header, file, header_len, framed_header };
    // Compare every existing byte on the owned handle before discarding a partial frame.
    claim.verify_contents(&bytes)?;
    claim.file.set_len(header_len)?;
    claim.file.sync_all()?;
    claim.complete(&plan)?;
    Ok(plan)
}
