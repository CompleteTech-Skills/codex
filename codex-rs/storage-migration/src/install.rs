//! Moving a staged home into place without losing anything it replaces.
//!
//! The plan is written before any file moves and names every unit of the swap. Each unit is then
//! moved by an operation whose state can be read back from the filesystem, so running the
//! installation again after a crash finishes it instead of repeating or undoing work. What the
//! install replaces is moved into a backup directory beside a manifest, and nothing in it is ever
//! deleted.

use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::collections::HashSet;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

const PLAN_FILE: &str = "storage-install.json";
const MANIFEST_FILE: &str = "manifest.json";
const BACKUPS_DIR: &str = "storage-backups";
const DIRECTORY_UNITS: [&str; 2] = ["sessions", "archived_sessions"];

/// What happens to one name in the home.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct InstallUnit {
    /// Path relative to the home.
    pub name: String,
    pub is_dir: bool,
    /// True for files that only move to the backup, like the write-ahead log of a replaced
    /// database, which must never sit next to its successor.
    pub backup_only: bool,
    /// Digest of the staged file that will be installed.
    pub staged_sha256: Option<String>,
    /// Digest of the live file that is moved to the backup.
    pub live_sha256: Option<String>,
    /// Entries and bytes of a directory that is moved to the backup.
    pub live_entries: Option<(u64, u64)>,
}

/// The durable description of one swap.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct InstallPlan {
    pub run_id: Uuid,
    pub staged_home: PathBuf,
    pub backup_dir: PathBuf,
    pub units: Vec<InstallUnit>,
}

fn plan_path(home: &Path) -> PathBuf {
    home.join(PLAN_FILE)
}

fn corrupt(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

pub(crate) fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1 << 16];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(crate) fn directory_totals(path: &Path) -> io::Result<(u64, u64)> {
    let mut entries = 0;
    let mut bytes = 0;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                entries += 1;
                bytes += metadata.len();
            }
        }
    }
    Ok((entries, bytes))
}

/// Decide what the swap will do and record it. Fails if a plan already exists.
pub fn plan_install(home: &Path, staged_home: &Path, run_id: Uuid) -> io::Result<InstallPlan> {
    let mut units = Vec::new();
    let mut staged_files = std::fs::read_dir(staged_home)?
        .collect::<io::Result<Vec<_>>>()?
        .into_iter()
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sqlite"))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    staged_files.sort();
    for name in staged_files {
        // A database moves with its sidecars, or an old log could corrupt its replacement.
        for sidecar in [format!("{name}-wal"), format!("{name}-shm")] {
            let live = home.join(&sidecar);
            if live.exists() {
                units.push(InstallUnit {
                    name: sidecar,
                    is_dir: false,
                    backup_only: true,
                    staged_sha256: None,
                    live_sha256: Some(sha256_file(&live)?),
                    live_entries: None,
                });
            }
        }
        let live = home.join(&name);
        units.push(InstallUnit {
            staged_sha256: Some(sha256_file(&staged_home.join(&name))?),
            live_sha256: if live.exists() {
                Some(sha256_file(&live)?)
            } else {
                None
            },
            name,
            is_dir: false,
            backup_only: false,
            live_entries: None,
        });
    }
    for name in DIRECTORY_UNITS {
        if staged_home.join(name).is_dir() {
            let live = home.join(name);
            units.push(InstallUnit {
                name: name.to_string(),
                is_dir: true,
                backup_only: false,
                staged_sha256: None,
                live_sha256: None,
                live_entries: if live.is_dir() {
                    Some(directory_totals(&live)?)
                } else {
                    None
                },
            });
        }
    }
    let plan = InstallPlan {
        run_id,
        staged_home: staged_home.to_path_buf(),
        backup_dir: home
            .join(BACKUPS_DIR)
            .join(format!("{run_id}-before-return")),
        units,
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(plan_path(home))?;
    serde_json::to_writer(&mut file, &plan).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(plan)
}

/// The plan a previous attempt left behind, if any.
pub fn read_plan(home: &Path) -> io::Result<Option<InstallPlan>> {
    match std::fs::read(plan_path(home)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| corrupt("install plan is unreadable")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Read the retained manifest for one completed installation without changing its backup.
pub(crate) fn read_backup_plan(home: &Path, run_id: Uuid) -> io::Result<InstallPlan> {
    let backups = home.join(BACKUPS_DIR);
    let backup = backups.join(format!("{run_id}-before-return"));
    for directory in [&backups, &backup] {
        let metadata = std::fs::symlink_metadata(directory)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(corrupt("return backup directory is not an owned directory"));
        }
    }
    let manifest = backup.join(MANIFEST_FILE);
    let metadata = std::fs::symlink_metadata(&manifest)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(corrupt("return backup manifest is not a regular file"));
    }
    serde_json::from_slice(&std::fs::read(manifest)?)
        .map_err(|_| corrupt("return backup manifest is unreadable"))
}

/// Refuse a plan whose identity or paths do not belong to this exact return operation.
pub(crate) fn validate_plan_identity(
    home: &Path,
    staged_home: &Path,
    run_id: Uuid,
    plan: &InstallPlan,
) -> io::Result<()> {
    validate_plan_paths(home, plan)?;
    if plan.run_id != run_id || plan.staged_home.as_path() != staged_home {
        return Err(corrupt("install plan does not match the return operation"));
    }
    Ok(())
}

fn validate_plan_paths(home: &Path, plan: &InstallPlan) -> io::Result<()> {
    let expected_backup = home
        .join(BACKUPS_DIR)
        .join(format!("{}-before-return", plan.run_id));
    if plan.backup_dir != expected_backup {
        return Err(corrupt(
            "install plan backup is outside its owned return path",
        ));
    }

    let mut names = HashSet::new();
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
            return Err(corrupt("install plan contains an unsafe unit path"));
        }
    }
    Ok(())
}

/// Forget a plan whose swap never moved anything.
pub fn discard_plan(home: &Path) -> io::Result<()> {
    match std::fs::remove_file(plan_path(home)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Run or finish the swap. Safe to repeat.
pub fn install(home: &Path, plan: &InstallPlan) -> io::Result<()> {
    validate_plan_paths(home, plan)?;
    std::fs::create_dir_all(&plan.backup_dir)?;
    let manifest = plan.backup_dir.join(MANIFEST_FILE);
    if !manifest.exists() {
        std::fs::write(
            &manifest,
            serde_json::to_vec_pretty(plan).map_err(io::Error::other)?,
        )?;
    }
    // Everything that leaves the live home moves first, so no unit ever sits next to a stale
    // sidecar of the one it replaces.
    for unit in &plan.units {
        let live = home.join(&unit.name);
        let backup = plan.backup_dir.join(&unit.name);
        let staged = plan.staged_home.join(&unit.name);
        let pending = unit.backup_only || staged.exists();
        if !pending || !live.exists() {
            continue;
        }
        if backup.exists() {
            return Err(corrupt("a backup already exists beside a live file"));
        }
        if let Some(parent) = backup.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(&live, &backup)?;
    }
    for unit in plan.units.iter().filter(|unit| !unit.backup_only) {
        let live = home.join(&unit.name);
        let staged = plan.staged_home.join(&unit.name);
        if !staged.exists() {
            if live.exists() {
                continue;
            }
            return Err(corrupt(
                "a staged unit is missing and nothing was installed",
            ));
        }
        if let Some(expected) = &unit.staged_sha256
            && &sha256_file(&staged)? != expected
        {
            return Err(corrupt("a staged file changed after it was verified"));
        }
        std::fs::rename(&staged, &live)?;
    }
    Ok(())
}

/// Check the backup against the digests recorded before anything moved.
pub fn verify_backup(plan: &InstallPlan) -> io::Result<bool> {
    for unit in &plan.units {
        let backup = plan.backup_dir.join(&unit.name);
        if let Some(expected) = &unit.live_sha256
            && (!backup.exists() || &sha256_file(&backup)? != expected)
        {
            return Ok(false);
        }
        if let Some(expected) = unit.live_entries
            && (!backup.is_dir() || directory_totals(&backup)? != expected)
        {
            return Ok(false);
        }
    }
    Ok(true)
}
