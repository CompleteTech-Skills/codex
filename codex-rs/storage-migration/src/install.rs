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

/// The planner installs database files, never staged WAL/SHM files. Refuse a
/// stage that may still depend on them, including orphaned sidecars. This is a
/// filesystem precondition, not a checkpoint or database-closure receipt.
pub(crate) fn validate_staged_sqlite_sidecars(staged_home: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(staged_home)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| corrupt("staged filename is not Unicode"))?;
        if name.ends_with(".sqlite-wal") || name.ends_with(".sqlite-shm") {
            return Err(corrupt(
                "staged SQLite sidecars require checkpoint and closure before installation",
            ));
        }
        if name.ends_with(".sqlite") && !entry.file_type()?.is_file() {
            return Err(corrupt("staged SQLite database is not a regular file"));
        }
    }
    Ok(())
}

/// Decide what the swap will do and record it. Fails if a plan already exists.
pub fn plan_install(home: &Path, staged_home: &Path, run_id: Uuid) -> io::Result<InstallPlan> {
    validate_staged_sqlite_sidecars(staged_home)?;
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
    validate_staged_sqlite_sidecars(&plan.staged_home)?;
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

#[cfg(test)]
mod staged_sidecar_tests {
    use super::*;

    #[test]
    fn late_staged_wal_refuses_before_backup_and_can_retry_the_same_plan() {
        let home = tempfile::tempdir().expect("isolated live home");
        let stage = tempfile::tempdir().expect("isolated stage");
        let live = home.path().join("memories_v2_1.sqlite");
        let staged = stage.path().join("memories_v2_1.sqlite");
        let wal = stage.path().join("memories_v2_1.sqlite-wal");
        std::fs::write(&live, b"original live database").unwrap();
        std::fs::write(&staged, b"verified staged database").unwrap();
        let plan = plan_install(home.path(), stage.path(), Uuid::new_v4()).unwrap();
        std::fs::write(&wal, b"late staged sidecar").unwrap();
        assert_eq!(
            install(home.path(), &plan).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(!plan.backup_dir.exists());
        assert_eq!(read_plan(home.path()).unwrap(), Some(plan.clone()));
        assert_eq!(std::fs::read(&live).unwrap(), b"original live database");
        assert_eq!(std::fs::read(&staged).unwrap(), b"verified staged database");
        assert_eq!(std::fs::read(&wal).unwrap(), b"late staged sidecar");

        // Fixture bytes are not a real WAL. Removing this fixture obstruction
        // only tests same-plan retry; production must checkpoint, never delete WAL.
        std::fs::remove_file(&wal).unwrap();
        install(home.path(), &plan).unwrap();
        install(home.path(), &plan).unwrap();
        assert_eq!(std::fs::read(&live).unwrap(), b"verified staged database");
        assert!(verify_backup(&plan).unwrap());
    }

    #[test]
    fn staged_sidecar_refusal_preserves_files_and_creates_no_plan() {
        for sidecar in ["memories_v2_1.sqlite-wal", "memories_v2_1.sqlite-shm"] {
            for staged_database_present in [false, true] {
                let home = tempfile::tempdir().expect("isolated live home");
                let stage = tempfile::tempdir().expect("isolated stage");
                let live = home.path().join("memories_v2_1.sqlite");
                std::fs::write(&live, b"original live database").expect("live fixture");
                if staged_database_present {
                    std::fs::write(
                        stage.path().join("memories_v2_1.sqlite"),
                        b"staged database",
                    )
                    .expect("staged fixture");
                }
                let wal = stage.path().join(sidecar);
                std::fs::write(&wal, b"retained staged sidecar").expect("sidecar fixture");
                let error = plan_install(home.path(), stage.path(), Uuid::new_v4())
                    .expect_err("present and orphaned staged sidecars must be refused");
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                assert!(!plan_path(home.path()).exists());
                assert_eq!(std::fs::read(&live).unwrap(), b"original live database");
                assert_eq!(std::fs::read(&wal).unwrap(), b"retained staged sidecar");
            }
        }
    }

    #[test]
    fn live_sidecar_is_preserved_in_backup_plan_for_self_contained_stage() {
        let home = tempfile::tempdir().expect("isolated live home");
        let stage = tempfile::tempdir().expect("isolated stage");
        std::fs::write(stage.path().join("state_5.sqlite"), b"staged database").unwrap();
        std::fs::write(home.path().join("state_5.sqlite-wal"), b"old live WAL").unwrap();
        let plan = plan_install(home.path(), stage.path(), Uuid::new_v4()).unwrap();
        assert!(
            plan.units
                .iter()
                .any(|unit| unit.name == "state_5.sqlite-wal" && unit.backup_only)
        );
        assert!(
            plan.units
                .iter()
                .any(|unit| unit.name == "state_5.sqlite" && !unit.backup_only)
        );
        assert_eq!(
            std::fs::read(home.path().join("state_5.sqlite-wal")).unwrap(),
            b"old live WAL"
        );
    }
}
