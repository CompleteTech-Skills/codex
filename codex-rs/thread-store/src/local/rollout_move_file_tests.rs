use pretty_assertions::assert_eq;

use super::*;

pub(in crate::local) fn move_rollout_noclobber(
    source: &Path,
    destination: &Path,
    codex_home: &Path,
) -> io::Result<()> {
    move_rollout_noclobber_retained(source, destination, codex_home)?;
    clear_rollout_move_intent(destination)
}

pub(in crate::local) fn move_rollout_noclobber_retained(
    source: &Path,
    destination: &Path,
    codex_home: &Path,
) -> io::Result<()> {
    move_rollout_with_before_quarantine(source, destination, codex_home, || Ok(()))
}

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
        sync_parent_directory,
    )
}

#[test]
fn bound_move_rejects_same_bytes_replacement_during_staging() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let original = sessions.join("original.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"original contents")?;
    let binding = SourceBinding::Journaled {
        identity: rollout_file_identity(&source)?,
        digest: rollout_file_digest(&source)?,
    };

    let error = move_rollout_with_hooks(
        &source,
        &destination,
        home.path(),
        binding,
        || {
            std::fs::rename(&source, &original)?;
            std::fs::copy(&original, &source)?;
            Ok(())
        },
        || Ok(()),
        sync_parent_directory,
    )
    .expect_err("replacement must be rejected before publication");

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(std::fs::read(&source)?, std::fs::read(&original)?);
    assert!(!destination.exists());
    assert!(!rollout_move_intent_path(&destination).exists());
    Ok(())
}

#[test]
fn bound_move_rejects_same_inode_digest_change_during_staging() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"original contents")?;
    let modified = std::fs::metadata(&source)?.modified()?;
    let identity = rollout_file_identity(&source)?;
    let binding = SourceBinding::Journaled {
        identity,
        digest: rollout_file_digest(&source)?,
    };

    let error = move_rollout_with_hooks(
        &source,
        &destination,
        home.path(),
        binding,
        || {
            std::fs::write(&source, b"modified contents")?;
            std::fs::OpenOptions::new()
                .write(true)
                .open(&source)?
                .set_times(FileTimes::new().set_modified(modified))
        },
        || Ok(()),
        sync_parent_directory,
    )
    .expect_err("content change must be rejected before publication");

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(rollout_file_identity(&source)?, identity);
    assert_eq!(std::fs::read(&source)?, b"modified contents");
    assert!(!destination.exists());
    assert!(!rollout_move_intent_path(&destination).exists());
    Ok(())
}

fn prepare_move_intent(source: &Path, destination: &Path) -> io::Result<RolloutMoveIntent> {
    let parent = destination
        .parent()
        .ok_or_else(|| io::Error::other("no parent"))?;
    let mut stage = tempfile::Builder::new()
        .prefix(".codex-rollout-stage-")
        .tempfile_in(parent)?;
    io::copy(&mut std::fs::File::open(source)?, &mut stage)?;
    stage.as_file().sync_all()?;
    let stage_id = rollout_file_identity(stage.path())?;
    let stage_digest = rollout_file_digest(stage.path())?;
    let (_file, stage_path) = stage.keep().map_err(|err| err.error)?;
    let source_metadata = std::fs::metadata(source)?;
    let quarantine_dir = tempfile::Builder::new()
        .prefix(".codex-rollout-quarantine-")
        .tempdir_in(
            source
                .parent()
                .ok_or_else(|| io::Error::other("no source parent"))?,
        )?
        .keep();
    let intent = RolloutMoveIntent {
        source: std::fs::canonicalize(source)?,
        destination: destination.to_path_buf(),
        source_len: source_metadata.len(),
        source_modified: source_metadata.modified()?,
        source_id: rollout_file_identity(source)?,
        source_digest: rollout_file_digest(source)?,
        stage_path,
        stage_id,
        stage_digest,
        quarantine_path: quarantine_dir.join("quarantined-source"),
        quarantine_id: None,
        receipt_publication: None,
    };
    write_rollout_move_intent(&rollout_move_intent_path(destination), &intent, sync_parent_directory)?;
    Ok(intent)
}

#[test]
fn staging_preserves_source_permissions() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout")?;
    let mut permissions = std::fs::metadata(&source)?.permissions();
    permissions.set_readonly(true);
    std::fs::set_permissions(&source, permissions)?;

    move_rollout_noclobber(&source, &destination, home.path())?;
    assert!(std::fs::metadata(&destination)?.permissions().readonly());
    #[cfg(windows)]
    {
        let mut permissions = std::fs::metadata(&destination)?.permissions();
        permissions.set_readonly(false);
        std::fs::set_permissions(&destination, permissions)?;
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn staging_preserves_source_mode() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout")?;
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o640))?;

    move_rollout_noclobber(&source, &destination, home.path())?;
    assert_eq!(
        std::fs::metadata(&destination)?.permissions().mode() & 0o777,
        0o640
    );
    Ok(())
}

#[test]
fn quarantine_name_cannot_be_discovered_as_a_rollout() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source =
        sessions.join("rollout-2026-01-01T00-00-00-00000000-0000-0000-0000-000000000001.jsonl");
    let destination = archived.join(source.file_name().expect("rollout filename"));
    std::fs::write(&source, b"rollout")?;
    let intent = prepare_move_intent(&source, &destination)?;
    assert_eq!(
        codex_rollout::rollout_id_from_path(&intent.quarantine_path),
        None
    );
    Ok(())
}

#[test]
fn oversized_intent_is_rejected_before_publication() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout")?;
    let mut intent = prepare_move_intent(&source, &destination)?;
    let intent_path = rollout_move_intent_path(&destination);
    std::fs::remove_file(&intent_path)?;
    intent.source = intent.source.join("x".repeat(4096));
    let error = write_rollout_move_intent(&intent_path, &intent, sync_parent_directory)
        .expect_err("receipt too large for recovery must not be published");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(!intent_path.exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn encoded_native_path_cannot_exceed_receipt_limit() -> io::Result<()> {
    use std::os::unix::ffi::OsStringExt;

    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout")?;
    let mut intent = prepare_move_intent(&source, &destination)?;
    let intent_path = rollout_move_intent_path(&destination);
    std::fs::remove_file(&intent_path)?;
    intent.source = std::ffi::OsString::from_vec(vec![0xff; 2048]).into();

    let error = write_rollout_move_intent(&intent_path, &intent, sync_parent_directory)
        .expect_err("encoded receipt too large for recovery must not be published");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(!intent_path.exists());
    Ok(())
}

#[test]
fn retry_completes_a_published_staged_move() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;

    // Fault point: the process exits after publication and before source unlink.
    let intent = prepare_move_intent(&source, &destination)?;
    tempfile::TempPath::try_from_path(intent.stage_path)?.persist_noclobber(&destination)?;
    move_rollout_noclobber(&source, &destination, home.path())?;

    assert!(!source.exists());
    assert_eq!(std::fs::read(&destination)?, b"rollout contents");
    assert!(!rollout_move_intent_path(&destination).exists());
    Ok(())
}

#[test]
fn retry_publishes_a_prepared_stage_after_crash() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;

    move_rollout_noclobber(&source, &destination, home.path())?;

    assert!(!source.exists());
    assert_eq!(std::fs::read(&destination)?, b"rollout contents");
    assert!(!intent.stage_path.exists());
    assert!(!rollout_move_intent_path(&destination).exists());
    Ok(())
}

#[test]
fn retry_resumes_an_intact_partial_stage_after_crash() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    // Fault point: the durable receipt exists, but only a prefix was copied.
    OpenOptions::new()
        .write(true)
        .open(&intent.stage_path)?
        .set_len(7)?;

    move_rollout_noclobber(&source, &destination, home.path())?;

    assert!(!source.exists());
    assert_eq!(std::fs::read(&destination)?, b"rollout contents");
    assert!(!intent.stage_path.exists());
    assert!(!rollout_move_intent_path(&destination).exists());
    Ok(())
}

#[test]
fn retry_rejects_a_changed_partial_stage_without_deleting_source() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    // A file at the recorded pathname is not authority to overwrite its contents.
    std::fs::write(&intent.stage_path, b"replaced prefix")?;

    let error = move_rollout_noclobber_retained(&source, &destination, home.path())
        .expect_err("changed stage must stop recovery");

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(std::fs::read(&source)?, b"rollout contents");
    assert!(!destination.exists());
    assert!(rollout_move_intent_path(&destination).exists());
    assert!(clear_rollout_move_intent(&destination).is_err());
    assert!(intent.stage_path.exists());
    Ok(())
}

#[test]
fn abandoned_partial_stage_is_cleared_only_with_its_receipt() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    OpenOptions::new()
        .write(true)
        .open(&intent.stage_path)?
        .set_len(7)?;

    clear_rollout_move_intent(&destination)?;

    assert_eq!(std::fs::read(&source)?, b"rollout contents");
    assert!(!intent.stage_path.exists());
    assert!(!rollout_move_intent_path(&destination).exists());
    Ok(())
}

#[test]
fn cleanup_refuses_stage_and_quarantined_source_together() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    OpenOptions::new()
        .write(true)
        .open(&intent.stage_path)?
        .set_len(7)?;
    std::fs::rename(&source, &intent.quarantine_path)?;

    assert!(clear_rollout_move_intent(&destination).is_err());

    assert_eq!(std::fs::read(&intent.quarantine_path)?, b"rollout contents");
    assert!(intent.stage_path.exists());
    assert!(rollout_move_intent_path(&destination).exists());
    Ok(())
}

#[test]
fn cleanup_finishes_a_published_stage_hard_link() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    // Fault point: no-clobber publication linked the stage but has not removed its name.
    std::fs::hard_link(&intent.stage_path, &destination)?;

    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    clear_rollout_move_intent(&destination)?;

    assert!(!source.exists());
    assert_eq!(std::fs::read(&destination)?, b"rollout contents");
    assert!(!intent.stage_path.exists());
    Ok(())
}

#[test]
fn retry_preserves_source_if_published_bytes_change_in_place() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    tempfile::TempPath::try_from_path(intent.stage_path)?.persist_noclobber(&destination)?;
    let published_id = rollout_file_identity(&destination)?;
    std::fs::write(&destination, b"changed in place")?;

    let err = move_rollout_noclobber(&source, &destination, home.path())
        .expect_err("modified published bytes must not consume source");

    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(rollout_file_identity(&destination)?, published_id);
    assert_eq!(std::fs::read(&source)?, b"rollout contents");
    Ok(())
}

#[test]
fn retry_rejects_competing_hard_link_to_source() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    std::fs::hard_link(&source, &destination)?;

    let error = move_rollout_noclobber(&source, &destination, home.path())
        .expect_err("a hard link to the source is not the staged file");

    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert!(source.exists());
    assert!(destination.exists());
    assert!(intent.stage_path.exists());
    assert!(rollout_move_intent_path(&destination).exists());
    Ok(())
}

#[test]
fn retry_preserves_an_unrelated_destination() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    std::fs::hard_link(&source, &destination)?;

    let error = move_rollout_noclobber(&source, &destination, home.path())
        .expect_err("unrelated destination must not be removed");

    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&source)?, b"rollout contents");
    assert_eq!(std::fs::read(&destination)?, b"rollout contents");
    Ok(())
}

#[test]
fn retry_preserves_ambiguous_copy_after_intent() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    std::fs::write(&destination, b"partial copy")?;

    let error = move_rollout_noclobber(&source, &destination, home.path())
        .expect_err("a copy with a different identity cannot be reconciled");

    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&source)?, b"rollout contents");
    assert_eq!(std::fs::read(&destination)?, b"partial copy");
    assert!(rollout_move_intent_path(&destination).exists());
    assert!(intent.stage_path.exists());
    Ok(())
}

#[test]
fn published_receipt_verifies_destination_after_source_unlink() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;

    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    verify_published_rollout_move(&source, &destination, home.path())?;

    std::fs::rename(&destination, archived.join("original.jsonl"))?;
    std::fs::write(&destination, b"replacement")?;
    assert!(verify_published_rollout_move(&source, &destination, home.path()).is_err());
    Ok(())
}

#[test]
fn retry_finishes_source_quarantined_before_process_exit() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;
    let intent = prepare_move_intent(&source, &destination)?;
    tempfile::TempPath::try_from_path(&intent.stage_path)?.persist_noclobber(&destination)?;
    std::fs::rename(&source, &intent.quarantine_path)?;

    verify_published_rollout_move(&source, &destination, home.path())?;

    assert!(!source.exists());
    assert!(!intent.quarantine_path.exists());
    assert!(!quarantine_directory(&intent)?.exists());
    assert_eq!(std::fs::read(destination)?, b"rollout contents");
    Ok(())
}

#[test]
fn same_length_same_mtime_rewrite_before_quarantine_is_preserved() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"original contents")?;
    let modified = std::fs::metadata(&source)?.modified()?;

    let error = move_rollout_with_before_quarantine(&source, &destination, home.path(), || {
        std::fs::write(&source, b"modified contents")?;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&source)?
            .set_times(FileTimes::new().set_modified(modified))
    })
    .expect_err("source rewrite must fail after publication");

    let intent = read_rollout_move_intent(&rollout_move_intent_path(&destination))?;
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(std::fs::read(&destination)?, b"original contents");
    assert_eq!(
        std::fs::read(&intent.quarantine_path)?,
        b"modified contents"
    );
    assert!(!source.exists());
    assert!(verify_published_rollout_move(&source, &destination, home.path()).is_err());
    Ok(())
}

#[test]
fn pathname_replacement_before_quarantine_is_preserved() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let original = sessions.join("preserved-original.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"original contents")?;

    let error = move_rollout_with_before_quarantine(&source, &destination, home.path(), || {
        std::fs::rename(&source, &original)?;
        std::fs::write(&source, b"replacement contents")
    })
    .expect_err("replacement pathname must fail after publication");

    let intent = read_rollout_move_intent(&rollout_move_intent_path(&destination))?;
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(std::fs::read(&original)?, b"original contents");
    assert_eq!(std::fs::read(&destination)?, b"original contents");
    assert_eq!(
        std::fs::read(&intent.quarantine_path)?,
        b"replacement contents"
    );
    assert!(!source.exists());
    assert!(verify_published_rollout_move(&source, &destination, home.path()).is_err());
    Ok(())
}

#[test]
fn retry_preserves_stage_after_published_intent_sync_fails() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let sessions = home.path().join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir(&sessions)?;
    std::fs::create_dir(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;

    let error = move_rollout_noclobber_retained_with_intent_sync(
        &source,
        &destination,
        home.path(),
        |path| {
            assert!(path.exists(), "the intent must already be published");
            Err(io::Error::other("injected intent directory sync failure"))
        },
    )
    .expect_err("failed intent sync must be reported");
    assert_eq!(error.to_string(), "injected intent directory sync failure");
    let intent = read_rollout_move_intent(&rollout_move_intent_path(&destination))?;
    assert_eq!(std::fs::read(&source)?, b"rollout contents");
    // The newer149 protocol records an empty stage before copying rollout bytes.
    assert_eq!(std::fs::read(&intent.stage_path)?, Vec::<u8>::new());
    assert!(!destination.exists());

    move_rollout_noclobber(&source, &destination, home.path())?;
    assert!(!source.exists());
    assert!(!intent.stage_path.exists());
    assert!(!rollout_move_intent_path(&destination).exists());
    assert_eq!(std::fs::read(&destination)?, b"rollout contents");
    Ok(())
}

#[test]
fn oversized_intent_preserves_source_before_publication() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let mut home = root.path().to_path_buf();
    for _ in 0..10 {
        home.push("p".repeat(/*n*/ 140));
    }
    let sessions = home.join(codex_rollout::SESSIONS_SUBDIR);
    let archived = home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    std::fs::create_dir_all(&sessions)?;
    std::fs::create_dir_all(&archived)?;
    let source = sessions.join("rollout.jsonl");
    let destination = archived.join("rollout.jsonl");
    std::fs::write(&source, b"rollout contents")?;

    let error = move_rollout_noclobber_retained(&source, &destination, &home)
        .expect_err("oversized receipt must fail before source unlink");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(std::fs::read(&source)?, b"rollout contents");
    assert!(!destination.exists());
    assert!(!rollout_move_intent_path(&destination).exists());
    Ok(())
}

fn move_rollout_noclobber_retained_with_intent_sync(
    source: &Path,
    destination: &Path,
    home: &Path,
    sync: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    move_rollout_with_hooks(
        source, destination, home, SourceBinding::Unbound, || Ok(()), || Ok(()), sync,
    )
}
