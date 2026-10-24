use pretty_assertions::assert_eq;

use super::*;
use crate::local::rollout_move_file::clear_rollout_move_intent;
use crate::local::rollout_move_file::read_rollout_move_intent;
use crate::local::rollout_move_file::rollout_move_intent_path;
use crate::local::rollout_move_identity::rollout_file_digest;

fn fixture() -> io::Result<(tempfile::TempDir, RolloutMoveIntent)> {
    let home = tempfile::tempdir()?;
    let root = std::fs::canonicalize(home.path())?;
    let source = root.join("source");
    std::fs::write(&source, b"original rollout")?;
    let stage = tempfile::Builder::new().prefix(".codex-rollout-stage-").tempfile_in(&root)?;
    let (file, stage_path) = stage.keep().map_err(|error| error.error)?;
    drop(file);
    let quarantine = tempfile::Builder::new().prefix(".codex-rollout-quarantine-").tempdir_in(&root)?.keep();
    let metadata = std::fs::metadata(&source)?;
    let intent = RolloutMoveIntent {
        source_id: rollout_file_identity(&source)?,
        source_digest: rollout_file_digest(&source)?,
        source_len: metadata.len(),
        source_modified: metadata.modified()?,
        source,
        destination: root.join("destination"),
        stage_id: rollout_file_identity(&stage_path)?,
        stage_digest: rollout_file_digest(&stage_path)?,
        stage_path,
        quarantine_path: quarantine.join("quarantined-source"),
        quarantine_id: None,
        receipt_publication: None,
    };
    Ok((home, intent))
}

fn publish_with_alias(intent: &RolloutMoveIntent) -> io::Result<PathBuf> {
    let canonical = rollout_move_intent_path(&intent.destination);
    // This is the actual dependency fallback's interrupted link/unlink boundary.
    write_with_publication(&canonical, intent, |alias, receipt| std::fs::hard_link(alias, receipt))?;
    Ok(canonical)
}

#[test]
fn restart_cleanup_removes_recorded_alias_before_receipt_and_stage() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = publish_with_alias(&intent)?;
    let bytes = std::fs::read(&canonical)?;
    assert_eq!(std::fs::read(staging_path(&intent.stage_path))?, bytes);
    // Only on-disk state is used by the public cleanup entry point after publication.
    clear_rollout_move_intent(&intent.destination)?;
    assert_eq!(std::fs::read(&intent.source)?, b"original rollout");
    assert_eq!((canonical.exists(), staging_path(&intent.stage_path).exists(), intent.stage_path.exists()), (false, false, false));
    Ok(())
}

#[test]
fn publication_error_after_link_retains_resources_for_retry() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = rollout_move_intent_path(&intent.destination);
    let failure = write_with_publication(&canonical, &intent, |alias, receipt| {
        std::fs::hard_link(alias, receipt)?;
        Err(io::Error::other("interrupted after publishing receipt"))
    });
    assert!(failure.is_err());
    assert_eq!((canonical.exists(), staging_path(&intent.stage_path).exists(), intent.stage_path.exists()), (true, true, true));
    clear_rollout_move_intent(&intent.destination)?;
    assert_eq!(std::fs::read(&intent.source)?, b"original rollout");
    Ok(())
}

#[test]
fn alias_directory_sync_failure_retains_canonical_authority_and_stage() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = publish_with_alias(&intent)?;
    let recorded = read_rollout_move_intent(&canonical)?;
    assert!(cleanup_with_sync(&canonical, &recorded, |_| Err(io::Error::other("directory sync failed"))).is_err());
    assert_eq!((canonical.exists(), staging_path(&intent.stage_path).exists(), intent.stage_path.exists()), (true, false, true));
    clear_rollout_move_intent(&intent.destination)?;
    assert!(!canonical.exists());
    Ok(())
}

#[test]
fn replaced_alias_with_equal_contents_is_preserved_without_partial_cleanup() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = publish_with_alias(&intent)?;
    let alias = staging_path(&intent.stage_path);
    let bytes = std::fs::read(&alias)?;
    std::fs::remove_file(&alias)?;
    std::fs::write(&alias, &bytes)?;
    assert!(clear_rollout_move_intent(&intent.destination).is_err());
    assert_eq!(std::fs::read(&alias)?, bytes);
    assert_eq!((canonical.exists(), intent.stage_path.exists(), intent.quarantine_path.parent().unwrap().exists()), (true, true, true));
    Ok(())
}

#[test]
fn copied_canonical_receipt_is_rejected_even_when_alias_is_absent() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = publish_with_alias(&intent)?;
    let bytes = std::fs::read(&canonical)?;
    std::fs::remove_file(staging_path(&intent.stage_path))?;
    let replacement = canonical.with_extension("replacement");
    std::fs::write(&replacement, &bytes)?;
    std::fs::remove_file(&canonical)?;
    std::fs::rename(&replacement, &canonical)?;
    assert!(clear_rollout_move_intent(&intent.destination).is_err());
    assert_eq!(std::fs::read(&canonical)?, bytes);
    assert!(intent.stage_path.exists());
    Ok(())
}

#[test]
fn unrelated_alias_path_cannot_authorize_cleanup() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = publish_with_alias(&intent)?;
    let unrelated = canonical.with_extension("unrelated");
    std::fs::hard_link(&canonical, &unrelated)?;
    let mut recorded = read_rollout_move_intent(&canonical)?;
    recorded.receipt_publication.as_mut().unwrap().path = unrelated.clone();
    let mut bytes = serde_json::to_vec(&recorded).map_err(io::Error::other)?;
    bytes.push(b'\n');
    std::fs::write(&canonical, &bytes)?;
    assert!(clear_rollout_move_intent(&intent.destination).is_err());
    assert_eq!(std::fs::read(&unrelated)?, bytes);
    assert!(intent.stage_path.exists());
    Ok(())
}

#[test]
fn legacy_receipt_retains_unrecorded_alias() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = rollout_move_intent_path(&intent.destination);
    let mut bytes = serde_json::to_vec(&intent).map_err(io::Error::other)?;
    bytes.push(b'\n');
    std::fs::write(&canonical, &bytes)?;
    let unrecorded = staging_path(&intent.stage_path);
    std::fs::hard_link(&canonical, &unrecorded)?;
    clear_rollout_move_intent(&intent.destination)?;
    assert_eq!(std::fs::read(&unrecorded)?, bytes);
    assert!(!canonical.exists());
    Ok(())
}

#[test]
fn malformed_partial_publication_record_is_rejected() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = rollout_move_intent_path(&intent.destination);
    let mut value = serde_json::to_value(&intent).map_err(io::Error::other)?;
    value["receipt_publication"] = serde_json::json!({"path": "missing-identity"});
    let mut bytes = serde_json::to_vec(&value).map_err(io::Error::other)?;
    bytes.push(b'\n');
    std::fs::write(&canonical, &bytes)?;
    assert!(clear_rollout_move_intent(&intent.destination).is_err());
    assert_eq!(std::fs::read(&canonical)?, bytes);
    assert!(intent.stage_path.exists());
    Ok(())
}

#[test]
fn oversized_receipt_cannot_publish_or_discard_its_source() -> io::Result<()> {
    let (_home, mut intent) = fixture()?;
    let source = intent.source.clone();
    intent.quarantine_path = intent.quarantine_path.parent().unwrap().join("x".repeat(/*n*/ 4096));
    let canonical = rollout_move_intent_path(&intent.destination);
    assert!(write(&canonical, &intent).is_err());
    assert_eq!(std::fs::read(&source)?, b"original rollout");
    assert_eq!(std::fs::read(staging_path(&intent.stage_path))?, Vec::<u8>::new());
    assert!(!canonical.exists());
    Ok(())
}

#[test]
fn in_place_receipt_rewrite_cannot_authorize_stale_cleanup() -> io::Result<()> {
    let (_home, intent) = fixture()?;
    let canonical = publish_with_alias(&intent)?;
    let original = read_rollout_move_intent(&canonical)?;
    let mut rewritten = original.clone();
    rewritten.source_digest = [42; 32];
    let mut bytes = serde_json::to_vec(&rewritten).map_err(io::Error::other)?;
    bytes.push(b'\n');
    // Both names still identify the same inode and contain the same updated record.
    std::fs::write(&canonical, &bytes)?;
    let alias = staging_path(&intent.stage_path);
    assert_eq!(std::fs::read(&alias)?, bytes);
    assert!(cleanup(&canonical, &original).is_err());
    assert_eq!((canonical.exists(), alias.exists(), intent.stage_path.exists()), (true, true, true));
    assert_eq!(std::fs::read(&intent.source)?, b"original rollout");
    Ok(())
}
