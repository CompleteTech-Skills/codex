//! Framed claim integrity tests; public recovery is exercised in transaction tests.
use pretty_assertions::assert_eq;
use super::*;

fn plan(thread_id: ThreadId) -> io::Result<Vec<u8>> {
    serde_json::to_vec(&serde_json::json!({
        "thread_id": thread_id,
        "direction": MoveDirection::Archive,
        "moves": [],
        "selected_destination": "fixture-path"
    })).map_err(io::Error::other)
}

#[test]
fn complete_plan_round_trips_exact_bytes() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let thread_id = ThreadId::new();
    let mut claim = create(home.path(), thread_id, MoveDirection::Archive)?;
    assert!(matches!(read(home.path(), thread_id)?, SeedRecord::Preparing));
    let bytes = plan(thread_id)?;
    claim.complete(&bytes)?;
    drop(claim);
    let SeedRecord::Prepared(actual) = read(home.path(), thread_id)? else {
        return Err(io::Error::other("complete frame was not readable"));
    };
    assert_eq!(actual, bytes);
    Ok(())
}

#[test]
fn partial_plan_remains_blocked_without_becoming_a_completed_move() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let thread_id = ThreadId::new();
    let claim = create(home.path(), thread_id, MoveDirection::Archive)?;
    let path = claim.path.clone();
    drop(claim);
    let mut file = OpenOptions::new().append(true).open(&path)?;
    file.write_all(b"{\"thread_id\":")?;
    file.sync_all()?;
    drop(file);
    let before = std::fs::read(&path)?;
    assert!(matches!(read(home.path(), thread_id)?, SeedRecord::Preparing));
    assert_eq!(std::fs::read(&path)?, before);
    Ok(())
}

#[test]
fn copied_canonical_seed_cannot_claim_original_file_identity() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let thread_id = ThreadId::new();
    let claim = create(home.path(), thread_id, MoveDirection::Archive)?;
    let path = claim.path.clone();
    drop(claim);
    let copied = path.with_extension("copied");
    std::fs::copy(&path, &copied)?;
    std::fs::remove_file(&path)?;
    std::fs::rename(&copied, &path)?;
    let before = std::fs::read(&path)?;
    assert!(read(home.path(), thread_id).is_err());
    assert_eq!(std::fs::read(&path)?, before);
    Ok(())
}

#[test]
fn different_operation_cannot_complete_claim_or_overwrite_existing_seed() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let thread_id = ThreadId::new();
    let mut claim = create(home.path(), thread_id, MoveDirection::Archive)?;
    assert!(create(home.path(), thread_id, MoveDirection::Archive).is_err());
    assert!(claim.complete(&plan(ThreadId::new())?).is_err());
    assert!(matches!(read(home.path(), thread_id)?, SeedRecord::Preparing));
    Ok(())
}

#[test]
fn modified_plan_digest_is_rejected_without_mutating_claim() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let thread_id = ThreadId::new();
    let mut claim = create(home.path(), thread_id, MoveDirection::Archive)?;
    claim.complete(&plan(thread_id)?)?;
    let path = claim.path.clone();
    let header_len = claim.header_len as usize;
    drop(claim);
    let mut bytes = std::fs::read(&path)?;
    bytes[header_len] = b'[';
    std::fs::write(&path, &bytes)?;
    assert!(read(home.path(), thread_id).is_err());
    assert_eq!(std::fs::read(&path)?, bytes);
    Ok(())
}

#[test]
fn oversized_plan_leaves_seed_uncompleted() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let thread_id = ThreadId::new();
    let mut claim = create(home.path(), thread_id, MoveDirection::Archive)?;
    let mut value: serde_json::Value = serde_json::from_slice(&plan(thread_id)?).map_err(io::Error::other)?;
    value["padding"] = serde_json::json!("x".repeat(/*n*/ MAX_PLAN_BYTES));
    let bytes = serde_json::to_vec(&value).map_err(io::Error::other)?;
    assert!(claim.complete(&bytes).is_err());
    assert!(matches!(read(home.path(), thread_id)?, SeedRecord::Preparing));
    Ok(())
}

#[test]
fn same_length_header_rewrite_cannot_complete_seed_or_authorize_source_move() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let source = home.path().join("rollout-source");
    std::fs::write(&source, b"untouched rollout")?;
    let thread_id = ThreadId::new();
    let mut claim = create(home.path(), thread_id, MoveDirection::Archive)?;
    let mut rewritten = std::fs::read(&claim.path)?;
    rewritten[0] = b'X';
    std::fs::write(&claim.path, &rewritten)?;
    assert!(claim.complete(&plan(thread_id)?).is_err());
    assert_eq!(std::fs::read(&claim.path)?, rewritten);
    assert_eq!(std::fs::read(&source)?, b"untouched rollout");
    assert!(read(home.path(), thread_id).is_err());
    Ok(())
}

#[test]
fn preparing_recovery_refuses_new_caller_revision_and_restores_original_plan() -> io::Result<()> {
    let home = tempfile::tempdir()?;
    let thread_id = ThreadId::new();
    let original = plan(thread_id)?;
    let mut claim = create_with_plan(home.path(), thread_id, MoveDirection::Archive, &original)?;
    let mut changed: serde_json::Value = serde_json::from_slice(&original).map_err(io::Error::other)?;
    changed["selected_destination"] = serde_json::json!("replacement-path");
    assert!(claim.complete(&serde_json::to_vec(&changed).map_err(io::Error::other)?).is_err());
    drop(claim);
    assert_eq!(recover_preparing(home.path(), thread_id)?, original);
    let SeedRecord::Prepared(actual) = read(home.path(), thread_id)? else {
        return Err(io::Error::other("preparation was not completed"));
    };
    assert_eq!(actual, original);
    Ok(())
}
