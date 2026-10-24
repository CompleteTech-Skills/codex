use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use pretty_assertions::assert_eq;
use uuid::Uuid;

use super::MoveDirection;
use super::begin_move;
use super::replay_pending_move;
use crate::ArchiveThreadParams;
use crate::DeleteThreadParams;
use crate::ForkBoundary;
use crate::PrepareForkParams;
use crate::ReadThreadByRolloutPathParams;
use crate::ReadThreadParams;
use crate::ResumeThreadParams;
use crate::RevertThreadParams;
use crate::ThreadPersistenceMetadata;
use crate::ThreadStore;
use crate::local::LocalThreadStore;
use crate::local::rollout_move_file::move_rollout_noclobber_retained;
use crate::local::test_support::test_config;
use crate::local::test_support::write_archived_session_file;
use crate::local::test_support::write_session_file;
use crate::local::test_support::write_session_file_with_history_mode;

fn write_legacy_journal(
    home: &Path,
    thread_id: ThreadId,
    direction: MoveDirection,
    selected_destination: &Path,
    moves: &[(PathBuf, PathBuf)],
) -> Result<(), Box<dyn std::error::Error>> {
    let pairs = moves
        .iter()
        .map(|(source, destination)| {
            serde_json::json!({
                "source": source,
                "destination": destination,
            })
        })
        .collect::<Vec<_>>();
    let journal = serde_json::json!({
        "thread_id": thread_id,
        "direction": direction,
        "selected_destination": selected_destination,
        "moves": pairs,
    });
    fs::write(
        home.join("rollout_move_transactions")
            .join(format!("{thread_id}.json")),
        format!("{journal}\n"),
    )?;
    Ok(())
}

#[tokio::test]
async fn legacy_journal_replays_partially_moved_rollouts() -> Result<(), Box<dyn std::error::Error>>
{
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(530);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let first = write_session_file(home.path(), "2025-01-03T20-00-00", uuid)?;
    let second = write_session_file(home.path(), "2025-01-03T20-00-01", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let first_destination = archive.join(first.file_name().expect("first filename"));
    let second_destination = archive.join(second.file_name().expect("second filename"));
    let moves = vec![
        (first.clone(), first_destination.clone()),
        (second.clone(), second_destination.clone()),
    ];
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &first_destination,
        &moves,
    )?;
    move_rollout_noclobber_retained(&first, &first_destination, home.path())?;
    drop(pending);
    write_legacy_journal(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &first_destination,
        &moves,
    )?;

    assert_eq!(
        replay_pending_move(&store, thread_id).await?,
        Some(MoveDirection::Archive)
    );
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    assert!(!first.exists());
    assert!(!second.exists());
    assert!(first_destination.exists());
    assert!(second_destination.exists());
    Ok(())
}

#[tokio::test]
async fn legacy_journal_rejects_modified_published_destination()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(531);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T20-00-02", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let moves = vec![(source.clone(), destination.clone())];
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &moves,
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending);
    write_legacy_journal(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &moves,
    )?;
    fs::write(&destination, b"unrelated content")?;

    assert!(replay_pending_move(&store, thread_id).await.is_err());
    assert_eq!(fs::read(&destination)?, b"unrelated content");
    assert!(!source.exists());
    assert!(
        home.path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn destination_only_replay_rejects_in_place_modification()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(519);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T16-00-04", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    let published_id = crate::local::rollout_move_identity::rollout_file_identity(&destination)?;
    fs::write(&destination, b"changed in place")?;
    drop(pending);

    assert!(replay_pending_move(&store, thread_id).await.is_err());

    assert_eq!(
        crate::local::rollout_move_identity::rollout_file_identity(&destination)?,
        published_id
    );
    assert!(!source.exists());
    assert!(
        home.path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn replay_finishes_partially_moved_archive_rollouts() -> Result<(), Box<dyn std::error::Error>>
{
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(510);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let first = write_session_file(home.path(), "2025-01-03T12-00-00", uuid)?;
    let second = write_session_file(home.path(), "2025-01-03T12-00-01", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let first_destination = archive.join(first.file_name().expect("first filename"));
    let second_destination = archive.join(second.file_name().expect("second filename"));
    let moves = vec![
        (first.clone(), first_destination.clone()),
        (second.clone(), second_destination.clone()),
    ];
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &first_destination,
        &moves,
    )?;
    move_rollout_noclobber_retained(&first, &first_destination, home.path())?;
    drop(pending); // Simulate process exit before the second move and SQLite update.

    replay_pending_move(&store, thread_id).await?;
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);

    assert!(!first.exists());
    assert!(!second.exists());
    assert!(first_destination.exists());
    assert!(second_destination.exists());
    assert!(
        !home
            .path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn replay_rejects_replaced_source_before_any_partial_move()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(526);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let first = write_session_file(home.path(), "2025-01-03T19-00-00", uuid)?;
    let second = write_session_file(home.path(), "2025-01-03T19-00-01", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let first_destination = archive.join(first.file_name().expect("first filename"));
    let second_destination = archive.join(second.file_name().expect("second filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &first_destination,
        &[
            (first.clone(), first_destination.clone()),
            (second.clone(), second_destination.clone()),
        ],
    )?;
    move_rollout_noclobber_retained(&first, &first_destination, home.path())?;
    let replacement = second.with_extension("replacement");
    fs::rename(&second, &replacement)?;
    fs::copy(&replacement, &second)?;
    drop(pending); // Crash before the second file's sidecar was written.

    assert!(replay_pending_move(&store, thread_id).await.is_err());
    assert_eq!(fs::read(&second)?, fs::read(&replacement)?);
    assert!(!second_destination.exists());
    assert!(first_destination.exists());
    assert!(
        home.path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[test]
fn live_move_rejects_replacement_after_journal_creation() -> Result<(), Box<dyn std::error::Error>>
{
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(527);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T19-00-02", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    let original = source.with_extension("original");
    fs::rename(&source, &original)?;
    fs::copy(&original, &source)?;

    assert!(pending.move_all(home.path()).is_err());
    assert_eq!(fs::read(&source)?, fs::read(&original)?);
    assert!(!destination.exists());
    assert!(
        home.path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[test]
fn live_move_prevalidates_later_source_before_publishing_first()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(528);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let first = write_session_file(home.path(), "2025-01-03T19-00-03", uuid)?;
    let second = write_session_file(home.path(), "2025-01-03T19-00-04", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let first_destination = archive.join(first.file_name().expect("first filename"));
    let second_destination = archive.join(second.file_name().expect("second filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &first_destination,
        &[
            (first.clone(), first_destination.clone()),
            (second.clone(), second_destination.clone()),
        ],
    )?;
    let original_second = second.with_extension("original");
    fs::rename(&second, &original_second)?;
    fs::copy(&original_second, &second)?;

    assert!(pending.move_all(home.path()).is_err());
    assert!(first.exists());
    assert!(second.exists());
    assert!(!first_destination.exists());
    assert!(!second_destination.exists());
    assert!(
        home.path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn replay_finishes_destination_only_unarchive() -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(511);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_archived_session_file(home.path(), "2025-01-03T13-00-00", uuid)?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    metadata.archived_at = Some(metadata.updated_at);
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let destination_directory = home.path().join("sessions/2025/01/03");
    fs::create_dir_all(&destination_directory)?;
    let destination = destination_directory.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Unarchive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending); // Simulate process exit before SQLite update.

    let restored = replay_pending_move(&store, thread_id).await?;
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);

    assert_eq!(restored, Some(MoveDirection::Unarchive));
    let updated = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(updated.rollout_path, destination.clone());
    assert_eq!(updated.archived_at, None);
    assert!(!source.exists());
    assert!(destination.exists());
    assert!(
        !home
            .path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn committed_archive_cleanup_preserves_sqlite_row() -> Result<(), Box<dyn std::error::Error>>
{
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(512);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T14-00-00", uuid)?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    runtime
        .mark_archived(thread_id, &destination, Utc::now())
        .await?;
    let before = runtime.get_thread(thread_id).await?.expect("SQLite row");
    let mut journal = fs::OpenOptions::new().append(true).open(&pending.path)?;
    journal.write_all(b"committed\n")?;
    journal.sync_all()?;
    drop(pending); // Simulate exit after SQLite commit and journal marker, before cleanup.

    replay_pending_move(&store, thread_id).await?;

    let after = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(after.rollout_path, before.rollout_path);
    assert_eq!(after.archived_at, before.archived_at);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    assert!(
        !destination
            .with_file_name(format!(
                "{}.codex-move-intent",
                destination.file_name().expect("filename").to_string_lossy()
            ))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn replay_after_sqlite_commit_preserves_updated_at() -> Result<(), Box<dyn std::error::Error>>
{
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(514);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T14-00-01", uuid)?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    runtime
        .mark_archived(thread_id, &destination, Utc::now())
        .await?;
    let before = runtime.get_thread(thread_id).await?.expect("SQLite row");
    drop(pending); // Simulate exit after SQLite commit but before journal marker.

    replay_pending_move(&store, thread_id).await?;

    let after = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(after, before);
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}

#[tokio::test]
async fn replay_rejects_replaced_destination_without_clearing_journal()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(513);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T15-00-00", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    fs::rename(&destination, archive.join("preserved-original.jsonl"))?;
    fs::write(
        &destination,
        fs::read(archive.join("preserved-original.jsonl"))?,
    )?;
    drop(pending);

    assert!(replay_pending_move(&store, thread_id).await.is_err());
    assert!(archive.join("preserved-original.jsonl").exists());
    assert!(
        home.path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}
#[tokio::test]
async fn read_replays_pending_archive_before_using_sqlite_path()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(515);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T16-00-00", uuid)?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending);

    let thread = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: true,
            include_history: false,
        })
        .await?;

    assert_eq!(thread.rollout_path, Some(destination.clone()));
    let updated = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(updated.rollout_path, destination);
    assert!(updated.archived_at.is_some());
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}

#[tokio::test]
async fn resume_replays_move_before_rejecting_old_explicit_path()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(516);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T16-00-01", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending);

    assert!(
        store
            .resume_thread(ResumeThreadParams {
                thread_id,
                rollout_path: Some(source.clone()),
                history: Some(Arc::new(Vec::new())),
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(home.path().to_path_buf()),
                    model_provider: "test-provider".into(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await
            .is_err()
    );

    assert!(!source.exists());
    assert!(destination.exists());
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}

#[tokio::test]
async fn delete_replays_pending_move_before_reference_scan()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(517);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T16-00-02", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending);

    store
        .delete_thread(DeleteThreadParams { thread_id })
        .await?;

    assert!(!source.exists());
    assert!(!destination.exists());
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}

#[tokio::test]
async fn revert_replays_pending_archive_before_resolving_source()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(518);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T16-00-03", uuid)?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending);

    assert!(
        store
            .revert_thread(RevertThreadParams {
                thread_id,
                before_turn_id: "turn-1".to_string(),
                multi_agent_version: None,
            })
            .await
            .is_err()
    );

    assert!(!source.exists());
    assert!(destination.exists());
    assert_eq!(
        runtime
            .get_thread(thread_id)
            .await?
            .expect("row")
            .rollout_path,
        destination
    );
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}

#[tokio::test]
async fn archive_replays_destination_only_and_partially_moved_rollouts()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(510);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let first = write_session_file(home.path(), "2025-01-03T12-00-00", uuid)?;
    let second = write_session_file(home.path(), "2025-01-03T12-00-01", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let first_destination = archive.join(first.file_name().expect("first filename"));
    let second_destination = archive.join(second.file_name().expect("second filename"));
    let moves = vec![
        (first.clone(), first_destination.clone()),
        (second.clone(), second_destination.clone()),
    ];
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &first_destination,
        &moves,
    )?;
    move_rollout_noclobber_retained(&first, &first_destination, home.path())?;
    drop(pending); // Simulate process exit before the second move and SQLite update.

    store
        .archive_thread(ArchiveThreadParams { thread_id })
        .await?;
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);

    assert!(!first.exists());
    assert!(!second.exists());
    assert!(first_destination.exists());
    assert!(second_destination.exists());
    assert!(
        !home
            .path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn unarchive_replays_destination_only_before_archived_lookup()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(511);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_archived_session_file(home.path(), "2025-01-03T13-00-00", uuid)?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    metadata.archived_at = Some(metadata.updated_at);
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let destination_directory = home.path().join("sessions/2025/01/03");
    fs::create_dir_all(&destination_directory)?;
    let destination = destination_directory.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Unarchive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending); // Simulate process exit before SQLite update.

    let restored = store
        .unarchive_thread(ArchiveThreadParams { thread_id })
        .await?;
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);

    assert_eq!(restored.rollout_path, Some(destination.clone()));
    let updated = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(updated.rollout_path, destination.clone());
    assert_eq!(updated.archived_at, None);
    assert!(!source.exists());
    assert!(destination.exists());
    assert!(
        !home
            .path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn archive_collision_preserves_normal_reads_without_a_journal()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(520);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    fs::write(&destination, b"unrelated destination")?;

    assert!(
        store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await
            .is_err()
    );
    let thread = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await?;

    assert_eq!(thread.rollout_path, Some(source.clone()));
    assert!(source.exists());
    assert_eq!(fs::read(destination)?, b"unrelated destination");
    assert!(
        !home
            .path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn collision_after_journal_publication_abandons_unmoved_intent()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(521);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-01", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    fs::write(&destination, b"unrelated destination")?;
    drop(pending);

    let thread = store
        .read_thread(ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        })
        .await?;

    assert_eq!(thread.rollout_path, Some(source.clone()));
    assert!(source.exists());
    assert_eq!(fs::read(destination)?, b"unrelated destination");
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}

#[tokio::test]
async fn unarchive_collision_leaves_archived_source_readable()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let uuid = Uuid::from_u128(522);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_archived_session_file(home.path(), "2025-01-03T17-00-02", uuid)?;
    let active = home.path().join("sessions/2025/01/03");
    fs::create_dir_all(&active)?;
    let destination = active.join(source.file_name().expect("filename"));
    fs::write(&destination, b"unrelated destination")?;

    assert!(
        store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .is_err()
    );
    let thread = store
        .read_thread_by_rollout_path(
            source.clone(),
            /*include_archived*/ true,
            /*include_history*/ false,
        )
        .await?;

    assert_eq!(thread.rollout_path, Some(source.clone()));
    assert!(source.exists());
    assert_eq!(fs::read(destination)?, b"unrelated destination");
    assert!(
        !home
            .path()
            .join("rollout_move_transactions")
            .join(format!("{thread_id}.json"))
            .exists()
    );
    Ok(())
}

#[tokio::test]
async fn direct_path_read_replays_before_resolving_missing_source()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(523);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T18-00-00", uuid)?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending);

    assert!(
        store
            .read_thread_by_rollout_path(
                source, /*include_archived*/ true, /*include_history*/ false
            )
            .await
            .is_err()
    );
    let thread = store
        .read_thread_by_rollout_path(
            destination.clone(),
            /*include_archived*/ true,
            /*include_history*/ false,
        )
        .await?;
    let updated = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(thread.rollout_path, Some(destination.clone()));
    assert_eq!(updated.rollout_path, destination);
    assert!(updated.archived_at.is_some());
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}

#[tokio::test]
async fn trait_path_read_replays_reverted_rollout_thread_id()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(524);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let original = write_session_file(home.path(), "2025-01-03T18-00-01", uuid)?;
    let source = original.with_file_name(format!(
        "{}_{}.jsonl",
        original.file_stem().expect("stem").to_string_lossy(),
        Uuid::from_u128(1524)
    ));
    fs::rename(&original, &source)?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending);

    let thread = ThreadStore::read_thread_by_rollout_path(
        &store,
        ReadThreadByRolloutPathParams {
            rollout_path: destination.clone(),
            include_archived: true,
            include_history: false,
        },
    )
    .await?;
    let updated = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(thread.rollout_path, Some(destination.clone()));
    assert_eq!(updated.rollout_path, destination);
    assert!(updated.archived_at.is_some());
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}

#[tokio::test]
async fn paginated_fork_replays_before_lineage_discovery() -> Result<(), Box<dyn std::error::Error>>
{
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(525);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file_with_history_mode(
        home.path(),
        "2025-01-03T18-00-02",
        uuid,
        ThreadHistoryMode::Paginated,
    )?;
    let runtime = codex_state::StateRuntime::init(
        config.sqlite.clone(),
        config.default_model_provider_id.clone(),
    )
    .await?;
    let mut metadata = codex_state::ThreadMetadataBuilder::new(
        thread_id,
        source.clone(),
        Utc::now(),
        SessionSource::Cli,
    )
    .build(config.default_model_provider_id.as_str());
    metadata.history_mode = ThreadHistoryMode::Paginated;
    runtime.upsert_thread(&metadata).await?;
    let store = LocalThreadStore::new(config, Some(runtime.clone()));
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let pending = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    move_rollout_noclobber_retained(&source, &destination, home.path())?;
    drop(pending);

    let fork_result = store
        .prepare_fork(PrepareForkParams {
            thread_id,
            boundary: ForkBoundary::Latest,
        })
        .await?;
    assert_eq!(fork_result.source_thread_id, thread_id);
    let updated = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(updated.rollout_path, destination);
    assert!(updated.archived_at.is_some());
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    Ok(())
}
