use std::fs;
use std::fs::FileTimes;
use std::sync::Arc;
use std::time::Duration;
use std::time::UNIX_EPOCH;

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
use crate::local::test_support::test_config;
use crate::local::test_support::write_archived_session_file;
use crate::local::test_support::write_session_file;
use crate::local::test_support::write_session_file_with_history_mode;

#[test]
fn move_journal_rejects_direction_that_disagrees_with_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(521);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T16-00-06", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));

    let result = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Unarchive,
        &destination,
        &[(source.clone(), destination.clone())],
    );

    assert!(result.is_err());
    assert!(source.exists());
    assert!(!destination.exists());
    assert!(!home.path().join("rollout_move_transactions").exists());
    Ok(())
}

#[test]
fn move_journal_rejects_a_record_it_cannot_replay() -> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(522);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T16-00-07", uuid)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("filename"));
    let moves = vec![(source.clone(), destination.clone()); 8_000];

    let result = begin_move(
        home.path(),
        thread_id,
        MoveDirection::Archive,
        &destination,
        &moves,
    );

    assert!(result.is_err());
    assert!(source.exists());
    assert!(!destination.exists());
    assert!(!home.path().join("rollout_move_transactions").exists());
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
    move_planned_pair(&pending, &source, home.path())?;
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
            .join(format!("{thread_id}.seed"))
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
    move_planned_pair(&pending, &first, home.path())?;
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
            .join(format!("{thread_id}.seed"))
            .exists()
    );
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn non_utf8_home_survives_archive_and_unarchive_replay()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::ffi::OsStringExt;

    let outer = tempfile::tempdir()?;
    let initial_home = outer.path().join("codex-utf8");
    fs::create_dir(&initial_home)?;
    let uuid = Uuid::from_u128(532);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    // The fixture serializes cwd into session JSON, which requires UTF-8. Move the
    // already valid rollout into a native-path home before exercising recovery.
    let initial_source = write_session_file(&initial_home, "2025-01-03T20-00-03", uuid)?;
    let home = outer
        .path()
        .join(std::ffi::OsString::from_vec(b"codex-\xff".to_vec()));
    fs::rename(&initial_home, &home)?;
    let source = home.join(initial_source.strip_prefix(&initial_home)?);
    let store = LocalThreadStore::new(test_config(&home), /*state_db*/ None);
    let archive = home.join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("rollout filename"));
    let journal = home
        .join("rollout_move_transactions")
        .join(format!("{thread_id}.seed"));

    let pending = begin_move(
        &home,
        thread_id,
        MoveDirection::Archive,
        &destination,
        &[(source.clone(), destination.clone())],
    )?;
    assert!(fs::read_to_string(&journal)?.contains("unixHex"));
    pending.move_all(&home)?;
    let mut sidecar = destination.as_os_str().to_owned();
    sidecar.push(".codex-move-intent");
    assert!(fs::read_to_string(sidecar)?.contains("unixHex"));
    drop(pending);

    assert_eq!(
        replay_pending_move(&store, thread_id).await?,
        Some(MoveDirection::Archive)
    );
    assert!(!source.exists());
    assert!(destination.exists());
    assert!(!journal.exists());

    let pending = begin_move(
        &home,
        thread_id,
        MoveDirection::Unarchive,
        &source,
        &[(destination.clone(), source.clone())],
    )?;
    pending.move_all(&home)?;
    drop(pending);
    assert_eq!(
        replay_pending_move(&store, thread_id).await?,
        Some(MoveDirection::Unarchive)
    );
    assert!(source.exists());
    assert!(!destination.exists());
    assert!(!journal.exists());
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
    move_planned_pair(&pending, &first, home.path())?;
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
            .join(format!("{thread_id}.seed"))
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
            .join(format!("{thread_id}.seed"))
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
            .join(format!("{thread_id}.seed"))
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
    move_planned_pair(&pending, &source, home.path())?;
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
            .join(format!("{thread_id}.seed"))
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
    move_planned_pair(&pending, &source, home.path())?;
    runtime
        .mark_archived(thread_id, &destination, Utc::now())
        .await?;
    let before = runtime.get_thread(thread_id).await?.expect("SQLite row");
    // Exercise compatibility with an existing committed legacy JSON journal.
    let legacy = super::transaction_path(home.path(), thread_id);
    let mut bytes = serde_json::to_vec(&pending.transaction)?;
    bytes.extend_from_slice(b"\ncommitted\n");
    fs::write(&legacy, bytes)?;
    fs::remove_file(&pending.path)?;
    fs::File::open(&legacy)?.sync_all()?;
    drop(pending); // Simulate exit after SQLite commit and journal marker, before cleanup.

    replay_pending_move(&store, thread_id).await?;

    let after = runtime.get_thread(thread_id).await?.expect("SQLite row");
    assert_eq!(after, before);
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
    move_planned_pair(&pending, &source, home.path())?;
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
async fn replay_after_unarchive_commit_preserves_rollout_mtime()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let config = test_config(home.path());
    let uuid = Uuid::from_u128(523);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_archived_session_file(home.path(), "2025-01-03T13-00-01", uuid)?;
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
    move_planned_pair(&pending, &source, home.path())?;
    runtime.mark_unarchived(thread_id, &destination).await?;
    fs::OpenOptions::new()
        .write(true)
        .open(&destination)?
        .set_times(
            FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(1_000_000_000)),
        )?;
    let before_mtime = fs::metadata(&destination)?.modified()?;
    let before = runtime.get_thread(thread_id).await?.expect("SQLite row");
    drop(pending); // Simulate exit after SQLite commit but before journal cleanup.

    assert_eq!(
        replay_pending_move(&store, thread_id).await?,
        Some(MoveDirection::Unarchive)
    );

    assert_eq!(fs::metadata(&destination)?.modified()?, before_mtime);
    assert_eq!(
        runtime.get_thread(thread_id).await?.expect("SQLite row"),
        before
    );
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
    move_planned_pair(&pending, &source, home.path())?;
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
            .join(format!("{thread_id}.seed"))
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
    move_planned_pair(&pending, &source, home.path())?;
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
    move_planned_pair(&pending, &source, home.path())?;
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
    move_planned_pair(&pending, &source, home.path())?;
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
    move_planned_pair(&pending, &source, home.path())?;
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
    move_planned_pair(&pending, &first, home.path())?;
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
            .join(format!("{thread_id}.seed"))
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
    move_planned_pair(&pending, &source, home.path())?;
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
            .join(format!("{thread_id}.seed"))
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
            .join(format!("{thread_id}.seed"))
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
            .join(format!("{thread_id}.seed"))
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
    move_planned_pair(&pending, &source, home.path())?;
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
    move_planned_pair(&pending, &source, home.path())?;
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
    move_planned_pair(&pending, &source, home.path())?;
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

#[tokio::test]
async fn incomplete_seed_blocks_public_read_without_moving_source()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(790);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let original = fs::read(&source)?;
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let claim = super::seed::create(home.path(), thread_id, MoveDirection::Archive)?;
    drop(claim);
    let path = super::seed::claim_path(home.path(), thread_id);
    let before = fs::read(&path)?;
    assert!(store.read_thread(ReadThreadParams {
        thread_id, include_archived: true, include_history: false,
    }).await.is_err());
    assert_eq!(fs::read(&source)?, original);
    assert_eq!(fs::read(&path)?, before);
    Ok(())
}

#[test]
fn legacy_and_seed_claims_refuse_replay_without_removing_either() -> std::io::Result<()> {
    let home = tempfile::tempdir()?;
    let thread_id = ThreadId::new();
    let claim = super::seed::create(home.path(), thread_id, MoveDirection::Archive)?;
    drop(claim);
    let legacy = super::transaction_path(home.path(), thread_id);
    fs::write(&legacy, b"legacy authority")?;
    let seed = super::seed::claim_path(home.path(), thread_id);
    let before = fs::read(&seed)?;
    assert!(super::load_move(home.path(), thread_id).is_err());
    assert_eq!(fs::read(&legacy)?, b"legacy authority");
    assert_eq!(fs::read(seed)?, before);
    Ok(())
}

#[tokio::test]
async fn replay_rejects_same_inode_valid_plan_rewrite_before_moving()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(791);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let original = fs::read(&source)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("rollout filename"));
    let pending = begin_move(home.path(), thread_id, MoveDirection::Archive,
        &destination, &[(source.clone(), destination.clone())])?;
    let path = pending.path.clone();
    let identity = super::rollout_file_identity(&path)?;
    let mut changed = serde_json::to_value(&pending.transaction)?;
    changed["moves"][0]["source_digest"][0] = serde_json::json!(255);
    // Ensure the replacement is different even if the actual first digest byte was255.
    if serde_json::to_value(&pending.transaction)? == changed {
        changed["moves"][0]["source_digest"][0] = serde_json::json!(0);
    }
    let plan = serde_json::to_vec(&changed)?;
    let bytes = fs::read(&path)?;
    let mut frame = Vec::new();
    let mut lines = bytes.split(|byte| *byte == b'\n');
    frame.extend_from_slice(lines.next().expect("seed magic"));
    frame.push(b'\n');
    let mut header: serde_json::Value = serde_json::from_slice(lines.next().expect("seed header"))?;
    header["plan"] = changed;
    let header = serde_json::to_vec(&header)?;
    frame.extend_from_slice(&header);
    frame.push(b'\n');
    frame.extend_from_slice(blake3::hash(&header).to_hex().as_bytes());
    frame.push(b'\n');
    frame.extend_from_slice(&plan);
    frame.push(b'\n');
    frame.extend_from_slice(blake3::hash(&plan).to_hex().as_bytes());
    frame.extend_from_slice(b"\nCOMPLETE\n");
    drop(pending);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let result = super::replay_pending_move_before_mutation(&store, thread_id, || {
        fs::write(&path, &frame)?;
        assert_eq!(super::rollout_file_identity(&path)?, identity);
        assert!(matches!(super::seed::read(home.path(), thread_id)?,
            super::seed::SeedRecord::Prepared(_)));
        Ok(())
    }).await;
    assert!(result.is_err());
    assert_eq!(fs::read(source)?, original);
    assert!(!destination.exists());
    assert_eq!(fs::read(path)?, frame);
    Ok(())
}

#[tokio::test]
async fn public_read_recovers_partial_frame_from_original_durable_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(792);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let original = fs::read(&source)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("rollout filename"));
    let pending = begin_move(home.path(), thread_id, MoveDirection::Archive,
        &destination, &[(source.clone(), destination.clone())])?;
    let path = pending.path.clone();
    let bytes = fs::read(&path)?;
    let header_end = bytes.iter().enumerate().filter(|(_, b)| **b == b'\n')
        .nth(2).expect("complete durable header").0 + 1;
    fs::write(&path, &bytes[..header_end + 1])?; // Interrupted first plan byte, same inode.
    drop(pending);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread = store.read_thread(ReadThreadParams {
        thread_id, include_archived: true, include_history: false,
    }).await?;
    assert_eq!(thread.rollout_path, Some(destination.clone()));
    assert_eq!(fs::read(destination)?, original);
    assert!(!source.exists());
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn public_read_rejects_header_missing_final_delimiter_without_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(793);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let original = fs::read(&source)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("rollout filename"));
    let pending = begin_move(home.path(), thread_id, MoveDirection::Archive,
        &destination, &[(source.clone(), destination.clone())])?;
    let path = pending.path.clone();
    let bytes = fs::read(&path)?;
    let header_end = bytes.iter().enumerate().filter(|(_, b)| **b == b'\n')
        .nth(2).expect("complete durable header").0;
    let incomplete = &bytes[..header_end];
    fs::write(&path, incomplete)?;
    drop(pending);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    assert!(store.read_thread(ReadThreadParams {
        thread_id, include_archived: true, include_history: false,
    }).await.is_err());
    assert_eq!(fs::read(source)?, original);
    assert!(!destination.exists());
    assert_eq!(fs::read(path)?, incomplete);
    Ok(())
}

fn move_planned_pair(pending: &super::PendingMove, source: &std::path::Path, home: &std::path::Path) -> std::io::Result<()> {
    let pair = pending.transaction.moves.iter().find(|pair| pair.source == source)
        .ok_or_else(|| std::io::Error::other("fixture source absent from actual plan"))?;
    pending.move_pair(pair, home)
}

#[tokio::test]
async fn replay_reuses_durably_claimed_resources_before_receipt_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(794);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let original = fs::read(&source)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("rollout filename"));
    let pending = begin_move(home.path(), thread_id, MoveDirection::Archive,
        &destination, &[(source.clone(), destination.clone())])?;
    assert!(pending.transaction.moves[0].preparation.is_some());
    let alias = fs::read_dir(&archive)?.map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?.into_iter()
        .find(|path| path.to_string_lossy().ends_with(".move-intent")).expect("owned empty alias");
    assert_eq!(fs::read(&alias)?, Vec::<u8>::new());
    let identities = fs::read_dir(&archive)?.map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?.into_iter()
        .map(|path| super::rollout_file_identity(&path)).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(identities.len(), 2);
    drop(pending); // Durable complete plan, but no canonical per-file receipt yet.
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread = store.read_thread(ReadThreadParams {
        thread_id, include_archived: true, include_history: false,
    }).await?;
    assert_eq!(thread.rollout_path, Some(destination.clone()));
    assert_eq!(fs::read(destination)?, original);
    assert!(!source.exists());
    assert!(!alias.exists());
    assert_eq!(fs::read_dir(archive)?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn replaced_prepared_alias_refuses_replay_without_source_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(795);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let original = fs::read(&source)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("rollout filename"));
    let pending = begin_move(home.path(), thread_id, MoveDirection::Archive,
        &destination, &[(source.clone(), destination.clone())])?;
    let alias = fs::read_dir(&archive)?.map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?.into_iter()
        .find(|path| path.to_string_lossy().ends_with(".move-intent")).expect("owned empty alias");
    let preserved = alias.with_extension("preserved");
    fs::rename(&alias, &preserved)?;
    fs::write(&alias, b"unrelated replacement")?;
    let seed = fs::read(&pending.path)?;
    let path = pending.path.clone();
    drop(pending);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    assert!(store.read_thread(ReadThreadParams {
        thread_id, include_archived: true, include_history: false,
    }).await.is_err());
    assert_eq!(fs::read(source)?, original);
    assert_eq!(fs::read(alias)?, b"unrelated replacement");
    assert_eq!(fs::read(preserved)?, Vec::<u8>::new());
    assert_eq!(fs::read(path)?, seed);
    assert!(!destination.exists());
    Ok(())
}

#[tokio::test]
async fn unmoved_collision_reclaims_only_durably_owned_empty_resources()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(796);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let original = fs::read(&source)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("rollout filename"));
    let pending = begin_move(home.path(), thread_id, MoveDirection::Archive,
        &destination, &[(source.clone(), destination.clone())])?;
    fs::write(&destination, b"unrelated collision")?;
    let path = pending.path.clone();
    drop(pending);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    assert_eq!(replay_pending_move(&store, thread_id).await?, None);
    assert_eq!(fs::read(&source)?, original);
    assert_eq!(fs::read(&destination)?, b"unrelated collision");
    assert!(!path.exists());
    assert_eq!(fs::read_dir(archive)?.map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?, vec![destination]);
    assert_eq!(fs::read_dir(source.parent().expect("source parent"))?
        .map(|entry| entry.map(|entry| entry.path())).collect::<Result<Vec<_>, _>>()?, vec![source]);
    Ok(())
}

#[tokio::test]
async fn public_replay_finishes_only_owned_partial_receipt_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let home = tempfile::tempdir()?;
    let uuid = Uuid::from_u128(797);
    let thread_id = ThreadId::from_string(&uuid.to_string())?;
    let source = write_session_file(home.path(), "2025-01-03T17-00-00", uuid)?;
    let original = fs::read(&source)?;
    let archive = home.path().join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
    fs::create_dir(&archive)?;
    let destination = archive.join(source.file_name().expect("rollout filename"));
    let pending = begin_move(home.path(), thread_id, MoveDirection::Archive,
        &destination, &[(source.clone(), destination.clone())])?;
    let intent = pending.transaction.moves[0].preparation.as_ref().expect("typed preparation");
    let expected = serde_json::to_vec(intent)?;
    let alias = fs::read_dir(&archive)?.map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?.into_iter()
        .find(|path| path.to_string_lossy().ends_with(".move-intent")).expect("owned receipt alias");
    let identity = super::rollout_file_identity(&alias)?;
    fs::write(&alias, &expected[..expected.len() / 2])?;
    assert_eq!(super::rollout_file_identity(&alias)?, identity);
    drop(pending);
    let store = LocalThreadStore::new(test_config(home.path()), /*state_db*/ None);
    let thread = store.read_thread(ReadThreadParams {
        thread_id, include_archived: true, include_history: false,
    }).await?;
    assert_eq!(thread.rollout_path, Some(destination.clone()));
    assert_eq!(fs::read(destination)?, original);
    assert!(!source.exists());
    assert!(!alias.exists());
    Ok(())
}
