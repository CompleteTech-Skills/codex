//! Failure injection for handing authority back to local files.

use super::tests::metadata;
use super::tests::reset_target;
use super::*;
use crate::Cutover;
use crate::CutoverError;
use crate::InstallPlan;
use crate::RecoveryOutcome;
use crate::ReturnCutover;
use crate::install;
use crate::verify_backup;
use chrono::Utc;
use codex_postgres_queue_store::PostgresQueueStore;
use codex_postgres_thread_catalog::PostgresThreadCatalog;
use codex_protocol::protocol::SessionSource;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_state::ThreadMetadata;
use codex_storage_authority::ActiveBackend;
use codex_storage_authority::adopt_quiesced_home;
use codex_storage_authority::load_authority;
use codex_storage_authority::read_cutover;
use codex_thread_store::QueueStore;
use codex_utils_absolute_path::test_support::PathExt;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

const CONTROL_FILES: [&str; 4] = [
    "storage-identity.json",
    "storage-activation.json",
    "storage-cutover.json",
    "storage-install.json",
];

/// Put the home and the store in the state a finished forward cutover leaves them in.
async fn remote_authority(pool: &Arc<PostgresPool>, source: &SqliteSource, home: &Path) {
    reset_target(pool).await;
    for file in CONTROL_FILES {
        let _ = std::fs::remove_file(home.join(file));
    }
    adopt_quiesced_home(home).expect("adopt");
    let migrator = Migrator::new(source.clone(), pool.clone());
    let summary = migrator.import().await.expect("import");
    migrator.verify(summary.run_id).await.expect("verify");
    Cutover::new(
        home.to_path_buf(),
        Migrator::new(source.clone(), pool.clone()),
    )
    .execute(summary.run_id)
    .await
    .expect("forward cutover");
}

/// Export the dataset into a staged home inside the live home, verified unless asked not to.
async fn staged_return(
    pool: &Arc<PostgresPool>,
    home: &Path,
    name: &str,
    verify: bool,
) -> (ReturnCutover, Uuid, PathBuf) {
    let staged_home = home.join("storage-staging").join(name);
    std::fs::create_dir_all(&staged_home).expect("staging directory");
    let target = SqliteTarget::create(
        SqliteConfig::new_for_testing(staged_home.abs()),
        home.to_path_buf(),
        "migration-provider",
    )
    .await
    .expect("staged databases");
    let staged_source =
        SqliteSource::new(target.staging().clone()).relocated_from(home.to_path_buf());
    let exporter = Migrator::new(staged_source.clone(), pool.clone());
    let summary = exporter.export(&target).await.expect("export");
    if verify {
        exporter
            .verify(summary.run_id)
            .await
            .expect("verify the export");
    }
    target.close().await;
    (
        ReturnCutover::new(
            home.to_path_buf(),
            staged_home.clone(),
            Migrator::new(staged_source, pool.clone()),
        ),
        summary.run_id,
        staged_home,
    )
}

fn backend(home: &Path) -> ActiveBackend {
    load_authority(home)
        .expect("settled authority")
        .marker
        .active_backend
}

async fn dataset_state(pool: &Arc<PostgresPool>, source: &SqliteSource) -> ActivationState {
    Migrator::new(source.clone(), pool.clone())
        .activation_state()
        .await
        .expect("activation state")
}

async fn completed_finish_is_repeatable(
    pool: &Arc<PostgresPool>,
    source: &SqliteSource,
    home: &Path,
) {
    remote_authority(pool, source, home).await;
    let (returning, run_id, staged_home) = staged_return(pool, home, "repeat-finish", true).await;
    let intent = returning.prepare(run_id).expect("prepare return");
    let plan = install::read_plan(home)
        .expect("read plan")
        .expect("install plan exists");
    let plan_bytes = std::fs::read(home.join("storage-install.json")).expect("plan bytes");
    returning
        .publish(&intent)
        .await
        .expect("retire exact export");
    returning.install().expect("install exact plan");
    let manifest_path = plan.backup_dir.join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path).expect("installed backup manifest");
    let backup_unit = plan
        .units
        .iter()
        .find(|unit| unit.live_sha256.is_some())
        .expect("plan has a file backup");
    let backup_file = plan.backup_dir.join(&backup_unit.name);
    let backup_file_bytes = std::fs::read(&backup_file).expect("file backup bytes");
    let live_unit = plan
        .units
        .iter()
        .find(|unit| !unit.is_dir && unit.staged_sha256.is_some())
        .expect("plan has a staged file");
    let live_file = home.join(&live_unit.name);
    let live_file_bytes = std::fs::read(&live_file).expect("installed live file bytes");
    let identity_before_finish =
        std::fs::read(home.join("storage-identity.json")).expect("identity before finish");
    let activation_before_finish =
        std::fs::read(home.join("storage-activation.json")).expect("activation before finish");
    let cutover_before_finish =
        std::fs::read(home.join("storage-cutover.json")).expect("cutover before finish");

    std::fs::remove_file(&manifest_path).expect("remove manifest before finish");
    assert!(returning.finish(&intent).is_err());
    assert!(!manifest_path.exists());
    assert_eq!(
        read_cutover(home).expect("intent after missing manifest"),
        Some(intent.clone())
    );
    assert_eq!(
        install::read_plan(home).expect("plan after missing manifest"),
        Some(plan.clone())
    );
    assert_eq!(
        std::fs::read(home.join("storage-identity.json")).expect("identity after missing manifest"),
        identity_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-activation.json"))
            .expect("activation after missing manifest"),
        activation_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-cutover.json")).expect("cutover after missing manifest"),
        cutover_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-install.json")).expect("plan after missing manifest"),
        plan_bytes
    );
    assert_eq!(
        std::fs::read(&live_file).expect("live after missing manifest"),
        live_file_bytes
    );
    assert_eq!(
        std::fs::read(&backup_file).expect("backup after missing manifest"),
        backup_file_bytes
    );
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore manifest before finish");

    let mut corrupt_backup_bytes = backup_file_bytes.clone();
    corrupt_backup_bytes.push(b'!');
    std::fs::write(&backup_file, &corrupt_backup_bytes).expect("corrupt backup before finish");
    assert!(returning.finish(&intent).is_err());
    assert_eq!(
        read_cutover(home).expect("intent after corrupt backup"),
        Some(intent.clone())
    );
    assert_eq!(
        install::read_plan(home).expect("plan after corrupt backup"),
        Some(plan.clone())
    );
    assert_eq!(
        std::fs::read(home.join("storage-identity.json")).expect("identity after corrupt backup"),
        identity_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-activation.json"))
            .expect("activation after corrupt backup"),
        activation_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-cutover.json")).expect("cutover after corrupt backup"),
        cutover_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-install.json")).expect("plan after corrupt backup"),
        plan_bytes
    );
    assert_eq!(
        std::fs::read(&manifest_path).expect("manifest after corrupt backup"),
        manifest_bytes
    );
    assert_eq!(
        std::fs::read(&live_file).expect("live after corrupt backup"),
        live_file_bytes
    );
    assert_eq!(
        std::fs::read(&backup_file).expect("corrupt backup remains"),
        corrupt_backup_bytes
    );
    std::fs::write(&backup_file, &backup_file_bytes).expect("restore backup before finish");

    let first = returning.finish(&intent).expect("first finish");
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    assert!(verify_backup(&plan).expect("backup before replay"));
    let identity_before =
        std::fs::read(home.join("storage-identity.json")).expect("identity before repeated finish");
    let activation_after_finish =
        std::fs::read(home.join("storage-activation.json")).expect("activation after finish");
    let assert_replay_refused_without_mutation =
        |candidate: &ReturnCutover, expected_manifest: &[u8], expected_backup: &[u8]| {
            assert!(candidate.finish(&intent).is_err());
            assert_eq!(
                read_cutover(home).expect("intent after refused replay"),
                None
            );
            assert_eq!(
                install::read_plan(home).expect("plan after refused replay"),
                None
            );
            assert_eq!(
                std::fs::read(home.join("storage-identity.json"))
                    .expect("identity after refused replay"),
                identity_before
            );
            assert_eq!(
                std::fs::read(home.join("storage-activation.json"))
                    .expect("activation after refused replay"),
                activation_after_finish
            );
            assert_eq!(
                std::fs::read(&manifest_path).expect("manifest after refused replay"),
                expected_manifest
            );
            assert_eq!(
                std::fs::read(&live_file).expect("live after refused replay"),
                live_file_bytes
            );
            assert_eq!(
                std::fs::read(&backup_file).expect("backup after refused replay"),
                expected_backup
            );
        };
    assert_eq!(
        returning
            .finish(&intent)
            .expect("completed finish must remain safely repeatable"),
        first
    );
    assert_eq!(
        std::fs::read(home.join("storage-identity.json")).expect("identity after replay"),
        identity_before
    );

    let mut foreign = intent.clone();
    foreign.run_id = Uuid::new_v4();
    assert!(returning.finish(&foreign).is_err());
    let mut wrong_dataset = intent.clone();
    wrong_dataset.dataset_id = Uuid::new_v4();
    assert!(returning.finish(&wrong_dataset).is_err());
    let mut wrong_generation = intent.clone();
    wrong_generation.from_generation += 1;
    wrong_generation.to_generation += 1;
    assert!(returning.finish(&wrong_generation).is_err());

    std::fs::remove_file(&manifest_path).expect("simulate missing completion evidence");
    assert!(returning.finish(&intent).is_err());
    assert!(!manifest_path.exists());
    assert_eq!(
        read_cutover(home).expect("intent after missing replay manifest"),
        None
    );
    assert_eq!(
        install::read_plan(home).expect("plan after missing replay manifest"),
        None
    );
    assert_eq!(
        std::fs::read(home.join("storage-identity.json"))
            .expect("identity after missing replay manifest"),
        identity_before
    );
    assert_eq!(
        std::fs::read(home.join("storage-activation.json"))
            .expect("activation after missing replay manifest"),
        activation_after_finish
    );
    assert_eq!(
        std::fs::read(&live_file).expect("live after missing replay manifest"),
        live_file_bytes
    );
    assert_eq!(
        std::fs::read(&backup_file).expect("backup after missing replay manifest"),
        backup_file_bytes
    );
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore completion evidence");

    let wrong_staged_return = ReturnCutover::new(
        home.to_path_buf(),
        staged_home.join("wrong-staged-home"),
        Migrator::new(source.clone(), pool.clone()),
    );
    assert_replay_refused_without_mutation(
        &wrong_staged_return,
        &manifest_bytes,
        &backup_file_bytes,
    );

    let mut wrong_staged_manifest = plan.clone();
    wrong_staged_manifest.staged_home = staged_home.join("wrong-staged-home");
    let wrong_staged_bytes =
        serde_json::to_vec(&wrong_staged_manifest).expect("wrong staged manifest");
    std::fs::write(&manifest_path, &wrong_staged_bytes).expect("write wrong staged manifest");
    assert_replay_refused_without_mutation(&returning, &wrong_staged_bytes, &backup_file_bytes);
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore staged manifest");

    let mut wrong_backup_manifest = plan.clone();
    wrong_backup_manifest.backup_dir = home.join("wrong-backup");
    let wrong_backup_bytes =
        serde_json::to_vec(&wrong_backup_manifest).expect("wrong backup manifest");
    std::fs::write(&manifest_path, &wrong_backup_bytes).expect("write wrong backup manifest");
    assert_replay_refused_without_mutation(&returning, &wrong_backup_bytes, &backup_file_bytes);
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore backup manifest");

    let mut unsafe_unit_manifest = plan.clone();
    unsafe_unit_manifest.units[0].name = "../escape".to_string();
    let unsafe_unit_bytes =
        serde_json::to_vec(&unsafe_unit_manifest).expect("unsafe unit manifest");
    std::fs::write(&manifest_path, &unsafe_unit_bytes).expect("write unsafe unit manifest");
    assert_replay_refused_without_mutation(&returning, &unsafe_unit_bytes, &backup_file_bytes);
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore unit manifest");

    let mut duplicate_unit_manifest = plan.clone();
    let first_unit = duplicate_unit_manifest.units[0].clone();
    duplicate_unit_manifest.units.push(first_unit);
    let duplicate_unit_bytes =
        serde_json::to_vec(&duplicate_unit_manifest).expect("duplicate unit manifest");
    std::fs::write(&manifest_path, &duplicate_unit_bytes).expect("write duplicate unit manifest");
    assert_replay_refused_without_mutation(&returning, &duplicate_unit_bytes, &backup_file_bytes);
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore duplicate unit manifest");

    let mut corrupt_replay_backup = backup_file_bytes.clone();
    corrupt_replay_backup.push(b'!');
    std::fs::write(&backup_file, &corrupt_replay_backup).expect("corrupt backup before replay");
    assert_replay_refused_without_mutation(&returning, &manifest_bytes, &corrupt_replay_backup);
    std::fs::write(&backup_file, &backup_file_bytes).expect("restore backup before replay");

    let control_path = home.join("storage-install.json");
    let mut foreign_plan = plan.clone();
    foreign_plan.run_id = Uuid::new_v4();
    foreign_plan.backup_dir = plan
        .backup_dir
        .parent()
        .expect("backup root")
        .join(format!("{}-before-return", foreign_plan.run_id));
    let foreign_bytes = serde_json::to_vec(&foreign_plan).expect("foreign plan bytes");
    std::fs::write(&control_path, &foreign_bytes).expect("write foreign plan");
    assert!(returning.finish(&intent).is_err());
    assert_eq!(
        std::fs::read(&control_path).expect("foreign plan remains"),
        foreign_bytes
    );
    std::fs::write(&control_path, &plan_bytes).expect("restore exact plan");
    assert_eq!(
        returning
            .finish(&intent)
            .expect("replay matching-plan cleanup"),
        first
    );
    assert_eq!(install::read_plan(home).expect("plan"), None);
    assert_eq!(
        std::fs::read(&manifest_path).expect("retained manifest"),
        manifest_bytes
    );
    assert!(verify_backup(&plan).expect("retained backup"));
    assert!(staged_home.exists());
}

/// Cancellation, completion and fault cases once the dataset has been retired.
async fn retired_return_cases(pool: &Arc<PostgresPool>, source: &SqliteSource, home: &Path) {
    // After retirement a return can only finish: cancelling reports the roll-forward.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "abort-retired", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    returning.publish(&intent).await.expect("retire");
    assert!(matches!(
        returning.abort().await,
        Ok(RecoveryOutcome::RolledForward { generation: 3 })
    ));
    assert_eq!(backend(home), ActiveBackend::Local);
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    // Without markers only the exact retired run counts as completed.
    assert_eq!(returning.completed(run_id).await.expect("done"), Some(3));
    assert!(matches!(
        returning.completed(Uuid::new_v4()).await,
        Err(CutoverError::Conflict)
    ));
    assert!(matches!(returning.abort().await, Ok(RecoveryOutcome::Idle)));
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "not-retired", true).await;
    assert_eq!(returning.completed(run_id).await.expect("open"), None);

    // A genuine install fault after retirement keeps every recovery marker.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "install-fault", true).await;
    let fault = home
        .join("storage-backups")
        .join(format!("{run_id}-before-return"));
    std::fs::create_dir_all(fault.parent().expect("backup root")).expect("backup root");
    std::fs::write(&fault, b"fault").expect("install fault");
    assert!(returning.execute(run_id).await.is_err());
    assert!(dataset_state(pool, source).await.retired);
    let intent = read_cutover(home).expect("intent").expect("kept intent");
    let plan = install::read_plan(home).expect("plan").expect("kept plan");
    assert!(returning.recover().await.is_err());
    assert_eq!(read_cutover(home).expect("intent"), Some(intent));
    assert_eq!(install::read_plan(home).expect("plan"), Some(plan));
    std::fs::remove_file(&fault).expect("clear fault");
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledForward { generation: 3 })
    ));
    assert_eq!(backend(home), ActiveBackend::Local);

    // A plan left by a failed cleanup is discarded only for the exact abandoned export.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "cleanup-fault", true).await;
    returning.prepare(run_id).expect("prepare");
    let plan_path = home.join("storage-install.json");
    let kept = home.join("storage-install.json.kept");
    std::fs::rename(&plan_path, &kept).expect("keep plan");
    std::fs::create_dir(&plan_path).expect("cleanup fault");
    assert!(returning.recover().await.is_err());
    let reopened = dataset_state(pool, source).await;
    assert!(!reopened.migrating && !reopened.retired);
    assert_eq!(read_cutover(home).expect("intent"), None);
    std::fs::remove_dir(&plan_path).expect("clear fault");
    let real = std::fs::read(&kept).expect("plan bytes");
    let mut foreign: InstallPlan = serde_json::from_slice(&real).expect("plan");
    foreign.run_id = Uuid::new_v4();
    let foreign = serde_json::to_vec(&foreign).expect("foreign plan");
    std::fs::write(&plan_path, &foreign).expect("foreign plan");
    assert!(returning.recover().await.is_err());
    assert_eq!(std::fs::read(&plan_path).expect("kept"), foreign);
    std::fs::write(&plan_path, &real).expect("restore plan");
    std::fs::remove_file(&kept).expect("drop copy");
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(install::read_plan(home).expect("plan"), None);
}

pub(super) async fn return_phase(
    pool: &Arc<PostgresPool>,
    source: &SqliteSource,
    home: &Path,
    threads: &[ThreadMetadata],
) {
    // Writes made while PostgreSQL was authoritative must come back with the return.
    remote_authority(pool, source, home).await;
    let remote_only = metadata(700, Utc::now(), SessionSource::Cli);
    PostgresThreadCatalog::new(pool.clone())
        .upsert_thread(&remote_only)
        .await
        .expect("remote-only thread");
    let queued = PostgresQueueStore::new(pool.clone())
        .enqueue(threads[0].id, "{\"text\":\"remote only\"}".to_string())
        .await
        .expect("remote-only queued message");
    let state_db_name = SqliteConfig::new_for_testing(home.abs())
        .state_db_path()
        .file_name()
        .expect("state database name")
        .to_string_lossy()
        .into_owned();

    let (returning, run_id, _staged_home) = staged_return(pool, home, "happy", true).await;
    let moved = returning.execute(run_id).await.expect("return");
    assert_eq!(moved.marker.active_backend, ActiveBackend::Local);
    assert_eq!(moved.identity.generation, 3);
    assert!(moved.marker.remote_ever_activated);
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    let state = dataset_state(pool, source).await;
    assert!(state.retired);
    assert_eq!(state.generation, 3);

    // The ordinary runtime opens the installed home and has the remote-only data.
    let runtime = StateRuntime::init(
        SqliteConfig::new_for_testing(home.abs()),
        "migration-provider".to_string(),
    )
    .await
    .expect("installed runtime");
    assert!(
        runtime
            .get_thread(remote_only.id)
            .await
            .expect("remote-only thread")
            .is_some()
    );
    let page = runtime
        .thread_queue()
        .list_page(threads[0].id, 0, 10)
        .await
        .expect("queue page");
    assert!(page.iter().any(|item| item.id == queued.id));
    runtime.close().await;

    // What the return replaced is kept, with a manifest that proves it.
    let backup = home
        .join("storage-backups")
        .join(format!("{run_id}-before-return"));
    let plan: InstallPlan =
        serde_json::from_slice(&std::fs::read(backup.join("manifest.json")).expect("manifest"))
            .expect("manifest json");
    assert!(verify_backup(&plan).expect("backup readback"));
    assert!(backup.join(&state_db_name).exists());

    // A retired dataset refuses every writer, and a return cannot run twice.
    let refused = PostgresQueueStore::new(pool.clone())
        .enqueue(threads[0].id, "{}".to_string())
        .await
        .expect_err("retired dataset");
    assert!(
        refused.to_string().contains("no longer accepts writes"),
        "{refused}"
    );
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::Idle)
    ));

    // Crash after the intent: nothing was published, so recovery undoes it and reopens the
    // dataset exactly as it was.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "after-intent", true).await;
    returning.prepare(run_id).expect("prepare");
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(backend(home), ActiveBackend::Remote);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    let reopened = dataset_state(pool, source).await;
    assert!(!reopened.migrating && !reopened.retired);
    PostgresQueueStore::new(pool.clone())
        .enqueue(threads[0].id, "{}".to_string())
        .await
        .expect("remote writes resume after the rollback");

    // Lost acknowledgement: the dataset was retired but nothing local moved. Recovery installs
    // the staged files and flips the records.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "after-retire", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    returning.publish(&intent).await.expect("retire");
    assert!(matches!(
        load_authority(home),
        Err(codex_storage_authority::AuthorityError::Blocked(
            "cutover in progress"
        ))
    ));
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledForward { generation: 3 })
    ));
    assert_eq!(backend(home), ActiveBackend::Local);

    // Crash in the middle of the swap: the old database is already in the backup and the new one
    // is not in place yet.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "mid-swap", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    returning.publish(&intent).await.expect("retire");
    let plan = install::read_plan(home)
        .expect("plan")
        .expect("plan exists");
    let state_unit = plan
        .units
        .iter()
        .find(|unit| unit.name.ends_with(".sqlite") && !unit.backup_only)
        .expect("a database unit");
    std::fs::create_dir_all(&plan.backup_dir).expect("backup directory");
    std::fs::rename(
        home.join(&state_unit.name),
        plan.backup_dir.join(&state_unit.name),
    )
    .expect("half of the swap");
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledForward { generation: 3 })
    ));
    assert!(home.join(&state_unit.name).exists());
    assert!(plan.backup_dir.join(&state_unit.name).exists());

    // Another client moved the dataset to a generation this intent does not own.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "conflict", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    {
        let mut connection = pool.acquire().await.expect("connection");
        sqlx::query("UPDATE storage_activation SET state = 'open', generation = 9")
            .execute(&mut *connection)
            .await
            .expect("foreign generation");
    }
    assert!(matches!(
        returning.recover().await,
        Err(CutoverError::Conflict)
    ));
    assert_eq!(read_cutover(home).expect("intent"), Some(intent));

    // Cancelling before the dataset is retired keeps it authoritative and reopens it.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "cancelled", true).await;
    returning.prepare(run_id).expect("prepare");
    assert!(matches!(
        returning.abort().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(backend(home), ActiveBackend::Remote);
    assert!(!dataset_state(pool, source).await.migrating);

    // An export that was never verified cannot retire the dataset, and the refusal undoes the
    // intent.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "unverified", false).await;
    let refused = returning.execute(run_id).await;
    assert!(
        matches!(
            refused,
            Err(CutoverError::Destination(MigrationError::NotVerified))
        ),
        "{refused:?}"
    );
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(backend(home), ActiveBackend::Remote);
    completed_finish_is_repeatable(pool, source, home).await;
    retired_return_cases(pool, source, home).await;
    reset_target(pool).await;
}
