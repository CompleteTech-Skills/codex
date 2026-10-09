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

fn wal_files(dir: &Path, skip: &Path) -> Vec<(PathBuf, u64, Option<String>)> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).expect("evidence directory") {
        let path = entry.expect("entry").path();
        if path == skip {
            continue;
        }
        if path.is_dir() {
            files.push((path.clone(), 0, None));
            files.extend(wal_files(&path, skip));
        } else {
            let len = std::fs::metadata(&path).expect("length").len();
            let hash = (!path.to_string_lossy().ends_with(".sqlite-shm"))
                .then(|| install::sha256_file(&path).expect("hash"));
            files.push((path, len, hash));
        }
    }
    files.sort();
    files
}
async fn wal_bound<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(60), future)
        .await
        .expect("prepare WAL operation timed out")
}
async fn prepare_wal(
    pool: &Arc<PostgresPool>,
    home: &Path,
    returning: &ReturnCutover,
    run: Uuid,
    stage: &Path,
) {
    let config = SqliteConfig::new_for_testing(stage.abs());
    let source = SqliteSource::new(config.clone()).relocated_from(home.to_path_buf());
    let state = dataset_state(pool, &source).await;
    assert!(state.migrating && !state.retired);
    assert_eq!(state.run_id, Some(run));
    assert_eq!(backend(home), ActiveBackend::Remote);
    let sql = "SELECT jsonb_build_object('activation', to_jsonb(a), 'run', to_jsonb(r), 'domains', (SELECT jsonb_agg(to_jsonb(d) ORDER BY domain) FROM storage_migration_domains d WHERE run_id = $1)) FROM storage_activation a JOIN storage_migration_runs r ON r.run_id = $1 WHERE a.singleton";
    let observed = || async {
        let query = sqlx::query_scalar::<_, serde_json::Value>(sql).bind(run);
        let mut connection = pool.acquire().await.expect("SQL evidence connection");
        query.fetch_one(&mut *connection).await.expect("SQL")
    };
    let before = wal_bound(observed()).await;
    assert_eq!(before["run"]["state"], "verified");
    assert_eq!(before["run"]["direction"], "export");
    let fingerprint = source.fingerprint().await.expect("fingerprint");
    assert_eq!(before["run"]["source_fingerprint"], fingerprint);
    let db = config.state_db_path();
    let open = config.open_read_write_pool(&db);
    let writer = wal_bound(open).await.expect("staged writer");
    let write = sqlx::query("CREATE TABLE qa_prepare_marker AS SELECT 1 AS value");
    let committed = wal_bound(write.execute(&writer)).await;
    committed.expect("committed private marker");
    let wal = db.with_extension("sqlite-wal");
    assert!(std::fs::metadata(wal).expect("real WAL").len() > 0);
    assert!(db.with_extension("sqlite-shm").is_file());
    assert_eq!(read_cutover(home).expect("no intent"), None);
    assert_eq!(install::read_plan(home).expect("no plan"), None);
    let live = || wal_files(home, &home.join("storage-staging"));
    let staged = || wal_files(stage, Path::new(""));
    let sentinel = home.join("storage-cutover.json");
    let expected = "staged SQLite sidecars";
    for blocked_intent in [false, true] {
        if blocked_intent {
            std::fs::create_dir(&sentinel).expect("intent creation sentinel");
        }
        let files = (live(), staged());
        let refused = returning.prepare(run).expect_err("real WAL refusal");
        let sidecar = matches!(refused, CutoverError::Authority(ref m) if m.contains(expected));
        assert!(sidecar, "{refused:?}");
        assert_eq!(wal_bound(observed()).await, before);
        assert_eq!((live(), staged()), files);
        if blocked_intent {
            std::fs::remove_dir(&sentinel).expect("remove empty fixture sentinel");
        }
    }
    let marker = sqlx::query_scalar::<_, i64>("SELECT value FROM qa_prepare_marker");
    let value = wal_bound(marker.fetch_one(&writer)).await.expect("marker");
    assert_eq!(value, 1);
    let checkpoint = sqlx::query_as::<_, (i64, i64, i64)>("PRAGMA wal_checkpoint(TRUNCATE)");
    let done = wal_bound(checkpoint.fetch_one(&writer));
    let result = done.await.expect("checkpoint");
    assert_eq!(result.0, 0);
    wal_bound(writer.close()).await;
    install::validate_staged_sqlite_sidecars(stage).expect("all staged sidecars gone");
    let migrator = Migrator::new(source, pool.clone());
    let verified = wal_bound(migrator.verify(run)).await.expect("reverify");
    assert_eq!(verified.run_id, run);
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

    let (returning, run_id, staged_home) = staged_return(pool, home, "happy", true).await;
    prepare_wal(pool, home, &returning, run_id, &staged_home).await;
    let moved = returning.execute(run_id).await.expect("return");
    assert_eq!(moved.marker.active_backend, ActiveBackend::Local);
    assert_eq!(moved.identity.generation, 3);
    assert!(moved.marker.remote_ever_activated);
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    let state = dataset_state(pool, source).await;
    assert!(state.retired);
    assert_eq!(state.run_id, Some(run_id));
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

    // Rollback lost its intent before plan cleanup failed. The remaining plan must
    // still reopen exactly its owned export instead of falsely reporting idle.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "rollback-plan-only", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    codex_storage_authority::abandon_cutover(home, &intent).expect("intent cleanup succeeded");
    assert!(install::read_plan(home).expect("retained plan").is_some());
    assert!(dataset_state(pool, source).await.migrating);
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert!(install::read_plan(home).expect("settled plan").is_none());
    assert_eq!(backend(home), ActiveBackend::Remote);
    let reopened = dataset_state(pool, source).await;
    assert!(!reopened.migrating && !reopened.retired);
    PostgresQueueStore::new(pool.clone())
        .enqueue(threads[0].id, "{}".to_string())
        .await
        .expect("remote writes after plan-only rollback");

    // Local authority was published, but the completed return's plan remains.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "completed-plan-only", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    returning.publish(&intent).await.expect("retire");
    returning.install().expect("install");
    codex_storage_authority::complete_cutover(home, &intent).expect("authority published");
    assert!(install::read_plan(home).expect("cleanup pending").is_some());
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledForward { generation: 3 })
    ));
    assert!(install::read_plan(home).expect("cleaned plan").is_none());
    assert_eq!(backend(home), ActiveBackend::Local);
    assert!(dataset_state(pool, source).await.retired);
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::Idle)
    ));

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
    reset_target(pool).await;
}
