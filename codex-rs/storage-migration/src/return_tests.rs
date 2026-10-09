//! Failure injection for handing authority back to local files.

use super::tests::metadata;
use super::tests::reset_target;
use super::*;
use crate::Cutover;
use crate::CutoverError;
use crate::CutoverError::Authority;
use crate::CutoverError::Conflict;
use crate::InstallPlan;
use crate::RecoveryOutcome;
use crate::RecoveryOutcome::RolledForward;
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
use codex_storage_authority::ActivationMarker;
use codex_storage_authority::ActiveBackend;
use codex_storage_authority::LocalIdentity;
use codex_storage_authority::adopt_quiesced_home;
use codex_storage_authority::load_authority;
use codex_storage_authority::read_cutover;
use codex_thread_store::QueueStore;
use codex_utils_absolute_path::test_support::PathExt;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::process::Command;
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

async fn return_sql(pool: &PostgresPool) -> String {
    let mut connection = pool.acquire().await.expect("SQL evidence");
    sqlx::query_scalar("SELECT jsonb_build_object('activation', (SELECT to_jsonb(a) FROM storage_activation a WHERE singleton), 'runs', (SELECT jsonb_agg(to_jsonb(r) ORDER BY run_id) FROM storage_migration_runs r), 'domains', (SELECT jsonb_agg(to_jsonb(d) ORDER BY run_id, domain) FROM storage_migration_domains d))::text")
        .fetch_one(&mut *connection).await.expect("full SQL evidence")
}

#[cfg(target_os = "linux")]
struct PlanMount(PathBuf);
#[cfg(target_os = "linux")]
impl Drop for PlanMount {
    fn drop(&mut self) {
        let ok = Command::new("umount")
            .arg(&self.0)
            .status()
            .is_ok_and(|s| s.success());
        assert!(ok || std::thread::panicking(), "release owned plan mount");
    }
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

    // Windows always exercises this phase; Linux needs explicit private-namespace admission.
    let required = std::env::var("CODEX_TEST_RETURN_FINISH_MOUNT_REQUIRED").as_deref() == Ok("1");
    if cfg!(windows) || required {
        if cfg!(not(any(windows, target_os = "linux"))) {
            panic!("unsupported finish fault platform");
        }
        #[cfg(windows)]
        use std::os::windows::fs::OpenOptionsExt;
        let control = home.join(CONTROL_FILES[3]);
        let records = &CONTROL_FILES[..3];
        let stable_files = || {
            return_files(home)
                .into_iter()
                .filter(|(path, _, _)| !records.iter().any(|name| path == &home.join(name)))
                .collect::<Vec<_>>()
        };
        let before = stable_files();
        let full_sql_before = return_sql(pool).await;
        let sql = dataset_state(pool, source).await;
        assert!(sql.retired);
        assert_eq!(sql.run_id, Some(run_id));
        assert_eq!(sql.dataset_id, Some(intent.dataset_id));
        assert_eq!(u64::try_from(sql.generation).unwrap(), intent.to_generation);
        assert_eq!(read_cutover(home).unwrap(), Some(intent.clone()));
        let bytes = fs::read(home.join(CONTROL_FILES[1])).unwrap();
        let marker: ActivationMarker = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(marker.active_backend, ActiveBackend::Remote);
        assert_eq!(marker.generation, intent.from_generation);
        #[cfg(windows)]
        let held = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0x1 | 0x2)
            .open(&control)
            .expect("readable plan, deny delete");
        #[cfg(target_os = "linux")]
        let held = {
            let parent =
                std::env::var("CODEX_TEST_RETURN_FINISH_MOUNT_PARENT").expect("mount parent");
            assert!(!parent.trim().is_empty());
            let current = fs::read_link("/proc/self/ns/mnt").unwrap();
            assert_ne!(current, PathBuf::from(parent));
            let mapping = fs::read_to_string("/proc/self/uid_map").unwrap();
            let ids: Vec<_> = mapping.split_whitespace().collect();
            assert!(matches!(ids.as_slice(), ["0", _, "1"]));
            let m = fs::read_to_string("/proc/self/mountinfo").unwrap();
            assert!(!m.contains(" shared:") && !m.contains(" master:"));
            assert!(control.is_absolute() && fs::symlink_metadata(&control).unwrap().is_file());
            let mut command = Command::new("mount");
            command.arg("--bind").arg(&control).arg(&control);
            assert!(command.status().unwrap().success());
            PlanMount(control.clone())
        };
        #[cfg(not(any(windows, target_os = "linux")))]
        let held = None::<std::fs::File>;
        assert_eq!(fs::read(&control).unwrap(), plan_bytes);
        let denied = fs::remove_file(&control).expect_err("native plan unlink fault");
        let errno = if cfg!(windows) { 32 } else { 16 };
        assert_eq!(denied.raw_os_error(), Some(errno));
        assert!(matches!(returning.finish(&intent), Err(Authority(_))));
        // These observations prove publication happened inside the failing call.
        let published = load_authority(home).expect("Local after failing finish");
        let mut expected_identity: LocalIdentity =
            serde_json::from_slice(&identity_before_finish).unwrap();
        expected_identity.generation = intent.to_generation;
        let mut expected_marker = marker;
        expected_marker.generation = intent.to_generation;
        expected_marker.active_backend = ActiveBackend::Local;
        assert_eq!(published.identity, expected_identity);
        assert_eq!(published.marker, expected_marker);
        assert_eq!(published.marker.active_backend, ActiveBackend::Local);
        assert_eq!(published.identity.dataset_id, intent.dataset_id);
        assert_eq!(published.identity.generation, intent.to_generation);
        assert_eq!(published.identity.format_version, intent.format_version);
        assert_eq!(read_cutover(home).unwrap(), None);
        assert_eq!(install::read_plan(home).unwrap(), Some(plan.clone()));
        assert_eq!(stable_files(), before);
        assert_eq!(dataset_state(pool, source).await, sql);
        assert_eq!(return_sql(pool).await, full_sql_before);
        assert!(verify_backup(&plan).unwrap());
        drop(held);
        assert_eq!(returning.finish(&intent).expect("retry cleanup"), published);
        assert_eq!(install::read_plan(home).unwrap(), None);
        assert_eq!(read_cutover(home).unwrap(), None);
        assert_eq!(dataset_state(pool, source).await, sql);
        assert_eq!(return_sql(pool).await, full_sql_before);
        assert!(verify_backup(&plan).unwrap());
        let remaining: Vec<_> = before
            .into_iter()
            .filter(|(path, _, _)| path != &control)
            .collect();
        assert_eq!(stable_files(), remaining);
        eprintln!("return-finish-cleanup-phase-qualified run={run_id}");
    }

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

/// Mutate evidence only after PostgreSQL reports this transaction blocking recovery.
async fn blocked_recovery(
    pool: &PostgresPool,
    returning: &ReturnCutover,
    run_id: Uuid,
    activation_lock: bool,
    change: impl FnOnce(),
) -> Result<RecoveryOutcome, CutoverError> {
    let mut locker = pool.acquire().await.expect("locker");
    let mut tx = locker.begin().await.expect("lock transaction");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .expect("locker pid");
    if activation_lock {
        sqlx::query("LOCK TABLE storage_activation IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await
            .expect("activation read barrier");
    } else {
        sqlx::query("SELECT run_id FROM storage_migration_runs WHERE run_id = $1 FOR UPDATE")
            .bind(run_id)
            .execute(&mut *tx)
            .await
            .expect("run barrier");
    }
    let mut observer = pool.acquire().await.expect("lock observer");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mutate = async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted AND $1 = ANY(pg_blocking_pids(pid)))")
                    .bind(pid).fetch_one(&mut *observer).await.expect("blocking pids");
                if blocked { break; }
                tokio::task::yield_now().await;
            }
            change();
            tx.commit().await.expect("release observed lock");
        };
        let (result, ()) = tokio::join!(returning.recover(), mutate);
        result
    }).await.expect("observed recovery wait within bound")
}

fn return_files(path: &Path) -> Vec<(PathBuf, bool, Vec<u8>)> {
    let directory = path.is_dir();
    let bytes = if directory {
        Vec::new()
    } else {
        fs::read(path).expect("file evidence")
    };
    let mut files = vec![(path.to_path_buf(), directory, bytes)];
    if directory {
        for entry in fs::read_dir(path).expect("directory evidence") {
            files.extend(return_files(&entry.expect("entry").path()));
        }
    }
    files.sort();
    files
}

async fn abandoned(pool: &Arc<PostgresPool>, source: &SqliteSource, expected: &ActivationState) {
    let mut expected = expected.clone();
    expected.migrating = false;
    expected.retired = false;
    assert_eq!(dataset_state(pool, source).await, expected);
    let mut connection = pool.acquire().await.expect("durable state");
    let state: String =
        sqlx::query_scalar("SELECT state FROM storage_migration_runs WHERE run_id = $1")
            .bind(expected.run_id.expect("owned run"))
            .fetch_one(&mut *connection)
            .await
            .expect("run state");
    assert_eq!(state, "abandoned");
}

async fn retired_return_cases(pool: &Arc<PostgresPool>, source: &SqliteSource, home: &Path) {
    // Retired abort reports the completed Local handoff, never cancellation.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "abort-retired", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    returning.publish(&intent).await.expect("retire");
    assert!(matches!(
        returning.abort().await,
        Ok(RolledForward { generation: 3 })
    ));
    assert_eq!(backend(home), ActiveBackend::Local);
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    // Without markers only the exact retired run counts as completed.
    assert_eq!(returning.completed(run_id).await.expect("done"), Some(3));
    assert!(matches!(
        returning.completed(Uuid::new_v4()).await,
        Err(Conflict)
    ));
    let backup = format!("{run_id}-before-return");
    let manifest = home
        .join("storage-backups")
        .join(backup)
        .join("manifest.json");
    let kept = fs::read(&manifest).expect("manifest");
    let mut forged: InstallPlan = serde_json::from_slice(&kept).expect("manifest plan");
    forged.staged_home = home.join("elsewhere");
    fs::write(&manifest, serde_json::to_vec(&forged).expect("forged")).expect("forge");
    assert!(matches!(returning.completed(run_id).await, Err(Conflict)));
    fs::write(&manifest, kept).expect("restore manifest");
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
    fs::create_dir_all(fault.parent().expect("backup root")).expect("backup root");
    fs::write(&fault, b"fault").expect("install fault");
    assert!(returning.execute(run_id).await.is_err());
    assert!(dataset_state(pool, source).await.retired);
    let intent = read_cutover(home).expect("intent").expect("kept intent");
    let plan = install::read_plan(home).expect("plan").expect("kept plan");
    assert!(returning.recover().await.is_err());
    assert_eq!(read_cutover(home).expect("intent"), Some(intent));
    assert_eq!(install::read_plan(home).expect("plan"), Some(plan));
    fs::remove_file(&fault).expect("clear fault");
    assert!(matches!(
        returning.recover().await,
        Ok(RolledForward { generation: 3 })
    ));
    assert_eq!(backend(home), ActiveBackend::Local);

    // Intent cleanup needs owned evidence; plan-only evidence remains conservatively held.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "cleanup-fault", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    let plan_path = home.join("storage-install.json");
    let real = fs::read(&plan_path).expect("plan bytes");
    for with_intent in [true, false] {
        let remote = dataset_state(pool, source).await;
        for variant in 0..3 {
            let mut forged: InstallPlan = serde_json::from_slice(&real).expect("plan");
            match variant {
                0 => forged.run_id = Uuid::new_v4(),
                1 => forged.staged_home = home.join("storage-staging").join("elsewhere"),
                _ => forged.backup_dir = home.join("elsewhere"),
            }
            let forged = serde_json::to_vec(&forged).expect("forged plan");
            fs::write(&plan_path, &forged).expect("forged plan");
            let before = return_files(home);
            assert!(returning.recover().await.is_err());
            assert_eq!(return_files(home), before);
            assert_eq!(dataset_state(pool, source).await, remote);
        }
        fs::write(&plan_path, &real).expect("restore plan");
        if with_intent {
            let kept = home.join("storage-install.json.kept");
            fs::rename(&plan_path, &kept).expect("keep valid plan");
            fs::create_dir(&plan_path).expect("unreadable plan");
            let before = return_files(home);
            assert!(returning.recover().await.is_err());
            assert_eq!(return_files(home), before);
            assert_eq!(dataset_state(pool, source).await, remote);
            fs::remove_dir(&plan_path).expect("clear fixture directory");
            fs::rename(&kept, &plan_path).expect("restore valid plan");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let permissions = fs::metadata(home).unwrap().permissions();
                let before = return_files(home);
                // Both markers remain readable/valid; unlink fails only after SQL commits.
                let result = blocked_recovery(pool, &returning, run_id, false, || {
                    fs::set_permissions(home, fs::Permissions::from_mode(0o555)).unwrap();
                })
                .await;
                fs::set_permissions(home, permissions).unwrap();
                assert!(matches!(
                    result,
                    Err(CutoverError::Destination(MigrationError::Staging(_)))
                ));
                assert_eq!(return_files(home), before);
                abandoned(pool, source, &remote).await;
            }
            codex_storage_authority::abandon_cutover(home, &intent)
                .expect("model intent-only cleanup");
        }
    }
    // Replace a valid captured Remote plan only after an actual SQL lock wait is observed.
    let mut forged: InstallPlan = serde_json::from_slice(&real).expect("plan");
    forged.backup_dir = home.join("elsewhere");
    let mut replaced = Vec::new();
    let remote = dataset_state(pool, source).await;
    let result = blocked_recovery(pool, &returning, run_id, false, || {
        fs::write(&plan_path, serde_json::to_vec(&forged).unwrap()).unwrap();
        replaced = return_files(home);
    })
    .await;
    assert!(matches!(
        result,
        Err(CutoverError::Destination(MigrationError::TargetBusy))
    ));
    assert_eq!(return_files(home), replaced);
    abandoned(pool, source, &remote).await;
    fs::write(&plan_path, &real).expect("restore captured plan");
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(install::read_plan(home).expect("plan"), None);

    // Local plan-only cleanup must retain a newly arrived intent during its SQL read.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "local-evidence", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    returning.publish(&intent).await.expect("retire");
    returning.install().expect("install");
    codex_storage_authority::complete_cutover(home, &intent).expect("publish Local, retain plan");
    let sql = dataset_state(pool, source).await;
    let full_sql = return_sql(pool).await;
    let mut foreign = intent;
    foreign.run_id = Uuid::new_v4();
    let mut replaced = Vec::new();
    let result = blocked_recovery(pool, &returning, run_id, true, || {
        fs::write(
            home.join("storage-cutover.json"),
            serde_json::to_vec(&foreign).unwrap(),
        )
        .unwrap();
        replaced = return_files(home);
    })
    .await;
    assert!(matches!(
        result,
        Err(CutoverError::Destination(MigrationError::TargetBusy))
    ));
    assert_eq!(return_files(home), replaced);
    assert!(dataset_state(pool, source).await.retired);
    assert_eq!(dataset_state(pool, source).await, sql);
    assert_eq!(return_sql(pool).await, full_sql);
    fs::remove_file(home.join("storage-cutover.json")).expect("clear fixture foreign intent");
    let before = return_files(home);
    assert_eq!(
        returning.recover().await.expect("Local cleanup retry"),
        RolledForward { generation: 3 }
    );
    assert_eq!(install::read_plan(home).unwrap(), None);
    assert_eq!(read_cutover(home).unwrap(), None);
    assert_eq!(dataset_state(pool, source).await, sql);
    assert_eq!(return_sql(pool).await, full_sql);
    let remaining: Vec<_> = before
        .into_iter()
        .filter(|(p, _, _)| p != &plan_path)
        .collect();
    assert_eq!(return_files(home), remaining);
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
