use super::*;
use codex_keyring_store::KeyringStore;
use codex_keyring_store::tests::MockKeyringStore;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_remote_storage::RemoteStorage;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_state::ThreadMetadataBuilder;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::RemotePostgresProfile;
use codex_storage_migration::InstallPlan;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

const KEYRING_SERVICE: &str = "codex-postgres-storage";

fn receipt(state: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(state.join("receipt.json")).expect("receipt"))
        .expect("receipt json")
}

fn password(state: &Path, role: &str) -> String {
    std::fs::read_to_string(state.join(format!("secrets/{role}.password")))
        .expect("password")
        .trim()
        .to_string()
}

fn profile(state: &Path, with_migrator: bool) -> RemotePostgresProfile {
    let mut value = serde_json::json!({
        "endpoint": "localhost",
        "port": receipt(state)["port"],
        "database": "codex",
        "namespace": "codex_storage",
        "credential": {"source": "keyring", "id": "service-runtime"},
        "tls": {"verification": "verify_full", "ca_certificate": state.join("secrets/ca.crt")},
        "connect_timeout_seconds": 5,
        "pool_acquire_timeout_seconds": 20,
        "max_connections": 4
    });
    if with_migrator {
        value["migrator_credential"] =
            serde_json::json!({"source": "keyring", "id": "service-migrator"});
    }
    serde_json::from_value(value).expect("profile")
}

fn keyring(state: &Path, runtime_password: Option<&str>) -> Arc<MockKeyringStore> {
    let keyring = MockKeyringStore::default();
    keyring
        .save(
            KEYRING_SERVICE,
            "service-runtime",
            runtime_password.unwrap_or(&password(state, "runtime")),
        )
        .expect("runtime credential");
    keyring
        .save(
            KEYRING_SERVICE,
            "service-migrator",
            &password(state, "migrator"),
        )
        .expect("migrator credential");
    Arc::new(keyring)
}

async fn reset_target(state: &Path) {
    let pool = PostgresPool::connect(ConnectionSettings {
        host: "localhost".to_string(),
        port: receipt(state)["port"].as_u64().expect("port") as u16,
        database: "codex".to_string(),
        username: "codex_migrator".to_string(),
        password: password(state, "migrator").into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(20),
            max_connections: 2,
        },
    })
    .await
    .expect("reset pool");
    codex_postgres_runtime::bootstrap_codex_storage(&pool)
        .await
        .expect("bootstrap disposable service fixture");
    let mut connection = pool.acquire().await.expect("connection");
    let mut transaction = sqlx::Acquire::begin(&mut *connection)
        .await
        .expect("fixture reset transaction");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *transaction)
        .await
        .expect("fixture owner role");
    sqlx::query("SET LOCAL search_path TO codex_storage, pg_catalog")
        .execute(&mut *transaction)
        .await
        .expect("fixture namespace");
    for statement in [
        "UPDATE storage_activation SET state = 'open', run_id = NULL, generation = 0, dataset_id = NULL",
        "DELETE FROM storage_migration_runs",
        "DELETE FROM threads",
        "DELETE FROM projects",
        "DELETE FROM logs",
        "DELETE FROM agent_board_posts",
        "DELETE FROM memory_stage1_outputs",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&mut *transaction)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    transaction.commit().await.expect("commit fixture reset");
    drop(connection);
    pool.close().await.expect("close fixture reset pool");
}

async fn populated_home() -> tempfile::TempDir {
    let home = tempfile::tempdir().expect("home");
    let sqlite = SqliteConfig::new_for_testing(home.path().abs());
    let runtime = StateRuntime::init(sqlite, "service-provider".to_string())
        .await
        .expect("runtime");
    let mut builder = ThreadMetadataBuilder::new(
        ThreadId::new(),
        home.path().join("rollout.jsonl"),
        chrono::Utc::now(),
        SessionSource::Cli,
    );
    builder.model_provider = Some("service-provider".to_string());
    runtime
        .upsert_thread(&builder.build("service-provider"))
        .await
        .expect("thread");
    runtime.close().await;
    home
}

fn service(
    home: &Path,
    state: &Path,
    candidate: Option<RemotePostgresProfile>,
    keyring: Arc<MockKeyringStore>,
) -> StorageService {
    let _ = state;
    StorageService::new(StorageServiceInputs {
        codex_home: home.to_path_buf(),
        sqlite: SqliteConfig::new_for_testing(home.abs()),
        candidate,
        default_model_provider_id: "service-provider".to_string(),
        host_label: "test-host".to_string(),
        keyring,
    })
}

async fn remote_fence_and_progress(
    service: &StorageService,
    run_id: Uuid,
) -> (codex_storage_migration::ActivationState, Vec<(String, u64)>) {
    let storage = service.connect().await.expect("remote fence connection");
    let migrator = service.migrator(&storage);
    let activation = migrator
        .activation_state()
        .await
        .expect("remote activation fence");
    let progress = migrator
        .progress(run_id)
        .await
        .expect("remote migration checkpoints");
    storage.close().await;
    (activation, progress)
}

fn stored_operation(service: &StorageService, operation_id: Uuid) -> crate::OperationRecord {
    service
        .journal
        .read(operation_id)
        .expect("read operation journal")
        .expect("operation record exists")
}

struct FailedReturnFixture {
    home: tempfile::TempDir,
    service: StorageService,
    operation_id: Uuid,
    run_id: Uuid,
    backup_obstruction: PathBuf,
    obstruction_bytes: Vec<u8>,
    intent_bytes: Vec<u8>,
    plan_bytes: Vec<u8>,
    retired: codex_storage_migration::ActivationState,
    progress: Vec<(String, u64)>,
}

async fn ready_return_service(
    state: &Path,
) -> (tempfile::TempDir, StorageService, crate::OperationRecord) {
    let home = populated_home().await;
    let service = service(
        home.path(),
        state,
        Some(profile(state, true)),
        keyring(state, None),
    );
    service
        .initialize_schema()
        .await
        .expect("initialize return recovery schema");

    let confirmed = Confirmation {
        writers_stopped: true,
    };
    let migration_plan = service
        .plan(PlanAction::Migrate)
        .await
        .expect("migration plan");
    let migration = Uuid::new_v4();
    let ready_migration = service
        .start_migration(migration, migration_plan.plan_id, confirmed)
        .await
        .expect("start local migration");
    assert_eq!(ready_migration.state, OperationState::Ready);
    assert_eq!(
        service
            .activate(migration)
            .await
            .expect("activate remote dataset")
            .state,
        OperationState::Active
    );

    let return_plan = service.plan(PlanAction::Return).await.expect("return plan");
    let operation_id = Uuid::new_v4();
    let ready_return = service
        .start_return(operation_id, return_plan.plan_id, confirmed)
        .await
        .expect("export return");
    assert_eq!(ready_return.state, OperationState::Ready);
    (home, service, ready_return)
}

async fn fail_return_install_at_backup_creation(state: &Path) -> FailedReturnFixture {
    let (home, service, ready) = ready_return_service(state).await;
    let operation_id = ready.operation_id;
    let run_id = ready.run_id.expect("return export run");
    let backup_obstruction = home
        .path()
        .join("storage-backups")
        .join(format!("{run_id}-before-return"));
    std::fs::create_dir_all(
        backup_obstruction
            .parent()
            .expect("backup parent directory"),
    )
    .expect("create backup parent");
    let obstruction_bytes = b"B5 test-owned backup path obstruction".to_vec();
    std::fs::write(&backup_obstruction, &obstruction_bytes).expect("write backup obstruction");

    let (before, progress_before) = remote_fence_and_progress(&service, run_id).await;
    assert!(before.migrating);
    assert!(!before.retired);
    assert_eq!(before.run_id, Some(run_id));

    assert!(matches!(
        service.activate(operation_id).await,
        Err(StorageError(BlockerCode::AuthorityInvalid))
    ));
    let failed = stored_operation(&service, operation_id);
    assert_eq!(failed.state, OperationState::Failed);
    assert_eq!(failed.blocker, Some(BlockerCode::AuthorityInvalid));

    let intent_path = home.path().join("storage-cutover.json");
    let intent = codex_storage_authority::read_cutover(home.path())
        .expect("read retained Local intent")
        .expect("intent remains after install error");
    assert_eq!(intent.run_id, run_id);
    assert_eq!(intent.target, codex_storage_authority::ActiveBackend::Local);
    let intent_bytes = std::fs::read(intent_path).expect("intent bytes");
    let plan_path = home.path().join("storage-install.json");
    let plan = codex_storage_migration::read_plan(home.path())
        .expect("read retained install plan")
        .expect("plan remains after install error");
    assert_eq!(plan.run_id, run_id);
    assert_eq!(plan.staged_home, service.staged_home(operation_id));
    assert_eq!(plan.backup_dir, backup_obstruction);
    let plan_bytes = std::fs::read(plan_path).expect("plan bytes");
    assert_eq!(
        std::fs::read(&backup_obstruction).expect("obstruction remains"),
        obstruction_bytes
    );
    assert!(matches!(
        codex_storage_authority::authority_state(home.path()),
        Ok(codex_storage_authority::AuthorityState::CutoverInProgress(current))
            if current == intent
    ));

    let (retired, progress) = remote_fence_and_progress(&service, run_id).await;
    assert!(!retired.migrating);
    assert!(retired.retired);
    assert_eq!(retired.run_id, Some(run_id));
    assert_eq!(retired.dataset_id, before.dataset_id);
    assert_eq!(retired.generation, before.generation + 1);
    assert_eq!(progress, progress_before);

    FailedReturnFixture {
        home,
        service,
        operation_id,
        run_id,
        backup_obstruction,
        obstruction_bytes,
        intent_bytes,
        plan_bytes,
        retired,
        progress,
    }
}

async fn assert_return_is_active_after_reconciliation(fixture: &FailedReturnFixture) {
    let record = stored_operation(&fixture.service, fixture.operation_id);
    assert_eq!(record.state, OperationState::Active);
    assert_eq!(record.blocker, None);
    assert_eq!(record.run_id, Some(fixture.run_id));
    assert_eq!(
        codex_storage_authority::read_cutover(fixture.home.path()).expect("read intent"),
        None
    );
    assert_eq!(
        codex_storage_migration::read_plan(fixture.home.path()).expect("read plan"),
        None
    );
    assert!(fixture.backup_obstruction.is_dir());
    let backup_manifest = fixture.backup_obstruction.join("manifest.json");
    let backup_plan: InstallPlan =
        serde_json::from_slice(&std::fs::read(backup_manifest).expect("backup manifest"))
            .expect("backup plan json");
    assert_eq!(backup_plan.run_id, fixture.run_id);
    assert_eq!(
        backup_plan.staged_home,
        fixture.service.staged_home(fixture.operation_id)
    );
    assert!(codex_storage_migration::verify_backup(&backup_plan).expect("verify backup"));
    assert!(matches!(
        codex_storage_authority::authority_state(fixture.home.path()),
        Ok(codex_storage_authority::AuthorityState::Local(_))
    ));
    let status = fixture.service.status(true).await;
    assert_eq!(status.active_backend, BackendName::LocalSqlite);
    assert_eq!(
        status
            .remote
            .expect("retired remote summary")
            .state
            .as_deref(),
        Some("retired")
    );
    let (remote, progress) = remote_fence_and_progress(&fixture.service, fixture.run_id).await;
    assert!(remote.retired);
    assert!(!remote.migrating);
    assert_eq!(remote.run_id, Some(fixture.run_id));
    assert_eq!(remote.dataset_id, fixture.retired.dataset_id);
    assert_eq!(remote.generation, fixture.retired.generation);
    assert_eq!(progress, fixture.progress);
}

#[tokio::test]
async fn failed_return_install_owner_recovers_after_real_io_error() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_STORAGE_SERVICE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    reset_target(state).await;
    let fixture = fail_return_install_at_backup_creation(state).await;
    assert_eq!(
        std::fs::read(fixture.home.path().join("storage-cutover.json"))
            .expect("intent remains before retry"),
        fixture.intent_bytes
    );
    assert_eq!(
        std::fs::read(fixture.home.path().join("storage-install.json"))
            .expect("plan remains before retry"),
        fixture.plan_bytes
    );
    assert_eq!(
        std::fs::read(&fixture.backup_obstruction).expect("obstruction remains before retry"),
        fixture.obstruction_bytes
    );

    std::fs::remove_file(&fixture.backup_obstruction).expect("remove test-owned obstruction");
    let report = fixture
        .service
        .recover()
        .await
        .expect("recover the exact failed return owner");
    assert_eq!(report.outcome, RecoveryKind::RolledForward);
    assert_eq!(report.operations.len(), 1);
    assert_eq!(report.operations[0].operation_id, fixture.operation_id);
    assert_eq!(report.operations[0].state, OperationState::Active);
    assert_return_is_active_after_reconciliation(&fixture).await;
    reset_target(state).await;
}

#[tokio::test]
async fn failed_return_cancel_roll_forward_keeps_active_journal_state() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_STORAGE_SERVICE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    reset_target(state).await;
    let fixture = fail_return_install_at_backup_creation(state).await;
    std::fs::remove_file(&fixture.backup_obstruction).expect("remove test-owned obstruction");

    assert_eq!(
        fixture.service.cancel(fixture.operation_id).await,
        Err(StorageError(BlockerCode::AlreadyRemote)),
        "cancel reports that recovery completed the local return"
    );
    assert_return_is_active_after_reconciliation(&fixture).await;
    let active = stored_operation(&fixture.service, fixture.operation_id);
    assert_eq!(
        fixture.service.cancel(fixture.operation_id).await,
        Err(StorageError(BlockerCode::AlreadyRemote))
    );
    assert_eq!(
        stored_operation(&fixture.service, fixture.operation_id),
        active
    );
    assert_eq!(active.state, OperationState::Active);
    reset_target(state).await;
}

#[tokio::test]
async fn failed_return_cancel_journal_error_cannot_become_cancelled() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_STORAGE_SERVICE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    reset_target(state).await;
    let fixture = fail_return_install_at_backup_creation(state).await;
    std::fs::remove_file(&fixture.backup_obstruction).expect("remove test-owned obstruction");

    let journal_temporary = fixture
        .home
        .path()
        .join("storage-operations")
        .join(format!("{}.json.tmp", fixture.operation_id));
    std::fs::create_dir_all(&journal_temporary).expect("block only the journal replacement");
    assert_eq!(
        fixture.service.cancel(fixture.operation_id).await,
        Err(StorageError(BlockerCode::Internal)),
        "failed Active journal write is reported after cutover completion"
    );
    let failed = stored_operation(&fixture.service, fixture.operation_id);
    assert_eq!(failed.state, OperationState::Failed);
    assert_eq!(
        codex_storage_authority::read_cutover(fixture.home.path()).expect("read intent"),
        None
    );
    assert_eq!(
        codex_storage_migration::read_plan(fixture.home.path()).expect("read plan"),
        None
    );
    let local = codex_storage_authority::authority_state(fixture.home.path())
        .expect("read local authority");
    assert!(matches!(
        local,
        codex_storage_authority::AuthorityState::Local(authority)
            if authority.identity.dataset_id == fixture.retired.dataset_id.expect("retired dataset")
                && authority.identity.generation == fixture.retired.generation as u64
    ));
    let (remote, progress) = remote_fence_and_progress(&fixture.service, fixture.run_id).await;
    assert!(remote.retired);
    assert_eq!(remote.run_id, Some(fixture.run_id));
    assert_eq!(remote.dataset_id, fixture.retired.dataset_id);
    assert_eq!(remote.generation, fixture.retired.generation);
    assert_eq!(progress, fixture.progress);

    let storage = fixture
        .service
        .connect()
        .await
        .expect("connect for retired-run refusal controls");
    let mut connection = storage
        .pool()
        .acquire()
        .await
        .expect("migration receipt connection");
    let (direction, source_fingerprint, run_state) = sqlx::query_as::<_, (String, String, String)>(
        "SELECT direction, source_fingerprint, state \
             FROM storage_migration_runs WHERE run_id = $1",
    )
    .bind(fixture.run_id)
    .fetch_one(&mut *connection)
    .await
    .expect("read exact retired export receipt");
    assert_eq!(direction, "export");
    assert_eq!(run_state, "activated");
    let changed =
        sqlx::query("UPDATE storage_migration_runs SET direction = 'import' WHERE run_id = $1")
            .bind(fixture.run_id)
            .execute(&mut *connection)
            .await
            .expect("install foreign direction");
    assert_eq!(changed.rows_affected(), 1);
    drop(connection);
    storage.close().await;
    assert_eq!(
        fixture.service.cancel(fixture.operation_id).await,
        Err(StorageError(BlockerCode::OperationConflict))
    );
    assert_eq!(
        stored_operation(&fixture.service, fixture.operation_id),
        failed
    );
    let (after_direction, direction_progress) =
        remote_fence_and_progress(&fixture.service, fixture.run_id).await;
    assert_eq!(after_direction, fixture.retired);
    assert_eq!(direction_progress, fixture.progress);

    let storage = fixture
        .service
        .connect()
        .await
        .expect("connect to restore direction and test fingerprint");
    let mut connection = storage
        .pool()
        .acquire()
        .await
        .expect("migration receipt connection");
    let changed = sqlx::query(
        "UPDATE storage_migration_runs SET direction = 'export', \
         source_fingerprint = 'foreign-source' WHERE run_id = $1",
    )
    .bind(fixture.run_id)
    .execute(&mut *connection)
    .await
    .expect("install foreign source fingerprint");
    assert_eq!(changed.rows_affected(), 1);
    drop(connection);
    storage.close().await;
    assert_eq!(
        fixture.service.cancel(fixture.operation_id).await,
        Err(StorageError(BlockerCode::OperationConflict))
    );
    assert_eq!(
        stored_operation(&fixture.service, fixture.operation_id),
        failed
    );
    let (after_fingerprint, fingerprint_progress) =
        remote_fence_and_progress(&fixture.service, fixture.run_id).await;
    assert_eq!(after_fingerprint, fixture.retired);
    assert_eq!(fingerprint_progress, fixture.progress);

    let storage = fixture
        .service
        .connect()
        .await
        .expect("restore exact export fingerprint");
    let mut connection = storage
        .pool()
        .acquire()
        .await
        .expect("migration receipt connection");
    sqlx::query("UPDATE storage_migration_runs SET source_fingerprint = $2 WHERE run_id = $1")
        .bind(fixture.run_id)
        .bind(source_fingerprint)
        .execute(&mut *connection)
        .await
        .expect("restore exact export fingerprint");
    drop(connection);
    storage.close().await;

    std::fs::remove_dir(&journal_temporary).expect("remove test-owned journal obstruction");
    assert_eq!(
        fixture.service.cancel(fixture.operation_id).await,
        Err(StorageError(BlockerCode::AlreadyRemote)),
        "the exact markerless retired owner settles Active instead of Cancelled"
    );
    assert_return_is_active_after_reconciliation(&fixture).await;
    reset_target(state).await;
}

#[tokio::test]
async fn failed_intent_only_return_owner_can_retry_unretired_rollback() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_STORAGE_SERVICE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    reset_target(state).await;
    let (home, service, ready) = ready_return_service(state).await;
    let operation_id = ready.operation_id;
    let run_id = ready.run_id.expect("return export run");
    let staged_home = service.staged_home(operation_id);
    let storage = service.connect().await.expect("return preparation storage");
    let returning = codex_storage_migration::ReturnCutover::new(
        home.path().to_path_buf(),
        staged_home.clone(),
        codex_storage_migration::Migrator::new(
            codex_storage_migration::SqliteSource::new(SqliteConfig::new_for_testing(
                staged_home.abs(),
            ))
            .relocated_from(home.path().to_path_buf()),
            storage.pool().clone(),
        ),
    );
    let intent = returning
        .prepare(run_id)
        .expect("persist exact return intent");
    assert_eq!(intent.run_id, run_id);
    assert_eq!(intent.target, codex_storage_authority::ActiveBackend::Local);
    storage.close().await;
    let intent_path = home.path().join("storage-cutover.json");
    let intent_bytes = std::fs::read(&intent_path).expect("intent bytes");
    let plan_path = home.path().join("storage-install.json");
    assert!(plan_path.exists());
    std::fs::remove_file(&plan_path).expect("model failed plan cleanup");

    let (before, progress_before) = remote_fence_and_progress(&service, run_id).await;
    assert!(before.migrating);
    assert!(!before.retired);
    assert_eq!(before.run_id, Some(run_id));
    let storage = service.connect().await.expect("inject foreign generation");
    let mut connection = storage
        .pool()
        .acquire()
        .await
        .expect("activation connection");
    sqlx::query(
        "UPDATE storage_activation SET state = 'open', generation = generation + 1 WHERE run_id = $1",
    )
    .bind(run_id)
    .execute(&mut *connection)
    .await
    .expect("install temporary conflicting activation state");
    drop(connection);
    storage.close().await;

    assert_eq!(
        service.activate(operation_id).await,
        Err(StorageError(BlockerCode::DatasetMismatch)),
        "the public activation records a retryable Failed owner when SQL evidence conflicts"
    );
    let failed = stored_operation(&service, operation_id);
    assert_eq!(failed.state, OperationState::Failed);
    assert_eq!(failed.blocker, Some(BlockerCode::DatasetMismatch));
    assert_eq!(
        std::fs::read(&intent_path).expect("intent remains"),
        intent_bytes
    );
    assert!(!plan_path.exists());
    let (foreign, foreign_progress) = remote_fence_and_progress(&service, run_id).await;
    assert!(!foreign.migrating);
    assert!(!foreign.retired);
    assert_eq!(foreign.generation, before.generation + 1);
    assert_eq!(foreign_progress, progress_before);

    let storage = service
        .connect()
        .await
        .expect("restore exact activation state");
    let mut connection = storage
        .pool()
        .acquire()
        .await
        .expect("activation connection");
    sqlx::query(
        "UPDATE storage_activation SET state = 'migrating', generation = $2 WHERE run_id = $1",
    )
    .bind(run_id)
    .bind(before.generation)
    .execute(&mut *connection)
    .await
    .expect("restore the exact unretired source generation");
    drop(connection);
    storage.close().await;
    let (restored, restored_progress) = remote_fence_and_progress(&service, run_id).await;
    assert!(restored.migrating);
    assert!(!restored.retired);
    assert_eq!(restored.generation, before.generation);
    assert_eq!(restored.dataset_id, before.dataset_id);
    assert_eq!(restored_progress, progress_before);

    let report = service
        .recover()
        .await
        .expect("retry the Failed intent-only rollback");
    assert_eq!(report.outcome, RecoveryKind::RolledBack);
    assert_eq!(report.operations.len(), 1);
    assert_eq!(report.operations[0].operation_id, operation_id);
    assert_eq!(report.operations[0].state, OperationState::Cancelled);
    assert_eq!(
        codex_storage_authority::read_cutover(home.path()).expect("read cleared intent"),
        None
    );
    assert_eq!(
        codex_storage_migration::read_plan(home.path()).expect("read plan"),
        None
    );
    let (reopened, final_progress) = remote_fence_and_progress(&service, run_id).await;
    assert!(!reopened.migrating);
    assert!(!reopened.retired);
    assert_eq!(reopened.generation, before.generation);
    assert_eq!(reopened.dataset_id, before.dataset_id);
    assert_eq!(final_progress, progress_before);
    reset_target(state).await;
}

#[tokio::test]
async fn failed_markerless_remote_return_can_cancel_exact_unretired_export() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_STORAGE_SERVICE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    reset_target(state).await;
    let (home, service, ready) = ready_return_service(state).await;
    let operation_id = ready.operation_id;
    let run_id = ready.run_id.expect("return export run");
    let staged_home = service.staged_home(operation_id);
    let displaced_staging = staged_home.with_file_name(format!("{operation_id}-b6-held"));
    assert!(staged_home.is_dir());
    assert!(!displaced_staging.exists());

    let (before, progress_before) = remote_fence_and_progress(&service, run_id).await;
    assert!(before.migrating);
    assert!(!before.retired);
    assert_eq!(before.run_id, Some(run_id));
    let remote_authority = match codex_storage_authority::authority_state(home.path())
        .expect("read initial authority")
    {
        codex_storage_authority::AuthorityState::Remote(authority) => authority,
        other => panic!("expected Remote authority before preparation failure, got {other:?}"),
    };
    assert!(remote_authority.marker.remote_ever_activated);
    assert_eq!(
        remote_authority.identity.dataset_id,
        before.dataset_id.unwrap()
    );
    assert_eq!(
        remote_authority.identity.generation,
        before.generation as u64
    );

    // Hide only this operation's staged directory. Activation reaches prepare, records and
    // abandons its Local intent, then fails while plan_install reads the missing staging path.
    std::fs::rename(&staged_home, &displaced_staging).expect("hide owned staging directory");
    let activation = service.activate(operation_id).await;
    std::fs::rename(&displaced_staging, &staged_home).expect("restore owned staging directory");
    assert_eq!(
        activation,
        Err(StorageError(BlockerCode::AuthorityInvalid)),
        "the public activation must fail in plan_install after intent cleanup"
    );
    let failed = stored_operation(&service, operation_id);
    assert_eq!(failed.state, OperationState::Failed);
    assert_eq!(failed.blocker, Some(BlockerCode::AuthorityInvalid));
    assert_eq!(
        codex_storage_authority::read_cutover(home.path()).expect("read cleaned intent"),
        None
    );
    assert_eq!(
        codex_storage_migration::read_plan(home.path()).expect("read cleaned plan"),
        None
    );
    assert!(matches!(
        codex_storage_authority::authority_state(home.path()),
        Ok(codex_storage_authority::AuthorityState::Remote(current))
            if current == remote_authority
    ));
    let (after_failure, progress_after_failure) = remote_fence_and_progress(&service, run_id).await;
    assert_eq!(after_failure, before);
    assert_eq!(progress_after_failure, progress_before);

    // A foreign-direction row cannot use the Remote cancellation route or change the fence.
    let storage = service
        .connect()
        .await
        .expect("connect for exact export ownership refusal");
    let mut connection = storage
        .pool()
        .acquire()
        .await
        .expect("migration receipt connection");
    let changed =
        sqlx::query("UPDATE storage_migration_runs SET direction = 'import' WHERE run_id = $1")
            .bind(run_id)
            .execute(&mut *connection)
            .await
            .expect("install foreign direction");
    assert_eq!(changed.rows_affected(), 1);
    drop(connection);
    storage.close().await;
    assert_eq!(
        service.cancel(operation_id).await,
        Err(StorageError(BlockerCode::OperationConflict))
    );
    assert_eq!(stored_operation(&service, operation_id), failed);
    let (after_refusal, progress_after_refusal) = remote_fence_and_progress(&service, run_id).await;
    assert_eq!(after_refusal, before);
    assert_eq!(progress_after_refusal, progress_before);

    let storage = service
        .connect()
        .await
        .expect("connect to restore the exact export row");
    let mut connection = storage
        .pool()
        .acquire()
        .await
        .expect("migration receipt connection");
    sqlx::query("UPDATE storage_migration_runs SET direction = 'export' WHERE run_id = $1")
        .bind(run_id)
        .execute(&mut *connection)
        .await
        .expect("restore exact export direction");
    drop(connection);
    storage.close().await;

    let journal_temporary = home
        .path()
        .join("storage-operations")
        .join(format!("{operation_id}.json.tmp"));
    std::fs::create_dir_all(&journal_temporary).expect("block the cancellation journal replace");
    assert_eq!(
        service.cancel(operation_id).await,
        Err(StorageError(BlockerCode::Internal)),
        "the durable SQL abandonment precedes journal acknowledgement"
    );
    assert_eq!(stored_operation(&service, operation_id), failed);
    let (after_journal_failure, progress_after_journal_failure) =
        remote_fence_and_progress(&service, run_id).await;
    assert!(!after_journal_failure.migrating);
    assert!(!after_journal_failure.retired);
    assert_eq!(after_journal_failure.run_id, Some(run_id));
    assert_eq!(after_journal_failure.dataset_id, before.dataset_id);
    assert_eq!(after_journal_failure.generation, before.generation);
    assert_eq!(progress_after_journal_failure, progress_before);
    std::fs::remove_dir(&journal_temporary).expect("remove test-owned journal obstruction");

    let cancelled = service
        .cancel(operation_id)
        .await
        .expect("retry cancellation from exact abandoned-run evidence");
    assert_eq!(cancelled.state, OperationState::Cancelled);
    assert_eq!(cancelled.blocker, None);
    assert_eq!(cancelled.run_id, Some(run_id));
    assert_eq!(stored_operation(&service, operation_id), cancelled);
    assert_eq!(
        service
            .cancel(operation_id)
            .await
            .expect("repeat cancelled operation"),
        cancelled
    );
    assert_eq!(
        codex_storage_authority::read_cutover(home.path()).expect("read final intent"),
        None
    );
    assert_eq!(
        codex_storage_migration::read_plan(home.path()).expect("read final plan"),
        None
    );
    assert!(matches!(
        codex_storage_authority::authority_state(home.path()),
        Ok(codex_storage_authority::AuthorityState::Remote(current))
            if current == remote_authority
    ));
    let (reopened, progress_after_cancel) = remote_fence_and_progress(&service, run_id).await;
    assert!(!reopened.migrating);
    assert!(!reopened.retired);
    assert_eq!(reopened.run_id, Some(run_id));
    assert_eq!(reopened.dataset_id, before.dataset_id);
    assert_eq!(reopened.generation, before.generation);
    assert_eq!(progress_after_cancel, progress_before);

    let credentials = keyring(state, None);
    let resolver = HostCredentialResolver::new(credentials.as_ref());
    let remote = RemoteStorage::connect(&profile(state, true), &resolver)
        .await
        .expect("connect after unretired cancellation");
    let mut builder = ThreadMetadataBuilder::new(
        ThreadId::new(),
        home.path().join("after-cancel.jsonl"),
        chrono::Utc::now(),
        SessionSource::Cli,
    );
    builder.model_provider = Some("service-provider".to_string());
    let written = builder.build("service-provider");
    remote
        .thread_catalog()
        .upsert_thread(&written)
        .await
        .expect("a new remote write succeeds after cancellation");
    assert_eq!(
        remote
            .thread_catalog()
            .get_thread(written.id)
            .await
            .expect("read new remote write")
            .as_ref(),
        Some(&written)
    );
    remote.close().await;
    reset_target(state).await;
}

#[tokio::test]
async fn real_postgres_storage_service() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_STORAGE_SERVICE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    reset_target(state).await;
    let home = populated_home().await;

    // Without a saved profile nothing can be tested or planned, and the home is plainly local.
    let bare = service(home.path(), state, None, keyring(state, None));
    let status = bare.status(true).await;
    assert_eq!(status.active_backend, BackendName::LocalSqlite);
    assert_eq!(status.authority, AuthorityLabel::Unmanaged);
    assert!(!status.candidate_configured);
    assert_eq!(
        bare.check_connection().await.blocker,
        Some(BlockerCode::NoCandidateProfile)
    );
    let plan = bare.plan(PlanAction::Migrate).await.expect("plan");
    assert!(plan.blockers.contains(&BlockerCode::NoCandidateProfile));
    assert!(!plan.is_startable());

    // The profile is tested with the runtime login; a wrong password stops at the connection.
    let service = service(
        home.path(),
        state,
        Some(profile(state, true)),
        keyring(state, None),
    );
    service
        .initialize_schema()
        .await
        .expect("initialize schema");
    let report = service.check_connection().await;
    assert_eq!(report.stage, CheckStage::Ready);
    assert_eq!(report.blocker, None);
    assert_eq!(report.empty, Some(true));
    assert_eq!(report.dataset_state.as_deref(), Some("open"));
    let wrong = self::service(
        home.path(),
        state,
        Some(profile(state, true)),
        keyring(state, Some("not-the-password")),
    );
    let rejected = wrong.check_connection().await;
    assert_eq!(rejected.stage, CheckStage::Connect);
    assert_eq!(rejected.blocker, Some(BlockerCode::ConnectionFailed));
    let without_owner = self::service(
        home.path(),
        state,
        Some(profile(state, false)),
        keyring(state, None),
    );
    assert_eq!(
        without_owner.initialize_schema().await,
        Err(StorageError(BlockerCode::MigratorCredentialMissing))
    );

    // A plan lists what moves and is startable; a return makes no sense yet.
    let plan = service.plan(PlanAction::Migrate).await.expect("plan");
    assert!(plan.is_startable(), "{:?}", plan.blockers);
    assert_eq!(
        plan.estimate.as_ref().map(|estimate| estimate.threads),
        Some(1)
    );
    assert_eq!(plan.host, "test-host");
    assert_eq!(plan.destination.matches('/').count(), 2);
    assert!(
        service
            .plan(PlanAction::Return)
            .await
            .expect("return plan")
            .blockers
            .contains(&BlockerCode::NotRemote)
    );

    // Nothing starts without the operator's promise, or against a plan that is not the current one.
    let operation = Uuid::new_v4();
    assert_eq!(
        service
            .start_migration(
                operation,
                plan.plan_id,
                Confirmation {
                    writers_stopped: false
                }
            )
            .await,
        Err(StorageError(BlockerCode::NotConfirmed))
    );
    assert_eq!(
        service
            .start_migration(
                operation,
                Uuid::new_v4(),
                Confirmation {
                    writers_stopped: true
                }
            )
            .await,
        Err(StorageError(BlockerCode::StalePlan))
    );
    assert_eq!(service.operations(), Vec::new());

    // The copy is verified and waits; a repeated request returns the same record.
    let confirmed = Confirmation {
        writers_stopped: true,
    };
    let ready = service
        .start_migration(operation, plan.plan_id, confirmed)
        .await
        .expect("start");
    assert_eq!(ready.state, OperationState::Ready);
    assert!(ready.run_id.is_some());
    assert!(
        ready
            .copied
            .iter()
            .any(|(domain, rows)| domain == "threads" && *rows == 1)
    );
    assert_eq!(
        service
            .start_migration(operation, plan.plan_id, confirmed)
            .await
            .expect("repeat"),
        ready
    );
    assert_eq!(service.operations(), vec![ready.clone()]);
    assert_eq!(
        service.status(false).await.active_backend,
        BackendName::LocalSqlite
    );

    // Activation makes it authoritative, once, and the dataset is no longer a target.
    let active = service.activate(operation).await.expect("activate");
    assert_eq!(active.state, OperationState::Active);
    assert_eq!(service.activate(operation).await.expect("again"), active);
    // The local files stay in place, and a record of them was kept before the switch.
    let manifest =
        codex_storage_migration::source_manifest_path(home.path(), active.run_id.expect("run id"));
    let recorded: codex_storage_migration::SourceManifest =
        serde_json::from_slice(&std::fs::read(manifest).expect("manifest")).expect("json");
    assert!(
        recorded
            .files
            .iter()
            .any(|file| file.name.ends_with(".sqlite") && file.bytes > 0)
    );
    let status = service.status(true).await;
    assert_eq!(status.active_backend, BackendName::RemotePostgres);
    assert_eq!(status.authority, AuthorityLabel::Remote);
    assert_eq!(status.local_generation, Some(2));
    assert!(status.remote_ever_activated);
    assert_eq!(status.blockers, Vec::new());
    let dataset_of_first_host = status.dataset_id;
    let remote = status.remote.expect("remote summary");
    assert_eq!(remote.dataset_matches, Some(true));
    assert_eq!(remote.generation, Some(2));
    assert!(
        service
            .plan(PlanAction::Migrate)
            .await
            .expect("plan after")
            .blockers
            .contains(&BlockerCode::AlreadyRemote)
    );
    assert_eq!(
        service.cancel(operation).await,
        Err(StorageError(BlockerCode::AlreadyRemote))
    );
    assert_eq!(
        service.recover().await.expect("recover").outcome,
        RecoveryKind::Idle
    );
    assert_eq!(
        service.operation(Uuid::new_v4()),
        Err(StorageError(BlockerCode::OperationNotFound))
    );

    // A second host joins the dataset without copying anything or touching its own history.
    let second_home = populated_home().await;
    let second = self::service(
        second_home.path(),
        state,
        Some(profile(state, true)),
        keyring(state, None),
    );
    let attach_plan = second.plan(PlanAction::Attach).await.expect("attach plan");
    assert!(attach_plan.is_startable(), "{:?}", attach_plan.blockers);
    assert_eq!(attach_plan.estimate, None);
    let dataset = attach_plan
        .connection
        .dataset_id
        .expect("the dataset to join");
    assert_eq!(Some(dataset), dataset_of_first_host);
    let local_history_before =
        std::fs::read(SqliteConfig::new_for_testing(second_home.path().abs()).state_db_path())
            .expect("second host history");
    let attach_operation = Uuid::new_v4();
    assert_eq!(
        second
            .attach_dataset(
                attach_operation,
                attach_plan.plan_id,
                Uuid::new_v4(),
                confirmed
            )
            .await,
        Err(StorageError(BlockerCode::DatasetMismatch))
    );
    assert_eq!(
        second
            .attach_dataset(
                attach_operation,
                attach_plan.plan_id,
                dataset,
                Confirmation {
                    writers_stopped: false
                }
            )
            .await,
        Err(StorageError(BlockerCode::NotConfirmed))
    );
    let attached = second
        .attach_dataset(attach_operation, attach_plan.plan_id, dataset, confirmed)
        .await
        .expect("attach");
    assert_eq!(attached.state, OperationState::Active);
    assert_eq!(
        second
            .attach_dataset(attach_operation, attach_plan.plan_id, dataset, confirmed)
            .await
            .expect("repeat"),
        attached
    );
    let joined = second.status(true).await;
    assert_eq!(joined.active_backend, BackendName::RemotePostgres);
    assert_eq!(joined.local_generation, Some(2));
    assert_eq!(joined.remote.expect("remote").dataset_matches, Some(true));
    assert_eq!(
        std::fs::read(SqliteConfig::new_for_testing(second_home.path().abs()).state_db_path())
            .expect("second host history after"),
        local_history_before
    );
    // A host that already belongs to a dataset cannot join another.
    assert!(
        service
            .plan(PlanAction::Attach)
            .await
            .expect("plan")
            .blockers
            .contains(&BlockerCode::HomeAlreadyManaged)
    );

    // Both hosts write to the one dataset and see each other's work.
    let credentials = keyring(state, None);
    let resolver = HostCredentialResolver::new(credentials.as_ref());
    let first_handle = RemoteStorage::connect(&profile(state, true), &resolver)
        .await
        .expect("first host handle");
    let second_handle = RemoteStorage::connect(&profile(state, true), &resolver)
        .await
        .expect("second host handle");
    let mut written = Vec::new();
    for (index, handle) in [&first_handle, &second_handle].into_iter().enumerate() {
        let mut builder = ThreadMetadataBuilder::new(
            ThreadId::new(),
            home.path().join(format!("shared-{index}.jsonl")),
            chrono::Utc::now(),
            SessionSource::Cli,
        );
        builder.model_provider = Some("service-provider".to_string());
        let thread = builder.build("service-provider");
        handle
            .thread_catalog()
            .upsert_thread(&thread)
            .await
            .expect("shared write");
        written.push(thread.id);
    }
    for handle in [&first_handle, &second_handle] {
        for id in &written {
            assert!(
                handle
                    .thread_catalog()
                    .get_thread(*id)
                    .await
                    .expect("shared read")
                    .is_some()
            );
        }
    }
    first_handle.close().await;
    second_handle.close().await;

    // Writes made while PostgreSQL is authoritative come back with the return.
    let remote = RemoteStorage::connect(
        &profile(state, true),
        &HostCredentialResolver::new(keyring(state, None).as_ref()),
    )
    .await
    .expect("remote handle");
    let mut remote_only = ThreadMetadataBuilder::new(
        ThreadId::new(),
        home.path().join("remote-only.jsonl"),
        chrono::Utc::now(),
        SessionSource::Cli,
    );
    remote_only.model_provider = Some("service-provider".to_string());
    let remote_only = remote_only.build("service-provider");
    remote
        .thread_catalog()
        .upsert_thread(&remote_only)
        .await
        .expect("remote-only thread");
    remote.close().await;

    let plan = service.plan(PlanAction::Return).await.expect("return plan");
    assert!(plan.is_startable(), "{:?}", plan.blockers);
    let returning = Uuid::new_v4();
    assert_eq!(
        service
            .start_return(
                returning,
                plan.plan_id,
                Confirmation {
                    writers_stopped: false
                }
            )
            .await,
        Err(StorageError(BlockerCode::NotConfirmed))
    );
    assert_eq!(
        service
            .start_return(returning, Uuid::new_v4(), confirmed)
            .await,
        Err(StorageError(BlockerCode::StalePlan))
    );
    let ready = service
        .start_return(returning, plan.plan_id, confirmed)
        .await
        .expect("export");
    assert_eq!(ready.state, OperationState::Ready);
    assert!(
        ready
            .copied
            .iter()
            .any(|(domain, rows)| domain == "threads" && *rows == 4)
    );
    // While the export waits, the dataset is closed to writers but the home is still remote.
    let waiting = service.status(true).await;
    assert_eq!(waiting.active_backend, BackendName::RemotePostgres);
    assert_eq!(
        waiting.remote.expect("remote summary").state.as_deref(),
        Some("migrating")
    );

    let run_id = ready.run_id.expect("return run id");
    let staged_home = service.staged_home(returning);
    let plan = InstallPlan {
        run_id,
        staged_home,
        backup_dir: home
            .path()
            .join("storage-backups")
            .join(format!("{run_id}-before-return")),
        units: Vec::new(),
    };
    let plan_bytes = serde_json::to_vec(&plan).expect("plan bytes");
    let plan_path = home.path().join("storage-install.json");
    std::fs::write(&plan_path, &plan_bytes).expect("plan-only recovery marker");
    assert_eq!(
        service.cancel(returning).await,
        Err(StorageError(BlockerCode::OperationConflict)),
        "a Ready journal entry cannot own cutover markers"
    );
    assert_eq!(service.operation(returning).expect("operation"), ready);

    let mut committing = ready.clone();
    committing.state = OperationState::Committing;
    committing.blocker = Some(BlockerCode::CutoverInProgress);
    service
        .journal
        .update(&committing)
        .expect("persist interrupted return");
    let mut foreign_plan = plan.clone();
    foreign_plan.staged_home = home.path().join("foreign-storage-staging");
    let foreign_plan_bytes = serde_json::to_vec(&foreign_plan).expect("foreign plan bytes");
    std::fs::write(&plan_path, &foreign_plan_bytes).expect("foreign staged path marker");
    assert_eq!(
        service.cancel(returning).await,
        Err(StorageError(BlockerCode::OperationConflict))
    );
    assert_eq!(
        std::fs::read(&plan_path).expect("foreign plan remains"),
        foreign_plan_bytes
    );
    assert_eq!(service.operation(returning).expect("operation"), committing);
    std::fs::write(&plan_path, &plan_bytes).expect("restore matching plan");

    // The service accepts the unique owner, then the cutover's plan-only conflict
    // follows the existing public DatasetMismatch mapping.
    let plan_only_remote_before = remote_fence_and_progress(&service, run_id).await;
    assert_eq!(
        service.recover().await,
        Err(StorageError(BlockerCode::DatasetMismatch))
    );
    assert_eq!(
        std::fs::read(&plan_path).expect("plan remains"),
        plan_bytes,
        "plan-only conflict must not delete recovery evidence"
    );
    assert_eq!(service.operation(returning).expect("operation"), committing);
    assert_eq!(
        remote_fence_and_progress(&service, run_id).await,
        plan_only_remote_before
    );
    assert_eq!(
        service.cancel(returning).await,
        Err(StorageError(BlockerCode::DatasetMismatch))
    );
    assert_eq!(
        std::fs::read(&plan_path).expect("plan remains after cancel"),
        plan_bytes
    );
    assert_eq!(service.operation(returning).expect("operation"), committing);
    assert_eq!(
        remote_fence_and_progress(&service, run_id).await,
        plan_only_remote_before
    );
    assert_eq!(
        service
            .status(true)
            .await
            .remote
            .expect("remote state")
            .state
            .as_deref(),
        Some("migrating")
    );
    std::fs::remove_file(&plan_path).expect("remove plan-only test marker");
    let mut ready = committing;
    ready.state = OperationState::Ready;
    ready.blocker = None;
    service
        .journal
        .update(&ready)
        .expect("restore Ready operation");

    // A matching Local intent and install plan still require one durable journal owner.
    let mut committing_owner = ready.clone();
    committing_owner.blocker = Some(BlockerCode::CutoverInProgress);
    service
        .save(&mut committing_owner, OperationState::Committing)
        .expect("persist owned local cutover");
    let storage = service.connect().await.expect("local cutover storage");
    let return_cutover = codex_storage_migration::ReturnCutover::new(
        home.path().to_path_buf(),
        service.staged_home(returning),
        codex_storage_migration::Migrator::new(
            codex_storage_migration::SqliteSource::new(SqliteConfig::new_for_testing(
                service.staged_home(returning).abs(),
            ))
            .relocated_from(home.path().to_path_buf()),
            storage.pool().clone(),
        ),
    );
    let intent = return_cutover
        .prepare(run_id)
        .expect("matching Local intent and install plan");
    storage.close().await;
    assert_eq!(intent.run_id, run_id);
    assert_eq!(intent.target, codex_storage_authority::ActiveBackend::Local);
    let plan = codex_storage_migration::read_plan(home.path())
        .expect("owned install plan")
        .expect("plan exists");
    assert_eq!(plan.run_id, run_id);
    assert_eq!(plan.staged_home, service.staged_home(returning));

    let intent_path = home.path().join("storage-cutover.json");
    let intent_bytes = std::fs::read(&intent_path).expect("owned Local intent bytes");
    let owned_plan_bytes = std::fs::read(&plan_path).expect("owned install plan bytes");
    let mut duplicate_owner = committing_owner.clone();
    duplicate_owner.operation_id = Uuid::new_v4();
    service
        .journal
        .create(&duplicate_owner)
        .expect("duplicate exact-run journal owner");

    let (remote_before, progress_before) = remote_fence_and_progress(&service, run_id).await;
    assert!(remote_before.migrating);
    assert_eq!(remote_before.run_id, Some(run_id));

    let assert_duplicate_owner_evidence = |expected_duplicate: &crate::OperationRecord| {
        assert_eq!(
            stored_operation(&service, returning),
            committing_owner,
            "the real owner must remain unchanged"
        );
        assert_eq!(
            stored_operation(&service, duplicate_owner.operation_id),
            *expected_duplicate,
            "the synthetic duplicate claim must remain unchanged"
        );
        assert_eq!(
            std::fs::read(&intent_path).expect("Local intent remains"),
            intent_bytes,
            "duplicate ownership must preserve exact intent bytes"
        );
        assert_eq!(
            std::fs::read(&plan_path).expect("install plan remains"),
            owned_plan_bytes,
            "duplicate ownership must preserve exact plan bytes"
        );
    };
    assert_eq!(
        service.recover().await,
        Err(StorageError(BlockerCode::OperationConflict)),
        "public recovery must refuse duplicate exact-run owners before cutover"
    );
    assert_duplicate_owner_evidence(&duplicate_owner);
    let (remote_after_recovery, progress_after_recovery) =
        remote_fence_and_progress(&service, run_id).await;
    assert_eq!(
        remote_after_recovery, remote_before,
        "public recovery must leave the full activation fence unchanged"
    );
    assert_eq!(
        progress_after_recovery, progress_before,
        "public recovery must leave remote checkpoints unchanged"
    );
    assert_eq!(
        service.cancel(returning).await,
        Err(StorageError(BlockerCode::OperationConflict)),
        "public cancellation must refuse duplicate exact-run owners before cutover"
    );
    assert_duplicate_owner_evidence(&duplicate_owner);
    let (remote_after_cancel, progress_after_cancel) =
        remote_fence_and_progress(&service, run_id).await;
    assert_eq!(
        remote_after_cancel, remote_before,
        "public cancellation must leave the full activation fence unchanged"
    );
    assert_eq!(
        progress_after_cancel, progress_before,
        "public cancellation must leave remote checkpoints unchanged"
    );

    // A terminal journal row still claims the run until its run_id is cleared.
    let mut terminal_duplicate = duplicate_owner.clone();
    terminal_duplicate.state = OperationState::Cancelled;
    terminal_duplicate.blocker = None;
    service
        .journal
        .update(&terminal_duplicate)
        .expect("mark synthetic duplicate terminal");
    assert_eq!(
        service.recover().await,
        Err(StorageError(BlockerCode::OperationConflict)),
        "a terminal duplicate still claiming the run must block recovery"
    );
    assert_duplicate_owner_evidence(&terminal_duplicate);
    let (remote_after_terminal_duplicate, progress_after_terminal_duplicate) =
        remote_fence_and_progress(&service, run_id).await;
    assert_eq!(
        remote_after_terminal_duplicate, remote_before,
        "a terminal duplicate claim must not change the activation fence"
    );
    assert_eq!(
        progress_after_terminal_duplicate, progress_before,
        "a terminal duplicate claim must not change remote checkpoints"
    );

    let mut duplicate_without_run_claim = terminal_duplicate.clone();
    duplicate_without_run_claim.run_id = None;
    service
        .journal
        .update(&duplicate_without_run_claim)
        .expect("remove synthetic duplicate run claim");
    assert_eq!(
        stored_operation(&service, duplicate_owner.operation_id),
        duplicate_without_run_claim
    );
    let recovered = service
        .recover()
        .await
        .expect("recover with the one exact owner");
    assert_eq!(recovered.outcome, RecoveryKind::RolledBack);
    assert_eq!(recovered.operations.len(), 1);
    assert_eq!(recovered.operations[0].operation_id, returning);
    assert_eq!(recovered.operations[0].state, OperationState::Cancelled);
    assert_eq!(recovered.operations[0].blocker, None);
    assert_eq!(
        codex_storage_authority::read_cutover(home.path()).expect("recovered intent"),
        None
    );
    assert_eq!(
        codex_storage_migration::read_plan(home.path()).expect("recovered plan"),
        None
    );
    assert!(!intent_path.exists());
    assert!(!plan_path.exists());
    assert_eq!(
        stored_operation(&service, duplicate_owner.operation_id),
        duplicate_without_run_claim
    );
    let (remote_after_rollback, progress_after_rollback) =
        remote_fence_and_progress(&service, run_id).await;
    let mut reopened = remote_before;
    reopened.migrating = false;
    assert_eq!(
        remote_after_rollback, reopened,
        "the exact source run must be open again without retirement"
    );
    assert_eq!(
        progress_after_rollback, progress_before,
        "rollback preserves the exported domain checkpoints"
    );

    // Cancelling reopens the dataset unchanged and a new export can be made.
    let cancelled = service.cancel(returning).await.expect("cancel");
    assert_eq!(cancelled.state, OperationState::Cancelled);
    assert_eq!(
        service
            .status(true)
            .await
            .remote
            .expect("remote")
            .state
            .as_deref(),
        Some("open")
    );
    let plan = service.plan(PlanAction::Return).await.expect("plan again");
    let second_return = Uuid::new_v4();
    let ready = service
        .start_return(second_return, plan.plan_id, confirmed)
        .await
        .expect("second export");
    assert_eq!(ready.state, OperationState::Ready);
    let done = service.activate(second_return).await.expect("return");
    assert_eq!(done.state, OperationState::Active);
    assert_eq!(service.activate(second_return).await.expect("again"), done);

    // The home is local again, the dataset is retired, and the local files hold everything.
    let status = service.status(true).await;
    assert_eq!(status.active_backend, BackendName::LocalSqlite);
    assert_eq!(status.authority, AuthorityLabel::Local);
    assert_eq!(status.local_generation, Some(3));
    assert!(status.remote_ever_activated);
    assert_eq!(
        status.remote.expect("remote").state.as_deref(),
        Some("retired")
    );
    let runtime = StateRuntime::init(
        SqliteConfig::new_for_testing(home.path().abs()),
        "service-provider".to_string(),
    )
    .await
    .expect("returned runtime");
    assert!(
        runtime
            .get_thread(remote_only.id)
            .await
            .expect("remote-only thread")
            .is_some()
    );
    runtime.close().await;
    assert!(
        service
            .plan(PlanAction::Migrate)
            .await
            .expect("plan after return")
            .blockers
            .contains(&BlockerCode::DatasetRetired)
    );
    assert_eq!(
        service.recover().await.expect("recover").outcome,
        RecoveryKind::Idle
    );
    // The host that joined is told plainly, not left writing a history nobody reads.
    let stranded = second.status(true).await;
    assert!(stranded.blockers.contains(&BlockerCode::DatasetRetired));
    reset_target(state).await;
}

fn dataset_report() -> ConnectionReport {
    ConnectionReport {
        stage: CheckStage::Dataset,
        blocker: None,
        schema_format: Some(codex_postgres_runtime::client_schema_format()),
        dataset_state: Some("open".to_string()),
        dataset_id: None,
        generation: Some(0),
        empty: None,
    }
}

#[test]
fn target_inspection_failure_is_redacted_and_never_ready() {
    let mut report = dataset_report();
    super::service::finish_target_inspection(
        &mut report,
        Err(codex_storage_migration::MigrationError::Target(
            "driver secret=password-canary endpoint=private-host".to_string(),
        )),
    );
    assert_eq!(report.stage, CheckStage::Dataset);
    assert_eq!(report.blocker, Some(BlockerCode::ConnectionFailed));
    assert_eq!(report.empty, None);
    let public = serde_json::to_string(&report).expect("public report");
    assert!(!public.contains("password-canary"));
    assert!(!public.contains("private-host"));
    assert!(!format!("{report:?}").contains("password-canary"));
    let plan = StoragePlan::build(
        PlanAction::Migrate,
        "test-host",
        None,
        crate::plan::PlanInputs {
            authority: Ok(codex_storage_authority::AuthorityState::Unmanaged),
            connection: report,
            estimate: None,
            sqlite_home_matches: true,
        },
    );
    assert!(!plan.is_startable());
    assert_eq!(plan.blockers, vec![BlockerCode::ConnectionFailed]);
}

#[test]
fn successful_target_inspection_preserves_empty_and_occupied_facts() {
    for empty in [true, false] {
        let mut report = dataset_report();
        super::service::finish_target_inspection(&mut report, Ok(empty));
        assert_eq!(report.stage, CheckStage::Ready);
        assert_eq!(report.blocker, None);
        assert_eq!(report.empty, Some(empty));
    }
}

#[test]
fn unknown_target_emptiness_still_blocks_migration() {
    let plan = StoragePlan::build(
        PlanAction::Migrate,
        "test-host",
        None,
        crate::plan::PlanInputs {
            authority: Ok(codex_storage_authority::AuthorityState::Unmanaged),
            connection: dataset_report(),
            estimate: None,
            sqlite_home_matches: true,
        },
    );
    assert!(!plan.is_startable());
    assert_eq!(plan.blockers, vec![BlockerCode::TargetNotEmpty]);
}
