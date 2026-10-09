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
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use std::path::Path;
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

async fn fixture_pool(state: &Path) -> PostgresPool {
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
    let mut connection = pool.acquire().await.expect("fixture identity");
    let identity: (String, i32, String) =
        sqlx::query_as("SELECT current_database(), inet_server_port(), current_user")
            .fetch_one(&mut *connection)
            .await
            .expect("bound fixture identity");
    let port = receipt(state)["port"].as_u64().unwrap() as i32;
    let login = "codex_migrator".to_owned();
    assert_eq!(identity, ("codex".to_owned(), port, login));
    drop(connection);
    pool
}

async fn reset_target(state: &Path) {
    let pool = fixture_pool(state).await;
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
        "DELETE FROM thread_spawn_edges",
        "TRUNCATE project_idempotency_keys, queued_thread_revisions, agent_board_deleted, external_agent_config_imports, threads, projects CASCADE",
        "DELETE FROM threads",
        "DELETE FROM projects",
        "DELETE FROM thread_sections WHERE id <> '01984de2-8f74-7c91-a3b2-5c5e937cf318'",
        "DELETE FROM logs",
        "DELETE FROM agent_board_posts",
        "DELETE FROM memory_stage1_outputs",
        "DELETE FROM memory_jobs",
        "UPDATE memory_consolidation_progress SET max_thread_count = 0",
        "DELETE FROM agent_board_channels",
        "DELETE FROM agent_board_subscriptions",
        "DELETE FROM agent_board_opt_outs",
        "UPDATE log_id_counter SET last_id = 0",
        "UPDATE queue_change_counter SET version = 0",
        "UPDATE agent_board_post_counter SET last_seq = 0",
        "UPDATE thread_timestamp_marks SET updated_at_ms = 0, recency_at_ms = 0",
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

// Actual row counts and durable counters, including a spawn edge without a parent thread.
const RESIDUE_STATE_SQL: &str = "SELECT ARRAY[(SELECT count(*) FROM codex_storage.thread_spawn_edges), (SELECT count(*) FROM codex_storage.threads), (SELECT count(*) FROM codex_storage.thread_sections WHERE id <> '01984de2-8f74-7c91-a3b2-5c5e937cf318'), (SELECT count(*) FROM codex_storage.thread_sections WHERE id = '01984de2-8f74-7c91-a3b2-5c5e937cf318'), (SELECT last_id FROM codex_storage.log_id_counter WHERE singleton), (SELECT version FROM codex_storage.queue_change_counter WHERE singleton), (SELECT last_seq FROM codex_storage.agent_board_post_counter WHERE singleton), (SELECT updated_at_ms FROM codex_storage.thread_timestamp_marks WHERE singleton), (SELECT recency_at_ms FROM codex_storage.thread_timestamp_marks WHERE singleton)]";

async fn fixture_residue_control(state: &Path) {
    let pool = fixture_pool(state).await;
    let mut connection = pool.acquire().await.expect("residue connection");
    let mut transaction = sqlx::Acquire::begin(&mut *connection)
        .await
        .expect("seed transaction");
    for statement in [
        "SET LOCAL ROLE codex_owner",
        "SET LOCAL search_path TO codex_storage, pg_catalog",
        "INSERT INTO thread_spawn_edges (parent_thread_id, child_thread_id, status) VALUES ('00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000002', 'open')",
        "INSERT INTO thread_sections (id, name) VALUES ('fixture-residue-section', 'Fixture residue')",
        "UPDATE log_id_counter SET last_id = 9",
        "UPDATE queue_change_counter SET version = 9",
        "UPDATE agent_board_post_counter SET last_seq = 9",
        "UPDATE thread_timestamp_marks SET updated_at_ms = 9, recency_at_ms = 9",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&mut *transaction)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    transaction.commit().await.expect("commit genuine residue");
    let owner = sqlx::query("SET ROLE codex_owner");
    owner.execute(&mut *connection).await.expect("role");
    let seeded: Vec<i64> = sqlx::query_scalar(RESIDUE_STATE_SQL)
        .fetch_one(&mut *connection)
        .await
        .expect("committed residue rows");
    assert_eq!(seeded, vec![1, 0, 1, 1, 9, 9, 9, 9, 9]);
    drop(connection);
    pool.close().await.expect("close residue seed pool");
    reset_target(state).await;
    let pool = fixture_pool(state).await;
    let mut connection = pool.acquire().await.expect("reset evidence");
    let owner = sqlx::query("SET ROLE codex_owner");
    owner.execute(&mut *connection).await.expect("role");
    let cleaned: Vec<i64> = sqlx::query_scalar(RESIDUE_STATE_SQL)
        .fetch_one(&mut *connection)
        .await
        .expect("reset rows and counters");
    assert_eq!(cleaned, vec![0, 0, 0, 1, 0, 0, 0, 0, 0]);
    drop(connection);
    pool.close().await.expect("close reset evidence pool");
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

#[tokio::test]
async fn real_postgres_storage_service() {
    let state = std::env::var("CODEX_TEST_POSTGRES_STORAGE_SERVICE_STATE");
    if std::env::var("CODEX_TEST_POSTGRES_REQUIRED").as_deref() == Ok("1") {
        assert!(
            state.as_ref().is_ok_and(|path| !path.trim().is_empty()),
            "required service fixture"
        );
    }
    let Ok(state) = state else {
        return;
    };
    let state = Path::new(&state);
    reset_target(state).await;
    fixture_residue_control(state).await;
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
    return_fault_cases(state).await;
    reset_target(state).await;
}

const CONFIRMED: Confirmation = Confirmation {
    writers_stopped: true,
};

/// A remote home with a verified export of its dataset waiting in `Ready`.
async fn ready_return(
    state: &Path,
) -> (
    tempfile::TempDir,
    StorageService,
    OperationRecord,
    (Uuid, OperationRecord),
) {
    reset_target(state).await;
    let home = populated_home().await;
    let profile = Some(profile(state, true));
    let svc = service(home.path(), state, profile, keyring(state, None));
    svc.initialize_schema().await.expect("initialize schema");
    let plan = svc.plan(PlanAction::Migrate).await.expect("migrate plan");
    let migration = Uuid::new_v4();
    let started = svc
        .start_migration(migration, plan.plan_id, CONFIRMED)
        .await;
    started.expect("migrate");
    svc.activate(migration).await.expect("activate");
    let plan = svc.plan(PlanAction::Return).await.expect("return plan");
    let (prepared, created) = svc
        .prepare_return(Uuid::new_v4(), plan.plan_id, CONFIRMED)
        .await
        .expect("prepare");
    assert!(created);
    let ready = svc.run_return(prepared.clone()).await.expect("export");
    assert_eq!(ready.state, OperationState::Ready);
    (home, svc, ready, (plan.plan_id, prepared))
}

/// Record the cutover intent and retire the exact exported run, as an interrupted return would.
async fn retire_export(svc: &StorageService, home: &Path, ready: &OperationRecord, finish: bool) {
    use codex_storage_migration::Migrator;
    use codex_storage_migration::ReturnCutover;
    use codex_storage_migration::SqliteSource;
    let remote = svc.connect().await.expect("remote before retirement");
    let staged = svc.staged_home(ready.operation_id);
    let source = SqliteSource::new(SqliteConfig::new_for_testing(staged.abs()))
        .relocated_from(home.to_path_buf());
    let migrator = Migrator::new(source, remote.pool().clone());
    let returning = ReturnCutover::new(home.to_path_buf(), staged, migrator);
    let intent = returning
        .prepare(ready.run_id.expect("run"))
        .expect("prepare");
    returning.publish(&intent).await.expect("retire exact run");
    if finish {
        returning.install().expect("install");
        codex_storage_authority::complete_cutover(home, &intent).expect("authority");
    }
    remote.close().await;
}

fn saved(svc: &StorageService, record: &OperationRecord, state: OperationState) -> OperationRecord {
    let mut record = record.clone();
    svc.save(&mut record, state).expect("journal state");
    record
}

fn markers(home: &Path) -> bool {
    let intent = codex_storage_authority::read_cutover(home).expect("intent");
    intent.is_some()
        || codex_storage_migration::read_plan(home)
            .expect("plan")
            .is_some()
}

fn settled(home: &Path) -> bool {
    let local = codex_storage_authority::authority_state(home);
    !markers(home) && matches!(local, Ok(codex_storage_authority::AuthorityState::Local(_)))
}

fn return_files(path: &Path) -> Vec<(std::path::PathBuf, bool, Vec<u8>)> {
    let directory = path.is_dir();
    let bytes = if directory {
        Vec::new()
    } else {
        std::fs::read(path).expect("file evidence")
    };
    let mut files = vec![(path.to_path_buf(), directory, bytes)];
    if directory {
        for entry in std::fs::read_dir(path).expect("directory evidence") {
            files.extend(return_files(&entry.expect("entry").path()));
        }
    }
    files.sort();
    files
}

async fn return_evidence(
    svc: &StorageService,
) -> (Vec<(std::path::PathBuf, bool, Vec<u8>)>, String) {
    let storage = svc.connect().await.expect("evidence pool");
    let mut connection = storage.pool().acquire().await.expect("evidence connection");
    let sql = sqlx::query_scalar("SELECT jsonb_build_object('activation', (SELECT to_jsonb(a) FROM storage_activation a WHERE singleton), 'runs', (SELECT jsonb_agg(to_jsonb(r) ORDER BY run_id) FROM storage_migration_runs r), 'domains', (SELECT jsonb_agg(to_jsonb(d) ORDER BY run_id, domain) FROM storage_migration_domains d))::text")
        .fetch_one(&mut *connection).await.expect("SQL evidence");
    drop(connection);
    storage.close().await;
    (return_files(&svc.inputs.codex_home), sql)
}

async fn return_fault_cases(state: &Path) {
    // A real install fault after retirement keeps the owner Committing, and the legacy Failed
    // owner the old code left for the same fault recovers too; evidence survives the fault.
    let (home, svc, ready, (plan_id, prepared)) = ready_return(state).await;
    let id = ready.operation_id;
    let backup = format!("{}-before-return", ready.run_id.expect("run"));
    let fault = home.path().join("storage-backups").join(backup);
    std::fs::create_dir_all(fault.parent().expect("root")).expect("backup root");
    std::fs::write(&fault, b"fault").expect("install fault");
    assert!(svc.activate(id).await.is_err());
    let held = svc.operation(id).expect("held owner");
    assert_eq!(held.state, OperationState::Committing);
    assert_eq!(held.blocker, Some(BlockerCode::CutoverInProgress));
    let remote = svc.connect().await.expect("remote");
    assert!(remote.activation().await.expect("activation").retired);
    remote.close().await;
    let before = return_evidence(&svc).await;
    assert!(matches!(
        svc.run_return(prepared).await,
        Err(StorageError(BlockerCode::OperationConflict))
    ));
    assert_eq!(return_evidence(&svc).await, before);
    assert_eq!(svc.operation(id).expect("same owner"), held);
    let retry = svc.start_return(id, plan_id, CONFIRMED).await;
    assert_eq!(retry.expect("retry"), held);
    let legacy = saved(&svc, &held, OperationState::Failed);
    let retry = svc.start_return(id, plan_id, CONFIRMED).await;
    assert_eq!(retry.expect("no restart"), legacy);
    let before = return_evidence(&svc).await;
    assert!(svc.run_return(legacy.clone()).await.is_err());
    assert!(svc.recover().await.is_err());
    assert_eq!(svc.operation(id).expect("kept"), legacy);
    assert_eq!(return_evidence(&svc).await, before);
    assert!(markers(home.path()));
    std::fs::remove_file(&fault).expect("clear fault");
    let recovered = svc.recover().await.expect("recover");
    assert_eq!(recovered.outcome, RecoveryKind::RolledForward);
    let done = svc.operation(id).expect("settled");
    assert_eq!(done.state, OperationState::Active);
    assert!(settled(home.path()));
    assert_eq!(svc.activate(id).await.expect("repeat"), done);

    // Cancelling after retirement finishes the return and settles Active, never Cancelled.
    let (home, svc, ready, _) = ready_return(state).await;
    let id = ready.operation_id;
    retire_export(&svc, home.path(), &ready, false).await;
    let mut foreign = ready.clone();
    foreign.run_id = Some(Uuid::new_v4());
    let foreign = saved(&svc, &foreign, OperationState::Committing);
    let before = return_evidence(&svc).await;
    assert!(matches!(
        svc.activate(id).await,
        Err(StorageError(BlockerCode::OperationConflict))
    ));
    assert_eq!(svc.operation(id).expect("foreign owner retained"), foreign);
    assert_eq!(return_evidence(&svc).await, before);
    saved(&svc, &ready, OperationState::Committing);
    let done = svc.cancel(id).await.expect("cancel finishes the return");
    assert_eq!(done.state, OperationState::Active);
    assert_eq!(svc.operation(id).expect("journal"), done);
    assert!(settled(home.path()));
    assert_eq!(svc.cancel(id).await.expect("repeat"), done);
    // A lost acknowledgement with no markers needs the exact retired run.
    saved(&svc, &done, OperationState::Committing);
    let marker_less = svc.cancel(id).await.expect("marker-less");
    assert_eq!(marker_less.state, OperationState::Active);
    let mut foreign = done.clone();
    foreign.run_id = Some(Uuid::new_v4());
    saved(&svc, &foreign, OperationState::Committing);
    assert!(svc.cancel(id).await.is_err());
    assert_eq!(
        svc.operation(id).expect("kept").state,
        OperationState::Committing
    );

    // A plan left after authority published is discarded only for the exact retired run.
    let (home, svc, ready, _) = ready_return(state).await;
    retire_export(&svc, home.path(), &ready, true).await;
    assert!(markers(home.path()));
    saved(&svc, &ready, OperationState::Committing);
    let leftover = svc.cancel(ready.operation_id).await.expect("leftover plan");
    assert_eq!(leftover.state, OperationState::Active);
    assert!(settled(home.path()));

    // A Failed owner without recovery evidence stays cancel-safe.
    let (_home, svc, ready, _) = ready_return(state).await;
    let failed = saved(&svc, &ready, OperationState::Failed);
    assert_eq!(
        svc.recover().await.expect("recover").outcome,
        RecoveryKind::Idle
    );
    let cancelled = svc.cancel(failed.operation_id).await.expect("cancel");
    assert_eq!(cancelled.state, OperationState::Cancelled);
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

mod finish_journal_control {
    use super::*;
    use pretty_assertions::assert_eq;
    use codex_storage_authority::ActiveBackend;
    use codex_storage_authority::load_authority;
    use codex_storage_authority::read_cutover;
    use codex_storage_migration::InstallPlan;
    use codex_storage_migration::read_plan;
    use codex_storage_migration::verify_backup;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;

    type Files = BTreeMap<PathBuf, (bool, Vec<u8>)>;
    const SQL: &str = "SELECT jsonb_build_object('activation', (SELECT to_jsonb(a) FROM storage_activation a WHERE singleton), 'runs', (SELECT jsonb_agg(to_jsonb(r) ORDER BY run_id) FROM storage_migration_runs r), 'domains', (SELECT jsonb_agg(to_jsonb(d) ORDER BY run_id, domain) FROM storage_migration_domains d))::text";

    async fn bounded<F: std::future::Future>(future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .expect("bounded service finish control")
    }

    async fn sql(connection: &mut sqlx::PgConnection) -> serde_json::Value {
        bounded(async {
            let mut tx = sqlx::Connection::begin(connection).await.unwrap();
            for statement in [
                "SET LOCAL ROLE codex_owner",
                "SET LOCAL search_path TO codex_storage, pg_catalog",
                "SET LOCAL statement_timeout = '5s'",
            ] {
                sqlx::query(sqlx::AssertSqlSafe(statement))
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            let text: String = sqlx::query_scalar(SQL).fetch_one(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
            serde_json::from_str(&text).unwrap()
        })
        .await
    }

    fn files(home: &Path) -> Files {
        return_files(home)
            .into_iter()
            .map(|(p, d, b)| (p, (d, b)))
            .collect()
    }

    fn stable(mut all: Files, home: &Path, id: Uuid) -> Files {
        for name in [
            "storage-identity.json",
            "storage-activation.json",
            "storage-cutover.json",
        ] {
            all.remove(&home.join(name));
        }
        all.remove(&home.join("storage-operations").join(format!("{id}.json")));
        all
    }

    fn installed(before: Files, home: &Path, id: Uuid, plan: &InstallPlan) -> Files {
        let mut expected = Files::new();
        for (path, value) in stable(before.clone(), home, id) {
            let mut destination = path.clone();
            for unit in &plan.units {
                let live = home.join(&unit.name);
                let staged = plan.staged_home.join(&unit.name);
                if !unit.backup_only && path.starts_with(&staged) {
                    destination = live.join(path.strip_prefix(&staged).unwrap());
                } else if path.starts_with(&live)
                    && (unit.backup_only || before.contains_key(&staged))
                {
                    destination = plan
                        .backup_dir
                        .join(&unit.name)
                        .join(path.strip_prefix(&live).unwrap());
                }
            }
            assert!(
                expected.insert(destination, value).is_none(),
                "unique swap path"
            );
        }
        expected.insert(home.join("storage-backups"), (true, Vec::new()));
        expected.insert(plan.backup_dir.clone(), (true, Vec::new()));
        expected.insert(
            plan.backup_dir.join("manifest.json"),
            (false, serde_json::to_vec_pretty(plan).unwrap()),
        );
        expected
    }

    #[cfg(target_os = "linux")]
    struct PlanMount(PathBuf);
    #[cfg(target_os = "linux")]
    impl Drop for PlanMount {
        fn drop(&mut self) {
            let ok = std::process::Command::new("umount")
                .arg(&self.0)
                .status()
                .is_ok_and(|s| s.success());
            assert!(
                ok || std::thread::panicking(),
                "release only owned plan mount"
            );
        }
    }

    fn fault(control: &Path) -> Box<dyn std::any::Any> {
        assert!(control.is_absolute() && fs::symlink_metadata(control).unwrap().is_file());
        #[cfg(target_os = "linux")]
        {
            assert_eq!(
                std::env::var("CODEX_TEST_RETURN_FINISH_MOUNT_REQUIRED").as_deref(),
                Ok("1")
            );
            let parent = std::env::var("CODEX_TEST_RETURN_FINISH_MOUNT_PARENT")
                .expect("actual mount parent");
            assert!(!parent.trim().is_empty());
            assert_ne!(
                fs::read_link("/proc/self/ns/mnt").unwrap(),
                PathBuf::from(parent)
            );
            let mapping = fs::read_to_string("/proc/self/uid_map").unwrap();
            let ids: Vec<_> = mapping.split_whitespace().collect();
            assert!(matches!(ids.as_slice(), ["0", _, "1"]));
            let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap();
            assert!(!mounts.contains(" shared:") && !mounts.contains(" master:"));
            let status = fs::read_to_string("/proc/self/status").unwrap();
            let caps = status
                .lines()
                .find_map(|l| l.strip_prefix("CapBnd:"))
                .unwrap();
            assert_eq!(u64::from_str_radix(caps.trim(), 16).unwrap() & 0xe, 0);
            assert!(
                std::process::Command::new("mount")
                    .arg("--bind")
                    .arg(control)
                    .arg(control)
                    .status()
                    .unwrap()
                    .success()
            );
            Box::new(PlanMount(control.to_path_buf()))
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            Box::new(
                fs::OpenOptions::new()
                    .read(true)
                    .share_mode(0x1 | 0x2)
                    .open(control)
                    .unwrap(),
            )
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            panic!("unsupported required service finish fault platform");
        }
    }

    fn run(sql: &serde_json::Value, id: Uuid) -> serde_json::Value {
        sql["runs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["run_id"].as_str() == Some(id.to_string().as_str()))
            .unwrap()
            .clone()
    }

    #[tokio::test]
    #[ignore = "requires owned real PostgreSQL and a native plan-unlink fault"]
    async fn real_postgres_storage_service_return_finish_journal() {
        tokio::time::timeout(Duration::from_secs(50), scenario())
            .await
            .expect("whole service finish bound");
    }

    async fn scenario() {
        assert_eq!(
            std::env::var("CODEX_TEST_POSTGRES_REQUIRED").as_deref(),
            Ok("1")
        );
        let state = std::env::var("CODEX_TEST_POSTGRES_STORAGE_SERVICE_STATE")
            .expect("required private fixture");
        assert!(!state.trim().is_empty());
        if cfg!(target_os = "linux") {
            assert_eq!(
                std::env::var("CODEX_TEST_RETURN_FINISH_MOUNT_REQUIRED").as_deref(),
                Ok("1")
            );
        }
        let (home, svc, ready, (plan_id, prepared)) =
            bounded(ready_return(Path::new(&state))).await;
        let home = home.path();
        let (id, run_id) = (ready.operation_id, ready.run_id.unwrap());
        let staged = home.join("storage-staging").join(id.to_string());
        let owner_path = home.join("storage-operations").join(format!("{id}.json"));
        let control = home.join("storage-install.json");
        let remote = load_authority(home).unwrap();
        assert_eq!(remote.marker.active_backend, ActiveBackend::Remote);
        assert_eq!(read_cutover(home).unwrap(), None);
        assert_eq!(read_plan(home).unwrap(), None);
        let stage_before = return_files(&staged);
        // A synthetic foreign journal sentinel tests filtering, never the causal handoff.
        let mut foreign = ready.clone();
        foreign.operation_id = Uuid::new_v4();
        foreign.run_id = Some(Uuid::new_v4());
        foreign.state = OperationState::Committing;
        svc.journal.create(&foreign).unwrap();
        let others = || {
            svc.operations()
                .into_iter()
                .filter(|r| r.operation_id != id)
                .collect::<Vec<_>>()
        };
        let journals_before = others();
        let pool = bounded(fixture_pool(Path::new(&state))).await;
        let mut locker = bounded(pool.acquire()).await.unwrap();
        let mut observer = bounded(pool.acquire()).await.unwrap();
        let sql_before = sql(&mut observer).await;
        assert_eq!(run(&sql_before, run_id)["state"], "verified");
        assert_eq!(run(&sql_before, run_id)["direction"], "export");
        assert!(
            !run(&sql_before, run_id)["source_fingerprint"]
                .as_str()
                .unwrap()
                .is_empty()
        );
        assert_eq!(sql_before["activation"]["state"], "migrating");
        assert_eq!(sql_before["activation"]["run_id"], run_id.to_string());
        assert_eq!(
            sql_before["activation"]["dataset_id"],
            remote.identity.dataset_id.to_string()
        );
        assert_eq!(
            sql_before["activation"]["generation"].as_u64(),
            Some(remote.identity.generation)
        );
        let mut tx = bounded(sqlx::Acquire::begin(&mut *locker)).await.unwrap();
        for statement in [
            "SET LOCAL ROLE codex_owner",
            "SET LOCAL search_path TO codex_storage, pg_catalog",
            "SET LOCAL statement_timeout = '5s'",
        ] {
            bounded(sqlx::query(sqlx::AssertSqlSafe(statement)).execute(&mut *tx))
                .await
                .unwrap();
        }
        let pid: i32 = bounded(sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut *tx))
            .await
            .unwrap();
        let locked: (Uuid, String, String) = bounded(sqlx::query_as("SELECT run_id, direction, state FROM storage_migration_runs WHERE run_id=$1 FOR UPDATE").bind(run_id).fetch_one(&mut *tx)).await.unwrap();
        assert_eq!(locked, (run_id, "export".to_owned(), "verified".to_owned()));
        let inject = async {
            bounded(async {
                loop {
                    let blocked: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_locks l WHERE NOT l.granted AND l.pid <> pg_backend_pid() AND l.pid <> $1 AND $1 = ANY(pg_blocking_pids(l.pid)) AND (l.database IS NULL OR l.database = (SELECT oid FROM pg_database WHERE datname = current_database())))")
                        .bind(pid).fetch_one(&mut *observer).await.unwrap();
                    if blocked { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await;
            let intent = read_cutover(home)
                .unwrap()
                .expect("public prepare before retire wait");
            let plan = read_plan(home).unwrap().expect("public owned plan");
            assert_eq!(intent.run_id, run_id);
            assert_eq!(intent.target, ActiveBackend::Local);
            assert_eq!(intent.dataset_id, remote.identity.dataset_id);
            assert_eq!(intent.from_generation, remote.identity.generation);
            assert_eq!(
                intent.to_generation,
                intent.from_generation.checked_add(1).unwrap()
            );
            assert_eq!(intent.format_version, remote.identity.format_version);
            assert_eq!(plan.run_id, run_id);
            assert!(!plan.units.is_empty());
            assert_eq!(plan.staged_home, staged);
            assert_eq!(
                plan.backup_dir,
                home.join("storage-backups")
                    .join(format!("{run_id}-before-return"))
            );
            assert!(!plan.backup_dir.exists());
            assert_eq!(load_authority(home).unwrap(), remote);
            assert_eq!(return_files(&staged), stage_before);
            assert_eq!(sql(&mut observer).await, sql_before);
            let committing = svc.operation(id).unwrap();
            let mut expected = ready.clone();
            expected.state = OperationState::Committing;
            expected.updated_at_ms = committing.updated_at_ms;
            assert_eq!(committing, expected);
            assert_eq!(others(), journals_before);
            let before = files(home);
            let plan_bytes = fs::read(&control).unwrap();
            let held = fault(&control);
            assert_eq!(fs::read(&control).unwrap(), plan_bytes);
            assert_eq!(
                fs::remove_file(&control)
                    .expect_err("mandatory native unlink refusal")
                    .raw_os_error(),
                Some(if cfg!(windows) { 32 } else { 16 })
            );
            bounded(tx.commit()).await.unwrap();
            (held, intent, plan, plan_bytes, before)
        };
        let (result, (held, intent, plan, plan_bytes, before)) =
            tokio::time::timeout(Duration::from_secs(20), async {
                tokio::join!(svc.activate(id), inject)
            })
            .await
            .expect("activate/inject join");
        drop(locker);
        // Prove the phase before expecting the new journal/error policy on BEFORE or AFTER.
        let local = load_authority(home).unwrap();
        let mut expected_local = remote;
        expected_local.identity.generation = intent.to_generation;
        expected_local.marker.generation = intent.to_generation;
        expected_local.marker.active_backend = ActiveBackend::Local;
        assert_eq!(local, expected_local);
        assert_eq!(read_cutover(home).unwrap(), None);
        assert_eq!(read_plan(home).unwrap(), Some(plan.clone()));
        assert_eq!(fs::read(&control).unwrap(), plan_bytes);
        for path in [
            home.join("storage-backups"),
            plan.backup_dir.clone(),
            plan.backup_dir.join("manifest.json"),
        ] {
            let metadata = fs::symlink_metadata(&path).unwrap();
            assert!(!metadata.file_type().is_symlink());
            assert_eq!(
                metadata.is_dir(),
                path.file_name().unwrap() != "manifest.json"
            );
        }
        assert!(verify_backup(&plan).unwrap());
        assert_eq!(
            stable(files(home), home, id),
            installed(before, home, id, &plan)
        );
        let retired = sql(&mut observer).await;
        assert_eq!(retired["activation"]["state"], "retired");
        assert_eq!(retired["activation"]["run_id"], run_id.to_string());
        assert_eq!(
            retired["activation"]["dataset_id"],
            intent.dataset_id.to_string()
        );
        assert_eq!(
            retired["activation"]["generation"].as_u64(),
            Some(intent.to_generation)
        );
        assert_eq!(run(&retired, run_id)["state"], "activated");
        assert_eq!(
            retired["activation"]["activated_at_ms"],
            run(&retired, run_id)["updated_at_ms"]
        );
        assert_eq!(
            retired["activation"]["updated_at_ms"],
            run(&retired, run_id)["updated_at_ms"]
        );
        assert_eq!(retired["domains"], sql_before["domains"]);
        let unchanged = |mut value: serde_json::Value, fields: &[&str]| {
            for field in fields {
                value.as_object_mut().unwrap().remove(*field);
            }
            value
        };
        assert_eq!(
            unchanged(
                retired["activation"].clone(),
                &["state", "generation", "activated_at_ms", "updated_at_ms"]
            ),
            unchanged(
                sql_before["activation"].clone(),
                &["state", "generation", "activated_at_ms", "updated_at_ms"]
            )
        );
        assert_eq!(
            unchanged(run(&retired, run_id), &["state", "updated_at_ms"]),
            unchanged(run(&sql_before, run_id), &["state", "updated_at_ms"])
        );
        let other_runs = |value: &serde_json::Value| {
            value["runs"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["run_id"] != run_id.to_string())
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(other_runs(&retired), other_runs(&sql_before));
        assert_eq!(others(), journals_before);
        eprintln!("service-return-finish-local-observed operation={id} run={run_id}");
        let committing = svc.operation(id).unwrap();
        assert_eq!(committing.state, OperationState::Committing);
        assert_eq!(committing.blocker, Some(BlockerCode::CutoverInProgress));
        let mut expected = ready.clone();
        expected.state = committing.state;
        expected.blocker = committing.blocker;
        expected.updated_at_ms = committing.updated_at_ms;
        assert_eq!(committing, expected);
        assert!(committing.updated_at_ms >= ready.updated_at_ms);
        assert_eq!(result, Err(StorageError(BlockerCode::CutoverInProgress)));
        let retained = files(home);
        assert_eq!(
            serde_json::from_slice::<OperationRecord>(&fs::read(&owner_path).unwrap()).unwrap(),
            committing
        );
        assert_eq!(
            bounded(svc.run_return(prepared)).await,
            Err(StorageError(BlockerCode::OperationConflict))
        );
        assert_eq!(files(home), retained);
        assert_eq!(sql(&mut observer).await, retired);
        assert_eq!(
            bounded(svc.start_return(id, plan_id, CONFIRMED))
                .await
                .unwrap(),
            committing
        );
        assert_eq!(files(home), retained);
        assert_eq!(sql(&mut observer).await, retired);
        assert_eq!(
            bounded(svc.recover()).await,
            Err(StorageError(BlockerCode::AuthorityInvalid))
        );
        assert_eq!(svc.operation(id).unwrap(), committing);
        assert_eq!(files(home), retained);
        assert_eq!(sql(&mut observer).await, retired);
        drop(held);
        let recovered = bounded(svc.recover()).await.unwrap();
        let active = svc.operation(id).unwrap();
        assert_eq!(recovered.outcome, RecoveryKind::RolledForward);
        assert_eq!(recovered.operations, vec![active.clone()]);
        expected.state = OperationState::Active;
        expected.blocker = None;
        expected.updated_at_ms = active.updated_at_ms;
        assert_eq!(active, expected);
        assert!(active.updated_at_ms >= committing.updated_at_ms);
        assert_eq!(others(), journals_before);
        assert_eq!(load_authority(home).unwrap(), local);
        assert_eq!(read_cutover(home).unwrap(), None);
        assert_eq!(read_plan(home).unwrap(), None);
        assert!(verify_backup(&plan).unwrap());
        assert_eq!(sql(&mut observer).await, retired);
        let mut remaining = stable(retained, home, id);
        remaining.remove(&control);
        assert_eq!(stable(files(home), home, id), remaining);
        let settled = files(home);
        assert_eq!(bounded(svc.cancel(id)).await.unwrap(), active);
        assert_eq!(files(home), settled);
        assert_eq!(sql(&mut observer).await, retired);
        drop(observer);
        bounded(pool.close()).await.unwrap();
        eprintln!("service-return-finish-journal-qualified operation={id} run={run_id}");
    }
}
