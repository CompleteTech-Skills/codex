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
