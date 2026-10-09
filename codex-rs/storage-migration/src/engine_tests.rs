use super::*;
use chrono::DateTime;
use chrono::Utc;
use codex_postgres_rollout_store::PostgresRolloutStore;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_postgres_runtime::bootstrap_named_namespace;
use codex_postgres_thread_catalog::PostgresThreadCatalog;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_state::LogEntry;
use codex_state::ProjectRoot;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_state::ThreadGoal;
use codex_state::ThreadGoalStatus;
use codex_state::ThreadMetadata;
use codex_state::ThreadMetadataBuilder;
use codex_state::ThreadSectionAppearance;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

fn settings(state: &Path, role: &str) -> ConnectionSettings {
    settings_for(state, &format!("codex_{role}"), role)
}

/// Settings for any login, reading the password the fixture stored for `password_role`.
fn settings_for(state: &Path, username: &str, password_role: &str) -> ConnectionSettings {
    let receipt: serde_json::Value = serde_json::from_slice(
        &std::fs::read(state.join("receipt.json")).expect("read isolated PostgreSQL receipt"),
    )
    .expect("parse PostgreSQL receipt");
    ConnectionSettings {
        host: "localhost".to_string(),
        port: receipt["port"].as_u64().expect("PostgreSQL port") as u16,
        database: "codex".to_string(),
        username: username.to_string(),
        password: std::fs::read_to_string(state.join(format!("secrets/{password_role}.password")))
            .expect("read private role credential")
            .trim()
            .to_string()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: std::time::Duration::from_secs(5),
            acquire_timeout: std::time::Duration::from_secs(20),
            max_connections: 8,
        },
    }
}

async fn connect(state: &Path, role: &str) -> Arc<PostgresPool> {
    Arc::new(
        PostgresPool::connect(settings(state, role))
            .await
            .unwrap_or_else(|error| panic!("{role} pool: {error:?}")),
    )
}

/// Leave the shared namespace with no migrated data and no run, as a fresh target would be.
/// Project keys cannot be deleted by the runtime role; the live-test fixtures clear them before
/// this test, and a rerun of the same source writes the same keys.
pub(super) async fn reset_target(pool: &PostgresPool) {
    let mut connection = pool.acquire().await.expect("connection");
    for statement in [
        "DELETE FROM storage_migration_runs",
        "UPDATE storage_activation SET state = 'open', run_id = NULL, generation = 0, dataset_id = NULL",
        "DELETE FROM thread_spawn_edges",
        "DELETE FROM threads",
        "DELETE FROM projects",
        "DELETE FROM thread_sections WHERE id <> '01984de2-8f74-7c91-a3b2-5c5e937cf318'",
        "DELETE FROM logs",
        "DELETE FROM memory_stage1_outputs",
        "DELETE FROM memory_jobs",
        "UPDATE memory_consolidation_progress SET max_thread_count = 0",
        "DELETE FROM agent_board_posts",
        "DELETE FROM agent_board_channels",
        "DELETE FROM agent_board_subscriptions",
        "DELETE FROM agent_board_opt_outs",
        "UPDATE log_id_counter SET last_id = 0",
        "UPDATE queue_change_counter SET version = 0",
        "UPDATE agent_board_post_counter SET last_seq = 0",
        "UPDATE thread_timestamp_marks SET updated_at_ms = 0, recency_at_ms = 0",
    ] {
        sqlx::query(statement)
            .execute(&mut *connection)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
}

pub(super) fn metadata(index: i64, base: DateTime<Utc>, source: SessionSource) -> ThreadMetadata {
    metadata_at(
        index,
        base,
        source,
        PathBuf::from(format!("/source-host/rollouts/thread-{index}.jsonl")),
    )
}

fn metadata_at(
    index: i64,
    base: DateTime<Utc>,
    source: SessionSource,
    rollout_path: PathBuf,
) -> ThreadMetadata {
    let created = base + chrono::Duration::seconds(index * 11);
    let mut builder = ThreadMetadataBuilder::new(ThreadId::new(), rollout_path, created, source);
    builder.updated_at = Some(created + chrono::Duration::seconds(7));
    builder.cwd = PathBuf::from(format!("/source-host/work/{index}"));
    builder.git_branch = Some(format!("branch-{index}"));
    builder.model_provider = Some("migration-provider".to_string());
    let mut metadata = builder.build("migration-provider");
    metadata.title = format!("thread {index} — café 🦀");
    metadata.preview = Some(format!("preview {index}"));
    metadata.first_user_message = Some(format!("hello {index}"));
    metadata.tokens_used = index * 100;
    metadata
}

/// Run statements against a SQLite file, creating it when the feature never ran on this home.
async fn sqlite_exec(path: &Path, statements: &[String]) {
    let pool = SqliteConfig::new_for_testing(path.parent().expect("parent").abs())
        .open_read_write_pool(path)
        .await
        .expect("open sqlite file");
    for statement in statements {
        sqlx::query(sqlx::AssertSqlSafe(statement.as_str()))
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    pool.close().await;
}

/// The message board creates its own file on first use; these statements mirror its schema.
const BOARD_SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS deleted_boards (board TEXT PRIMARY KEY NOT NULL)",
    "CREATE TABLE IF NOT EXISTS channels (board TEXT NOT NULL, name TEXT NOT NULL, \
     name_search TEXT NOT NULL, created_at TEXT NOT NULL, timestamp INTEGER NOT NULL, \
     author TEXT NOT NULL, PRIMARY KEY(board,name))",
    "CREATE TABLE IF NOT EXISTS posts (seq INTEGER PRIMARY KEY AUTOINCREMENT, \
     board TEXT NOT NULL, id TEXT NOT NULL, channel TEXT NOT NULL, root TEXT NOT NULL, \
     author TEXT NOT NULL, timestamp INTEGER NOT NULL, body_search TEXT NOT NULL, \
     payload TEXT NOT NULL, request_id TEXT NOT NULL, request TEXT NOT NULL, \
     UNIQUE(board,id), UNIQUE(board,request_id))",
    "CREATE TABLE IF NOT EXISTS subscriptions (board TEXT NOT NULL, target TEXT NOT NULL, \
     agent TEXT NOT NULL, PRIMARY KEY(board,target,agent))",
    "CREATE TABLE IF NOT EXISTS subscription_opt_outs (board TEXT NOT NULL, target TEXT NOT NULL, \
     agent TEXT NOT NULL, PRIMARY KEY(board,target,agent))",
];

/// A SQLite home with every catalog feature in use, built through the real runtime.
async fn populate(home: &Path) -> Vec<ThreadMetadata> {
    let config = SqliteConfig::new_for_testing(home.abs());
    let runtime = StateRuntime::init(config.clone(), "migration-provider".to_string())
        .await
        .expect("sqlite runtime");
    let base = Utc::now() - chrono::Duration::days(3);
    let mut threads: Vec<ThreadMetadata> = Vec::new();
    for index in 0..9 {
        let mut thread = metadata_at(
            index,
            base,
            SessionSource::Cli,
            home.join(format!("rollouts/thread-{index}.jsonl")),
        );
        if index >= 6 {
            thread.source =
                serde_json::to_string(&SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                    parent_thread_id: threads[0].id,
                    depth: 1,
                    agent_path: None,
                    agent_nickname: None,
                    agent_role: None,
                }))
                .expect("source");
        }
        runtime.upsert_thread(&thread).await.expect("upsert");
        threads.push(thread);
    }
    let work = runtime
        .create_thread_section(
            "Work",
            Some(ThreadSectionAppearance {
                icon: Some("star".to_string()),
                color: None,
            }),
        )
        .await
        .expect("section");
    let later = runtime
        .create_thread_section("Later", None)
        .await
        .expect("section");
    for (index, thread) in threads.iter().take(4).enumerate() {
        runtime
            .move_thread_to_section(thread.id, Some(&work.id), None)
            .await
            .expect("move");
        if index == 3 {
            runtime
                .move_thread_to_section(thread.id, Some(&later.id), None)
                .await
                .expect("move again");
        }
    }
    let created = runtime
        .create_project(
            "Migrated project".to_string(),
            vec![ProjectRoot {
                path: "/source-host/project".to_string(),
            }],
            BTreeMap::from([("kind".to_string(), "test".to_string())]),
            &[threads[1].id.to_string(), threads[2].id.to_string()],
            "migration-key",
        )
        .await
        .expect("project");
    assert!(created.created);
    for thread in threads.iter().take(3) {
        for index in 0..2 {
            runtime
                .add_thread_attachment(
                    thread.id,
                    "file",
                    &format!("file-{index}.txt"),
                    &serde_json::json!({"index": index}),
                )
                .await
                .expect("attachment");
        }
    }
    runtime
        .thread_goals()
        .insert_thread_goal(
            threads[0].id,
            "ship the migration",
            ThreadGoalStatus::Active,
            Some(5000),
        )
        .await
        .expect("goal")
        .expect("goal inserted");
    runtime
        .thread_goals()
        .replace_thread_goal_snapshot(&ThreadGoal {
            thread_id: threads[1].id,
            goal_id: "goal-with-deferral".to_string(),
            objective: "defer once".to_string(),
            status: ThreadGoalStatus::Paused,
            token_budget: None,
            tokens_used: 12,
            time_used_seconds: 3,
            created_at: base,
            updated_at: base + chrono::Duration::seconds(5),
        })
        .await
        .expect("goal snapshot");
    let mut item_ids = Vec::new();
    for (thread, count) in [(&threads[0], 3), (&threads[1], 2)] {
        for index in 0..count {
            let item = runtime
                .thread_queue()
                .enqueue(thread.id, &format!("{{\"text\":\"queued {index}\"}}"))
                .await
                .expect("enqueue");
            item_ids.push((thread.id, item.id));
        }
    }
    assert!(
        runtime
            .thread_queue()
            .delete(item_ids[1].0, &item_ids[1].1)
            .await
            .expect("delete queued item")
    );
    runtime
        .set_thread_memory_mode(threads[4].id, "disabled")
        .await
        .expect("memory mode");
    runtime
        .mark_archived(
            threads[5].id,
            Path::new("/source-host/archived/thread-5.jsonl"),
            base + chrono::Duration::days(1),
        )
        .await
        .expect("archive");
    std::fs::create_dir_all(home.join("rollouts")).expect("rollouts directory");
    let header = |thread: &ThreadMetadata, history_base: serde_json::Value| {
        serde_json::json!({
            "timestamp": "2026-09-18T12:00:00Z",
            "type": "session_meta",
            "payload": {"id": thread.id.to_string(), "history_base": history_base},
        })
        .to_string()
    };
    let parent_lines = [
        header(&threads[0], serde_json::Value::Null),
        r#"{"timestamp":"2026-09-18T12:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"héllo 🦀"}}"#.to_string(),
        r#"{"timestamp":"2026-09-18T12:00:02Z","type":"event_msg","payload":{"type":"agent_message","message":"two"}}"#.to_string(),
    ];
    std::fs::write(
        threads[0].rollout_path.clone(),
        format!("{}\n", parent_lines.join("\n")),
    )
    .expect("parent rollout");
    // The fork inherits the first two parent records and writes its own after them.
    let fork_lines = [
        header(
            &threads[1],
            serde_json::json!({
                "thread_id": threads[0].id.to_string(),
                "end_ordinal_exclusive": 2,
                "end_byte_offset": 0
            }),
        ),
        r#"{"timestamp":"2026-09-18T12:00:03Z","type":"event_msg","payload":{"type":"user_message","message":"fork"}}"#.to_string(),
    ];
    std::fs::write(
        threads[1].rollout_path.clone(),
        format!("{}\n\n", fork_lines.join("\n")),
    )
    .expect("fork rollout");
    let run = Uuid::new_v4();
    let entries: Vec<LogEntry> = (0..5)
        .map(|index| LogEntry {
            ts: base.timestamp() + index,
            ts_nanos: 7 + index,
            level: "INFO".to_string(),
            target: "migration".to_string(),
            message: Some(format!("message {index}")),
            feedback_log_body: Some(format!("body {index} 🦀")),
            thread_id: (index % 2 == 0).then(|| format!("thread-{run}")),
            process_uuid: Some(format!("process-{run}")),
            module_path: Some("module".to_string()),
            file: Some("file.rs".to_string()),
            line: Some(index),
        })
        .collect();
    runtime.insert_logs(&entries).await.expect("logs");
    let (first, second) = (threads[0].id, threads[1].id);
    sqlite_exec(
        &config.memories_db_path(),
        &[
            format!(
                "INSERT INTO stage1_outputs (thread_id, source_updated_at, raw_memory, \
                 rollout_summary, rollout_slug, generated_at, usage_count, last_usage, \
                 selected_for_phase2, selected_for_phase2_source_updated_at) VALUES \
                 ('{first}', 100, 'raw one', 'summary one', 'slug-one', 110, 3, 120, 1, 100), \
                 ('{second}', 200, 'raw two', 'summary two', NULL, 210, NULL, NULL, 0, NULL)"
            ),
            format!(
                "INSERT INTO jobs (kind, job_key, status, worker_id, ownership_token, started_at, \
                 finished_at, lease_until, retry_at, retry_remaining, last_error, input_watermark, \
                 last_success_watermark) VALUES \
                 ('memory_stage1', '{first}', 'done', 'w1', 'tok', 1, 2, 3, NULL, 3, NULL, 100, 100), \
                 ('memory_consolidate_global', 'global', 'error', NULL, NULL, NULL, NULL, NULL, \
                 999, 1, 'boom', 5, NULL)"
            ),
            "UPDATE consolidation_progress SET max_thread_count = 7".to_string(),
        ],
    )
    .await;
    let mut board = BOARD_SCHEMA
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    board.extend([
        "INSERT INTO deleted_boards (board) VALUES ('retired-board')".to_string(),
        "INSERT INTO channels VALUES ('main', 'general', 'general', '2026-09-18T12:00:00Z', 1, 'a'), \
         ('main', 'Ünï', 'ünï', '2026-09-18T12:00:01Z', 2, 'b')"
            .to_string(),
        "INSERT INTO posts (board, id, channel, root, author, timestamp, body_search, payload, \
         request_id, request) VALUES \
         ('main', 'p1', 'general', 'p1', 'a', 3, 'hello', '{\"text\":\"hello\"}', 'r1', '{}'), \
         ('main', 'p2', 'general', 'p1', 'b', 4, 'reply', '{\"text\":\"reply\"}', 'r2', '{}'), \
         ('main', 'p3', 'Ünï', 'p3', 'b', 5, 'third', '{\"text\":\"third\"}', 'r3', '{}')"
            .to_string(),
        "INSERT INTO subscriptions VALUES ('main', 'general', 'a'), ('main', 'Ünï', 'b')".to_string(),
        "INSERT INTO subscription_opt_outs VALUES ('main', 'general', 'b')".to_string(),
    ]);
    sqlite_exec(&home.join("agent_message_board_1.sqlite"), &board).await;
    sqlite_exec(
        &config.state_db_path(),
        &["INSERT INTO external_agent_config_imports (import_id, completed_at_ms, successes,            failures, provider_id) VALUES ('import-b', 20, '[\"x\"]', '[]', 'provider'),            ('import-a', 10, '[]', '[\"y\"]', NULL)"
            .to_string()],
    )
    .await;
    // The source owns the checkpointed state, so the runtime must be done writing.
    drop(runtime);
    threads
}

fn migration_fixture_state<'a>(
    required_mode: Option<&std::ffi::OsStr>,
    state: Option<&'a std::ffi::OsStr>,
) -> Result<Option<&'a str>, &'static str> {
    let required = match required_mode {
        None => false,
        Some(mode) if mode == "1" => true,
        Some(_) => return Err("invalid migration fixture required mode; use 1 or leave unset"),
    };
    let state = state.and_then(std::ffi::OsStr::to_str);
    if required && state.is_none_or(|value| value.trim().is_empty()) {
        return Err("required migration fixture state is absent, empty, or not Unicode");
    }
    Ok(state)
}

#[test]
fn migration_fixture_admission_requires_declared_fixture() {
    use std::ffi::OsStr;

    assert_eq!(migration_fixture_state(None, None), Ok(None));
    assert_eq!(
        migration_fixture_state(None, Some(OsStr::new(""))),
        Ok(Some(""))
    );
    for state in [None, Some(OsStr::new("")), Some(OsStr::new(" "))] {
        assert!(migration_fixture_state(Some(OsStr::new("1")), state).is_err());
    }
    assert!(migration_fixture_state(Some(OsStr::new("0")), Some(OsStr::new("owned"))).is_err());
    assert_eq!(
        migration_fixture_state(Some(OsStr::new("1")), Some(OsStr::new("owned"))),
        Ok(Some("owned"))
    );
    #[cfg(windows)]
    let invalid = {
        use std::os::windows::ffi::OsStringExt;
        std::ffi::OsString::from_wide(&[0xD800])
    };
    #[cfg(unix)]
    let invalid = {
        use std::os::unix::ffi::OsStringExt;
        std::ffi::OsString::from_vec(vec![0xff])
    };
    #[cfg(any(windows, unix))]
    {
        assert_eq!(migration_fixture_state(None, Some(&invalid)), Ok(None));
        assert!(migration_fixture_state(Some(OsStr::new("1")), Some(&invalid)).is_err());
        assert!(migration_fixture_state(Some(&invalid), Some(OsStr::new("owned"))).is_err());
    }
}

#[tokio::test]
async fn real_postgres_catalog_migration() {
    let required_mode = std::env::var_os("CODEX_TEST_POSTGRES_REQUIRED");
    let state = std::env::var_os("CODEX_TEST_POSTGRES_MIGRATION_STATE");
    let Some(state) = migration_fixture_state(required_mode.as_deref(), state.as_deref())
        .expect("migration fixture admission failed")
    else {
        return;
    };
    let state = Path::new(&state);
    bootstrap_codex_storage(&*connect(state, "migrator").await)
        .await
        .expect("bootstrap migration schema");
    let default_pool = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, "runtime"), /*namespace*/ None)
            .await
            .expect("default namespace pool"),
    );
    scenario(default_pool).await;

    // The same scenario runs unchanged in a separate named namespace of the same database.
    let namespace = NamedNamespace::new("codex_storage_isolation").expect("named namespace");
    let migrator = PostgresPool::connect(settings_for(
        state,
        namespace.migrator_login(),
        "isolation_migrator",
    ))
    .await
    .expect("named migrator pool");
    bootstrap_named_namespace(&migrator, &namespace)
        .await
        .expect("bootstrap named namespace");
    // Earlier failed runs leave rows the runtime role cannot delete, so the owner clears them.
    let mut owner = migrator.acquire().await.expect("named migrator connection");
    for statement in [
        "SET ROLE codex_isolation_owner",
        "TRUNCATE codex_storage_isolation.project_idempotency_keys,          codex_storage_isolation.queued_thread_revisions,          codex_storage_isolation.agent_board_deleted,          codex_storage_isolation.external_agent_config_imports,          codex_storage_isolation.threads, codex_storage_isolation.projects CASCADE",
        "RESET ROLE",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&mut *owner)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    drop(owner);
    let named_pool = Arc::new(
        PostgresPool::connect_in_namespace(
            settings_for(state, namespace.runtime_login(), "isolation_runtime"),
            Some(&namespace),
        )
        .await
        .expect("named namespace pool"),
    );
    scenario(named_pool).await;
}

async fn scenario(pool: Arc<PostgresPool>) {
    let home = tempfile::tempdir().expect("source home");
    let threads = populate(home.path()).await;
    let config = SqliteConfig::new_for_testing(home.path().abs());
    let source = SqliteSource::new(config.clone());

    // An interrupted run resumes from its checkpoints, and nobody else can take the store.
    reset_target(&pool).await;
    let interrupted = Migrator::new(source.clone(), pool.clone())
        .with_batch_size(2)
        .with_batch_limit(3)
        .import()
        .await;
    assert!(
        matches!(interrupted, Err(MigrationError::Interrupted)),
        "{interrupted:?}"
    );
    let competing = Migrator::new(
        SqliteSource::new(SqliteConfig::new_for_testing(
            tempfile::tempdir().expect("other").path().abs(),
        )),
        pool.clone(),
    )
    .import()
    .await;
    assert!(
        matches!(competing, Err(MigrationError::TargetBusy)),
        "{competing:?}"
    );
    let summary = Migrator::new(source.clone(), pool.clone())
        .with_batch_size(2)
        .import()
        .await
        .expect("resumed import");
    assert!(summary.resumed);
    let report = Migrator::new(source.clone(), pool.clone())
        .with_batch_size(2)
        .verify(summary.run_id)
        .await
        .expect("verification");
    assert_eq!(
        report
            .domains
            .iter()
            .map(|(domain, digest)| (domain.name(), digest.count))
            .collect::<Vec<_>>(),
        vec![
            ("sections", 3),
            ("projects", 1),
            ("project_keys", 1),
            ("threads", 9),
            ("attachments", 6),
            ("spawn_edges", 3),
            ("rollouts", 9),
            ("goals", 2),
            ("queued_items", 4),
            ("queue_revisions", 2),
            ("logs", 5),
            ("memory_outputs", 2),
            ("memory_jobs", 2),
            ("memory_progress", 1),
            ("board_deleted", 1),
            ("board_channels", 2),
            ("board_posts", 3),
            ("board_subscriptions", 2),
            ("board_opt_outs", 1),
            ("external_imports", 2),
        ]
    );

    // Counters resume after every imported value, so new work never reuses an imported id.
    let counters: [(&str, &str); 5] = [
        (
            "SELECT last_id FROM log_id_counter",
            "SELECT MAX(id) FROM logs",
        ),
        (
            "SELECT version FROM queue_change_counter",
            "SELECT MAX(revision) FROM queued_thread_revisions",
        ),
        (
            "SELECT last_seq FROM agent_board_post_counter",
            "SELECT MAX(seq) FROM agent_board_posts",
        ),
        (
            "SELECT updated_at_ms FROM thread_timestamp_marks",
            "SELECT MAX(updated_at_ms) FROM threads",
        ),
        (
            "SELECT recency_at_ms FROM thread_timestamp_marks",
            "SELECT MAX(recency_at_ms) FROM threads",
        ),
    ];
    for (counter, highest) in counters {
        let mut connection = pool.acquire().await.expect("connection");
        let counter_value: i64 = sqlx::query_scalar(counter)
            .fetch_one(&mut *connection)
            .await
            .expect(counter);
        let highest_value: i64 = sqlx::query_scalar(highest)
            .fetch_one(&mut *connection)
            .await
            .expect(highest);
        assert!(
            counter_value >= highest_value,
            "{counter}: {counter_value} < {highest_value}"
        );
    }

    // The migrated catalog answers exactly as the source does.
    let runtime = StateRuntime::init(config.clone(), "migration-provider".to_string())
        .await
        .expect("source runtime");
    let catalog = PostgresThreadCatalog::new(pool.clone());
    for thread in &threads {
        assert_eq!(
            catalog.get_thread(thread.id).await.expect("target thread"),
            runtime.get_thread(thread.id).await.expect("source thread"),
            "thread {}",
            thread.id
        );
    }
    drop(runtime);
    let rollouts = PostgresRolloutStore::new(pool.clone());
    let expected_fork = {
        let parent = std::fs::read_to_string(&threads[0].rollout_path).expect("parent");
        let fork = std::fs::read_to_string(&threads[1].rollout_path).expect("fork");
        parent
            .lines()
            .take(2)
            .chain(fork.lines().filter(|line| !line.is_empty()))
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        rollouts
            .read_all(threads[1].id)
            .await
            .expect("fork lines")
            .into_iter()
            .map(|stored| stored.line)
            .collect::<Vec<_>>(),
        expected_fork
    );
    assert_eq!(
        rollouts.next_position(threads[2].id).await.expect("empty"),
        0
    );

    // A second run on a populated target is refused, a changed source fails verification, and a
    // tampered row is caught.
    reset_target(&pool).await;
    let first = Migrator::new(source.clone(), pool.clone())
        .import()
        .await
        .expect("import again");
    sqlx::query("UPDATE threads SET title = 'tampered' WHERE id = $1::uuid")
        .bind(threads[3].id.to_string())
        .execute(&mut *pool.acquire().await.expect("connection"))
        .await
        .expect("tamper");
    let mismatch = Migrator::new(source.clone(), pool.clone())
        .verify(first.run_id)
        .await;
    assert!(
        matches!(
            mismatch,
            Err(MigrationError::Mismatch { domain: "threads" })
        ),
        "{mismatch:?}"
    );
    let runtime = StateRuntime::init(config.clone(), "migration-provider".to_string())
        .await
        .expect("runtime");
    runtime
        .upsert_thread(&metadata(99, Utc::now(), SessionSource::Cli))
        .await
        .expect("late thread");
    drop(runtime);
    // Repair the tampered row so only the late source change remains.
    sqlx::query("UPDATE threads SET title = $2 WHERE id = $1::uuid")
        .bind(threads[3].id.to_string())
        .bind(&threads[3].title)
        .execute(&mut *pool.acquire().await.expect("connection"))
        .await
        .expect("repair");
    let changed = Migrator::new(source.clone(), pool.clone())
        .verify(first.run_id)
        .await;
    assert!(
        matches!(changed, Err(MigrationError::Mismatch { domain: "threads" })),
        "{changed:?}"
    );
    super::gate_tests::gate_phase(&pool, &source, threads[0].id).await;
    super::cutover_tests::cutover_phase(&pool, &source, home.path()).await;
    super::export_tests::export_phase(&pool, &source, &threads).await;
    super::return_tests::return_phase(&pool, &source, home.path(), &threads).await;
}
