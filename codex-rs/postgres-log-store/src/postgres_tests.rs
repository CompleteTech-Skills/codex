use super::*;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::path::Path;
use tempfile::TempDir;

fn settings(state: &Path, role: &str) -> ConnectionSettings {
    let receipt: Value = serde_json::from_slice(
        &std::fs::read(state.join("receipt.json")).expect("read isolated PostgreSQL receipt"),
    )
    .expect("parse PostgreSQL receipt");
    ConnectionSettings {
        host: "localhost".to_string(),
        port: receipt["port"].as_u64().expect("PostgreSQL port") as u16,
        database: "codex".to_string(),
        username: format!("codex_{role}"),
        password: std::fs::read_to_string(state.join(format!("secrets/{role}.password")))
            .expect("read private role credential")
            .trim()
            .to_string()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(20),
            max_connections: 8,
        },
    }
}

async fn connect(state: &Path, role: &str) -> Arc<PostgresPool> {
    let pool = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, role), None)
            .await
            .unwrap_or_else(|error| panic!("{role} pool: {error:?}")),
    );
    if role == "runtime" {
        pool.bind_current_activation()
            .await
            .unwrap_or_else(|error| panic!("actual runtime activation binding: {error}"));
    }
    pool
}

fn entry(
    ts: i64,
    level: &str,
    thread: Option<&str>,
    process: Option<&str>,
    token: &str,
    body: Option<&str>,
) -> LogEntry {
    LogEntry {
        ts,
        ts_nanos: ts % 1_000,
        level: level.to_string(),
        target: "parity".to_string(),
        message: None,
        feedback_log_body: body.map(str::to_string),
        thread_id: thread.map(str::to_string),
        process_uuid: process.map(str::to_string),
        module_path: Some(format!("{token}::mod_a")),
        file: Some("src/a.rs".to_string()),
        line: Some(7),
    }
}

fn describe(base: i64, rows: Vec<LogRow>) -> Vec<String> {
    rows.into_iter()
        .map(|row| {
            format!(
                "{} {} {} {} {} {:?} {:?} {:?} {:?} {:?}",
                row.id - base,
                row.ts,
                row.ts_nanos,
                row.level,
                row.target,
                row.message,
                row.thread_id,
                row.process_uuid,
                row.file,
                row.line
            )
        })
        .collect()
}

fn token() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("pgparity{nanos}")
}

/// Runs the log operations a caller can observe and records their results. Row ids are
/// reported relative to the first row the scenario inserts, and every query
/// is scoped to this run's module path so rows from other runs cannot leak in.
async fn scenario(store: &dyn RuntimeLogStore, token: &str, now: i64) -> Vec<String> {
    let mut log = Vec::new();
    let thread = |n: u8| format!("{token}-thread-{n}");
    let process = |n: u8| format!("{token}-process-{n}");
    // Module filters are alternatives, so a query that sets its own pattern carries the token.
    let scoped = |query: LogQuery| LogQuery {
        module_like: if query.module_like.is_empty() {
            vec![token.to_string()]
        } else {
            query.module_like.clone()
        },
        ..query
    };
    let mut message_only = entry(
        now + 4,
        "INFO",
        Some(&thread(1)),
        Some(&process(1)),
        token,
        None,
    );
    message_only.message = Some("fallback message".to_string());
    let batch = vec![
        entry(
            now + 1,
            "INFO",
            Some(&thread(1)),
            Some(&process(1)),
            token,
            Some("one needle"),
        ),
        entry(
            now + 2,
            "warn",
            Some(&thread(1)),
            Some(&process(1)),
            token,
            Some("two\n"),
        ),
        entry(
            now + 3,
            "ERROR",
            Some(&thread(2)),
            Some(&process(2)),
            token,
            Some("three"),
        ),
        message_only,
        entry(
            now + 5,
            "DEBUG",
            None,
            Some(&process(1)),
            token,
            Some("threadless p1"),
        ),
        entry(
            now + 6,
            "INFO",
            None,
            Some(&process(2)),
            token,
            Some("threadless p2"),
        ),
        entry(
            now + 7,
            "INFO",
            None,
            None,
            token,
            Some("threadless no process"),
        ),
        entry(
            now + 8,
            "INFO",
            Some(&thread(1)),
            Some(&process(1)),
            token,
            None,
        ),
    ];
    store.insert_logs(&batch).await.expect("insert batch");
    store.insert_logs(&[]).await.expect("insert empty batch");
    // Rows pruned in earlier runs can leave the counter ahead of the highest stored id, so
    // ids are made relative to this run's first row.
    let first = store
        .query_logs(&scoped(LogQuery {
            limit: Some(1),
            ..Default::default()
        }))
        .await
        .expect("first row");
    let base = first[0].id - 1;

    let queries = vec![
        ("all", LogQuery::default()),
        (
            "descending limit",
            LogQuery {
                descending: true,
                limit: Some(3),
                ..Default::default()
            },
        ),
        (
            "warn",
            LogQuery {
                levels_upper: vec!["WARN".to_string()],
                ..Default::default()
            },
        ),
        (
            "time window",
            LogQuery {
                from_ts: Some(now + 2),
                to_ts: Some(now + 5),
                ..Default::default()
            },
        ),
        (
            "module like",
            LogQuery {
                module_like: vec![format!("{token}::MOD_A")],
                ..Default::default()
            },
        ),
        (
            "file like",
            LogQuery {
                file_like: vec!["SRC/A".to_string()],
                ..Default::default()
            },
        ),
        (
            "thread",
            LogQuery {
                thread_ids: vec![thread(1)],
                ..Default::default()
            },
        ),
        (
            "thread and threadless",
            LogQuery {
                thread_ids: vec![thread(2)],
                include_threadless: true,
                ..Default::default()
            },
        ),
        (
            "threadless",
            LogQuery {
                include_threadless: true,
                ..Default::default()
            },
        ),
        (
            "after id",
            LogQuery {
                after_id: Some(base + 3),
                ..Default::default()
            },
        ),
        (
            "search",
            LogQuery {
                search: Some("needle".to_string()),
                ..Default::default()
            },
        ),
        (
            "search fallback",
            LogQuery {
                search: Some("fallback".to_string()),
                ..Default::default()
            },
        ),
        (
            "combined",
            LogQuery {
                levels_upper: vec!["INFO".to_string()],
                thread_ids: vec![thread(1)],
                after_id: Some(base + 1),
                descending: true,
                limit: Some(2),
                ..Default::default()
            },
        ),
        (
            "no match",
            LogQuery {
                thread_ids: vec![format!("{token}-missing")],
                ..Default::default()
            },
        ),
    ];
    for (label, query) in queries {
        let query = scoped(query);
        let found = store.query_logs(&query).await.expect("query");
        log.push(format!("{label}: {:?}", describe(base, found)));
        let max = store.max_log_id(&query).await.expect("max id");
        log.push(format!(
            "{label} max: {}",
            if max == 0 { 0 } else { max - base }
        ));
    }

    let feedback = |threads: Vec<String>| async move {
        let refs: Vec<&str> = threads.iter().map(String::as_str).collect();
        String::from_utf8(
            store
                .query_feedback_logs_for_threads(&refs)
                .await
                .expect("feedback"),
        )
        .expect("utf8 feedback")
    };
    log.push(format!(
        "feedback one: {:?}",
        feedback(vec![thread(1)]).await
    ));
    log.push(format!(
        "feedback two: {:?}",
        feedback(vec![thread(1), thread(2)]).await
    ));
    log.push(format!(
        "feedback unknown: {:?}",
        feedback(vec![format!("{token}-missing")]).await
    ));
    log.push(format!("feedback none: {:?}", feedback(Vec::new()).await));

    // A thread keeps 1000 rows, so the oldest of 1005 are pruned in the same batch.
    let overflow: Vec<LogEntry> = (0..1_005)
        .map(|index| {
            entry(
                now + 100 + index,
                "INFO",
                Some(&thread(3)),
                Some(&process(3)),
                token,
                Some(&format!("row {index}")),
            )
        })
        .collect();
    store.insert_logs(&overflow).await.expect("insert overflow");
    let kept = store
        .query_logs(&scoped(LogQuery {
            thread_ids: vec![thread(3)],
            ..Default::default()
        }))
        .await
        .expect("query overflow");
    log.push(format!(
        "row limit: {} first {:?} last {:?}",
        kept.len(),
        kept.first().and_then(|row| row.message.clone()),
        kept.last().and_then(|row| row.message.clone())
    ));

    // Byte pruning keeps the newest rows whose cumulative size stays within 10 MiB.
    let big = "x".repeat(4 * 1024 * 1024);
    let heavy: Vec<LogEntry> = (0..3)
        .map(|index| {
            entry(
                now + 5_000 + index,
                "INFO",
                Some(&thread(4)),
                Some(&process(4)),
                token,
                Some(&format!("{index}{big}")),
            )
        })
        .collect();
    store.insert_logs(&heavy).await.expect("insert heavy");
    let kept = store
        .query_logs(&scoped(LogQuery {
            thread_ids: vec![thread(4)],
            ..Default::default()
        }))
        .await
        .expect("query heavy");
    log.push(format!(
        "byte limit: {:?}",
        kept.iter()
            .map(|row| row.message.as_deref().map(|text| text[..1].to_string()))
            .collect::<Vec<_>>()
    ));
    let exported = feedback(vec![thread(4)]).await;
    log.push(format!(
        "feedback within cap: {}",
        exported.len() <= 10 * 1024 * 1024
    ));

    // Threadless rows are capped per process, and rows without a process share one partition.
    for (label, process_uuid) in [("process", Some(process(5))), ("null process", None)] {
        let rows: Vec<LogEntry> = (0..1_002)
            .map(|index| {
                entry(
                    now + 10_000 + index,
                    "INFO",
                    None,
                    process_uuid.as_deref(),
                    token,
                    Some(&format!("{label} {index}")),
                )
            })
            .collect();
        store
            .insert_logs(&rows)
            .await
            .expect("insert threadless overflow");
        let kept = store
            .query_logs(&scoped(LogQuery {
                include_threadless: true,
                search: Some(label.to_string()),
                ..Default::default()
            }))
            .await
            .expect("query threadless overflow");
        log.push(format!(
            "{label} limit: {} first {:?} last {:?}",
            kept.len(),
            kept.first().and_then(|row| row.message.clone()),
            kept.last().and_then(|row| row.message.clone())
        ));
    }
    log
}

async fn real_postgres_logs_match_sqlite() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_LOG_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    bootstrap_codex_storage(&*connect(state, "migrator").await)
        .await
        .expect("bootstrap log schema");
    let postgres = PostgresLogStore::new(connect(state, "runtime").await);
    let sqlite_home = TempDir::new().expect("sqlite fixture home");
    let sqlite = StateRuntime::init(
        SqliteConfig::new_for_testing(sqlite_home.path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("sqlite state runtime");

    let token = token();
    let now = Utc::now().timestamp() * 1_000;
    let expected = scenario(&*sqlite, &token, now).await;
    let actual = scenario(&postgres, &token, now).await;
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(&expected) {
        // Entries can hold multi-megabyte bodies, so only a bounded prefix is shown on failure.
        let shown = |text: &str| text.chars().take(600).collect::<String>();
        assert_eq!(shown(actual), shown(expected));
        assert_eq!(actual.len(), expected.len());
    }
}

async fn real_postgres_logs_allocate_ids_in_commit_order() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_LOG_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    bootstrap_codex_storage(&*connect(state, "migrator").await)
        .await
        .expect("bootstrap log schema");
    let runtime = connect(state, "runtime").await;
    let store = PostgresLogStore::new(runtime.clone());
    let token = token();
    let thread = format!("{token}-concurrent");

    // Concurrent batches get disjoint, contiguous id ranges.
    let mut tasks = Vec::new();
    for writer in 0..8 {
        let store = store.clone();
        let (token, thread) = (token.clone(), thread.clone());
        tasks.push(tokio::spawn(async move {
            for batch in 0..10 {
                let entries: Vec<LogEntry> = (0..5)
                    .map(|item| {
                        entry(
                            1_000_000,
                            "INFO",
                            Some(&thread),
                            None,
                            &token,
                            Some(&format!("{writer}-{batch}-{item}")),
                        )
                    })
                    .collect();
                store
                    .insert_logs(&entries)
                    .await
                    .expect("concurrent insert");
            }
        }));
    }
    for task in tasks {
        task.await.expect("writer task");
    }
    let stored = store
        .query_logs(&LogQuery {
            thread_ids: vec![thread.clone()],
            ..Default::default()
        })
        .await
        .expect("query concurrent rows");
    assert_eq!(stored.len(), 8 * 10 * 5);
    let unique: BTreeSet<i64> = stored.iter().map(|row| row.id).collect();
    assert_eq!(unique.len(), stored.len());
    for writer in 0..8 {
        for batch in 0..10 {
            let prefix = format!("{writer}-{batch}-");
            let ids: Vec<i64> = stored
                .iter()
                .filter(|row| {
                    row.message
                        .as_deref()
                        .is_some_and(|text| text.starts_with(&prefix))
                })
                .map(|row| row.id)
                .collect();
            assert_eq!(ids.len(), 5);
            assert_eq!(ids[4] - ids[0], 4, "batch ids are contiguous");
        }
    }

    // A writer holding the counter blocks later writers, so a higher id never becomes visible
    // before a lower one.
    let before = store
        .max_log_id(&LogQuery::default())
        .await
        .expect("max before");
    let mut holder = runtime.acquire().await.expect("holder connection");
    let mut open = holder.begin().await.expect("begin held write");
    sqlx::query("UPDATE codex_storage.log_id_counter SET last_id = last_id + 1 WHERE singleton")
        .execute(&mut *open)
        .await
        .expect("take held id");
    let waiting = {
        let store = store.clone();
        let entries = vec![entry(
            1_000_001,
            "INFO",
            Some(&thread),
            None,
            &token,
            Some("late"),
        )];
        tokio::spawn(async move { store.insert_logs(&entries).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !waiting.is_finished(),
        "a later writer waits for the earlier commit"
    );
    assert_eq!(
        store
            .max_log_id(&LogQuery::default())
            .await
            .expect("max while held"),
        before
    );
    open.commit().await.expect("commit held write");
    waiting.await.expect("waiting task").expect("late insert");
    let late = store
        .query_logs(&LogQuery {
            search: Some("late".to_string()),
            thread_ids: vec![thread],
            ..Default::default()
        })
        .await
        .expect("query late row");
    assert_eq!(late.len(), 1);
    assert!(
        late[0].id > before + 1,
        "the late row is numbered after the held id"
    );
}

/// Both checks read ids from the single global counter, so they run one after another instead
/// of in parallel on the shared fixture.
#[tokio::test]
async fn real_postgres_logs() {
    real_postgres_logs_match_sqlite().await;
    real_postgres_logs_allocate_ids_in_commit_order().await;
}
