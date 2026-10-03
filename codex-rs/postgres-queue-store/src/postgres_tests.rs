use super::*;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_thread_store::LocalQueueStore;
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

async fn insert_thread(runtime: &PostgresPool, thread_id: ThreadId) {
    let mut connection = runtime.acquire().await.expect("runtime connection");
    sqlx::query(
        "INSERT INTO codex_storage.threads (id, origin_rollout_path, created_at_ms, \
         updated_at_ms, recency_at_ms, source, history_mode, model_provider, origin_cwd, \
         cli_version, title, sandbox_policy, approval_mode) \
         VALUES ($1::uuid, 'origin', 1, 1, 1, 'cli', 'legacy', 'provider', 'cwd', '1', 'title', \
         'sandbox', 'approval')",
    )
    .bind(thread_id.to_string())
    .execute(&mut *connection)
    .await
    .expect("insert thread row");
}

fn payloads(records: &[QueuedUserSubmissionRecord]) -> Vec<&str> {
    records
        .iter()
        .map(|record| record.payload.as_str())
        .collect()
}

fn failure(error: ThreadStoreError) -> String {
    error.to_string()
}

/// Runs the queue operations a caller can observe and records their results. Item ids differ
/// between stores, so they are tracked by position instead of value.
async fn scenario(store: &dyn QueueStore, first: ThreadId, second: ThreadId) -> Vec<String> {
    let mut log = Vec::new();
    macro_rules! record {
        ($label:expr, $value:expr) => {
            log.push(format!("{}: {:?}", $label, $value));
        };
    }
    record!(
        "empty page",
        store.list_page(first, 0, 10).await.expect("empty").len()
    );
    record!(
        "empty changes",
        store.changes_since(0, &[]).await.expect("none")
    );

    let mut items = Vec::new();
    for payload in ["one", "two", "three"] {
        let record = store
            .enqueue(first, payload.to_string())
            .await
            .expect("enqueue");
        assert_eq!(record.thread_id, first);
        items.push(record);
    }
    let other = store
        .enqueue(second, "other".to_string())
        .await
        .expect("enqueue other thread");
    record!("enqueued", payloads(&items));

    let page = |offset, limit| async move {
        let records = store
            .list_page(first, offset, limit)
            .await
            .expect("list page");
        records
            .iter()
            .map(|record| record.payload.clone())
            .collect::<Vec<_>>()
    };
    record!("page 0..2", page(0, 2).await);
    record!("page 2..4", page(2, 2).await);
    record!("page past end", page(10, 5).await);

    let initial = store
        .changes_since(0, &[first, second])
        .await
        .expect("initial changes");
    record!(
        "initial changes order",
        initial
            .iter()
            .map(|(thread, _)| *thread == first)
            .collect::<Vec<_>>()
    );
    let first_revision = initial
        .iter()
        .find(|(thread, _)| *thread == first)
        .expect("first revision")
        .1;
    let second_revision = initial
        .iter()
        .find(|(thread, _)| *thread == second)
        .expect("second revision")
        .1;
    assert!(
        first_revision < second_revision,
        "the later write has the later revision"
    );

    record!(
        "update",
        store
            .update(first, items[1].id.clone(), "two!".to_string())
            .await
            .expect("update")
            .map(|record| record.payload)
    );
    record!(
        "update unknown",
        store
            .update(first, "missing".to_string(), "x".to_string())
            .await
            .expect("update unknown")
    );
    record!(
        "update other thread's item",
        store
            .update(first, other.id.clone(), "hijack".to_string())
            .await
            .expect("cross-thread update")
    );
    record!(
        "changes after first only",
        store
            .changes_since(second_revision, &[first, second])
            .await
            .expect("changes")
            .iter()
            .map(|(thread, _)| *thread == first)
            .collect::<Vec<_>>()
    );
    record!(
        "unloaded threads are not reported",
        store
            .changes_since(0, &[ThreadId::new()])
            .await
            .expect("unloaded")
    );

    record!(
        "reorder",
        store
            .reorder(
                first,
                vec![
                    items[2].id.clone(),
                    items[0].id.clone(),
                    items[1].id.clone()
                ],
            )
            .await
            .map_err(failure)
    );
    record!("order after reorder", page(0, 10).await);
    record!(
        "reorder missing item",
        store
            .reorder(first, vec![items[0].id.clone(), items[1].id.clone()])
            .await
            .map_err(failure)
    );
    record!(
        "reorder duplicate",
        store
            .reorder(
                first,
                vec![
                    items[0].id.clone(),
                    items[0].id.clone(),
                    items[1].id.clone()
                ],
            )
            .await
            .map_err(failure)
    );
    record!(
        "reorder foreign item",
        store
            .reorder(
                first,
                vec![items[0].id.clone(), items[1].id.clone(), other.id.clone()],
            )
            .await
            .map_err(failure)
    );
    record!("order after rejected reorders", page(0, 10).await);

    record!(
        "delete other thread's item",
        store
            .delete(first, other.id.clone())
            .await
            .expect("delete foreign")
    );
    record!(
        "delete",
        store
            .delete(first, items[0].id.clone())
            .await
            .expect("delete")
    );
    record!(
        "delete again",
        store
            .delete(first, items[0].id.clone())
            .await
            .expect("delete again")
    );
    record!("order after delete", page(0, 10).await);

    // The second thread already holds one item, so one fewer fills it.
    for index in 0..MAX_QUEUE_ITEMS - 1 {
        store
            .enqueue(second, format!("fill {index}"))
            .await
            .unwrap_or_else(|error| panic!("fill {index}: {error}"));
    }
    record!(
        "enqueue past the limit",
        store
            .enqueue(second, "overflow".to_string())
            .await
            .map(|record| record.payload)
            .map_err(failure)
    );
    log
}

#[tokio::test]
async fn real_postgres_queue_matches_sqlite() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_QUEUE_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("migrator pool");
    bootstrap_codex_storage(&migrator)
        .await
        .expect("bootstrap queue schema");
    let runtime = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
            .await
            .expect("runtime pool"),
    );
    runtime
        .bind_current_activation()
        .await
        .expect("actual runtime activation binding");
    let postgres = PostgresQueueStore::new(runtime.clone());
    let sqlite_home = TempDir::new().expect("sqlite fixture home");
    let sqlite_runtime = StateRuntime::init(
        SqliteConfig::new_for_testing(sqlite_home.path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("sqlite state runtime");
    let sqlite = LocalQueueStore::new(sqlite_runtime);

    let (sqlite_first, sqlite_second) = (ThreadId::new(), ThreadId::new());
    let (postgres_first, postgres_second) = (ThreadId::new(), ThreadId::new());
    insert_thread(&runtime, postgres_first).await;
    insert_thread(&runtime, postgres_second).await;
    let expected = scenario(&sqlite, sqlite_first, sqlite_second).await;
    let actual = scenario(&postgres, postgres_first, postgres_second).await;
    assert_eq!(actual, expected);
}

async fn real_postgres_queue_serializes_writers_and_orders_versions_by_commit() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_QUEUE_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("migrator pool");
    bootstrap_codex_storage(&migrator)
        .await
        .expect("bootstrap queue schema");
    let runtime = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
            .await
            .expect("runtime pool"),
    );
    runtime
        .bind_current_activation()
        .await
        .expect("actual runtime activation binding");
    let store = PostgresQueueStore::new(runtime.clone());
    let thread = ThreadId::new();
    insert_thread(&runtime, thread).await;

    // Concurrent writers cannot exceed the per-thread limit or reuse a queue position.
    let attempts = MAX_QUEUE_ITEMS + 20;
    let mut tasks = Vec::new();
    for index in 0..attempts {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store.enqueue(thread, format!("item {index}")).await
        }));
    }
    let mut accepted = 0;
    let mut rejected = 0;
    for task in tasks {
        match task.await.expect("enqueue task") {
            Ok(_) => accepted += 1,
            Err(ThreadStoreError::InvalidRequest { .. }) => rejected += 1,
            Err(error) => panic!("unexpected enqueue failure: {error}"),
        }
    }
    assert_eq!(
        (accepted, rejected),
        (MAX_QUEUE_ITEMS, attempts - MAX_QUEUE_ITEMS)
    );
    let queued = store
        .list_page(thread, 0, attempts)
        .await
        .expect("list after concurrent enqueue");
    assert_eq!(queued.len(), MAX_QUEUE_ITEMS);

    // A writer that has taken its version but not committed blocks later writers, so a later
    // version can never become visible before an earlier one.
    let held_thread = ThreadId::new();
    insert_thread(&runtime, held_thread).await;
    let before = store.change_version().await.expect("version before");
    let mut holder = runtime.acquire().await.expect("holder connection");
    let mut open = holder.begin().await.expect("begin held write");
    let held_version = next_version(&mut open).await.expect("take held version");
    assert!(
        held_version > before,
        "a new write is numbered after every visible version"
    );
    let waiting = {
        let store = store.clone();
        tokio::spawn(async move { store.enqueue(held_thread, "late".to_string()).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !waiting.is_finished(),
        "a later write must wait for the earlier commit"
    );
    // Holding the counter lock stops every other writer from committing, so the visible
    // version is exactly the one before ours.
    assert_eq!(
        store.change_version().await.expect("version while held"),
        held_version - 1,
        "an uncommitted version is not visible"
    );
    record_revision(&mut open, held_thread, held_version)
        .await
        .expect("publish held revision");
    open.commit().await.expect("commit held write");
    waiting
        .await
        .expect("waiting task")
        .expect("late write completes after the earlier commit");
    let changes = store
        .changes_since(before, &[held_thread])
        .await
        .expect("changes after held write");
    assert_eq!(changes.len(), 1);
    assert!(
        changes[0].1 > held_version,
        "the late write is numbered after the earlier commit"
    );
    assert!(
        store.change_version().await.expect("version after") > held_version,
        "both writes are visible once committed"
    );
}

async fn real_postgres_queue_changes_survive_reconnect_and_thread_removal() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_QUEUE_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("migrator pool");
    bootstrap_codex_storage(&migrator)
        .await
        .expect("bootstrap queue schema");
    let first_pool = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
            .await
            .expect("first runtime pool"),
    );
    first_pool
        .bind_current_activation()
        .await
        .expect("actual runtime activation binding");
    let first = PostgresQueueStore::new(first_pool.clone());
    let thread = ThreadId::new();
    insert_thread(&first_pool, thread).await;
    let item = first
        .enqueue(thread, "durable".to_string())
        .await
        .expect("enqueue");
    let version = first.change_version().await.expect("version");
    let recorded = first
        .changes_since(0, &[thread])
        .await
        .expect("changes before reconnect");
    first_pool.close().await.expect("close first pool");

    let second_pool = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
            .await
            .expect("second runtime pool"),
    );
    second_pool
        .bind_current_activation()
        .await
        .expect("actual runtime activation binding");
    let second = PostgresQueueStore::new(second_pool.clone());
    // Other tests share the counter, so it may have moved on, but it never goes backwards.
    assert!(second.change_version().await.expect("version") >= version);
    assert_eq!(
        second.changes_since(0, &[thread]).await.expect("changes"),
        recorded
    );
    assert_eq!(
        payloads(&second.list_page(thread, 0, 5).await.expect("list")),
        vec!["durable"]
    );

    // Removing a thread's queue is itself a change a watcher can observe.
    assert!(
        second
            .delete_thread_queue(thread)
            .await
            .expect("delete queue")
    );
    assert!(
        !second
            .delete_thread_queue(thread)
            .await
            .expect("delete empty queue")
    );
    assert!(
        second
            .list_page(thread, 0, 5)
            .await
            .expect("list")
            .is_empty()
    );
    let removal = second
        .changes_since(version, &[thread])
        .await
        .expect("removal change");
    assert_eq!(removal.len(), 1);
    assert!(removal[0].1 > version);
    assert!(
        !second
            .delete(thread, item.id)
            .await
            .expect("delete removed item")
    );

    // A queue needs its thread row; the missing row is a typed rejection.
    let orphan = ThreadId::new();
    assert_eq!(
        second
            .enqueue(orphan, "orphan".to_string())
            .await
            .map_err(failure),
        Err(ThreadStoreError::ThreadNotFound { thread_id: orphan }.to_string())
    );
}

/// These checks read and compare the single global change counter, so they run one after
/// another in one test instead of in parallel with other tests on the shared fixture.
#[tokio::test]
async fn real_postgres_queue_store_orders_and_persists_changes() {
    real_postgres_queue_serializes_writers_and_orders_versions_by_commit().await;
    real_postgres_queue_changes_survive_reconnect_and_thread_removal().await;
}
