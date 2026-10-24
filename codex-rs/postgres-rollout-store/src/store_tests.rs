use super::*;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_postgres_thread_catalog::PostgresThreadCatalog;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::path::Path;

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
            max_connections: 12,
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

async fn insert_thread(pool: &PostgresPool, thread_id: ThreadId) {
    let mut connection = pool.acquire().await.expect("runtime connection");
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

fn batch(lines: &[&str]) -> Vec<(Option<u64>, String)> {
    lines
        .iter()
        .enumerate()
        .map(|(index, line)| (Some(index as u64), (*line).to_string()))
        .collect()
}

#[tokio::test]
async fn real_postgres_rollouts() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_ROLLOUT_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    bootstrap_codex_storage(&*connect(state, "migrator").await)
        .await
        .expect("bootstrap rollout schema");
    let pool = connect(state, "runtime").await;
    let store = PostgresRolloutStore::new(pool.clone());
    let thread = ThreadId::new();
    insert_thread(&pool, thread).await;

    // Lines come back exactly as stored, in position order, including unusual content.
    let unusual = format!(
        "{{\"timestamp\":\"2026-01-01T00:00:00.000Z\",\"text\":\"{}\"}}",
        "é\\u0000🦀\\n\\\"quoted\\\" \u{2028} \u{feff} tab\\t"
    );
    let large = format!("{{\"blob\":\"{}\"}}", "x".repeat(2 * 1024 * 1024));
    assert_eq!(store.next_position(thread).await, Ok(0));
    let first = batch(&["{\"a\":1}", &unusual, &large]);
    assert_eq!(store.append(thread, 0, first.clone()).await, Ok(3));
    let stored = store.read_all(thread).await.expect("read all");
    assert_eq!(
        stored
            .iter()
            .map(|line| (line.position, line.ordinal, line.line.clone()))
            .collect::<Vec<_>>(),
        first
            .iter()
            .enumerate()
            .map(|(index, (ordinal, line))| (index as u64, *ordinal, line.clone()))
            .collect::<Vec<_>>()
    );
    let page = store.read(thread, 1, 1).await.expect("read page");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].position, 1);

    // A retry after an ambiguous commit is recognized, and a different batch is a conflict.
    assert_eq!(store.append(thread, 0, first.clone()).await, Ok(3));
    assert_eq!(store.next_position(thread).await, Ok(3));
    assert_eq!(
        store.append(thread, 0, batch(&["{\"a\":2}"])).await,
        Err(RolloutStoreError::Conflict {
            expected: 0,
            stored: 3
        })
    );
    assert_eq!(
        store.append(thread, 5, batch(&["{\"gap\":true}"])).await,
        Err(RolloutStoreError::Conflict {
            expected: 5,
            stored: 3
        })
    );
    assert_eq!(store.append(thread, 3, batch(&["{\"b\":1}"])).await, Ok(4));
    assert_eq!(
        store
            .append(thread, 3, batch(&["{\"b\":1}", "{\"c\":1}"]))
            .await,
        Err(RolloutStoreError::Conflict {
            expected: 3,
            stored: 4
        })
    );
    assert_eq!(store.append(thread, 4, Vec::new()).await, Ok(4));

    // Racing writers at one position: exactly one batch lands, the others are told.
    let racing = ThreadId::new();
    insert_thread(&pool, racing).await;
    let results = futures_join_all((0..8).map(|index| {
        let store = store.clone();
        async move {
            store
                .append(racing, 0, batch(&[&format!("{{\"writer\":{index}}}")]))
                .await
        }
    }))
    .await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(store.next_position(racing).await, Ok(1));

    // Reverting and forking work on whole prefixes.
    assert_eq!(store.truncate(thread, 2).await, Ok(2));
    assert_eq!(store.next_position(thread).await, Ok(2));
    assert_eq!(
        store.append(thread, 2, batch(&["{\"again\":true}"])).await,
        Ok(3)
    );
    let fork = ThreadId::new();
    insert_thread(&pool, fork).await;
    assert_eq!(store.copy_prefix(thread, fork, 3).await, Ok(()));
    assert_eq!(
        store
            .read_all(fork)
            .await
            .expect("read fork")
            .into_iter()
            .map(|line| line.line)
            .collect::<Vec<_>>(),
        store
            .read_all(thread)
            .await
            .expect("read source")
            .into_iter()
            .map(|line| line.line)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        store.copy_prefix(thread, fork, 3).await,
        Err(RolloutStoreError::Conflict {
            expected: 0,
            stored: 3
        })
    );
    let short = ThreadId::new();
    insert_thread(&pool, short).await;
    assert_eq!(
        store.copy_prefix(thread, short, 99).await,
        Err(RolloutStoreError::Corrupt(
            "the source holds 3 of the 99 requested lines".to_string()
        ))
    );

    // Lines cannot exist without their thread, and disappear with it.
    let missing = ThreadId::new();
    assert_eq!(
        store.append(missing, 0, batch(&["{}"])).await,
        Err(RolloutStoreError::MissingThread(missing))
    );
    assert_eq!(
        store.next_position(missing).await,
        Err(RolloutStoreError::MissingThread(missing))
    );
    let catalog = PostgresThreadCatalog::new(pool.clone());
    assert_eq!(catalog.delete_thread(thread).await.expect("delete"), 1);
    assert_eq!(store.read_all(thread).await, Ok(Vec::new()));
}

async fn futures_join_all<I, F, T>(futures: I) -> Vec<T>
where
    I: IntoIterator<Item = F>,
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handles: Vec<_> = futures.into_iter().map(tokio::spawn).collect();
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.expect("racing task"));
    }
    results
}

#[tokio::test]
#[ignore = "requires ROOT-admitted isolated canonical rollout fixture"]
async fn actual_direct_append_refusal_preserves_transaction_and_retry() {
    let state = std::env::var("CODEX_TEST_POSTGRES_ROLLOUT_STORE_STATE")
        .expect("declared real PostgreSQL fixture required");
    let pool = connect(Path::new(&state), "runtime").await;
    let thread = ThreadId::new();
    insert_thread(&pool, thread).await;
    let store = PostgresRolloutStore::new(Arc::clone(&pool));
    let exact = vec![
        (None, "opaque Unicode \u{1f980}\r\n".to_owned()),
        (
            Some(i64::MAX as u64),
            "unknown JSON fields retained".to_owned(),
        ),
    ];
    let mut connection = pool.acquire().await.expect("original connection");
    let mut tx = connection.begin().await.expect("caller transaction");
    let bad = vec![
        (Some(0), "must not be inserted".to_owned()),
        (Some(u64::MAX), "last invalid ordinal".to_owned()),
    ];
    assert!(matches!(
        append_in(&mut tx, thread, 0, &bad).await,
        Err(RolloutStoreError::InvalidRequest(_))
    ));
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM thread_rollout_lines WHERE thread_id = $1::uuid")
            .bind(thread.to_string())
            .fetch_one(&mut *tx)
            .await
            .expect("same transaction unchanged");
    assert_eq!(count, 0);
    assert_eq!(append_in(&mut tx, thread, 0, &exact).await, Ok(2));
    tx.commit().await.expect("actual original commit");
    drop(connection);
    let committed = store
        .read_all(thread)
        .await
        .expect("committed exact payload");
    assert_eq!(
        committed
            .iter()
            .map(|line| (line.ordinal, line.line.clone()))
            .collect::<Vec<_>>(),
        exact
    );
    // Replay the exact original batch, simulating a lost acknowledgement only.
    assert_eq!(store.append(thread, 0, exact.clone()).await, Ok(2));
    assert_eq!(
        store.read_all(thread).await.expect("retry history"),
        committed
    );
    assert!(matches!(
        store.append(thread, 2, bad).await,
        Err(RolloutStoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        store.truncate(thread, u64::MAX).await,
        Err(RolloutStoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        store.read(thread, u64::MAX, 1).await,
        Err(RolloutStoreError::InvalidRequest(_))
    ));
    assert_eq!(
        store
            .read_all(thread)
            .await
            .expect("all refusals preserve canonical data"),
        committed
    );
}
