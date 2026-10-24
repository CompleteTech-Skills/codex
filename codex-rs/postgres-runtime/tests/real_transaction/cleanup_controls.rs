use super::PostgresPool;
use super::cleanup_transaction_probe_with_deadline;
use super::exercise_transaction_outcomes;
use super::join_error_diagnostic;
use super::settings;
use super::transaction_probe_oid;
use sqlx::Acquire;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

const CLEANUP_STATE_ENV: &str = "CODEX_TEST_POSTGRES_TRANSACTION_CLEANUP_STATE";
const QUEUED_LOCK_SQL: &str = "SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_locks AS waiting
    JOIN pg_catalog.pg_stat_activity AS activity USING (pid)
    WHERE waiting.locktype = 'relation'
      AND waiting.relation::bigint = $1
      AND waiting.mode = 'AccessExclusiveLock'
      AND NOT waiting.granted
      AND activity.wait_event_type = 'Lock'
      AND activity.query LIKE 'LOCK TABLE codex_storage.transaction_probe IN ACCESS EXCLUSIVE MODE%'
)";

#[derive(Debug, Eq, PartialEq)]
struct ProbeSnapshot {
    oid: Option<i64>,
    row_count: i64,
    row_value: Option<i32>,
}

fn cleanup_state() -> Option<PathBuf> {
    match std::env::var(CLEANUP_STATE_ENV) {
        Ok(state) => {
            assert!(!state.is_empty(), "{CLEANUP_STATE_ENV} must not be empty");
            Some(PathBuf::from(state))
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("read {CLEANUP_STATE_ENV}: {error}"),
    }
}

async fn create_owned_probe(state: &Path, row_value: i32) -> u32 {
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("connect cleanup-control migrator");
    let mut connection = migrator
        .acquire()
        .await
        .expect("acquire cleanup-control migrator");
    let mut owner = connection
        .begin()
        .await
        .expect("begin owned cleanup-control fixture");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *owner)
        .await
        .expect("assume owner for cleanup-control fixture");
    sqlx::query(
        "CREATE TABLE codex_storage.transaction_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
    )
    .execute(&mut *owner)
    .await
    .expect("create owned cleanup-control probe");
    sqlx::query("INSERT INTO codex_storage.transaction_probe VALUES (1, $1)")
        .bind(row_value)
        .execute(&mut *owner)
        .await
        .expect("seed owned cleanup-control probe");
    let oid = transaction_probe_oid(&mut owner)
        .await
        .expect("read owned cleanup-control probe OID")
        .expect("owned cleanup-control probe has an OID");
    owner
        .commit()
        .await
        .expect("commit owned cleanup-control probe");
    drop(connection);
    migrator
        .close()
        .await
        .expect("close cleanup-control migrator pool");
    u32::try_from(oid).expect("PostgreSQL relation OID fits in u32")
}

async fn replace_owned_probe(state: &Path, row_value: i32) -> u32 {
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("connect replacement-control migrator");
    let mut connection = migrator
        .acquire()
        .await
        .expect("acquire replacement-control migrator");
    let mut owner = connection
        .begin()
        .await
        .expect("begin replacement-control fixture");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *owner)
        .await
        .expect("assume owner for replacement-control fixture");
    sqlx::query("DROP TABLE codex_storage.transaction_probe")
        .execute(&mut *owner)
        .await
        .expect("replace test-owned transaction probe");
    sqlx::query(
        "CREATE TABLE codex_storage.transaction_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)",
    )
    .execute(&mut *owner)
    .await
    .expect("create replacement sentinel relation");
    sqlx::query("INSERT INTO codex_storage.transaction_probe VALUES (1, $1)")
        .bind(row_value)
        .execute(&mut *owner)
        .await
        .expect("seed replacement sentinel relation");
    let oid = transaction_probe_oid(&mut owner)
        .await
        .expect("read replacement sentinel OID")
        .expect("replacement sentinel has an OID");
    owner.commit().await.expect("commit replacement sentinel");
    drop(connection);
    migrator
        .close()
        .await
        .expect("close replacement-control migrator pool");
    u32::try_from(oid).expect("PostgreSQL relation OID fits in u32")
}

async fn probe_snapshot(state: &Path) -> ProbeSnapshot {
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("connect probe observer");
    let mut connection = migrator.acquire().await.expect("acquire probe observer");
    let mut owner = connection.begin().await.expect("begin probe observation");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *owner)
        .await
        .expect("assume owner for probe observation");
    let oid = transaction_probe_oid(&mut owner)
        .await
        .expect("observe transaction probe OID");
    let (row_count, row_value) = if oid.is_some() {
        let count =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM codex_storage.transaction_probe")
                .fetch_one(&mut *owner)
                .await
                .expect("count probe sentinel rows");
        let value = sqlx::query_scalar::<_, i32>(
            "SELECT value FROM codex_storage.transaction_probe WHERE id = 1",
        )
        .fetch_optional(&mut *owner)
        .await
        .expect("read probe sentinel row");
        (count, value)
    } else {
        (0, None)
    };
    owner.rollback().await.expect("finish probe observation");
    drop(connection);
    migrator.close().await.expect("close probe observer pool");
    ProbeSnapshot {
        oid,
        row_count,
        row_value,
    }
}

async fn wait_for_queued_cleanup_lock(
    observer: &PostgresPool,
    oid: u32,
    wait: Duration,
) -> Result<bool, String> {
    tokio::time::timeout(wait, async {
        loop {
            let mut connection = observer
                .acquire()
                .await
                .map_err(|error| format!("acquire queued-lock observer: {error}"))?;
            let queued = sqlx::query_scalar::<sqlx::Postgres, bool>(QUEUED_LOCK_SQL)
                .bind(i64::from(oid))
                .fetch_one(&mut *connection)
                .await
                .map_err(|error| format!("inspect PostgreSQL lock queue: {error}"))?;
            drop(connection);
            if queued {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "timed out observing queued ACCESS EXCLUSIVE lock".to_string())?
}

#[tokio::test]
async fn cleanup_rejects_stale_oid_and_preserves_replacement_relation() {
    let Some(state) = cleanup_state() else {
        return;
    };
    let original_oid = create_owned_probe(&state, 101).await;
    let replacement_oid = replace_owned_probe(&state, 707).await;
    assert_ne!(
        original_oid, replacement_oid,
        "replacement must be a different relation identity"
    );

    let stale_result =
        cleanup_transaction_probe_with_deadline(&state, original_oid, Duration::from_secs(3)).await;
    let stale_error = stale_result.expect_err("stale OID must not delete the replacement");
    assert!(
        stale_error.contains("owned probe identity mismatch"),
        "unexpected stale-OID diagnostic: {stale_error}"
    );
    assert_eq!(
        probe_snapshot(&state).await,
        ProbeSnapshot {
            oid: Some(i64::from(replacement_oid)),
            row_count: 1,
            row_value: Some(707),
        },
        "stale cleanup must preserve the test-owned replacement and its data"
    );

    cleanup_transaction_probe_with_deadline(&state, replacement_oid, Duration::from_secs(3))
        .await
        .expect("cleanup may remove only the verified replacement OID");
}

#[tokio::test]
async fn setup_collision_preserves_obstruction_without_recording_ownership() {
    let Some(state) = cleanup_state() else {
        return;
    };
    let sentinel_oid = create_owned_probe(&state, 909).await;
    let owned_oid = Arc::new(AtomicU32::new(0));
    let exercise_state = state.clone();
    let exercise_owned_oid = Arc::clone(&owned_oid);
    let outcome = tokio::spawn(async move {
        exercise_transaction_outcomes(&exercise_state, &exercise_owned_oid, false).await;
    })
    .await;

    let observed_oid = owned_oid.load(Ordering::Acquire);
    let obstruction = probe_snapshot(&state).await;
    let cleanup_result =
        cleanup_transaction_probe_with_deadline(&state, sentinel_oid, Duration::from_secs(3)).await;

    assert_eq!(
        observed_oid, 0,
        "a CREATE collision must not record ownership"
    );
    let panic = outcome.expect_err("setup must fail on the pre-existing sentinel");
    let diagnostic = join_error_diagnostic("obstructed transaction setup", panic);
    assert!(
        diagnostic.contains("create transaction probe"),
        "setup failed before the expected CREATE collision: {diagnostic}"
    );
    assert_eq!(
        obstruction,
        ProbeSnapshot {
            oid: Some(i64::from(sentinel_oid)),
            row_count: 1,
            row_value: Some(909),
        },
        "failed setup must preserve the existing relation and data"
    );
    cleanup_result.expect("cleanup should remove only the sentinel OID owned by this control");
}

#[tokio::test]
async fn cleanup_timeout_aborts_lock_wait_and_preserves_owned_probe() {
    let Some(state) = cleanup_state() else {
        return;
    };
    let owned_oid = create_owned_probe(&state, 1111).await;

    let blocker_pool = PostgresPool::connect(settings(&state, "migrator"))
        .await
        .expect("connect lock blocker");
    let mut blocker_connection = blocker_pool.acquire().await.expect("acquire lock blocker");
    let mut blocker = blocker_connection
        .begin()
        .await
        .expect("begin lock blocker");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *blocker)
        .await
        .expect("assume owner for lock blocker");
    sqlx::query("LOCK TABLE codex_storage.transaction_probe IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold ACCESS EXCLUSIVE probe lock");

    let observer_pool = PostgresPool::connect(settings(&state, "migrator"))
        .await
        .expect("connect lock-queue observer");
    let cleanup_state = state.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_transaction_probe_with_deadline(&cleanup_state, owned_oid, Duration::from_secs(3))
            .await
    });
    let queued =
        wait_for_queued_cleanup_lock(&observer_pool, owned_oid, Duration::from_secs(2)).await;
    let cleanup_outcome = cleanup.await;
    let blocker_result = blocker.rollback().await;
    drop(blocker_connection);
    let observer_close = observer_pool.close().await;
    let blocker_close = blocker_pool.close().await;

    let cleanup_error = match cleanup_outcome {
        Ok(Err(error)) => error,
        Ok(Ok(())) => panic!("blocked cleanup unexpectedly completed successfully"),
        Err(error) => panic!("cleanup wrapper task failed to join: {error}"),
    };
    assert_eq!(
        queued,
        Ok(true),
        "the cleanup LOCK must be visible as an ungranted PostgreSQL lock"
    );
    assert!(
        cleanup_error.contains("cleanup exceeded 3 seconds")
            && cleanup_error.contains("abort_requested=true")
            && cleanup_error.contains("cleanup task was aborted and joined"),
        "timeout must report abort and join: {cleanup_error}"
    );
    blocker_result.expect("release lock blocker transaction");
    observer_close.expect("close lock-queue observer pool");
    blocker_close.expect("close lock blocker pool");
    assert_eq!(
        probe_snapshot(&state).await,
        ProbeSnapshot {
            oid: Some(i64::from(owned_oid)),
            row_count: 1,
            row_value: Some(1111),
        },
        "timed-out cleanup must leave the owned relation and row intact"
    );
    cleanup_transaction_probe_with_deadline(&state, owned_oid, Duration::from_secs(3))
        .await
        .expect("cleanup after releasing the blocker");
}
