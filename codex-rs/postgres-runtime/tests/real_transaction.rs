#![expect(
    clippy::expect_used,
    reason = "isolated PostgreSQL fixture failures should identify their source"
)]

use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::TransactionError;
use serde_json::Value;
use sqlx::Acquire;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);
const PANIC_INJECTION_ENV: &str = "CODEX_TEST_POSTGRES_TRANSACTION_INJECT_PANIC_AFTER_COMMIT";
const PANIC_INJECTION_VALUE: &str = "I_UNDERSTAND_DISPOSABLE_FIXTURE";
const PANIC_INJECTION_MESSAGE: &str = "injected failure after committed transaction probe creation";
const TRANSACTION_PROBE_OID_SQL: &str = "SELECT cls.oid::bigint FROM pg_catalog.pg_class AS cls JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace WHERE nsp.nspname = 'codex_storage' AND cls.relname = 'transaction_probe'";
const TRANSACTION_PROBE_ABSENT_SQL: &str = "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class AS cls JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace WHERE nsp.nspname = 'codex_storage' AND cls.relname = 'transaction_probe')";

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
            acquire_timeout: Duration::from_secs(5),
            max_connections: 4,
        },
    }
}

#[tokio::test]
async fn real_serializable_conflict_deadlock_and_cancelled_write() {
    let inject_panic = match std::env::var(PANIC_INJECTION_ENV) {
        Ok(value) if value == PANIC_INJECTION_VALUE => true,
        Ok(_) => panic!("{PANIC_INJECTION_ENV} requires the explicit disposable-fixture token"),
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => panic!("read {PANIC_INJECTION_ENV}: {error}"),
    };
    let state = match std::env::var("CODEX_TEST_POSTGRES_TRANSACTION_STATE") {
        Ok(state) => state,
        Err(std::env::VarError::NotPresent) if !inject_panic => return,
        Err(std::env::VarError::NotPresent) => {
            panic!("{PANIC_INJECTION_ENV} requires CODEX_TEST_POSTGRES_TRANSACTION_STATE")
        }
        Err(error) => panic!("read CODEX_TEST_POSTGRES_TRANSACTION_STATE: {error}"),
    };
    let state = Path::new(&state);
    // Join the exercise so panics are observed before owned fixture cleanup.
    let exercise_state = state.to_path_buf();
    let owned_probe_oid = Arc::new(AtomicU32::new(0));
    let exercise_owned_probe_oid = Arc::clone(&owned_probe_oid);
    let outcome = tokio::spawn(async move {
        exercise_transaction_outcomes(&exercise_state, &exercise_owned_probe_oid, inject_panic)
            .await;
    })
    .await
    .map_err(|error| join_error_diagnostic("transaction exercise", error));

    let owned_probe_oid = owned_probe_oid.load(Ordering::Acquire);
    let cleanup = if owned_probe_oid != 0 {
        cleanup_transaction_probe_with_deadline(state, owned_probe_oid, CLEANUP_TIMEOUT).await
    } else {
        Ok(())
    };

    if inject_panic {
        match (outcome, cleanup) {
            (Err(primary), Ok(())) => {
                assert!(
                    primary.contains(PANIC_INJECTION_MESSAGE),
                    "primary panic diagnostic must survive cleanup: {primary}"
                );
                return;
            }
            (Err(primary), Err(cleanup)) => panic!(
                "injected primary panic was preserved but cleanup failed: {primary}; {cleanup}"
            ),
            (Ok(()), cleanup) => panic!("injected failure did not occur; cleanup={cleanup:?}"),
        }
    }

    let result = match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(format!("owned probe cleanup failed: {cleanup}")),
        (Err(primary), Err(cleanup)) => Err(format!(
            "{primary}; owned probe cleanup also failed: {cleanup}"
        )),
    };
    result.expect("transaction exercise and owned fixture cleanup should complete");
}

fn join_error_diagnostic(context: &str, error: tokio::task::JoinError) -> String {
    if error.is_cancelled() {
        return format!("{context} task was cancelled");
    }
    if error.is_panic() {
        let payload = error.into_panic();
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&'static str>()
                    .map(|message| (*message).to_string())
            })
            .unwrap_or_else(|| "non-string panic payload".to_string());
        return format!("{context} task panicked: {message}");
    }
    format!("{context} task failed: {error}")
}

async fn transaction_probe_oid(
    connection: &mut sqlx::PgConnection,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(TRANSACTION_PROBE_OID_SQL)
        .fetch_optional(&mut *connection)
        .await
}

async fn cleanup_transaction_probe_with_deadline(
    state: &Path,
    owned_oid: u32,
    deadline: Duration,
) -> Result<(), String> {
    let cleanup_state = state.to_path_buf();
    let mut cleanup =
        tokio::spawn(async move { cleanup_transaction_probe(&cleanup_state, owned_oid).await });
    match tokio::time::timeout(deadline, &mut cleanup).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(join_error_diagnostic("owned probe cleanup", error)),
        Err(_) => {
            let abort_requested = !cleanup.is_finished();
            if abort_requested {
                cleanup.abort();
            }
            // Always join after abort so the test cannot leave its cleanup task detached.
            let joined = cleanup.await;
            let joined_outcome = match joined {
                Ok(Ok(())) => "cleanup completed as the deadline fired".to_string(),
                Ok(Err(error)) => format!("cleanup returned an error: {error}"),
                Err(error) if error.is_cancelled() => {
                    "cleanup task was aborted and joined".to_string()
                }
                Err(error) => join_error_diagnostic("owned probe cleanup", error),
            };
            Err(format!(
                "cleanup exceeded {} seconds; abort_requested={abort_requested}; {joined_outcome}",
                deadline.as_secs()
            ))
        }
    }
}

async fn cleanup_transaction_probe(state: &Path, owned_oid: u32) -> Result<(), String> {
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .map_err(|error| format!("connect cleanup migrator: {error}"))?;
    let cleanup_result = async {
        let mut connection = migrator
            .acquire()
            .await
            .map_err(|error| format!("acquire cleanup migrator: {error}"))?;
        let mut owner = connection
            .begin()
            .await
            .map_err(|error| format!("begin probe cleanup: {error}"))?;
        sqlx::query("SET LOCAL ROLE codex_owner")
            .execute(&mut *owner)
            .await
            .map_err(|error| format!("assume owner for probe cleanup: {error}"))?;

        match transaction_probe_oid(&mut owner)
            .await
            .map_err(|error| format!("inspect owned probe before cleanup: {error}"))?
        {
            None => {
                owner
                    .rollback()
                    .await
                    .map_err(|error| format!("finish missing-probe cleanup: {error}"))?;
            }
            Some(found_oid) if found_oid != i64::from(owned_oid) => {
                owner
                    .rollback()
                    .await
                    .map_err(|error| format!("finish mismatched-probe cleanup: {error}"))?;
                return Err(format!(
                    "owned probe identity mismatch: expected OID {owned_oid}, found OID {found_oid}"
                ));
            }
            Some(_) => {
                let lock_result = sqlx::query(
                    "LOCK TABLE codex_storage.transaction_probe IN ACCESS EXCLUSIVE MODE",
                )
                .execute(&mut *owner)
                .await;
                if let Err(error) = lock_result {
                    let missing = error
                        .as_database_error()
                        .and_then(sqlx::error::DatabaseError::code)
                        .as_deref()
                        == Some("42P01");
                    let error_message = error.to_string();
                    owner
                        .rollback()
                        .await
                        .map_err(|rollback| format!("rollback failed probe lock: {rollback}"))?;
                    if !missing {
                        return Err(format!("lock owned probe for cleanup: {error_message}"));
                    }
                    if let Some(found_oid) = transaction_probe_oid(&mut connection)
                        .await
                        .map_err(|inspect| {
                            format!("inspect probe after concurrent drop: {inspect}")
                        })?
                    {
                        return Err(format!(
                            "probe changed while acquiring cleanup lock: found OID {found_oid}"
                        ));
                    }
                } else {
                    let locked_oid = transaction_probe_oid(&mut owner)
                        .await
                        .map_err(|error| format!("recheck locked probe OID: {error}"))?;
                    if locked_oid != Some(i64::from(owned_oid)) {
                        owner.rollback().await.map_err(|error| {
                            format!("rollback changed-probe cleanup: {error}")
                        })?;
                        return Err(format!(
                            "owned probe identity changed under lock: expected OID {owned_oid}, found {locked_oid:?}"
                        ));
                    }
                    sqlx::query("DROP TABLE codex_storage.transaction_probe")
                        .execute(&mut *owner)
                        .await
                        .map_err(|error| format!("drop owned transaction probe: {error}"))?;
                    owner
                        .commit()
                        .await
                        .map_err(|error| format!("commit probe cleanup: {error}"))?;
                }
            }
        }

        let absent = sqlx::query_scalar::<_, bool>(TRANSACTION_PROBE_ABSENT_SQL)
            .fetch_one(&mut *connection)
            .await
            .map_err(|error| format!("verify transaction probe absence: {error}"))?;
        if !absent {
            return Err("transaction probe remains after owned cleanup".to_string());
        }
        Ok(())
    }
    .await;
    let close_result = migrator
        .close()
        .await
        .map_err(|error| format!("close cleanup migrator: {error}"));
    match (cleanup_result, close_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(close)) => Err(close),
        (Err(primary), Err(close)) => Err(format!("{primary}; {close}")),
    }
}

async fn exercise_transaction_outcomes(state: &Path, owned_oid: &AtomicU32, inject_panic: bool) {
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("migrator pool");
    let mut connection = migrator.acquire().await.expect("acquire migrator");
    let mut owner = connection.begin().await.expect("begin owner transaction");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *owner)
        .await
        .expect("assume owner");

    // A same-name pre-existing object is an obstruction, not this run's cleanup target.
    sqlx::query("CREATE TABLE codex_storage.transaction_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)")
        .execute(&mut *owner)
        .await
        .expect("create transaction probe");
    let oid = transaction_probe_oid(&mut owner)
        .await
        .expect("read created transaction probe OID")
        .expect("created transaction probe must have an OID");
    let oid = u32::try_from(oid).expect("PostgreSQL relation OID must fit in u32");
    owned_oid.store(oid, Ordering::Release);
    sqlx::query("INSERT INTO codex_storage.transaction_probe VALUES (1, 0), (2, 0)")
        .execute(&mut *owner)
        .await
        .expect("seed transaction probe");
    owner.commit().await.expect("commit transaction probe");
    let committed_oid = transaction_probe_oid(&mut connection)
        .await
        .expect("verify committed transaction probe before injected failure");
    assert_eq!(
        committed_oid,
        Some(i64::from(oid)),
        "the recorded relation OID must identify the committed probe"
    );
    drop(connection);
    migrator.close().await.expect("close setup migrator pool");
    if inject_panic {
        panic!("{PANIC_INJECTION_MESSAGE}");
    }
    let pool = Arc::new(
        PostgresPool::connect(settings(state, "runtime"))
            .await
            .expect("runtime pool"),
    );

    let mut first = pool.begin_serializable().await.expect("first transaction");
    let mut second = pool.begin_serializable().await.expect("second transaction");
    for transaction in [&mut first, &mut second] {
        let value: i32 =
            sqlx::query_scalar("SELECT value FROM codex_storage.transaction_probe WHERE id = 1")
                .fetch_one(transaction.connection())
                .await
                .expect("read same snapshot");
        assert_eq!(value, 0);
    }
    sqlx::query("UPDATE codex_storage.transaction_probe SET value = 1 WHERE id = 1")
        .execute(first.connection())
        .await
        .expect("first writer updates");
    assert_eq!(first.commit().await, Ok(()));
    let conflict = sqlx::query("UPDATE codex_storage.transaction_probe SET value = 2 WHERE id = 1")
        .execute(second.connection())
        .await
        .expect_err("stale serializable writer must abort");
    assert_eq!(
        TransactionError::classify_statement(&conflict),
        TransactionError::SerializationConflict
    );
    second.rollback().await.expect("finish aborted transaction");

    let mut left = pool.begin_serializable().await.expect("left transaction");
    let mut right = pool.begin_serializable().await.expect("right transaction");
    sqlx::query("UPDATE codex_storage.transaction_probe SET value = value + 1 WHERE id = 1")
        .execute(left.connection())
        .await
        .expect("left locks first row");
    sqlx::query("UPDATE codex_storage.transaction_probe SET value = value + 1 WHERE id = 2")
        .execute(right.connection())
        .await
        .expect("right locks second row");
    let (left_result, right_result) = tokio::join!(
        sqlx::query("UPDATE codex_storage.transaction_probe SET value = value + 1 WHERE id = 2")
            .execute(left.connection()),
        sqlx::query("UPDATE codex_storage.transaction_probe SET value = value + 1 WHERE id = 1")
            .execute(right.connection())
    );
    let outcomes = [left_result, right_result]
        .into_iter()
        .filter_map(Result::err)
        .map(|error| TransactionError::classify_statement(&error))
        .collect::<Vec<_>>();
    assert!(outcomes.contains(&TransactionError::Deadlock));
    left.rollback().await.expect("finish left transaction");
    right.rollback().await.expect("finish right transaction");

    let (ready, started) = tokio::sync::oneshot::channel();
    let cancelled_pool = Arc::clone(&pool);
    let pending = tokio::spawn(async move {
        let mut transaction = cancelled_pool
            .begin_serializable()
            .await
            .expect("begin cancelled transaction");
        sqlx::query("UPDATE codex_storage.transaction_probe SET value = 99 WHERE id = 1")
            .execute(transaction.connection())
            .await
            .expect("write uncommitted value");
        ready.send(()).expect("signal uncommitted write");
        let _ = sqlx::query("SELECT pg_sleep(10)")
            .execute(transaction.connection())
            .await;
        transaction.commit().await
    });
    started.await.expect("wait for uncommitted write");
    pending.abort();
    assert!(
        pending
            .await
            .expect_err("cancel pending transaction")
            .is_cancelled()
    );
    pool.health()
        .await
        .expect("pool remains healthy after cancellation");
    let mut reader = pool.acquire().await.expect("inspect rollback");
    let values: Vec<i32> =
        sqlx::query_scalar("SELECT value FROM codex_storage.transaction_probe ORDER BY id")
            .fetch_all(&mut *reader)
            .await
            .expect("read committed probe values");
    assert_eq!(values, vec![1, 0]);
    drop(reader);
}

#[path = "real_transaction/cleanup_controls.rs"]
mod cleanup_controls;
