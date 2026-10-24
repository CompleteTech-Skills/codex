#![expect(
    clippy::expect_used,
    reason = "isolated PostgreSQL fixture failures should identify their source"
)]

use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use serde_json::Value;
use sqlx::Acquire;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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
            .to_string(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: 2,
        },
    }
}

async fn owner_sql(pool: &PostgresPool, owner: &'static str, sql: &'static str) {
    let mut connection = pool.acquire().await.expect("acquire migrator connection");
    let mut transaction = connection.begin().await.expect("begin owner transaction");
    sqlx::query(owner)
        .execute(&mut *transaction)
        .await
        .expect("assume matching owner");
    sqlx::query(sql)
        .execute(&mut *transaction)
        .await
        .expect("create isolated probe");
    transaction.commit().await.expect("commit isolated probe");
}

async fn sqlstate(pool: &PostgresPool, sql: &'static str) -> String {
    let mut connection = pool.acquire().await.expect("acquire role connection");
    let error = sqlx::query(sql)
        .execute(&mut *connection)
        .await
        .expect_err("cross-schema operation must fail");
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .expect("database SQLSTATE")
        .to_string()
}

#[tokio::test]
async fn real_postgres_roles_isolate_two_schemas() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let sidecar: Value = serde_json::from_slice(
        &std::fs::read(state.join("isolation-fixture.json")).expect("read isolation receipt"),
    )
    .expect("parse isolation receipt");
    assert_eq!(sidecar["activation_permitted"], false);
    let default_migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("default migrator");
    let isolation_migrator = PostgresPool::connect(settings(state, "isolation_migrator"))
        .await
        .expect("isolation migrator");
    let default_runtime = PostgresPool::connect(settings(state, "runtime"))
        .await
        .expect("default runtime");
    let isolation_runtime = PostgresPool::connect(settings(state, "isolation_runtime"))
        .await
        .expect("isolation runtime");

    let default_migrator = Arc::new(default_migrator);
    let isolation_migrator = Arc::new(isolation_migrator);
    // Refuse pre-existing objects before arming cleanup; they are not this test's work.
    for (pool, schema) in [
        (&default_migrator, "codex_storage"),
        (&isolation_migrator, "codex_storage_isolation"),
    ] {
        let mut connection = pool.acquire().await.expect("inspect probe namespace");
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = 'isolation_probe')")
            .bind(schema)
            .fetch_one(&mut *connection)
            .await
            .expect("inspect existing probe");
        assert!(
            !exists,
            "refuse pre-existing isolation probe; quarantine fixture"
        );
    }
    let default_cleanup = default_migrator.clone();
    let isolation_cleanup = isolation_migrator.clone();
    // Join even a panicking assertion before cleanup, so no local writer is detached.
    let outcome = tokio::spawn(async move {
        owner_sql(
            &default_migrator,
            "SET LOCAL ROLE codex_owner",
            "CREATE TABLE codex_storage.isolation_probe (value INTEGER NOT NULL)",
        )
        .await;
        owner_sql(
            &isolation_migrator,
            "SET LOCAL ROLE codex_isolation_owner",
            "CREATE TABLE codex_storage_isolation.isolation_probe (value INTEGER NOT NULL)",
        )
        .await;

        for (pool, own, other) in [
            (
                &default_runtime,
                "INSERT INTO codex_storage.isolation_probe VALUES (1)",
                "INSERT INTO codex_storage_isolation.isolation_probe VALUES (3)",
            ),
            (
                &isolation_runtime,
                "INSERT INTO codex_storage_isolation.isolation_probe VALUES (2)",
                "INSERT INTO codex_storage.isolation_probe VALUES (4)",
            ),
        ] {
            let mut connection = pool.acquire().await.expect("acquire runtime");
            sqlx::query(own)
                .execute(&mut *connection)
                .await
                .expect("write own schema");
            drop(connection);
            assert_eq!(sqlstate(pool, other).await, "42501");
        }
        let mut first = default_runtime.acquire().await.expect("read first schema");
        let value: i32 = sqlx::query_scalar("SELECT value FROM codex_storage.isolation_probe")
            .fetch_one(&mut *first)
            .await
            .expect("default runtime reads own schema");
        assert_eq!(value, 1);
        drop(first);
        let mut second = isolation_runtime
            .acquire()
            .await
            .expect("read second schema");
        let value: i32 =
            sqlx::query_scalar("SELECT value FROM codex_storage_isolation.isolation_probe")
                .fetch_one(&mut *second)
                .await
                .expect("isolation runtime reads own schema");
        assert_eq!(value, 2);
        drop(second);
        assert_eq!(
            sqlstate(
                &default_runtime,
                "SELECT value FROM codex_storage_isolation.isolation_probe"
            )
            .await,
            "42501"
        );
        assert_eq!(
            sqlstate(
                &isolation_runtime,
                "SELECT value FROM codex_storage.isolation_probe"
            )
            .await,
            "42501"
        );
        for (pool, statement) in [
            (
                &default_runtime,
                "CREATE TABLE codex_storage.forbidden(id INTEGER)",
            ),
            (
                &isolation_runtime,
                "CREATE TABLE codex_storage_isolation.forbidden(id INTEGER)",
            ),
            (&default_migrator, "SET ROLE codex_isolation_owner"),
            (&isolation_migrator, "SET ROLE codex_owner"),
        ] {
            assert_eq!(sqlstate(pool, statement).await, "42501");
        }
    })
    .await;
    let mut cleanup_errors = Vec::new();
    for (pool, owner, statement) in [
        (
            &default_cleanup,
            "SET LOCAL ROLE codex_owner",
            "DROP TABLE IF EXISTS codex_storage.isolation_probe",
        ),
        (
            &isolation_cleanup,
            "SET LOCAL ROLE codex_isolation_owner",
            "DROP TABLE IF EXISTS codex_storage_isolation.isolation_probe",
        ),
    ] {
        let cleanup = async {
            let mut connection = pool
                .acquire()
                .await
                .map_err(|_| "acquire cleanup connection")?;
            let mut transaction = connection
                .begin()
                .await
                .map_err(|_| "begin cleanup transaction")?;
            sqlx::query(owner)
                .execute(&mut *transaction)
                .await
                .map_err(|_| "assume cleanup owner")?;
            sqlx::query(statement)
                .execute(&mut *transaction)
                .await
                .map_err(|_| "drop owned probe")?;
            transaction
                .commit()
                .await
                .map_err(|_| "commit cleanup; outcome uncertain")
        }
        .await;
        if let Err(error) = cleanup {
            cleanup_errors.push(error);
        }
    }
    if !cleanup_errors.is_empty() {
        eprintln!("probe cleanup failed; quarantine fixture: {cleanup_errors:?}");
    }
    if let Err(error) = outcome {
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
        panic!("probe task cancelled; quarantine fixture: {error}");
    }
    assert!(
        cleanup_errors.is_empty(),
        "probe cleanup failed; quarantine fixture"
    );
}
