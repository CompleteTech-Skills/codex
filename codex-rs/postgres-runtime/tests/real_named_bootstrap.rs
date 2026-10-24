#![expect(
    clippy::expect_used,
    reason = "isolated PostgreSQL fixture failures should identify their source"
)]

use codex_postgres_runtime::BootstrapError;
use codex_postgres_runtime::ClientCapabilities;
use codex_postgres_runtime::CompatibilityError;
use codex_postgres_runtime::CompatibilityResult;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::RequiredAccess;
use codex_postgres_runtime::bootstrap_named_namespace;
use codex_postgres_runtime::check_named_namespace_compatibility;
use serde_json::Value;
use sqlx::Acquire;
use std::path::Path;
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
            .to_string()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: 2,
        },
    }
}

async fn owner_sql(pool: &PostgresPool, sql: &'static str) {
    let mut connection = pool.acquire().await.expect("acquire named migrator");
    let mut transaction = connection.begin().await.expect("begin owner transaction");
    sqlx::query("SET LOCAL ROLE codex_isolation_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume named owner");
    sqlx::query(sql)
        .execute(&mut *transaction)
        .await
        .expect("run owner fixture SQL");
    transaction
        .commit()
        .await
        .expect("commit owner fixture SQL");
}

// Required SQL qualification must never succeed through the optional-fixture return.
fn bootstrap_fixture_state<'a>(
    required_mode: Option<&std::ffi::OsStr>,
    state: Option<&'a std::ffi::OsStr>,
) -> Result<Option<&'a str>, &'static str> {
    let required = match required_mode {
        None => false,
        Some(mode) if mode == "1" => true,
        Some(_) => return Err("invalid bootstrap fixture required mode; use 1 or leave unset"),
    };
    let state = state.and_then(std::ffi::OsStr::to_str);
    if required && state.is_none_or(|value| value.trim().is_empty()) {
        return Err("required bootstrap fixture state is absent, empty, or not Unicode");
    }
    Ok(state)
}

#[test]
fn bootstrap_fixture_admission_requires_declared_fixture() {
    use std::ffi::OsStr;

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;

        let invalid = std::ffi::OsString::from_wide(&[0xD800]);
        assert_eq!(
            bootstrap_fixture_state(None, Some(invalid.as_os_str())),
            Ok(None)
        );
        assert!(bootstrap_fixture_state(Some(OsStr::new("1")), Some(invalid.as_os_str())).is_err());
        assert!(
            bootstrap_fixture_state(Some(invalid.as_os_str()), Some(OsStr::new("owned"))).is_err()
        );
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;

        let invalid = OsStr::from_bytes(&[0xff]);
        assert_eq!(bootstrap_fixture_state(None, Some(invalid)), Ok(None));
        assert!(bootstrap_fixture_state(Some(OsStr::new("1")), Some(invalid)).is_err());
        assert!(bootstrap_fixture_state(Some(invalid), Some(OsStr::new("owned"))).is_err());
    }
    assert_eq!(bootstrap_fixture_state(None, None), Ok(None));
    assert_eq!(
        bootstrap_fixture_state(None, Some(OsStr::new(""))),
        Ok(Some(""))
    );
    assert!(bootstrap_fixture_state(Some(OsStr::new("1")), None).is_err());
    assert!(bootstrap_fixture_state(Some(OsStr::new("1")), Some(OsStr::new(""))).is_err());
    assert!(bootstrap_fixture_state(Some(OsStr::new("1")), Some(OsStr::new(" "))).is_err());
    assert!(bootstrap_fixture_state(Some(OsStr::new("0")), Some(OsStr::new("owned"))).is_err());
    assert_eq!(
        bootstrap_fixture_state(Some(OsStr::new("1")), Some(OsStr::new("owned"))),
        Ok(Some("owned"))
    );
}

#[tokio::test]
async fn real_named_namespace_bootstrap_is_isolated_and_rejects_wrong_roles() {
    let required_mode = std::env::var_os("CODEX_TEST_POSTGRES_ISOLATION_REQUIRED");
    let state = std::env::var_os("CODEX_TEST_POSTGRES_ISOLATION_STATE");
    let Some(state) = bootstrap_fixture_state(required_mode.as_deref(), state.as_deref())
        .expect("bootstrap fixture admission failed")
    else {
        return;
    };
    let state = Path::new(&state);
    let namespace = NamedNamespace::new("codex_storage_isolation").expect("valid named schema");
    assert!(NamedNamespace::new("codex_storage_isolation\";DROP SCHEMA public").is_err());
    let first = PostgresPool::connect(settings(state, "isolation_migrator"))
        .await
        .expect("first named migrator");
    let second = PostgresPool::connect(settings(state, "isolation_migrator"))
        .await
        .expect("second named migrator");
    let runtime = PostgresPool::connect(settings(state, "isolation_runtime"))
        .await
        .expect("named runtime");
    let default_migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("default migrator");

    assert_eq!(
        bootstrap_named_namespace(&runtime, &namespace).await,
        Err(BootstrapError::Privilege)
    );
    assert_eq!(
        bootstrap_named_namespace(&default_migrator, &namespace).await,
        Err(BootstrapError::Privilege)
    );
    owner_sql(
        &first,
        "CREATE TYPE codex_storage_isolation.occupied_type AS ENUM ('x')",
    )
    .await;
    assert_eq!(
        bootstrap_named_namespace(&first, &namespace).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    owner_sql(&first, "DROP TYPE codex_storage_isolation.occupied_type").await;

    let (a, b) = tokio::join!(
        bootstrap_named_namespace(&first, &namespace),
        bootstrap_named_namespace(&second, &namespace)
    );
    assert_eq!((a, b), (Ok(()), Ok(())));
    assert_eq!(bootstrap_named_namespace(&first, &namespace).await, Ok(()));

    let mut owner = first.acquire().await.expect("read named ledger");
    let mut transaction = owner.begin().await.expect("begin named ledger read");
    sqlx::query("SET LOCAL ROLE codex_isolation_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume named owner for ledger");
    let (format, history): (i32, i64) = sqlx::query_as(
        "SELECT (SELECT format_version FROM codex_storage_isolation.codex_schema_meta), (SELECT COUNT(*) FROM codex_storage_isolation._codex_pg_migrations)",
    )
    .fetch_one(&mut *transaction)
    .await
    .expect("read named metadata and history");
    assert_eq!((format, history), (19, 19));
    transaction
        .rollback()
        .await
        .expect("finish named ledger read");
    drop(owner);
    let mut runtime_connection = runtime.acquire().await.expect("read named metadata");
    let visible: i32 =
        sqlx::query_scalar("SELECT format_version FROM codex_storage_isolation.codex_schema_meta")
            .fetch_one(&mut *runtime_connection)
            .await
            .expect("runtime reads named metadata");
    assert_eq!(visible, 19);
    let pinned: (String, String, Option<String>) = sqlx::query_as(
        "SELECT id, name, appearance FROM codex_storage_isolation.thread_sections WHERE name = 'Pinned'",
    )
    .fetch_one(&mut *runtime_connection)
    .await
    .expect("named runtime reads pinned section");
    assert_eq!(
        pinned,
        (
            "01984de2-8f74-7c91-a3b2-5c5e937cf318".to_string(),
            "Pinned".to_string(),
            None
        )
    );
    let denied = sqlx::query("SELECT version FROM codex_storage_isolation._codex_pg_migrations")
        .execute(&mut *runtime_connection)
        .await
        .expect_err("runtime must not read named migration history");
    assert_eq!(
        denied
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501")
    );
    let denied = sqlx::query("SELECT id FROM codex_storage.thread_sections")
        .execute(&mut *runtime_connection)
        .await
        .expect_err("named runtime cannot read default sections");
    assert_eq!(
        denied
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501")
    );
    let denied = sqlx::query("SELECT id FROM codex_storage.threads")
        .execute(&mut *runtime_connection)
        .await
        .expect_err("named runtime cannot read default thread metadata");
    assert_eq!(
        denied
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501")
    );
    drop(runtime_connection);
    let default_runtime = PostgresPool::connect(settings(state, "runtime"))
        .await
        .expect("default runtime pool");
    let mut default_connection = default_runtime.acquire().await.expect("default runtime");
    let denied = sqlx::query("SELECT id FROM codex_storage_isolation.threads")
        .execute(&mut *default_connection)
        .await
        .expect_err("default runtime cannot read named thread metadata");
    assert_eq!(
        denied
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501")
    );
    let denied = sqlx::query("SELECT id FROM codex_storage_isolation.thread_sections")
        .execute(&mut *default_connection)
        .await
        .expect_err("default runtime cannot read named sections");
    assert_eq!(
        denied
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501")
    );
    drop(default_connection);

    let capabilities = ClientCapabilities {
        min_schema_format: 19,
        max_schema_format: 19,
        reader_version: 19,
        writer_version: 19,
    };
    let compatible = Ok(CompatibilityResult {
        schema_format: 19,
        activation_permitted: false,
    });
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadOnly
        )
        .await,
        compatible
    );
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadWrite
        )
        .await,
        compatible
    );
    for wrong in [&runtime, &default_migrator] {
        assert_eq!(
            check_named_namespace_compatibility(
                wrong,
                &namespace,
                capabilities,
                RequiredAccess::ReadOnly
            )
            .await,
            Err(CompatibilityError::Privilege)
        );
    }

    owner_sql(
        &first,
        "UPDATE codex_storage_isolation.codex_schema_meta SET min_writer_version = 20",
    )
    .await;
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadOnly
        )
        .await,
        compatible
    );
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadWrite
        )
        .await,
        Err(CompatibilityError::WriterTooOld)
    );
    owner_sql(
        &first,
        "UPDATE codex_storage_isolation.codex_schema_meta SET min_reader_version = 20",
    )
    .await;
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadOnly
        )
        .await,
        Err(CompatibilityError::ReaderTooOld)
    );
    owner_sql(
        &first,
        "UPDATE codex_storage_isolation.codex_schema_meta SET format_version = 20, min_reader_version = 19, min_writer_version = 19",
    )
    .await;
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadOnly
        )
        .await,
        Err(CompatibilityError::UnsupportedSchema)
    );
    owner_sql(
        &first,
        "UPDATE codex_storage_isolation.codex_schema_meta SET format_version = 19",
    )
    .await;

    owner_sql(
        &first,
        "UPDATE codex_storage_isolation._codex_pg_migrations SET success = FALSE WHERE version = 1",
    )
    .await;
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadOnly
        )
        .await,
        Err(CompatibilityError::DirtyMigration)
    );
    owner_sql(
        &first,
        "UPDATE codex_storage_isolation._codex_pg_migrations SET success = TRUE WHERE version = 1",
    )
    .await;
    let mut connection = first.acquire().await.expect("read named checksum");
    let mut transaction = connection.begin().await.expect("begin named checksum read");
    sqlx::query("SET LOCAL ROLE codex_isolation_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume owner for checksum read");
    let checksum: Vec<u8> = sqlx::query_scalar(
        "SELECT checksum FROM codex_storage_isolation._codex_pg_migrations WHERE version = 1",
    )
    .fetch_one(&mut *transaction)
    .await
    .expect("read named checksum");
    transaction.rollback().await.expect("finish checksum read");
    drop(connection);
    owner_sql(
        &first,
        "UPDATE codex_storage_isolation._codex_pg_migrations SET checksum = '\\x00'::bytea WHERE version = 1",
    )
    .await;
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadOnly
        )
        .await,
        Err(CompatibilityError::IncompatibleHistory)
    );
    let mut connection = first.acquire().await.expect("restore named checksum");
    let mut transaction = connection
        .begin()
        .await
        .expect("begin named checksum restore");
    sqlx::query("SET LOCAL ROLE codex_isolation_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume owner for checksum restore");
    sqlx::query(
        "UPDATE codex_storage_isolation._codex_pg_migrations SET checksum = $1 WHERE version = 1",
    )
    .bind(checksum)
    .execute(&mut *transaction)
    .await
    .expect("restore named checksum");
    transaction.commit().await.expect("commit checksum restore");
    drop(connection);
    owner_sql(
        &first,
        "INSERT INTO codex_storage_isolation._codex_pg_migrations (version, description, success, checksum, execution_time) SELECT 999, description, TRUE, checksum, execution_time FROM codex_storage_isolation._codex_pg_migrations WHERE version = 1",
    )
    .await;
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadOnly
        )
        .await,
        Err(CompatibilityError::IncompatibleHistory)
    );
    owner_sql(
        &first,
        "DELETE FROM codex_storage_isolation._codex_pg_migrations WHERE version = 999",
    )
    .await;
    let mut connection = first.acquire().await.expect("begin uncommitted marker");
    let mut transaction = connection
        .begin()
        .await
        .expect("begin uncommitted transaction");
    sqlx::query("SET LOCAL ROLE codex_isolation_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume owner for uncommitted marker");
    sqlx::query(
        "UPDATE codex_storage_isolation._codex_pg_migrations SET success = FALSE WHERE version = 1",
    )
    .execute(&mut *transaction)
    .await
    .expect("write uncommitted dirty marker");
    drop(transaction);
    drop(connection);
    assert_eq!(
        check_named_namespace_compatibility(
            &first,
            &namespace,
            capabilities,
            RequiredAccess::ReadOnly
        )
        .await,
        compatible
    );
}
