#![expect(
    clippy::expect_used,
    reason = "isolated PostgreSQL fixture failures should identify their source"
)]

use codex_postgres_runtime::BootstrapError;
use codex_postgres_runtime::ClientCapabilities;
use codex_postgres_runtime::CompatibilityError;
use codex_postgres_runtime::CompatibilityResult;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::RequiredAccess;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_postgres_runtime::check_codex_storage_compatibility;
use pretty_assertions::assert_eq;
use serde_json::Value;
use sqlx::Acquire;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

#[path = "compatibility/real_compatibility_tests.rs"]
mod real_compatibility_cases;

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

async fn owner_query(pool: &PostgresPool, sql: &'static str) {
    let mut connection = pool.acquire().await.expect("acquire migrator connection");
    let mut transaction = connection.begin().await.expect("begin owner transaction");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume schema owner role");
    sqlx::raw_sql(sql)
        .execute(&mut *transaction)
        .await
        .expect("execute owner fixture SQL");
    transaction
        .commit()
        .await
        .expect("commit owner fixture SQL");
}

async fn migrator_query(pool: &PostgresPool, sql: &'static str) {
    let mut connection = pool.acquire().await.expect("acquire migrator connection");
    sqlx::raw_sql(sql)
        .execute(&mut *connection)
        .await
        .expect("execute migrator fixture SQL");
}

async fn role_query(pool: &PostgresPool, sql: &'static str) {
    let mut connection = pool.acquire().await.expect("acquire role connection");
    sqlx::raw_sql(sql)
        .execute(&mut *connection)
        .await
        .expect("execute role fixture SQL");
}

async fn reject_namespace_objects(pool: &PostgresPool) {
    for (create, drop) in [
        (
            "CREATE COLLATION codex_storage.occupied_collation FROM pg_catalog.\"C\"",
            "DROP COLLATION IF EXISTS codex_storage.occupied_collation",
        ),
        (
            "CREATE OPERATOR codex_storage.=== (FUNCTION = pg_catalog.int4eq, LEFTARG = integer, RIGHTARG = integer)",
            "DROP OPERATOR IF EXISTS codex_storage.=== (integer, integer)",
        ),
        (
            "CREATE TEXT SEARCH CONFIGURATION codex_storage.occupied_search (COPY = pg_catalog.simple)",
            "DROP TEXT SEARCH CONFIGURATION IF EXISTS codex_storage.occupied_search",
        ),
    ] {
        owner_query(pool, drop).await;
        owner_query(pool, create).await;
        assert_eq!(
            bootstrap_codex_storage(pool).await,
            Err(BootstrapError::IncompatibleNamespace)
        );
        owner_query(pool, drop).await;
    }
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
async fn real_postgres_bootstrap_is_atomic_role_scoped_and_idempotent() {
    let required_mode = std::env::var_os("CODEX_TEST_POSTGRES_REQUIRED");
    let state = std::env::var_os("CODEX_TEST_POSTGRES_STATE");
    let Some(state) = bootstrap_fixture_state(required_mode.as_deref(), state.as_deref())
        .expect("bootstrap fixture admission failed")
    else {
        return;
    };
    let state = Path::new(&state);
    let migrator_a = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("first migrator pool");
    let migrator_b = Arc::new(
        PostgresPool::connect(settings(state, "migrator"))
            .await
            .expect("second migrator pool"),
    );
    let runtime = PostgresPool::connect(settings(state, "runtime"))
        .await
        .expect("runtime pool");
    let backup = PostgresPool::connect(settings(state, "backup"))
        .await
        .expect("backup pool");

    migrator_query(
        &migrator_a,
        "REVOKE codex_bootstrap_graph_bridge FROM codex_bootstrap_graph_principal",
    )
    .await;
    migrator_query(
        &migrator_a,
        "GRANT codex_bootstrap_graph_bridge TO codex_bootstrap_graph_principal WITH INHERIT FALSE, SET FALSE, ADMIN TRUE",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::Privilege)
    );
    migrator_query(
        &migrator_a,
        "REVOKE codex_bootstrap_graph_bridge FROM codex_bootstrap_graph_principal",
    )
    .await;
    migrator_query(
        &migrator_a,
        "GRANT codex_bootstrap_graph_bridge TO codex_backup WITH INHERIT FALSE, SET FALSE, ADMIN TRUE",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::Privilege)
    );
    migrator_query(
        &migrator_a,
        "REVOKE codex_bootstrap_graph_bridge FROM codex_backup",
    )
    .await;
    migrator_query(
        &migrator_a,
        "GRANT codex_bootstrap_graph_bridge TO codex_backup WITH INHERIT FALSE, SET TRUE",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::Privilege)
    );
    migrator_query(
        &migrator_a,
        "REVOKE codex_bootstrap_graph_bridge FROM codex_backup",
    )
    .await;

    owner_query(
        &migrator_a,
        "DROP TABLE IF EXISTS codex_storage.occupied_probe",
    )
    .await;
    owner_query(
        &migrator_a,
        "DROP TYPE IF EXISTS codex_storage.occupied_type",
    )
    .await;
    reject_namespace_objects(&migrator_a).await;
    owner_query(
        &migrator_a,
        "CREATE TABLE codex_storage.occupied_probe(id BIGINT)",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    let mut owner = migrator_a.acquire().await.expect("inspect occupied schema");
    let mut owner_transaction = owner.begin().await.expect("begin owner inspection");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *owner_transaction)
        .await
        .expect("assume owner for inspection");
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('codex_storage.occupied_probe') IS NOT NULL")
            .fetch_one(&mut *owner_transaction)
            .await
            .expect("occupied object was preserved");
    assert!(exists);
    owner_transaction
        .rollback()
        .await
        .expect("finish inspection");
    drop(owner);
    owner_query(&migrator_a, "DROP TABLE codex_storage.occupied_probe").await;

    let (first, second) = tokio::join!(
        bootstrap_codex_storage(&migrator_a),
        bootstrap_codex_storage(&migrator_b)
    );
    assert_eq!(first, Ok(()));
    assert_eq!(second, Ok(()));
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    reject_namespace_objects(&migrator_a).await;

    // Match the actual restore transaction's lock, not the implementation's
    // constant, and observe bootstrap waiting before allowing it to continue.
    let mut restore_connection = migrator_a.acquire().await.expect("restore connection");
    let mut restore = restore_connection
        .begin()
        .await
        .expect("restore transaction");
    sqlx::query("SELECT pg_advisory_xact_lock(1414676819, 1)")
        .execute(&mut *restore)
        .await
        .expect("hold restore schema lock");
    let bootstrap = tokio::spawn({
        let pool = Arc::clone(&migrator_b);
        async move { bootstrap_codex_storage(&pool).await }
    });
    let mut observer = migrator_a.acquire().await.expect("lock observer");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid = 1414676819 AND objid = 1 AND objsubid = 2 AND NOT granted)")
                .fetch_one(&mut *observer).await.expect("observe bootstrap lock wait");
            if waiting { break; }
            assert!(!bootstrap.is_finished(), "bootstrap bypassed restore lock");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("bootstrap waits for restore");
    restore.rollback().await.expect("release restore lock");
    assert_eq!(bootstrap.await.expect("bootstrap task"), Ok(()));
    drop(observer);
    drop(restore_connection);

    for (grant, revoke, privilege_query) in [
        (
            "GRANT UPDATE (format_version) ON codex_storage.codex_schema_meta TO codex_runtime",
            "REVOKE UPDATE (format_version) ON codex_storage.codex_schema_meta FROM codex_runtime",
            "SELECT has_column_privilege('codex_runtime', 'codex_storage.codex_schema_meta', 'format_version', 'UPDATE')",
        ),
        (
            "GRANT UPDATE ON codex_storage.codex_schema_meta TO PUBLIC",
            "REVOKE UPDATE ON codex_storage.codex_schema_meta FROM PUBLIC",
            "SELECT has_table_privilege('codex_runtime', 'codex_storage.codex_schema_meta', 'UPDATE')",
        ),
        (
            "GRANT SELECT (version) ON codex_storage._codex_pg_migrations TO codex_runtime",
            "REVOKE SELECT (version) ON codex_storage._codex_pg_migrations FROM codex_runtime",
            "SELECT has_column_privilege('codex_runtime', 'codex_storage._codex_pg_migrations', 'version', 'SELECT')",
        ),
        (
            "GRANT SELECT ON codex_storage._codex_pg_migrations TO PUBLIC",
            "REVOKE SELECT ON codex_storage._codex_pg_migrations FROM PUBLIC",
            "SELECT has_table_privilege('codex_runtime', 'codex_storage._codex_pg_migrations', 'SELECT')",
        ),
        (
            "GRANT UPDATE (format_version) ON codex_storage.codex_schema_meta TO codex_backup",
            "REVOKE UPDATE (format_version) ON codex_storage.codex_schema_meta FROM codex_backup",
            "SELECT has_column_privilege('codex_backup', 'codex_storage.codex_schema_meta', 'format_version', 'UPDATE')",
        ),
        (
            "GRANT UPDATE (version) ON codex_storage._codex_pg_migrations TO codex_backup",
            "REVOKE UPDATE (version) ON codex_storage._codex_pg_migrations FROM codex_backup",
            "SELECT has_column_privilege('codex_backup', 'codex_storage._codex_pg_migrations', 'version', 'UPDATE')",
        ),
        (
            "GRANT CREATE ON SCHEMA codex_storage TO codex_backup",
            "REVOKE CREATE ON SCHEMA codex_storage FROM codex_backup",
            "SELECT has_schema_privilege('codex_backup', 'codex_storage', 'CREATE')",
        ),
        (
            "GRANT UPDATE (format_version) ON codex_storage.codex_schema_meta TO codex_bootstrap_graph_principal",
            "REVOKE UPDATE (format_version) ON codex_storage.codex_schema_meta FROM codex_bootstrap_graph_principal",
            "SELECT has_column_privilege('codex_bootstrap_graph_principal', 'codex_storage.codex_schema_meta', 'format_version', 'UPDATE')",
        ),
    ] {
        owner_query(&migrator_a, grant).await;
        assert_eq!(
            bootstrap_codex_storage(&migrator_a).await,
            Err(BootstrapError::Privilege)
        );
        let mut connection = runtime.acquire().await.expect("privilege observer");
        let preserved: bool = sqlx::query_scalar(privilege_query)
            .fetch_one(&mut *connection)
            .await
            .expect("refusal retains original grants");
        assert!(preserved);
        drop(connection);
        owner_query(&migrator_a, revoke).await;
        assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    }

    owner_query(
        &migrator_a,
        "CREATE SCHEMA codex_view_fixture AUTHORIZATION codex_owner; CREATE VIEW codex_view_fixture.metadata_proxy_base AS SELECT singleton, format_version, min_reader_version, min_writer_version FROM codex_storage.codex_schema_meta; CREATE VIEW codex_view_fixture.metadata_proxy AS SELECT singleton, format_version, min_reader_version, min_writer_version FROM codex_view_fixture.metadata_proxy_base; GRANT USAGE ON SCHEMA codex_view_fixture TO codex_runtime; GRANT UPDATE ON codex_view_fixture.metadata_proxy TO codex_runtime",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::Privilege)
    );
    owner_query(&migrator_a, "DROP SCHEMA codex_view_fixture CASCADE").await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));

    owner_query(
        &migrator_a,
        "CREATE SCHEMA codex_reporting_fixture AUTHORIZATION codex_owner; CREATE VIEW codex_reporting_fixture.metadata_report AS SELECT singleton, format_version FROM codex_storage.codex_schema_meta; CREATE VIEW codex_reporting_fixture.security_invoker_report WITH (security_invoker = true) AS SELECT singleton, format_version FROM codex_storage.codex_schema_meta; GRANT USAGE ON SCHEMA codex_reporting_fixture TO codex_runtime; GRANT SELECT ON codex_reporting_fixture.metadata_report, codex_reporting_fixture.security_invoker_report TO codex_runtime",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    owner_query(
        &migrator_a,
        "CREATE VIEW codex_reporting_fixture.history_report AS SELECT version FROM codex_storage._codex_pg_migrations; GRANT SELECT ON codex_reporting_fixture.history_report TO codex_runtime",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::Privilege)
    );
    owner_query(&migrator_a, "DROP SCHEMA codex_reporting_fixture CASCADE").await;

    owner_query(
        &migrator_a,
        "CREATE SCHEMA codex_rewrite_fixture AUTHORIZATION codex_owner; CREATE TABLE codex_rewrite_fixture.proxy (id INTEGER); CREATE RULE proxy_update AS ON UPDATE TO codex_rewrite_fixture.proxy DO ALSO UPDATE codex_storage.codex_schema_meta SET format_version = 1; GRANT USAGE ON SCHEMA codex_rewrite_fixture TO codex_runtime; GRANT UPDATE ON codex_rewrite_fixture.proxy TO codex_runtime",
    )
    .await;
    role_query(
        &runtime,
        "BEGIN; UPDATE codex_rewrite_fixture.proxy SET id = 1; ROLLBACK",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::Privilege)
    );
    owner_query(&migrator_a, "DROP SCHEMA codex_rewrite_fixture CASCADE").await;

    owner_query(
        &migrator_a,
        "CREATE SCHEMA codex_backup_view_fixture AUTHORIZATION codex_owner; GRANT USAGE, CREATE ON SCHEMA codex_backup_view_fixture TO codex_backup",
    )
    .await;
    role_query(
        &backup,
        "CREATE VIEW codex_backup_view_fixture.history_proxy AS SELECT version FROM codex_storage._codex_pg_migrations",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    owner_query(&migrator_a, "DROP SCHEMA codex_backup_view_fixture CASCADE").await;

    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage._codex_pg_migrations ALTER COLUMN installed_on SET DEFAULT NULL",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage._codex_pg_migrations ALTER COLUMN installed_on SET DEFAULT CURRENT_TIMESTAMP",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));

    migrator_query(
        &migrator_a,
        "SET ROLE codex_bootstrap_graph_superuser; DROP SCHEMA IF EXISTS codex_security_definer_fixture CASCADE; CREATE SCHEMA codex_security_definer_fixture; CREATE FUNCTION codex_security_definer_fixture.write_metadata() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, codex_storage AS $$ BEGIN UPDATE codex_storage.codex_schema_meta SET format_version = 1; END $$; REVOKE ALL ON FUNCTION codex_security_definer_fixture.write_metadata() FROM PUBLIC; GRANT USAGE ON SCHEMA codex_security_definer_fixture TO codex_runtime, codex_backup; GRANT EXECUTE ON FUNCTION codex_security_definer_fixture.write_metadata() TO codex_runtime, codex_backup; RESET ROLE",
    )
    .await;
    role_query(
        &runtime,
        "BEGIN; SELECT codex_security_definer_fixture.write_metadata(); ROLLBACK",
    )
    .await;
    role_query(
        &backup,
        "BEGIN; SELECT codex_security_definer_fixture.write_metadata(); ROLLBACK",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::Privilege)
    );
    migrator_query(
        &migrator_a,
        "SET ROLE codex_bootstrap_graph_superuser; REVOKE EXECUTE ON FUNCTION codex_security_definer_fixture.write_metadata() FROM codex_runtime, codex_backup; RESET ROLE",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    migrator_query(
        &migrator_a,
        "SET ROLE codex_bootstrap_graph_superuser; DROP SCHEMA codex_security_definer_fixture CASCADE; RESET ROLE",
    )
    .await;

    migrator_query(
        &migrator_a,
        "SET ROLE codex_bootstrap_graph_superuser; DROP ROLE IF EXISTS codex_bootstrap_graph_inert; CREATE ROLE codex_bootstrap_graph_inert NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS; GRANT codex_bootstrap_graph_principal TO codex_bootstrap_graph_inert WITH INHERIT FALSE, SET FALSE, ADMIN TRUE; GRANT codex_bootstrap_graph_principal TO codex_backup WITH INHERIT FALSE, SET TRUE; GRANT codex_bootstrap_graph_inert TO codex_runtime WITH INHERIT FALSE, SET FALSE, ADMIN FALSE; RESET ROLE",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    migrator_query(
        &migrator_a,
        "SET ROLE codex_bootstrap_graph_superuser; REVOKE codex_bootstrap_graph_principal FROM codex_backup; DROP ROLE codex_bootstrap_graph_inert; RESET ROLE",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));

    migrator_query(
        &migrator_a,
        "SET ROLE codex_bootstrap_graph_superuser; CREATE SCHEMA codex_superuser_view_fixture; CREATE VIEW codex_superuser_view_fixture.metadata_proxy AS SELECT singleton, format_version, min_reader_version, min_writer_version FROM codex_storage.codex_schema_meta; GRANT USAGE ON SCHEMA codex_superuser_view_fixture TO codex_runtime; GRANT UPDATE(format_version) ON codex_superuser_view_fixture.metadata_proxy TO codex_runtime; RESET ROLE",
    )
    .await;
    role_query(
        &runtime,
        "BEGIN; UPDATE codex_superuser_view_fixture.metadata_proxy SET format_version = 1; ROLLBACK",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::Privilege)
    );
    migrator_query(
        &migrator_a,
        "SET ROLE codex_bootstrap_graph_superuser; DROP SCHEMA codex_superuser_view_fixture CASCADE; RESET ROLE",
    )
    .await;

    for (raise_minimum, reset_minimum) in [
        (
            "UPDATE codex_storage.codex_schema_meta SET min_reader_version = 20",
            "UPDATE codex_storage.codex_schema_meta SET min_reader_version = 19",
        ),
        (
            "UPDATE codex_storage.codex_schema_meta SET min_writer_version = 20",
            "UPDATE codex_storage.codex_schema_meta SET min_writer_version = 19",
        ),
    ] {
        owner_query(&migrator_a, raise_minimum).await;
        assert_eq!(
            bootstrap_codex_storage(&migrator_a).await,
            Err(BootstrapError::IncompatibleNamespace)
        );
        owner_query(&migrator_a, reset_minimum).await;
        assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    }

    owner_query(
        &migrator_a,
        "REVOKE SELECT ON codex_storage.codex_schema_meta FROM codex_backup",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    let mut backup_reader = backup.acquire().await.expect("backup metadata reader");
    let backup_can_read: i64 =
        sqlx::query_scalar("SELECT count(*) FROM codex_storage.codex_schema_meta")
            .fetch_one(&mut *backup_reader)
            .await
            .expect("backup can read metadata");
    assert_eq!(backup_can_read, 1);
    drop(backup_reader);

    for (drop_key, restore_key) in [
        (
            "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT codex_schema_meta_pkey",
            "ALTER TABLE codex_storage.codex_schema_meta ADD PRIMARY KEY (singleton)",
        ),
        (
            "ALTER TABLE codex_storage._codex_pg_migrations DROP CONSTRAINT _codex_pg_migrations_pkey",
            "ALTER TABLE codex_storage._codex_pg_migrations ADD PRIMARY KEY (version)",
        ),
    ] {
        owner_query(&migrator_a, drop_key).await;
        assert_eq!(
            bootstrap_codex_storage(&migrator_a).await,
            Err(BootstrapError::IncompatibleNamespace)
        );
        owner_query(&migrator_a, restore_key).await;
        assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    }

    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT codex_schema_meta_singleton_check",
    )
    .await;
    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage.codex_schema_meta ADD CONSTRAINT codex_schema_meta_singleton_check CHECK (TRUE)",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT codex_schema_meta_singleton_check",
    )
    .await;
    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage.codex_schema_meta ADD CONSTRAINT codex_schema_meta_singleton_check CHECK (singleton)",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));

    for (drop, repair) in [
        (
            "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT codex_schema_meta_format_version_check",
            "ALTER TABLE codex_storage.codex_schema_meta ADD CONSTRAINT codex_schema_meta_format_version_check CHECK (format_version > 0)",
        ),
        (
            "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT codex_schema_meta_min_reader_version_check",
            "ALTER TABLE codex_storage.codex_schema_meta ADD CONSTRAINT codex_schema_meta_min_reader_version_check CHECK (min_reader_version > 0)",
        ),
        (
            "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT codex_schema_meta_min_writer_version_check",
            "ALTER TABLE codex_storage.codex_schema_meta ADD CONSTRAINT codex_schema_meta_min_writer_version_check CHECK (min_writer_version > 0)",
        ),
    ] {
        owner_query(&migrator_a, drop).await;
        assert_eq!(
            bootstrap_codex_storage(&migrator_a).await,
            Err(BootstrapError::IncompatibleNamespace)
        );
        owner_query(&migrator_a, repair).await;
        assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    }
    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage.codex_schema_meta ADD CONSTRAINT metadata_extra_check CHECK (format_version >= 1)",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT metadata_extra_check",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));

    owner_query(
        &migrator_a,
        "CREATE SCHEMA codex_typed_fixture AUTHORIZATION codex_owner; CREATE TYPE codex_typed_fixture.codex_meta_type AS (singleton BOOLEAN, format_version INTEGER, min_reader_version INTEGER, min_writer_version INTEGER); DROP TABLE codex_storage.codex_schema_meta; CREATE TABLE codex_storage.codex_schema_meta OF codex_typed_fixture.codex_meta_type (CONSTRAINT codex_schema_meta_pkey PRIMARY KEY (singleton), CONSTRAINT codex_schema_meta_singleton_check CHECK (singleton), CONSTRAINT codex_schema_meta_format_version_check CHECK (format_version > 0), CONSTRAINT codex_schema_meta_min_reader_version_check CHECK (min_reader_version > 0), CONSTRAINT codex_schema_meta_min_writer_version_check CHECK (min_writer_version > 0)); INSERT INTO codex_storage.codex_schema_meta VALUES (TRUE, 19, 19, 19)",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    owner_query(
        &migrator_a,
        "DROP TABLE codex_storage.codex_schema_meta; DROP SCHEMA codex_typed_fixture CASCADE; CREATE TABLE codex_storage.codex_schema_meta (singleton BOOLEAN DEFAULT TRUE, format_version INTEGER NOT NULL, min_reader_version INTEGER NOT NULL, min_writer_version INTEGER NOT NULL, CONSTRAINT codex_schema_meta_pkey PRIMARY KEY (singleton), CONSTRAINT codex_schema_meta_singleton_check CHECK (singleton), CONSTRAINT codex_schema_meta_format_version_check CHECK (format_version > 0), CONSTRAINT codex_schema_meta_min_reader_version_check CHECK (min_reader_version > 0), CONSTRAINT codex_schema_meta_min_writer_version_check CHECK (min_writer_version > 0)); INSERT INTO codex_storage.codex_schema_meta VALUES (TRUE, 19, 19, 19); REVOKE ALL ON codex_storage.codex_schema_meta FROM codex_runtime, codex_backup; GRANT SELECT ON codex_storage.codex_schema_meta TO codex_runtime, codex_backup",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));

    owner_query(
        &migrator_a,
        "CREATE SCHEMA codex_typed_history_fixture AUTHORIZATION codex_owner; CREATE TYPE codex_typed_history_fixture.codex_history_type AS (version BIGINT, description TEXT, installed_on TIMESTAMPTZ, success BOOLEAN, checksum BYTEA, execution_time BIGINT); CREATE TABLE codex_typed_history_fixture.history_saved AS TABLE codex_storage._codex_pg_migrations; DROP TABLE codex_storage._codex_pg_migrations; CREATE TABLE codex_storage._codex_pg_migrations OF codex_typed_history_fixture.codex_history_type (version WITH OPTIONS NOT NULL, description WITH OPTIONS NOT NULL, installed_on WITH OPTIONS DEFAULT CURRENT_TIMESTAMP NOT NULL, success WITH OPTIONS NOT NULL, checksum WITH OPTIONS NOT NULL, execution_time WITH OPTIONS NOT NULL, CONSTRAINT _codex_pg_migrations_pkey PRIMARY KEY (version)); INSERT INTO codex_storage._codex_pg_migrations SELECT * FROM codex_typed_history_fixture.history_saved",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    owner_query(
        &migrator_a,
        "DROP TABLE codex_storage._codex_pg_migrations; CREATE TABLE codex_storage._codex_pg_migrations (version BIGINT NOT NULL PRIMARY KEY, description TEXT NOT NULL, installed_on TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, success BOOLEAN NOT NULL, checksum BYTEA NOT NULL, execution_time BIGINT NOT NULL); INSERT INTO codex_storage._codex_pg_migrations SELECT * FROM codex_typed_history_fixture.history_saved; DROP SCHEMA codex_typed_history_fixture CASCADE; REVOKE ALL ON codex_storage._codex_pg_migrations FROM codex_runtime, codex_backup; GRANT SELECT ON codex_storage._codex_pg_migrations TO codex_backup",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));

    owner_query(
        &migrator_a,
        "REVOKE USAGE ON SCHEMA codex_storage FROM codex_runtime, codex_backup",
    )
    .await;
    assert_eq!(bootstrap_codex_storage(&migrator_a).await, Ok(()));
    let mut schema_observer = migrator_a.acquire().await.expect("inspect schema grants");
    let schema_usage: (bool, bool) = sqlx::query_as(
        "SELECT has_schema_privilege('codex_runtime', 'codex_storage', 'USAGE'), has_schema_privilege('codex_backup', 'codex_storage', 'USAGE')",
    )
    .fetch_one(&mut *schema_observer)
    .await
    .expect("reader roles regain schema access");
    assert_eq!(schema_usage, (true, true));
    drop(schema_observer);

    for (damage, repair) in [
        (
            "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT codex_schema_meta_pkey; ALTER TABLE codex_storage.codex_schema_meta ADD CONSTRAINT codex_schema_meta_pkey PRIMARY KEY (format_version)",
            "ALTER TABLE codex_storage.codex_schema_meta DROP CONSTRAINT codex_schema_meta_pkey; ALTER TABLE codex_storage.codex_schema_meta ADD CONSTRAINT codex_schema_meta_pkey PRIMARY KEY (singleton)",
        ),
        (
            "ALTER TABLE codex_storage._codex_pg_migrations DROP CONSTRAINT _codex_pg_migrations_pkey; ALTER TABLE codex_storage._codex_pg_migrations ADD CONSTRAINT _codex_pg_migrations_pkey PRIMARY KEY (description)",
            "ALTER TABLE codex_storage._codex_pg_migrations DROP CONSTRAINT _codex_pg_migrations_pkey; ALTER TABLE codex_storage._codex_pg_migrations ADD CONSTRAINT _codex_pg_migrations_pkey PRIMARY KEY (version)",
        ),
    ] {
        owner_query(&migrator_a, damage).await;
        assert_eq!(
            bootstrap_codex_storage(&migrator_a).await,
            Err(BootstrapError::IncompatibleNamespace)
        );
        owner_query(&migrator_a, repair).await;
    }
    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage.codex_schema_meta ENABLE ROW LEVEL SECURITY",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    owner_query(
        &migrator_a,
        "ALTER TABLE codex_storage.codex_schema_meta DISABLE ROW LEVEL SECURITY",
    )
    .await;
    for (damage, repair, expected_error) in [
        (
            "ALTER TABLE codex_storage._codex_pg_migrations ADD COLUMN required_probe TEXT NOT NULL DEFAULT 'present'; ALTER TABLE codex_storage._codex_pg_migrations ALTER COLUMN required_probe DROP DEFAULT",
            "ALTER TABLE codex_storage._codex_pg_migrations DROP COLUMN required_probe",
            BootstrapError::IncompatibleNamespace,
        ),
        (
            "ALTER TABLE codex_storage._codex_pg_migrations ADD CONSTRAINT history_version_limit CHECK (version <= 19)",
            "ALTER TABLE codex_storage._codex_pg_migrations DROP CONSTRAINT history_version_limit",
            BootstrapError::IncompatibleNamespace,
        ),
        (
            "ALTER TABLE codex_storage.codex_schema_meta SET UNLOGGED",
            "ALTER TABLE codex_storage.codex_schema_meta SET LOGGED",
            BootstrapError::IncompatibleNamespace,
        ),
        (
            "CREATE RULE history_update_rewrite AS ON UPDATE TO codex_storage._codex_pg_migrations DO INSTEAD NOTHING",
            "DROP RULE history_update_rewrite ON codex_storage._codex_pg_migrations",
            BootstrapError::IncompatibleNamespace,
        ),
        (
            "CREATE TRIGGER metadata_update_probe BEFORE UPDATE ON codex_storage.codex_schema_meta FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()",
            "DROP TRIGGER metadata_update_probe ON codex_storage.codex_schema_meta",
            BootstrapError::IncompatibleNamespace,
        ),
        (
            "CREATE SCHEMA codex_external AUTHORIZATION codex_owner; CREATE TABLE codex_external.metadata_child () INHERITS (codex_storage.codex_schema_meta)",
            "DROP SCHEMA codex_external CASCADE",
            BootstrapError::IncompatibleNamespace,
        ),
        (
            "GRANT SELECT(version) ON codex_storage._codex_pg_migrations TO codex_backup WITH GRANT OPTION",
            "REVOKE GRANT OPTION FOR SELECT(version) ON codex_storage._codex_pg_migrations FROM codex_backup CASCADE",
            BootstrapError::Privilege,
        ),
    ] {
        owner_query(&migrator_a, damage).await;
        assert_eq!(
            bootstrap_codex_storage(&migrator_a).await,
            Err(expected_error)
        );
        owner_query(&migrator_a, repair).await;
    }

    owner_query(
        &migrator_a,
        "CREATE TYPE codex_storage.occupied_type AS ENUM ('x')",
    )
    .await;
    assert_eq!(
        bootstrap_codex_storage(&migrator_a).await,
        Err(BootstrapError::IncompatibleNamespace)
    );
    owner_query(&migrator_a, "DROP TYPE codex_storage.occupied_type").await;

    let mut reader = runtime.acquire().await.expect("runtime connection");
    let readable: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM codex_storage.codex_schema_meta)")
            .fetch_one(&mut *reader)
            .await
            .expect("runtime reads schema metadata");
    assert!(readable);
    for forbidden in [
        "UPDATE codex_storage.codex_schema_meta SET format_version = 99",
        "CREATE TABLE codex_storage.forbidden_probe(id BIGINT)",
        "SELECT version FROM codex_storage._codex_pg_migrations",
    ] {
        let error = sqlx::query(forbidden)
            .execute(&mut *reader)
            .await
            .expect_err("runtime must not alter or read migration history");
        assert_eq!(
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .as_deref(),
            Some("42501")
        );
    }
    drop(reader);
    assert_eq!(
        bootstrap_codex_storage(&runtime).await,
        Err(BootstrapError::Privilege)
    );

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
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadOnly)
            .await,
        compatible
    );
    assert_eq!(
        check_codex_storage_compatibility(&runtime, capabilities, RequiredAccess::ReadOnly).await,
        Err(CompatibilityError::Privilege)
    );

    owner_query(
        &migrator_a,
        "UPDATE codex_storage.codex_schema_meta SET min_writer_version = 20",
    )
    .await;
    assert_eq!(
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadOnly)
            .await,
        compatible
    );
    assert_eq!(
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadWrite)
            .await,
        Err(CompatibilityError::WriterTooOld)
    );
    owner_query(
        &migrator_a,
        "UPDATE codex_storage.codex_schema_meta SET min_reader_version = 20",
    )
    .await;
    assert_eq!(
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadOnly)
            .await,
        Err(CompatibilityError::ReaderTooOld)
    );
    owner_query(
        &migrator_a,
        "UPDATE codex_storage.codex_schema_meta SET format_version = 20, min_reader_version = 19, min_writer_version = 19",
    )
    .await;
    assert_eq!(
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadOnly)
            .await,
        Err(CompatibilityError::UnsupportedSchema)
    );
    owner_query(
        &migrator_a,
        "UPDATE codex_storage.codex_schema_meta SET format_version = 19",
    )
    .await;

    owner_query(
        &migrator_a,
        "UPDATE codex_storage._codex_pg_migrations SET success = FALSE WHERE version = 1",
    )
    .await;
    assert_eq!(
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadOnly)
            .await,
        Err(CompatibilityError::DirtyMigration)
    );
    owner_query(
        &migrator_a,
        "UPDATE codex_storage._codex_pg_migrations SET success = TRUE WHERE version = 1",
    )
    .await;
    let mut connection = migrator_a.acquire().await.expect("save migration checksum");
    let mut transaction = connection.begin().await.expect("begin checksum read");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume owner for checksum read");
    let checksum: Vec<u8> = sqlx::query_scalar(
        "SELECT checksum FROM codex_storage._codex_pg_migrations WHERE version = 1",
    )
    .fetch_one(&mut *transaction)
    .await
    .expect("read checksum");
    transaction.rollback().await.expect("finish checksum read");
    drop(connection);
    owner_query(
        &migrator_a,
        "UPDATE codex_storage._codex_pg_migrations SET checksum = '\\x00'::bytea WHERE version = 1",
    )
    .await;
    assert_eq!(
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadOnly)
            .await,
        Err(CompatibilityError::IncompatibleHistory)
    );
    owner_query(
        &migrator_a,
        "UPDATE codex_storage._codex_pg_migrations SET checksum = repeat('x', 1048576)::bytea WHERE version = 1",
    )
    .await;
    assert_eq!(
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadOnly)
            .await,
        Err(CompatibilityError::IncompatibleHistory)
    );
    let mut connection = migrator_a
        .acquire()
        .await
        .expect("restore migration checksum");
    let mut transaction = connection.begin().await.expect("begin checksum restore");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume owner for checksum restore");
    sqlx::query("UPDATE codex_storage._codex_pg_migrations SET checksum = $1 WHERE version = 1")
        .bind(checksum)
        .execute(&mut *transaction)
        .await
        .expect("restore checksum");
    transaction.commit().await.expect("commit checksum restore");
    drop(connection);
    let mut connection = migrator_a
        .acquire()
        .await
        .expect("begin interrupted migration");
    let mut transaction = connection
        .begin()
        .await
        .expect("begin interrupted transaction");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume owner for interrupted transaction");
    sqlx::query("UPDATE codex_storage._codex_pg_migrations SET success = FALSE WHERE version = 1")
        .execute(&mut *transaction)
        .await
        .expect("write uncommitted dirty marker");
    drop(transaction);
    drop(connection);
    assert_eq!(
        check_codex_storage_compatibility(&migrator_a, capabilities, RequiredAccess::ReadOnly)
            .await,
        compatible
    );
    real_compatibility_cases::run(&migrator_a, state).await;
}
