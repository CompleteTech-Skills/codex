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
use std::path::PathBuf;
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
            acquire_timeout: Duration::from_secs(5),
            max_connections: 2,
        },
    }
}

#[tokio::test]
async fn real_postgres_import_records_match_sqlite() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_IMPORT_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("migrator pool");
    bootstrap_codex_storage(&migrator)
        .await
        .expect("bootstrap import schema");
    let runtime = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
            .await
            .expect("runtime pool"),
    );
    runtime
        .bind_current_activation()
        .await
        .expect("actual runtime activation binding");
    let postgres = PostgresExternalAgentImportStore::new(runtime.clone());
    let sqlite_home = TempDir::new().expect("sqlite fixture home");
    let sqlite = StateRuntime::init(
        SqliteConfig::new_for_testing(sqlite_home.path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("sqlite state runtime");
    assert_eq!(
        postgres
            .external_agent_config_import_details_record("missing")
            .await,
        Ok(None)
    );
    assert_eq!(
        sqlite
            .external_agent_config_import_details_record("missing")
            .await
            .expect("sqlite missing"),
        None
    );
    let first = ExternalAgentConfigImportSuccessRecord {
        item_type: "CONFIG".to_string(),
        cwd: Some(PathBuf::from("Z:\\origin-host\\missing\\project")),
        source: Some("settings.json".to_string()),
        target: Some("config.toml".to_string()),
        title: None,
    };
    let replacement = ExternalAgentConfigImportSuccessRecord {
        item_type: "MCP_SERVER_CONFIG".to_string(),
        cwd: Some(PathBuf::from("/origin-host/missing/project")),
        source: Some("service".to_string()),
        target: Some("service".to_string()),
        title: Some("Imported service".to_string()),
    };
    let failure = ExternalAgentConfigImportFailureRecord {
        item_type: "CONFIG".to_string(),
        error_type: Some("invalid".to_string()),
        sub_error_type: None,
        failure_stage: "import".to_string(),
        message: "unavailable".to_string(),
        cwd: Some(PathBuf::from("/origin-host/missing/project")),
        source: Some("broken".to_string()),
    };
    for (import_id, provider_id, successes, failures) in [
        ("a", Some("provider-a"), vec![first], vec![]),
        ("b", None, vec![], vec![failure.clone()]),
        ("a", None, vec![replacement.clone()], vec![failure]),
        ("c", Some("provider-c"), vec![], vec![]),
    ] {
        sqlite
            .record_external_agent_config_import_completed(
                import_id,
                provider_id,
                &successes,
                &failures,
            )
            .await
            .expect("sqlite import upsert");
        postgres
            .record_external_agent_config_import_completed(
                import_id,
                provider_id,
                &successes,
                &failures,
            )
            .await
            .expect("PostgreSQL import upsert");
    }
    for import_id in ["a", "b", "c"] {
        assert_eq!(
            postgres
                .external_agent_config_import_details_record(import_id)
                .await
                .expect("PostgreSQL details"),
            sqlite
                .external_agent_config_import_details_record(import_id)
                .await
                .expect("SQLite details")
        );
    }
    assert_eq!(
        postgres
            .external_agent_config_import_details_record("a")
            .await
            .expect("replaced details"),
        Some(ExternalAgentConfigImportDetailsRecord {
            successes: vec![replacement],
            failures: vec![ExternalAgentConfigImportFailureRecord {
                item_type: "CONFIG".to_string(),
                error_type: Some("invalid".to_string()),
                sub_error_type: None,
                failure_stage: "import".to_string(),
                message: "unavailable".to_string(),
                cwd: Some(PathBuf::from("/origin-host/missing/project")),
                source: Some("broken".to_string()),
            }],
        })
    );
    let sqlite_db = sqlite
        .sqlite()
        .open_read_write_pool(&sqlite.sqlite().state_db_path())
        .await
        .expect("open SQLite fixture database");
    for (import_id, completed_at_ms) in [("a", 1000_i64), ("b", 1000), ("c", 2000)] {
        sqlx::query(
            "UPDATE external_agent_config_imports SET completed_at_ms = ? WHERE import_id = ?",
        )
        .bind(completed_at_ms)
        .bind(import_id)
        .execute(&sqlite_db)
        .await
        .expect("set SQLite tie timestamp");
        let mut connection = runtime.acquire().await.expect("PostgreSQL connection");
        sqlx::query("UPDATE codex_storage.external_agent_config_imports SET completed_at_ms = $1 WHERE import_id = $2")
            .bind(completed_at_ms)
            .bind(import_id)
            .execute(&mut *connection)
            .await
            .expect("set PostgreSQL tie timestamp");
    }
    let sqlite_history = sqlite
        .external_agent_config_import_history_records()
        .await
        .expect("SQLite history");
    let postgres_history = postgres
        .external_agent_config_import_history_records()
        .await
        .expect("PostgreSQL history");
    assert_eq!(postgres_history, sqlite_history);
    assert_eq!(
        postgres_history
            .iter()
            .map(|record| record.import_id.as_str())
            .collect::<Vec<_>>(),
        vec!["c", "a", "b"]
    );
    assert_eq!(postgres_history[1].provider_id, None);
    let sqlite_json: String = sqlx::query_scalar(
        "SELECT successes FROM external_agent_config_imports WHERE import_id = 'a'",
    )
    .fetch_one(&sqlite_db)
    .await
    .expect("SQLite JSON");
    let mut connection = runtime.acquire().await.expect("PostgreSQL JSON connection");
    let postgres_json: String = sqlx::query_scalar(
        "SELECT successes FROM codex_storage.external_agent_config_imports WHERE import_id = 'a'",
    )
    .fetch_one(&mut *connection)
    .await
    .expect("PostgreSQL JSON");
    assert_eq!(postgres_json, sqlite_json);
    drop(connection);
    let isolation = PostgresPool::connect(settings(state, "isolation_runtime"))
        .await
        .expect("isolation runtime pool");
    let mut isolation_connection = isolation.acquire().await.expect("isolation connection");
    let denied = sqlx::query("SELECT import_id FROM codex_storage.external_agent_config_imports")
        .fetch_optional(&mut *isolation_connection)
        .await
        .expect_err("isolation role cannot read default import records");
    assert_eq!(
        denied
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("42501")
    );
    drop(isolation_connection);
    sqlite_db.close().await;
    runtime.close().await.expect("close runtime pool");
    isolation.close().await.expect("close isolation pool");
    migrator.close().await.expect("close migrator pool");
}
