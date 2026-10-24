use super::*;
use crate::fixture_support::settings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_postgres_runtime::bootstrap_named_namespace;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionSource;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_state::ThreadMetadataBuilder;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::path::Path;

async fn clear_parity(state: &Path, namespace: SectionNamespace) {
    let named = matches!(namespace, SectionNamespace::Named(_));
    let schema = match &namespace {
        SectionNamespace::Default => "codex_storage",
        SectionNamespace::Named(value) => value.schema_name(),
    };
    let migrator = PostgresPool::connect(settings(
        state,
        if named {
            "isolation_migrator"
        } else {
            "migrator"
        },
    ))
    .await
    .expect("owned migrator");
    match &namespace {
        SectionNamespace::Default => bootstrap_codex_storage(&migrator)
            .await
            .expect("v5 catalog"),
        SectionNamespace::Named(value) => bootstrap_named_namespace(&migrator, value)
            .await
            .expect("named v5 catalog"),
    }
    let pool = Arc::new(
        PostgresPool::connect(settings(
            state,
            if named {
                "isolation_runtime"
            } else {
                "runtime"
            },
        ))
        .await
        .expect("owned runtime"),
    );
    let adapter = PostgresThreadSectionStore::new(Arc::clone(&pool), namespace.clone());
    let home = tempfile::tempdir().expect("SQLite home");
    let sqlite = StateRuntime::init(
        SqliteConfig::new_for_testing(home.path().abs()),
        "test".to_string(),
    )
    .await
    .expect("SQLite");
    let section = adapter
        .create_thread_section("clear-owned", /*appearance*/ None)
        .await
        .expect("section");
    let sqlite_section = sqlite
        .create_thread_section("clear-owned", /*appearance*/ None)
        .await
        .expect("SQLite section");
    let id = ThreadId::new();
    let metadata = ThreadMetadataBuilder::new(
        id,
        home.path().join("rollout.jsonl"),
        chrono::Utc::now(),
        SessionSource::Cli,
    )
    .build("test");
    sqlite
        .upsert_thread(&metadata)
        .await
        .expect("SQLite member");
    sqlite
        .move_thread_to_section(id, Some(&sqlite_section.id), /*before_thread_id*/ None)
        .await
        .expect("SQLite assignment");
    let mut expected_sqlite = sqlite
        .get_thread(id)
        .await
        .expect("SQLite row")
        .expect("member");
    expected_sqlite.section = None;
    expected_sqlite.section_position = None;
    expected_sqlite.section_entered_at = None;
    let mut connection = pool.acquire().await.expect("owned PG row");
    sqlx::query(AssertSqlSafe(format!("INSERT INTO \"{schema}\".threads(id,origin_rollout_path,created_at_ms,updated_at_ms,recency_at_ms,source,history_mode,model_provider,origin_cwd,cli_version,title,sandbox_policy,approval_mode,thread_section_id,section_position,section_entered_at_ms) VALUES ($1::uuid,'origin',1,1,1,'cli','full','test','origin','test','clear-probe','read-only','never',$2,100,123)")))
        .bind(id.to_string()).bind(&section.id).execute(&mut *connection).await.expect("PG assignment");
    let row_query = format!("SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid");
    let mut expected: Value = sqlx::query_scalar(AssertSqlSafe(row_query.clone()))
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("complete original row");
    for key in [
        "thread_section_id",
        "section_position",
        "section_entered_at_ms",
    ] {
        expected[key] = Value::Null;
    }
    drop(connection);
    for _ in 0..2 {
        assert_eq!(adapter.clear_thread_section_assignment(id).await, Ok(true));
        assert!(
            sqlite
                .move_thread_to_section(id, /*section*/ None, /*before_thread_id*/ None)
                .await
                .expect("SQLite clear")
        );
        assert_eq!(
            sqlite.get_thread(id).await.expect("retained SQLite"),
            Some(expected_sqlite.clone())
        );
        let mut connection = pool.acquire().await.expect("PG readback");
        let actual: Value = sqlx::query_scalar(AssertSqlSafe(row_query.clone()))
            .bind(id.to_string())
            .fetch_one(&mut *connection)
            .await
            .expect("retained full PG row");
        assert_eq!(actual, expected);
    }
    let missing = ThreadId::new();
    assert_eq!(
        adapter.clear_thread_section_assignment(missing).await,
        Ok(false)
    );
    assert!(
        !sqlite
            .move_thread_to_section(
                missing, /*section*/ None, /*before_thread_id*/ None
            )
            .await
            .expect("missing SQLite")
    );
    let mut connection = pool.acquire().await.expect("owned cleanup");
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM \"{schema}\".threads WHERE id=$1::uuid"
    )))
    .bind(id.to_string())
    .execute(&mut *connection)
    .await
    .expect("owned thread cleanup");
    drop(connection);
    assert_eq!(adapter.delete_thread_section(&section.id).await, Ok(true));
    sqlite.close().await;
    pool.close().await.expect("runtime close");
    migrator.close().await.expect("migrator close");
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_default_clear_assignment_preserves_complete_thread() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_SECTION_STORE_STATE").expect("required default fixture");
    clear_parity(Path::new(&state), SectionNamespace::Default).await;
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_named_clear_assignment_preserves_complete_thread() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE").expect("required named fixture");
    clear_parity(
        Path::new(&state),
        SectionNamespace::Named(
            NamedNamespace::new("codex_storage_isolation").expect("qualified namespace"),
        ),
    )
    .await;
}
