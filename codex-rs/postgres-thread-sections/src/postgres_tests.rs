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
use sqlx::Acquire;
use sqlx::AssertSqlSafe;
use std::path::Path;

async fn owner_permissions(pool: &PostgresPool, schema: &str, named: bool, grant: bool) {
    let (owner, role) = if named {
        ("codex_isolation_owner", "codex_isolation_runtime")
    } else {
        ("codex_owner", "codex_runtime")
    };
    let mut connection = pool.acquire().await.expect("owner fixture connection");
    let mut transaction = connection.begin().await.expect("permission transaction");
    sqlx::query(AssertSqlSafe(format!("SET LOCAL ROLE {owner}")))
        .execute(&mut *transaction)
        .await
        .expect("fixture owner");
    let statement = if grant {
        format!("GRANT DELETE ON \"{schema}\".thread_sections TO {role}")
    } else {
        format!("REVOKE DELETE ON \"{schema}\".thread_sections FROM {role}")
    };
    sqlx::query(AssertSqlSafe(statement))
        .execute(&mut *transaction)
        .await
        .expect("fixture permission");
    transaction.commit().await.expect("permission commit");
}

async fn parity_and_atomic_cleanup(state: &Path, namespace: SectionNamespace) {
    let named = matches!(namespace, SectionNamespace::Named(_));
    let schema = match &namespace {
        SectionNamespace::Default => "codex_storage",
        SectionNamespace::Named(namespace) => namespace.schema_name(),
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
    .expect("migrator");
    match &namespace {
        SectionNamespace::Default => bootstrap_codex_storage(&migrator).await.expect("schema"),
        SectionNamespace::Named(namespace) => bootstrap_named_namespace(&migrator, namespace)
            .await
            .expect("named schema"),
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
        .expect("runtime"),
    );
    let adapter = PostgresThreadSectionStore::new(Arc::clone(&pool), namespace.clone());
    let home = tempfile::tempdir().expect("SQLite home");
    let sqlite = StateRuntime::init(
        SqliteConfig::new_for_testing(home.path().abs()),
        "test".to_string(),
    )
    .await
    .expect("SQLite");
    let appearance = ThreadSectionAppearance {
        icon: Some("folder".to_string()),
        color: None,
    };
    let imported = sqlite
        .create_thread_section("imported", Some(appearance.clone()))
        .await
        .expect("SQLite section");
    let retained = adapter
        .create_thread_section("retained", Some(appearance.clone()))
        .await
        .expect("generated section");
    assert_eq!(
        Uuid::parse_str(&retained.id)
            .expect("stable ID")
            .get_version_num(),
        7
    );
    let mut expected = sqlite
        .create_thread_section("retained", Some(appearance.clone()))
        .await
        .expect("SQLite creation parity");
    expected.id = retained.id.clone();
    assert_eq!(retained, expected);
    assert_eq!(
        adapter
            .rename_thread_section(&retained.id, &retained.name, AppearanceChange::Keep)
            .await,
        Ok(Some(retained.clone()))
    );
    let empty = adapter
        .create_thread_section("empty", /*appearance*/ None)
        .await
        .expect("null appearance creation");
    let mut connection = pool.acquire().await.expect("seed");
    for section in [&retained, &empty] {
        let stored: (String, String, Option<String>) = sqlx::query_as(AssertSqlSafe(format!(
            "SELECT id,name,appearance FROM \"{schema}\".thread_sections WHERE id=$1"
        )))
        .bind(&section.id)
        .fetch_one(&mut *connection)
        .await
        .expect("persisted creation");
        assert_eq!(
            stored,
            (
                section.id.clone(),
                section.name.clone(),
                section
                    .appearance
                    .as_ref()
                    .map(|value| serde_json::to_string(value).expect("appearance"))
            )
        );
    }

    sqlx::query(AssertSqlSafe(format!(
        "INSERT INTO \"{schema}\".thread_sections(id,name,appearance) VALUES ($1,$2,$3)"
    )))
    .bind(&imported.id)
    .bind(&imported.name)
    .bind(serde_json::to_string(&appearance).expect("appearance"))
    .execute(&mut *connection)
    .await
    .expect("migrated explicit ID");
    drop(connection);
    for change in [
        AppearanceChange::Keep,
        AppearanceChange::Clear,
        AppearanceChange::Set(appearance),
    ] {
        let sqlite_change = match &change {
            AppearanceChange::Keep => None,
            AppearanceChange::Clear => Some(None),
            AppearanceChange::Set(value) => Some(Some(value.clone())),
        };
        assert_eq!(
            adapter
                .rename_thread_section(&imported.id, "renamed", change)
                .await
                .expect("PG rename"),
            sqlite
                .rename_thread_section(&imported.id, "renamed", sqlite_change)
                .await
                .expect("SQLite rename")
        );
    }
    assert_eq!(
        adapter
            .rename_thread_section(
                PINNED_THREAD_SECTION_ID,
                "forbidden",
                AppearanceChange::Keep
            )
            .await,
        Err(SectionStoreError::Protected)
    );
    assert_eq!(
        adapter
            .delete_thread_section(PINNED_THREAD_SECTION_ID)
            .await,
        Err(SectionStoreError::Protected)
    );
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
        .move_thread_to_section(id, Some(&imported.id), /*before_thread_id*/ None)
        .await
        .expect("SQLite assignment");
    let before_sqlite = sqlite
        .get_thread(id)
        .await
        .expect("SQLite row")
        .expect("member");
    let mut connection = pool.acquire().await.expect("PG member");
    sqlx::query(AssertSqlSafe(format!("INSERT INTO \"{schema}\".threads(id,origin_rollout_path,created_at_ms,updated_at_ms,recency_at_ms,source,history_mode,model_provider,origin_cwd,cli_version,title,sandbox_policy,approval_mode,thread_section_id,section_position,section_entered_at_ms) VALUES ($1::uuid,'origin',1,1,1,'cli','full','test','origin','test','section-probe','read-only','never',$2,100,123)")))
        .bind(id.to_string()).bind(&imported.id).execute(&mut *connection).await.expect("PG assignment");
    let row_query = format!("SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid");
    let before: Value = sqlx::query_scalar(AssertSqlSafe(row_query.clone()))
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("full original row");
    drop(connection);
    // Deny only DELETE, so the adapter's preceding membership UPDATE must roll back.
    owner_permissions(&migrator, schema, named, /*grant*/ false).await;
    let failed = adapter.delete_thread_section(&imported.id).await;
    owner_permissions(&migrator, schema, named, /*grant*/ true).await; // Restore before asserting.
    assert_eq!(failed, Err(SectionStoreError::Unavailable));
    let mut connection = pool.acquire().await.expect("rollback readback");
    let unchanged: Value = sqlx::query_scalar(AssertSqlSafe(row_query.clone()))
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("row after rollback");
    assert_eq!(unchanged, before);
    drop(connection);
    assert_eq!(adapter.delete_thread_section(&imported.id).await, Ok(true));
    assert!(
        sqlite
            .delete_thread_section(&imported.id)
            .await
            .expect("SQLite delete")
    );
    let mut expected_sqlite = before_sqlite;
    expected_sqlite.section = None;
    expected_sqlite.section_position = None;
    expected_sqlite.section_entered_at = None;
    assert_eq!(
        sqlite.get_thread(id).await.expect("SQLite after"),
        Some(expected_sqlite)
    );
    let mut expected = before;
    for name in [
        "thread_section_id",
        "section_position",
        "section_entered_at_ms",
    ] {
        expected[name] = Value::Null;
    }
    let mut connection = pool.acquire().await.expect("full row after");
    let after: Value = sqlx::query_scalar(AssertSqlSafe(row_query))
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("preserved thread");
    assert_eq!(after, expected);
    sqlx::query(AssertSqlSafe(format!(
        "DELETE FROM \"{schema}\".threads WHERE id=$1::uuid"
    )))
    .bind(id.to_string())
    .execute(&mut *connection)
    .await
    .expect("owned fixture cleanup");
    drop(connection);
    assert_eq!(adapter.delete_thread_section(&imported.id).await, Ok(false));
    assert_eq!(
        adapter
            .rename_thread_section(&imported.id, "missing", AppearanceChange::Keep)
            .await,
        Ok(None)
    );
    assert_eq!(adapter.delete_thread_section(&retained.id).await, Ok(true));
    assert_eq!(adapter.delete_thread_section(&empty.id).await, Ok(true));
    sqlite.close().await;
    pool.close().await.expect("close runtime");
    migrator.close().await.expect("close migrator");
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_default_crud_matches_sqlite_and_rolls_back_cleanup() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_SECTION_STORE_STATE").expect("required default fixture");
    parity_and_atomic_cleanup(Path::new(&state), SectionNamespace::Default).await;
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_named_crud_matches_sqlite_and_rolls_back_cleanup() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE").expect("required named fixture");
    let namespace = NamedNamespace::new("codex_storage_isolation").expect("qualified identifier");
    parity_and_atomic_cleanup(Path::new(&state), SectionNamespace::Named(namespace)).await;
}
