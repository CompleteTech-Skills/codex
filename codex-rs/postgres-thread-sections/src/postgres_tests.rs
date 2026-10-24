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
            .to_string(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: 3,
        },
    }
}

#[tokio::test]
async fn real_postgres_sections_match_sqlite_and_clean_membership() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_SECTION_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("migrator pool");
    bootstrap_codex_storage(&migrator)
        .await
        .expect("section schema");
    let runtime = Arc::new(
        PostgresPool::connect(settings(state, "runtime"))
            .await
            .expect("runtime pool"),
    );
    let postgres = PostgresThreadSectionStore::new(runtime.clone());
    // Nextest may retry after an assertion; clear only this test's fixture rows.
    let mut cleanup = runtime.acquire().await.expect("cleanup connection");
    sqlx::query("DELETE FROM codex_storage.threads WHERE title = 'section-probe'")
        .execute(&mut *cleanup)
        .await
        .expect("clean probe threads");
    sqlx::query("DELETE FROM codex_storage.thread_sections WHERE name IN ('same name', 'renamed', 'created', 'race')")
        .execute(&mut *cleanup).await.expect("clean probe sections");
    drop(cleanup);
    let home = TempDir::new().expect("SQLite fixture home");
    let sqlite = StateRuntime::init(
        SqliteConfig::new_for_testing(home.path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("SQLite state");

    let appearance = ThreadSectionAppearance {
        icon: Some("star".to_string()),
        color: None,
    };
    let section = sqlite
        .create_thread_section("same name", Some(appearance.clone()))
        .await
        .expect("SQLite section");
    let duplicate = sqlite
        .create_thread_section("same name", None)
        .await
        .expect("SQLite duplicate name");
    let mut connection = runtime.acquire().await.expect("runtime connection");
    for item in [&section, &duplicate] {
        sqlx::query(
            "INSERT INTO codex_storage.thread_sections (id, name, appearance) VALUES ($1, $2, $3)",
        )
        .bind(&item.id)
        .bind(&item.name)
        .bind(
            item.appearance
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .expect("appearance JSON"),
        )
        .execute(&mut *connection)
        .await
        .expect("seed matching PostgreSQL ID");
    }
    drop(connection);
    assert_eq!(
        postgres
            .get_thread_section(&section.id)
            .await
            .expect("PostgreSQL get"),
        sqlite
            .get_thread_section(&section.id)
            .await
            .expect("SQLite get")
    );
    let mut cursor = None;
    loop {
        let pg_page = postgres
            .list_thread_sections(cursor.as_deref(), 1)
            .await
            .expect("PostgreSQL page");
        let sqlite_page = sqlite
            .list_thread_sections(cursor.as_deref(), 1)
            .await
            .expect("SQLite page");
        assert_eq!(pg_page, sqlite_page);
        cursor = pg_page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
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
            postgres
                .rename_thread_section(&section.id, "renamed", change)
                .await
                .expect("PostgreSQL rename"),
            sqlite
                .rename_thread_section(&section.id, "renamed", sqlite_change)
                .await
                .expect("SQLite rename")
        );
    }
    assert_eq!(
        postgres
            .rename_thread_section(PINNED_THREAD_SECTION_ID, "x", AppearanceChange::Keep)
            .await,
        Err(SectionStoreError::Protected)
    );
    assert_eq!(
        postgres
            .delete_thread_section(PINNED_THREAD_SECTION_ID)
            .await,
        Err(SectionStoreError::Protected)
    );
    let created = postgres
        .create_thread_section("created", None)
        .await
        .expect("PostgreSQL create");
    assert_eq!(
        Uuid::parse_str(&created.id)
            .expect("UUIDv7")
            .get_version_num(),
        7
    );
    assert_eq!(postgres.delete_thread_section(&created.id).await, Ok(true));

    let mut connection = runtime.acquire().await.expect("thread connection");
    let thread_id = Uuid::now_v7();
    sqlx::query("INSERT INTO codex_storage.threads (id, origin_rollout_path, created_at_ms, updated_at_ms, recency_at_ms, source, history_mode, model_provider, origin_cwd, cli_version, title, sandbox_policy, approval_mode, thread_section_id, section_position, section_entered_at_ms) VALUES ($1, 'origin', 1, 1, 1, 'cli', 'full', 'test', 'origin', 'test', 'section-probe', 'read-only', 'never', NULL, NULL, NULL)")
        .bind(thread_id).execute(&mut *connection).await.expect("probe thread");
    sqlx::query("UPDATE codex_storage.threads SET thread_section_id = $1, section_position = 100, section_entered_at_ms = 123 WHERE id = $2")
        .bind(&section.id).bind(thread_id).execute(&mut *connection).await.expect("move member");
    drop(connection);
    assert_eq!(postgres.delete_thread_section(&section.id).await, Ok(true));
    let mut connection = runtime.acquire().await.expect("read member");
    let membership: (Option<String>, Option<i64>, Option<i64>) = sqlx::query_as("SELECT thread_section_id, section_position, section_entered_at_ms FROM codex_storage.threads WHERE id = $1")
        .bind(thread_id).fetch_one(&mut *connection).await.expect("member remains");
    assert_eq!(membership, (None, None, None));
    drop(connection);

    // A concurrent member move can win, or SERIALIZABLE can reject deletion.
    // Both terminal states must retain a valid section/rank relationship.
    let move_section = postgres
        .create_thread_section("race", None)
        .await
        .expect("race section");
    let mut move_tx = runtime
        .begin_serializable()
        .await
        .expect("move transaction");
    sqlx::query("UPDATE codex_storage.threads SET thread_section_id = $1, section_position = 200, section_entered_at_ms = 234 WHERE id = $2")
        .bind(&move_section.id).bind(thread_id).execute(move_tx.connection()).await.expect("pending move");
    let deleting = {
        let postgres = postgres.clone();
        let id = move_section.id.clone();
        tokio::spawn(async move { postgres.delete_thread_section(&id).await })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut observer = runtime.acquire().await.expect("observer connection");
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE usename = current_user \
                 AND wait_event_type = 'Lock' \
                 AND query LIKE 'SELECT id FROM codex_storage.thread_sections WHERE id = $1 FOR UPDATE%'",
            )
            .fetch_one(&mut *observer)
            .await
            .expect("observe delete lock wait");
            drop(observer);
            if blocked > 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("delete reached row-lock wait");
    move_tx.commit().await.expect("commit competing move");
    let result = deleting.await.expect("deletion task");
    assert!(matches!(
        result,
        Ok(true) | Err(SectionStoreError::SerializationConflict)
    ));
    let section_after = postgres
        .get_thread_section(&move_section.id)
        .await
        .expect("race section readback");
    let mut connection = runtime.acquire().await.expect("race readback connection");
    let member_after: (Option<String>, Option<i64>, Option<i64>) = sqlx::query_as("SELECT thread_section_id, section_position, section_entered_at_ms FROM codex_storage.threads WHERE id = $1")
        .bind(thread_id).fetch_one(&mut *connection).await.expect("race member readback");
    assert!(
        (result == Ok(true) && section_after.is_none() && member_after == (None, None, None))
            || (result == Err(SectionStoreError::SerializationConflict)
                && section_after == Some(move_section.clone())
                && member_after == (Some(move_section.id.clone()), Some(200), Some(234)))
    );
    drop(connection);
    runtime.close().await.expect("close runtime");
    migrator.close().await.expect("close migrator");
}
