use super::*;
use crate::fixture_support::settings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_postgres_runtime::bootstrap_named_namespace;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use sqlx::AssertSqlSafe;
use std::path::Path;

async fn read_parity(state: &Path, namespace: SectionNamespace) {
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
    let home = tempfile::tempdir().expect("SQLite mirror");
    let config = SqliteConfig::new_for_testing(home.path().abs());
    let sqlite = StateRuntime::init(config.clone(), "test".to_string())
        .await
        .expect("SQLite");
    let mirror = config
        .open_read_write_pool(&config.state_db_path())
        .await
        .expect("mirror connection");
    let prefix = format!("read-{}-", Uuid::now_v7());
    let mut connection = pool.acquire().await.expect("owned catalog");
    let mut owned = Vec::new();
    for suffix in ["A", "a", "e\u{301}", "é", "Z"] {
        let id = format!("{prefix}{suffix}");
        let appearance = if suffix == "a" {
            None
        } else {
            Some(r#"{"icon":"folder","color":"purple"}"#.to_string())
        };
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO \"{schema}\".thread_sections(id,name,appearance) VALUES ($1,$2,$3)"
        )))
        .bind(&id)
        .bind("duplicate name")
        .bind(&appearance)
        .execute(&mut *connection)
        .await
        .expect("migrated opaque ID");
        owned.push(id);
    }
    // Mirror the complete existing catalog without resetting any owned upgrade evidence.
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT id,name,appearance FROM \"{schema}\".thread_sections"
    )))
    .fetch_all(&mut *connection)
    .await
    .expect("complete source catalog");
    for (id, name, appearance) in &rows {
        sqlx::query("INSERT INTO thread_sections(id,name,appearance) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,appearance=excluded.appearance")
            .bind(id).bind(name).bind(appearance).execute(&mirror).await.expect("same-ID SQLite mirror");
        assert_eq!(
            adapter.get_thread_section(id).await.expect("Postgres get"),
            sqlite.get_thread_section(id).await.expect("SQLite get")
        );
    }
    let missing = format!("{prefix}J");
    assert_eq!(adapter.get_thread_section(&missing).await, Ok(None));
    assert_eq!(
        sqlite
            .get_thread_section(&missing)
            .await
            .expect("missing SQLite"),
        None
    );
    for limit in [0, 1, 2, 100] {
        let mut cursor = None;
        let mut seen = Vec::new();
        for _ in 0..1000 {
            let page = adapter
                .list_thread_sections(cursor.as_deref(), limit)
                .await
                .expect("Postgres page");
            assert_eq!(
                page,
                sqlite
                    .list_thread_sections(cursor.as_deref(), limit)
                    .await
                    .expect("SQLite page")
            );
            seen.extend(page.sections.iter().map(|section| section.id.clone()));
            if page.next_cursor.is_none() {
                break;
            }
            assert_ne!(page.next_cursor, cursor);
            cursor = page.next_cursor;
        }
        let mut expected: Vec<_> = rows.iter().map(|row| row.0.clone()).collect();
        expected.sort();
        assert_eq!(seen, expected);
        // A cursor need not identify a retained row: continuation is an exclusive key bound.
        assert_eq!(
            adapter
                .list_thread_sections(Some(&missing), limit)
                .await
                .expect("absent cursor"),
            sqlite
                .list_thread_sections(Some(&missing), limit)
                .await
                .expect("SQLite absent cursor")
        );
    }
    if let Ok(limit) = usize::try_from(i64::MAX) {
        assert_eq!(
            adapter.list_thread_sections(/*cursor*/ None, limit).await,
            Err(SectionStoreError::InvalidSection)
        );
        assert!(
            sqlite
                .list_thread_sections(/*cursor*/ None, limit)
                .await
                .is_err()
        );
    }
    let invalid = format!("{prefix}invalid");
    sqlx::query(AssertSqlSafe(format!("INSERT INTO \"{schema}\".thread_sections(id,name,appearance) VALUES ($1,'invalid','not-json')")))
        .bind(&invalid).execute(&mut *connection).await.expect("invalid imported appearance");
    sqlx::query("INSERT INTO thread_sections(id,name,appearance) VALUES (?,'invalid','not-json')")
        .bind(&invalid)
        .execute(&mirror)
        .await
        .expect("same invalid SQLite appearance");
    assert_eq!(
        adapter.get_thread_section(&invalid).await,
        Err(SectionStoreError::InvalidSection)
    );
    assert!(sqlite.get_thread_section(&invalid).await.is_err());
    assert_eq!(
        adapter.list_thread_sections(Some(&prefix), 100).await,
        Err(SectionStoreError::InvalidSection)
    );
    assert!(
        sqlite
            .list_thread_sections(Some(&prefix), 100)
            .await
            .is_err()
    );
    owned.push(invalid);
    for id in owned {
        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM \"{schema}\".thread_sections WHERE id=$1"
        )))
        .bind(id)
        .execute(&mut *connection)
        .await
        .expect("remove only test-owned rows");
    }
    drop(connection);
    mirror.close().await;
    sqlite.close().await;
    pool.close().await.expect("close runtime");
    migrator.close().await.expect("close migrator");
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_default_section_reads_and_cursors_match_sqlite() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_SECTION_STORE_STATE").expect("required default fixture");
    read_parity(Path::new(&state), SectionNamespace::Default).await;
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_named_section_reads_and_cursors_match_sqlite() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE").expect("required named fixture");
    read_parity(
        Path::new(&state),
        SectionNamespace::Named(
            NamedNamespace::new("codex_storage_isolation").expect("qualified namespace"),
        ),
    )
    .await;
}
