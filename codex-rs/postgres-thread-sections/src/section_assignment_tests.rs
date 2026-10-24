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

async fn ordered_parity(state: &Path, namespace: SectionNamespace) {
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
        SectionNamespace::Default => bootstrap_codex_storage(&migrator).await.expect("v5"),
        SectionNamespace::Named(value) => bootstrap_named_namespace(&migrator, value)
            .await
            .expect("named v5"),
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
    let home = tempfile::tempdir().expect("SQLite");
    let config = SqliteConfig::new_for_testing(home.path().abs());
    let sqlite = StateRuntime::init(config.clone(), "test".to_string())
        .await
        .expect("SQLite");
    let mirror = config
        .open_read_write_pool(&config.state_db_path())
        .await
        .expect("SQLite rank fixture");
    let mut connection = pool.acquire().await.expect("owned PG setup");
    let mut sections = Vec::new();
    for name in ["ordered-a", "ordered-b"] {
        let section = sqlite
            .create_thread_section(name, /*appearance*/ None)
            .await
            .expect("section");
        sqlx::query(AssertSqlSafe(format!(
            "INSERT INTO \"{schema}\".thread_sections(id,name) VALUES ($1,$2)"
        )))
        .bind(&section.id)
        .bind(name)
        .execute(&mut *connection)
        .await
        .expect("same-ID PG section");
        sections.push(section.id);
    }
    let ids = [
        ThreadId::new(),
        ThreadId::new(),
        ThreadId::new(),
        ThreadId::new(),
    ];
    let mut original = Vec::new();
    for id in ids {
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
            .expect("SQLite thread");
        sqlx::query(AssertSqlSafe(format!("INSERT INTO \"{schema}\".threads(id,origin_rollout_path,created_at_ms,updated_at_ms,recency_at_ms,source,history_mode,model_provider,origin_cwd,cli_version,title,sandbox_policy,approval_mode) VALUES ($1::uuid,'origin',1,1,1,'cli','full','test','origin','test','ordered-probe','read-only','never')")))
            .bind(id.to_string()).execute(&mut *connection).await.expect("PG thread");
        let row: Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid"
        )))
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("full row baseline");
        original.push(row);
    }
    drop(connection);
    for (actor, destination, before) in [
        (0, 0, None),
        (1, 0, None),
        (2, 0, Some(0)),
        (0, 0, None),
        (0, 1, None),
        (0, 0, Some(1)),
    ] {
        let before_id = before.map(|index| ids[index]);
        let placement = before_id.map_or(SectionPlacement::Append, SectionPlacement::Before);
        let old = sqlite
            .get_thread(ids[actor])
            .await
            .expect("old SQLite")
            .expect("retained");
        let mut connection = pool.acquire().await.expect("old PG");
        let old_entered: Option<i64> = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT section_entered_at_ms FROM \"{schema}\".threads WHERE id=$1::uuid"
        )))
        .bind(ids[actor].to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("old entry time");
        let mut prior = Vec::new();
        for id in ids {
            let row: Value = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid"
            )))
            .bind(id.to_string())
            .fetch_one(&mut *connection)
            .await
            .expect("frozen peer row");
            prior.push(row);
        }
        drop(connection);
        let started = chrono::Utc::now().timestamp_millis();
        assert_eq!(
            adapter
                .assign_thread_to_section(ids[actor], &sections[destination], placement)
                .await,
            Ok(true)
        );
        assert!(
            sqlite
                .move_thread_to_section(ids[actor], Some(&sections[destination]), before_id)
                .await
                .expect("SQLite move")
        );
        let finished = chrono::Utc::now().timestamp_millis();
        let mut connection = pool.acquire().await.expect("full PG readback");
        for (index, id) in ids.iter().enumerate() {
            let row: Value = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid"
            )))
            .bind(id.to_string())
            .fetch_one(&mut *connection)
            .await
            .expect("actual complete row");
            let sqlite_row = sqlite
                .get_thread(*id)
                .await
                .expect("SQLite readback")
                .expect("retained");
            let mut expected = original[index].clone();
            expected["thread_section_id"] = sqlite_row
                .section
                .as_ref()
                .map_or(Value::Null, |section| Value::String(section.id.clone()));
            expected["section_position"] =
                sqlite_row.section_position.map_or(Value::Null, Value::from);
            expected["section_entered_at_ms"] = if index == actor {
                row["section_entered_at_ms"].clone()
            } else {
                prior[index]["section_entered_at_ms"].clone()
            };
            assert_eq!(row, expected);
            if index == actor {
                let entered = row["section_entered_at_ms"].as_i64().expect("entry time");
                if old
                    .section
                    .as_ref()
                    .is_some_and(|value| value.id == sections[destination])
                {
                    assert_eq!(Some(entered), old_entered);
                    assert_eq!(sqlite_row.section_entered_at, old.section_entered_at);
                } else {
                    assert!((started..=finished).contains(&entered));
                    assert!(
                        (started..=finished).contains(
                            &sqlite_row
                                .section_entered_at
                                .expect("SQLite entry")
                                .timestamp_millis()
                        )
                    );
                }
            }
        }
    }
    assert_eq!(
        adapter
            .assign_thread_to_section(ids[3], &sections[0], SectionPlacement::Append)
            .await,
        Ok(true)
    );
    assert!(
        sqlite
            .move_thread_to_section(ids[3], Some(&sections[0]), /*before_thread_id*/ None)
            .await
            .expect("fourth peer")
    );
    for positions in [
        [Some(2), Some(3), Some(1), Some(4)],
        [Some(10), Some(i64::MAX), Some(20), Some(30)],
        [Some(100), None, Some(i64::MAX), Some(i64::MAX)],
    ] {
        let mut connection = pool.acquire().await.expect("owned dense/overflow fixture");
        for (id, position) in ids.iter().zip(positions) {
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE \"{schema}\".threads SET section_position=$1 WHERE id=$2::uuid"
            )))
            .bind(position)
            .bind(id.to_string())
            .execute(&mut *connection)
            .await
            .expect("PG ranks");
            sqlx::query("UPDATE threads SET section_position=? WHERE id=?")
                .bind(position)
                .bind(id.to_string())
                .execute(&mirror)
                .await
                .expect("SQLite ranks");
        }
        let mut before_rows = Vec::new();
        for id in ids {
            let row: Value = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid"
            )))
            .bind(id.to_string())
            .fetch_one(&mut *connection)
            .await
            .expect("frozen renumber row");
            before_rows.push(row);
        }
        drop(connection);
        let (actor, before) = if positions[1] == Some(i64::MAX) || positions[1].is_none() {
            (0, None)
        } else {
            (2, Some(ids[1]))
        };
        assert_eq!(
            adapter
                .assign_thread_to_section(
                    ids[actor],
                    &sections[0],
                    before.map_or(SectionPlacement::Append, SectionPlacement::Before)
                )
                .await,
            Ok(true)
        );
        assert!(
            sqlite
                .move_thread_to_section(ids[actor], Some(&sections[0]), before)
                .await
                .expect("SQLite renumber")
        );
        let mut connection = pool.acquire().await.expect("renumber readback");
        for (id, mut expected) in ids.iter().zip(before_rows) {
            let actual: Value = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid"
            )))
            .bind(id.to_string())
            .fetch_one(&mut *connection)
            .await
            .expect("complete renumber row");
            expected["section_position"] = sqlite
                .get_thread(*id)
                .await
                .expect("SQLite rank")
                .expect("thread")
                .section_position
                .map_or(Value::Null, Value::from);
            assert_eq!(actual, expected);
        }
    }
    let refusal_query = format!(
        "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id::text=ANY($1::text[]) ORDER BY id"
    );
    let mut connection = pool.acquire().await.expect("refusal baseline");
    let baseline: Vec<Value> = sqlx::query_scalar(AssertSqlSafe(refusal_query.clone()))
        .bind(ids.map(|id| id.to_string()).to_vec())
        .fetch_all(&mut *connection)
        .await
        .expect("complete refusal baseline");
    drop(connection);
    let mut sqlite_baseline = Vec::new();
    for id in ids {
        sqlite_baseline.push(
            sqlite
                .get_thread(id)
                .await
                .expect("SQLite refusal baseline"),
        );
    }
    let absent_before = ThreadId::new();
    for (section, placement, before) in [
        (&sections[0], SectionPlacement::Before(ids[0]), Some(ids[0])),
        (&sections[1], SectionPlacement::Before(ids[1]), Some(ids[1])),
        (
            &sections[0],
            SectionPlacement::Before(absent_before),
            Some(absent_before),
        ),
    ] {
        assert_eq!(
            adapter
                .assign_thread_to_section(ids[0], section, placement)
                .await,
            Err(SectionStoreError::InvalidSection)
        );
        assert!(
            sqlite
                .move_thread_to_section(ids[0], Some(section), before)
                .await
                .is_err()
        );
    }
    assert_eq!(
        adapter
            .assign_thread_to_section(ids[0], "missing", SectionPlacement::Append)
            .await,
        Err(SectionStoreError::InvalidSection)
    );
    assert!(
        sqlite
            .move_thread_to_section(ids[0], Some("missing"), /*before_thread_id*/ None)
            .await
            .is_err()
    );
    let missing = ThreadId::new();
    assert_eq!(
        adapter
            .assign_thread_to_section(missing, "missing", SectionPlacement::Before(missing))
            .await,
        Ok(false)
    );
    assert!(
        !sqlite
            .move_thread_to_section(missing, Some("missing"), Some(missing))
            .await
            .expect("missing takes precedence")
    );
    let mut connection = pool.acquire().await.expect("refusal readback");
    let unchanged: Vec<Value> = sqlx::query_scalar(AssertSqlSafe(refusal_query))
        .bind(ids.map(|id| id.to_string()).to_vec())
        .fetch_all(&mut *connection)
        .await
        .expect("complete refused rows");
    assert_eq!(unchanged, baseline);
    for (id, expected) in ids.iter().zip(sqlite_baseline) {
        assert_eq!(
            sqlite.get_thread(*id).await.expect("SQLite refused row"),
            expected
        );
    }
    drop(connection);
    let mut connection = pool.acquire().await.expect("owned cleanup");
    for id in ids {
        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM \"{schema}\".threads WHERE id=$1::uuid"
        )))
        .bind(id.to_string())
        .execute(&mut *connection)
        .await
        .expect("own threads");
    }
    drop(connection);
    for section in sections {
        assert_eq!(adapter.delete_thread_section(&section).await, Ok(true));
    }
    mirror.close().await;
    sqlite.close().await;
    pool.close().await.expect("runtime close");
    migrator.close().await.expect("migrator close");
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_default_ordered_assignment_matches_sqlite() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_SECTION_STORE_STATE").expect("required default fixture");
    ordered_parity(Path::new(&state), SectionNamespace::Default).await;
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_named_ordered_assignment_matches_sqlite() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE").expect("required named fixture");
    ordered_parity(
        Path::new(&state),
        SectionNamespace::Named(NamedNamespace::new("codex_storage_isolation").expect("namespace")),
    )
    .await;
}
