use super::*;
use crate::NamedNamespace;
use crate::bootstrap_named_namespace;
use crate::check_named_namespace_compatibility;
use crate::named_bootstrap::namespaced_migrations;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL v4 upgrade fixture"]
async fn real_named_v4_upgrade_to_section_catalog_is_atomic() {
    let state = std::env::var("CODEX_TEST_POSTGRES_SECTION_UPGRADE_STATE")
        .expect("required disposable v4 upgrade fixture");
    let state = Path::new(&state);
    let namespace = NamedNamespace::new("codex_storage_isolation").expect("named fixture schema");
    let first = PostgresPool::connect(settings(state, "isolation_migrator"))
        .await
        .expect("first named migrator");
    let second = PostgresPool::connect(settings(state, "isolation_migrator"))
        .await
        .expect("second named migrator");
    let history = format!("{}.\"_codex_pg_migrations\"", namespace.quoted_schema());
    let v4 = Migrator {
        migrations: Cow::Owned(
            namespaced_migrations(&namespace)
                .expect("reviewed named migrations")
                .into_iter()
                .take(4)
                .collect(),
        ),
        table_name: Cow::Owned(history.clone()),
        locking: false,
        ignore_missing: false,
        ..Migrator::DEFAULT
    };
    let mut connection = first.acquire().await.expect("acquire named migrator");
    let mut transaction = connection.begin().await.expect("begin v4 fixture");
    sqlx::query("SET LOCAL ROLE codex_isolation_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume named owner");
    v4.run_direct(/*target*/ None, &mut *transaction, /*skip*/ false)
        .await
        .expect("materialize exact named v4 prefix");
    transaction.commit().await.expect("commit named v4");
    drop(connection);
    let old = ClientCapabilities {
        min_schema_format: 4,
        max_schema_format: 4,
        reader_version: 4,
        writer_version: 4,
    };
    assert_eq!(
        check_named_namespace_compatibility(&first, &namespace, old, RequiredAccess::ReadWrite)
            .await,
        Ok(CompatibilityResult {
            schema_format: 4,
            activation_permitted: false,
        })
    );

    let mut connection = first.acquire().await.expect("acquire interrupted migrator");
    let mut transaction = connection.begin().await.expect("begin interrupted upgrade");
    sqlx::query("SET LOCAL ROLE codex_isolation_owner")
        .execute(&mut *transaction)
        .await
        .expect("assume named owner for upgrade");
    let all = Migrator {
        migrations: Cow::Owned(
            namespaced_migrations(&namespace).expect("reviewed named migrations"),
        ),
        table_name: Cow::Owned(history),
        locking: false,
        ignore_missing: false,
        ..Migrator::DEFAULT
    };
    all.run_direct(/*target*/ None, &mut *transaction, /*skip*/ false)
        .await
        .expect("run v5 inside interrupted transaction");
    transaction.rollback().await.expect("roll back named v5");
    drop(connection);
    let mut observer = first.acquire().await.expect("inspect named rollback");
    let mut inspection = observer.begin().await.expect("begin inspection");
    sqlx::query("SET LOCAL ROLE codex_isolation_owner")
        .execute(&mut *inspection)
        .await
        .expect("assume named owner for inspection");
    let rolled_back: (i32, i64, bool) = sqlx::query_as("SELECT (SELECT format_version FROM codex_storage_isolation.codex_schema_meta), (SELECT COUNT(*) FROM codex_storage_isolation._codex_pg_migrations), to_regclass('codex_storage_isolation.thread_sections') IS NOT NULL")
        .fetch_one(&mut *inspection)
        .await
        .expect("read rolled-back named v4");
    assert_eq!(rolled_back, (4, 4, false));
    inspection.rollback().await.expect("finish inspection");
    drop(observer);
    let (a, b) = tokio::join!(
        bootstrap_named_namespace(&first, &namespace),
        bootstrap_named_namespace(&second, &namespace)
    );
    assert_eq!((a, b), (Ok(()), Ok(())));
    let current = ClientCapabilities {
        min_schema_format: 5,
        max_schema_format: 5,
        reader_version: 5,
        writer_version: 5,
    };
    assert_eq!(
        check_named_namespace_compatibility(&first, &namespace, current, RequiredAccess::ReadWrite)
            .await,
        Ok(CompatibilityResult {
            schema_format: 5,
            activation_permitted: false,
        })
    );
    assert_eq!(
        check_named_namespace_compatibility(&first, &namespace, old, RequiredAccess::ReadWrite)
            .await,
        Err(CompatibilityError::UnsupportedSchema)
    );
    let runtime = PostgresPool::connect(settings(state, "isolation_runtime"))
        .await
        .expect("named runtime");
    let mut connection = runtime.acquire().await.expect("runtime connection");
    let pinned: (String, String, Option<String>) = sqlx::query_as("SELECT id, name, appearance FROM codex_storage_isolation.thread_sections WHERE name = 'Pinned'")
        .fetch_one(&mut *connection)
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
}
