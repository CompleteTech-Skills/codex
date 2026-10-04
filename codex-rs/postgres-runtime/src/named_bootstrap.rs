//! Transactional bootstrap of one preprovisioned named namespace.

use crate::BootstrapError;
use crate::NamedNamespace;
use crate::PostgresPool;
use crate::bootstrap::BASE_MIGRATOR;
use crate::bootstrap::BOOTSTRAP_TIMEOUT;
use crate::bootstrap::LOCK_CLASS;
use crate::bootstrap::LOCK_RESOURCE;
use crate::schema_registry::MIGRATION_SHAPES;
use crate::schema_registry::PROTECTED_TABLES;
use crate::schema_registry::known_relations;
use sqlx::Acquire;
use sqlx::AssertSqlSafe;
use sqlx::SqlSafeStr;
use sqlx::migrate::Migration;
use sqlx::migrate::Migrator;
use std::borrow::Cow;
use tokio::time::timeout;

fn classify(error: &sqlx::Error) -> BootstrapError {
    crate::bootstrap::classify_sqlx(error)
}

pub(crate) fn namespaced_migrations(
    namespace: &NamedNamespace,
) -> Result<Vec<Migration>, BootstrapError> {
    let base = BASE_MIGRATOR.migrations.as_ref();
    if base.len() != MIGRATION_SHAPES.len() {
        return Err(BootstrapError::Migration);
    }
    for (migration, shape) in base.iter().zip(MIGRATION_SHAPES) {
        let source = migration.sql.as_str();
        if migration.version != shape.version
            || migration.no_tx
            || shape
                .starts_with
                .is_some_and(|opening| !source.starts_with(opening))
            || shape.contains.iter().any(|needle| !source.contains(needle))
            || source.matches("codex_storage.").count() != shape.qualified_identifiers
        {
            return Err(BootstrapError::Migration);
        }
    }
    let qualified_prefix = format!("{}.", namespace.quoted_schema());
    Ok(base
        .iter()
        .map(|base| {
            let sql = base
                .sql
                .as_str()
                .replace("codex_storage.", &qualified_prefix);
            Migration::new(
                base.version,
                base.description.clone(),
                base.migration_type,
                AssertSqlSafe(sql).into_sql_str(),
                base.no_tx,
            )
        })
        .collect())
}

/// Bootstrap a distinct preprovisioned schema using its matching migrator login.
/// This only creates metadata/history; it never selects or activates a backend.
pub async fn bootstrap_named_namespace(
    pool: &PostgresPool,
    namespace: &NamedNamespace,
) -> Result<(), BootstrapError> {
    let migrations = namespaced_migrations(namespace)?;
    timeout(BOOTSTRAP_TIMEOUT, async {
        let mut connection = pool.acquire().await.map_err(BootstrapError::Connection)?;
        let mut transaction = connection.begin().await.map_err(|error| classify(&error))?;
        let login: String = sqlx::query_scalar("SELECT session_user")
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| classify(&error))?;
        if login != namespace.migrator {
            return Err(BootstrapError::Privilege);
        }
        sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
            .bind(LOCK_CLASS)
            .bind(LOCK_RESOURCE)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| classify(&error))?;
        sqlx::query(AssertSqlSafe(format!(
            "SET LOCAL ROLE {}",
            namespace.quoted_owner()
        )))
        .execute(&mut *transaction)
        .await
        .map_err(|error| classify(&error))?;

        let owner: Option<String> = sqlx::query_scalar(
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = $1",
        )
        .bind(&namespace.schema)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|error| classify(&error))?;
        if owner.as_deref() != Some(namespace.owner.as_str()) {
            return Err(BootstrapError::IncompatibleNamespace);
        }
        let unexpected_objects: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1) AND relname <> ALL($2)) OR EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)) OR EXISTS (SELECT 1 FROM pg_type WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1) AND typtype <> 'b' AND typrelid = 0)",
        )
        .bind(&namespace.schema)
        .bind(known_relations())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| classify(&error))?;
        if unexpected_objects {
            return Err(BootstrapError::IncompatibleNamespace);
        }
        let history_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1) AND relname = '_codex_pg_migrations' AND relkind = 'r')",
        )
        .bind(&namespace.schema)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| classify(&error))?;
        if history_exists {
            let metadata = format!(
                "SELECT format_version FROM {}.\"codex_schema_meta\" WHERE singleton = TRUE",
                namespace.quoted_schema()
            );
            let format: Option<i32> = sqlx::query_scalar(AssertSqlSafe(metadata))
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| BootstrapError::IncompatibleNamespace)?;
            let history_query = format!(
                "SELECT version, success, checksum FROM {}.\"_codex_pg_migrations\" ORDER BY version",
                namespace.quoted_schema()
            );
            let history = sqlx::query(AssertSqlSafe(history_query))
                .fetch_all(&mut *transaction)
                .await
                .map_err(|_| BootstrapError::IncompatibleNamespace)?;
            if !format.is_some_and(|format| {
                crate::bootstrap::history_matches(&history, &migrations, format)
            }) {
                return Err(BootstrapError::IncompatibleNamespace);
            }
        } else {
            let occupied: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)) OR EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)) OR EXISTS (SELECT 1 FROM pg_type WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1))",
            )
            .bind(&namespace.schema)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| classify(&error))?;
            if occupied {
                return Err(BootstrapError::IncompatibleNamespace);
            }
        }

        let qualified_schema = namespace.quoted_schema();
        let history = format!("{qualified_schema}.\"_codex_pg_migrations\"");
        let migrator = Migrator {
            migrations: Cow::Owned(migrations),
            table_name: Cow::Owned(history.clone()),
            locking: false,
            ignore_missing: false,
            ..Migrator::DEFAULT
        };
        migrator
            .run_direct(/*target*/ None, &mut *transaction, /*skip*/ false)
            .await
            .map_err(|_| BootstrapError::Migration)?;
        for table in PROTECTED_TABLES {
            for statement in [
                format!(
                    "REVOKE ALL ON {qualified_schema}.\"{}\" FROM {}",
                    table.name,
                    namespace.quoted_runtime()
                ),
                format!(
                    "GRANT {} ON {qualified_schema}.\"{}\" TO {}",
                    table.runtime_privileges,
                    table.name,
                    namespace.quoted_runtime()
                ),
            ] {
                sqlx::query(AssertSqlSafe(statement))
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| classify(&error))?;
            }
        }
        sqlx::query(AssertSqlSafe(format!(
            "REVOKE ALL ON {qualified_schema}.\"codex_schema_meta\" FROM {}",
            namespace.quoted_runtime()
        )))
        .execute(&mut *transaction)
        .await
        .map_err(|error| classify(&error))?;
        sqlx::query(AssertSqlSafe(format!(
            "GRANT SELECT ON {qualified_schema}.\"codex_schema_meta\" TO {}",
            namespace.quoted_runtime()
        )))
        .execute(&mut *transaction)
        .await
        .map_err(|error| classify(&error))?;
        sqlx::query(AssertSqlSafe(format!(
            "REVOKE ALL ON {history} FROM {}",
            namespace.quoted_runtime()
        )))
        .execute(&mut *transaction)
        .await
        .map_err(|error| classify(&error))?;
        transaction.commit().await.map_err(|error| classify(&error))?;
        Ok(())
    })
    .await
    .map_err(|_| BootstrapError::Timeout)?
}
