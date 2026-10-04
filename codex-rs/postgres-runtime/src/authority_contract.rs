//! Inactive prior-version catalog observations for a future authority transition.
//!
//! This module is intentionally undeclared. It cannot admit a writer, alter roles,
//! execute an upgrade, or certify backend/session retirement. A supplied connection
//! is a catalog observation route, not proof of the selected pool's provenance.

use crate::NamedNamespace;
use crate::schema_registry::PROTECTED_TABLES;
use sqlx::PgConnection;
use sqlx::Row;

/// An expected relation comes only from the compiled canonical registry.
pub(crate) struct PriorWriterRelation {
    pub(crate) name: &'static str,
    pub(crate) runtime_privileges: &'static str,
}

pub(crate) fn prior_writer_relations() -> Vec<PriorWriterRelation> {
    PROTECTED_TABLES
        .iter()
        .map(|table| PriorWriterRelation {
            name: table.name,
            runtime_privileges: table.runtime_privileges,
        })
        .collect()
}

/// Actual server values, never an admission or physical-terminal receipt.
pub(crate) struct ObservedWriterRelation {
    pub(crate) oid: i64,
    pub(crate) name: String,
    pub(crate) runtime_insert: bool,
    pub(crate) runtime_update: bool,
    pub(crate) runtime_delete: bool,
    pub(crate) runtime_truncate: bool,
    pub(crate) runtime_trigger: bool,
    pub(crate) noninternal_triggers: i64,
    pub(crate) rewrite_rules: i64,
}

pub(crate) struct PriorCatalogObservation {
    pub(crate) database_oid: i64,
    pub(crate) namespace_oid: i64,
    pub(crate) owner_oid: i64,
    pub(crate) runtime_oid: i64,
    pub(crate) relations: Vec<ObservedWriterRelation>,
}

pub(crate) enum CatalogObservationError {
    Driver(sqlx::Error),
    UnexpectedCatalog,
}

impl From<sqlx::Error> for CatalogObservationError {
    fn from(error: sqlx::Error) -> Self {
        Self::Driver(error)
    }
}

/// Read a bounded complete registry-table set from actual catalogs. No application
/// relation is resolved through search_path. The owner/runtime identifiers derive
/// from the selected validated namespace, never arbitrary role-name inputs.
pub(crate) async fn observe_prior_catalog(
    connection: &mut PgConnection,
    namespace: Option<&NamedNamespace>,
) -> Result<PriorCatalogObservation, CatalogObservationError> {
    let (schema, owner, runtime) = namespace.map_or(
        ("codex_storage", "codex_owner", "codex_runtime"),
        |selected| {
            (
                selected.schema.as_str(),
                selected.owner.as_str(),
                selected.runtime.as_str(),
            )
        },
    );
    let identity = sqlx::query(
        "SELECT database.oid::bigint AS database_oid,
                namespace.oid::bigint AS namespace_oid,
                owner.oid::bigint AS owner_oid, runtime.oid::bigint AS runtime_oid
         FROM pg_catalog.pg_database database
         JOIN pg_catalog.pg_namespace namespace ON namespace.nspname = $1
         JOIN pg_catalog.pg_roles owner ON owner.rolname = $2
         JOIN pg_catalog.pg_roles runtime ON runtime.rolname = $3
         WHERE database.datname = pg_catalog.current_database()
           AND namespace.nspowner = owner.oid
           AND NOT owner.rolcanlogin AND NOT owner.rolsuper
           AND NOT owner.rolcreaterole AND NOT owner.rolcreatedb
           AND NOT owner.rolreplication AND NOT owner.rolbypassrls
           AND runtime.rolcanlogin AND NOT runtime.rolsuper
           AND NOT runtime.rolcreaterole AND NOT runtime.rolcreatedb
           AND NOT runtime.rolreplication AND NOT runtime.rolbypassrls",
    )
    .bind(schema)
    .bind(owner)
    .bind(runtime)
    .fetch_optional(&mut *connection)
    .await?
    .ok_or(CatalogObservationError::UnexpectedCatalog)?;
    let namespace_oid: i64 = identity.try_get("namespace_oid")?;
    let owner_oid: i64 = identity.try_get("owner_oid")?;
    let runtime_oid: i64 = identity.try_get("runtime_oid")?;
    let expected = prior_writer_relations();
    let names: Vec<&str> = expected.iter().map(|relation| relation.name).collect();
    // One extra row makes surplus base tables a refusal, rather than a hidden
    // pagination omission. Metadata and SQLx history are explicit exceptions.
    let rows = sqlx::query(
        "SELECT relation.oid::bigint AS oid, relation.relname::text AS name,
                relation.relowner::bigint AS owner_oid, relation.relkind::text AS kind,
                relation.relispartition,
                EXISTS (SELECT 1 FROM pg_catalog.pg_inherits inheritance
                        WHERE inheritance.inhrelid = relation.oid
                           OR inheritance.inhparent = relation.oid) AS inherited,
                pg_catalog.has_table_privilege($2::oid, relation.oid, 'INSERT')
                  OR pg_catalog.has_any_column_privilege($2::oid, relation.oid, 'INSERT') AS runtime_insert,
                pg_catalog.has_table_privilege($2::oid, relation.oid, 'UPDATE')
                  OR pg_catalog.has_any_column_privilege($2::oid, relation.oid, 'UPDATE') AS runtime_update,
                pg_catalog.has_table_privilege($2::oid, relation.oid, 'DELETE') AS runtime_delete,
                pg_catalog.has_table_privilege($2::oid, relation.oid, 'TRUNCATE') AS runtime_truncate,
                pg_catalog.has_table_privilege($2::oid, relation.oid, 'TRIGGER') AS runtime_trigger,
                (SELECT count(*) FROM pg_catalog.pg_trigger trigger
                 WHERE trigger.tgrelid = relation.oid AND NOT trigger.tgisinternal) AS noninternal_triggers,
                (SELECT count(*) FROM pg_catalog.pg_rewrite rule
                 WHERE rule.ev_class = relation.oid) AS rewrite_rules
         FROM pg_catalog.pg_class relation
         WHERE relation.relnamespace = $1::oid AND relation.relkind IN ('r', 'p', 'f')
           AND relation.relname NOT IN ('codex_schema_meta', '_codex_pg_migrations')
         ORDER BY relation.relname LIMIT $3",
    )
    .bind(namespace_oid)
    .bind(runtime_oid)
    .bind(i64::try_from(names.len() + 1).map_err(|_| CatalogObservationError::UnexpectedCatalog)?)
    .fetch_all(connection)
    .await?;
    if rows.len() != names.len() {
        return Err(CatalogObservationError::UnexpectedCatalog);
    }
    let mut relations = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("name")?;
        if !names.contains(&name.as_str())
            || row.try_get::<i64, _>("owner_oid")? != owner_oid
            || row.try_get::<String, _>("kind")? != "r"
            || row.try_get::<bool, _>("relispartition")?
            || row.try_get::<bool, _>("inherited")?
            || row.try_get::<i64, _>("noninternal_triggers")? != 0
            || row.try_get::<i64, _>("rewrite_rules")? != 0
        {
            return Err(CatalogObservationError::UnexpectedCatalog);
        }
        relations.push(ObservedWriterRelation {
            oid: row.try_get("oid")?,
            name,
            runtime_insert: row.try_get("runtime_insert")?,
            runtime_update: row.try_get("runtime_update")?,
            runtime_delete: row.try_get("runtime_delete")?,
            runtime_truncate: row.try_get("runtime_truncate")?,
            runtime_trigger: row.try_get("runtime_trigger")?,
            noninternal_triggers: row.try_get("noninternal_triggers")?,
            rewrite_rules: row.try_get("rewrite_rules")?,
        });
    }
    Ok(PriorCatalogObservation {
        database_oid: identity.try_get("database_oid")?,
        namespace_oid,
        owner_oid,
        runtime_oid,
        relations,
    })
}
