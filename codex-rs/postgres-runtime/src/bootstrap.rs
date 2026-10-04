//! Transactional metadata bootstrap for the preprovisioned `codex_storage` schema.

use crate::PoolError;
use crate::PostgresPool;
use sqlx::Acquire;
use sqlx::AssertSqlSafe;
use sqlx::PgConnection;
use sqlx::Row;
use sqlx::migrate::MigrateError;
use sqlx::migrate::Migration;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgRow;
use std::borrow::Cow;
use std::fmt;
use std::time::Duration;
use tokio::time::timeout;

pub(crate) const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
// The schema-wide lock must match scripts/postgres/container/restore-guard.sql.
pub(crate) const LOCK_CLASS: i32 = 1_414_676_819;
pub(crate) const LOCK_RESOURCE: i32 = 1; // Fixed codex_storage metadata namespace.
const MIGRATIONS_TABLE: &str = "codex_storage._codex_pg_migrations";
pub(crate) static BASE_MIGRATOR: Migrator = sqlx_macros::migrate!("./migrations");

/// Immutable expectations from this build's embedded migrations, not observed
/// server history or an admission/upgrade permission. No caller supplies hashes.
pub fn embedded_migration_checksums() -> impl Iterator<Item = (i64, &'static [u8])> {
    BASE_MIGRATOR
        .migrations
        .iter()
        .map(|migration| (migration.version, migration.checksum.as_ref()))
}

pub(crate) fn history_matches(rows: &[PgRow], migrations: &[Migration], format: i32) -> bool {
    if !crate::schema_registry::supported_format(format)
        || rows.len() != format as usize
        || rows.len() > migrations.len()
    {
        return false;
    }
    rows.iter().zip(migrations).all(|(row, migration)| {
        row.try_get::<i64, _>("version").ok() == Some(migration.version)
            && row.try_get::<bool, _>("success").ok() == Some(true)
            && row
                .try_get::<Vec<u8>, _>("checksum")
                .is_ok_and(|checksum| checksum.as_slice() == migration.checksum.as_ref())
    })
}

/// Whether the recorded history is exactly the first `format` embedded migrations.
///
/// Checksums are compared on the server so an oversized recorded value never becomes a client
/// allocation, and one extra row is enough to reject surplus history.
pub(crate) async fn recorded_history_matches(
    connection: &mut PgConnection,
    migrations: &[Migration],
    format: i32,
) -> Result<bool, sqlx::Error> {
    let Ok(expected_len) = usize::try_from(format) else {
        return Ok(false);
    };
    if expected_len == 0 || expected_len > migrations.len() {
        return Ok(false);
    }
    let versions: Vec<i64> = migrations
        .iter()
        .map(|migration| migration.version)
        .collect();
    let checksums: Vec<&[u8]> = migrations
        .iter()
        .map(|migration| migration.checksum.as_ref())
        .collect();
    let rows = sqlx::query(
        "SELECT history.version, history.success, COALESCE(history.checksum = expected.checksum, FALSE) AS checksum_matches
         FROM ONLY codex_storage._codex_pg_migrations history
         LEFT JOIN unnest($1::bigint[], $2::bytea[]) expected(version, checksum) ON history.version = expected.version
         ORDER BY history.version LIMIT $3",
    )
    .bind(versions)
    .bind(checksums)
    .bind(expected_len as i64 + 1)
    .fetch_all(connection)
    .await?;
    Ok(rows.len() == expected_len
        && rows.iter().zip(migrations).all(|(row, migration)| {
            row.try_get::<i64, _>("version").ok() == Some(migration.version)
                && row.try_get::<bool, _>("success").ok() == Some(true)
                && row.try_get::<bool, _>("checksum_matches").ok() == Some(true)
        }))
}

/// A redacted bootstrap outcome; SQLx diagnostics may contain server details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapError {
    Timeout,
    Connection(PoolError),
    Privilege,
    IncompatibleNamespace,
    Unavailable,
    Migration,
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL bootstrap error: {self:?}")
    }
}

impl std::error::Error for BootstrapError {}

pub(crate) fn classify_sqlx(error: &sqlx::Error) -> BootstrapError {
    match error {
        sqlx::Error::Database(error) if error.code().as_deref() == Some("42501") => {
            BootstrapError::Privilege
        }
        sqlx::Error::Database(error)
            if error.code().is_some_and(|code| {
                code.starts_with("08") || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
            }) =>
        {
            BootstrapError::Unavailable
        }
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::BeginFailed => BootstrapError::Unavailable,
        _ => BootstrapError::Migration,
    }
}

fn classify_migration(error: sqlx::migrate::MigrateError) -> BootstrapError {
    captured_migration(error, None)
}
pub(crate) fn captured_migration(
    error: MigrateError,
    capture: Option<&crate::bootstrap_session_owner::DdlReservation>,
) -> BootstrapError {
    let code = match &error {
        MigrateError::Execute(error) | MigrateError::ExecuteMigration(error, _) => {
            classify_sqlx(error)
        }
        _ => BootstrapError::Migration,
    };
    if let Some(capture) = capture {
        capture.failure(error);
    }
    code
}
pub(crate) fn captured_sqlx(
    error: sqlx::Error,
    capture: Option<&crate::bootstrap_session_owner::DdlReservation>,
    classify: fn(&sqlx::Error) -> BootstrapError,
) -> BootstrapError {
    let code = classify(&error);
    if let Some(capture) = capture {
        capture.failure(error);
    }
    code
}
fn captured_namespace(
    error: sqlx::Error,
    capture: Option<&crate::bootstrap_session_owner::DdlReservation>,
) -> BootstrapError {
    match capture {
        Some(capture) => captured_sqlx(error, Some(capture), classify_namespace_validation_ref),
        None => classify_namespace_validation(error),
    }
}

pub(crate) fn classify_namespace_validation(error: sqlx::Error) -> BootstrapError {
    classify_namespace_validation_ref(&error)
}
pub(crate) fn classify_namespace_validation_ref(error: &sqlx::Error) -> BootstrapError {
    match error {
        sqlx::Error::ColumnDecode { .. } => BootstrapError::IncompatibleNamespace,
        sqlx::Error::Database(error)
            if matches!(
                error.code().as_deref(),
                Some("42P01" | "42703" | "42804" | "42809")
            ) =>
        {
            BootstrapError::IncompatibleNamespace
        }
        sqlx::Error::Database(error) if error.code().as_deref() == Some("42501") => {
            BootstrapError::Privilege
        }
        _ => BootstrapError::Unavailable,
    }
}

pub(crate) async fn namespace_has_unexpected_objects(
    connection: &mut PgConnection,
    permitted_relations: &[&str],
) -> Result<bool, BootstrapError> {
    namespace_has_unexpected_objects_retained(connection, permitted_relations, None).await
}
async fn namespace_has_unexpected_objects_retained(
    connection: &mut PgConnection,
    permitted_relations: &[&str],
    capture: Option<&crate::bootstrap_session_owner::DdlReservation>,
) -> Result<bool, BootstrapError> {
    sqlx::query_scalar(
        "WITH expected_relations AS (
            SELECT oid FROM pg_class
            WHERE relnamespace = 'codex_storage'::regnamespace AND relname = ANY($1)
        ), expected_types AS (
            SELECT oid FROM pg_type WHERE typrelid IN (SELECT oid FROM expected_relations)
            UNION SELECT typarray FROM pg_type WHERE typrelid IN (SELECT oid FROM expected_relations)
        ), namespace_objects AS (
            SELECT 'pg_class'::regclass AS classid, oid AS objid FROM pg_class
            WHERE relnamespace = 'codex_storage'::regnamespace
            UNION SELECT 'pg_type'::regclass, oid FROM pg_type
            WHERE typnamespace = 'codex_storage'::regnamespace
            UNION SELECT classid, objid FROM pg_depend
            WHERE refclassid = 'pg_namespace'::regclass
              AND refobjid = 'codex_storage'::regnamespace
              AND NOT (classid = 'pg_default_acl'::regclass AND objid IN (
                  SELECT oid FROM pg_default_acl
                  WHERE defaclrole = 'codex_owner'::regrole AND defaclobjtype IN ('r', 'S')
              ))
        ) SELECT EXISTS (
            SELECT 1 FROM namespace_objects
            WHERE NOT (classid = 'pg_class'::regclass AND objid IN (SELECT oid FROM expected_relations))
              AND NOT (classid = 'pg_type'::regclass AND objid IN (SELECT oid FROM expected_types))
        )",
    )
    .bind(permitted_relations)
    .fetch_one(connection)
    .await
    .map_err(|error| captured_sqlx(error, capture, classify_sqlx))
}

pub(crate) async fn require_safe_protected_privileges(
    transaction: &mut PgConnection,
) -> Result<(), BootstrapError> {
    require_safe_protected_privileges_retained(transaction, None).await
}
async fn require_safe_protected_privileges_retained(
    connection: &mut PgConnection,
    capture: Option<&crate::bootstrap_session_owner::DdlReservation>,
) -> Result<(), BootstrapError> {
    // Check effective access, including column ACLs, PUBLIC, inherited roles,
    // and roles either login could assume explicitly.
    let unsafe_privileges: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_roles
            WHERE (pg_has_role('codex_runtime', oid, 'USAGE') OR pg_has_role('codex_runtime', oid, 'SET'))
              AND (
                has_schema_privilege(oid, 'codex_storage', 'CREATE')
                OR has_table_privilege(oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                OR has_any_column_privilege(oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,REFERENCES')
                OR has_table_privilege(oid, 'codex_storage._codex_pg_migrations', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                OR has_any_column_privilege(oid, 'codex_storage._codex_pg_migrations', 'SELECT,INSERT,UPDATE,REFERENCES')
              )
        ) OR EXISTS (
            SELECT 1 FROM pg_roles
            WHERE (rolname = 'codex_backup'
                   OR pg_has_role('codex_backup', oid, 'USAGE')
                   OR pg_has_role('codex_backup', oid, 'SET'))
              AND (
                has_schema_privilege(oid, 'codex_storage', 'CREATE')
                OR has_table_privilege(oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                OR has_any_column_privilege(oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,REFERENCES')
                OR has_table_privilege(oid, 'codex_storage._codex_pg_migrations', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                OR has_any_column_privilege(oid, 'codex_storage._codex_pg_migrations', 'INSERT,UPDATE,REFERENCES')
              )
        ) OR EXISTS (
            SELECT 1 FROM pg_roles candidate
            WHERE (candidate.rolname = 'codex_backup'
                   OR pg_has_role('codex_backup', candidate.oid, 'USAGE')
                   OR pg_has_role('codex_backup', candidate.oid, 'SET'))
              AND (pg_has_role(candidate.oid, 'codex_runtime', 'USAGE')
                   OR pg_has_role(candidate.oid, 'codex_runtime', 'SET'))
        ) OR EXISTS (
            SELECT 1 FROM pg_roles candidate
            WHERE candidate.rolname NOT IN ('codex_owner', 'codex_migrator')
              AND left(candidate.rolname, 3) <> 'pg_'
              AND NOT candidate.rolsuper
              AND (
                pg_has_role(candidate.oid, 'codex_owner', 'USAGE')
                OR pg_has_role(candidate.oid, 'codex_owner', 'SET')
                OR has_schema_privilege(candidate.oid, 'codex_storage', 'CREATE')
                OR has_table_privilege(candidate.oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                OR has_any_column_privilege(candidate.oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,REFERENCES')
                OR has_table_privilege(candidate.oid, 'codex_storage._codex_pg_migrations', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                OR has_any_column_privilege(candidate.oid, 'codex_storage._codex_pg_migrations', 'INSERT,UPDATE,REFERENCES')
              )
        ) OR EXISTS (
            SELECT 1
            FROM pg_proc routine
            JOIN pg_namespace routine_namespace ON routine_namespace.oid = routine.pronamespace
            JOIN pg_roles routine_owner ON routine_owner.oid = routine.proowner
            WHERE routine.prosecdef
              AND routine.prokind IN ('f', 'p')
              AND routine_namespace.nspname <> 'information_schema'
              AND left(routine_namespace.nspname, 3) <> 'pg_'
              AND (
                (
                  has_schema_privilege('codex_runtime', routine_namespace.oid, 'USAGE')
                  AND has_function_privilege('codex_runtime', routine.oid, 'EXECUTE')
                  AND (
                    pg_has_role(routine_owner.oid, 'codex_owner', 'USAGE')
                    OR pg_has_role(routine_owner.oid, 'codex_owner', 'SET')
                    OR has_schema_privilege(routine_owner.oid, 'codex_storage', 'CREATE')
                    OR has_table_privilege(routine_owner.oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                    OR has_any_column_privilege(routine_owner.oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,REFERENCES')
                    OR has_table_privilege(routine_owner.oid, 'codex_storage._codex_pg_migrations', 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                    OR has_any_column_privilege(routine_owner.oid, 'codex_storage._codex_pg_migrations', 'SELECT,INSERT,UPDATE,REFERENCES')
                  )
                ) OR (
                  has_schema_privilege('codex_backup', routine_namespace.oid, 'USAGE')
                  AND has_function_privilege('codex_backup', routine.oid, 'EXECUTE')
                  AND (
                    pg_has_role(routine_owner.oid, 'codex_owner', 'USAGE')
                    OR pg_has_role(routine_owner.oid, 'codex_owner', 'SET')
                    OR has_schema_privilege(routine_owner.oid, 'codex_storage', 'CREATE')
                    OR has_table_privilege(routine_owner.oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                    OR has_any_column_privilege(routine_owner.oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,REFERENCES')
                    OR has_table_privilege(routine_owner.oid, 'codex_storage._codex_pg_migrations', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                    OR has_any_column_privilege(routine_owner.oid, 'codex_storage._codex_pg_migrations', 'INSERT,UPDATE,REFERENCES')
                  )
                )
              )
        ) OR EXISTS (
            WITH RECURSIVE owner_roles(roleid) AS (
                SELECT 'codex_owner'::regrole
                UNION
                SELECT membership.member
                FROM pg_auth_members membership
                JOIN owner_roles parent ON parent.roleid = membership.roleid
            )
            SELECT 1 FROM pg_auth_members membership
            JOIN owner_roles parent ON parent.roleid = membership.roleid
            WHERE membership.admin_option
              AND membership.member <> 'codex_migrator'::regrole
        ) OR EXISTS (
            WITH RECURSIVE runtime_roles(roleid) AS (
                SELECT 'codex_runtime'::regrole
                UNION
                SELECT membership.member
                FROM pg_auth_members membership
                JOIN runtime_roles parent ON parent.roleid = membership.roleid
                WHERE membership.inherit_option
                   OR membership.set_option
                   OR membership.admin_option
            )
            SELECT 1
            FROM pg_auth_members membership
            JOIN runtime_roles target ON target.roleid = membership.roleid
            WHERE membership.admin_option
              AND (
                  pg_has_role('codex_backup', membership.member, 'USAGE')
                  OR pg_has_role('codex_backup', membership.member, 'SET')
              )
        ) OR EXISTS (
            SELECT 1 FROM pg_class history,
                 LATERAL aclexplode(coalesce(history.relacl, acldefault('r', history.relowner))) acl
            WHERE history.oid = 'codex_storage._codex_pg_migrations'::regclass
              AND acl.privilege_type = 'SELECT'
              AND acl.is_grantable
              AND acl.grantee NOT IN ('codex_owner'::regrole, 'codex_migrator'::regrole)
        ) OR EXISTS (
            SELECT 1 FROM pg_attribute attribute,
                 LATERAL aclexplode(attribute.attacl) acl
            WHERE attribute.attrelid = 'codex_storage._codex_pg_migrations'::regclass
              AND acl.privilege_type = 'SELECT'
              AND acl.is_grantable
              AND acl.grantee NOT IN ('codex_owner'::regrole, 'codex_migrator'::regrole)
        ) OR EXISTS (
            WITH RECURSIVE protected_relations(oid, depends_metadata, depends_history) AS (
                VALUES (
                    'codex_storage.codex_schema_meta'::regclass::oid,
                    TRUE,
                    FALSE
                ), (
                    'codex_storage._codex_pg_migrations'::regclass::oid,
                    FALSE,
                    TRUE
                )
            ), protected_owner_access(roleid, metadata_write, history_read, history_write) AS (
                SELECT role.oid,
                       (
                           pg_has_role(role.oid, 'codex_owner', 'USAGE')
                           OR pg_has_role(role.oid, 'codex_owner', 'SET')
                           OR has_schema_privilege(role.oid, 'codex_storage', 'CREATE')
                           OR has_table_privilege(role.oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                           OR has_any_column_privilege(role.oid, 'codex_storage.codex_schema_meta', 'INSERT,UPDATE,REFERENCES')
                       ),
                       (
                           has_table_privilege(role.oid, 'codex_storage._codex_pg_migrations', 'SELECT')
                           OR has_any_column_privilege(role.oid, 'codex_storage._codex_pg_migrations', 'SELECT')
                       ),
                       (
                           has_table_privilege(role.oid, 'codex_storage._codex_pg_migrations', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                           OR has_any_column_privilege(role.oid, 'codex_storage._codex_pg_migrations', 'INSERT,UPDATE,REFERENCES')
                       )
                FROM pg_roles role
            ), dependent_relations(oid, depends_metadata, depends_history) AS (
                SELECT relation.oid
                     , protected.depends_metadata
                     , protected.depends_history
                FROM pg_class relation
                JOIN pg_rewrite rewrite ON rewrite.ev_class = relation.oid
                JOIN pg_depend dependency
                  ON dependency.classid = 'pg_rewrite'::regclass
                 AND dependency.objid = rewrite.oid
                 AND dependency.refclassid = 'pg_class'::regclass
                JOIN protected_relations protected ON protected.oid = dependency.refobjid
                WHERE relation.oid NOT IN (SELECT oid FROM protected_relations)
                UNION
                SELECT relation.oid
                     , parent.depends_metadata
                     , parent.depends_history
                FROM pg_class relation
                JOIN pg_rewrite rewrite ON rewrite.ev_class = relation.oid
                JOIN pg_depend dependency
                  ON dependency.classid = 'pg_rewrite'::regclass
                 AND dependency.objid = rewrite.oid
                 AND dependency.refclassid = 'pg_class'::regclass
                JOIN dependent_relations parent ON parent.oid = dependency.refobjid
                WHERE relation.oid NOT IN (SELECT oid FROM protected_relations)
            )
            SELECT 1
            FROM dependent_relations dependent
            JOIN pg_class relation ON relation.oid = dependent.oid
            JOIN protected_owner_access owner_access ON owner_access.roleid = relation.relowner
            WHERE NOT (
                relation.relkind = 'v'
                AND coalesce(relation.reloptions, ARRAY[]::text[]) @> ARRAY['security_invoker=true']
            )
              AND (
                (
                    (
                        has_table_privilege('codex_runtime', relation.oid, 'SELECT')
                        OR has_any_column_privilege('codex_runtime', relation.oid, 'SELECT')
                    )
                    AND dependent.depends_history
                    AND owner_access.history_read
                ) OR (
                    (
                        has_table_privilege('codex_runtime', relation.oid, 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                        OR has_any_column_privilege('codex_runtime', relation.oid, 'INSERT,UPDATE,REFERENCES')
                    )
                    AND (
                        (dependent.depends_metadata AND owner_access.metadata_write)
                        OR (dependent.depends_history AND owner_access.history_write)
                    )
                ) OR (
                    (
                        has_table_privilege('codex_backup', relation.oid, 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
                        OR has_any_column_privilege('codex_backup', relation.oid, 'INSERT,UPDATE,REFERENCES')
                    )
                    AND (
                        (dependent.depends_metadata AND owner_access.metadata_write)
                        OR (dependent.depends_history AND owner_access.history_write)
                    )
                )
              )
        )",
    )
    .fetch_one(connection)
    .await
    .map_err(|error| captured_sqlx(error, capture, classify_sqlx))?;
    if unsafe_privileges {
        return Err(BootstrapError::Privilege);
    }
    Ok(())
}

/// Bootstrap only the preprovisioned schema, using a separate migrator login.
/// The caller must supply `codex_migrator`; this never selects an active backend.
pub async fn bootstrap_codex_storage(pool: &PostgresPool) -> Result<(), BootstrapError> {
    bootstrap_codex_storage_with_owner(pool, None).await
}
pub async fn bootstrap_codex_storage_retained(
    owner: &std::sync::Arc<crate::BootstrapSessionOwner>,
) -> Result<(), BootstrapError> {
    bootstrap_codex_storage_with_owner(owner.original_pool(), Some(owner)).await
}
async fn bootstrap_codex_storage_with_owner(
    pool: &PostgresPool,
    session_owner: Option<&std::sync::Arc<crate::BootstrapSessionOwner>>,
) -> Result<(), BootstrapError> {
    timeout(BOOTSTRAP_TIMEOUT, async {
        let mut retained = match session_owner { Some(owner) => Some(owner.loan().await?), None => None };
        let mut connection = match session_owner { None => Some(pool.acquire().await.map_err(BootstrapError::Connection)?), Some(_) => None };
        let mut transaction = match (&mut retained, &mut connection) {
            (Some(loan), _) => loan.begin().await?,
            (_, Some(connection)) => connection.begin().await.map_err(|error| classify_sqlx(&error))?,
            _ => return Err(BootstrapError::Migration),
        };
        if let Some(owner) = session_owner { owner.stamp(&mut transaction).await?; }
        let capture = session_owner.map(|owner| owner.reserve_ddl()).transpose()?;
        sqlx::query("SET LOCAL ROLE codex_owner")
            .execute(&mut *transaction)
            .await
            .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
            .bind(LOCK_CLASS)
            .bind(LOCK_RESOURCE)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;

        let owner: Option<String> = sqlx::query_scalar(
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'codex_storage'",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        if owner.as_deref() != Some("codex_owner") {
            return Err(BootstrapError::IncompatibleNamespace);
        }

        // pg_depend covers namespace-scoped objects across catalogs, including
        // operators, collations, and text search objects. The fixture's known
        // default ACLs are policy; effective table privileges are checked below.
        let unexpected_objects = namespace_has_unexpected_objects_retained(
            &mut transaction,
            &crate::schema_registry::known_relations(), capture.as_ref(),
        ).await?;
        if unexpected_objects {
            return Err(BootstrapError::IncompatibleNamespace);
        }

        let history_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace = 'codex_storage'::regnamespace AND relname = '_codex_pg_migrations' AND relkind = 'r')",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        if history_exists {
            let known_schema_complete: bool = sqlx::query_scalar(
                "SELECT NOT EXISTS (
                    SELECT 1 FROM (VALUES
                        ('codex_schema_meta', 'r'),
                        ('_codex_pg_migrations', 'r'),
                        ('codex_schema_meta_pkey', 'i'),
                        ('_codex_pg_migrations_pkey', 'i')
                    ) AS required(name, kind)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM pg_class relation
                        WHERE relation.relnamespace = 'codex_storage'::regnamespace
                          AND relation.relname = required.name
                          AND relation.relkind::text = required.kind
                          AND relation.relowner = 'codex_owner'::regrole
                          AND NOT relation.relrowsecurity
                          AND relation.relpersistence = 'p'
                    )
                ) AND NOT EXISTS (
                    SELECT 1 FROM pg_inherits inheritance
                    WHERE inheritance.inhrelid IN (
                        'codex_storage.codex_schema_meta'::regclass,
                        'codex_storage._codex_pg_migrations'::regclass
                    ) OR inheritance.inhparent IN (
                        'codex_storage.codex_schema_meta'::regclass,
                        'codex_storage._codex_pg_migrations'::regclass
                    )
                ) AND NOT EXISTS (
                    SELECT 1 FROM pg_trigger row_trigger
                    WHERE row_trigger.tgrelid IN (
                        'codex_storage.codex_schema_meta'::regclass,
                        'codex_storage._codex_pg_migrations'::regclass
                    ) AND NOT row_trigger.tgisinternal
                ) AND NOT EXISTS (
                    SELECT 1 FROM pg_rewrite rewrite
                    WHERE rewrite.ev_class IN (
                        'codex_storage.codex_schema_meta'::regclass,
                        'codex_storage._codex_pg_migrations'::regclass
                    )
                ) AND NOT EXISTS (
                    SELECT 1 FROM pg_class relation
                    WHERE relation.relnamespace = 'codex_storage'::regnamespace
                      AND relation.relname IN ('codex_schema_meta', '_codex_pg_migrations')
                      AND relation.reloftype <> 0
                ) AND EXISTS (
                    SELECT 1 FROM pg_constraint cst
                    WHERE cst.conrelid = 'codex_storage.codex_schema_meta'::regclass
                      AND cst.contype = 'p'
                      AND cst.conindid = 'codex_storage.codex_schema_meta_pkey'::regclass
                      AND cst.conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = cst.conrelid AND attname = 'singleton')]
                ) AND EXISTS (
                    SELECT 1 FROM pg_constraint cst
                    WHERE cst.conrelid = 'codex_storage._codex_pg_migrations'::regclass
                      AND cst.contype = 'p'
                      AND cst.conindid = 'codex_storage._codex_pg_migrations_pkey'::regclass
                      AND cst.conkey = ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = cst.conrelid AND attname = 'version')]
                ) AND (SELECT count(*) FROM pg_constraint
                       WHERE conrelid = 'codex_storage._codex_pg_migrations'::regclass) = 1
                AND (SELECT count(*) FROM pg_attribute
                     WHERE attrelid = 'codex_storage._codex_pg_migrations'::regclass
                       AND attnum > 0
                       AND NOT attisdropped) = 6
                AND NOT EXISTS (
                    SELECT 1
                    FROM (VALUES
                        ('version'::name, 'bigint'::regtype, TRUE, FALSE),
                        ('description'::name, 'text'::regtype, TRUE, FALSE),
                        ('installed_on'::name, 'timestamptz'::regtype, TRUE, TRUE),
                        ('success'::name, 'boolean'::regtype, TRUE, FALSE),
                        ('checksum'::name, 'bytea'::regtype, TRUE, FALSE),
                        ('execution_time'::name, 'bigint'::regtype, TRUE, FALSE)
                    ) AS required(name, type_oid, not_null, has_default)
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM pg_attribute attribute
                        WHERE attribute.attrelid = 'codex_storage._codex_pg_migrations'::regclass
                          AND attribute.attnum > 0
                          AND NOT attribute.attisdropped
                          AND attribute.attname = required.name
                          AND attribute.atttypid = required.type_oid
                          AND attribute.attnotnull = required.not_null
                          AND attribute.atthasdef = required.has_default
                    )
                )
                AND EXISTS (
                    SELECT 1 FROM pg_attribute attribute
                    JOIN pg_attrdef definition ON definition.adrelid = attribute.attrelid
                      AND definition.adnum = attribute.attnum
                    WHERE attribute.attrelid = 'codex_storage._codex_pg_migrations'::regclass
                      AND attribute.attname = 'installed_on'
                      AND pg_get_expr(definition.adbin, definition.adrelid) IN ('now()', 'CURRENT_TIMESTAMP')
                )
                AND (SELECT count(*) FROM pg_constraint
                     WHERE conrelid = 'codex_storage.codex_schema_meta'::regclass) = 5
                AND NOT EXISTS (
                    SELECT 1
                    FROM (VALUES
                        ('codex_schema_meta_pkey'::name, 'p'::\"char\", NULL::text),
                        ('codex_schema_meta_singleton_check'::name, 'c'::\"char\", 'CHECK (singleton)'),
                        ('codex_schema_meta_format_version_check'::name, 'c'::\"char\", 'CHECK ((format_version > 0))'),
                        ('codex_schema_meta_min_reader_version_check'::name, 'c'::\"char\", 'CHECK ((min_reader_version > 0))'),
                        ('codex_schema_meta_min_writer_version_check'::name, 'c'::\"char\", 'CHECK ((min_writer_version > 0))')
                    ) AS required(name, contype, definition)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM pg_constraint cst
                        WHERE cst.conrelid = 'codex_storage.codex_schema_meta'::regclass
                          AND cst.conname = required.name
                          AND cst.contype = required.contype
                          AND (required.definition IS NULL OR pg_get_constraintdef(cst.oid) = required.definition)
                    )
                )",
            )
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| captured_namespace(error, capture.as_ref()))?;
            if !known_schema_complete {
                return Err(BootstrapError::IncompatibleNamespace);
            }
            let version: Option<(i32, i32, i32)> = sqlx::query_as(
                "SELECT format_version, min_reader_version, min_writer_version FROM codex_storage.codex_schema_meta WHERE singleton = TRUE",
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|error| captured_namespace(error, capture.as_ref()))?;
            let Some((format, min_reader, min_writer)) = version else {
                return Err(BootstrapError::IncompatibleNamespace);
            };
            // Each migration records format, reader, and writer versions as one number.
            if min_reader != format || min_writer != format {
                return Err(BootstrapError::IncompatibleNamespace);
            }
            if !recorded_history_matches(&mut transaction, BASE_MIGRATOR.migrations.as_ref(), format)
                .await
                .map_err(|error| captured_namespace(error, capture.as_ref()))?
            {
                return Err(BootstrapError::IncompatibleNamespace);
            }
            // Refuse privilege drift on an existing namespace before any ACL
            // changes, so bootstrap cannot silently repair caller-owned policy.
            require_safe_protected_privileges_retained(&mut transaction, capture.as_ref()).await?;
        } else {
            let occupied = namespace_has_unexpected_objects_retained(&mut transaction, &[], capture.as_ref()).await?;
            if occupied {
                return Err(BootstrapError::IncompatibleNamespace);
            }
        }

        let migrator = Migrator {
            migrations: Cow::Borrowed(BASE_MIGRATOR.migrations.as_ref()),
            table_name: Cow::Borrowed(MIGRATIONS_TABLE),
            locking: false,
            ignore_missing: false,
            ..Migrator::DEFAULT
        };
        migrator
            .run_direct(/*target*/ None, &mut *transaction, /*skip*/ false)
            .await
            .map_err(|error| match capture.as_ref() {
                Some(capture) => captured_migration(error, Some(capture)),
                None => classify_migration(error),
            })?;
        for table in crate::schema_registry::PROTECTED_TABLES {
            for statement in [
                format!(
                    "REVOKE ALL ON codex_storage.{} FROM codex_runtime, codex_backup",
                    table.name
                ),
                format!(
                    "GRANT {} ON codex_storage.{} TO codex_runtime",
                    table.runtime_privileges, table.name
                ),
                format!("GRANT SELECT ON codex_storage.{} TO codex_backup", table.name),
            ] {
                sqlx::query(AssertSqlSafe(statement))
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
            }
        }
        // The fixture's default grants are broad; metadata and history must be immutable to runtime.
        sqlx::query("REVOKE ALL ON codex_storage.codex_schema_meta FROM codex_runtime")
            .execute(&mut *transaction)
            .await
            .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        sqlx::query("GRANT SELECT ON codex_storage.codex_schema_meta TO codex_runtime")
            .execute(&mut *transaction)
            .await
            .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        sqlx::query("GRANT SELECT ON codex_storage.codex_schema_meta TO codex_backup")
            .execute(&mut *transaction)
            .await
            .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        sqlx::query("REVOKE ALL ON codex_storage._codex_pg_migrations FROM codex_runtime, codex_backup")
            .execute(&mut *transaction)
            .await
            .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        sqlx::query("GRANT SELECT ON codex_storage._codex_pg_migrations TO codex_backup")
            .execute(&mut *transaction)
            .await
            .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        sqlx::query("GRANT USAGE ON SCHEMA codex_storage TO codex_runtime, codex_backup")
            .execute(&mut *transaction)
            .await
            .map_err(|error| captured_sqlx(error, capture.as_ref(), classify_sqlx))?;
        require_safe_protected_privileges_retained(&mut transaction, capture.as_ref()).await?;
        let unexpected_objects = namespace_has_unexpected_objects_retained(
            &mut transaction,
            &crate::schema_registry::known_relations(), capture.as_ref(),
        )
        .await?;
        if unexpected_objects {
            return Err(BootstrapError::IncompatibleNamespace);
        }
        if let Some(capture) = capture { capture.success(); }
        match session_owner {
            Some(owner) => owner.commit(transaction).await?,
            None => transaction.commit().await.map_err(|error| classify_sqlx(&error))?,
        }
        Ok(())
    })
    .await
    .map_err(|_| BootstrapError::Timeout)?
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "bootstrap_v2_tests.rs"]
mod v2_tests;
