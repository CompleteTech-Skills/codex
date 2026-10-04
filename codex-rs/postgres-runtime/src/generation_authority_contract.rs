//! Undeclared version-20 definition prerequisite, never an executable migration.
//! Deployment requires native-bound administrative retirement of ALL reachable
//! writer principals/backends/prepared transactions, exact prior history/catalog
//! eligibility, privileged role setup and atomic caller/grant/validator upgrades.
//! No function here executes SQL, enrolls a principal or constructs a permit.

use crate::NamedNamespace;
use crate::schema_registry::PROTECTED_TABLES;

const CONTROL_TABLES: &[&str] = &[
    "storage_activation",
    "storage_migration_runs",
    "storage_migration_domains",
];

/// Definitions are derived from the compiled namespace and canonical table set.
/// An operator must provision this distinct NOLOGIN role before deployment.
pub(crate) struct GenerationAuthorityDefinition {
    pub(crate) schema: String,
    pub(crate) fence_owner: String,
    pub(crate) enrollment_ddl: String,
    pub(crate) admission_ddl: String,
    pub(crate) trigger_ddl: Vec<String>,
    pub(crate) control_row_ddl: String,
}

/// This is source text only. It is NOT eligibility, role provisioning, upgrade
/// activation, credential retirement, a coherent catalog observation or authority.
pub(crate) fn generation_authority_definition(
    namespace: Option<&NamedNamespace>,
) -> GenerationAuthorityDefinition {
    let schema = namespace
        .map_or("codex_storage", NamedNamespace::schema)
        .to_owned();
    let fence_owner = namespace.map_or_else(
        || "codex_fence".to_owned(),
        |selected| {
            format!(
                "codex_{}_fence",
                &selected.schema()["codex_storage_".len()..]
            )
        },
    );
    let qualified = format!("\"{schema}\"");
    let control_names = CONTROL_TABLES
        .iter()
        .map(|name| format!("'{name}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let enrollment_ddl = format!(
        r#"
CREATE TABLE {qualified}.storage_generation_principals (
    principal_oid OID PRIMARY KEY,
    principal_name NAME NOT NULL UNIQUE,
    system_identifier TEXT NOT NULL,
    database_oid OID NOT NULL,
    dataset_id TEXT,
    generation BIGINT NOT NULL CHECK (generation >= 0),
    purpose TEXT NOT NULL CHECK (purpose IN ('runtime', 'transition', 'control')),
    run_id UUID,
    direction TEXT CHECK (direction IN ('import', 'export')),
    source_fingerprint TEXT,
    publish_generation BIGINT,
    publish_dataset_id TEXT,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    CHECK ((purpose = 'runtime' AND run_id IS NULL)
        OR (purpose IN ('transition', 'control') AND run_id IS NOT NULL)),
    CHECK (purpose <> 'runtime' OR dataset_id IS NOT NULL),
    CHECK (purpose = 'runtime' OR
        (direction IS NOT NULL AND source_fingerprint IS NOT NULL
         AND publish_generation IS NOT NULL AND publish_generation = generation + 1
         AND publish_dataset_id IS NOT NULL))
);
REVOKE ALL ON {qualified}.storage_generation_principals FROM PUBLIC;
-- Table ownership and NOLOGIN fence-owner SELECT grant are explicit privileged
-- deployment steps, NOT inferred from existing Migrator membership.
"#
    );
    let admission_ddl = format!(
        r#"
CREATE FUNCTION {qualified}.require_generation_write(control_write BOOLEAN)
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $codex_generation_gate$
DECLARE
    authenticated RECORD;
    activation RECORD;
    enrolled RECORD;
    actual_system TEXT;
    actual_database OID;
BEGIN
    SELECT oid, rolname, rolcanlogin, rolsuper, rolcreaterole, rolcreatedb,
           rolreplication, rolbypassrls INTO authenticated
    FROM pg_catalog.pg_roles WHERE rolname = session_user;
    IF NOT FOUND OR NOT authenticated.rolcanlogin OR authenticated.rolsuper
       OR authenticated.rolcreaterole OR authenticated.rolcreatedb
       OR authenticated.rolreplication OR authenticated.rolbypassrls THEN
        RAISE EXCEPTION 'generation principal refused' USING ERRCODE = '55000';
    END IF;
    -- This SAME activation-row lock lasts through actual transaction terminal,
    -- including PREPARE TRANSACTION. Backend termination alone cannot release it.
    SELECT state, dataset_id, generation, run_id INTO activation
    FROM {qualified}.storage_activation WHERE singleton = TRUE FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'activation absent' USING ERRCODE = '55000';
    END IF;
    SELECT * INTO enrolled FROM {qualified}.storage_generation_principals
    WHERE principal_oid = authenticated.oid
      AND principal_name = authenticated.rolname AND active;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'generation enrollment absent' USING ERRCODE = '55000';
    END IF;
    SELECT system_identifier::TEXT INTO STRICT actual_system
    FROM pg_catalog.pg_control_system();
    SELECT oid INTO STRICT actual_database FROM pg_catalog.pg_database
    WHERE datname = pg_catalog.current_database();
    IF enrolled.system_identifier IS DISTINCT FROM actual_system
       OR enrolled.database_oid IS DISTINCT FROM actual_database
       OR enrolled.dataset_id IS DISTINCT FROM activation.dataset_id
       OR enrolled.generation IS DISTINCT FROM activation.generation THEN
        RAISE EXCEPTION 'generation origin refused' USING ERRCODE = '55000';
    END IF;
    IF enrolled.purpose = 'runtime' THEN
        IF control_write IS DISTINCT FROM FALSE
           OR activation.state IS DISTINCT FROM 'open' THEN
            RAISE EXCEPTION 'runtime generation fenced' USING ERRCODE = '55000';
        END IF;
    ELSIF enrolled.purpose = 'control' THEN
        -- A separate one-operation control login, explicitly enrolled by the
        -- retained administrative producer after confirming the original plan.
        -- It cannot write data or serve as an owner/Migrator role exemption.
        -- The captured nullable dataset supports a genuinely empty target.
        IF control_write IS DISTINCT FROM TRUE
           OR activation.state NOT IN ('open', 'migrating')
           OR (activation.state = 'migrating'
               AND enrolled.run_id IS DISTINCT FROM activation.run_id) THEN
            RAISE EXCEPTION 'control generation fenced' USING ERRCODE = '55000';
        END IF;
        -- Initial open-to-migrating uses this principal. Final activation checks
        -- the ORIGINAL tuple before the statement; changing generation/dataset
        -- immediately makes this enrollment stale for subsequent statements.
    ELSIF enrolled.purpose = 'transition' THEN
        -- Unique explicitly administered transition login, never a blanket
        -- codex_owner/Migrator exemption or caller-supplied GUC/run marker.
        IF activation.state IS DISTINCT FROM 'migrating'
           OR enrolled.run_id IS DISTINCT FROM activation.run_id THEN
            RAISE EXCEPTION 'transition generation fenced' USING ERRCODE = '55000';
        END IF;
    ELSE
        RAISE EXCEPTION 'generation purpose refused' USING ERRCODE = '55000';
    END IF;
END;
$codex_generation_gate$;
REVOKE ALL ON FUNCTION {qualified}.require_generation_write(BOOLEAN) FROM PUBLIC;
ALTER FUNCTION {qualified}.require_generation_write(BOOLEAN)
    OWNER TO "{fence_owner}";
CREATE FUNCTION {qualified}.generation_statement_gate()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $codex_statement_gate$
BEGIN
    IF TG_TABLE_SCHEMA IS DISTINCT FROM '{schema}' THEN
        RAISE EXCEPTION 'generation relation refused' USING ERRCODE = '55000';
    END IF;
    IF TG_TABLE_NAME IN ({control_names}) AND TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'control truncate refused' USING ERRCODE = '55000';
    END IF;
    PERFORM {qualified}.require_generation_write(
        TG_TABLE_NAME IN ({control_names}));
    RETURN NULL;
END;
$codex_statement_gate$;
REVOKE ALL ON FUNCTION {qualified}.generation_statement_gate() FROM PUBLIC;
ALTER FUNCTION {qualified}.generation_statement_gate() OWNER TO "{fence_owner}";
-- Explicit privileged prerequisites: fence role NOLOGIN/NOSUPERUSER and no
-- membership/SET/CREATE/schema-meta/SQLx-history access; activation SELECT+UPDATE
-- only for the FOR SHARE dependency, enrollment SELECT, migration-runs SELECT
-- only for actual control/domain association checks, builtin EXECUTE. No other
-- control mutation grant or protected data/table grant belongs to this role.
-- Exact catalog ACL/body/proconfig/dependency validators must precede use.
-- ALTER FUNCTION OWNER requires the new owner CREATE on this schema unless a
-- genuine superuser executes it. Privileged setup may temporarily grant CREATE
-- inside the SAME deployment transaction, but must revoke it before final
-- validation/commit; no Runtime access is enabled during that temporary phase.
-- Enrollment table owner/grants, current19 checksums, version20 shape/history,
-- runtime vs transition credentials, all reachable writers and extra catalog
-- relations/rules/partitions/triggers must be validated atomically by deployment.
-- Metadata/history remain owner-only and are not part of this30-table trigger set.
"#
    );
    let control_row_ddl = format!(
        r#"
CREATE FUNCTION {qualified}.generation_control_row_gate()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $codex_control_row_gate$
DECLARE
    enrolled RECORD;
    selected_run RECORD;
BEGIN
    PERFORM {qualified}.require_generation_write(TRUE);
    SELECT enrollment.* INTO STRICT enrolled
    FROM {qualified}.storage_generation_principals enrollment
    JOIN pg_catalog.pg_roles authenticated ON authenticated.oid = enrollment.principal_oid
    WHERE authenticated.rolname = session_user AND enrollment.active;
    IF TG_TABLE_SCHEMA IS DISTINCT FROM '{schema}'
       OR enrolled.purpose NOT IN ('control', 'transition') OR TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'control row refused' USING ERRCODE = '55000';
    END IF;
    IF TG_TABLE_NAME = 'storage_migration_runs' THEN
        IF NEW.run_id IS DISTINCT FROM enrolled.run_id
           OR NEW.direction IS DISTINCT FROM enrolled.direction
           OR NEW.source_fingerprint IS DISTINCT FROM enrolled.source_fingerprint THEN
            RAISE EXCEPTION 'run association refused' USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'INSERT' AND NEW.state IS DISTINCT FROM 'running' THEN
            RAISE EXCEPTION 'initial run state refused' USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'UPDATE' AND OLD.state = 'activated'
           AND NEW.state IS DISTINCT FROM OLD.state THEN
            RAISE EXCEPTION 'activated run rewrite refused' USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'UPDATE' AND OLD.state = 'abandoned'
           AND NEW.state IS DISTINCT FROM OLD.state
           AND NOT (NEW.state = 'running' AND OLD.direction = 'import'
               AND enrolled.direction = 'import' AND EXISTS (
                   SELECT 1 FROM {qualified}.storage_activation held
                   WHERE held.singleton AND held.state = 'migrating'
                     AND held.run_id = enrolled.run_id
                     AND held.generation = enrolled.generation
                     AND held.dataset_id IS NOT DISTINCT FROM enrolled.dataset_id)) THEN
            RAISE EXCEPTION 'abandoned run resume refused' USING ERRCODE = '55000';
        END IF;
        IF TG_OP = 'UPDATE' AND (OLD.run_id IS DISTINCT FROM enrolled.run_id
           OR OLD.direction IS DISTINCT FROM NEW.direction
           OR OLD.source_fingerprint IS DISTINCT FROM NEW.source_fingerprint
           OR OLD.started_at_ms IS DISTINCT FROM NEW.started_at_ms) THEN
            RAISE EXCEPTION 'run history rewrite refused' USING ERRCODE = '55000';
        END IF;
    ELSIF TG_TABLE_NAME = 'storage_migration_domains' THEN
        IF NEW.run_id IS DISTINCT FROM enrolled.run_id
           OR (TG_OP = 'UPDATE' AND OLD.run_id IS DISTINCT FROM NEW.run_id) THEN
            RAISE EXCEPTION 'domain association refused' USING ERRCODE = '55000';
        END IF;
        SELECT state INTO STRICT selected_run FROM {qualified}.storage_migration_runs
        WHERE run_id = enrolled.run_id;
        IF selected_run.state NOT IN ('running', 'verified') THEN
            RAISE EXCEPTION 'terminal domain mutation refused' USING ERRCODE = '55000';
        END IF;
    ELSIF TG_TABLE_NAME = 'storage_activation' THEN
        IF TG_OP IS DISTINCT FROM 'UPDATE' OR NOT NEW.singleton OR NOT OLD.singleton
           OR OLD.generation IS DISTINCT FROM enrolled.generation
           OR OLD.dataset_id IS DISTINCT FROM enrolled.dataset_id
           OR NEW.run_id IS DISTINCT FROM enrolled.run_id THEN
            RAISE EXCEPTION 'activation association refused' USING ERRCODE = '55000';
        END IF;
        SELECT direction, state INTO STRICT selected_run FROM {qualified}.storage_migration_runs
        WHERE run_id = enrolled.run_id;
        IF OLD.state = 'open' AND NEW.state = 'migrating'
           AND NEW.generation = OLD.generation
           AND NEW.dataset_id IS NOT DISTINCT FROM OLD.dataset_id
           AND selected_run.state = 'running' THEN
            RETURN NEW;
        END IF;
        IF OLD.state = 'migrating' AND OLD.run_id = enrolled.run_id THEN
            IF NEW.state = 'migrating' AND NEW.generation = OLD.generation
               AND NEW.dataset_id IS NOT DISTINCT FROM OLD.dataset_id THEN
                RETURN NEW;
            END IF;
            IF NEW.state = 'open' AND enrolled.direction = 'export'
               AND selected_run.state = 'abandoned' AND NEW.generation = OLD.generation
               AND NEW.dataset_id IS NOT DISTINCT FROM OLD.dataset_id THEN
                RETURN NEW;
            END IF;
            IF selected_run.state = 'activated'
               AND NEW.generation = enrolled.publish_generation
               AND NEW.dataset_id IS NOT DISTINCT FROM enrolled.publish_dataset_id
               AND ((enrolled.direction = 'import' AND NEW.state = 'open')
                 OR (enrolled.direction = 'export' AND NEW.state = 'retired')) THEN
                RETURN NEW;
            END IF;
        END IF;
        RAISE EXCEPTION 'activation transition refused' USING ERRCODE = '55000';
    ELSE
        RAISE EXCEPTION 'control relation refused' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$codex_control_row_gate$;
REVOKE ALL ON FUNCTION {qualified}.generation_control_row_gate() FROM PUBLIC;
ALTER FUNCTION {qualified}.generation_control_row_gate() OWNER TO "{fence_owner}";
-- Enrollment intent fields must come from actual selected original plan/SQL,
-- not a public DTO. Row checks constrain identity/activation transition only;
-- domain completeness, bytes/digests and migration run state verification remain
-- genuine engine proofs, not implied by a state='activated' SQL value.
-- A transition import may capture dataset=NULL in an actually empty target;
-- it still requires exact migrating run/current generation and an actual planned
-- nonnull published dataset. Ordinary runtime enrollment can never use NULL.
"#
    );
    let trigger_ddl = PROTECTED_TABLES.iter().map(|table| {
        format!(
            "CREATE TRIGGER codex_generation_statement BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {qualified}.\"{}\" FOR EACH STATEMENT EXECUTE FUNCTION {qualified}.generation_statement_gate();",
            table.name,
        )
    }).collect();
    let mut trigger_ddl: Vec<String> = trigger_ddl;
    trigger_ddl.extend(CONTROL_TABLES.iter().map(|table| format!(
        "CREATE TRIGGER codex_generation_control_row BEFORE INSERT OR UPDATE OR DELETE ON {qualified}.\"{table}\" FOR EACH ROW EXECUTE FUNCTION {qualified}.generation_control_row_gate();"
    )));
    GenerationAuthorityDefinition {
        schema,
        fence_owner,
        enrollment_ddl,
        admission_ddl,
        trigger_ddl,
        control_row_ddl,
    }
}
