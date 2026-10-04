//! PostgreSQL 17 native queries for the original administrative task.
//! Raw snapshots are not a catalog validator, credential seal or retirement ACK.
//! Statistics can be transaction-cached; future retirement polling must refresh
//! its actual snapshot and retain its original native results on each attempt.
//! Row limits do not bound relation JSON/ACL aggregate payload bytes. A future
//! validator must reserve and enforce a total byte budget before using them;
//! this prerequisite makes no total memory or performance qualification claim.
pub(super) struct Statement {
    pub(super) sql: &'static str,
    pub(super) rows: usize,
}

// Limits semantic inspection of retained native values, not prior SQLx receive
// allocation, row overhead, or server-side aggregate construction.
pub(super) const PAYLOAD_BUDGET: usize = 8 * 1024 * 1024;

pub(super) enum PayloadRefusal {
    MissingSnapshot,
    Driver(sqlx::Error),
    NativeValue(Box<dyn std::error::Error + Send + Sync>),
    Budget {
        retained_before: usize,
        next_value_bytes: usize,
    },
}

pub(super) fn retained_payload_bytes(
    rows: &[sqlx::postgres::PgRow],
    retained_before: usize,
) -> Result<usize, PayloadRefusal> {
    use sqlx::Row;
    use sqlx::ValueRef;

    let mut total = retained_before;
    for row in rows {
        for index in 0..row.columns().len() {
            let value = row.try_get_raw(index).map_err(PayloadRefusal::Driver)?;
            if value.is_null() {
                continue;
            }
            let bytes = value.as_bytes().map_err(PayloadRefusal::NativeValue)?.len();
            let next = total
                .checked_add(bytes)
                .filter(|next| *next <= PAYLOAD_BUDGET);
            match next {
                Some(next) => total = next,
                None => {
                    return Err(PayloadRefusal::Budget {
                        retained_before: total,
                        next_value_bytes: bytes,
                    });
                }
            }
        }
    }
    Ok(total)
}

impl std::fmt::Debug for PayloadRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingSnapshot => {}
            Self::Driver(raw) => {
                let _ = raw;
            }
            Self::NativeValue(raw) => {
                let _ = raw;
            }
            Self::Budget {
                retained_before,
                next_value_bytes,
            } => {
                let _ = (retained_before, next_value_bytes);
            }
        }
        formatter.write_str("CatalogPayloadRefused([native values retained])")
    }
}
pub(super) const STATEMENTS: [Statement; 6] = [
    Statement {
        rows: 256,
        sql: r#"
SELECT relation.oid::bigint AS relation_oid, namespace.oid::bigint AS namespace_oid,
       namespace.nspowner::bigint AS namespace_owner, namespace.nspacl::text AS namespace_acl,
       relation.relname::text, relation.relkind::text, relation.relowner::bigint,
       relation.relacl::text, relation.relispartition, relation.relrowsecurity,
       relation.relforcerowsecurity,
       EXISTS (SELECT 1 FROM pg_catalog.pg_policy policy WHERE policy.polrelid=relation.oid) AS has_policies,
       EXISTS (SELECT 1 FROM pg_catalog.pg_trigger trigger_row
               WHERE trigger_row.tgrelid=relation.oid AND NOT trigger_row.tgisinternal) AS has_user_triggers,
       EXISTS (SELECT 1 FROM pg_catalog.pg_index index_row
               WHERE index_row.indexrelid=relation.oid
                 AND (NOT index_row.indisvalid OR NOT index_row.indisready OR NOT index_row.indislive)) AS unusable_index,
       (SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
           'native',pg_catalog.to_jsonb(definition),
           'expression',pg_catalog.pg_get_expr(definition.adbin,definition.adrelid)) ORDER BY definition.adnum)
        FROM pg_catalog.pg_attrdef definition WHERE definition.adrelid=relation.oid) AS defaults,
       (SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
           'native',pg_catalog.to_jsonb(index_row),
           'definition',pg_catalog.pg_get_indexdef(index_row.indexrelid),
           'predicate',pg_catalog.pg_get_expr(index_row.indpred,index_row.indrelid),
           'expressions',pg_catalog.pg_get_expr(index_row.indexprs,index_row.indrelid)) ORDER BY index_row.indexrelid)
        FROM pg_catalog.pg_index index_row WHERE index_row.indexrelid=relation.oid OR index_row.indrelid=relation.oid) AS indexes,
       (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(policy) ORDER BY policy.oid)
        FROM pg_catalog.pg_policy policy WHERE policy.polrelid=relation.oid) AS policies,
       EXISTS (SELECT 1 FROM pg_catalog.pg_inherits inherited
               WHERE inherited.inhrelid=relation.oid OR inherited.inhparent=relation.oid) AS has_inheritance,
       EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite rewritten
               WHERE rewritten.ev_class=relation.oid) AS has_rules,
       (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(attribute) ORDER BY attribute.attnum)
        FROM pg_catalog.pg_attribute attribute
        WHERE attribute.attrelid=relation.oid AND attribute.attnum>0) AS attributes,
       (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(constraint_row) ORDER BY constraint_row.oid)
        FROM pg_catalog.pg_constraint constraint_row
        WHERE constraint_row.conrelid=relation.oid OR constraint_row.confrelid=relation.oid) AS constraints,
       (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(trigger_row) ORDER BY trigger_row.oid)
        FROM pg_catalog.pg_trigger trigger_row WHERE trigger_row.tgrelid=relation.oid) AS triggers,
       (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(rule_row) ORDER BY rule_row.oid)
        FROM pg_catalog.pg_rewrite rule_row WHERE rule_row.ev_class=relation.oid) AS rules,
       (SELECT pg_catalog.jsonb_agg(pg_catalog.to_jsonb(inheritance) ORDER BY inheritance.inhrelid)
        FROM pg_catalog.pg_inherits inheritance
        WHERE inheritance.inhrelid=relation.oid OR inheritance.inhparent=relation.oid) AS inheritance
FROM pg_catalog.pg_class relation JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
WHERE namespace.nspname=$1
ORDER BY relation.oid LIMIT 257"#,
    },
    Statement {
        rows: 64,
        sql: r#"
SELECT routine.oid::bigint, routine.proname::text, routine.proowner::bigint,
       routine.prolang::bigint, routine.prokind::text, routine.prosecdef,
       routine.proconfig, routine.proacl::text, routine.proargtypes::text,
       routine.prorettype::bigint,
       CASE WHEN routine.prokind IN ('f','p','w')
            THEN pg_catalog.left(pg_catalog.pg_get_functiondef(routine.oid),65537) END AS definition,
       CASE WHEN routine.prokind IN ('f','p','w')
            THEN pg_catalog.octet_length(pg_catalog.pg_get_functiondef(routine.oid))>65536
            ELSE TRUE END AS oversized
FROM pg_catalog.pg_proc routine JOIN pg_catalog.pg_namespace namespace ON namespace.oid=routine.pronamespace
WHERE namespace.nspname=$1 ORDER BY routine.oid LIMIT 65"#,
    },
    Statement {
        rows: 32768,
        sql: r#"
SELECT role.oid::bigint AS principal_oid, role.rolname::text, role.rolcanlogin,
       role.rolsuper, role.rolcreaterole, role.rolbypassrls,
       relation.oid::bigint AS relation_oid, relation.relkind::text,
       pg_catalog.pg_has_role(role.oid,relation.relowner,'USAGE') AS inherited_owner,
       pg_catalog.pg_has_role(role.oid,relation.relowner,'SET') AS assumable_owner,
       pg_catalog.has_schema_privilege(role.oid,namespace.oid,'CREATE') AS schema_create,
       CASE WHEN relation.relkind='S'
            THEN pg_catalog.has_sequence_privilege(role.oid,relation.oid,'USAGE,UPDATE')
            ELSE pg_catalog.has_table_privilege(role.oid,relation.oid,'INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')
                 OR pg_catalog.has_any_column_privilege(role.oid,relation.oid,'INSERT,UPDATE')
       END AS write_or_trigger_privilege
FROM pg_catalog.pg_roles role CROSS JOIN pg_catalog.pg_class relation
JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
WHERE namespace.nspname=$1 AND relation.relkind IN ('r','p','v','m','f','S')
ORDER BY role.oid,relation.oid LIMIT 32769"#,
    },
    Statement {
        rows: 4096,
        sql: r#"
SELECT membership.oid::bigint, membership.roleid::bigint, membership.member::bigint,
       membership.grantor::bigint, membership.admin_option,
       membership.inherit_option, membership.set_option
FROM pg_catalog.pg_auth_members membership WHERE $1::text IS NOT NULL
ORDER BY membership.oid LIMIT 4097"#,
    },
    Statement {
        rows: 4096,
        sql: r#"
SELECT backend.datid::bigint, backend.pid, backend.usesysid::bigint,
       backend.usename::text, backend.backend_start, backend.xact_start,
       backend.backend_xid::text, backend.backend_type, backend.state,
       backend.wait_event_type, backend.wait_event
FROM pg_catalog.pg_stat_activity backend
WHERE backend.datid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
  AND $1::text IS NOT NULL ORDER BY backend.pid LIMIT 4097"#,
    },
    Statement {
        rows: 4096,
        sql: r#"
SELECT prepared.transaction::text AS xid, prepared.gid, prepared.prepared,
       prepared.owner::text, role.oid::bigint AS principal_oid,
       prepared.database::text, database.oid::bigint AS database_oid
FROM pg_catalog.pg_prepared_xacts prepared
LEFT JOIN pg_catalog.pg_roles role ON role.rolname=prepared.owner
LEFT JOIN pg_catalog.pg_database database ON database.datname=prepared.database
WHERE prepared.database=current_database() AND $1::text IS NOT NULL
ORDER BY prepared.gid LIMIT 4097"#,
    },
];

// Fixed current19 canonical names, pinned to the accompanying schema registry.
const CURRENT_RELATIONS: &[&str] = &[
    "_codex_pg_migrations",
    "_codex_pg_migrations_pkey",
    "codex_schema_meta",
    "codex_schema_meta_pkey",
    "thread_spawn_edges",
    "thread_spawn_edges_pkey",
    "idx_thread_spawn_edges_parent_status",
    "external_agent_config_imports",
    "external_agent_config_imports_pkey",
    "idx_external_agent_config_imports_history",
    "threads",
    "threads_pkey",
    "idx_threads_recency_id",
    "idx_threads_section_recency",
    "idx_threads_section_position",
    "idx_threads_project_id",
    "thread_sections",
    "thread_sections_pkey",
    "thread_writer_ownership",
    "thread_writer_ownership_pkey",
    "thread_goals",
    "thread_goals_pkey",
    "thread_goal_continuation_deferrals",
    "thread_goal_continuation_deferrals_pkey",
    "queued_items",
    "queued_items_pkey",
    "queued_items_thread_order_idx",
    "queue_change_counter",
    "queue_change_counter_pkey",
    "queued_thread_revisions",
    "queued_thread_revisions_pkey",
    "queued_thread_revisions_revision_idx",
    "logs",
    "logs_pkey",
    "logs_ts_idx",
    "logs_thread_id_ts_idx",
    "logs_threadless_process_ts_idx",
    "log_id_counter",
    "log_id_counter_pkey",
    "memory_stage1_outputs",
    "memory_stage1_outputs_pkey",
    "memory_stage1_outputs_source_updated_idx",
    "memory_jobs",
    "memory_jobs_pkey",
    "memory_jobs_kind_status_retry_lease_idx",
    "memory_consolidation_progress",
    "memory_consolidation_progress_pkey",
    "agent_board_deleted",
    "agent_board_deleted_pkey",
    "agent_board_channels",
    "agent_board_channels_pkey",
    "agent_board_posts",
    "agent_board_posts_pkey",
    "agent_board_posts_id_key",
    "agent_board_posts_request_key",
    "agent_board_posts_channel_idx",
    "agent_board_posts_channel_ts_idx",
    "agent_board_posts_roots_idx",
    "agent_board_posts_root_idx",
    "agent_board_posts_root_ts_idx",
    "agent_board_posts_board_ts_idx",
    "agent_board_subscriptions",
    "agent_board_subscriptions_pkey",
    "agent_board_opt_outs",
    "agent_board_opt_outs_pkey",
    "agent_board_post_counter",
    "agent_board_post_counter_pkey",
    "thread_timestamp_marks",
    "thread_timestamp_marks_pkey",
    "projects",
    "projects_pkey",
    "idx_projects_position",
    "project_roots",
    "project_roots_pkey",
    "project_idempotency_keys",
    "project_idempotency_keys_pkey",
    "thread_attachments",
    "thread_attachments_pkey",
    "thread_attachments_identity_key",
    "idx_thread_attachments_thread_created_id",
    "thread_rollout_lines",
    "thread_rollout_lines_pkey",
    "idx_thread_rollout_lines_ordinal",
    "storage_migration_runs",
    "storage_migration_runs_pkey",
    "storage_migration_domains",
    "storage_migration_domains_pkey",
    "storage_activation",
    "storage_activation_pkey",
];

// These names are proposed contracts, not proof that migrations20/21 ran.
const GENERATION_RELATIONS: &[&str] = &[
    "storage_generation_principals",
    "storage_generation_principals_pkey",
];
const MEMORY_V2_RELATIONS: &[&str] = &[
    "memory_v2_stage1_outputs",
    "memory_v2_stage1_outputs_pkey",
    "memory_v2_stage1_outputs_source_updated_idx",
    "memory_v2_jobs",
    "memory_v2_jobs_pkey",
    "memory_v2_jobs_kind_status_retry_lease_idx",
    "memory_v2_consolidation_progress",
    "memory_v2_consolidation_progress_pkey",
];

pub(super) enum RelationRefusal {
    Driver(sqlx::Error),
    MissingSnapshot,
    NonCanonical { relation: String },
    Missing { relation: &'static str },
    PartialExtension,
}

pub(super) enum HistoryRefusal {
    Driver(sqlx::Error),
    MissingSnapshot,
    UnregisteredFormat,
    ChangedHistory,
}

// Fixed current19 table scope; ONLY avoids silently admitting descendants.
// SHARE is native write-blocking until rollback, not a role/ACL/schema-creation
// lock or backend/credential/prepared-transaction retirement permission.
pub(super) fn structural_lock_sql(namespace: &str) -> String {
    let qualified = format!("\"{}\"", namespace.replace('"', "\"\""));
    let tables = CURRENT_TABLES
        .iter()
        .map(|table| format!("ONLY {qualified}.\"{table}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!("LOCK TABLE {tables} IN SHARE MODE")
}

pub(super) fn metadata_sql(namespace: &str) -> String {
    let qualified = format!("\"{}\"", namespace.replace('"', "\"\""));
    format!(
        "SELECT singleton, format_version, min_reader_version, min_writer_version FROM ONLY {qualified}.codex_schema_meta ORDER BY singleton LIMIT 2"
    )
}

pub(super) fn history_sql(namespace: &str) -> String {
    let qualified = format!("\"{}\"", namespace.replace('"', "\"\""));
    format!(
        "SELECT history.version, history.success, COALESCE(history.checksum OPERATOR(pg_catalog.=) expected.checksum,FALSE) AS checksum_matches FROM ONLY {qualified}._codex_pg_migrations history LEFT JOIN ROWS FROM (pg_catalog.unnest($1::pg_catalog.int8[]),pg_catalog.unnest($2::pg_catalog.bytea[])) AS expected(version,checksum) ON history.version OPERATOR(pg_catalog.=) expected.version ORDER BY history.version LIMIT 20"
    )
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum NativeHistoryProbe {
    LowerReader,
    LowerWriter,
    HighReader,
    NullReader,
    TextReader,
    Checksum,
    Failed,
    Missing,
    Surplus,
}
#[cfg(test)]
impl NativeHistoryProbe {
    pub(super) fn ddl(self, namespace: &str) -> Vec<String> {
        let qualified = format!("\"{}\"", namespace.replace('"', "\"\""));
        let meta = format!("{qualified}.codex_schema_meta");
        let history = format!("{qualified}._codex_pg_migrations");
        match self {
            Self::LowerReader => vec![format!("UPDATE {meta} SET min_reader_version=1")],
            Self::LowerWriter => vec![format!("UPDATE {meta} SET min_writer_version=1")],
            Self::HighReader => vec![format!("UPDATE {meta} SET min_reader_version=20")],
            Self::NullReader => vec![
                format!("ALTER TABLE {meta} ALTER COLUMN min_reader_version DROP NOT NULL"),
                format!("UPDATE {meta} SET min_reader_version=NULL"),
            ],
            Self::TextReader => vec![
                format!(
                    "ALTER TABLE {meta} DROP CONSTRAINT codex_schema_meta_min_reader_version_check"
                ),
                format!(
                    "ALTER TABLE {meta} ALTER COLUMN min_reader_version TYPE pg_catalog.text USING min_reader_version::pg_catalog.text"
                ),
            ],
            Self::Checksum => vec![format!(
                "UPDATE {history} SET checksum=pg_catalog.decode('00','hex') WHERE version=19"
            )],
            Self::Failed => vec![format!(
                "UPDATE {history} SET success=FALSE WHERE version=19"
            )],
            Self::Missing => vec![format!("DELETE FROM {history} WHERE version=19")],
            Self::Surplus => vec![format!(
                "INSERT INTO {history}(version,description,installed_on,success,checksum,execution_time) SELECT 20,description,installed_on,success,checksum,execution_time FROM {history} WHERE version=19"
            )],
        }
    }
    pub(super) fn expected(self, raw: &HistoryRefusal) -> bool {
        match (self, raw) {
            (
                Self::LowerReader | Self::LowerWriter | Self::HighReader,
                HistoryRefusal::UnregisteredFormat,
            ) => true,
            (
                Self::NullReader | Self::TextReader,
                HistoryRefusal::Driver(sqlx::Error::ColumnDecode { index, .. }),
            ) => index == "min_reader_version",
            (
                Self::Checksum | Self::Failed | Self::Missing | Self::Surplus,
                HistoryRefusal::ChangedHistory,
            ) => true,
            _ => false,
        }
    }
}

pub(super) fn inspect_history(
    metadata: &[sqlx::postgres::PgRow],
    history: &[sqlx::postgres::PgRow],
) -> Result<(), HistoryRefusal> {
    use sqlx::Row;
    if metadata.len() != 1 {
        return Err(HistoryRefusal::MissingSnapshot);
    }
    let row = &metadata[0];
    let singleton: bool = row.try_get("singleton").map_err(HistoryRefusal::Driver)?;
    let format: i32 = row
        .try_get("format_version")
        .map_err(HistoryRefusal::Driver)?;
    let reader: i32 = row
        .try_get("min_reader_version")
        .map_err(HistoryRefusal::Driver)?;
    let writer: i32 = row
        .try_get("min_writer_version")
        .map_err(HistoryRefusal::Driver)?;
    // No executable canonical20/21 contracts exist in this stage.
    if !singleton || format != 19 || reader != 19 || writer != 19 {
        return Err(HistoryRefusal::UnregisteredFormat);
    }
    let expected = codex_postgres_runtime::embedded_migration_checksums()
        .take(19)
        .collect::<Vec<_>>();
    if expected.len() != 19 || history.len() != 19 {
        return Err(HistoryRefusal::ChangedHistory);
    }
    for (row, (version, _)) in history.iter().zip(expected) {
        let actual: i64 = row.try_get("version").map_err(HistoryRefusal::Driver)?;
        let success: bool = row.try_get("success").map_err(HistoryRefusal::Driver)?;
        let matches: bool = row
            .try_get("checksum_matches")
            .map_err(HistoryRefusal::Driver)?;
        if actual != version || !success || !matches {
            return Err(HistoryRefusal::ChangedHistory);
        }
    }
    Ok(())
}

impl std::fmt::Debug for HistoryRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Self::Driver(raw) = self {
            let _ = raw;
        }
        formatter.write_str("CatalogHistoryRefused([native rows retained])")
    }
}

pub(super) struct RelationObservation {
    pub(super) namespace_oid: i64,
    pub(super) relation_count: usize,
    pub(super) generation_relations_present: bool,
    pub(super) memory_v2_relations_present: bool,
}

// A selected relation-name/kind closure only. This is not history eligibility,
// column/index/constraint/trigger/routine/ACL validation, a coherent snapshot of
// the six query groups, or a permission/retirement receipt. V2 relation presence
// is schema-object presence only, never migrated dataset/file presence or empty
// data. Final21 presence metadata is a separate required producer; no version
// selection or source-presence interpretation follows from these names.
pub(super) fn inspect_relations(
    rows: &[sqlx::postgres::PgRow],
) -> Result<RelationObservation, RelationRefusal> {
    use sqlx::Row;
    use std::collections::BTreeSet;
    let mut names = BTreeSet::new();
    let mut oids = BTreeSet::new();
    let mut namespace = None;
    for row in rows {
        let name: String = row.try_get("relname").map_err(RelationRefusal::Driver)?;
        let oid: i64 = row
            .try_get("relation_oid")
            .map_err(RelationRefusal::Driver)?;
        let ns: i64 = row
            .try_get("namespace_oid")
            .map_err(RelationRefusal::Driver)?;
        let kind: String = row.try_get("relkind").map_err(RelationRefusal::Driver)?;
        let partition: bool = row
            .try_get("relispartition")
            .map_err(RelationRefusal::Driver)?;
        let inherited: bool = row
            .try_get("has_inheritance")
            .map_err(RelationRefusal::Driver)?;
        let rules: bool = row.try_get("has_rules").map_err(RelationRefusal::Driver)?;
        let table = CURRENT_RELATIONS.contains(&name.as_str())
            || GENERATION_RELATIONS.contains(&name.as_str())
            || MEMORY_V2_RELATIONS.contains(&name.as_str());
        let expected_kind = if CURRENT_TABLES.contains(&name.as_str())
            || name == "storage_generation_principals"
            || matches!(
                name.as_str(),
                "memory_v2_stage1_outputs" | "memory_v2_jobs" | "memory_v2_consolidation_progress"
            ) {
            "r"
        } else {
            "i"
        };
        if !table
            || kind != expected_kind
            || partition
            || inherited
            || rules
            || oid <= 0
            || ns <= 0
            || !oids.insert(oid)
            || namespace.is_some_and(|known| known != ns)
            || !names.insert(name.clone())
        {
            return Err(RelationRefusal::NonCanonical { relation: name });
        }
        namespace = Some(ns);
    }
    for expected in CURRENT_RELATIONS {
        if !names.contains(*expected) {
            return Err(RelationRefusal::Missing { relation: expected });
        }
    }
    let generation_count = GENERATION_RELATIONS
        .iter()
        .filter(|name| names.contains(**name))
        .count();
    let memory_count = MEMORY_V2_RELATIONS
        .iter()
        .filter(|name| names.contains(**name))
        .count();
    if (generation_count != 0 && generation_count != GENERATION_RELATIONS.len())
        || (memory_count != 0 && memory_count != MEMORY_V2_RELATIONS.len())
    {
        return Err(RelationRefusal::PartialExtension);
    }
    Ok(RelationObservation {
        namespace_oid: namespace.ok_or(RelationRefusal::MissingSnapshot)?,
        relation_count: names.len(),
        generation_relations_present: generation_count != 0,
        memory_v2_relations_present: memory_count != 0,
    })
}

// Immutable current19 migrations define no user triggers, policies or RLS.
// Internal FK triggers remain native rows requiring exact constraint comparison.
// This is a surface refusal, not exact columns/defaults/index/ACL permission.
pub(super) fn inspect_current19_surface(
    rows: &[sqlx::postgres::PgRow],
) -> Result<(), RelationRefusal> {
    use sqlx::Row;
    for row in rows {
        let relation: String = row.try_get("relname").map_err(RelationRefusal::Driver)?;
        for field in [
            "relrowsecurity",
            "relforcerowsecurity",
            "has_policies",
            "has_user_triggers",
            "unusable_index",
        ] {
            let present: bool = row.try_get(field).map_err(RelationRefusal::Driver)?;
            if present {
                return Err(RelationRefusal::NonCanonical { relation });
            }
        }
    }
    Ok(())
}

impl std::fmt::Debug for RelationRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Driver(raw) => {
                let _ = raw;
            }
            Self::NonCanonical { relation } => {
                let _ = relation;
            }
            Self::Missing { relation } => {
                let _ = relation;
            }
            Self::MissingSnapshot | Self::PartialExtension => {}
        }
        formatter.write_str("CatalogRelationRefused([original catalog retained])")
    }
}
const CURRENT_TABLES: &[&str] = &[
    "_codex_pg_migrations",
    "codex_schema_meta",
    "thread_spawn_edges",
    "external_agent_config_imports",
    "threads",
    "thread_sections",
    "thread_writer_ownership",
    "thread_goals",
    "thread_goal_continuation_deferrals",
    "queued_items",
    "queue_change_counter",
    "queued_thread_revisions",
    "logs",
    "log_id_counter",
    "memory_stage1_outputs",
    "memory_jobs",
    "memory_consolidation_progress",
    "agent_board_deleted",
    "agent_board_channels",
    "agent_board_posts",
    "agent_board_subscriptions",
    "agent_board_opt_outs",
    "agent_board_post_counter",
    "thread_timestamp_marks",
    "projects",
    "project_roots",
    "project_idempotency_keys",
    "thread_attachments",
    "thread_rollout_lines",
    "storage_migration_runs",
    "storage_migration_domains",
    "storage_activation",
];

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum NativePayloadProbe {
    Exact,
    Overflow,
}

#[cfg(test)]
impl NativePayloadProbe {
    pub(super) fn sql(self, index: usize) -> Option<&'static str> {
        match index {
            0 => Some(
                "SELECT NULL::text AS absent, repeat('x',4194304)::text AS text_value, decode('000102','hex') AS binary_value WHERE $1::text IS NOT NULL",
            ),
            1 => Some("SELECT repeat('y',4194301)::text AS text_value WHERE $1::text IS NOT NULL"),
            2 if matches!(self, Self::Overflow) => {
                Some("SELECT 'z'::text AS text_value WHERE $1::text IS NOT NULL")
            }
            _ => None,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) enum NativeRelationProbe {
    Rls,
    Policy,
    UserTrigger,
    Unknown,
    WrongKind,
    MissingIndex,
    Partial20,
    Partial21,
}

#[cfg(test)]
impl NativeRelationProbe {
    pub(super) fn ddl(self, namespace: &str) -> Vec<String> {
        let qualified = format!("\"{}\"", namespace.replace('"', "\"\""));
        match self {
            Self::Rls => vec![format!(
                "ALTER TABLE {qualified}.threads ENABLE ROW LEVEL SECURITY"
            )],
            Self::Policy => vec![format!(
                "CREATE POLICY codex_surface_control ON {qualified}.threads USING (TRUE)"
            )],
            Self::UserTrigger => vec![
                format!(
                    "CREATE FUNCTION {qualified}.codex_surface_control() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END;'"
                ),
                format!(
                    "CREATE TRIGGER codex_surface_control BEFORE INSERT ON {qualified}.threads FOR EACH ROW EXECUTE FUNCTION {qualified}.codex_surface_control()"
                ),
            ],
            Self::Unknown => vec![format!(
                "CREATE TABLE {qualified}.codex_relation_control_unknown (probe INTEGER)"
            )],
            Self::WrongKind => vec![
                format!("DROP INDEX {qualified}.idx_threads_recency_id"),
                format!("CREATE TABLE {qualified}.idx_threads_recency_id (probe INTEGER)"),
            ],
            Self::MissingIndex => vec![format!("DROP INDEX {qualified}.idx_threads_recency_id")],
            Self::Partial20 => vec![format!(
                "CREATE TABLE {qualified}.storage_generation_principals (probe INTEGER)"
            )],
            Self::Partial21 => vec![format!(
                "CREATE TABLE {qualified}.memory_v2_jobs (probe INTEGER)"
            )],
        }
    }
}
