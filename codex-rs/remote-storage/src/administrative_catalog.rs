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
pub(super) const STATEMENTS: [Statement; 6] = [
    Statement {
        rows: 64,
        sql: r#"
SELECT relation.oid::bigint AS relation_oid, namespace.oid::bigint AS namespace_oid,
       namespace.nspowner::bigint AS namespace_owner, namespace.nspacl::text AS namespace_acl,
       relation.relname::text, relation.relkind::text, relation.relowner::bigint,
       relation.relacl::text, relation.relispartition, relation.relrowsecurity,
       relation.relforcerowsecurity,
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
WHERE namespace.nspname=$1 AND relation.relkind IN ('r','p','v','m','f','S')
ORDER BY relation.oid LIMIT 65"#,
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
