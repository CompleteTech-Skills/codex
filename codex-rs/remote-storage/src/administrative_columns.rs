//! Private current19 column expectations derived from immutable original DDL.
//! The original structural task retains native rows before comparison.
//! This is a column check, not complete schema or retirement authority.
//! PRIMARY KEY promotes nullability. C collation differs from the type default.
//! Default expressions, constraints, indexes, triggers, ACL/routine ownership and
//! complete selected namespace closure remain mandatory separate native checks.

const EXPECTED: &str = r#"
('codex_schema_meta','singleton','pg_catalog.bool',true,true,false),
('codex_schema_meta','format_version','pg_catalog.int4',true,false,false),
('codex_schema_meta','min_reader_version','pg_catalog.int4',true,false,false),
('codex_schema_meta','min_writer_version','pg_catalog.int4',true,false,false),
('thread_spawn_edges','parent_thread_id','pg_catalog.uuid',true,false,false),
('thread_spawn_edges','child_thread_id','pg_catalog.uuid',true,false,false),
('thread_spawn_edges','status','pg_catalog.text',true,false,false),
('external_agent_config_imports','import_id','pg_catalog.text',true,false,false),
('external_agent_config_imports','provider_id','pg_catalog.text',false,false,false),
('external_agent_config_imports','completed_at_ms','pg_catalog.int8',true,false,false),
('external_agent_config_imports','successes','pg_catalog.text',true,false,false),
('external_agent_config_imports','failures','pg_catalog.text',true,false,false),
('threads','id','pg_catalog.uuid',true,false,false),
('threads','originator','pg_catalog.text',false,false,false),
('threads','creator_user_id','pg_catalog.text',false,false,false),
('threads','creator_account_id','pg_catalog.text',false,false,false),
('threads','origin_rollout_path','pg_catalog.text',true,false,false),
('threads','created_at_ms','pg_catalog.int8',true,false,false),
('threads','updated_at_ms','pg_catalog.int8',true,false,false),
('threads','recency_at_ms','pg_catalog.int8',true,false,false),
('threads','source','pg_catalog.text',true,false,false),
('threads','history_mode','pg_catalog.text',true,false,false),
('threads','thread_source','pg_catalog.text',false,false,false),
('threads','agent_nickname','pg_catalog.text',false,false,false),
('threads','agent_role','pg_catalog.text',false,false,false),
('threads','agent_path','pg_catalog.text',false,false,false),
('threads','model_provider','pg_catalog.text',true,false,false),
('threads','model','pg_catalog.text',false,false,false),
('threads','reasoning_effort','pg_catalog.text',false,false,false),
('threads','origin_cwd','pg_catalog.text',true,false,false),
('threads','cli_version','pg_catalog.text',true,false,false),
('threads','title','pg_catalog.text',true,false,false),
('threads','name','pg_catalog.text',false,false,false),
('threads','preview','pg_catalog.text',false,false,false),
('threads','sandbox_policy','pg_catalog.text',true,false,false),
('threads','approval_mode','pg_catalog.text',true,false,false),
('threads','tokens_used','pg_catalog.int8',true,true,false),
('threads','first_user_message','pg_catalog.text',false,false,false),
('threads','archived_at_s','pg_catalog.int8',false,false,false),
('threads','thread_section_id','pg_catalog.text',false,false,false),
('threads','section_position','pg_catalog.int8',false,false,false),
('threads','section_entered_at_ms','pg_catalog.int8',false,false,false),
('threads','project_id','pg_catalog.text',false,false,false),
('threads','daybreak_enabled','pg_catalog.bool',false,false,false),
('threads','git_sha','pg_catalog.text',false,false,false),
('threads','git_branch','pg_catalog.text',false,false,false),
('threads','git_origin_url','pg_catalog.text',false,false,false),
('threads','memory_mode','pg_catalog.text',true,true,false),
('thread_sections','id','pg_catalog.text',true,false,true),
('thread_sections','name','pg_catalog.text',true,false,false),
('thread_sections','appearance','pg_catalog.text',false,false,false),
('thread_writer_ownership','thread_id','pg_catalog.uuid',true,false,false),
('thread_writer_ownership','token','pg_catalog.int8',true,false,false),
('thread_writer_ownership','owner_id','pg_catalog.uuid',false,false,false),
('thread_writer_ownership','lease_until','pg_catalog.timestamptz',false,false,false),
('thread_goals','thread_id','pg_catalog.uuid',true,false,false),
('thread_goals','goal_id','pg_catalog.text',true,false,true),
('thread_goals','objective','pg_catalog.text',true,false,false),
('thread_goals','status','pg_catalog.text',true,false,true),
('thread_goals','token_budget','pg_catalog.int8',false,false,false),
('thread_goals','tokens_used','pg_catalog.int8',true,true,false),
('thread_goals','time_used_seconds','pg_catalog.int8',true,true,false),
('thread_goals','created_at_ms','pg_catalog.int8',true,false,false),
('thread_goals','updated_at_ms','pg_catalog.int8',true,false,false),
('thread_goal_continuation_deferrals','thread_id','pg_catalog.uuid',true,false,false),
('queued_items','id','pg_catalog.text',true,false,true),
('queued_items','thread_id','pg_catalog.uuid',true,false,false),
('queued_items','payload_json','pg_catalog.text',true,false,false),
('queued_items','queue_order','pg_catalog.int8',true,false,false),
('queued_items','created_at_ms','pg_catalog.int8',true,false,false),
('queued_items','updated_at_ms','pg_catalog.int8',true,false,false),
('queue_change_counter','singleton','pg_catalog.bool',true,true,false),
('queue_change_counter','version','pg_catalog.int8',true,false,false),
('queued_thread_revisions','thread_id','pg_catalog.uuid',true,false,false),
('queued_thread_revisions','revision','pg_catalog.int8',true,false,false),
('logs','id','pg_catalog.int8',true,false,false),
('logs','ts','pg_catalog.int8',true,false,false),
('logs','ts_nanos','pg_catalog.int8',true,false,false),
('logs','level','pg_catalog.text',true,false,false),
('logs','target','pg_catalog.text',true,false,false),
('logs','feedback_log_body','pg_catalog.text',false,false,false),
('logs','module_path','pg_catalog.text',false,false,false),
('logs','file','pg_catalog.text',false,false,false),
('logs','line','pg_catalog.int8',false,false,false),
('logs','thread_id','pg_catalog.text',false,false,false),
('logs','process_uuid','pg_catalog.text',false,false,false),
('logs','estimated_bytes','pg_catalog.int8',true,true,false),
('log_id_counter','singleton','pg_catalog.bool',true,true,false),
('log_id_counter','last_id','pg_catalog.int8',true,false,false),
('memory_stage1_outputs','thread_id','pg_catalog.uuid',true,false,false),
('memory_stage1_outputs','source_updated_at','pg_catalog.int8',true,false,false),
('memory_stage1_outputs','raw_memory','pg_catalog.text',true,false,false),
('memory_stage1_outputs','rollout_summary','pg_catalog.text',true,false,false),
('memory_stage1_outputs','rollout_slug','pg_catalog.text',false,false,false),
('memory_stage1_outputs','generated_at','pg_catalog.int8',true,false,false),
('memory_stage1_outputs','usage_count','pg_catalog.int8',false,false,false),
('memory_stage1_outputs','last_usage','pg_catalog.int8',false,false,false),
('memory_stage1_outputs','selected_for_phase2','pg_catalog.int8',true,true,false),
('memory_stage1_outputs','selected_for_phase2_source_updated_at','pg_catalog.int8',false,false,false),
('memory_jobs','kind','pg_catalog.text',true,false,false),
('memory_jobs','job_key','pg_catalog.text',true,false,false),
('memory_jobs','status','pg_catalog.text',true,false,false),
('memory_jobs','worker_id','pg_catalog.text',false,false,false),
('memory_jobs','ownership_token','pg_catalog.text',false,false,false),
('memory_jobs','started_at','pg_catalog.int8',false,false,false),
('memory_jobs','finished_at','pg_catalog.int8',false,false,false),
('memory_jobs','lease_until','pg_catalog.int8',false,false,false),
('memory_jobs','retry_at','pg_catalog.int8',false,false,false),
('memory_jobs','retry_remaining','pg_catalog.int8',true,false,false),
('memory_jobs','last_error','pg_catalog.text',false,false,false),
('memory_jobs','input_watermark','pg_catalog.int8',false,false,false),
('memory_jobs','last_success_watermark','pg_catalog.int8',false,false,false),
('memory_consolidation_progress','singleton','pg_catalog.bool',true,true,false),
('memory_consolidation_progress','max_thread_count','pg_catalog.int8',true,true,false),
('agent_board_deleted','board','pg_catalog.text',true,false,true),
('agent_board_channels','board','pg_catalog.text',true,false,true),
('agent_board_channels','name','pg_catalog.text',true,false,true),
('agent_board_channels','name_search','pg_catalog.text',true,false,false),
('agent_board_channels','created_at','pg_catalog.text',true,false,false),
('agent_board_channels','timestamp','pg_catalog.int8',true,false,false),
('agent_board_channels','author','pg_catalog.text',true,false,false),
('agent_board_posts','seq','pg_catalog.int8',true,false,false),
('agent_board_posts','board','pg_catalog.text',true,false,true),
('agent_board_posts','id','pg_catalog.text',true,false,true),
('agent_board_posts','channel','pg_catalog.text',true,false,true),
('agent_board_posts','root','pg_catalog.text',true,false,true),
('agent_board_posts','author','pg_catalog.text',true,false,false),
('agent_board_posts','timestamp','pg_catalog.int8',true,false,false),
('agent_board_posts','body_search','pg_catalog.text',true,false,false),
('agent_board_posts','payload','pg_catalog.text',true,false,false),
('agent_board_posts','request_id','pg_catalog.text',true,false,true),
('agent_board_posts','request','pg_catalog.text',true,false,false),
('agent_board_subscriptions','board','pg_catalog.text',true,false,true),
('agent_board_subscriptions','target','pg_catalog.text',true,false,true),
('agent_board_subscriptions','agent','pg_catalog.text',true,false,true),
('agent_board_opt_outs','board','pg_catalog.text',true,false,true),
('agent_board_opt_outs','target','pg_catalog.text',true,false,true),
('agent_board_opt_outs','agent','pg_catalog.text',true,false,true),
('agent_board_post_counter','singleton','pg_catalog.bool',true,true,false),
('agent_board_post_counter','last_seq','pg_catalog.int8',true,false,false),
('thread_timestamp_marks','singleton','pg_catalog.bool',true,true,false),
('thread_timestamp_marks','updated_at_ms','pg_catalog.int8',true,true,false),
('thread_timestamp_marks','recency_at_ms','pg_catalog.int8',true,true,false),
('projects','id','pg_catalog.text',true,false,false),
('projects','name','pg_catalog.text',true,false,false),
('projects','metadata','pg_catalog.text',true,true,false),
('projects','position','pg_catalog.int8',true,false,false),
('projects','created_at_ms','pg_catalog.int8',true,false,false),
('projects','updated_at_ms','pg_catalog.int8',true,false,false),
('project_roots','project_id','pg_catalog.text',true,false,false),
('project_roots','position','pg_catalog.int8',true,false,false),
('project_roots','path','pg_catalog.text',true,false,false),
('project_idempotency_keys','key','pg_catalog.text',true,false,false),
('project_idempotency_keys','project_id','pg_catalog.text',true,false,false),
('project_idempotency_keys','created_at_ms','pg_catalog.int8',true,false,false),
('thread_attachments','id','pg_catalog.text',true,false,true),
('thread_attachments','thread_id','pg_catalog.uuid',true,false,false),
('thread_attachments','attachment_type','pg_catalog.text',true,false,false),
('thread_attachments','identity_key','pg_catalog.text',true,false,false),
('thread_attachments','payload','pg_catalog.text',true,false,false),
('thread_attachments','created_at','pg_catalog.int8',true,false,false),
('thread_rollout_lines','thread_id','pg_catalog.uuid',true,false,false),
('thread_rollout_lines','position','pg_catalog.int8',true,false,false),
('thread_rollout_lines','ordinal','pg_catalog.int8',false,false,false),
('thread_rollout_lines','line','pg_catalog.text',true,false,false),
('storage_migration_runs','run_id','pg_catalog.uuid',true,false,false),
('storage_migration_runs','direction','pg_catalog.text',true,false,false),
('storage_migration_runs','source_fingerprint','pg_catalog.text',true,false,false),
('storage_migration_runs','state','pg_catalog.text',true,false,false),
('storage_migration_runs','started_at_ms','pg_catalog.int8',true,false,false),
('storage_migration_runs','updated_at_ms','pg_catalog.int8',true,false,false),
('storage_migration_runs','detail','pg_catalog.text',false,false,false),
('storage_migration_domains','run_id','pg_catalog.uuid',true,false,false),
('storage_migration_domains','domain','pg_catalog.text',true,false,false),
('storage_migration_domains','resume_cursor','pg_catalog.text',false,false,false),
('storage_migration_domains','done','pg_catalog.bool',true,true,false),
('storage_migration_domains','row_count','pg_catalog.int8',true,true,false),
('storage_migration_domains','digest','pg_catalog.text',false,false,false),
('storage_activation','singleton','pg_catalog.bool',true,true,false),
('storage_activation','state','pg_catalog.text',true,false,false),
('storage_activation','run_id','pg_catalog.uuid',false,false,false),
('storage_activation','generation','pg_catalog.int8',true,false,false),
('storage_activation','updated_at_ms','pg_catalog.int8',true,false,false),
('storage_activation','dataset_id','pg_catalog.text',false,false,false),
('storage_activation','activated_at_ms','pg_catalog.int8',false,false,false),
('_codex_pg_migrations','version','pg_catalog.int8',true,false,false),
('_codex_pg_migrations','description','pg_catalog.text',true,false,false),
('_codex_pg_migrations','installed_on','pg_catalog.timestamptz',true,true,false),
('_codex_pg_migrations','success','pg_catalog.bool',true,false,false),
('_codex_pg_migrations','checksum','pg_catalog.bytea',true,false,false),
('_codex_pg_migrations','execution_time','pg_catalog.int8',true,false,false)
"#;

// The expected input is private embedded DDL data, never a caller-supplied tuple.
// A coherent consumer must retain all returned actual rows/raw driver errors and
// budget values before classification while SAME current19 SHARE locks are held.
// This query deliberately includes extra live columns through FULL JOIN. Missing
// relation/index/history closure is independently checked by the original task.
pub(super) fn observation_sql() -> String {
    format!(
        r#"
WITH expected(table_name,column_name,type_name,not_null,has_default,collate_c) AS (VALUES {EXPECTED}),
actual AS (
    SELECT relation.relname::text AS table_name, attribute.attname::text AS column_name,
           attribute.atttypid, attribute.atttypmod, attribute.attnotnull, attribute.atthasdef,
           attribute.attidentity::text, attribute.attgenerated::text, attribute.attcollation,
           native_type.typcollation AS type_collation
    FROM pg_catalog.pg_namespace namespace
    JOIN pg_catalog.pg_class relation ON relation.relnamespace=namespace.oid
    JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid=relation.oid
    JOIN pg_catalog.pg_type native_type ON native_type.oid=attribute.atttypid
    WHERE namespace.nspname=$1 AND attribute.attnum>0 AND NOT attribute.attisdropped
      AND relation.relname IN (SELECT table_name FROM expected)
)
SELECT COALESCE(actual.table_name,expected.table_name) AS table_name,
       COALESCE(actual.column_name,expected.column_name) AS column_name,
       actual.atttypid::bigint AS observed_type_oid, actual.atttypmod,
       actual.attnotnull, actual.atthasdef, actual.attidentity, actual.attgenerated,
       actual.attcollation::bigint AS observed_collation_oid,
       expected.type_name::pg_catalog.regtype::bigint AS expected_type_oid,
       expected.not_null, expected.has_default, expected.collate_c,
       (actual.column_name IS NULL OR expected.column_name IS NULL
        OR actual.atttypid IS DISTINCT FROM expected.type_name::pg_catalog.regtype
        OR actual.atttypmod IS DISTINCT FROM -1
        OR actual.attnotnull IS DISTINCT FROM expected.not_null
        OR actual.atthasdef IS DISTINCT FROM expected.has_default
        OR actual.attidentity IS DISTINCT FROM '' OR actual.attgenerated IS DISTINCT FROM ''
        OR actual.attcollation IS DISTINCT FROM
           CASE WHEN expected.collate_c THEN 'pg_catalog."C"'::pg_catalog.regcollation
                ELSE actual.type_collation END) AS mismatch
FROM actual FULL JOIN expected ON actual.table_name=expected.table_name
 AND actual.column_name=expected.column_name
ORDER BY table_name,column_name LIMIT 513
"#
    )
}

/// Only a column comparison; this cannot confer complete schema or migration authority.
pub(super) enum ColumnRefusal {
    Count { observed: usize },
    Mismatch,
    Driver(sqlx::Error),
}

pub(super) fn inspect(rows: &[sqlx::postgres::PgRow]) -> Result<(), ColumnRefusal> {
    use sqlx::Row;
    if rows.len() != 191 {
        return Err(ColumnRefusal::Count {
            observed: rows.len(),
        });
    }
    for row in rows {
        if row
            .try_get::<bool, _>("mismatch")
            .map_err(ColumnRefusal::Driver)?
        {
            return Err(ColumnRefusal::Mismatch);
        }
    }
    Ok(())
}
