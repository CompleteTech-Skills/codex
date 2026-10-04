//! Real PostgreSQL writer calls after activation, never migration import helpers.
use super::tests::metadata;
use anyhow::Result;
use anyhow::ensure;
use chrono::Utc;
use codex_postgres_goal_store::PostgresGoalStore;
use codex_postgres_import_store::PostgresExternalAgentImportStore;
use codex_postgres_log_store::PostgresLogStore;
use codex_postgres_memory_store::PostgresMemoryStore;
use codex_postgres_queue_store::PostgresQueueStore;
use codex_postgres_rollout_store::PostgresRolloutStore;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_thread_catalog::PostgresThreadCatalog;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_state::LogEntry;
use codex_state::Phase2JobClaimOutcome;
use codex_state::ProjectRoot;
use codex_state::RuntimeLogStore;
use codex_state::RuntimeMemoryStore;
use codex_state::Stage1JobClaimOutcome;
use codex_state::ThreadGoalStatus;
use codex_state::ThreadGoalStore;
use codex_state::ThreadMetadata;
use codex_thread_store::QueueStore;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) struct LatestWrites {
    pub parent: ThreadMetadata,
    pub child: ThreadMetadata,
    pub queue_id: String,
}

pub(super) async fn write(pool: &Arc<PostgresPool>) -> Result<LatestWrites> {
    let catalog = PostgresThreadCatalog::new(Arc::clone(pool));
    let base = Utc::now() - chrono::Duration::days(1);
    let parent = metadata(700, base, SessionSource::Cli);
    catalog.upsert_thread(&parent).await?;
    let mut child = metadata(701, base, SessionSource::Cli);
    child.source = serde_json::to_string(&SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id: parent.id,
        depth: 1,
        agent_path: None,
        agent_nickname: None,
        agent_role: None,
    }))?;
    catalog.upsert_thread(&child).await?;
    let section = catalog
        .create_thread_section("Latest remote section", None)
        .await?;
    catalog
        .move_thread_to_section(parent.id, Some(&section.id), None)
        .await?;
    let project = catalog
        .create_project(
            "Latest remote project".into(),
            vec![ProjectRoot {
                path: "/latest-remote/project".into(),
            }],
            BTreeMap::from([("latest".into(), "remote-only".into())]),
            &[parent.id.to_string(), child.id.to_string()],
            &format!("latest-project-{}", parent.id),
        )
        .await?;
    ensure!(project.created, "latest project was not newly created");
    catalog
        .add_thread_attachment(
            parent.id,
            "file",
            "latest-remote.txt",
            &serde_json::json!({"latest":"remote-only"}),
        )
        .await?;
    let goal = PostgresGoalStore::new(Arc::clone(pool))
        .insert_thread_goal(
            parent.id,
            "Latest remote goal",
            ThreadGoalStatus::Active,
            Some(9000),
        )
        .await?;
    ensure!(goal.is_some(), "latest goal was not inserted");
    let queue = PostgresQueueStore::new(Arc::clone(pool));
    let kept = queue
        .enqueue(parent.id, "{\"text\":\"latest remote queued item\"}".into())
        .await?;
    let deleted = queue
        .enqueue(child.id, "{\"text\":\"latest deleted item\"}".into())
        .await?;
    ensure!(
        queue.delete(child.id, deleted.id).await?,
        "latest queue deletion missing"
    );
    let rollout = PostgresRolloutStore::new(Arc::clone(pool));
    for (thread, history) in [
        (&parent, serde_json::Value::Null),
        (
            &child,
            serde_json::json!({
                "thread_id": parent.id.to_string(), "end_ordinal_exclusive": 2, "end_byte_offset": 0
            }),
        ),
    ] {
        let position = if thread.id == child.id {
            rollout.copy_prefix(parent.id, child.id, 2).await?;
            2
        } else {
            0
        };
        let header = serde_json::json!({"timestamp":"2026-10-04T12:00:00Z", "type":"session_meta", "payload":{
            "id":thread.id.to_string(), "history_base":history
        }}).to_string();
        let event =
            serde_json::json!({"timestamp":"2026-10-04T12:00:01Z", "type":"event_msg", "payload":{
                "type":"user_message", "message":format!("latest remote {} café 🦀", thread.id)
            }})
            .to_string();
        rollout
            .append(thread.id, position, vec![(None, header), (None, event)])
            .await?;
    }
    PostgresLogStore::new(Arc::clone(pool))
        .insert_logs(&[LogEntry {
            ts: Utc::now().timestamp(),
            ts_nanos: 17,
            level: "INFO".into(),
            target: "latest-remote".into(),
            message: Some("latest remote log".into()),
            feedback_log_body: Some("latest remote café 🦀".into()),
            thread_id: Some(parent.id.to_string()),
            process_uuid: Some(uuid::Uuid::new_v4().to_string()),
            module_path: Some("fresh_reverse".into()),
            file: Some("latest_remote_writes.rs".into()),
            line: Some(1),
        }])
        .await?;
    let memory = PostgresMemoryStore::new(Arc::clone(pool));
    // The imported fixture progress is7. Eight genuine new outputs make the
    // production monotonic progress update observably change rather than pass vacuously.
    for index in 0..8 {
        let thread = metadata(800 + index, base, SessionSource::Cli);
        catalog.upsert_thread(&thread).await?;
        let token = match memory
            .try_claim_stage1_job(thread.id, parent.id, thread.updated_at.timestamp(), 600, 64)
            .await?
        {
            Stage1JobClaimOutcome::Claimed { ownership_token } => ownership_token,
            other => anyhow::bail!("latest stage1 job not claimed: {other:?}"),
        };
        ensure!(
            memory
                .mark_stage1_job_succeeded(
                    thread.id,
                    &token,
                    thread.updated_at.timestamp(),
                    &format!("latest remote memory{index}"),
                    &format!("latest remote summary{index}"),
                    Some("latest-remote")
                )
                .await?,
            "latest stage1 completion refused"
        );
    }
    memory
        .enqueue_global_consolidation(Utc::now().timestamp())
        .await?;
    let (token, watermark) = match memory.try_claim_global_phase2_job(parent.id, 600).await? {
        Phase2JobClaimOutcome::Claimed {
            ownership_token,
            input_watermark,
        } => (ownership_token, input_watermark),
        other => anyhow::bail!("latest global job not claimed: {other:?}"),
    };
    let outputs = memory.list_stage1_outputs_for_global(64).await?;
    let selected = outputs
        .into_iter()
        .filter(|output| output.raw_memory.starts_with("latest remote memory"))
        .collect::<Vec<_>>();
    ensure!(selected.len() == 8, "latest memory selection incomplete");
    ensure!(
        memory
            .mark_global_phase2_job_succeeded(&token, watermark, &selected)
            .await?,
        "latest global completion refused"
    );
    PostgresExternalAgentImportStore::new(Arc::clone(pool))
        .record_external_agent_config_import_completed(
            &format!("latest-remote-import-{}", parent.id),
            Some("latest-remote-provider"),
            &[],
            &[],
        )
        .await?;
    super::latest_remote_board::write(pool, parent.id, child.id).await?;
    Ok(LatestWrites {
        parent,
        child,
        queue_id: kept.id,
    })
}
