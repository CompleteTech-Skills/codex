use super::*;
use chrono::DateTime;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_protocol::protocol::SessionSource;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_state::ThreadMetadata;
use codex_state::ThreadMetadataBuilder;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::path::Path;
use tempfile::TempDir;

fn settings(state: &Path, role: &str) -> ConnectionSettings {
    let receipt: Value = serde_json::from_slice(
        &std::fs::read(state.join("receipt.json")).expect("read isolated PostgreSQL receipt"),
    )
    .expect("parse PostgreSQL receipt");
    ConnectionSettings {
        host: "localhost".to_string(),
        port: receipt["port"].as_u64().expect("PostgreSQL port") as u16,
        database: "codex".to_string(),
        username: format!("codex_{role}"),
        password: std::fs::read_to_string(state.join(format!("secrets/{role}.password")))
            .expect("read private role credential")
            .trim()
            .to_string()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: StdDuration::from_secs(5),
            acquire_timeout: StdDuration::from_secs(20),
            max_connections: 16,
        },
    }
}

async fn connect(state: &Path, role: &str) -> Arc<PostgresPool> {
    let pool = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, role), None)
            .await
            .unwrap_or_else(|error| panic!("{role} pool: {error:?}")),
    );
    if role == "runtime" {
        pool.bind_current_activation()
            .await
            .unwrap_or_else(|error| panic!("actual runtime activation binding: {error}"));
    }
    pool
}

fn token() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("pgmemory{nanos}")
}

/// One enabled thread the memory pipeline can pick up.
fn thread(token: &str, index: i64, now: i64) -> ThreadMetadata {
    let created_at = DateTime::<Utc>::from_timestamp(now - 48 * 3600 - index, 0).expect("created");
    let mut builder = ThreadMetadataBuilder::new(
        ThreadId::new(),
        PathBuf::from(format!("/rollouts/{token}-{index}.jsonl")),
        created_at,
        SessionSource::Custom(token.to_string()),
    );
    // Newer indexes are older threads, and all are idle for more than the claim threshold.
    builder.updated_at = DateTime::<Utc>::from_timestamp(now - (6 + index) * 3600, 0);
    builder.cwd = PathBuf::from(format!("/work/{token}-{index}"));
    builder.git_branch = Some(format!("branch-{index}"));
    let mut metadata = builder.build("provider");
    metadata.preview = Some("preview".to_string());
    metadata
}

async fn insert_postgres_thread(pool: &PostgresPool, metadata: &ThreadMetadata) {
    let mut connection = pool.acquire().await.expect("runtime connection");
    sqlx::query(
        "INSERT INTO codex_storage.threads (id, origin_rollout_path, created_at_ms, \
         updated_at_ms, recency_at_ms, source, history_mode, model_provider, origin_cwd, \
         cli_version, title, preview, sandbox_policy, approval_mode, git_branch) \
         VALUES ($1::uuid, $2, $3, $4, $5, $6, 'legacy', 'provider', $7, $8, $9, 'preview', \
         $10, $11, $12)",
    )
    .bind(metadata.id.to_string())
    .bind(metadata.rollout_path.to_string_lossy().to_string())
    .bind(metadata.created_at.timestamp_millis())
    .bind(metadata.updated_at.timestamp_millis())
    .bind(metadata.recency_at.timestamp_millis())
    .bind(&metadata.source)
    .bind(metadata.cwd.to_string_lossy().to_string())
    .bind(&metadata.cli_version)
    .bind(&metadata.title)
    .bind(&metadata.sandbox_policy)
    .bind(&metadata.approval_mode)
    .bind(&metadata.git_branch)
    .execute(&mut *connection)
    .await
    .expect("insert thread row");
}

fn describe_outputs(outputs: &[Stage1Output]) -> Vec<String> {
    outputs
        .iter()
        .map(|output| {
            format!(
                "{} {} {:?} {:?} {:?} {} {:?} {:?}",
                output.thread_id,
                output.source_updated_at.timestamp(),
                output.raw_memory,
                output.rollout_summary,
                output.rollout_slug,
                output.rollout_path.display(),
                output.cwd.display().to_string(),
                output.git_branch
            )
        })
        .collect()
}

fn claim_label(outcome: &Stage1JobClaimOutcome) -> &'static str {
    match outcome {
        Stage1JobClaimOutcome::Claimed { .. } => "claimed",
        Stage1JobClaimOutcome::SkippedUpToDate => "up to date",
        Stage1JobClaimOutcome::SkippedRunning => "running",
        Stage1JobClaimOutcome::SkippedRetryBackoff => "retry backoff",
        Stage1JobClaimOutcome::SkippedRetryExhausted => "retry exhausted",
    }
}

fn claimed_token(outcome: Stage1JobClaimOutcome) -> String {
    match outcome {
        Stage1JobClaimOutcome::Claimed { ownership_token } => ownership_token,
        other => panic!("expected a claim, got {}", claim_label(&other)),
    }
}

fn phase2_label(outcome: &Phase2JobClaimOutcome) -> String {
    match outcome {
        Phase2JobClaimOutcome::Claimed {
            input_watermark, ..
        } => format!("claimed at {input_watermark}"),
        Phase2JobClaimOutcome::SkippedRunning => "running".to_string(),
        Phase2JobClaimOutcome::SkippedCooldown => "cooldown".to_string(),
        Phase2JobClaimOutcome::SkippedRetryUnavailable => "retry unavailable".to_string(),
    }
}

fn phase2_token(outcome: Phase2JobClaimOutcome) -> String {
    match outcome {
        Phase2JobClaimOutcome::Claimed {
            ownership_token, ..
        } => ownership_token,
        other => panic!("expected a phase-2 claim, got {}", phase2_label(&other)),
    }
}

/// Runs the memory pipeline through every operation and records each observable result.
/// Ownership tokens are random, so only their effects are recorded.
async fn scenario(
    store: &dyn RuntimeMemoryStore,
    threads: &[ThreadMetadata],
    current: ThreadId,
    now: i64,
) -> Vec<String> {
    let mut log = Vec::new();
    let [a, b, c, d] = threads else {
        panic!("the scenario needs four threads");
    };
    let source = |thread: &ThreadMetadata| thread.updated_at.timestamp();

    store.clear_memory_data().await.expect("clear");
    log.push(format!(
        "progress: {}",
        store.max_consolidated_thread_count().await.expect("count")
    ));

    // Startup claims take the newest eligible threads up to the claim cap.
    let claims = store
        .claim_stage1_jobs_for_startup(
            current,
            Stage1StartupClaimParams {
                scan_limit: 10,
                max_claimed: 2,
                max_age_days: 30,
                min_rollout_idle_hours: 1,
                allowed_sources: std::slice::from_ref(&a.source),
                lease_seconds: 600,
            },
        )
        .await
        .expect("startup claims");
    log.push(format!(
        "startup claims: {:?}",
        claims
            .iter()
            .map(|claim| format!(
                "{} {} {} {} {:?} {}",
                claim.thread.id,
                claim.thread.rollout_path.display(),
                claim.thread.cwd.display(),
                claim.thread.updated_at.timestamp(),
                claim.thread.git_branch,
                claim.thread.source
            ))
            .collect::<Vec<_>>()
    ));
    let a_token = claims
        .iter()
        .find(|claim| claim.thread.id == a.id)
        .expect("a claimed")
        .ownership_token
        .clone();

    let running = store
        .try_claim_stage1_job(a.id, current, source(a), 600, 64)
        .await
        .expect("claim running");
    log.push(format!("claim a again: {}", claim_label(&running)));
    let limited = store
        .try_claim_stage1_job(c.id, current, source(c), 600, 2)
        .await
        .expect("claim over the limit");
    log.push(format!(
        "claim c at the running limit: {}",
        claim_label(&limited)
    ));
    let c_claim = store
        .try_claim_stage1_job(c.id, current, source(c), 600, 3)
        .await
        .expect("claim c");
    log.push(format!("claim c with room: {}", claim_label(&c_claim)));
    let c_token = claimed_token(c_claim);

    log.push(format!(
        "a wrong token: {}",
        store
            .mark_stage1_job_succeeded(a.id, "wrong", source(a), "raw a", "sum a", None)
            .await
            .expect("wrong token")
    ));
    log.push(format!(
        "a succeeded: {}",
        store
            .mark_stage1_job_succeeded(a.id, &a_token, source(a), "raw a", "sum a", Some("slug-a"))
            .await
            .expect("a succeeded")
    ));
    log.push(format!(
        "listed: {:?}",
        describe_outputs(
            &store
                .list_stage1_outputs_for_global(10)
                .await
                .expect("list")
        )
    ));
    let up_to_date = store
        .try_claim_stage1_job(a.id, current, source(a), 600, 64)
        .await
        .expect("claim up to date");
    log.push(format!("claim a up to date: {}", claim_label(&up_to_date)));
    let newer = store
        .try_claim_stage1_job(a.id, current, source(a) + 1, 600, 64)
        .await
        .expect("claim newer");
    log.push(format!("claim a newer source: {}", claim_label(&newer)));
    let _ = claimed_token(newer);

    // Failures back off, and a job with no retries left only runs again for newer input.
    let b_token = claims
        .iter()
        .find(|claim| claim.thread.id == b.id)
        .expect("b claimed")
        .ownership_token
        .clone();
    log.push(format!(
        "b failed: {}",
        store
            .mark_stage1_job_failed(b.id, &b_token, "boom", 3600)
            .await
            .expect("b failed")
    ));
    log.push(format!(
        "b failed twice: {}",
        store
            .mark_stage1_job_failed(b.id, &b_token, "boom", 3600)
            .await
            .expect("b failed again")
    ));
    let backoff = store
        .try_claim_stage1_job(b.id, current, source(b), 600, 64)
        .await
        .expect("claim backoff");
    log.push(format!("claim b during backoff: {}", claim_label(&backoff)));

    log.push(format!(
        "c failed: {}",
        store
            .mark_stage1_job_failed(c.id, &c_token, "boom", 0)
            .await
            .expect("c failed")
    ));
    for round in 0..2 {
        let claim = store
            .try_claim_stage1_job(c.id, current, source(c), 600, 64)
            .await
            .expect("retry claim");
        log.push(format!("c retry {round}: {}", claim_label(&claim)));
        let retry_token = claimed_token(claim);
        log.push(format!(
            "c retry {round} failed: {}",
            store
                .mark_stage1_job_failed(c.id, &retry_token, "boom", 0)
                .await
                .expect("retry failed")
        ));
    }
    let exhausted = store
        .try_claim_stage1_job(c.id, current, source(c), 600, 64)
        .await
        .expect("claim exhausted");
    log.push(format!("c exhausted: {}", claim_label(&exhausted)));
    let reset = store
        .try_claim_stage1_job(c.id, current, source(c) + 1, 600, 64)
        .await
        .expect("claim reset");
    log.push(format!("c newer source resets: {}", claim_label(&reset)));
    let c_token = claimed_token(reset);
    log.push(format!(
        "c succeeded: {}",
        store
            .mark_stage1_job_succeeded(c.id, &c_token, source(c) + 1, "raw c", "sum c", None)
            .await
            .expect("c succeeded")
    ));

    // An old output nobody uses is retained until pruned.
    let old_source = now - 100 * 24 * 3600;
    let d_token = claimed_token(
        store
            .try_claim_stage1_job(d.id, current, old_source, 600, 64)
            .await
            .expect("claim d"),
    );
    log.push(format!(
        "d succeeded: {}",
        store
            .mark_stage1_job_succeeded(d.id, &d_token, old_source, "raw d", "sum d", Some("slug-d"))
            .await
            .expect("d succeeded")
    ));

    log.push(format!(
        "usage: {}",
        store
            .record_stage1_output_usage(&[a.id, c.id, ThreadId::new()])
            .await
            .expect("usage")
    ));
    log.push(format!(
        "usage again: {}",
        store
            .record_stage1_output_usage(&[a.id])
            .await
            .expect("usage again")
    ));
    log.push(format!(
        "empty usage: {}",
        store
            .record_stage1_output_usage(&[])
            .await
            .expect("empty usage")
    ));
    log.push(format!(
        "selection: {:?}",
        describe_outputs(
            &store
                .get_phase2_input_selection(10, 30)
                .await
                .expect("selection")
        )
    ));
    log.push(format!(
        "top selection: {:?}",
        describe_outputs(&store.get_phase2_input_selection(1, 30).await.expect("top"))
    ));
    log.push(format!(
        "listed with d: {:?}",
        describe_outputs(
            &store
                .list_stage1_outputs_for_global(10)
                .await
                .expect("list")
        )
    ));
    log.push(format!(
        "listed one: {:?}",
        describe_outputs(
            &store
                .list_stage1_outputs_for_global(1)
                .await
                .expect("list one")
        )
    ));

    // The global consolidation job follows the stage-1 work.
    let first = store
        .try_claim_global_phase2_job(current, 600)
        .await
        .expect("phase 2 claim");
    log.push(format!("phase 2 claim: {}", phase2_label(&first)));
    let phase2_token_value = phase2_token(first);
    log.push(format!(
        "phase 2 again: {}",
        phase2_label(
            &store
                .try_claim_global_phase2_job(current, 600)
                .await
                .expect("again")
        )
    ));
    log.push(format!(
        "heartbeat wrong: {}",
        store
            .heartbeat_global_phase2_job("wrong", 600)
            .await
            .expect("heartbeat wrong")
    ));
    log.push(format!(
        "heartbeat: {}",
        store
            .heartbeat_global_phase2_job(&phase2_token_value, 600)
            .await
            .expect("heartbeat")
    ));
    log.push(format!(
        "phase 2 failed wrong: {}",
        store
            .mark_global_phase2_job_failed("wrong", "boom", 3600)
            .await
            .expect("failed wrong")
    ));
    log.push(format!(
        "phase 2 failed: {}",
        store
            .mark_global_phase2_job_failed(&phase2_token_value, "boom", 3600)
            .await
            .expect("failed")
    ));
    log.push(format!(
        "phase 2 backoff: {}",
        phase2_label(
            &store
                .try_claim_global_phase2_job(current, 600)
                .await
                .expect("backoff")
        )
    ));
    log.push(format!(
        "failed if unowned when not running: {}",
        store
            .mark_global_phase2_job_failed_if_unowned("wrong", "boom", 0)
            .await
            .expect("unowned")
    ));
    store
        .enqueue_global_consolidation(now)
        .await
        .expect("enqueue consolidation");
    let retried = store
        .try_claim_global_phase2_job(current, 600)
        .await
        .expect("phase 2 retry");
    log.push(format!("phase 2 after enqueue: {}", phase2_label(&retried)));
    let phase2_token_value = phase2_token(retried);
    let selection = store
        .get_phase2_input_selection(10, 30)
        .await
        .expect("selection for success");
    log.push(format!(
        "phase 2 success wrong token: {}",
        store
            .mark_global_phase2_job_succeeded("wrong", now, &selection)
            .await
            .expect("success wrong")
    ));
    log.push(format!(
        "phase 2 success: {}",
        store
            .mark_global_phase2_job_succeeded(&phase2_token_value, now, &selection)
            .await
            .expect("success")
    ));
    log.push(format!(
        "progress: {}",
        store.max_consolidated_thread_count().await.expect("count")
    ));
    log.push(format!(
        "phase 2 cooldown: {}",
        phase2_label(
            &store
                .try_claim_global_phase2_job(current, 600)
                .await
                .expect("cooldown")
        )
    ));

    // Pruning keeps the output selected by the last consolidation and removes stale unused ones.
    log.push(format!(
        "pruned: {}",
        store
            .prune_stage1_outputs_for_retention(30, 10)
            .await
            .expect("prune")
    ));
    log.push(format!(
        "pruned none: {}",
        store
            .prune_stage1_outputs_for_retention(30, 0)
            .await
            .expect("prune zero")
    ));
    log.push(format!(
        "after prune: {:?}",
        describe_outputs(
            &store
                .list_stage1_outputs_for_global(10)
                .await
                .expect("list")
        )
    ));

    // Polluting a selected thread hides its memory and queues consolidation again.
    log.push(format!(
        "polluted: {}",
        store
            .mark_thread_memory_mode_polluted(a.id)
            .await
            .expect("polluted")
    ));
    log.push(format!(
        "polluted again: {}",
        store
            .mark_thread_memory_mode_polluted(a.id)
            .await
            .expect("polluted again")
    ));
    log.push(format!(
        "after pollution: {:?}",
        describe_outputs(
            &store
                .list_stage1_outputs_for_global(10)
                .await
                .expect("list")
        )
    ));

    // No-output completion removes an existing output.
    let c_token = claimed_token(
        store
            .try_claim_stage1_job(c.id, current, source(c) + 2, 600, 64)
            .await
            .expect("claim c again"),
    );
    log.push(format!(
        "c no output wrong token: {}",
        store
            .mark_stage1_job_succeeded_no_output(c.id, "wrong")
            .await
            .expect("no output wrong")
    ));
    log.push(format!(
        "c no output: {}",
        store
            .mark_stage1_job_succeeded_no_output(c.id, &c_token)
            .await
            .expect("no output")
    ));
    log.push(format!(
        "after no output: {:?}",
        describe_outputs(
            &store
                .list_stage1_outputs_for_global(10)
                .await
                .expect("list")
        )
    ));
    store
        .delete_thread_memory(a.id)
        .await
        .expect("delete a memory");
    store
        .delete_thread_memory(a.id)
        .await
        .expect("delete a again");
    log.push(format!(
        "after delete: {:?}",
        describe_outputs(
            &store
                .list_stage1_outputs_for_global(10)
                .await
                .expect("list")
        )
    ));
    let rerun = store
        .try_claim_stage1_job(a.id, current, source(a) + 3, 600, 64)
        .await
        .expect("claim after delete");
    log.push(format!("claim a after delete: {}", claim_label(&rerun)));

    store.clear_memory_data().await.expect("final clear");
    log.push(format!(
        "after clear: {:?} progress {}",
        describe_outputs(
            &store
                .list_stage1_outputs_for_global(10)
                .await
                .expect("list")
        ),
        store.max_consolidated_thread_count().await.expect("count")
    ));
    log
}

async fn bootstrap(state: &Path) {
    bootstrap_codex_storage(&*connect(state, "migrator").await)
        .await
        .expect("bootstrap memory schema");
}

async fn real_postgres_memory_matches_sqlite() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_MEMORY_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    bootstrap(state).await;
    let pool = connect(state, "runtime").await;
    let postgres = PostgresMemoryStore::new(pool.clone());
    let sqlite_home = TempDir::new().expect("sqlite fixture home");
    let sqlite = StateRuntime::init(
        SqliteConfig::new_for_testing(sqlite_home.path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("sqlite state runtime");

    let token = token();
    let now = Utc::now().timestamp();
    let threads: Vec<ThreadMetadata> = (0..4).map(|index| thread(&token, index, now)).collect();
    let current = ThreadId::new();
    for metadata in &threads {
        sqlite.upsert_thread(metadata).await.expect("sqlite thread");
        insert_postgres_thread(&pool, metadata).await;
    }
    let expected = scenario(sqlite.memories(), &threads, current, now).await;
    let actual = scenario(&postgres, &threads, current, now).await;
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(&expected) {
        assert_eq!(actual, expected);
    }
}

async fn real_postgres_memory_serializes_competing_workers() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_MEMORY_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    bootstrap(state).await;
    let pool = connect(state, "runtime").await;
    let store = PostgresMemoryStore::new(pool.clone());
    let token = token();
    let now = Utc::now().timestamp();
    let threads: Vec<ThreadMetadata> = (0..10).map(|index| thread(&token, index, now)).collect();
    for metadata in &threads {
        insert_postgres_thread(&pool, metadata).await;
    }
    store.clear_memory_data().await.expect("clear");

    // Racing claims cannot exceed the running-job limit.
    let mut tasks = Vec::new();
    for metadata in &threads {
        let store = store.clone();
        let (thread_id, updated) = (metadata.id, metadata.updated_at.timestamp());
        tasks.push(tokio::spawn(async move {
            store
                .try_claim_stage1_job(thread_id, ThreadId::new(), updated, 600, 3)
                .await
        }));
    }
    let mut tokens = Vec::new();
    for (task, metadata) in tasks.into_iter().zip(&threads) {
        if let Stage1JobClaimOutcome::Claimed { ownership_token } =
            task.await.expect("claim task").expect("claim")
        {
            tokens.push((metadata.clone(), ownership_token));
        }
    }
    assert_eq!(tokens.len(), 3);

    // Completions and a consolidation claim can race without deadlocking.
    let mut tasks = Vec::new();
    for (metadata, ownership_token) in tokens {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store
                .mark_stage1_job_succeeded(
                    metadata.id,
                    &ownership_token,
                    metadata.updated_at.timestamp(),
                    "raw",
                    "summary",
                    None,
                )
                .await
        }));
    }
    let phase2 = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .try_claim_global_phase2_job(ThreadId::new(), 600)
                .await
        })
    };
    for task in tasks {
        assert!(task.await.expect("completion task").expect("completion"));
    }
    phase2.await.expect("phase 2 task").expect("phase 2 claim");

    // Only one of many racing workers can own the consolidation lock.
    store.clear_memory_data().await.expect("clear again");
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store
                .try_claim_global_phase2_job(ThreadId::new(), 600)
                .await
        }));
    }
    let mut claimed = 0;
    for task in tasks {
        if matches!(
            task.await.expect("phase 2 task").expect("phase 2 claim"),
            Phase2JobClaimOutcome::Claimed { .. }
        ) {
            claimed += 1;
        }
    }
    assert_eq!(claimed, 1);
    store.clear_memory_data().await.expect("final clear");
}

/// Both checks use the global memory tables, so they run one after another.
#[tokio::test]
async fn real_postgres_memory() {
    real_postgres_memory_matches_sqlite().await;
    real_postgres_memory_serializes_competing_workers().await;
}
