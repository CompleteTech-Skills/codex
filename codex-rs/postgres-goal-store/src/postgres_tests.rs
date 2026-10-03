use super::*;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
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
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: 4,
        },
    }
}

/// The part of a goal both stores must agree on exactly. Identifiers and timestamps differ per
/// store and are checked through their invariants instead.
#[derive(Debug, Eq, PartialEq)]
struct View {
    objective: String,
    status: ThreadGoalStatus,
    token_budget: Option<i64>,
    tokens_used: i64,
    time_used_seconds: i64,
}

fn view(goal: &ThreadGoal) -> View {
    View {
        objective: goal.objective.clone(),
        status: goal.status,
        token_budget: goal.token_budget,
        tokens_used: goal.tokens_used,
        time_used_seconds: goal.time_used_seconds,
    }
}

fn view_of(goal: Option<ThreadGoal>) -> Option<View> {
    goal.as_ref().map(view)
}

fn accounted(outcome: GoalAccountingOutcome) -> (bool, Option<View>) {
    match outcome {
        GoalAccountingOutcome::Unchanged(goal) => (false, view_of(goal)),
        GoalAccountingOutcome::Updated(goal) => (true, Some(view(&goal))),
    }
}

fn update(
    objective: Option<&str>,
    status: Option<ThreadGoalStatus>,
    token_budget: Option<Option<i64>>,
    expected_goal_id: Option<&str>,
) -> GoalUpdate {
    GoalUpdate {
        objective: objective.map(str::to_string),
        status,
        token_budget,
        expected_goal_id: expected_goal_id.map(str::to_string),
    }
}

/// Runs every goal operation and records what a caller can observe.
async fn scenario(store: &dyn ThreadGoalStore, thread_id: ThreadId) -> Vec<String> {
    use ThreadGoalStatus::Active;
    use ThreadGoalStatus::Blocked;
    use ThreadGoalStatus::BudgetLimited;
    use ThreadGoalStatus::Complete;
    use ThreadGoalStatus::Paused;
    let mut log = Vec::new();
    macro_rules! record {
        ($label:expr, $value:expr) => {
            log.push(format!("{}: {:?}", $label, $value));
        };
    }

    record!(
        "get missing",
        view_of(store.get_thread_goal(thread_id).await.expect("get"))
    );

    let first = store
        .replace_thread_goal(thread_id, "ship", Active, Some(10))
        .await
        .expect("replace");
    record!("replace active", view(&first));
    let second = store
        .replace_thread_goal(thread_id, "ship again", Active, Some(0))
        .await
        .expect("replace again");
    record!("replace over budget", view(&second));
    assert_ne!(
        first.goal_id, second.goal_id,
        "replacement gets a new goal id"
    );
    assert!(second.created_at >= first.created_at);
    record!(
        "insert existing",
        view_of(
            store
                .insert_thread_goal(thread_id, "ignored", Active, None)
                .await
                .expect("insert existing")
        )
    );
    record!(
        "update objective",
        view_of(
            store
                .update_thread_goal(thread_id, update(Some("renamed"), None, None, None))
                .await
                .expect("update objective")
        )
    );
    let kept = store
        .get_thread_goal(thread_id)
        .await
        .expect("get")
        .expect("goal");
    assert_eq!(kept.goal_id, second.goal_id, "an update keeps the goal id");
    assert_eq!(
        kept.created_at, second.created_at,
        "an update keeps created_at"
    );
    assert!(kept.updated_at >= second.updated_at);
    record!(
        "stale expected id",
        view_of(
            store
                .update_thread_goal(thread_id, update(Some("nope"), None, None, Some("stale")))
                .await
                .expect("stale update")
        )
    );
    record!(
        "matching expected id",
        view_of(
            store
                .update_thread_goal(
                    thread_id,
                    update(Some("matched"), None, None, Some(kept.goal_id.as_str()))
                )
                .await
                .expect("matched update")
        )
    );
    record!(
        "noop stale",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, None, None, Some("stale")))
                .await
                .expect("noop stale")
        )
    );
    record!(
        "noop current",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, None, None, None))
                .await
                .expect("noop current")
        )
    );
    record!(
        "activate while over budget",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, Some(Active), None, None))
                .await
                .expect("activate")
        )
    );
    record!(
        "pause budget limited",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, Some(Paused), None, None))
                .await
                .expect("pause budget limited")
        )
    );
    record!(
        "raise budget and activate",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, Some(Active), Some(Some(100)), None))
                .await
                .expect("raise budget")
        )
    );
    record!(
        "clear budget",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, None, Some(None), None))
                .await
                .expect("clear budget")
        )
    );
    record!(
        "budget at usage",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, None, Some(Some(0)), None))
                .await
                .expect("budget at usage")
        )
    );
    record!(
        "block budget limited",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, Some(Blocked), None, None))
                .await
                .expect("block")
        )
    );
    record!(
        "status and budget",
        view_of(
            store
                .update_thread_goal(
                    thread_id,
                    update(Some("final"), Some(Active), Some(Some(50)), None)
                )
                .await
                .expect("status and budget")
        )
    );

    record!(
        "zero delta",
        accounted(
            store
                .account_thread_goal_usage(thread_id, 0, 0, GoalAccountingMode::ActiveOnly, None)
                .await
                .expect("zero delta")
        )
    );
    record!(
        "negative delta",
        accounted(
            store
                .account_thread_goal_usage(thread_id, -5, -5, GoalAccountingMode::ActiveOnly, None)
                .await
                .expect("negative delta")
        )
    );
    record!(
        "stale expected usage",
        accounted(
            store
                .account_thread_goal_usage(
                    thread_id,
                    3,
                    4,
                    GoalAccountingMode::ActiveOnly,
                    Some("stale")
                )
                .await
                .expect("stale usage")
        )
    );
    record!(
        "active usage",
        accounted(
            store
                .account_thread_goal_usage(
                    thread_id,
                    3,
                    20,
                    GoalAccountingMode::ActiveStatusOnly,
                    None
                )
                .await
                .expect("active usage")
        )
    );
    record!(
        "crossing budget",
        accounted(
            store
                .account_thread_goal_usage(thread_id, 1, 40, GoalAccountingMode::ActiveOnly, None)
                .await
                .expect("crossing budget")
        )
    );
    record!(
        "status only skips budget limited",
        accounted(
            store
                .account_thread_goal_usage(
                    thread_id,
                    1,
                    1,
                    GoalAccountingMode::ActiveStatusOnly,
                    None
                )
                .await
                .expect("status only")
        )
    );
    record!(
        "active only accounts budget limited",
        accounted(
            store
                .account_thread_goal_usage(thread_id, 2, 2, GoalAccountingMode::ActiveOnly, None)
                .await
                .expect("active only")
        )
    );
    record!(
        "usage limit budget limited",
        view_of(
            store
                .usage_limit_active_thread_goal(thread_id)
                .await
                .expect("usage limit")
        )
    );
    record!(
        "stopped mode accounts usage limited",
        accounted(
            store
                .account_thread_goal_usage(
                    thread_id,
                    1,
                    1,
                    GoalAccountingMode::ActiveOrStopped,
                    None
                )
                .await
                .expect("stopped usage")
        )
    );
    record!(
        "pause when not active",
        view_of(
            store
                .pause_active_thread_goal(thread_id)
                .await
                .expect("pause")
        )
    );
    record!(
        "reactivate",
        view_of(
            store
                .update_thread_goal(
                    thread_id,
                    update(None, Some(Active), Some(Some(1000)), None)
                )
                .await
                .expect("reactivate")
        )
    );
    record!(
        "pause active",
        view_of(
            store
                .pause_active_thread_goal(thread_id)
                .await
                .expect("pause")
        )
    );
    record!(
        "complete",
        view_of(
            store
                .update_thread_goal(thread_id, update(None, Some(Complete), None, None))
                .await
                .expect("complete")
        )
    );
    record!(
        "complete mode accounts completed goal",
        accounted(
            store
                .account_thread_goal_usage(
                    thread_id,
                    1,
                    1,
                    GoalAccountingMode::ActiveOrComplete,
                    None
                )
                .await
                .expect("complete accounting")
        )
    );
    record!(
        "active only skips completed goal",
        accounted(
            store
                .account_thread_goal_usage(thread_id, 1, 1, GoalAccountingMode::ActiveOnly, None)
                .await
                .expect("completed skipped")
        )
    );
    let replaced = store
        .insert_thread_goal(thread_id, "after complete", Active, Some(5))
        .await
        .expect("insert after complete")
        .expect("completed goal is replaceable");
    record!("insert after complete", view(&replaced));
    assert_ne!(replaced.goal_id, kept.goal_id);

    let snapshot = ThreadGoal {
        thread_id,
        goal_id: "snapshot-goal".to_string(),
        objective: "imported".to_string(),
        status: BudgetLimited,
        token_budget: Some(7),
        tokens_used: 9,
        time_used_seconds: 11,
        created_at: replaced.created_at,
        updated_at: replaced.updated_at,
    };
    record!(
        "deferral before snapshot",
        store
            .has_thread_goal_continuation_deferral(thread_id)
            .await
            .expect("deferral")
    );
    store
        .replace_thread_goal_snapshot(&snapshot)
        .await
        .expect("snapshot");
    let stored = store
        .get_thread_goal(thread_id)
        .await
        .expect("get snapshot")
        .expect("snapshot goal");
    assert_eq!(stored, snapshot, "a snapshot is stored exactly");
    record!("snapshot", view(&stored));
    record!(
        "deferral after snapshot",
        store
            .has_thread_goal_continuation_deferral(thread_id)
            .await
            .expect("deferral")
    );
    store
        .clear_thread_goal_continuation_deferral(thread_id)
        .await
        .expect("clear deferral");
    record!(
        "deferral after clear",
        store
            .has_thread_goal_continuation_deferral(thread_id)
            .await
            .expect("deferral")
    );
    record!(
        "delete",
        view_of(store.delete_thread_goal(thread_id).await.expect("delete"))
    );
    record!(
        "delete again",
        view_of(
            store
                .delete_thread_goal(thread_id)
                .await
                .expect("delete again")
        )
    );
    record!(
        "get after delete",
        view_of(store.get_thread_goal(thread_id).await.expect("get"))
    );
    log
}

async fn insert_thread(runtime: &PostgresPool, thread_id: ThreadId) {
    let mut connection = runtime.acquire().await.expect("runtime connection");
    sqlx::query(
        "INSERT INTO codex_storage.threads (id, origin_rollout_path, created_at_ms, \
         updated_at_ms, recency_at_ms, source, history_mode, model_provider, origin_cwd, \
         cli_version, title, sandbox_policy, approval_mode) \
         VALUES ($1::uuid, 'origin', 1, 1, 1, 'cli', 'legacy', 'provider', 'cwd', '1', 'title', \
         'sandbox', 'approval')",
    )
    .bind(thread_id.to_string())
    .execute(&mut *connection)
    .await
    .expect("insert thread row");
}

#[tokio::test]
async fn real_postgres_goals_match_sqlite() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_GOAL_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("migrator pool");
    bootstrap_codex_storage(&migrator)
        .await
        .expect("bootstrap goal schema");
    let runtime = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
            .await
            .expect("runtime pool"),
    );
    runtime
        .bind_current_activation()
        .await
        .expect("actual runtime activation binding");
    let postgres = PostgresGoalStore::new(runtime.clone());
    let sqlite_home = TempDir::new().expect("sqlite fixture home");
    let sqlite = StateRuntime::init(
        SqliteConfig::new_for_testing(sqlite_home.path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("sqlite state runtime");

    let sqlite_thread = ThreadId::new();
    let postgres_thread = ThreadId::new();
    insert_thread(&runtime, postgres_thread).await;
    let expected = scenario(sqlite.thread_goals(), sqlite_thread).await;
    let actual = scenario(&postgres, postgres_thread).await;
    assert_eq!(actual, expected);

    // Goals follow their thread: deleting the thread row removes the goal and its deferral.
    let cascading = ThreadId::new();
    insert_thread(&runtime, cascading).await;
    let goal = ThreadGoal {
        thread_id: cascading,
        goal_id: "cascade".to_string(),
        objective: "follows thread".to_string(),
        status: ThreadGoalStatus::Active,
        token_budget: None,
        tokens_used: 0,
        time_used_seconds: 0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    postgres
        .replace_thread_goal_snapshot(&goal)
        .await
        .expect("snapshot for cascade");
    let mut connection = runtime.acquire().await.expect("runtime connection");
    sqlx::query("DELETE FROM codex_storage.threads WHERE id = $1::uuid")
        .bind(cascading.to_string())
        .execute(&mut *connection)
        .await
        .expect("delete thread row");
    drop(connection);
    assert_eq!(postgres.get(cascading).await, Ok(None));
    assert_eq!(postgres.has_deferral(cascading).await, Ok(false));

    // A goal needs its thread row; the missing row is a typed error, not a silent insert.
    assert_eq!(
        postgres
            .upsert(
                ThreadId::new(),
                "orphan",
                ThreadGoalStatus::Active,
                None,
                /*only_replace_complete*/ false,
            )
            .await,
        Err(GoalStoreError::ThreadNotFound)
    );
}
