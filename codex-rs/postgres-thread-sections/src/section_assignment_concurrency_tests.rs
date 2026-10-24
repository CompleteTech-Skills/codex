use super::*;
use crate::fixture_support::settings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_postgres_runtime::bootstrap_named_namespace;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use std::path::Path;

type AssignmentRow = (String, Option<String>, Option<i64>, Option<i64>);

async fn concurrent_append(state: &Path, namespace: SectionNamespace) {
    let named = matches!(namespace, SectionNamespace::Named(_));
    let schema = match &namespace {
        SectionNamespace::Default => "codex_storage",
        SectionNamespace::Named(value) => value.schema_name(),
    };
    let migrator = PostgresPool::connect(settings(
        state,
        if named {
            "isolation_migrator"
        } else {
            "migrator"
        },
    ))
    .await
    .expect("owned migrator");
    match &namespace {
        SectionNamespace::Default => bootstrap_codex_storage(&migrator).await.expect("v5"),
        SectionNamespace::Named(value) => bootstrap_named_namespace(&migrator, value)
            .await
            .expect("named v5"),
    }
    let role = if named {
        "isolation_runtime"
    } else {
        "runtime"
    };
    let pool = Arc::new(
        PostgresPool::connect(settings(state, role))
            .await
            .expect("runtime"),
    );
    // Independent observer pool avoids consuming a participant's last connection.
    let observer = PostgresPool::connect(settings(state, role))
        .await
        .expect("same-role observer");
    let adapter = PostgresThreadSectionStore::new(Arc::clone(&pool), namespace.clone());
    let section = adapter
        .create_thread_section("concurrent-owned", /*appearance*/ None)
        .await
        .expect("section");
    let ids = [ThreadId::new(), ThreadId::new()];
    let mut connection = pool.acquire().await.expect("setup");
    for id in ids {
        sqlx::query(AssertSqlSafe(format!("INSERT INTO \"{schema}\".threads(id,origin_rollout_path,created_at_ms,updated_at_ms,recency_at_ms,source,history_mode,model_provider,origin_cwd,cli_version,title,sandbox_policy,approval_mode) VALUES ($1::uuid,'origin',1,1,1,'cli','full','test','origin','test','concurrent-probe','read-only','never')")))
        .bind(id.to_string()).execute(&mut *connection).await.expect("owned threads");
    }
    drop(connection);
    let mut blocker = pool.begin_serializable().await.expect("section blocker");
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM \"{schema}\".thread_sections WHERE id=$1 FOR UPDATE"
    )))
    .bind(&section.id)
    .execute(blocker.connection())
    .await
    .expect("hold destination");
    let mut observation = observer.acquire().await.expect("observer");
    let tasks = ids.map(|id| {
        let adapter = adapter.clone();
        let section = section.id.clone();
        tokio::spawn(async move {
            adapter
                .assign_thread_to_section(id, &section, SectionPlacement::Append)
                .await
        })
    });
    let observed = timeout(Duration::from_secs(/*secs*/30), async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE usename=current_user AND state='active' AND wait_event_type='Lock' AND query LIKE '%thread_sections%' AND pid<>pg_backend_pid()")
                .fetch_one(&mut *observation).await?;
            if waiting == 2 { break Ok::<(), sqlx::Error>(()); }
            tokio::task::yield_now().await;
        }
    }).await;
    drop(observation);
    let released = blocker.commit().await;
    let mut outcomes = Vec::new();
    let mut timed_out = Vec::new();
    for (id, mut task) in ids.iter().zip(tasks) {
        let outcome = match timeout(Duration::from_secs(/*secs*/ 30), &mut task).await {
            Ok(outcome) => outcome,
            Err(_) => {
                timed_out.push(*id);
                // Cancel only this test-owned future, then join to prevent detached writes.
                task.abort();
                task.await
            }
        };
        outcomes.push((*id, outcome));
    }
    // Every participant has been joined before any causal failure is asserted.
    assert!(timed_out.is_empty(), "participant timeout: {timed_out:?}");
    assert!(
        observed.is_ok_and(|result| result.is_ok()),
        "contention was not established"
    );
    released.expect("release destination lock");
    let mut successes = 0;
    let mut rejected = Vec::new();
    for (id, outcome) in outcomes {
        let result = outcome.expect("participant joined");
        match result {
            Ok(true) => successes += 1,
            Err(SectionStoreError::SerializationConflict | SectionStoreError::Deadlock) => {
                rejected.push(id);
            }
            other => panic!("unexpected concurrency outcome: {other:?}"),
        }
    }
    assert!(successes >= 1);
    // All participants have finished; retry only explicitly rejected transactions.
    for id in rejected {
        assert_eq!(
            adapter
                .assign_thread_to_section(id, &section.id, SectionPlacement::Append)
                .await,
            Ok(true)
        );
    }
    let mut connection = pool.acquire().await.expect("terminal readback");
    let rows:Vec<AssignmentRow>=sqlx::query_as(AssertSqlSafe(format!("SELECT id::text,thread_section_id,section_position,section_entered_at_ms FROM \"{schema}\".threads WHERE id::text=ANY($1::text[]) ORDER BY section_position,id")))
        .bind(ids.map(|id|id.to_string()).to_vec()).fetch_all(&mut *connection).await.expect("both actual rows");
    let mut expected_ids: Vec<_> = ids.map(|id| id.to_string()).to_vec();
    expected_ids.sort();
    let mut actual_ids: Vec<_> = rows.iter().map(|row| row.0.clone()).collect();
    actual_ids.sort();
    assert_eq!(actual_ids, expected_ids);
    assert_eq!(
        rows.iter()
            .map(|row| (row.1.clone(), row.2))
            .collect::<Vec<_>>(),
        vec![
            (Some(section.id.clone()), Some(1_000_000)),
            (Some(section.id.clone()), Some(2_000_000))
        ]
    );
    assert!(
        rows.iter()
            .all(|row| row.3.is_some_and(|entered| entered > 0))
    );
    let mut baseline = Vec::new();
    for id in ids {
        let row: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid"
        )))
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("complete concurrency baseline");
        baseline.push(row);
    }
    drop(connection);
    // Cancel after the assignment has acquired its thread lock and reached the section wait.
    let mut blocker = pool.begin_serializable().await.expect("cancel blocker");
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM \"{schema}\".thread_sections WHERE id=$1 FOR UPDATE"
    )))
    .bind(&section.id)
    .execute(blocker.connection())
    .await
    .expect("hold cancel destination");
    let mut observation = observer.acquire().await.expect("cancel observer");
    let cancelled = {
        let adapter = adapter.clone();
        let section = section.id.clone();
        tokio::spawn(async move {
            adapter
                .assign_thread_to_section(ids[0], &section, SectionPlacement::Append)
                .await
        })
    };
    let reached = timeout(Duration::from_secs(/*secs*/30), async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE usename=current_user AND state='active' AND wait_event_type='Lock' AND query LIKE '%thread_sections%' AND pid<>pg_backend_pid()")
                .fetch_one(&mut *observation).await?;
            if waiting==1 {break Ok::<(),sqlx::Error>(());}
            tokio::task::yield_now().await;
        }
    }).await;
    cancelled.abort();
    let cancelled_outcome = cancelled.await;
    let rollback_observed = timeout(Duration::from_secs(/*secs*/ 30), async {
        loop {
            let result = sqlx::query(AssertSqlSafe(format!(
                "SELECT id FROM \"{schema}\".threads WHERE id=$1::uuid FOR UPDATE NOWAIT"
            )))
            .bind(ids[0].to_string())
            .execute(&mut *observation)
            .await;
            match result {
                Ok(_) => break Ok::<(), sqlx::Error>(()),
                Err(error)
                    if error
                        .as_database_error()
                        .and_then(sqlx::error::DatabaseError::code)
                        .as_deref()
                        == Some("55P03") =>
                {
                    tokio::task::yield_now().await
                }
                Err(error) => break Err(error),
            }
        }
    })
    .await;
    drop(observation);
    let released = blocker.rollback().await;
    assert!(
        reached.is_ok_and(|result| result.is_ok()),
        "cancelled participant never reached actual lock wait"
    );
    assert!(cancelled_outcome.is_err_and(|error| error.is_cancelled()));
    assert!(
        rollback_observed.is_ok_and(|result| result.is_ok()),
        "cancelled transaction did not release thread lock"
    );
    released.expect("cancel blocker release");
    let mut connection = pool.acquire().await.expect("cancellation readback");
    for (id, expected) in ids.iter().zip(&baseline) {
        let actual: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid"
        )))
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("complete cancelled row");
        assert_eq!(&actual, expected);
    }
    drop(connection);
    // Actual production assignment and deletion contend on the same owned destination.
    let mut blocker = pool
        .begin_serializable()
        .await
        .expect("delete race blocker");
    sqlx::query(AssertSqlSafe(format!(
        "SELECT id FROM \"{schema}\".thread_sections WHERE id=$1 FOR UPDATE"
    )))
    .bind(&section.id)
    .execute(blocker.connection())
    .await
    .expect("hold race destination");
    let mut observation = observer.acquire().await.expect("delete race observer");
    let assigning = {
        let adapter = adapter.clone();
        let section = section.id.clone();
        tokio::spawn(async move {
            adapter
                .assign_thread_to_section(ids[0], &section, SectionPlacement::Append)
                .await
        })
    };
    let deleting = {
        let adapter = adapter.clone();
        let section = section.id.clone();
        tokio::spawn(async move { adapter.delete_thread_section(&section).await })
    };
    let mut last_waiting = 0;
    let mut max_waiting = 0;
    let reached=timeout(Duration::from_secs(/*secs*/30),async {
        loop {let waiting:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE usename=current_user AND state='active' AND wait_event_type='Lock' AND query LIKE '%thread_sections%' AND pid<>pg_backend_pid()")
            .fetch_one(&mut *observation).await?;
            last_waiting = waiting;
            max_waiting = max_waiting.max(waiting);
            if waiting==2 {break Ok::<(),sqlx::Error>(());} tokio::task::yield_now().await;
        }
    }).await;
    drop(observation);
    let released = blocker.commit().await;
    let mut outcomes = Vec::new();
    let mut timed_out = false;
    for mut task in [assigning, deleting] {
        let outcome = match timeout(Duration::from_secs(/*secs*/ 30), &mut task).await {
            Ok(outcome) => outcome,
            Err(_) => {
                timed_out = true;
                task.abort();
                task.await
            }
        };
        outcomes.push(outcome);
    }
    assert!(
        !timed_out,
        "owned delete race participant timed out after all participants drained"
    );
    assert!(
        reached.as_ref().is_ok_and(std::result::Result::is_ok),
        "actual assignment/delete contention not observed: observed={reached:?}, last_waiting={last_waiting}, max_waiting={max_waiting}, joined={outcomes:?}"
    );
    released.expect("race blocker released");
    let assignment = outcomes.remove(0).expect("assignment joined");
    let deletion = outcomes.remove(0).expect("deletion joined");
    assert!(matches!(
        assignment,
        Ok(true)
            | Err(SectionStoreError::SerializationConflict
                | SectionStoreError::Deadlock
                | SectionStoreError::InvalidSection)
    ));
    assert!(matches!(
        deletion,
        Ok(true) | Err(SectionStoreError::SerializationConflict | SectionStoreError::Deadlock)
    ));
    let deleted = deletion == Ok(true);
    assert_eq!(
        adapter
            .get_thread_section(&section.id)
            .await
            .expect("actual catalog outcome"),
        if deleted { None } else { Some(section.clone()) }
    );
    let mut connection = pool.acquire().await.expect("full delete race rows");
    for (index, id) in ids.iter().enumerate() {
        let actual: serde_json::Value = sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT to_jsonb(t) FROM \"{schema}\".threads t WHERE id=$1::uuid"
        )))
        .bind(id.to_string())
        .fetch_one(&mut *connection)
        .await
        .expect("retained race thread");
        let mut expected = baseline[index].clone();
        if deleted {
            for key in [
                "thread_section_id",
                "section_position",
                "section_entered_at_ms",
            ] {
                expected[key] = serde_json::Value::Null;
            }
        } else if assignment == Ok(true) && index == 0 {
            expected["section_position"] = serde_json::Value::from(
                baseline[1]["section_position"]
                    .as_i64()
                    .expect("other rank")
                    + 1_000_000,
            );
        } else {
            assert_ne!(assignment, Err(SectionStoreError::InvalidSection));
        }
        assert_eq!(actual, expected);
    }
    for id in ids {
        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM \"{schema}\".threads WHERE id=$1::uuid"
        )))
        .bind(id.to_string())
        .execute(&mut *connection)
        .await
        .expect("own thread cleanup");
    }
    drop(connection);
    assert_eq!(
        adapter.delete_thread_section(&section.id).await,
        Ok(!deleted)
    );
    observer.close().await.expect("observer close");
    pool.close().await.expect("runtime close");
    migrator.close().await.expect("migrator close");
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_default_concurrent_append_has_unique_terminal_ranks() {
    let state = std::env::var("CODEX_TEST_POSTGRES_SECTION_STORE_STATE").expect("required fixture");
    concurrent_append(Path::new(&state), SectionNamespace::Default).await;
}

#[tokio::test]
#[ignore = "requires exclusive disposable PostgreSQL fixture and receipt"]
async fn real_named_concurrent_append_has_unique_terminal_ranks() {
    let state =
        std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE").expect("required named fixture");
    concurrent_append(
        Path::new(&state),
        SectionNamespace::Named(NamedNamespace::new("codex_storage_isolation").expect("namespace")),
    )
    .await;
}
