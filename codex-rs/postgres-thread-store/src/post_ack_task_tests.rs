// Four faults occur in the SAME production task AFTER actual xid ACK,
// external original capture and pool return, BEFORE result delivery.
#[tokio::test]
#[ignore = "requires admitted current default PostgreSQL fixture and retained host"]
async fn actual_post_ack_panic_and_abort_preserve_xid_bytes_and_join_cause() -> ThreadStoreResult<()>
{
    let state = std::env::var("CODEX_TEST_POSTGRES_THREAD_STORE_STATE").map_err(internal)?;
    let state = Path::new(&state);
    let cwd = tempfile::tempdir().map_err(internal)?.keep();
    let observer = connect(state, "runtime").await;
    let mut limits = settings(state, "runtime");
    limits.limits.max_connections = 1;
    let pool = Arc::new(
        PostgresPool::connect_in_namespace(limits, None)
            .await
            .map_err(internal)?,
    );
    pool.bind_current_activation().await.map_err(internal)?;
    let writer = PostgresThreadStore::new(Arc::clone(&pool), "post-ack-original".to_string());
    for rollback in [false, true] {
        for panic in [true, false] {
            let id = ThreadId::new();
            writer
                .create_thread(create_params(id, "post-ack-original", &cwd))
                .await
                .held()
                .await;
            let original = writer.live_thread(id).await.held().await;
            writer
                .persist_thread(id, PersistContext::Standard)
                .await
                .held()
                .await;
            let before = original_task_snapshot(&observer, id).await.held().await;
            let before_lines = post_ack_lines(&observer, id).await.held().await;
            let expected_pid: i32 = {
                let mut connection = pool.acquire().await.held().await;
                sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
                    .fetch_one(&mut *connection)
                    .await
                    .held()
                    .await
            };
            let (ready, observed) = tokio::sync::oneshot::channel();
            original
                .lock()
                .await
                .inject_post_ack_fault(panic, rollback, ready);
            let mut append = writer.append_items(AppendThreadItemsParams {
                thread_id: id,
                items: vec![user_message("actual post-ACK original bytes")],
            });
            let witnessed = tokio::select! { biased;
                ready = tokio::time::timeout(Duration::from_secs(10), observed) => ready.map_err(internal).and_then(|ready| ready.map_err(internal)),
                result = &mut append => result.and(Err(internal("caller returned before post-ACK readiness"))),
            };
            drop(append); // Caller cancellation never substitutes/replaces the original task.
            let (task_id, staged) = {
                let original = original.lock().await;
                (
                    original.original_mutation_task_id(),
                    original.original_staged_lines().map(<[_]>::to_vec),
                )
            };
            if !panic || witnessed.is_err() {
                original.lock().await.abort_original_mutation_task();
            }
            // Every readiness/error path joins the same task and finishes actual cleanup
            // before assertions. Uncertainty keeps this original host and every owner.
            let mut cleanup = writer.discard_thread(id);
            let (cleaned, late) =
                match tokio::time::timeout(Duration::from_secs(10), &mut cleanup).await {
                    Ok(result) => (result, false),
                    Err(_) => (cleanup.await, true),
                };
            if cleaned.is_err() {
                loop {
                    let _ = (
                        &cleaned, &original, &writer, &pool, &observer, &witnessed, &staged,
                    );
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
            let after = original_task_snapshot(&observer, id).await.held().await;
            let after_lines = post_ack_lines(&observer, id).await.held().await;
            let (same_cause, remaining) = {
                let original = original.lock().await;
                (
                    original
                        .reconciled_original_join_error()
                        .is_some_and(|error| {
                            Some(error.id()) == task_id
                                && if panic {
                                    error.is_panic()
                                } else {
                                    error.is_cancelled()
                                }
                        }),
                    original.original_staged_lines().map(<[_]>::to_vec),
                )
            };
            let archived = writer.pending_writers.retains_mutation_frame(&original)?;
            let removed = matches!(
                writer.live_thread(id).await,
                Err(ThreadStoreError::ThreadNotFound)
            );
            let actual_ack = witnessed?; // Original task and cleanup are terminal before diagnostic refusal.
            let actual_status: Option<String> = {
                let mut connection = observer.acquire().await.held().await;
                sqlx::query_scalar("SELECT pg_catalog.pg_xact_status($1::pg_catalog.xid8)")
                    .bind(actual_ack.original_xid())
                    .fetch_one(&mut *connection)
                    .await
                    .held()
                    .await
            };
            let (returned_pid, released): (i32, bool) = {
                let mut connection = pool.acquire().await.held().await;
                sqlx::query_as("SELECT pg_catalog.pg_backend_pid(),owner_id IS NULL AND lease_until IS NULL FROM codex_storage.thread_writer_ownership WHERE thread_id=$1::uuid").bind(id.to_string()).fetch_one(&mut *connection).await.held().await
            };
            let mut expected_lines = before_lines.clone();
            if !rollback {
                if let Some(staged) = staged.as_ref() {
                    expected_lines.extend(staged.iter().cloned());
                }
            }
            if late
                || !same_cause
                || staged.is_none()
                || !archived
                || !removed
                || !released
                || returned_pid != expected_pid
                || actual_ack.committed() == rollback
                || actual_status.as_deref() != Some(if rollback { "aborted" } else { "committed" })
                || after_lines != expected_lines
                || if rollback {
                    remaining != staged || before != after
                } else {
                    remaining.is_some() || before == after
                }
            {
                return Err(internal(
                    "actual post-ACK task/xid/staged-byte/cause recovery failed",
                ));
            }
        }
    }
    Ok(())
}
async fn post_ack_lines(
    pool: &PostgresPool,
    id: ThreadId,
) -> ThreadStoreResult<Vec<(Option<u64>, String)>> {
    let mut connection = pool.acquire().await.map_err(internal)?;
    let rows: Vec<(Option<i64>, String)> = sqlx::query_as("SELECT ordinal,line FROM codex_storage.thread_rollout_lines WHERE thread_id=$1::uuid ORDER BY position").bind(id.to_string()).fetch_all(&mut *connection).await.map_err(internal)?;
    rows.into_iter()
        .map(|(ordinal, line)| {
            Ok((
                ordinal.map(u64::try_from).transpose().map_err(internal)?,
                line,
            ))
        })
        .collect()
}
