// Both faults run inside the actual retained production mutation task, after its SQL.
#[tokio::test]
#[ignore = "requires admitted current default PostgreSQL fixture and retained host"]
async fn actual_original_mutation_panic_and_cancel_restore_session_and_staged_bytes()
-> ThreadStoreResult<()> {
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
    let writer = PostgresThreadStore::new(Arc::clone(&pool), "original-task-control".to_string());
    for panic in [true, false] {
        let id = ThreadId::new();
        writer
            .create_thread(create_params(id, "original-task-control", &cwd))
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
            .inject_original_mutation_fault(panic, ready);
        let mut append = writer.append_items(AppendThreadItemsParams {
            thread_id: id,
            items: vec![user_message("original task must roll back")],
        });
        let witnessed = tokio::select! {
            biased;
            ready = tokio::time::timeout(Duration::from_secs(10), observed) => ready.map_err(internal).and_then(|ready| ready.map_err(internal)),
            result = &mut append => result.and(Err(internal("append returned before actual mutation fault readiness"))),
        };
        drop(append); // Only the external caller: SAME mutation task remains store-owned.
        let (original_id, staged) = {
            let original = original.lock().await;
            (
                original.original_mutation_task_id(),
                original.original_staged_lines().map(<[_]>::to_vec),
            )
        };
        if !panic || witnessed.is_err() {
            // Even failure paths abort/join the actual task, never a replacement helper.
            original.lock().await.abort_original_mutation_task();
        }
        let retained = writer.pending_writers.retains_mutation_frame(&original);
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
        let (same_cause, same_staged) = {
            let original = original.lock().await;
            let error = original.reconciled_original_join_error();
            (
                error.is_some_and(|error| {
                    Some(error.id()) == original_id
                        && if panic {
                            error.is_panic()
                        } else {
                            error.is_cancelled()
                        }
                }),
                original.original_staged_lines().map(<[_]>::to_vec) == staged,
            )
        };
        let archived = writer.pending_writers.retains_mutation_frame(&original)?;
        let removed = matches!(
            writer.live_thread(id).await,
            Err(ThreadStoreError::ThreadNotFound)
        );
        let mut connection = pool.acquire().await.held().await;
        let returned_pid: i32 = sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await
            .held()
            .await;
        let released: bool = sqlx::query_scalar(
            "SELECT owner_id IS NULL AND lease_until IS NULL FROM codex_storage.thread_writer_ownership WHERE thread_id=$1::uuid")
            .bind(id.to_string()).fetch_one(&mut *connection).await.held().await;
        if late
            || witnessed? != expected_pid
            || returned_pid != expected_pid
            || !retained?
            || !same_cause
            || !same_staged
            || staged.is_none()
            || !archived
            || !removed
            || !released
            || before != after
        {
            return Err(internal(
                "actual original mutation task/session/staged-byte recovery failed",
            ));
        }
    }
    Ok(())
}
async fn original_task_snapshot(pool: &PostgresPool, id: ThreadId) -> ThreadStoreResult<Value> {
    let mut connection = pool.acquire().await.map_err(internal)?;
    let snapshot: String = sqlx::query_scalar(
        "SELECT json_build_object('catalog',(SELECT to_jsonb(t) FROM codex_storage.threads t WHERE id=$1::uuid), \
         'rollout',(SELECT json_agg(to_jsonb(r) ORDER BY position) FROM codex_storage.thread_rollout_lines r WHERE thread_id=$1::uuid))::text")
        .bind(id.to_string()).fetch_one(&mut *connection).await.map_err(internal)?;
    serde_json::from_str(&snapshot).map_err(internal)
}
