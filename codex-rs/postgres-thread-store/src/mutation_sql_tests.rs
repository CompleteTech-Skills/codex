// Actual dedicated default-namespace fixture only; ROOT owns admission and serial execution.
// AFTER INSERT blocks after the real catalog/rollout changes, then raises: retained task
// must finish the original rollback despite cancellation of the external append caller.
#[tokio::test]
#[ignore = "requires separately admitted pristine PostgreSQL fixture and owner DDL lease"]
async fn actual_mutation_rollback_survives_canceled_caller() -> ThreadStoreResult<()> {
    use sqlx::Acquire as _;
    let state = std::env::var("CODEX_TEST_POSTGRES_THREAD_STORE_STATE").map_err(internal)?;
    let state = Path::new(&state);
    let cwd = tempfile::tempdir().map_err(internal)?.keep();
    let observer = connect(state, "runtime").await;
    let owner = connect(state, "migrator").await;
    let mut limits = settings(state, "runtime");
    limits.limits.max_connections = 1;
    let pool = Arc::new(
        PostgresPool::connect_in_namespace(limits, None)
            .await
            .map_err(internal)?,
    );
    pool.bind_current_activation().await.map_err(internal)?;
    let writer = PostgresThreadStore::new(Arc::clone(&pool), "mutation-control".to_string());
    let id = ThreadId::new();
    writer
        .create_thread(create_params(id, "mutation-control", &cwd))
        .await
        .held()
        .await;
    let original = writer.live_thread(id).await.held().await;
    // From here every refusal retains the original writer frame through checked cleanup.
    let prepared: ThreadStoreResult<(Value, i32)> = async {
        writer.persist_thread(id, PersistContext::Standard).await?;
        let mut connection = pool.acquire().await.map_err(internal)?;
        let pid = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection).await.map_err(internal)?;
        let mut read = observer.acquire().await.map_err(internal)?;
        let snapshot: String = sqlx::query_scalar(
            "SELECT json_build_object('catalog',(SELECT to_jsonb(t) FROM codex_storage.threads t WHERE id=$1::uuid), \
             'rollout',(SELECT json_agg(to_jsonb(r) ORDER BY position) FROM codex_storage.thread_rollout_lines r WHERE thread_id=$1::uuid))::text"
        ).bind(id.to_string()).fetch_one(&mut *read).await.map_err(internal)?;
        Ok((serde_json::from_str(&snapshot).map_err(internal)?, pid))
    }.await;
    let (before, writer_pid) = match prepared {
        Ok(value) => value,
        Err(error) => {
            if writer.discard_thread(id).await.is_err() {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
            return Err(error);
        }
    };
    let mut ddl_connection = owner.acquire().await.held().await;
    let mut ddl = ddl_connection.begin().await.held().await;
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *ddl)
        .await
        .held()
        .await;
    // Never adopt preexisting trigger/function definitions in a reused fixture.
    let absent: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='codex_storage' AND p.proname='owned_mutation_rollback_control')")
        .fetch_one(&mut *ddl).await.held().await;
    if !absent {
        ddl.rollback().await.held().await;
        writer.discard_thread(id).await.held().await;
        return Err(internal("dedicated mutation fixture is not pristine"));
    }
    sqlx::query("CREATE FUNCTION codex_storage.owned_mutation_rollback_control() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(732410219::bigint); RAISE EXCEPTION 'owned mutation rollback control'; END; $$")
        .execute(&mut *ddl).await.held().await;
    sqlx::query("CREATE TRIGGER owned_mutation_rollback_control AFTER INSERT ON codex_storage.thread_rollout_lines FOR EACH ROW EXECUTE FUNCTION codex_storage.owned_mutation_rollback_control()")
        .execute(&mut *ddl).await.held().await;
    // The exact owner connection stays held if committed fixture DDL is uncertain.
    ddl.commit().await.held().await;
    let mut barrier_connection = observer.acquire().await.held().await;
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *barrier_connection)
        .await
        .held()
        .await;
    let mut barrier = barrier_connection.begin().await.held().await;
    sqlx::query("SELECT pg_advisory_xact_lock(732410219::bigint)")
        .execute(&mut *barrier)
        .await
        .held()
        .await;
    let blocked = async {
        let mut read = observer.acquire().await.map_err(internal)?;
        loop {
            let actual: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.pid=$1 AND a.xact_start IS NOT NULL AND $2=ANY(pg_blocking_pids(a.pid)) AND EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory' AND NOT l.granted))")
                .bind(writer_pid).bind(blocker_pid).fetch_one(&mut *read).await.map_err(internal)?;
            if actual {
                return Ok::<_, ThreadStoreError>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let mut append = writer.append_items(AppendThreadItemsParams {
        thread_id: id,
        items: vec![user_message("must roll back")],
    });
    let witnessed = tokio::select! {
        result = &mut append => result.and(Err(internal("append completed before actual AFTER-trigger barrier"))),
        result = tokio::time::timeout(Duration::from_secs(5), blocked) => result.map_err(internal).and_then(|value| value),
    };
    // Deliberately cancel only the external caller, not the retained mutation task.
    drop(append);
    let task_id = { original.lock().await.original_mutation_task_id() };
    let retained_before = writer.pending_writers.retains_mutation_frame(&original);
    let still_blocked: ThreadStoreResult<bool> = async {
        let mut read = observer.acquire().await.map_err(internal)?;
        sqlx::query_scalar("SELECT $2=ANY(pg_blocking_pids($1))")
            .bind(writer_pid)
            .bind(blocker_pid)
            .fetch_one(&mut *read)
            .await
            .map_err(internal)
    }
    .await;
    // Rollback the exact barrier even on witness/refusal paths, then reconcile original cleanup.
    let mut release = Box::pin(barrier.rollback());
    let (released, release_late) =
        match tokio::time::timeout(Duration::from_secs(5), release.as_mut()).await {
            Ok(result) => (result, false),
            Err(_) => (release.await, true),
        };
    let mut cleanup = writer.discard_thread(id);
    let (cleaned, cleanup_late) =
        match tokio::time::timeout(Duration::from_secs(10), &mut cleanup).await {
            Ok(result) => (result, false),
            Err(_) => (cleanup.await, true),
        };
    if released.is_err() || cleaned.is_err() {
        loop {
            let _ = (
                &released, &cleaned, &writer, &original, &pool, &observer, &owner,
            );
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
    let mut removal = ddl_connection.begin().await.held().await;
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(&mut *removal)
        .await
        .held()
        .await;
    sqlx::query(
        "DROP TRIGGER owned_mutation_rollback_control ON codex_storage.thread_rollout_lines",
    )
    .execute(&mut *removal)
    .await
    .held()
    .await;
    sqlx::query("DROP FUNCTION codex_storage.owned_mutation_rollback_control()")
        .execute(&mut *removal)
        .await
        .held()
        .await;
    removal.commit().await.held().await;
    let same_task = { original.lock().await.original_mutation_task_id() == task_id };
    let original_cause = { original.lock().await.original_rollback_cause().is_some() };
    let archived = writer.pending_writers.retains_mutation_frame(&original);
    let removed = writer.live_thread(id).await.is_err();
    let mut read = observer.acquire().await.map_err(internal)?;
    let snapshot: String = sqlx::query_scalar("SELECT json_build_object('catalog',(SELECT to_jsonb(t) FROM codex_storage.threads t WHERE id=$1::uuid), 'rollout',(SELECT json_agg(to_jsonb(r) ORDER BY position) FROM codex_storage.thread_rollout_lines r WHERE thread_id=$1::uuid))::text")
        .bind(id.to_string()).fetch_one(&mut *read).await.map_err(internal)?;
    let after: Value = serde_json::from_str(&snapshot).map_err(internal)?;
    // Same max-one original connection acknowledges another command only after original rollback.
    let mut original_connection = pool.acquire().await.map_err(internal)?;
    let same_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *original_connection)
        .await
        .map_err(internal)?;
    drop(original_connection);
    let idle: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND xact_start IS NULL AND state='idle')")
        .bind(writer_pid).fetch_one(&mut *read).await.map_err(internal)?;
    witnessed?;
    if task_id.is_none()
        || !retained_before?
        || !still_blocked.map_err(internal)?
        || !same_task
        || !original_cause
        || !archived?
        || !removed
        || before != after
        || same_pid != writer_pid
        || !idle
        || release_late
        || cleanup_late
    {
        return Err(internal(
            "actual original mutation rollback/ownership control failed",
        ));
    }
    Ok(())
}

// Uncertain setup/removal paths retain this entire caller frame (original writer,
// pools, exact DDL connection and known fixture objects) in the existing test host.
// No exit code, dropped future, or job-kill fallback is a physical cleanup receipt.
trait HeldMutationControl<T> {
    async fn held(self) -> T;
}
impl<T, E> HeldMutationControl<T> for Result<T, E> {
    async fn held(self) -> T {
        match self {
            Ok(value) => value,
            Err(original) => loop {
                let _ = &original;
                tokio::time::sleep(Duration::from_secs(60)).await;
            },
        }
    }
}
