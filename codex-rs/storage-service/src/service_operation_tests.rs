//! Actual service Activate(Active) dispatch, no SQL/network or replacement task.
use super::*;
use crate::OperationState;
use crate::PlanAction;
use crate::StorageServiceInputs;
use codex_keyring_store::tests::MockKeyringStore;
use codex_state::SqliteConfig;
use codex_utils_absolute_path::test_support::PathExt;
use std::time::Duration;
type ControlResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn service(home: &std::path::Path) -> StorageService {
    StorageService::new(StorageServiceInputs {
        codex_home: home.to_path_buf(),
        sqlite: SqliteConfig::new_for_testing(home.abs()),
        candidate: None,
        default_model_provider_id: "original-operation-control".into(),
        host_label: "control".into(),
        keyring: Arc::new(MockKeyringStore::default()),
    })
}
fn active(service: &StorageService) -> ControlResult<Uuid> {
    let id = Uuid::new_v4();
    service.journal.create(&OperationRecord {
        operation_id: id,
        action: PlanAction::Migrate,
        plan_digest: "control".into(),
        state: OperationState::Active,
        run_id: None,
        return_source: None,
        created_at_ms: 0,
        updated_at_ms: 0,
        blocker: None,
        copied: Vec::new(),
    })?;
    Ok(id)
}
#[tokio::test]
async fn thirty_two_canceled_waiters_reap_actual_successful_service_tasks() -> ControlResult<()> {
    let home = tempfile::tempdir()?;
    let service = service(home.path());
    for _ in 0..32 {
        let id = active(&service)?;
        let probe = Arc::new(Probe::default());
        *service
            .original_operations
            .probe
            .lock()
            .map_err(|_| "probe poisoned")? = Some(Arc::clone(&probe));
        let original = service.original_operations.capture(
            service.operation_context(),
            Kind::Activate,
            Some(id),
            None,
        )?;
        let entered = tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()).await;
        let task_id = original
            .state
            .lock()
            .map(|state| state.task.as_ref().map(JoinHandle::id))
            .map_err(|_| "task observation poisoned");
        let mut waiter = Box::pin(service.original_operations.finish(Arc::clone(&original)));
        let early = tokio::select! { biased;
            result = &mut waiter => { let _ = result; true },
            _ = tokio::task::yield_now() => false,
        };
        drop(waiter);
        let restored = original
            .state
            .lock()
            .map(|state| state.task.as_ref().map(JoinHandle::id))
            .map_err(|_| "restored observation poisoned");
        probe.release.notify_one(); // Always release same real production body.
        let returned =
            tokio::time::timeout(Duration::from_secs(5), probe.returned.notified()).await;
        let reaped = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let done = {
                    let mut entries = service
                        .original_operations
                        .originals
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let mut history = service
                        .original_operations
                        .history
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    Operations::reap_ready(&mut entries, &mut history); // SAME admission producer.
                    !entries.iter().any(|entry| Arc::ptr_eq(entry, &original))
                };
                if done {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        // Even regression branches join the same original before diagnostics.
        let result = service
            .original_operations
            .finish(Arc::clone(&original))
            .await;
        if entered.is_err()
            || returned.is_err()
            || reaped.is_err()
            || early
            || task_id? != restored?
            || !matches!(result, Ok(Output::Record(record)) if record.operation_id == id)
        {
            return Err("actual canceled-success service ownership failed".into());
        }
    }
    Ok(())
}
#[tokio::test]
async fn last_external_service_drop_preserves_pending_until_actual_join() -> ControlResult<()> {
    let home = tempfile::tempdir()?;
    let service = service(home.path());
    let id = active(&service)?;
    let probe = Arc::new(Probe::default());
    *service
        .original_operations
        .probe
        .lock()
        .map_err(|_| "probe poisoned")? = Some(Arc::clone(&probe));
    let registry = Arc::clone(&service.original_operations);
    let weak = Arc::downgrade(&registry);
    let original = registry.capture(service.operation_context(), Kind::Activate, Some(id), None)?;
    let entered = tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()).await;
    drop(service);
    drop(registry);
    probe.release.notify_one();
    let returned = tokio::time::timeout(Duration::from_secs(5), probe.returned.notified()).await;
    // Actual body explicitly dropped its service context BEFORE returned notification.
    let retained = weak.upgrade();
    let witnessed = retained.is_some();
    // Diagnostic fallback still joins SAME original if pending hold regressed.
    let cleanup_registry = retained.unwrap_or_else(|| Arc::new(Operations::default()));
    let result = cleanup_registry.finish(Arc::clone(&original)).await;
    drop(cleanup_registry);
    let released = weak.upgrade().is_none();
    if entered.is_err()
        || returned.is_err()
        || !witnessed
        || !released
        || !matches!(result, Ok(Output::Record(record)) if record.operation_id == id)
    {
        return Err("last-owner original operation retention failed".into());
    }
    Ok(())
}

// Filesystem/actual command-dispatch archive proof, not SDK/SQL reuse qualification.
#[tokio::test]
async fn actual_failed_command_is_archived_before_same_key_fresh_success() -> ControlResult<()> {
    let home = tempfile::tempdir()?;
    let service = service(home.path());
    let id = active(&service)?;
    let mut record = service.operation(id)?;
    record.state = OperationState::Planned;
    service.journal.update(&record)?;
    let first = service.original_operations.capture(
        service.operation_context(),
        Kind::Activate,
        Some(id),
        None,
    )?;
    let first_result = service.original_operations.finish(Arc::clone(&first)).await;
    // Original body and cleanup are already physically joined before diagnostics.
    let archived = {
        let history = service
            .original_operations
            .history
            .lock()
            .map_err(|_| "history poisoned")?;
        history.iter().any(|original| Arc::ptr_eq(original, &first))
    };
    record.state = OperationState::Active;
    service.journal.update(&record)?;
    let second = service.original_operations.capture(
        service.operation_context(),
        Kind::Activate,
        Some(id),
        None,
    )?;
    let distinct = !Arc::ptr_eq(&first, &second);
    let second_result = service
        .original_operations
        .finish(Arc::clone(&second))
        .await;
    let original_failure_retained = {
        let state = first.state.lock().map_err(|_| "original result poisoned")?;
        matches!(
            state.result,
            Some(Err(StorageError(BlockerCode::OperationConflict)))
        )
    };
    if !matches!(
        first_result,
        Err(StorageError(BlockerCode::OperationConflict))
    ) || !archived
        || !distinct
        || !original_failure_retained
        || !matches!(second_result, Ok(Output::Record(record)) if record.operation_id == id)
    {
        return Err("original failure archive or fresh same-key retry failed".into());
    }
    Ok(())
}

fn sdk_service(
    home: &std::path::Path,
    state: &std::path::Path,
    namespace: &str,
    prefix: &str,
) -> ControlResult<StorageService> {
    use codex_keyring_store::KeyringStore;
    let receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("receipt.json"))?)?;
    let port = receipt["port"]
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .ok_or("fixture port unavailable")?;
    let keyring = MockKeyringStore::default();
    for (id, role) in [("sdk-runtime", "runtime"), ("sdk-migrator", "migrator")] {
        let password =
            std::fs::read_to_string(state.join(format!("secrets/{prefix}{role}.password")))?;
        keyring.save("codex-postgres-storage", id, password.trim())?;
    }
    let candidate = serde_json::from_value(serde_json::json!({
        "endpoint":"localhost", "port":port, "database":"codex", "namespace":namespace,
        "credential":{"source":"keyring","id":"sdk-runtime"},
        "migrator_credential":{"source":"keyring","id":"sdk-migrator"},
        "tls":{"verification":"verify_full","ca_certificate":state.join("secrets/ca.crt")},
        "connect_timeout_seconds":5,"pool_acquire_timeout_seconds":5,"max_connections":1
    }))?;
    Ok(StorageService::new(StorageServiceInputs {
        codex_home: home.to_path_buf(),
        sqlite: SqliteConfig::new_for_testing(home.abs()),
        candidate: Some(candidate),
        default_model_provider_id: "sdk-lifetime-control".into(),
        host_label: "control".into(),
        keyring: Arc::new(keyring),
    }))
}
// SDK factories/lifetime only: actual copy/cutover and all-eight journal workflows
// still require separate qualification. ROOT must hold admitted protected fixtures.
async fn selected_thirty_two_sdk_cycles(
    state: &std::path::Path,
    namespace: &str,
    prefix: &str,
) -> ControlResult<()> {
    let home = tempfile::tempdir()?.keep(); // Never delete failed original evidence.
    let service = sdk_service(&home, state, namespace, prefix)?; // SAME service for all cycles.
    for _ in 0..32 {
        let id = active(&service)?;
        let probe = Arc::new(Probe {
            exercise_sdk: true,
            ..Probe::default()
        });
        *service
            .original_operations
            .probe
            .lock()
            .map_err(|_| "probe poisoned")? = Some(Arc::clone(&probe));
        let original = service.original_operations.capture(
            service.operation_context(),
            Kind::Activate,
            Some(id),
            None,
        )?;
        let entered = tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()).await;
        let original_id = original
            .state
            .lock()
            .map(|state| state.task.as_ref().map(JoinHandle::id))
            .map_err(|_| "original task observation poisoned");
        let mut waiter = Box::pin(service.original_operations.finish(Arc::clone(&original)));
        let early = tokio::select! { biased; result = &mut waiter => { let _ = result; true }, _ = tokio::task::yield_now() => false };
        drop(waiter); // Cancel caller, never the actual SQL/cleanup production task.
        let restored = original
            .state
            .lock()
            .map(|state| state.task.as_ref().map(JoinHandle::id))
            .map_err(|_| "restored observation poisoned");
        probe.release.notify_one();
        let returned =
            tokio::time::timeout(Duration::from_secs(20), probe.returned.notified()).await;
        // Timeout is an observation: reconcile SAME original task before verdict.
        let result = service
            .original_operations
            .finish(Arc::clone(&original))
            .await;
        let snapshot = original
            .resources
            .actual_runtime_sdk_snapshot()
            .map_err(|_| "original SDK snapshot poisoned");
        let mut pool_count = 0;
        let mut every_pool_closed = true;
        if let Ok(snapshot) = snapshot.as_ref() {
            for pool in snapshot.iter().flatten() {
                pool_count += 1;
                // Release an unexpectedly returned connection before any diagnostics.
                every_pool_closed &= match pool.acquire().await {
                    Err(codex_postgres_runtime::PoolError::Closed) => true,
                    Ok(connection) => {
                        drop(connection);
                        false
                    }
                    Err(_) => false,
                };
            }
        }
        let startups = service.migration_startups.original_startups();
        if entered.is_err()
            || returned.is_err()
            || early
            || original_id? != restored?
            || snapshot.is_err()
            || pool_count != 1
            || !every_pool_closed
            || !original.resources.cleanup_completed()
            || !startups?.is_empty()
            || !matches!(result, Ok(Output::Record(record)) if record.operation_id == id)
        {
            return Err("same-service actual SDK lifetime/reuse/canceled caller failed".into());
        }
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires ROOT retained execution host, fresh admitted default SQL fixture and clock/resources"]
async fn real_default_same_service_thirty_two_sdk_canceled_success_cycles() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE")?;
    selected_thirty_two_sdk_cycles(std::path::Path::new(&state), "codex_storage", "").await
}
#[tokio::test]
#[ignore = "requires ROOT retained execution host, fresh admitted named SQL fixture and clock/resources"]
async fn real_named_same_service_thirty_two_sdk_canceled_success_cycles() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE")?;
    selected_thirty_two_sdk_cycles(
        std::path::Path::new(&state),
        "codex_storage_isolation",
        "isolation_",
    )
    .await
}

// Actual registered task fault AFTER both original SDK factories, never replacement tasks.
async fn selected_sdk_task_faults(
    state: &std::path::Path,
    namespace: &str,
    prefix: &str,
) -> ControlResult<()> {
    let home = tempfile::tempdir()?.keep();
    let service = sdk_service(&home, state, namespace, prefix)?;
    for (aborted, poison) in [(false, false), (true, false), (false, true)] {
        let id = active(&service)?;
        let probe = Arc::new(Probe {
            exercise_sdk: true,
            fail_after_sdk: true,
            ..Probe::default()
        });
        *service
            .original_operations
            .probe
            .lock()
            .map_err(|_| "probe poisoned")? = Some(Arc::clone(&probe));
        let original = service.original_operations.capture(
            service.operation_context(),
            Kind::Activate,
            Some(id),
            None,
        )?;
        probe.release.notify_one();
        let ready = tokio::time::timeout(Duration::from_secs(20), probe.sdk_ready.notified()).await;
        let task_id = {
            let state = original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let task_id = state.task.as_ref().map(JoinHandle::id);
            if aborted || ready.is_err() {
                if let Some(task) = state.task.as_ref() {
                    task.abort();
                }
            }
            task_id
        };
        if poison {
            let retained = Arc::clone(&original);
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _state = retained
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                std::panic::resume_unwind(Box::new("original state poison control"));
            }));
        }
        probe.sdk_release.notify_one(); // Always release original body before diagnostics.
        // No subsequent RPC/finish drives this cleanup: the registered reaper must
        // join the original task and close its SDK resources automatically.
        let automatic = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let joined = {
                    original
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .failure
                        .is_some()
                };
                if joined && original.resources.cleanup_completed() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        let mut finish = Box::pin(service.original_operations.finish(Arc::clone(&original)));
        let timed = tokio::time::timeout(Duration::from_secs(20), &mut finish).await;
        let (late, result) = match timed {
            Ok(result) => (false, result),
            Err(_) => (true, finish.await), // SAME retained future/handles, no orphan continuation.
        };
        let (same_failure, no_task) = {
            let state = original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.failure.as_ref().is_some_and(|cause| {
                    Some(cause.id()) == task_id
                        && if aborted {
                            cause.is_cancelled()
                        } else {
                            cause.is_panic()
                        }
                }),
                state.task.is_none(),
            )
        };
        let snapshot = original.resources.actual_runtime_sdk_snapshot();
        let mut closed = true;
        let mut count = 0;
        if let Ok(pools) = snapshot.as_ref() {
            for pool in pools.iter().flatten() {
                count += 1;
                closed &= match pool.acquire().await {
                    Err(codex_postgres_runtime::PoolError::Closed) => true,
                    Ok(connection) => {
                        drop(connection);
                        false
                    }
                    Err(_) => false,
                };
            }
        }
        let archived = service
            .original_operations
            .history
            .lock()
            .map(|history| history.iter().any(|saved| Arc::ptr_eq(saved, &original)))
            .unwrap_or(false);
        let retained = service
            .original_operations
            .originals
            .lock()
            .map(|entries| entries.iter().any(|saved| Arc::ptr_eq(saved, &original)))
            .unwrap_or(false);
        if ready.is_err()
            || automatic.is_err()
            || late
            || task_id.is_none()
            || !same_failure
            || !no_task
            || snapshot.is_err()
            || count != 1
            || !closed
            || if poison {
                !retained || archived
            } else {
                !archived
            }
            || !original.resources.cleanup_completed()
            || result.is_ok()
        {
            return Err("same original SDK/task panic abort poison cleanup failed".into());
        }
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires ROOT admitted default SQL fixture and original execution host"]
async fn real_default_original_sdk_task_panic_abort_poison_cleanup() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE")?;
    selected_sdk_task_faults(std::path::Path::new(&state), "codex_storage", "").await
}
#[tokio::test]
#[ignore = "requires ROOT admitted named SQL fixture and original execution host"]
async fn real_named_original_sdk_task_panic_abort_poison_cleanup() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE")?;
    selected_sdk_task_faults(
        std::path::Path::new(&state),
        "codex_storage_isolation",
        "isolation_",
    )
    .await
}

#[tokio::test]
async fn missing_original_reaper_handle_never_mints_terminal_ack() -> ControlResult<()> {
    let home = tempfile::tempdir()?.keep();
    let service = service(&home);
    let id = active(&service)?;
    let probe = Arc::new(Probe::default());
    *service
        .original_operations
        .probe
        .lock()
        .map_err(|_| "probe poisoned")? = Some(Arc::clone(&probe));
    let original = service.original_operations.capture(
        service.operation_context(),
        Kind::Activate,
        Some(id),
        None,
    )?;
    let (handle, task_id) = {
        let mut state = original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let handle = state.reaper.take();
        let task_id = handle.as_ref().map(JoinHandle::id);
        (handle, task_id)
    };
    probe.release.notify_one();
    let refused = service
        .original_operations
        .finish(Arc::clone(&original))
        .await
        .is_err();
    // Retain and physically join the SAME temporarily removed original handle,
    // then cache its actual outcome before diagnostics or final registry cleanup.
    let actual = if let Some(handle) = handle {
        Some(handle.await)
    } else {
        None
    };
    {
        let mut state = original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(actual) = actual {
            state.reaper_terminal = true;
            if let Err(cause) = actual {
                state.reaper_failure = Some(cause);
            }
        }
    }
    let reconciled = service
        .original_operations
        .finish(Arc::clone(&original))
        .await;
    if !refused
        || task_id.is_none()
        || reconciled.is_err()
        || !original.resources.cleanup_completed()
    {
        return Err("missing original reaper handle accepted without physical proof".into());
    }
    Ok(())
}

// Fresh production capture must reclaim actual ready handles, without later per-RPC finish.
async fn selected_thirty_two_automatic_sdk_cycles(
    state: &std::path::Path,
    namespace: &str,
    prefix: &str,
) -> ControlResult<()> {
    let home = tempfile::tempdir()?.keep();
    let service = sdk_service(&home, state, namespace, prefix)?;
    let mut originals: Vec<Arc<Original>> = Vec::new();
    originals.try_reserve(32)?;
    let verdict: ControlResult<()> = async {
        for _ in 0..32 {
            let id = active(&service)?;
            let probe = Arc::new(Probe { exercise_sdk: true, ..Probe::default() });
            *service.original_operations.probe.lock().map_err(|_| "probe poisoned")? = Some(Arc::clone(&probe));
            let original = service.original_operations.capture(service.operation_context(), Kind::Activate, Some(id), None)?;
            originals.push(Arc::clone(&original)); // Before any fallible observation/await.
            let entered = tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()).await;
            let mut caller = Box::pin(service.original_operations.finish(Arc::clone(&original)));
            let early = tokio::select! { biased; _ = &mut caller => true, _ = tokio::task::yield_now() => false };
            drop(caller); // No replacement command or per-operation finish drives progress.
            probe.release.notify_one();
            let automatic = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let ready_candidate = {
                        let state = original.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        state.task.is_none() && state.result.is_some() && state.failure.is_none() &&
                            state.reaper.as_ref().is_some_and(JoinHandle::is_finished)
                    };
                    // is_finished is only a scheduling candidate. NEXT actual capture
                    // must physically poll this SAME handle Ready before reclamation.
                    if ready_candidate && original.resources.cleanup_completed() { break; }
                    tokio::task::yield_now().await;
                }
            }).await;
            if entered.is_err() || early || automatic.is_err() { return Err("automatic production SDK cycle did not complete".into()); }
            if originals.len() > 1 {
                let previous = &originals[originals.len() - 2];
                let physically_reaped = {
                    let state = previous.state.lock().map_err(|_| "previous state poisoned")?;
                    state.reaper_terminal && state.reaper.is_none() && state.reaper_failure.is_none()
                };
                if !physically_reaped { return Err("fresh capture did not physically reap same original cleanup task".into()); }
            }
        }
        Ok(())
    }.await;
    // Reconcile every SAME retained original before ALL verdicts, including setup,
    // capacity, candidate-deadline or unexpected-command failures.
    let mut cleanup_ok = true;
    let mut every_sdk_closed = true;
    for original in &originals {
        let outcome = service
            .original_operations
            .finish(Arc::clone(original))
            .await;
        cleanup_ok &= outcome.is_ok() && original.resources.cleanup_completed();
        match original.resources.actual_runtime_sdk_snapshot() {
            Ok(pools) => {
                for pool in pools.iter().flatten() {
                    every_sdk_closed &= match pool.acquire().await {
                        Err(codex_postgres_runtime::PoolError::Closed) => true,
                        Ok(connection) => {
                            drop(connection);
                            false
                        }
                        Err(_) => false,
                    };
                }
            }
            Err(_) => every_sdk_closed = false,
        }
    }
    verdict?;
    if originals.len() != 32 || !cleanup_ok || !every_sdk_closed {
        return Err("automatic original SDK lifetime/reuse cleanup failed".into());
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires ROOT admitted default SQL fixture and execution host"]
async fn real_default_thirty_two_canceled_callers_automatic_sdk_reuse() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE")?;
    selected_thirty_two_automatic_sdk_cycles(std::path::Path::new(&state), "codex_storage", "")
        .await
}
#[tokio::test]
#[ignore = "requires ROOT admitted named SQL fixture and execution host"]
async fn real_named_thirty_two_canceled_callers_automatic_sdk_reuse() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE")?;
    selected_thirty_two_automatic_sdk_cycles(
        std::path::Path::new(&state),
        "codex_storage_isolation",
        "isolation_",
    )
    .await
}

async fn selected_concurrent_original_sdk_borrowers(
    state: &std::path::Path,
    namespace: &str,
    prefix: &str,
) -> ControlResult<()> {
    for poison in [false, true] {
        let home = tempfile::tempdir()?.keep();
        let service = sdk_service(&home, state, namespace, prefix)?;
        let mut originals: Vec<Arc<Original>> = Vec::new();
        let mut probes: Vec<Arc<Probe>> = Vec::new();
        let mut original_pools: Vec<Arc<codex_postgres_runtime::PostgresPool>> = Vec::new();
        originals.try_reserve(3)?;
        probes.try_reserve(3)?;
        original_pools.try_reserve(2)?;
        let verdict: ControlResult<()> = async {
            let first_probe = Arc::new(Probe { exercise_sdk: true, hold_after_sdk: true, ..Probe::default() });
            probes.push(Arc::clone(&first_probe));
            *service.original_operations.probe.lock().map_err(|_| "probe poisoned")? = Some(Arc::clone(&first_probe));
            let first = service.original_operations.capture(service.operation_context(), Kind::Activate, Some(active(&service)?), None)?;
            originals.push(Arc::clone(&first)); first_probe.release.notify_one();
            tokio::time::timeout(Duration::from_secs(20), first_probe.sdk_ready.notified()).await?;
            let runtimes = first.resources.actual_runtime_snapshot().map_err(|_| "runtime snapshot refused")?;
            let runtime = runtimes.into_iter().flatten().next().ok_or("actual runtime missing")?;
            original_pools.push(Arc::clone(runtime.pool()));
            let start = service.migration_startups.original_startups()?.into_iter().next().ok_or("actual startup missing")?;
            if !Arc::ptr_eq(start.original_runtime(), runtime.pool()) { return Err("original runtime identity differs".into()); }
            original_pools.push(start.finish_original().await.map_err(|_| "actual startup outcome refused")?);
            let second_probe = Arc::new(Probe { exercise_sdk: true, hold_after_sdk: true, hold_connection: true,
                shared_runtime: Some(Arc::clone(&runtime)), ..Probe::default() });
            probes.push(Arc::clone(&second_probe));
            *service.original_operations.probe.lock().map_err(|_| "probe poisoned")? = Some(Arc::clone(&second_probe));
            let second = service.original_operations.capture(service.operation_context(), Kind::Activate, Some(active(&service)?), None)?;
            originals.push(Arc::clone(&second)); second_probe.release.notify_one();
            tokio::time::timeout(Duration::from_secs(20), second_probe.sdk_ready.notified()).await?;
            // B holds a real checked-out connection from A's SAME migrator pool.
            first_probe.sdk_release.notify_one();
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if service.migration_startups.actual_entry_closing(runtime.pool())? { break; }
                    tokio::task::yield_now().await;
                }
                Ok::<(), StorageError>(())
            }).await??;
            if first.resources.cleanup_completed() { return Err("SDK closed while admitted original borrower still active".into()); }
            if poison { service.migration_startups.poison_actual_registry(); }
            let late_probe = Arc::new(Probe { exercise_sdk: true, shared_runtime: Some(Arc::clone(&runtime)), ..Probe::default() });
            probes.push(Arc::clone(&late_probe));
            *service.original_operations.probe.lock().map_err(|_| "probe poisoned")? = Some(Arc::clone(&late_probe));
            let late = service.original_operations.capture(service.operation_context(), Kind::Activate, Some(active(&service)?), None)?;
            originals.push(Arc::clone(&late)); late_probe.release.notify_one();
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if late.resources.body_has_returned().map_err(|_| "late body observation refused")? { break; }
                    tokio::task::yield_now().await;
                }
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }).await??;
            if !late.resources.actual_body_failed().map_err(|_| "late body result refused")? ||
                late.resources.cleanup_completed() || first.resources.cleanup_completed() {
                return Err("late admission granted or shared SDK closed before real borrower returned".into());
            }
            // Cancel only an actual public cleanup waiter while B remains live.
            let mut caller = Box::pin(service.original_operations.finish(Arc::clone(&first)));
            let early = tokio::select! { biased; _ = &mut caller => true, _ = tokio::task::yield_now() => false };
            drop(caller);
            if early { return Err("cleanup completed while same original connection remained borrowed".into()); }
            Ok(())
        }.await;
        // ALL paths release the actual first-poll/body/held-connection gates and
        // reconcile SAME operation+cleanup handles before diagnostics.
        for probe in &probes {
            probe.release.notify_one();
            probe.sdk_release.notify_one();
        }
        let mut outcomes = Vec::new();
        for original in &originals {
            outcomes.push(
                service
                    .original_operations
                    .finish(Arc::clone(original))
                    .await,
            );
        }
        let mut sdk_closed = true;
        for pool in &original_pools {
            sdk_closed &= match pool.acquire().await {
                Err(codex_postgres_runtime::PoolError::Closed) => true,
                Ok(connection) => {
                    drop(connection);
                    false
                }
                Err(_) => false,
            };
        }
        verdict?;
        if outcomes.len() != 3 || !sdk_closed || outcomes[2].is_ok() {
            return Err("actual concurrent/late original SDK ownership cleanup failed".into());
        }
        if poison {
            if outcomes.iter().any(Result::is_ok)
                || service.migration_startups.original_startups().is_ok()
            {
                return Err(
                    "poisoned registry minted successful admission/cleanup diagnostic".into(),
                );
            }
        } else if outcomes[0].is_err()
            || outcomes[1].is_err()
            || originals
                .iter()
                .any(|original| !original.resources.cleanup_completed())
        {
            return Err("admitted originals failed same SDK cleanup or late refusal cleanup remained stranded".into());
        }
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires ROOT admitted default SQL fixture and execution host"]
async fn real_default_concurrent_original_sdk_borrower_late_admission_poison_cleanup()
-> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE")?;
    selected_concurrent_original_sdk_borrowers(std::path::Path::new(&state), "codex_storage", "")
        .await
}
#[tokio::test]
#[ignore = "requires ROOT admitted named SQL fixture and execution host"]
async fn real_named_concurrent_original_sdk_borrower_late_admission_poison_cleanup()
-> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE")?;
    selected_concurrent_original_sdk_borrowers(
        std::path::Path::new(&state),
        "codex_storage_isolation",
        "isolation_",
    )
    .await
}
