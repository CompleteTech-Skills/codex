//! Actual public futures, not a replacement SDK/task factory.
use super::tests::ControlResult;
use super::tests::sdk_service;
use super::*;
use std::time::Duration;
#[derive(Clone)]
enum PublicValue {
    Probe(crate::ConnectionReport),
    Schema(i32),
}
async fn selected_public_canceled_caller(
    state: &std::path::Path,
    namespace: &str,
    prefix: &str,
    bootstrap: bool,
) -> ControlResult<()> {
    let home = tempfile::tempdir()?.keep();
    let service = sdk_service(&home, state, namespace, prefix)?;
    let probe = Arc::new(Probe {
        require_empty_bootstrap: bootstrap,
        ..Probe::default()
    });
    *service
        .original_operations
        .probe
        .lock()
        .map_err(|_| "probe poisoned")? = Some(Arc::clone(&probe));
    let kind = if bootstrap { Kind::Schema } else { Kind::Probe };
    let mut caller = Box::pin(async {
        if bootstrap {
            service.initialize_schema().await.map(PublicValue::Schema)
        } else {
            Ok(PublicValue::Probe(service.check_connection().await))
        }
    });
    // Poll the ACTUAL public caller far enough to register its actual task.
    let entered = tokio::select! { biased;
        _ = &mut caller => false,
        entered = tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()) => entered.is_ok(),
    };
    let original = {
        let originals = service
            .original_operations
            .originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        originals
            .iter()
            .find(|original| original.kind == kind)
            .map(Arc::clone)
    };
    let Some(original) = original else {
        // Unknown original dispatch is not safe to classify or dispose. Keep the
        // actual pinned caller/service/host frame alive for ROOT reconciliation.
        probe.release.notify_one();
        eprintln!("public route original task missing; retaining unresolved caller host");
        let retained = (&mut caller, &service);
        let result = std::future::pending::<ControlResult<()>>().await;
        let _ = retained;
        return result;
    };
    drop(caller); // Cancel caller only; same operation task remains in its registry.
    let (original_id, restored_id) = {
        let state = original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.operation_task_id,
            state.task.as_ref().map(JoinHandle::id),
        )
    };
    probe.release.notify_one();
    let mut finish = Box::pin(service.original_operations.finish(Arc::clone(&original)));
    let observed = tokio::time::timeout(Duration::from_secs(30), &mut finish).await;
    let (late, outcome) = match observed {
        Ok(outcome) => (false, outcome),
        Err(_) => (true, finish.await), // Same original continuation/handles retained.
    };
    let pools = original.resources.actual_runtime_sdk_snapshot();
    let mut closed = true;
    let mut count = 0;
    if let Ok(pools) = pools.as_ref() {
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
    let native_binding = if bootstrap {
        original
            .resources
            .actual_bootstrap_sessions_snapshot()
            .is_ok_and(|owners| {
                let actual = owners.iter().flatten().collect::<Vec<_>>();
                actual.len() == 1
                    && actual[0].original_control_privilege() == Some(true)
                    && actual[0]
                        .original_native_status()
                        .is_some_and(|(_, status)| status.as_deref() == Some("committed"))
                    && actual[0].diagnostic_pool().is_some_and(|diagnostic| {
                        !Arc::ptr_eq(actual[0].original_pool(), diagnostic)
                            && pools.as_ref().is_ok_and(|pools| {
                                pools
                                    .iter()
                                    .flatten()
                                    .any(|pool| Arc::ptr_eq(pool, actual[0].original_pool()))
                                    && pools
                                        .iter()
                                        .flatten()
                                        .any(|pool| Arc::ptr_eq(pool, diagnostic))
                            })
                    })
            })
    } else {
        true
    };
    // Actual result projection is read only AFTER original task+SDK cleanup.
    let value = match outcome {
        Ok(Output::Observed(value)) if bootstrap => value
            .downcast_ref::<i32>()
            .copied()
            .map(PublicValue::Schema),
        Ok(Output::Observed(value)) => value
            .downcast_ref::<crate::ConnectionReport>()
            .cloned()
            .map(PublicValue::Probe),
        _ => None,
    };
    let valid = match value {
        Some(PublicValue::Schema(format)) => {
            format == codex_postgres_runtime::client_schema_format()
        }
        Some(PublicValue::Probe(report)) => {
            report.blocker.is_none()
                && report.stage == crate::CheckStage::Ready
                && report.schema_format == Some(codex_postgres_runtime::client_schema_format())
        }
        None => false,
    };
    if !entered
        || late
        || original_id.is_none()
        || original_id != restored_id
        || pools.is_err()
        || count != if bootstrap { 2 } else { 1 }
        || !native_binding
        || !closed
        || !original.resources.cleanup_completed()
        || !valid
    {
        return Err("actual public caller/constructor SDK ownership failed".into());
    }
    if bootstrap {
        {
            let mut configured = service
                .original_operations
                .probe
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *configured = None;
        }
        // A constant/no-op bootstrap cannot pass: real Runtime constructor must
        // now read the actual canonical metadata in the SAME selected namespace.
        let report = service.check_connection().await;
        if report.blocker.is_some()
            || report.stage != crate::CheckStage::Ready
            || report.schema_format != Some(codex_postgres_runtime::client_schema_format())
        {
            return Err(
                "actual bootstrap metadata not readable through original Runtime route".into(),
            );
        }
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires ROOT admitted current default SQL fixture/retained execution host"]
async fn real_default_public_probe_canceled_original_constructor_cleanup() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE")?;
    selected_public_canceled_caller(std::path::Path::new(&state), "codex_storage", "", false).await
}
#[tokio::test]
#[ignore = "requires ROOT admitted current named SQL fixture/retained execution host"]
async fn real_named_public_probe_canceled_original_constructor_cleanup() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE")?;
    selected_public_canceled_caller(
        std::path::Path::new(&state),
        "codex_storage_isolation",
        "isolation_",
        false,
    )
    .await
}
#[tokio::test]
#[ignore = "requires ROOT fresh preprovisioned EMPTY default namespace, real migrator login and host"]
async fn real_empty_default_public_bootstrap_canceled_original_constructor_cleanup()
-> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_STATE")?;
    selected_public_canceled_caller(std::path::Path::new(&state), "codex_storage", "", true).await
}
#[tokio::test]
#[ignore = "requires ROOT fresh preprovisioned EMPTY named namespace, real migrator login and host"]
async fn real_empty_named_public_bootstrap_canceled_original_constructor_cleanup()
-> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_STATE")?;
    selected_public_canceled_caller(
        std::path::Path::new(&state),
        "codex_storage_isolation",
        "isolation_",
        true,
    )
    .await
}

// Real public plan body and real SQLite refusal, not a substitute factory task.
async fn selected_public_plan_canceled_sqlite_refusal(
    state: &std::path::Path,
    namespace: &str,
    prefix: &str,
) -> ControlResult<()> {
    let home = tempfile::tempdir()?.keep();
    let service = sdk_service(&home, state, namespace, prefix)?;
    // Deliberately valid SQLite with missing production threads table. Retain the
    // actual setup pool before its first acquisition and close it on every result.
    let seed = sqlx::sqlite::SqlitePoolOptions::new()
        .min_connections(0)
        .max_connections(1)
        .connect_lazy_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(service.inputs.sqlite.state_db_path())
                .create_if_missing(true),
        );
    let seeded = sqlx::query("CREATE TABLE unrelated_control (value INTEGER)")
        .execute(&seed)
        .await;
    seed.close().await;
    seeded?;
    let probe = Arc::new(Probe::default());
    *service
        .original_operations
        .probe
        .lock()
        .map_err(|_| "probe poisoned")? = Some(Arc::clone(&probe));
    let mut caller = Box::pin(service.plan(crate::PlanAction::Migrate));
    let entered = tokio::select! { biased;
        _ = &mut caller => false,
        result = tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()) => result.is_ok(),
    };
    let original = {
        let originals = service
            .original_operations
            .originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        originals
            .iter()
            .find(|original| original.kind == Kind::Plan(crate::PlanAction::Migrate))
            .map(Arc::clone)
    };
    let Some(original) = original else {
        probe.release.notify_one();
        let retained = (&mut caller, &service);
        let result = std::future::pending::<ControlResult<()>>().await;
        let _ = retained;
        return result;
    };
    drop(caller);
    let (id, stored) = {
        let state = original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.operation_task_id,
            state.task.as_ref().map(JoinHandle::id),
        )
    };
    // Subsequent real nested Runtime probe must not inherit this entry pause.
    *service
        .original_operations
        .probe
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    probe.release.notify_one();
    let mut finish = Box::pin(service.original_operations.finish(Arc::clone(&original)));
    let (late, result) = match tokio::time::timeout(Duration::from_secs(30), &mut finish).await {
        Ok(result) => (false, result),
        Err(_) => (true, finish.await),
    };
    let actual_task_terminal = original
        .state
        .lock()
        .map(|state| state.task.is_none() && (state.result.is_some() || state.failure.is_some()))
        .unwrap_or(false);
    if !actual_task_terminal {
        // A refused observer is not original query-task terminal evidence. Keep
        // the exact source/operation/service owner instead of closing live reads.
        let retained = (&original, &service);
        let result = std::future::pending::<ControlResult<()>>().await;
        let _ = retained;
        return result;
    }
    let sources = original.resources.actual_read_sources();
    let mut source_count = 0;
    let mut genuine_cause = false;
    let mut closed = true;
    if let Ok(sources) = sources.as_ref() {
        for source in sources.iter().flatten() {
            source_count += 1;
            genuine_cause |= source
                .original_query_failure()
                .ok()
                .flatten()
                .is_some_and(|cause| cause.downcast_ref::<sqlx::Error>().is_some());
            // Observe actual admission closure BEFORE any diagnostic cleanup.
            match source.original_read_pools() {
                Ok(pools) => {
                    let mut count = 0;
                    for pool in pools.into_iter().flatten() {
                        count += 1;
                        closed &= match pool.acquire().await {
                            Err(sqlx::Error::PoolClosed) => true,
                            Ok(connection) => {
                                drop(connection);
                                false
                            }
                            Err(_) => false,
                        };
                    }
                    closed &= count == 1;
                }
                Err(_) => closed = false,
            }
            // All known originals reconciled even when the closure observation fails.
            closed &= source.close_original_read_pools().await.is_ok();
        }
    }
    if !entered
        || late
        || id.is_none()
        || id != stored
        || sources.is_err()
        || source_count != 1
        || !genuine_cause
        || !closed
        || !original.resources.cleanup_completed()
        || !matches!(
            result,
            Err(crate::StorageError(crate::BlockerCode::SourceUnreadable))
        )
    {
        return Err("actual public plan canceled caller/read query cause cleanup failed".into());
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires ROOT current default SQL fixture and admitted SQLite/retained host"]
async fn real_default_public_plan_canceled_original_sqlite_query_refusal() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE")?;
    selected_public_plan_canceled_sqlite_refusal(std::path::Path::new(&state), "codex_storage", "")
        .await
}
#[tokio::test]
#[ignore = "requires ROOT current named SQL fixture and admitted SQLite/retained host"]
async fn real_named_public_plan_canceled_original_sqlite_query_refusal() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE")?;
    selected_public_plan_canceled_sqlite_refusal(
        std::path::Path::new(&state),
        "codex_storage_isolation",
        "isolation_",
    )
    .await
}
