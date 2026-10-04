//! Real selected pools; execution requires independently admitted fixtures.
use super::*;
use std::path::Path;
use std::time::Duration;
type ControlResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn settings(state: &Path, role: &str) -> ControlResult<ConnectionSettings> {
    let receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("receipt.json"))?)?;
    let port = receipt["port"]
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .ok_or("fixture port unavailable")?;
    Ok(ConnectionSettings {
        host: "localhost".into(),
        port,
        database: "codex".into(),
        username: format!("codex_{role}"),
        password: std::fs::read_to_string(state.join(format!("secrets/{role}.password")))?
            .trim()
            .to_string()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: crate::PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: 1,
        },
    })
}
async fn selected_close(
    state: &Path,
    prefix: &str,
    namespace: Option<NamedNamespace>,
) -> ControlResult<()> {
    // Resolve secret-bearing settings before any physical pool exists.
    let runtime_settings = settings(state, &format!("{prefix}runtime"))?;
    let migration_settings = settings(state, &format!("{prefix}migrator"))?;
    let runtime =
        Arc::new(PostgresPool::connect_in_namespace(runtime_settings, namespace.as_ref()).await?);
    let mut captured = None;
    let setup = async {
        runtime.bind_current_activation().await?;
        let original = match OriginalMigrationStartup::prepare(
            Arc::clone(&runtime),
            migration_settings,
            namespace.as_ref(),
        ) {
            Ok(original) => original,
            Err(error) => {
                captured = Some(Arc::clone(&error.original));
                return Err(error.into());
            }
        };
        captured = Some(Arc::clone(&original));
        original.start_original()?;
        let pool = original.finish_original().await?;
        let held = pool.acquire().await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((original, pool, held))
    }
    .await;
    let (original, pool, held) = match setup {
        Ok(resources) => resources,
        Err(primary) => {
            if let Some(original) = captured.as_ref() {
                let cleanup = original.close_original().await;
                let (settled, has_pool) = {
                    let state = original
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    (
                        (!state.started || state.terminal) && state.join_failure.is_none(),
                        state.pool.is_some(),
                    )
                };
                let closed = {
                    let close = original
                        .close
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    !has_pool || (close.sdk_done && close.joined && close.failure.is_none())
                };
                if !settled || !closed {
                    let retained = (&runtime, &captured, primary, cleanup);
                    std::future::pending::<()>().await;
                    std::hint::black_box(&retained);
                    return Err("unreachable setup uncertainty hold".into());
                }
            }
            runtime.pool.close().await;
            return Err(primary);
        }
    };
    let mut closing = Box::pin(original.close_original());
    let mut early = false;
    let witnessed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! { biased;
                result = &mut closing => { let _ = result; early = true; return false; },
                _ = tokio::task::yield_now() => {
                    if pool.pool.is_closed() { return true; }
                }
            }
        }
    })
    .await;
    // Read exact installed production task while caller owns its loan.
    drop(closing); // Cancel caller ONLY; production close continues in external owner.
    let restored = original
        .close
        .lock()
        .map(|state| {
            (
                state.task.as_ref().map(JoinHandle::id),
                state.sdk_done,
                state.joined,
            )
        })
        .map_err(|_| "close observation poisoned");
    let original_id = original
        .close
        .lock()
        .map(|state| state.original_task_id)
        .map_err(|_| "close identity observation poisoned");
    let acquisition = match pool.acquire().await {
        Ok(unexpected) => {
            drop(unexpected);
            Err("closed pool unexpectedly acquired")
        }
        Err(cause) => Ok(cause),
    };
    // Always release actual checked-out connection before same original reconciliation.
    drop(held);
    let mut retry = Box::pin(original.close_original());
    let (result, late) = match tokio::time::timeout(Duration::from_secs(15), &mut retry).await {
        Ok(result) => (result, false),
        Err(_) => ((&mut retry).await, true),
    };
    drop(retry);
    let receipt = match result {
        Ok(receipt) => receipt,
        Err(error) => {
            let retained = (&original, &pool, &runtime, error);
            std::future::pending::<()>().await;
            std::hint::black_box(&retained);
            return Err("unreachable same-original cleanup hold".into());
        }
    };
    runtime.pool.close().await;
    let restored = restored.map_err(|_| "close state poisoned after cleanup")?;
    let original_id = original_id.map_err(|_| "close task identity poisoned after cleanup")?;
    let state = original
        .close
        .lock()
        .map_err(|_| "close state poisoned after cleanup")?;
    if !matches!(witnessed, Ok(true))
        || early
        || late
        || restored.0.is_none()
        || restored.0 != original_id
        || restored.1
        || restored.2
        || !matches!(acquisition, Ok(PoolError::Closed))
        || !Arc::ptr_eq(receipt.original(), &original)
        || state.task.is_some()
        || !state.sdk_done
        || !state.joined
        || state.failure.is_some()
    {
        return Err("held connection/canceled caller original SDK close control failed".into());
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires admitted default fixture and retained original execution host"]
async fn real_default_checked_out_connection_canceled_close() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE")?;
    selected_close(Path::new(&state), "", None).await
}
#[tokio::test]
#[ignore = "requires admitted named fixture and retained original execution host"]
async fn real_named_checked_out_connection_canceled_close() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE")?;
    let namespace =
        NamedNamespace::new("codex_storage_isolation").map_err(|_| "named namespace rejected")?;
    selected_close(Path::new(&state), "isolation_", Some(namespace)).await
}
