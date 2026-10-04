//! Ignored genuine origin factories; no grants, bootstrap or provisioned roles.
use super::*;
use codex_keyring_store::DefaultKeyringStore;
use std::future::Future;
use std::io::Read;
use std::task::Poll;
use std::time::Duration;
struct Failure {
    original: Option<Arc<OriginalRuntimeOrigin>>,
    constructor: Option<crate::RemoteStorageError>,
    constructor_outcome: Option<Result<Arc<crate::RemoteStorage>, crate::RemoteStorageError>>,
    constructor_join: Option<Arc<tokio::task::JoinError>>,
    query_join: Option<Arc<tokio::task::JoinError>>,
    cleanup: Option<RuntimeOriginRefusal>,
    acquisition: Option<PoolError>,
    fixture: Option<Box<dyn std::error::Error + Send + Sync>>,
    diagnostic: &'static str,
}
impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _originals = (
            &self.original,
            &self.constructor,
            &self.constructor_outcome,
            &self.constructor_join,
            &self.query_join,
            &self.cleanup,
            &self.acquisition,
            &self.fixture,
            self.diagnostic,
        );
        f.write_str("RuntimeOriginControlFailure([originals retained])")
    }
}
fn failure(original: Option<Arc<OriginalRuntimeOrigin>>, diagnostic: &'static str) -> Failure {
    Failure {
        original,
        constructor: None,
        constructor_outcome: None,
        constructor_join: None,
        query_join: None,
        cleanup: None,
        acquisition: None,
        fixture: None,
        diagnostic,
    }
}
fn profile(named: bool) -> Result<Arc<RemotePostgresProfile>, Failure> {
    let variable = if named {
        "CODEX_REAL_RUNTIME_ORIGIN_NAMED_PROFILE"
    } else {
        "CODEX_REAL_RUNTIME_ORIGIN_PROFILE"
    };
    let read = || -> Result<RemotePostgresProfile, Box<dyn std::error::Error + Send + Sync>> {
        let path = std::path::PathBuf::from(
            std::env::var_os(variable).ok_or("missing explicit runtime origin fixture")?,
        );
        if !path.is_absolute() {
            return Err("runtime origin fixture path must be absolute".into());
        }
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(65537)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            return Err("runtime origin fixture exceeds bound".into());
        }
        let profile: RemotePostgresProfile = serde_json::from_slice(&bytes)?;
        if (profile.namespace() != "codex_storage") != named || profile.max_connections() != 1 {
            return Err("fixture must select expected namespace and max-one pool".into());
        }
        Ok(profile)
    };
    read().map(Arc::new).map_err(|cause| {
        let mut failed = failure(None, "fixture refused");
        failed.fixture = Some(cause);
        failed
    })
}
type Capture = Arc<dyn Fn(Arc<PostgresPool>) -> Result<(), PoolError> + Send + Sync>;
fn capture(slot: &Arc<Mutex<Option<Arc<PostgresPool>>>>) -> Capture {
    let slot = Arc::clone(slot);
    Arc::new(move |pool| {
        let (mut state, poison) = match slot.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        *state = Some(pool);
        if poison {
            Err(PoolError::Unavailable)
        } else {
            Ok(())
        }
    })
}
async fn finish_close(
    original: &Arc<OriginalRuntimeOrigin>,
) -> (Result<(), RuntimeOriginRefusal>, bool) {
    let mut same_close = Box::pin(original.close_original());
    match tokio::time::timeout(Duration::from_secs(90), same_close.as_mut()).await {
        Ok(result) => (result, false),
        Err(_) => (same_close.await, true), // Same future; deadline is never ACK.
    }
}
async fn positive(named: bool) -> Result<(), Failure> {
    let original = OriginalRuntimeOrigin::prepare(profile(named)?);
    let sdk = Arc::new(Mutex::new(None));
    let returned = crate::RemoteStorage::connect_with_original_runtime_origin(
        &original,
        Arc::new(DefaultKeyringStore),
        capture(&sdk),
    )
    .await;
    let pool = sdk
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    let native = original.original_native_origin();
    let associated = match (&returned, &pool) {
        (Ok(storage), Some(pool)) => Arc::ptr_eq(storage.pool(), pool),
        _ => false,
    };
    let (cleanup, late) = finish_close(&original).await;
    if !original.known_original_closed() {
        let mut failed = failure(Some(original), "physical closure unresolved");
        failed.constructor = returned.err();
        failed.cleanup = cleanup.err();
        return Err(failed);
    }
    let acquisition = match pool.as_ref() {
        Some(pool) => match pool.acquire().await {
            Ok(connection) => {
                drop(connection);
                None
            }
            Err(error) => Some(error),
        },
        None => None,
    };
    if !associated
        || native.is_none()
        || returned.is_err()
        || cleanup.is_err()
        || late
        || acquisition != Some(PoolError::Closed)
    {
        let mut failed = failure(Some(original), "origin factory/closure verdict");
        failed.constructor = returned.err();
        failed.cleanup = cleanup.err();
        failed.acquisition = acquisition;
        return Err(failed);
    }
    Ok(())
}
async fn canceled_constructor(named: bool) -> Result<(), Failure> {
    let original = OriginalRuntimeOrigin::prepare(profile(named)?);
    let sdk = Arc::new(Mutex::new(None));
    let (ready, witness) = tokio::sync::oneshot::channel();
    let probe = Arc::new(ConstructorProbe {
        ready: Mutex::new(Some(ready)),
        release: Semaphore::new(0),
    });
    original
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .constructor_probe = Some(Arc::clone(&probe));
    let (task_id, early) = {
        let mut caller = Box::pin(crate::RemoteStorage::connect_with_original_runtime_origin(
            &original,
            Arc::new(DefaultKeyringStore),
            capture(&sdk),
        ));
        let observed = tokio::select! {
            witnessed = witness => (witnessed.ok(), None),
            returned = caller.as_mut() => (None, Some(returned)),
        };
        drop(caller); // Actual cancellation of observer restores SAME constructor JH.
        observed
    };
    let restored = original
        .state
        .lock()
        .map(|state| {
            state.constructor_task.as_ref().map(JoinHandle::id) == task_id && task_id.is_some()
        })
        .map_err(|_| "state observation poisoned")
        .unwrap_or(false);
    let mut same_close = Box::pin(original.close_original());
    let first = std::future::poll_fn(|context| {
        Poll::Ready(match same_close.as_mut().poll(context) {
            Poll::Pending => None,
            Poll::Ready(result) => Some(result),
        })
    })
    .await;
    let first_pending = first.is_none();
    let closed_before_release = original.known_original_closed();
    probe.release.add_permits(1); // Unconditional original body release BEFORE verdict.
    let (cleanup, late) = match first {
        Some(result) => (result, false),
        None => match tokio::time::timeout(Duration::from_secs(90), same_close.as_mut()).await {
            Ok(result) => (result, false),
            Err(_) => (same_close.await, true),
        },
    };
    let pool = sdk
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    if !original.known_original_closed() {
        let mut failed = failure(
            Some(Arc::clone(&original)),
            "same constructor/closure unresolved",
        );
        failed.constructor = early.and_then(Result::err);
        failed.cleanup = cleanup.err();
        return Err(failed);
    }
    let acquisition = match pool.as_ref() {
        Some(pool) => match pool.acquire().await {
            Ok(connection) => {
                drop(connection);
                None
            }
            Err(error) => Some(error),
        },
        None => None,
    };
    let (constructor_outcome, constructor_join, query_join, exact_sealed_refusal) = {
        let state = original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let expected = matches!(
            state.constructor_returned.as_ref(),
            Some(Err(crate::RemoteStorageError::Connection(
                PoolError::Unavailable
            )))
        ) && state.constructor_join_failure.is_none()
            && state.join_failure.is_none()
            && state.query.is_none()
            && state.acquisition.is_none()
            && state.runtime_failure.is_none()
            && state.native.is_none()
            && state.row.is_none()
            && !state.dispatched
            && state.sealed
            && state.constructor_dispatched
            && state.constructor_task.is_none()
            && state.task.is_none()
            && state.collision.is_none()
            && !original.state.is_poisoned()
            && !sdk.is_poisoned()
            && state.refused
            && state.rejected_pool.is_none();
        (
            state.constructor_returned.clone(),
            state.constructor_join_failure.clone(),
            state.join_failure.clone(),
            expected,
        )
    };
    let owning_expected_cleanup = cleanup
        .as_ref()
        .err()
        .is_some_and(|refused| Arc::ptr_eq(refused.original_owner(), &original));
    if !restored
        || !first_pending
        || !exact_sealed_refusal
        || !owning_expected_cleanup
        || closed_before_release
        || early.is_some()
        || pool.is_none()
        || late
        || acquisition != Some(PoolError::Closed)
    {
        let mut failed = failure(
            Some(Arc::clone(&original)),
            "constructor cancellation/late capture verdict",
        );
        failed.constructor = early.and_then(Result::err);
        failed.constructor_outcome = constructor_outcome;
        failed.constructor_join = constructor_join;
        failed.query_join = query_join;
        failed.cleanup = cleanup.err();
        failed.acquisition = acquisition;
        return Err(failed);
    }
    // Cleanup may refuse admitted origin because close sealed it before factory;
    // that genuine refusal remains in the retained original, not an origin grant.
    Ok(())
}
#[tokio::test]
#[ignore = "requires isolated default Runtime with explicit native-origin privilege; no provisioning"]
async fn real_default_runtime_origin_max_one() -> Result<(), Failure> {
    positive(false).await
}
#[tokio::test]
#[ignore = "requires isolated named Runtime with explicit native-origin privilege; no provisioning"]
async fn real_named_runtime_origin_max_one() -> Result<(), Failure> {
    positive(true).await
}
#[tokio::test]
#[ignore = "requires explicit default Runtime profile and credential; closes same latent SDK"]
async fn real_default_canceled_constructor_before_capture() -> Result<(), Failure> {
    canceled_constructor(false).await
}
#[tokio::test]
#[ignore = "requires explicit named Runtime profile and credential; closes same latent SDK"]
async fn real_named_canceled_constructor_before_capture() -> Result<(), Failure> {
    canceled_constructor(true).await
}
