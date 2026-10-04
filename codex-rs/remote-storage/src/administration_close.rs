//! Inactive same-original native/SDK close owner. No remote xid or exclusive proof.
use codex_postgres_runtime::PostgresPool;
use sqlx::Connection;
use sqlx::Postgres;
use sqlx::pool::PoolConnection;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

type NativeFuture = Pin<Box<dyn Future<Output = Result<(), sqlx::Error>> + Send>>;
type NativeResult = Result<(), Arc<sqlx::Error>>;

enum Source {
    Unsealed,
    Absent,
    Captured,
}
struct State {
    source: Source,
    pool: Option<Arc<PostgresPool>>,
    native_future: Option<NativeFuture>,
    native_task: Option<JoinHandle<NativeResult>>,
    native_result: Option<NativeResult>,
    native_joined: bool,
    native_failure: Option<Arc<tokio::task::JoinError>>,
    sdk_task: Option<JoinHandle<()>>,
    sdk_ack: bool,
    sdk_joined: bool,
    sdk_failure: Option<Arc<tokio::task::JoinError>>,
}

pub(super) struct OriginalAdministrativeClose {
    state: Mutex<State>,
    serial: Arc<Semaphore>,
}

impl OriginalAdministrativeClose {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                source: Source::Unsealed,
                pool: None,
                native_future: None,
                native_task: None,
                native_result: None,
                native_joined: false,
                native_failure: None,
                sdk_task: None,
                sdk_ack: false,
                sdk_joined: false,
                sdk_failure: None,
            }),
            serial: Arc::new(Semaphore::new(1)),
        })
    }

    /// Only the owning startup may call after SAME task terminal proof. Captured
    /// physical connection is transferred before any native close future poll.
    pub(super) fn seal_source(
        &self,
        pool: Option<Arc<PostgresPool>>,
        original: &mut Option<PoolConnection<Postgres>>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if matches!(state.source, Source::Unsealed) {
            state.pool = pool;
            state.source = match original.take() {
                Some(connection) => {
                    state.native_future = Some(Box::pin(connection.detach().close_hard()));
                    Source::Captured
                }
                None => Source::Absent,
            };
        }
    }

    /// Serializes both SAME actual task loans. A raw native error/panic retains
    /// the future and refuses SDK cleanup; known close ACK can progress cleanup
    /// while the original panic/poison still forbids a clean outward receipt.
    pub(super) async fn finish(self: &Arc<Self>) -> bool {
        let Ok(permit) = Arc::clone(&self.serial).acquire_owned().await else {
            return false;
        };
        let native = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if matches!(state.source, Source::Unsealed) {
                return false;
            }
            if matches!(state.source, Source::Captured)
                && state.native_result.is_none()
                && state.native_failure.is_none()
                && state.native_task.is_none()
                && state.native_future.is_some()
            {
                let body = Arc::clone(self);
                state.native_task = Some(tokio::spawn(async move {
                    let future = body
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .native_future
                        .take();
                    let mut loan = FutureLoan {
                        original: Arc::clone(&body),
                        future,
                    };
                    let returned = match loan.future.as_mut() {
                        Some(future) => future.await.map_err(Arc::new),
                        None => Err(Arc::new(sqlx::Error::Protocol(
                            "missing original administrative close future".into(),
                        ))),
                    };
                    body.state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .native_result = Some(returned.clone());
                    if returned.is_ok() {
                        loan.future = None;
                    }
                    returned
                }));
            }
            state.native_task.take()
        };
        let mut loan = CloseLoan {
            original: Arc::clone(self),
            native,
            sdk: None,
            _permit: permit,
        };
        if let Some(task) = loan.native.as_mut() {
            let joined = task.await;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.native_joined = true;
            match joined {
                Ok(result) => state.native_result = Some(result),
                Err(error) => state.native_failure = Some(Arc::new(error)),
            }
            loan.native = None;
        }
        let sdk = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let native_ack = matches!(state.source, Source::Absent)
                || (state.native_joined && matches!(state.native_result, Some(Ok(()))));
            if !native_ack {
                return false;
            }
            if state.sdk_task.is_none() && !state.sdk_joined && state.sdk_failure.is_none() {
                if let Some(pool) = state.pool.clone() {
                    let body = Arc::clone(self);
                    state.sdk_task = Some(tokio::spawn(async move {
                        {
                            let _registered =
                                body.state.lock().unwrap_or_else(|error| error.into_inner());
                        }
                        pool.close_original_sdk().await;
                        body.state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .sdk_ack = true;
                    }));
                } else if matches!(state.source, Source::Absent) {
                    // No SDK existed after proved never-dispatched/known terminal
                    // startup. This is not a receipt that a missing pool closed.
                    state.sdk_joined = true;
                } else {
                    return false;
                }
            }
            state.sdk_task.take()
        };
        loan.sdk = sdk;
        if let Some(task) = loan.sdk.as_mut() {
            let joined = task.await;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.sdk_joined = true;
            if let Err(error) = joined {
                state.sdk_failure = Some(Arc::new(error));
            }
            loan.sdk = None;
        }
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.sdk_joined
            && (state.pool.is_none() || state.sdk_ack)
            && state.native_failure.is_none()
            && state.sdk_failure.is_none()
            && !self.state.is_poisoned()
    }
}

struct FutureLoan {
    original: Arc<OriginalAdministrativeClose>,
    future: Option<NativeFuture>,
}
impl Drop for FutureLoan {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            self.original
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .native_future = Some(future);
        }
    }
}
struct CloseLoan {
    original: Arc<OriginalAdministrativeClose>,
    native: Option<JoinHandle<NativeResult>>,
    sdk: Option<JoinHandle<()>>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for CloseLoan {
    fn drop(&mut self) {
        let mut state = self
            .original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(task) = self.native.take() {
            state.native_task = Some(task);
        }
        if let Some(task) = self.sdk.take() {
            state.sdk_task = Some(task);
        }
    }
}
