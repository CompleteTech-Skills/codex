//! Exterior owner of the SAME consumed original hard-close future.
//! Undeclared prerequisite; typed session conversion is wired atomically later.
use crate::PostgresPool;
use crate::RetainedWriteSession;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
type OriginalFuture = Pin<Box<dyn Future<Output = Result<(), sqlx::Error>> + Send>>;
type OriginalResult = Result<(), Arc<sqlx::Error>>;

pub(crate) struct OriginalClose {
    original_pool: Arc<PostgresPool>,
    state: Mutex<State>,
    permit: Arc<Semaphore>,
}
struct State {
    future: Option<OriginalFuture>,
    task: Option<tokio::task::JoinHandle<OriginalResult>>,
    failure: Option<Arc<tokio::task::JoinError>>,
    terminal: Option<OriginalResult>,
}
pub(crate) struct Refusal {
    pub(crate) original: Arc<OriginalClose>,
}
pub(crate) struct Closed {
    original: Arc<OriginalClose>,
}
impl Closed {
    pub(crate) fn covers_pool(&self, pool: &Arc<PostgresPool>) -> bool {
        Arc::ptr_eq(&self.original.original_pool, pool)
    }
}
impl OriginalClose {
    pub(crate) fn capture(original: RetainedWriteSession) -> Arc<Self> {
        let original_pool = Arc::clone(original.original_pool());
        // Conversion transfers ownership, never supplies an ACK. Future owns
        // the exact detached PgConnection before its first native close poll.
        let future = original.into_original_hard_close();
        let owner = Arc::new(Self {
            original_pool,
            state: Mutex::new(State {
                future: Some(future),
                task: None,
                failure: None,
                terminal: None,
            }),
            permit: Arc::new(Semaphore::new(1)),
        });
        {
            let mut state = owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let task_owner = Arc::clone(&owner);
            let task = tokio::spawn(async move {
                // Sender holds this exact gate until the original handle is stored.
                let future = {
                    let mut state = task_owner
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.future.take()
                };
                match future {
                    Some(future) => future.await.map_err(Arc::new),
                    None => Err(Arc::new(sqlx::Error::Protocol(
                        "missing original close future".into(),
                    ))),
                }
            });
            state.task = Some(task);
        }
        owner
    }
    pub(crate) async fn finish(self: &Arc<Self>) -> Result<Closed, Refusal> {
        let refusal = || Refusal {
            original: Arc::clone(self),
        };
        let permit = Arc::clone(&self.permit)
            .acquire_owned()
            .await
            .map_err(|_| refusal())?;
        let (task, terminal, failed, poison) = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            // Cached error retains the actual original driver cause forever.
            (
                if state.terminal.is_none() {
                    state.task.take()
                } else {
                    None
                },
                state.terminal.clone(),
                state.failure.is_some(),
                poison,
            )
        };
        if failed {
            return Err(refusal());
        }
        if let Some(result) = terminal {
            return match result {
                Ok(()) if !poison => Ok(Closed {
                    original: Arc::clone(self),
                }),
                _ => Err(refusal()),
            };
        }
        let mut loan = Loan {
            original: Arc::clone(self),
            task,
            _permit: permit,
        };
        let task = loan.task.as_mut().ok_or_else(refusal)?;
        let joined = task.await;
        let result = match joined {
            Ok(result) => result,
            Err(cause) => {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.failure = Some(Arc::new(cause));
                loan.task = None;
                return Err(refusal());
            }
        };
        let poisoned = {
            let (mut state, poisoned) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.terminal = Some(result.clone()); // BEFORE future disposal or SDK closure.
            loan.task = None; // SAME task physically joined; no future can be repolled.
            poisoned
        };
        match result {
            Ok(()) if !poison && !poisoned => Ok(Closed {
                original: Arc::clone(self),
            }),
            _ => Err(refusal()),
        }
    }
}
struct Loan {
    original: Arc<OriginalClose>,
    task: Option<tokio::task::JoinHandle<OriginalResult>>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Loan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task = Some(task);
        }
    } // Caller cancellation restores SAME task before the serialized permit.
}
