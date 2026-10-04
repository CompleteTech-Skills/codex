//! Service-local original operation tasks, retained before body first poll.
//! Task completion is not a socket, native, Home or generation drain permit.
use crate::BlockerCode;
use crate::OperationRecord;
use crate::RecoveryReport;
use crate::StorageError;
use crate::StorageService;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use uuid::Uuid;
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum Kind {
    Forward,
    Return,
    Activate,
    Recover,
    Cancel,
    Probe,
    AuthorityOrigin,
    Schema,
    Plan(crate::PlanAction),
    Progress,
    Attach,
    PrepareMigration,
    PrepareReturn,
    Status(bool),
}
#[derive(Clone)]
pub(crate) enum Output {
    Record(Box<OperationRecord>),
    Recovery(RecoveryReport),
    Observed(Arc<dyn std::any::Any + Send + Sync>),
}
type OperationBody = Box<
    dyn FnOnce(
            StorageService,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Output, StorageError>> + Send>,
        > + Send,
>;
#[derive(Default)]
pub(crate) struct Operations {
    originals: Mutex<Vec<Arc<Original>>>,
    history: Mutex<Vec<Arc<Original>>>,
    // Intentional bounded pending ownership survives last external Service drop.
    // Only original-handle reaping and an empty registry can release this hold.
    pending_hold: Mutex<Option<Arc<Operations>>>,
    #[cfg(test)]
    probe: Mutex<Option<Arc<Probe>>>,
}
struct Original {
    kind: Kind,
    id: Option<Uuid>,
    state: Mutex<State>,
    join: Semaphore,
    reaper_join: Semaphore,
    resources: Arc<crate::operation_resources::Resources>,
}
#[derive(Default)]
struct State {
    task: Option<JoinHandle<Result<Output, StorageError>>>,
    #[cfg(test)]
    operation_task_id: Option<tokio::task::Id>,
    reaper: Option<JoinHandle<()>>,
    reaper_failure: Option<tokio::task::JoinError>,
    reaper_terminal: bool,
    result: Option<Result<Output, StorageError>>,
    failure: Option<tokio::task::JoinError>,
}
fn unavailable() -> StorageError {
    StorageError(BlockerCode::Internal)
}
impl Operations {
    fn capture(
        &self,
        service: StorageService,
        kind: Kind,
        id: Option<Uuid>,
        record: Option<OperationRecord>,
    ) -> Result<Arc<Original>, StorageError> {
        self.capture_body(service, kind, id, record, None)
    }
    fn capture_body(
        &self,
        mut service: StorageService,
        kind: Kind,
        id: Option<Uuid>,
        record: Option<OperationRecord>,
        body: Option<OperationBody>,
    ) -> Result<Arc<Original>, StorageError> {
        #[cfg(test)]
        let probe = self.probe.lock().map_err(|_| unavailable())?.clone();
        let mut originals = self.originals.lock().map_err(|_| unavailable())?;
        let mut history = self.history.lock().map_err(|_| unavailable())?;
        Self::reap_ready(&mut originals, &mut history);
        if let Some(original) = originals
            .iter()
            .find(|original| original.kind == kind && original.id == id)
        {
            return Ok(Arc::clone(original));
        }
        if originals.len() + history.len() >= 16 {
            return Err(unavailable());
        }
        originals.try_reserve(1).map_err(|_| unavailable())?;
        history
            .try_reserve(originals.len() + 1)
            .map_err(|_| unavailable())?;
        *self.pending_hold.lock().map_err(|_| unavailable())? =
            Some(Arc::clone(&service.original_operations));
        let resources = crate::operation_resources::Resources::new(
            Arc::clone(&service.inputs),
            Arc::clone(&service.migration_startups),
        );
        #[cfg(test)]
        resources.expect_empty_bootstrap(
            probe
                .as_ref()
                .is_some_and(|probe| probe.require_empty_bootstrap),
        );
        service.operation_resources = Some(Arc::clone(&resources));
        let original = Arc::new(Original {
            kind,
            id,
            state: Mutex::default(),
            join: Semaphore::new(1),
            reaper_join: Semaphore::new(1),
            resources: Arc::clone(&resources),
        });
        // Reserve registry ownership and stored handle BEFORE allowing body first poll.
        originals.push(Arc::clone(&original));
        let (release, entry) = tokio::sync::oneshot::channel();
        let disposed = Arc::new(tokio::sync::Notify::new());
        let wake = DisposalWake(Arc::clone(&disposed));
        let operations = Arc::clone(&service.original_operations);
        let task = tokio::spawn(async move {
            let _wake = wake;
            entry.await.map_err(|_| unavailable())?;
            #[cfg(test)]
            if let Some(probe) = probe.as_ref() {
                probe.entered.notify_one();
                probe.release.notified().await;
            }
            let result = async {
                #[cfg(test)]
                if probe.as_ref().is_some_and(|probe| probe.exercise_sdk) {
                    // Real constructors/registry inside SAME pre-registered task.
                    let runtime = if let Some(runtime) = probe
                        .as_ref()
                        .and_then(|probe| probe.shared_runtime.as_ref())
                    {
                        // Retain the actual first producer's SAME owners in this registered
                        // frame before any real shared-pool borrow or SQL is polled.
                        resources
                            .reserve_runtime()
                            .map_err(|_| unavailable())?
                            .capture_original_arc(Arc::clone(runtime))
                            .map_err(|_| unavailable())?;
                        resources
                            .reserve_runtime_pool()
                            .map_err(|_| unavailable())?
                            .capture_original_arc(Arc::clone(runtime.pool()))
                            .map_err(|_| unavailable())?;
                        Arc::clone(runtime)
                    } else {
                        service.connect().await?
                    };
                    let pool = service.migration_pool_for(&runtime).await?;
                    let mut connection = Some(pool.acquire().await.map_err(|_| unavailable())?);
                    if !probe.as_ref().is_some_and(|probe| probe.hold_connection) {
                        drop(connection.take());
                    }
                    if let Some(probe) = probe
                        .as_ref()
                        .filter(|probe| probe.fail_after_sdk || probe.hold_after_sdk)
                    {
                        probe.sdk_ready.notify_one();
                        probe.sdk_release.notified().await;
                    }
                    drop(connection); // Actual held borrower releases before body terminal.
                    if probe.as_ref().is_some_and(|probe| probe.fail_after_sdk) {
                        std::panic::resume_unwind(Box::new("original operation SDK fault"));
                    }
                }
                if let Some(body) = body {
                    return body(service.operation_context()).await;
                }
                match kind {
                    Kind::Forward => service
                        .run_migration_original(record.ok_or_else(unavailable)?)
                        .await
                        .map(|record| Output::Record(Box::new(record))),
                    Kind::Return => service
                        .run_return_original(record.ok_or_else(unavailable)?)
                        .await
                        .map(|record| Output::Record(Box::new(record))),
                    Kind::Activate => service
                        .activate_original(id.ok_or_else(unavailable)?)
                        .await
                        .map(|record| Output::Record(Box::new(record))),
                    Kind::Recover => service.recover_original().await.map(Output::Recovery),
                    Kind::Cancel => service
                        .cancel_original(id.ok_or_else(unavailable)?)
                        .await
                        .map(|record| Output::Record(Box::new(record))),
                    _ => Err(unavailable()), // An observed route requires its actual body.
                }
            }
            .await;
            resources.record_body_returned(result.clone());
            let cleanup = resources.finish_cleanup(&service.migration_startups).await;
            let result = match cleanup {
                Ok(receipt) if Arc::ptr_eq(receipt.original(), &resources) => result,
                Err(owning) => {
                    let _ = owning.original;
                    Err(unavailable())
                }
                Ok(_) => Err(unavailable()),
            };
            drop(service); // SDK cleanup has reconciled before body context release.
            #[cfg(test)]
            if let Some(probe) = probe.as_ref() {
                probe.returned.notify_one();
            }
            result
        });
        {
            let mut state = original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            #[cfg(test)]
            {
                state.operation_task_id = Some(task.id());
            }
            state.task = Some(task);
        }
        let retained = Arc::clone(&original);
        // Store the cleanup task before operation entry release. Disposal is merely
        // a wake; finish_core must physically join the SAME original operation JH.
        let reaper = tokio::spawn(async move {
            disposed.notified().await;
            let _ = operations.finish_core(retained).await;
        });
        original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reaper = Some(reaper);
        // Even a failed entry release leaves the same stored task to be reaped.
        let _ = release.send(());
        Ok(original)
    }
    fn reap_ready(originals: &mut Vec<Arc<Original>>, history: &mut Vec<Arc<Original>>) {
        // Reap actual ready original handles even if every RPC waiter was canceled.
        // Pending/loaned/poisoned/failing tasks remain owned; is_finished is no proof.
        originals.retain(|original| {
            let Ok(_reaper_join) = original.reaper_join.try_acquire() else {
                return true;
            };
            let Ok(_join) = original.join.try_acquire() else {
                return true;
            };
            let Ok(mut state) = original.state.lock() else {
                return true;
            };
            if let Some(reaper) = state.reaper.as_mut() {
                let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                if let std::task::Poll::Ready(result) =
                    std::future::Future::poll(std::pin::Pin::new(reaper), &mut context)
                {
                    state.reaper.take();
                    state.reaper_terminal = true;
                    if let Err(cause) = result {
                        state.reaper_failure = Some(cause);
                    }
                }
            }
            if state.result.is_none() && state.failure.is_none() {
                if let Some(task) = state.task.as_mut() {
                    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
                    if let std::task::Poll::Ready(result) =
                        std::future::Future::poll(std::pin::Pin::new(task), &mut context)
                    {
                        state.task.take();
                        match result {
                            Ok(result) => state.result = Some(result),
                            Err(cause) => state.failure = Some(cause),
                        }
                    }
                }
            }
            if state.task.is_none()
                && state.reaper.is_none()
                && state.reaper_failure.is_none()
                && state.reaper_terminal
                && (state.result.is_some() || state.failure.is_some())
                && original.resources.cleanup_completed()
            {
                if state.failure.is_some()
                    || matches!(state.result, Some(Err(_)))
                    || original.resources.retains_original_causes()
                {
                    history.push(Arc::clone(original));
                }
                false
            } else {
                true
            }
        });
    }
    async fn finish(&self, original: Arc<Original>) -> Result<Output, StorageError> {
        let result = self.finish_core(Arc::clone(&original)).await;
        // finish_core has released original.join before waiting for its reaper.
        let _permit = original
            .reaper_join
            .acquire()
            .await
            .map_err(|_| unavailable())?;
        let task = {
            original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .reaper
                .take()
        };
        if let Some(task) = task {
            let mut loan = ReaperLoan {
                original: Arc::clone(&original),
                task: Some(task),
            };
            let outcome = match loan.task.as_mut() {
                Some(task) => task.await,
                None => return Err(unavailable()),
            };
            loan.task.take();
            original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .reaper_terminal = true;
            if let Err(cause) = outcome {
                original
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .reaper_failure = Some(cause);
            }
        }
        let reaper_acknowledged = original.state.lock().is_ok_and(|state| {
            state.reaper_terminal && state.reaper.is_none() && state.reaper_failure.is_none()
        });
        if !reaper_acknowledged {
            return Err(unavailable());
        }
        // Only physical acknowledgment of BOTH original tasks permits eviction.
        let reconciled = self.finish_core(original).await;
        result.and(reconciled)
    }
    async fn finish_core(&self, original: Arc<Original>) -> Result<Output, StorageError> {
        let _permit = original.join.acquire().await.map_err(|_| unavailable())?;
        let (task, mut poisoned) = {
            let (mut state, poisoned) = match original.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            (state.task.take(), poisoned)
        };
        if let Some(task) = task {
            let mut loan = Loan {
                original: Arc::clone(&original),
                task: Some(task),
            };
            let result = match loan.task.as_mut() {
                Some(task) => task.await,
                None => return Err(unavailable()),
            };
            loan.task.take();
            let mut state = original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match result {
                Ok(result) => state.result = Some(result),
                Err(cause) => state.failure = Some(cause),
            }
        }
        let (result, joined_failure) = {
            let (state, state_poisoned) = match original.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            poisoned |= state_poisoned;
            (state.result.clone(), state.failure.is_some())
        };
        if joined_failure {
            // Cache the actual original JoinError BEFORE waking resource borrowers.
            original.resources.record_joined_task_terminal();
        }
        if result.is_none() && !joined_failure {
            return Err(unavailable()); // Missing original handle is not terminal proof.
        }
        // Reconcile the same external resources even when command admission is refused.
        // A canceled cleanup waiter restores its SAME stored cleanup handle.
        let cleanup = original
            .resources
            .finish_cleanup(&original.resources.original_startups())
            .await;
        if cleanup.is_err() {
            return Err(unavailable());
        }
        let result = if poisoned || joined_failure {
            Err(unavailable())
        } else {
            result.ok_or_else(unavailable)?
        };
        // Actual command and cleanup terminal proof precede success reclaim or
        // failed-result archive. Same-key fresh retry never erases the prior cause.
        let reaper_joined = {
            let state = original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.reaper_terminal && state.reaper.is_none() && state.reaper_failure.is_none()
        };
        if original.resources.cleanup_completed() && reaper_joined {
            let mut originals = self.originals.lock().map_err(|_| unavailable())?;
            let mut history = self.history.lock().map_err(|_| unavailable())?;
            if let Some(index) = originals
                .iter()
                .position(|entry| Arc::ptr_eq(entry, &original))
            {
                let removed = originals.remove(index);
                if result.is_err() || original.resources.retains_original_causes() {
                    history.push(removed);
                }
            }
            if originals.is_empty() && history.is_empty() {
                self.pending_hold.lock().map_err(|_| unavailable())?.take();
            }
        }
        result
    }
}
// Wake does not certify completion, rollback, resources or a physical task join.
struct DisposalWake(Arc<tokio::sync::Notify>);
impl Drop for DisposalWake {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
struct ReaperLoan {
    original: Arc<Original>,
    task: Option<JoinHandle<()>>,
}
impl Drop for ReaperLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .reaper = Some(task);
        }
    }
}
struct Loan {
    original: Arc<Original>,
    task: Option<JoinHandle<Result<Output, StorageError>>>,
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
    }
}
impl StorageService {
    // Type erasure carries only the actual original body's result projection;
    // all resources/tasks/native ownership stay in the same external Original.
    pub(crate) async fn original_observation<T, F, Fut>(
        &self,
        kind: Kind,
        id: Option<Uuid>,
        body: F,
    ) -> Result<T, StorageError>
    where
        T: Clone + Send + Sync + 'static,
        F: FnOnce(StorageService) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, StorageError>> + Send + 'static,
    {
        let body: OperationBody = Box::new(move |service| {
            Box::pin(async move {
                body(service)
                    .await
                    .map(|value| Output::Observed(Arc::new(value)))
            })
        });
        let original = self.original_operations.capture_body(
            self.operation_context(),
            kind,
            id,
            None,
            Some(body),
        )?;
        match self.original_operations.finish(original).await? {
            Output::Observed(value) => value.downcast_ref::<T>().cloned().ok_or_else(unavailable),
            _ => Err(unavailable()),
        }
    }
    pub(crate) async fn original_operation(
        &self,
        kind: Kind,
        id: Option<Uuid>,
        record: Option<OperationRecord>,
    ) -> Result<Output, StorageError> {
        let original =
            self.original_operations
                .capture(self.operation_context(), kind, id, record)?;
        self.original_operations.finish(original).await
    }
}

#[cfg(test)]
#[derive(Default)]
struct Probe {
    exercise_sdk: bool,
    require_empty_bootstrap: bool,
    fail_after_sdk: bool,
    hold_after_sdk: bool,
    hold_connection: bool,
    shared_runtime: Option<Arc<codex_remote_storage::RemoteStorage>>,
    sdk_ready: tokio::sync::Notify,
    sdk_release: tokio::sync::Notify,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    returned: tokio::sync::Notify,
}
#[cfg(test)]
#[path = "service_operation_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "service_public_route_tests.rs"]
mod public_route_tests;
