//! Service-local original operation tasks, retained before body first poll.
//! Task completion is not a socket, native, Home or generation drain permit.
use crate::{BlockerCode, OperationRecord, RecoveryReport, StorageError, StorageService};
use std::sync::{Arc, Mutex};
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
}
#[derive(Clone)]
pub(crate) enum Output {
    Record(Box<OperationRecord>),
    Recovery(RecoveryReport),
}
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
    resources: Arc<crate::operation_resources::Resources>,
}
#[derive(Default)]
struct State {
    task: Option<JoinHandle<Result<Output, StorageError>>>,
    result: Option<Result<Output, StorageError>>,
    failure: Option<tokio::task::JoinError>,
}
fn unavailable() -> StorageError {
    StorageError(BlockerCode::Internal)
}
impl Operations {
    fn capture(
        &self,
        mut service: StorageService,
        kind: Kind,
        id: Option<Uuid>,
        record: Option<OperationRecord>,
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
        service.operation_resources = Some(Arc::clone(&resources));
        let original = Arc::new(Original {
            kind,
            id,
            state: Mutex::default(),
            join: Semaphore::new(1),
            resources: Arc::clone(&resources),
        });
        // Reserve registry ownership and stored handle BEFORE allowing body first poll.
        originals.push(Arc::clone(&original));
        let (release, entry) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
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
                    let runtime = service.connect().await?;
                    let pool = service.migration_pool_for(&runtime).await?;
                    let connection = pool.acquire().await.map_err(|_| unavailable())?;
                    drop(connection); // Real borrower ends before actual SDK-close boundary.
                    if let Some(probe) = probe.as_ref().filter(|probe| probe.fail_after_sdk) {
                        probe.sdk_ready.notify_one();
                        probe.sdk_release.notified().await;
                        std::panic::resume_unwind(Box::new("original operation SDK fault"));
                    }
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
        original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .task = Some(task);
        // Even a failed entry release leaves the same stored task to be reaped.
        let _ = release.send(());
        Ok(original)
    }
    fn reap_ready(originals: &mut Vec<Arc<Original>>, history: &mut Vec<Arc<Original>>) {
        // Reap actual ready original handles even if every RPC waiter was canceled.
        // Pending/loaned/poisoned/failing tasks remain owned; is_finished is no proof.
        originals.retain(|original| {
            let Ok(_join) = original.join.try_acquire() else {
                return true;
            };
            let Ok(mut state) = original.state.lock() else {
                return true;
            };
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
                && state.result.is_some()
                && state.failure.is_none()
                && original.resources.cleanup_completed()
            {
                if matches!(state.result, Some(Err(_)))
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
        if original.resources.cleanup_completed() {
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
    fail_after_sdk: bool,
    sdk_ready: tokio::sync::Notify,
    sdk_release: tokio::sync::Notify,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    returned: tokio::sync::Notify,
}
#[cfg(test)]
#[path = "service_operation_tests.rs"]
mod tests;
