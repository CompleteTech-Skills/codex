//! Original resume query task retained across waiter cancellation.
use super::copy_error;
use super::internal;
use codex_rollout::RolloutItem;
use codex_thread_store::ThreadStoreResult;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Semaphore;

pub(super) type LoadedHistory = Arc<(Vec<RolloutItem>, u64, Option<u64>)>;
type Handle = tokio::task::JoinHandle<ThreadStoreResult<LoadedHistory>>;
#[derive(Default)]
struct State {
    closed: bool,
    started: bool,
    task: Option<Handle>,
    result: Option<ThreadStoreResult<LoadedHistory>>,
    failure: Option<tokio::task::JoinError>,
    #[cfg(test)]
    task_id: Option<tokio::task::Id>,
}
pub(super) struct PendingLoad {
    state: Mutex<State>,
    join: Semaphore,
}
impl Default for PendingLoad {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            join: Semaphore::new(1),
        }
    }
}
/// Private proof of this original Rust task's join, not PostgreSQL rollback proof.
pub(super) struct LoadTaskJoined<'a> {
    _original_owner: &'a PendingLoad,
}

struct OriginalHandle<'a> {
    owner: &'a PendingLoad,
    task: Option<Handle>,
}
impl Drop for OriginalHandle<'_> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task = Some(task);
        }
    }
}
impl PendingLoad {
    pub(super) fn start(
        &self,
        future: impl Future<Output = ThreadStoreResult<LoadedHistory>> + Send + 'static,
    ) -> ThreadStoreResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| internal("original load registry poisoned"))?;
        if state.closed || state.started {
            return Err(internal("original load admission unavailable"));
        }
        // Existing PendingWriter owns this slot before spawn; no fallible step follows.
        state.started = true;
        let task = tokio::spawn(future);
        #[cfg(test)]
        {
            state.task_id = Some(task.id());
        }
        state.task = Some(task);
        Ok(())
    }
    pub(super) async fn observe(&self, close: bool) -> ThreadStoreResult<Option<LoadedHistory>> {
        if close {
            self.state
                .lock()
                .map_err(|_| internal("original load registry poisoned"))?
                .closed = true;
        }
        let _join = self.join.acquire().await.map_err(internal)?;
        let mut original = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| internal("original load registry poisoned"))?;
            OriginalHandle {
                owner: self,
                task: state.task.take(),
            }
        };
        if let Some(task) = original.task.as_mut() {
            let outcome = task.await;
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match outcome {
                Ok(result) => state.result = Some(result),
                Err(error) => state.failure = Some(error),
            }
            original.task = None; // Cached actual terminal, never poll the same handle twice.
        }
        let state = self
            .state
            .lock()
            .map_err(|_| internal("original load registry poisoned"))?;
        if state.failure.is_some() {
            return Err(internal("original resume query task failed"));
        }
        match state.result.as_ref() {
            Some(Ok(result)) => Ok(Some(Arc::clone(result))),
            Some(Err(error)) => Err(copy_error(error)),
            None => Ok(None),
        }
    }
    pub(super) async fn close_join(&self) -> ThreadStoreResult<LoadTaskJoined<'_>> {
        let _query_outcome = self.observe(true).await;
        // Completed query refusal is distinct from failed observation or a failed join.
        // Poison never grants authority, even if recovery could inspect cached state.
        let state = self
            .state
            .lock()
            .map_err(|_| internal("original load terminal registry poisoned"))?;
        if !state.closed
            || state.task.is_some()
            || state.failure.is_some()
            || (state.started && state.result.is_none())
        {
            return Err(internal("original load terminal proof unavailable"));
        }
        Ok(LoadTaskJoined {
            _original_owner: self,
        })
    }

    #[cfg(test)]
    pub(super) fn failed_join(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
            .is_some()
    }
    #[cfg(test)]
    pub(super) fn original_task_id(&self) -> Option<tokio::task::Id> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .task_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn canceled_waiter_restores_exact_original_handle() -> ThreadStoreResult<()> {
        let owner = PendingLoad::default();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let entered_task = Arc::clone(&entered);
        let release_task = Arc::clone(&release);
        owner.start(async move {
            entered_task.notify_one();
            release_task.notified().await;
            Ok(Arc::new((Vec::new(), 0, None)))
        })?;
        entered.notified().await;
        let original_id = owner.original_task_id();
        let canceled = tokio::time::timeout(Duration::from_millis(10), owner.observe(false)).await;
        let retained = owner
            .state
            .lock()
            .map_err(|_| internal("test owner poisoned"))?
            .task
            .as_ref()
            .map(tokio::task::JoinHandle::id);
        let duplicate = owner.start(async { Ok(Arc::new((Vec::new(), 1, None))) });
        release.notify_one();
        // Always release and join the actual original before diagnostics.
        let completed = owner.observe(true).await?;
        assert!(canceled.is_err());
        assert_eq!(retained, original_id);
        assert!(original_id.is_some());
        assert!(duplicate.is_err());
        assert_eq!(completed.as_ref().map(|data| data.1), Some(0));
        let cached = owner.observe(true).await?;
        assert!(
            matches!((completed, cached), (Some(first), Some(second)) if Arc::ptr_eq(&first, &second))
        );
        assert!(
            owner
                .start(async { Ok(Arc::new((Vec::new(), 2, None))) })
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn task_panic_retains_actual_terminal_cause() -> ThreadStoreResult<()> {
        let owner = PendingLoad::default();
        owner.start(async { panic!("owned load test panic") })?;
        let original_id = owner.original_task_id();
        assert!(owner.observe(true).await.is_err());
        assert!(owner.failed_join());
        {
            let state = owner
                .state
                .lock()
                .map_err(|_| internal("test owner poisoned"))?;
            assert!(
                matches!(state.failure.as_ref(), Some(error) if error.is_panic() && Some(error.id()) == original_id)
            );
        }
        assert!(owner.observe(true).await.is_err());
        Ok(())
    }
}
