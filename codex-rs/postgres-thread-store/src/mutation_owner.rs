//! Ownership of one original SQL mutation task. Inactive until the live writer wires it.
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use std::future::Future;
use tokio::task::JoinError;
use tokio::task::JoinHandle;

pub(crate) enum MutationOutcome {
    Committed,
    AdmissionUncertain(codex_postgres_runtime::ActivationGateError),
    /// The original command refused, and the original transaction acknowledged rollback.
    RolledBack(ThreadStoreError),
    /// No physical completion acknowledgment exists. Keep every original cause.
    Uncertain {
        command: Option<ThreadStoreError>,
        database: sqlx::Error,
    },
}

/// The original handle never leaves its owner while an observer awaits it.
/// Cancellation of observation does not cancel, detach, or replace the mutation.
#[derive(Default)]
pub(crate) struct MutationOwner {
    task: Option<JoinHandle<MutationOutcome>>,
    terminal: Option<Result<MutationOutcome, JoinError>>,
    refusals: Vec<ThreadStoreError>,
    #[cfg(test)]
    original_task_id: Option<tokio::task::Id>,
}

pub(crate) enum MutationTerminal<'a> {
    Committed,
    RolledBack(&'a ThreadStoreError),
}

impl MutationOwner {
    pub(crate) fn start(
        &mut self,
        operation: impl Future<Output = MutationOutcome> + Send + 'static,
    ) -> ThreadStoreResult<()> {
        if self.task.is_some() || self.terminal.is_some() {
            return Err(refusal("original mutation is still owned"));
        }
        // Reserve the next cause slot before native/SQL dispatch. No history eviction.
        if self.refusals.len() >= 4 {
            return Err(refusal("original mutation refusal history is full"));
        }
        self.refusals
            .try_reserve(1)
            .map_err(|_| refusal("cannot retain original mutation refusal"))?;
        let task = tokio::spawn(operation);
        #[cfg(test)]
        {
            self.original_task_id = Some(task.id());
        }
        self.task = Some(task);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn original_task_id(&self) -> Option<tokio::task::Id> {
        self.original_task_id
    }

    #[cfg(test)]
    pub(crate) fn original_rollback_cause(&self) -> Option<&ThreadStoreError> {
        match self.terminal.as_ref() {
            Some(Ok(MutationOutcome::RolledBack(error))) => Some(error),
            _ => None,
        }
    }

    pub(crate) fn has_original_failures(&self) -> bool {
        !self.refusals.is_empty()
            || matches!(
                self.terminal,
                Some(Ok(MutationOutcome::RolledBack(_)))
                    | Some(Ok(MutationOutcome::Uncertain { .. }))
                    | Some(Ok(MutationOutcome::AdmissionUncertain(_)))
                    | Some(Err(_))
            )
    }

    pub(crate) fn is_present(&self) -> bool {
        self.task.is_some() || self.terminal.is_some()
    }

    pub(crate) async fn observe(&mut self) -> ThreadStoreResult<MutationTerminal<'_>> {
        if self.terminal.is_none() {
            let task = self
                .task
                .as_mut()
                .ok_or_else(|| refusal("original mutation handle is absent"))?;
            let result = task.await;
            // No await between actual join and caching its original result.
            self.terminal = Some(result);
            self.task = None;
        }
        match self.terminal.as_ref() {
            Some(Ok(MutationOutcome::Committed)) => Ok(MutationTerminal::Committed),
            Some(Ok(MutationOutcome::RolledBack(error))) => Ok(MutationTerminal::RolledBack(error)),
            Some(Ok(MutationOutcome::AdmissionUncertain(error))) => {
                let _ = error;
                Err(refusal(
                    "original mutation admission completion is uncertain",
                ))
            }
            Some(Ok(MutationOutcome::Uncertain { command, database })) => {
                // Retain both originals; never interpolate database/command payload in logs.
                let _ = (command, database);
                Err(refusal("original mutation SQL completion is uncertain"))
            }
            Some(Err(error)) => {
                let _ = error;
                Err(refusal(
                    "original mutation task terminated without SQL acknowledgment",
                ))
            }
            None => Err(refusal("original mutation completion is absent")),
        }
    }

    /// Permit a staged-byte retry only after the previous original acknowledged rollback.
    pub(crate) fn prepare_retry(&mut self) -> ThreadStoreResult<()> {
        if self.task.is_some() || !matches!(self.terminal, Some(Ok(MutationOutcome::RolledBack(_))))
        {
            return Err(refusal("original mutation does not permit retry"));
        }
        if let Some(Ok(MutationOutcome::RolledBack(error))) = self.terminal.take() {
            // Capacity was reserved before the original dispatch.
            self.refusals.push(error);
        }
        Ok(())
    }

    pub(crate) fn acknowledge_commit(&mut self) -> ThreadStoreResult<()> {
        if !matches!(self.terminal, Some(Ok(MutationOutcome::Committed))) {
            return Err(refusal("original mutation commit is not acknowledged"));
        }
        self.terminal = None;
        Ok(())
    }
}

fn refusal(message: &str) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn canceled_observer_keeps_original_task_and_acknowledges_once() -> ThreadStoreResult<()>
    {
        let mut original = MutationOwner::default();
        let (release, held) = tokio::sync::oneshot::channel();
        original.start(async move {
            let _ = held.await;
            MutationOutcome::Committed
        })?;
        let id = original.task.as_ref().map(JoinHandle::id);
        let timed_out = tokio::time::timeout(Duration::from_millis(10), original.observe())
            .await
            .is_err();
        let same = original.task.as_ref().map(JoinHandle::id) == id;
        let _ = release.send(());
        let terminal = original.observe().await;
        let committed = matches!(terminal, Ok(MutationTerminal::Committed));
        original.acknowledge_commit()?;
        if !timed_out || !same || !committed || original.is_present() {
            return Err(refusal("original task cancellation/commit control failed"));
        }
        if original.acknowledge_commit().is_ok() {
            return Err(refusal("duplicate commit acknowledgment succeeded"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn actual_task_panic_stays_owned_and_never_permits_retry() -> ThreadStoreResult<()> {
        let mut original = MutationOwner::default();
        original.start(async { panic!("literal mutation control panic") })?;
        let id = original.task.as_ref().map(JoinHandle::id);
        let refused = original.observe().await.is_err();
        let actual = match original.terminal.as_ref() {
            Some(Err(error)) => Some(error.id()),
            _ => None,
        };
        if !refused || actual != id || !original.is_present() || original.prepare_retry().is_ok() {
            return Err(refusal("original JoinError was not retained"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn retry_preserves_each_original_refusal_and_caps_before_dispatch()
    -> ThreadStoreResult<()> {
        let mut original = MutationOwner::default();
        for _ in 0..4 {
            original.start(async { MutationOutcome::RolledBack(refusal("original command")) })?;
            if !matches!(original.observe().await?, MutationTerminal::RolledBack(_)) {
                return Err(refusal("rollback receipt was lost"));
            }
            original.prepare_retry()?;
        }
        let dispatched = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let witness = std::sync::Arc::clone(&dispatched);
        let refused = original
            .start(async move {
                witness.store(true, std::sync::atomic::Ordering::SeqCst);
                MutationOutcome::Committed
            })
            .is_err();
        if !refused
            || dispatched.load(std::sync::atomic::Ordering::SeqCst)
            || original.refusals.len() != 4
        {
            return Err(refusal("mutation history admission failed"));
        }
        Ok(())
    }
}
