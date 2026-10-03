//! Pending writer ownership before SQL dispatch; included by the owning store stage.

use super::internal;
use super::renew_lease;
use crate::live::LiveThread;
use crate::live::WriterOwnership;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::ThreadOwnership;
use codex_postgres_runtime::ThreadOwnershipAttempt;
use codex_postgres_runtime::ThreadOwnershipError;
use codex_postgres_runtime::ThreadOwnershipNamespace;
use codex_protocol::ThreadId;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as RegistryMutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

// Finite retained-frame admission, not a bound on payload bytes or native resources.
const MAX_RETAINED_WRITERS: usize = 256;
const MAX_REFUSAL_OBSERVATIONS: usize = 4;

#[derive(Default)]
pub(super) struct PendingWriters {
    originals: RegistryMutex<HashMap<ThreadId, Arc<PendingWriter>>>,
    failed_history: RegistryMutex<Vec<Arc<PendingWriter>>>,
}

pub(super) struct PendingWriter {
    original: Arc<Mutex<LiveThread>>,
    original_pool: Arc<PostgresPool>,
    pub(super) load: super::pending_load::PendingLoad,
    attempt: Arc<RegistryMutex<Option<Arc<ThreadOwnershipAttempt>>>>,
    completion: RegistryMutex<Completion>,
    completion_join: Semaphore,
    cleanup_gate: Semaphore,
    // Keep the actual claim failure even if same-attempt recovery succeeds.
    claim_failure: Arc<RegistryMutex<Option<ThreadOwnershipError>>>,
    no_lease_created: Arc<AtomicBool>,
    cleanup_claim: RegistryMutex<Option<Arc<ThreadOwnership>>>,
    refusals: RegistryMutex<Refusals>,
    #[cfg(test)]
    task_id: RegistryMutex<Option<tokio::task::Id>>,
}

#[derive(Default)]
struct Refusals {
    admitted: usize,
    errors: Vec<ThreadStoreError>,
}

/// Reserved before a fallible metadata/history observation, never minted afterward.
pub(super) struct RefusalSlot(Arc<PendingWriter>);

impl Refusals {
    fn reserve(&mut self) -> ThreadStoreResult<()> {
        if self.admitted >= MAX_REFUSAL_OBSERVATIONS {
            return Err(internal("pending observation admission exhausted"));
        }
        let additional = self.admitted + 1 - self.errors.len();
        self.errors
            .try_reserve(additional)
            .map_err(|_| internal("pending refusal history capacity unavailable"))?;
        self.admitted += 1;
        Ok(())
    }

    fn record(&mut self, error: ThreadStoreError) {
        self.errors.push(error);
    }
}

fn admit_population(active: usize, historical: usize) -> ThreadStoreResult<()> {
    if active.saturating_add(historical) >= MAX_RETAINED_WRITERS {
        return Err(internal("retained writer admission exhausted"));
    }
    Ok(())
}

impl RefusalSlot {
    pub(super) fn record(self, error: ThreadStoreError) {
        self.0
            .refusals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record(error);
    }
}

struct Completion {
    task: Option<tokio::task::JoinHandle<ThreadStoreResult<()>>>,
    outcome: Option<ThreadStoreResult<()>>,
    task_error: Option<tokio::task::JoinError>,
}

/// Borrow-bound proof of this original claim task join, not SQL rollback proof.
struct ClaimTaskJoined<'a> {
    _original_owner: &'a PendingWriter,
}

// A canceled waiter restores the same original handle before releasing its permit.
struct OriginalClaimTask<'a> {
    owner: &'a PendingWriter,
    task: Option<tokio::task::JoinHandle<ThreadStoreResult<()>>>,
}
impl Drop for OriginalClaimTask<'_> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.owner
                .completion
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task = Some(task);
        }
    }
}

/// Constructed only after the original task and renewer have terminated and its
/// original-pool cleanup has completed (or a known no-lease command completed).
pub(super) struct PendingCleanupCompleted(Arc<PendingWriter>);

pub(super) struct WriterLaunch {
    pub(super) pool: Arc<PostgresPool>,
    pub(super) namespace: ThreadOwnershipNamespace,
    pub(super) thread_id: ThreadId,
    pub(super) lease: Duration,
    pub(super) renew_every: Duration,
    pub(super) original: Arc<Mutex<LiveThread>>,
    #[cfg(test)]
    pub(super) renewal_probe: Option<Arc<super::renewal_probe::RenewalProbe>>,
    #[cfg(test)]
    pub(super) lose_claim_response: Option<Arc<AtomicBool>>,
}

impl PendingWriters {
    /// Registry insertion precedes task creation. No await or fallible allocation is
    /// permitted between spawning the task and retaining its original JoinHandle.
    pub(super) fn start(&self, launch: WriterLaunch) -> ThreadStoreResult<Arc<PendingWriter>> {
        let mut originals = self
            .originals
            .lock()
            .map_err(|_| internal("pending writer registry poisoned"))?;
        if originals.contains_key(&launch.thread_id) {
            return Err(internal("an original writer launch is already pending"));
        }
        // All removals hold originals while archiving, so this observes the same
        // active + historical population; transferring a failure consumes no new slot.
        let retained_failures = self
            .failed_history
            .lock()
            .map_err(|_| internal("pending failure history poisoned"))?
            .len();
        admit_population(originals.len(), retained_failures)?;
        originals
            .try_reserve(1)
            .map_err(|_| internal("pending writer registry capacity unavailable"))?;
        let failure = Arc::new(RegistryMutex::new(None));
        let retained_attempt = Arc::new(RegistryMutex::new(None));
        let no_lease_created = Arc::new(AtomicBool::new(false));
        let pending = Arc::new(PendingWriter {
            original: Arc::clone(&launch.original),
            original_pool: Arc::clone(&launch.pool),
            load: super::pending_load::PendingLoad::default(),
            attempt: Arc::clone(&retained_attempt),
            completion_join: Semaphore::new(1),
            cleanup_gate: Semaphore::new(1),
            completion: RegistryMutex::new(Completion {
                task: None,
                outcome: None,
                task_error: None,
            }),
            claim_failure: Arc::clone(&failure),
            no_lease_created: Arc::clone(&no_lease_created),
            cleanup_claim: RegistryMutex::new(None),
            refusals: RegistryMutex::new(Refusals::default()),
            #[cfg(test)]
            task_id: RegistryMutex::new(None),
        });
        // The completion slot is private and uncontended before the registry is unlocked.
        let mut completion = pending
            .completion
            .try_lock()
            .map_err(|_| internal("pending completion slot unavailable"))?;
        originals.insert(launch.thread_id, Arc::clone(&pending));
        let task = tokio::spawn(async move {
            let busy = || ThreadStoreError::InvalidRequest {
                message: format!("thread {} already has a live writer", launch.thread_id),
            };
            let attempt = match launch
                .pool
                .prepare_thread_ownership_attempt(launch.namespace, &launch.thread_id.to_string())
                .await
            {
                Ok(attempt) => attempt,
                Err(error) => {
                    // Preparation reads a UUID; it has never dispatched lease DML.
                    no_lease_created.store(true, Ordering::SeqCst);
                    return Err(internal(error));
                }
            };
            let attempt = Arc::new(attempt);
            *retained_attempt
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&attempt));
            // Registry retains the actual opaque attempt even if recovery itself fails.
            // This original attempt lives in the retained task across every SQL await.
            let response = launch
                .pool
                .claim_thread_ownership_attempt(&attempt, launch.lease)
                .await;
            #[cfg(test)]
            let response = match response {
                Ok(Some(claim))
                    if launch
                        .lose_claim_response
                        .as_ref()
                        .is_some_and(|probe| probe.swap(false, Ordering::SeqCst)) =>
                {
                    // Only discard a genuine committed response; retain its exact attempt.
                    drop(claim);
                    Err(ThreadOwnershipError::Transaction(
                        codex_postgres_runtime::TransactionError::CommitOutcomeUnknown,
                    ))
                }
                response => response,
            };
            let claim = match response {
                Ok(Some(claim)) => claim,
                Ok(None) => {
                    no_lease_created.store(true, Ordering::SeqCst);
                    return Err(busy());
                }
                Err(error) => {
                    *failure
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                    launch
                        .pool
                        .recover_thread_ownership_attempt(&attempt)
                        .await
                        .map_err(internal)?
                        .ok_or_else(busy)?
                }
            };
            let lost = Arc::new(AtomicBool::new(false));
            let ownership = WriterOwnership {
                renewer: tokio::spawn(renew_lease(
                    Arc::clone(&launch.pool),
                    claim.clone(),
                    launch.lease,
                    launch.renew_every,
                    Arc::clone(&lost),
                    #[cfg(test)]
                    launch.renewal_probe.clone(),
                )),
                claim,
                lost,
                renewal_joined: false,
                renewal_error: None,
            };
            // Registry owns the original instance even when its caller has been canceled.
            launch.original.lock().await.ownership = Some(ownership);
            Ok(())
        });
        #[cfg(test)]
        {
            *pending
                .task_id
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task.id());
        }
        completion.task = Some(task);
        drop(completion);
        drop(originals);
        Ok(pending)
    }

    pub(super) fn get(&self, id: ThreadId) -> ThreadStoreResult<Option<Arc<PendingWriter>>> {
        Ok(self
            .originals
            .lock()
            .map_err(|_| internal("pending writer registry poisoned"))?
            .get(&id)
            .cloned())
    }

    /// Caller holds the live registry guard and supplies its actual installed Arc.
    pub(super) fn transfer_installed(
        &self,
        id: ThreadId,
        pending: &Arc<PendingWriter>,
        installed: &Arc<Mutex<LiveThread>>,
    ) -> ThreadStoreResult<()> {
        let completion = pending
            .completion
            .try_lock()
            .map_err(|_| internal("original writer task still observed"))?;
        if !matches!(completion.outcome.as_ref(), Some(Ok(()))) {
            return Err(internal(
                "original writer task has not completed successfully",
            ));
        }
        if !Arc::ptr_eq(installed, &pending.original) {
            return Err(internal(
                "pending writer transfer differs from original instance",
            ));
        }
        let mut originals = self
            .originals
            .lock()
            .map_err(|_| internal("pending writer registry poisoned"))?;
        if originals
            .get(&id)
            .is_none_or(|actual| !Arc::ptr_eq(actual, pending))
        {
            return Err(internal("pending writer registry original changed"));
        }
        self.preserve_failure(pending, &completion)?;
        originals.remove(&id);
        Ok(())
    }

    fn preserve_failure(
        &self,
        pending: &Arc<PendingWriter>,
        completion: &Completion,
    ) -> ThreadStoreResult<()> {
        if pending.has_claim_failure()
            || completion.task_error.is_some()
            || matches!(completion.outcome.as_ref(), Some(Err(_)))
            || !pending
                .refusals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .errors
                .is_empty()
        {
            let mut history = self
                .failed_history
                .lock()
                .map_err(|_| internal("pending failure history poisoned"))?;
            history
                .try_reserve(1)
                .map_err(|_| internal("pending failure history capacity unavailable"))?;
            history.push(Arc::clone(pending));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn failure_history(&self) -> Vec<Arc<PendingWriter>> {
        self.failed_history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn remove_cleaned(
        &self,
        id: ThreadId,
        completed: PendingCleanupCompleted,
    ) -> ThreadStoreResult<()> {
        let pending = completed.0;
        let completion = pending
            .completion
            .try_lock()
            .map_err(|_| internal("original pending completion is still borrowed"))?;
        let mut originals = self
            .originals
            .lock()
            .map_err(|_| internal("pending writer registry poisoned"))?;
        if originals
            .get(&id)
            .is_none_or(|actual| !Arc::ptr_eq(actual, &pending))
        {
            return Err(internal("original pending cleanup registry changed"));
        }
        self.preserve_failure(&pending, &completion)?;
        originals.remove(&id);
        Ok(())
    }
}

impl PendingWriter {
    #[cfg(test)]
    pub(super) fn original_task_id(&self) -> Option<tokio::task::Id> {
        *self
            .task_id
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn original(&self) -> &Arc<Mutex<LiveThread>> {
        &self.original
    }

    /// Canceling this waiter releases its mutex guard, retaining the same task handle.
    /// No pending launch is aborted or interpreted as a SQL-terminal acknowledgment.
    pub(super) async fn wait(&self) -> ThreadStoreResult<()> {
        let _join = self.completion_join.acquire().await.map_err(internal)?;
        let task = {
            let mut completion = self
                .completion
                .lock()
                .map_err(|_| internal("original completion registry poisoned"))?;
            if completion.outcome.is_none() && completion.task_error.is_none() {
                Some(
                    completion
                        .task
                        .take()
                        .ok_or_else(|| internal("original pending task missing"))?,
                )
            } else {
                None
            }
        };
        let mut original = OriginalClaimTask { owner: self, task };
        if let Some(task) = original.task.as_mut() {
            let result = task.await;
            let mut completion = self
                .completion
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match result {
                Ok(outcome) => completion.outcome = Some(outcome),
                Err(error) => completion.task_error = Some(error),
            }
            original.task = None;
        }
        let completion = self
            .completion
            .lock()
            .map_err(|_| internal("original completion registry poisoned"))?;
        match completion.outcome.as_ref() {
            Some(Ok(())) => Ok(()),
            Some(Err(error)) => Err(super::copy_error(error)),
            None => Err(internal("original writer launch task failed")),
        }
    }

    pub(super) fn reserve_refusal(self: &Arc<Self>) -> ThreadStoreResult<RefusalSlot> {
        let mut refusals = self
            .refusals
            .lock()
            .map_err(|_| internal("pending refusal history poisoned"))?;
        // Reserve before observation; cancellation still consumes its admission.
        refusals.reserve()?;
        Ok(RefusalSlot(Arc::clone(self)))
    }

    async fn join_claim_for_cleanup(&self) -> ThreadStoreResult<ClaimTaskJoined<'_>> {
        let _command_outcome = self.wait().await;
        let completion = self
            .completion
            .lock()
            .map_err(|_| internal("original claim terminal registry poisoned"))?;
        // Only this owner's actual joined result grants cleanup. An original task
        // panic, refused observation, missing handle or poison remains uncertainty.
        if completion.task.is_some()
            || completion.task_error.is_some()
            || completion.outcome.is_none()
        {
            return Err(internal("original claim terminal proof unavailable"));
        }
        Ok(ClaimTaskJoined {
            _original_owner: self,
        })
    }

    pub(super) async fn cleanup(self: &Arc<Self>) -> ThreadStoreResult<PendingCleanupCompleted> {
        let _cleanup = self.cleanup_gate.acquire().await.map_err(internal)?;
        // Even failed launch results must be actually joined, not aborted or detached.
        let _claim_terminal = self.join_claim_for_cleanup().await?;
        let _load_terminal = self.load.close_join().await?;
        let mut original = Arc::clone(&self.original).lock_owned().await;
        original.seal();
        original.finish_renewal().await?;
        if let Some(owner) = original.ownership.as_ref() {
            self.original_pool
                .release_thread_ownership(&owner.claim)
                .await
                .map_err(internal)?;
        } else if !self.no_lease_created.load(Ordering::SeqCst) {
            let cached = self
                .cleanup_claim
                .lock()
                .map_err(|_| internal("original cleanup claim registry poisoned"))?
                .clone();
            let claim = match cached {
                Some(claim) => claim,
                None => {
                    let attempt = self.original_attempt().ok_or_else(|| {
                        internal("original launch outcome unresolved without attempt")
                    })?;
                    let claim = self
                        .original_pool
                        .recover_thread_ownership_attempt(&attempt)
                        .await
                        .map_err(internal)?
                        .ok_or_else(|| {
                            internal("original launch outcome unresolved; no owner observed")
                        })?;
                    let claim = Arc::new(claim);
                    *self
                        .cleanup_claim
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(Arc::clone(&claim));
                    claim
                }
            };
            self.original_pool
                .release_thread_ownership(&claim)
                .await
                .map_err(internal)?;
        }
        drop(original);
        Ok(PendingCleanupCompleted(Arc::clone(self)))
    }

    pub(super) fn original_pool(&self) -> &Arc<PostgresPool> {
        &self.original_pool
    }

    pub(super) fn original_attempt(&self) -> Option<Arc<ThreadOwnershipAttempt>> {
        self.attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(Arc::clone)
    }

    #[cfg(test)]
    pub(super) fn original_refusals(&self) -> Vec<ThreadStoreError> {
        self.refusals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .errors
            .iter()
            .map(super::copy_error)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn original_claim_failure(&self) -> Option<ThreadOwnershipError> {
        *self
            .claim_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn has_claim_failure(&self) -> bool {
        self.claim_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
}

#[cfg(test)]
mod history_admission_tests {
    use super::*;

    #[test]
    fn retained_population_admission_counts_archived_originals() {
        assert!(admit_population(255, 0).is_ok());
        assert!(admit_population(0, 255).is_ok());
        assert!(admit_population(255, 1).is_err());
        assert!(admit_population(0, 256).is_err());
        assert!(admit_population(usize::MAX, 1).is_err());
        // Archiving a failed original instead of removing it cannot free admission.
        assert!(admit_population(128, 128).is_err());
        assert!(admit_population(127, 128).is_ok());
    }

    #[test]
    fn preadmitted_refusals_preserve_first_and_subsequent_causes() -> ThreadStoreResult<()> {
        let mut history = Refusals::default();
        for index in 0..MAX_REFUSAL_OBSERVATIONS {
            history.reserve()?;
            history.record(ThreadStoreError::Internal {
                message: index.to_string(),
            });
        }
        assert!(history.reserve().is_err());
        assert_eq!(history.errors.len(), MAX_REFUSAL_OBSERVATIONS);
        for (index, error) in history.errors.iter().enumerate() {
            assert!(
                matches!(error, ThreadStoreError::Internal { message } if message == &index.to_string())
            );
        }
        Ok(())
    }

    #[test]
    fn canceled_observation_reservation_cannot_be_reused() -> ThreadStoreResult<()> {
        let mut history = Refusals::default();
        for _ in 0..MAX_REFUSAL_OBSERVATIONS {
            history.reserve()?;
        }
        assert!(history.errors.is_empty());
        assert!(history.reserve().is_err());
        // The admitted original result can still be captured without another reserve.
        let capacity = history.errors.capacity();
        history.record(ThreadStoreError::Internal {
            message: "original".into(),
        });
        assert_eq!(history.errors.capacity(), capacity);
        assert_eq!(history.errors.len(), 1);
        Ok(())
    }
}
