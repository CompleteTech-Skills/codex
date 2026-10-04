//! Original physical session ownership stays outside the mutation task future.
use codex_postgres_runtime::OriginalTransactionAcknowledgment;
use codex_postgres_runtime::OriginalTransactionOutcome;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::RetainedWriteSession;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

pub(crate) enum SessionLoanError {
    Owner(ThreadStoreError),
    Acquisition(codex_postgres_runtime::PoolError),
}

/// Capacity is reserved before dispatch. A contradictory restoration retains both
/// originals and refuses later admission instead of overwriting/dropping either.
struct OriginalSlot<T>(Mutex<Vec<T>>);
impl<T> OriginalSlot<T> {
    fn new() -> ThreadStoreResult<Arc<Self>> {
        let mut originals = Vec::new();
        originals
            .try_reserve_exact(2)
            .map_err(|_| refusal("original session slot capacity unavailable"))?;
        Ok(Arc::new(Self(Mutex::new(originals))))
    }
    fn take(&self) -> ThreadStoreResult<Option<T>> {
        let mut originals = self
            .0
            .lock()
            .map_err(|_| refusal("original session slot poisoned"))?;
        if originals.len() > 1 {
            return Err(refusal("contradictory original session ownership"));
        }
        Ok(originals.pop())
    }
}
struct OriginalLoan<T> {
    slot: Arc<OriginalSlot<T>>,
    original: Option<T>,
}
impl<T> Drop for OriginalLoan<T> {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            self.slot
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(original);
        }
    }
}

pub(crate) struct OriginalSessionOwner {
    pool: Arc<PostgresPool>,
    slot: Arc<OriginalSlot<RetainedWriteSession>>,
    gate: Arc<Semaphore>,
    acknowledged: Mutex<Option<OriginalTransactionAcknowledgment>>,
}
/// Drop order restores the actual connection before exclusive admission reopens.
pub(crate) struct SessionLoan {
    original: OriginalLoan<RetainedWriteSession>,
    _owner: Arc<OriginalSessionOwner>,
    _permit: OwnedSemaphorePermit,
}
impl OriginalSessionOwner {
    pub(crate) fn new(pool: Arc<PostgresPool>) -> ThreadStoreResult<Arc<Self>> {
        Ok(Arc::new(Self {
            pool,
            slot: OriginalSlot::new()?,
            gate: Arc::new(Semaphore::new(1)),
            acknowledged: Mutex::new(None),
        }))
    }
    #[cfg(test)]
    pub(crate) fn captured_ack(&self) -> ThreadStoreResult<OriginalTransactionAcknowledgment> {
        self.acknowledged
            .lock()
            .map_err(|_| refusal("ack poisoned"))?
            .clone()
            .ok_or_else(|| refusal("actual acknowledgment missing"))
    }
    pub(crate) fn prepare_next_dispatch(&self) -> ThreadStoreResult<()> {
        if !self
            .slot
            .0
            .lock()
            .map_err(|_| refusal("original session slot poisoned"))?
            .is_empty()
        {
            return Err(refusal("prior original connection is still retained"));
        }
        self.acknowledged
            .lock()
            .map_err(|_| refusal("original acknowledgment slot poisoned"))?
            .take();
        Ok(())
    }
    pub(crate) fn belongs_to(&self, pool: &Arc<PostgresPool>) -> bool {
        Arc::ptr_eq(&self.pool, pool)
    }
    pub(crate) async fn loan(self: &Arc<Self>) -> Result<SessionLoan, SessionLoanError> {
        let permit = Arc::clone(&self.gate)
            .acquire_owned()
            .await
            .map_err(|_| SessionLoanError::Owner(refusal("original session admission closed")))?;
        let original = match self.slot.take().map_err(SessionLoanError::Owner)? {
            Some(original) => original,
            None => self
                .pool
                .acquire_retained_write()
                .await
                .map_err(SessionLoanError::Acquisition)?,
        };
        // No await/allocation/fallible step between acquisition and restoring guard.
        Ok(SessionLoan {
            original: OriginalLoan {
                slot: Arc::clone(&self.slot),
                original: Some(original),
            },
            _owner: Arc::clone(self),
            _permit: permit,
        })
    }
}
pub(crate) enum OriginalRecoveryError {
    Owner(ThreadStoreError),
    Database(sqlx::Error),
}
/// Constructed only by status observation on the existing original connection.
pub(crate) struct ResolvedOriginalSession {
    loan: Option<SessionLoan>,
    outcome: OriginalTransactionOutcome,
}
impl ResolvedOriginalSession {
    pub(crate) fn committed(&self) -> bool {
        matches!(self.outcome, OriginalTransactionOutcome::Committed)
    }
    pub(crate) fn return_acknowledged(mut self) {
        if let Some(loan) = self.loan.as_mut() {
            loan.return_acknowledged();
        }
    }
}
impl OriginalSessionOwner {
    pub(crate) async fn reconcile(
        self: &Arc<Self>,
    ) -> Result<ResolvedOriginalSession, OriginalRecoveryError> {
        let permit = Arc::clone(&self.gate).acquire_owned().await.map_err(|_| {
            OriginalRecoveryError::Owner(refusal("original recovery admission closed"))
        })?;
        let acknowledged = self
            .acknowledged
            .lock()
            .map_err(|_| {
                OriginalRecoveryError::Owner(refusal("original acknowledgment slot poisoned"))
            })?
            .clone();
        if let Some(acknowledged) = acknowledged {
            return Ok(ResolvedOriginalSession {
                loan: None,
                outcome: if acknowledged.committed() {
                    OriginalTransactionOutcome::Committed
                } else {
                    OriginalTransactionOutcome::Aborted
                },
            });
        }
        let original = self
            .slot
            .take()
            .map_err(OriginalRecoveryError::Owner)?
            .ok_or_else(|| {
                OriginalRecoveryError::Owner(refusal("original recovery connection unavailable"))
            })?;
        // Reconciliation never reacquires or substitutes another pool connection.
        let mut loan = SessionLoan {
            original: OriginalLoan {
                slot: Arc::clone(&self.slot),
                original: Some(original),
            },
            _owner: Arc::clone(self),
            _permit: permit,
        };
        if let Some(acknowledged) = loan
            .session()
            .map_err(OriginalRecoveryError::Owner)?
            .original_acknowledgment()
        {
            return Ok(ResolvedOriginalSession {
                loan: Some(loan),
                outcome: if acknowledged.committed() {
                    OriginalTransactionOutcome::Committed
                } else {
                    OriginalTransactionOutcome::Aborted
                },
            });
        }
        let outcome = loan
            .session()
            .map_err(OriginalRecoveryError::Owner)?
            .observe_original_transaction()
            .await
            .map_err(OriginalRecoveryError::Database)?;
        Ok(ResolvedOriginalSession {
            loan: Some(loan),
            outcome,
        })
    }
}
impl SessionLoan {
    pub(crate) fn session(&mut self) -> ThreadStoreResult<&mut RetainedWriteSession> {
        self.original
            .original
            .as_mut()
            .ok_or_else(|| refusal("original session loan is empty"))
    }
    /// Only genuine commit/rollback acknowledgment callers use this. Returning to
    /// its original pool is NOT a socket-closed or all-writers-drained receipt.
    pub(crate) fn return_acknowledged(&mut self) {
        if let Some(original) = self.original.original.as_ref() {
            if let Some(acknowledged) = original.original_acknowledgment() {
                let mut saved = self
                    ._owner
                    .acknowledged
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if saved
                    .as_ref()
                    .is_some_and(|previous| !previous.shares_original_transaction(&acknowledged))
                {
                    return;
                }
                *saved = Some(acknowledged);
            }
        }
        self.original.original.take();
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
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    struct Original(Arc<AtomicUsize>);
    impl Drop for Original {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn unwind_restores_original_before_owner_release() -> ThreadStoreResult<()> {
        let slot = OriginalSlot::new()?;
        let dropped = Arc::new(AtomicUsize::new(0));
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _loan = OriginalLoan {
                slot: Arc::clone(&slot),
                original: Some(Original(Arc::clone(&dropped))),
            };
            panic!("original loan control panic");
        }))
        .is_err();
        let restored = slot.take()?;
        let live = dropped.load(Ordering::SeqCst) == 0;
        drop(restored);
        if !failed || !live || dropped.load(Ordering::SeqCst) != 1 {
            return Err(refusal("original unwind restoration failed"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn actual_task_cancellation_restores_original_without_replacement()
    -> ThreadStoreResult<()> {
        let slot = OriginalSlot::new()?;
        let dropped = Arc::new(AtomicUsize::new(0));
        let original = OriginalLoan {
            slot: Arc::clone(&slot),
            original: Some(Original(Arc::clone(&dropped))),
        };
        let (ready, observed) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _original = original;
            let _ = ready.send(());
            std::future::pending::<()>().await;
        });
        let ready = observed.await.is_ok();
        let id = task.id();
        task.abort();
        let result = task.await;
        let same = result
            .as_ref()
            .is_err_and(|error| error.is_cancelled() && error.id() == id);
        let restored = slot.take()?;
        let live = dropped.load(Ordering::SeqCst) == 0;
        drop(restored);
        if !ready || !same || !live || dropped.load(Ordering::SeqCst) != 1 {
            return Err(refusal("same original canceled task restoration failed"));
        }
        Ok(())
    }
}
