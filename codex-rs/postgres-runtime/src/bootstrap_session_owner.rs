//! Same physical bootstrap session retained outside the DDL task.
//! No Home, activation, generation, or migration-selection authority is supplied.
use crate::BootstrapError;
use crate::PostgresPool;
use crate::RetainedWriteSession;
use sqlx::Acquire;
use sqlx::PgConnection;
use sqlx::Postgres;
use sqlx::Transaction;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

pub struct BootstrapSessionOwner {
    pool: Arc<PostgresPool>,
    state: Mutex<State>,
    join: Arc<Semaphore>,
    #[cfg(test)]
    stamp_probe: Mutex<Option<Arc<StampProbe>>>,
    #[cfg(test)]
    commit_probe: Mutex<Option<Arc<StampProbe>>>,
    #[cfg(test)]
    outcome_probe: Mutex<Option<Arc<StampProbe>>>,
}
struct State {
    sealed: bool,
    cleanup_complete: bool,
    session: Option<RetainedWriteSession>,
    started: bool,
    acquisition: Option<crate::PoolError>,
    xid: Option<Arc<String>>,
    outcome: Option<bool>,
    causes: [Cause; 8],
}
#[derive(Default)]
struct Cause {
    reserved: bool,
    original: Option<Arc<dyn std::error::Error + Send + Sync>>,
}
impl BootstrapSessionOwner {
    pub fn new(pool: Arc<PostgresPool>) -> Arc<Self> {
        Arc::new(Self {
            pool,
            join: Arc::new(Semaphore::new(1)),
            #[cfg(test)]
            stamp_probe: Mutex::new(None),
            #[cfg(test)]
            commit_probe: Mutex::new(None),
            #[cfg(test)]
            outcome_probe: Mutex::new(None),
            state: Mutex::new(State {
                sealed: false,
                cleanup_complete: false,
                session: None,
                started: false,
                acquisition: None,
                xid: None,
                outcome: None,
                causes: std::array::from_fn(|_| Cause::default()),
            }),
        })
    }
    pub fn original_pool(&self) -> &Arc<PostgresPool> {
        &self.pool
    }
    /// Original diagnostic causes only; these cannot authorize SQL or cleanup.
    pub fn original_acquisition_failure(&self) -> Option<crate::PoolError> {
        self.state.lock().ok().and_then(|state| state.acquisition)
    }
    pub fn original_driver_causes(
        &self,
    ) -> Option<[Option<Arc<dyn std::error::Error + Send + Sync>>; 8]> {
        self.state
            .lock()
            .ok()
            .map(|state| std::array::from_fn(|index| state.causes[index].original.clone()))
    }
    pub fn observed_original_outcome(&self) -> Option<(Arc<String>, bool)> {
        self.state
            .lock()
            .ok()
            .and_then(|state| Some((state.xid.clone()?, state.outcome?)))
    }
    pub(crate) async fn loan(self: &Arc<Self>) -> Result<SessionLoan, BootstrapError> {
        let permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|_| BootstrapError::Migration)?;
        let needs_acquire = {
            let mut state = self.state.lock().map_err(|_| BootstrapError::Migration)?;
            if state.sealed || state.xid.is_some() || state.started && state.session.is_none() {
                return Err(BootstrapError::Migration);
            }
            let needs = !state.started;
            state.started = true;
            needs
        };
        if needs_acquire {
            match self.pool.acquire_retained_write().await {
                Ok(session) => {
                    let (mut state, poison) = match self.state.lock() {
                        Ok(state) => (state, false),
                        Err(cause) => (cause.into_inner(), true),
                    };
                    state.session = Some(session); // Before any poison refusal.
                    if poison {
                        return Err(BootstrapError::Migration);
                    }
                }
                Err(original) => {
                    self.state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .acquisition = Some(original);
                    return Err(BootstrapError::Connection(original));
                }
            }
        }
        let session = self
            .state
            .lock()
            .map_err(|_| BootstrapError::Migration)?
            .session
            .take();
        Ok(SessionLoan {
            original: Arc::clone(self),
            session,
            _permit: permit,
        })
    }
    async fn query<T>(
        self: &Arc<Self>,
        future: impl std::future::Future<Output = Result<T, sqlx::Error>>,
        cleanup: bool,
    ) -> Result<T, BootstrapError> {
        let index = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            if poison && !cleanup {
                return Err(BootstrapError::Migration);
            }
            let index = state
                .causes
                .iter()
                .position(|cause| !cause.reserved && cause.original.is_none())
                .ok_or(BootstrapError::Migration)?;
            state.causes[index].reserved = true; // Before actual SQL future first poll.
            index
        };
        let reservation = DdlReservation {
            original: Arc::clone(self),
            index,
            interrupted: Arc::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "original bootstrap SQL future interrupted; outcome unresolved",
            )),
        };
        match future.await {
            Ok(value) => {
                self.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .causes[index]
                    .reserved = false;
                reservation.success();
                Ok(value)
            }
            Err(original) => {
                let classified = crate::bootstrap::classify_sqlx(&original);
                self.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .causes[index]
                    .original = Some(Arc::new(original));
                Err(classified)
            }
        }
    }
    pub(crate) fn reserve_ddl(self: &Arc<Self>) -> Result<DdlReservation, BootstrapError> {
        let mut state = self.state.lock().map_err(|_| BootstrapError::Migration)?;
        let index = state
            .causes
            .iter()
            .position(|cause| !cause.reserved && cause.original.is_none())
            .ok_or(BootstrapError::Migration)?;
        state.causes[index].reserved = true;
        Ok(DdlReservation {
            original: Arc::clone(self),
            index,
            interrupted: Arc::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "original bootstrap DDL future interrupted; outcome unresolved",
            )),
        })
    }
    pub(crate) async fn stamp(
        self: &Arc<Self>,
        transaction: &mut Transaction<'_, Postgres>,
    ) -> Result<(), BootstrapError> {
        let stamp = self
            .query(
                sqlx::query_scalar::<_, String>("SELECT pg_catalog.pg_current_xact_id()::text")
                    .fetch_one(&mut **transaction),
                false,
            )
            .await?;
        {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            if state.xid.is_some() {
                return Err(BootstrapError::Migration);
            }
            state.xid = Some(Arc::new(stamp)); // Capture returned native fact BEFORE poison refusal.
            if poison {
                return Err(BootstrapError::Migration);
            }
        }
        #[cfg(test)]
        {
            let probe = self.stamp_probe.lock().ok().and_then(|probe| probe.clone());
            if let Some(probe) = probe {
                probe.entered.notify_one();
                probe.release.notified().await;
            }
        }
        Ok(())
    }
    pub(crate) async fn commit(
        self: &Arc<Self>,
        mut transaction: Transaction<'_, Postgres>,
    ) -> Result<(), BootstrapError> {
        let stamp = self
            .state
            .lock()
            .map_err(|_| BootstrapError::Migration)?
            .xid
            .clone()
            .ok_or(BootstrapError::Migration)?;
        #[cfg(test)]
        self.wait_probe(&self.commit_probe).await;
        self.query(sqlx::query("COMMIT").execute(&mut *transaction), false)
            .await?;
        let status = self
            .query(
                sqlx::query_scalar::<_, Option<String>>(
                    "SELECT pg_catalog.pg_xact_status($1::pg_catalog.xid8)",
                )
                .bind(stamp.as_str())
                .fetch_one(&mut *transaction),
                false,
            )
            .await?;
        let committed = match status.as_deref() {
            Some("committed") => true,
            Some("aborted") => false,
            _ => return Err(BootstrapError::Migration),
        };
        // External actual outcome precedes bookkeeping, pool return, task result.
        let poison = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.outcome = Some(committed); // Retain actual returned status BEFORE refusal.
            poison
        };
        if poison {
            return Err(BootstrapError::Migration);
        }
        #[cfg(test)]
        self.wait_probe(&self.outcome_probe).await;
        self.query(transaction.rollback(), false).await?;
        if !committed {
            return Err(BootstrapError::Migration);
        }
        Ok(())
    }
    #[cfg(test)]
    async fn wait_probe(&self, slot: &Mutex<Option<Arc<StampProbe>>>) {
        let probe = slot.lock().ok().and_then(|probe| probe.clone());
        if let Some(probe) = probe {
            probe.entered.notify_one();
            probe.release.notified().await;
        }
    }
    /// Seal new loans, reconcile the SAME session, then release it to its own pool.
    /// SDK/task caller must retain this owner through cancellation and uncertainty.
    pub async fn reconcile_original(
        self: &Arc<Self>,
    ) -> Result<BootstrapSessionCleanup, BootstrapSessionRefusal> {
        let refusal = || BootstrapSessionRefusal {
            original: Arc::clone(self),
        };
        let poison = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.sealed = true;
            poison
        };
        let permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|_| refusal())?;
        let (session, stamp, complete) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.session.take(),
                state.xid.clone(),
                state.cleanup_complete,
            )
        };
        let mut loan = SessionLoan {
            original: Arc::clone(self),
            session,
            _permit: permit,
        };
        if let Some(session) = loan.session.as_mut() {
            if !complete {
                self.query(
                    sqlx::query("ROLLBACK").execute(session.bootstrap_connection()),
                    true,
                )
                .await
                .map_err(|_| refusal())?;
                if let Some(stamp) = stamp {
                    let status = self
                        .query(
                            sqlx::query_scalar::<_, Option<String>>(
                                "SELECT pg_catalog.pg_xact_status($1::pg_catalog.xid8)",
                            )
                            .bind(stamp.as_str())
                            .fetch_one(session.bootstrap_connection()),
                            true,
                        )
                        .await
                        .map_err(|_| refusal())?;
                    let committed = match status.as_deref() {
                        Some("committed") => true,
                        Some("aborted") => false,
                        _ => return Err(refusal()),
                    };
                    self.state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .outcome = Some(committed);
                }
            }
        } else if stamp.is_some() && !complete {
            return Err(refusal());
        }
        // Without a returned session, no DDL could start: only loan() supplies the
        // private connection and stamp() must return before either real DDL body.
        let refused = {
            let (mut state, final_poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.cleanup_complete = true; // Actual ACK BEFORE return to original pool.
            poison || final_poison
        };
        drop(loan.session.take());
        Ok(BootstrapSessionCleanup {
            original: Arc::clone(self),
            refused,
        })
    }
}
pub(crate) struct SessionLoan {
    original: Arc<BootstrapSessionOwner>,
    session: Option<RetainedWriteSession>,
    _permit: OwnedSemaphorePermit,
}
impl SessionLoan {
    pub(crate) fn connection(&mut self) -> Result<&mut PgConnection, BootstrapError> {
        self.session
            .as_mut()
            .map(RetainedWriteSession::bootstrap_connection)
            .ok_or(BootstrapError::Migration)
    }
    pub(crate) async fn begin(&mut self) -> Result<Transaction<'_, Postgres>, BootstrapError> {
        let original = Arc::clone(&self.original);
        original.query(self.connection()?.begin(), false).await
    }
}
impl Drop for SessionLoan {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            self.original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .session = Some(session);
        } // Same original restored BEFORE its serialized permit releases.
    }
}
pub struct BootstrapSessionCleanup {
    original: Arc<BootstrapSessionOwner>,
    refused: bool,
}
impl BootstrapSessionCleanup {
    pub fn covers_pool(&self, pool: &Arc<PostgresPool>) -> bool {
        Arc::ptr_eq(self.original.original_pool(), pool)
    }
    pub fn refused(&self) -> bool {
        self.refused
    }
}
pub struct BootstrapSessionRefusal {
    pub original: Arc<BootstrapSessionOwner>,
}
impl std::fmt::Debug for BootstrapSessionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BootstrapSessionRefusal([original retained])")
    }
}
impl std::fmt::Display for BootstrapSessionRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original bootstrap session unresolved")
    }
}
impl std::error::Error for BootstrapSessionRefusal {}

pub(crate) struct DdlReservation {
    original: Arc<BootstrapSessionOwner>,
    index: usize,
    interrupted: Arc<std::io::Error>,
}
impl Drop for DdlReservation {
    fn drop(&mut self) {
        let mut state = self
            .original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let cause = &mut state.causes[self.index];
        if cause.reserved && cause.original.is_none() {
            cause.original =
                Some(Arc::clone(&self.interrupted) as Arc<dyn std::error::Error + Send + Sync>);
        }
    }
}
impl DdlReservation {
    pub(crate) fn failure<E: std::error::Error + Send + Sync + 'static>(&self, original: E) {
        let mut state = self
            .original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.causes[self.index].original.is_none() {
            state.causes[self.index].original = Some(Arc::new(original));
        }
    }
    pub(crate) fn success(self) {
        self.original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .causes[self.index]
            .reserved = false;
    }
}

#[cfg(test)]
#[derive(Default)]
struct StampProbe {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[cfg(test)]
#[path = "bootstrap_session_owner_tests.rs"]
mod tests;
