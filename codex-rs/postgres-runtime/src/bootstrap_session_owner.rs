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
#[cfg(test)]
type ControlTerminal = Result<Result<(), BootstrapError>, Arc<tokio::task::JoinError>>;

pub struct BootstrapSessionOwner {
    pool: Arc<PostgresPool>,
    diagnostic: Option<Arc<crate::bootstrap_diagnostic_session::DiagnosticSession>>,
    state: Mutex<State>,
    join: Arc<Semaphore>,
    #[cfg(test)]
    stamp_probe: Mutex<Option<Arc<StampProbe>>>,
    #[cfg(test)]
    commit_probe: Mutex<Option<Arc<StampProbe>>>,
    #[cfg(test)]
    outcome_probe: Mutex<Option<Arc<StampProbe>>>,
    #[cfg(test)]
    control_task: Mutex<Option<tokio::task::JoinHandle<Result<(), BootstrapError>>>>,
    #[cfg(test)]
    control_hold: Mutex<Option<Arc<BootstrapSessionOwner>>>,
    #[cfg(test)]
    control_blocker: Mutex<Option<sqlx::pool::PoolConnection<Postgres>>>,
    #[cfg(test)]
    control_terminal: Mutex<Option<ControlTerminal>>,
}
struct State {
    sealed: bool,
    cleanup_complete: bool,
    session: Option<RetainedWriteSession>,
    started: bool,
    acquisition: Option<crate::PoolError>,
    xid: Option<Arc<String>>,
    outcome: Option<bool>,
    native_origin: Option<crate::bootstrap_diagnostic_session::NativeOrigin>,
    native_status: Option<Option<String>>,
    control_privilege: Option<bool>,
    close: Option<Arc<crate::bootstrap_original_close::OriginalClose>>,
    causes: [Cause; 8],
}
#[derive(Default)]
struct Cause {
    reserved: bool,
    original: Option<Arc<dyn std::error::Error + Send + Sync>>,
}
impl BootstrapSessionOwner {
    pub fn new(pool: Arc<PostgresPool>) -> Arc<Self> {
        Self::with_diagnostic(pool, None)
    }
    pub fn new_recoverable(
        pool: Arc<PostgresPool>,
        diagnostic_pool: Arc<PostgresPool>,
    ) -> Arc<Self> {
        let aliases_original = Arc::ptr_eq(&pool, &diagnostic_pool);
        let selected_namespace_matches = pool.selected_search_path.is_some()
            && pool.selected_search_path == diagnostic_pool.selected_search_path;
        let owner = Self::with_diagnostic(
            pool,
            Some(crate::bootstrap_diagnostic_session::DiagnosticSession::new(
                diagnostic_pool,
            )),
        );
        if aliases_original || !selected_namespace_matches {
            owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .sealed = true;
        }
        owner
    }
    fn with_diagnostic(
        pool: Arc<PostgresPool>,
        diagnostic: Option<Arc<crate::bootstrap_diagnostic_session::DiagnosticSession>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool,
            diagnostic,
            join: Arc::new(Semaphore::new(1)),
            #[cfg(test)]
            stamp_probe: Mutex::new(None),
            #[cfg(test)]
            commit_probe: Mutex::new(None),
            #[cfg(test)]
            outcome_probe: Mutex::new(None),
            #[cfg(test)]
            control_task: Mutex::new(None),
            #[cfg(test)]
            control_hold: Mutex::new(None),
            #[cfg(test)]
            control_blocker: Mutex::new(None),
            #[cfg(test)]
            control_terminal: Mutex::new(None),
            state: Mutex::new(State {
                sealed: false,
                cleanup_complete: false,
                session: None,
                started: false,
                acquisition: None,
                xid: None,
                outcome: None,
                native_origin: None,
                native_status: None,
                control_privilege: None,
                close: None,
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
        if self.diagnostic.is_some() {
            let permitted = self.query(sqlx::query_scalar::<_, bool>(
                "SELECT pg_catalog.has_function_privilege(CURRENT_USER, 'pg_catalog.pg_control_system()', 'EXECUTE')",
            ).fetch_one(&mut **transaction), false).await?;
            let poison = {
                let (mut state, poison) = match self.state.lock() {
                    Ok(state) => (state, false),
                    Err(cause) => (cause.into_inner(), true),
                };
                state.control_privilege = Some(permitted);
                poison
            };
            if poison {
                return Err(BootstrapError::Migration);
            }
            if !permitted {
                return Err(BootstrapError::Privilege);
            }
            let origin = self
                .query(
                    crate::bootstrap_diagnostic_session::NativeOrigin::capture(&mut **transaction),
                    false,
                )
                .await?;
            let poison = {
                let (mut state, poison) = match self.state.lock() {
                    Ok(state) => (state, false),
                    Err(cause) => (cause.into_inner(), true),
                };
                state.native_origin = Some(origin);
                poison
            };
            if poison {
                return Err(BootstrapError::Migration);
            }
        }
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
                let backend = self
                    .query(
                        sqlx::query_scalar::<_, i32>("SELECT pg_catalog.pg_backend_pid()")
                            .fetch_one(&mut **transaction),
                        false,
                    )
                    .await?;
                *probe
                    .backend
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(backend);
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
        let prior_close = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .close
            .clone();
        if prior_close.is_some() {
            self.recover_broken(&mut loan).await?;
            return Ok(BootstrapSessionCleanup {
                original: Arc::clone(self),
                refused: poison || self.state.is_poisoned(),
            });
        }
        if let Some(session) = loan.session.as_mut() {
            if !complete {
                let rollback = self
                    .query(
                        sqlx::query("ROLLBACK").execute(session.bootstrap_connection()),
                        true,
                    )
                    .await;
                if rollback.is_err() {
                    self.recover_broken(&mut loan).await?;
                    return Ok(BootstrapSessionCleanup {
                        original: Arc::clone(self),
                        refused: poison || self.state.is_poisoned(),
                    });
                }
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
                        .await;
                    let status = match status {
                        Ok(status) => status,
                        Err(_) => {
                            self.recover_broken(&mut loan).await?;
                            return Ok(BootstrapSessionCleanup {
                                original: Arc::clone(self),
                                refused: poison || self.state.is_poisoned(),
                            });
                        }
                    };
                    self.state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .native_status = Some(status.clone());
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
        if let Some(diagnostic) = &self.diagnostic {
            diagnostic.close_original().await.map_err(|cause| {
                let _ = &cause.original;
                refusal()
            })?;
        }
        drop(loan.session.take());
        Ok(BootstrapSessionCleanup {
            original: Arc::clone(self),
            refused: refused || self.state.is_poisoned(),
        })
    }
    #[cfg(test)]
    async fn control_close_original_and_observe(
        self: &Arc<Self>,
    ) -> Result<BootstrapSessionCleanup, BootstrapSessionRefusal> {
        let refusal = || BootstrapSessionRefusal {
            original: Arc::clone(self),
        };
        let permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|_| refusal())?;
        let session = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.sealed = true;
            state.session.take()
        };
        let mut loan = SessionLoan {
            original: Arc::clone(self),
            session,
            _permit: permit,
        };
        self.recover_broken(&mut loan).await?;
        Ok(BootstrapSessionCleanup {
            original: Arc::clone(self),
            refused: self.state.is_poisoned(),
        })
    }
    async fn recover_broken(
        self: &Arc<Self>,
        loan: &mut SessionLoan,
    ) -> Result<(), BootstrapSessionRefusal> {
        let refusal = || BootstrapSessionRefusal {
            original: Arc::clone(self),
        };
        let (origin, xid, prior_close) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.native_origin.clone(),
                state.xid.clone(),
                state.close.clone(),
            )
        };
        let diagnostic = self.diagnostic.as_ref().ok_or_else(refusal)?;
        let xid = xid.ok_or_else(refusal)?;
        let close = match prior_close {
            Some(close) => close,
            None => {
                let original = loan.session.take().ok_or_else(refusal)?;
                let close = crate::bootstrap_original_close::OriginalClose::capture(original);
                self.state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .close = Some(Arc::clone(&close));
                close
            }
        };
        match close.finish().await {
            Ok(receipt) if receipt.covers_pool(&self.pool) => {}
            Err(cause) => {
                let _ = &cause.original;
                return Err(refusal());
            }
            _ => return Err(refusal()),
        }
        // The server may need actual frontend shutdown to abort an in-progress
        // original transaction. Physical close itself is never an xid outcome.
        if !diagnostic.has_original_observation(&xid) {
            let observed = diagnostic
                .observe(&origin.ok_or_else(refusal)?, &xid)
                .await
                .map_err(|cause| {
                    let _ = &cause.original;
                    refusal()
                })?;
            if !Arc::ptr_eq(&observed.original_diagnostic, diagnostic)
                || !Arc::ptr_eq(&observed.xid, &xid)
            {
                return Err(refusal());
            }
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.outcome = Some(observed.committed); // Native same-instance original-xid fact, not driver ACK.
        }
        diagnostic.close_original().await.map_err(|cause| {
            let _ = &cause.original;
            refusal()
        })?;
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cleanup_complete = true;
        Ok(())
    }
    /// Returned native status associated with the same immutable original xid.
    /// An observed NULL remains distinct from no status query and grants no ACK.
    pub fn original_native_status(&self) -> Option<(Arc<String>, Option<String>)> {
        let state = self.state.lock().ok()?;
        Some((state.xid.clone()?, state.native_status.clone()?))
    }
    pub fn diagnostic_pool(&self) -> Option<&Arc<PostgresPool>> {
        self.diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.original_pool())
    }
    /// Observed function access only; never a grant or SQL cleanup capability.
    pub fn original_control_privilege(&self) -> Option<bool> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.control_privilege)
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
    backend: Mutex<Option<i32>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[cfg(test)]
#[path = "bootstrap_session_owner_tests.rs"]
mod tests;
