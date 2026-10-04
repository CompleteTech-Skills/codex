//! Original Runtime origin observations. No exclusive permit.
//! Exterior must retain this frame before the existing retained factory polls.
use crate::administration_close::OriginalAdministrativeClose;
use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
use codex_storage_authority::RemotePostgresProfile;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgRow;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

pub struct RuntimeNativeOrigin {
    original_profile: Arc<RemotePostgresProfile>,
    original_pool: Arc<PostgresPool>,
    pub(crate) system_identifier: String,
    pub(crate) database_oid: i64,
    pub(crate) login_oid: i64,
    pub(crate) login: String,
}
struct State {
    pool: Option<Arc<PostgresPool>>,
    rejected_pool: Option<Arc<PostgresPool>>,
    row: Option<PgRow>,
    native: Option<Arc<RuntimeNativeOrigin>>,
    acquisition: Option<PoolError>,
    constructor_failure: Option<crate::RemoteStorageError>,
    expected_login: Option<String>,
    query: Option<sqlx::Error>,
    refused: bool,
    constructor_dispatched: bool,
    constructor_task:
        Option<JoinHandle<Result<Arc<crate::RemoteStorage>, crate::RemoteStorageError>>>,
    constructor_returned: Option<Result<Arc<crate::RemoteStorage>, crate::RemoteStorageError>>,
    constructor_join_failure: Option<Arc<tokio::task::JoinError>>,
    dispatched: bool,
    sealed: bool,
    task: Option<JoinHandle<Result<Arc<RuntimeNativeOrigin>, ()>>>,
    terminal: Option<Result<Arc<RuntimeNativeOrigin>, ()>>,
    join_failure: Option<Arc<tokio::task::JoinError>>,
    collision: Option<JoinHandle<Result<Arc<RuntimeNativeOrigin>, ()>>>,
    runtime_failure: Option<tokio::runtime::TryCurrentError>,
    hold: Option<Arc<OriginalRuntimeOrigin>>,
    #[cfg(test)]
    constructor_probe: Option<Arc<ConstructorProbe>>,
}
/// Exact profile and SDK owner, not an endpoint comparison or caller tuple.
pub struct OriginalRuntimeOrigin {
    profile: Arc<RemotePostgresProfile>,
    state: Mutex<State>,
    session: Arc<AsyncMutex<Option<PoolConnection<Postgres>>>>,
    join: Arc<Semaphore>,
    constructor_join: Arc<Semaphore>,
    close: Arc<OriginalAdministrativeClose>,
}
pub struct RuntimeOriginRefusal {
    pub(crate) original: Arc<OriginalRuntimeOrigin>,
}
impl OriginalRuntimeOrigin {
    pub fn prepare(profile: Arc<RemotePostgresProfile>) -> Arc<Self> {
        Arc::new(Self {
            profile,
            session: Arc::new(AsyncMutex::new(None)),
            join: Arc::new(Semaphore::new(1)),
            constructor_join: Arc::new(Semaphore::new(1)),
            close: OriginalAdministrativeClose::new(),
            state: Mutex::new(State {
                pool: None,
                rejected_pool: None,
                row: None,
                native: None,
                acquisition: None,
                constructor_failure: None,
                expected_login: None,
                query: None,
                refused: false,
                constructor_dispatched: false,
                constructor_task: None,
                constructor_returned: None,
                constructor_join_failure: None,
                dispatched: false,
                sealed: false,
                task: None,
                terminal: None,
                join_failure: None,
                collision: None,
                runtime_failure: None,
                hold: None,
                #[cfg(test)]
                constructor_probe: None,
            }),
        })
    }
    pub fn original_profile(&self) -> &Arc<RemotePostgresProfile> {
        &self.profile
    }
    pub(crate) async fn run_constructor<F>(
        self: &Arc<Self>,
        body: F,
    ) -> Result<Arc<crate::RemoteStorage>, crate::RemoteStorageError>
    where
        F: std::future::Future<
                Output = Result<Arc<crate::RemoteStorage>, crate::RemoteStorageError>,
            > + Send
            + 'static,
    {
        let unavailable = || crate::RemoteStorageError::Connection(PoolError::Unavailable);
        let permit = Arc::clone(&self.constructor_join)
            .acquire_owned()
            .await
            .map_err(|_| unavailable())?;
        let task = {
            let mut state = self.state.lock().map_err(|_| unavailable())?;
            if state.sealed || state.constructor_join_failure.is_some() {
                return Err(unavailable());
            }
            if let Some(returned) = state.constructor_returned.as_ref() {
                return match returned {
                    Err(error) => Err(*error),
                    Ok(storage) if !state.refused => Ok(Arc::clone(storage)),
                    Ok(_) => Err(unavailable()),
                };
            }
            if !state.constructor_dispatched {
                let runtime = match tokio::runtime::Handle::try_current() {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        state.runtime_failure = Some(error);
                        state.refused = true;
                        return Err(unavailable());
                    }
                };
                state.constructor_dispatched = true;
                state.hold = Some(Arc::clone(self));
                let original = Arc::clone(self);
                state.constructor_task = Some(runtime.spawn(async move {
                    // Gate first credential/factory/body poll behind original JH registration.
                    {
                        let _registered = original
                            .state
                            .lock()
                            .unwrap_or_else(|error| error.into_inner());
                    }
                    body.await
                }));
            }
            state.constructor_task.take().ok_or_else(unavailable)?
        };
        let mut loan = RuntimeConstructorLoan {
            original: Arc::clone(self),
            task: Some(task),
            _permit: permit,
        };
        self.join_constructor_loan(&mut loan)
            .await
            .map_err(|_| unavailable())?;
        let state = self.state.lock().map_err(|_| unavailable())?;
        match state.constructor_returned.as_ref() {
            Some(Err(error)) => Err(*error),
            Some(Ok(storage)) if !state.refused && state.constructor_join_failure.is_none() => {
                Ok(Arc::clone(storage))
            }
            _ => Err(unavailable()),
        }
    }
    async fn join_constructor_loan(&self, loan: &mut RuntimeConstructorLoan) -> Result<(), ()> {
        let returned = loan.task.as_mut().ok_or(())?.await;
        let (mut state, poison) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        match returned {
            Ok(result) => state.constructor_returned = Some(result),
            Err(error) => {
                state.constructor_join_failure = Some(Arc::new(error));
                state.refused = true;
            }
        }
        loan.task = None; // SAME raw result/JoinError is exterior first.
        state.refused |= poison;
        Ok(())
    }
    async fn seal_and_join_constructor(self: &Arc<Self>) -> Result<(), RuntimeOriginRefusal> {
        let refusal = || RuntimeOriginRefusal {
            original: Arc::clone(self),
        };
        let permit = Arc::clone(&self.constructor_join)
            .acquire_owned()
            .await
            .map_err(|_| refusal())?;
        let task = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(error) => (error.into_inner(), true),
            };
            state.refused |= poison;
            state.sealed = true;
            state.constructor_task.take()
        };
        if let Some(task) = task {
            let mut loan = RuntimeConstructorLoan {
                original: Arc::clone(self),
                task: Some(task),
                _permit: permit,
            };
            self.join_constructor_loan(&mut loan)
                .await
                .map_err(|_| refusal())?;
        }
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.constructor_task.is_some()
            || (state.constructor_dispatched
                && state.constructor_returned.is_none()
                && state.constructor_join_failure.is_none())
        {
            return Err(refusal());
        }
        Ok(()) // Physical constructor terminal only, not admission/native outcome.
    }
    #[cfg(test)]
    pub(crate) async fn hold_before_factory(&self) {
        let probe = {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .constructor_probe
                .clone()
        };
        if let Some(probe) = probe {
            if let Some(ready) = probe
                .ready
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _delivered = ready.send(tokio::task::id());
            }
            let _release = probe.release.acquire().await;
        }
    }
    pub(crate) fn bind_expected_login(&self, login: String) -> Result<(), PoolError> {
        let mut state = self.state.lock().map_err(|_| PoolError::Unavailable)?;
        if state.expected_login.is_some() || state.dispatched || state.sealed {
            return Err(PoolError::Unavailable);
        }
        state.expected_login = Some(login);
        Ok(())
    }
    pub(crate) fn record_constructor_failure(&self, error: crate::RemoteStorageError) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.constructor_failure.is_none() {
            state.constructor_failure = Some(error);
        }
        state.refused = true;
    }
    /// Actual captured native fact only; never caller-supplied or exclusive access.
    pub fn original_native_origin(&self) -> Option<Arc<RuntimeNativeOrigin>> {
        let state = self.state.lock().ok()?;
        if state.refused {
            None
        } else {
            state.native.as_ref().map(Arc::clone)
        }
    }
    pub fn captured_original_pool(&self) -> Option<Arc<PostgresPool>> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pool
            .as_ref()
            .map(Arc::clone)
    }
    /// Same frame's actual native+SDK task results; no remote transaction outcome.
    pub fn known_original_closed(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let constructor_terminal = !state.constructor_dispatched
            || (state.constructor_task.is_none()
                && (state.constructor_returned.is_some()
                    || state.constructor_join_failure.is_some()));
        constructor_terminal && self.close.physical_terminal()
    }
    /// Called by the real retained factory before its first acquisition/SQL.
    /// Incoming actual pool is retained even when poisoned state refuses capture.
    pub(crate) fn capture_pool(&self, pool: Arc<PostgresPool>) -> Result<(), PoolError> {
        let (mut state, poisoned) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        if let Some(original) = state.pool.as_ref() {
            if !Arc::ptr_eq(original, &pool) {
                if state.rejected_pool.is_none() {
                    state.rejected_pool = Some(pool);
                }
                state.refused = true;
                return Err(PoolError::Unavailable);
            }
        } else {
            state.pool = Some(pool);
        }
        state.refused |= poisoned || state.sealed;
        if state.refused {
            Err(PoolError::Unavailable)
        } else {
            Ok(())
        }
    }
    /// Must run inside the SAME already-owned startup task, before schema or
    /// activation queries. The same stored query task is joined; no controller
    /// or grant is created.
    /// Privilege/query refusal retains actual driver cause and cannot be retried
    /// to replace the original observation. Caller cancellation is still a
    /// consumer-owned boundary; no origin is published without a complete row.
    pub(crate) async fn observe_before_schema(
        self: &Arc<Self>,
    ) -> Result<Arc<RuntimeNativeOrigin>, RuntimeOriginRefusal> {
        let refuse = || RuntimeOriginRefusal {
            original: Arc::clone(self),
        };
        let permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|_| refuse())?;
        let task = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(error) => (error.into_inner(), true),
            };
            state.refused |= poison;
            if state.join_failure.is_some() || (state.refused && !state.dispatched) {
                return Err(refuse());
            }
            if let Some(result) = state.terminal.as_ref() {
                if state.refused {
                    return Err(refuse());
                }
                return result.clone().map_err(|_| refuse());
            }
            if !state.dispatched {
                if state.sealed {
                    return Err(refuse());
                }
                let runtime = match tokio::runtime::Handle::try_current() {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        state.runtime_failure = Some(error);
                        state.refused = true;
                        return Err(refuse());
                    }
                };
                state.dispatched = true;
                state.hold = Some(Arc::clone(self));
                let body = Arc::clone(self);
                state.task = Some(runtime.spawn(async move {
                    // Sender retains this same mutex until the actual JH is saved.
                    {
                        let _registered =
                            body.state.lock().unwrap_or_else(|error| error.into_inner());
                    }
                    body.query_original().await.map_err(|_| ())
                }));
            }
            state.task.take().ok_or_else(refuse)?
        };
        let mut loan = RuntimeQueryLoan {
            original: Arc::clone(self),
            task: Some(task),
            _permit: permit,
        };
        let returned = loan.task.as_mut().ok_or_else(refuse)?.await;
        let (mut state, poison) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        match returned {
            Ok(result) => state.terminal = Some(result),
            Err(error) => {
                state.join_failure = Some(Arc::new(error));
                state.refused = true;
            }
        }
        // Raw original JoinError/result exterior BEFORE handle removal.
        loan.task = None;
        state.refused |= poison;
        if state.refused {
            return Err(refuse());
        }
        state
            .terminal
            .as_ref()
            .ok_or_else(refuse)?
            .clone()
            .map_err(|_| refuse())
    }
    /// Only after the SAME query JH terminal. Close its exact physical session
    /// before a max-one SDK pool can acquire for schema/activation checks.
    pub(crate) async fn reconcile_native_before_schema(
        self: &Arc<Self>,
    ) -> Result<(), RuntimeOriginRefusal> {
        let refuse = || RuntimeOriginRefusal {
            original: Arc::clone(self),
        };
        let _permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|_| refuse())?;
        let (pool, poisoned) = {
            let (state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(error) => (error.into_inner(), true),
            };
            if state.task.is_some() || (state.terminal.is_none() && state.join_failure.is_none()) {
                return Err(refuse());
            }
            (state.pool.as_ref().map(Arc::clone), poison)
        };
        {
            let mut session = Arc::clone(&self.session).lock_owned().await;
            self.close.seal_source(pool, &mut session);
        }
        let closed = self.close.finish_native_original().await;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.refused |= poisoned || self.state.is_poisoned();
        if !closed || state.refused {
            return Err(refuse());
        }
        Ok(())
    }
    /// Cleanup-only joins the exact captured native and SDK close tasks after
    /// original query terminal. Failed/panicked query still remains an owning
    /// refusal; physical closure cannot turn it into an origin observation.
    pub async fn close_original(self: &Arc<Self>) -> Result<(), RuntimeOriginRefusal> {
        let refusal = || RuntimeOriginRefusal {
            original: Arc::clone(self),
        };
        self.seal_and_join_constructor().await?;
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.sealed = true;
            if !state.dispatched && state.task.is_none() {
                state.terminal = Some(Err(())); // Sealed never-dispatched query, not native origin.
            }
        }
        // Restore/join the SAME stored original even after an actual query refusal.
        let _query_result = self.observe_before_schema().await;
        let _native_result = self.reconcile_native_before_schema().await;
        let terminal = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.task.is_none()
                && state.collision.is_none()
                && (state.terminal.is_some() || state.join_failure.is_some())
        };
        if !terminal {
            return Err(refusal());
        }
        let physically_closed = self.close.finish().await;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.refused |= self.state.is_poisoned();
        if !physically_closed || state.refused {
            return Err(refusal());
        }
        state.hold = None;
        Ok(())
    }
    async fn query_original(
        self: &Arc<Self>,
    ) -> Result<Arc<RuntimeNativeOrigin>, RuntimeOriginRefusal> {
        let refuse = || RuntimeOriginRefusal {
            original: Arc::clone(self),
        };
        let pool = {
            let state = self.state.lock().map_err(|_| refuse())?;
            if state.refused {
                return Err(refuse());
            }
            let pool = state.pool.as_ref().map(Arc::clone).ok_or_else(refuse)?;
            pool
        };
        // Own the exterior slot guard BEFORE acquiring: no await can separate
        // a returned physical connection from installation in its original slot.
        let mut original_session = Arc::clone(&self.session).lock_owned().await;
        if original_session.is_some() {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .refused = true;
            return Err(refuse());
        }
        let connection = match pool.acquire().await {
            Ok(connection) => connection,
            Err(error) => {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                state.acquisition = Some(error);
                state.refused = true;
                return Err(refuse());
            }
        };
        *original_session = Some(connection);
        let connection = original_session.as_mut().ok_or_else(refuse)?;
        let row = sqlx::query(
            "SELECT control.system_identifier::text AS system_identifier,
                    database.oid::bigint AS database_oid,
                    login.oid::bigint AS login_oid, session_user::text AS login
             FROM pg_catalog.pg_control_system() control
             JOIN pg_catalog.pg_database database
               ON database.datname = pg_catalog.current_database()
             JOIN pg_catalog.pg_roles login ON login.rolname = session_user",
        )
        .fetch_one(&mut **connection)
        .await;
        let (mut state, poisoned) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        // Returned native row/cause is exterior BEFORE decode/poison refusal.
        match row {
            Ok(row) => state.row = Some(row),
            Err(error) => {
                state.query = Some(error);
                state.refused = true;
            }
        }
        state.refused |= poisoned;
        if state.refused {
            return Err(refuse());
        }
        let row = state.row.as_ref().ok_or_else(refuse)?;
        let decoded = (|| -> Result<RuntimeNativeOrigin, sqlx::Error> {
            Ok(RuntimeNativeOrigin {
                original_profile: Arc::clone(&self.profile),
                original_pool: Arc::clone(&pool),
                system_identifier: row.try_get("system_identifier")?,
                database_oid: row.try_get("database_oid")?,
                login_oid: row.try_get("login_oid")?,
                login: row.try_get("login")?,
            })
        })();
        let native = match decoded {
            Ok(native) => native,
            Err(error) => {
                state.query = Some(error);
                state.refused = true;
                return Err(refuse());
            }
        };
        if native.system_identifier.is_empty()
            || !native
                .system_identifier
                .bytes()
                .all(|byte| byte.is_ascii_digit())
            || native.system_identifier.bytes().all(|byte| byte == b'0')
            || native.database_oid <= 0
            || native.login_oid <= 0
            || state.expected_login.as_deref() != Some(native.login.as_str())
        {
            state.refused = true;
            return Err(refuse());
        }
        let native = Arc::new(native);
        state.native = Some(Arc::clone(&native));
        Ok(native)
    }
}

struct RuntimeQueryLoan {
    original: Arc<OriginalRuntimeOrigin>,
    task: Option<JoinHandle<Result<Arc<RuntimeNativeOrigin>, ()>>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for RuntimeQueryLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            let mut state = self
                .original
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.task.is_none() {
                state.task = Some(task);
            } else {
                state.collision = Some(task);
                state.refused = true;
            }
        }
    }
}

impl RuntimeNativeOrigin {
    /// Exact original factory inputs only; no endpoint equality, historical
    /// enrollment or exclusive permission follows from this association.
    pub fn belongs_to_target(&self, profile: &Arc<RemotePostgresProfile>) -> bool {
        Arc::ptr_eq(&self.original_profile, profile)
    }
    pub fn belongs_to_pool(&self, pool: &Arc<PostgresPool>) -> bool {
        Arc::ptr_eq(&self.original_pool, pool)
    }

    pub fn system_identifier(&self) -> &str {
        &self.system_identifier
    }
    pub fn database_oid(&self) -> i64 {
        self.database_oid
    }
    pub fn login_oid(&self) -> i64 {
        self.login_oid
    }
    pub fn login(&self) -> &str {
        &self.login
    }
}
impl std::fmt::Debug for RuntimeOriginRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self
            .original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _original_causes = (
            &state.acquisition,
            &state.query,
            &state.constructor_failure,
            &state.runtime_failure,
            &state.join_failure,
            &state.constructor_join_failure,
            &state.rejected_pool,
        );
        formatter.write_str("RuntimeOriginRefused([original retained])")
    }
}
impl RuntimeOriginRefusal {
    pub fn original_owner(&self) -> &Arc<OriginalRuntimeOrigin> {
        &self.original
    }
}

struct RuntimeConstructorLoan {
    original: Arc<OriginalRuntimeOrigin>,
    task: Option<JoinHandle<Result<Arc<crate::RemoteStorage>, crate::RemoteStorageError>>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for RuntimeConstructorLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            let mut state = self
                .original
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // Same single constructor permit excludes another loan/capture.
            state.constructor_task = Some(task);
        }
    }
}

#[cfg(test)]
pub(crate) struct ConstructorProbe {
    ready: Mutex<Option<tokio::sync::oneshot::Sender<tokio::task::Id>>>,
    release: Semaphore,
}
#[cfg(test)]
#[path = "runtime_origin_tests.rs"]
mod tests;
