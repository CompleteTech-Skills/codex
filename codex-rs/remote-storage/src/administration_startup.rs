//! Inactive original administrative startup/session observation owner.
//! No credential retirement, role mutation, drain or exclusive permit is exposed.

use crate::administration_close::OriginalAdministrativeClose;
use crate::administration_settings::AdministrativeFactoryError;
use crate::administration_settings::administrative_settings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::RemoteAdministrationInput;
use sqlx::PgConnection;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgRow;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

pub struct AdministrativeNativeObservation {
    pub system_identifier: String,
    pub database_oid: i64,
    pub backend_pid: i32,
    pub login: String,
    pub can_create_roles: bool,
    pub can_administer_original_roles: bool,
    pub can_signal_backends: bool,
    pub can_read_all_backend_stats: bool,
}

enum Cause {
    Factory(AdministrativeFactoryError),
    Acquisition(PoolError),
    Query(sqlx::Error),
    Runtime(tokio::runtime::TryCurrentError),
    RefusedNativeFacts,
}

type Outcome = Result<Arc<AdministrativeNativeObservation>, Arc<Cause>>;

struct State {
    pool: Option<Arc<PostgresPool>>,
    task: Option<JoinHandle<Outcome>>,
    returned: Option<Outcome>,
    join_failure: Option<Arc<tokio::task::JoinError>>,
    raw_role_row: Option<PgRow>,
    raw_identity_row: Option<PgRow>,
    poison_seen: bool,
    hold: Option<Arc<OriginalAdministrativeStartup>>,
}

pub struct OriginalAdministrativeStartup {
    input: Arc<RemoteAdministrationInput>,
    state: Mutex<State>,
    // Owned guard restores to this exterior original slot on panic/cancellation.
    session: Arc<AsyncMutex<Option<PoolConnection<Postgres>>>>,
    join: Arc<Semaphore>,
    close: Arc<OriginalAdministrativeClose>,
}

pub struct AdministrativeStartupRefusal {
    pub original: Arc<OriginalAdministrativeStartup>,
}

impl AdministrativeStartupRefusal {
    /// Static classification only. Actual original causes stay in the owned frame;
    /// driver text, credential names, endpoints and SQL are never formatted here.
    pub fn diagnostic_kind(&self) -> &'static str {
        let state = self
            .original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.join_failure.is_some() {
            return "administrative_task_join_failed";
        }
        match state.returned.as_ref() {
            Some(Err(cause)) => match cause.as_ref() {
                Cause::Factory(error) => match error {
                    AdministrativeFactoryError::Credential(original) => {
                        let _retained = original;
                        "administrative_credential_refused"
                    }
                    AdministrativeFactoryError::Driver(original) => {
                        let _retained = original;
                        "administrative_factory_refused"
                    }
                    AdministrativeFactoryError::InvalidNamespace
                    | AdministrativeFactoryError::MissingExplicitCa => {
                        "administrative_input_refused"
                    }
                },
                Cause::Acquisition(original) => {
                    let _retained = original;
                    "administrative_acquisition_refused"
                }
                Cause::Query(original) => {
                    let _retained = original;
                    "administrative_query_refused"
                }
                Cause::Runtime(original) => {
                    let _retained = original;
                    "administrative_runtime_unavailable"
                }
                Cause::RefusedNativeFacts => "administrative_native_facts_refused",
            },
            _ => "administrative_original_uncertain",
        }
    }
}

struct TaskLoan {
    original: Arc<OriginalAdministrativeStartup>,
    task: Option<JoinHandle<Outcome>>,
}

impl Drop for TaskLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            let mut state = self
                .original
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.task.is_none() {
                state.task = Some(task);
            }
        }
    }
}

impl OriginalAdministrativeStartup {
    /// Resolve explicit credential settings synchronously, own the original
    /// frame and latent SDK before any factory/role query can be polled.
    pub fn start(
        input: Arc<RemoteAdministrationInput>,
        resolver: &HostCredentialResolver<'_>,
    ) -> Result<Arc<Self>, AdministrativeStartupRefusal> {
        let original = Arc::new(Self {
            input,
            state: Mutex::new(State {
                pool: None,
                task: None,
                returned: None,
                join_failure: None,
                raw_role_row: None,
                raw_identity_row: None,
                poison_seen: false,
                hold: None,
            }),
            session: Arc::new(AsyncMutex::new(None)),
            join: Arc::new(Semaphore::new(1)),
            close: OriginalAdministrativeClose::new(),
        });
        let prepared = administrative_settings(&original.input, resolver);
        let (settings, namespace) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                original
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .returned = Some(Err(Arc::new(Cause::Factory(error))));
                return Err(AdministrativeStartupRefusal { original });
            }
        };
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(error) => {
                original
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .returned = Some(Err(Arc::new(Cause::Runtime(error))));
                return Err(AdministrativeStartupRefusal { original });
            }
        };
        let mut state = original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.hold = Some(Arc::clone(&original));
        let body_original = Arc::clone(&original);
        state.task = Some(runtime.spawn(async move {
            // SAME sender lock prevents first factory poll before handle capture.
            {
                let _registered = body_original
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
            }
            let capture_original = Arc::clone(&body_original);
            let mut capture = move |pool| {
                let mut state = match capture_original.state.lock() {
                    Ok(state) => state,
                    Err(error) => {
                        let mut state = error.into_inner();
                        state.poison_seen = true;
                        state
                    }
                };
                state.pool = Some(pool);
                if state.poison_seen {
                    Err(PoolError::Unavailable)
                } else {
                    Ok(())
                }
            };
            let pool = PostgresPool::connect_retained_in_namespace(
                settings,
                namespace.as_ref(),
                &mut capture,
            )
            .await
            .map_err(|error| Arc::new(Cause::Acquisition(error)))?;
            let mut session = Arc::clone(&body_original.session).lock_owned().await;
            let connection = pool
                .acquire()
                .await
                .map_err(|error| Arc::new(Cause::Acquisition(error)))?;
            *session = Some(connection);
            let connection: &mut PgConnection = session
                .as_mut()
                .ok_or_else(|| Arc::new(Cause::RefusedNativeFacts))?;
            body_original
                .observe_native(connection, namespace.as_ref())
                .await
        }));
        drop(state);
        Ok(original)
    }

    async fn observe_native(
        &self,
        connection: &mut PgConnection,
        namespace: Option<&NamedNamespace>,
    ) -> Outcome {
        let (runtime, migrator, owner) = namespace.map_or_else(
            || {
                (
                    "codex_runtime".to_owned(),
                    "codex_migrator".to_owned(),
                    "codex_owner".to_owned(),
                )
            },
            |namespace| {
                let stem = namespace
                    .schema()
                    .strip_prefix("codex_storage_")
                    .unwrap_or_default();
                (
                    namespace.runtime_login().to_owned(),
                    namespace.migrator_login().to_owned(),
                    format!("codex_{stem}_owner"),
                )
            },
        );
        let targets = vec![runtime, migrator, owner];
        let role = sqlx::query(
            "SELECT session_user::text AS login, current_user::text AS current_role,
                    authenticated.rolsuper OR authenticated.rolcreaterole AS create_roles,
                    (SELECT count(*) = 3 AND bool_and(authenticated.rolsuper OR EXISTS
                       (SELECT 1 FROM pg_catalog.pg_auth_members membership
                        WHERE membership.roleid = target.oid AND membership.member = authenticated.oid
                          AND membership.admin_option))
                     FROM pg_catalog.pg_roles target WHERE target.rolname = ANY($1::text[])) AS administer_roles,
                    authenticated.rolsuper OR pg_catalog.pg_has_role(authenticated.oid, 'pg_signal_backend', 'USAGE') AS signal_backends,
                    authenticated.rolsuper OR pg_catalog.pg_has_role(authenticated.oid, 'pg_read_all_stats', 'USAGE') AS read_stats
             FROM pg_catalog.pg_roles authenticated WHERE authenticated.rolname = session_user",
        ).bind(targets).fetch_one(&mut *connection).await.map_err(|error| Arc::new(Cause::Query(error)))?;
        {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(error) => {
                    let mut state = error.into_inner();
                    state.poison_seen = true;
                    state
                }
            };
            state.raw_role_row = Some(role);
        }
        let (
            login,
            current_role,
            can_create_roles,
            can_administer_original_roles,
            can_signal_backends,
            can_read_all_backend_stats,
        ) = {
            let state = self
                .state
                .lock()
                .map_err(|_| Arc::new(Cause::RefusedNativeFacts))?;
            let role = state
                .raw_role_row
                .as_ref()
                .ok_or_else(|| Arc::new(Cause::RefusedNativeFacts))?;
            (
                role.try_get::<String, _>("login"),
                role.try_get::<String, _>("current_role"),
                role.try_get::<bool, _>("create_roles"),
                role.try_get::<bool, _>("administer_roles"),
                role.try_get::<bool, _>("signal_backends"),
                role.try_get::<bool, _>("read_stats"),
            )
        };
        let decode = |error| Arc::new(Cause::Query(error));
        let login = login.map_err(decode)?;
        let can_create_roles = can_create_roles.map_err(decode)?;
        let can_administer_original_roles = can_administer_original_roles.map_err(decode)?;
        let can_signal_backends = can_signal_backends.map_err(decode)?;
        let can_read_all_backend_stats = can_read_all_backend_stats.map_err(decode)?;
        if login != self.input.login()
            || current_role.map_err(decode)? != login
            || !can_create_roles
            || !can_administer_original_roles
            || !can_signal_backends
            || !can_read_all_backend_stats
        {
            return Err(Arc::new(Cause::RefusedNativeFacts));
        }
        let identity = sqlx::query(
            "SELECT control.system_identifier::text AS system_identifier, database.oid::bigint AS database_oid,
                    pg_catalog.pg_backend_pid() AS backend_pid
             FROM pg_catalog.pg_control_system() control
             JOIN pg_catalog.pg_database database ON database.datname = pg_catalog.current_database()",
        ).fetch_one(connection).await.map_err(decode)?;
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(error) => {
                let mut state = error.into_inner();
                state.poison_seen = true;
                state
            }
        };
        state.raw_identity_row = Some(identity);
        let identity = state
            .raw_identity_row
            .as_ref()
            .ok_or_else(|| Arc::new(Cause::RefusedNativeFacts))?;
        let observation = AdministrativeNativeObservation {
            system_identifier: identity.try_get("system_identifier").map_err(decode)?,
            database_oid: identity.try_get("database_oid").map_err(decode)?,
            backend_pid: identity.try_get("backend_pid").map_err(decode)?,
            login,
            can_create_roles,
            can_administer_original_roles,
            can_signal_backends,
            can_read_all_backend_stats,
        };
        if state.poison_seen
            || observation.database_oid <= 0
            || observation.backend_pid <= 0
            || observation
                .system_identifier
                .parse::<u64>()
                .ok()
                .is_none_or(|identity| identity == 0)
        {
            return Err(Arc::new(Cause::RefusedNativeFacts));
        }
        Ok(Arc::new(observation))
    }

    pub async fn finish_original(
        self: &Arc<Self>,
    ) -> Result<Arc<AdministrativeNativeObservation>, AdministrativeStartupRefusal> {
        let refused = || AdministrativeStartupRefusal {
            original: Arc::clone(self),
        };
        let _permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|_| refused())?;
        let task = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.task.take()
        };
        if let Some(task) = task {
            // Loan precedes permit in drop order, so canceled waiter restores the
            // SAME original task before another observer can borrow it.
            let mut loan = TaskLoan {
                original: Arc::clone(self),
                task: Some(task),
            };
            let returned = loan.task.as_mut().ok_or_else(refused)?.await;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match returned {
                Ok(outcome) => state.returned = Some(outcome),
                Err(error) => state.join_failure = Some(Arc::new(error)),
            }
            loan.task = None;
        }
        let state = self.state.lock().map_err(|_| refused())?;
        if state.poison_seen || state.join_failure.is_some() {
            return Err(refused());
        }
        match state.returned.as_ref() {
            Some(Ok(observation)) => Ok(Arc::clone(observation)),
            _ => Err(refused()),
        }
        // No hold release: same actual physical session + SDK require an original
        // close producer before this module may be declared in a real consumer.
    }

    /// Cleanup admission is separate from role admission. Reap SAME actual
    /// startup even after native refusal/poison, then transfer its actual session
    /// into the preallocated exterior close frame before native close polling.
    pub async fn close_original(self: &Arc<Self>) -> Result<(), AdministrativeStartupRefusal> {
        let _admission = self.finish_original().await;
        let (pool, terminal) = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            (
                state.pool.clone(),
                state.task.is_none() && (state.returned.is_some() || state.join_failure.is_some()),
            )
        };
        if !terminal {
            return Err(AdministrativeStartupRefusal {
                original: Arc::clone(self),
            });
        }
        {
            let mut original_session = Arc::clone(&self.session).lock_owned().await;
            self.close.seal_source(pool, &mut original_session);
        }
        let known_closed = self.close.finish().await;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !known_closed
            || state.poison_seen
            || self.state.is_poisoned()
            || state.join_failure.is_some()
            || !matches!(state.returned, Some(Ok(_)))
        {
            return Err(AdministrativeStartupRefusal {
                original: Arc::clone(self),
            });
        }
        state.hold = None; // Both actual close tasks physically joined first.
        Ok(())
    }
}

#[cfg(test)]
#[path = "administration_startup_tests.rs"]
mod tests;
