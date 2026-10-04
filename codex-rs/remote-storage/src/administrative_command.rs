//! Undeclared child-module prerequisite for administration.rs.
//! Retains the SAME scoped original administrative session before SQL dispatch.
//! Only read-only retirement preflight is defined; no role mutation, enrollment,
//! backend termination, prepared resolution or exclusive permit is implemented.
//! Actual confirmed-plan association and privilege/catalog closure must arrive
//! with real callers before declaration. Returned rows/status never mint authority.

use super::OriginalAdministrativeRuntimeSession;
use sqlx::Row;
use sqlx::postgres::PgRow;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

const CAUSES: usize = 8;
const PREFLIGHT: &str = "SELECT role.oid::bigint AS role_oid, role.rolname::text AS role_name,
    role.rolcanlogin, role.rolsuper, role.rolcreaterole, role.rolcreatedb,
    role.rolreplication, role.rolbypassrls,
    pg_catalog.pg_has_role(session_user, role.oid, 'USAGE') AS inherited,
    pg_catalog.pg_has_role(session_user, role.oid, 'SET') AS assumable
    FROM pg_catalog.pg_roles role ORDER BY role.oid LIMIT 1001";

pub(super) enum CommandCause {
    Driver(sqlx::Error),
    Runtime(tokio::runtime::TryCurrentError),
    MissingSession,
    UnknownStatus,
}

struct State {
    task: Option<JoinHandle<()>>,
    dispatched: bool,
    terminal: bool,
    joined_failure: Option<Arc<tokio::task::JoinError>>,
    raw_stamp: Option<PgRow>,
    xid: Option<String>,
    raw_rows: Option<Vec<PgRow>>,
    observed_status: Option<Option<String>>,
    commit_attempted: bool,
    rollback_attempted: bool,
    causes: [Option<Arc<CommandCause>>; CAUSES],
    refused: bool,
    hold: Option<Arc<OriginalAdministrativeCommand>>,
}

pub(super) struct OriginalAdministrativeCommand {
    original: Arc<AsyncMutex<Option<OriginalAdministrativeRuntimeSession>>>,
    state: Mutex<State>,
    join: Arc<Semaphore>,
}

pub(super) struct AdministrativeCommandRefusal {
    original: Arc<OriginalAdministrativeCommand>,
}
impl std::fmt::Debug for AdministrativeCommandRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self
            .original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        for cause in state.causes.iter().flatten() {
            match cause.as_ref() {
                CommandCause::Driver(raw) => {
                    let _ = raw;
                }
                CommandCause::Runtime(raw) => {
                    let _ = raw;
                }
                CommandCause::MissingSession | CommandCause::UnknownStatus => {}
            }
        }
        let _retained = (
            &state.joined_failure,
            &state.raw_rows,
            &state.raw_stamp,
            &state.xid,
            &state.observed_status,
            state.commit_attempted,
            state.rollback_attempted,
        );
        formatter.write_str("AdministrativeCommandRefused([original retained])")
    }
}

struct TaskLoan {
    original: Arc<OriginalAdministrativeCommand>,
    task: Option<JoinHandle<()>>,
}
impl Drop for TaskLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            let mut state = self
                .original
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.task = Some(task);
            state.refused |= self.original.state.is_poisoned();
        }
    }
}

impl OriginalAdministrativeCommand {
    pub(super) fn prepare(original: OriginalAdministrativeRuntimeSession) -> Arc<Self> {
        let original = Arc::new(Self {
            original: Arc::new(AsyncMutex::new(Some(original))),
            state: Mutex::new(State {
                task: None,
                dispatched: false,
                terminal: false,
                joined_failure: None,
                raw_stamp: None,
                xid: None,
                raw_rows: None,
                observed_status: None,
                commit_attempted: false,
                rollback_attempted: false,
                causes: std::array::from_fn(|_| None),
                refused: false,
                hold: None,
            }),
            join: Arc::new(Semaphore::new(1)),
        });
        // Also own the prepared native session and raw admission causes when
        // runtime entry/first dispatch refuses. Only real cleanup/archive may
        // release this pending hold; last caller Drop is not terminal evidence.
        original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .hold = Some(Arc::clone(&original));
        original
    }

    fn cause(&self, cause: CommandCause) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.refused = true;
        if let Some(slot) = state.causes.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(Arc::new(cause));
        }
        // At most BEGIN/stamp/preflight/rollback/status and runtime/absence causes
        // exist for this single immutable dispatch. No retry overwrites history.
    }

    pub(super) fn start_preflight(self: &Arc<Self>) -> Result<(), AdministrativeCommandRefusal> {
        let refused = || AdministrativeCommandRefusal {
            original: Arc::clone(self),
        };
        let mut state = self.state.lock().map_err(|_| refused())?;
        if state.dispatched || state.refused {
            return Err(refused());
        }
        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(raw) => {
                state.causes[0] = Some(Arc::new(CommandCause::Runtime(raw)));
                state.refused = true;
                return Err(refused());
            }
        };
        state.dispatched = true;
        state.hold = Some(Arc::clone(self));
        let original = Arc::clone(self);
        state.task = Some(runtime.spawn(async move {
            // Sender still owns the SAME mutex until its actual JH is stored.
            {
                let _entry = original
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
            }
            original.run_preflight().await;
        }));
        Ok(())
    }

    async fn run_preflight(&self) {
        let mut original = Arc::clone(&self.original).lock_owned().await;
        let Some(frame) = original.as_mut() else {
            self.cause(CommandCause::MissingSession);
            return;
        };
        let Some(connection) = frame.session.as_mut() else {
            self.cause(CommandCause::MissingSession);
            return;
        };
        if let Err(raw) = sqlx::query("BEGIN").execute(&mut **connection).await {
            self.cause(CommandCause::Driver(raw));
            return;
        }
        let stamp = sqlx::query("SELECT pg_catalog.pg_current_xact_id()::text AS xid")
            .fetch_one(&mut **connection)
            .await;
        match stamp {
            Ok(row) => {
                let decoded = {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    state.raw_stamp = Some(row);
                    let decoded = state
                        .raw_stamp
                        .as_ref()
                        .map(|row| row.try_get::<String, _>("xid"));
                    state.refused |= self.state.is_poisoned();
                    decoded
                };
                match decoded {
                    Some(Ok(xid)) => {
                        let mut state =
                            self.state.lock().unwrap_or_else(|error| error.into_inner());
                        state.xid = Some(xid);
                        state.refused |= self.state.is_poisoned();
                    }
                    Some(Err(raw)) => self.cause(CommandCause::Driver(raw)),
                    None => self.cause(CommandCause::UnknownStatus),
                }
            }
            Err(raw) => self.cause(CommandCause::Driver(raw)),
        }
        let can_query = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            !state.refused && !self.state.is_poisoned()
        };
        if can_query {
            match sqlx::query(PREFLIGHT).fetch_all(&mut **connection).await {
                Ok(rows) => {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    state.refused |= self.state.is_poisoned() || rows.len() > 1000;
                    state.raw_rows = Some(rows); // Actual returned rows before classification.
                }
                Err(raw) => self.cause(CommandCause::Driver(raw)),
            }
        }
        // This immutable preflight is READ-ONLY. Always explicitly rollback the
        // SAME native transaction; no commit proof/retirement effect is claimed.
        {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .rollback_attempted = true;
        }
        if let Err(raw) = sqlx::query("ROLLBACK").execute(&mut **connection).await {
            self.cause(CommandCause::Driver(raw));
            return;
        }
        let xid = {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .xid
                .clone()
        };
        if let Some(xid) = xid {
            match sqlx::query_scalar::<_, Option<String>>(
                "SELECT pg_catalog.pg_xact_status($1::xid8)::text",
            )
            .bind(xid)
            .fetch_one(&mut **connection)
            .await
            {
                Ok(raw_status) => {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    let known = raw_status.as_deref() == Some("aborted");
                    state.observed_status = Some(raw_status); // None differs from no query.
                    state.refused |= self.state.is_poisoned() || !known;
                }
                Err(raw) => self.cause(CommandCause::Driver(raw)),
            }
        } else {
            self.cause(CommandCause::UnknownStatus);
        }
    }

    pub(super) async fn finish_original(
        self: &Arc<Self>,
    ) -> Result<(), AdministrativeCommandRefusal> {
        let refused = || AdministrativeCommandRefusal {
            original: Arc::clone(self),
        };
        let _permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|_| refused())?;
        let task = {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .task
                .take()
        };
        if let Some(task) = task {
            let mut loan = TaskLoan {
                original: Arc::clone(self),
                task: Some(task),
            };
            let returned = loan.task.as_mut().ok_or_else(refused)?.await;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Err(raw) = returned {
                state.joined_failure = Some(Arc::new(raw));
            }
            state.terminal = true;
            loan.task = None; // Original raw task result saved before handle clear.
        }
        let state = self.state.lock().map_err(|_| refused())?;
        if !state.terminal
            || state.task.is_some()
            || state.refused
            || state.joined_failure.is_some()
            || self.state.is_poisoned()
            || state
                .observed_status
                .as_ref()
                .and_then(|value| value.as_deref())
                != Some("aborted")
        {
            return Err(refused());
        }
        Ok(())
    }
    // Selfhold is intentionally NOT released by query completion. Native session
    // and SDK cleanup and finite failure-archive handoff are separate mandatory
    // consumer steps; absence/JoinError/query refusal never substitutes for ACK.
}
