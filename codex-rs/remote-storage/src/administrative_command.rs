//! Original administrative preflight and physical cleanup owner.
//! Retains the SAME scoped original administrative session before SQL dispatch.
//! Only nonmutating application-data preflight is defined; native transaction
//! commands and session destruction have effects. No role mutation, enrollment,
//! backend termination, prepared resolution or exclusive permit is implemented.
//! Actual confirmed-plan association and privilege/catalog closure remain
//! mandatory before mutating retirement callers. Returned rows/status never mint authority.

use super::AdministrativeSessionAttemptRefusal;
use super::OriginalAdministrativeRuntimeSession;
use super::OriginalAdministrativeSessionAttempt;
use super::RemoteAdministrativeObserver;
use crate::administration_startup::OriginalAdministrativeStartup;
use crate::runtime_origin::RuntimeNativeOrigin;
use sqlx::Row;
use sqlx::postgres::PgRow;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

#[path = "administrative_catalog.rs"]
mod catalog;

type CloseOutcome = Result<(), Arc<dyn std::error::Error + Send + Sync>>;
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
    Wait(tokio::sync::AcquireError),
    Catalog(catalog::PayloadRefusal),
    Relations(catalog::RelationRefusal),
    RetainedDriver(Arc<sqlx::Error>),
    History(Arc<catalog::HistoryRefusal>),
}

struct State {
    task: Option<JoinHandle<()>>,
    dispatched: bool,
    sealed: bool,
    terminal: bool,
    joined_failure: Option<Arc<tokio::task::JoinError>>,
    native_baseline: Option<Result<sqlx::postgres::PgQueryResult, Arc<sqlx::Error>>>,
    raw_stamp: Option<PgRow>,
    xid: Option<String>,
    raw_rows: Option<Vec<PgRow>>,
    raw_catalog: [Option<Vec<PgRow>>; 6],
    catalog_payload_bytes: usize,
    catalog_relations: Option<catalog::RelationObservation>,
    structural: bool,
    structural_bounds: Option<Result<PgRow, Arc<sqlx::Error>>>,
    structural_lock: Option<Result<sqlx::postgres::PgQueryResult, Arc<sqlx::Error>>>,
    structural_metadata: Option<Result<Vec<PgRow>, Arc<sqlx::Error>>>,
    structural_history: Option<Result<Vec<PgRow>, Arc<sqlx::Error>>>,
    structural_refusal: Option<Arc<catalog::HistoryRefusal>>,
    structural_history_accepted: bool,
    #[cfg(test)]
    catalog_probe: Option<catalog::NativePayloadProbe>,
    #[cfg(test)]
    relation_probe: Option<catalog::NativeRelationProbe>,
    #[cfg(test)]
    relation_baseline: Option<Vec<PgRow>>,
    #[cfg(test)]
    relation_format: Option<PgRow>,
    #[cfg(test)]
    relation_restored: Option<Vec<PgRow>>,
    #[cfg(test)]
    relation_ddl: [Option<Result<sqlx::postgres::PgQueryResult, Arc<sqlx::Error>>>; 2],
    observed_status: Option<Option<String>>,
    commit_attempted: bool,
    rollback_attempted: bool,
    causes: [Option<Arc<CommandCause>>; CAUSES],
    refused: bool,
    hold: Option<Arc<OriginalAdministrativeCommand>>,
    observer: Arc<RemoteAdministrativeObserver>,
    origin: Arc<RuntimeNativeOrigin>,
    startup: Arc<OriginalAdministrativeStartup>,
    attempt: Option<Arc<OriginalAdministrativeSessionAttempt>>,
    close_task: Option<JoinHandle<CloseOutcome>>,
    close_returned: Option<CloseOutcome>,
    close_joined_failure: Option<Arc<tokio::task::JoinError>>,
}

pub struct OriginalAdministrativeCommand {
    original: Arc<AsyncMutex<Option<OriginalAdministrativeRuntimeSession>>>,
    state: Mutex<State>,
    join: Arc<Semaphore>,
    close_join: Arc<Semaphore>,
}

pub struct AdministrativeCommandRefusal {
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
                CommandCause::Wait(raw) => {
                    let _ = raw;
                }
                CommandCause::Catalog(raw) => {
                    let _ = raw;
                }
                CommandCause::Relations(raw) => {
                    let _ = raw;
                }
                CommandCause::RetainedDriver(raw) => {
                    let _ = raw;
                }
                CommandCause::History(raw) => {
                    let _ = raw;
                }
                CommandCause::MissingSession | CommandCause::UnknownStatus => {}
            }
        }
        let _retained = (
            &state.joined_failure,
            &state.raw_rows,
            &state.raw_catalog,
            state.catalog_payload_bytes,
            state.structural,
            &state.structural_bounds,
            &state.structural_lock,
            &state.structural_metadata,
            &state.structural_history,
            &state.structural_refusal,
            state.structural_history_accepted,
            &state.raw_stamp,
            &state.native_baseline,
            &state.xid,
            &state.observed_status,
            state.commit_attempted,
            state.rollback_attempted,
            &state.close_returned,
            &state.close_joined_failure,
            &state.observer,
            &state.origin,
            &state.startup,
        );
        if let Some(observed) = &state.catalog_relations {
            let _ = (
                observed.namespace_oid,
                observed.relation_count,
                observed.generation_relations_present,
                observed.memory_v2_relations_present,
            );
        }
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

pub struct AdministrativeCommandArchive {
    original: Arc<OriginalAdministrativeCommand>,
}
impl std::fmt::Debug for AdministrativeCommandArchive {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = &self.original;
        formatter.write_str("AdministrativeCommandArchive([original causes retained])")
    }
}
enum CloseSource {
    Frame(OriginalAdministrativeRuntimeSession),
    Attempt(Arc<OriginalAdministrativeSessionAttempt>),
}
struct CloseTaskLoan {
    original: Arc<OriginalAdministrativeCommand>,
    task: Option<JoinHandle<CloseOutcome>>,
}
impl Drop for CloseTaskLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.original
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .close_task = Some(task);
        }
    }
}

impl std::fmt::Display for AdministrativeCommandRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for AdministrativeCommandRefusal {}
impl OriginalAdministrativeCommand {
    pub fn belongs_to_observer(&self, observer: &Arc<RemoteAdministrativeObserver>) -> bool {
        let state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
        Arc::ptr_eq(&state.observer, observer)
    }
    pub fn retains_original_causes(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
        self.state.is_poisoned()
            || state.refused
            || state.causes.iter().any(Option::is_some)
            || state.joined_failure.is_some()
            || state.close_joined_failure.is_some()
            || state.close_returned.as_ref().is_some_and(Result::is_err)
    }

    pub fn prepare(original: OriginalAdministrativeRuntimeSession) -> Arc<Self> {
        let observer = Arc::clone(&original.original);
        let origin = Arc::clone(&original.origin);
        let startup = Arc::clone(original.borrowed.original_startup());
        Self::reserve(Some(original), None, observer, origin, startup)
    }
    pub(super) fn prepare_attempt(attempt: Arc<OriginalAdministrativeSessionAttempt>) -> Arc<Self> {
        let (observer, origin, startup) = attempt.original_parts();
        Self::reserve(None, Some(attempt), observer, origin, startup)
    }
    fn reserve(
        frame: Option<OriginalAdministrativeRuntimeSession>,
        attempt: Option<Arc<OriginalAdministrativeSessionAttempt>>,
        observer: Arc<RemoteAdministrativeObserver>,
        origin: Arc<RuntimeNativeOrigin>,
        startup: Arc<OriginalAdministrativeStartup>,
    ) -> Arc<Self> {
        let original = Arc::new(Self {
            original: Arc::new(AsyncMutex::new(frame)),
            state: Mutex::new(State {
                task: None,
                dispatched: false,
                sealed: false,
                terminal: false,
                joined_failure: None,
                native_baseline: None,
                raw_stamp: None,
                xid: None,
                raw_rows: None,
                raw_catalog: std::array::from_fn(|_| None),
                catalog_payload_bytes: 0,
                catalog_relations: None,
                structural: false,
                structural_bounds: None,
                structural_lock: None,
                structural_metadata: None,
                structural_history: None,
                structural_refusal: None,
                structural_history_accepted: false,
                #[cfg(test)]
                catalog_probe: None,
                #[cfg(test)]
                relation_probe: None,
                #[cfg(test)]
                relation_baseline: None,
                #[cfg(test)]
                relation_format: None,
                #[cfg(test)]
                relation_restored: None,
                #[cfg(test)]
                relation_ddl: std::array::from_fn(|_| None),
                observed_status: None,
                commit_attempted: false,
                rollback_attempted: false,
                causes: std::array::from_fn(|_| None),
                refused: false,
                hold: None,
                observer,
                origin,
                startup,
                attempt,
                close_task: None,
                close_returned: None,
                close_joined_failure: None,
            }),
            join: Arc::new(Semaphore::new(1)),
            close_join: Arc::new(Semaphore::new(1)),
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

    pub(super) async fn capture_attempt_session(
        self: &Arc<Self>,
        attempt: &Arc<OriginalAdministrativeSessionAttempt>,
    ) -> Result<(), AdministrativeSessionAttemptRefusal> {
        // This exterior slot was retained in Resources BEFORE first native join.
        let mut slot = Arc::clone(&self.original).lock_owned().await;
        {
            let state = self.state.lock().map_err(|_| attempt.refusal())?;
            if state.sealed
                || state.refused
                || state.dispatched
                || slot.is_some()
                || !state
                    .attempt
                    .as_ref()
                    .is_some_and(|original| Arc::ptr_eq(original, attempt))
            {
                return Err(attempt.refusal());
            }
        }
        let frame = attempt.finish_original().await?;
        *slot = Some(frame); // SAME returned guard/borrower before any next await.
        let state = self.state.lock().map_err(|_| attempt.refusal())?;
        if state.sealed || state.refused {
            return Err(attempt.refusal());
        }
        Ok(())
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

    fn history_refusal(&self, raw: catalog::HistoryRefusal) {
        let raw = Arc::new(raw);
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            // Dedicated original cause survives generic diagnostic capacity.
            if state.structural_refusal.is_none() {
                state.structural_refusal = Some(Arc::clone(&raw));
            }
        }
        self.cause(CommandCause::History(raw));
    }

    fn can_capture_structural(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
        state.structural && !state.refused && !self.state.is_poisoned()
    }

    async fn capture_structural_history(
        &self,
        connection: &mut sqlx::PgConnection,
        namespace: &str,
    ) {
        if !self.can_capture_structural() {
            return;
        }
        let bounds = sqlx::query("SELECT pg_catalog.set_config('search_path','pg_catalog',true), pg_catalog.set_config('lock_timeout','5000ms',true), pg_catalog.set_config('statement_timeout','30000ms',true)")
            .fetch_one(&mut *connection).await.map_err(Arc::new);
        let error = bounds.as_ref().err().cloned();
        {
            let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
            state.structural_bounds = Some(bounds);
            state.refused |= self.state.is_poisoned();
        }
        if let Some(raw) = error {
            self.cause(CommandCause::RetainedDriver(raw));
        }
        if !self.can_capture_structural() {
            return;
        }
        let locked = sqlx::query(&catalog::structural_lock_sql(namespace))
            .execute(&mut *connection)
            .await
            .map_err(Arc::new);
        let error = locked.as_ref().err().cloned();
        {
            let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
            state.structural_lock = Some(locked); // Native ACK before ANY classification/await.
            state.refused |= self.state.is_poisoned();
        }
        if let Some(raw) = error {
            self.cause(CommandCause::RetainedDriver(raw));
        }
        if !self.can_capture_structural() {
            return;
        }
        let qualified = format!("\"{}\"", namespace.replace('"', "\"\""));
        let metadata = sqlx::query(&format!("SELECT singleton, format_version, min_reader_version, min_writer_version FROM ONLY {qualified}.codex_schema_meta ORDER BY singleton LIMIT 2"))
            .fetch_all(&mut *connection).await.map_err(Arc::new);
        let error = metadata.as_ref().err().cloned();
        {
            let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
            state.structural_metadata = Some(metadata);
            state.refused |= self.state.is_poisoned();
        }
        if let Some(raw) = error {
            self.cause(CommandCause::RetainedDriver(raw));
        }
        if !self.can_capture_structural() {
            return;
        }
        let expected = codex_postgres_runtime::embedded_migration_checksums()
            .take(19)
            .collect::<Vec<_>>();
        let versions = expected
            .iter()
            .map(|(version, _)| *version)
            .collect::<Vec<_>>();
        let checksums = expected
            .iter()
            .map(|(_, checksum)| *checksum)
            .collect::<Vec<_>>();
        // Native comparison bounds client values even for oversized corrupt checksum.
        let history = sqlx::query(&format!("SELECT history.version, history.success, COALESCE(history.checksum OPERATOR(pg_catalog.=) expected.checksum,FALSE) AS checksum_matches FROM ONLY {qualified}._codex_pg_migrations history LEFT JOIN ROWS FROM (pg_catalog.unnest($1::pg_catalog.int8[]),pg_catalog.unnest($2::pg_catalog.bytea[])) AS expected(version,checksum) ON history.version OPERATOR(pg_catalog.=) expected.version ORDER BY history.version LIMIT 20"))
            .bind(versions).bind(checksums).fetch_all(&mut *connection).await.map_err(Arc::new);
        let error = history.as_ref().err().cloned();
        let inspected = {
            let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
            state.structural_history = Some(history);
            state.refused |= self.state.is_poisoned();
            match (
                state.structural_metadata.as_ref(),
                state.structural_history.as_ref(),
            ) {
                (Some(Ok(metadata)), Some(Ok(history))) => {
                    let bytes =
                        catalog::retained_payload_bytes(metadata, state.catalog_payload_bytes)
                            .and_then(|bytes| catalog::retained_payload_bytes(history, bytes));
                    match bytes {
                        Ok(bytes) => {
                            let inspected = catalog::inspect_history(metadata, history);
                            state.catalog_payload_bytes = bytes;
                            Some(Ok(inspected))
                        }
                        Err(raw) => Some(Err(raw)),
                    }
                }
                _ => None,
            }
        };
        if let Some(raw) = error {
            self.cause(CommandCause::RetainedDriver(raw));
        }
        match inspected {
            Some(Ok(Ok(()))) => {
                let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
                state.structural_history_accepted = true;
                state.refused |= self.state.is_poisoned();
            }
            Some(Ok(Err(raw))) => self.history_refusal(raw),
            Some(Err(raw)) => self.cause(CommandCause::Catalog(raw)),
            None if self.can_capture_structural() => {
                self.history_refusal(catalog::HistoryRefusal::MissingSnapshot)
            }
            None => {}
        }
    }

    pub fn start_preflight(self: &Arc<Self>) -> Result<(), AdministrativeCommandRefusal> {
        self.start_preflight_mode(false)
    }

    /// Explicit current19 structural observation. Takes native SHARE table locks
    /// until the original rollback; requires actual lock privileges. This neither
    /// enrolls principals nor certifies ACL/global-role/credential retirement.
    pub fn start_structural_preflight(
        self: &Arc<Self>,
    ) -> Result<(), AdministrativeCommandRefusal> {
        self.start_preflight_mode(true)
    }

    fn start_preflight_mode(
        self: &Arc<Self>,
        structural: bool,
    ) -> Result<(), AdministrativeCommandRefusal> {
        let refused = || AdministrativeCommandRefusal {
            original: Arc::clone(self),
        };
        let mut state = self.state.lock().map_err(|_| refused())?;
        if state.dispatched || state.refused || state.sealed {
            return Err(refused());
        }
        state.structural = structural;
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
        // PostgreSQL 17 RequireTransactionBlock rejects SAVEPOINT outside an
        // explicit transaction with 25P01. SQLx transaction depth cannot detect
        // raw BEGIN. Keep this native result before classification; unexpected
        // success means an existing transaction and MUST NOT authorize BEGIN or
        // rollback of an unknown owner's work. This probe is a native command,
        // not an exclusive-access/privilege proof. Same owned session guard is
        // retained continuously through the probe and subsequent transaction.
        let baseline = sqlx::query("SAVEPOINT codex_administrative_baseline")
            .execute(&mut **connection)
            .await;
        let idle = matches!(&baseline, Err(sqlx::Error::Database(raw))
            if raw.code().as_deref() == Some("25P01"));
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.native_baseline = Some(baseline.map_err(Arc::new));
            state.refused |= self.state.is_poisoned() || !idle;
            if state.refused {
                return;
            }
        }
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
        let namespace = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .observer
                .original_input()
                .target()
                .namespace()
                .to_owned()
        };
        let structural = {
            self.state
                .lock()
                .unwrap_or_else(|raw| raw.into_inner())
                .structural
        };
        if structural {
            self.capture_structural_history(connection, &namespace)
                .await;
        }
        for (index, statement) in catalog::STATEMENTS.iter().enumerate() {
            #[cfg(test)]
            if index == 0 {
                let probe = {
                    self.state
                        .lock()
                        .unwrap_or_else(|raw| raw.into_inner())
                        .relation_probe
                };
                if let Some(probe) = probe {
                    self.prepare_relation_probe(connection, &namespace, probe)
                        .await;
                }
            }
            #[cfg(not(test))]
            let (statement_sql, payload_probe) = (statement.sql, false);
            #[cfg(test)]
            let (statement_sql, payload_probe) = {
                let state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
                match state.catalog_probe {
                    Some(probe) => match probe.sql(index) {
                        Some(sql) => (sql, true),
                        None => break,
                    },
                    None => (statement.sql, false),
                }
            };
            let admitted = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                !state.refused && !self.state.is_poisoned()
            };
            if !admitted {
                break;
            }
            match sqlx::query(statement_sql)
                .bind(&namespace)
                .fetch_all(&mut **connection)
                .await
            {
                Ok(rows) => {
                    let oversized = {
                        let mut state =
                            self.state.lock().unwrap_or_else(|error| error.into_inner());
                        state.refused |= self.state.is_poisoned()
                            || rows.len() > statement.rows
                            || (index == 0 && rows.is_empty());
                        state.raw_catalog[index] = Some(rows); // Before ANY decode/refusal.
                        let measured = match state.raw_catalog[index].as_ref() {
                            Some(rows) => {
                                catalog::retained_payload_bytes(rows, state.catalog_payload_bytes)
                            }
                            None => Err(catalog::PayloadRefusal::MissingSnapshot),
                        };
                        match measured {
                            Ok(bytes) => state.catalog_payload_bytes = bytes,
                            Err(raw) => {
                                drop(state);
                                self.cause(CommandCause::Catalog(raw));
                                break;
                            }
                        }
                        if index == 0 && !payload_probe {
                            let observation = match state.raw_catalog[0].as_ref() {
                                Some(rows) => catalog::inspect_relations(rows),
                                None => Err(catalog::RelationRefusal::MissingSnapshot),
                            };
                            match observation {
                                Ok(observed) => {
                                    let unregistered = state.structural
                                        && (observed.generation_relations_present
                                            || observed.memory_v2_relations_present);
                                    state.catalog_relations = Some(observed);
                                    if unregistered {
                                        drop(state);
                                        self.history_refusal(
                                            catalog::HistoryRefusal::UnregisteredFormat,
                                        );
                                        break;
                                    }
                                }
                                Err(raw) => {
                                    drop(state);
                                    self.cause(CommandCause::Relations(raw));
                                    break;
                                }
                            }
                        }
                        if index == 1 && !payload_probe {
                            state.raw_catalog[index].as_ref().map(|rows| {
                                rows.iter()
                                    .map(|row| row.try_get::<bool, _>("oversized"))
                                    .collect::<Result<Vec<_>, _>>()
                            })
                        } else {
                            None
                        }
                    };
                    match oversized {
                        Some(Ok(bounds)) if bounds.iter().any(|bound| *bound) => {
                            self.cause(CommandCause::UnknownStatus)
                        }
                        Some(Err(raw)) => self.cause(CommandCause::Driver(raw)),
                        _ => {}
                    }
                }
                Err(raw) => self.cause(CommandCause::Driver(raw)),
            }
        }
        // This preflight reads application data but BEGIN and xid allocation
        // affect native transaction state. Always explicitly rollback the
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
        #[cfg(test)]
        {
            let restore_probe = {
                let state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
                state.relation_probe.is_some()
                    && !self.state.is_poisoned()
                    && state
                        .observed_status
                        .as_ref()
                        .and_then(|status| status.as_deref())
                        == Some("aborted")
            };
            if restore_probe {
                match sqlx::query(catalog::STATEMENTS[0].sql)
                    .bind(&namespace)
                    .fetch_all(&mut **connection)
                    .await
                {
                    Ok(rows) => {
                        let checked = {
                            let mut state =
                                self.state.lock().unwrap_or_else(|raw| raw.into_inner());
                            state.relation_restored = Some(rows);
                            state.relation_restored.as_ref().map(|rows| {
                                catalog::retained_payload_bytes(rows, 0)
                                    .map(|_| catalog::inspect_relations(rows))
                            })
                        };
                        match checked {
                            Some(Ok(Ok(_))) => {}
                            Some(Err(raw)) => self.cause(CommandCause::Catalog(raw)),
                            Some(Ok(Err(raw))) => self.cause(CommandCause::Relations(raw)),
                            _ => self.cause(CommandCause::UnknownStatus),
                        }
                    }
                    Err(raw) => self.cause(CommandCause::Driver(raw)),
                }
            }
        }
    }

    // Actual transactional catalog adversaries on an explicitly isolated current19
    // fixture. Native rows/DDL results are captured before every classification.
    #[cfg(test)]
    async fn prepare_relation_probe(
        &self,
        connection: &mut sqlx::PgConnection,
        namespace: &str,
        probe: catalog::NativeRelationProbe,
    ) {
        let qualified = format!("\"{}\"", namespace.replace('"', "\"\""));
        match sqlx::query(&format!(
            "SELECT format_version FROM {qualified}.codex_schema_meta WHERE singleton"
        ))
        .fetch_one(&mut *connection)
        .await
        {
            Ok(row) => {
                let decoded = {
                    let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
                    state.relation_format = Some(row);
                    state.refused |= self.state.is_poisoned();
                    state
                        .relation_format
                        .as_ref()
                        .map(|row| row.try_get::<i32, _>("format_version"))
                };
                match decoded {
                    Some(Ok(19)) => {}
                    Some(Err(raw)) => {
                        self.cause(CommandCause::Driver(raw));
                        return;
                    }
                    _ => {
                        self.cause(CommandCause::UnknownStatus);
                        return;
                    }
                }
            }
            Err(raw) => {
                self.cause(CommandCause::Driver(raw));
                return;
            }
        }
        match sqlx::query(catalog::STATEMENTS[0].sql)
            .bind(namespace)
            .fetch_all(&mut *connection)
            .await
        {
            Ok(rows) => {
                let validation = {
                    let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
                    state.relation_baseline = Some(rows);
                    state.refused |= self.state.is_poisoned();
                    state.relation_baseline.as_ref().map(|rows| {
                        catalog::retained_payload_bytes(rows, 0)
                            .map(|_| catalog::inspect_relations(rows))
                    })
                };
                match validation {
                    Some(Ok(Ok(observed)))
                        if !observed.generation_relations_present
                            && !observed.memory_v2_relations_present => {}
                    Some(Err(raw)) => {
                        self.cause(CommandCause::Catalog(raw));
                        return;
                    }
                    Some(Ok(Err(raw))) => {
                        self.cause(CommandCause::Relations(raw));
                        return;
                    }
                    _ => {
                        self.cause(CommandCause::UnknownStatus);
                        return;
                    }
                }
            }
            Err(raw) => {
                self.cause(CommandCause::Driver(raw));
                return;
            }
        }
        for (index, sql) in probe.ddl(namespace).iter().enumerate() {
            let admitted = {
                let state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
                !state.refused && !self.state.is_poisoned()
            };
            if !admitted {
                break;
            }
            let result = sqlx::query(sql).execute(&mut *connection).await;
            let result = result.map_err(Arc::new);
            let failed = result.is_err();
            {
                let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
                state.relation_ddl[index] = Some(result);
                state.refused |= self.state.is_poisoned() || failed;
            }
            if failed {
                break;
            }
        }
    }

    pub async fn finish_original(self: &Arc<Self>) -> Result<(), AdministrativeCommandRefusal> {
        let refused = || AdministrativeCommandRefusal {
            original: Arc::clone(self),
        };
        let _permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|raw| {
                self.cause(CommandCause::Wait(raw));
                refused()
            })?;
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
    /// Reconcile the SAME query task, then register the actual close before
    /// consuming the scoped frame. Empty original Option never proves closure.
    pub async fn close_into_archive(
        self: &Arc<Self>,
    ) -> Result<AdministrativeCommandArchive, AdministrativeCommandRefusal> {
        let refused = || AdministrativeCommandRefusal {
            original: Arc::clone(self),
        };
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.sealed = true;
            state.refused |= self.state.is_poisoned();
        }
        let _query_result = self.finish_original().await;
        let _close_permit = Arc::clone(&self.close_join)
            .acquire_owned()
            .await
            .map_err(|raw| {
                self.cause(CommandCause::Wait(raw));
                refused()
            })?;
        let needs_close = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let terminal = (!state.dispatched && state.task.is_none())
                || (state.terminal && state.task.is_none());
            if !terminal {
                return Err(refused());
            }
            state.close_task.is_none()
                && state.close_returned.is_none()
                && state.close_joined_failure.is_none()
        };
        if needs_close {
            let mut slot = Arc::clone(&self.original).lock_owned().await;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let runtime = match tokio::runtime::Handle::try_current() {
                Ok(runtime) => runtime,
                Err(raw) => {
                    if let Some(cause) = state.causes.iter_mut().find(|cause| cause.is_none()) {
                        *cause = Some(Arc::new(CommandCause::Runtime(raw)));
                    }
                    state.refused = true;
                    return Err(refused());
                }
            };
            let source = match slot.take() {
                Some(frame) => CloseSource::Frame(frame),
                None => match state.attempt.as_ref() {
                    Some(attempt) => CloseSource::Attempt(Arc::clone(attempt)),
                    None => return Err(refused()),
                },
            };
            let original = Arc::clone(self);
            // observer/origin/startup were stored before frame was first polled.
            // The original frame/registry owns native resources even if this
            // close body panics; raw task cause still requires reconciliation.
            state.close_task = Some(runtime.spawn(async move {
                {
                    let _entry = original
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                }
                match source {
                    CloseSource::Frame(frame) => frame
                        .close_original()
                        .await
                        .map_err(|raw| Arc::new(raw) as Arc<dyn std::error::Error + Send + Sync>),
                    CloseSource::Attempt(attempt) => attempt
                        .close_original()
                        .await
                        .map_err(|raw| Arc::new(raw) as Arc<dyn std::error::Error + Send + Sync>),
                }
            }));
        }
        let task = {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .close_task
                .take()
        };
        if let Some(task) = task {
            let mut loan = CloseTaskLoan {
                original: Arc::clone(self),
                task: Some(task),
            };
            let returned = loan.task.as_mut().ok_or_else(refused)?.await;
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match returned {
                Ok(result) => state.close_returned = Some(result),
                Err(raw) => state.close_joined_failure = Some(Arc::new(raw)),
            }
            loan.task = None; // Raw original close result cached before clear.
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let query_terminal =
            (!state.dispatched && state.task.is_none()) || (state.terminal && state.task.is_none());
        if !query_terminal
            || !state.sealed
            || state.close_task.is_some()
            || state.close_returned.is_none()
            || state.close_joined_failure.is_some()
            || !state.startup.known_physical_closed()
        {
            return Err(refused());
        }
        // This is a plain owning resource/failure handoff, NOT a clean business
        // result, remote-xid proof, retirement/exclusive permit or role admission.
        // Caller must already own a reserved history slot before SQL dispatch.
        let archive = AdministrativeCommandArchive {
            original: Arc::clone(self),
        };
        state.hold = None;
        Ok(archive)
    }
    // Selfhold is intentionally NOT released by query completion. Native session
    // and SDK cleanup and finite failure-archive handoff are separate mandatory
    // consumer steps; absence/JoinError/query refusal never substitutes for ACK.
}

#[cfg(test)]
#[path = "administrative_command_tests.rs"]
mod tests;
