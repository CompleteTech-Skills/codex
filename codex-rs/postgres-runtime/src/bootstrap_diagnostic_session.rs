//! Separate original diagnostic session, never a replacement command session.
//! Undeclared prerequisite: real factory/owner and original-origin wiring follow.
use crate::PostgresPool;
use crate::RetainedWriteSession;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
type NativeStatus = (Arc<String>, Option<String>);

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct NativeOrigin {
    system_identifier: String,
    database_oid: i64,
}
impl NativeOrigin {
    pub(crate) async fn capture(connection: &mut sqlx::PgConnection) -> Result<Self, sqlx::Error> {
        let (system_identifier, database_oid): (String, i64) = sqlx::query_as(
            "SELECT c.system_identifier::text, d.oid::bigint FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_database d WHERE d.datname=pg_catalog.current_database()",
        ).fetch_one(connection).await?;
        Ok(Self {
            system_identifier,
            database_oid,
        })
    }
}

pub(crate) struct DiagnosticSession {
    original_pool: Arc<PostgresPool>,
    state: Mutex<State>,
    gate: Arc<Semaphore>,
}
struct State {
    attempted: bool,
    original: Option<RetainedWriteSession>,
    acquisition: Option<crate::PoolError>,
    origin: Option<NativeOrigin>,
    original_request: Option<(NativeOrigin, Arc<String>)>,
    observation: Option<(Arc<String>, bool)>,
    causes: [Cause; 8],
    sealed: bool,
    #[cfg(test)]
    control_payload: Option<bool>,
    close: Option<Arc<crate::bootstrap_original_close::OriginalClose>>,
}
pub(crate) struct Observation {
    pub(crate) original_diagnostic: Arc<DiagnosticSession>,
    pub(crate) xid: Arc<String>,
    pub(crate) committed: bool,
}
pub(crate) struct Refusal {
    pub(crate) original: Arc<DiagnosticSession>,
}
impl DiagnosticSession {
    // Caller must capture this exact separately credentialed pool BEFORE its
    // first acquisition. Existing broken original command remains retained.
    pub(crate) fn new(original_pool: Arc<PostgresPool>) -> Arc<Self> {
        Arc::new(Self {
            original_pool,
            gate: Arc::new(Semaphore::new(1)),
            state: Mutex::new(State {
                attempted: false,
                original: None,
                acquisition: None,
                origin: None,
                original_request: None,
                observation: None,
                causes: std::array::from_fn(|_| Cause::default()),
                sealed: false,
                #[cfg(test)]
                control_payload: None,
                close: None,
            }),
        })
    }
    pub(crate) fn original_pool(&self) -> &Arc<PostgresPool> {
        &self.original_pool
    }
    #[cfg(test)]
    pub(crate) fn original_native_status(&self) -> Option<(Arc<String>, Option<String>)> {
        self.state
            .lock()
            .ok()?
            .causes
            .iter()
            .rev()
            .find_map(|cause| {
                cause
                    .native_status
                    .as_ref()
                    .map(|status| status.as_ref().clone())
            })
    }
    #[cfg(test)]
    pub(crate) fn control_origin_difference(&self, xid: &Arc<String>) -> Option<(bool, bool)> {
        let state = self.state.lock().ok()?;
        let (requested, original_xid) = state.original_request.as_ref()?;
        let observed = state.origin.as_ref()?;
        if !Arc::ptr_eq(original_xid, xid) {
            return None;
        }
        Some((
            requested.system_identifier != observed.system_identifier,
            requested.database_oid != observed.database_oid,
        ))
    }
    pub(crate) fn has_original_observation(&self, xid: &Arc<String>) -> bool {
        self.state.lock().ok().is_some_and(|state| {
            state
                .observation
                .as_ref()
                .is_some_and(|(observed, _)| Arc::ptr_eq(observed, xid))
        })
    }
    pub(crate) async fn observe(
        self: &Arc<Self>,
        original_origin: &NativeOrigin,
        original_xid: &Arc<String>,
    ) -> Result<Observation, Refusal> {
        let refusal = || Refusal {
            original: Arc::clone(self),
        };
        let permit = Arc::clone(&self.gate)
            .acquire_owned()
            .await
            .map_err(|_| refusal())?;
        let acquire = {
            let mut state = self.state.lock().map_err(|_| refusal())?;
            if state.sealed {
                return Err(refusal());
            }
            // Retain the genuine command association BEFORE diagnostic acquire.
            // A retry may query the same request; it cannot rebind this owner to
            // another command, even an equal textual xid from a foreign origin.
            match &state.original_request {
                Some((origin, xid))
                    if origin != original_origin || !Arc::ptr_eq(xid, original_xid) =>
                {
                    return Err(refusal());
                }
                Some(_) => {}
                None => {
                    state.original_request =
                        Some((original_origin.clone(), Arc::clone(original_xid)));
                }
            }
            if state.attempted && state.original.is_none() {
                return Err(refusal());
            }
            let acquire = !state.attempted;
            state.attempted = true;
            acquire
        };
        if acquire {
            let acquired = self.original_pool.acquire_retained_write().await;
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            match acquired {
                Ok(original) => state.original = Some(original),
                Err(cause) => {
                    state.acquisition = Some(cause);
                    return Err(refusal());
                }
            }
            if poison {
                return Err(refusal());
            }
        }
        let original = self.state.lock().map_err(|_| refusal())?.original.take();
        let mut loan = Loan {
            owner: Arc::clone(self),
            original,
            _permit: permit,
        };
        let connection = loan
            .original
            .as_mut()
            .ok_or_else(refusal)?
            .bootstrap_connection();
        let reservation = self.reserve().map_err(|_| refusal())?;
        let origin = match NativeOrigin::capture(connection).await {
            Ok(origin) => {
                reservation.success();
                origin
            }
            Err(cause) => {
                reservation.failure(cause);
                return Err(refusal());
            }
        };
        let poison = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.origin = Some(origin.clone()); // Actual returned fact before refusal.
            poison
        };
        if poison || origin != *original_origin {
            return Err(refusal());
        }
        let reservation = self.reserve().map_err(|_| refusal())?;
        let status_index = reservation.index;
        let status = match sqlx::query_scalar::<_, Option<String>>(
            "SELECT pg_catalog.pg_xact_status($1::pg_catalog.xid8)",
        )
        .bind(original_xid.as_str())
        .fetch_one(connection)
        .await
        {
            Ok(status) => {
                reservation.success();
                status
            }
            Err(cause) => {
                reservation.failure(cause);
                return Err(refusal());
            }
        };
        let status = Arc::new((Arc::clone(original_xid), status));
        let poison = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.causes[status_index].native_status = Some(Arc::clone(&status));
            poison
        };
        if poison {
            return Err(refusal());
        }
        let committed = match status.1.as_deref() {
            Some("committed") => true,
            Some("aborted") => false,
            _ => return Err(refusal()),
        };
        let poison = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.observation = Some((Arc::clone(original_xid), committed));
            poison
        };
        if poison {
            return Err(refusal());
        }
        Ok(Observation {
            original_diagnostic: Arc::clone(self),
            xid: Arc::clone(original_xid),
            committed,
        })
    }
    #[cfg(test)]
    async fn control_query<T>(
        self: &Arc<Self>,
        future: impl std::future::Future<Output = Result<T, sqlx::Error>>,
    ) -> Result<T, Refusal> {
        let refusal = || Refusal {
            original: Arc::clone(self),
        };
        let reservation = self.reserve().map_err(|_| refusal())?;
        match future.await {
            Ok(value) => {
                reservation.success();
                Ok(value)
            }
            Err(cause) => {
                reservation.failure(cause);
                Err(refusal())
            }
        }
    }
    #[cfg(test)]
    pub(crate) async fn control_verify_committed_bootstrap(
        self: &Arc<Self>,
        namespace: Option<&crate::NamedNamespace>,
    ) -> Result<bool, Refusal> {
        use sqlx::Row;
        let refusal = || Refusal {
            original: Arc::clone(self),
        };
        let permit = Arc::clone(&self.gate)
            .acquire_owned()
            .await
            .map_err(|_| refusal())?;
        let original = {
            let mut state = self.state.lock().map_err(|_| refusal())?;
            if state.sealed
                || !state
                    .observation
                    .as_ref()
                    .is_some_and(|(_, committed)| *committed)
                || self.original_pool.selected_search_path.as_deref()
                    != Some(crate::owned_namespace_search_path(namespace).as_str())
            {
                return Err(refusal());
            }
            state.original.take()
        };
        let mut loan = Loan {
            owner: Arc::clone(self),
            original,
            _permit: permit,
        };
        let connection = loan
            .original
            .as_mut()
            .ok_or_else(refusal)?
            .bootstrap_connection();
        let format = crate::client_schema_format();
        let metadata: Vec<(bool, i32, i32, i32)> = self.control_query(sqlx::query_as(
            "SELECT singleton, format_version, min_reader_version, min_writer_version FROM ONLY codex_schema_meta LIMIT 2"
        ).fetch_all(&mut *connection)).await?;
        let activation: Vec<(bool, String, Option<String>, i64, i64, Option<String>)> = self.control_query(sqlx::query_as(
            "SELECT singleton, state, run_id::text, generation, updated_at_ms, dataset_id::text FROM ONLY storage_activation LIMIT 2"
        ).fetch_all(&mut *connection)).await?;
        let counts: (i64, i64) = self.control_query(sqlx::query_as(
            "SELECT (SELECT count(*) FROM ONLY storage_migration_runs), (SELECT count(*) FROM ONLY storage_migration_domains)"
        ).fetch_one(&mut *connection)).await?;
        let named = match namespace {
            Some(namespace) => Some(
                crate::named_bootstrap::namespaced_migrations(namespace).map_err(|_| refusal())?,
            ),
            None => None,
        };
        let migrations = match named.as_ref() {
            Some(named) => named.as_slice(),
            None => crate::bootstrap::BASE_MIGRATOR.migrations.as_ref(),
        };
        let versions: Vec<i64> = migrations
            .iter()
            .map(|migration| migration.version)
            .collect();
        let checksums: Vec<&[u8]> = migrations
            .iter()
            .map(|migration| migration.checksum.as_ref())
            .collect();
        let rows = self.control_query(sqlx::query(
            "SELECT h.version, h.success, COALESCE(h.checksum=expected.checksum,FALSE) AS checksum_matches FROM ONLY _codex_pg_migrations h LEFT JOIN unnest($1::bigint[], $2::bytea[]) expected(version,checksum) ON h.version=expected.version ORDER BY h.version LIMIT $3"
        ).bind(versions).bind(checksums).bind(i64::from(format)+1).fetch_all(&mut *connection)).await?;
        let mut history =
            rows.len() == migrations.len() && i32::try_from(migrations.len()).ok() == Some(format);
        for (row, migration) in rows.iter().zip(migrations) {
            let actual = self
                .control_query(async {
                    Ok((
                        row.try_get::<i64, _>("version")?,
                        row.try_get::<bool, _>("success")?,
                        row.try_get::<bool, _>("checksum_matches")?,
                    ))
                })
                .await?;
            history &= actual == (migration.version, true, true);
        }
        let valid = metadata == vec![(true, format, format, format)]
            && activation == vec![(true, "open".into(), None, 0, 0, None)]
            && counts == (0, 0)
            && history;
        let poison = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.control_payload = Some(valid); // Actual returned query facts classified only afterward.
            poison
        };
        if poison { Err(refusal()) } else { Ok(valid) }
    }
    #[cfg(test)]
    pub(crate) fn control_payload_verified(&self) -> bool {
        self.state
            .lock()
            .ok()
            .is_some_and(|state| state.control_payload == Some(true))
    }
    fn reserve(self: &Arc<Self>) -> Result<Reservation, ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        let index = state
            .causes
            .iter()
            .position(|cause| {
                !cause.reserved && cause.original.is_none() && cause.native_status.is_none()
            })
            .ok_or(())?;
        state.causes[index].reserved = true;
        Ok(Reservation {
            owner: Arc::clone(self),
            index,
            interrupted: Arc::new(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "original diagnostic query interrupted; outcome unknown",
            )),
        })
    }
    pub(crate) async fn close_original(self: &Arc<Self>) -> Result<(), Refusal> {
        let refusal = || Refusal {
            original: Arc::clone(self),
        };
        let _permit = Arc::clone(&self.gate)
            .acquire_owned()
            .await
            .map_err(|_| refusal())?;
        let (close, poison) = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            state.sealed = true;
            if state.close.is_none() {
                if let Some(original) = state.original.take() {
                    state.close = Some(crate::bootstrap_original_close::OriginalClose::capture(
                        original,
                    ));
                }
            }
            (state.close.clone(), poison)
        };
        if let Some(close) = close {
            match close.finish().await {
                Ok(receipt) if receipt.covers_pool(&self.original_pool) => {}
                _ => return Err(refusal()),
            }
        }
        if poison || self.state.is_poisoned() {
            Err(refusal())
        } else {
            Ok(())
        }
    }
}
struct Loan {
    owner: Arc<DiagnosticSession>,
    original: Option<RetainedWriteSession>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for Loan {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            self.owner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .original = Some(original);
        }
    } // Original diagnostic connection restored BEFORE serialized permit release.
}

#[derive(Default)]
struct Cause {
    reserved: bool,
    original: Option<Arc<dyn std::error::Error + Send + Sync>>,
    native_status: Option<Arc<NativeStatus>>,
}
struct Reservation {
    owner: Arc<DiagnosticSession>,
    index: usize,
    interrupted: Arc<std::io::Error>,
}
impl Reservation {
    fn success(self) {
        self.owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .causes[self.index]
            .reserved = false;
    }
    fn failure(self, original: sqlx::Error) {
        self.owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .causes[self.index]
            .original = Some(Arc::new(original));
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self
            .owner
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
