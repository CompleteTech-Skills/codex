//! Real empty-bootstrap admission and original-session outcome controls.
use super::*;
use crate::ConnectionSettings;
use crate::NamedNamespace;
use crate::PoolLimits;
use rustls::pki_types::pem::PemObject;
use sha2::Digest;
use std::io::Read;
use std::path::Path;
use std::time::Duration;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
struct TaskLoan {
    original: Arc<BootstrapSessionOwner>,
    task: Option<tokio::task::JoinHandle<std::result::Result<(), BootstrapError>>>,
}
impl Drop for TaskLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            *self
                .original
                .control_task
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
        }
    }
}
struct BlockerLoan {
    original: Arc<BootstrapSessionOwner>,
    connection: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
}
impl Drop for BlockerLoan {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            *self
                .original
                .control_blocker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(connection);
        }
    }
}

// Exterior owner of BOTH actual factory futures before any first acquire.
// Cancellation retains the same task/pools; it does not certify SDK closure.
struct FactoryOwner {
    state: Mutex<FactoryState>,
    join: Arc<tokio::sync::Semaphore>,
}
#[derive(Default)]
struct FactoryProbe {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[derive(Default)]
struct FactoryState {
    registered: bool,
    first_result: Option<std::result::Result<Arc<crate::PostgresPool>, crate::PoolError>>,
    probe: Option<Arc<FactoryProbe>>,
    original: Option<Arc<crate::PostgresPool>>,
    diagnostic: Option<Arc<crate::PostgresPool>>,
    task: Option<tokio::task::JoinHandle<FactoryResults>>,
    terminal: Option<std::result::Result<Arc<FactoryResults>, Arc<tokio::task::JoinError>>>,
    hold: Option<Arc<FactoryOwner>>,
}
#[derive(Clone)]
struct FactoryResults {
    original: std::result::Result<Arc<crate::PostgresPool>, crate::PoolError>,
    diagnostic: Option<std::result::Result<Arc<crate::PostgresPool>, crate::PoolError>>,
}
struct FactoryLoan {
    original: Arc<FactoryOwner>,
    task: Option<tokio::task::JoinHandle<FactoryResults>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for FactoryLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task = Some(task);
        }
    }
}
struct FactoryRefusal(Arc<FactoryOwner>);
impl std::fmt::Debug for FactoryRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_safe(f)
    }
}
impl std::fmt::Display for FactoryRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_safe(f)
    }
}
impl FactoryRefusal {
    fn fmt_safe(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = &self.0;
        f.write_str("original bootstrap factory task/pools retained; completion uncertain")
    }
}
impl std::error::Error for FactoryRefusal {}
impl FactoryOwner {
    fn capture(
        settings: ConnectionSettings,
        namespace: Option<NamedNamespace>,
        diagnostic_settings: Option<ConnectionSettings>,
        diagnostic_namespace: Option<NamedNamespace>,
        probe: Option<Arc<FactoryProbe>>,
    ) -> Arc<Self> {
        let original = Arc::new(Self {
            state: Mutex::new(FactoryState::default()),
            join: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        let mut state = original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.hold = Some(Arc::clone(&original)); // BEFORE dispatch; body terminal cannot erase owner.
        state.probe = probe.clone();
        let actual = Arc::clone(&original);
        state.task = Some(tokio::spawn(async move {
            // Same lock gates first factory poll until the actual JH is stored.
            let admitted = actual.state.lock().is_ok_and(|state| state.registered);
            if !admitted {
                return FactoryResults {
                    original: Err(crate::PoolError::Unavailable),
                    diagnostic: None,
                };
            }
            let result = crate::PostgresPool::connect_retained_in_namespace(
                settings,
                namespace.as_ref(),
                &mut |pool| {
                    let (mut state, poison) = match actual.state.lock() {
                        Ok(state) => (state, false),
                        Err(cause) => (cause.into_inner(), true),
                    };
                    state.original = Some(pool); // Exact latent SDK BEFORE fallible refusal/first acquire.
                    if poison {
                        Err(crate::PoolError::Unavailable)
                    } else {
                        Ok(())
                    }
                },
            )
            .await;
            actual
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .first_result = Some(result.clone());
            if let Some(probe) = probe {
                // Actual first factory/role/acquire future returned; its exact
                // pool/result is externally retained BEFORE this pause.
                probe.entered.notify_one();
                probe.release.notified().await;
            }
            let diagnostic = if let Some(settings) = diagnostic_settings {
                Some(
                    crate::PostgresPool::connect_retained_in_namespace(
                        settings,
                        diagnostic_namespace.as_ref(),
                        &mut |pool| {
                            let (mut state, poison) = match actual.state.lock() {
                                Ok(state) => (state, false),
                                Err(cause) => (cause.into_inner(), true),
                            };
                            state.diagnostic = Some(pool);
                            if poison {
                                Err(crate::PoolError::Unavailable)
                            } else {
                                Ok(())
                            }
                        },
                    )
                    .await,
                )
            } else {
                None
            };
            FactoryResults {
                original: result,
                diagnostic,
            }
        }));
        state.registered = true; // Immutable dispatch fact; loaning the JH cannot erase it.
        drop(state);
        original
    }
    fn release_after_known_sdk_closure(&self) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.task.is_some() || !matches!(&state.terminal, Some(Ok(_))) {
            return false;
        }
        state.hold.take();
        true
    }
    async fn finish(self: &Arc<Self>) -> std::result::Result<Arc<FactoryResults>, FactoryRefusal> {
        let refusal = || FactoryRefusal(Arc::clone(self));
        let permit = Arc::clone(&self.join)
            .acquire_owned()
            .await
            .map_err(|_| refusal())?;
        let task = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.task.take()
        };
        let mut loan = FactoryLoan {
            original: Arc::clone(self),
            task,
            _permit: permit,
        };
        if let Some(task) = loan.task.as_mut() {
            let joined = task.await.map(Arc::new).map_err(Arc::new);
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .terminal = Some(joined);
            loan.task = None; // Only SAME physical JH completion permits clearing.
        }
        let state = self.state.lock().map_err(|_| refusal())?;
        match &state.terminal {
            Some(Ok(result)) => Ok(Arc::clone(result)),
            _ => Err(refusal()),
        }
    }
}

struct Failure {
    original: Arc<BootstrapSessionOwner>,
    factories: Arc<FactoryOwner>,
    result: std::result::Result<(), BootstrapError>,
    acquisition: Option<crate::PoolError>,
    task_failure: Option<Arc<tokio::task::JoinError>>,
    diagnostic_acquisition: Option<crate::PoolError>,
    diagnostic_cleanup_acquisition: Option<crate::PoolError>,
    relay_report: Option<Arc<serde_json::Value>>,
    relay_error: Option<Arc<dyn std::error::Error + Send + Sync>>,
}
impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_safe(f)
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_safe(f)
    }
}
impl Failure {
    fn fmt_safe(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let retained = (
            &self.original,
            &self.factories,
            &self.result,
            &self.acquisition,
            &self.task_failure,
            &self.diagnostic_acquisition,
            &self.diagnostic_cleanup_acquisition,
            &self.relay_report,
            &self.relay_error,
        );
        let _ = retained;
        f.write_str("bootstrap control refusal [original diagnostics retained]")
    }
}
impl std::error::Error for Failure {}
async fn require_actual_empty(
    owner: &Arc<BootstrapSessionOwner>,
    namespace: Option<&NamedNamespace>,
) -> std::result::Result<(), BootstrapError> {
    let mut loan = owner.loan().await?;
    let empty = owner.query(sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1) AND NOT EXISTS (
SELECT 1 FROM (
SELECT relnamespace AS namespace FROM pg_catalog.pg_class
UNION ALL SELECT pronamespace FROM pg_catalog.pg_proc
UNION ALL SELECT typnamespace FROM pg_catalog.pg_type
UNION ALL SELECT collnamespace FROM pg_catalog.pg_collation
UNION ALL SELECT connamespace FROM pg_catalog.pg_constraint
UNION ALL SELECT connamespace FROM pg_catalog.pg_conversion
UNION ALL SELECT oprnamespace FROM pg_catalog.pg_operator
UNION ALL SELECT opcnamespace FROM pg_catalog.pg_opclass
UNION ALL SELECT opfnamespace FROM pg_catalog.pg_opfamily
UNION ALL SELECT cfgnamespace FROM pg_catalog.pg_ts_config
UNION ALL SELECT dictnamespace FROM pg_catalog.pg_ts_dict
UNION ALL SELECT prsnamespace FROM pg_catalog.pg_ts_parser
UNION ALL SELECT tmplnamespace FROM pg_catalog.pg_ts_template
UNION ALL SELECT stxnamespace FROM pg_catalog.pg_statistic_ext
UNION ALL SELECT extnamespace FROM pg_catalog.pg_extension
) objects JOIN pg_catalog.pg_namespace n ON n.oid=objects.namespace WHERE n.nspname=$1)").bind(namespace.map_or("codex_storage", NamedNamespace::schema)).fetch_one(loan.connection()?), false).await?;
    if empty {
        Ok(())
    } else {
        Err(BootstrapError::IncompatibleNamespace)
    }
}

fn original_provider_der_hash(state: &Path, receipt: &serde_json::Value) -> Result<String> {
    let mut bytes = Vec::new();
    std::fs::File::open(state.join("secrets/server.crt"))?
        .take(16 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 16 * 1024
        || receipt["file_hashes"]["server.crt"].as_str()
            != Some(format!("{:x}", sha2::Sha256::digest(&bytes)).as_str())
    {
        return Err("original provider PEM hash/size admission refused".into());
    }
    let mut leaves = rustls::pki_types::CertificateDer::pem_slice_iter(&bytes);
    let leaf = leaves
        .next()
        .transpose()?
        .ok_or("original provider leaf absent")?;
    if leaves.next().is_some() {
        return Err("ambiguous original provider leaf".into());
    }
    Ok(format!("{:x}", sha2::Sha256::digest(leaf.as_ref())))
}
fn original_relay_report(path: &str) -> Result<serde_json::Value> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(64 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 64 * 1024 {
        return Err("relay observation too large".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
async fn selected_bootstrap(state: &Path, named: bool, fault: u8) -> Result<()> {
    selected_bootstrap_with_diagnostic(state, named, fault, false).await
}
async fn selected_bootstrap_with_diagnostic(
    state: &Path,
    named: bool,
    fault: u8,
    recoverable: bool,
) -> Result<()> {
    let receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("receipt.json"))?)?;
    let role = if named {
        "isolation_migrator"
    } else {
        "migrator"
    };
    let namespace = named
        .then(|| NamedNamespace::new("codex_storage_isolation"))
        .transpose()?;
    // Fault 7 uses the OTHER genuine selected namespace, never inherited
    // search_path or a caller-made native server identity.
    let diagnostic_namespace = if fault == 7 {
        if named {
            None
        } else {
            Some(NamedNamespace::new("codex_storage_isolation")?)
        }
    } else {
        namespace.clone()
    };
    let mut settings = ConnectionSettings {
        host: "localhost".into(),
        port: u16::try_from(receipt["port"].as_u64().ok_or("missing fixture port")?)?,
        database: "codex".into(),
        username: format!("codex_{role}"),
        password: std::fs::read_to_string(state.join(format!("secrets/{role}.password")))?
            .trim()
            .to_owned()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: if fault == 5 { 2 } else { 1 },
        },
    };
    // Independently read the same protected genuine Migrator profile BEFORE
    // either factory future is polled. Original relay port never flows here.
    // ROOT must independently admit this OTHER protected diagnostic fixture.
    // These settings select a real endpoint/database; the native query, not
    // this locator, must witness the system/database mismatch below.
    let foreign_state = if fault == 8 {
        Some(std::path::PathBuf::from(std::env::var(
            "CODEX_TEST_BOOTSTRAP_FOREIGN_DIAGNOSTIC_STATE",
        )?))
    } else {
        None
    };
    let diagnostic_state = foreign_state.as_deref().unwrap_or(state);
    let diagnostic_receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(diagnostic_state.join("receipt.json"))?)?;
    let diagnostic_database = if fault == 9 {
        std::env::var("CODEX_TEST_BOOTSTRAP_FOREIGN_DIAGNOSTIC_DATABASE")?
    } else {
        "codex".to_owned()
    };
    let diagnostic_settings = recoverable
        .then(|| -> Result<ConnectionSettings> {
            Ok(ConnectionSettings {
                host: "localhost".into(),
                port: u16::try_from(
                    diagnostic_receipt["port"]
                        .as_u64()
                        .ok_or("missing diagnostic fixture port")?,
                )?,
                database: diagnostic_database.clone(),
                username: format!("codex_{role}"),
                password: std::fs::read_to_string(
                    diagnostic_state.join(format!("secrets/{role}.password")),
                )?
                .trim()
                .to_owned()
                .into(),
                ca_certificate: diagnostic_state.join("secrets/ca.crt"),
                limits: PoolLimits {
                    connect_timeout: Duration::from_secs(5),
                    acquire_timeout: Duration::from_secs(5),
                    max_connections: 1,
                },
            })
        })
        .transpose()?;
    let expected_provider_der = if recoverable && (fault == 6 || fault == 12) {
        Some(original_provider_der_hash(state, &receipt)?)
    } else {
        None
    };
    let relay_inputs = if fault == 6 || fault == 12 {
        let ready_path = std::env::var("CODEX_TEST_BOOTSTRAP_COMMIT_RELAY_READY")?;
        let ready: serde_json::Value = serde_json::from_slice(&std::fs::read(ready_path)?)?;
        settings.port = u16::try_from(ready["port"].as_u64().ok_or("relay port absent")?)?;
        Some((
            ready,
            std::env::var("CODEX_TEST_BOOTSTRAP_COMMIT_RELAY_RESULT")?,
        ))
    } else {
        None
    };
    // Actual factory task and latent SDK slots are exterior BEFORE first poll.
    let factory_probe = (fault == 10).then(|| Arc::new(FactoryProbe::default()));
    let factories = FactoryOwner::capture(
        settings,
        namespace.clone(),
        diagnostic_settings,
        diagnostic_namespace,
        factory_probe.clone(),
    );
    let factory_weak = Arc::downgrade(&factories);
    let mut factory_cancel_witness = true;
    if let Some(probe) = factory_probe {
        let original_id = {
            let state = factories.state.lock();
            state
                .ok()
                .and_then(|state| state.task.as_ref().map(tokio::task::JoinHandle::id))
        };
        let mut first = Box::pin(factories.finish());
        let entered = tokio::select! {
            biased;
            _ = &mut first => false,
            entered = tokio::time::timeout(Duration::from_secs(10), probe.entered.notified()) => entered.is_ok(),
        };
        drop(first); // Cancel caller ONLY; same stored task continues to own both factories.
        let restored = {
            let state = factories.state.lock();
            state.ok().is_some_and(|state| {
                state.task.as_ref().map(tokio::task::JoinHandle::id) == original_id
                    && original_id.is_some() && state.terminal.is_none()
                    && state.first_result.as_ref().zip(state.original.as_ref())
                        .is_some_and(|(result, captured)| matches!(result, Ok(returned) if Arc::ptr_eq(returned, captured)))
                    && state.probe.as_ref().is_some_and(|original| Arc::ptr_eq(original, &probe))
            })
        };
        factory_cancel_witness = entered && restored;
        probe.release.notify_one(); // Unconditional release BEFORE same finish and ANY verdict.
    }
    let factory_results = factories.finish().await?;
    let (pool, diagnostic_pool) = {
        let state = factories
            .state
            .lock()
            .map_err(|_| FactoryRefusal(Arc::clone(&factories)))?;
        (state.original.clone(), state.diagnostic.clone())
    };
    let Some(pool) = pool else {
        return Err(Box::new(FactoryRefusal(factories)));
    };
    let connected = factory_results.original.clone();
    let diagnostic_result = factory_results.diagnostic.clone();
    let owner = match diagnostic_pool.as_ref() {
        Some(diagnostic) => {
            BootstrapSessionOwner::new_recoverable(Arc::clone(&pool), Arc::clone(diagnostic))
        }
        None => BootstrapSessionOwner::new(Arc::clone(&pool)),
    };
    *owner
        .control_hold
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&owner));
    if recoverable && (fault == 6 || fault == 12) {
        *owner
            .control_verify_payload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(namespace.clone());
    }
    let mut task_failure = None;
    let mut witnessed = fault == 0 || fault == 10;
    let mut native_unknown_witnessed = !recoverable || fault != 3;
    let mut diagnostic_error = None;
    let diagnostic_ok = match diagnostic_result {
        None => !recoverable,
        Some(Ok(returned)) => diagnostic_pool
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(actual, &returned) && !Arc::ptr_eq(actual, &pool)),
        Some(Err(cause)) => {
            diagnostic_error = Some(cause);
            false
        } // Original driver remains in captured SDK ledger.
    };
    if fault == 7 {
        let original_factory_ok =
            matches!(&connected, Ok(returned) if Arc::ptr_eq(returned, &pool));
        let selected_mismatch = diagnostic_pool.as_ref().is_some_and(|diagnostic| {
            pool.selected_search_path.is_some()
                && diagnostic.selected_search_path.is_some()
                && pool.selected_search_path != diagnostic.selected_search_path
        });
        let refused_loan = owner.loan().await.is_err();
        let never_dispatched = owner.state.lock().ok().is_some_and(|state| {
            state.sealed
                && !state.started
                && state.session.is_none()
                && state.xid.is_none()
                && state.native_origin.is_none()
        });
        // Even a wrong test verdict must physically reconcile both actual
        // factory-created pools before returning an owning diagnostic.
        let proof = match owner.reconcile_original().await {
            Ok(proof) if proof.covers_pool(&pool) => proof,
            other => {
                let retained = (
                    owner,
                    pool,
                    diagnostic_pool,
                    connected,
                    diagnostic_error,
                    other,
                );
                let terminal = std::future::pending::<Result<()>>().await;
                let _ = retained;
                return terminal;
            }
        };
        let mut close = Box::pin(pool.close_original_sdk());
        let late = tokio::time::timeout(Duration::from_secs(10), &mut close)
            .await
            .is_err();
        if late {
            close.await;
        }
        let mut diagnostic_late = false;
        let mut diagnostic_closed = false;
        let mut diagnostic_cleanup_acquisition = None;
        if let Some(diagnostic) = diagnostic_pool.as_ref() {
            let mut close = Box::pin(diagnostic.close_original_sdk());
            diagnostic_late = tokio::time::timeout(Duration::from_secs(10), &mut close)
                .await
                .is_err();
            if diagnostic_late {
                close.await;
            }
            diagnostic_closed = match diagnostic.acquire().await {
                Err(crate::PoolError::Closed) => true,
                Ok(connection) => {
                    drop(connection);
                    false
                }
                Err(cause) => {
                    diagnostic_cleanup_acquisition = Some(cause);
                    false
                }
            };
        }
        let mut acquisition = None;
        let closed = match pool.acquire().await {
            Err(crate::PoolError::Closed) => true,
            Ok(connection) => {
                drop(connection);
                false
            }
            Err(cause) => {
                acquisition = Some(cause);
                false
            }
        };
        let factory_released =
            closed && diagnostic_closed && factories.release_after_known_sdk_closure();
        if factory_released {
            owner
                .control_hold
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
        if !original_factory_ok
            || !diagnostic_ok
            || !selected_mismatch
            || !refused_loan
            || !never_dispatched
            || proof.refused()
            || late
            || diagnostic_late
            || !closed
            || !diagnostic_closed
            || !factory_released
        {
            return Err(Box::new(Failure {
                original: owner,
                factories,
                result: Err(BootstrapError::Migration),
                acquisition,
                task_failure: None,
                diagnostic_acquisition: diagnostic_error,
                diagnostic_cleanup_acquisition,
                relay_report: None,
                relay_error: None,
            }));
        }
        drop(factories);
        if let Some(original) = factory_weak.upgrade() {
            return Err(Box::new(FactoryRefusal(original)));
        }
        return Ok(());
    }
    let result = match connected {
        Ok(returned) if Arc::ptr_eq(&returned, &pool) => {
            async {
                if !diagnostic_ok { return Err(BootstrapError::Unavailable); }
                require_actual_empty(&owner, namespace.as_ref()).await?;
                if fault == 0 || fault == 10 {
                    match namespace.as_ref() {
                        Some(namespace) => {
                            crate::bootstrap_named_namespace_retained(&owner, namespace).await
                        }
                        None => crate::bootstrap_codex_storage_retained(&owner).await,
                    }
                } else {
                    let mut blocker = BlockerLoan { original: Arc::clone(&owner), connection: None };
                    if fault == 5 {
                        let connection = pool.acquire().await.map_err(BootstrapError::Connection)?;
                        *owner.control_blocker.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(connection);
                        blocker.connection = owner.control_blocker.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
                        if let Some(connection) = blocker.connection.as_mut() {
                            owner.query(sqlx::query("SELECT pg_catalog.pg_advisory_lock($1,$2)")
                                .bind(crate::bootstrap::LOCK_CLASS).bind(crate::bootstrap::LOCK_RESOURCE)
                                .fetch_one(&mut **connection), false).await?;
                        }
                    }
                    let probe = Arc::new(StampProbe::default());
                    *match fault {
                        3 => &owner.commit_probe,
                        4 | 8 | 9 => &owner.outcome_probe,
                        _ => &owner.stamp_probe,
                    }
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&probe));
                    let actual = Arc::clone(&owner);
                    let selected = namespace.clone();
                    let start = Arc::new(tokio::sync::Notify::new());
                    let entry = Arc::clone(&start);
                    *owner
                        .control_hold
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(Arc::clone(&owner));
                    let task = tokio::spawn(async move {
                        entry.notified().await;
                        match selected.as_ref() {
                            Some(namespace) => {
                                crate::bootstrap_named_namespace_retained(&actual, namespace).await
                            }
                            None => crate::bootstrap_codex_storage_retained(&actual).await,
                        }
                    });
                    let original_id = task.id();
                    *owner
                        .control_task
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task);
                    let mut loan = TaskLoan {
                        original: Arc::clone(&owner),
                        task: owner
                            .control_task
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take(),
                    };
                    let Some(task) = loan.task.as_mut() else {
                        return Err(BootstrapError::Migration);
                    };
                    start.notify_one(); // Original handle captured before first SQL poll.
                    witnessed =
                        tokio::time::timeout(Duration::from_secs(10), probe.entered.notified())
                            .await
                            .is_ok();
                    if fault == 5 && witnessed {
                        let backend = *probe.backend.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        probe.release.notify_one(); // Now actual production advisory SQL can block.
                        witnessed = false;
                        if let (Some(backend), Some(connection)) = (backend, blocker.connection.as_mut()) {
                            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                            while tokio::time::Instant::now() < deadline {
                                let waiting = owner.query(sqlx::query_scalar::<_, bool>(
                                    "SELECT pg_catalog.pg_backend_pid() = ANY(pg_catalog.pg_blocking_pids($1)) AND EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted)")
                                    .bind(backend).fetch_one(&mut **connection), false).await;
                                match waiting {
                                    Ok(true) => { witnessed = true; break; }
                                    Ok(false) => tokio::task::yield_now().await,
                                    Err(_) => break,
                                }
                            }
                        }
                    }
                    if fault == 2 && witnessed {
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            let _guard = owner
                                .state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            std::panic::resume_unwind(Box::new(
                                "actual owner state poison control",
                            ));
                        }));
                    }
                    if recoverable && fault == 3 && witnessed {
                        // Actual DDL has run and its SAME task is paused before
                        // COMMIT. Observe its real in-progress xid before abort.
                        let original_facts = {
                            let state = owner.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            state.native_origin.clone().zip(state.xid.clone())
                        };
                        if let (Some(diagnostic), Some((origin, xid))) = (owner.diagnostic.as_ref(), original_facts) {
                            let observed = diagnostic.observe(&origin, &xid).await;
                            native_unknown_witnessed = observed.is_err() && diagnostic.original_native_status().is_some_and(|(actual, status)| {
                                Arc::ptr_eq(&actual, &xid) && status.as_deref() == Some("in progress")
                            });
                        }
                    }
                    if fault != 6 && fault != 8 && fault != 9 && fault != 12 { task.abort(); }
                    probe.release.notify_one();
                    let joined = task.await.map_err(Arc::new);
                    *owner.control_terminal.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(joined.clone());
                    loan.task = None; // Only actual SAME JoinHandle await can clear ownership.

                    if let Some(connection) = blocker.connection.as_mut() {
                        if owner.query(sqlx::query("SELECT pg_catalog.pg_advisory_unlock_all()")
                            .execute(&mut **connection), true).await.is_err() {
                            // Original blocker and task owner remain pinned, no false SDK close.
                            std::future::pending::<()>().await;
                        }
                    }
                    blocker.connection.take(); // Only after checked original unlock.
                    match joined {
                        // SAME original physical join before reconciliation/verdict.
                        Err(original) => {
                            witnessed &= original.is_cancelled() && original.id() == original_id;
                            task_failure = Some(original);
                            Err(BootstrapError::Migration)
                        }
                        Ok(result) => {
                            if fault == 6 || fault == 12 {
                                witnessed &= result == Err(BootstrapError::Unavailable);
                            } else if fault == 8 || fault == 9 {
                                witnessed &= result.is_ok();
                            } else { witnessed = false; }
                            result
                        }
                    }
                }
            }
            .await
        }
        Ok(_) => Err(BootstrapError::Migration),
        Err(error) => Err(BootstrapError::Connection(error)),
    };
    // Setup refusal/cancellation can restore the SAME blocker before task dispatch.
    let recovered_blocker = owner
        .control_blocker
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    let mut recovered_blocker = BlockerLoan {
        original: Arc::clone(&owner),
        connection: recovered_blocker,
    };
    if let Some(connection) = recovered_blocker.connection.as_mut() {
        if owner
            .query(
                sqlx::query("SELECT pg_catalog.pg_advisory_unlock_all()")
                    .execute(&mut **connection),
                true,
            )
            .await
            .is_err()
        {
            std::future::pending::<()>().await;
        }
    }
    recovered_blocker.connection.take();
    let foreign_origin_witness = if fault == 8 || fault == 9 {
        let original_facts = {
            let state = owner.state.lock().ok();
            state.and_then(|state| state.native_origin.clone().zip(state.xid.clone()))
        };
        match (owner.diagnostic.as_ref(), original_facts) {
            (Some(diagnostic), Some((origin, xid))) => {
                let refused = diagnostic.observe(&origin, &xid).await.is_err();
                let difference = diagnostic.control_origin_difference(&xid);
                refused
                    && diagnostic.original_native_status().is_none()
                    && !diagnostic.has_original_observation(&xid)
                    && if fault == 8 {
                        difference.is_some_and(|(system, _)| system)
                    } else {
                        difference == Some((false, true))
                    }
            }
            _ => false,
        }
    } else {
        true
    };
    // Never infer task/session termination from a bare bootstrap error.
    let reconciled = if recoverable && fault != 6 && fault != 8 && fault != 9 && fault != 12 {
        let mut result = owner.control_close_original_and_observe().await;
        for _ in 0..6 {
            let in_progress = owner.diagnostic.as_ref().is_some_and(|diagnostic| {
                diagnostic
                    .original_native_status()
                    .is_some_and(|(_, status)| status.as_deref() == Some("in progress"))
            });
            if result.is_ok() || !in_progress {
                break;
            }
            // Original native status is the retry witness, never a timeout ACK.
            tokio::task::yield_now().await;
            result = owner.control_close_original_and_observe().await;
        }
        result
    } else {
        let mut result = owner.reconcile_original().await;
        if fault == 12 {
            for _ in 0..6 {
                let original_xid = owner.state.lock().ok().and_then(|state| state.xid.clone());
                let same_original_in_progress =
                    owner.diagnostic.as_ref().is_some_and(|diagnostic| {
                        diagnostic
                            .original_native_status()
                            .zip(original_xid)
                            .is_some_and(|((observed, status), original)| {
                                Arc::ptr_eq(&observed, &original)
                                    && status.as_deref() == Some("in progress")
                            })
                    });
                if result.is_ok() || !same_original_in_progress {
                    break;
                }
                // Closing upstream can precede server abort. Retry only actual
                // same-xid in-progress evidence, with SAME cached hardclose and
                // diagnostic physical session; never a timeout or new factory.
                tokio::task::yield_now().await;
                result = owner.reconcile_original().await;
            }
        }
        result
    };
    let proof = match reconciled {
        Ok(proof) => proof,
        Err(original) => {
            if fault == 6 {
                // This report is observation from ROOT's protected original
                // relay process, never a SQL/session cleanup capability.
                let actual_session = {
                    let state = owner
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.session.is_some() && state.xid.is_some() && state.outcome.is_none()
                };
                let driver_cause = owner.original_driver_causes().is_some_and(|causes| {
                    causes
                        .iter()
                        .flatten()
                        .any(|cause| cause.downcast_ref::<sqlx::Error>().is_some())
                });
                let report = relay_inputs
                    .as_ref()
                    .and_then(|(_, path)| std::fs::read(path).ok())
                    .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok());
                let actual_server_tag = report.as_ref().is_some_and(|report| {
                    report["server_commit_tag_observed"] == true
                        && report["driver_commit_tag_forwarded"] == false
                        && report["original_threads_joined"] == true
                        && report["all_original_threads_started"] == true
                        && report["cleanup_refused"] == false
                        && report["release_authority"] == false
                        && report["provider_port"].as_u64() == receipt["port"].as_u64()
                        && relay_inputs.as_ref().is_some_and(|(ready, _)| {
                            ready["pid"].as_u64().is_some()
                                && report["pid"].as_u64() == ready["pid"].as_u64()
                                && report["relay_port"].as_u64() == ready["port"].as_u64()
                        })
                });
                if witnessed && actual_session && driver_cause && actual_server_tag {
                    eprintln!(
                        "actual bootstrap COMMIT reply withheld: original session/xid/cause retained; no cleanup ACK"
                    );
                } else {
                    eprintln!(
                        "COMMIT uncertainty control inconclusive: retaining same originals for ROOT reconciliation"
                    );
                }
                // No success verdict or SDK close from a server tag, driver
                // error, idle observation, or timeout. Same host stays owned.
                let retained = (owner, pool, original, result, relay_inputs, report);
                let terminal = std::future::pending::<Result<()>>().await;
                let _ = retained;
                return terminal;
            }
            let retained = (owner, pool, original, result);
            let terminal = std::future::pending::<Result<()>>().await;
            let _ = retained;
            return terminal;
        }
    };
    let outcome = {
        let state = owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.xid.clone().zip(state.outcome)
    };
    let native_recovery_fact = !recoverable
        || fault == 8
        || fault == 9
        || owner.diagnostic.as_ref().is_some_and(|diagnostic| {
            let original_xid = owner.state.lock().ok().and_then(|state| state.xid.clone());
            diagnostic
                .original_native_status()
                .zip(original_xid)
                .is_some_and(|((observed, status), original)| {
                    Arc::ptr_eq(&observed, &original)
                        && status.as_deref()
                            == Some(
                                if fault == 0
                                    || fault == 4
                                    || fault == 10
                                    || recoverable && fault == 6
                                {
                                    "committed"
                                } else {
                                    "aborted"
                                },
                            )
                })
        });
    let mut relay_error = None;
    let relay_report = if recoverable && (fault == 6 || fault == 12) {
        match relay_inputs
            .as_ref()
            .map(|(_, path)| original_relay_report(path))
        {
            Some(Ok(report)) => Some(Arc::new(report)),
            Some(Err(cause)) => {
                relay_error = Some(Arc::<dyn std::error::Error + Send + Sync>::from(cause));
                None
            }
            None => None,
        }
    } else {
        None
    };
    let lost_reply_witness = !recoverable
        || (fault != 6 && fault != 12)
        || relay_report.as_ref().is_some_and(|report| {
            let mode_witness = if fault == 12 {
                report["kind"] == "fixture-commit-request-suppression"
                    && report["fault_mode"] == "commit_request"
                    && report["client_commit_request_observed"] == true
                    && report["client_commit_request_forwarded"] == false
                    && matches!(
                        report["client_commit_request_kind"].as_str(),
                        Some("Q" | "P")
                    )
                    && report["client_commit_request_sha256"]
                        .as_str()
                        .is_some_and(|hash| {
                            hash.len() == 64
                                && hash.bytes().all(|byte| {
                                    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                                })
                        })
                    && report["server_commit_tag_observed"] == false
            } else {
                report["kind"] == "fixture-commit-reply-suppression"
                    && report["server_commit_tag_observed"] == true
                    && report["command_tag_frame_sha256"].as_str()
                        == Some(
                            format!("{:x}", sha2::Sha256::digest(b"C\0\0\0\x0bCOMMIT\0")).as_str(),
                        )
            };
            mode_witness
                && report["release_authority"] == false
                && report["driver_commit_tag_forwarded"] == false
                && report["original_threads_joined"] == true
                && report["all_original_threads_started"] == true
                && report["cleanup_refused"] == false
                && report["provider_port"].as_u64() == receipt["port"].as_u64()
                && report["provider_leaf_sha256"].as_str() == expected_provider_der.as_deref()
                && relay_inputs.as_ref().is_some_and(|(ready, _)| {
                    ready["kind"] == "fixture-commit-relay-ready"
                        && (fault != 12 || ready["fault_mode"] == "commit_request")
                        && ready["release_authority"] == false
                        && ready["pid"].as_u64().is_some_and(|pid| pid > 0)
                        && report["pid"].as_u64() == ready["pid"].as_u64()
                        && report["relay_port"].as_u64() == ready["port"].as_u64()
                })
                && owner
                    .diagnostic
                    .as_ref()
                    .is_some_and(|diagnostic| diagnostic.control_payload_verified())
                && result == Err(BootstrapError::Unavailable)
                && owner.original_driver_causes().is_some_and(|causes| {
                    causes
                        .iter()
                        .flatten()
                        .any(|cause| cause.downcast_ref::<sqlx::Error>().is_some())
                })
        });
    let covers = proof.covers_pool(&pool);
    let refused = proof.refused();
    if !covers {
        return Err(Box::new(Failure {
            original: owner,
            factories,
            result,
            acquisition: None,
            task_failure: task_failure.clone(),
            diagnostic_acquisition: diagnostic_error,
            diagnostic_cleanup_acquisition: None,
            relay_report: relay_report.clone(),
            relay_error: relay_error.clone(),
        }));
    }
    // Session proof precedes real SDK closure; both finish before diagnostics.
    let mut close = Box::pin(pool.close_original_sdk());
    let late = tokio::time::timeout(Duration::from_secs(10), &mut close)
        .await
        .is_err();
    if late {
        close.await;
    }
    let mut diagnostic_late = false;
    let mut diagnostic_cleanup_acquisition = None;
    let mut diagnostic_closed = !recoverable;
    if let Some(diagnostic) = diagnostic_pool.as_ref() {
        let mut close = Box::pin(diagnostic.close_original_sdk());
        diagnostic_late = tokio::time::timeout(Duration::from_secs(10), &mut close)
            .await
            .is_err();
        if diagnostic_late {
            close.await;
        }
        diagnostic_closed = match diagnostic.acquire().await {
            Err(crate::PoolError::Closed) => true,
            Ok(connection) => {
                drop(connection);
                false
            }
            Err(cause) => {
                diagnostic_cleanup_acquisition = Some(cause);
                false
            }
        };
    }
    let mut acquisition = None;
    let closed = match pool.acquire().await {
        Err(crate::PoolError::Closed) => true,
        Ok(connection) => {
            drop(connection);
            false
        }
        Err(error) => {
            acquisition = Some(error);
            false
        }
    };
    let factory_released =
        closed && diagnostic_closed && factories.release_after_known_sdk_closure();
    if factory_released {
        owner
            .control_hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
    if (fault == 6 || fault == 12) && !recoverable
        || (fault == 0 && result.is_err())
        || !witnessed
        || late
        || !closed
        || diagnostic_late
        || !diagnostic_closed
        || !diagnostic_ok
        || !lost_reply_witness
        || !foreign_origin_witness
        || !factory_released
        || !factory_cancel_witness
    {
        return Err(Box::new(Failure {
            original: owner,
            factories,
            result,
            acquisition,
            task_failure: task_failure.clone(),
            diagnostic_acquisition: diagnostic_error,
            diagnostic_cleanup_acquisition,
            relay_report: relay_report.clone(),
            relay_error: relay_error.clone(),
        }));
    }
    if !covers
        || !native_unknown_witnessed
        || !native_recovery_fact
        || refused != (fault == 2)
        || !matches!(outcome, Some((_, committed)) if committed == (fault == 0 || fault == 4 || fault == 8 || fault == 9 || fault == 10 || recoverable && fault == 6))
    {
        return Err(Box::new(Failure {
            original: owner,
            factories,
            result,
            acquisition,
            task_failure: task_failure.clone(),
            diagnostic_acquisition: diagnostic_error,
            diagnostic_cleanup_acquisition,
            relay_report: relay_report.clone(),
            relay_error: relay_error.clone(),
        }));
    }
    drop(factories);
    if let Some(original) = factory_weak.upgrade() {
        return Err(Box::new(FactoryRefusal(original)));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "ROOT retained uncertainty host only: actual default relay/session reconciliation required"]
async fn actual_default_commit_reply_lost_original_session_retained() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_LOST_ACK_STATE",
        )?),
        false,
        6,
    )
    .await
}
#[tokio::test]
#[ignore = "ROOT retained uncertainty host only: actual named relay/session reconciliation required"]
async fn actual_named_commit_reply_lost_original_session_retained() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_LOST_ACK_STATE",
        )?),
        true,
        6,
    )
    .await
}
#[tokio::test]
#[ignore = "requires an admitted isolated default PostgreSQL bootstrap fixture"]
async fn actual_default_bootstrap_original_committed_session() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_STATE",
        )?),
        false,
        0,
    )
    .await
}
#[tokio::test]
#[ignore = "requires an admitted isolated named PostgreSQL bootstrap fixture"]
async fn actual_named_bootstrap_original_committed_session() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var("CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_STATE")?),
        true,
        0,
    )
    .await
}

#[tokio::test]
#[ignore = "requires an admitted fresh empty default bootstrap fixture"]
async fn actual_default_xid_abort_and_poison_reconcile_original() -> Result<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_STATE")?;
    selected_bootstrap(Path::new(&state), false, 1).await?;
    selected_bootstrap(Path::new(&state), false, 2).await
}
#[tokio::test]
#[ignore = "requires an admitted fresh empty named bootstrap fixture"]
async fn actual_named_xid_abort_and_poison_reconcile_original() -> Result<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_STATE")?;
    selected_bootstrap(Path::new(&state), true, 1).await?;
    selected_bootstrap(Path::new(&state), true, 2).await
}

#[tokio::test]
#[ignore = "requires a separate admitted empty default precommit fixture"]
async fn actual_default_after_ddl_before_commit_abort() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_PRECOMMIT_STATE",
        )?),
        false,
        3,
    )
    .await
}
#[tokio::test]
#[ignore = "requires a separate admitted empty named precommit fixture"]
async fn actual_named_after_ddl_before_commit_abort() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_PRECOMMIT_STATE",
        )?),
        true,
        3,
    )
    .await
}
#[tokio::test]
#[ignore = "requires a separate admitted empty default postcommit fixture"]
async fn actual_default_committed_status_before_return_abort() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_POSTCOMMIT_STATE",
        )?),
        false,
        4,
    )
    .await
}
#[tokio::test]
#[ignore = "requires a separate admitted empty named postcommit fixture"]
async fn actual_named_committed_status_before_return_abort() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_POSTCOMMIT_STATE",
        )?),
        true,
        4,
    )
    .await
}

#[tokio::test]
#[ignore = "requires a separate admitted empty default advisory-wait fixture"]
async fn actual_default_advisory_wait_original_abort() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_WAIT_STATE",
        )?),
        false,
        5,
    )
    .await
}
#[tokio::test]
#[ignore = "requires a separate admitted empty named advisory-wait fixture"]
async fn actual_named_advisory_wait_original_abort() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_WAIT_STATE",
        )?),
        true,
        5,
    )
    .await
}

#[tokio::test]
#[ignore = "requires fresh admitted default empty schema and genuine pg_control_system privilege; no grants"]
async fn actual_default_diagnostic_owner_committed_bootstrap() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_DIAGNOSTIC_COMMIT_STATE",
        )?),
        false,
        0,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires fresh admitted named empty schema and genuine pg_control_system privilege; no grants"]
async fn actual_named_diagnostic_owner_committed_bootstrap() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_DIAGNOSTIC_COMMIT_STATE",
        )?),
        true,
        0,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires separate admitted default empty schema and genuine pg_control_system privilege; no grants"]
async fn actual_default_diagnostic_owner_actual_ddl_abort() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_DIAGNOSTIC_ABORT_STATE",
        )?),
        false,
        3,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires separate admitted named empty schema and genuine pg_control_system privilege; no grants"]
async fn actual_named_diagnostic_owner_actual_ddl_abort() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_DIAGNOSTIC_ABORT_STATE",
        )?),
        true,
        3,
        true,
    )
    .await
}

#[tokio::test]
#[ignore = "requires separately owned actual TLS relay, fresh default fixture and migrator capability; no grants"]
async fn actual_default_lost_commit_reply_native_recovery() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_LOST_REPLY_RECOVERY_STATE",
        )?),
        false,
        6,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires separately owned actual TLS relay, fresh named fixture and migrator capability; no grants"]
async fn actual_named_lost_commit_reply_native_recovery() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_LOST_REPLY_RECOVERY_STATE",
        )?),
        true,
        6,
        true,
    )
    .await
}

#[tokio::test]
#[ignore = "requires ROOT-admitted default and named migrator namespaces; no privilege fallback"]
async fn default_bootstrap_rejects_named_diagnostic_namespace() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_FOREIGN_NAMESPACE_STATE",
        )?),
        false,
        7,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires ROOT-admitted named and default migrator namespaces; no privilege fallback"]
async fn named_bootstrap_rejects_default_diagnostic_namespace() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_FOREIGN_NAMESPACE_STATE",
        )?),
        true,
        7,
        true,
    )
    .await
}

#[tokio::test]
#[ignore = "requires separate ROOT-admitted actual foreign PostgreSQL instance and migrator control-function privilege"]
async fn default_bootstrap_rejects_foreign_instance_diagnostic() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_FOREIGN_INSTANCE_STATE",
        )?),
        false,
        8,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires separate ROOT-admitted actual foreign PostgreSQL instance and named migrator privilege"]
async fn named_bootstrap_rejects_foreign_instance_diagnostic() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_FOREIGN_INSTANCE_STATE",
        )?),
        true,
        8,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires ROOT-admitted second actual database on same instance with genuine migrator control-function privilege"]
async fn default_bootstrap_rejects_foreign_database_diagnostic() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_FOREIGN_DATABASE_STATE",
        )?),
        false,
        9,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires ROOT-admitted second actual database on same instance with genuine named migrator privilege"]
async fn named_bootstrap_rejects_foreign_database_diagnostic() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_FOREIGN_DATABASE_STATE",
        )?),
        true,
        9,
        true,
    )
    .await
}

#[tokio::test]
#[ignore = "requires ROOT-admitted actual empty default namespace and migrator control-function capability"]
async fn default_factory_caller_cancel_retains_same_native_task() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_FACTORY_CANCEL_STATE",
        )?),
        false,
        10,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires ROOT-admitted actual empty named namespace and migrator control-function capability"]
async fn named_factory_caller_cancel_retains_same_native_task() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_FACTORY_CANCEL_STATE",
        )?),
        true,
        10,
        true,
    )
    .await
}

#[tokio::test]
#[ignore = "requires ROOT-admitted original empty default fixture and protected pre-forward COMMIT request-loss relay"]
async fn default_commit_request_loss_recovers_actual_aborted_xid_and_empty_namespace() -> Result<()>
{
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_COMMIT_REQUEST_LOSS_STATE",
        )?),
        false,
        12,
        true,
    )
    .await
}
#[tokio::test]
#[ignore = "requires ROOT-admitted original empty named fixture and protected pre-forward COMMIT request-loss relay"]
async fn named_commit_request_loss_recovers_actual_aborted_xid_and_empty_namespace() -> Result<()> {
    selected_bootstrap_with_diagnostic(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_COMMIT_REQUEST_LOSS_STATE",
        )?),
        true,
        12,
        true,
    )
    .await
}
