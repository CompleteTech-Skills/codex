//! Original operation resources retained outside command and cleanup tasks.
//! No typed terminal/cleanup authority is constructed by a slot or by Drop.
use crate::service::StorageServiceInputs;
use codex_remote_storage::RemoteStorage;
use codex_state::SqliteConfig;
use codex_storage_migration::SqliteTarget;
use std::sync::Arc;
use std::sync::Mutex;
const CAPACITY: usize = 8;
pub(crate) struct Resources {
    _original_inputs: Arc<StorageServiceInputs>,
    _original_startups: Arc<crate::MigrationStartupRegistry>,
    runtimes: Arc<Slots<RemoteStorage>>,
    runtime_pools: Arc<Slots<codex_postgres_runtime::PostgresPool>>,
    targets: Arc<Slots<SqliteTarget>>,
    staged_configs: Arc<Slots<SqliteConfig>>,
    read_sources: Arc<Slots<codex_storage_migration::RetainedSqliteSource>>,
    bootstrap_sessions: Arc<Slots<codex_postgres_runtime::BootstrapSessionOwner>>,
    bootstrap_cleanup_causes:
        Mutex<[Option<Arc<codex_postgres_runtime::BootstrapSessionRefusal>>; CAPACITY]>,
    cleanup_causes: Mutex<[Option<Arc<std::io::Error>>; CAPACITY]>,
    admission_failure: Mutex<Option<Refusal>>,
    body_result: Mutex<Option<Result<crate::service_operations::Output, crate::StorageError>>>,
    body_returned: Mutex<bool>,
    body_done: tokio::sync::Notify,
    #[cfg(test)]
    bootstrap_expected_empty: std::sync::atomic::AtomicBool,
    cleanup: Mutex<Cleanup>,
    cleanup_join: tokio::sync::Semaphore,
}
impl Resources {
    pub(crate) fn new(
        original_inputs: Arc<StorageServiceInputs>,
        original_startups: Arc<crate::MigrationStartupRegistry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            _original_inputs: original_inputs,
            _original_startups: original_startups,
            runtimes: Arc::default(),
            runtime_pools: Arc::default(),
            targets: Arc::default(),
            staged_configs: Arc::default(),
            read_sources: Arc::default(),
            bootstrap_sessions: Arc::default(),
            bootstrap_cleanup_causes: Mutex::new(std::array::from_fn(|_| None)),
            cleanup_causes: Mutex::new(std::array::from_fn(|_| None)),
            body_result: Mutex::new(None),
            admission_failure: Mutex::new(None),
            body_returned: Mutex::new(false),
            body_done: tokio::sync::Notify::new(),
            #[cfg(test)]
            bootstrap_expected_empty: std::sync::atomic::AtomicBool::new(false),
            cleanup: Mutex::new(Cleanup::default()),
            cleanup_join: tokio::sync::Semaphore::new(1),
        })
    }
    #[cfg(test)]
    pub(crate) fn expect_empty_bootstrap(&self, expected: bool) {
        self.bootstrap_expected_empty
            .store(expected, std::sync::atomic::Ordering::Relaxed);
    }
    #[cfg(test)]
    pub(crate) fn bootstrap_expected_empty(&self) -> bool {
        self.bootstrap_expected_empty
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    pub(crate) fn original_startups(&self) -> Arc<crate::MigrationStartupRegistry> {
        Arc::clone(&self._original_startups)
    }
    // Called only after the original dispatch body's actual future returns.
    pub(crate) fn record_body_returned(
        &self,
        result: Result<crate::service_operations::Output, crate::StorageError>,
    ) {
        *self
            .body_result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        *self
            .body_returned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.body_done.notify_waiters();
    }
    // Only the physically joined SAME operation task can prove a panicked/aborted
    // body can no longer install resources. This is not a successful body result.
    pub(crate) fn record_joined_task_terminal(&self) {
        *self
            .body_returned
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.body_done.notify_waiters();
    }
    pub(crate) fn body_has_returned(&self) -> Result<bool, Refusal> {
        self.body_returned
            .lock()
            .map(|returned| *returned)
            .map_err(|_| Refusal)
    }
    pub(crate) async fn wait_body_returned(&self) -> Result<(), Refusal> {
        loop {
            let wake = self.body_done.notified();
            tokio::pin!(wake);
            wake.as_mut().enable(); // Register before checking the actual cached producer state.
            if self.body_has_returned()? {
                return Ok(());
            }
            wake.await;
        }
    }
    pub(crate) fn record_admission_failure(&self, original: Refusal) {
        let mut saved = self
            .admission_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if saved.is_none() {
            *saved = Some(original);
        }
    }
    pub(crate) fn reserve_runtime(&self) -> Result<Creation<RemoteStorage>, Refusal> {
        self.runtimes.reserve()
    }
    pub(crate) fn reserve_runtime_pool(
        &self,
    ) -> Result<Creation<codex_postgres_runtime::PostgresPool>, Refusal> {
        self.runtime_pools.reserve()
    }
    #[cfg(test)]
    pub(crate) fn actual_runtime_sdk_snapshot(
        &self,
    ) -> Result<[Option<Arc<codex_postgres_runtime::PostgresPool>>; CAPACITY], Refusal> {
        let state = self.runtime_pools.state.lock().map_err(|_| Refusal)?;
        Ok(std::array::from_fn(|index| {
            state.entries[index].installed.as_ref().map(Arc::clone)
        }))
    }
    pub(crate) fn retains_original_causes(&self) -> bool {
        if self
            .admission_failure
            .lock()
            .map_or(true, |failure| failure.is_some())
        {
            return true;
        }
        if self
            .cleanup_causes
            .lock()
            .map_or(true, |causes| causes.iter().any(Option::is_some))
        {
            return true;
        }
        let pools = self.runtime_pools.state.lock();
        let Ok(pools) = pools else {
            return true;
        };
        pools.entries.iter().any(|entry| {
            entry.returned_failure.is_some()
                || entry.installed.as_ref().is_some_and(|pool| {
                    pool.retains_original_runtime_causes().unwrap_or(true)
                        || self
                            ._original_startups
                            .causes_for_runtime(pool)
                            .unwrap_or(true)
                })
        })
    }
    #[cfg(test)]
    pub(crate) fn actual_runtime_snapshot(
        &self,
    ) -> Result<[Option<Arc<RemoteStorage>>; CAPACITY], Refusal> {
        let state = self.runtimes.state.lock().map_err(|_| Refusal)?;
        Ok(std::array::from_fn(|index| {
            state.entries[index].installed.as_ref().map(Arc::clone)
        }))
    }
    #[cfg(test)]
    pub(crate) fn actual_body_failed(&self) -> Result<bool, Refusal> {
        self.body_result
            .lock()
            .map(|result| matches!(*result, Some(Err(_))))
            .map_err(|_| Refusal)
    }
    pub(crate) fn owns_original_runtime_pool(
        &self,
        original: &Arc<codex_postgres_runtime::PostgresPool>,
    ) -> Result<bool, Refusal> {
        let state = self.runtime_pools.state.lock().map_err(|_| Refusal)?;
        Ok(state.entries.iter().any(|entry| {
            entry
                .installed
                .as_ref()
                .is_some_and(|pool| Arc::ptr_eq(pool, original))
        }))
    }
    pub(crate) fn retain_staged_config(&self, original: SqliteConfig) -> Result<(), Refusal> {
        let slot = self.staged_configs.reserve()?;
        slot.install(original).map_err(|_| Refusal)?;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn actual_read_sources(
        &self,
    ) -> Result<[Option<Arc<codex_storage_migration::RetainedSqliteSource>>; CAPACITY], Refusal>
    {
        let state = self.read_sources.state.lock().map_err(|_| Refusal)?;
        Ok(std::array::from_fn(|index| {
            state.entries[index].installed.as_ref().map(Arc::clone)
        }))
    }
    pub(crate) fn retain_bootstrap_session(
        &self,
        pool: Arc<codex_postgres_runtime::PostgresPool>,
        diagnostic_pool: Arc<codex_postgres_runtime::PostgresPool>,
    ) -> Result<Arc<codex_postgres_runtime::BootstrapSessionOwner>, Refusal> {
        let slot = self.bootstrap_sessions.reserve()?;
        let owner =
            codex_postgres_runtime::BootstrapSessionOwner::new_recoverable(pool, diagnostic_pool);
        slot.capture_original_arc(Arc::clone(&owner))?;
        Ok(owner)
    }
    pub(crate) fn retain_read_source(
        &self,
        source: codex_storage_migration::SqliteSource,
    ) -> Result<Arc<codex_storage_migration::RetainedSqliteSource>, Refusal> {
        let slot = self.read_sources.reserve()?;
        slot.install(codex_storage_migration::RetainedSqliteSource::new(source))
            .map_err(|_| Refusal)
    }
    pub(crate) fn reserve_target(&self) -> Result<Creation<SqliteTarget>, Refusal> {
        self.targets.reserve()
    }
}
pub(crate) struct Refusal;
pub(crate) struct Slots<T> {
    state: Mutex<State<T>>,
}
struct State<T> {
    entries: [Entry<T>; CAPACITY],
    sealed: bool,
}
struct Entry<T> {
    reserved: bool,
    installed: Option<Arc<T>>,
    returned_failure: Option<Arc<dyn std::error::Error + Send + Sync>>,
    absence_proven: bool,
}
impl<T> Default for Slots<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                entries: std::array::from_fn(|_| Entry {
                    reserved: false,
                    installed: None,
                    returned_failure: None,
                    absence_proven: false,
                }),
                sealed: false,
            }),
        }
    }
}
impl<T> Slots<T> {
    fn seal_cleanup_snapshot(&self) -> ([Option<Arc<T>>; CAPACITY], bool, bool) {
        let (mut state, poison) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(cause) => (cause.into_inner(), true),
        };
        state.sealed = true;
        let unresolved = state
            .entries
            .iter()
            .any(|entry| entry.reserved && entry.installed.is_none() && !entry.absence_proven);
        (
            std::array::from_fn(|index| state.entries[index].installed.as_ref().map(Arc::clone)),
            unresolved,
            poison,
        )
    }

    fn reserve(self: &Arc<Self>) -> Result<Creation<T>, Refusal> {
        let mut state = self.state.lock().map_err(|_| Refusal)?;
        if state.sealed {
            return Err(Refusal);
        }
        let index = state
            .entries
            .iter()
            .position(|entry| !entry.reserved)
            .ok_or(Refusal)?;
        let entry = state.entries.get_mut(index).ok_or(Refusal)?;
        entry.reserved = true; // Before any caller creation future is polled.
        Ok(Creation {
            original: Arc::clone(self),
            index,
        })
    }
}
/// Private slot identity is generated only by the original retained array.
/// Unwind/caller Drop deliberately leaves an unresolved original creation slot.
pub(crate) struct Creation<T> {
    original: Arc<Slots<T>>,
    index: usize,
}
pub(crate) struct InstallationRefusal<T> {
    pub(crate) original_value: Arc<T>,
    original_slots: Arc<Slots<T>>,
}
impl<T> InstallationRefusal<T> {
    pub(crate) fn original_slots(&self) -> &Arc<Slots<T>> {
        &self.original_slots
    }
}
impl<T> Creation<T> {
    // Called only when the actual creation future returned this original failure.
    // Unwind/Drop still leaves unresolved, and captures no fabricated completion.
    pub(crate) fn record_returned_failure(self, cause: Arc<dyn std::error::Error + Send + Sync>) {
        let mut state = self
            .original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = state.entries.get_mut(self.index) {
            if entry.reserved {
                if entry.returned_failure.is_none() {
                    entry.returned_failure = Some(cause);
                }
                if entry.installed.is_none() {
                    entry.absence_proven = true;
                }
            }
        }
    }

    pub(crate) fn capture_original_arc(&self, value: Arc<T>) -> Result<(), Refusal> {
        let (mut state, poisoned) = match self.original.state.lock() {
            Ok(state) => (state, false),
            Err(cause) => (cause.into_inner(), true),
        };
        let sealed = state.sealed;
        if let Some(entry) = state.entries.get_mut(self.index) {
            if entry.reserved && entry.installed.is_none() {
                entry.installed = Some(value); // Retain actual SDK BEFORE any refusal/acquire.
                if !poisoned && !sealed {
                    return Ok(());
                }
            }
        }
        Err(Refusal)
    }
    pub(crate) fn install(self, value: T) -> Result<Arc<T>, InstallationRefusal<T>> {
        let value = Arc::new(value);
        let (mut state, poisoned) = match self.original.state.lock() {
            Ok(state) => (state, false),
            Err(original_poison) => (original_poison.into_inner(), true),
        };
        let sealed = state.sealed;
        if let Some(entry) = state.entries.get_mut(self.index) {
            if entry.reserved && entry.installed.is_none() {
                entry.installed = Some(Arc::clone(&value)); // Retain before ANY refusal.
                if !poisoned && !sealed {
                    return Ok(value);
                }
            }
        }
        Err(InstallationRefusal {
            original_value: value,
            original_slots: Arc::clone(&self.original),
        })
    }
}

#[derive(Default)]
struct Cleanup {
    task: Option<tokio::task::JoinHandle<bool>>,
    result: Option<bool>,
    failure: Option<tokio::task::JoinError>,
}
/// Scoped completion of actual original SDK-close futures, never Home/socket authority.
pub(crate) struct ResourcesClosed {
    original: Arc<Resources>,
}
impl ResourcesClosed {
    pub(crate) fn original(&self) -> &Arc<Resources> {
        &self.original
    }
}
pub(crate) struct CleanupRefusal {
    pub(crate) original: Arc<Resources>,
}
impl Resources {
    pub(crate) fn cleanup_completed(&self) -> bool {
        self.cleanup.lock().is_ok_and(|state| {
            state.result == Some(true) && state.task.is_none() && state.failure.is_none()
        })
    }
    pub(crate) async fn finish_cleanup(
        self: &Arc<Self>,
        startups: &Arc<crate::MigrationStartupRegistry>,
    ) -> Result<ResourcesClosed, CleanupRefusal> {
        let refused = || CleanupRefusal {
            original: Arc::clone(self),
        };
        let _permit = self.cleanup_join.acquire().await.map_err(|_| refused())?;
        let task = {
            let (mut state, cleanup_poison) = match self.cleanup.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            if state.task.is_none() && state.result.is_none() && state.failure.is_none() {
                let (returned, body_poison) = match self.body_returned.lock() {
                    Ok(state) => (*state, false),
                    Err(cause) => (*cause.into_inner(), true),
                };
                if !returned {
                    return Err(refused());
                }
                let (runtimes, runtime_unresolved, runtime_poison) =
                    self.runtimes.seal_cleanup_snapshot();
                let (pools, pool_unresolved, pool_poison) =
                    self.runtime_pools.seal_cleanup_snapshot();
                let (targets, target_unresolved, target_poison) =
                    self.targets.seal_cleanup_snapshot();
                let (configs, config_unresolved, config_poison) =
                    self.staged_configs.seal_cleanup_snapshot();
                let (read_sources, read_unresolved, read_poison) =
                    self.read_sources.seal_cleanup_snapshot();
                let (bootstrap_sessions, bootstrap_unresolved, bootstrap_poison) =
                    self.bootstrap_sessions.seal_cleanup_snapshot();
                let original = Arc::clone(self);
                let startups = Arc::clone(startups);
                state.task = Some(tokio::spawn(async move {
                    {
                        let _entry = original
                            .cleanup
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                    let mut complete = !(cleanup_poison
                        || body_poison
                        || runtime_poison
                        || pool_poison
                        || target_poison
                        || config_poison
                        || runtime_unresolved
                        || pool_unresolved
                        || target_unresolved
                        || config_unresolved
                        || read_unresolved
                        || read_poison
                        || bootstrap_unresolved
                        || bootstrap_poison);
                    let mut blocked_bootstrap_pools =
                        std::array::from_fn::<_, CAPACITY, _>(|_| None);
                    let mut blocked_diagnostic_pools =
                        std::array::from_fn::<_, CAPACITY, _>(|_| None);
                    for (index, owner) in bootstrap_sessions.into_iter().enumerate() {
                        if let Some(owner) = owner {
                            match owner.reconcile_original().await {
                                Ok(receipt) if receipt.covers_pool(owner.original_pool()) => {
                                    complete &= !receipt.refused();
                                }
                                Ok(_) => {
                                    blocked_bootstrap_pools[index] =
                                        Some(Arc::clone(owner.original_pool()));
                                    blocked_diagnostic_pools[index] =
                                        owner.diagnostic_pool().map(Arc::clone);
                                    complete = false;
                                }
                                Err(cause) => {
                                    blocked_bootstrap_pools[index] =
                                        Some(Arc::clone(owner.original_pool()));
                                    blocked_diagnostic_pools[index] =
                                        owner.diagnostic_pool().map(Arc::clone);
                                    original
                                        .bootstrap_cleanup_causes
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        [index] = Some(Arc::new(cause));
                                    complete = false;
                                }
                            }
                        }
                    }
                    for source in read_sources.into_iter().flatten() {
                        // Diagnostic failure stays in the SAME externally retained source;
                        // it is distinct from the actual SDK-close completion below.
                        complete &= source.original_read_causes().is_ok();
                        if source.close_original_read_pools().await.is_err() {
                            complete = false;
                        }
                    }
                    for target in targets.into_iter().flatten() {
                        target.close().await;
                    }
                    for (index, config) in configs.into_iter().enumerate() {
                        if let Some(config) = config {
                            match config.seal_and_close_write_pools().await {
                                Ok(receipt) if receipt.covers_config(&config) => {}
                                Ok(_) => complete = false,
                                Err(original_cause) => {
                                    original
                                        .cleanup_causes
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                                        [index] = Some(Arc::new(original_cause));
                                    complete = false;
                                }
                            }
                        }
                    }
                    for pool in pools.into_iter().flatten() {
                        if blocked_bootstrap_pools
                            .iter()
                            .chain(blocked_diagnostic_pools.iter())
                            .flatten()
                            .any(|blocked| Arc::ptr_eq(blocked, &pool))
                        {
                            continue;
                        }
                        if let Some(runtime) = runtimes
                            .iter()
                            .flatten()
                            .find(|runtime| Arc::ptr_eq(runtime.pool(), &pool))
                        {
                            match startups.close_for(runtime, &original).await {
                                Ok(receipt) => {
                                    complete &= !receipt.refused;
                                }
                                Err(_) => {
                                    complete = false;
                                    continue; // No actual startup ACK: keep original Runtime SDK.
                                }
                            }
                        }
                        pool.close_original_sdk().await;
                    }
                    complete
                }));
            }
            state.task.take()
        };
        if let Some(task) = task {
            let mut loan = CleanupLoan {
                original: Arc::clone(self),
                task: Some(task),
            };
            let result = match loan.task.as_mut() {
                Some(task) => task.await,
                None => return Err(refused()),
            };
            loan.task.take();
            let mut state = self
                .cleanup
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match result {
                Ok(result) => state.result = Some(result),
                Err(cause) => state.failure = Some(cause),
            }
        }
        if !self.cleanup_completed() {
            return Err(refused());
        }
        Ok(ResourcesClosed {
            original: Arc::clone(self),
        })
    }
}
struct CleanupLoan {
    original: Arc<Resources>,
    task: Option<tokio::task::JoinHandle<bool>>,
}
impl Drop for CleanupLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.original
                .cleanup
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task = Some(task);
        }
    }
}
