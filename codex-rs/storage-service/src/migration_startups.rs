//! Per-service originals retained before migration login/role SQL is polled.
//! This registry is not a Home, physical-socket, or all-writers-drained permit.
use crate::BlockerCode;
use crate::StorageError;
use crate::StorageService;
use crate::operation_resources::Resources;
use codex_postgres_runtime::OriginalMigrationStartup;
use codex_postgres_runtime::PostgresPool;
use codex_remote_storage::LoginRole;
use codex_remote_storage::RemoteStorage;
use codex_remote_storage::connection_settings;
use codex_storage_authority::HostCredentialResolver;
use std::sync::Arc;
use std::sync::Mutex;

const MAX_ORIGINAL_STARTUPS: usize = 16;
#[derive(Default)]
pub struct MigrationStartupRegistry {
    state: Mutex<RegistryState>,
}
#[derive(Default)]
struct RegistryState {
    originals: Vec<Entry>,
    history: Vec<Arc<OriginalMigrationStartup>>,
}
struct Entry {
    original: Arc<OriginalMigrationStartup>,
    borrowers: Vec<Arc<Resources>>,
    closing: bool,
}
// Private actual cleanup evidence, separate from the preserved admission diagnostic.
// NoStartup is constructed only under an unpoisoned actual registry observation.
pub(crate) struct StartupCleanup {
    _original: Option<codex_postgres_runtime::OriginalMigrationCleanupCompleted>,
    pub(crate) refused: bool,
}
impl MigrationStartupRegistry {
    fn borrow_existing(
        &self,
        runtime: &Arc<PostgresPool>,
        borrower: &Arc<Resources>,
    ) -> Result<Option<Arc<OriginalMigrationStartup>>, StorageError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        let Some(entry) = state
            .originals
            .iter_mut()
            .find(|entry| Arc::ptr_eq(entry.original.original_runtime(), runtime))
        else {
            return Ok(None);
        };
        Self::borrow_entry(entry, borrower)?;
        Ok(Some(Arc::clone(&entry.original)))
    }
    fn borrow_entry(entry: &mut Entry, borrower: &Arc<Resources>) -> Result<(), StorageError> {
        if borrower.body_has_returned().map_err(|_| unavailable())? {
            return Err(unavailable());
        }
        if !entry
            .borrowers
            .iter()
            .any(|saved| Arc::ptr_eq(saved, borrower))
        {
            if entry.closing {
                return Err(unavailable());
            }
            if entry.borrowers.len() >= MAX_ORIGINAL_STARTUPS {
                return Err(unavailable());
            }
            entry.borrowers.try_reserve(1).map_err(|_| unavailable())?;
            entry.borrowers.push(Arc::clone(borrower));
        }
        Ok(())
    }
    fn retain(
        &self,
        original: Arc<OriginalMigrationStartup>,
        refused: bool,
        borrower: &Arc<Resources>,
    ) -> Result<Arc<OriginalMigrationStartup>, StorageError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if let Some(existing) = state.originals.iter_mut().find(|entry| {
            Arc::ptr_eq(
                entry.original.original_runtime(),
                original.original_runtime(),
            )
        }) {
            Self::borrow_entry(existing, borrower)?;
            return Ok(Arc::clone(&existing.original));
        }
        if state.originals.len() + state.history.len() >= MAX_ORIGINAL_STARTUPS {
            return Err(unavailable());
        }
        state.originals.try_reserve(1).map_err(|_| unavailable())?;
        // Reserve cause archive BEFORE dispatch; successful clean slots are reusable.
        let reserve = state.originals.len() + 1;
        state
            .history
            .try_reserve(reserve)
            .map_err(|_| unavailable())?;
        let mut entry = Entry {
            original: Arc::clone(&original),
            borrowers: Vec::new(),
            closing: false,
        };
        Self::borrow_entry(&mut entry, borrower)?;
        state.originals.push(entry);
        if !refused {
            original.start_original().map_err(|_| unavailable())?;
        }
        Ok(original)
    }
    pub(crate) async fn close_for(
        &self,
        runtime: &Arc<RemoteStorage>,
        borrower: &Arc<Resources>,
    ) -> Result<StartupCleanup, StorageError> {
        if !borrower
            .owns_original_runtime_pool(runtime.pool())
            .map_err(|_| unavailable())?
        {
            return Err(unavailable());
        }
        let (original, borrowers, registry_poison) = {
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(cause) => (cause.into_inner(), true),
            };
            let Some(entry) = state
                .originals
                .iter_mut()
                .find(|entry| Arc::ptr_eq(entry.original.original_runtime(), runtime.pool()))
            else {
                if poison {
                    return Err(unavailable());
                }
                return Ok(StartupCleanup {
                    _original: None,
                    refused: false,
                });
            };
            // Cleanup-only late refusal gains no borrower membership/SQL grant.
            // Seal under the SAME gate and retain every actual admitted owner.
            entry.closing = true;
            let borrowers: [Option<Arc<Resources>>; MAX_ORIGINAL_STARTUPS] =
                std::array::from_fn(|index| entry.borrowers.get(index).map(Arc::clone));
            (Arc::clone(&entry.original), borrowers, poison)
        };
        for saved in borrowers.into_iter().flatten() {
            saved
                .wait_body_returned()
                .await
                .map_err(|_| unavailable())?;
        }
        let receipt = original
            .cleanup_original()
            .await
            .map_err(|_| unavailable())?;
        if !Arc::ptr_eq(receipt.original(), &original) {
            return Err(unavailable());
        }
        // A cause-observation refusal never erases the original or its actual ACK.
        let causes = original.retains_original_causes();
        let (mut state, final_poison) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(cause) => (cause.into_inner(), true),
        };
        let refused = registry_poison || final_poison || causes.is_err();
        if !refused {
            if let Some(index) = state
                .originals
                .iter()
                .position(|entry| Arc::ptr_eq(&entry.original, &original))
            {
                let removed = state.originals.remove(index);
                if causes.unwrap_or(true) {
                    state.history.push(removed.original);
                }
            }
        } // Poison/unknown diagnostic retains the SAME original entry and causes.
        Ok(StartupCleanup {
            _original: Some(receipt),
            refused,
        })
    }
    #[cfg(test)]
    pub(crate) fn poison_actual_registry(self: &Arc<Self>) {
        let original = Arc::clone(self);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _state = original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::panic::resume_unwind(Box::new("original startup registry poison control"));
        }));
    }
    #[cfg(test)]
    pub(crate) fn actual_entry_closing(
        &self,
        runtime: &Arc<PostgresPool>,
    ) -> Result<bool, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        Ok(state
            .originals
            .iter()
            .any(|entry| Arc::ptr_eq(entry.original.original_runtime(), runtime) && entry.closing))
    }
    pub(crate) fn causes_for_runtime(
        &self,
        runtime: &Arc<PostgresPool>,
    ) -> Result<bool, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        for original in state
            .originals
            .iter()
            .map(|entry| &entry.original)
            .chain(state.history.iter())
        {
            if Arc::ptr_eq(original.original_runtime(), runtime)
                && original
                    .retains_original_causes()
                    .map_err(|_| unavailable())?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Actual retained originals only; snapshots do not authorize SQL or cleanup.
    pub fn original_startups(&self) -> Result<Vec<Arc<OriginalMigrationStartup>>, StorageError> {
        let state = self.state.lock().map_err(|_| unavailable())?;
        let originals = &state.originals;
        let mut snapshot = Vec::new();
        snapshot
            .try_reserve_exact(originals.len() + state.history.len())
            .map_err(|_| unavailable())?;
        snapshot.extend(originals.iter().map(|entry| Arc::clone(&entry.original)));
        snapshot.extend(state.history.iter().map(Arc::clone));
        Ok(snapshot)
    }
}
impl StorageService {
    /// Owning host retains this exact service registry BEFORE calling service futures.
    pub fn original_migration_startups(&self) -> &Arc<MigrationStartupRegistry> {
        &self.migration_startups
    }
    pub(crate) async fn migration_pool_or_close(
        &self,
        storage: &RemoteStorage,
    ) -> Result<Arc<PostgresPool>, StorageError> {
        match self.migration_pool_for(storage).await {
            Ok(pool) => Ok(pool),
            Err(original) => Err(original),
        }
    }
    pub(crate) async fn migration_pool_for(
        &self,
        storage: &RemoteStorage,
    ) -> Result<Arc<PostgresPool>, StorageError> {
        let borrower = self.operation_resources.as_ref().ok_or_else(unavailable)?;
        if let Some(original) = self
            .migration_startups
            .borrow_existing(storage.pool(), borrower)?
        {
            return original.finish_original().await.map_err(|_| unavailable());
        }
        let profile = self
            .inputs
            .candidate
            .as_ref()
            .ok_or(StorageError(BlockerCode::NoCandidateProfile))?;
        let resolver = HostCredentialResolver::new(self.inputs.keyring.as_ref());
        // Genuine separately resolved migration credential. No Runtime fallback.
        let (settings, namespace) = connection_settings(profile, &resolver, LoginRole::Migrator)
            .map_err(|error| StorageError(error.into()))?;
        let prepared = OriginalMigrationStartup::prepare(
            Arc::clone(storage.pool()),
            settings,
            namespace.as_ref(),
        );
        let (original, refused) = match prepared {
            Ok(original) => (original, false),
            Err(error) => (error.original, true),
        };
        // No asynchronous/native poll precedes insertion of the original frame.
        let retained = self
            .migration_startups
            .retain(Arc::clone(&original), refused, borrower)?;
        if refused {
            return Err(unavailable());
        }
        retained.finish_original().await.map_err(|_| unavailable())
    }
}
fn unavailable() -> StorageError {
    StorageError(BlockerCode::ConnectionFailed)
}
