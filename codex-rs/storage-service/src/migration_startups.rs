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
    ) -> Result<(), StorageError> {
        let (original, borrowers) = {
            let mut state = self.state.lock().map_err(|_| unavailable())?;
            let Some(entry) = state
                .originals
                .iter_mut()
                .find(|entry| Arc::ptr_eq(entry.original.original_runtime(), runtime.pool()))
            else {
                // No migration SDK was produced for this original runtime.
                return Ok(());
            };
            if !entry
                .borrowers
                .iter()
                .any(|saved| Arc::ptr_eq(saved, borrower))
            {
                return Err(unavailable());
            }
            // Same registry gate seals later borrows BEFORE any actual SDK close.
            entry.closing = true;
            let borrowers: [Option<Arc<Resources>>; MAX_ORIGINAL_STARTUPS] =
                std::array::from_fn(|index| entry.borrowers.get(index).map(Arc::clone));
            (Arc::clone(&entry.original), borrowers)
        };
        // The SAME retained cleanup task waits for already-admitted original bodies;
        // no unknown/aborted body is relabeled terminal, and no close races a borrower.
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
        let retain_causes = original
            .retains_original_causes()
            .map_err(|_| unavailable())?;
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if let Some(index) = state
            .originals
            .iter()
            .position(|entry| Arc::ptr_eq(&entry.original, &original))
        {
            let removed = state.originals.remove(index);
            if retain_causes {
                state.history.push(removed.original);
            }
        }
        Ok(())
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
