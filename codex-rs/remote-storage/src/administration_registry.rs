//! Inactive bounded observation-startup registry. No administrative permit.
//! Only startup observation borrowers exist here; role mutation actors are absent.
use crate::administration_startup::AdministrativeNativeObservation;
use crate::administration_startup::AdministrativeStartupRefusal;
use crate::administration_startup::OriginalAdministrativeStartup;
use codex_postgres_runtime::PostgresPool;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::RemoteAdministrationInput;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Semaphore;

const CAPACITY: usize = 16;
enum Slot {
    Empty,
    Active {
        original: Arc<OriginalAdministrativeStartup>,
        pool: Option<Arc<PostgresPool>>,
        borrowers: usize,
        sealed: bool,
    },
    Archived(Arc<OriginalAdministrativeStartup>),
}

struct State {
    slots: [Slot; CAPACITY],
    hold: Option<Arc<OriginalAdministrativeRegistry>>,
}
pub struct OriginalAdministrativeRegistry {
    state: Mutex<State>,
    cleanup: Arc<Semaphore>,
}
pub struct AdministrativeRegistryRefusal {
    original_registry: Arc<OriginalAdministrativeRegistry>,
    original_startup: Option<Arc<OriginalAdministrativeStartup>>,
    returned: Option<AdministrativeStartupRefusal>,
    archive: Option<AdministrativeFailureArchive>,
    reason: &'static str,
}
impl fmt::Debug for AdministrativeRegistryRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _owned = (
            &self.original_registry,
            &self.original_startup,
            &self.returned,
            &self.archive,
        );
        formatter.write_str(self.reason)
    }
}
pub struct AdministrativeObservationBorrow {
    registry: Arc<OriginalAdministrativeRegistry>,
    original: Arc<OriginalAdministrativeStartup>,
    slot: usize,
}
impl AdministrativeObservationBorrow {
    pub async fn observe_original(
        &self,
    ) -> Result<Arc<AdministrativeNativeObservation>, AdministrativeStartupRefusal> {
        self.original.finish_original().await
    }
}
impl Drop for AdministrativeObservationBorrow {
    fn drop(&mut self) {
        let mut state = self
            .registry
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Slot::Active {
            original,
            borrowers,
            ..
        } = &mut state.slots[self.slot]
        {
            if Arc::ptr_eq(original, &self.original) && *borrowers > 0 {
                *borrowers -= 1;
            }
        }
        // Startup task/session completion is proved by cleanup, never this Drop.
    }
}
/// Owns the same finite archived frames and their original raw causes after handoff.
/// No serialization, permission or store-wide quiescence proof is produced.
pub struct AdministrativeFailureArchive {
    originals: [Option<Arc<OriginalAdministrativeStartup>>; CAPACITY],
}
impl AdministrativeFailureArchive {
    pub fn len(&self) -> usize {
        self.originals
            .iter()
            .filter(|original| original.is_some())
            .count()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl OriginalAdministrativeRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                slots: std::array::from_fn(|_| Slot::Empty),
                hold: None,
            }),
            cleanup: Arc::new(Semaphore::new(1)),
        })
    }
    fn refusal(
        self: &Arc<Self>,
        original: Option<Arc<OriginalAdministrativeStartup>>,
        returned: Option<AdministrativeStartupRefusal>,
        reason: &'static str,
    ) -> AdministrativeRegistryRefusal {
        AdministrativeRegistryRefusal {
            original_registry: Arc::clone(self),
            original_startup: original,
            returned,
            archive: None,
            reason,
        }
    }
    /// Fixed active+archive reservation precedes settings resolution/dispatch.
    /// Exact input Arc only; endpoint equality cannot reuse a session.
    pub fn capture(
        self: &Arc<Self>,
        input: Arc<RemoteAdministrationInput>,
        resolver: &HostCredentialResolver<'_>,
    ) -> Result<AdministrativeObservationBorrow, AdministrativeRegistryRefusal> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| self.refusal(None, None, "registry_poisoned"))?;
        for (slot, entry) in state.slots.iter_mut().enumerate() {
            if let Slot::Active {
                original,
                pool,
                borrowers,
                sealed,
            } = entry
            {
                if original.input_is(&input) {
                    if *sealed {
                        return Err(self.refusal(
                            Some(Arc::clone(original)),
                            None,
                            "original_sealed",
                        ));
                    }
                    let captured = original.captured_pool();
                    match (&*pool, captured) {
                        (Some(saved), Some(actual)) if !Arc::ptr_eq(saved, &actual) => {
                            return Err(self.refusal(
                                Some(Arc::clone(original)),
                                None,
                                "original_pool_changed",
                            ));
                        }
                        (Some(_), None) => {
                            return Err(self.refusal(
                                Some(Arc::clone(original)),
                                None,
                                "original_pool_missing",
                            ));
                        }
                        (None, actual) => *pool = actual,
                        _ => {}
                    }
                    *borrowers = borrowers.checked_add(1).ok_or_else(|| {
                        self.refusal(Some(Arc::clone(original)), None, "borrower_budget")
                    })?;
                    return Ok(AdministrativeObservationBorrow {
                        registry: Arc::clone(self),
                        original: Arc::clone(original),
                        slot,
                    });
                }
            }
        }
        let slot = state
            .slots
            .iter()
            .position(|slot| matches!(slot, Slot::Empty))
            .ok_or_else(|| self.refusal(None, None, "original_archive_budget"))?;
        let original = OriginalAdministrativeStartup::reserved(input);
        state.slots[slot] = Slot::Active {
            original: Arc::clone(&original),
            pool: None,
            borrowers: 1,
            sealed: false,
        };
        state.hold = Some(Arc::clone(self));
        // Registry owns actual frame before first factory/credential dispatch.
        if let Err(returned) = original.dispatch_original(resolver) {
            if let Slot::Active { borrowers, .. } = &mut state.slots[slot] {
                *borrowers = 0;
            }
            return Err(self.refusal(Some(original), Some(returned), "startup_refused"));
        }
        Ok(AdministrativeObservationBorrow {
            registry: Arc::clone(self),
            original,
            slot,
        })
    }
    /// Startup observation borrowers cannot launch arbitrary SQL. Cleanup seals
    /// their admissions and joins SAME startup/native/SDK tasks before reclaim.
    pub async fn close_input(
        self: &Arc<Self>,
        input: &Arc<RemoteAdministrationInput>,
    ) -> Result<(), AdministrativeRegistryRefusal> {
        let _permit = Arc::clone(&self.cleanup)
            .acquire_owned()
            .await
            .map_err(|_| self.refusal(None, None, "cleanup_admission_refused"))?;
        let (index, original, registry_poison) = {
            // Cleanup-only recovery snapshots the SAME known owners. Capture
            // and archive handoff still reject this permanently poisoned gate.
            let (mut state, poison) = match self.state.lock() {
                Ok(state) => (state, false),
                Err(error) => (error.into_inner(), true),
            };
            let mut found = None;
            for (index, slot) in state.slots.iter_mut().enumerate() {
                if let Slot::Active {
                    original,
                    borrowers,
                    sealed,
                    ..
                } = slot
                {
                    if original.input_is(input) {
                        *sealed = true;
                        if *borrowers != 0 {
                            return Err(self.refusal(
                                Some(Arc::clone(original)),
                                None,
                                "original_borrowed",
                            ));
                        }
                        found = Some((index, Arc::clone(original)));
                        break;
                    }
                }
            }
            let (index, original) =
                found.ok_or_else(|| self.refusal(None, None, "original_not_registered"))?;
            (index, original, poison)
        };
        let returned = original.close_original().await;
        let (mut state, final_poison) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        let poison = registry_poison || final_poison;
        if !matches!(&state.slots[index], Slot::Active { original: saved, borrowers: 0, .. } if Arc::ptr_eq(saved, &original))
        {
            return Err(self.refusal(Some(original), returned.err(), "original_registry_changed"));
        }
        if returned.is_ok() && !poison {
            state.slots[index] = Slot::Empty;
        } else {
            // Reserve was made before dispatch; no allocation or cause rewrite.
            state.slots[index] = Slot::Archived(Arc::clone(&original));
            if !original.archive_closed_original() {
                state.slots[index] = Slot::Active {
                    original: Arc::clone(&original),
                    pool: original.captured_pool(),
                    borrowers: 0,
                    sealed: true,
                };
            }
        }
        if state.slots.iter().all(|slot| matches!(slot, Slot::Empty)) {
            state.hold = None;
        }
        if poison {
            return Err(self.refusal(Some(original), returned.err(), "registry_poisoned"));
        }
        returned.map_err(|returned| {
            self.refusal(Some(original), Some(returned), "original_cleanup_refused")
        })
    }
    /// Move finite known-closed failures into a caller-owned, raw-cause archive.
    /// Active/uncertain originals remain registered and keep the pending hold.
    pub fn take_failure_archive(
        self: &Arc<Self>,
    ) -> Result<AdministrativeFailureArchive, AdministrativeRegistryRefusal> {
        let (mut state, poison) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        let mut archive = AdministrativeFailureArchive {
            originals: std::array::from_fn(|_| None),
        };
        for (index, slot) in state.slots.iter_mut().enumerate() {
            if matches!(slot, Slot::Archived(_)) {
                if let Slot::Archived(original) = std::mem::replace(slot, Slot::Empty) {
                    archive.originals[index] = Some(original);
                }
            }
        }
        if state.slots.iter().all(|slot| matches!(slot, Slot::Empty)) {
            state.hold = None;
        }
        if poison {
            let mut refused = self.refusal(None, None, "registry_poisoned");
            refused.archive = Some(archive);
            return Err(refused);
        }
        Ok(archive)
    }
}

#[cfg(test)]
#[path = "administration_registry_tests.rs"]
mod tests;
