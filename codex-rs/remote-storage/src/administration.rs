//! Explicit host-owned administrative observations, never an exclusive permit.
use crate::administration_registry::AdministrativeFailureArchive;
use crate::administration_registry::AdministrativeRegistryRefusal;
use crate::administration_registry::OriginalAdministrativeRegistry;
use crate::administration_startup::AdministrativeNativeObservation;
use crate::administration_startup::AdministrativeStartupRefusal;
use codex_keyring_store::KeyringStore;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::RemoteAdministrationInput;
use std::fmt;
use std::sync::Arc;

/// The host owns the exact input, credential resolver and registry before any
/// factory dispatch. No Runtime/Migrator credential fallback is available.
pub struct RemoteAdministrativeObserver {
    input: Arc<RemoteAdministrationInput>,
    keyring: Arc<dyn KeyringStore>,
    registry: Arc<OriginalAdministrativeRegistry>,
}

/// Outward-only original owner; never cached into the registry it retains.
pub struct AdministrativeObservationRefusal {
    original: Arc<RemoteAdministrativeObserver>,
    capture: Option<AdministrativeRegistryRefusal>,
    observation: Option<AdministrativeStartupRefusal>,
    cleanup: Option<AdministrativeRegistryRefusal>,
}
impl fmt::Debug for AdministrativeObservationRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _retained = (
            &self.original,
            &self.capture,
            &self.observation,
            &self.cleanup,
        );
        let _original_diagnostic = self
            .observation
            .as_ref()
            .map(|original| original.diagnostic_kind());
        formatter.write_str("AdministrativeObservationRefused([originals retained])")
    }
}
impl AdministrativeObservationRefusal {
    /// Borrow the SAME host owner for reconciliation; no replacement is opened.
    pub fn original_observer(&self) -> &Arc<RemoteAdministrativeObserver> {
        &self.original
    }
}
impl RemoteAdministrativeObserver {
    pub fn new(input: Arc<RemoteAdministrationInput>, keyring: Arc<dyn KeyringStore>) -> Arc<Self> {
        Arc::new(Self {
            input,
            keyring,
            registry: OriginalAdministrativeRegistry::new(),
        })
    }
    pub fn original_input(&self) -> &Arc<RemoteAdministrationInput> {
        &self.input
    }

    /// Actual selected login/native rows only. A successful observation is
    /// returned AFTER the same native session and SDK pool cleanup completes.
    /// It is not historical server identity, exclusive access or role authority.
    pub async fn observe_original(
        self: &Arc<Self>,
    ) -> Result<Arc<AdministrativeNativeObservation>, AdministrativeObservationRefusal> {
        let resolver = HostCredentialResolver::new(self.keyring.as_ref());
        let borrowed = match self.registry.capture(Arc::clone(&self.input), &resolver) {
            Ok(borrowed) => borrowed,
            Err(capture) => {
                // Capture may own a genuinely failed startup. Reconcile that
                // SAME entry, keeping both primary and cleanup causes outward.
                let cleanup = self.registry.close_input(&self.input).await.err();
                return Err(AdministrativeObservationRefusal {
                    original: Arc::clone(self),
                    capture: Some(capture),
                    observation: None,
                    cleanup,
                });
            }
        };
        let observed = borrowed.observe_original().await;
        drop(borrowed);
        let cleanup = self.registry.close_input(&self.input).await;
        match (observed, cleanup) {
            (Ok(observed), Ok(())) => Ok(observed),
            (observed, cleanup) => Err(AdministrativeObservationRefusal {
                original: Arc::clone(self),
                capture: None,
                observation: observed.err(),
                cleanup: cleanup.err(),
            }),
        }
    }
    /// Cancellation preserves original startup/close tasks in the registry.
    /// Retry this same owner to reconcile a known pending entry; absence refuses.
    pub async fn close_original(&self) -> Result<(), AdministrativeRegistryRefusal> {
        self.registry.close_input(&self.input).await
    }
    /// Transfer known locally closed raw failures before genuine fresh retries.
    /// The caller must retain the returned archive or owning poisoned refusal.
    pub fn take_failure_archive(
        &self,
    ) -> Result<AdministrativeFailureArchive, AdministrativeRegistryRefusal> {
        self.registry.take_failure_archive()
    }
}

#[cfg(test)]
#[path = "administration_tests.rs"]
mod tests;
