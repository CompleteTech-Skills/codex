//! Explicit host-owned administrative observations, never an exclusive permit.
#[path = "administrative_command.rs"]
mod administrative_command;
#[path = "administrative_session_attempt.rs"]
mod administrative_session_attempt;
pub use administrative_command::AdministrativeCommandArchive;
pub use administrative_command::AdministrativeCommandRefusal;
pub use administrative_command::OriginalAdministrativeCommand;
pub use administrative_session_attempt::AdministrativeSessionAttemptCleanupRefusal;
pub use administrative_session_attempt::AdministrativeSessionAttemptRefusal;
pub use administrative_session_attempt::OriginalAdministrativeSessionAttempt;

use crate::administration_registry::AdministrativeFailureArchive;
use crate::administration_registry::AdministrativeObservationBorrow;
use crate::administration_registry::AdministrativeRegistryRefusal;
use crate::administration_registry::OriginalAdministrativeRegistry;
use crate::administration_startup::AdministrativeNativeObservation;
use crate::administration_startup::AdministrativeStartupRefusal;
use crate::runtime_origin::RuntimeNativeOrigin;
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

/// Retains the original administrative physical session and its registry borrower.
/// Native identity agreement is observational, not administrative permission.
pub struct OriginalAdministrativeRuntimeSession {
    original: Arc<RemoteAdministrativeObserver>,
    origin: Arc<RuntimeNativeOrigin>,
    borrowed: AdministrativeObservationBorrow,
    session: tokio::sync::OwnedMutexGuard<Option<sqlx::pool::PoolConnection<sqlx::Postgres>>>,
}
impl OriginalAdministrativeRuntimeSession {
    pub fn runtime_origin(&self) -> &Arc<RuntimeNativeOrigin> {
        &self.origin
    }
    /// Release this exact scoped borrower before reconciling the original close.
    /// Cancellation leaves the close task and actual pool in the same registry.
    pub async fn close_original(self) -> Result<(), AdministrativeRegistryRefusal> {
        let Self {
            original,
            origin,
            borrowed,
            session,
        } = self;
        drop(session);
        drop(borrowed);
        drop(origin);
        original.close_original().await
    }
}

/// Outward-only original owner; never cached into the registry it retains.
pub struct AdministrativeObservationRefusal {
    original: Arc<RemoteAdministrativeObserver>,
    capture: Option<AdministrativeRegistryRefusal>,
    observation: Option<AdministrativeStartupRefusal>,
    cleanup: Option<AdministrativeRegistryRefusal>,
    runtime_origin: Option<Arc<RuntimeNativeOrigin>>,
}
impl fmt::Debug for AdministrativeObservationRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _retained = (
            &self.original,
            &self.capture,
            &self.observation,
            &self.cleanup,
            &self.runtime_origin,
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
impl std::fmt::Display for AdministrativeObservationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for AdministrativeObservationRefusal {}
impl RemoteAdministrativeObserver {
    pub fn new(input: Arc<RemoteAdministrationInput>, keyring: Arc<dyn KeyringStore>) -> Arc<Self> {
        Arc::new(Self {
            input,
            keyring,
            registry: OriginalAdministrativeRegistry::new(),
        })
    }
    /// Caller retains this Observer before synchronous credential/startup capture,
    /// then retains the returned attempt before its asynchronous session join.
    pub fn prepare_runtime_session(
        self: &Arc<Self>,
        origin: Arc<RuntimeNativeOrigin>,
    ) -> Result<Arc<OriginalAdministrativeSessionAttempt>, AdministrativeObservationRefusal> {
        OriginalAdministrativeSessionAttempt::prepare(self, origin)
    }
    pub fn original_input(&self) -> &Arc<RemoteAdministrationInput> {
        &self.input
    }

    /// Retain the actual startup/session; unlike observe_original this does not
    /// close the session before returning. No mutation or exclusive API is given.
    pub async fn retain_runtime_session(
        self: &Arc<Self>,
        origin: Arc<RuntimeNativeOrigin>,
    ) -> Result<OriginalAdministrativeRuntimeSession, AdministrativeObservationRefusal> {
        if !origin.belongs_to_target(self.input.target()) {
            return Err(AdministrativeObservationRefusal {
                original: Arc::clone(self),
                capture: None,
                observation: None,
                cleanup: None,
                runtime_origin: Some(origin),
            });
        }
        let resolver = HostCredentialResolver::new(self.keyring.as_ref());
        let borrowed = self
            .registry
            .capture_runtime_origin(Arc::clone(&self.input), Arc::clone(&origin), &resolver)
            .map_err(|capture| AdministrativeObservationRefusal {
                original: Arc::clone(self),
                capture: Some(capture),
                observation: None,
                cleanup: None,
                runtime_origin: Some(Arc::clone(&origin)),
            })?;
        let session = match borrowed.original_runtime_session(Arc::clone(&origin)).await {
            Ok(session) => session,
            Err(observation) => {
                return Err(AdministrativeObservationRefusal {
                    original: Arc::clone(self),
                    capture: None,
                    observation: Some(observation),
                    cleanup: None,
                    runtime_origin: Some(Arc::clone(&origin)),
                });
            }
        };
        let retained_origin = Arc::clone(&origin);
        borrowed
            .finish_scoped(&retained_origin, session.is_some(), |borrowed| {
                OriginalAdministrativeRuntimeSession {
                    original: Arc::clone(self),
                    origin,
                    borrowed,
                    session,
                }
            })
            .map_err(|(borrowed, capture)| {
                drop(borrowed);
                AdministrativeObservationRefusal {
                    original: Arc::clone(self),
                    capture: Some(capture),
                    observation: None,
                    cleanup: None,
                    runtime_origin: Some(retained_origin),
                }
            })
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
                    runtime_origin: None,
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
                runtime_origin: None,
            }),
        }
    }
    /// Cancellation preserves original startup/close tasks in the registry.
    /// Retry this same owner to reconcile a known pending entry; absence refuses.
    pub async fn close_original(&self) -> Result<(), AdministrativeRegistryRefusal> {
        self.registry.close_input(&self.input).await
    }
    /// SAME known failed startup's local physical joins only, not business success.
    /// Empty registry or unknown original never substitutes for native/SDK ACK.
    pub fn known_closed_failed_original(&self) -> bool {
        self.registry.known_closed_input(&self.input)
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
