//! Synchronous original startup capture before asynchronous session joining.
//! Lifetime/cleanup only; no retirement, enrollment or exclusive permission.
use super::AdministrativeObservationRefusal;
use super::OriginalAdministrativeRuntimeSession;
use super::RemoteAdministrativeObserver;
use crate::administration_registry::AdministrativeObservationBorrow;
use crate::administration_registry::AdministrativeRegistryRefusal;
use crate::runtime_origin::RuntimeNativeOrigin;
use codex_storage_authority::HostCredentialResolver;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OwnedMutexGuard;

pub struct OriginalAdministrativeSessionAttempt {
    observer: Arc<RemoteAdministrativeObserver>,
    origin: Arc<RuntimeNativeOrigin>,
    borrowed: Arc<AsyncMutex<Option<AdministrativeObservationBorrow>>>,
    sealed: Mutex<bool>,
}
pub struct AdministrativeSessionAttemptRefusal {
    original: Arc<OriginalAdministrativeSessionAttempt>,
    cause: Option<AdministrativeObservationRefusal>,
}
impl std::fmt::Debug for AdministrativeSessionAttemptRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = (&self.original, &self.cause);
        f.write_str("AdministrativeSessionAttemptRefused([original retained])")
    }
}
impl std::fmt::Display for AdministrativeSessionAttemptRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for AdministrativeSessionAttemptRefusal {}
pub struct AdministrativeSessionAttemptCleanupRefusal {
    original: Arc<OriginalAdministrativeSessionAttempt>,
    cleanup: Option<AdministrativeRegistryRefusal>,
}
impl std::fmt::Debug for AdministrativeSessionAttemptCleanupRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = (&self.original, &self.cleanup);
        f.write_str("AdministrativeSessionAttemptCleanupRefused([original retained])")
    }
}
impl std::fmt::Display for AdministrativeSessionAttemptCleanupRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for AdministrativeSessionAttemptCleanupRefusal {}
struct BorrowLoan {
    slot: OwnedMutexGuard<Option<AdministrativeObservationBorrow>>,
    borrowed: Option<AdministrativeObservationBorrow>,
}
impl Drop for BorrowLoan {
    fn drop(&mut self) {
        if let Some(borrowed) = self.borrowed.take() {
            *self.slot = Some(borrowed);
        }
    }
}
impl OriginalAdministrativeSessionAttempt {
    pub(super) fn prepare(
        observer: &Arc<RemoteAdministrativeObserver>,
        origin: Arc<RuntimeNativeOrigin>,
    ) -> Result<Arc<Self>, AdministrativeObservationRefusal> {
        let refused = |capture| AdministrativeObservationRefusal {
            original: Arc::clone(observer),
            capture,
            observation: None,
            cleanup: None,
            runtime_origin: Some(Arc::clone(&origin)),
        };
        if !origin.belongs_to_target(observer.input.target()) {
            return Err(refused(None));
        }
        let resolver = HostCredentialResolver::new(observer.keyring.as_ref());
        // Existing registry captures SAME startup before settings/factory dispatch.
        // Caller already retains Observer; no asynchronous work precedes capture.
        let borrowed = observer
            .registry
            .capture_runtime_origin(Arc::clone(&observer.input), Arc::clone(&origin), &resolver)
            .map_err(|capture| refused(Some(capture)))?;
        Ok(Arc::new(Self {
            observer: Arc::clone(observer),
            origin,
            borrowed: Arc::new(AsyncMutex::new(Some(borrowed))),
            sealed: Mutex::new(false),
        }))
    }
    pub fn belongs_to_observer(&self, observer: &Arc<RemoteAdministrativeObserver>) -> bool {
        Arc::ptr_eq(&self.observer, observer)
    }
    pub fn runtime_origin(&self) -> &Arc<RuntimeNativeOrigin> {
        &self.origin
    }
    pub async fn finish_original(
        self: &Arc<Self>,
    ) -> Result<OriginalAdministrativeRuntimeSession, AdministrativeSessionAttemptRefusal> {
        let refused = |cause| AdministrativeSessionAttemptRefusal {
            original: Arc::clone(self),
            cause,
        };
        let mut slot = Arc::clone(&self.borrowed).lock_owned().await;
        {
            let sealed = self.sealed.lock().map_err(|_| refused(None))?;
            if *sealed {
                return Err(refused(None));
            }
        }
        let borrowed = slot.take().ok_or_else(|| refused(None))?;
        let mut loan = BorrowLoan {
            slot,
            borrowed: Some(borrowed),
        };
        let session = match loan
            .borrowed
            .as_ref()
            .ok_or_else(|| refused(None))?
            .original_runtime_session(Arc::clone(&self.origin))
            .await
        {
            Ok(session) => session,
            Err(observation) => {
                return Err(refused(Some(AdministrativeObservationRefusal {
                    original: Arc::clone(&self.observer),
                    capture: None,
                    observation: Some(observation),
                    cleanup: None,
                    runtime_origin: Some(Arc::clone(&self.origin)),
                })));
            }
        };
        // Seal observation is final and held through synchronous frame creation.
        // Existing finish_scoped also holds SAME registry/startup guards.
        let sealed = self.sealed.lock().map_err(|_| refused(None))?;
        if *sealed {
            return Err(refused(None));
        }
        let borrowed = loan.borrowed.take().ok_or_else(|| refused(None))?;
        match borrowed.finish_scoped(&self.origin, session.is_some(), |borrowed| {
            OriginalAdministrativeRuntimeSession {
                original: Arc::clone(&self.observer),
                origin: Arc::clone(&self.origin),
                borrowed,
                session,
            }
        }) {
            Ok(frame) => Ok(frame),
            Err((borrowed, capture)) => {
                loan.borrowed = Some(borrowed);
                Err(refused(Some(AdministrativeObservationRefusal {
                    original: Arc::clone(&self.observer),
                    capture: Some(capture),
                    observation: None,
                    cleanup: None,
                    runtime_origin: Some(Arc::clone(&self.origin)),
                })))
            }
        }
    }
    /// Seal this attempt, release its SAME restored borrower, then physically
    /// reconcile existing startup/native/SDK originals. Empty slot means only
    /// borrower transfer; registry close must still prove real terminal cleanup.
    pub async fn close_original(
        self: &Arc<Self>,
    ) -> Result<(), AdministrativeSessionAttemptCleanupRefusal> {
        {
            *self.sealed.lock().unwrap_or_else(|raw| raw.into_inner()) = true;
        }
        {
            let mut borrowed = Arc::clone(&self.borrowed).lock_owned().await;
            borrowed.take();
        }
        let cleanup = self.observer.close_original().await;
        if cleanup.is_err() || self.sealed.is_poisoned() {
            return Err(AdministrativeSessionAttemptCleanupRefusal {
                original: Arc::clone(self),
                cleanup: cleanup.err(),
            });
        }
        Ok(())
    }
}
