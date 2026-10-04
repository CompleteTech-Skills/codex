//! Original service/target lifetime association only; no native or exclusive authority.
use crate::StorageService;
use codex_remote_storage::RemoteAdministrativeObserver;
use codex_storage_authority::CredentialSource;
use codex_storage_authority::InvalidAdministrativeInput;
use codex_storage_authority::RemoteAdministrationInput;
use std::fmt;
use std::sync::Arc;

/// Prepared synchronously before credential lookup or administrative dispatch.
/// The host must retain this beside its genuine original host owner and validate
/// that owner before polling the observer. This binding supplies no host proof.
pub struct OriginalServiceAdministration {
    service: Arc<StorageService>,
    observer: Arc<RemoteAdministrativeObserver>,
}

/// Outward-only refusal retaining the actual service, never cached into it.
pub struct ServiceAdministrationPreparationRefusal {
    service: Arc<StorageService>,
    invalid: Option<InvalidAdministrativeInput>,
}
impl fmt::Debug for ServiceAdministrationPreparationRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _original = (&self.service, &self.invalid);
        formatter.write_str("ServiceAdministrationPreparationRefused([original retained])")
    }
}
impl ServiceAdministrationPreparationRefusal {
    pub fn original_service(&self) -> &Arc<StorageService> {
        &self.service
    }
}
impl OriginalServiceAdministration {
    pub fn belongs_to_service(&self, service: &Arc<StorageService>) -> bool {
        Arc::ptr_eq(&self.service, service)
    }
    pub fn original_service(&self) -> &Arc<StorageService> {
        &self.service
    }
    /// Same observer and selected target; borrowing does not validate the host.
    pub fn original_observer(&self) -> &Arc<RemoteAdministrativeObserver> {
        &self.observer
    }
}
impl StorageService {
    /// No resolver, factory, task or SQL is polled by preparation. The selected
    /// candidate is the private Arc captured once by this service constructor.
    pub fn prepare_administration(
        self: &Arc<Self>,
        login: String,
        credential: CredentialSource,
    ) -> Result<Arc<OriginalServiceAdministration>, ServiceAdministrationPreparationRefusal> {
        let target = self.original_candidate.as_ref().ok_or_else(|| {
            ServiceAdministrationPreparationRefusal {
                service: Arc::clone(self),
                invalid: None,
            }
        })?;
        let input = RemoteAdministrationInput::new(Arc::clone(target), login, credential).map_err(
            |invalid| ServiceAdministrationPreparationRefusal {
                service: Arc::clone(self),
                invalid: Some(invalid),
            },
        )?;
        Ok(Arc::new(OriginalServiceAdministration {
            service: Arc::clone(self),
            observer: RemoteAdministrativeObserver::new(
                Arc::new(input),
                Arc::clone(&self.inputs.keyring),
            ),
        }))
    }
}
