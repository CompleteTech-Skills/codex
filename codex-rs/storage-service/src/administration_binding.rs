//! Original service/target lifetime association only; no native or exclusive authority.
use crate::StorageService;
use codex_remote_storage::RemoteAdministrativeObserver;
use codex_storage_authority::CredentialSource;
use codex_storage_authority::InvalidAdministrativeInput;
use codex_storage_authority::RemoteAdministrationInput;
use std::fmt;
use std::sync::Arc;

#[path = "administrative_operation_plan.rs"]
mod operation_plan;
pub use operation_plan::AdministrativeOperationPlanRefusal;
pub(crate) use operation_plan::OperationPlanFacts;
#[cfg(test)]
pub(crate) use operation_plan::OperationPlanRevalidationRefusal;
pub use operation_plan::OriginalAdministrativeOperationPlan;

/// Prepared synchronously before credential lookup or administrative dispatch.
/// The host must retain this beside its genuine original host owner and validate
/// that owner before polling the observer. This binding supplies no host proof.
pub struct OriginalServiceAdministration {
    service: Arc<StorageService>,
    observer: Arc<RemoteAdministrativeObserver>,
    preflight_id: uuid::Uuid,
    native_origin: std::sync::Mutex<Option<Arc<codex_remote_storage::RuntimeNativeOrigin>>>,
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
/// Outward-only failure retains binding and existing operation/resource history.
pub struct ServiceRuntimeOriginRefusal {
    original: Arc<OriginalServiceAdministration>,
    cause: crate::StorageError,
    native_origin: Option<Arc<codex_remote_storage::RuntimeNativeOrigin>>,
}
impl std::fmt::Debug for ServiceRuntimeOriginRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = (&self.original, &self.cause, &self.native_origin);
        formatter.write_str("ServiceRuntimeOriginRefused([original retained])")
    }
}
impl ServiceRuntimeOriginRefusal {
    pub fn original_binding(&self) -> &Arc<OriginalServiceAdministration> {
        &self.original
    }
}
impl OriginalServiceAdministration {
    /// Host must validate its genuine owner before first polling this explicit
    /// privileged-origin route. Result is native observation, never exclusivity.
    pub async fn observe_runtime_origin(
        self: &Arc<Self>,
    ) -> Result<Arc<codex_remote_storage::RuntimeNativeOrigin>, ServiceRuntimeOriginRefusal> {
        let result = self.service.original_observation(
            crate::service_operations::Kind::AuthorityOrigin, None, |service| async move {
                let resources = service.operation_resources.as_ref().ok_or(crate::StorageError(crate::BlockerCode::Internal))?;
                let profile = service.original_candidate.as_ref().ok_or(crate::StorageError(crate::BlockerCode::NoCandidateProfile))?;
                let origin = resources.retain_runtime_origin(codex_remote_storage::OriginalRuntimeOrigin::prepare(Arc::clone(profile))).map_err(|_| crate::StorageError(crate::BlockerCode::Internal))?;
                let pool_slot = Arc::new(resources.reserve_runtime_pool().map_err(|_| crate::StorageError(crate::BlockerCode::Internal))?);
                let runtime_slot = resources.reserve_runtime().map_err(|_| crate::StorageError(crate::BlockerCode::Internal))?;
                let captured_pool_slot = Arc::clone(&pool_slot);
                let capture: Arc<dyn Fn(Arc<codex_postgres_runtime::PostgresPool>) -> Result<(), codex_postgres_runtime::PoolError> + Send + Sync> = Arc::new(move |pool| {
                    captured_pool_slot.capture_original_arc(pool).map_err(|_| codex_postgres_runtime::PoolError::Unavailable)
                });
                let storage = match codex_remote_storage::RemoteStorage::connect_with_original_runtime_origin(
                    &origin, Arc::clone(&service.inputs.keyring), capture,
                ).await {
                    Ok(storage) => storage,
                    Err(error) => {
                        pool_slot.record_retained_failure(Arc::new(error));
                        runtime_slot.record_returned_failure(Arc::new(error));
                        return Err(crate::StorageError(error.into()));
                    }
                };
                runtime_slot.capture_original_arc(storage).map_err(|_| crate::StorageError(crate::BlockerCode::Internal))?;
                origin.original_native_origin().ok_or(crate::StorageError(crate::BlockerCode::Internal))
            },
        ).await;
        result.map_err(|cause| ServiceRuntimeOriginRefusal {
            original: Arc::clone(self),
            cause,
            native_origin: None,
        })
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
            preflight_id: uuid::Uuid::new_v4(),
            native_origin: std::sync::Mutex::new(None),
            observer: RemoteAdministrativeObserver::new(
                Arc::new(input),
                Arc::clone(&self.inputs.keyring),
            ),
        }))
    }
}

impl OriginalServiceAdministration {
    /// Actual registered nonmutating preflight only. Host validation is required
    /// before first poll; result grants no writer retirement or exclusive access.
    pub async fn preflight_original(
        self: &Arc<Self>,
        origin: Arc<codex_remote_storage::RuntimeNativeOrigin>,
    ) -> Result<(), ServiceRuntimeOriginRefusal> {
        self.preflight_associated(origin, None).await
    }
    fn retain_original_origin(
        self: &Arc<Self>,
        origin: &Arc<codex_remote_storage::RuntimeNativeOrigin>,
    ) -> Result<(), ServiceRuntimeOriginRefusal> {
        let refused = || ServiceRuntimeOriginRefusal {
            original: Arc::clone(self),
            cause: crate::StorageError(crate::BlockerCode::Internal),
            native_origin: Some(Arc::clone(origin)),
        };
        if !origin.belongs_to_target(self.observer.original_input().target()) {
            return Err(refused());
        }
        {
            let mut original = self.native_origin.lock().map_err(|_| refused())?;
            if let Some(captured) = original.as_ref() {
                if !Arc::ptr_eq(captured, origin) {
                    return Err(refused());
                }
            } else {
                *original = Some(Arc::clone(origin));
            }
        }
        Ok(())
    }
    async fn preflight_associated(
        self: &Arc<Self>,
        origin: Arc<codex_remote_storage::RuntimeNativeOrigin>,
        operation: Option<Arc<OperationPlanFacts>>,
    ) -> Result<(), ServiceRuntimeOriginRefusal> {
        self.retain_original_origin(&origin)?;
        let dispatch_id = operation
            .as_ref()
            .map_or(self.preflight_id, |facts| facts.dispatch_id);
        let retained_origin = Arc::clone(&origin);
        let observer = Arc::clone(&self.observer);
        let result = self
            .service
            .original_observation(
                crate::service_operations::Kind::AdministrativePreflight,
                Some(dispatch_id),
                move |service| async move {
                    let refused = || crate::StorageError(crate::BlockerCode::Internal);
                    let resources = service.operation_resources.as_ref().ok_or_else(refused)?;
                    if let Some(operation) = operation {
                        if !operation.matches_original(&service.inputs, &observer, &origin) {
                            return Err(refused());
                        }
                        resources
                            .retain_administrative_operation(operation, &service)
                            .map_err(|_| refused())?;
                    }
                    let observer_slot = resources
                        .reserve_administrative_observer()
                        .map_err(|_| refused())?;
                    let attempt_slot = resources
                        .reserve_administrative_attempt()
                        .map_err(|_| refused())?;
                    let command_slot = resources
                        .reserve_administrative_command()
                        .map_err(|_| refused())?;
                    observer_slot
                        .capture_original_arc(Arc::clone(&observer))
                        .map_err(|_| refused())?;
                    let attempt = match observer.prepare_runtime_session(origin) {
                        Ok(attempt) => attempt,
                        Err(raw) => {
                            let cause = Arc::new(raw);
                            observer_slot.record_retained_failure(cause.clone());
                            attempt_slot.record_returned_failure(cause.clone());
                            command_slot.record_returned_failure(cause);
                            return Err(refused());
                        }
                    };
                    attempt_slot
                        .capture_original_arc(Arc::clone(&attempt))
                        .map_err(|_| refused())?;
                    let command = attempt.prepare_preflight();
                    command_slot
                        .capture_original_arc(Arc::clone(&command))
                        .map_err(|_| refused())?;
                    if let Err(raw) = attempt.finish_preflight_frame(&command).await {
                        command_slot.record_retained_failure(Arc::new(raw));
                        return Err(refused());
                    }
                    if let Err(raw) = command.start_preflight() {
                        command_slot.record_retained_failure(Arc::new(raw));
                        return Err(refused());
                    }
                    if let Err(raw) = command.finish_original().await {
                        command_slot.record_retained_failure(Arc::new(raw));
                        return Err(refused());
                    }
                    Ok(())
                },
            )
            .await;
        result.map_err(|cause| ServiceRuntimeOriginRefusal {
            original: Arc::clone(self),
            cause,
            native_origin: Some(retained_origin),
        })
    }
}
