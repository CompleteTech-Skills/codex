//! Actual journal/registered-plan association, never exclusive server authority.
//! No serialized plan, confirmation or writers-stopped Boolean constructs this.
//! Native activation/run revalidation and privileged retirement remain required.
use super::OriginalServiceAdministration;
use super::ServiceRuntimeOriginRefusal;
use crate::OperationRecord;
use crate::OperationState;
use crate::PlanAction;
use crate::StorageError;
use crate::StoragePlan;
use codex_remote_storage::RemoteAdministrativeObserver;
use codex_remote_storage::RuntimeNativeOrigin;
use std::sync::Arc;
use uuid::Uuid;

/// Outward lifetime owner. Resource history stores only its cycle-free facts.
pub struct OriginalAdministrativeOperationPlan {
    binding: Arc<OriginalServiceAdministration>,
    pub(super) facts: Arc<OperationPlanFacts>,
}
pub(crate) struct OperationPlanFacts {
    inputs: Arc<crate::StorageServiceInputs>,
    target: Arc<codex_storage_authority::RemotePostgresProfile>,
    observer: Arc<RemoteAdministrativeObserver>,
    pub(super) origin: Arc<RuntimeNativeOrigin>,
    record: Arc<OperationRecord>,
    plan: Arc<StoragePlan>,
    pub(super) dispatch_id: Uuid,
}
impl OperationPlanFacts {
    pub(crate) fn revalidate_journal(
        &self,
        service: &crate::StorageService,
    ) -> Result<(), OperationPlanRevalidationRefusal> {
        let mut refused = OperationPlanRevalidationRefusal {
            expected: Arc::clone(&self.record),
            returned: None,
            cause: None,
        };
        match service.journal.read(self.record.operation_id) {
            Ok(returned) => {
                refused.returned = returned.map(Arc::new);
                if Arc::ptr_eq(&self.inputs, &service.inputs)
                    && refused
                        .returned
                        .as_ref()
                        .is_some_and(|record| record.as_ref() == self.record.as_ref())
                {
                    Ok(())
                } else {
                    Err(refused)
                }
            }
            Err(cause) => {
                refused.cause = Some(cause);
                Err(refused)
            }
        }
    }
    pub(crate) fn matches_original(
        &self,
        inputs: &Arc<crate::StorageServiceInputs>,
        observer: &Arc<RemoteAdministrativeObserver>,
        origin: &Arc<RuntimeNativeOrigin>,
    ) -> bool {
        Arc::ptr_eq(&self.inputs, inputs)
            && Arc::ptr_eq(&self.target, observer.original_input().target())
            && Arc::ptr_eq(&self.observer, observer)
            && Arc::ptr_eq(&self.origin, origin)
            && self.origin.belongs_to_target(&self.target)
            && self.record.state == OperationState::Planned
            && self.record.run_id.is_none()
            && self.record.action == self.plan.action
            && self.record.plan_digest == self.plan.digest
            && self.plan.is_startable()
    }
}
pub(crate) struct OperationPlanRevalidationRefusal {
    expected: Arc<OperationRecord>,
    returned: Option<Arc<OperationRecord>>,
    cause: Option<std::io::Error>,
}
impl std::fmt::Debug for OperationPlanRevalidationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = (&self.expected, &self.returned, &self.cause);
        f.write_str("AdministrativeOperationChanged([original journal facts retained])")
    }
}
impl std::fmt::Display for OperationPlanRevalidationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for OperationPlanRevalidationRefusal {}
pub struct AdministrativeOperationPlanRefusal {
    binding: Arc<OriginalServiceAdministration>,
    origin: Arc<RuntimeNativeOrigin>,
    operation: Option<Arc<OriginalAdministrativeOperationPlan>>,
    record: Option<Arc<OperationRecord>>,
    returned_record: Option<Arc<OperationRecord>>,
    plan: Option<Arc<StoragePlan>>,
    cause: StorageError,
    preflight: Option<ServiceRuntimeOriginRefusal>,
    journal_failure: Option<Arc<std::io::Error>>,
}
impl std::fmt::Debug for AdministrativeOperationPlanRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = (
            &self.binding,
            &self.origin,
            &self.operation,
            &self.record,
            &self.returned_record,
            &self.plan,
            &self.cause,
            &self.preflight,
            &self.journal_failure,
        );
        f.write_str("AdministrativeOperationPlanRefused([original retained])")
    }
}
impl std::fmt::Display for AdministrativeOperationPlanRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for AdministrativeOperationPlanRefusal {}
impl OriginalAdministrativeOperationPlan {
    pub fn belongs_to_binding(&self, binding: &Arc<OriginalServiceAdministration>) -> bool {
        Arc::ptr_eq(&self.binding, binding)
    }
    pub fn operation_id(&self) -> Uuid {
        self.facts.record.operation_id
    }
}
impl OriginalServiceAdministration {
    /// Re-read this service's actual Planned journal operation around its actual
    /// registered plan task. This associates operator intent, not paused writers
    /// or a native transition. It neither creates a journal nor changes credentials.
    /// Resumed/copy-complete operations require a separate native revalidation path.
    pub async fn prepare_operation_plan(
        self: &Arc<Self>,
        origin: Arc<RuntimeNativeOrigin>,
        operation_id: Uuid,
        expected_plan_id: Uuid,
    ) -> Result<Arc<OriginalAdministrativeOperationPlan>, AdministrativeOperationPlanRefusal> {
        let mut refusal = AdministrativeOperationPlanRefusal {
            binding: Arc::clone(self),
            origin: Arc::clone(&origin),
            operation: None,
            record: None,
            returned_record: None,
            plan: None,
            cause: StorageError(crate::BlockerCode::StalePlan),
            preflight: None,
            journal_failure: None,
        };
        if let Err(raw) = self.retain_original_origin(&origin) {
            refusal.preflight = Some(raw);
            return Err(refusal);
        }
        let record = match self.service.journal.read(operation_id) {
            Ok(Some(record)) => Arc::new(record),
            Ok(None) => {
                refusal.cause = StorageError(crate::BlockerCode::OperationNotFound);
                return Err(refusal);
            }
            Err(raw) => {
                refusal.journal_failure = Some(Arc::new(raw));
                refusal.cause = StorageError(crate::BlockerCode::Internal);
                return Err(refusal);
            }
        };
        refusal.record = Some(Arc::clone(&record));
        if record.operation_id != operation_id
            || record.state != OperationState::Planned
            || record.run_id.is_some()
            || record.blocker.is_some()
            || !matches!(record.action, PlanAction::Migrate | PlanAction::Return)
        {
            return Err(refusal);
        }
        // The registered task owns its factories/queries/cleanup if this waiter
        // is canceled. Its cached result is a plain plan, never this binding Arc.
        let plan = match self.service.plan(record.action).await {
            Ok(plan) => Arc::new(plan),
            Err(raw) => {
                refusal.cause = raw;
                return Err(refusal);
            }
        };
        refusal.plan = Some(Arc::clone(&plan));
        let returned_record = match self.service.journal.read(operation_id) {
            Ok(Some(record)) => Arc::new(record),
            Ok(None) => {
                refusal.cause = StorageError(crate::BlockerCode::OperationNotFound);
                return Err(refusal);
            }
            Err(raw) => {
                refusal.journal_failure = Some(Arc::new(raw));
                refusal.cause = StorageError(crate::BlockerCode::Internal);
                return Err(refusal);
            }
        };
        refusal.returned_record = Some(Arc::clone(&returned_record));
        if returned_record.as_ref() != record.as_ref()
            || plan.plan_id != expected_plan_id
            || plan.action != record.action
            || plan.digest != record.plan_digest
            || !plan.is_startable()
        {
            return Err(refusal);
        }
        let retained = match self.native_origin.lock() {
            Ok(retained) => retained,
            Err(_) => return Err(refusal),
        };
        if !retained
            .as_ref()
            .is_some_and(|captured| Arc::ptr_eq(captured, &origin))
        {
            return Err(refusal);
        }
        let facts = Arc::new(OperationPlanFacts {
            inputs: Arc::clone(&self.service.inputs),
            target: Arc::clone(self.observer.original_input().target()),
            observer: Arc::clone(&self.observer),
            origin,
            record,
            plan,
            dispatch_id: Uuid::new_v4(),
        });
        Ok(Arc::new(OriginalAdministrativeOperationPlan {
            binding: Arc::clone(self),
            facts,
        }))
    }

    /// Same preflight only, with privately produced operation facts retained in
    /// the existing task Resources before any administrative session join.
    pub async fn preflight_operation_plan(
        self: &Arc<Self>,
        operation: Arc<OriginalAdministrativeOperationPlan>,
    ) -> Result<(), AdministrativeOperationPlanRefusal> {
        let refused = |raw| AdministrativeOperationPlanRefusal {
            binding: Arc::clone(self),
            origin: Arc::clone(&operation.facts.origin),
            operation: Some(Arc::clone(&operation)),
            record: Some(Arc::clone(&operation.facts.record)),
            returned_record: None,
            plan: Some(Arc::clone(&operation.facts.plan)),
            cause: StorageError(crate::BlockerCode::Internal),
            preflight: raw,
            journal_failure: None,
        };
        if !operation.belongs_to_binding(self)
            || !operation.facts.matches_original(
                &self.service.inputs,
                &self.observer,
                &operation.facts.origin,
            )
        {
            return Err(refused(None));
        }
        self.preflight_associated(
            Arc::clone(&operation.facts.origin),
            Some(Arc::clone(&operation.facts)),
        )
        .await
        .map_err(|raw| refused(Some(raw)))
    }
}
