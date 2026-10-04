//! Actual fixture plan/journal and original preflight task boundaries, unexecuted.
//! No writer pause, journal/native lock, administrative grant or exclusive proof.
use super::*;

#[derive(Clone, Copy)]
enum Mode {
    Mismatch,
    Foreign,
    Changed,
    Corrupt,
}
struct PlanFailure {
    binding: Arc<crate::OriginalServiceAdministration>,
    origin: Option<Arc<codex_remote_storage::RuntimeNativeOrigin>>,
    plan: Option<crate::StoragePlan>,
    operation: Option<Arc<crate::OriginalAdministrativeOperationPlan>>,
    original: Option<Arc<Original>>,
    origin_failure: Option<crate::ServiceRuntimeOriginRefusal>,
    preparation_failure: Option<crate::ServiceAdministrationPreparationRefusal>,
    plan_failure: Option<crate::AdministrativeOperationPlanRefusal>,
    journal_failure: Option<std::io::Error>,
    service_failure: Option<StorageError>,
    changed: Option<crate::OperationRecord>,
    raw_revalidation: Option<Arc<dyn std::error::Error + Send + Sync>>,
    returned: Option<Result<Output, StorageError>>,
}
impl std::fmt::Debug for PlanFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = (
            &self.binding,
            &self.origin,
            &self.plan,
            &self.operation,
            &self.original,
            &self.origin_failure,
            &self.preparation_failure,
            &self.plan_failure,
            &self.journal_failure,
            &self.service_failure,
            &self.changed,
            &self.raw_revalidation,
            &self.returned,
        );
        f.write_str("AdministrativePlanControlFailure([actual owners and causes retained])")
    }
}
impl std::fmt::Display for PlanFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for PlanFailure {}
fn no_administrative_task(service: &StorageService) -> bool {
    let Ok(active) = service.original_operations.originals.lock() else {
        return false;
    };
    let Ok(history) = service.original_operations.history.lock() else {
        return false;
    };
    !active
        .iter()
        .chain(history.iter())
        .any(|original| original.kind == Kind::AdministrativePreflight)
}
async fn actual_plan_negative(named: bool, mode: Mode) -> super::super::tests::ControlResult<()> {
    let binding = fixture_binding(named)?;
    let service = Arc::clone(binding.original_service());
    let mut failure = PlanFailure {
        binding: Arc::clone(&binding),
        origin: None,
        plan: None,
        operation: None,
        original: None,
        origin_failure: None,
        preparation_failure: None,
        plan_failure: None,
        journal_failure: None,
        service_failure: None,
        changed: None,
        raw_revalidation: None,
        returned: None,
    };
    let origin = match binding.observe_runtime_origin().await {
        Ok(origin) => origin,
        Err(raw) => {
            failure.origin_failure = Some(raw);
            return Err(Box::new(failure));
        }
    };
    failure.origin = Some(Arc::clone(&origin));
    let plan = match service.plan(crate::PlanAction::Migrate).await {
        Ok(plan) => plan,
        Err(raw) => {
            failure.service_failure = Some(raw);
            return Err(Box::new(failure));
        }
    };
    failure.plan = Some(plan.clone());
    if !plan.is_startable() {
        return Err(Box::new(failure)); // An explicitly empty destination is required.
    }
    let operation_id = Uuid::new_v4();
    // Exercise the existing journal producer only. Its public promise Boolean
    // is NOT consumed by the new association as native writer-pause permission.
    let record = match service
        .prepare_migration(
            operation_id,
            plan.plan_id,
            crate::Confirmation {
                writers_stopped: true,
            },
        )
        .await
    {
        Ok((record, true)) => record,
        Ok((record, false)) => {
            failure.changed = Some(record);
            return Err(Box::new(failure));
        }
        Err(raw) => {
            failure.service_failure = Some(raw);
            return Err(Box::new(failure));
        }
    };
    if matches!(mode, Mode::Mismatch) {
        let rejected = binding
            .prepare_operation_plan(Arc::clone(&origin), operation_id, Uuid::new_v4())
            .await;
        let expected = rejected
            .as_ref()
            .err()
            .is_some_and(|raw| raw.actual_plan_mismatch(operation_id, plan.plan_id))
            && no_administrative_task(&service);
        if let Ok(operation) = rejected.as_ref() {
            failure.operation = Some(Arc::clone(operation));
        }
        failure.plan_failure = rejected.err();
        if !expected {
            return Err(Box::new(failure));
        }
        return Ok(());
    }
    let operation = match binding
        .prepare_operation_plan(Arc::clone(&origin), operation_id, plan.plan_id)
        .await
    {
        Ok(operation) => operation,
        Err(raw) => {
            failure.plan_failure = Some(raw);
            return Err(Box::new(failure));
        }
    };
    failure.operation = Some(Arc::clone(&operation));
    if matches!(mode, Mode::Foreign) {
        let input = binding.original_observer().original_input();
        let foreign = match service
            .prepare_administration(input.login().to_owned(), input.credential().clone())
        {
            Ok(foreign) => foreign,
            Err(raw) => {
                failure.preparation_failure = Some(raw);
                return Err(Box::new(failure));
            }
        };
        let rejected = foreign
            .preflight_operation_plan(Arc::clone(&operation))
            .await;
        let expected = rejected
            .as_ref()
            .err()
            .is_some_and(|raw| raw.actual_foreign_binding(&operation))
            && no_administrative_task(&service);
        failure.plan_failure = rejected.err();
        if !expected {
            return Err(Box::new(failure));
        }
        return Ok(());
    }
    let probe = Arc::new(Probe::default());
    let installed = match service.original_operations.probe.lock() {
        Ok(mut slot) => {
            *slot = Some(Arc::clone(&probe));
            true
        }
        Err(_) => false,
    };
    if !installed {
        return Err(Box::new(failure));
    }
    let mut caller = Box::pin(binding.preflight_operation_plan(Arc::clone(&operation)));
    let mut early = None;
    let entered = tokio::select! { biased;
        returned = &mut caller => { early = Some(returned); false },
        result = tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()) => result.is_ok(),
    };
    let original = {
        let active = service
            .original_operations
            .originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let history = service
            .original_operations
            .history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active
            .iter()
            .chain(history.iter())
            .find(|original| original.kind == Kind::AdministrativePreflight)
            .map(Arc::clone)
    };
    let Some(original) = original else {
        probe.release.notify_one();
        if let Some(returned) = early {
            drop(caller);
            failure.plan_failure = returned.err();
            return Err(Box::new(failure));
        }
        let retained = (&mut caller, &binding, &service, &failure);
        let result = std::future::pending::<super::super::tests::ControlResult<()>>().await;
        let _ = retained;
        return result; // Unknown dispatch never becomes absence/terminal proof.
    };
    failure.original = Some(Arc::clone(&original));
    drop(caller);
    if matches!(mode, Mode::Changed) {
        let mut changed = record;
        changed.blocker = Some(crate::BlockerCode::StalePlan);
        failure.journal_failure = service.journal.update(&changed).err();
        failure.changed = Some(changed);
    } else {
        // This is the fresh test-owned home/operation, not an existing user journal.
        let path = service
            .inputs
            .codex_home
            .join("storage-operations")
            .join(format!("{operation_id}.json"));
        failure.journal_failure = std::fs::write(path, b"{").err();
    }
    probe.release.notify_one(); // Always release actual body before any verdict.
    let mut finish = Box::pin(service.original_operations.finish(Arc::clone(&original)));
    let (late, returned) = match tokio::time::timeout(Duration::from_secs(30), &mut finish).await {
        Ok(returned) => (false, returned),
        Err(_) => (true, finish.await),
    };
    failure.plan_failure = early.and_then(Result::err);
    let raw = original
        .resources
        .actual_plan_refusal_before_administrative_capture();
    let valid_raw = raw
        .as_ref()
        .and_then(|raw| {
            raw.downcast_ref::<crate::administration_binding::OperationPlanRevalidationRefusal>()
        })
        .is_some_and(|raw| match mode {
            Mode::Changed => {
                raw.actual_changed_record() == failure.changed.as_ref()
                    && raw.actual_read_error().is_none()
            }
            Mode::Corrupt => raw
                .actual_read_error()
                .is_some_and(|raw| raw.kind() == std::io::ErrorKind::InvalidData),
            _ => false,
        });
    failure.raw_revalidation = raw;
    let rejected = matches!(&returned, Err(raw) if raw.0 == crate::BlockerCode::Internal);
    let actual_terminal = {
        let state = original.state.lock();
        state.is_ok_and(|state| {
            state.task.is_none() && state.failure.is_none()
            && state.reaper_terminal && state.reaper.is_none() && state.reaper_failure.is_none()
            && state.result.as_ref().is_some_and(|returned|
                matches!(returned, Err(raw) if raw.0 == crate::BlockerCode::Internal))
        })
    };
    failure.returned = Some(returned);
    if !entered
        || late
        || !rejected
        || !valid_raw
        || !actual_terminal
        || failure.journal_failure.is_some()
        || !original.resources.cleanup_completed()
    {
        return Err(Box::new(failure));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicit empty default fixture and genuine Runtime origin privilege"]
async fn default_actual_operation_plan_negatives() -> super::super::tests::ControlResult<()> {
    for mode in [Mode::Mismatch, Mode::Foreign, Mode::Changed, Mode::Corrupt] {
        actual_plan_negative(false, mode).await?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires explicit empty named fixture and genuine Runtime origin privilege"]
async fn named_actual_operation_plan_negatives() -> super::super::tests::ControlResult<()> {
    for mode in [Mode::Mismatch, Mode::Foreign, Mode::Changed, Mode::Corrupt] {
        actual_plan_negative(true, mode).await?;
    }
    Ok(())
}
