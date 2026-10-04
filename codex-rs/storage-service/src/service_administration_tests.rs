//! Explicit generic-server fixtures; public caller cancellation at task entry.
//! No in-flight query, host, privilege provisioning or exclusive authority proof.
use super::*;
use codex_storage_authority::CredentialSource;
use codex_storage_authority::RemotePostgresProfile;
use codex_utils_absolute_path::test_support::PathExt;
use std::time::Duration;

struct Failure {
    binding: Arc<crate::OriginalServiceAdministration>,
    original: Option<Arc<Original>>,
    origin: Option<Arc<codex_remote_storage::RuntimeNativeOrigin>>,
    cause: Option<crate::ServiceRuntimeOriginRefusal>,
    returned: Option<Result<Output, StorageError>>,
}
impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = (
            &self.binding,
            &self.original,
            &self.origin,
            &self.cause,
            &self.returned,
        );
        f.write_str("AdministrativeCallerControlFailed([original resources retained])")
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
impl std::error::Error for Failure {}

async fn canceled_public_preflight(named: bool) -> super::tests::ControlResult<()> {
    let variable = if named {
        "CODEX_REAL_EXTERNAL_ADMIN_NAMED_INPUT"
    } else {
        "CODEX_REAL_EXTERNAL_ADMIN_INPUT"
    };
    let path =
        std::path::PathBuf::from(std::env::var(variable).map_err(|_| "explicit fixture required")?);
    if !path.is_absolute() || std::fs::metadata(&path)?.len() > 65_536 {
        return Err("bounded absolute fixture input required".into());
    }
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path)?).map_err(|_| "fixture input invalid")?;
    let fields = value.as_object_mut().ok_or("fixture fields invalid")?;
    if fields.len() != 3
        || !fields.contains_key("target")
        || !fields.contains_key("login")
        || !fields.contains_key("administrative_credential")
    {
        return Err("exact explicit fixture fields required".into());
    }
    let target: RemotePostgresProfile =
        serde_json::from_value(fields.remove("target").ok_or("target missing")?)
            .map_err(|_| "fixture target invalid")?;
    if (target.namespace() != "codex_storage") != named || target.max_connections() < 2 {
        return Err("selected namespace and two-connection fixture required".into());
    }
    let login = fields
        .remove("login")
        .and_then(|login| login.as_str().map(str::to_owned))
        .ok_or("explicit administrative login required")?;
    let credential: CredentialSource = serde_json::from_value(
        fields
            .remove("administrative_credential")
            .ok_or("administrative credential missing")?,
    )
    .map_err(|_| "credential reference invalid")?;
    let home = tempfile::tempdir()?.keep();
    let service = Arc::new(StorageService::new(crate::StorageServiceInputs {
        codex_home: home.clone(),
        sqlite: codex_state::SqliteConfig::new_for_testing(home.abs()),
        candidate: Some(target),
        default_model_provider_id: "administrative-caller-control".into(),
        host_label: "explicit-control".into(),
        keyring: Arc::new(codex_keyring_store::DefaultKeyringStore),
    }));
    let binding = service
        .prepare_administration(login, credential)
        .map_err(|_| "administrative preparation refused before dispatch")?;
    let origin = match binding.observe_runtime_origin().await {
        Ok(origin) => origin,
        Err(cause) => {
            return Err(Box::new(Failure {
                binding,
                original: None,
                origin: None,
                cause: Some(cause),
                returned: None,
            }));
        }
    };
    let probe = Arc::new(Probe::default());
    let installed = match service.original_operations.probe.lock() {
        Ok(mut slot) => {
            *slot = Some(Arc::clone(&probe));
            true
        }
        Err(_) => false,
    };
    if !installed {
        return Err(Box::new(Failure {
            binding,
            original: None,
            origin: Some(origin),
            cause: None,
            returned: None,
        })); // Runtime origin producer already physically reconciled its own SDK.
    }
    let mut caller = Box::pin(binding.preflight_original(Arc::clone(&origin)));
    let mut early_returned = None;
    let entered = tokio::select! { biased;
        returned = &mut caller => { early_returned = Some(returned); false },
        result = tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()) => result.is_ok(),
    };
    let original = {
        let originals = service
            .original_operations
            .originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        originals
            .iter()
            .find(|original| original.kind == Kind::AdministrativePreflight)
            .map(Arc::clone)
    };
    let Some(original) = original else {
        probe.release.notify_one();
        if let Some(returned) = early_returned {
            drop(caller);
            return Err(Box::new(Failure {
                binding,
                original: None,
                origin: Some(origin),
                cause: returned.err(),
                returned: None,
            })); // Exact caller return retained; binding keeps failed operation history.
        }
        // An unknown dispatched task cannot be classified by an outward error.
        // Retain the same pinned caller, binding, service and native origin.
        let retained = (&mut caller, &binding, &service, &origin);
        let result = std::future::pending::<super::tests::ControlResult<()>>().await;
        let _ = retained;
        return result;
    };
    drop(caller); // Only the actual public waiter is canceled.
    let (actual_id, retained_id) = {
        let state = original
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.operation_task_id,
            state.task.as_ref().map(JoinHandle::id),
        )
    };
    probe.release.notify_one(); // Unconditional before all diagnostic verdicts.
    let mut finish = Box::pin(service.original_operations.finish(Arc::clone(&original)));
    let (late, returned) = match tokio::time::timeout(Duration::from_secs(30), &mut finish).await {
        Ok(returned) => (false, returned),
        Err(_) => (true, finish.await), // Same registered operation/reaper/cleanup.
    };
    let physical = original.resources.cleanup_completed();
    let archive = physical
        && original
            .resources
            .actual_administrative_archive_for(binding.original_observer());
    let valid =
        matches!(&returned, Ok(Output::Observed(value)) if value.downcast_ref::<()>().is_some());
    if !entered
        || late
        || actual_id.is_none()
        || actual_id != retained_id
        || !physical
        || !archive
        || !valid
    {
        return Err(Box::new(Failure {
            binding,
            original: Some(original),
            origin: Some(origin),
            cause: early_returned.and_then(Result::err),
            returned: Some(returned),
        }));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicitly admitted Runtime origin privilege and separate administrative fixture"]
async fn default_public_administrative_caller_cancellation() -> super::tests::ControlResult<()> {
    canceled_public_preflight(false).await
}
#[tokio::test]
#[ignore = "requires explicitly admitted named Runtime origin privilege and separate administrative fixture"]
async fn named_public_administrative_caller_cancellation() -> super::tests::ControlResult<()> {
    canceled_public_preflight(true).await
}
