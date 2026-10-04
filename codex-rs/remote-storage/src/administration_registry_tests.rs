//! Actual external admin fixtures only. No role/grant/HBA mutation.
use super::*;
use codex_keyring_store::DefaultKeyringStore;
use codex_postgres_runtime::PoolError;

async fn poisoned_registry_control(named: bool) -> Result<(), Failure> {
    let registry = OriginalAdministrativeRegistry::new();
    let mut failure = Failure {
        registry: Arc::clone(&registry),
        original: None,
        input: None,
        first_capture: None,
        second_capture: None,
        first_observation: None,
        second_observation: None,
        admission: None,
        late_admission: None,
        cleanup: None,
        acquisition: None,
        archives: std::array::from_fn(|_| None),
        raw_poison: None,
    };
    let input = match crate::administration_startup::tests::input(named) {
        Ok(input) => input,
        Err(error) => {
            failure.input = Some(error);
            return Err(failure);
        }
    };
    let keyring = DefaultKeyringStore;
    let resolver = HostCredentialResolver::new(&keyring);
    let borrowed = match registry.capture(Arc::clone(&input), &resolver) {
        Ok(borrowed) => Some(borrowed),
        Err(error) => {
            failure.first_capture = Some(error);
            None
        }
    };
    let original = borrowed
        .as_ref()
        .map(|borrowed| Arc::clone(&borrowed.original));
    failure.original = original.clone();
    if let Some(borrowed) = &borrowed {
        failure.first_observation = borrowed.observe_original().await.err();
    }
    let pool = original
        .as_ref()
        .and_then(|original| original.captured_pool());
    let controller = Arc::clone(&registry);
    let task = tokio::spawn(async move {
        if let Ok(_guard) = controller.state.lock() {
            panic!("injected administrative registry poison");
        }
    });
    // Cache the actual joined panic cause BEFORE any original cleanup await.
    failure.raw_poison = task.await.err().map(Arc::new);
    drop(borrowed);
    let late = registry.capture(Arc::clone(&input), &resolver);
    let refused_late = matches!(&late, Err(error) if error.reason == "registry_poisoned");
    match late {
        Ok(unexpected) => drop(unexpected),
        Err(error) => failure.late_admission = Some(error),
    }
    let cleanup = registry.close_input(&input).await;
    let refused_cleanup = matches!(&cleanup, Err(error) if error.reason == "registry_poisoned");
    failure.cleanup = cleanup.err();
    let handoff = registry.take_failure_archive();
    let same_archive = matches!(&handoff, Err(error) if error.reason == "registry_poisoned"
        && error.archive.as_ref().is_some_and(|archive| archive.len() == 1
            && original.as_ref().is_some_and(|original|
                archive.originals.iter().flatten().any(|saved| Arc::ptr_eq(saved, original)))));
    match handoff {
        Ok(archive) => failure.archives[0] = Some(archive),
        Err(error) => failure.admission = Some(error),
    }
    let closed = original
        .as_ref()
        .is_some_and(|original| original.archive_closed_original());
    if !closed {
        return Err(failure);
    }
    let sdk_closed = if let Some(pool) = pool {
        match pool.acquire().await {
            Err(PoolError::Closed) => true,
            Err(error) => {
                failure.acquisition = Some(error);
                false
            }
            Ok(unexpected) => {
                drop(unexpected);
                false
            }
        }
    } else {
        false
    };
    if !refused_late
        || !refused_cleanup
        || !same_archive
        || !sdk_closed
        || failure.first_capture.is_some()
        || failure.first_observation.is_some()
        || !failure
            .raw_poison
            .as_ref()
            .is_some_and(|cause| cause.is_panic())
    {
        return Err(failure);
    }
    Ok(())
}

async fn refused_role_control(named: bool) -> Result<(), Failure> {
    let registry = OriginalAdministrativeRegistry::new();
    let mut failure = Failure {
        registry: Arc::clone(&registry),
        original: None,
        input: None,
        first_capture: None,
        second_capture: None,
        first_observation: None,
        second_observation: None,
        admission: None,
        late_admission: None,
        cleanup: None,
        acquisition: None,
        archives: std::array::from_fn(|_| None),
        raw_poison: None,
    };
    let variable = if named {
        "CODEX_REAL_EXTERNAL_ADMIN_NAMED_DENIED_INPUT"
    } else {
        "CODEX_REAL_EXTERNAL_ADMIN_DENIED_INPUT"
    };
    let input = match crate::administration_startup::tests::input_at(named, variable) {
        Ok(input) => input,
        Err(error) => {
            failure.input = Some(error);
            return Err(failure);
        }
    };
    let keyring = DefaultKeyringStore;
    let resolver = HostCredentialResolver::new(&keyring);
    let mut previous: Option<Arc<OriginalAdministrativeStartup>> = None;
    for index in 0..2 {
        let borrowed = match registry.capture(Arc::clone(&input), &resolver) {
            Ok(borrowed) => Some(borrowed),
            Err(error) => {
                failure.first_capture = Some(error);
                None
            }
        };
        let original = borrowed
            .as_ref()
            .map(|borrowed| Arc::clone(&borrowed.original));
        failure.original = original.clone();
        let observation = if let Some(borrowed) = &borrowed {
            borrowed.observe_original().await.err()
        } else {
            None
        };
        let actual_role_refusal = observation
            .as_ref()
            .is_some_and(|error| error.diagnostic_kind() == "administrative_native_facts_refused");
        let fresh_original = previous.as_ref().is_none_or(|previous| {
            original
                .as_ref()
                .is_some_and(|original| !Arc::ptr_eq(previous, original))
        });
        let pool = original
            .as_ref()
            .and_then(|original| original.captured_pool());
        drop(borrowed);
        failure.first_observation = observation;
        let cleanup = registry.close_input(&input).await;
        let refused = cleanup.is_err();
        failure.cleanup = cleanup.err();
        let archive = match registry.take_failure_archive() {
            Ok(archive) => archive,
            Err(error) => {
                failure.admission = Some(error);
                return Err(failure);
            }
        };
        let same_archived = archive.len() == 1
            && original.as_ref().is_some_and(|original| {
                archive
                    .originals
                    .iter()
                    .flatten()
                    .any(|saved| Arc::ptr_eq(saved, original))
            });
        // Unknown owners remain registered. No post-close acquisition until
        // SAME native/SDK task completion is actually recorded.
        let closed = original
            .as_ref()
            .is_some_and(|original| original.archive_closed_original());
        failure.archives[index] = Some(archive);
        if !closed {
            return Err(failure);
        }
        let sdk_closed = if let Some(pool) = pool {
            match pool.acquire().await {
                Err(PoolError::Closed) => true,
                Err(error) => {
                    failure.acquisition = Some(error);
                    false
                }
                Ok(unexpected) => {
                    drop(unexpected);
                    false
                }
            }
        } else {
            false
        };
        if !actual_role_refusal || !fresh_original || !refused || !same_archived || !sdk_closed {
            return Err(failure);
        }
        previous = original;
    }
    Ok(())
}

struct Failure {
    registry: Arc<OriginalAdministrativeRegistry>,
    original: Option<Arc<OriginalAdministrativeStartup>>,
    input: Option<crate::administration_startup::tests::Failure>,
    first_capture: Option<AdministrativeRegistryRefusal>,
    second_capture: Option<AdministrativeRegistryRefusal>,
    first_observation: Option<AdministrativeStartupRefusal>,
    second_observation: Option<AdministrativeStartupRefusal>,
    admission: Option<AdministrativeRegistryRefusal>,
    late_admission: Option<AdministrativeRegistryRefusal>,
    cleanup: Option<AdministrativeRegistryRefusal>,
    acquisition: Option<PoolError>,
    archives: [Option<AdministrativeFailureArchive>; 2],
    raw_poison: Option<Arc<tokio::task::JoinError>>,
}
impl fmt::Debug for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _owned = (
            &self.registry,
            &self.original,
            &self.input,
            &self.first_capture,
            &self.second_capture,
            &self.first_observation,
            &self.second_observation,
            &self.admission,
            &self.late_admission,
            &self.cleanup,
            &self.acquisition,
            &self.archives,
            &self.raw_poison,
        );
        formatter.write_str("ExternalRegistryControlFailure([originals retained])")
    }
}
async fn control(named: bool) -> Result<(), Failure> {
    let registry = OriginalAdministrativeRegistry::new();
    let mut failure = Failure {
        registry: Arc::clone(&registry),
        original: None,
        input: None,
        first_capture: None,
        second_capture: None,
        first_observation: None,
        second_observation: None,
        admission: None,
        late_admission: None,
        cleanup: None,
        acquisition: None,
        archives: std::array::from_fn(|_| None),
        raw_poison: None,
    };
    let input = match crate::administration_startup::tests::input(named) {
        Ok(input) => input,
        Err(error) => {
            failure.input = Some(error);
            return Err(failure);
        }
    };
    let keyring = DefaultKeyringStore;
    let resolver = HostCredentialResolver::new(&keyring);
    // One long-lived registry, >16 actual successful startup/session/SDK cycles.
    for _ in 0..32 {
        let first = match registry.capture(Arc::clone(&input), &resolver) {
            Ok(first) => Some(first),
            Err(error) => {
                failure.first_capture = Some(error);
                None
            }
        };
        let second = if first.is_some() {
            match registry.capture(Arc::clone(&input), &resolver) {
                Ok(second) => Some(second),
                Err(error) => {
                    failure.second_capture = Some(error);
                    None
                }
            }
        } else {
            None
        };
        let original = first.as_ref().map(|first| Arc::clone(&first.original));
        failure.original = original.clone();
        let same_original = match (&first, &second) {
            (Some(first), Some(second)) => Arc::ptr_eq(&first.original, &second.original),
            _ => false,
        };
        if let Some(first) = &first {
            failure.first_observation = first.observe_original().await.err();
        }
        if let Some(second) = &second {
            failure.second_observation = second.observe_original().await.err();
        }
        let pool = original
            .as_ref()
            .and_then(|original| original.captured_pool());
        let same_pool = match (&pool, &second) {
            (Some(pool), Some(second)) => second
                .original
                .captured_pool()
                .is_some_and(|second| Arc::ptr_eq(pool, &second)),
            _ => false,
        };
        // A real live observation borrow must prevent cleanup/reclamation.
        let gate = registry.close_input(&input).await;
        let borrowed_refusal = matches!(&gate, Err(error) if error.reason == "original_borrowed");
        failure.admission = gate.err();
        let late = registry.capture(Arc::clone(&input), &resolver);
        let late_refusal = matches!(&late, Err(error) if error.reason == "original_sealed");
        match late {
            Ok(unexpected) => drop(unexpected),
            Err(error) => failure.late_admission = Some(error),
        }
        // Every unexpected/refusal path releases all actual borrowers before
        // reconciling SAME startup/native/SDK tasks, then evaluates diagnostics.
        drop(first);
        drop(second);
        let cleanup = registry.close_input(&input).await;
        if let Err(error) = cleanup {
            failure.cleanup = Some(error);
            return Err(failure); // original registry retains unknown/archived owners
        }
        let pool_closed = if let Some(pool) = pool {
            match pool.acquire().await {
                Err(PoolError::Closed) => true,
                Err(error) => {
                    failure.acquisition = Some(error);
                    false
                }
                Ok(unexpected) => {
                    drop(unexpected);
                    false
                }
            }
        } else {
            false
        };
        if !same_original
            || !same_pool
            || !borrowed_refusal
            || !late_refusal
            || !pool_closed
            || failure.first_capture.is_some()
            || failure.second_capture.is_some()
            || failure.first_observation.is_some()
            || failure.second_observation.is_some()
        {
            return Err(failure);
        }
        // Known expected refusal wrappers can leave this test's verdict frame;
        // successful originals are already physically closed/reclaimed.
        failure.admission = None;
        failure.late_admission = None;
        failure.original = None;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires admitted external admin default target; 32 real SDK cycles"]
async fn default_registry_reuses_only_same_original_and_reclaims_closed_success()
-> Result<(), Failure> {
    control(false).await
}
#[tokio::test]
#[ignore = "requires admitted external admin named target; 32 real SDK cycles"]
async fn named_registry_reuses_only_same_original_and_reclaims_closed_success()
-> Result<(), Failure> {
    control(true).await
}

#[tokio::test]
#[ignore = "requires authenticated default login missing admin privileges; no grants"]
async fn default_failed_role_originals_archive_before_fresh_retry() -> Result<(), Failure> {
    refused_role_control(false).await
}
#[tokio::test]
#[ignore = "requires authenticated named login missing admin privileges; no grants"]
async fn named_failed_role_originals_archive_before_fresh_retry() -> Result<(), Failure> {
    refused_role_control(true).await
}

#[tokio::test]
#[ignore = "requires admitted default admin fixture; real original registry poison cleanup"]
async fn default_poison_reconciles_known_originals_before_owning_refusal() -> Result<(), Failure> {
    poisoned_registry_control(false).await
}
#[tokio::test]
#[ignore = "requires admitted named admin fixture; real original registry poison cleanup"]
async fn named_poison_reconciles_known_originals_before_owning_refusal() -> Result<(), Failure> {
    poisoned_registry_control(true).await
}
