//! Real host caller controls; no roles, grants or database provisioning.
use super::*;
use codex_keyring_store::DefaultKeyringStore;

struct Failure {
    observer: Option<Arc<RemoteAdministrativeObserver>>,
    input: Option<crate::administration_startup::tests::Failure>,
    observed: Option<AdministrativeObservationRefusal>,
    archive_refusal: Option<AdministrativeRegistryRefusal>,
    archives: [Option<AdministrativeFailureArchive>; 2],
}
impl fmt::Debug for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _originals = (
            &self.observer,
            &self.input,
            &self.observed,
            &self.archive_refusal,
            &self.archives,
        );
        formatter.write_str("AdministrativeHostControlFailure([originals retained])")
    }
}
async fn control(named: bool, denied: bool) -> Result<(), Failure> {
    let mut failure = Failure {
        observer: None,
        input: None,
        observed: None,
        archive_refusal: None,
        archives: std::array::from_fn(|_| None),
    };
    let input = if denied {
        crate::administration_startup::tests::input_at(
            named,
            if named {
                "CODEX_REAL_EXTERNAL_ADMIN_NAMED_DENIED_INPUT"
            } else {
                "CODEX_REAL_EXTERNAL_ADMIN_DENIED_INPUT"
            },
        )
    } else {
        crate::administration_startup::tests::input(named)
    };
    let input = match input {
        Ok(input) => input,
        Err(error) => {
            failure.input = Some(error);
            return Err(failure);
        }
    };
    let observer =
        RemoteAdministrativeObserver::new(Arc::clone(&input), Arc::new(DefaultKeyringStore));
    failure.observer = Some(Arc::clone(&observer));
    let mut native_identity = None;
    for index in 0..if denied { 2 } else { 32 } {
        // The actual public caller owns the full capture/observe/cleanup path.
        // Err owns pending originals; never interpret a refusal as a join ACK.
        let observed = observer.observe_original().await;
        let verdict = match observed {
            Ok(observed) => {
                let same_native = native_identity.as_ref().is_none_or(|(system, database)| {
                    system == &observed.system_identifier && *database == observed.database_oid
                });
                native_identity = Some((observed.system_identifier.clone(), observed.database_oid));
                !denied
                    && same_native
                    && Arc::ptr_eq(observer.original_input(), &input)
                    && observed.login == input.login()
            }
            Err(error) => {
                let same_owner = Arc::ptr_eq(error.original_observer(), &observer);
                let native_refusal = error.observation.as_ref().is_some_and(|original| {
                    original.diagnostic_kind() == "administrative_native_facts_refused"
                });
                failure.observed = Some(error);
                denied && same_owner && native_refusal
            }
        };
        let archive = match observer.take_failure_archive() {
            Ok(archive) => archive,
            Err(error) => {
                failure.archive_refusal = Some(error);
                return Err(failure);
            }
        };
        let archived = if denied {
            archive.len() == 1
        } else {
            archive.is_empty()
        };
        if denied {
            failure.archives[index] = Some(archive);
        }
        if !verdict || !archived {
            return Err(failure);
        }
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires admitted external admin default target; real public caller 32 cycles"]
async fn default_public_observer_reconciles_before_success() -> Result<(), Failure> {
    control(false, false).await
}
#[tokio::test]
#[ignore = "requires admitted external admin named target; real public caller 32 cycles"]
async fn named_public_observer_reconciles_before_success() -> Result<(), Failure> {
    control(true, false).await
}
#[tokio::test]
#[ignore = "requires explicit authenticated default denied-admin fixture; no grants"]
async fn default_public_observer_owns_failed_originals_across_retry() -> Result<(), Failure> {
    control(false, true).await
}
#[tokio::test]
#[ignore = "requires explicit authenticated named denied-admin fixture; no grants"]
async fn named_public_observer_owns_failed_originals_across_retry() -> Result<(), Failure> {
    control(true, true).await
}
