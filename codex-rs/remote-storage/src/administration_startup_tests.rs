//! Ignored actual external PostgreSQL controls. No roles/grants/HBA are created.
use super::*;
use crate::administration_close::SdkProbe;
use codex_keyring_store::DefaultKeyringStore;
use codex_storage_authority::CredentialSource;
use codex_storage_authority::RemotePostgresProfile;
use std::fmt;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

pub(crate) struct Failure {
    original: Option<Arc<OriginalAdministrativeStartup>>,
    admission: Option<AdministrativeStartupRefusal>,
    cleanup: Option<AdministrativeStartupRefusal>,
    acquisition: Option<PoolError>,
    input: Option<&'static str>,
}
impl fmt::Debug for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _retained = (
            &self.original,
            &self.admission,
            &self.cleanup,
            &self.acquisition,
            &self.input,
        );
        formatter.write_str("ExternalAdministrativeControlFailure([original retained])")
    }
}
fn input_failure(code: &'static str) -> Failure {
    Failure {
        original: None,
        admission: None,
        cleanup: None,
        acquisition: None,
        input: Some(code),
    }
}
pub(crate) fn input(named: bool) -> Result<Arc<RemoteAdministrationInput>, Failure> {
    let variable = if named {
        "CODEX_REAL_EXTERNAL_ADMIN_NAMED_INPUT"
    } else {
        "CODEX_REAL_EXTERNAL_ADMIN_INPUT"
    };
    input_at(named, variable)
}

pub(crate) fn input_at(
    named: bool,
    variable: &str,
) -> Result<Arc<RemoteAdministrationInput>, Failure> {
    let path = PathBuf::from(
        std::env::var_os(variable)
            .ok_or_else(|| input_failure("explicit external admin input required"))?,
    );
    if !path.is_absolute() {
        return Err(input_failure("absolute host input path required"));
    }
    let file = std::fs::File::open(path).map_err(|_| input_failure("input read failed"))?;
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| input_failure("input read failed"))?;
    if bytes.len() > 65536 {
        return Err(input_failure("input too large"));
    }
    let mut value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| input_failure("input invalid"))?;
    let fields = value
        .as_object_mut()
        .ok_or_else(|| input_failure("input invalid"))?;
    if fields.len() != 3
        || !fields.contains_key("target")
        || !fields.contains_key("login")
        || !fields.contains_key("administrative_credential")
    {
        return Err(input_failure("exact input fields required"));
    }
    let target: RemotePostgresProfile = serde_json::from_value(
        fields
            .remove("target")
            .ok_or_else(|| input_failure("target missing"))?,
    )
    .map_err(|_| input_failure("target invalid"))?;
    if (target.namespace() != "codex_storage") != named || target.max_connections() < 2 {
        return Err(input_failure(
            "selected namespace and two-connection fixture required",
        ));
    }
    let login = fields
        .remove("login")
        .and_then(|login| login.as_str().map(str::to_owned))
        .ok_or_else(|| input_failure("login invalid"))?;
    let credential: CredentialSource = serde_json::from_value(
        fields
            .remove("administrative_credential")
            .ok_or_else(|| input_failure("credential missing"))?,
    )
    .map_err(|_| input_failure("credential invalid"))?;
    RemoteAdministrationInput::new(Arc::new(target), login, credential)
        .map(Arc::new)
        .map_err(|_| input_failure("administrative input refused"))
}

async fn control(named: bool, cancel_close_waiter: bool) -> Result<(), Failure> {
    let selected = input(named)?;
    let store = DefaultKeyringStore;
    let resolver = HostCredentialResolver::new(&store);
    let original =
        OriginalAdministrativeStartup::start(selected, &resolver).map_err(|admission| Failure {
            original: Some(Arc::clone(&admission.original)),
            admission: Some(admission),
            cleanup: None,
            acquisition: None,
            input: None,
        })?;
    let admission = original.finish_original().await;
    let pool = {
        original
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pool
            .clone()
    };
    let mut acquisition = None;
    let mut held = None;
    if admission.is_ok() && cancel_close_waiter {
        if let Some(pool) = &pool {
            match pool.acquire().await {
                Ok(connection) => held = Some(connection),
                Err(error) => acquisition = Some(error),
            }
        }
    }
    let mut witnessed = !cancel_close_waiter;
    let mut restored = !cancel_close_waiter;
    let mut late = false;
    if held.is_some() {
        let (sender, mut ready) = tokio::sync::oneshot::channel();
        let probe = Arc::new(SdkProbe {
            ready: Mutex::new(Some(sender)),
            release: Semaphore::new(0),
        });
        original.close.install_probe(Arc::clone(&probe));
        let mut observed_task = None;
        {
            let caller = original.close_original();
            tokio::pin!(caller);
            let observed = tokio::time::timeout(Duration::from_secs(15), async {
                tokio::select! { biased;
                    result = &mut caller => { let _owned = result; None },
                    result = &mut ready => result.ok(),
                }
            })
            .await;
            if let Ok(Some((task, pending))) = observed {
                witnessed = pending;
                // Dropping ONLY the caller restores the original SDK JH.
                observed_task = Some(task);
            } else {
                late = observed.is_err();
            }
        }
        if let Some(task) = observed_task {
            restored = original.close.same_sdk_task_restored(task);
        }
        // Every witness/refusal path releases both original probe and checked-out
        // connection before continuing the SAME production close tasks.
        probe.release.add_permits(1);
    }
    drop(held);
    let cleanup = original.close_original();
    tokio::pin!(cleanup);
    let closed = match tokio::time::timeout(Duration::from_secs(15), &mut cleanup).await {
        Ok(result) => result,
        Err(_) => {
            late = true;
            cleanup.await
        }
    };
    let actual_closed = original.close.known_native_sdk_closed();
    if !actual_closed {
        return Err(Failure {
            original: Some(original),
            admission: admission.err(),
            cleanup: closed.err(),
            acquisition,
            input: Some("original physical close remains unresolved"),
        });
    }
    let pool_closed = if let Some(pool) = &pool {
        match pool.acquire().await {
            Err(PoolError::Closed) => true,
            Err(error) => {
                acquisition = Some(error);
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
    if admission.is_err()
        || closed.is_err()
        || !actual_closed
        || !pool_closed
        || acquisition.is_some()
        || !witnessed
        || !restored
        || late
    {
        return Err(Failure {
            original: Some(original),
            admission: admission.err(),
            cleanup: closed.err(),
            acquisition,
            input: None,
        });
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires explicitly admitted external admin default target; no provisioning"]
async fn default_original_native_and_sdk_close() -> Result<(), Failure> {
    control(false, false).await
}
#[tokio::test]
#[ignore = "requires explicitly admitted external admin named target; no provisioning"]
async fn named_original_native_and_sdk_close() -> Result<(), Failure> {
    control(true, false).await
}
#[tokio::test]
#[ignore = "requires real external admin default target and checked-out connection"]
async fn default_canceled_sdk_observer_keeps_same_original() -> Result<(), Failure> {
    control(false, true).await
}
#[tokio::test]
#[ignore = "requires real external admin named target and checked-out connection"]
async fn named_canceled_sdk_observer_keeps_same_original() -> Result<(), Failure> {
    control(true, true).await
}
