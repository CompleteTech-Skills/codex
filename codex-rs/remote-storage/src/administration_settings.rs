//! Inactive administrative TLS factory prerequisite, not a privilege permit.
//!
//! Declaration requires the administrative input export and an original task/
//! session consumer registered before polling this factory. Returned SDK pools
//! do not authorize role changes, credential retirement or exclusive transition.

use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use codex_storage_authority::CredentialResolutionError;
use codex_storage_authority::HostCredentialResolver;
use codex_storage_authority::RemoteAdministrationInput;
use std::sync::Arc;
use std::time::Duration;

pub enum AdministrativeFactoryError {
    InvalidNamespace,
    MissingExplicitCa,
    Credential(CredentialResolutionError),
    Driver(PoolError),
}

pub(super) fn administrative_settings(
    original: &Arc<RemoteAdministrationInput>,
    resolver: &HostCredentialResolver<'_>,
) -> Result<(ConnectionSettings, Option<NamedNamespace>), AdministrativeFactoryError> {
    let target = original.target();
    let namespace = if target.namespace() == "codex_storage" {
        None
    } else {
        Some(
            NamedNamespace::new(target.namespace())
                .map_err(|_| AdministrativeFactoryError::InvalidNamespace)?,
        )
    };
    let ca = target
        .ca_certificate()
        .ok_or(AdministrativeFactoryError::MissingExplicitCa)?;
    // Only the explicit administrative source is resolved. Neither ordinary
    // target credential is read as an alternative on lookup or login refusal.
    let credential = resolver
        .resolve(original.credential())
        .map_err(AdministrativeFactoryError::Credential)?;
    Ok((
        ConnectionSettings {
            host: target.endpoint().to_owned(),
            port: target.port(),
            database: target.database().to_owned(),
            username: original.login().to_owned(),
            password: credential.into_zeroizing().into(),
            ca_certificate: ca.to_path_buf(),
            limits: PoolLimits {
                connect_timeout: Duration::from_secs(u64::from(target.connect_timeout_seconds())),
                acquire_timeout: Duration::from_secs(u64::from(
                    target.pool_acquire_timeout_seconds(),
                )),
                max_connections: u32::from(target.max_connections()),
            },
        },
        namespace,
    ))
}

/// Only the pre-registered original startup task consumes these already
/// explicitly resolved settings. Its exterior frame captures SAME SDK before
/// any acquire, including callback refusal. There is no parallel public connector.
pub(super) async fn connect_administration_prepared(
    settings: ConnectionSettings,
    namespace: Option<NamedNamespace>,
    capture: &mut (dyn FnMut(Arc<PostgresPool>) -> Result<(), PoolError> + Send),
) -> Result<Arc<PostgresPool>, AdministrativeFactoryError> {
    // Central factory enforces verify_full, exact version and original namespace;
    // no migration_login initialization or owner SET ROLE is requested here.
    PostgresPool::connect_retained_in_namespace(settings, namespace.as_ref(), capture)
        .await
        .map_err(AdministrativeFactoryError::Driver)
}
