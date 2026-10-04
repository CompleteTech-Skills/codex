//! A bounded PostgreSQL connection pool for host-resolved credentials.
//!
//! The pool does not select the active storage backend or grant authority to a
//! candidate configuration. Callers that need the preprovisioned storage
//! schema must explicitly invoke the transactional `bootstrap_codex_storage`
//! entry point.
//! Only PostgreSQL 17.11 is qualified by the current real-server fixture. This
//! exact-version gate does not assert support for every PostgreSQL 17 release.

#![expect(
    clippy::disallowed_methods,
    reason = "this is the centralized PostgreSQL connection shim"
)]

use sqlx::ConnectOptions;
use sqlx::PgPool;
use sqlx::Postgres;
use sqlx::pool::PoolConnection;
use sqlx_postgres::PgConnectOptions;
use sqlx_postgres::PgPoolOptions;
use sqlx_postgres::PgSslMode;
// The workspace SQLx facade includes SQLite. Every enabled driver must provide
// the offline API once PostgreSQL enables it on their shared sqlx-core crate.
use sqlx_sqlite as _;
use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::timeout;
use zeroize::Zeroizing;

mod activation_gate;
mod migration_causes;
mod migration_startup;
pub use migration_startup::MigrationStartupRefusal;
pub use migration_startup::OriginalMigrationCleanupCompleted;
pub use migration_startup::OriginalMigrationPoolClosed;
pub use migration_startup::OriginalMigrationStartup;
mod migration_login;
mod session_binding;
pub use activation_gate::ActivationGateError;
pub use activation_gate::require_storage_open;
pub use session_binding::BoundWrite;
pub use session_binding::OriginalSessionRollback;
pub use session_binding::OriginalTransactionAcknowledgment;
pub use session_binding::OriginalTransactionOutcome;
pub use session_binding::RetainedWriteAdmissionError;
pub use session_binding::RetainedWriteSession;
mod bootstrap_session_owner;
pub use bootstrap_session_owner::BootstrapSessionCleanup;
pub use bootstrap_session_owner::BootstrapSessionOwner;
pub use bootstrap_session_owner::BootstrapSessionRefusal;
mod bootstrap;
pub use bootstrap::BootstrapError;
pub use bootstrap::bootstrap_codex_storage;
pub use bootstrap::bootstrap_codex_storage_retained;
mod compatibility;
pub use compatibility::ClientCapabilities;
pub use compatibility::CompatibilityError;
pub use compatibility::CompatibilityResult;
pub use compatibility::RequiredAccess;
pub use compatibility::check_codex_storage_compatibility;
mod namespace;
mod runtime_schema;
pub use runtime_schema::RuntimeSchemaError;
pub use runtime_schema::check_runtime_schema;
pub use runtime_schema::client_schema_format;
mod schema_registry;
pub use namespace::InvalidNamespace;
pub use namespace::NamedNamespace;
mod named_bootstrap;
pub use named_bootstrap::bootstrap_named_namespace;
pub use named_bootstrap::bootstrap_named_namespace_retained;
mod named_compatibility;
pub use named_compatibility::check_named_namespace_compatibility;
mod transaction;
pub use transaction::PostgresTransaction;
pub use transaction::TransactionError;
mod thread_ownership;
pub use thread_ownership::ThreadOwnership;
pub use thread_ownership::ThreadOwnershipAttempt;
pub use thread_ownership::ThreadOwnershipError;
pub use thread_ownership::ThreadOwnershipNamespace;
mod verified_target;
pub use verified_target::TargetVerificationError;
pub use verified_target::VerifiedTarget;
pub use verified_target::check_verified_named_target_compatibility;
pub use verified_target::check_verified_target_compatibility;
pub use verified_target::verify_target_artifact;

const MAX_WAIT: Duration = Duration::from_secs(30);
const MAX_CONNECTIONS: u32 = 32;
const QUALIFIED_SERVER_VERSION_NUM: &str = "170011";

/// Resolved by the owning host. The password must not be logged or persisted.
pub struct ConnectionSettings {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: SecretPassword,
    pub ca_certificate: PathBuf,
    pub limits: PoolLimits,
}

/// A zeroizing owner for a host-resolved password. SQLx may retain its own
/// driver-managed copy after connection options are constructed.
pub struct SecretPassword(Zeroizing<String>);

impl SecretPassword {
    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for SecretPassword {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

impl From<Zeroizing<String>> for SecretPassword {
    fn from(value: Zeroizing<String>) -> Self {
        Self(value)
    }
}

impl fmt::Debug for SecretPassword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretPassword([redacted])")
    }
}

impl fmt::Debug for ConnectionSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConnectionSettings([redacted])")
    }
}

/// Every operation has a finite wait; the host supplies validated limits.
#[derive(Clone, Copy, Debug)]
pub struct PoolLimits {
    pub connect_timeout: Duration,
    pub acquire_timeout: Duration,
    pub max_connections: u32,
}

/// Safe to show to a caller; no SQLx or driver error text is exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PoolError {
    InvalidSettings,
    Timeout,
    Authentication,
    Tls,
    Unavailable,
    Closed,
    UnsupportedServer,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL pool error: {self:?}")
    }
}

impl std::error::Error for PoolError {}

fn classify(error: &sqlx::Error) -> PoolError {
    match error {
        sqlx::Error::PoolTimedOut => PoolError::Timeout,
        sqlx::Error::PoolClosed => PoolError::Closed,
        sqlx::Error::Database(error) if error.code().as_deref() == Some("28P01") => {
            PoolError::Authentication
        }
        sqlx::Error::Tls(_) => PoolError::Tls,
        sqlx::Error::Io(error) if matches!(error.get_ref(), Some(source) if source.is::<rustls::Error>()) => {
            PoolError::Tls
        }
        _ => PoolError::Unavailable,
    }
}

fn require_qualified_server_version(version: &str) -> Result<(), PoolError> {
    if version == QUALIFIED_SERVER_VERSION_NUM {
        Ok(())
    } else {
        Err(PoolError::UnsupportedServer)
    }
}

#[derive(Clone, PartialEq, Eq)]
struct ConnectionTarget {
    host: String,
    port: u16,
    database: String,
    ca: PathBuf,
}
impl ConnectionTarget {
    fn from_settings(settings: &ConnectionSettings) -> Self {
        Self {
            host: settings.host.clone(),
            port: settings.port,
            database: settings.database.clone(),
            ca: settings.ca_certificate.clone(),
        }
    }
}
/// A standalone pool that cannot mutate SQLite or Codex storage selection.
pub struct PostgresPool {
    pool: PgPool,
    original_target: Option<ConnectionTarget>,
    acquire_timeout: Duration,
    selected_search_path: Option<String>,
    activation_binding: std::sync::OnceLock<Arc<session_binding::ActivationBinding>>,
    runtime_causes: Option<Arc<migration_causes::MigrationCauses>>,
}

enum ConnectedPool {
    Legacy(PostgresPool),
    Retained(Arc<PostgresPool>),
}

impl PostgresPool {
    pub async fn connect(settings: ConnectionSettings) -> Result<Self, PoolError> {
        Self::connect_with_search_path(settings, None, None).await
    }

    /// Connect so that unqualified table names resolve inside one namespace.
    ///
    /// `None` selects the default `codex_storage` schema. Stores that write unqualified SQL use
    /// this so one set of statements serves the default and every named namespace.
    pub async fn connect_in_namespace(
        settings: ConnectionSettings,
        namespace: Option<&NamedNamespace>,
    ) -> Result<Self, PoolError> {
        Self::connect_with_search_path(settings, Some(owned_namespace_search_path(namespace)), None)
            .await
    }

    /// Separately credentialed migration pool. Authenticates the genuine derived
    /// migrator login and selects its owner role on every physical connection.
    /// It does not bootstrap, change grants, select a backend, or grant quiescence.
    pub async fn connect_migration_in_namespace(
        settings: ConnectionSettings,
        namespace: Option<&NamedNamespace>,
    ) -> Result<Self, PoolError> {
        let login = migration_login::MigrationLogin::new(namespace);
        if !login.accepts_login(&settings.username) {
            return Err(PoolError::InvalidSettings);
        }
        Self::connect_with_search_path(
            settings,
            Some(owned_namespace_search_path(namespace)),
            Some(login),
        )
        .await
    }

    /// Retain the actual original runtime SDK pool before first physical acquisition.
    /// The callback must retain it outside this future. This grants no activation,
    /// native-home, physical-socket, or mutation authority.
    pub async fn connect_retained_in_namespace(
        settings: ConnectionSettings,
        namespace: Option<&NamedNamespace>,
        capture: &mut (dyn FnMut(Arc<Self>) -> Result<(), PoolError> + Send),
    ) -> Result<Arc<Self>, PoolError> {
        Self::connect_retained_with_search_path(
            settings,
            Some(owned_namespace_search_path(namespace)),
            None,
            capture,
        )
        .await
    }

    async fn connect_with_search_path(
        settings: ConnectionSettings,
        search_path: Option<String>,
        migration_login: Option<migration_login::MigrationLogin>,
    ) -> Result<Self, PoolError> {
        match Self::connect_factory(settings, search_path, migration_login, None).await? {
            ConnectedPool::Legacy(pool) => Ok(pool),
            ConnectedPool::Retained(_) => Err(PoolError::Unavailable),
        }
    }

    // Actual SDK pool is captured BEFORE any physical acquire/after_connect Role SQL.
    async fn connect_retained_with_search_path(
        settings: ConnectionSettings,
        search_path: Option<String>,
        migration_login: Option<migration_login::MigrationLogin>,
        capture: &mut (dyn FnMut(Arc<Self>) -> Result<(), PoolError> + Send),
    ) -> Result<Arc<Self>, PoolError> {
        match Self::connect_factory(settings, search_path, migration_login, Some(capture)).await? {
            ConnectedPool::Retained(pool) => Ok(pool),
            ConnectedPool::Legacy(_) => Err(PoolError::Unavailable),
        }
    }

    async fn connect_factory(
        settings: ConnectionSettings,
        search_path: Option<String>,
        migration_login: Option<migration_login::MigrationLogin>,
        capture: Option<&mut (dyn FnMut(Arc<Self>) -> Result<(), PoolError> + Send)>,
    ) -> Result<ConnectedPool, PoolError> {
        let original_target = Some(ConnectionTarget::from_settings(&settings));
        let selected_search_path = search_path.clone();
        let limits = settings.limits;
        let valid_host = settings.host.parse::<IpAddr>().is_ok()
            || (settings.host.len() <= 253
                && settings.host.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && label
                            .as_bytes()
                            .first()
                            .is_some_and(u8::is_ascii_alphanumeric)
                        && label
                            .as_bytes()
                            .last()
                            .is_some_and(u8::is_ascii_alphanumeric)
                        && label
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                }));
        if !valid_host
            || settings.database.is_empty()
            || settings.username.is_empty()
            || settings.password.expose().is_empty()
            || !settings.ca_certificate.is_absolute()
            || settings.port == 0
            || limits.max_connections == 0
            || limits.max_connections > MAX_CONNECTIONS
            || limits.connect_timeout.is_zero()
            || limits.acquire_timeout.is_zero()
            || limits.connect_timeout > MAX_WAIT
            || limits.acquire_timeout > MAX_WAIT
        {
            return Err(PoolError::InvalidSettings);
        }
        let options = PgConnectOptions::new()
            .host(&settings.host)
            .port(settings.port)
            .database(&settings.database)
            .username(&settings.username)
            .password(settings.password.expose())
            .ssl_mode(PgSslMode::VerifyFull)
            .ssl_root_cert(&settings.ca_certificate)
            .disable_statement_logging();
        let rejected_version = Arc::new(AtomicBool::new(false));
        // Reuse the bounded original-cause storage primitive, not migration-login
        // credentials/role policy. Captured original pool owns this ledger.
        let runtime_causes = (capture.is_some() && migration_login.is_none())
            .then(migration_causes::MigrationCauses::new);
        let runtime_driver_reservation = runtime_causes
            .as_ref()
            .map(|causes| causes.reserve())
            .transpose()
            .map_err(|_| PoolError::Unavailable)?;
        let driver_reservation = migration_login
            .as_ref()
            .map(|login| login.reserve_driver())
            .transpose()
            .map_err(|_| PoolError::Unavailable)?;
        let configured = PgPoolOptions::new()
            .max_connections(limits.max_connections)
            // SQLx uses this limit during startup too. The outer timeouts
            // enforce each operation's own deadline.
            .acquire_timeout(limits.connect_timeout.max(limits.acquire_timeout))
            .after_connect({
                let rejected_version = Arc::clone(&rejected_version);
                let runtime_causes = runtime_causes.clone();
                move |connection, _| {
                    let rejected_version = Arc::clone(&rejected_version);
                    let search_path = search_path.clone();
                    let migration_login = migration_login.clone();
                    let runtime_causes = runtime_causes.clone();
                    Box::pin(async move {
                        let original_runtime_attempt = runtime_causes
                            .as_ref()
                            .map(|causes| causes.reserve())
                            .transpose()?;
                        let result = async {
                            if let Some(login) = migration_login {
                                login.initialize(&mut *connection).await?;
                            }
                            if let Some(search_path) = search_path {
                                sqlx::query("SELECT set_config('search_path', $1, false)")
                                    .bind(search_path)
                                    .execute(&mut *connection)
                                    .await
                                    .map_err(|error| {
                                        if runtime_causes.is_some() {
                                            error
                                        } else {
                                            sqlx::Error::Configuration(Box::new(classify(&error)))
                                        }
                                    })?;
                            }
                            let version =
                                sqlx::query_scalar::<_, String>("SHOW server_version_num")
                                    .fetch_one(connection)
                                    .await
                                    .map_err(|error| {
                                        if runtime_causes.is_some() {
                                            error
                                        } else {
                                            sqlx::Error::Configuration(Box::new(classify(&error)))
                                        }
                                    })?;
                            require_qualified_server_version(&version).map_err(|error| {
                                rejected_version.store(true, Ordering::Relaxed);
                                sqlx::Error::Configuration(Box::new(error))
                            })
                        }
                        .await;
                        match (original_runtime_attempt, result) {
                            (Some(reservation), Ok(())) => {
                                reservation.success();
                                Ok(())
                            }
                            (Some(reservation), Err(original)) => {
                                Err(reservation.failure(original))
                            }
                            (None, result) => result,
                        }
                    })
                }
            });
        let mut retained = None;
        let connected = if let Some(capture) = capture {
            let original = Arc::new(Self {
                pool: configured.min_connections(0).connect_lazy_with(options),
                original_target: original_target.clone(),
                acquire_timeout: limits.acquire_timeout,
                selected_search_path: selected_search_path.clone(),
                activation_binding: std::sync::OnceLock::new(),
                runtime_causes: runtime_causes.clone(),
            });
            capture(Arc::clone(&original))?;
            // Capture is synchronous; the first physical acquire cannot outrun it.
            let result = timeout(limits.connect_timeout, original.pool.acquire())
                .await
                .map(|result| {
                    result.map(|connection| {
                        drop(connection);
                        original.pool.clone()
                    })
                });
            retained = Some(original);
            result
        } else {
            // Preserve existing unretained/default construction semantics exactly.
            timeout(limits.connect_timeout, configured.connect_with(options)).await
        };
        let pool = connected
            .map_err(|_| PoolError::Timeout)
            .and_then(|result| match result {
                Ok(pool) => {
                    if let Some(reservation) = driver_reservation {
                        reservation.success();
                    }
                    if let Some(reservation) = runtime_driver_reservation {
                        reservation.success();
                    }
                    Ok(pool)
                }
                Err(original) => {
                    let classified = classify(&original);
                    if let Some(reservation) = runtime_driver_reservation {
                        let _ = reservation.failure(original);
                    } else if let Some(reservation) = driver_reservation {
                        let _ = reservation.failure(original);
                    }
                    Err(classified)
                }
            })
            .map_err(|error| {
                // SQLx discards and retries failed after_connect checks. Preserve the
                // initial version rejection when no qualified connection was found.
                if rejected_version.load(Ordering::Relaxed) {
                    PoolError::UnsupportedServer
                } else {
                    error
                }
            })?;
        if let Some(original) = retained {
            Ok(ConnectedPool::Retained(original))
        } else {
            Ok(ConnectedPool::Legacy(Self {
                pool,
                original_target,
                acquire_timeout: limits.acquire_timeout,
                selected_search_path,
                activation_binding: std::sync::OnceLock::new(),
                runtime_causes: runtime_causes.clone(),
            }))
        }
    }

    /// Reserve before polling the original query; retain its driver cause before redaction.
    /// Dropping this future preserves interrupted evidence, never a terminal acknowledgment.
    pub(crate) async fn retain_runtime_query<T>(
        &self,
        query: impl std::future::Future<Output = Result<T, sqlx::Error>>,
    ) -> Result<T, sqlx::Error> {
        let reservation = self
            .runtime_causes
            .as_ref()
            .map(|owner| owner.reserve())
            .transpose()?;
        match query.await {
            Ok(value) => {
                if let Some(reservation) = reservation {
                    reservation.success();
                }
                Ok(value)
            }
            Err(original) => Err(match reservation {
                Some(reservation) => reservation.failure(original),
                None => original,
            }),
        }
    }

    /// Original retained Runtime causes only; no socket/transaction outcome authority.
    pub fn retains_original_runtime_causes(&self) -> Result<bool, PoolError> {
        self.runtime_causes
            .as_ref()
            .map(|causes| causes.retained_count())
            .transpose()
            .map(|count| count.is_some_and(|count| count != 0))
            .map_err(|_| PoolError::Unavailable)
    }

    pub async fn health(&self) -> Result<(), PoolError> {
        let mut connection = self.acquire().await?;
        timeout(
            self.acquire_timeout,
            sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&mut *connection),
        )
        .await
        .map_err(|_| PoolError::Timeout)?
        .map_err(|error| classify(&error))?;
        Ok(())
    }

    /// Caller externally retains exact pool/task before polling this actual SDK close.
    /// Completion is not Native/Home/all-writer authority.
    pub async fn close_original_sdk(&self) {
        self.pool.close().await;
    }

    pub async fn close(&self) -> Result<(), PoolError> {
        timeout(self.acquire_timeout, self.pool.close())
            .await
            .map_err(|_| PoolError::Timeout)
    }

    pub async fn acquire(&self) -> Result<PoolConnection<Postgres>, PoolError> {
        let reservation = self
            .runtime_causes
            .as_ref()
            .map(|owner| owner.reserve())
            .transpose()
            .map_err(|_| PoolError::Unavailable)?;
        match timeout(self.acquire_timeout, self.pool.acquire()).await {
            Ok(Ok(connection)) => {
                if let Some(reservation) = reservation {
                    reservation.success();
                }
                Ok(connection)
            }
            Ok(Err(original)) => {
                let projected = classify(&original);
                if let Some(reservation) = reservation {
                    let _ = reservation.failure(original);
                }
                Err(projected)
            }
            Err(_) => Err(PoolError::Timeout), // Reservation Drop keeps interrupted evidence.
        }
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

#[cfg(all(test, unix))]
mod exclusive_fixture;

#[cfg(all(test, unix))]
mod exclusive_fixture_io;

#[cfg(all(test, unix))]
mod exclusive_fixture_settings;

pub use namespace::owned_namespace_search_path;
