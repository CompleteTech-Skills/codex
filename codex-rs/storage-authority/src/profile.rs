//! Candidate connection settings; parsing one never selects a storage backend.

use super::CredentialRef;
use super::valid_pg_identifier;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::de::Error as _;
use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;

const MAX_TIMEOUT_SECONDS: u16 = 30;
const MAX_POOL_CONNECTIONS: u16 = 32;

/// A saved proposal, separate from the host-owned active backend identity.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageCandidateProfile {
    LocalSqlite,
    RemotePostgres(RemotePostgresProfile),
}

/// Managed constraint on a proposal's destination, excluding its credential.
#[derive(Clone, Eq, PartialEq, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageDestination {
    LocalSqlite,
    RemotePostgres {
        endpoint: String,
        port: u16,
        database: String,
        namespace: String,
        #[serde(default)]
        tls: TlsSettings,
    },
}

impl fmt::Debug for StorageDestination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalSqlite => f.write_str("LocalSqlite"),
            Self::RemotePostgres { .. } => f.write_str("RemotePostgres([redacted])"),
        }
    }
}

impl StorageDestination {
    pub fn validate(&self) -> Result<(), ProfileError> {
        if let Self::RemotePostgres {
            endpoint,
            port,
            database,
            namespace,
            tls,
        } = self
            && (!valid_endpoint(endpoint)
                || *port == 0
                || !valid_pg_identifier(database)
                || !valid_pg_identifier(namespace)
                || tls
                    .ca_certificate
                    .as_ref()
                    .is_some_and(|path| !path.is_absolute()))
        {
            return Err(ProfileError::InvalidDestination);
        }
        Ok(())
    }
}

impl StorageCandidateProfile {
    pub fn destination(&self) -> StorageDestination {
        match self {
            Self::LocalSqlite => StorageDestination::LocalSqlite,
            Self::RemotePostgres(profile) => StorageDestination::RemotePostgres {
                endpoint: profile.endpoint.clone(),
                port: profile.port,
                database: profile.database.clone(),
                namespace: profile.namespace.clone(),
                tls: profile.tls.clone(),
            },
        }
    }
}

impl fmt::Debug for StorageCandidateProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalSqlite => f.write_str("LocalSqlite"),
            Self::RemotePostgres(_) => f.write_str("RemotePostgres([redacted])"),
        }
    }
}

/// A host-owned reference to a protected credential, never its value.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum CredentialSource {
    Environment { variable: EnvironmentVariableName },
    Keyring { id: CredentialRef },
}

impl fmt::Debug for CredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Environment { .. } => f.write_str("Environment([redacted])"),
            Self::Keyring { .. } => f.write_str("Keyring([redacted])"),
        }
    }
}

/// An environment variable name selected by the owning host.
#[derive(Clone, Eq, PartialEq, schemars::JsonSchema)]
#[schemars(transparent)]
pub struct EnvironmentVariableName(String);

impl EnvironmentVariableName {
    pub fn parse(value: String) -> Result<Self, ProfileError> {
        let mut bytes = value.bytes();
        let valid_first = bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_uppercase() || byte == b'_');
        if !valid_first
            || value.len() > 128
            || !bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(ProfileError::InvalidCredentialReference);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EnvironmentVariableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EnvironmentVariableName([redacted])")
    }
}

impl Serialize for EnvironmentVariableName {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for EnvironmentVariableName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::parse(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl Serialize for CredentialRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CredentialRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::parse(String::deserialize(deserializer)?)
            .map_err(|_| D::Error::custom(ProfileError::InvalidCredentialReference))
    }
}

/// The only TLS policy admitted by this initial candidate format.
#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TlsVerification {
    #[default]
    VerifyFull,
}

/// Verify the certificate and server name, using system roots or a host path.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TlsSettings {
    #[serde(default)]
    pub verification: TlsVerification,
    pub ca_certificate: Option<PathBuf>,
}

impl Default for TlsSettings {
    fn default() -> Self {
        Self {
            verification: TlsVerification::VerifyFull,
            ca_certificate: None,
        }
    }
}

impl fmt::Debug for TlsSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TlsSettings(verify_full, [redacted])")
    }
}

/// PostgreSQL connection candidate. Deserialization validates structure only;
/// it does not resolve credentials, open a socket, or claim authority.
#[derive(Clone, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RemotePostgresProfile {
    endpoint: String,
    #[schemars(range(min = 1))]
    port: u16,
    database: String,
    namespace: String,
    credential: CredentialSource,
    #[serde(default)]
    tls: TlsSettings,
    #[schemars(range(min = 1, max = 30))]
    connect_timeout_seconds: u16,
    #[schemars(range(min = 1, max = 30))]
    pool_acquire_timeout_seconds: u16,
    #[schemars(range(min = 1, max = 32))]
    max_connections: u16,
}

impl fmt::Debug for RemotePostgresProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RemotePostgresProfile([redacted])")
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRemotePostgresProfile {
    endpoint: String,
    port: u16,
    database: String,
    namespace: String,
    credential: CredentialSource,
    #[serde(default)]
    tls: TlsSettings,
    connect_timeout_seconds: u16,
    pool_acquire_timeout_seconds: u16,
    max_connections: u16,
}

impl<'de> Deserialize<'de> for RemotePostgresProfile {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RawRemotePostgresProfile::deserialize(deserializer)?;
        let profile = Self {
            endpoint: raw.endpoint,
            port: raw.port,
            database: raw.database,
            namespace: raw.namespace,
            credential: raw.credential,
            tls: raw.tls,
            connect_timeout_seconds: raw.connect_timeout_seconds,
            pool_acquire_timeout_seconds: raw.pool_acquire_timeout_seconds,
            max_connections: raw.max_connections,
        };
        profile.validate().map_err(D::Error::custom)?;
        Ok(profile)
    }
}

impl RemotePostgresProfile {
    /// Rejects malformed proposals without connecting or reading a credential.
    pub fn validate(&self) -> Result<(), ProfileError> {
        if !valid_endpoint(&self.endpoint) {
            return Err(ProfileError::InvalidEndpoint);
        }
        if self.port == 0 {
            return Err(ProfileError::InvalidPort);
        }
        if !valid_pg_identifier(&self.database) || !valid_pg_identifier(&self.namespace) {
            return Err(ProfileError::InvalidIdentifier);
        }
        if self
            .tls
            .ca_certificate
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(ProfileError::InvalidTlsSettings);
        }
        if !(1..=MAX_TIMEOUT_SECONDS).contains(&self.connect_timeout_seconds)
            || !(1..=MAX_TIMEOUT_SECONDS).contains(&self.pool_acquire_timeout_seconds)
            || !(1..=MAX_POOL_CONNECTIONS).contains(&self.max_connections)
        {
            return Err(ProfileError::InvalidConnectionLimits);
        }
        Ok(())
    }
}

fn valid_endpoint(value: &str) -> bool {
    if value.parse::<IpAddr>().is_ok() {
        return true;
    }
    if value.split('.').count() == 4
        && value
            .split('.')
            .all(|label| !label.is_empty() && label.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return false;
    }
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
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
        })
}

/// Redacted structural validation result. Input values never enter diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileError {
    InvalidDestination,
    InvalidCredentialReference,
    InvalidEndpoint,
    InvalidPort,
    InvalidIdentifier,
    InvalidTlsSettings,
    InvalidConnectionLimits,
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::InvalidDestination => "invalid managed storage destination",
            Self::InvalidCredentialReference => "invalid credential reference",
            Self::InvalidEndpoint => "invalid PostgreSQL endpoint",
            Self::InvalidPort => "invalid PostgreSQL port",
            Self::InvalidIdentifier => "invalid PostgreSQL database or namespace",
            Self::InvalidTlsSettings => "invalid PostgreSQL TLS settings",
            Self::InvalidConnectionLimits => "invalid PostgreSQL connection limits",
        };
        f.write_str(reason)
    }
}

impl std::error::Error for ProfileError {}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
