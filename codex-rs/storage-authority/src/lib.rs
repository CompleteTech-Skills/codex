//! Host-owned storage identity and candidate validation primitives.
//!
//! This crate deliberately does not select a runtime backend. Callers must fence
//! existing writers before adopting a populated legacy home. This initial API
//! only initializes a fresh, empty directory.

use serde::Deserialize;
use serde::Serialize;
#[cfg(unix)]
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use uuid::Uuid;

mod administrative_profile;
mod credential_resolution;
mod cutover;
pub use administrative_profile::InvalidAdministrativeInput;
pub use administrative_profile::RemoteAdministrationInput;
pub use credential_resolution::CredentialResolutionError;
pub use credential_resolution::HostCredentialResolver;
pub use credential_resolution::KEYRING_SERVICE;
pub use credential_resolution::ResolvedCredential;
pub use cutover::AuthorityState;
pub use cutover::CutoverIntent;
pub use cutover::abandon_cutover;
pub use cutover::attach_remote_dataset;
pub use cutover::authority_state;
pub use cutover::begin_cutover;
pub use cutover::complete_cutover;
pub use cutover::load_authority;
pub use cutover::read_cutover;

const IDENTITY_FILE: &str = "storage-identity.json";
const ACTIVATION_FILE: &str = "storage-activation.json";
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalIdentity {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub instance_id: Uuid,
    pub generation: u64,
    pub home_id: Uuid,
}

/// Which store currently holds the authoritative history.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActiveBackend {
    #[default]
    Local,
    Remote,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationMarker {
    pub format_version: u32,
    pub dataset_id: Uuid,
    pub instance_id: Uuid,
    pub home_id: Uuid,
    pub generation: u64,
    pub remote_ever_activated: bool,
    /// Records written before cutovers existed have no backend and are local.
    #[serde(default)]
    pub active_backend: ActiveBackend,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalAuthority {
    pub identity: LocalIdentity,
    pub marker: ActivationMarker,
}

/// A host-supplied reference to protected credential material, never a secret.
#[derive(Clone, Eq, PartialEq, schemars::JsonSchema)]
#[schemars(transparent)]
pub struct CredentialRef(String);

impl std::fmt::Debug for CredentialRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CredentialRef([redacted])")
    }
}

impl CredentialRef {
    pub fn parse(value: String) -> Result<Self, AuthorityError> {
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(AuthorityError::Blocked("invalid credential reference"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresCandidate {
    pub server_name: String,
    pub port: u16,
    pub database: String,
    pub namespace: String,
    pub credential: CredentialRef,
    pub ca_certificate: std::path::PathBuf,
}

impl PostgresCandidate {
    /// Validates a host-owned candidate. A successful result is not authority,
    /// connection success, credential availability or permission to migrate.
    pub fn validate(&self) -> Result<(), AuthorityError> {
        if self.server_name.is_empty()
            || self.server_name.len() > 253
            || self
                .server_name
                .bytes()
                .any(|byte| matches!(byte, b'/' | b'\\' | b'@' | b':' | b'?' | b'#'))
            || self.port == 0
            || !valid_pg_identifier(&self.database)
            || !valid_pg_identifier(&self.namespace)
            || !self.ca_certificate.is_absolute()
        {
            return Err(AuthorityError::Blocked("invalid PostgreSQL candidate"));
        }
        Ok(())
    }
}

fn valid_pg_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_lowercase() || (index > 0 && byte.is_ascii_digit())
        })
}

mod profile;
pub use profile::CredentialSource;
pub use profile::EnvironmentVariableName;
pub use profile::ProfileError;
pub use profile::RemotePostgresProfile;
pub use profile::StorageCandidateProfile;
pub use profile::TlsSettings;
pub use profile::TlsVerification;

pub enum AuthorityError {
    Io(io::Error),
    Blocked(&'static str),
}

impl std::fmt::Debug for AuthorityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for AuthorityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(_) => write!(f, "storage authority I/O error"),
            Self::Blocked(reason) => write!(f, "storage authority blocked: {reason}"),
        }
    }
}

impl std::error::Error for AuthorityError {}

impl From<io::Error> for AuthorityError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

/// Initializes only a caller-owned, previously empty directory. The caller must
/// prevent other processes from claiming this directory during initialization.
/// This is not an adoption path for an existing Codex home.
pub fn initialize_empty_home(home: &Path) -> Result<LocalAuthority, AuthorityError> {
    if std::fs::symlink_metadata(home)?.file_type().is_symlink() {
        return Err(AuthorityError::Blocked("home is a symlink"));
    }
    let entries = std::fs::read_dir(home)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    if entries.len() == 2
        && entries.contains(&IDENTITY_FILE.into())
        && entries.contains(&ACTIVATION_FILE.into())
    {
        return load_local_authority(home);
    }
    if !entries.is_empty() {
        return Err(AuthorityError::Blocked(
            "home requires fenced reconciliation",
        ));
    }

    create_records(home)
}

/// Give an existing, populated home its authority records.
///
/// The caller must have stopped every writer of the home before calling this; nothing here can
/// fence a running process. A home that already has complete records returns them unchanged, and
/// one with partial or interrupted records is refused.
pub fn adopt_quiesced_home(home: &Path) -> Result<LocalAuthority, AuthorityError> {
    if std::fs::symlink_metadata(home)?.file_type().is_symlink() {
        return Err(AuthorityError::Blocked("home is a symlink"));
    }
    let identity = home.join(IDENTITY_FILE).exists();
    let marker = home.join(ACTIVATION_FILE).exists();
    match (identity, marker) {
        (true, true) => load_authority(home),
        (false, false) => {
            if cutover::read_cutover(home)?.is_some() {
                return Err(AuthorityError::Blocked("cutover in progress"));
            }
            create_records(home)
        }
        _ => Err(AuthorityError::Blocked(
            "home requires fenced reconciliation",
        )),
    }
}

fn create_records(home: &Path) -> Result<LocalAuthority, AuthorityError> {
    let identity = LocalIdentity {
        format_version: FORMAT_VERSION,
        dataset_id: Uuid::new_v4(),
        instance_id: Uuid::new_v4(),
        generation: 1,
        home_id: Uuid::new_v4(),
    };
    let marker = initial_marker(&identity);
    write_new(&home.join(IDENTITY_FILE), &identity)?;
    write_new(&home.join(ACTIVATION_FILE), &marker)?;
    sync_dir(home)?;
    Ok(LocalAuthority { identity, marker })
}

fn initial_marker(identity: &LocalIdentity) -> ActivationMarker {
    ActivationMarker {
        format_version: FORMAT_VERSION,
        dataset_id: identity.dataset_id,
        instance_id: identity.instance_id,
        home_id: identity.home_id,
        generation: identity.generation,
        remote_ever_activated: false,
        active_backend: ActiveBackend::Local,
    }
}

/// Reads both independent host-owned records and refuses missing, inconsistent,
/// future-version or previously remote-authoritative state.
pub fn load_local_authority(home: &Path) -> Result<LocalAuthority, AuthorityError> {
    if std::fs::symlink_metadata(home)?.file_type().is_symlink() {
        return Err(AuthorityError::Blocked("home is a symlink"));
    }
    if cutover::read_cutover(home)?.is_some() {
        return Err(AuthorityError::Blocked("cutover in progress"));
    }
    let identity: LocalIdentity = read_record(&home.join(IDENTITY_FILE))?;
    let marker: ActivationMarker = read_record(&home.join(ACTIVATION_FILE))?;
    if identity.format_version != FORMAT_VERSION || marker.format_version != FORMAT_VERSION {
        return Err(AuthorityError::Blocked(
            "unsupported authority record version",
        ));
    }
    if identity.dataset_id != marker.dataset_id
        || identity.instance_id != marker.instance_id
        || identity.home_id != marker.home_id
        || identity.generation != marker.generation
    {
        return Err(AuthorityError::Blocked("authority records disagree"));
    }
    if !(1..=i64::MAX as u64).contains(&identity.generation) {
        return Err(AuthorityError::Blocked("invalid authority generation"));
    }
    if marker.remote_ever_activated {
        return Err(AuthorityError::Blocked(
            "remote authority requires reconciliation",
        ));
    }
    Ok(LocalAuthority { identity, marker })
}

fn write_new<T: Serialize>(path: &Path, value: &T) -> Result<(), AuthorityError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    serde_json::to_writer(&mut file, value)
        .map_err(|_| AuthorityError::Blocked("authority record encoding failed"))?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn read_record<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, AuthorityError> {
    if !std::fs::symlink_metadata(path)
        .map_err(|_| AuthorityError::Blocked("authority record missing"))?
        .file_type()
        .is_file()
    {
        return Err(AuthorityError::Blocked("authority record is not regular"));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    // Validate the opened handle too: the path may change after metadata lookup.
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AuthorityError::Blocked("authority record is not regular"));
    }
    let mut bytes = Vec::new();
    file.take(4097).read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err(AuthorityError::Blocked("authority record too large"));
    }
    serde_json::from_slice(&bytes).map_err(|_| AuthorityError::Blocked("authority record invalid"))
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
