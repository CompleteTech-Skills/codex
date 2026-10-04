//! Explicit administrative input. Parsing never grants server authority.

use crate::CredentialSource;
use crate::RemotePostgresProfile;
use crate::valid_pg_identifier;
use std::fmt;
use std::sync::Arc;

/// Host-selected administrative credentials for the SAME original target profile.
/// There is deliberately no Runtime/Migrator fallback or serialized project input.
pub struct RemoteAdministrationInput {
    target: Arc<RemotePostgresProfile>,
    login: String,
    credential: CredentialSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidAdministrativeInput {
    InvalidLogin,
    InvalidTarget,
    MissingExplicitCa,
}

impl RemoteAdministrationInput {
    pub fn new(
        target: Arc<RemotePostgresProfile>,
        login: String,
        credential: CredentialSource,
    ) -> Result<Self, InvalidAdministrativeInput> {
        if !valid_pg_identifier(&login) {
            return Err(InvalidAdministrativeInput::InvalidLogin);
        }
        target
            .validate()
            .map_err(|_| InvalidAdministrativeInput::InvalidTarget)?;
        if target.ca_certificate().is_none() {
            return Err(InvalidAdministrativeInput::MissingExplicitCa);
        }
        Ok(Self {
            target,
            login,
            credential,
        })
    }

    pub fn target(&self) -> &Arc<RemotePostgresProfile> {
        &self.target
    }

    pub fn login(&self) -> &str {
        &self.login
    }

    pub fn credential(&self) -> &CredentialSource {
        &self.credential
    }

    /// Pointer association only; no endpoint equality or native server proof.
    pub fn belongs_to_target(&self, target: &Arc<RemotePostgresProfile>) -> bool {
        Arc::ptr_eq(&self.target, target)
    }
}

impl fmt::Debug for RemoteAdministrationInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RemoteAdministrationInput([redacted])")
    }
}
