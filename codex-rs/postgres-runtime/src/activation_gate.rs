//! The write gate that keeps every store out of a namespace while a migration owns it.
//!
//! Each writing transaction calls [`require_storage_open`] first. The check takes a shared lock
//! on the activation row, so a migration that wants the row exclusively waits for in-flight
//! writers to commit, and writers that begin afterwards see the migration and stop.

use sqlx::PgConnection;
use std::fmt;

/// Why a write could not start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationGateError {
    /// A migration holds the store; writes resume when it finishes or is abandoned.
    Migrating,
    /// The dataset was handed back to local storage and no longer accepts writes.
    Retired,
    /// The namespace now contains another dataset or activation generation.
    GenerationChanged,
    /// The activation state could not be read, so the write cannot be proven safe.
    Unavailable,
}

impl fmt::Display for ActivationGateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Migrating => {
                formatter.write_str("storage is being migrated and cannot accept writes")
            }
            Self::Retired => formatter
                .write_str("storage was handed back to local storage and no longer accepts writes"),
            Self::GenerationChanged => {
                formatter.write_str("the connected storage identity changed")
            }
            Self::Unavailable => {
                formatter.write_str("the storage activation state could not be read")
            }
        }
    }
}

impl std::error::Error for ActivationGateError {}

/// Refuse to continue the current transaction while a migration holds the store.
///
/// Call this inside the transaction that performs the write, before the first data change.
pub async fn require_storage_open(
    connection: &mut PgConnection,
) -> Result<(), ActivationGateError> {
    let state: String =
        sqlx::query_scalar("SELECT state FROM storage_activation WHERE singleton FOR SHARE")
            .fetch_one(connection)
            .await
            .map_err(|_| ActivationGateError::Unavailable)?;
    match state.as_str() {
        "open" => Ok(()),
        "migrating" => Err(ActivationGateError::Migrating),
        "retired" => Err(ActivationGateError::Retired),
        _ => Err(ActivationGateError::Unavailable),
    }
}
