//! The check an ordinary client makes with its runtime login before it reads or writes.
//!
//! Compatibility preflight needs migrator credentials. A running client has only the runtime
//! login, so it verifies the one fact it can read: the stored schema format is exactly the one
//! this build writes. Anything older must be migrated by the host, anything newer needs a newer
//! client, and neither is repaired here.

use crate::PostgresPool;
use crate::schema_registry::MIGRATION_SHAPES;
use sqlx::Row;
use std::fmt;
use std::time::Duration;
use tokio::time::timeout;

const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a client cannot use the store it connected to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeSchemaError {
    /// The metadata could not be read; the failure may be transient.
    Unavailable,
    /// The store was created by an older release and must be upgraded by the host first.
    NeedsUpgrade,
    /// The store requires a newer client than this build.
    ClientTooOld,
    /// The metadata is missing or malformed.
    Invalid,
}

impl fmt::Display for RuntimeSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL runtime schema check: {self:?}")
    }
}

impl std::error::Error for RuntimeSchemaError {}

/// The schema format this build reads and writes.
pub fn client_schema_format() -> i32 {
    MIGRATION_SHAPES
        .last()
        .and_then(|shape| i32::try_from(shape.version).ok())
        .unwrap_or(0)
}

/// Verify the connected namespace holds exactly the schema format this client writes.
///
/// The pool must have been connected with [`PostgresPool::connect_in_namespace`] so the
/// metadata table resolves inside the intended namespace.
pub async fn check_runtime_schema(pool: &PostgresPool) -> Result<i32, RuntimeSchemaError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|_| RuntimeSchemaError::Unavailable)?;
    let row = timeout(
        CHECK_TIMEOUT,
        pool.retain_runtime_query(
            sqlx::query(
                "SELECT format_version, min_reader_version, min_writer_version \
             FROM codex_schema_meta WHERE singleton = TRUE",
            )
            .fetch_optional(&mut *connection),
        ),
    )
    .await
    .map_err(|_| RuntimeSchemaError::Unavailable)?
    .map_err(|_| RuntimeSchemaError::Unavailable)?
    .ok_or(RuntimeSchemaError::Invalid)?;
    let format: i32 = pool
        .retain_runtime_query(async { row.try_get("format_version") })
        .await
        .map_err(|_| RuntimeSchemaError::Invalid)?;
    let min_reader: i32 = pool
        .retain_runtime_query(async { row.try_get("min_reader_version") })
        .await
        .map_err(|_| RuntimeSchemaError::Invalid)?;
    let min_writer: i32 = pool
        .retain_runtime_query(async { row.try_get("min_writer_version") })
        .await
        .map_err(|_| RuntimeSchemaError::Invalid)?;
    let supported = client_schema_format();
    if format <= 0 || min_reader <= 0 || min_writer <= 0 {
        return Err(RuntimeSchemaError::Invalid);
    }
    if min_reader > supported || min_writer > supported || format > supported {
        return Err(RuntimeSchemaError::ClientTooOld);
    }
    if format < supported {
        return Err(RuntimeSchemaError::NeedsUpgrade);
    }
    Ok(format)
}
