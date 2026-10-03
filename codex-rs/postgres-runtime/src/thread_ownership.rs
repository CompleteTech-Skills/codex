//! Inactive per-thread lease records for cooperating clients. No runtime write
//! path consumes these yet, and runtime credentials can directly modify rows.
//!
//! A future writer must validate ownership while holding the ownership row lock
//! in the same transaction as its data changes. An earlier `observe` call is not
//! a server-enforced fence. Old clients and local rollout files are outside
//! this primitive. Privileged enforcement belongs to a later stage.

use crate::NamedNamespace;
use crate::PostgresPool;
use crate::TransactionError;
use crate::session_binding::ActivationBinding;
use sqlx::AssertSqlSafe;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

const MAX_LEASE: Duration = Duration::from_secs(30);

/// The exact preprovisioned namespace containing a thread ownership row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ThreadOwnershipNamespace {
    Default,
    Named(NamedNamespace),
}

impl ThreadOwnershipNamespace {
    fn table(&self) -> String {
        let schema = match self {
            Self::Default => "\"codex_storage\"".to_owned(),
            Self::Named(namespace) => namespace.quoted_schema(),
        };
        format!("{schema}.\"thread_writer_ownership\"")
    }
}

/// A durable claim returned after its PostgreSQL transaction commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreadOwnership {
    pub namespace: ThreadOwnershipNamespace,
    pub thread_id: String,
    pub owner_id: String,
    pub token: i64,
    // Only an actual operation on the original bound pool can construct this field.
    original_binding: Option<Arc<ActivationBinding>>,
}

/// A fresh actual database-generated attempt retained on its original bound pool.
/// Fields are private: scalar owner IDs cannot reconstruct mutation authority.
pub struct ThreadOwnershipAttempt {
    namespace: ThreadOwnershipNamespace,
    thread_id: String,
    owner_id: String,
    original_binding: Arc<ActivationBinding>,
}

/// Redacted ownership error. An unknown commit outcome requires readback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadOwnershipError {
    InvalidLease,
    Transaction(TransactionError),
}

impl fmt::Display for ThreadOwnershipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL thread ownership error: {self:?}")
    }
}

impl std::error::Error for ThreadOwnershipError {}

impl From<TransactionError> for ThreadOwnershipError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

fn lease_millis(duration: Duration) -> Result<i64, ThreadOwnershipError> {
    if duration < Duration::from_millis(1) || duration > MAX_LEASE {
        return Err(ThreadOwnershipError::InvalidLease);
    }
    Ok(duration.as_millis() as i64)
}

impl PostgresPool {
    fn lease_binding(
        &self,
        namespace: &ThreadOwnershipNamespace,
    ) -> Result<Arc<ActivationBinding>, ThreadOwnershipError> {
        let namespace = match namespace {
            ThreadOwnershipNamespace::Default => None,
            ThreadOwnershipNamespace::Named(namespace) => Some(namespace),
        };
        if self.selected_search_path.as_deref()
            != Some(crate::owned_namespace_search_path(namespace).as_str())
        {
            return Err(TransactionError::Rejected.into());
        }
        self.activation_binding
            .get()
            .cloned()
            .ok_or_else(|| TransactionError::Rejected.into())
    }

    fn check_lease_binding(&self, claim: &ThreadOwnership) -> Result<(), ThreadOwnershipError> {
        let original = self.lease_binding(&claim.namespace)?;
        if claim
            .original_binding
            .as_ref()
            .is_some_and(|binding| Arc::ptr_eq(&original, binding))
        {
            Ok(())
        } else {
            Err(TransactionError::Rejected.into())
        }
    }

    /// Prepare before claim dispatch so an ambiguous outcome keeps the same original attempt.
    pub async fn prepare_thread_ownership_attempt(
        &self,
        namespace: ThreadOwnershipNamespace,
        thread_id: &str,
    ) -> Result<ThreadOwnershipAttempt, ThreadOwnershipError> {
        let original_binding = self.lease_binding(&namespace)?;
        let mut connection = self
            .acquire()
            .await
            .map_err(|_| TransactionError::Unavailable)?;
        let owner_id: String = timeout(
            self.acquire_timeout,
            sqlx::query_scalar("SELECT pg_catalog.gen_random_uuid()::text")
                .fetch_one(&mut *connection),
        )
        .await
        .map_err(|_| TransactionError::Timeout)?
        .map_err(|error| TransactionError::classify_statement(&error))?;
        Ok(ThreadOwnershipAttempt {
            namespace,
            thread_id: thread_id.to_owned(),
            owner_id,
            original_binding,
        })
    }

    pub async fn claim_thread_ownership_attempt(
        &self,
        attempt: &ThreadOwnershipAttempt,
        lease: Duration,
    ) -> Result<Option<ThreadOwnership>, ThreadOwnershipError> {
        let binding = self.lease_binding(&attempt.namespace)?;
        if !Arc::ptr_eq(&binding, &attempt.original_binding) {
            return Err(TransactionError::Rejected.into());
        }
        self.claim_thread_ownership(
            attempt.namespace.clone(),
            &attempt.thread_id,
            &attempt.owner_id,
            lease,
        )
        .await
    }

    /// Recover only an opaque original attempt, with the same generation gate and lock.
    pub async fn recover_thread_ownership_attempt(
        &self,
        attempt: &ThreadOwnershipAttempt,
    ) -> Result<Option<ThreadOwnership>, ThreadOwnershipError> {
        let binding = self.lease_binding(&attempt.namespace)?;
        if !Arc::ptr_eq(&binding, &attempt.original_binding) {
            return Err(TransactionError::Rejected.into());
        }
        let mut transaction = self.begin_serializable().await?;
        self.prepare_selected_namespace(transaction.connection())
            .await
            .map_err(|_| TransactionError::Rejected)?;
        self.require_bound_storage_open(transaction.connection())
            .await
            .map_err(|_| TransactionError::Rejected)?;
        let table = attempt.namespace.table();
        let token = timeout(self.acquire_timeout,
            sqlx::query_scalar(AssertSqlSafe(format!("SELECT token FROM {table} WHERE thread_id=$1::uuid AND owner_id=$2::uuid AND lease_until > clock_timestamp() FOR SHARE")))
                .bind(&attempt.thread_id).bind(&attempt.owner_id).fetch_optional(transaction.connection()),
        ).await.map_err(|_| TransactionError::Timeout)?
            .map_err(|error| TransactionError::classify_statement(&error))?;
        transaction.rollback().await?;
        Ok(token.map(|token| ThreadOwnership {
            namespace: attempt.namespace.clone(),
            thread_id: attempt.thread_id.clone(),
            owner_id: attempt.owner_id.clone(),
            token,
            original_binding: Some(Arc::clone(&attempt.original_binding)),
        }))
    }
    /// Claim an absent or expired thread lease. `owner_id` must be a fresh UUID
    /// per claim attempt so an unknown COMMIT can be resolved by readback.
    /// Contention yields `None` or a serialization conflict.
    pub async fn claim_thread_ownership(
        &self,
        namespace: ThreadOwnershipNamespace,
        thread_id: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<Option<ThreadOwnership>, ThreadOwnershipError> {
        let millis = lease_millis(lease)?;
        let original_binding = self.lease_binding(&namespace)?;
        let table = namespace.table();
        let mut transaction = self.begin_serializable().await?;
        self.prepare_selected_namespace(transaction.connection())
            .await
            .map_err(|_| TransactionError::Rejected)?;
        self.require_bound_storage_open(transaction.connection())
            .await
            .map_err(|_| TransactionError::Rejected)?;
        timeout(
            self.acquire_timeout,
            sqlx::query(AssertSqlSafe(format!("INSERT INTO {table} (thread_id, token) VALUES ($1::uuid, 0) ON CONFLICT (thread_id) DO NOTHING")))
                .bind(thread_id)
                .execute(transaction.connection()),
        )
        .await
        .map_err(|_| TransactionError::Timeout)?
        .map_err(|error| TransactionError::classify_statement(&error))?;
        let token: Option<i64> = timeout(
            self.acquire_timeout,
            sqlx::query_scalar(AssertSqlSafe(format!(
                "UPDATE {table} \
                 SET token = token + 1, owner_id = $2::uuid, \
                     lease_until = clock_timestamp() + ($3::bigint * interval '1 millisecond') \
                 WHERE thread_id = $1::uuid AND token < 9223372036854775807 \
                   AND (owner_id IS NULL OR lease_until <= clock_timestamp()) \
                 RETURNING token"
            )))
            .bind(thread_id)
            .bind(owner_id)
            .bind(millis)
            .fetch_optional(transaction.connection()),
        )
        .await
        .map_err(|_| TransactionError::Timeout)?
        .map_err(|error| TransactionError::classify_statement(&error))?;
        transaction.commit().await?;
        Ok(token.map(|token| ThreadOwnership {
            namespace,
            thread_id: thread_id.to_owned(),
            owner_id: owner_id.to_owned(),
            token,
            original_binding: Some(original_binding),
        }))
    }

    /// Extend only the current, unexpired token using the server clock.
    pub async fn renew_thread_ownership(
        &self,
        claim: &ThreadOwnership,
        lease: Duration,
    ) -> Result<bool, ThreadOwnershipError> {
        let millis = lease_millis(lease)?;
        self.check_lease_binding(claim)?;
        let table = claim.namespace.table();
        let mut transaction = self.begin_serializable().await?;
        self.prepare_selected_namespace(transaction.connection())
            .await
            .map_err(|_| TransactionError::Rejected)?;
        self.require_bound_storage_open(transaction.connection())
            .await
            .map_err(|_| TransactionError::Rejected)?;
        let updated = timeout(
            self.acquire_timeout,
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE {table} \
                 SET lease_until = clock_timestamp() + ($4::bigint * interval '1 millisecond') \
                 WHERE thread_id = $1::uuid AND owner_id = $2::uuid AND token = $3 \
                   AND lease_until > clock_timestamp()"
            )))
            .bind(&claim.thread_id)
            .bind(&claim.owner_id)
            .bind(claim.token)
            .bind(millis)
            .execute(transaction.connection()),
        )
        .await
        .map_err(|_| TransactionError::Timeout)?
        .map_err(|error| TransactionError::classify_statement(&error))?;
        transaction.commit().await?;
        Ok(updated.rows_affected() == 1)
    }

    /// Release only this unexpired token. The row and its counter remain durable.
    pub async fn release_thread_ownership(
        &self,
        claim: &ThreadOwnership,
    ) -> Result<bool, ThreadOwnershipError> {
        self.check_lease_binding(claim)?;
        let table = claim.namespace.table();
        let mut transaction = self.begin_serializable().await?;
        self.prepare_selected_namespace(transaction.connection())
            .await
            .map_err(|_| TransactionError::Rejected)?;
        self.require_bound_storage_cleanup(transaction.connection())
            .await
            .map_err(|_| TransactionError::Rejected)?;
        let updated = timeout(
            self.acquire_timeout,
            sqlx::query(AssertSqlSafe(format!(
                "UPDATE {table} \
                 SET owner_id = NULL, lease_until = NULL \
                 WHERE thread_id = $1::uuid AND owner_id = $2::uuid AND token = $3 \
                   AND lease_until > clock_timestamp()"
            )))
            .bind(&claim.thread_id)
            .bind(&claim.owner_id)
            .bind(claim.token)
            .execute(transaction.connection()),
        )
        .await
        .map_err(|_| TransactionError::Timeout)?
        .map_err(|error| TransactionError::classify_statement(&error))?;
        transaction.commit().await?;
        Ok(updated.rows_affected() == 1)
    }

    /// Readback for an ambiguous claim result. This is not write authorization.
    pub async fn observe_thread_ownership(
        &self,
        claim: &ThreadOwnership,
    ) -> Result<bool, ThreadOwnershipError> {
        let table = claim.namespace.table();
        let mut connection = self.acquire().await.map_err(|error| {
            ThreadOwnershipError::Transaction(match error {
                crate::PoolError::Timeout => TransactionError::Timeout,
                _ => TransactionError::Unavailable,
            })
        })?;
        timeout(
            self.acquire_timeout,
            sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT EXISTS (SELECT 1 FROM {table} \
                 WHERE thread_id = $1::uuid AND owner_id = $2::uuid AND token = $3 \
                   AND lease_until > clock_timestamp())"
            )))
            .bind(&claim.thread_id)
            .bind(&claim.owner_id)
            .bind(claim.token)
            .fetch_one(&mut *connection),
        )
        .await
        .map_err(|_| TransactionError::Timeout)?
        .map_err(|error| TransactionError::classify_statement(&error))
        .map_err(Into::into)
    }

    /// Resolve an uncertain claim COMMIT by its unique owner attempt ID.
    /// This readback is not write authorization or proof of a prior mutation.
    pub async fn recover_thread_ownership(
        &self,
        namespace: ThreadOwnershipNamespace,
        thread_id: &str,
        owner_id: &str,
    ) -> Result<Option<ThreadOwnership>, ThreadOwnershipError> {
        self.lease_binding(&namespace)?;
        let table = namespace.table();
        let mut connection = self.acquire().await.map_err(|error| {
            ThreadOwnershipError::Transaction(match error {
                crate::PoolError::Timeout => TransactionError::Timeout,
                _ => TransactionError::Unavailable,
            })
        })?;
        let token = timeout(
            self.acquire_timeout,
            sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT token FROM {table} \
                 WHERE thread_id = $1::uuid AND owner_id = $2::uuid \
                   AND lease_until > clock_timestamp()"
            )))
            .bind(thread_id)
            .bind(owner_id)
            .fetch_optional(&mut *connection),
        )
        .await
        .map_err(|_| TransactionError::Timeout)?
        .map_err(|error| TransactionError::classify_statement(&error))?;
        Ok(token.map(|token| ThreadOwnership {
            namespace,
            thread_id: thread_id.to_owned(),
            owner_id: owner_id.to_owned(),
            token,
            // Scalar readback remains observable, but cannot renew/release a lease.
            original_binding: None,
        }))
    }
}
