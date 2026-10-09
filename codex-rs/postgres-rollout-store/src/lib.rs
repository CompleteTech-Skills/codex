//! Unwired fixed-namespace PostgreSQL canonical storage for rollout lines.

mod store;

pub use store::PostgresRolloutStore;
pub use store::RolloutStoreError;
pub use store::StoredRolloutLine;
pub use store::append_in;
pub use store::validate_append_request;
