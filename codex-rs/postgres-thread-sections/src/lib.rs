//! Unwired fixed-namespace PostgreSQL section catalog persistence.

mod postgres;

pub use postgres::AppearanceChange;
pub use postgres::PostgresThreadSectionStore;
pub use postgres::SectionStoreError;
