//! Unwired PostgreSQL section mutations for qualified default and named namespaces.

mod postgres;

pub use postgres::AppearanceChange;
pub use postgres::PostgresThreadSectionStore;
pub use postgres::SectionNamespace;
pub use postgres::SectionStoreError;

#[cfg(test)]
mod fixture_support;
