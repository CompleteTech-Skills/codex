//! One connection to a remote PostgreSQL dataset and the stores that live on it.
//!
//! A [`RemoteStorage`] is built from a trusted storage profile and a host credential resolver.
//! It connects with the runtime login, verifies that the store holds the schema format this build
//! writes, and then hands out the PostgreSQL implementations of every store trait. It never
//! creates tables, never falls back to local files, and reports failures as typed outcomes that
//! say whether trying again can help.

mod administration;
mod administration_close;
mod administration_registry;
mod administration_settings;
mod administration_startup;
mod error;
mod runtime_origin;
mod storage;
pub use runtime_origin::OriginalRuntimeOrigin;
pub use runtime_origin::RuntimeNativeOrigin;
pub use runtime_origin::RuntimeOriginRefusal;

pub use administration::AdministrativeObservationRefusal;
pub use administration::OriginalAdministrativeRuntimeSession;
pub use administration::RemoteAdministrativeObserver;
pub use administration_registry::AdministrativeFailureArchive;
pub use administration_registry::AdministrativeRegistryRefusal;
pub use administration_startup::AdministrativeNativeObservation;
pub use error::RemoteStorageError;
pub use storage::LoginRole;
pub use storage::RemoteStorage;
pub use storage::StorageActivation;
pub use storage::connection_settings;
