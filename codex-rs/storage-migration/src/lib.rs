//! Verified movement of a Codex store between SQLite and PostgreSQL.
//!
//! The engine reads every domain from a source, writes it to a target in resumable batches,
//! and proves the target matches by comparing a canonical digest of each domain. It never
//! switches the active backend; activation is a separate, explicit step.

mod attachments;
mod board;
mod cutover;
mod digest;
mod domain;
mod engine;
mod estimate;
mod external_imports;
mod goals;
mod install;
mod logs;
mod memory;
mod projects;
mod queue;
mod return_cutover;
mod rollouts;
mod sections;
mod source;
mod source_manifest;
mod sqlite_target;
mod threads;

pub use cutover::Cutover;
pub use cutover::CutoverError;
pub use cutover::CutoverStatus;
pub use cutover::RecoveryOutcome;
pub use digest::DomainDigest;
pub use domain::Domain;
pub use engine::ActivationState;
pub use engine::ActivationTarget;
pub use engine::MigrationError;
pub use engine::Migrator;
pub use engine::ReturnOperationFence;
pub use engine::RunSummary;
pub use engine::VerificationReport;
pub use engine::target_is_empty;
pub use estimate::SourceEstimate;
pub use estimate::estimate_retained_source;
pub use estimate::estimate_source;
pub use install::InstallPlan;
pub use install::InstallUnit;
pub use install::read_plan;
pub use install::verify_backup;
pub use return_cutover::ReturnCutover;
pub use source::RetainedSqliteSource;
pub use source::SqliteSource;
pub use source_manifest::SourceManifest;
pub use source_manifest::source_manifest_path;
pub use source_manifest::write_source_manifest;
pub use sqlite_target::SqliteTarget;
