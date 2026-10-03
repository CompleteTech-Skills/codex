//! The authoritative remote stores of this process, installed once at startup.
//!
//! When PostgreSQL holds the authoritative history, the process connects before it builds any
//! session machinery and installs the stores here. Store constructors consult this registry, so
//! no consumer can quietly fall back to local files.

use codex_agent_graph_store::AgentGraphStore;
use codex_postgres_runtime::PostgresPool;
use codex_thread_store::QueueStore;
use codex_thread_store::ThreadStore;
use std::sync::Arc;
use std::sync::OnceLock;

/// The remote implementations of the stores consumers build from configuration.
#[derive(Clone)]
pub struct RemoteBackend {
    /// The SAME original pool captured by genuine remote connection startup.
    /// Store projections/scalar generation are not substitutes. Retention does
    /// not itself prove current SQL admission or whole-home physical drain.
    pub original_pool: Arc<PostgresPool>,
    pub thread_store: Arc<dyn ThreadStore>,
    pub queue_store: Arc<dyn QueueStore>,
    pub agent_graph_store: Arc<dyn AgentGraphStore>,
}

/// A backend was already installed for this process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteBackendAlreadyInstalled;

static REMOTE_BACKEND: OnceLock<RemoteBackend> = OnceLock::new();

/// Install the process's remote stores. A second call is refused so a process never switches
/// persistence backend underneath running threads.
pub fn install_remote_backend(backend: RemoteBackend) -> Result<(), RemoteBackendAlreadyInstalled> {
    REMOTE_BACKEND
        .set(backend)
        .map_err(|_| RemoteBackendAlreadyInstalled)
}

/// The installed remote stores, if this process runs against remote storage.
pub fn remote_backend() -> Option<&'static RemoteBackend> {
    REMOTE_BACKEND.get()
}
