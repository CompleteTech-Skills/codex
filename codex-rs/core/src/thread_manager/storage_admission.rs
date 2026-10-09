//! Retain/seal the actual production session-tree admission producers of one manager.
//! Joined trees do NOT prove pool/rollout/other-process quiescence, original native
//! HomeArc/current SQL generation, or authorize filesystem adoption/retirement.
use super::*;
use crate::agent::control::AgentTreeShutdownState;
use crate::agent::control::LocalAgentRuntime;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
struct Admission {
    sealed: bool,
    trees: Vec<LocalAgentRuntime>,
}
pub(super) struct StorageSessionAdmission {
    home: PathBuf,
    admission: Mutex<Admission>,
    #[cfg(test)]
    after_retain: Mutex<Option<Arc<RetainPause>>>,
}
impl StorageSessionAdmission {
    pub(super) fn new(home: PathBuf) -> Self {
        Self {
            home,
            #[cfg(test)]
            after_retain: Mutex::new(None),
            admission: Mutex::new(Admission {
                sealed: false,
                trees: Vec::new(),
            }),
        }
    }
    pub(super) fn require_home(&self, home: &Path) -> CodexResult<()> {
        if home != self.home {
            return Err(CodexErr::InvalidRequest(
                "session home differs from its manager".to_owned(),
            ));
        }
        Ok(())
    }
    fn retain(&self, runtime: &LocalAgentRuntime) -> CodexResult<()> {
        let mut admission = self.admission.lock().map_err(|_| {
            CodexErr::InvalidRequest("session writer admission poisoned".to_owned())
        })?;
        if admission.sealed {
            return Err(CodexErr::InvalidRequest(
                "session writer admission sealed".to_owned(),
            ));
        }
        if admission
            .trees
            .iter()
            .any(|owner| owner.shares_shutdown_owner(runtime))
        {
            return Ok(());
        }
        // Retaining an owner is not proof that its tree remains live or has joined.
        // Admission must not impose a lifetime-start ceiling on a long-lived manager.
        // Keep exact owners until seal; allocation failure remains a fallible refusal.
        admission
            .trees
            .try_reserve(1)
            .map_err(|error| CodexErr::InvalidRequest(error.to_string()))?;
        // Exact real per-tree owner is retained BEFORE its membership/admission.
        admission.trees.push(runtime.clone());
        Ok(())
    }
    fn seal(&self) -> io::Result<Vec<Arc<AgentTreeShutdownState>>> {
        let mut admission = self
            .admission
            .lock()
            .map_err(|_| io::Error::other("session writer admission poisoned"))?;
        let mut states = Vec::new();
        states
            .try_reserve_exact(admission.trees.len())
            .map_err(io::Error::other)?;
        admission.sealed = true; // same gate as every real root/delegate admission
        for runtime in &admission.trees {
            states.push(runtime.request_shutdown());
        }
        Ok(states)
    }
}
// Exact per-original-runtime causal seam; absent from production builds.
#[cfg(test)]
struct RetainPause {
    owner: LocalAgentRuntime,
    entered: std::sync::mpsc::Sender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
    fired: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
impl StorageSessionAdmission {
    fn pause_after_retain(&self, runtime: &LocalAgentRuntime) -> CodexResult<()> {
        let pause = self
            .after_retain
            .lock()
            .map_err(|_| CodexErr::InvalidRequest("test retain seam poisoned".to_owned()))?
            .clone();
        if let Some(pause) = pause
            && pause.owner.shares_shutdown_owner(runtime)
            && !pause.fired.swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            pause
                .entered
                .send(())
                .map_err(|_| CodexErr::InvalidRequest("test retain witness lost".to_owned()))?;
            pause
                .release
                .lock()
                .map_err(|_| CodexErr::InvalidRequest("test release poisoned".to_owned()))?
                .recv_timeout(std::time::Duration::from_secs(2))
                .map_err(|_| CodexErr::InvalidRequest("test retain release timeout".to_owned()))?;
        }
        Ok(())
    }
}
impl ThreadManagerState {
    pub(crate) fn retain_storage_session_tree(
        &self,
        runtime: &LocalAgentRuntime,
    ) -> CodexResult<()> {
        self.storage_sessions.retain(runtime)?;
        #[cfg(test)]
        self.storage_sessions.pause_after_retain(runtime)?;
        Ok(())
    }
}
/// Exact sealed production trees, not an all-writers or storage-transition permit.
/// Original manager/runtimes remain owned; no remove_thread releases this evidence.
pub struct ManagerSessionTreesJoined {
    _original: Arc<ThreadManager>,
}
struct JoinError {
    cause: io::Error,
    _original: Arc<ThreadManager>,
}
impl std::fmt::Debug for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ManagerSessionTreesJoinError")
            .field(&self.cause)
            .finish()
    }
}
impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for JoinError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
impl ThreadManager {
    /// Seal actual start producers including failed/in-flight starts, then await
    /// existing membership teardown. Cancellation retains owners in this manager;
    /// callers must retain it and retry, never treat timeout as joined evidence.
    pub async fn seal_and_join_session_trees(
        self: &Arc<Self>,
    ) -> io::Result<ManagerSessionTreesJoined> {
        let original = Arc::clone(self); // before allocation, seal or wait refusal
        let result = async {
            let states = self.state.storage_sessions.seal()?;
            let mut failure = None;
            for state in states {
                if let Err(error) = state.wait().await {
                    failure.get_or_insert_with(|| io::Error::other(error.to_string()));
                }
            }
            match failure {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }
        .await;
        match result {
            Ok(()) => Ok(ManagerSessionTreesJoined {
                _original: original,
            }),
            Err(cause) => Err(io::Error::other(JoinError {
                cause,
                _original: original,
            })),
        }
    }
}

#[cfg(test)]
#[path = "storage_admission_tests.rs"]
mod tests;
