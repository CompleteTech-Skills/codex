//! Original migration startup registered externally before any connection polling.
use crate::ConnectionSettings;
use crate::NamedNamespace;
use crate::PoolError;
use crate::PostgresPool;
use crate::migration_login::MigrationLogin;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

pub struct OriginalMigrationStartup {
    original_runtime: Arc<PostgresPool>,
    login: MigrationLogin,
    state: Mutex<State>,
    join: Semaphore,
    close: Mutex<SdkCloseState>,
    close_join: Semaphore,
}
struct State {
    settings: Option<ConnectionSettings>,
    task: Option<JoinHandle<()>>,
    started: bool,
    pool: Option<Arc<PostgresPool>>,
    refusal: Option<PoolError>,
    activation_refusal: Option<crate::ActivationGateError>,
    join_failure: Option<tokio::task::JoinError>,
    terminal: bool,
}
/// Every post-construction refusal retains the same original frame and causes.
pub struct MigrationStartupRefusal {
    pub original: Arc<OriginalMigrationStartup>,
}
impl std::fmt::Debug for MigrationStartupRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MigrationStartupRefusal([retained original])")
    }
}
impl std::fmt::Display for MigrationStartupRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original migration startup refused")
    }
}
impl std::error::Error for MigrationStartupRefusal {}
impl OriginalMigrationStartup {
    /// Synchronous: owning host must register this Arc BEFORE calling start_original.
    pub fn prepare(
        original_runtime: Arc<PostgresPool>,
        settings: ConnectionSettings,
        namespace: Option<&NamedNamespace>,
    ) -> Result<Arc<Self>, MigrationStartupRefusal> {
        let login = MigrationLogin::new(namespace);
        let refused = original_runtime.activation_binding.get().is_none()
            || original_runtime.selected_search_path.as_deref()
                != Some(crate::owned_namespace_search_path(namespace).as_str())
            || !login.accepts_login(&settings.username)
            || original_runtime.original_target.as_ref()
                != Some(&crate::ConnectionTarget::from_settings(&settings));
        let original = Arc::new(Self {
            original_runtime,
            login,
            state: Mutex::new(State {
                settings: Some(settings),
                task: None,
                started: false,
                pool: None,
                refusal: if refused {
                    Some(PoolError::InvalidSettings)
                } else {
                    None
                },
                activation_refusal: None,
                join_failure: None,
                terminal: false,
            }),
            join: Semaphore::new(1),
            close: Mutex::new(SdkCloseState::default()),
            close_join: Semaphore::new(1),
        });
        if refused {
            Err(original.refused())
        } else {
            Ok(original)
        }
    }
    pub fn original_runtime(&self) -> &Arc<PostgresPool> {
        &self.original_runtime
    }
    pub fn start_original(self: &Arc<Self>) -> Result<(), MigrationStartupRefusal> {
        let mut state = self.state.lock().map_err(|_| self.refused())?;
        if state.started || state.refusal.is_some() || state.activation_refusal.is_some() {
            return Err(self.refused());
        }
        let settings = state.settings.take().ok_or_else(|| self.refused())?;
        state.started = true;
        let original = Arc::clone(self);
        // Same original task stored under the frame gate before the host can join it.
        state.task = Some(tokio::spawn(async move {
            // Sender retains this same gate until the exact original handle is installed.
            // A multi-thread executor cannot poll connection SQL ahead of registration.
            {
                if original.state.lock().is_err() {
                    original
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .refusal = Some(PoolError::Unavailable);
                    return;
                }
            }
            let result = PostgresPool::connect_with_search_path(
                settings,
                original.original_runtime.selected_search_path.clone(),
                Some(original.login.clone()),
            )
            .await;
            let pool = match result {
                Ok(pool) => Arc::new(pool),
                Err(error) => {
                    original
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .refusal = Some(error);
                    return;
                }
            };
            // Retain actual pool before any subsequent SQL poll or failure.
            original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pool = Some(Arc::clone(&pool));
            let activation = pool.bind_current_activation().await;
            let admitted = activation.is_ok()
                && pool.activation_binding.get()
                    == original.original_runtime.activation_binding.get();
            if !admitted {
                if let Err(original_gate) = activation {
                    original
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .activation_refusal = Some(original_gate);
                }
                original
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .refusal = Some(PoolError::Unavailable);
            }
        }));
        Ok(())
    }
    pub async fn finish_original(
        self: &Arc<Self>,
    ) -> Result<Arc<PostgresPool>, MigrationStartupRefusal> {
        let _permit = self.join.acquire().await.map_err(|_| self.refused())?;
        let task = { self.state.lock().map_err(|_| self.refused())?.task.take() };
        if let Some(task) = task {
            let mut loan = TaskLoan {
                original: Arc::clone(self),
                task: Some(task),
            };
            let result = match loan.task.as_mut() {
                Some(task) => task.await,
                None => return Err(self.refused()),
            };
            loan.task.take();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.terminal = true;
            if let Err(original) = result {
                state.join_failure = Some(original);
            }
        }
        let state = self.state.lock().map_err(|_| self.refused())?;
        if !state.terminal
            || state.refusal.is_some()
            || state.activation_refusal.is_some()
            || state.join_failure.is_some()
        {
            return Err(self.refused());
        }
        state
            .pool
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| self.refused())
    }
    /// Only an actual completed original SQLx pool-close task can construct this.
    /// It grants no native socket, Home, selected generation, or writer-drain authority.
    pub async fn close_original(
        self: &Arc<Self>,
    ) -> Result<OriginalMigrationPoolClosed, MigrationStartupRefusal> {
        // Cleanup observes physical startup completion independently of admission.
        // Poison remains a refusal, but must not prevent reaping the same original.
        let startup_refused = {
            let _join = self.join.acquire().await.map_err(|_| self.refused())?;
            let task = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task
                .take();
            if let Some(task) = task {
                let mut loan = TaskLoan {
                    original: Arc::clone(self),
                    task: Some(task),
                };
                let result = match loan.task.as_mut() {
                    Some(task) => task.await,
                    None => return Err(self.refused()),
                };
                loan.task.take();
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.terminal = true;
                if let Err(cause) = result {
                    state.join_failure = Some(cause);
                }
            }
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.started && !state.terminal {
                return Err(self.refused());
            }
            self.state.is_poisoned()
                || state.refusal.is_some()
                || state.activation_refusal.is_some()
                || state.join_failure.is_some()
        };
        let pool = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pool
            .as_ref()
            .map(Arc::clone);
        // No pool is only physically empty after startup was reaped or never dispatched.
        let Some(pool) = pool else {
            return Err(self.refused());
        };
        let _permit = self
            .close_join
            .acquire()
            .await
            .map_err(|_| self.refused())?;
        let task = {
            let mut close = self
                .close
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !close.started {
                close.started = true;
                let original = Arc::clone(self);
                close.task = Some(tokio::spawn(async move {
                    // Same gate keeps first pool-close poll behind exact handle insertion.
                    {
                        let _entry = original
                            .close
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                    pool.pool.close().await;
                    original
                        .close
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .sdk_done = true;
                }));
            }
            close.task.take()
        };
        if let Some(task) = task {
            let mut loan = CloseLoan {
                original: Arc::clone(self),
                task: Some(task),
            };
            let result = match loan.task.as_mut() {
                Some(task) => task.await,
                None => return Err(self.refused()),
            };
            loan.task.take();
            let mut close = self
                .close
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            close.joined = true;
            if let Err(cause) = result {
                close.failure = Some(cause);
            }
        }
        let close = self.close.lock().map_err(|_| self.refused())?;
        if startup_refused || !close.sdk_done || !close.joined || close.failure.is_some() {
            return Err(self.refused());
        }
        Ok(OriginalMigrationPoolClosed {
            original: Arc::clone(self),
        })
    }
    /// Diagnostic for preserving original retry/interruption causes on archive.
    pub fn retains_original_causes(self: &Arc<Self>) -> Result<bool, MigrationStartupRefusal> {
        self.login
            .retained_cause_count()
            .map(|count| count != 0)
            .map_err(|_| self.refused())
    }
    fn refused(self: &Arc<Self>) -> MigrationStartupRefusal {
        MigrationStartupRefusal {
            original: Arc::clone(self),
        }
    }
}
struct TaskLoan {
    original: Arc<OriginalMigrationStartup>,
    task: Option<JoinHandle<()>>,
}
impl Drop for TaskLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task = Some(task);
        }
    }
}

/// Opaque same-owner SQLx cleanup receipt, not a serialized/native authority.
pub struct OriginalMigrationPoolClosed {
    original: Arc<OriginalMigrationStartup>,
}
impl OriginalMigrationPoolClosed {
    pub fn original(&self) -> &Arc<OriginalMigrationStartup> {
        &self.original
    }
}
#[derive(Default)]
struct SdkCloseState {
    task: Option<JoinHandle<()>>,
    started: bool,
    sdk_done: bool,
    joined: bool,
    failure: Option<tokio::task::JoinError>,
}
struct CloseLoan {
    original: Arc<OriginalMigrationStartup>,
    task: Option<JoinHandle<()>>,
}
impl Drop for CloseLoan {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.original
                .close
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task = Some(task);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn canceled_original_join_restores_exact_task_to_external_frame()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Arc::new(PostgresPool {
            pool: sqlx_postgres::PgPoolOptions::new()
                .connect_lazy_with(sqlx_postgres::PgConnectOptions::new()),
            original_target: None,
            acquire_timeout: std::time::Duration::from_secs(1),
            selected_search_path: None,
            activation_binding: std::sync::OnceLock::new(),
        });
        let settings = ConnectionSettings {
            host: "localhost".to_string(),
            port: 5432,
            database: "db".to_string(),
            username: "codex_migrator".to_string(),
            password: "private".to_string().into(),
            ca_certificate: std::path::PathBuf::from("C:/fixture/ca.crt"),
            limits: crate::PoolLimits {
                connect_timeout: std::time::Duration::from_secs(1),
                acquire_timeout: std::time::Duration::from_secs(1),
                max_connections: 1,
            },
        };
        let rejected = match OriginalMigrationStartup::prepare(Arc::clone(&runtime), settings, None)
        {
            Ok(_) => return Err("unbound runtime accepted".into()),
            Err(error) => error.original,
        };
        assert!(rejected.start_original().is_err());
        {
            let state = rejected.state.lock().map_err(|_| "state poisoned")?;
            assert!(state.settings.is_some());
            assert!(state.task.is_none());
            assert!(!state.started);
        }
        let original = Arc::new(OriginalMigrationStartup {
            original_runtime: runtime,
            login: MigrationLogin::new(None),
            join: Semaphore::new(1),
            close: Mutex::new(SdkCloseState::default()),
            close_join: Semaphore::new(1),
            state: Mutex::new(State {
                settings: None,
                task: None,
                started: true,
                pool: None,
                refusal: None,
                activation_refusal: None,
                join_failure: None,
                terminal: false,
            }),
        });
        let (release, receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = receiver.await;
        });
        let id = task.id();
        {
            let mut state = original.state.lock().map_err(|_| "state poisoned")?;
            state.task = Some(task);
        }
        let mut finishing = Box::pin(original.finish_original());
        let early = tokio::select! { biased;
            result = &mut finishing => { let _ = result; true },
            _ = tokio::task::yield_now() => false,
        };
        drop(finishing);
        let restored = original
            .state
            .lock()
            .map(|state| (state.task.as_ref().map(JoinHandle::id), state.terminal))
            .map_err(|_| "state poisoned");
        let released = release.send(()).is_ok();
        // Rejoin the same cached/original task on every diagnostic path.
        let refused_after_join = original.finish_original().await.is_err();
        // Helper-only poison cleanup: retain refusal, reap the SAME stored handle.
        let original_task = if original.state.is_poisoned() {
            original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .task
                .take()
        } else {
            None
        };
        if let Some(original_task) = original_task {
            let result = original_task.await;
            let mut state = original
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.terminal = true;
            if let Err(original_failure) = result {
                state.join_failure = Some(original_failure);
            }
        }
        let restored = restored.map_err(|_| "state poisoned after original join")?;
        assert!(!early);
        assert!(released);
        assert!(refused_after_join);
        let state = original.state.lock().map_err(|_| "state poisoned")?;
        assert_eq!(restored.0, Some(id));
        assert!(!restored.1);
        assert!(state.terminal);
        assert!(state.task.is_none());
        assert!(state.join_failure.is_none());
        Ok(())
    }
}
