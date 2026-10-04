//! Original SQLite pools retained before first connection, not a home-wide permit.
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;

const MAX_WRITE_POOLS: usize = 4096;
#[derive(Debug, Default)]
struct Admission {
    sealed: bool,
    pools: Vec<SqlitePool>,
}
#[derive(Debug, Default)]
pub(super) struct WritePoolOwner {
    admission: Mutex<Admission>,
}
impl WritePoolOwner {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(super) fn register_lazy(
        self: &Arc<Self>,
        options: SqlitePoolOptions,
        connection: SqliteConnectOptions,
    ) -> io::Result<SqlitePool> {
        let mut gate = self
            .admission
            .lock()
            .map_err(|_| self.failure("SQLite pool admission poisoned"))?;
        if gate.sealed {
            return Err(self.failure("SQLite writable pool admission sealed"));
        }
        if gate.pools.len() >= MAX_WRITE_POOLS {
            return Err(self.failure("SQLite writable pool owner limit"));
        }
        gate.pools
            .try_reserve(1)
            .map_err(|e| self.failure(e.to_string()))?;
        let pool = options.min_connections(0).connect_lazy_with(connection);
        gate.pools.push(pool.clone());
        Ok(pool)
    }
    pub(super) async fn seal_and_close(self: &Arc<Self>) -> io::Result<ClosedWritePools> {
        let pools = {
            let mut gate = self
                .admission
                .lock()
                .map_err(|_| self.failure("SQLite pool admission poisoned"))?;
            gate.sealed = true;
            let mut pools = Vec::new();
            pools
                .try_reserve_exact(gate.pools.len())
                .map_err(|e| self.failure(e.to_string()))?;
            pools.extend(gate.pools.iter().cloned());
            pools
        };
        // Cancellation leaves all originals in the sealed owner. A retry
        // closes the same pools; no logical is_closed flag substitutes for wait.
        // SQLx 0.9 marks the pool closed synchronously when close() is
        // called. Close admission on ALL originals before waiting for any one.
        // Dropping this first future is not physical closure evidence.
        for pool in &pools {
            drop(pool.close());
        }
        for pool in pools {
            pool.close().await;
        }
        Ok(ClosedWritePools {
            original: Arc::clone(self),
        })
    }
    fn failure(self: &Arc<Self>, cause: impl Into<String>) -> io::Error {
        io::Error::other(OwnerError {
            original: Arc::clone(self),
            cause: cause.into(),
        })
    }
}
/// Evidence limited to the exact registered pool owner, after awaited close.
#[derive(Debug)]
pub struct ClosedWritePools {
    original: Arc<WritePoolOwner>,
}
impl ClosedWritePools {
    pub fn covers_config(&self, config: &super::SqliteConfig) -> bool {
        Arc::ptr_eq(&self.original, &config.write_pools)
    }
}
#[derive(Debug)]
struct OwnerError {
    original: Arc<WritePoolOwner>,
    cause: String,
}
impl std::fmt::Display for OwnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Keep the original owner live through every escaping refusal.
        let _retained = &self.original;
        f.write_str(&self.cause)
    }
}
impl std::error::Error for OwnerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn held_actual_connection_prevents_close_ack_after_cancellation() -> io::Result<()> {
        let owner = Arc::new(WritePoolOwner::new());
        let pool = owner.register_lazy(
            SqlitePoolOptions::new().max_connections(1),
            SqliteConnectOptions::new().in_memory(true),
        )?;
        let original = match pool.acquire().await {
            Ok(connection) => connection,
            Err(error) => {
                owner.seal_and_close().await?;
                return Err(io::Error::other(error));
            }
        };
        let pending = tokio::time::timeout(Duration::from_millis(10), owner.seal_and_close()).await;
        let late = owner.register_lazy(
            SqlitePoolOptions::new(),
            SqliteConnectOptions::new().in_memory(true),
        );
        let retry = tokio::time::timeout(Duration::from_millis(10), owner.seal_and_close()).await;
        drop(original); // unconditional release of exact actual connection before assertions
        let acknowledged = owner.seal_and_close().await?;
        assert!(pending.is_err());
        assert!(retry.is_err());
        assert!(late.is_err());
        assert!(pool.is_closed());
        assert!(Arc::ptr_eq(&acknowledged.original, &owner));
        Ok(())
    }

    #[tokio::test]
    async fn seal_requests_all_actual_pools_before_blocked_first_close() -> io::Result<()> {
        let owner = Arc::new(WritePoolOwner::new());
        let first = owner.register_lazy(
            SqlitePoolOptions::new().max_connections(1),
            SqliteConnectOptions::new().in_memory(true),
        )?;
        let second = owner.register_lazy(
            SqlitePoolOptions::new().max_connections(1),
            SqliteConnectOptions::new().in_memory(true),
        )?;
        let held = match first.acquire().await {
            Ok(connection) => connection,
            Err(error) => {
                owner.seal_and_close().await?;
                return Err(io::Error::other(error));
            }
        };
        let pending = tokio::time::timeout(Duration::from_millis(10), owner.seal_and_close()).await;
        let first_closed = first.is_closed();
        let second_closed = second.is_closed();
        drop(held);
        owner.seal_and_close().await?;
        assert!(pending.is_err());
        assert!(first_closed && second_closed);
        assert!(second.acquire().await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_initializing_connection_keeps_original_pool_for_physical_close()
    -> io::Result<()> {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;
        let owner = Arc::new(WritePoolOwner::new());
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(tokio::sync::Notify::new());
        let observed = Arc::clone(&entered);
        let held = Arc::clone(&release);
        let options = SqlitePoolOptions::new().max_connections(1).after_connect(
            move |_connection, _metadata| {
                let observed = Arc::clone(&observed);
                let held = Arc::clone(&held);
                Box::pin(async move {
                    observed.store(true, Ordering::Release); // actual SQLite connection exists
                    held.notified().await;
                    Ok(())
                })
            },
        );
        let pool = owner.register_lazy(options, SqliteConnectOptions::new().in_memory(true))?;
        let mut opening = Box::pin(pool.acquire());
        let pending = tokio::time::timeout(Duration::from_millis(100), &mut opening).await;
        let actual_connection_observed = entered.load(Ordering::Acquire);
        let timed_out = pending.is_err();
        drop(pending); // release any unexpectedly returned real connection before cleanup
        release.notify_one(); // release on every body-result path
        drop(opening); // cancelled original initialization, not physical close evidence
        drop(pool); // only original registry retains pool
        let receipt = owner.seal_and_close().await?;
        assert!(timed_out);
        assert!(actual_connection_observed);
        assert!(Arc::ptr_eq(&receipt.original, &owner));
        Ok(())
    }

    #[tokio::test]
    async fn public_open_retains_actual_pool_and_exact_config_identity() -> io::Result<()> {
        let home = std::env::temp_dir().join(format!("codex-pool-owner-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&home)?;
        let config = super::super::SqliteConfig::from_sqlite_home(
            codex_utils_absolute_path::AbsolutePathBuf::from_absolute_path(&home)?,
        );
        let path = home.join("owner.sqlite");
        let result = async {
            let pool = config
                .open_read_write_pool(&path)
                .await
                .map_err(io::Error::other)?;
            sqlx::query("CREATE TABLE actual_owner (value INTEGER)")
                .execute(&pool)
                .await
                .map_err(io::Error::other)?;
            drop(pool); // only the ORIGINAL config registration now retains this pool
            let clone = config.clone();
            let independent = super::super::SqliteConfig::from_sqlite_home(
                codex_utils_absolute_path::AbsolutePathBuf::from_absolute_path(&home)?,
            );
            let receipt = config.seal_and_close_write_pools().await?;
            assert!(config.shares_write_pool_owner(&clone));
            assert!(!config.shares_write_pool_owner(&independent));
            assert!(receipt.covers_config(&clone));
            assert!(!receipt.covers_config(&independent));
            assert!(clone.open_read_write_pool(&path).await.is_err());
            Ok::<(), io::Error>(())
        }
        .await;
        // Every body-result path awaits the same original physical pool closure.
        config.seal_and_close_write_pools().await?;
        result?;
        std::fs::remove_dir_all(&home)?;
        Ok(())
    }
}
