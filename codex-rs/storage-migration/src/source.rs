//! Read-only access to a local SQLite home.

use anyhow::Result;
use codex_state::SqliteConfig;
use sqlx::SqlitePool;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

/// The SQLite databases of one Codex home, opened only to read.
#[derive(Clone)]
pub struct SqliteSource {
    config: SqliteConfig,
    /// The home these files will be installed into, when this source is a staged copy whose
    /// recorded paths already name that final location.
    relocated_from: Option<PathBuf>,
}

/// Which database file a read targets.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SourceDatabase {
    State,
    Goals,
    Queue,
    Logs,
    Memories,
    Board,
}

impl SqliteSource {
    pub fn new(config: SqliteConfig) -> Self {
        Self {
            config,
            relocated_from: None,
        }
    }

    /// Read a staged home whose recorded paths point into `final_home`, which does not exist
    /// yet: those paths are looked up in the staged directory instead.
    pub fn relocated_from(mut self, final_home: PathBuf) -> Self {
        self.relocated_from = Some(final_home);
        self
    }

    /// The file a recorded path refers to on this source.
    pub(crate) fn resolve(&self, recorded: &str) -> PathBuf {
        let path = PathBuf::from(recorded);
        if let Some(final_home) = &self.relocated_from
            && let Ok(relative) = path.strip_prefix(final_home)
        {
            return self.home().join(relative);
        }
        if path.is_absolute() {
            path
        } else {
            self.home().join(path)
        }
    }

    pub fn home(&self) -> &Path {
        self.config.home()
    }

    /// Identifies which home a run was started for, so a run is only ever resumed against the
    /// same source. Content drift is not part of the identity; verification catches it by
    /// comparing every domain's digest.
    pub async fn fingerprint(&self) -> Result<String> {
        use sha2::Digest;
        let home = tokio::fs::canonicalize(self.config.home())
            .await
            .unwrap_or_else(|_| self.config.home().to_path_buf());
        Ok(sha2::Sha256::digest(home.to_string_lossy().as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }

    /// Open one database for reading, or `None` when the home never created it.
    pub(crate) async fn pool(&self, database: SourceDatabase) -> Result<Option<SqlitePool>> {
        let path = self.database_path(database);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(
            self.config
                .open_read_only_pool(&path, Some(Duration::from_secs(30)))
                .await?,
        ))
    }
    fn database_path(&self, database: SourceDatabase) -> PathBuf {
        match database {
            SourceDatabase::State => self.config.state_db_path(),
            SourceDatabase::Goals => self.config.goals_db_path(),
            SourceDatabase::Queue => self.config.queue_db_path(),
            SourceDatabase::Logs => self.config.logs_db_path(),
            SourceDatabase::Memories => self.config.memories_db_path(),
            SourceDatabase::Board => self.config.home().join("agent_message_board_1.sqlite"),
        }
    }
}

/// Exact source/config and read-only SDK originals retained outside an operation task.
/// This is a local SDK lifecycle owner, not a Home or database transaction permit.
pub struct RetainedSqliteSource {
    source: SqliteSource,
    reads: std::sync::Mutex<RetainedReads>,
    query_cause: std::sync::Mutex<Option<std::sync::Arc<dyn std::error::Error + Send + Sync>>>,
}
struct RetainedReads {
    sealed: bool,
    entries: [RetainedRead; 6],
}
#[derive(Default)]
struct RetainedRead {
    reserved: bool,
    absent: bool,
    pool: Option<SqlitePool>,
    cause: Option<std::sync::Arc<sqlx::Error>>,
}
impl RetainedSqliteSource {
    pub fn new(source: SqliteSource) -> Self {
        Self {
            source,
            query_cause: std::sync::Mutex::new(None),
            reads: std::sync::Mutex::new(RetainedReads {
                sealed: false,
                entries: std::array::from_fn(|_| RetainedRead::default()),
            }),
        }
    }
    pub(crate) fn source(&self) -> &SqliteSource {
        &self.source
    }
    pub(crate) async fn pool(&self, database: SourceDatabase) -> Result<Option<SqlitePool>> {
        let index = database as usize;
        {
            let mut reads = self
                .reads
                .lock()
                .map_err(|_| anyhow::anyhow!("read owner poisoned"))?;
            if reads.sealed || reads.entries[index].reserved {
                anyhow::bail!("read owner admission refused");
            }
            reads.entries[index].reserved = true;
        }
        let path = self.source.database_path(database);
        if !path.exists() {
            let mut reads = self
                .reads
                .lock()
                .map_err(|_| anyhow::anyhow!("read owner poisoned"))?;
            reads.entries[index].absent = true;
            return Ok(None);
        }
        let mut capture = |pool| {
            let (mut reads, poison) = match self.reads.lock() {
                Ok(reads) => (reads, false),
                Err(cause) => (cause.into_inner(), true),
            };
            let sealed = reads.sealed;
            reads.entries[index].pool = Some(pool); // Retain BEFORE any refusal/first acquire.
            if poison || sealed {
                Err(sqlx::Error::Io(std::io::Error::other("read owner refused")))
            } else {
                Ok(())
            }
        };
        match self
            .source
            .config
            .open_read_only_pool_retained(&path, Some(Duration::from_secs(30)), &mut capture)
            .await
        {
            Ok(pool) => Ok(Some(pool)),
            Err(cause) => {
                let mut reads = self
                    .reads
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                reads.entries[index].cause = Some(std::sync::Arc::new(cause));
                anyhow::bail!("retained read acquisition failed");
            }
        }
    }
    pub(crate) fn retain_query_failure(&self, cause: anyhow::Error) {
        let mut saved = self
            .query_cause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if saved.is_none() {
            *saved = Some(std::sync::Arc::from(cause.into_boxed_dyn_error()));
        }
    }
    pub fn original_query_failure(
        &self,
    ) -> Result<Option<std::sync::Arc<dyn std::error::Error + Send + Sync>>> {
        self.query_cause
            .lock()
            .map(|saved| saved.clone())
            .map_err(|_| anyhow::anyhow!("query cause owner poisoned"))
    }
    /// Borrow the retained original driver failures; this is diagnostic evidence only.
    pub fn original_read_causes(&self) -> Result<[Option<std::sync::Arc<sqlx::Error>>; 6]> {
        let reads = self
            .reads
            .lock()
            .map_err(|_| anyhow::anyhow!("read owner poisoned"))?;
        Ok(std::array::from_fn(|index| {
            reads.entries[index].cause.clone()
        }))
    }
    /// Call only after the same creation/query task physically terminates. Every
    /// known original is closed even on poison; unresolved creation refuses ACK.
    pub async fn close_original_read_pools(&self) -> Result<()> {
        let (pools, unresolved, poison) = {
            let (mut reads, poison) = match self.reads.lock() {
                Ok(reads) => (reads, false),
                Err(cause) => (cause.into_inner(), true),
            };
            reads.sealed = true;
            let unresolved = reads
                .entries
                .iter()
                .any(|entry| entry.reserved && !entry.absent && entry.pool.is_none());
            (
                std::array::from_fn::<_, 6, _>(|index| reads.entries[index].pool.clone()),
                unresolved,
                poison,
            )
        };
        for pool in pools.into_iter().flatten() {
            pool.close().await;
        }
        if poison || unresolved {
            anyhow::bail!("read cleanup refused");
        }
        Ok(())
    }
}
