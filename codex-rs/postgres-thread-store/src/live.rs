//! The write path of one live thread.
//!
//! Items accumulate in memory until a barrier materializes them, like the local recorder.
//! Serialized lines are staged before they are written and kept until the write is known to
//! have committed, so a retry after an ambiguous commit sends identical bytes and is recognized
//! instead of duplicated.

use chrono::SecondsFormat;
use chrono::Utc;
use codex_git_utils::collect_git_info;
use codex_git_utils::get_git_repo_root;
use codex_postgres_rollout_store::RolloutStoreError;
use codex_postgres_rollout_store::append_in;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::ThreadOwnership;
use codex_postgres_thread_catalog::get_thread_in;
use codex_postgres_thread_catalog::upsert_thread_in;
use codex_protocol::ThreadId;
use codex_protocol::protocol::GitInfo as ProtocolGitInfo;
use codex_protocol::protocol::HistoryPosition;
use codex_protocol::protocol::SessionContextWindow;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutLine;
use codex_rollout::builder_from_items;
use codex_state::apply_rollout_item;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use codex_utils_absolute_path::AbsolutePathBuf;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering as AtomicOrdering;

/// Per-thread record ordinals: legacy rollouts carry none, paginated ones count upward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ordinals {
    Legacy,
    Paginated { next: Option<u64> },
}

impl Ordinals {
    pub(crate) fn for_new(history_mode: ThreadHistoryMode, base: Option<HistoryPosition>) -> Self {
        match history_mode {
            ThreadHistoryMode::Legacy => Self::Legacy,
            ThreadHistoryMode::Paginated => Self::Paginated {
                next: Some(base.map_or(0, |base| base.end_ordinal_exclusive)),
            },
        }
    }

    /// Continue after the last stored ordinal when a thread is reopened.
    pub(crate) fn resume(history_mode: ThreadHistoryMode, last_ordinal: Option<u64>) -> Self {
        match history_mode {
            ThreadHistoryMode::Legacy => Self::Legacy,
            ThreadHistoryMode::Paginated => Self::Paginated {
                next: Some(last_ordinal.map_or(0, |ordinal| ordinal.saturating_add(1))),
            },
        }
    }

    fn current(self) -> ThreadStoreResult<Option<u64>> {
        match self {
            Self::Legacy => Ok(None),
            Self::Paginated { next } => next.map(Some).ok_or(ThreadStoreError::Internal {
                message: "paginated rollout record ordinal overflow".to_string(),
            }),
        }
    }

    fn advance(&mut self) {
        if let Self::Paginated { next } = self
            && let Some(ordinal) = *next
        {
            *next = ordinal.checked_add(1);
        }
    }
}

/// The lease that makes this process the only writer of a thread, and its renewal.
pub(crate) struct WriterOwnership {
    pub(crate) claim: ThreadOwnership,
    /// Set by the renewal task when the lease can no longer be trusted.
    pub(crate) lost: Arc<AtomicBool>,
    pub(crate) renewer: tokio::task::JoinHandle<()>,
    pub(crate) renewal_joined: bool,
    pub(crate) renewal_failed: bool,
}

impl Drop for WriterOwnership {
    fn drop(&mut self) {
        self.renewer.abort();
    }
}

/// Serialized lines with the items they carry, kept until their commit is confirmed.
type StagedBatch = (Vec<(Option<u64>, String)>, Vec<RolloutItem>);

/// Everything the store tracks for a thread it holds open for writing.
pub(crate) struct LiveThread {
    pub(crate) history_mode: ThreadHistoryMode,
    pub(crate) cwd: PathBuf,
    pub(crate) memory_mode: &'static str,
    /// Canonical header not yet written.
    meta: Option<SessionMeta>,
    pending: Vec<RolloutItem>,
    /// Lines already serialized but whose commit is not confirmed, with the items they carry.
    staged: Option<StagedBatch>,
    pub(crate) materialized: bool,
    pub(crate) next_position: u64,
    ordinals: Ordinals,
    pub(crate) ownership: Option<WriterOwnership>,
    sealed: bool,
}

impl LiveThread {
    pub(crate) fn for_create(params: &CreateThreadParams) -> Self {
        let cwd = params.metadata.cwd.clone().unwrap_or_default();
        let meta = SessionMeta {
            session_id: params.session_id,
            id: params.thread_id,
            forked_from_id: params.forked_from_id,
            forked_from_ordinal_exclusive: params
                .forked_from_id
                .and(params.history_base)
                .map(|base| base.end_ordinal_exclusive),
            parent_thread_id: params.parent_thread_id,
            timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            cwd: cwd.clone(),
            runtime_workspace_roots: params.runtime_workspace_roots.clone().map(|roots| {
                roots
                    .into_iter()
                    .map(AbsolutePathBuf::into_path_buf)
                    .collect()
            }),
            originator: params.originator.clone(),
            creator_user_id: params.creator_user_id.clone(),
            creator_account_id: params.creator_account_id.clone(),
            cli_version: env!("CARGO_PKG_VERSION").to_string(),
            agent_nickname: params.source.get_nickname(),
            agent_role: params.source.get_agent_role(),
            agent_path: params.source.get_agent_path().map(Into::into),
            source: params.source.clone(),
            thread_source: params.thread_source.clone(),
            model_provider: Some(params.metadata.model_provider.clone()),
            base_instructions: Some(params.base_instructions.clone()),
            dynamic_tools: (!params.dynamic_tools.is_empty()).then(|| params.dynamic_tools.clone()),
            selected_capability_roots: params.selected_capability_roots.clone(),
            memory_mode: matches!(params.metadata.memory_mode, ThreadMemoryMode::Disabled)
                .then_some("disabled".to_string()),
            history_mode: params.history_mode,
            history_base: params.history_base,
            subagent_history_start_ordinal: params.subagent_history_start_ordinal,
            multi_agent_version: params.multi_agent_version,
            context_window: Some(SessionContextWindow::new(params.initial_window_id.clone())),
        };
        Self {
            history_mode: params.history_mode,
            cwd,
            memory_mode: memory_mode_str(params.metadata.memory_mode),
            meta: Some(meta),
            pending: Vec::new(),
            staged: None,
            materialized: false,
            next_position: 0,
            ordinals: Ordinals::for_new(params.history_mode, params.history_base),
            ownership: None,
            sealed: false,
        }
    }

    /// A reopened thread already has its header and lines in storage.
    pub(crate) fn for_resume(
        history_mode: ThreadHistoryMode,
        cwd: PathBuf,
        memory_mode: ThreadMemoryMode,
        next_position: u64,
        last_ordinal: Option<u64>,
    ) -> Self {
        Self {
            history_mode,
            cwd,
            memory_mode: memory_mode_str(memory_mode),
            meta: None,
            pending: Vec::new(),
            staged: None,
            materialized: true,
            next_position,
            ordinals: Ordinals::resume(history_mode, last_ordinal),
            ownership: None,
            sealed: false,
        }
    }

    pub(crate) fn require_writer(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        if self.sealed
            || self
                .ownership
                .as_ref()
                .is_none_or(|owner| owner.lost.load(AtomicOrdering::SeqCst))
        {
            return Err(ownership_lost(thread_id));
        }
        Ok(())
    }

    /// Cancellation leaves the original JoinHandle in this retained instance.
    /// Task termination is not a receipt that remote SQL rollback has completed.
    pub(crate) async fn finish_renewal(&mut self) -> ThreadStoreResult<()> {
        if let Some(owner) = self.ownership.as_mut() {
            if !owner.renewal_joined {
                owner.renewer.abort();
                let result = (&mut owner.renewer).await;
                owner.renewal_joined = true;
                owner.renewal_failed = result.is_err_and(|error| !error.is_cancelled());
            }
            if owner.renewal_failed {
                return Err(ThreadStoreError::Internal {
                    message: "writer lease renewal task failed during cleanup".to_string(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn is_sealed(&self) -> bool {
        self.sealed
    }

    pub(crate) fn seal(&mut self) {
        self.sealed = true;
        if let Some(owner) = self.ownership.as_ref() {
            owner.renewer.abort();
        }
    }

    pub(crate) fn queue(
        &mut self,
        thread_id: ThreadId,
        items: Vec<RolloutItem>,
    ) -> ThreadStoreResult<()> {
        self.require_writer(thread_id)?;
        self.pending.extend(items);
        Ok(())
    }

    /// True while a newly created thread has written nothing and has nothing to write.
    pub(crate) fn is_deferred_and_empty(&self) -> bool {
        !self.materialized && self.pending.is_empty() && self.staged.is_none()
    }

    /// Write everything queued, materializing the thread on first use.
    pub(crate) async fn write(
        &mut self,
        pool: &PostgresPool,
        thread_id: ThreadId,
        default_model_provider_id: &str,
    ) -> ThreadStoreResult<()> {
        self.require_writer(thread_id)?;
        if self.staged.is_none() {
            let mut items = Vec::new();
            if let Some(meta) = self.meta.as_ref() {
                let git = if get_git_repo_root(&self.cwd).is_some() {
                    collect_git_info(&self.cwd)
                        .await
                        .map(|info| ProtocolGitInfo {
                            commit_hash: info.commit_hash,
                            branch: info.branch,
                            repository_url: info.repository_url,
                        })
                } else {
                    None
                };
                items.push(RolloutItem::SessionMeta(SessionMetaLine {
                    meta: meta.clone(),
                    git,
                }));
            }
            items.extend(self.pending.iter().cloned());
            if items.is_empty() {
                return Ok(());
            }
            let mut ordinals = self.ordinals;
            let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
            let mut lines = Vec::with_capacity(items.len());
            for item in items.iter().cloned() {
                let ordinal = ordinals.current()?;
                let line = RolloutLine {
                    timestamp: timestamp.clone(),
                    ordinal,
                    item,
                };
                let json =
                    serde_json::to_string(&line).map_err(|error| ThreadStoreError::Internal {
                        message: format!("failed to serialize a rollout line: {error}"),
                    })?;
                lines.push((ordinal, json));
                ordinals.advance();
            }
            self.staged = Some((lines, items));
            self.ordinals = ordinals;
            self.meta = None;
            self.pending.clear();
        }
        self.commit_staged(pool, thread_id, default_model_provider_id)
            .await
    }

    async fn commit_staged(
        &mut self,
        pool: &PostgresPool,
        thread_id: ThreadId,
        default_model_provider_id: &str,
    ) -> ThreadStoreResult<()> {
        let Some((lines, items)) = self.staged.clone() else {
            return Ok(());
        };
        let count = lines.len() as u64;
        if self
            .ownership
            .as_ref()
            .is_some_and(|ownership| ownership.lost.load(AtomicOrdering::SeqCst))
        {
            return Err(ownership_lost(thread_id));
        }
        let fence = self
            .ownership
            .as_ref()
            .map(|ownership| (ownership.claim.owner_id.clone(), ownership.claim.token));
        let expected = self.next_position;
        let memory_mode = self.memory_mode;
        let history_mode = self.history_mode;
        let default_provider = default_model_provider_id.to_string();
        let outcome: Result<(), ThreadStoreError> =
            async {
                let mut admitted = pool.begin_selected_write().await.map_err(|error| {
                    ThreadStoreError::Internal {
                        message: format!("PostgreSQL thread storage cannot accept writes: {error}"),
                    }
                })?;
                // The lease is checked while holding its row, so a takeover cannot slip in between
                // this check and the commit that follows it.
                if let Some((owner_id, token)) = fence.as_ref() {
                    let held: Option<i32> = sqlx::query_scalar(
                        "SELECT 1 FROM thread_writer_ownership \
                     WHERE thread_id = $1::uuid AND owner_id = $2::uuid AND token = $3 \
                       FOR SHARE",
                    )
                    .bind(thread_id.to_string())
                    .bind(owner_id)
                    .bind(token)
                    .fetch_optional(admitted.connection())
                    .await
                    .map_err(database)?;
                    if held.is_none() {
                        return Err(ownership_lost(thread_id));
                    }
                }
                // The catalog row follows the items it summarizes, in the same transaction.
                let existing = get_thread_in(admitted.connection(), thread_id, /*lock*/ true)
                    .await
                    .map_err(|error| ThreadStoreError::Internal {
                        message: format!("failed to read thread metadata: {error}"),
                    })?;
                // Both row waits can outlast an unchanged lease. Check the server clock only
                // after those waits; the held ownership row excludes takeover until commit.
                if let Some((owner_id, token)) = fence.as_ref() {
                    let unexpired: Option<bool> = sqlx::query_scalar(
                        "SELECT lease_until > clock_timestamp() FROM thread_writer_ownership \
                         WHERE thread_id = $1::uuid AND owner_id = $2::uuid AND token = $3",
                    )
                    .bind(thread_id.to_string())
                    .bind(owner_id)
                    .bind(token)
                    .fetch_optional(admitted.connection())
                    .await
                    .map_err(database)?;
                    if unexpired != Some(true) {
                        return Err(ownership_lost(thread_id));
                    }
                }
                let mut metadata = match existing {
                    Some(metadata) => metadata,
                    None => {
                        let builder = builder_from_items(&items, std::path::Path::new(""))
                            .ok_or_else(|| ThreadStoreError::Internal {
                                message: "the first rollout items carry no session metadata"
                                    .to_string(),
                            })?;
                        let mut metadata = builder.build(&default_provider);
                        metadata.rollout_path = PathBuf::new();
                        metadata.history_mode = history_mode;
                        metadata
                    }
                };
                for item in &items {
                    apply_rollout_item(&mut metadata, item, &default_provider);
                }
                metadata.updated_at = Utc::now();
                upsert_thread_in(admitted.connection(), &metadata, memory_mode)
                    .await
                    .map_err(|error| ThreadStoreError::Internal {
                        message: format!("failed to write thread metadata: {error}"),
                    })?;
                append_in(&mut admitted, thread_id, expected, &lines)
                    .await
                    .map_err(rollout_error)?;
                admitted.commit().await.map_err(database)?;
                Ok(())
            }
            .await;
        outcome?;
        self.next_position = expected + count;
        self.staged = None;
        self.materialized = true;
        Ok(())
    }
}

fn memory_mode_str(mode: ThreadMemoryMode) -> &'static str {
    match mode {
        ThreadMemoryMode::Enabled => "enabled",
        ThreadMemoryMode::Disabled => "disabled",
    }
}

fn database(error: sqlx::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: match error {
            sqlx::Error::Database(database) => format!(
                "database error {}",
                database.code().as_deref().unwrap_or("unknown")
            ),
            _ => "the database operation failed".to_string(),
        },
    }
}

pub(crate) fn rollout_error(error: RolloutStoreError) -> ThreadStoreError {
    match error {
        RolloutStoreError::MissingThread(thread_id) => {
            ThreadStoreError::ThreadNotFound { thread_id }
        }
        RolloutStoreError::Conflict { expected, stored } => ThreadStoreError::Conflict {
            message: format!(
                "another writer advanced the thread history: expected position {expected}, stored {stored}"
            ),
        },
        other => ThreadStoreError::Internal {
            message: other.to_string(),
        },
    }
}

/// Forked histories keep copied source headers after the new thread header, so the thread
/// contract comes from the first one.
pub(crate) fn canonical_history_mode(items: &[RolloutItem]) -> ThreadHistoryMode {
    items
        .iter()
        .find_map(|item| match item {
            RolloutItem::SessionMeta(meta_line) => Some(meta_line.meta.history_mode),
            _ => None,
        })
        .unwrap_or_default()
}

fn ownership_lost(thread_id: ThreadId) -> ThreadStoreError {
    ThreadStoreError::Conflict {
        message: format!("the writer lease for thread {thread_id} was lost"),
    }
}
