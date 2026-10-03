//! The PostgreSQL implementation of the thread store.
//!
//! Metadata lives in the thread catalog and canonical history in the rollout store, so a second
//! host with no local rollout files can list, resume, search and archive the same threads.
//! History uses the legacy contract: whole-history loads and the shared persistence policy.

use crate::adapters;
use crate::live::LiveThread;
use crate::live::WriterOwnership;
use crate::live::rollout_error;
use crate::meta::apply_patch;
use crate::meta::enum_to_string;
use crate::meta::stored_thread_from_metadata;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use codex_postgres_rollout_store::PostgresRolloutStore;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::ThreadOwnership;
use codex_postgres_runtime::ThreadOwnershipNamespace;
use codex_postgres_thread_catalog::PostgresThreadCatalog;
use codex_protocol::ThreadId;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutItem;
use codex_rollout::is_persisted_rollout_item;
use codex_rollout::parse_rollout_line;
use codex_state::Anchor;
use codex_state::SortDirection as StateSortDirection;
use codex_state::SortKey;
use codex_state::ThreadFilterOptions;
use codex_state::ThreadMetadata;
use codex_thread_store::AddThreadAttachmentOutcome;
use codex_thread_store::AddThreadAttachmentParams;
use codex_thread_store::AppendThreadItemsParams;
use codex_thread_store::ArchiveThreadParams;
use codex_thread_store::ArchiveThreadsParams;
use codex_thread_store::CreateProjectParams;
use codex_thread_store::CreateThreadParams;
use codex_thread_store::CreateThreadSectionParams;
use codex_thread_store::CreatedProject;
use codex_thread_store::DeleteThreadParams;
use codex_thread_store::DeleteThreadSectionParams;
use codex_thread_store::DeleteThreadsParams;
use codex_thread_store::DeletedProject;
use codex_thread_store::ListProjectsParams;
use codex_thread_store::ListThreadAttachmentsParams;
use codex_thread_store::ListThreadSectionsParams;
use codex_thread_store::ListThreadsParams;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::MoveProjectParams;
use codex_thread_store::MoveThreadToSectionParams;
use codex_thread_store::PersistContext;
use codex_thread_store::ProjectMoveOutcome;
use codex_thread_store::ReadThreadByRolloutPathParams;
use codex_thread_store::ReadThreadParams;
use codex_thread_store::RemoveThreadAttachmentOutcome;
use codex_thread_store::RemoveThreadAttachmentParams;
use codex_thread_store::RenameThreadSectionParams;
use codex_thread_store::ResumeThreadParams;
use codex_thread_store::StoredModelContext;
use codex_thread_store::StoredProject;
use codex_thread_store::StoredProjectsPage;
use codex_thread_store::StoredThread;
use codex_thread_store::StoredThreadHistory;
use codex_thread_store::StoredThreadSection;
use codex_thread_store::StoredThreadSectionsPage;
use codex_thread_store::ThreadAttachmentPage;
use codex_thread_store::ThreadMetadataPatch;
use codex_thread_store::ThreadPage;
use codex_thread_store::ThreadRelationFilter;
use codex_thread_store::ThreadSortKey;
use codex_thread_store::ThreadStore;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreFuture;
use codex_thread_store::ThreadStoreResult;
use codex_thread_store::UpdateProjectParams;
use codex_thread_store::UpdateThreadMetadataParams;
use codex_thread_store::UpdatedProject;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;

/// Fixed-namespace thread store. Construction does not activate PostgreSQL.
pub struct PostgresThreadStore {
    pool: Arc<PostgresPool>,
    catalog: PostgresThreadCatalog,
    rollouts: PostgresRolloutStore,
    default_model_provider_id: String,
    live: Mutex<HashMap<ThreadId, Arc<Mutex<LiveThread>>>>,
    pending_metadata: Mutex<HashMap<ThreadId, ThreadMetadataPatch>>,
    writer_lease: Duration,
    renew_every: Duration,
    namespace: ThreadOwnershipNamespace,
}

fn internal(error: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: error.to_string(),
    }
}

impl PostgresThreadStore {
    pub fn new(pool: Arc<PostgresPool>, default_model_provider_id: impl Into<String>) -> Self {
        Self {
            catalog: PostgresThreadCatalog::new(pool.clone()),
            rollouts: PostgresRolloutStore::new(pool.clone()),
            pool,
            default_model_provider_id: default_model_provider_id.into(),
            live: Mutex::default(),
            pending_metadata: Mutex::default(),
            writer_lease: Duration::from_secs(30),
            renew_every: Duration::from_secs(10),
            namespace: ThreadOwnershipNamespace::Default,
        }
    }

    /// Claim writer leases in the same namespace as the store's data.
    pub fn with_namespace(mut self, namespace: ThreadOwnershipNamespace) -> Self {
        self.namespace = namespace;
        self
    }

    /// Tune the writer lease. The lease must outlast several renewals so a brief stall does not
    /// cost a healthy writer its thread.
    pub fn with_writer_lease(mut self, lease: Duration, renew_every: Duration) -> Self {
        self.writer_lease = lease;
        self.renew_every = renew_every;
        self
    }

    /// Become the only writer of a thread across every host, or learn that someone else is.
    async fn claim_writer(&self, thread_id: ThreadId) -> ThreadStoreResult<WriterOwnership> {
        let busy = || ThreadStoreError::InvalidRequest {
            message: format!("thread {thread_id} already has a live writer"),
        };
        let namespace = self.namespace.clone();
        let id = thread_id.to_string();
        let attempt = self
            .pool
            .prepare_thread_ownership_attempt(namespace, &id)
            .await
            .map_err(internal)?;
        let claim = match self
            .pool
            .claim_thread_ownership_attempt(&attempt, self.writer_lease)
            .await
        {
            Ok(Some(claim)) => claim,
            Ok(None) => return Err(busy()),
            // An unknown commit outcome is resolved by reading back this attempt's owner id.
            Err(_) => self
                .pool
                .recover_thread_ownership_attempt(&attempt)
                .await
                .map_err(internal)?
                .ok_or_else(busy)?,
        };
        let lost = Arc::new(AtomicBool::new(false));
        let renewer = tokio::spawn(renew_lease(
            self.pool.clone(),
            claim.clone(),
            self.writer_lease,
            self.renew_every,
            lost.clone(),
        ));
        Ok(WriterOwnership {
            claim,
            lost,
            renewer,
            renewal_joined: false,
            renewal_failed: false,
        })
    }

    async fn release_writer(&self, ownership: Option<WriterOwnership>) {
        if let Some(ownership) = ownership {
            ownership.renewer.abort();
            if let Err(error) = self.pool.release_thread_ownership(&ownership.claim).await {
                tracing::warn!("failed to release a writer lease: {error}");
            }
        }
    }

    async fn live_thread(&self, thread_id: ThreadId) -> ThreadStoreResult<Arc<Mutex<LiveThread>>> {
        self.live
            .lock()
            .await
            .get(&thread_id)
            .cloned()
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })
    }

    async fn catalog_thread(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<Option<ThreadMetadata>> {
        self.catalog.get_thread(thread_id).await.map_err(internal)
    }

    /// Decode every stored line of a thread in replay order.
    async fn load_items(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreResult<(Vec<RolloutItem>, u64, Option<u64>)> {
        let lines = self
            .rollouts
            .read_all(thread_id)
            .await
            .map_err(rollout_error)?;
        let next_position = lines.last().map_or(0, |line| line.position + 1);
        let last_ordinal = lines.iter().rev().find_map(|line| line.ordinal);
        let items = lines
            .iter()
            .map(|line| {
                parse_rollout_line(&line.line)
                    .map(|parsed| parsed.item)
                    .map_err(|error| ThreadStoreError::Internal {
                        message: format!(
                            "stored rollout line {} of thread {thread_id} is unreadable: {error}",
                            line.position
                        ),
                    })
            })
            .collect::<ThreadStoreResult<Vec<_>>>()?;
        Ok((items, next_position, last_ordinal))
    }

    async fn session_meta(&self, thread_id: ThreadId) -> ThreadStoreResult<Option<SessionMeta>> {
        let first = self
            .rollouts
            .read(thread_id, 0, 1)
            .await
            .map_err(rollout_error)?;
        let Some(line) = first.first() else {
            return Ok(None);
        };
        let parsed = parse_rollout_line(&line.line).map_err(internal)?;
        Ok(match parsed.item {
            RolloutItem::SessionMeta(meta_line) => Some(meta_line.meta),
            _ => None,
        })
    }

    fn revision(thread_id: ThreadId, next_position: u64) -> String {
        format!("postgres:{thread_id}:{next_position}")
    }

    async fn stored_thread(
        &self,
        thread_id: ThreadId,
        include_archived: bool,
        include_history: bool,
    ) -> ThreadStoreResult<StoredThread> {
        let missing = || ThreadStoreError::InvalidRequest {
            message: format!("no rollout found for thread id {thread_id}"),
        };
        let metadata = self.catalog_thread(thread_id).await?.ok_or_else(missing)?;
        if metadata.archived_at.is_some() && !include_archived {
            return Err(missing());
        }
        let meta = self.session_meta(thread_id).await?;
        let parent_thread_id = meta.as_ref().and_then(|meta| meta.parent_thread_id);
        let forked_from_id = meta.as_ref().and_then(|meta| meta.forked_from_id);
        let mut thread = stored_thread_from_metadata(
            metadata,
            &self.default_model_provider_id,
            parent_thread_id,
            forked_from_id,
        );
        if let Some(meta) = meta {
            thread.history_mode = meta.history_mode;
            thread.originator = thread
                .originator
                .or_else(|| Some(meta.originator).filter(|originator| !originator.is_empty()));
        }
        if include_history {
            let (items, next_position, _) = self.load_items(thread_id).await?;
            thread.history = Some(StoredThreadHistory {
                thread_id,
                items,
                revision: Some(Self::revision(thread_id, next_position)),
            });
        }
        Ok(thread)
    }

    async fn write_live(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let live = self.live_thread(thread_id).await?;
        let mut live = live.lock_owned().await;
        live.write(&self.pool, thread_id, &self.default_model_provider_id)
            .await
    }

    async fn apply_metadata_update(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreResult<StoredThread> {
        let thread_id = params.thread_id;
        let mut patch = params.patch;
        if let Some(staged) = self.pending_metadata.lock().await.get(&thread_id).cloned() {
            let mut merged = staged;
            merged.merge(patch);
            patch = merged;
        }
        if patch.is_empty() {
            return self
                .stored_thread(thread_id, params.include_archived, false)
                .await;
        }
        let mut existing = self.catalog_thread(thread_id).await?;
        if existing.is_none() {
            // A thread that is still only in memory becomes durable before it can be patched.
            if self.live.lock().await.contains_key(&thread_id) {
                self.write_live(thread_id).await?;
                existing = self.catalog_thread(thread_id).await?;
            }
        }
        let mut metadata = existing.ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("thread not found: {thread_id}"),
        })?;
        let history_mode = metadata.history_mode;
        let name = patch.name.clone();
        let project_id = patch.project_id.clone();
        let git_info = patch.git_info.clone();
        let memory_mode = patch.memory_mode;
        let advance_recency_at = patch.advance_recency_at;
        let daybreak_enabled = patch.daybreak_enabled;
        // Names are stored per history contract, as the local store does.
        let mut generic = patch;
        generic.name = None;
        generic.project_id = None;
        apply_patch(&mut metadata, generic);
        self.catalog
            .upsert_thread(&metadata)
            .await
            .map_err(internal)?;
        if let Some(git_info) = git_info {
            self.catalog
                .update_thread_git_info(
                    thread_id,
                    git_info.sha.as_ref().map(|sha| sha.as_deref()),
                    git_info.branch.as_ref().map(|branch| branch.as_deref()),
                    git_info.origin_url.as_ref().map(|url| url.as_ref()),
                )
                .await
                .map_err(internal)?;
        }
        if let Some(project_id) = project_id {
            self.catalog
                .set_thread_project(&thread_id.to_string(), project_id.as_deref())
                .await
                .map_err(|error| {
                    let message = error.to_string();
                    if message.contains("project not found") {
                        ThreadStoreError::InvalidRequest { message }
                    } else {
                        internal(message)
                    }
                })?;
        }
        if let Some(name) = name {
            match history_mode {
                ThreadHistoryMode::Legacy => {
                    self.catalog
                        .update_thread_title(thread_id, name.as_deref().unwrap_or_default())
                        .await
                }
                ThreadHistoryMode::Paginated => {
                    self.catalog
                        .update_thread_name(thread_id, name.as_deref())
                        .await
                }
            }
            .map_err(internal)?;
        }
        if let Some(recency_at) = advance_recency_at {
            self.catalog
                .touch_thread_recency_at(thread_id, recency_at)
                .await
                .map_err(internal)?;
        }
        if let Some(memory_mode) = memory_mode {
            self.catalog
                .set_thread_memory_mode(
                    thread_id,
                    match memory_mode {
                        ThreadMemoryMode::Enabled => "enabled",
                        ThreadMemoryMode::Disabled => "disabled",
                    },
                )
                .await
                .map_err(internal)?;
        }
        if let Some(daybreak_enabled) = daybreak_enabled {
            self.catalog
                .set_thread_daybreak_enabled(thread_id, daybreak_enabled)
                .await
                .map_err(internal)?;
        }
        self.pending_metadata.lock().await.remove(&thread_id);
        self.stored_thread(thread_id, params.include_archived, false)
            .await
    }
}

fn encode_cursor(anchor: &Anchor) -> String {
    match anchor.id {
        Some(id) => format!("{}|{id}", anchor.ts.timestamp_millis()),
        None => anchor.ts.timestamp_millis().to_string(),
    }
}

fn decode_cursor(cursor: &str) -> ThreadStoreResult<Anchor> {
    let invalid = || ThreadStoreError::InvalidRequest {
        message: format!("invalid cursor: {cursor}"),
    };
    let (millis, id) = match cursor.split_once('|') {
        Some((millis, id)) => (
            millis,
            Some(ThreadId::from_string(id).map_err(|_| invalid())?),
        ),
        None => (cursor, None),
    };
    let millis: i64 = millis.parse().map_err(|_| invalid())?;
    let ts: DateTime<Utc> = Utc
        .timestamp_millis_opt(millis)
        .single()
        .ok_or_else(invalid)?;
    Ok(Anchor { ts, id })
}

impl ThreadStore for PostgresThreadStore {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn create_thread(&self, params: CreateThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let thread_id = params.thread_id;
            if self.live.lock().await.contains_key(&thread_id) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("thread {thread_id} already has a live writer"),
                });
            }
            let ownership = self.claim_writer(thread_id).await?;
            let mut live_thread = LiveThread::for_create(&params);
            live_thread.ownership = Some(ownership);
            let rejected = {
                let mut live = self.live.lock().await;
                match live.entry(thread_id) {
                    std::collections::hash_map::Entry::Occupied(_) => Some(live_thread),
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert(Arc::new(Mutex::new(live_thread)));
                        None
                    }
                }
            };
            if let Some(mut rejected) = rejected {
                self.release_writer(rejected.ownership.take()).await;
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("thread {thread_id} already has a live writer"),
                });
            }
            Ok(())
        })
    }

    fn stage_pending_thread_metadata(
        &self,
        thread_id: ThreadId,
        patch: ThreadMetadataPatch,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            if patch.rollout_path.is_some() {
                return Err(ThreadStoreError::InvalidRequest {
                    message: "pending thread metadata cannot set rollout_path".to_string(),
                });
            }
            self.pending_metadata.lock().await.insert(thread_id, patch);
            Ok(())
        })
    }

    fn read_pending_thread_metadata(
        &self,
        thread_id: ThreadId,
    ) -> ThreadStoreFuture<'_, Option<ThreadMetadataPatch>> {
        Box::pin(async move { Ok(self.pending_metadata.lock().await.get(&thread_id).cloned()) })
    }

    fn remove_pending_thread_metadata(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            self.pending_metadata.lock().await.remove(&thread_id);
            Ok(())
        })
    }

    fn resume_thread(
        &self,
        params: ResumeThreadParams,
    ) -> ThreadStoreFuture<'_, Arc<Vec<RolloutItem>>> {
        Box::pin(async move {
            let thread_id = params.thread_id;
            if self.live.lock().await.contains_key(&thread_id) {
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("thread {thread_id} already has a live writer"),
                });
            }
            let ownership = self.claim_writer(thread_id).await?;
            let loaded = async {
                let metadata = self
                    .catalog_thread(thread_id)
                    .await?
                    .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
                if metadata.archived_at.is_some() && !params.include_archived {
                    return Err(ThreadStoreError::InvalidRequest {
                        message: format!("thread {thread_id} is archived"),
                    });
                }
                self.load_items(thread_id).await
            }
            .await;
            let (stored, next_position, last_ordinal) = match loaded {
                Ok(loaded) => loaded,
                Err(error) => {
                    self.release_writer(Some(ownership)).await;
                    return Err(error);
                }
            };
            let revision = Self::revision(thread_id, next_position);
            let history = match params.history {
                Some(history)
                    if !matches!(
                        history.first(),
                        Some(RolloutItem::SessionMeta(meta)) if meta.meta.id == thread_id
                    ) =>
                {
                    history
                }
                Some(history) if params.history_revision.as_deref() == Some(revision.as_str()) => {
                    history
                }
                _ => Arc::new(stored),
            };
            let history_mode = crate::live::canonical_history_mode(&history);
            let cwd = params.metadata.cwd.clone().unwrap_or_default();
            let mut live = LiveThread::for_resume(
                history_mode,
                cwd,
                params.metadata.memory_mode,
                next_position,
                last_ordinal,
            );
            live.ownership = Some(ownership);
            let rejected = {
                let mut live_threads = self.live.lock().await;
                match live_threads.entry(thread_id) {
                    std::collections::hash_map::Entry::Occupied(_) => Some(live),
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert(Arc::new(Mutex::new(live)));
                        None
                    }
                }
            };
            if let Some(mut rejected) = rejected {
                self.release_writer(rejected.ownership.take()).await;
                return Err(ThreadStoreError::InvalidRequest {
                    message: format!("thread {thread_id} already has a live writer"),
                });
            }
            Ok(history)
        })
    }

    fn append_items(&self, params: AppendThreadItemsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let thread_id = params.thread_id;
            let live = self.live_thread(thread_id).await?;
            let mut live = live.lock_owned().await;
            live.require_writer(thread_id)?;
            let history_mode = live.history_mode;
            let items: Vec<RolloutItem> = params
                .items
                .into_iter()
                .filter(|item| is_persisted_rollout_item(item, history_mode))
                .collect();
            if items.is_empty() {
                return Ok(());
            }
            live.queue(thread_id, items)?;
            // Once the thread is durable every append is acknowledged only after it commits.
            if live.materialized {
                live.write(&self.pool, thread_id, &self.default_model_provider_id)
                    .await?;
            }
            Ok(())
        })
    }

    fn persist_thread(
        &self,
        thread_id: ThreadId,
        context: PersistContext,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            match context {
                PersistContext::SubagentSpawn => Ok(()),
                PersistContext::ThreadPreparation => self.flush_thread_inner(thread_id).await,
                PersistContext::Standard
                | PersistContext::TurnStart
                | PersistContext::SteeredUserInput => self.write_live(thread_id).await,
            }
        })
    }

    fn flush_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(self.flush_thread_inner(thread_id))
    }

    fn shutdown_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let original = self.live_thread(thread_id).await?;
            let mut live = original.lock().await;
            if !live.is_sealed() {
                live.require_writer(thread_id)?;
                if !live.is_deferred_and_empty() {
                    live.write(&self.pool, thread_id, &self.default_model_provider_id)
                        .await?;
                }
                live.seal();
            }
            // Original remains in the registry through canceled/failed release; a retry
            // completes cleanup without reopening or rewriting the sealed instance.
            live.finish_renewal().await?;
            if let Some(owner) = live.ownership.as_ref() {
                self.pool
                    .release_thread_ownership(&owner.claim)
                    .await
                    .map_err(internal)?;
            }
            self.evict_live_instance(thread_id, &original).await;
            Ok(())
        })
    }

    fn discard_thread(&self, thread_id: ThreadId) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let original = self.live_thread(thread_id).await?;
            let mut live = original.lock().await;
            live.seal();
            live.finish_renewal().await?;
            if let Some(owner) = live.ownership.as_ref() {
                self.pool
                    .release_thread_ownership(&owner.claim)
                    .await
                    .map_err(internal)?;
            }
            self.pending_metadata.lock().await.remove(&thread_id);
            self.evict_live_instance(thread_id, &original).await;
            Ok(())
        })
    }

    fn load_history(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredThreadHistory> {
        Box::pin(async move {
            let thread = self
                .stored_thread(
                    params.thread_id,
                    params.include_archived,
                    /*include_history*/ true,
                )
                .await?;
            thread.history.ok_or(ThreadStoreError::ThreadNotFound {
                thread_id: params.thread_id,
            })
        })
    }

    fn load_latest_model_context(
        &self,
        params: LoadThreadHistoryParams,
    ) -> ThreadStoreFuture<'_, StoredModelContext> {
        Box::pin(async move {
            let history = self
                .stored_thread(params.thread_id, params.include_archived, true)
                .await?
                .history
                .ok_or(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                })?;
            Ok(StoredModelContext {
                thread_id: history.thread_id,
                items: history.items,
                revision: history.revision,
            })
        })
    }

    fn read_thread(&self, params: ReadThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(self.stored_thread(
            params.thread_id,
            params.include_archived,
            params.include_history,
        ))
    }

    fn read_thread_by_rollout_path(
        &self,
        _params: ReadThreadByRolloutPathParams,
    ) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async {
            Err(ThreadStoreError::Unsupported {
                operation: "read_thread_by_rollout_path",
            })
        })
    }

    fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreFuture<'_, ThreadPage> {
        Box::pin(async move {
            let anchor = params.cursor.as_deref().map(decode_cursor).transpose()?;
            let sources: Vec<String> = params.allowed_sources.iter().map(enum_to_string).collect();
            let model_providers = match &params.model_providers {
                None => Some(vec![self.default_model_provider_id.clone()]),
                Some(providers) if providers.is_empty() => None,
                Some(providers) => Some(providers.clone()),
            };
            let sort_key = match params.sort_key {
                ThreadSortKey::CreatedAt => SortKey::CreatedAt,
                ThreadSortKey::UpdatedAt => SortKey::UpdatedAt,
                ThreadSortKey::RecencyAt => SortKey::RecencyAt,
                ThreadSortKey::SectionPosition => SortKey::SectionPosition,
            };
            let sort_direction = match params.sort_direction {
                codex_thread_store::SortDirection::Asc => StateSortDirection::Asc,
                codex_thread_store::SortDirection::Desc => StateSortDirection::Desc,
            };
            let cwd_filters = params.cwd_filters.clone();
            let filters = ThreadFilterOptions {
                archived_only: params.archived,
                allowed_sources: &sources,
                model_providers: model_providers.as_deref(),
                cwd_filters: cwd_filters.as_deref(),
                section: params.section.as_ref().map(Option::as_deref),
                project_id: params.project_id.as_ref().map(Option::as_deref),
                anchor: anchor.as_ref(),
                sort_key,
                sort_direction,
                search_term: params.search_term.as_deref(),
            };
            let page = match params.relation_filter {
                Some(ThreadRelationFilter::DirectChildrenOf(parent)) => {
                    self.catalog
                        .list_threads_by_parent(params.page_size, parent, filters)
                        .await
                }
                Some(ThreadRelationFilter::DescendantsOf(ancestor)) => {
                    self.catalog
                        .list_threads_by_relation(
                            params.page_size,
                            codex_state::ThreadRelationFilter::DescendantsOf(ancestor),
                            filters,
                        )
                        .await
                }
                None => self.catalog.list_threads(params.page_size, filters).await,
            }
            .map_err(internal)?;
            let items = page
                .items
                .into_iter()
                .map(|metadata| {
                    let parent = page.parent_thread_ids.get(&metadata.id).copied();
                    stored_thread_from_metadata(
                        metadata,
                        &self.default_model_provider_id,
                        parent,
                        None,
                    )
                })
                .collect();
            Ok(ThreadPage {
                items,
                next_cursor: page.next_anchor.as_ref().map(encode_cursor),
            })
        })
    }

    fn supports_thread_sections(&self) -> bool {
        true
    }

    fn list_thread_sections(
        &self,
        params: ListThreadSectionsParams,
    ) -> ThreadStoreFuture<'_, StoredThreadSectionsPage> {
        Box::pin(adapters::list_thread_sections(&self.catalog, params))
    }

    fn create_thread_section(
        &self,
        params: CreateThreadSectionParams,
    ) -> ThreadStoreFuture<'_, StoredThreadSection> {
        Box::pin(adapters::create_thread_section(&self.catalog, params))
    }

    fn rename_thread_section(
        &self,
        params: RenameThreadSectionParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThreadSection>> {
        Box::pin(adapters::rename_thread_section(&self.catalog, params))
    }

    fn delete_thread_section(
        &self,
        params: DeleteThreadSectionParams,
    ) -> ThreadStoreFuture<'_, bool> {
        Box::pin(adapters::delete_thread_section(&self.catalog, params))
    }

    fn supports_thread_attachments(&self) -> bool {
        true
    }

    fn copy_thread_attachments(
        &self,
        source_thread_id: ThreadId,
        destination_thread_id: ThreadId,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(adapters::copy_thread_attachments(
            &self.catalog,
            source_thread_id,
            destination_thread_id,
        ))
    }

    fn add_thread_attachment(
        &self,
        params: AddThreadAttachmentParams,
    ) -> ThreadStoreFuture<'_, AddThreadAttachmentOutcome> {
        Box::pin(adapters::add_thread_attachment(&self.catalog, params))
    }

    fn list_thread_attachments(
        &self,
        params: ListThreadAttachmentsParams,
    ) -> ThreadStoreFuture<'_, ThreadAttachmentPage> {
        Box::pin(adapters::list_thread_attachments(&self.catalog, params))
    }

    fn remove_thread_attachment(
        &self,
        params: RemoveThreadAttachmentParams,
    ) -> ThreadStoreFuture<'_, RemoveThreadAttachmentOutcome> {
        Box::pin(adapters::remove_thread_attachment(&self.catalog, params))
    }

    fn supports_projects(&self) -> bool {
        true
    }

    fn list_projects(
        &self,
        params: ListProjectsParams,
    ) -> ThreadStoreFuture<'_, StoredProjectsPage> {
        Box::pin(adapters::list_projects(&self.catalog, params))
    }

    fn read_project(&self, project_id: String) -> ThreadStoreFuture<'_, Option<StoredProject>> {
        Box::pin(adapters::read_project(&self.catalog, project_id))
    }

    fn create_project(&self, params: CreateProjectParams) -> ThreadStoreFuture<'_, CreatedProject> {
        Box::pin(adapters::create_project(&self.catalog, params))
    }

    fn update_project(
        &self,
        params: UpdateProjectParams,
    ) -> ThreadStoreFuture<'_, Option<UpdatedProject>> {
        Box::pin(adapters::update_project(&self.catalog, params))
    }

    fn move_project(
        &self,
        params: MoveProjectParams,
    ) -> ThreadStoreFuture<'_, Option<ProjectMoveOutcome>> {
        Box::pin(adapters::move_project(&self.catalog, params))
    }

    fn delete_project(&self, project_id: String) -> ThreadStoreFuture<'_, Option<DeletedProject>> {
        Box::pin(adapters::delete_project(&self.catalog, project_id))
    }

    fn update_thread_metadata(
        &self,
        params: UpdateThreadMetadataParams,
    ) -> ThreadStoreFuture<'_, Option<StoredThread>> {
        Box::pin(async move { self.apply_metadata_update(params).await.map(Some) })
    }

    fn move_thread_to_section(
        &self,
        params: MoveThreadToSectionParams,
    ) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            if params
                .section
                .as_deref()
                .is_some_and(|section| section.trim().is_empty())
            {
                return Err(ThreadStoreError::InvalidRequest {
                    message: "section must not be empty".to_owned(),
                });
            }
            let moved = self
                .catalog
                .move_thread_to_section(
                    params.thread_id,
                    params.section.as_deref(),
                    params.before_thread_id,
                )
                .await
                .map_err(|error| ThreadStoreError::InvalidRequest {
                    message: error.to_string(),
                })?;
            if moved {
                Ok(())
            } else {
                Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                })
            }
        })
    }

    fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move { self.archive_one(params.thread_id).await })
    }

    fn archive_threads(
        &self,
        params: ArchiveThreadsParams,
    ) -> ThreadStoreFuture<'_, Vec<ThreadId>> {
        Box::pin(async move {
            let mut archived = Vec::new();
            for thread_id in params.thread_ids {
                match self.archive_one(thread_id).await {
                    Ok(()) => archived.push(thread_id),
                    Err(error) if archived.is_empty() => return Err(error),
                    Err(error) => tracing::warn!(
                        "failed to archive spawned descendant thread {thread_id}: {error}"
                    ),
                }
            }
            Ok(archived)
        })
    }

    fn unarchive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreFuture<'_, StoredThread> {
        Box::pin(async move {
            let thread_id = params.thread_id;
            let missing = || ThreadStoreError::InvalidRequest {
                message: format!("no archived rollout found for thread id {thread_id}"),
            };
            let metadata = self.catalog_thread(thread_id).await?.ok_or_else(missing)?;
            if metadata.archived_at.is_none() {
                return Err(missing());
            }
            self.catalog
                .mark_unarchived(thread_id, Path::new(""))
                .await
                .map_err(internal)?;
            self.stored_thread(thread_id, /*include_archived*/ false, false)
                .await
        })
    }

    fn delete_thread(&self, params: DeleteThreadParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let originals = self.seal_live_instances(&[params.thread_id]).await?;
            let deleted = self
                .catalog
                .delete_thread(params.thread_id)
                .await
                .map_err(internal)?;
            self.pending_metadata.lock().await.remove(&params.thread_id);
            for (id, original) in originals {
                self.evict_live_instance(id, &original).await;
            }
            if deleted == 0 {
                Err(ThreadStoreError::ThreadNotFound {
                    thread_id: params.thread_id,
                })
            } else {
                Ok(())
            }
        })
    }

    fn delete_threads(&self, params: DeleteThreadsParams) -> ThreadStoreFuture<'_, ()> {
        Box::pin(async move {
            let originals = self.seal_live_instances(&params.thread_ids).await?;
            self.catalog
                .delete_threads_strict(&params.thread_ids)
                .await
                .map_err(internal)?;
            for (id, original) in originals {
                self.pending_metadata.lock().await.remove(&id);
                self.evict_live_instance(id, &original).await;
            }
            Ok(())
        })
    }
}

impl PostgresThreadStore {
    async fn seal_live_instances(
        &self,
        ids: &[ThreadId],
    ) -> ThreadStoreResult<Vec<(ThreadId, Arc<Mutex<LiveThread>>)>> {
        let originals: Vec<_> = {
            let instances = self.live.lock().await;
            ids.iter()
                .filter_map(|id| instances.get(id).map(|live| (*id, Arc::clone(live))))
                .collect()
        };
        for (_, original) in &originals {
            let mut live = original.lock().await;
            live.seal();
            live.finish_renewal().await?;
        }
        Ok(originals)
    }

    async fn evict_live_instance(&self, thread_id: ThreadId, original: &Arc<Mutex<LiveThread>>) {
        let mut instances = self.live.lock().await;
        if instances
            .get(&thread_id)
            .is_some_and(|current| Arc::ptr_eq(current, original))
        {
            instances.remove(&thread_id);
        }
    }

    async fn flush_thread_inner(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        let live = self.live_thread(thread_id).await?;
        let mut live = live.lock_owned().await;
        live.require_writer(thread_id)?;
        if live.is_deferred_and_empty() {
            return Ok(());
        }
        live.write(&self.pool, thread_id, &self.default_model_provider_id)
            .await
    }

    async fn archive_one(&self, thread_id: ThreadId) -> ThreadStoreResult<()> {
        if self.live.lock().await.contains_key(&thread_id) {
            return Err(ThreadStoreError::Conflict {
                message: format!("thread {thread_id} already has an active writer"),
            });
        }
        let missing = || ThreadStoreError::InvalidRequest {
            message: format!("no rollout found for thread id {thread_id}"),
        };
        let metadata = self.catalog_thread(thread_id).await?.ok_or_else(missing)?;
        if metadata.archived_at.is_some() {
            return Err(missing());
        }
        self.catalog
            .mark_archived(thread_id, Path::new(""), Utc::now())
            .await
            .map_err(internal)
    }
}

/// Keep a writer lease alive. The writer is marked lost when a renewal is refused or when the
/// lease has gone unrenewed for a full term, because after that another host may own the thread.
async fn renew_lease(
    pool: Arc<PostgresPool>,
    claim: ThreadOwnership,
    lease: Duration,
    renew_every: Duration,
    lost: Arc<AtomicBool>,
) {
    let mut last_success = Instant::now();
    loop {
        tokio::time::sleep(renew_every).await;
        match pool.renew_thread_ownership(&claim, lease).await {
            Ok(true) => last_success = Instant::now(),
            Ok(false) => {
                lost.store(true, AtomicOrdering::SeqCst);
                return;
            }
            Err(_) if last_success.elapsed() >= lease => {
                lost.store(true, AtomicOrdering::SeqCst);
                return;
            }
            Err(_) => {}
        }
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
