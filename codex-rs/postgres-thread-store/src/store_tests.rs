use super::*;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_protocol::models::BaseInstructions;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::UserMessageEvent;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::SortDirection;
use codex_thread_store::ThreadPersistenceMetadata;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::time::Duration;

fn settings(state: &Path, role: &str) -> ConnectionSettings {
    let receipt: Value = serde_json::from_slice(
        &std::fs::read(state.join("receipt.json")).expect("read isolated PostgreSQL receipt"),
    )
    .expect("parse PostgreSQL receipt");
    ConnectionSettings {
        host: "localhost".to_string(),
        port: receipt["port"].as_u64().expect("PostgreSQL port") as u16,
        database: "codex".to_string(),
        username: format!("codex_{role}"),
        password: std::fs::read_to_string(state.join(format!("secrets/{role}.password")))
            .expect("read private role credential")
            .trim()
            .to_string()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: std::time::Duration::from_secs(5),
            acquire_timeout: std::time::Duration::from_secs(20),
            max_connections: 12,
        },
    }
}

async fn connect(state: &Path, role: &str) -> Arc<PostgresPool> {
    let pool = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, role), None)
            .await
            .unwrap_or_else(|error| panic!("{role} pool: {error:?}")),
    );
    if role == "runtime" {
        pool.bind_current_activation()
            .await
            .unwrap_or_else(|error| panic!("actual runtime activation binding: {error}"));
    }
    pool
}

fn create_params(thread_id: ThreadId, provider: &str, cwd: &Path) -> CreateThreadParams {
    CreateThreadParams {
        creator_user_id: None,
        creator_account_id: None,
        session_id: thread_id.into(),
        thread_id,
        extra_config: None,
        forked_from_id: None,
        parent_thread_id: None,
        source: SessionSource::Exec,
        thread_source: None,
        originator: "test_originator".to_string(),
        base_instructions: BaseInstructions::default(),
        dynamic_tools: Vec::new(),
        selected_capability_roots: Vec::new(),
        multi_agent_version: None,
        history_mode: ThreadHistoryMode::Legacy,
        history_base: None,
        subagent_history_start_ordinal: None,
        initial_window_id: "window-1".to_string(),
        runtime_workspace_roots: None,
        metadata: ThreadPersistenceMetadata {
            cwd: Some(cwd.to_path_buf()),
            model_provider: provider.to_string(),
            memory_mode: ThreadMemoryMode::Enabled,
        },
    }
}

fn user_message(message: &str) -> RolloutItem {
    RolloutItem::EventMsg(EventMsg::UserMessage(UserMessageEvent {
        client_id: None,
        message: message.to_string(),
        images: None,
        local_images: Vec::new(),
        text_elements: Vec::new(),
        ..Default::default()
    }))
}

fn describe_item(item: &RolloutItem) -> String {
    match item {
        RolloutItem::SessionMeta(line) => format!(
            "session_meta {} {:?} {:?} {:?}",
            line.meta.id, line.meta.history_mode, line.meta.source, line.meta.memory_mode
        ),
        other => serde_json::to_string(other).expect("serialize item"),
    }
}

fn describe_thread(thread: &StoredThread) -> String {
    format!(
        "{} preview {:?} name {:?} provider {} archived {} section {:?} project {:?} \
         cwd {} source {:?} mode {:?} first {:?} approval {:?} history {}",
        thread.thread_id,
        thread.preview,
        thread.name,
        thread.model_provider,
        thread.archived_at.is_some(),
        thread.section.as_ref().map(|section| section.name.clone()),
        thread.project_id,
        thread.cwd.display(),
        thread.source,
        thread.history_mode,
        thread.first_user_message,
        thread.approval_mode,
        thread
            .history
            .as_ref()
            .map(|history| history
                .items
                .iter()
                .map(describe_item)
                .collect::<Vec<_>>()
                .join(" | "))
            .unwrap_or_default(),
    )
}

fn outcome<T>(result: ThreadStoreResult<T>, show: impl FnOnce(&T) -> String) -> String {
    match result {
        Ok(value) => format!("ok {}", show(&value)),
        Err(error) => format!("err {error}"),
    }
}

/// Runs a thread lifecycle through the shared trait and records each observable result.
async fn scenario(
    store: &dyn ThreadStore,
    provider: &str,
    cwd: &Path,
    ids: [ThreadId; 3],
) -> Vec<String> {
    let mut log = Vec::new();
    let [first, second, missing] = ids;

    log.push(outcome(
        store
            .create_thread(create_params(first, provider, cwd))
            .await,
        |_| String::new(),
    ));
    log.push(outcome(
        store
            .create_thread(create_params(first, provider, cwd))
            .await,
        |_| String::new(),
    ));
    // Nothing is durable until a barrier, so the thread cannot be read yet.
    log.push(outcome(
        store
            .read_thread(ReadThreadParams {
                thread_id: first,
                include_archived: false,
                include_history: false,
            })
            .await,
        describe_thread,
    ));
    log.push(outcome(
        store
            .append_items(AppendThreadItemsParams {
                thread_id: first,
                items: vec![user_message("hello world")],
            })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(
        store.persist_thread(first, PersistContext::Standard).await,
        |_| String::new(),
    ));
    log.push(outcome(
        store
            .read_thread(ReadThreadParams {
                thread_id: first,
                include_archived: false,
                include_history: true,
            })
            .await,
        describe_thread,
    ));
    log.push(outcome(
        store
            .append_items(AppendThreadItemsParams {
                thread_id: first,
                items: vec![user_message("second turn"), user_message("third turn")],
            })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(store.flush_thread(first).await, |_| String::new()));
    log.push(outcome(
        store
            .update_thread_metadata(UpdateThreadMetadataParams {
                thread_id: first,
                include_archived: false,
                patch: ThreadMetadataPatch {
                    preview: Some("hello world".to_string()),
                    title: Some("hello".to_string()),
                    first_user_message: Some("hello world".to_string()),
                    source: Some(SessionSource::Exec),
                    cwd: Some(cwd.to_path_buf()),
                    ..Default::default()
                },
            })
            .await,
        |thread| thread.as_ref().map(describe_thread).unwrap_or_default(),
    ));
    log.push(outcome(
        store
            .update_thread_metadata(UpdateThreadMetadataParams {
                thread_id: missing,
                include_archived: false,
                patch: ThreadMetadataPatch {
                    preview: Some("nope".to_string()),
                    ..Default::default()
                },
            })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(store.shutdown_thread(first).await, |_| {
        String::new()
    }));
    log.push(outcome(store.shutdown_thread(first).await, |_| {
        String::new()
    }));

    // Reopening returns everything that was durable, and a second writer is refused.
    let metadata = ThreadPersistenceMetadata {
        cwd: Some(cwd.to_path_buf()),
        model_provider: provider.to_string(),
        memory_mode: ThreadMemoryMode::Enabled,
    };
    let resume = |include_archived: bool| ResumeThreadParams {
        thread_id: first,
        rollout_path: None,
        history: None,
        history_revision: None,
        include_archived,
        metadata: metadata.clone(),
    };
    log.push(outcome(store.resume_thread(resume(false)).await, |items| {
        items
            .iter()
            .map(describe_item)
            .collect::<Vec<_>>()
            .join(" | ")
    }));
    log.push(outcome(store.resume_thread(resume(false)).await, |_| {
        String::new()
    }));
    log.push(outcome(
        store
            .append_items(AppendThreadItemsParams {
                thread_id: first,
                items: vec![user_message("after resume")],
            })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(store.shutdown_thread(first).await, |_| {
        String::new()
    }));
    log.push(outcome(
        store
            .load_history(LoadThreadHistoryParams {
                thread_id: first,
                include_archived: false,
            })
            .await,
        |history| {
            history
                .items
                .iter()
                .map(describe_item)
                .collect::<Vec<_>>()
                .join(" | ")
        },
    ));
    log.push(outcome(
        store
            .load_history(LoadThreadHistoryParams {
                thread_id: missing,
                include_archived: false,
            })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(
        store
            .load_latest_model_context(LoadThreadHistoryParams {
                thread_id: first,
                include_archived: false,
            })
            .await,
        |context| {
            context
                .items
                .iter()
                .map(describe_item)
                .collect::<Vec<_>>()
                .join(" | ")
        },
    ));

    // A second thread, discarded before it ever becomes durable.
    log.push(outcome(
        store
            .create_thread(create_params(second, provider, cwd))
            .await,
        |_| String::new(),
    ));
    log.push(outcome(store.flush_thread(second).await, |_| String::new()));
    log.push(outcome(store.discard_thread(second).await, |_| {
        String::new()
    }));
    log.push(outcome(store.discard_thread(second).await, |_| {
        String::new()
    }));
    log.push(outcome(
        store
            .read_thread(ReadThreadParams {
                thread_id: second,
                include_archived: false,
                include_history: false,
            })
            .await,
        describe_thread,
    ));

    // Listing, archiving, sections, projects, attachments and deletion.
    let list = |archived: bool| ListThreadsParams {
        page_size: 10,
        cursor: None,
        sort_key: ThreadSortKey::UpdatedAt,
        sort_direction: SortDirection::Desc,
        allowed_sources: Vec::new(),
        model_providers: Some(vec![provider.to_string()]),
        cwd_filters: None,
        section: None,
        project_id: None,
        archived,
        search_term: None,
        relation_filter: None,
        use_state_db_only: true,
    };
    let listing = |page: &ThreadPage| {
        page.items
            .iter()
            .map(describe_thread)
            .collect::<Vec<_>>()
            .join(" || ")
    };
    log.push(outcome(store.list_threads(list(false)).await, listing));
    log.push(outcome(store.list_threads(list(true)).await, listing));
    log.push(outcome(
        store
            .archive_thread(ArchiveThreadParams { thread_id: first })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(
        store
            .archive_thread(ArchiveThreadParams { thread_id: first })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(store.list_threads(list(false)).await, listing));
    log.push(outcome(store.list_threads(list(true)).await, listing));
    log.push(outcome(
        store
            .read_thread(ReadThreadParams {
                thread_id: first,
                include_archived: false,
                include_history: false,
            })
            .await,
        describe_thread,
    ));
    log.push(outcome(
        store
            .read_thread(ReadThreadParams {
                thread_id: first,
                include_archived: true,
                include_history: false,
            })
            .await,
        describe_thread,
    ));
    log.push(outcome(
        store
            .unarchive_thread(ArchiveThreadParams { thread_id: first })
            .await,
        describe_thread,
    ));
    log.push(outcome(
        store
            .unarchive_thread(ArchiveThreadParams { thread_id: first })
            .await,
        describe_thread,
    ));
    log.push(outcome(
        store
            .update_thread_metadata(UpdateThreadMetadataParams {
                thread_id: first,
                include_archived: false,
                patch: ThreadMetadataPatch {
                    name: Some(Some("Renamed thread".to_string())),
                    model: Some("test-model".to_string()),
                    ..Default::default()
                },
            })
            .await,
        |thread| thread.as_ref().map(describe_thread).unwrap_or_default(),
    ));

    log.push(outcome(
        store
            .add_thread_attachment(AddThreadAttachmentParams {
                thread_id: first,
                attachment_type: "file".to_string(),
                identity_key: "a.txt".to_string(),
                payload: serde_json::json!({"size": 1}),
            })
            .await,
        |outcome| match outcome {
            AddThreadAttachmentOutcome::Created(a) => {
                format!("created {} {}", a.attachment_type, a.identity_key)
            }
            AddThreadAttachmentOutcome::Existing(a) => {
                format!("existing {} {}", a.attachment_type, a.identity_key)
            }
        },
    ));
    log.push(outcome(
        store
            .list_thread_attachments(ListThreadAttachmentsParams {
                thread_id: first,
                cursor: None,
                limit: 10,
            })
            .await,
        |page| format!("{} attachments", page.attachments.len()),
    ));

    log.push(outcome(
        store
            .delete_thread(DeleteThreadParams { thread_id: first })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(
        store
            .delete_thread(DeleteThreadParams { thread_id: first })
            .await,
        |_| String::new(),
    ));
    log.push(outcome(store.list_threads(list(false)).await, listing));
    log
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_postgres_thread_store() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_THREAD_STORE_STATE") else {
        return;
    };
    let state = Path::new(&state);
    bootstrap_codex_storage(&*connect(state, "migrator").await)
        .await
        .expect("bootstrap thread schema");
    let pool = connect(state, "runtime").await;

    let home = tempfile::tempdir().expect("local fixture home");
    let provider = format!(
        "store{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let cwd = home.path().join("work");
    std::fs::create_dir_all(&cwd).expect("create cwd");
    let sqlite = SqliteConfig::new_for_testing(home.path().abs());
    let state_db = StateRuntime::init(sqlite.clone(), provider.clone())
        .await
        .expect("sqlite runtime");
    let local = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: home.path().to_path_buf(),
            sqlite,
            default_model_provider_id: provider.clone(),
        },
        Some(state_db),
    );
    let postgres = PostgresThreadStore::new(pool, provider.clone());

    let ids = [ThreadId::new(), ThreadId::new(), ThreadId::new()];
    let expected = scenario(&local, &provider, &cwd, ids).await;
    let actual = scenario(&postgres, &provider, &cwd, ids).await;
    assert_eq!(actual.len(), expected.len());
    // Only the backend name inside a message may differ.
    let normalize = |text: &String| text.replace("live local writer", "live writer");
    let mismatches: Vec<String> = actual
        .iter()
        .zip(&expected)
        .enumerate()
        .filter(|(_, (actual, expected))| normalize(actual) != normalize(expected))
        .map(|(index, (actual, expected))| {
            format!("[{index}]\n  postgres: {actual}\n  local:    {expected}")
        })
        .collect();
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));

    // The patched model is stored and read back (the local store rebuilds it from its files).
    let again = ThreadId::new();
    postgres
        .create_thread(create_params(again, &provider, &cwd))
        .await
        .expect("create");
    postgres
        .persist_thread(again, PersistContext::Standard)
        .await
        .expect("persist");
    let patched = postgres
        .update_thread_metadata(UpdateThreadMetadataParams {
            thread_id: again,
            include_archived: false,
            patch: ThreadMetadataPatch {
                model: Some("test-model".to_string()),
                ..Default::default()
            },
        })
        .await
        .expect("patch")
        .expect("thread");
    assert_eq!(patched.model.as_deref(), Some("test-model"));
    postgres
        .delete_thread(DeleteThreadParams { thread_id: again })
        .await
        .expect("delete");

    // A second host with its own connections and no shared files sees the same thread.
    let host_a = PostgresThreadStore::new(connect(state, "runtime").await, provider.clone());
    let host_b = PostgresThreadStore::new(connect(state, "runtime").await, provider.clone());
    let shared = ThreadId::new();
    host_a
        .create_thread(create_params(shared, &provider, &cwd))
        .await
        .expect("create on host a");
    host_a
        .append_items(AppendThreadItemsParams {
            thread_id: shared,
            items: vec![user_message("from host a")],
        })
        .await
        .expect("append on host a");
    host_a
        .persist_thread(shared, PersistContext::Standard)
        .await
        .expect("persist on host a");
    host_a.shutdown_thread(shared).await.expect("close host a");
    let seen = host_b
        .load_history(LoadThreadHistoryParams {
            thread_id: shared,
            include_archived: false,
        })
        .await
        .expect("host b reads host a history");
    assert_eq!(seen.items.len(), 2);
    let listed = host_b
        .list_threads(ListThreadsParams {
            page_size: 10,
            cursor: None,
            sort_key: ThreadSortKey::UpdatedAt,
            sort_direction: SortDirection::Desc,
            allowed_sources: Vec::new(),
            model_providers: Some(vec![provider.clone()]),
            cwd_filters: None,
            section: None,
            project_id: None,
            archived: false,
            search_term: None,
            relation_filter: None,
            use_state_db_only: true,
        })
        .await
        .expect("host b lists");
    assert!(listed.items.iter().any(|thread| thread.thread_id == shared));
    assert!(
        listed
            .items
            .iter()
            .all(|thread| thread.rollout_path.is_none())
    );

    // Only one host can hold a thread open. A host whose lease lapsed without renewal is fenced
    // out of its writes as soon as another host takes over, and nothing it wrote is lost.
    let metadata = ThreadPersistenceMetadata {
        cwd: Some(cwd.clone()),
        model_provider: provider.clone(),
        memory_mode: ThreadMemoryMode::Enabled,
    };
    let reopen = || ResumeThreadParams {
        thread_id: shared,
        rollout_path: None,
        history: None,
        history_revision: None,
        include_archived: false,
        metadata: metadata.clone(),
    };
    let slow_a = PostgresThreadStore::new(connect(state, "runtime").await, provider.clone())
        .with_writer_lease(Duration::from_secs(2), Duration::from_secs(3600));
    let host_b = PostgresThreadStore::new(connect(state, "runtime").await, provider.clone());
    slow_a
        .resume_thread(reopen())
        .await
        .expect("reopen on host a");
    let refused = host_b.resume_thread(reopen()).await;
    assert!(
        matches!(refused, Err(ThreadStoreError::InvalidRequest { .. })),
        "a second host must not open a held thread: {refused:?}"
    );
    slow_a
        .append_items(AppendThreadItemsParams {
            thread_id: shared,
            items: vec![user_message("host a continues")],
        })
        .await
        .expect("host a appends while its lease is valid");
    // Host a never renews, so its two second lease lapses and host b can take over.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match host_b.resume_thread(reopen()).await {
            Ok(_) => break,
            Err(error) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "host b never took over: {error}"
                );
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }
    }
    let fenced = slow_a
        .append_items(AppendThreadItemsParams {
            thread_id: shared,
            items: vec![user_message("host a is fenced")],
        })
        .await;
    assert!(
        matches!(fenced, Err(ThreadStoreError::Conflict { .. })),
        "a writer that lost its lease must be refused: {fenced:?}"
    );
    host_b
        .append_items(AppendThreadItemsParams {
            thread_id: shared,
            items: vec![user_message("host b takes over")],
        })
        .await
        .expect("host b appends");
    slow_a.discard_thread(shared).await.expect("discard host a");
    host_b.shutdown_thread(shared).await.expect("close host b");
    let final_history = host_b
        .load_history(LoadThreadHistoryParams {
            thread_id: shared,
            include_archived: false,
        })
        .await
        .expect("final history");
    assert_eq!(
        final_history
            .items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::EventMsg(EventMsg::UserMessage(event)) => Some(event.message.clone()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![
            "from host a".to_string(),
            "host a continues".to_string(),
            "host b takes over".to_string()
        ]
    );
    host_a
        .delete_thread(DeleteThreadParams { thread_id: shared })
        .await
        .expect("delete shared thread");
    original_metadata_refusal_control(state, &provider, &cwd).await;
    original_claim_response_loss_control(state, &provider, &cwd).await;
    canceled_original_claim_control(state, &provider, &cwd).await;
    renewer_cancellation_control(state, &provider, &cwd).await;
    retained_live_controls(state, &provider, &cwd).await;
    // Same opt-in fixture/test: avoid parallel cases contaminating lifecycle list assertions.
    for ownership_lock in [true, false] {
        for expire in [true, false] {
            lease_wait_control(state, &provider, &cwd, ownership_lock, expire).await;
        }
    }
}

// Exercise real lock waits, including a wait after the first ownership predicate.
async fn lease_wait_control(
    state: &Path,
    provider: &str,
    cwd: &Path,
    ownership_lock: bool,
    expire: bool,
) {
    let observer = connect(state, "runtime").await;
    let migrator = connect(state, "migrator").await;
    // One original physical connection: capture its PID before dispatch, then require the
    // observed blocked transaction to be on that exact backend. A reconnect cannot pass.
    let mut writer_settings = settings(state, "runtime");
    writer_settings.limits.max_connections = 1;
    let writer_pool = Arc::new(
        PostgresPool::connect_in_namespace(writer_settings, None)
            .await
            .expect("single original writer pool"),
    );
    writer_pool
        .bind_current_activation()
        .await
        .expect("bind original writer activation");
    let writer = PostgresThreadStore::new(Arc::clone(&writer_pool), provider.to_string())
        .with_writer_lease(
            Duration::from_secs(if expire { 5 } else { 30 }),
            Duration::from_secs(3600),
        );
    let id = ThreadId::new();
    writer
        .create_thread(create_params(id, provider, cwd))
        .await
        .expect("create wait fixture");
    writer
        .persist_thread(id, PersistContext::Standard)
        .await
        .expect("materialize wait fixture");
    let before = lease_wait_snapshot(&observer, id).await;
    let position = writer
        .live_thread(id)
        .await
        .expect("live fixture")
        .lock_owned()
        .await
        .next_position;
    let mut lock = migrator
        .begin_serializable()
        .await
        .expect("authenticated blocker transaction");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(lock.connection())
        .await
        .expect("authorized owner role for blocker");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(lock.connection())
        .await
        .expect("blocker PID");
    if ownership_lock {
        sqlx::query("SELECT thread_id FROM codex_storage.thread_writer_ownership WHERE thread_id = $1::uuid FOR UPDATE")
            .bind(id.to_string()).fetch_one(lock.connection()).await.expect("ownership row lock without update");
    } else {
        sqlx::query("SELECT id FROM codex_storage.threads WHERE id = $1::uuid FOR UPDATE")
            .bind(id.to_string())
            .fetch_one(lock.connection())
            .await
            .expect("catalog row lock without update");
    }
    let writer_pid: i32 = {
        let mut original = writer_pool
            .acquire()
            .await
            .expect("original exclusive writer connection");
        sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
            .fetch_one(&mut *original)
            .await
            .expect("original physical writer PID")
    };
    // Retain the exact original future through deadline observations and reconciliation.
    let mut write = Box::pin(writer.append_items(AppendThreadItemsParams {
        thread_id: id,
        items: vec![user_message("blocked lease write")],
    }));
    let monitor = async {
        let mut connection = observer
            .acquire()
            .await
            .map_err(|_| "observer acquire failed".to_string())?;
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity a \
                 WHERE a.pid = $2 AND a.xact_start IS NOT NULL \
                 AND $1 = ANY(pg_blocking_pids(a.pid)) AND EXISTS \
                 (SELECT 1 FROM pg_locks l WHERE l.pid = a.pid AND NOT l.granted))",
            )
            .bind(blocker_pid)
            .bind(writer_pid)
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| "observe native lock wait failed".to_string())?;
            if waiting {
                let valid: bool = sqlx::query_scalar(
                    "SELECT lease_until > clock_timestamp() FROM codex_storage.thread_writer_ownership WHERE thread_id = $1::uuid",
                ).bind(id.to_string()).fetch_one(&mut *connection).await.map_err(|_| "lease valid at observed lock wait failed".to_string())?;
                if !valid {
                    return Err("first observed lock wait was already expired".to_string());
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if expire {
            loop {
                let expired: bool = sqlx::query_scalar(
                    "SELECT lease_until <= clock_timestamp() FROM codex_storage.thread_writer_ownership WHERE thread_id = $1::uuid",
                ).bind(id.to_string()).fetch_one(&mut *connection).await.map_err(|_| "observe server-clock expiry failed".to_string())?;
                if expired {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        } else {
            let valid: bool = sqlx::query_scalar(
                "SELECT lease_until > clock_timestamp() FROM codex_storage.thread_writer_ownership WHERE thread_id = $1::uuid",
            ).bind(id.to_string()).fetch_one(&mut *connection).await.map_err(|_| "valid waiting lease failed".to_string())?;
            if !valid {
                return Err("control lease expired before releasing lock".to_string());
            }
        }
        Ok(())
    };
    let (observation, completed) = tokio::select! {
        result = &mut write => (Err(format!("writer finished before lock observation: {result:?}")), Some(result)),
        observed = tokio::time::timeout(Duration::from_secs(15), monitor) => (observed.map_err(|_| "lock/expiry observation timed out".to_string()).and_then(|result| result), None),
    };
    // Release BEFORE assertions; unwind also rolls back the owned transaction and drops write.
    let mut rollback = Box::pin(lock.rollback());
    let rollback_deadline = tokio::time::timeout(Duration::from_secs(10), &mut rollback).await;
    let (rollback_late, released) = match rollback_deadline {
        Ok(result) => (false, result),
        Err(_) => {
            eprintln!(
                "blocker deadline exceeded; original rollback retained pending reconciliation"
            );
            (true, rollback.await)
        }
    };
    let (writer_late, result) = match completed {
        Some(result) => (false, result),
        None => match tokio::time::timeout(Duration::from_secs(10), &mut write).await {
            Ok(result) => (false, result),
            Err(_) => {
                eprintln!(
                    "writer deadline exceeded; original writer future retained pending reconciliation"
                );
                (true, (&mut write).await)
            }
        },
    };
    drop(write);
    // Assertions follow reconciliation of both original SQL futures, never deadline cancellation.
    released.expect("release owned blocker");
    assert!(
        !rollback_late && !writer_late,
        "original SQL exceeded reconciliation deadline"
    );
    assert!(observation.is_ok(), "{observation:?}");
    if expire {
        assert!(
            matches!(result, Err(ThreadStoreError::Conflict { .. })),
            "expired blocked writer: {result:?}"
        );
        assert_eq!(lease_wait_snapshot(&observer, id).await, before);
        assert_eq!(
            writer
                .live_thread(id)
                .await
                .expect("live after refusal")
                .lock_owned()
                .await
                .next_position,
            position
        );
        // Repeating the same staged batch cannot commit after the refusal.
        let retry = writer.flush_thread(id).await;
        assert!(
            matches!(retry, Err(ThreadStoreError::Conflict { .. })),
            "expired retry: {retry:?}"
        );
        assert_eq!(lease_wait_snapshot(&observer, id).await, before);
        assert_eq!(
            writer
                .live_thread(id)
                .await
                .expect("live after retry")
                .lock_owned()
                .await
                .next_position,
            position
        );
    } else {
        result.expect("valid blocked writer commits");
        assert_eq!(
            writer
                .live_thread(id)
                .await
                .expect("live committed")
                .lock_owned()
                .await
                .next_position,
            position + 1
        );
        let history = writer
            .load_history(LoadThreadHistoryParams {
                thread_id: id,
                include_archived: false,
            })
            .await
            .expect("valid history");
        assert_eq!(
            history
                .items
                .iter()
                .filter_map(|item| match item {
                    RolloutItem::EventMsg(EventMsg::UserMessage(event)) =>
                        Some(event.message.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec!["blocked lease write"]
        );
    }
    writer
        .discard_thread(id)
        .await
        .expect("drop writer and abort renewal");
    PostgresThreadStore::new(observer, provider.to_string())
        .delete_thread(DeleteThreadParams { thread_id: id })
        .await
        .expect("delete owned wait fixture");
}

async fn lease_wait_snapshot(pool: &PostgresPool, id: ThreadId) -> Value {
    let mut connection = pool.acquire().await.expect("snapshot connection");
    let text: String = sqlx::query_scalar(
        "SELECT json_build_object('catalog', (SELECT to_jsonb(t) FROM codex_storage.threads t WHERE id = $1::uuid), \
         'rollout', (SELECT json_agg(to_jsonb(r) ORDER BY position) FROM codex_storage.thread_rollout_lines r WHERE thread_id = $1::uuid))::text",
    ).bind(id.to_string()).fetch_one(&mut *connection).await.expect("complete catalog and ordered rollout snapshot");
    serde_json::from_str(&text).expect("snapshot JSON")
}

// Acquired before closure: same-generation handles must not regain writable authority
// when shutdown/discard removes the claim or a replacement writer acquires a new token.
async fn retained_live_controls(state: &Path, provider: &str, cwd: &Path) {
    let pool = connect(state, "runtime").await;
    let observer = connect(state, "runtime").await;
    let store = PostgresThreadStore::new(Arc::clone(&pool), provider.to_string());
    for discard in [false, true] {
        let id = ThreadId::new();
        store
            .create_thread(create_params(id, provider, cwd))
            .await
            .expect("create retained fixture");
        store
            .persist_thread(id, PersistContext::Standard)
            .await
            .expect("persist retained fixture");
        let original = store
            .live_thread(id)
            .await
            .expect("retain exact original instance");
        let original_token = original
            .lock()
            .await
            .ownership
            .as_ref()
            .expect("original writer")
            .claim
            .token;
        if discard {
            store.discard_thread(id).await.expect("discard original");
        } else {
            store.shutdown_thread(id).await.expect("shutdown original");
        }
        store
            .resume_thread(ResumeThreadParams {
                thread_id: id,
                rollout_path: None,
                history: None,
                history_revision: None,
                include_archived: false,
                metadata: ThreadPersistenceMetadata {
                    cwd: Some(cwd.to_path_buf()),
                    model_provider: provider.to_string(),
                    memory_mode: ThreadMemoryMode::Enabled,
                },
            })
            .await
            .expect("new token same-generation replacement");
        let replacement = store.live_thread(id).await.expect("replacement instance");
        assert!(!Arc::ptr_eq(&original, &replacement));
        assert!(
            replacement
                .lock()
                .await
                .ownership
                .as_ref()
                .expect("replacement owner")
                .claim
                .token
                > original_token
        );
        let before = lease_wait_snapshot(&observer, id).await;
        let mut stale = original.lock().await;
        let position = stale.next_position;
        assert!(matches!(
            stale.queue(id, vec![user_message("stale queued")]),
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert!(matches!(
            stale.write(&pool, id, provider).await,
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert!(matches!(
            stale.write(&pool, id, provider).await,
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert_eq!(stale.next_position, position);
        drop(stale);
        assert_eq!(lease_wait_snapshot(&observer, id).await, before);
        // A delayed original eviction cannot erase its already-installed replacement.
        store.evict_live_instance(id, &original).await;
        assert!(Arc::ptr_eq(
            &replacement,
            &store.live_thread(id).await.expect("replacement preserved")
        ));
        store
            .append_items(AppendThreadItemsParams {
                thread_id: id,
                items: vec![user_message("replacement writes")],
            })
            .await
            .expect("replacement genuine positive");
        store.shutdown_thread(id).await.expect("close replacement");
        store
            .delete_thread(DeleteThreadParams { thread_id: id })
            .await
            .expect("remove retained fixture");
    }
    for bulk in [false, true] {
        let id = ThreadId::new();
        store
            .create_thread(create_params(id, provider, cwd))
            .await
            .expect("create deletion fixture");
        store
            .persist_thread(id, PersistContext::Standard)
            .await
            .expect("persist deletion fixture");
        let original = store.live_thread(id).await.expect("retain before deletion");
        if bulk {
            store
                .delete_threads(DeleteThreadsParams {
                    thread_ids: vec![id],
                })
                .await
                .expect("bulk deletion");
        } else {
            store
                .delete_thread(DeleteThreadParams { thread_id: id })
                .await
                .expect("single deletion");
        }
        let before = lease_wait_snapshot(&observer, id).await;
        let mut stale = original.lock().await;
        let position = stale.next_position;
        assert!(matches!(
            stale.queue(id, vec![user_message("resurrection")]),
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert!(matches!(
            stale.write(&pool, id, provider).await,
            Err(ThreadStoreError::Conflict { .. })
        ));
        assert_eq!(stale.next_position, position);
        drop(stale);
        assert_eq!(lease_wait_snapshot(&observer, id).await, before);
    }
    let id = ThreadId::new();
    let mut unowned = LiveThread::for_create(&create_params(id, provider, cwd));
    let before = lease_wait_snapshot(&observer, id).await;
    assert!(unowned.is_deferred_and_empty());
    assert!(matches!(
        unowned.queue(id, vec![user_message("missing owner")]),
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(matches!(
        unowned.write(&pool, id, provider).await,
        Err(ThreadStoreError::Conflict { .. })
    ));
    assert!(unowned.is_deferred_and_empty());
    assert_eq!(lease_wait_snapshot(&observer, id).await, before);
}

// This probe enters the actual production renewal future at its original spawn.
// Its original handle is never replaced or moved out of the retained writer owner.
async fn renewer_cancellation_control(state: &Path, provider: &str, cwd: &Path) {
    let probe = Arc::new(super::renewal_probe::RenewalProbe::default());
    let mut store = PostgresThreadStore::new(connect(state, "runtime").await, provider.to_string());
    store.renewal_probe = Some(Arc::clone(&probe));
    let id = ThreadId::new();
    store
        .create_thread(create_params(id, provider, cwd))
        .await
        .expect("actual renewal owner");
    let original = store.live_thread(id).await.expect("registered exact owner");
    let task_id = original
        .lock()
        .await
        .ownership
        .as_ref()
        .expect("actual production renewer")
        .renewer
        .id();
    // No assertion may bypass the unconditional original destructor release and join.
    let ready = tokio::time::timeout(Duration::from_secs(5), probe.ready()).await;
    let mut close = Box::pin(store.shutdown_thread(id));
    let pending = tokio::time::timeout(Duration::from_millis(100), &mut close).await;
    let entered = tokio::time::timeout(Duration::from_secs(5), probe.dropping()).await;
    drop(close); // Cancel only the waiter, never the original registered JoinHandle.
    let registered = store.live_thread(id).await;
    let same_handle = {
        let live = original.lock().await;
        live.ownership
            .as_ref()
            .is_some_and(|owner| owner.renewer.id() == task_id && !owner.renewal_joined)
    };
    let premature = probe.returned();
    probe.release();
    let mut retry = Box::pin(store.shutdown_thread(id));
    let deadline = tokio::time::timeout(Duration::from_secs(10), &mut retry).await;
    let (late, result) = match deadline {
        Ok(result) => (false, result),
        Err(_) => (true, (&mut retry).await),
    };
    drop(retry);
    assert!(matches!(ready, Ok(())), "task readiness: {ready:?}");
    assert!(
        pending.is_err(),
        "pending destructor cannot acknowledge closure"
    );
    assert!(
        matches!(entered, Ok(())),
        "actual destructor entered: {entered:?}"
    );
    assert!(registered.is_ok_and(|live| Arc::ptr_eq(&live, &original)));
    assert!(same_handle && !premature);
    assert!(!late);
    result.expect("same original task joined and conditional lease release complete");
    assert!(probe.returned());
    let live = original.lock().await;
    let owner = live.ownership.as_ref().expect("retained original claim");
    assert!(owner.renewal_joined);
    assert!(
        owner
            .renewal_error
            .as_ref()
            .is_some_and(|error| error.is_cancelled() && error.id() == task_id)
    );
    assert!(matches!(
        store.live_thread(id).await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
}

async fn canceled_original_claim_control(state: &Path, provider: &str, cwd: &Path) {
    let observer = connect(state, "runtime").await;
    let seed = PostgresThreadStore::new(Arc::clone(&observer), provider.to_string());
    let id = ThreadId::new();
    seed.create_thread(create_params(id, provider, cwd))
        .await
        .expect("create original row");
    seed.persist_thread(id, PersistContext::Standard)
        .await
        .expect("materialize original row");
    seed.shutdown_thread(id)
        .await
        .expect("release original seed");
    let before = lease_wait_snapshot(&observer, id).await;
    let mut selected = settings(state, "runtime");
    selected.limits.max_connections = 1;
    let pool = Arc::new(
        PostgresPool::connect_in_namespace(selected, None)
            .await
            .expect("original single writer pool"),
    );
    pool.bind_current_activation()
        .await
        .expect("original selected activation");
    let writer_pid: i32 = {
        let mut connection = pool.acquire().await.expect("original writer connection");
        sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await
            .expect("exact original PID")
    };
    let writer = PostgresThreadStore::new(Arc::clone(&pool), provider.to_string());
    let migrator = connect(state, "migrator").await;
    let mut blocker = migrator
        .begin_serializable()
        .await
        .expect("original blocker");
    sqlx::query("SET LOCAL ROLE codex_owner")
        .execute(blocker.connection())
        .await
        .expect("authorized owner lock");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
        .fetch_one(blocker.connection())
        .await
        .expect("blocker PID");
    sqlx::query("SELECT thread_id FROM codex_storage.thread_writer_ownership WHERE thread_id=$1::uuid FOR UPDATE").bind(id.to_string()).fetch_one(blocker.connection()).await.expect("actual unchanged original row lock");
    let mut create = Box::pin(writer.create_thread(create_params(id, provider, cwd)));
    let witness = async {
        let mut connection = observer
            .acquire()
            .await
            .map_err(|_| "observer unavailable")?;
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.pid=$1 AND a.xact_start IS NOT NULL AND $2=ANY(pg_blocking_pids(a.pid)) AND EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND NOT l.granted))")
                .bind(writer_pid).bind(blocker_pid).fetch_one(&mut *connection).await.map_err(|_| "native writer wait probe failed")?;
            if waiting {
                return Ok::<(), &str>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let (observed, prematurely_completed) = tokio::select! {
        result = &mut create => (Err("create completed before native claim wait"), Some(result)),
        result = tokio::time::timeout(Duration::from_secs(5), witness) => (result.map_err(|_| "native wait deadline").and_then(|result| result), None),
    };
    let pending_before = writer.pending_writers.get(id);
    let original_task_id = pending_before
        .as_ref()
        .ok()
        .and_then(|pending| pending.as_ref())
        .and_then(|pending| pending.original_task_id());
    let original_attempt = pending_before
        .as_ref()
        .ok()
        .and_then(|pending| pending.as_ref())
        .and_then(|pending| pending.original_attempt());
    drop(create); // Deliberately cancel ONLY the production caller, not its registered task.
    let pending_after = writer.pending_writers.get(id);
    let same_owner = match (&pending_before, &pending_after) {
        (Ok(Some(before)), Ok(Some(after))) => {
            Arc::ptr_eq(before, after)
                && original_task_id.is_some()
                && after.original_task_id() == original_task_id
                && Arc::ptr_eq(after.original_pool(), &pool)
                && matches!((&original_attempt, after.original_attempt()), (Some(before), Some(after)) if Arc::ptr_eq(before, &after))
        }
        _ => false,
    };
    // Release and reconcile every original owned SQL future before asserting diagnostics.
    let mut release = Box::pin(blocker.rollback());
    let deadline = tokio::time::timeout(Duration::from_secs(10), &mut release).await;
    let (release_late, released) = match deadline {
        Ok(result) => (false, result),
        Err(_) => (true, (&mut release).await),
    };
    drop(release);
    let mut cleanup = Box::pin(writer.discard_thread(id));
    let deadline = tokio::time::timeout(Duration::from_secs(10), &mut cleanup).await;
    let (cleanup_late, cleaned) = match deadline {
        Ok(result) => (false, result),
        Err(_) => (true, (&mut cleanup).await),
    };
    drop(cleanup);
    if released.is_err() || cleaned.is_err() {
        eprintln!("original SQL cleanup uncertain; retaining writer, pool, task and fixture host");
        std::future::pending::<()>().await;
    }
    released.expect("original blocker rollback terminal");
    assert!(
        observed.is_ok() && prematurely_completed.is_none(),
        "exact original claim wait: {observed:?}"
    );
    assert!(same_owner, "cancel retains exact task, attempt and pool");
    assert!(!release_late && !cleanup_late);
    cleaned.expect("actual pending claim terminal, renewer join and lease release");
    assert!(
        writer
            .pending_writers
            .get(id)
            .expect("pending readback")
            .is_none()
    );
    assert!(matches!(
        writer.live_thread(id).await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
    assert_eq!(lease_wait_snapshot(&observer, id).await, before);
    let mut connection = observer
        .acquire()
        .await
        .expect("actual cleanup lease readback");
    let lease: (Option<String>, Option<bool>) = sqlx::query_as("SELECT owner_id::text, lease_until IS NOT NULL FROM codex_storage.thread_writer_ownership WHERE thread_id=$1::uuid")
        .bind(id.to_string()).fetch_one(&mut *connection).await.expect("original lease after cleanup");
    assert_eq!(lease, (None, Some(false)));
    drop(connection);
    seed.delete_thread(DeleteThreadParams { thread_id: id })
        .await
        .expect("delete qualification fixture");
}

async fn original_claim_response_loss_control(state: &Path, provider: &str, cwd: &Path) {
    let pool = connect(state, "runtime").await;
    let probe = Arc::new(AtomicBool::new(true));
    let mut writer = PostgresThreadStore::new(Arc::clone(&pool), provider.to_owned());
    writer.lose_claim_response = Some(Arc::clone(&probe));
    let id = ThreadId::new();
    let created = writer.create_thread(create_params(id, provider, cwd)).await;
    let history = writer.pending_writers.failure_history();
    let retained = history.first().cloned();
    let live = writer.live_thread(id).await.ok();
    let claim = match live.as_ref() {
        Some(live) => live
            .lock()
            .await
            .ownership
            .as_ref()
            .map(|owner| owner.claim.clone()),
        None => None,
    };
    let recovered = match retained
        .as_ref()
        .and_then(|pending| pending.original_attempt())
    {
        Some(attempt) => pool.recover_thread_ownership_attempt(&attempt).await,
        None => Ok(None),
    };
    let mut cleanup = Box::pin(writer.discard_thread(id));
    let (late, cleaned) = match tokio::time::timeout(Duration::from_secs(10), &mut cleanup).await {
        Ok(result) => (false, result),
        Err(_) => (true, (&mut cleanup).await),
    };
    drop(cleanup);
    if cleaned.is_err() {
        eprintln!("lost-response cleanup uncertain; retaining original writer and SQL pool");
        std::future::pending::<()>().await;
    }
    // All diagnostic assertions follow the original owned cleanup continuation.
    assert!(
        created.is_ok(),
        "same original attempt must recover committed response"
    );
    assert!(!late);
    assert!(!probe.load(Ordering::SeqCst));
    assert_eq!(history.len(), 1);
    let retained = retained.expect("original failure history owner");
    assert!(Arc::ptr_eq(retained.original_pool(), &pool));
    assert!(
        matches!((live.as_ref(), retained.original()), (Some(live), original) if Arc::ptr_eq(live, original))
    );
    assert_eq!(
        retained.original_claim_failure(),
        Some(codex_postgres_runtime::ThreadOwnershipError::Transaction(
            codex_postgres_runtime::TransactionError::CommitOutcomeUnknown
        ))
    );
    assert_eq!(recovered.expect("genuine same-attempt recovery"), claim);
    assert!(claim.is_some());
    assert!(
        writer
            .pending_writers
            .get(id)
            .expect("pending readback")
            .is_none()
    );
    assert!(matches!(
        writer.live_thread(id).await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
    // The failure frame remains owned after successful cleanup; no diagnostic eviction.
    assert!(Arc::ptr_eq(
        &writer.pending_writers.failure_history()[0],
        &retained
    ));
}

async fn original_metadata_refusal_control(state: &Path, provider: &str, cwd: &Path) {
    let pool = connect(state, "runtime").await;
    let writer = PostgresThreadStore::new(Arc::clone(&pool), provider.to_owned());
    let id = ThreadId::new();
    let before = lease_wait_snapshot(&pool, id).await;
    let params = ResumeThreadParams {
        thread_id: id,
        rollout_path: None,
        history: None,
        history_revision: None,
        include_archived: false,
        metadata: create_params(id, provider, cwd).metadata,
    };
    let mut resume = Box::pin(writer.resume_thread(params));
    let (late, resumed) = match tokio::time::timeout(Duration::from_secs(10), &mut resume).await {
        Ok(result) => (false, result),
        Err(_) => (true, (&mut resume).await),
    };
    drop(resume);
    // Reconcile unexpected retained work before diagnostics; never drop its owner.
    let needs_cleanup = match writer.pending_writers.get(id) {
        Ok(Some(_)) => true,
        Ok(None) => match writer.live_thread(id).await {
            Ok(_) => true,
            Err(ThreadStoreError::ThreadNotFound { .. }) => false,
            Err(_) => std::future::pending::<bool>().await,
        },
        Err(_) => std::future::pending::<bool>().await,
    };
    let mut cleanup_late = false;
    if needs_cleanup {
        let mut cleanup = Box::pin(writer.discard_thread(id));
        let cleaned = match tokio::time::timeout(Duration::from_secs(10), &mut cleanup).await {
            Ok(result) => result,
            Err(_) => {
                cleanup_late = true;
                (&mut cleanup).await
            }
        };
        drop(cleanup);
        if cleaned.is_err() {
            eprintln!("metadata refusal cleanup uncertain; original SQL owner retained");
            std::future::pending::<()>().await;
        }
    }
    let history = writer.pending_writers.failure_history();
    assert!(!late && !cleanup_late);
    assert!(
        matches!(resumed, Err(ThreadStoreError::ThreadNotFound { thread_id }) if thread_id == id)
    );
    assert_eq!(history.len(), 1);
    let original = &history[0];
    assert!(Arc::ptr_eq(original.original_pool(), &pool));
    assert!(original.original_attempt().is_some());
    assert!(!original.has_claim_failure());
    let refusals = original.original_refusals();
    assert_eq!(refusals.len(), 1);
    assert!(
        matches!(&refusals[0], ThreadStoreError::ThreadNotFound { thread_id } if *thread_id == id)
    );
    assert!(matches!(
        writer.live_thread(id).await,
        Err(ThreadStoreError::ThreadNotFound { .. })
    ));
    assert_eq!(lease_wait_snapshot(&pool, id).await, before);
    let mut connection = pool
        .acquire()
        .await
        .expect("metadata cleanup original readback");
    let lease: (Option<String>, Option<bool>) = sqlx::query_as("SELECT owner_id::text, lease_until IS NOT NULL FROM codex_storage.thread_writer_ownership WHERE thread_id=$1::uuid")
        .bind(id.to_string()).fetch_one(&mut *connection).await.expect("real claimed and released metadata attempt");
    assert_eq!(lease, (None, Some(false)));
}
