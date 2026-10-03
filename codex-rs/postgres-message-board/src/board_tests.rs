use super::*;
use codex_agent_message_board_extension::AgentMessageBoard;
use codex_agent_message_board_extension::ChannelQuery;
use codex_agent_message_board_extension::LocalAgentMessageBoard;
use codex_agent_message_board_extension::NotificationDelivery;
use codex_agent_message_board_extension::PageRequest;
use codex_agent_message_board_extension::PostPreview;
use codex_agent_message_board_extension::PostQuery;
use codex_agent_message_board_extension::ReadThreadRequest;
use codex_agent_message_board_extension::SortDirection;
use codex_agent_message_board_extension::ThreadQuery;
use codex_agent_message_board_extension::ThreadSort;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_state::SqliteConfig;
use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;

struct Host {
    clock: AtomicI64,
    members: HashMap<ThreadId, AgentPath>,
    notifications: Mutex<Vec<(ThreadId, PostMetadata)>>,
}

impl Host {
    fn new(members: HashMap<ThreadId, AgentPath>) -> Arc<Self> {
        Arc::new(Self {
            clock: AtomicI64::default(),
            members,
            notifications: Mutex::default(),
        })
    }

    fn take_notifications(&self) -> Vec<(ThreadId, PostMetadata)> {
        let mut taken = std::mem::take(
            &mut *self
                .notifications
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        taken.sort_by_key(|(recipient, _)| recipient.to_string());
        taken
    }
}

impl MessageBoardHost for Host {
    fn agent_path(&self, caller: ThreadId) -> BoxFuture<'_, Result<AgentPath>> {
        Box::pin(async move {
            self.members
                .get(&caller)
                .cloned()
                .ok_or(CodexErr::ThreadNotFound(caller))
        })
    }

    fn resolve_agent(&self, path: AgentPath) -> BoxFuture<'_, Result<ThreadId>> {
        Box::pin(async move {
            self.members
                .iter()
                .find_map(|(id, member)| (*member == path).then_some(*id))
                .ok_or_else(|| CodexErr::InvalidRequest("unknown agent".into()))
        })
    }

    fn current_time(&self, _caller: ThreadId) -> BoxFuture<'_, Result<DateTime<Utc>>> {
        Box::pin(async move {
            Ok(DateTime::parse_from_rfc3339("2026-09-18T12:00:00Z")
                .map_err(|error| CodexErr::Io(std::io::Error::other(error)))?
                .with_timezone(&Utc)
                + chrono::Duration::seconds(self.clock.fetch_add(1, Ordering::SeqCst)))
        })
    }

    fn notify(
        &self,
        recipient: ThreadId,
        post: PostPreview,
    ) -> BoxFuture<'_, Result<NotificationDelivery>> {
        Box::pin(async move {
            self.notifications
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((recipient, post.metadata));
            Ok(NotificationDelivery::Accepted)
        })
    }
}

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
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(20),
            max_connections: 16,
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

fn page(cursor: Option<String>, limit: u32) -> PageRequest {
    PageRequest {
        cursor,
        limit: NonZeroU32::new(limit).expect("page limit"),
    }
}

/// Renders results with message IDs replaced by labels, so boards that generate their own IDs
/// can be compared.
struct Labels(Vec<Uuid>);

impl Labels {
    fn render(&self, value: &dyn Debug) -> String {
        let mut text = format!("{value:?}");
        for (index, id) in self.0.iter().enumerate() {
            text = text.replace(&id.to_string(), &format!("post-{index}"));
        }
        text
    }
}

fn failed<T>(result: Result<T>) -> String {
    match result {
        Ok(_) => "unexpected success".to_string(),
        Err(error) => error.to_string(),
    }
}

/// Runs the board contract through the shared trait and records each observable result.
async fn scenario(
    board: &dyn AgentMessageBoard,
    host: &Host,
    root: ThreadId,
    worker: ThreadId,
    stranger: ThreadId,
) -> (Vec<String>, Vec<Uuid>) {
    let mut log = Vec::new();
    let mut labels = Labels(Vec::new());
    let worker_path = AgentPath::root().join("worker").expect("worker path");
    let post =
        |request_id: &str, destination: PostDestination, text: &str, notify: Vec<AgentPath>| {
            PostRequest {
                request_id: request_id.to_string(),
                destination,
                text: text.to_string(),
                agents_to_notify: notify,
            }
        };

    log.push(
        labels.render(
            &board
                .create_channel(
                    root,
                    CreateChannelRequest {
                        channel_name: "general".into(),
                        subscription: SubscriptionChange::Subscribe,
                    },
                )
                .await,
        ),
    );
    for name in ["general", "", " padded", "tab\tname"] {
        log.push(failed(
            board
                .create_channel(
                    root,
                    CreateChannelRequest {
                        channel_name: name.into(),
                        subscription: SubscriptionChange::Subscribe,
                    },
                )
                .await,
        ));
    }
    log.push(failed(
        board
            .create_channel(
                root,
                CreateChannelRequest {
                    channel_name: "x".repeat(129),
                    subscription: SubscriptionChange::Subscribe,
                },
            )
            .await,
    ));
    log.push(failed(
        board
            .create_channel(
                stranger,
                CreateChannelRequest {
                    channel_name: "denied".into(),
                    subscription: SubscriptionChange::Subscribe,
                },
            )
            .await,
    ));
    log.push(
        labels.render(
            &board
                .create_channel(
                    worker,
                    CreateChannelRequest {
                        channel_name: "Ideas".into(),
                        subscription: SubscriptionChange::Unsubscribe,
                    },
                )
                .await,
        ),
    );

    // Posting creates channels, threads and replies, and notifies subscribers and targets.
    let proofs = board
        .post(
            worker,
            post(
                "create-proofs",
                PostDestination::NewChannel("proofs".into()),
                "Share proofs here.",
                Vec::new(),
            ),
        )
        .await
        .expect("create proofs");
    labels.0.push(proofs.message_id);
    log.push(labels.render(&proofs));
    let announcement = board
        .post(
            root,
            post(
                "call-1",
                PostDestination::Channel("proofs".into()),
                "é🦀 PROOF of the theorem",
                vec![worker_path.clone()],
            ),
        )
        .await
        .expect("post proof");
    labels.0.push(announcement.message_id);
    log.push(labels.render(&announcement));
    log.push(labels.render(&host.take_notifications()));
    log.push(
        labels.render(
            &board
                .post(
                    root,
                    post(
                        "call-1",
                        PostDestination::Channel("proofs".into()),
                        "é🦀 PROOF of the theorem",
                        vec![worker_path.clone()],
                    ),
                )
                .await,
        ),
    );
    log.push(labels.render(&host.take_notifications()));
    log.push(failed(
        board
            .post(
                root,
                post(
                    "call-1",
                    PostDestination::Channel("proofs".into()),
                    "different text",
                    Vec::new(),
                ),
            )
            .await,
    ));
    let reply = board
        .post(
            worker,
            post(
                "reply-1",
                PostDestination::Thread(announcement.message_id),
                "Looks right to me.",
                Vec::new(),
            ),
        )
        .await
        .expect("reply");
    labels.0.push(reply.message_id);
    log.push(labels.render(&reply));
    log.push(labels.render(&host.take_notifications()));
    log.push(failed(
        board
            .post(
                worker,
                post(
                    "reply-to-reply",
                    PostDestination::Thread(reply.message_id),
                    "nested",
                    Vec::new(),
                ),
            )
            .await,
    ));
    log.push(failed(
        board
            .post(
                worker,
                post(
                    "missing-channel",
                    PostDestination::Channel("nowhere".into()),
                    "text",
                    Vec::new(),
                ),
            )
            .await,
    ));
    log.push(failed(
        board
            .post(
                worker,
                post(
                    "missing-thread",
                    PostDestination::Thread(Uuid::now_v7()),
                    "text",
                    Vec::new(),
                ),
            )
            .await
            .map(|_| ()),
    ));
    log.push(failed(
        board
            .post(
                worker,
                post(
                    "unknown-recipient",
                    PostDestination::Channel("proofs".into()),
                    "text",
                    vec![AgentPath::root().join("ghost").expect("ghost path")],
                ),
            )
            .await,
    ));
    for (id, text) in [
        ("empty-text".to_string(), String::new()),
        ("oversize".to_string(), "x".repeat(64 * 1024 + 1)),
        ("long-id-".repeat(100), "text".to_string()),
    ] {
        log.push(failed(
            board
                .post(
                    worker,
                    post(
                        &id,
                        PostDestination::Channel("proofs".into()),
                        &text,
                        Vec::new(),
                    ),
                )
                .await,
        ));
    }
    log.push(failed(
        board
            .post(
                stranger,
                post(
                    "stranger",
                    PostDestination::Channel("proofs".into()),
                    "text",
                    Vec::new(),
                ),
            )
            .await,
    ));
    log.push(labels.render(&host.take_notifications()));

    // A second discussion, so ordering and activity sorting have something to compare.
    let second = board
        .post(
            root,
            post(
                "call-2",
                PostDestination::Channel("proofs".into()),
                "Second topic",
                Vec::new(),
            ),
        )
        .await
        .expect("second root");
    labels.0.push(second.message_id);
    let late = board
        .post(
            worker,
            post(
                "reply-2",
                PostDestination::Thread(announcement.message_id),
                "Late reply on the first topic",
                Vec::new(),
            ),
        )
        .await
        .expect("late reply");
    labels.0.push(late.message_id);
    host.take_notifications();

    // Subscriptions: explicit changes, targets, and opt-outs that survive participation.
    for (target, agent, change) in [
        (
            SubscriptionTarget::Thread(second.message_id),
            None,
            SubscriptionChange::Subscribe,
        ),
        (
            SubscriptionTarget::Thread(announcement.message_id),
            Some(worker_path.clone()),
            SubscriptionChange::Unsubscribe,
        ),
        (
            SubscriptionTarget::Channel("proofs".into()),
            Some(worker_path.clone()),
            SubscriptionChange::Subscribe,
        ),
        (
            SubscriptionTarget::Channel("missing".into()),
            None,
            SubscriptionChange::Subscribe,
        ),
        (
            SubscriptionTarget::Thread(reply.message_id),
            None,
            SubscriptionChange::Subscribe,
        ),
    ] {
        log.push(
            labels.render(
                &board
                    .set_subscription(
                        root,
                        SubscriptionRequest {
                            target,
                            target_agent: agent,
                            change,
                        },
                    )
                    .await,
            ),
        );
    }
    let after_opt_out = board
        .post(
            worker,
            post(
                "after-opt-out",
                PostDestination::Thread(announcement.message_id),
                "Participating again keeps the opt-out",
                Vec::new(),
            ),
        )
        .await
        .expect("post after opt-out");
    labels.0.push(after_opt_out.message_id);
    log.push(labels.render(&host.take_notifications()));
    // The opt-out survived the worker's participation, so another author's reply skips it.
    let heard = board
        .post(
            root,
            post(
                "heard-by-others",
                PostDestination::Thread(announcement.message_id),
                "Only the remaining subscribers hear this",
                Vec::new(),
            ),
        )
        .await
        .expect("post after the opt-out");
    labels.0.push(heard.message_id);
    log.push(labels.render(&host.take_notifications()));
    let to_channel = board
        .post(
            root,
            post(
                "to-channel",
                PostDestination::Channel("proofs".into()),
                "Channel subscribers hear about new roots",
                Vec::new(),
            ),
        )
        .await
        .expect("post to channel");
    labels.0.push(to_channel.message_id);
    log.push(labels.render(&host.take_notifications()));

    // Listing and search.
    for query in [
        ChannelQuery {
            query: None,
            direction: SortDirection::NewestFirst,
            page: page(None, 20),
        },
        ChannelQuery {
            query: Some("IDE".into()),
            direction: SortDirection::OldestFirst,
            page: page(None, 20),
        },
        ChannelQuery {
            query: None,
            direction: SortDirection::OldestFirst,
            page: page(None, 1),
        },
    ] {
        let listed = board.list_channels(root, query).await;
        log.push(labels.render(&listed));
        if let Ok(first) = listed
            && let Some(cursor) = first.next_cursor
        {
            log.push(
                labels.render(
                    &board
                        .list_channels(
                            root,
                            ChannelQuery {
                                query: None,
                                direction: SortDirection::OldestFirst,
                                page: page(Some(cursor), 1),
                            },
                        )
                        .await,
                ),
            );
        }
    }
    log.push(failed(
        board
            .list_channels(
                root,
                ChannelQuery {
                    query: None,
                    direction: SortDirection::OldestFirst,
                    page: page(Some("not a cursor".into()), 5),
                },
            )
            .await,
    ));
    for (sort, direction, limit, chars) in [
        (ThreadSort::Created, SortDirection::NewestFirst, 20, 1000),
        (ThreadSort::Created, SortDirection::OldestFirst, 1, 1000),
        (ThreadSort::Activity, SortDirection::NewestFirst, 20, 1000),
        (ThreadSort::Activity, SortDirection::OldestFirst, 20, 8),
    ] {
        let listed = board
            .list_threads(
                root,
                ThreadQuery {
                    channel_name: "proofs".into(),
                    sort,
                    direction,
                    page: page(None, limit),
                    max_chars_per_post: NonZeroU32::new(chars).expect("chars"),
                },
            )
            .await;
        log.push(labels.render(&listed));
        if let Ok(first) = listed
            && let Some(cursor) = first.next_cursor
        {
            log.push(
                labels.render(
                    &board
                        .list_threads(
                            root,
                            ThreadQuery {
                                channel_name: "proofs".into(),
                                sort,
                                direction,
                                page: page(Some(cursor), limit),
                                max_chars_per_post: NonZeroU32::new(chars).expect("chars"),
                            },
                        )
                        .await,
                ),
            );
        }
    }
    log.push(failed(
        board
            .list_threads(
                root,
                ThreadQuery {
                    channel_name: "nowhere".into(),
                    sort: ThreadSort::Created,
                    direction: SortDirection::NewestFirst,
                    page: page(None, 5),
                    max_chars_per_post: NonZeroU32::new(50).expect("chars"),
                },
            )
            .await,
    ));
    for query in [
        PostQuery {
            channel_name: None,
            query: Some("PROOF".into()),
            after_message_id: None,
            author: None,
            page: page(None, 20),
            max_chars_per_post: NonZeroU32::new(10).expect("chars"),
        },
        PostQuery {
            channel_name: Some("proofs".into()),
            query: None,
            after_message_id: None,
            author: Some(worker_path.clone()),
            page: page(None, 2),
            max_chars_per_post: NonZeroU32::new(100).expect("chars"),
        },
        PostQuery {
            channel_name: None,
            query: Some("é🦀".into()),
            after_message_id: None,
            author: None,
            page: page(None, 20),
            max_chars_per_post: NonZeroU32::new(100).expect("chars"),
        },
        PostQuery {
            channel_name: None,
            query: None,
            after_message_id: Some(announcement.message_id),
            author: None,
            page: page(None, 20),
            max_chars_per_post: NonZeroU32::new(100).expect("chars"),
        },
        PostQuery {
            channel_name: None,
            query: None,
            after_message_id: Some(Uuid::now_v7()),
            author: None,
            page: page(None, 20),
            max_chars_per_post: NonZeroU32::new(100).expect("chars"),
        },
    ] {
        log.push(labels.render(&board.search_posts(root, query).await));
    }
    for (offset, limit) in [(0, 1000), (2, 3), (9999, 5)] {
        log.push(
            labels.render(
                &board
                    .read_post(
                        root,
                        ReadPostRequest {
                            message_id: announcement.message_id,
                            offset_chars: offset,
                            limit_chars: NonZeroU32::new(limit).expect("limit"),
                        },
                    )
                    .await,
            ),
        );
    }
    log.push(failed(
        board
            .read_post(
                root,
                ReadPostRequest {
                    message_id: Uuid::now_v7(),
                    offset_chars: 0,
                    limit_chars: NonZeroU32::new(10).expect("limit"),
                },
            )
            .await,
    ));
    for (thread_id, limit) in [(announcement.message_id, 1), (announcement.message_id, 20)] {
        let read = board
            .read_thread(
                root,
                ReadThreadRequest {
                    thread_id,
                    page: page(None, limit),
                    max_chars_per_post: NonZeroU32::new(100).expect("chars"),
                },
            )
            .await;
        log.push(labels.render(&read));
        if let Ok(first) = read
            && let Some(cursor) = first.replies.next_cursor
        {
            log.push(
                labels.render(
                    &board
                        .read_thread(
                            root,
                            ReadThreadRequest {
                                thread_id,
                                page: page(Some(cursor), limit),
                                max_chars_per_post: NonZeroU32::new(100).expect("chars"),
                            },
                        )
                        .await,
                ),
            );
        }
    }
    log.push(failed(
        board
            .read_thread(
                root,
                ReadThreadRequest {
                    thread_id: reply.message_id,
                    page: page(None, 5),
                    max_chars_per_post: NonZeroU32::new(100).expect("chars"),
                },
            )
            .await,
    ));
    (log, labels.0)
}

async fn setup(state: &Path) -> Arc<PostgresPool> {
    bootstrap_codex_storage(&*connect(state, "migrator").await)
        .await
        .expect("bootstrap board schema");
    connect(state, "runtime").await
}

fn members(root: ThreadId, worker: ThreadId) -> HashMap<ThreadId, AgentPath> {
    [
        (root, AgentPath::root()),
        (
            worker,
            AgentPath::root().join("worker").expect("worker path"),
        ),
    ]
    .into()
}

async fn real_postgres_board_matches_sqlite() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_MESSAGE_BOARD_STATE") else {
        return;
    };
    let pool = setup(Path::new(&state)).await;
    let (root, worker, stranger) = (ThreadId::new(), ThreadId::new(), ThreadId::new());

    let sqlite_home = tempfile::tempdir().expect("sqlite fixture home");
    let sqlite_host = Host::new(members(root, worker));
    let sqlite = LocalAgentMessageBoard::open(
        &SqliteConfig::new_for_testing(sqlite_home.path().abs()),
        SessionId::from(root),
        sqlite_host.clone(),
    )
    .await
    .expect("open sqlite board");
    let postgres_host = Host::new(members(root, worker));
    let postgres =
        PostgresAgentMessageBoard::new(pool, SessionId::from(root), postgres_host.clone());

    let (expected, _) = scenario(&sqlite, &sqlite_host, root, worker, stranger).await;
    let (actual, _) = scenario(&postgres, &postgres_host, root, worker, stranger).await;
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(&expected) {
        assert_eq!(actual, expected);
    }
}

async fn real_postgres_board_serializes_clients_and_keeps_deleted_boards_deleted() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_MESSAGE_BOARD_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let pool = setup(state).await;
    let (root, worker) = (ThreadId::new(), ThreadId::new());
    let host = Host::new(members(root, worker));
    let tree = SessionId::from(root);
    let board = PostgresAgentMessageBoard::new(pool.clone(), tree, host.clone());

    // Racing clients create one channel once.
    let created = futures::future::join_all((0..8).map(|_| {
        let board = board.clone();
        async move {
            board
                .create_channel(
                    root,
                    CreateChannelRequest {
                        channel_name: "race".into(),
                        subscription: SubscriptionChange::Subscribe,
                    },
                )
                .await
        }
    }))
    .await;
    assert_eq!(created.iter().filter(|result| result.is_ok()).count(), 1);

    // A retried call racing itself is accepted once and notifies once.
    let request = PostRequest {
        request_id: "same-call".into(),
        destination: PostDestination::Channel("race".into()),
        text: "once".into(),
        agents_to_notify: vec![AgentPath::root().join("worker").expect("worker path")],
    };
    let second_client =
        PostgresAgentMessageBoard::new(connect(state, "runtime").await, tree, host.clone());
    let (first, duplicate) = tokio::join!(
        board.post(root, request.clone()),
        second_client.post(root, request.clone())
    );
    let first = first.expect("first call");
    assert_eq!(duplicate.expect("duplicate call"), first);
    assert_eq!(host.take_notifications(), vec![(worker, first.clone())]);

    // Distinct posts from several clients all land, and both clients observe them.
    let posted = futures::future::join_all((0..12).map(|index| {
        let client = if index % 2 == 0 {
            board.clone()
        } else {
            second_client.clone()
        };
        async move {
            client
                .post(
                    worker,
                    PostRequest {
                        request_id: format!("call-{index}"),
                        destination: PostDestination::Channel("race".into()),
                        text: format!("post {index}"),
                        agents_to_notify: Vec::new(),
                    },
                )
                .await
        }
    }))
    .await;
    let ids: std::collections::HashSet<Uuid> = posted
        .into_iter()
        .map(|result| result.expect("concurrent post").message_id)
        .collect();
    assert_eq!(ids.len(), 12);
    for client in [&board, &second_client] {
        let listed = client
            .list_threads(
                root,
                ThreadQuery {
                    channel_name: "race".into(),
                    sort: ThreadSort::Created,
                    direction: SortDirection::OldestFirst,
                    page: page(None, 50),
                    max_chars_per_post: NonZeroU32::new(50).expect("chars"),
                },
            )
            .await
            .expect("list threads");
        assert_eq!(listed.results.len(), 13);
    }

    // Deleting a board while posts race it leaves nothing behind and blocks later writes.
    let other_root = ThreadId::new();
    let other = PostgresAgentMessageBoard::new(
        pool.clone(),
        SessionId::from(other_root),
        Host::new(members(other_root, worker)),
    );
    other
        .create_channel(
            other_root,
            CreateChannelRequest {
                channel_name: "kept".into(),
                subscription: SubscriptionChange::Subscribe,
            },
        )
        .await
        .expect("create channel on another board");
    let racing_posts = (0..10).map(|index| {
        let client = board.clone();
        tokio::spawn(async move {
            client
                .post(
                    worker,
                    PostRequest {
                        request_id: format!("racing-{index}"),
                        destination: PostDestination::Channel("race".into()),
                        text: format!("racing {index}"),
                        agents_to_notify: Vec::new(),
                    },
                )
                .await
        })
    });
    let roots = [tree];
    let deletion = PostgresAgentMessageBoard::delete_boards(&pool, &roots);
    let (racing, deleted) = tokio::join!(futures::future::join_all(racing_posts), deletion);
    deleted.expect("delete board");
    for result in racing {
        // A racing post either committed before deletion or was refused by the tombstone.
        let _ = result.expect("racing task");
    }
    PostgresAgentMessageBoard::delete_boards(&pool, &[tree])
        .await
        .expect("deleting again is safe");
    let remaining = board
        .search_posts(
            root,
            PostQuery {
                channel_name: None,
                query: None,
                after_message_id: None,
                author: None,
                page: page(None, 50),
                max_chars_per_post: NonZeroU32::new(50).expect("chars"),
            },
        )
        .await
        .expect("search deleted board");
    assert_eq!(remaining.results.len(), 0);
    let refusals = [
        failed(
            board
                .create_channel(
                    root,
                    CreateChannelRequest {
                        channel_name: "revived".into(),
                        subscription: SubscriptionChange::Subscribe,
                    },
                )
                .await,
        ),
        failed(
            second_client
                .post(
                    worker,
                    PostRequest {
                        request_id: "after-delete".into(),
                        destination: PostDestination::NewChannel("revived".into()),
                        text: "late".into(),
                        agents_to_notify: Vec::new(),
                    },
                )
                .await,
        ),
    ];
    for refused in refusals {
        assert!(
            refused.contains("permanently deleted"),
            "unexpected refusal: {refused}"
        );
    }
    let kept = other
        .list_channels(
            other_root,
            ChannelQuery {
                query: None,
                direction: SortDirection::OldestFirst,
                page: page(None, 10),
            },
        )
        .await
        .expect("list the unaffected board");
    assert_eq!(kept.results.len(), 1);
}

async fn real_postgres_board_errors_never_reveal_credentials() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_MESSAGE_BOARD_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let pool = connect(state, "runtime").await;
    let (root, worker) = (ThreadId::new(), ThreadId::new());
    let board = PostgresAgentMessageBoard::new(
        pool.clone(),
        SessionId::from(root),
        Host::new(members(root, worker)),
    );
    pool.close().await.expect("close pool");
    let message = failed(
        board
            .create_channel(
                root,
                CreateChannelRequest {
                    channel_name: "closed".into(),
                    subscription: SubscriptionChange::Subscribe,
                },
            )
            .await,
    );
    let password = std::fs::read_to_string(state.join("secrets/runtime.password"))
        .expect("read runtime credential");
    assert!(message.contains("message board storage failed"));
    assert!(!message.contains(password.trim()));
    assert!(!message.contains("postgres://"));
}

/// The checks share one namespace and the writer lock, so they run one after another.
#[tokio::test]
async fn real_postgres_board() {
    real_postgres_board_matches_sqlite().await;
    real_postgres_board_serializes_clients_and_keeps_deleted_boards_deleted().await;
    real_postgres_board_errors_never_reveal_credentials().await;
}
