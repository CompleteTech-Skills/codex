//! Real board persistence with a bounded fixture membership/clock host.
//! Notifications are deliberately inactive; this proves data writes, not delivery.
use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use codex_agent_message_board_extension::CreateChannelRequest;
use codex_agent_message_board_extension::MessageBoardHost;
use codex_agent_message_board_extension::NotificationDelivery;
use codex_agent_message_board_extension::PostDestination;
use codex_agent_message_board_extension::PostPreview;
use codex_agent_message_board_extension::PostRequest;
use codex_agent_message_board_extension::SubscriptionChange;
use codex_agent_message_board_extension::SubscriptionRequest;
use codex_agent_message_board_extension::SubscriptionTarget;
use codex_postgres_message_board::PostgresAgentMessageBoard;
use codex_postgres_runtime::PostgresPool;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as HostResult;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

type HostFuture<'a, T> = Pin<Box<dyn Future<Output = HostResult<T>> + Send + 'a>>;
struct Host {
    root: ThreadId,
    child: ThreadId,
}
impl MessageBoardHost for Host {
    fn agent_path(&self, caller: ThreadId) -> HostFuture<'_, AgentPath> {
        Box::pin(async move {
            if caller == self.root {
                Ok(AgentPath::root())
            } else if caller == self.child {
                AgentPath::root()
                    .join("latest-child")
                    .map_err(|cause| CodexErr::InvalidRequest(cause.to_string()))
            } else {
                Err(CodexErr::ThreadNotFound(caller))
            }
        })
    }
    fn resolve_agent(&self, path: AgentPath) -> HostFuture<'_, ThreadId> {
        Box::pin(async move {
            if path == AgentPath::root() {
                Ok(self.root)
            } else if path
                == AgentPath::root()
                    .join("latest-child")
                    .map_err(|cause| CodexErr::InvalidRequest(cause.to_string()))?
            {
                Ok(self.child)
            } else {
                Err(CodexErr::InvalidRequest("unknown fixture member".into()))
            }
        })
    }
    fn current_time(&self, _caller: ThreadId) -> HostFuture<'_, DateTime<Utc>> {
        Box::pin(async { Ok(Utc::now()) })
    }
    fn notify(
        &self,
        _recipient: ThreadId,
        _post: PostPreview,
    ) -> HostFuture<'_, NotificationDelivery> {
        Box::pin(async { Ok(NotificationDelivery::SkippedInactive) })
    }
}

pub(super) async fn write(pool: &Arc<PostgresPool>, root: ThreadId, child: ThreadId) -> Result<()> {
    let board = PostgresAgentMessageBoard::new(
        Arc::clone(pool),
        root.into(),
        Arc::new(Host { root, child }),
    );
    board
        .create_channel(
            root,
            CreateChannelRequest {
                channel_name: "latest-remote".into(),
                subscription: SubscriptionChange::Subscribe,
            },
        )
        .await?;
    let post = board
        .post(
            root,
            PostRequest {
                request_id: format!("latest-remote-post-{root}"),
                destination: PostDestination::Channel("latest-remote".into()),
                text: "Latest remote board café 🦀".into(),
                agents_to_notify: Vec::new(),
            },
        )
        .await?;
    board
        .post(
            child,
            PostRequest {
                request_id: format!("latest-remote-reply-{child}"),
                destination: PostDestination::Thread(post.message_id),
                text: "Latest remote child reply".into(),
                agents_to_notify: Vec::new(),
            },
        )
        .await?;
    board
        .set_subscription(
            child,
            SubscriptionRequest {
                target: SubscriptionTarget::Channel("latest-remote".into()),
                target_agent: None,
                change: SubscriptionChange::Unsubscribe,
            },
        )
        .await?;
    // The live board still has the root subscription; this distinct tombstone
    // cannot accidentally erase the rows whose latest writes must return.
    PostgresAgentMessageBoard::delete_boards(pool, &[SessionId::new()]).await?;
    Ok(())
}
