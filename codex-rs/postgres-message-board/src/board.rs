//! PostgreSQL-backed boards. The tree ID scopes every read and write.
//!
//! Writers take the post counter row lock first, which serializes mutations across every
//! handle in the namespace the way the SQLite store's immediate transactions do. Accepted posts
//! survive runtime unload and process restart, but cannot recreate a board after its root has
//! been permanently deleted.

use crate::paging::preview;
use caseless::default_case_fold_str;
use chrono::DateTime;
use chrono::Utc;
use codex_agent_message_board_extension::ChannelSummary;
use codex_agent_message_board_extension::CreateChannelRequest;
use codex_agent_message_board_extension::MessageBoardHost;
use codex_agent_message_board_extension::PostContent;
use codex_agent_message_board_extension::PostDestination;
use codex_agent_message_board_extension::PostMetadata;
use codex_agent_message_board_extension::PostRequest;
use codex_agent_message_board_extension::ReadPostRequest;
use codex_agent_message_board_extension::SubscriptionChange;
use codex_agent_message_board_extension::SubscriptionRequest;
use codex_agent_message_board_extension::SubscriptionState;
use codex_agent_message_board_extension::SubscriptionTarget;
use codex_postgres_runtime::PoolError;
use codex_postgres_runtime::PostgresPool;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use futures::StreamExt;
use futures::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use sqlx::Acquire;
use sqlx::PgConnection;
use sqlx::Row;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

pub(crate) const MAX_READ_CHARS: usize = 20_000;
const MAX_POST_BYTES: usize = 64 * 1024;
const MAX_CHANNEL_BYTES: usize = 128;
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) const LOCK_WRITERS: &str =
    "SELECT 1 FROM agent_board_post_counter WHERE singleton FOR UPDATE";

#[derive(Clone)]
pub struct PostgresAgentMessageBoard {
    pub(crate) identity: SessionId,
    pub(crate) pool: Arc<PostgresPool>,
    pub(crate) host: Arc<dyn MessageBoardHost>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredPost {
    pub(crate) metadata: PostMetadata,
    pub(crate) text: String,
}

/// What a post attempt produced inside its transaction.
enum Posted {
    Existing(PostMetadata),
    Accepted {
        post: StoredPost,
        recipients: HashSet<ThreadId>,
    },
}

impl PostgresAgentMessageBoard {
    /// Opens the board for a root, child or resumed runtime. The schema belongs to the storage
    /// bootstrap, so opening never creates or alters tables.
    pub fn new(
        pool: Arc<PostgresPool>,
        identity: SessionId,
        host: Arc<dyn MessageBoardHost>,
    ) -> Self {
        Self {
            identity,
            pool,
            host,
        }
    }

    /// Run one board write in a transaction that holds the writer lock, after refusing
    /// boards whose root has been permanently deleted.
    pub(crate) async fn write<T, F>(&self, operation: F) -> Result<T>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T>>,
    {
        let board = self.identity.to_string();
        let mut connection = self.pool.acquire().await.map_err(pool_error)?;
        timeout(QUERY_TIMEOUT, async {
            let mut tx = connection.begin().await.map_err(storage)?;
            self.pool
                .prepare_selected_write(&mut tx)
                .await
                .map_err(|error| storage_message(&error.to_string()))?;
            self.pool
                .require_bound_storage_open(&mut tx)
                .await
                .map_err(|error| storage_message(&error.to_string()))?;
            sqlx::query(LOCK_WRITERS)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            let deleted: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM agent_board_deleted WHERE board = $1)",
            )
            .bind(&board)
            .fetch_one(&mut *tx)
            .await
            .map_err(storage)?;
            if deleted {
                return Err(invalid(
                    "the message board's root has been permanently deleted",
                ));
            }
            let value = operation(&mut tx).await?;
            tx.commit().await.map_err(storage)?;
            Ok(value)
        })
        .await
        .map_err(|_| storage_message("the operation timed out"))?
    }

    /// Run reads against one snapshot, like a deferred SQLite transaction.
    pub(crate) async fn read<T, F>(&self, operation: F) -> Result<T>
    where
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T>>,
    {
        let mut connection = self.pool.acquire().await.map_err(pool_error)?;
        timeout(QUERY_TIMEOUT, async {
            let mut tx = connection.begin().await.map_err(storage)?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            let value = operation(&mut tx).await?;
            tx.commit().await.map_err(storage)?;
            Ok(value)
        })
        .await
        .map_err(|_| storage_message("the operation timed out"))?
    }

    pub async fn create_channel(
        &self,
        caller: ThreadId,
        request: CreateChannelRequest,
    ) -> Result<ChannelSummary> {
        validate_channel(&request.channel_name)?;
        let author = self.host.agent_path(caller).await?;
        let now = self.host.current_time(caller).await?;
        let board = self.identity.to_string();
        self.write(move |connection| {
            Box::pin(async move {
                insert_channel(connection, &board, &request.channel_name, &author, now).await?;
                if request.subscription == SubscriptionChange::Subscribe {
                    subscribe(
                        connection,
                        &board,
                        &SubscriptionTarget::Channel(request.channel_name.clone()),
                        caller,
                    )
                    .await?;
                }
                channel_summary(connection, &board, &request.channel_name).await
            })
        })
        .await
    }

    /// Once started, finish the accepted write and fanout even if the tool caller
    /// disconnects. Delivery is attempted once; retries never replay old notices
    /// into a recipient's later turn. Once committed, notification failures are
    /// logged without failing the post; the persisted content remains readable.
    pub async fn post(&self, caller: ThreadId, request: PostRequest) -> Result<PostMetadata> {
        let board = self.clone();
        tokio::spawn(async move { board.post_inner(caller, request).await })
            .await
            .map_err(|_| storage_message("the post task failed"))?
    }

    async fn post_inner(&self, caller: ThreadId, request: PostRequest) -> Result<PostMetadata> {
        if request.text.len() > MAX_POST_BYTES
            || request.text.is_empty()
            || request.request_id.is_empty()
            || request.request_id.len() > 512
            || request.agents_to_notify.len() > 256
        {
            return Err(invalid(
                "post text, request ID or recipient count exceeds the board limits",
            ));
        }
        let author = self.host.agent_path(caller).await?;
        let request_id = format!("{caller}:{}", request.request_id);
        let request_json = serde_json::to_string(&request).map_err(serialization)?;
        let board = self.identity.to_string();
        let earlier = self
            .read(|connection| {
                let (board, request_id, request_json) =
                    (board.clone(), request_id.clone(), request_json.clone());
                Box::pin(async move {
                    existing_post(connection, &board, &request_id, &request_json).await
                })
            })
            .await?;
        if let Some(post) = earlier {
            return Ok(post.metadata);
        }
        let mut recipients = HashSet::new();
        for path in &request.agents_to_notify {
            recipients.insert(self.host.resolve_agent(path.clone()).await?);
        }
        let now = self.host.current_time(caller).await?;
        let posted = self
            .write(move |connection| {
                Box::pin(async move {
                    if let Some(post) =
                        existing_post(connection, &board, &request_id, &request_json).await?
                    {
                        return Ok(Posted::Existing(post.metadata));
                    }
                    let id = Uuid::now_v7();
                    let (channel, root, target) = match &request.destination {
                        PostDestination::Channel(channel) => {
                            let exists: bool = sqlx::query_scalar(
                                "SELECT EXISTS(SELECT 1 FROM agent_board_channels \
                                 WHERE board = $1 AND name = $2)",
                            )
                            .bind(&board)
                            .bind(channel)
                            .fetch_one(&mut *connection)
                            .await
                            .map_err(storage)?;
                            if !exists {
                                return Err(invalid("channel not found in this board"));
                            }
                            (
                                channel.clone(),
                                id,
                                SubscriptionTarget::Channel(channel.clone()),
                            )
                        }
                        PostDestination::NewChannel(channel) => {
                            validate_channel(channel)?;
                            insert_channel(connection, &board, channel, &author, now).await?;
                            subscribe(
                                connection,
                                &board,
                                &SubscriptionTarget::Channel(channel.clone()),
                                caller,
                            )
                            .await?;
                            (
                                channel.clone(),
                                id,
                                SubscriptionTarget::Channel(channel.clone()),
                            )
                        }
                        PostDestination::Thread(root) => {
                            let post = load_post(connection, &board, *root).await?;
                            if post.metadata.thread_id != *root {
                                return Err(invalid("thread_id must identify a top-level post"));
                            }
                            (
                                post.metadata.channel_name,
                                *root,
                                SubscriptionTarget::Thread(*root),
                            )
                        }
                    };
                    let subscribed: Vec<String> = sqlx::query_scalar(
                        "SELECT agent FROM agent_board_subscriptions \
                         WHERE board = $1 AND target = $2",
                    )
                    .bind(&board)
                    .bind(target_key(&target)?)
                    .fetch_all(&mut *connection)
                    .await
                    .map_err(storage)?;
                    for recipient in subscribed {
                        recipients.insert(
                            ThreadId::from_string(&recipient)
                                .map_err(|_| storage_message("invalid subscriber id"))?,
                        );
                    }
                    recipients.remove(&caller);
                    let post = StoredPost {
                        metadata: PostMetadata {
                            message_id: id,
                            channel_name: channel.clone(),
                            author: author.clone(),
                            thread_id: root,
                            created_at: now,
                        },
                        text: request.text.clone(),
                    };
                    let seq: i64 = sqlx::query_scalar(
                        "UPDATE agent_board_post_counter \
                         SET last_seq = last_seq + 1 WHERE singleton RETURNING last_seq",
                    )
                    .fetch_one(&mut *connection)
                    .await
                    .map_err(storage)?;
                    sqlx::query(
                        "INSERT INTO agent_board_posts(seq, board, id, channel, \
                         root, author, timestamp, body_search, payload, request_id, request) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                    )
                    .bind(seq)
                    .bind(&board)
                    .bind(id.to_string())
                    .bind(channel)
                    .bind(root.to_string())
                    .bind(author.to_string())
                    .bind(now.timestamp_micros())
                    .bind(default_case_fold_str(&request.text))
                    .bind(serde_json::to_string(&post).map_err(serialization)?)
                    .bind(request_id)
                    .bind(request_json)
                    .execute(&mut *connection)
                    .await
                    .map_err(storage)?;
                    // Participation subscribes by default, without overriding an explicit opt-out.
                    subscribe(
                        connection,
                        &board,
                        &SubscriptionTarget::Thread(root),
                        caller,
                    )
                    .await?;
                    Ok(Posted::Accepted { post, recipients })
                })
            })
            .await?;

        let (post, recipients) = match posted {
            Posted::Existing(metadata) => return Ok(metadata),
            Posted::Accepted { post, recipients } => (post, recipients),
        };
        // A committed post succeeds even if a best-effort notice cannot be delivered.
        let notice = preview(post.clone(), /*max_chars*/ 150);
        futures::stream::iter(recipients)
            .for_each_concurrent(/*limit*/ 16, |recipient| {
                let notice = notice.clone();
                async move {
                    if let Err(error) = self.host.notify(recipient, notice).await {
                        tracing::warn!(%recipient, %error, "Failed to deliver message-board notification");
                    }
                }
            })
            .await;
        Ok(post.metadata)
    }

    pub async fn set_subscription(
        &self,
        caller: ThreadId,
        request: SubscriptionRequest,
    ) -> Result<SubscriptionState> {
        let caller_path = self.host.agent_path(caller).await?;
        let target_path = request.target_agent.unwrap_or(caller_path);
        let target_agent = self.host.resolve_agent(target_path.clone()).await?;
        let board = self.identity.to_string();
        let target = request.target;
        let change = request.change;
        let (channel, root, last) = self
            .write(move |connection| {
                Box::pin(async move {
                    let (channel, root, last) = match &target {
                        SubscriptionTarget::Channel(name) => {
                            let summary = channel_summary(connection, &board, name).await?;
                            (name.clone(), None, summary.last_message_id)
                        }
                        SubscriptionTarget::Thread(root) => {
                            let post = load_post(connection, &board, *root).await?;
                            if post.metadata.thread_id != *root {
                                return Err(invalid("thread_id must identify a top-level post"));
                            }
                            let last: String = sqlx::query_scalar(
                                "SELECT id FROM agent_board_posts \
                                 WHERE board = $1 AND root = $2 \
                                 ORDER BY timestamp DESC, seq DESC LIMIT 1",
                            )
                            .bind(&board)
                            .bind(root.to_string())
                            .fetch_one(&mut *connection)
                            .await
                            .map_err(storage)?;
                            (
                                post.metadata.channel_name,
                                Some(*root),
                                Some(
                                    Uuid::parse_str(&last)
                                        .map_err(|_| storage_message("invalid post id"))?,
                                ),
                            )
                        }
                    };
                    // Keep active subscriptions readable by older binaries. Opt-outs only
                    // prevent implicit subscription when this agent participates again.
                    let statements = match change {
                        SubscriptionChange::Subscribe => [
                            "DELETE FROM agent_board_opt_outs \
                             WHERE board = $1 AND target = $2 AND agent = $3",
                            "INSERT INTO agent_board_subscriptions(board, target, agent) \
                             VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        ],
                        SubscriptionChange::Unsubscribe => [
                            "DELETE FROM agent_board_subscriptions \
                             WHERE board = $1 AND target = $2 AND agent = $3",
                            "INSERT INTO agent_board_opt_outs(board, target, agent) \
                             VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        ],
                    };
                    for statement in statements {
                        sqlx::query(statement)
                            .bind(&board)
                            .bind(target_key(&target)?)
                            .bind(target_agent.to_string())
                            .execute(&mut *connection)
                            .await
                            .map_err(storage)?;
                    }
                    Ok((channel, root, last))
                })
            })
            .await?;
        Ok(SubscriptionState {
            channel_name: channel,
            thread_id: root,
            target_agent: target_path,
            enabled: change == SubscriptionChange::Subscribe,
            last_message_id: last,
        })
    }

    pub async fn read_post(
        &self,
        caller: ThreadId,
        request: ReadPostRequest,
    ) -> Result<PostContent> {
        self.host.agent_path(caller).await?;
        let board = self.identity.to_string();
        let post = self
            .read(move |connection| {
                Box::pin(async move { load_post(connection, &board, request.message_id).await })
            })
            .await?;
        let n_chars = post.text.chars().count();
        let offset = (request.offset_chars as usize).min(n_chars);
        let text: String = post
            .text
            .chars()
            .skip(offset)
            .take((request.limit_chars.get() as usize).min(MAX_READ_CHARS))
            .collect();
        let next_offset_chars = offset + text.chars().count();
        Ok(PostContent {
            metadata: post.metadata,
            text,
            n_chars,
            next_offset_chars,
        })
    }
}

pub(crate) async fn insert_channel(
    connection: &mut PgConnection,
    board: &str,
    name: &str,
    author: &AgentPath,
    now: DateTime<Utc>,
) -> Result<()> {
    let inserted = sqlx::query(
        "INSERT INTO agent_board_channels(board, name, name_search, created_at, \
         timestamp, author) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
    )
    .bind(board)
    .bind(name)
    .bind(default_case_fold_str(name))
    .bind(now.to_rfc3339())
    .bind(now.timestamp_micros())
    .bind(author.to_string())
    .execute(connection)
    .await
    .map_err(storage)?
    .rows_affected();
    if inserted == 0 {
        return Err(invalid("channel already exists"));
    }
    Ok(())
}

async fn subscribe(
    connection: &mut PgConnection,
    board: &str,
    target: &SubscriptionTarget,
    agent: ThreadId,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_board_subscriptions(board, target, agent) \
         SELECT $1, $2, $3 WHERE NOT EXISTS ( \
           SELECT 1 FROM agent_board_opt_outs \
           WHERE board = $1 AND target = $2 AND agent = $3) \
         ON CONFLICT DO NOTHING",
    )
    .bind(board)
    .bind(target_key(target)?)
    .bind(agent.to_string())
    .execute(connection)
    .await
    .map_err(storage)?;
    Ok(())
}

pub(crate) async fn load_post(
    connection: &mut PgConnection,
    board: &str,
    id: Uuid,
) -> Result<StoredPost> {
    let payload: Option<String> =
        sqlx::query_scalar("SELECT payload FROM agent_board_posts WHERE board = $1 AND id = $2")
            .bind(board)
            .bind(id.to_string())
            .fetch_optional(connection)
            .await
            .map_err(storage)?;
    serde_json::from_str(&payload.ok_or_else(|| invalid("post not found in this board"))?)
        .map_err(serialization)
}

async fn existing_post(
    connection: &mut PgConnection,
    board: &str,
    request_id: &str,
    request: &str,
) -> Result<Option<StoredPost>> {
    let row = sqlx::query(
        "SELECT payload, request FROM agent_board_posts \
         WHERE board = $1 AND request_id = $2",
    )
    .bind(board)
    .bind(request_id)
    .fetch_optional(connection)
    .await
    .map_err(storage)?;
    row.map(|row| {
        if row.try_get::<String, _>("request").map_err(storage)? != request {
            return Err(invalid("request ID was already used for a different post"));
        }
        serde_json::from_str(&row.try_get::<String, _>("payload").map_err(storage)?)
            .map_err(serialization)
    })
    .transpose()
}

pub(crate) async fn channel_summary(
    connection: &mut PgConnection,
    board: &str,
    name: &str,
) -> Result<ChannelSummary> {
    let row = sqlx::query(
        "SELECT c.created_at, c.author, \
         (SELECT COUNT(*) FROM agent_board_posts p \
          WHERE p.board = c.board AND p.channel = c.name) AS message_count, \
         (SELECT p.id FROM agent_board_posts p \
          WHERE p.board = c.board AND p.channel = c.name \
          ORDER BY p.timestamp DESC, p.seq DESC LIMIT 1) AS last_message_id \
         FROM agent_board_channels c WHERE c.board = $1 AND c.name = $2",
    )
    .bind(board)
    .bind(name)
    .fetch_optional(connection)
    .await
    .map_err(storage)?
    .ok_or_else(|| invalid("channel not found in this board"))?;
    let created_at: String = row.try_get("created_at").map_err(storage)?;
    let author: String = row.try_get("author").map_err(storage)?;
    let last_message_id: Option<String> = row.try_get("last_message_id").map_err(storage)?;
    Ok(ChannelSummary {
        channel_name: name.to_string(),
        created_at: DateTime::parse_from_rfc3339(&created_at)
            .map_err(|_| storage_message("invalid channel timestamp"))?
            .with_timezone(&Utc),
        created_by: AgentPath::try_from(author).map_err(invalid)?,
        message_count: row.try_get::<i64, _>("message_count").map_err(storage)? as usize,
        last_message_id: last_message_id
            .map(|id| Uuid::parse_str(&id))
            .transpose()
            .map_err(|_| storage_message("invalid post id"))?,
    })
}

fn validate_channel(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > MAX_CHANNEL_BYTES
        || name.trim() != name
        || name.chars().any(char::is_control)
    {
        return Err(invalid(
            "channel names must contain 1–128 bytes without edge whitespace or control characters",
        ));
    }
    Ok(())
}

pub(crate) fn invalid(message: impl Into<String>) -> CodexErr {
    CodexErr::InvalidRequest(message.into())
}

/// Storage failures never echo connection details or server messages, only the SQLSTATE code.
pub(crate) fn storage(error: sqlx::Error) -> CodexErr {
    match error {
        sqlx::Error::Database(database) => match database.code() {
            Some(code) => storage_message(&format!("database error {code}")),
            None => storage_message("database error"),
        },
        _ => storage_message("the database operation failed"),
    }
}

pub(crate) fn storage_message(message: &str) -> CodexErr {
    CodexErr::Io(std::io::Error::other(format!(
        "message board storage failed: {message}"
    )))
}

pub(crate) fn pool_error(error: PoolError) -> CodexErr {
    storage_message(&format!("PostgreSQL is unavailable ({error:?})"))
}

pub(crate) fn serialization(error: serde_json::Error) -> CodexErr {
    storage_message(&format!("a stored value could not be encoded ({error})"))
}

fn target_key(target: &SubscriptionTarget) -> Result<String> {
    serde_json::to_string(target).map_err(serialization)
}

#[cfg(test)]
#[path = "board_tests.rs"]
mod tests;
