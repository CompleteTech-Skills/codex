use super::*;
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::bootstrap_codex_storage;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::path::Path;

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
            acquire_timeout: Duration::from_secs(5),
            max_connections: 2,
        },
    }
}

#[tokio::test]
async fn real_postgres_graph_adapter_preserves_order_and_edge_semantics() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_GRAPH_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let migrator = PostgresPool::connect(settings(state, "migrator"))
        .await
        .expect("migrator pool");
    bootstrap_codex_storage(&migrator)
        .await
        .expect("bootstrap graph schema");
    let runtime = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
            .await
            .expect("runtime pool"),
    );
    runtime
        .bind_current_activation()
        .await
        .expect("actual runtime activation binding");
    let store = PostgresAgentGraphStore::new(runtime.clone());
    let [root, first, second, third, grandchild, under_closed, other] =
        std::array::from_fn(|_| ThreadId::new());
    let mut children = [first, second, third];
    children.sort_by_key(ToString::to_string);
    let [first, second, third] = children;
    for (parent, child, status) in [
        (root, third, ThreadSpawnEdgeStatus::Closed),
        (root, second, ThreadSpawnEdgeStatus::Open),
        (root, first, ThreadSpawnEdgeStatus::Open),
        (first, grandchild, ThreadSpawnEdgeStatus::Open),
        (third, under_closed, ThreadSpawnEdgeStatus::Open),
    ] {
        store
            .upsert_thread_spawn_edge(parent, child, status)
            .await
            .expect("upsert edge");
    }
    assert_eq!(
        store
            .list_thread_spawn_children(root, None)
            .await
            .expect("all children"),
        vec![first, second, third]
    );
    assert_eq!(
        store
            .list_thread_spawn_children(root, Some(ThreadSpawnEdgeStatus::Open))
            .await
            .expect("open children"),
        vec![first, second]
    );
    assert_eq!(
        store
            .list_thread_spawn_descendants(root, None)
            .await
            .expect("all descendants"),
        {
            let mut second_depth = [grandchild, under_closed];
            second_depth.sort_by_key(ToString::to_string);
            vec![first, second, third, second_depth[0], second_depth[1]]
        }
    );
    assert_eq!(
        store
            .list_thread_spawn_descendants(root, Some(ThreadSpawnEdgeStatus::Open))
            .await
            .expect("open descendants"),
        vec![first, second, grandchild]
    );
    store
        .set_thread_spawn_edge_status(other, ThreadSpawnEdgeStatus::Closed)
        .await
        .expect("missing child is no-op");
    store
        .set_thread_spawn_edge_status(second, ThreadSpawnEdgeStatus::Closed)
        .await
        .expect("close edge");
    assert_eq!(
        store
            .list_thread_spawn_children(root, Some(ThreadSpawnEdgeStatus::Open))
            .await
            .expect("remaining open child"),
        vec![first]
    );
    store
        .upsert_thread_spawn_edge(other, second, ThreadSpawnEdgeStatus::Open)
        .await
        .expect("reparent child");
    assert_eq!(
        store
            .list_thread_spawn_children(other, None)
            .await
            .expect("reparented child"),
        vec![second]
    );
    assert_eq!(
        store
            .list_thread_spawn_children(root, None)
            .await
            .expect("root children after reparent"),
        vec![first, third]
    );
    let mut connection = runtime.acquire().await.expect("runtime connection");
    let create = sqlx::query("CREATE TABLE codex_storage.graph_runtime_forbidden (id int)")
        .execute(&mut *connection)
        .await;
    assert_eq!(
        create.err().and_then(|error| error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .map(std::borrow::Cow::into_owned)),
        Some("42501".to_string())
    );
    drop(connection);
    let isolation = PostgresPool::connect(settings(state, "isolation_runtime"))
        .await
        .expect("isolation runtime pool");
    let mut isolation_connection = isolation.acquire().await.expect("isolation connection");
    let cross_namespace =
        sqlx::query("SELECT child_thread_id FROM codex_storage.thread_spawn_edges")
            .fetch_optional(&mut *isolation_connection)
            .await;
    assert_eq!(
        cross_namespace.err().and_then(|error| {
            error
                .as_database_error()
                .and_then(sqlx::error::DatabaseError::code)
                .map(std::borrow::Cow::into_owned)
        }),
        Some("42501".to_string())
    );
    drop(isolation_connection);
    store
        .upsert_thread_spawn_edge(grandchild, root, ThreadSpawnEdgeStatus::Open)
        .await
        .expect("fixture cycle edge");
    assert_eq!(
        store
            .list_thread_spawn_descendants(root, None)
            .await
            .err()
            .map(|error| error.to_string()),
        Some("agent graph store internal error: PostgreSQL graph operation failed".to_string())
    );
    runtime.close().await.expect("close runtime pool");
    isolation.close().await.expect("close isolation pool");
    migrator.close().await.expect("close migrator pool");
}
