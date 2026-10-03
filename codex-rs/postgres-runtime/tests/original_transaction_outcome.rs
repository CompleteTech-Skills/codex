//! Requires separately admitted, already bootstrapped default/named fixtures.
//! No credential/bootstrap/clock authority is manufactured by these controls.
use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::OriginalTransactionOutcome;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use serde_json::Value;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
type ControlResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
fn refusal(message: &str) -> Box<dyn Error + Send + Sync> {
    std::io::Error::other(message).into()
}
fn settings(state: &Path, role: &str) -> ControlResult<ConnectionSettings> {
    let receipt: Value = serde_json::from_slice(&std::fs::read(state.join("receipt.json"))?)?;
    let port = receipt["port"]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .ok_or_else(|| refusal("fixture port unavailable"))?;
    Ok(ConnectionSettings {
        host: "localhost".to_string(),
        port,
        database: "codex".to_string(),
        username: format!("codex_{role}"),
        password: std::fs::read_to_string(state.join(format!("secrets/{role}.password")))?
            .trim()
            .to_string()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: 1,
        },
    })
}
async fn selected_outcomes(
    state: &Path,
    role: &str,
    namespace: Option<NamedNamespace>,
) -> ControlResult<()> {
    let original = Arc::new(
        PostgresPool::connect_in_namespace(settings(state, role)?, namespace.clone()).await?,
    );
    let independent =
        Arc::new(PostgresPool::connect_in_namespace(settings(state, role)?, namespace).await?);
    original.bind_current_activation().await?;
    independent.bind_current_activation().await?;
    for commit in [true, false] {
        let id = format!(
            "{:032x}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        // The exact original physical session remains outside the cancelable body.
        let mut session = original.acquire_retained_write().await?;
        let mut body = Box::pin(async {
            let mut transaction = session.begin().await?;
            let original_pid: i32 = sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
                .fetch_one(transaction.connection())
                .await?;
            let absent: Option<i64> = sqlx::query_scalar(
                "SELECT token FROM thread_writer_ownership WHERE thread_id=$1::uuid",
            )
            .bind(&id)
            .fetch_optional(transaction.connection())
            .await?;
            if absent.is_some() {
                return Err(refusal("control row was not absent"));
            }
            sqlx::query("INSERT INTO thread_writer_ownership(thread_id,token) VALUES($1::uuid,7)")
                .bind(&id)
                .execute(transaction.connection())
                .await?;
            if commit {
                transaction.commit().await?;
            } else {
                transaction.rollback().await?;
            }
            let status = session.observe_original_transaction().await?;
            let correct_status = matches!(
                (commit, status),
                (true, OriginalTransactionOutcome::Committed)
                    | (false, OriginalTransactionOutcome::Aborted)
            );
            let mut read = independent.begin_selected_write().await?;
            let read_pid: i32 = sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
                .fetch_one(read.connection())
                .await?;
            let actual: Option<(i64, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT token,owner_id::text,lease_until::text FROM thread_writer_ownership WHERE thread_id=$1::uuid")
                .bind(&id).fetch_optional(read.connection()).await?;
            read.rollback().await?;
            Ok::<_, Box<dyn Error + Send + Sync>>((original_pid, read_pid, correct_status, actual))
        });
        let (result, late) = match tokio::time::timeout(Duration::from_secs(15), &mut body).await {
            Ok(result) => (result, false),
            // Timeout is observation; continue this SAME original future, not a replacement.
            Err(_) => ((&mut body).await, true),
        };
        drop(body);
        let (original_pid, read_pid, status, actual) = match result {
            Ok(result) => result,
            Err(error) => {
                // Keep original session, pools, ID and original cause in this same host.
                // A ROLLBACK/idle marker cannot classify an uncertain earlier commit.
                let _retained = (&session, &original, &independent, &id, error);
                std::future::pending::<()>().await;
                std::hint::black_box(&_retained);
                return Err(refusal("unreachable original-session uncertainty hold"));
            }
        };
        // Remove only our unique fixture row through its selected original generation.
        let mut cleanup = Box::pin(async {
            let mut transaction = independent.begin_selected_write().await?;
            sqlx::query("DELETE FROM thread_writer_ownership WHERE thread_id=$1::uuid")
                .bind(&id)
                .execute(transaction.connection())
                .await?;
            transaction.commit().await?;
            Ok::<_, Box<dyn Error + Send + Sync>>(())
        });
        let (cleaned, cleanup_late) =
            match tokio::time::timeout(Duration::from_secs(15), &mut cleanup).await {
                Ok(result) => (result, false),
                Err(_) => ((&mut cleanup).await, true),
            };
        drop(cleanup);
        if let Err(error) = cleaned {
            let _retained = (&session, &original, &independent, &id, error);
            std::future::pending::<()>().await;
            std::hint::black_box(&_retained);
            return Err(refusal("unreachable cleanup uncertainty hold"));
        }
        drop(session); // Genuine status was observed; NOT socket-closed/global-drain proof.
        let mut returned = original.begin_selected_write().await?;
        let returned_pid: i32 = sqlx::query_scalar("SELECT pg_catalog.pg_backend_pid()")
            .fetch_one(returned.connection())
            .await?;
        returned.rollback().await?;
        let expected = commit.then_some((7, None, None));
        if late
            || cleanup_late
            || !status
            || actual != expected
            || original_pid == read_pid
            || returned_pid != original_pid
        {
            return Err(refusal(
                "original commit/abort outcome or exact payload control failed",
            ));
        }
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires admitted original default fixture and retained execution host"]
async fn real_default_original_transaction_commit_and_abort() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE")?;
    selected_outcomes(Path::new(&state), "runtime", None).await
}
#[tokio::test]
#[ignore = "requires admitted original named fixture and retained execution host"]
async fn real_named_original_transaction_commit_and_abort() -> ControlResult<()> {
    let state = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE")?;
    let namespace = NamedNamespace::new("codex_storage_isolation")
        .map_err(|_| refusal("fixture named namespace rejected"))?;
    selected_outcomes(Path::new(&state), "isolation_runtime", Some(namespace)).await
}
