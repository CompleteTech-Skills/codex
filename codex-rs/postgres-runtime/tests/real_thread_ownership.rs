#![expect(
    clippy::expect_used,
    reason = "isolated PostgreSQL fixture failures should identify their source"
)]

use codex_postgres_runtime::ConnectionSettings;
use codex_postgres_runtime::NamedNamespace;
use codex_postgres_runtime::PoolLimits;
use codex_postgres_runtime::PostgresPool;
use codex_postgres_runtime::ThreadOwnershipError;
use codex_postgres_runtime::ThreadOwnershipNamespace;
use codex_postgres_runtime::TransactionError;
use codex_postgres_runtime::bootstrap_codex_storage;
use codex_postgres_runtime::bootstrap_named_namespace;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::path::Path;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

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
async fn real_two_clients_reject_stale_thread_owners() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_OWNERSHIP_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let migrator = PostgresPool::connect_in_namespace(settings(state, "migrator"), None)
        .await
        .expect("migrator pool");
    bootstrap_codex_storage(&migrator)
        .await
        .expect("bootstrap ownership schema");
    let first = PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
        .await
        .expect("first runtime client");
    let second = PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
        .await
        .expect("second runtime client");
    first
        .bind_current_activation()
        .await
        .expect("first actual original activation");
    second
        .bind_current_activation()
        .await
        .expect("second actual original activation");
    let thread_id = format!(
        "{:032x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    );
    let contender_owner = "00000000000000000000000000000002";
    let replacement_owner = "00000000000000000000000000000003";
    let reclaim_owner = "00000000000000000000000000000004";
    let after_cancel_owner = "00000000000000000000000000000006";
    let short = Duration::from_secs(2);
    let namespace = ThreadOwnershipNamespace::Default;
    let original_attempt = first
        .prepare_thread_ownership_attempt(namespace.clone(), &thread_id)
        .await
        .expect("actual SQL generated attempt");
    let committed = first
        .claim_thread_ownership_attempt(&original_attempt, short)
        .await
        .expect("first actual attempt claim")
        .expect("first owner acquired");
    let first_owner = committed.owner_id.clone();
    assert_eq!(committed.token, 1);
    drop(committed); // Lose only the response, retaining the actual original attempt.
    let readback = second
        .recover_thread_ownership(namespace.clone(), &thread_id, &first_owner)
        .await
        .expect("scalar observational readback")
        .expect("same committed lease observed");
    assert_eq!(readback.token, 1);
    assert_eq!(first.observe_thread_ownership(&readback).await, Ok(true));
    assert_eq!(second.observe_thread_ownership(&readback).await, Ok(true));
    assert_eq!(
        first.renew_thread_ownership(&readback, short).await,
        Err(ThreadOwnershipError::Transaction(
            TransactionError::Rejected
        ))
    );
    assert_eq!(
        second.release_thread_ownership(&readback).await,
        Err(ThreadOwnershipError::Transaction(
            TransactionError::Rejected
        ))
    );
    assert_eq!(
        second
            .recover_thread_ownership_attempt(&original_attempt)
            .await,
        Err(ThreadOwnershipError::Transaction(
            TransactionError::Rejected
        ))
    );
    assert_eq!(
        second
            .claim_thread_ownership_attempt(&original_attempt, short)
            .await,
        Err(ThreadOwnershipError::Transaction(
            TransactionError::Rejected
        ))
    );
    let claim = first
        .recover_thread_ownership_attempt(&original_attempt)
        .await
        .expect("recover actual original pool attempt")
        .expect("original writable owner");
    assert_eq!(claim.owner_id, first_owner);
    assert_eq!(claim.token, 1);
    assert_eq!(first.renew_thread_ownership(&claim, short).await, Ok(true));
    assert_eq!(
        second
            .claim_thread_ownership(namespace.clone(), &thread_id, contender_owner, short)
            .await,
        Ok(None)
    );
    // The server clock decides expiry and can step during a long test run, so poll for the
    // takeover until a generous deadline instead of trusting one fixed sleep.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let replacement = loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if let Some(claim) = second
            .claim_thread_ownership(
                namespace.clone(),
                &thread_id,
                replacement_owner,
                Duration::from_secs(2),
            )
            .await
            .expect("expired takeover")
        {
            break claim;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "second owner acquired after the first lease expired"
        );
    };
    assert_eq!(replacement.token, claim.token + 1);
    assert_eq!(first.observe_thread_ownership(&claim).await, Ok(false));
    assert_eq!(
        first
            .recover_thread_ownership(namespace.clone(), &thread_id, &first_owner)
            .await,
        Ok(None)
    );
    assert_eq!(first.renew_thread_ownership(&claim, short).await, Ok(false));
    assert_eq!(first.release_thread_ownership(&claim).await, Ok(false));
    assert_eq!(
        second.release_thread_ownership(&replacement).await,
        Ok(true)
    );
    assert_eq!(
        second.observe_thread_ownership(&replacement).await,
        Ok(false)
    );
    let latest = first
        .claim_thread_ownership(namespace.clone(), &thread_id, reclaim_owner, short)
        .await
        .expect("reclaim after release")
        .expect("first owner reclaims");
    assert_eq!(latest.token, replacement.token + 1);
    assert_eq!(first.release_thread_ownership(&latest).await, Ok(true));
    let mut lock = first
        .begin_serializable()
        .await
        .expect("hold ownership row");
    sqlx::query("SELECT token FROM codex_storage.thread_writer_ownership WHERE thread_id = $1::uuid FOR UPDATE")
        .bind(&thread_id)
        .fetch_one(lock.connection())
        .await
        .expect("lock ownership row");
    let canceled_attempt = second
        .prepare_thread_ownership_attempt(namespace.clone(), &thread_id)
        .await
        .expect("original canceled attempt retained");
    let canceled = tokio::time::timeout(
        Duration::from_millis(100),
        second.claim_thread_ownership_attempt(&canceled_attempt, short),
    )
    .await;
    // This fixture deliberately tests primitive SQLx request cancellation, not a store
    // drain receipt; release the original barrier before any assertion.
    lock.rollback().await.expect("release ownership row lock");
    assert!(
        canceled.is_err(),
        "actual blocked primitive request canceled"
    );
    assert_eq!(
        second
            .recover_thread_ownership_attempt(&canceled_attempt)
            .await,
        Ok(None)
    );
    let after_cancel = second
        .claim_thread_ownership(namespace.clone(), &thread_id, after_cancel_owner, short)
        .await
        .expect("claim after cancelled request")
        .expect("second owner acquired after cancellation");
    assert_eq!(after_cancel.token, latest.token + 1);
    assert_eq!(
        second.release_thread_ownership(&after_cancel).await,
        Ok(true)
    );
    assert_eq!(
        first
            .claim_thread_ownership(
                namespace.clone(),
                &thread_id,
                "00000000000000000000000000000009",
                Duration::ZERO
            )
            .await,
        Err(ThreadOwnershipError::InvalidLease)
    );
    assert_eq!(
        first
            .claim_thread_ownership(
                namespace.clone(),
                &thread_id,
                "0000000000000000000000000000000a",
                Duration::from_nanos(1)
            )
            .await,
        Err(ThreadOwnershipError::InvalidLease)
    );

    let concurrent_id = format!(
        "{:032x}",
        u128::from_str_radix(&thread_id, 16).expect("thread UUID") + 1
    );
    let (left, right) = tokio::join!(
        first.claim_thread_ownership(
            namespace.clone(),
            &concurrent_id,
            "00000000000000000000000000000007",
            Duration::from_secs(2)
        ),
        second.claim_thread_ownership(
            namespace.clone(),
            &concurrent_id,
            "00000000000000000000000000000008",
            Duration::from_secs(2)
        ),
    );
    let granted = [left.as_ref(), right.as_ref()]
        .into_iter()
        .filter(|result| matches!(result, Ok(Some(_))))
        .count();
    assert_eq!(granted, 1);
    for result in [left, right] {
        assert!(matches!(
            result,
            Ok(Some(_))
                | Ok(None)
                | Err(ThreadOwnershipError::Transaction(
                    TransactionError::SerializationConflict
                ))
        ));
    }
}

#[tokio::test]
async fn real_named_thread_ownership_is_namespace_scoped() {
    let Ok(state) = std::env::var("CODEX_TEST_POSTGRES_ISOLATION_STATE") else {
        return;
    };
    let state = Path::new(&state);
    let named = NamedNamespace::new("codex_storage_isolation").expect("fixture namespace");
    let migrator =
        PostgresPool::connect_in_namespace(settings(state, "isolation_migrator"), Some(&named))
            .await
            .expect("named migrator");
    bootstrap_named_namespace(&migrator, &named)
        .await
        .expect("bootstrap named ownership schema");
    let named_runtime =
        PostgresPool::connect_in_namespace(settings(state, "isolation_runtime"), Some(&named))
            .await
            .expect("named runtime");
    let default_migrator = PostgresPool::connect_in_namespace(settings(state, "migrator"), None)
        .await
        .expect("actual default bootstrap owner");
    bootstrap_codex_storage(&default_migrator)
        .await
        .expect("bootstrap default ownership schema");
    let default_runtime = PostgresPool::connect_in_namespace(settings(state, "runtime"), None)
        .await
        .expect("default runtime");
    named_runtime
        .bind_current_activation()
        .await
        .expect("actual named activation");
    default_runtime
        .bind_current_activation()
        .await
        .expect("actual default activation");
    let thread_id = format!(
        "{:032x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos()
    );
    let named_namespace = ThreadOwnershipNamespace::Named(named);
    let named_attempt = named_runtime
        .prepare_thread_ownership_attempt(named_namespace.clone(), &thread_id)
        .await
        .expect("actual named original attempt");
    let committed = named_runtime
        .claim_thread_ownership_attempt(&named_attempt, Duration::from_secs(2))
        .await
        .expect("named original claim")
        .expect("named owner acquired");
    let owner = committed.owner_id.clone();
    drop(committed);
    let named_claim = named_runtime
        .recover_thread_ownership_attempt(&named_attempt)
        .await
        .expect("named original pool recovery")
        .expect("named writable owner");
    assert_eq!(
        default_runtime
            .recover_thread_ownership_attempt(&named_attempt)
            .await,
        Err(ThreadOwnershipError::Transaction(
            TransactionError::Rejected
        ))
    );
    assert_eq!(named_claim.token, 1);
    assert_eq!(
        named_runtime.observe_thread_ownership(&named_claim).await,
        Ok(true)
    );
    assert_eq!(
        default_runtime
            .recover_thread_ownership(ThreadOwnershipNamespace::Default, &thread_id, &owner)
            .await,
        Ok(None)
    );
    assert_eq!(
        named_runtime.release_thread_ownership(&named_claim).await,
        Ok(true)
    );
    let default_claim = default_runtime
        .claim_thread_ownership(
            ThreadOwnershipNamespace::Default,
            &thread_id,
            "0000000000000000000000000000000c",
            Duration::from_millis(1),
        )
        .await
        .expect("minimum valid lease")
        .expect("default owner acquired");
    assert_eq!(default_claim.token, 1);
}
