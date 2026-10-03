use super::*;
use pretty_assertions::assert_eq;

#[cfg(unix)]
#[test]
fn preexisting_quarantine_is_preserved_and_refuses_arming() {
    let directory = tempfile::tempdir().expect("owned fixture directory");
    let first = ExclusiveFixture::arm(directory.path(), FixtureScope::Default).expect("first arm");
    let before = std::fs::read(&first.quarantine).expect("published marker");
    let second = ExclusiveFixture::arm(directory.path(), FixtureScope::Default);
    assert!(matches!(second,Err(error) if error.kind()==std::io::ErrorKind::AlreadyExists));
    assert_eq!(
        std::fs::read(&first.quarantine).expect("retained marker"),
        before
    );
}

#[cfg(unix)]
#[tokio::test]
async fn captured_exercise_error_preserves_cause_and_quarantine() {
    let directory = tempfile::tempdir().expect("owned fixture directory");
    let guard = ExclusiveFixture::arm(directory.path(), FixtureScope::Named).expect("arm");
    let retained = [
        guard.retain(lazy_pool()).expect("first retained pool"),
        guard.retain(lazy_pool()).expect("second retained pool"),
    ];
    let before = std::fs::read(&guard.quarantine).expect("marker");
    let result = guard
        .supervise(FixtureDeadline::Upgrade, async {
            Err::<(), _>("causal_fixture_failure")
        })
        .await;
    let failure = result.expect_err("exercise must fail");
    assert_eq!(failure.primary, Some("causal_fixture_failure"));
    assert_eq!(failure.pool_closures, vec![true, true]);
    for pool in retained {
        assert_eq!(pool.acquire().await.err(), Some(crate::PoolError::Closed));
    }
    assert_eq!(
        std::fs::read(&failure.quarantine).expect("retained marker"),
        before
    );
}

#[cfg(unix)]
#[tokio::test]
async fn panic_is_joined_before_supervisor_returns_and_quarantine_remains() {
    let directory = tempfile::tempdir().expect("owned fixture directory");
    let guard = ExclusiveFixture::arm(directory.path(), FixtureScope::Default).expect("arm");
    let retained = [
        guard.retain(lazy_pool()).expect("first retained pool"),
        guard.retain(lazy_pool()).expect("second retained pool"),
    ];
    let before = std::fs::read(&guard.quarantine).expect("published marker");
    let result: Result<(), _> = guard
        .supervise(FixtureDeadline::Upgrade, async {
            panic!("owned fixture causal panic")
        })
        .await;
    let failure = result.expect_err("panic must be captured");
    assert_eq!(failure.primary, Some("fixture_exercise_panicked"));
    assert_eq!(failure.pool_closures, vec![true, true]);
    for pool in retained {
        assert_eq!(pool.acquire().await.err(), Some(crate::PoolError::Closed));
    }
    assert_eq!(
        std::fs::read(&failure.quarantine).expect("retained marker"),
        before
    );
    assert!(failure.quarantine.is_file());
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires separate receipt-owned quiet disposable PostgreSQL fixture"]
async fn query_timeout_disposes_retained_pool_and_exact_owned_backend() {
    use crate::exclusive_fixture_settings::settings;
    let state = std::path::PathBuf::from(
        std::env::var("CODEX_TEST_POSTGRES_DISPOSAL_STATE")
            .expect("required distinct disposable fixture"),
    );
    let state_for_identity = state.clone();
    // Join identity verification; each command owns its deadline and disposal.
    let verified = tokio::spawn(async move { settings(&state_for_identity, "runtime").await })
        .await
        .expect("captured identity")
        .expect("fixture identity must match");
    let observer_settings = settings(&state, "runtime")
        .await
        .expect("observer fixture identity must match");
    let guard = ExclusiveFixture::arm(&state, FixtureScope::DisposalProbe).expect("arm");
    let retained = guard.clone();
    let identity = Arc::new(Mutex::new(None));
    let actor_identity = identity.clone();
    let setup=tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(/*secs*/ 8),async move {
            let pool=retained.retain(PostgresPool::connect(verified).await
                .map_err(|_| "disposal_pool_connect")?)?;
            let mut connection=pool.acquire().await.map_err(|_| "disposal_acquire")?;
            connection.close_on_drop();
            let actual:(i32,String)=sqlx::query_as("SELECT pid,backend_start::text FROM pg_catalog.pg_stat_activity WHERE pid=pg_backend_pid()")
                .fetch_one(&mut *connection).await.map_err(|_| "disposal_identity")?;
            *actor_identity.lock().map_err(|_| "disposal_identity_registry")?=Some(actual);
            Ok::<_,&'static str>(connection)
        }).await
    }).await;
    let observed_identity = identity.lock().map(|slot| slot.clone()).ok().flatten();
    let readiness_identity = observed_identity.clone();
    let observers = Arc::new(Mutex::new(None));
    let observer_registry = observers.clone();
    let (ready, readiness) = tokio::sync::oneshot::channel();
    let active_query = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(/*secs*/ 8),async move {
            let observer=Arc::new(PostgresPool::connect(observer_settings).await.map_err(|_| "observer_connect")?);
            *observer_registry.lock().map_err(|_| "observer_registry")?=Some(observer.clone());
            let (pid,start)=readiness_identity.ok_or("query_not_reached")?;
            let mut connection=observer.acquire().await.map_err(|_| "observer_acquire")?;
            connection.close_on_drop();
            loop {
                let active:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND backend_start::text=$2 AND state='active' AND query='SELECT pg_catalog.pg_sleep(60)')")
                    .bind(pid).bind(&start).fetch_one(&mut *connection).await.map_err(|_| "observer_readiness")?;
                if active { ready.send(()).map_err(|_| "readiness_receiver_lost")?;return Ok::<_,&'static str>(true); }
                tokio::time::sleep(Duration::from_millis(/*millis*/ 10)).await;
            }
        }).await
    });
    let result: Result<(), _> = guard
        .supervise(FixtureDeadline::ObservedQuery(readiness), async move {
            let mut connection = match setup {
                Ok(Ok(Ok(connection))) => connection,
                _ => return Err("disposal_setup_not_completed"),
            };
            sqlx::query("SELECT pg_catalog.pg_sleep(60)")
                .execute(&mut *connection)
                .await
                .map_err(|_| "disposal_query")?;
            Ok(())
        })
        .await;
    let readiness_outcome = active_query.await;
    let disposed_pool = observers.lock().map(|slot| slot.clone()).ok().flatten();
    let observed=tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(/*secs*/ 8),async move {
            let observer=disposed_pool.ok_or("observer_missing")?;
            let (pid,start)=observed_identity.ok_or("query_not_reached")?;
            let mut connection=observer.acquire().await.map_err(|_| "observer_acquire")?;
            connection.close_on_drop();
            loop {
                let present:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND backend_start::text=$2)")
                    .bind(pid).bind(&start).fetch_one(&mut *connection).await.map_err(|_| "observer_query")?;
                if !present { return Ok::<_,&'static str>(false); }
                tokio::time::sleep(Duration::from_millis(/*millis*/ 10)).await;
            }
        }).await
    }).await;
    let observer_pool = observers.lock().map(|slot| slot.clone());
    let observer_closed = match observer_pool {
        Ok(Some(pool)) => tokio::time::timeout(Duration::from_secs(/*secs*/ 5), pool.close())
            .await
            .is_ok_and(|result| result.is_ok()),
        _ => false,
    };
    let failure = result.expect_err("causal long query must timeout");
    assert_eq!(failure.primary, Some("fixture_exercise_timeout"));
    assert_eq!(failure.pool_closures, vec![true]);
    assert!(matches!(readiness_outcome, Ok(Ok(Ok(true)))));
    assert!(matches!(observed, Ok(Ok(Ok(false)))));
    assert!(observer_closed);
    assert!(failure.quarantine.is_file());
}

#[cfg(unix)]
#[tokio::test]
async fn successful_exercise_returns_result_and_retains_quarantine_for_external_verification() {
    let directory = tempfile::tempdir().expect("owned fixture directory");
    let guard = ExclusiveFixture::arm(directory.path(), FixtureScope::Default).expect("arm");
    let retained = [
        guard.retain(lazy_pool()).expect("first retained pool"),
        guard.retain(lazy_pool()).expect("second retained pool"),
    ];
    let quarantine = guard.quarantine.clone();
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(&quarantine).expect("marker metadata");
    let before = (
        metadata.dev(),
        metadata.ino(),
        std::fs::read(&quarantine).expect("published marker"),
    );
    let artifact = directory.path().join("exercise-result");
    let result = guard
        .supervise(FixtureDeadline::Upgrade, async move {
            std::fs::write(&artifact, b"completed exercise").map_err(|_| "exercise_write")?;
            std::fs::read(&artifact).map_err(|_| "exercise_read")
        })
        .await
        .expect("successful exercise");
    assert_eq!(result, b"completed exercise");
    for pool in retained {
        assert_eq!(pool.acquire().await.err(), Some(crate::PoolError::Closed));
    }
    let second = ExclusiveFixture::arm(directory.path(), FixtureScope::Default);
    assert!(matches!(second, Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists));
    let metadata = std::fs::metadata(&quarantine).expect("retained metadata");
    assert_eq!(
        (
            metadata.dev(),
            metadata.ino(),
            std::fs::read(&quarantine).expect("retained marker")
        ),
        before
    );
}

#[tokio::test]
async fn missing_query_observation_refuses_and_closes_retained_pool() {
    let directory = tempfile::tempdir().expect("owned fixture directory");
    let guard = ExclusiveFixture::arm(directory.path(), FixtureScope::DisposalProbe).expect("arm");
    let retained = guard.retain(lazy_pool()).expect("retained pool");
    let before = std::fs::read(&guard.quarantine).expect("published marker");
    let (sender, readiness) = tokio::sync::oneshot::channel();
    drop(sender);
    let failure = guard
        .supervise(FixtureDeadline::ObservedQuery(readiness), async {
            std::future::pending::<Result<(), &'static str>>().await
        })
        .await
        .expect_err("unobserved query must refuse");
    assert_eq!(failure.primary, Some("disposal_query_not_observed"));
    assert_eq!(failure.pool_closures, vec![true]);
    assert_eq!(
        retained.acquire().await.err(),
        Some(crate::PoolError::Closed)
    );
    assert_eq!(
        std::fs::read(&failure.quarantine).expect("retained marker"),
        before
    );
}

fn lazy_pool() -> PostgresPool {
    PostgresPool {
        pool: sqlx_postgres::PgPoolOptions::new().connect_lazy_with(
            sqlx_postgres::PgConnectOptions::new()
                .host("127.0.0.1")
                .port(1),
        ),
        acquire_timeout: Duration::from_secs(1),
        selected_search_path: None,
        activation_binding: std::sync::OnceLock::new(),
    }
}
