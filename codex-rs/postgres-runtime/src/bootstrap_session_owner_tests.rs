//! Real empty-bootstrap admission and original-session outcome controls.
use super::*;
use crate::ConnectionSettings;
use crate::NamedNamespace;
use crate::PoolLimits;
use std::path::Path;
use std::time::Duration;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Failure {
    original: Arc<BootstrapSessionOwner>,
    result: std::result::Result<(), BootstrapError>,
    acquisition: Option<crate::PoolError>,
}
impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_safe(f)
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_safe(f)
    }
}
impl Failure {
    fn fmt_safe(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let retained = (&self.original, &self.result, &self.acquisition);
        let _ = retained;
        f.write_str("bootstrap control refusal [original diagnostics retained]")
    }
}
impl std::error::Error for Failure {}
async fn require_actual_empty(
    owner: &Arc<BootstrapSessionOwner>,
    namespace: Option<&NamedNamespace>,
) -> std::result::Result<(), BootstrapError> {
    let mut loan = owner.loan().await?;
    let empty = owner.query(sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1) AND NOT EXISTS (
SELECT 1 FROM (
SELECT relnamespace AS namespace FROM pg_catalog.pg_class
UNION ALL SELECT pronamespace FROM pg_catalog.pg_proc
UNION ALL SELECT typnamespace FROM pg_catalog.pg_type
UNION ALL SELECT collnamespace FROM pg_catalog.pg_collation
UNION ALL SELECT connamespace FROM pg_catalog.pg_constraint
UNION ALL SELECT connamespace FROM pg_catalog.pg_conversion
UNION ALL SELECT oprnamespace FROM pg_catalog.pg_operator
UNION ALL SELECT opcnamespace FROM pg_catalog.pg_opclass
UNION ALL SELECT opfnamespace FROM pg_catalog.pg_opfamily
UNION ALL SELECT cfgnamespace FROM pg_catalog.pg_ts_config
UNION ALL SELECT dictnamespace FROM pg_catalog.pg_ts_dict
UNION ALL SELECT prsnamespace FROM pg_catalog.pg_ts_parser
UNION ALL SELECT tmplnamespace FROM pg_catalog.pg_ts_template
UNION ALL SELECT stxnamespace FROM pg_catalog.pg_statistic_ext
UNION ALL SELECT extnamespace FROM pg_catalog.pg_extension
) objects JOIN pg_catalog.pg_namespace n ON n.oid=objects.namespace WHERE n.nspname=$1)").bind(namespace.map_or("codex_storage", NamedNamespace::schema)).fetch_one(loan.connection()?), false).await?;
    if empty {
        Ok(())
    } else {
        Err(BootstrapError::IncompatibleNamespace)
    }
}

async fn selected_bootstrap(state: &Path, named: bool) -> Result<()> {
    let receipt: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("receipt.json"))?)?;
    let role = if named {
        "isolation_migrator"
    } else {
        "migrator"
    };
    let namespace = named
        .then(|| NamedNamespace::new("codex_storage_isolation"))
        .transpose()?;
    let settings = ConnectionSettings {
        host: "localhost".into(),
        port: u16::try_from(receipt["port"].as_u64().ok_or("missing fixture port")?)?,
        database: "codex".into(),
        username: format!("codex_{role}"),
        password: std::fs::read_to_string(state.join(format!("secrets/{role}.password")))?
            .trim()
            .to_owned()
            .into(),
        ca_certificate: state.join("secrets/ca.crt"),
        limits: PoolLimits {
            connect_timeout: Duration::from_secs(5),
            acquire_timeout: Duration::from_secs(5),
            max_connections: 1,
        },
    };
    // Capture the SAME latent SDK pool before the factory's first SQL poll.
    let mut captured = None;
    let connected = crate::PostgresPool::connect_retained_in_namespace(
        settings,
        namespace.as_ref(),
        &mut |pool| {
            captured = Some(pool);
            Ok(())
        },
    )
    .await;
    let Some(pool) = captured else {
        return Err("no SDK dispatch captured".into());
    };
    let owner = BootstrapSessionOwner::new(Arc::clone(&pool));
    let result = match connected {
        Ok(returned) if Arc::ptr_eq(&returned, &pool) => {
            async {
                require_actual_empty(&owner, namespace.as_ref()).await?;
                match namespace.as_ref() {
                    Some(namespace) => {
                        crate::bootstrap_named_namespace_retained(&owner, namespace).await
                    }
                    None => crate::bootstrap_codex_storage_retained(&owner).await,
                }
            }
            .await
        }
        Ok(_) => Err(BootstrapError::Migration),
        Err(error) => Err(BootstrapError::Connection(error)),
    };
    // Never infer task/session termination from a bare bootstrap error.
    let reconciled = owner.reconcile_original().await;
    let proof = match reconciled {
        Ok(proof) => proof,
        Err(original) => {
            let retained = (owner, pool, original, result);
            let terminal = std::future::pending::<Result<()>>().await;
            let _ = retained;
            return terminal;
        }
    };
    let outcome = owner.observed_original_outcome();
    let covers = proof.covers_pool(&pool);
    let refused = proof.refused();
    if !covers {
        return Err(Box::new(Failure {
            original: owner,
            result,
            acquisition: None,
        }));
    }
    // Session proof precedes real SDK closure; both finish before diagnostics.
    let mut close = Box::pin(pool.close_original_sdk());
    let late = tokio::time::timeout(Duration::from_secs(10), &mut close)
        .await
        .is_err();
    if late {
        close.await;
    }
    let mut acquisition = None;
    let closed = match pool.acquire().await {
        Err(crate::PoolError::Closed) => true,
        Ok(connection) => {
            drop(connection);
            false
        }
        Err(error) => {
            acquisition = Some(error);
            false
        }
    };
    if result.is_err() || late || !closed {
        return Err(Box::new(Failure {
            original: owner,
            result,
            acquisition,
        }));
    }
    if !covers || refused || !matches!(outcome, Some((_, true))) {
        return Err(Box::new(Failure {
            original: owner,
            result,
            acquisition,
        }));
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires an admitted isolated default PostgreSQL bootstrap fixture"]
async fn actual_default_bootstrap_original_committed_session() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var(
            "CODEX_TEST_POSTGRES_BOOTSTRAP_DEFAULT_STATE",
        )?),
        false,
    )
    .await
}
#[tokio::test]
#[ignore = "requires an admitted isolated named PostgreSQL bootstrap fixture"]
async fn actual_named_bootstrap_original_committed_session() -> Result<()> {
    selected_bootstrap(
        Path::new(&std::env::var("CODEX_TEST_POSTGRES_BOOTSTRAP_NAMED_STATE")?),
        true,
    )
    .await
}
