//! Unactivated all-domain observations for the real reverse-migration fixture.
//! Production stores must produce every changed domain before this is a test.
use crate::domain::Domain;
use crate::domain::DomainOps;
use crate::source::SqliteSource;
use anyhow::Result;
use anyhow::ensure;
use codex_postgres_runtime::PostgresPool;
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) type Matrix = BTreeMap<&'static str, Vec<Value>>;

async fn remote_domain<D: DomainOps>(pool: &PostgresPool) -> Result<Vec<Value>> {
    let mut connection = pool.acquire().await?;
    let mut after = None;
    let mut records = Vec::new();
    for _ in 0..1024 {
        let page = D::read_back(&mut connection, after.as_deref(), 32).await?;
        if page.is_empty() {
            return Ok(records);
        }
        let next = D::key(page.last().expect("nonempty page"));
        ensure!(
            after.as_ref().is_none_or(|old| old < &next),
            "nonadvancing {} page",
            D::DOMAIN.name()
        );
        records.extend(
            page.iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
        );
        after = Some(next);
    }
    anyhow::bail!("bounded remote {} observation exhausted", D::DOMAIN.name())
}

async fn local_domain<D: DomainOps>(source: &SqliteSource) -> Result<Vec<Value>> {
    let mut after = None;
    let mut records = Vec::new();
    for _ in 0..1024 {
        let page = D::export(source, after.as_deref(), 32).await?;
        if page.is_empty() {
            return Ok(records);
        }
        let next = D::key(page.last().expect("nonempty page"));
        ensure!(
            after.as_ref().is_none_or(|old| old < &next),
            "nonadvancing {} page",
            D::DOMAIN.name()
        );
        records.extend(
            page.iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
        );
        after = Some(next);
    }
    anyhow::bail!("bounded local {} observation exhausted", D::DOMAIN.name())
}

macro_rules! every_domain {
    ($matrix:ident, $operation:ident, $original:expr) => {{
        macro_rules! observe {
            ($kind:ty) => {
                $matrix.insert(
                    <$kind as DomainOps>::DOMAIN.name(),
                    $operation::<$kind>($original).await?,
                );
            };
        }
        observe!(crate::sections::Sections);
        observe!(crate::projects::Projects);
        observe!(crate::projects::ProjectKeys);
        observe!(crate::threads::Threads);
        observe!(crate::attachments::Attachments);
        observe!(crate::attachments::SpawnEdges);
        observe!(crate::rollouts::Rollouts);
        observe!(crate::goals::Goals);
        observe!(crate::queue::QueuedItems);
        observe!(crate::queue::QueueRevisions);
        observe!(crate::logs::Logs);
        observe!(crate::memory::Stage1Outputs);
        observe!(crate::memory::MemoryJobs);
        observe!(crate::memory::MemoryProgress);
        observe!(crate::board::DeletedBoards);
        observe!(crate::board::Channels);
        observe!(crate::board::Posts);
        observe!(crate::board::Subscriptions);
        observe!(crate::board::OptOuts);
        observe!(crate::external_imports::ExternalImports);
        let expected = Domain::ALL
            .iter()
            .map(|domain| domain.name())
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(
            $matrix
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                == expected,
            "domain observation coverage differs from canonical ALL"
        );
    }};
}

pub(super) async fn remote(pool: &PostgresPool) -> Result<Matrix> {
    let mut matrix = Matrix::new();
    every_domain!(matrix, remote_domain, pool);
    Ok(matrix)
}

/// The coherent caller must close every captured original read SDK before verdict.
/// A source without the mandatory read-owner hook is not a cleanup proof.
pub(super) async fn local(source: &SqliteSource) -> Result<Matrix> {
    let mut matrix = Matrix::new();
    every_domain!(matrix, local_domain, source);
    Ok(matrix)
}

pub(super) fn changed_all(before: &Matrix, after: &Matrix) -> Result<()> {
    ensure!(
        before.len() == Domain::ALL.len() && after.len() == Domain::ALL.len(),
        "incomplete domain matrix"
    );
    for domain in Domain::ALL {
        let rows = after
            .get(domain.name())
            .ok_or_else(|| anyhow::anyhow!("missing {}", domain.name()))?;
        ensure!(!rows.is_empty(), "vacuous {} latest write", domain.name());
        ensure!(
            before.get(domain.name()) != Some(rows),
            "no latest remote change in {}",
            domain.name()
        );
    }
    Ok(())
}

/// Fixed fixture-only SDK ledger. Source::pool captures each lazy pool before
/// acquisition; caller closes only after the same observation body returns.
pub(crate) struct ReadOwner {
    state: std::sync::Mutex<Reads>,
}
struct Reads {
    pools: Vec<sqlx::SqlitePool>,
    rejected: Option<sqlx::SqlitePool>,
    sealed: bool,
    hold: Option<std::sync::Arc<ReadOwner>>,
}
impl ReadOwner {
    pub(super) fn prepare() -> std::sync::Arc<Self> {
        let original = std::sync::Arc::new(Self {
            state: std::sync::Mutex::new(Reads {
                pools: Vec::with_capacity(512),
                rejected: None,
                sealed: false,
                hold: None,
            }),
        });
        original.state.lock().expect("new fixture ledger").hold =
            Some(std::sync::Arc::clone(&original));
        original
    }
    pub(crate) fn capture(&self, pool: sqlx::SqlitePool) -> Result<()> {
        let observed = self.state.lock();
        let poisoned = observed.is_err();
        let mut state = observed.unwrap_or_else(|raw| raw.into_inner());
        if state.sealed || state.pools.len() == 512 {
            // The first refused acquisition terminates this serial observer.
            // The exact refused SDK still has an exterior owner for cleanup.
            ensure!(state.rejected.is_none(), "multiple rejected read creations");
            state.rejected = Some(pool);
            anyhow::bail!("fixture read admission refused");
        }
        state.pools.push(pool);
        ensure!(!poisoned, "fixture read owner poisoned");
        Ok(())
    }
    pub(super) async fn close_after_body(&self) -> Result<()> {
        let pools = {
            let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
            state.sealed = true;
            state
                .pools
                .iter()
                .chain(state.rejected.iter())
                .cloned()
                .collect::<Vec<_>>()
        };
        for pool in pools {
            pool.close().await;
        }
        let mut state = self.state.lock().unwrap_or_else(|raw| raw.into_inner());
        // SAME actual SDK close futures all completed before hold release.
        ensure!(!self.state.is_poisoned(), "fixture read cleanup poisoned");
        state.hold = None;
        Ok(())
    }
}
