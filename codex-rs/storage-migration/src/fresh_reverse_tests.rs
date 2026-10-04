//! Real latest remote writes returned to a separate fresh local home.
//! Same-machine fixture only, not packaged install/second-host/global admission.
use super::fresh_reverse_observation as observe;
use super::latest_remote_writes;
use crate::Domain;
use crate::Migrator;
use crate::ReturnCutover;
use crate::SqliteSource;
use crate::SqliteTarget;
use anyhow::Result;
use anyhow::ensure;
use codex_postgres_runtime::PostgresPool;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_storage_authority::ActiveBackend;
use codex_storage_authority::attach_remote_dataset;
use codex_storage_authority::load_authority;
use codex_utils_absolute_path::test_support::PathExt;
use std::path::Path;
use std::sync::Arc;

pub(super) async fn fresh_reverse_phase(
    pool: &Arc<PostgresPool>,
    source: &SqliteSource,
    retired_home: &Path,
) {
    let reads = observe::ReadOwner::prepare();
    let staged_reads = observe::ReadOwner::prepare();
    let fresh_reads = observe::ReadOwner::prepare();
    let old_source = source.clone().with_reverse_reads(Arc::clone(&reads));
    // Forward import and its actual verification use the same retained source.
    super::return_tests::remote_authority(pool, &old_source, retired_home).await;
    let fresh_directory = tempfile::tempdir().expect("distinct fresh destination");
    let fresh = fresh_directory.path().join("fresh-local");
    std::fs::create_dir(&fresh).expect("fresh destination");
    let mut target: Option<SqliteTarget> = None;
    let mut runtime: Option<Arc<StateRuntime>> = None;
    let result: Result<()> = async {
        ensure!(
            fresh != retired_home && !fresh.starts_with(retired_home),
            "destination aliases retired source"
        );
        ensure!(
            std::fs::read_dir(&fresh)?.next().is_none(),
            "destination already contains data"
        );
        let old_authority = load_authority(retired_home)?;
        let activation = Migrator::new(source.clone(), Arc::clone(pool))
            .activation_state()
            .await?;
        ensure!(
            !activation.migrating && !activation.retired,
            "remote is not authoritative"
        );
        let dataset = activation
            .dataset_id
            .ok_or_else(|| anyhow::anyhow!("actual dataset missing"))?;
        let generation = u64::try_from(activation.generation)?;
        let published_generation = generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("generation exhausted"))?;
        ensure!(
            old_authority.identity.dataset_id == dataset
                && old_authority.identity.generation == generation,
            "old home differs from actual remote tuple"
        );
        let attached = attach_remote_dataset(&fresh, dataset, generation)?;
        ensure!(
            attached.identity.dataset_id == dataset && attached.identity.generation == generation,
            "fresh attachment differs from actual remote tuple"
        );
        ensure!(
            attached.identity.home_id != old_authority.identity.home_id
                && attached.identity.instance_id != old_authority.identity.instance_id,
            "fresh identity copied old home"
        );
        let original_local = observe::local(&old_source).await?;
        let before = observe::remote(pool).await?;
        let writes = latest_remote_writes::write(pool).await?;
        let latest = observe::remote(pool).await?;
        observe::changed_all(&before, &latest)?;
        ensure!(
            observe::local(&old_source).await? == original_local,
            "latest writes leaked into retired local files"
        );
        reads.close_after_body().await?;
        let staging = fresh.join("storage-staging/latest-remote");
        std::fs::create_dir_all(&staging)?;
        target = Some(
            SqliteTarget::create(
                SqliteConfig::new_for_testing(staging.abs()),
                fresh.clone(),
                "migration-provider",
            )
            .await?,
        );
        let original_target = target
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("target missing"))?;
        let staged_source = SqliteSource::new(original_target.staging().clone())
            .relocated_from(fresh.clone())
            .with_reverse_reads(Arc::clone(&staged_reads));
        let exporter = Migrator::new(staged_source.clone(), Arc::clone(pool)).with_batch_size(2);
        let exported = exporter.export(original_target).await?;
        ensure!(
            exported
                .domains
                .iter()
                .map(|(domain, _)| *domain)
                .collect::<Vec<_>>()
                == Domain::ALL,
            "export omitted domain"
        );
        let verified = exporter.verify(exported.run_id).await?;
        ensure!(
            verified.domains.len() == Domain::ALL.len(),
            "verification omitted domain"
        );
        // Verification and recursive rollout prefix reads have returned. Close
        // their exact SDKs before the installation renames the staged files.
        staged_reads.close_after_body().await?;
        original_target.close().await; // SAME original pools before actual file installation.
        let returned = ReturnCutover::new(
            fresh.clone(),
            staging,
            Migrator::new(staged_source, Arc::clone(pool)),
        )
        .execute(exported.run_id)
        .await?;
        ensure!(
            returned.marker.active_backend == ActiveBackend::Local
                && returned.identity.dataset_id == dataset,
            "fresh return did not publish actual local dataset"
        );
        ensure!(
            returned.identity.home_id == attached.identity.home_id
                && returned.identity.instance_id == attached.identity.instance_id
                && returned.identity.generation == published_generation,
            "fresh authority identity changed unexpectedly"
        );
        let local_source = SqliteSource::new(SqliteConfig::new_for_testing(fresh.abs()))
            .with_reverse_reads(Arc::clone(&fresh_reads));
        let actual_local = observe::local(&local_source).await?;
        ensure!(
            actual_local == latest,
            "latest twenty-domain remote records differ in fresh SQLite"
        );
        fresh_reads.close_after_body().await?;
        runtime = Some(
            StateRuntime::init(
                SqliteConfig::new_for_testing(fresh.abs()),
                "migration-provider".into(),
            )
            .await?,
        );
        let original_runtime = runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("runtime missing"))?;
        let parent = original_runtime
            .get_thread(writes.parent.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("latest parent missing"))?;
        let child = original_runtime
            .get_thread(writes.child.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("latest child missing"))?;
        ensure!(
            parent.title == writes.parent.title && child.source == writes.child.source,
            "runtime parent/child identity changed"
        );
        let queued = original_runtime
            .thread_queue()
            .list_page(writes.parent.id, 0, 32)
            .await?;
        ensure!(
            queued.iter().any(|item| item.id == writes.queue_id),
            "latest queue item absent from ordinary runtime"
        );
        ensure!(
            original_runtime
                .thread_goals()
                .get_thread_goal(writes.parent.id)
                .await?
                .is_some(),
            "latest goal absent"
        );
        ensure!(
            parent.rollout_path.starts_with(&fresh) && parent.rollout_path.is_file(),
            "rollout points at retired host"
        );
        let retired = exporter.activation_state().await?;
        ensure!(
            retired.retired && retired.generation == i64::try_from(published_generation)?,
            "actual remote was not retired"
        );
        Ok(())
    }
    .await;
    // Business refusal does not skip same original SDK cleanup. The observer
    // body has returned; no timed-out/replacement query task is used as evidence.
    if let Some(original) = runtime.as_ref() {
        original.close().await;
    }
    if let Some(original) = target.as_ref() {
        original.close().await;
    }
    let read_cleanup = reads.close_after_body().await;
    let staged_cleanup = staged_reads.close_after_body().await;
    let fresh_cleanup = fresh_reads.close_after_body().await;
    // Shared PostgreSQL pool is owned/closed by the enclosing real fixture.
    // This caller does not claim broader reclamation-worker/global closure from
    // the legacy runtime close API or arbitrary cancellation of this outer test.
    if read_cleanup.is_err() || staged_cleanup.is_err() || fresh_cleanup.is_err() {
        panic!(
            "original read SDK cleanup refused: old={read_cleanup:?}, staged={staged_cleanup:?}, fresh={fresh_cleanup:?}; body={result:?}"
        );
    }
    result.expect("latest remote all-domain fresh reverse");
}
