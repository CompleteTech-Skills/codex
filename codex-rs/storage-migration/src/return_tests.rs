//! Failure injection for handing authority back to local files.

use super::tests::metadata;
use super::tests::reset_target;
use super::*;
use crate::Cutover;
use crate::CutoverError;
use crate::InstallPlan;
use crate::RecoveryOutcome;
use crate::ReturnCutover;
use crate::install;
use crate::verify_backup;
use chrono::Utc;
use codex_postgres_queue_store::PostgresQueueStore;
use codex_postgres_thread_catalog::PostgresThreadCatalog;
use codex_protocol::protocol::SessionSource;
use codex_state::SqliteConfig;
use codex_state::StateRuntime;
use codex_state::ThreadMetadata;
use codex_storage_authority::ActiveBackend;
use codex_storage_authority::adopt_quiesced_home;
use codex_storage_authority::load_authority;
use codex_storage_authority::read_cutover;
use codex_thread_store::QueueStore;
use codex_utils_absolute_path::test_support::PathExt;
use sqlx::Acquire;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

const CONTROL_FILES: [&str; 4] = [
    "storage-identity.json",
    "storage-activation.json",
    "storage-cutover.json",
    "storage-install.json",
];

struct ForeignOwnerFaultObjects {
    schema: String,
    owner_role: String,
    function: String,
    trigger: String,
}

struct ReturnTestFixture<'a> {
    pool: &'a Arc<PostgresPool>,
    owner_pool: &'a Arc<PostgresPool>,
    schema: &'a str,
    owner_role: &'a str,
    source: &'a SqliteSource,
    home: &'a Path,
    threads: &'a [ThreadMetadata],
}

struct PreparedReturn {
    returning: ReturnCutover,
    run_id: Uuid,
    staged_home: PathBuf,
    intent: codex_storage_authority::CutoverIntent,
    plan: InstallPlan,
    intent_bytes: Vec<u8>,
    plan_bytes: Vec<u8>,
}

fn quote_test_identifier(identifier: &str) -> String {
    assert!(
        !identifier.is_empty()
            && identifier
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "test SQL identifier must be a lowercase ASCII identifier"
    );
    format!("\"{identifier}\"")
}

async fn owner_ddl(
    owner_pool: &Arc<PostgresPool>,
    schema: &str,
    owner_role: &str,
    statements: &[String],
) {
    let quoted_schema = quote_test_identifier(schema);
    let quoted_owner = quote_test_identifier(owner_role);
    let mut connection = owner_pool.acquire().await.expect("owner connection");
    let mut transaction = connection.begin().await.expect("owner transaction");
    for statement in [
        format!("SET LOCAL ROLE {quoted_owner}"),
        format!("SET LOCAL search_path TO pg_catalog, {quoted_schema}"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement.clone()))
            .execute(&mut *transaction)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    let current_user: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&mut *transaction)
        .await
        .expect("scoped owner role");
    assert_eq!(
        current_user, owner_role,
        "test DDL uses the namespace owner"
    );
    let resolved_schema: Option<String> = sqlx::query_scalar(
        "SELECT n.nspname FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE c.oid = to_regclass('storage_migration_runs')",
    )
    .fetch_optional(&mut *transaction)
    .await
    .expect("resolve the migration table through the scoped search_path");
    assert_eq!(
        resolved_schema.as_deref(),
        Some(schema),
        "test DDL search_path resolves to the selected owned namespace"
    );
    for statement in statements {
        sqlx::query(sqlx::AssertSqlSafe(statement.clone()))
            .execute(&mut *transaction)
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    transaction.commit().await.expect("owner DDL commit");
}

async fn install_foreign_owner_fault(
    owner_pool: &Arc<PostgresPool>,
    schema: &str,
    owner_role: &str,
    run_id: Uuid,
    foreign_run_id: Uuid,
) -> ForeignOwnerFaultObjects {
    let token = Uuid::new_v4().simple().to_string();
    let function = format!("rfo_fn_{token}");
    let trigger = format!("rfo_tr_{token}");
    let quoted_schema = quote_test_identifier(schema);
    let quoted_function = quote_test_identifier(&function);
    let quoted_trigger = quote_test_identifier(&trigger);
    let function_sql = format!(
        "CREATE FUNCTION {quoted_schema}.{quoted_function}() RETURNS trigger LANGUAGE plpgsql AS $body$ BEGIN IF OLD.run_id = '{run_id}'::uuid AND OLD.state = 'migrating' AND NEW.state = 'open' THEN NEW.run_id := '{foreign_run_id}'::uuid; END IF; RETURN NEW; END $body$"
    );
    let trigger_sql = format!(
        "CREATE TRIGGER {quoted_trigger} BEFORE UPDATE OF state ON {quoted_schema}.\"storage_activation\" FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.{quoted_function}()"
    );
    owner_ddl(owner_pool, schema, owner_role, &[function_sql, trigger_sql]).await;
    ForeignOwnerFaultObjects {
        schema: schema.to_string(),
        owner_role: owner_role.to_string(),
        function,
        trigger,
    }
}

async fn remove_foreign_owner_fault(
    owner_pool: &Arc<PostgresPool>,
    objects: &ForeignOwnerFaultObjects,
) {
    let quoted_schema = quote_test_identifier(&objects.schema);
    let quoted_function = quote_test_identifier(&objects.function);
    let quoted_trigger = quote_test_identifier(&objects.trigger);
    owner_ddl(
        owner_pool,
        &objects.schema,
        &objects.owner_role,
        &[
            format!("DROP TRIGGER {quoted_trigger} ON {quoted_schema}.\"storage_activation\""),
            format!("DROP FUNCTION {quoted_schema}.{quoted_function}()"),
        ],
    )
    .await;
}

/// Put the home and the store in the state a finished forward cutover leaves them in.
async fn remote_authority(pool: &Arc<PostgresPool>, source: &SqliteSource, home: &Path) {
    reset_target(pool).await;
    for file in CONTROL_FILES {
        let _ = std::fs::remove_file(home.join(file));
    }
    adopt_quiesced_home(home).expect("adopt");
    let migrator = Migrator::new(source.clone(), pool.clone());
    let summary = migrator.import().await.expect("import");
    migrator.verify(summary.run_id).await.expect("verify");
    Cutover::new(
        home.to_path_buf(),
        Migrator::new(source.clone(), pool.clone()),
    )
    .execute(summary.run_id)
    .await
    .expect("forward cutover");
}

/// Export the dataset into a staged home inside the live home, verified unless asked not to.
async fn staged_return(
    pool: &Arc<PostgresPool>,
    home: &Path,
    name: &str,
    verify: bool,
) -> (ReturnCutover, Uuid, PathBuf) {
    let staged_home = home.join("storage-staging").join(name);
    std::fs::create_dir_all(&staged_home).expect("staging directory");
    let target = SqliteTarget::create(
        SqliteConfig::new_for_testing(staged_home.abs()),
        home.to_path_buf(),
        "migration-provider",
    )
    .await
    .expect("staged databases");
    let staged_source =
        SqliteSource::new(target.staging().clone()).relocated_from(home.to_path_buf());
    let exporter = Migrator::new(staged_source.clone(), pool.clone());
    let summary = exporter.export(&target).await.expect("export");
    if verify {
        exporter
            .verify(summary.run_id)
            .await
            .expect("verify the export");
    }
    target.close().await;
    (
        ReturnCutover::new(
            home.to_path_buf(),
            staged_home.clone(),
            Migrator::new(staged_source, pool.clone()),
        ),
        summary.run_id,
        staged_home,
    )
}

fn backend(home: &Path) -> ActiveBackend {
    load_authority(home)
        .expect("settled authority")
        .marker
        .active_backend
}

async fn dataset_state(pool: &Arc<PostgresPool>, source: &SqliteSource) -> ActivationState {
    Migrator::new(source.clone(), pool.clone())
        .activation_state()
        .await
        .expect("activation state")
}

async fn completed_finish_is_repeatable(
    pool: &Arc<PostgresPool>,
    source: &SqliteSource,
    home: &Path,
) {
    remote_authority(pool, source, home).await;
    let (returning, run_id, staged_home) = staged_return(pool, home, "repeat-finish", true).await;
    let intent = returning.prepare(run_id).expect("prepare return");
    let plan = install::read_plan(home)
        .expect("read plan")
        .expect("install plan exists");
    let plan_bytes = std::fs::read(home.join("storage-install.json")).expect("plan bytes");
    returning
        .publish(&intent)
        .await
        .expect("retire exact export");
    returning.install().expect("install exact plan");
    let manifest_path = plan.backup_dir.join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path).expect("installed backup manifest");
    let backup_unit = plan
        .units
        .iter()
        .find(|unit| unit.live_sha256.is_some())
        .expect("plan has a file backup");
    let backup_file = plan.backup_dir.join(&backup_unit.name);
    let backup_file_bytes = std::fs::read(&backup_file).expect("file backup bytes");
    let live_unit = plan
        .units
        .iter()
        .find(|unit| !unit.is_dir && unit.staged_sha256.is_some())
        .expect("plan has a staged file");
    let live_file = home.join(&live_unit.name);
    let live_file_bytes = std::fs::read(&live_file).expect("installed live file bytes");
    let identity_before_finish =
        std::fs::read(home.join("storage-identity.json")).expect("identity before finish");
    let activation_before_finish =
        std::fs::read(home.join("storage-activation.json")).expect("activation before finish");
    let cutover_before_finish =
        std::fs::read(home.join("storage-cutover.json")).expect("cutover before finish");

    std::fs::remove_file(&manifest_path).expect("remove manifest before finish");
    assert!(returning.finish(&intent).is_err());
    assert!(!manifest_path.exists());
    assert_eq!(
        read_cutover(home).expect("intent after missing manifest"),
        Some(intent.clone())
    );
    assert_eq!(
        install::read_plan(home).expect("plan after missing manifest"),
        Some(plan.clone())
    );
    assert_eq!(
        std::fs::read(home.join("storage-identity.json")).expect("identity after missing manifest"),
        identity_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-activation.json"))
            .expect("activation after missing manifest"),
        activation_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-cutover.json")).expect("cutover after missing manifest"),
        cutover_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-install.json")).expect("plan after missing manifest"),
        plan_bytes
    );
    assert_eq!(
        std::fs::read(&live_file).expect("live after missing manifest"),
        live_file_bytes
    );
    assert_eq!(
        std::fs::read(&backup_file).expect("backup after missing manifest"),
        backup_file_bytes
    );
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore manifest before finish");

    let mut corrupt_backup_bytes = backup_file_bytes.clone();
    corrupt_backup_bytes.push(b'!');
    std::fs::write(&backup_file, &corrupt_backup_bytes).expect("corrupt backup before finish");
    assert!(returning.finish(&intent).is_err());
    assert_eq!(
        read_cutover(home).expect("intent after corrupt backup"),
        Some(intent.clone())
    );
    assert_eq!(
        install::read_plan(home).expect("plan after corrupt backup"),
        Some(plan.clone())
    );
    assert_eq!(
        std::fs::read(home.join("storage-identity.json")).expect("identity after corrupt backup"),
        identity_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-activation.json"))
            .expect("activation after corrupt backup"),
        activation_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-cutover.json")).expect("cutover after corrupt backup"),
        cutover_before_finish
    );
    assert_eq!(
        std::fs::read(home.join("storage-install.json")).expect("plan after corrupt backup"),
        plan_bytes
    );
    assert_eq!(
        std::fs::read(&manifest_path).expect("manifest after corrupt backup"),
        manifest_bytes
    );
    assert_eq!(
        std::fs::read(&live_file).expect("live after corrupt backup"),
        live_file_bytes
    );
    assert_eq!(
        std::fs::read(&backup_file).expect("corrupt backup remains"),
        corrupt_backup_bytes
    );
    std::fs::write(&backup_file, &backup_file_bytes).expect("restore backup before finish");

    let first = returning.finish(&intent).expect("first finish");
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    assert!(verify_backup(&plan).expect("backup before replay"));
    let identity_before =
        std::fs::read(home.join("storage-identity.json")).expect("identity before repeated finish");
    let activation_after_finish =
        std::fs::read(home.join("storage-activation.json")).expect("activation after finish");
    let assert_replay_refused_without_mutation =
        |candidate: &ReturnCutover, expected_manifest: &[u8], expected_backup: &[u8]| {
            assert!(candidate.finish(&intent).is_err());
            assert_eq!(
                read_cutover(home).expect("intent after refused replay"),
                None
            );
            assert_eq!(
                install::read_plan(home).expect("plan after refused replay"),
                None
            );
            assert_eq!(
                std::fs::read(home.join("storage-identity.json"))
                    .expect("identity after refused replay"),
                identity_before
            );
            assert_eq!(
                std::fs::read(home.join("storage-activation.json"))
                    .expect("activation after refused replay"),
                activation_after_finish
            );
            assert_eq!(
                std::fs::read(&manifest_path).expect("manifest after refused replay"),
                expected_manifest
            );
            assert_eq!(
                std::fs::read(&live_file).expect("live after refused replay"),
                live_file_bytes
            );
            assert_eq!(
                std::fs::read(&backup_file).expect("backup after refused replay"),
                expected_backup
            );
        };
    assert_eq!(
        returning
            .finish(&intent)
            .expect("completed finish must remain safely repeatable"),
        first
    );
    assert_eq!(
        std::fs::read(home.join("storage-identity.json")).expect("identity after replay"),
        identity_before
    );

    let mut foreign = intent.clone();
    foreign.run_id = Uuid::new_v4();
    assert!(returning.finish(&foreign).is_err());
    let mut wrong_dataset = intent.clone();
    wrong_dataset.dataset_id = Uuid::new_v4();
    assert!(returning.finish(&wrong_dataset).is_err());
    let mut wrong_generation = intent.clone();
    wrong_generation.from_generation += 1;
    wrong_generation.to_generation += 1;
    assert!(returning.finish(&wrong_generation).is_err());

    std::fs::remove_file(&manifest_path).expect("simulate missing completion evidence");
    assert!(returning.finish(&intent).is_err());
    assert!(!manifest_path.exists());
    assert_eq!(
        read_cutover(home).expect("intent after missing replay manifest"),
        None
    );
    assert_eq!(
        install::read_plan(home).expect("plan after missing replay manifest"),
        None
    );
    assert_eq!(
        std::fs::read(home.join("storage-identity.json"))
            .expect("identity after missing replay manifest"),
        identity_before
    );
    assert_eq!(
        std::fs::read(home.join("storage-activation.json"))
            .expect("activation after missing replay manifest"),
        activation_after_finish
    );
    assert_eq!(
        std::fs::read(&live_file).expect("live after missing replay manifest"),
        live_file_bytes
    );
    assert_eq!(
        std::fs::read(&backup_file).expect("backup after missing replay manifest"),
        backup_file_bytes
    );
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore completion evidence");

    let wrong_staged_return = ReturnCutover::new(
        home.to_path_buf(),
        staged_home.join("wrong-staged-home"),
        Migrator::new(source.clone(), pool.clone()),
    );
    assert_replay_refused_without_mutation(
        &wrong_staged_return,
        &manifest_bytes,
        &backup_file_bytes,
    );

    let mut wrong_staged_manifest = plan.clone();
    wrong_staged_manifest.staged_home = staged_home.join("wrong-staged-home");
    let wrong_staged_bytes =
        serde_json::to_vec(&wrong_staged_manifest).expect("wrong staged manifest");
    std::fs::write(&manifest_path, &wrong_staged_bytes).expect("write wrong staged manifest");
    assert_replay_refused_without_mutation(&returning, &wrong_staged_bytes, &backup_file_bytes);
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore staged manifest");

    let mut wrong_backup_manifest = plan.clone();
    wrong_backup_manifest.backup_dir = home.join("wrong-backup");
    let wrong_backup_bytes =
        serde_json::to_vec(&wrong_backup_manifest).expect("wrong backup manifest");
    std::fs::write(&manifest_path, &wrong_backup_bytes).expect("write wrong backup manifest");
    assert_replay_refused_without_mutation(&returning, &wrong_backup_bytes, &backup_file_bytes);
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore backup manifest");

    let mut unsafe_unit_manifest = plan.clone();
    unsafe_unit_manifest.units[0].name = "../escape".to_string();
    let unsafe_unit_bytes =
        serde_json::to_vec(&unsafe_unit_manifest).expect("unsafe unit manifest");
    std::fs::write(&manifest_path, &unsafe_unit_bytes).expect("write unsafe unit manifest");
    assert_replay_refused_without_mutation(&returning, &unsafe_unit_bytes, &backup_file_bytes);
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore unit manifest");

    let mut duplicate_unit_manifest = plan.clone();
    let first_unit = duplicate_unit_manifest.units[0].clone();
    duplicate_unit_manifest.units.push(first_unit);
    let duplicate_unit_bytes =
        serde_json::to_vec(&duplicate_unit_manifest).expect("duplicate unit manifest");
    std::fs::write(&manifest_path, &duplicate_unit_bytes).expect("write duplicate unit manifest");
    assert_replay_refused_without_mutation(&returning, &duplicate_unit_bytes, &backup_file_bytes);
    std::fs::write(&manifest_path, &manifest_bytes).expect("restore duplicate unit manifest");

    let mut corrupt_replay_backup = backup_file_bytes.clone();
    corrupt_replay_backup.push(b'!');
    std::fs::write(&backup_file, &corrupt_replay_backup).expect("corrupt backup before replay");
    assert_replay_refused_without_mutation(&returning, &manifest_bytes, &corrupt_replay_backup);
    std::fs::write(&backup_file, &backup_file_bytes).expect("restore backup before replay");

    let control_path = home.join("storage-install.json");
    let mut foreign_plan = plan.clone();
    foreign_plan.run_id = Uuid::new_v4();
    foreign_plan.backup_dir = plan
        .backup_dir
        .parent()
        .expect("backup root")
        .join(format!("{}-before-return", foreign_plan.run_id));
    let foreign_bytes = serde_json::to_vec(&foreign_plan).expect("foreign plan bytes");
    std::fs::write(&control_path, &foreign_bytes).expect("write foreign plan");
    assert!(returning.finish(&intent).is_err());
    assert_eq!(
        std::fs::read(&control_path).expect("foreign plan remains"),
        foreign_bytes
    );
    std::fs::write(&control_path, &plan_bytes).expect("restore exact plan");
    assert_eq!(
        returning
            .finish(&intent)
            .expect("replay matching-plan cleanup"),
        first
    );
    assert_eq!(install::read_plan(home).expect("plan"), None);
    assert_eq!(
        std::fs::read(&manifest_path).expect("retained manifest"),
        manifest_bytes
    );
    assert!(verify_backup(&plan).expect("retained backup"));
    assert!(staged_home.exists());
}

async fn prepare_remote_return(fixture: &ReturnTestFixture<'_>, name: &str) -> PreparedReturn {
    remote_authority(fixture.pool, fixture.source, fixture.home).await;
    let (returning, run_id, staged_home) =
        staged_return(fixture.pool, fixture.home, name, true).await;
    let intent = returning.prepare(run_id).expect("prepare return");
    let plan = install::read_plan(fixture.home)
        .expect("read install plan")
        .expect("install plan exists");
    let intent_bytes =
        std::fs::read(fixture.home.join("storage-cutover.json")).expect("cutover intent bytes");
    let plan_bytes =
        std::fs::read(fixture.home.join("storage-install.json")).expect("install plan bytes");
    PreparedReturn {
        returning,
        run_id,
        staged_home,
        intent,
        plan,
        intent_bytes,
        plan_bytes,
    }
}

async fn assert_remote_open(fixture: &ReturnTestFixture<'_>, run_id: Uuid, message: &str) {
    let authority = load_authority(fixture.home).expect("remote authority");
    assert_eq!(authority.marker.active_backend, ActiveBackend::Remote);
    let state = dataset_state(fixture.pool, fixture.source).await;
    assert!(!state.migrating && !state.retired);
    assert_eq!(state.run_id, Some(run_id));
    assert_eq!(
        state.generation,
        i64::try_from(authority.identity.generation).expect("remote generation")
    );
    assert_eq!(state.dataset_id, Some(authority.identity.dataset_id));
    PostgresQueueStore::new(fixture.pool.clone())
        .enqueue(fixture.threads[0].id, message.to_string())
        .await
        .expect("remote writes resume after return recovery");
}

async fn plan_only_remote_is_preserved_until_exact_recovery(fixture: &ReturnTestFixture<'_>) {
    let prepared = prepare_remote_return(fixture, "plan-only").await;
    let generation = i64::try_from(prepared.intent.from_generation).expect("source generation");
    let staged_source =
        SqliteSource::new(SqliteConfig::new_for_testing(prepared.staged_home.abs()))
            .relocated_from(fixture.home.to_path_buf());
    let cleanup = Migrator::new(staged_source, fixture.pool.clone())
        .abandon_export_after(
            prepared.run_id,
            ActivationTarget {
                dataset_id: prepared.intent.dataset_id,
                generation,
            },
            false,
            || {
                codex_storage_authority::abandon_cutover(fixture.home, &prepared.intent)
                    .map_err(|error| MigrationError::Staging(error.to_string()))?;
                Err(MigrationError::Staging(
                    "injected install-plan cleanup failure".to_string(),
                ))
            },
        )
        .await;
    assert!(matches!(cleanup, Err(MigrationError::Staging(_))));

    assert_eq!(read_cutover(fixture.home).expect("intent"), None);
    assert!(prepared.staged_home.exists());
    assert!(!prepared.plan.backup_dir.exists());
    assert!(matches!(
        prepared.returning.recover().await,
        Err(CutoverError::Conflict)
    ));
    assert!(matches!(
        prepared.returning.abort().await,
        Err(CutoverError::Conflict)
    ));
    assert_eq!(
        std::fs::read(fixture.home.join("storage-install.json")).expect("plan bytes retained"),
        prepared.plan_bytes
    );
    assert_eq!(
        install::read_plan(fixture.home).expect("plan"),
        Some(prepared.plan.clone())
    );
    assert!(prepared.staged_home.exists());
    assert!(!prepared.plan.backup_dir.exists());
    assert_remote_open(fixture, prepared.run_id, "write after plan-only refusal").await;
}

async fn intent_only_remote_recovery(fixture: &ReturnTestFixture<'_>) {
    let prepared = prepare_remote_return(fixture, "intent-only").await;
    install::discard_plan(fixture.home).expect("simulate a missing plan before local installation");
    assert!(read_cutover(fixture.home).expect("intent").is_some());
    assert_eq!(install::read_plan(fixture.home).expect("plan"), None);

    assert!(matches!(
        prepared.returning.recover().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(read_cutover(fixture.home).expect("intent"), None);
    assert_eq!(install::read_plan(fixture.home).expect("plan"), None);
    assert_remote_open(fixture, prepared.run_id, "write after intent-only recovery").await;
}

async fn retired_intent_without_plan_is_refused(fixture: &ReturnTestFixture<'_>) {
    let prepared = prepare_remote_return(fixture, "retired-intent-only").await;
    prepared
        .returning
        .publish(&prepared.intent)
        .await
        .expect("retire exact export");
    install::discard_plan(fixture.home).expect("simulate missing install plan");

    assert!(matches!(
        prepared.returning.recover().await,
        Err(CutoverError::Conflict)
    ));
    assert_eq!(
        std::fs::read(fixture.home.join("storage-cutover.json")).expect("intent retained"),
        prepared.intent_bytes
    );
    assert_eq!(install::read_plan(fixture.home).expect("plan"), None);
    assert_eq!(
        codex_storage_authority::authority_state(fixture.home).expect("cutover state"),
        codex_storage_authority::AuthorityState::CutoverInProgress(prepared.intent.clone())
    );
    assert!(dataset_state(fixture.pool, fixture.source).await.retired);
    assert!(prepared.staged_home.exists());

    std::fs::write(
        fixture.home.join("storage-install.json"),
        &prepared.plan_bytes,
    )
    .expect("restore exact install plan");
    assert!(matches!(
        prepared.returning.recover().await,
        Ok(RecoveryOutcome::RolledForward { generation: 3 })
    ));
    assert_eq!(read_cutover(fixture.home).expect("intent"), None);
    assert_eq!(install::read_plan(fixture.home).expect("plan"), None);
    assert_eq!(backend(fixture.home), ActiveBackend::Local);
}

async fn second_lock_foreign_owner_refusal(fixture: &ReturnTestFixture<'_>) {
    let prepared = prepare_remote_return(fixture, "second-lock-owner").await;
    let foreign_run_id = Uuid::new_v4();
    let objects = install_foreign_owner_fault(
        fixture.owner_pool,
        fixture.schema,
        fixture.owner_role,
        prepared.run_id,
        foreign_run_id,
    )
    .await;
    let recovery = prepared.returning.recover().await;
    remove_foreign_owner_fault(fixture.owner_pool, &objects).await;
    eprintln!(
        "fault_kind=second-lock-foreign-owner original_run={} substituted_run={} recovery_result={recovery:?}",
        prepared.run_id, foreign_run_id
    );

    assert!(matches!(
        recovery,
        Err(CutoverError::Destination(MigrationError::TargetBusy))
    ));
    assert_eq!(
        std::fs::read(fixture.home.join("storage-cutover.json")).expect("intent remains"),
        prepared.intent_bytes
    );
    assert_eq!(
        std::fs::read(fixture.home.join("storage-install.json")).expect("plan remains"),
        prepared.plan_bytes
    );
    let substituted = dataset_state(fixture.pool, fixture.source).await;
    assert!(!substituted.migrating && !substituted.retired);
    assert_eq!(substituted.run_id, Some(foreign_run_id));

    let quoted_schema = quote_test_identifier(fixture.schema);
    owner_ddl(
        fixture.owner_pool,
        fixture.schema,
        fixture.owner_role,
        &[
            format!(
                "UPDATE {quoted_schema}.\"storage_migration_runs\" SET state = 'verified' WHERE run_id = '{}'::uuid",
                prepared.run_id
            ),
            format!(
                "UPDATE {quoted_schema}.\"storage_activation\" SET state = 'migrating', run_id = '{}'::uuid WHERE singleton",
                prepared.run_id
            ),
        ],
    )
    .await;
    assert!(matches!(
        prepared.returning.recover().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(read_cutover(fixture.home).expect("intent"), None);
    assert_eq!(install::read_plan(fixture.home).expect("plan"), None);
    assert_remote_open(
        fixture,
        prepared.run_id,
        "write after ownership revalidation",
    )
    .await;
}

pub(super) async fn return_phase(
    pool: &Arc<PostgresPool>,
    owner_pool: &Arc<PostgresPool>,
    schema: &str,
    owner_role: &str,
    source: &SqliteSource,
    home: &Path,
    threads: &[ThreadMetadata],
) {
    let fixture = ReturnTestFixture {
        pool,
        owner_pool,
        schema,
        owner_role,
        source,
        home,
        threads,
    };
    // Writes made while PostgreSQL was authoritative must come back with the return.
    remote_authority(pool, source, home).await;
    let remote_only = metadata(700, Utc::now(), SessionSource::Cli);
    PostgresThreadCatalog::new(pool.clone())
        .upsert_thread(&remote_only)
        .await
        .expect("remote-only thread");
    let queued = PostgresQueueStore::new(pool.clone())
        .enqueue(threads[0].id, "{\"text\":\"remote only\"}".to_string())
        .await
        .expect("remote-only queued message");
    let state_db_name = SqliteConfig::new_for_testing(home.abs())
        .state_db_path()
        .file_name()
        .expect("state database name")
        .to_string_lossy()
        .into_owned();

    let (returning, run_id, _staged_home) = staged_return(pool, home, "happy", true).await;
    let moved = returning.execute(run_id).await.expect("return");
    assert_eq!(moved.marker.active_backend, ActiveBackend::Local);
    assert_eq!(moved.identity.generation, 3);
    assert!(moved.marker.remote_ever_activated);
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    let state = dataset_state(pool, source).await;
    assert!(state.retired);
    assert_eq!(state.generation, 3);

    // The ordinary runtime opens the installed home and has the remote-only data.
    let runtime = StateRuntime::init(
        SqliteConfig::new_for_testing(home.abs()),
        "migration-provider".to_string(),
    )
    .await
    .expect("installed runtime");
    assert!(
        runtime
            .get_thread(remote_only.id)
            .await
            .expect("remote-only thread")
            .is_some()
    );
    let page = runtime
        .thread_queue()
        .list_page(threads[0].id, 0, 10)
        .await
        .expect("queue page");
    assert!(page.iter().any(|item| item.id == queued.id));
    runtime.close().await;

    // What the return replaced is kept, with a manifest that proves it.
    let backup = home
        .join("storage-backups")
        .join(format!("{run_id}-before-return"));
    let plan: InstallPlan =
        serde_json::from_slice(&std::fs::read(backup.join("manifest.json")).expect("manifest"))
            .expect("manifest json");
    assert!(verify_backup(&plan).expect("backup readback"));
    assert!(backup.join(&state_db_name).exists());

    // A retired dataset refuses every writer, and a return cannot run twice.
    let refused = PostgresQueueStore::new(pool.clone())
        .enqueue(threads[0].id, "{}".to_string())
        .await
        .expect_err("retired dataset");
    assert!(
        refused.to_string().contains("no longer accepts writes"),
        "{refused}"
    );
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::Idle)
    ));

    // Crash after the intent: nothing was published, so recovery undoes it and reopens the
    // dataset exactly as it was.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "after-intent", true).await;
    returning.prepare(run_id).expect("prepare");
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(backend(home), ActiveBackend::Remote);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    let reopened = dataset_state(pool, source).await;
    assert!(!reopened.migrating && !reopened.retired);
    PostgresQueueStore::new(pool.clone())
        .enqueue(threads[0].id, "{}".to_string())
        .await
        .expect("remote writes resume after the rollback");

    // Lost acknowledgement: the dataset was retired but nothing local moved. Recovery installs
    // the staged files and flips the records.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "after-retire", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    returning.publish(&intent).await.expect("retire");
    assert!(matches!(
        load_authority(home),
        Err(codex_storage_authority::AuthorityError::Blocked(
            "cutover in progress"
        ))
    ));
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledForward { generation: 3 })
    ));
    assert_eq!(backend(home), ActiveBackend::Local);

    // Crash in the middle of the swap: the old database is already in the backup and the new one
    // is not in place yet.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "mid-swap", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    returning.publish(&intent).await.expect("retire");
    let plan = install::read_plan(home)
        .expect("plan")
        .expect("plan exists");
    let state_unit = plan
        .units
        .iter()
        .find(|unit| unit.name.ends_with(".sqlite") && !unit.backup_only)
        .expect("a database unit");
    std::fs::create_dir_all(&plan.backup_dir).expect("backup directory");
    std::fs::rename(
        home.join(&state_unit.name),
        plan.backup_dir.join(&state_unit.name),
    )
    .expect("half of the swap");
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledForward { generation: 3 })
    ));
    assert!(home.join(&state_unit.name).exists());
    assert!(plan.backup_dir.join(&state_unit.name).exists());

    // Another client moved the dataset to a generation this intent does not own.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "conflict", true).await;
    let intent = returning.prepare(run_id).expect("prepare");
    {
        let mut connection = pool.acquire().await.expect("connection");
        sqlx::query("UPDATE storage_activation SET state = 'open', generation = 9")
            .execute(&mut *connection)
            .await
            .expect("foreign generation");
    }
    assert!(matches!(
        returning.recover().await,
        Err(CutoverError::Conflict)
    ));
    assert_eq!(read_cutover(home).expect("intent"), Some(intent));

    // Cancelling before the dataset is retired keeps it authoritative and reopens it.
    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "cancelled", true).await;
    returning.prepare(run_id).expect("prepare");
    assert!(matches!(
        returning.abort().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(backend(home), ActiveBackend::Remote);
    assert!(!dataset_state(pool, source).await.migrating);

    remote_authority(pool, source, home).await;
    let (returning, run_id, _) = staged_return(pool, home, "unverified", false).await;
    let refused = returning.execute(run_id).await;
    assert!(
        matches!(
            refused,
            Err(CutoverError::Destination(MigrationError::NotVerified))
        ),
        "{refused:?}"
    );
    let refused_intent = read_cutover(home)
        .expect("intent")
        .expect("failed activation keeps its intent");
    assert!(install::read_plan(home).expect("plan").is_some());
    assert_eq!(
        codex_storage_authority::authority_state(home).expect("cutover state"),
        codex_storage_authority::AuthorityState::CutoverInProgress(refused_intent)
    );
    assert!(matches!(
        returning.recover().await,
        Ok(RecoveryOutcome::RolledBack)
    ));
    assert_eq!(read_cutover(home).expect("intent"), None);
    assert_eq!(install::read_plan(home).expect("plan"), None);
    assert_eq!(backend(home), ActiveBackend::Remote);
    completed_finish_is_repeatable(pool, source, home).await;

    plan_only_remote_is_preserved_until_exact_recovery(&fixture).await;
    intent_only_remote_recovery(&fixture).await;
    retired_intent_without_plan_is_refused(&fixture).await;
    second_lock_foreign_owner_refusal(&fixture).await;
    reset_target(pool).await;
}
