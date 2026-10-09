//! Genuine WAL controls through the codex-state SQLite shim.
use super::*;
use crate::SqliteTarget;
use codex_state::SqliteConfig;
use codex_utils_absolute_path::test_support::PathExt;
use sqlx::SqlitePool;
use std::time::Duration;
const LIVE: &[u8] = b"original live database";
const NAME: &str = "qa_staged.sqlite";
async fn bounded<T>(what: &str, future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .unwrap_or_else(|_| panic!("{what} exceeded its bound"))
}
async fn open(config: &SqliteConfig, path: &Path) -> SqlitePool {
    bounded("open", config.open_read_write_pool(path))
        .await
        .expect("open database through the shim")
}
async fn put(pool: &SqlitePool, value: &str) {
    let create = sqlx::query("CREATE TABLE IF NOT EXISTS qa_marker (value TEXT NOT NULL)");
    create.execute(pool).await.expect("create marker table");
    let insert = sqlx::query("INSERT INTO qa_marker (value) VALUES (?)").bind(value);
    insert.execute(pool).await.expect("commit marker");
}
async fn markers(pool: &SqlitePool) -> Vec<String> {
    let query = sqlx::query_scalar("SELECT value FROM qa_marker ORDER BY rowid");
    query.fetch_all(pool).await.expect("read markers")
}
async fn close(pool: SqlitePool) {
    bounded("pool close", pool.close()).await;
}
/// (busy, log frames, checkpointed frames); the wait on readers is bounded to 100 ms.
async fn truncate_checkpoint(pool: &SqlitePool) -> (i64, i64, i64) {
    let mut connection = bounded("checkpoint connection", pool.acquire())
        .await
        .expect("checkpoint connection");
    let wait = sqlx::query("PRAGMA busy_timeout = 100");
    wait.execute(&mut *connection).await.expect("bounded wait");
    let query = sqlx::query_as::<_, (i64, i64, i64)>("PRAGMA wal_checkpoint(TRUNCATE)");
    let result = bounded("checkpoint", query.fetch_one(&mut *connection)).await;
    result.expect("checkpoint result")
}
fn names(dir: &Path) -> Vec<String> {
    let mut names = std::fs::read_dir(dir)
        .expect("list directory")
        .map(|entry| entry.expect("entry").file_name())
        .map(|name| name.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}
fn sidecars(dir: &Path) -> Vec<String> {
    names(dir)
        .into_iter()
        .filter(|name| {
            let name = name.to_ascii_lowercase();
            name.ends_with(".sqlite-wal") || name.ends_with(".sqlite-shm")
        })
        .collect()
}
// SHM length only: Windows holds byte-range locks inside it.
fn evidence(dir: &Path) -> (Vec<String>, String, String, u64, u64) {
    let wal = dir.join(format!("{NAME}-wal"));
    let shm = dir.join(format!("{NAME}-shm"));
    (
        names(dir),
        sha256_file(&dir.join(NAME)).expect("hash database"),
        sha256_file(&wal).expect("hash wal"),
        std::fs::metadata(&wal).expect("wal metadata").len(),
        std::fs::metadata(&shm).expect("shm metadata").len(),
    )
}
fn live_home() -> tempfile::TempDir {
    let home = tempfile::tempdir().expect("isolated live home");
    std::fs::write(home.path().join(NAME), LIVE).expect("live fixture");
    home
}
fn refused(stage: &Path, home: &Path) {
    for _ in 0..2 {
        let error = validate_staged_sqlite_sidecars(stage).expect_err("guard must refuse");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let error = plan_install(home, stage, Uuid::new_v4()).expect_err("plan must refuse");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!plan_path(home).exists());
        assert_eq!(std::fs::read(home.join(NAME)).unwrap(), LIVE);
    }
}
async fn installed(home: &Path, name: &str) -> Vec<String> {
    let config = SqliteConfig::new_for_testing(home.abs());
    let pool = open(&config, &home.join(name)).await;
    let found = markers(&pool).await;
    close(pool).await;
    found
}
#[tokio::test]
async fn real_wal_writer_is_refused_with_all_evidence_unchanged() {
    let home = live_home();
    let stage = tempfile::tempdir().expect("isolated stage");
    let config = SqliteConfig::new_for_testing(stage.path().abs());
    let pool = open(&config, &stage.path().join(NAME)).await;
    put(&pool, "committed before refusal").await;
    let wal = stage.path().join(format!("{NAME}-wal"));
    assert!(std::fs::metadata(wal).unwrap().len() > 0);
    assert!(stage.path().join(format!("{NAME}-shm")).exists());
    let before = evidence(stage.path());
    refused(stage.path(), home.path());
    assert_eq!(evidence(stage.path()), before);
    assert_eq!(markers(&pool).await, ["committed before refusal"]);
    close(pool).await;
}
#[tokio::test]
async fn checkpoint_alone_keeps_sidecars_and_full_close_permits_install() {
    let home = live_home();
    let stage = tempfile::tempdir().expect("isolated stage");
    let config = SqliteConfig::new_for_testing(stage.path().abs());
    let pool = open(&config, &stage.path().join(NAME)).await;
    put(&pool, "first").await;
    put(&pool, "second").await;
    let (busy, _, _) = truncate_checkpoint(&pool).await;
    assert_eq!(busy, 0);
    assert_eq!(sidecars(stage.path()).len(), 2);
    refused(stage.path(), home.path());
    close(pool).await;
    assert_eq!(sidecars(stage.path()), Vec::<String>::new());
    validate_staged_sqlite_sidecars(stage.path()).expect("closed stage is accepted");
    let plan = plan_install(home.path(), stage.path(), Uuid::new_v4()).expect("plan");
    install(home.path(), &plan).expect("install");
    install(home.path(), &plan).expect("repeated install is safe");
    assert!(verify_backup(&plan).unwrap());
    assert_eq!(std::fs::read(plan.backup_dir.join(NAME)).unwrap(), LIVE);
    assert!(!stage.path().join(NAME).exists());
    assert_eq!(installed(home.path(), NAME).await, ["first", "second"]);
}
#[tokio::test]
async fn late_real_wal_refuses_install_before_backup_and_needs_a_fresh_plan() {
    let home = live_home();
    let stage = tempfile::tempdir().expect("isolated stage");
    let config = SqliteConfig::new_for_testing(stage.path().abs());
    let path = stage.path().join(NAME);
    let pool = open(&config, &path).await;
    put(&pool, "planned").await;
    assert_eq!(truncate_checkpoint(&pool).await.0, 0);
    close(pool).await;
    let plan = plan_install(home.path(), stage.path(), Uuid::new_v4()).expect("plan");
    let pool = open(&config, &path).await;
    put(&pool, "late").await;
    let error = install(home.path(), &plan).expect_err("late WAL must refuse install");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(!plan.backup_dir.exists());
    assert_eq!(read_plan(home.path()).unwrap(), Some(plan.clone()));
    assert_eq!(std::fs::read(home.path().join(NAME)).unwrap(), LIVE);
    assert_eq!(truncate_checkpoint(&pool).await.0, 0);
    close(pool).await;
    discard_plan(home.path()).expect("discard unused plan");
    let plan = plan_install(home.path(), stage.path(), Uuid::new_v4()).expect("fresh plan");
    install(home.path(), &plan).expect("install");
    assert_eq!(installed(home.path(), NAME).await, ["planned", "late"]);
}
#[tokio::test]
async fn long_lived_reader_holds_the_checkpoint_until_it_fully_releases() {
    let home = live_home();
    let stage = tempfile::tempdir().expect("isolated stage");
    let config = SqliteConfig::new_for_testing(stage.path().abs());
    let path = stage.path().join(NAME);
    let writer = open(&config, &path).await;
    let reader_pool = open(&config, &path).await;
    put(&writer, "before reader").await;
    let mut reader = bounded("reader connection", reader_pool.acquire())
        .await
        .expect("reader connection");
    let begin = sqlx::query("BEGIN");
    begin.execute(&mut *reader).await.expect("begin read");
    let count = "SELECT COUNT(*) FROM qa_marker";
    let query = sqlx::query_scalar::<_, i64>(count);
    let seen = query.fetch_one(&mut *reader).await.expect("snapshot");
    assert_eq!(seen, 1);
    put(&writer, "after reader").await;
    let (busy, log, checkpointed) = truncate_checkpoint(&writer).await;
    assert_eq!(busy, 1, "log={log} checkpointed={checkpointed}");
    assert!(log > checkpointed && checkpointed >= 0);
    let wal = stage.path().join(format!("{NAME}-wal"));
    assert!(std::fs::metadata(wal).unwrap().len() > 0);
    let before = evidence(stage.path());
    refused(stage.path(), home.path());
    assert_eq!(evidence(stage.path()), before);
    let query = sqlx::query_scalar::<_, i64>(count);
    let still = query.fetch_one(&mut *reader).await.expect("snapshot read");
    assert_eq!(still, 1);
    let rollback = sqlx::query("ROLLBACK");
    rollback.execute(&mut *reader).await.expect("end read");
    drop(reader);
    close(reader_pool).await;
    assert_eq!(truncate_checkpoint(&writer).await.0, 0);
    close(writer).await;
    assert_eq!(sidecars(stage.path()), Vec::<String>::new());
    let plan = plan_install(home.path(), stage.path(), Uuid::new_v4()).expect("plan");
    install(home.path(), &plan).expect("install");
    let found = installed(home.path(), NAME).await;
    assert_eq!(found, ["before reader", "after reader"]);
}
#[tokio::test]
async fn production_sqlite_target_close_leaves_a_stage_that_installs() {
    let home = tempfile::tempdir().expect("isolated live home");
    let stage = tempfile::tempdir().expect("isolated stage");
    let config = SqliteConfig::new_for_testing(stage.path().abs());
    let create = SqliteTarget::create(config.clone(), home.path().to_path_buf(), "provider");
    let target = bounded("target create", create).await.expect("target");
    let board = stage.path().join("agent_message_board_1.sqlite");
    let pools = [
        (config.state_db_path(), &target.state),
        (config.goals_db_path(), &target.goals),
        (config.queue_db_path(), &target.queue),
        (config.logs_db_path(), &target.logs),
        (config.memories_db_path(), &target.memories),
        (board, &target.board),
    ];
    for (path, pool) in &pools {
        put(pool, &path.file_name().unwrap().to_string_lossy()).await;
        assert!(path.with_extension("sqlite-wal").is_file());
        assert!(path.with_extension("sqlite-shm").is_file());
    }
    bounded("target close", target.close()).await;
    assert!(pools.iter().all(|(_, pool)| pool.is_closed()));
    assert_eq!(sidecars(stage.path()), Vec::<String>::new());
    let plan = plan_install(home.path(), stage.path(), Uuid::new_v4()).expect("plan");
    install(home.path(), &plan).expect("install");
    for (path, _) in &pools {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let moved = plan.units.iter().any(|u| u.name == name && !u.backup_only);
        assert!(moved);
        assert_eq!(installed(home.path(), &name).await, [name.as_str()]);
    }
}
fn nonregular(entry: &Path, kind: &str, _sibling: &Path) {
    match kind {
        #[cfg(unix)]
        "symlink" => std::os::unix::fs::symlink(_sibling, entry).expect("symlink"),
        _ => std::fs::create_dir(entry).expect("directory"),
    }
}
fn kinds() -> Vec<&'static str> {
    if cfg!(unix) {
        vec!["directory", "symlink"]
    } else {
        vec!["directory"]
    }
}
#[test]
fn nonregular_staged_database_is_refused_before_a_plan_exists() {
    for kind in kinds() {
        let home = live_home();
        let stage = tempfile::tempdir().expect("isolated stage");
        let sibling = stage.path().join("sibling.bin");
        std::fs::write(&sibling, b"retained sibling").unwrap();
        let entry = stage.path().join(NAME);
        nonregular(&entry, kind, &sibling);
        refused(stage.path(), home.path());
        let kind = std::fs::symlink_metadata(&entry).unwrap().file_type();
        assert!(!kind.is_file());
        assert_eq!(std::fs::read(&sibling).unwrap(), b"retained sibling");
    }
}
#[test]
fn staged_database_that_turns_nonregular_after_planning_is_refused_before_backup() {
    for kind in kinds() {
        let home = live_home();
        let stage = tempfile::tempdir().expect("isolated stage");
        let entry = stage.path().join(NAME);
        std::fs::write(&entry, b"verified staged database").unwrap();
        let plan = plan_install(home.path(), stage.path(), Uuid::new_v4()).expect("plan");
        let moved = stage.path().join("moved.bin");
        std::fs::rename(&entry, &moved).expect("move the verified file aside");
        nonregular(&entry, kind, &moved);
        let error = install(home.path(), &plan).expect_err("nonregular entry must refuse");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!plan.backup_dir.exists());
        assert_eq!(read_plan(home.path()).unwrap(), Some(plan.clone()));
        assert_eq!(std::fs::read(home.path().join(NAME)).unwrap(), LIVE);
        assert_eq!(std::fs::read(&moved).unwrap(), b"verified staged database");
    }
}
