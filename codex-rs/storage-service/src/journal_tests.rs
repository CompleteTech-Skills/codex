use super::*;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Mutex;

fn record(state: OperationState, copied: u64) -> OperationRecord {
    OperationRecord {
        operation_id: Uuid::from_u128(7),
        action: PlanAction::Migrate,
        plan_digest: "digest".to_string(),
        state,
        run_id: None,
        return_source: None,
        created_at_ms: 1,
        updated_at_ms: 2,
        blocker: None,
        copied: vec![("threads".to_string(), copied)],
    }
}

#[test]
fn the_observer_sees_each_durable_change_in_order() {
    let home = tempfile::tempdir().expect("home");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let journal = Journal::new(home.path()).observed_by(Arc::new(move |record| {
        sink.lock().expect("sink").push(record.clone());
    }));

    let planned = record(OperationState::Planned, 0);
    let copying = record(OperationState::Copying, 3);
    journal.create(&planned).expect("create");
    journal.update(&copying).expect("update");

    assert_eq!(*seen.lock().expect("seen"), vec![planned, copying.clone()]);
    assert_eq!(
        journal.read(copying.operation_id).expect("read"),
        Some(copying)
    );
}

#[test]
fn a_rejected_write_is_not_reported() {
    let home = tempfile::tempdir().expect("home");
    let seen = Arc::new(Mutex::new(0usize));
    let sink = Arc::clone(&seen);
    let journal = Journal::new(home.path()).observed_by(Arc::new(move |_| {
        *sink.lock().expect("sink") += 1;
    }));

    journal
        .create(&record(OperationState::Planned, 0))
        .expect("create");
    journal
        .create(&record(OperationState::Planned, 0))
        .expect_err("the id is already taken");

    assert_eq!(*seen.lock().expect("seen"), 1);
}

#[test]
fn return_identity_survives_restart_and_export_cancel_exclude_each_other() {
    let home = tempfile::tempdir().expect("home");
    let journal = Journal::new(home.path());
    let mut planned = record(OperationState::Planned, 0);
    planned.action = PlanAction::Return;
    planned.run_id = Some(Uuid::new_v4());
    planned.return_source = Some(ReturnSource {
        dataset_id: Uuid::new_v4(),
        generation: 2,
        destination: "host:5432/database/namespace".into(),
    });
    journal
        .create(&planned)
        .expect("persist before any remote fence");
    let restarted = Journal::new(home.path());
    assert_eq!(
        restarted.read(planned.operation_id).expect("restart"),
        Some(planned.clone())
    );
    let lock = journal.path(planned.operation_id).with_extension("lock");
    std::fs::write(&lock, b"persistent owner bytes").expect("existing lock");
    let export = journal
        .claim_return(planned.operation_id)
        .expect("export owner");
    assert_eq!(
        restarted
            .claim_return(planned.operation_id)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(export);
    assert_eq!(std::fs::read(&lock).unwrap(), b"persistent owner bytes");
    let cancel = restarted
        .claim_return(planned.operation_id)
        .expect("cancel after exporter exits");
    drop(cancel);
    assert_eq!(std::fs::read(&lock).unwrap(), b"persistent owner bytes");
    let other = Uuid::new_v4();
    std::fs::create_dir(journal.path(other).with_extension("lock")).unwrap();
    assert_ne!(
        journal.claim_return(other).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    // A failed ownership journal creation cannot announce a usable export receipt.
    assert!(journal.create(&planned).is_err());
    assert_eq!(
        restarted.read(planned.operation_id).expect("unchanged"),
        Some(planned)
    );
}
