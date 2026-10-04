//! Actual same-native-session baseline controls; no roles/grants are changed.
use super::*;
use crate::runtime_origin::OriginalRuntimeOrigin;
use crate::runtime_origin::RuntimeOriginRefusal;
use codex_keyring_store::DefaultKeyringStore;

#[derive(Clone, Copy)]
enum Mode {
    Idle,
    Begin,
    Aborted,
    BudgetExact,
    BudgetOverflow,
    Relation(catalog::NativeRelationProbe),
}
struct Failure {
    runtime: Option<Arc<OriginalRuntimeOrigin>>,
    runtime_result: Option<Result<Arc<crate::RemoteStorage>, crate::RemoteStorageError>>,
    runtime_cleanup: Option<RuntimeOriginRefusal>,
    observer: Option<Arc<RemoteAdministrativeObserver>>,
    input: Option<crate::administration_startup::tests::Failure>,
    session: Option<super::super::AdministrativeObservationRefusal>,
    observer_cleanup: Option<AdministrativeRegistryRefusal>,
    setup: [Option<Result<sqlx::postgres::PgQueryResult, Arc<sqlx::Error>>>; 2],
    command: Option<Arc<OriginalAdministrativeCommand>>,
    dispatch: Option<AdministrativeCommandRefusal>,
    query: Option<AdministrativeCommandRefusal>,
    cleanup: Option<AdministrativeCommandRefusal>,
    archive: Option<AdministrativeCommandArchive>,
    payload_decode: [Option<Arc<sqlx::Error>>; 5],
}
impl std::fmt::Debug for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _retained = (
            &self.runtime,
            &self.runtime_result,
            &self.runtime_cleanup,
            &self.observer,
            &self.input,
            &self.session,
            &self.observer_cleanup,
            &self.setup,
            &self.command,
            &self.dispatch,
            &self.query,
            &self.cleanup,
            &self.archive,
            &self.payload_decode,
        );
        f.write_str("AdministrativeBaselineControlFailure([originals retained])")
    }
}
fn remember_decode<T>(
    result: Result<T, sqlx::Error>,
    failure: &mut Failure,
    index: usize,
) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(raw) => {
            failure.payload_decode[index] = Some(Arc::new(raw));
            None
        }
    }
}
async fn control(named: bool, mode: Mode) -> Result<(), Failure> {
    let mut failure = Failure {
        runtime: None,
        runtime_result: None,
        runtime_cleanup: None,
        observer: None,
        input: None,
        session: None,
        observer_cleanup: None,
        setup: std::array::from_fn(|_| None),
        command: None,
        dispatch: None,
        query: None,
        cleanup: None,
        archive: None,
        payload_decode: std::array::from_fn(|_| None),
    };
    let input = match crate::administration_startup::tests::input(named) {
        Ok(input) => input,
        Err(raw) => {
            failure.input = Some(raw);
            return Err(failure);
        }
    };
    let runtime = OriginalRuntimeOrigin::prepare(Arc::clone(input.target()));
    failure.runtime = Some(Arc::clone(&runtime));
    let constructor = crate::RemoteStorage::connect_with_original_runtime_origin(
        &runtime,
        Arc::new(DefaultKeyringStore),
        Arc::new(|_| Ok(())),
    )
    .await;
    let origin = runtime.original_native_origin();
    // The same original constructor/query/native/SDK close is physically joined
    // before returning any fixture verdict. No timeout is terminal evidence.
    let runtime_cleanup = runtime.close_original().await;
    let runtime_good = constructor.is_ok()
        && runtime_cleanup.is_ok()
        && runtime.known_original_closed()
        && origin.is_some();
    failure.runtime_result = Some(constructor);
    failure.runtime_cleanup = runtime_cleanup.err();
    if !runtime_good {
        return Err(failure);
    }
    let Some(origin) = origin else {
        return Err(failure);
    };
    let observer = RemoteAdministrativeObserver::new(input, Arc::new(DefaultKeyringStore));
    failure.observer = Some(Arc::clone(&observer));
    let mut frame = match observer.retain_runtime_session(origin).await {
        Ok(frame) => frame,
        Err(raw) => {
            failure.session = Some(raw);
            // Registry retains pending originals; never invent a terminal ACK.
            failure.observer_cleanup = observer.close_original().await.err();
            return Err(failure);
        }
    };
    // Genuine raw SQL on the actual scoped session, not a helper task/session.
    // After setup begins, all verdicts are postponed until original command close.
    if let Some(connection) = frame.session.as_mut() {
        if matches!(mode, Mode::Begin | Mode::Aborted) {
            failure.setup[0] = Some(
                sqlx::query("BEGIN")
                    .execute(&mut **connection)
                    .await
                    .map_err(Arc::new),
            );
        }
        if matches!(mode, Mode::Aborted) {
            failure.setup[1] = Some(
                sqlx::query("SELECT 1 / 0")
                    .execute(&mut **connection)
                    .await
                    .map_err(Arc::new),
            );
        }
    }
    let original = OriginalAdministrativeCommand::prepare(frame);
    {
        let mut state = original.state.lock().unwrap_or_else(|raw| raw.into_inner());
        state.catalog_probe = match mode {
            Mode::BudgetExact => Some(catalog::NativePayloadProbe::Exact),
            Mode::BudgetOverflow => Some(catalog::NativePayloadProbe::Overflow),
            _ => None,
        };
        state.relation_probe = match mode {
            Mode::Relation(probe) => Some(probe),
            _ => None,
        };
    }
    failure.command = Some(Arc::clone(&original));
    let dispatched = original.start_preflight();
    failure.dispatch = dispatched.err();
    let queried = original.finish_original().await;
    let query_good = queried.is_ok();
    failure.query = queried.err();
    let baseline_good = {
        let state = original.state.lock().unwrap_or_else(|raw| raw.into_inner());
        let no_unowned_transaction = state.raw_stamp.is_none()
            && state.xid.is_none()
            && state.raw_rows.is_none()
            && state.observed_status.is_none()
            && !state.rollback_attempted
            && !state.commit_attempted;
        let code = state
            .native_baseline
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .and_then(|raw| raw.as_database_error())
            .and_then(|raw| raw.code());
        let raw_expected = match mode {
            Mode::Idle => {
                code.as_deref() == Some("25P01")
                    && query_good
                    && state.observed_status.as_ref().and_then(|x| x.as_deref()) == Some("aborted")
            }
            Mode::Begin => {
                state.native_baseline.as_ref().is_some_and(Result::is_ok)
                    && !query_good
                    && no_unowned_transaction
            }
            Mode::Aborted => {
                code.as_deref() == Some("25P02") && !query_good && no_unowned_transaction
            }
            Mode::BudgetExact | Mode::BudgetOverflow => {
                let first = state.raw_catalog[0].as_ref().and_then(|rows| rows.first());
                let actual_values = first.is_some_and(|row| {
                    remember_decode(row.try_get::<Option<String>, _>("absent"), &mut failure, 0)
                        .is_some_and(|value| value.is_none())
                        && remember_decode(row.try_get::<String, _>("text_value"), &mut failure, 1)
                            .is_some_and(|value| {
                                value.len() == 4194304 && value.bytes().all(|byte| byte == b'x')
                            })
                        && remember_decode(
                            row.try_get::<Vec<u8>, _>("binary_value"),
                            &mut failure,
                            2,
                        )
                        .is_some_and(|value| value == [0, 1, 2])
                });
                let second = state.raw_catalog[1]
                    .as_ref()
                    .and_then(|rows| rows.first())
                    .is_some_and(|row| {
                        remember_decode(row.try_get::<String, _>("text_value"), &mut failure, 3)
                            .is_some_and(|value| {
                                value.len() == 4194301 && value.bytes().all(|byte| byte == b'y')
                            })
                    });
                let expected_cause = match mode {
                    Mode::BudgetExact => {
                        query_good
                            && state.causes.iter().all(Option::is_none)
                            && state.raw_catalog[2].is_none()
                    }
                    Mode::BudgetOverflow => {
                        !query_good
                            && state.causes.iter().flatten().count() == 1
                            && state.causes.iter().flatten().any(|cause| {
                                matches!(
                                    cause.as_ref(),
                                    CommandCause::Catalog(catalog::PayloadRefusal::Budget {
                                        retained_before: catalog::PAYLOAD_BUDGET,
                                        next_value_bytes: 1
                                    })
                                )
                            })
                            && state.raw_catalog[2]
                                .as_ref()
                                .and_then(|rows| rows.first())
                                .is_some_and(|row| {
                                    remember_decode(
                                        row.try_get::<String, _>("text_value"),
                                        &mut failure,
                                        4,
                                    )
                                    .is_some_and(|value| value == "z")
                                })
                    }
                    _ => false,
                };
                code.as_deref() == Some("25P01")
                    && actual_values
                    && second
                    && expected_cause
                    && state.catalog_payload_bytes == catalog::PAYLOAD_BUDGET
                    && state.catalog_relations.is_none()
                    && state.rollback_attempted
                    && !state.commit_attempted
                    && state.xid.is_some()
                    && state
                        .observed_status
                        .as_ref()
                        .and_then(|status| status.as_deref())
                        == Some("aborted")
            }
            Mode::Relation(probe) => {
                let expected = state.causes.iter().flatten().count() == 1
                    && state
                        .causes
                        .iter()
                        .flatten()
                        .any(|cause| match (probe, cause.as_ref()) {
                            (
                                catalog::NativeRelationProbe::Unknown,
                                CommandCause::Relations(catalog::RelationRefusal::NonCanonical {
                                    relation,
                                }),
                            ) => relation == "codex_relation_control_unknown",
                            (
                                catalog::NativeRelationProbe::WrongKind,
                                CommandCause::Relations(catalog::RelationRefusal::NonCanonical {
                                    relation,
                                }),
                            ) => relation == "idx_threads_recency_id",
                            (
                                catalog::NativeRelationProbe::MissingIndex,
                                CommandCause::Relations(catalog::RelationRefusal::Missing {
                                    relation,
                                }),
                            ) => *relation == "idx_threads_recency_id",
                            (
                                catalog::NativeRelationProbe::Partial20
                                | catalog::NativeRelationProbe::Partial21,
                                CommandCause::Relations(catalog::RelationRefusal::PartialExtension),
                            ) => true,
                            _ => false,
                        });
                let before = state.relation_baseline.as_ref().and_then(|rows| {
                    remember_decode(
                        rows.iter()
                            .map(|row| row.try_get::<i64, _>("relation_oid"))
                            .collect::<Result<Vec<_>, _>>(),
                        &mut failure,
                        3,
                    )
                });
                let restored = state.relation_restored.as_ref().and_then(|rows| {
                    remember_decode(
                        rows.iter()
                            .map(|row| row.try_get::<i64, _>("relation_oid"))
                            .collect::<Result<Vec<_>, _>>(),
                        &mut failure,
                        4,
                    )
                });
                let actual_fixture = state.relation_format.as_ref().is_some_and(|row| {
                    remember_decode(row.try_get::<i32, _>("format_version"), &mut failure, 2)
                        .is_some_and(|format| format == 19)
                });
                let ddl_count = if matches!(probe, catalog::NativeRelationProbe::WrongKind) {
                    2
                } else {
                    1
                };
                code.as_deref() == Some("25P01")
                    && !query_good
                    && expected
                    && actual_fixture
                    && state.relation_ddl.iter().flatten().count() == ddl_count
                    && state.relation_ddl.iter().flatten().all(Result::is_ok)
                    && before.as_ref().is_some_and(|oids| oids.len() == 89)
                    && before == restored
                    && restored.is_some()
                    && state.raw_catalog[0]
                        .as_ref()
                        .is_some_and(|rows| !rows.is_empty())
                    && state.raw_catalog[1..].iter().all(Option::is_none)
                    && state.catalog_relations.is_none()
                    && state.rollback_attempted
                    && !state.commit_attempted
                    && state.xid.is_some()
                    && state
                        .observed_status
                        .as_ref()
                        .and_then(|status| status.as_deref())
                        == Some("aborted")
            }
        };
        raw_expected
            && state.terminal
            && state.task.is_none()
            && state.joined_failure.is_none()
            && !original.state.is_poisoned()
    };
    let setup_good = match mode {
        Mode::Idle | Mode::BudgetExact | Mode::BudgetOverflow | Mode::Relation(_) => {
            failure.setup.iter().all(Option::is_none)
        }
        Mode::Begin => failure.setup[0].as_ref().is_some_and(Result::is_ok),
        Mode::Aborted => {
            failure.setup[0].as_ref().is_some_and(Result::is_ok)
                && failure.setup[1]
                    .as_ref()
                    .and_then(|x| x.as_ref().err())
                    .and_then(|raw| raw.as_database_error())
                    .and_then(|raw| raw.code())
                    .as_deref()
                    == Some("22012")
        }
    };
    let closed = original.close_into_archive().await;
    let physically_closed = {
        let state = original.state.lock().unwrap_or_else(|raw| raw.into_inner());
        state.startup.known_physical_closed()
            && state.close_task.is_none()
            && state.close_returned.is_some()
            && state.close_joined_failure.is_none()
    };
    match closed {
        Ok(archive) => failure.archive = Some(archive),
        Err(raw) => failure.cleanup = Some(raw),
    }
    if !baseline_good
        || !setup_good
        || failure.dispatch.is_some()
        || !physically_closed
        || failure.archive.is_none()
    {
        return Err(failure);
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires admitted default admin/runtime same-target origin privileges; real SQL"]
async fn default_native_baseline() -> Result<(), Failure> {
    for mode in [Mode::Idle, Mode::Begin, Mode::Aborted] {
        control(false, mode).await?;
    }
    Ok(())
}
#[tokio::test]
#[ignore = "requires admitted named admin/runtime same-target origin privileges; real SQL"]
async fn named_native_baseline() -> Result<(), Failure> {
    for mode in [Mode::Idle, Mode::Begin, Mode::Aborted] {
        control(true, mode).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires admitted default same-target native origin/admin privileges; actual SQLx rows"]
async fn default_native_catalog_payload_budget() -> Result<(), Failure> {
    control(false, Mode::BudgetExact).await?;
    control(false, Mode::BudgetOverflow).await
}

#[tokio::test]
#[ignore = "requires admitted named same-target native origin/admin privileges; actual SQLx rows"]
async fn named_native_catalog_payload_budget() -> Result<(), Failure> {
    control(true, Mode::BudgetExact).await?;
    control(true, Mode::BudgetOverflow).await
}

#[tokio::test]
#[ignore = "requires exclusively isolated current19 default namespace/admin DDL capability; transactional real SQL"]
async fn default_native_relation_refusals() -> Result<(), Failure> {
    for probe in [
        catalog::NativeRelationProbe::Unknown,
        catalog::NativeRelationProbe::WrongKind,
        catalog::NativeRelationProbe::MissingIndex,
        catalog::NativeRelationProbe::Partial20,
        catalog::NativeRelationProbe::Partial21,
    ] {
        control(false, Mode::Relation(probe)).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires exclusively isolated current19 named namespace/admin DDL capability; transactional real SQL"]
async fn named_native_relation_refusals() -> Result<(), Failure> {
    for probe in [
        catalog::NativeRelationProbe::Unknown,
        catalog::NativeRelationProbe::WrongKind,
        catalog::NativeRelationProbe::MissingIndex,
        catalog::NativeRelationProbe::Partial20,
        catalog::NativeRelationProbe::Partial21,
    ] {
        control(true, Mode::Relation(probe)).await?;
    }
    Ok(())
}
