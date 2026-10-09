use super::*;
use crate::app::test_support::make_test_app;
use crate::chatwidget::tests::helpers::render_bottom_popup;
use crate::chatwidget::tests::make_chatwidget_manual_with_sender;
use codex_app_server_protocol::StorageCopiedDomain;
use codex_app_server_protocol::StorageRemoteSummary;
use codex_app_server_protocol::StorageSourceEstimate;
use crossterm::event::KeyCode;
use pretty_assertions::assert_eq;

fn status(
    backend: StorageBackend,
    authority: StorageAuthority,
    candidate_configured: bool,
    blockers: Vec<StorageBlocker>,
) -> StorageStatus {
    StorageStatus {
        host: "build-host".to_string(),
        active_backend: backend,
        authority,
        local_generation: Some(2),
        dataset_id: Some("0194e0a0-0000-7000-8000-000000000001".to_string()),
        remote_ever_activated: backend == StorageBackend::RemotePostgres,
        candidate_configured,
        remote: None::<StorageRemoteSummary>,
        blockers,
    }
}

fn estimate() -> StorageSourceEstimate {
    StorageSourceEstimate {
        threads: 412,
        sections: 3,
        projects: 5,
        attachments: 0,
        queued_items: 2,
        goals: 7,
        logs: 90_321,
        memory_outputs: 12,
        board_posts: 0,
        rollout_files: 410,
        rollout_bytes: 733_118_201,
    }
}

fn plan(action: StoragePlanAction, blockers: Vec<StorageBlocker>) -> StoragePlan {
    StoragePlan {
        plan_id: "0194e0a0-0000-7000-8000-0000000000aa".to_string(),
        action,
        digest: "digest".to_string(),
        host: "build-host".to_string(),
        destination: "db.internal:5432/codex/codex_storage".to_string(),
        local_generation: Some(1),
        estimate: (action == StoragePlanAction::Migrate).then(estimate),
        connection: StorageConnectionReport {
            stage: StorageCheckStage::Ready,
            blocker: None,
            schema_format: Some(19),
            dataset_state: Some("open".to_string()),
            dataset_id: None,
            generation: Some(0),
            empty: Some(true),
        },
        blockers,
        requires_pause: true,
    }
}

fn attach_plan(blockers: Vec<StorageBlocker>) -> StoragePlan {
    let mut attach = plan(StoragePlanAction::Attach, blockers);
    attach.estimate = None;
    attach.connection.dataset_id = Some("0194e0a0-0000-7000-8000-000000000001".to_string());
    attach.connection.empty = Some(false);
    attach
}

fn operation(state: StorageOperationState, blocker: Option<StorageBlocker>) -> StorageOperation {
    StorageOperation {
        operation_id: "0194e0a0-0000-7000-8000-0000000000bb".to_string(),
        action: StoragePlanAction::Migrate,
        plan_digest: "digest".to_string(),
        state,
        run_id: None,
        created_at: 1_790_000_000,
        updated_at: 1_790_000_100,
        blocker,
        copied: vec![
            StorageCopiedDomain {
                domain: "threads".to_string(),
                rows: 412,
            },
            StorageCopiedDomain {
                domain: "logs".to_string(),
                rows: 40_000,
            },
        ],
    }
}

async fn app_with(
    view: SelectionViewParams,
) -> (App, tokio::sync::mpsc::UnboundedReceiver<AppEvent>) {
    let mut app = make_test_app().await;
    let (chat, _, rx, _) = make_chatwidget_manual_with_sender().await;
    app.chat_widget = chat;
    app.chat_widget.show_selection_view(view);
    (app, rx)
}

#[tokio::test]
async fn status_views_keep_the_active_backend_apart_from_a_saved_profile() {
    for (name, view) in [
        (
            "storage_status_local_without_profile",
            status_view(&status(
                StorageBackend::LocalSqlite,
                StorageAuthority::Unmanaged,
                /*candidate_configured*/ false,
                Vec::new(),
            )),
        ),
        (
            "storage_status_local_with_saved_profile",
            status_view(&status(
                StorageBackend::LocalSqlite,
                StorageAuthority::Local,
                /*candidate_configured*/ true,
                Vec::new(),
            )),
        ),
        (
            "storage_status_remote_active",
            status_view(&status(
                StorageBackend::RemotePostgres,
                StorageAuthority::Remote,
                /*candidate_configured*/ true,
                Vec::new(),
            )),
        ),
        (
            "storage_status_cutover_interrupted",
            status_view(&status(
                StorageBackend::LocalSqlite,
                StorageAuthority::CutoverInProgress,
                /*candidate_configured*/ true,
                vec![StorageBlocker::CutoverInProgress],
            )),
        ),
        (
            "storage_status_records_unreadable",
            status_view(&status(
                StorageBackend::LocalSqlite,
                StorageAuthority::Invalid,
                /*candidate_configured*/ false,
                vec![StorageBlocker::AuthorityInvalid],
            )),
        ),
        (
            "storage_server_without_support",
            unavailable_view(
                "This server does not offer storage management, or refused the request.",
            ),
        ),
    ] {
        let (app, _rx) = app_with(view).await;
        insta::assert_snapshot!(name, render_bottom_popup(&app.chat_widget, /*width*/ 80));
    }
}

#[tokio::test]
async fn only_a_never_managed_host_with_a_profile_is_offered_the_join() {
    let (app, _rx) = app_with(status_view(&status(
        StorageBackend::LocalSqlite,
        StorageAuthority::Unmanaged,
        /*candidate_configured*/ true,
        Vec::new(),
    )))
    .await;
    insta::assert_snapshot!(
        "storage_status_never_managed_with_profile",
        render_bottom_popup(&app.chat_widget, /*width*/ 80)
    );
}

#[tokio::test]
async fn a_selection_never_starts_anything_by_itself() {
    let (mut app, mut rx) = app_with(status_view(&status(
        StorageBackend::LocalSqlite,
        StorageAuthority::Local,
        /*candidate_configured*/ true,
        Vec::new(),
    )))
    .await;
    // The first row tests the connection, the second previews the migration.
    app.chat_widget.handle_key_event(KeyCode::Enter.into());
    assert!(matches!(
        rx.try_recv().unwrap(),
        AppEvent::StorageCheckRequested
    ));
    app.chat_widget.show_selection_view(status_view(&status(
        StorageBackend::LocalSqlite,
        StorageAuthority::Local,
        /*candidate_configured*/ true,
        Vec::new(),
    )));
    app.chat_widget.handle_key_event(KeyCode::Down.into());
    app.chat_widget.handle_key_event(KeyCode::Enter.into());
    assert!(matches!(
        rx.try_recv().unwrap(),
        AppEvent::StoragePlanRequested(StoragePlanAction::Migrate)
    ));
    // A profile that was never saved leaves both actions disabled.
    app.chat_widget.show_selection_view(status_view(&status(
        StorageBackend::LocalSqlite,
        StorageAuthority::Unmanaged,
        /*candidate_configured*/ false,
        Vec::new(),
    )));
    app.chat_widget.handle_key_event(KeyCode::Enter.into());
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn connection_results_name_where_the_test_stopped() {
    let ready = StorageConnectionReport {
        stage: StorageCheckStage::Ready,
        blocker: None,
        schema_format: Some(19),
        dataset_state: Some("open".to_string()),
        dataset_id: None,
        generation: Some(0),
        empty: Some(true),
    };
    let stopped = StorageConnectionReport {
        stage: StorageCheckStage::Connect,
        blocker: Some(StorageBlocker::ConnectionFailed),
        schema_format: None,
        dataset_state: None,
        dataset_id: None,
        generation: None,
        empty: None,
    };
    for (name, report) in [
        ("storage_check_ready", ready),
        ("storage_check_connection_failed", stopped),
    ] {
        let (app, _rx) = app_with(check_view(&report)).await;
        insta::assert_snapshot!(name, render_bottom_popup(&app.chat_widget, /*width*/ 80));
    }
}

#[tokio::test]
async fn plans_list_what_moves_and_only_a_clear_plan_can_start() {
    for (name, plan, width) in [
        (
            "storage_plan_migrate_ready",
            plan(StoragePlanAction::Migrate, Vec::new()),
            80,
        ),
        (
            "storage_plan_migrate_narrow",
            plan(StoragePlanAction::Migrate, Vec::new()),
            44,
        ),
        (
            "storage_plan_migrate_blocked",
            plan(
                StoragePlanAction::Migrate,
                vec![
                    StorageBlocker::TargetNotEmpty,
                    StorageBlocker::SqliteHomeDiffersFromCodexHome,
                ],
            ),
            80,
        ),
        (
            "storage_plan_return_ready",
            plan(StoragePlanAction::Return, Vec::new()),
            80,
        ),
        (
            "storage_plan_credentials_missing",
            plan(
                StoragePlanAction::Migrate,
                vec![
                    StorageBlocker::CredentialUnavailable,
                    StorageBlocker::MigratorCredentialMissing,
                ],
            ),
            80,
        ),
        ("storage_plan_attach_ready", attach_plan(Vec::new()), 80),
        (
            "storage_plan_attach_already_managed",
            attach_plan(vec![StorageBlocker::HomeAlreadyManaged]),
            80,
        ),
    ] {
        let (app, _rx) = app_with(plan_view(&plan)).await;
        insta::assert_snapshot!(name, render_bottom_popup(&app.chat_widget, width));
    }

    // Starting is the first row and asks for an explicit accept.
    let (mut app, mut rx) =
        app_with(plan_view(&plan(StoragePlanAction::Migrate, Vec::new()))).await;
    app.chat_widget.handle_key_event(KeyCode::Enter.into());
    match rx.try_recv().unwrap() {
        AppEvent::StorageStartRequested {
            action,
            plan_id,
            dataset_id,
        } => {
            assert_eq!(action, StoragePlanAction::Migrate);
            assert_eq!(plan_id, "0194e0a0-0000-7000-8000-0000000000aa");
            assert_eq!(dataset_id, None);
        }
        other => panic!("expected a start request, got {other:?}"),
    }

    // Joining sends back the dataset the operator was shown.
    let (mut app, mut rx) = app_with(plan_view(&attach_plan(Vec::new()))).await;
    app.chat_widget.handle_key_event(KeyCode::Enter.into());
    match rx.try_recv().unwrap() {
        AppEvent::StorageStartRequested {
            action, dataset_id, ..
        } => {
            assert_eq!(action, StoragePlanAction::Attach);
            assert_eq!(
                dataset_id.as_deref(),
                Some("0194e0a0-0000-7000-8000-000000000001")
            );
        }
        other => panic!("expected a start request, got {other:?}"),
    }

    // A blocked plan offers no start row at all.
    let (mut app, mut rx) = app_with(plan_view(&plan(
        StoragePlanAction::Migrate,
        vec![StorageBlocker::TargetNotEmpty],
    )))
    .await;
    app.chat_widget.handle_key_event(KeyCode::Enter.into());
    assert!(matches!(rx.try_recv().unwrap(), AppEvent::OpenStorageMenu));
}

#[tokio::test]
async fn operations_show_progress_and_offer_only_what_their_state_allows() {
    for (name, operation) in [
        (
            "storage_operation_copying",
            operation(StorageOperationState::Copying, /*blocker*/ None),
        ),
        (
            "storage_operation_ready",
            operation(StorageOperationState::Ready, /*blocker*/ None),
        ),
        (
            "storage_operation_active",
            operation(StorageOperationState::Active, /*blocker*/ None),
        ),
        (
            "storage_operation_failed",
            operation(
                StorageOperationState::Failed,
                Some(StorageBlocker::VerificationFailed),
            ),
        ),
    ] {
        let (app, _rx) = app_with(operation_view(&operation)).await;
        insta::assert_snapshot!(name, render_bottom_popup(&app.chat_widget, /*width*/ 80));
    }

    // Switching over is the first row of a verified copy and asks for an explicit accept.
    let (mut app, mut rx) = app_with(operation_view(&operation(
        StorageOperationState::Ready,
        /*blocker*/ None,
    )))
    .await;
    app.chat_widget.handle_key_event(KeyCode::Enter.into());
    assert!(matches!(
        rx.try_recv().unwrap(),
        AppEvent::StorageActivateRequested { operation_id } if operation_id == "0194e0a0-0000-7000-8000-0000000000bb"
    ));
}
