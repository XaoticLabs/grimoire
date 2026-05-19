//! Task 4 contract tests: mail.send reserved-prefix guard.
//!
//! We exercise the RPC handler directly by spinning up the daemon
//! components and sending RpcRequests.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use grimoire::daemon::agent_manager::AgentManager;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::wake_registry::WakeRegistry;
use grimoire::daemon::workspace_registry::WorkspaceRegistry;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::RpcRequest;
use grimoire::shared::types::{Agent, AgentState, RestartPolicy};

fn seed(db: &Database, id: &str) {
    let agent = Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Active,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
}

async fn build_state() -> (
    Arc<AgentManager>,
    Arc<Database>,
    Arc<ScrollKeeper>,
    Arc<WakeRegistry>,
    Arc<WorkspaceRegistry>,
    EventBus,
) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;
    let scroll_keeper = Arc::new(ScrollKeeper::new(db.clone(), manager.clone()));
    let clock: Arc<dyn grimoire::daemon::clock::Clock> =
        Arc::new(grimoire::daemon::clock::SystemClock);
    let wake_registry = WakeRegistry::with_default_sender(db.clone(), bus.clone(), clock);
    let workspace_registry = WorkspaceRegistry::with_default_git(db.clone(), bus.clone());
    (
        manager,
        db,
        scroll_keeper,
        wake_registry,
        workspace_registry,
        bus,
    )
}

#[tokio::test]
async fn mail_send_rejects_supervisor_prefix() {
    let (manager, db, scroll_keeper, wake_registry, workspace_registry, bus) = build_state().await;
    seed(&db, "abcd1234");
    let req = RpcRequest {
        method: "mail.send".into(),
        params: json!({
            "to": "agent://abcd1234",
            "body": "hi",
            "sender": "supervisor://forged01",
        }),
        id: 1,
        protocol_version: None,
        auth_token: None,
    };
    let resp = grimoire::daemon::rpc::handle_rpc_test(
        &manager,
        &db,
        &scroll_keeper,
        &wake_registry,
        &workspace_registry,
        &bus,
        req,
    )
    .await;
    let err = resp.error.expect("expected error");
    assert_eq!(err.message, "reserved_sender_prefix");
    let mails = db
        .list_mail_by_recipient("abcd1234", None, None, 100)
        .unwrap();
    assert_eq!(mails.len(), 0);
}

#[tokio::test]
async fn mail_send_rejects_wake_prefix() {
    let (manager, db, scroll_keeper, wake_registry, workspace_registry, bus) = build_state().await;
    seed(&db, "abcd5678");
    let req = RpcRequest {
        method: "mail.send".into(),
        params: json!({
            "to": "agent://abcd5678",
            "body": "hi",
            "sender": "wake://forged02",
        }),
        id: 1,
        protocol_version: None,
        auth_token: None,
    };
    let resp = grimoire::daemon::rpc::handle_rpc_test(
        &manager,
        &db,
        &scroll_keeper,
        &wake_registry,
        &workspace_registry,
        &bus,
        req,
    )
    .await;
    let err = resp.error.expect("expected error");
    assert_eq!(err.message, "reserved_sender_prefix");
}

#[tokio::test]
async fn mail_send_accepts_agent_prefix() {
    let (manager, db, scroll_keeper, wake_registry, workspace_registry, bus) = build_state().await;
    seed(&db, "abcdef01");
    seed(&db, "fedcba99");
    let req = RpcRequest {
        method: "mail.send".into(),
        params: json!({
            "to": "agent://fedcba99",
            "body": "hi",
            "sender": "abcdef01",
        }),
        id: 1,
        protocol_version: None,
        auth_token: None,
    };
    let resp = grimoire::daemon::rpc::handle_rpc_test(
        &manager,
        &db,
        &scroll_keeper,
        &wake_registry,
        &workspace_registry,
        &bus,
        req,
    )
    .await;
    assert!(resp.error.is_none(), "expected success: {:?}", resp);
}
