//! Handler-level tests for `RpcRequest.protocol_version`. The wire-level
//! serde shape is covered in `tests/federation_slice1.rs`; this file
//! exercises the dispatcher rejection path.

use std::sync::Arc;

use serde_json::json;

use grimoire::daemon::agent_manager::AgentManager;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::wake_registry::WakeRegistry;
use grimoire::daemon::workspace_registry::WorkspaceRegistry;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::RpcRequest;

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

fn req(pv: Option<u32>) -> RpcRequest {
    RpcRequest {
        method: "daemon.status".into(),
        params: json!({}),
        id: 1,
        protocol_version: pv,
        auth_token: None,
    }
}

async fn dispatch(req: RpcRequest) -> grimoire::shared::protocol::RpcResponse {
    let (manager, db, scroll_keeper, wake_registry, workspace_registry, bus) = build_state().await;
    grimoire::daemon::rpc::handle_rpc_test(
        &manager,
        &db,
        &scroll_keeper,
        &wake_registry,
        &workspace_registry,
        &bus,
        req,
    )
    .await
}

#[tokio::test]
async fn omitted_protocol_version_is_accepted_as_v1() {
    let resp = dispatch(req(None)).await;
    // daemon.status is implemented; we expect a success or a non-version
    // error. Crucially, we must *not* see unsupported_protocol_version.
    if let Some(err) = resp.error {
        assert_ne!(err.message, "unsupported_protocol_version");
    }
}

#[tokio::test]
async fn explicit_v1_is_accepted() {
    let resp = dispatch(req(Some(1))).await;
    if let Some(err) = resp.error {
        assert_ne!(err.message, "unsupported_protocol_version");
    }
}

#[tokio::test]
async fn unknown_protocol_version_is_rejected() {
    let resp = dispatch(req(Some(999))).await;
    let err = resp
        .error
        .expect("expected error for unknown protocol version");
    assert_eq!(err.message, "unsupported_protocol_version");
}

#[tokio::test]
async fn future_protocol_version_is_rejected() {
    // Any non-1 version, even one above the current max, is rejected on a
    // v1-only daemon. When v2 ships, this test moves to "v1 and v2 accepted".
    let resp = dispatch(req(Some(2))).await;
    let err = resp.error.expect("expected error for v2");
    assert_eq!(err.message, "unsupported_protocol_version");
}
