//! Contract tests for handle_summon validation.

use std::sync::Arc;

use serde_json::json;

use grimoire::daemon::agent_manager::AgentManager;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::wake_registry::WakeRegistry;
use grimoire::daemon::workspace_registry::WorkspaceRegistry;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::{RpcRequest, SummonResult};
use grimoire::shared::types::RestartPolicy;

async fn build() -> (
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
    let sk = Arc::new(ScrollKeeper::new(db.clone(), manager.clone()));
    let clock: Arc<dyn grimoire::daemon::clock::Clock> =
        Arc::new(grimoire::daemon::clock::SystemClock);
    let wr = WakeRegistry::with_default_sender(db.clone(), bus.clone(), clock);
    let wsr = WorkspaceRegistry::with_default_git(db.clone(), bus.clone());
    (manager, db, sk, wr, wsr, bus)
}

async fn summon(params: serde_json::Value) -> grimoire::shared::protocol::RpcResponse {
    let (m, db, sk, wr, wsr, bus) = build().await;
    let req = RpcRequest {
        method: "agent.summon".into(),
        params,
        id: 1,
        protocol_version: None,
        auth_token: None,
    };
    grimoire::daemon::rpc::handle_rpc_test(&m, &db, &sk, &wr, &wsr, &bus, req).await
}

#[tokio::test]
async fn summon_on_failure_persists_full_config() {
    let (m, db, sk, wr, wsr, bus) = build().await;
    let req = RpcRequest {
        method: "agent.summon".into(),
        params: json!({
            "task": "t",
            "restart_policy": "on_failure",
            "max_restarts": 3,
            "restart_window_secs": 60,
            "escalate_to": "topic://x",
        }),
        id: 1,
        protocol_version: None,
        auth_token: None,
    };
    let resp = grimoire::daemon::rpc::handle_rpc_test(&m, &db, &sk, &wr, &wsr, &bus, req).await;
    assert!(resp.error.is_none(), "{resp:?}");
    let r: SummonResult = serde_json::from_value(resp.result.unwrap()).unwrap();
    let cfg = db.get_supervision(&r.id).unwrap().unwrap();
    assert_eq!(cfg.policy, RestartPolicy::OnFailure);
    assert_eq!(cfg.max_restarts, Some(3));
    assert_eq!(cfg.window_secs, Some(60));
    assert_eq!(cfg.escalate_to.as_deref(), Some("topic://x"));
}

#[tokio::test]
async fn summon_idempotency_key_collapses_duplicates() {
    let (m, db, sk, wr, wsr, bus) = build().await;
    let mk = |id: u64| RpcRequest {
        method: "agent.summon".into(),
        params: json!({"task": "build the thing", "idempotency_key": "deploy-2026-06-12"}),
        id,
        protocol_version: None,
        auth_token: None,
    };
    let r1 = grimoire::daemon::rpc::handle_rpc_test(&m, &db, &sk, &wr, &wsr, &bus, mk(1)).await;
    let a1: SummonResult = serde_json::from_value(r1.result.unwrap()).unwrap();
    // A second summon with the same key returns the same agent, not a new one.
    let r2 = grimoire::daemon::rpc::handle_rpc_test(&m, &db, &sk, &wr, &wsr, &bus, mk(2)).await;
    let a2: SummonResult = serde_json::from_value(r2.result.unwrap()).unwrap();
    assert_eq!(
        a1.id, a2.id,
        "same idempotency key must return the same agent"
    );
    // Exactly one agent exists.
    assert_eq!(db.list_agents(None).unwrap().len(), 1);

    // A different key mints a distinct agent.
    let other = RpcRequest {
        method: "agent.summon".into(),
        params: json!({"task": "other", "idempotency_key": "different"}),
        id: 3,
        protocol_version: None,
        auth_token: None,
    };
    let r3 = grimoire::daemon::rpc::handle_rpc_test(&m, &db, &sk, &wr, &wsr, &bus, other).await;
    let a3: SummonResult = serde_json::from_value(r3.result.unwrap()).unwrap();
    assert_ne!(a1.id, a3.id);
}

#[tokio::test]
async fn summon_never_persists_defaults() {
    let (m, db, sk, wr, wsr, bus) = build().await;
    let req = RpcRequest {
        method: "agent.summon".into(),
        params: json!({"task": "t"}),
        id: 1,
        protocol_version: None,
        auth_token: None,
    };
    let resp = grimoire::daemon::rpc::handle_rpc_test(&m, &db, &sk, &wr, &wsr, &bus, req).await;
    let r: SummonResult = serde_json::from_value(resp.result.unwrap()).unwrap();
    let agent = db.get_agent(&r.id).unwrap().unwrap();
    assert_eq!(agent.restart_policy, RestartPolicy::Never);
    assert_eq!(agent.restart_count, 0);
}

#[tokio::test]
async fn summon_escalate_without_policy_rejects() {
    let resp = summon(json!({
        "task": "t",
        "escalate_to": "topic://x",
    }))
    .await;
    let err = resp.error.unwrap();
    assert_eq!(err.message, "escalate_requires_policy");
}

#[tokio::test]
async fn summon_on_failure_without_max_restarts_rejects() {
    let resp = summon(json!({
        "task": "t",
        "restart_policy": "on_failure",
    }))
    .await;
    assert_eq!(resp.error.unwrap().message, "max_restarts_required");
}

#[tokio::test]
async fn summon_max_restarts_zero_rejects() {
    let resp = summon(json!({
        "task": "t",
        "restart_policy": "on_failure",
        "max_restarts": 0,
        "restart_window_secs": 60,
    }))
    .await;
    assert_eq!(resp.error.unwrap().message, "max_restarts_zero");
}

#[tokio::test]
async fn summon_window_too_large_rejects() {
    let resp = summon(json!({
        "task": "t",
        "restart_policy": "on_failure",
        "max_restarts": 3,
        "restart_window_secs": 700_000,
    }))
    .await;
    assert_eq!(resp.error.unwrap().message, "window_too_large");
}

#[tokio::test]
async fn summon_self_escalation_rejects() {
    // IDs are random, so true self-escalation can't be triggered deterministically
    // here; that path is covered by integration testing. This exercises the
    // `escalate_to` address-validation branch with an invalid-hex agent address.
    let resp = summon(json!({
        "task": "t",
        "restart_policy": "on_failure",
        "max_restarts": 3,
        "restart_window_secs": 60,
        "escalate_to": "agent://zzzzzzzz",
    }))
    .await;
    // 'z' is invalid hex, should fail with invalid_agent_id
    assert_eq!(resp.error.unwrap().message, "invalid_agent_id");
}

#[tokio::test]
async fn summon_invalid_escalate_address_rejects() {
    let resp = summon(json!({
        "task": "t",
        "restart_policy": "on_failure",
        "max_restarts": 3,
        "restart_window_secs": 60,
        "escalate_to": "bogus://x",
    }))
    .await;
    assert_eq!(resp.error.unwrap().message, "invalid_address");
}
