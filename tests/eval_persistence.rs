//! Contract tests for the `eval_results` storage layer + RPC round-trip.

use std::sync::Arc;

use serde_json::json;

use grimoire::daemon::agent_manager::{AgentManager, Lane};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::wake_registry::WakeRegistry;
use grimoire::daemon::workspace_registry::WorkspaceRegistry;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::{EvalListResult, EvalRecordResult, RpcRequest};

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

async fn call(
    m: &Arc<AgentManager>,
    db: &Arc<Database>,
    sk: &Arc<ScrollKeeper>,
    wr: &Arc<WakeRegistry>,
    wsr: &Arc<WorkspaceRegistry>,
    bus: &EventBus,
    method: &str,
    params: serde_json::Value,
) -> grimoire::shared::protocol::RpcResponse {
    let req = RpcRequest {
        method: method.into(),
        params,
        id: 1,
        protocol_version: None,
        auth_token: None,
    };
    grimoire::daemon::rpc::handle_rpc_test(m, db, sk, wr, wsr, bus, req).await
}

async fn seed_agent(manager: &Arc<AgentManager>) -> String {
    let a = manager
        .enqueue(
            "task",
            None,
            None,
            None,
            std::path::Path::new("/tmp"),
            Lane::Adhoc,
        )
        .await
        .unwrap();
    a.id
}

#[tokio::test]
async fn record_and_list_roundtrip() {
    let (m, db, sk, wr, wsr, bus) = build().await;
    let target = seed_agent(&m).await;
    let evaluator = seed_agent(&m).await;

    let resp = call(
        &m,
        &db,
        &sk,
        &wr,
        &wsr,
        &bus,
        "eval.record",
        json!({
            "target_id": target,
            "evaluator_id": evaluator,
            "score": 0.87,
            "verdict": "pass",
            "rationale": "rubric satisfied",
        }),
    )
    .await;
    assert!(resp.error.is_none(), "{resp:?}");
    let recorded: EvalRecordResult = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert!(!recorded.id.is_empty());

    let resp = call(
        &m,
        &db,
        &sk,
        &wr,
        &wsr,
        &bus,
        "eval.list",
        json!({"target_id": target}),
    )
    .await;
    assert!(resp.error.is_none(), "{resp:?}");
    let listed: EvalListResult = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(listed.results.len(), 1);
    let r = &listed.results[0];
    assert_eq!(r.id, recorded.id);
    assert_eq!(r.target_id, target);
    assert_eq!(r.evaluator_id, evaluator);
    assert!((r.score - 0.87).abs() < 1e-6);
    assert_eq!(r.verdict.as_deref(), Some("pass"));
}

#[tokio::test]
async fn record_rejects_unknown_target() {
    let (m, db, sk, wr, wsr, bus) = build().await;
    let evaluator = seed_agent(&m).await;
    let resp = call(
        &m,
        &db,
        &sk,
        &wr,
        &wsr,
        &bus,
        "eval.record",
        json!({
            "target_id": "deadbeef",
            "evaluator_id": evaluator,
            "score": 0.5,
        }),
    )
    .await;
    assert_eq!(
        resp.error.expect("expected error").message,
        "target_not_found"
    );
}

#[tokio::test]
async fn list_returns_results_newest_first() {
    let (m, db, sk, wr, wsr, bus) = build().await;
    let target = seed_agent(&m).await;
    let evaluator = seed_agent(&m).await;
    // Insert directly so created_at can be controlled.
    let id_old = db
        .insert_eval_result(&target, &evaluator, 0.4, Some("fail"), None)
        .unwrap();
    // Sleep so the second row has a strictly later created_at second.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let id_new = db
        .insert_eval_result(&target, &evaluator, 0.9, Some("pass"), None)
        .unwrap();

    let resp = call(
        &m,
        &db,
        &sk,
        &wr,
        &wsr,
        &bus,
        "eval.list",
        json!({"target_id": target}),
    )
    .await;
    let listed: EvalListResult = serde_json::from_value(resp.result.unwrap()).unwrap();
    assert_eq!(listed.results.len(), 2);
    assert_eq!(listed.results[0].id, id_new, "newest first");
    assert_eq!(listed.results[1].id, id_old);
}

#[tokio::test]
async fn latest_eval_score_returns_most_recent() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;
    let target = seed_agent(&manager).await;
    let evaluator = seed_agent(&manager).await;

    assert!(db.latest_eval_score(&target).unwrap().is_none());
    db.insert_eval_result(&target, &evaluator, 0.30, None, None)
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    db.insert_eval_result(&target, &evaluator, 0.95, None, None)
        .unwrap();

    let latest = db.latest_eval_score(&target).unwrap().unwrap();
    assert!((latest - 0.95).abs() < 1e-6);
}
