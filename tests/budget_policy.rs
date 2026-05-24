//! Contract tests for the budget + policy admission gates.
//!
//! Two enforcement paths live in different layers and need separate
//! exercise:
//!
//!   * **Budget gate**: runs inside `AgentManager::dispatch_internal`,
//!     after the scheduler has already promoted a `Queued` row. Tested by
//!     pre-seeding `budget_spend` and asserting `dispatch_internal` either
//!     refuses (hard) or proceeds (soft).
//!   * **Policy gate**: runs inside `handle_summon` before the row is
//!     even enqueued. Tested over the RPC surface, which is the only
//!     code path that consults `[policy]`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;

use grimoire::daemon::agent_manager::{AgentManager, Lane};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::executor::{ExecuteRequest, Executor, ExecutorHandle};
use grimoire::daemon::persistence::Database;
use grimoire::daemon::provider::TokenBreakdown;
use grimoire::daemon::scheduler::Dispatcher;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::wake_registry::WakeRegistry;
use grimoire::daemon::workspace_registry::WorkspaceRegistry;
use grimoire::shared::config::{BudgetConfig, Config, PolicyConfig, ProviderPricing};
use grimoire::shared::protocol::RpcRequest;
use grimoire::shared::types::AgentState;

// --- A no-op executor; budget gate runs before `executor.start`. ----------

#[derive(Default)]
struct ExecutorLog {
    calls: Mutex<Vec<ExecuteRequest>>,
}

struct MockExecutor {
    log: Arc<ExecutorLog>,
}

#[async_trait]
impl Executor for MockExecutor {
    async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle> {
        self.log.calls.lock().unwrap().push(req);
        let completion = tokio::spawn(async {
            grimoire::daemon::process_manager::MonitorResult {
                state: AgentState::Complete,
                exit_code: Some(0),
                ..Default::default()
            }
        });
        Ok(ExecutorHandle {
            worker_id: None,
            pid: Some(0),
            cancel: Box::new(|| {}),
            completion,
        })
    }

    fn name(&self) -> &'static str {
        "mock"
    }
}

async fn manager_with(config: Config) -> (Arc<Database>, Arc<AgentManager>, Arc<ExecutorLog>) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let log = Arc::new(ExecutorLog::default());
    let executor: Arc<dyn Executor> = Arc::new(MockExecutor { log: log.clone() });
    let manager = AgentManager::new_with_executor(db.clone(), bus, config, executor).await;
    (db, manager, log)
}

fn config_with_budget(name: &str, daily_usd: f64, hard: bool) -> Config {
    let mut config = Config::default();
    config.budgets.insert(
        name.to_string(),
        BudgetConfig {
            daily_usd,
            providers: vec!["claude".into()],
            hard,
        },
    );
    config
}

async fn enqueue_and_claim(
    db: &Database,
    manager: &Arc<AgentManager>,
) -> grimoire::daemon::persistence::QueueRow {
    let agent = manager
        .enqueue(
            "task",
            None,
            None,
            Some("claude".into()),
            Path::new("/tmp"),
            Lane::Adhoc,
        )
        .await
        .unwrap();
    let row = db.peek_next_dispatch().unwrap().expect("queue row visible");
    assert_eq!(row.id, agent.id);
    assert!(db.claim_for_dispatch(&row.id).unwrap());
    row
}

// --- Budget gate ----------------------------------------------------------

#[tokio::test]
async fn hard_budget_exhausted_refuses_dispatch() {
    let config = config_with_budget("team", 1.0, true);
    let (db, manager, log) = manager_with(config).await;

    // Pre-spend the cap for today so the gate trips on the next dispatch.
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    db.add_budget_spend("team", &today, 1.50).unwrap();

    let row = enqueue_and_claim(&db, &manager).await;
    let err = (manager.as_ref() as &dyn Dispatcher)
        .dispatch(row)
        .await
        .expect_err("hard budget should refuse dispatch");
    assert!(
        err.to_string().contains("budget_exhausted"),
        "expected budget_exhausted, got: {err}"
    );
    assert!(
        log.calls.lock().unwrap().is_empty(),
        "executor must not be invoked when the budget gate trips"
    );
}

#[tokio::test]
async fn soft_budget_exceeded_proceeds_with_warning() {
    let config = config_with_budget("team", 1.0, false); // soft
    let (db, manager, log) = manager_with(config).await;

    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    db.add_budget_spend("team", &today, 5.00).unwrap();

    let row = enqueue_and_claim(&db, &manager).await;
    (manager.as_ref() as &dyn Dispatcher)
        .dispatch(row)
        .await
        .expect("soft budget should not refuse");
    assert_eq!(
        log.calls.lock().unwrap().len(),
        1,
        "executor must be invoked despite soft-budget overrun"
    );
}

#[tokio::test]
async fn budget_under_cap_dispatches_normally() {
    let config = config_with_budget("team", 10.0, true);
    let (db, manager, log) = manager_with(config).await;

    let row = enqueue_and_claim(&db, &manager).await;
    (manager.as_ref() as &dyn Dispatcher)
        .dispatch(row)
        .await
        .expect("under-cap dispatch should succeed");
    assert_eq!(log.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn budget_only_applies_to_listed_providers() {
    // Budget scoped to "aider" only; agent runs on "claude". Even a
    // zero cap shouldn't gate it.
    let mut config = Config::default();
    config.budgets.insert(
        "aider-budget".into(),
        BudgetConfig {
            daily_usd: 0.0,
            providers: vec!["aider".into()],
            hard: true,
        },
    );
    let (db, manager, log) = manager_with(config).await;

    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    db.add_budget_spend("aider-budget", &today, 999.0).unwrap();

    let row = enqueue_and_claim(&db, &manager).await;
    (manager.as_ref() as &dyn Dispatcher)
        .dispatch(row)
        .await
        .expect("provider mismatch should bypass budget");
    assert_eq!(log.calls.lock().unwrap().len(), 1);
}

// --- Cost math ------------------------------------------------------------

#[test]
fn token_breakdown_cost_uses_per_bucket_pricing() {
    let pricing = ProviderPricing {
        input_per_mtok: 3.0,
        output_per_mtok: 15.0,
        cache_read_per_mtok: Some(0.3),
        cache_creation_per_mtok: Some(3.75),
    };
    let b = TokenBreakdown {
        input: 1_000_000,
        output: 1_000_000,
        cache_read: 1_000_000,
        cache_creation: 1_000_000,
    };
    let usd = b.cost_usd(&pricing);
    // 3 + 15 + 0.3 + 3.75 = 22.05
    assert!((usd - 22.05).abs() < 1e-6, "got {usd}");
}

#[test]
fn token_breakdown_cost_uses_pricing_defaults_for_cache() {
    let pricing = ProviderPricing {
        input_per_mtok: 10.0,
        output_per_mtok: 50.0,
        cache_read_per_mtok: None,     // defaults to input/10 = 1.0
        cache_creation_per_mtok: None, // defaults to input*1.25 = 12.5
    };
    let b = TokenBreakdown {
        input: 0,
        output: 0,
        cache_read: 1_000_000,
        cache_creation: 1_000_000,
    };
    let usd = b.cost_usd(&pricing);
    assert!((usd - 13.5).abs() < 1e-6, "got {usd}");
}

// --- Policy gate (RPC layer) ---------------------------------------------

async fn rpc_summon(
    config: Config,
    params: serde_json::Value,
) -> grimoire::shared::protocol::RpcResponse {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), config).await;
    let sk = Arc::new(ScrollKeeper::new(db.clone(), manager.clone()));
    let clock: Arc<dyn grimoire::daemon::clock::Clock> =
        Arc::new(grimoire::daemon::clock::SystemClock);
    let wr = WakeRegistry::with_default_sender(db.clone(), bus.clone(), clock);
    let wsr = WorkspaceRegistry::with_default_git(db.clone(), bus.clone());
    let req = RpcRequest {
        method: "agent.summon".into(),
        params,
        id: 1,
        protocol_version: None,
        auth_token: None,
    };
    grimoire::daemon::rpc::handle_rpc_test(&manager, &db, &sk, &wr, &wsr, &bus, req).await
}

fn config_with_policy(p: PolicyConfig) -> Config {
    Config {
        policy: Some(p),
        ..Config::default()
    }
}

#[tokio::test]
async fn policy_denies_explicit_provider() {
    let config = config_with_policy(PolicyConfig {
        provider_deny: vec!["claude".into()],
        ..PolicyConfig::default()
    });
    let resp = rpc_summon(
        config,
        json!({"task": "t", "provider": "claude", "cwd": "/tmp"}),
    )
    .await;
    let err = resp.error.expect("expected denial");
    assert_eq!(err.message, "policy_provider_denied");
}

#[tokio::test]
async fn policy_blocks_provider_outside_allow_list() {
    let config = config_with_policy(PolicyConfig {
        provider_allow: vec!["pi".into()],
        ..PolicyConfig::default()
    });
    let resp = rpc_summon(
        config,
        json!({"task": "t", "provider": "claude", "cwd": "/tmp"}),
    )
    .await;
    assert_eq!(
        resp.error.expect("expected denial").message,
        "policy_provider_not_allowed"
    );
}

#[tokio::test]
async fn policy_allows_provider_when_on_allow_list() {
    let config = config_with_policy(PolicyConfig {
        provider_allow: vec!["claude".into()],
        ..PolicyConfig::default()
    });
    let resp = rpc_summon(
        config,
        json!({"task": "t", "provider": "claude", "cwd": "/tmp"}),
    )
    .await;
    assert!(resp.error.is_none(), "{resp:?}");
}

#[tokio::test]
async fn policy_denies_cwd_under_blocked_prefix() {
    let tmp = std::env::temp_dir().join("grim_policy_deny_test");
    std::fs::create_dir_all(&tmp).unwrap();
    let config = config_with_policy(PolicyConfig {
        cwd_deny_prefixes: vec![tmp.clone()],
        ..PolicyConfig::default()
    });
    let resp = rpc_summon(config, json!({"task": "t", "cwd": tmp.to_str().unwrap()})).await;
    let err = resp.error.expect("expected denial");
    assert_eq!(err.message, "policy_cwd_denied");
}

#[tokio::test]
async fn policy_denies_cwd_outside_allow_prefix() {
    let allow = std::env::temp_dir().join("grim_policy_allow_only");
    std::fs::create_dir_all(&allow).unwrap();
    let config = config_with_policy(PolicyConfig {
        cwd_allow_prefixes: vec![allow],
        ..PolicyConfig::default()
    });
    let resp = rpc_summon(config, json!({"task": "t", "cwd": "/tmp"})).await;
    let err = resp.error.expect("expected denial");
    assert_eq!(err.message, "policy_cwd_not_allowed");
}
