//! Contract test: `wake.add` file-watch roots resolve to the *agent's* cwd,
//! regardless of what root the client sends. (Older CLIs sent their own
//! current_dir, which silently watched the wrong directory whenever the
//! operator's shell wasn't sitting in the agent's cwd.)

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

fn seed_with_cwd(db: &Database, id: &str, cwd: PathBuf) {
    let agent = Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Dormant,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd,
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

#[tokio::test]
async fn wake_add_file_watch_root_is_agent_cwd_not_client_root() {
    let agent_cwd = tempfile::tempdir().unwrap();
    let client_cwd = tempfile::tempdir().unwrap();

    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;
    let scroll_keeper = Arc::new(ScrollKeeper::new(db.clone(), manager.clone()));
    let clock: Arc<dyn grimoire::daemon::clock::Clock> =
        Arc::new(grimoire::daemon::clock::SystemClock);
    let wake_registry = WakeRegistry::with_default_sender(db.clone(), bus.clone(), clock);
    let workspace_registry = WorkspaceRegistry::with_default_git(db.clone(), bus.clone());

    seed_with_cwd(&db, "abcd1234", agent_cwd.path().to_path_buf());

    let req = RpcRequest {
        method: "wake.add".into(),
        params: json!({
            "agent_id": "abcd1234",
            "kind": "file_watch",
            "config": {
                "globs": ["**"],
                "ignore": [],
                // A stale/foreign root, as an old CLI would send.
                "root": client_cwd.path(),
            },
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
    assert!(resp.error.is_none(), "wake.add failed: {:?}", resp.error);

    let sources = db.list_armed_wake_sources().unwrap();
    assert_eq!(sources.len(), 1);
    let cfg: serde_json::Value = serde_json::from_str(&sources[0].config_json).unwrap();
    assert_eq!(
        cfg["root"],
        json!(agent_cwd.path()),
        "file-watch root must be the agent's cwd, not the client's"
    );
}
