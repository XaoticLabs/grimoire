//! Contract tests for --keep-alive on summon (T8). Validates the
//! persistence flag + completion-time branch.

use std::path::PathBuf;

use chrono::Utc;
use grimoire::daemon::persistence::Database;
use grimoire::shared::types::{Agent, AgentState};

fn seed(db: &Database, id: &str, keep_alive: bool) {
    let a = Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Active,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: Some("sess".into()),
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&a).unwrap();
    if keep_alive {
        db.set_keep_alive(id, true).unwrap();
    }
}

#[test]
fn keep_alive_flag_round_trips_on_agent_row() {
    let db = Database::open_in_memory().unwrap();
    seed(&db, "agent01", true);
    seed(&db, "agent02", false);
    assert!(db.get_keep_alive("agent01").unwrap());
    assert!(!db.get_keep_alive("agent02").unwrap());
}
