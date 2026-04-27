//! Contract tests for the boot-time `migrate_dormant_agents` migration
//! (Task 1). Asserts that exactly the Complete-with-session agents are
//! promoted to Dormant, the migration is idempotent, and unrelated rows
//! are not touched.

use std::path::PathBuf;

use chrono::Utc;
use grimoire::daemon::persistence::Database;
use grimoire::shared::types::{Agent, AgentState};

fn seed(db: &Database, id: &str, state: AgentState, session_id: Option<&str>) {
    let agent = Agent {
        id: id.to_string(),
        name: None,
        state,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: session_id.map(|s| s.to_string()),
        exit_code: Some(0),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
    };
    db.insert_agent(&agent).unwrap();
    if let Some(sid) = session_id {
        db.update_agent_session_id(id, sid).unwrap();
    }
}

#[test]
fn migration_promotes_complete_with_session() {
    let db = Database::open_in_memory().unwrap();
    seed(&db, "withsess", AgentState::Complete, Some("sess-1"));
    seed(&db, "nosess00", AgentState::Complete, None);

    let migrated = db.migrate_dormant_agents().unwrap();
    assert_eq!(migrated, vec!["withsess".to_string()]);

    assert_eq!(
        db.get_agent("withsess").unwrap().unwrap().state,
        AgentState::Dormant
    );
    assert_eq!(
        db.get_agent("nosess00").unwrap().unwrap().state,
        AgentState::Complete
    );
}

#[test]
fn migration_is_idempotent() {
    let db = Database::open_in_memory().unwrap();
    seed(&db, "abcd1234", AgentState::Complete, Some("sess"));
    let first = db.migrate_dormant_agents().unwrap();
    assert_eq!(first.len(), 1);
    let second = db.migrate_dormant_agents().unwrap();
    assert!(second.is_empty(), "second run must be a no-op");
}

#[test]
fn migration_skips_failed_with_session() {
    let db = Database::open_in_memory().unwrap();
    seed(&db, "failsess", AgentState::Failed, Some("sess"));
    let migrated = db.migrate_dormant_agents().unwrap();
    assert!(migrated.is_empty());
    assert_eq!(
        db.get_agent("failsess").unwrap().unwrap().state,
        AgentState::Failed
    );
}

#[test]
fn migration_skips_banished_with_session() {
    let db = Database::open_in_memory().unwrap();
    seed(&db, "bansess1", AgentState::Banished, Some("sess"));
    let migrated = db.migrate_dormant_agents().unwrap();
    assert!(migrated.is_empty());
    assert_eq!(
        db.get_agent("bansess1").unwrap().unwrap().state,
        AgentState::Banished
    );
}
