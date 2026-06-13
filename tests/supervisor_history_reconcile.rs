//! Contract tests for history reconciliation + should_wake.

use std::path::PathBuf;

use chrono::Utc;

use grimoire::daemon::persistence::Database;
use grimoire::daemon::scheduler::Scheduler;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{Agent, AgentState, RestartHistoryOutcome, RestartPolicy};

fn seed_agent(db: &Database, id: &str, state: AgentState) {
    let agent = Agent {
        id: id.to_string(),
        name: None,
        state,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: Some(0),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
}

#[test]
fn complete_after_restart_marks_succeeded() {
    let db = Database::open_in_memory().unwrap();
    seed_agent(&db, "rec00001", AgentState::Active);
    db.insert_restart_history_row(
        "rec00001",
        Utc::now().timestamp(),
        RestartHistoryOutcome::Scheduled,
        None,
    )
    .unwrap();
    let n = db
        .update_latest_scheduled_outcome("rec00001", RestartHistoryOutcome::Succeeded)
        .unwrap();
    assert_eq!(n, 1);
    db.bump_restart_count("rec00001").unwrap();
    let agent = db.get_agent("rec00001").unwrap().unwrap();
    assert_eq!(agent.restart_count, 1);
}

#[test]
fn failed_again_after_restart_marks_failed_again() {
    let db = Database::open_in_memory().unwrap();
    seed_agent(&db, "rec00002", AgentState::Active);
    db.insert_restart_history_row(
        "rec00002",
        Utc::now().timestamp(),
        RestartHistoryOutcome::Scheduled,
        None,
    )
    .unwrap();
    let n = db
        .update_latest_scheduled_outcome("rec00002", RestartHistoryOutcome::FailedAgain)
        .unwrap();
    assert_eq!(n, 1);
    // window count includes failed_again rows
    let count = db.count_restarts_in_window("rec00002", 0).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn should_wake_includes_restart_scheduled() {
    let ev = StreamEvent::RestartScheduled {
        agent_id: "x".into(),
        attempt: 1,
        max: 3,
        fire_at_unix: 0,
        rate_limited: false,
    };
    assert!(Scheduler::should_wake(&ev));
}
