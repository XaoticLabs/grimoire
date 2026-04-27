//! Task 1 contract tests: schema migrations and supervision CRUD.

use chrono::Utc;
use std::path::PathBuf;

use grimoire::daemon::persistence::Database;
use grimoire::shared::types::{
    Agent, AgentState, RestartHistoryOutcome, RestartPolicy, SupervisionConfig,
};

fn seed_agent(db: &Database, id: &str, state: AgentState) -> Agent {
    let now = Utc::now();
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
        exit_code: None,
        created_at: now,
        updated_at: now,
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
    agent
}

#[test]
fn migration_adds_restart_history_table() {
    let db = Database::open_in_memory().unwrap();
    db.with_test_conn(|c| {
        let n: i64 = c
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='restart_history'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);

        for idx in [
            "restart_history_by_agent_window",
            "restart_history_by_time",
        ] {
            let n: i64 = c
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                    [idx],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1, "missing index {}", idx);
        }
    });
}

#[test]
fn migration_adds_supervision_columns() {
    let db = Database::open_in_memory().unwrap();
    db.with_test_conn(|c| {
        for col in [
            "restart_policy",
            "max_restarts",
            "restart_window_secs",
            "escalate_to",
            "restart_count",
            "escalation_depth",
        ] {
            let sql = format!("SELECT {} FROM agents LIMIT 0", col);
            assert!(c.prepare(&sql).is_ok(), "missing column {}", col);
        }
    });
}

#[test]
fn migration_is_idempotent() {
    // Calling open_in_memory creates the schema; opening another
    // connection on the same path would re-run migrate. To exercise
    // idempotency, simulate by opening a temp file twice.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.db");
    {
        let _ = Database::open(&path).unwrap();
    }
    {
        let _ = Database::open(&path).unwrap();
    }
}

#[test]
fn set_get_supervision_round_trips() {
    let db = Database::open_in_memory().unwrap();
    seed_agent(&db, "abcd0001", AgentState::Failed);
    let cfg = SupervisionConfig {
        policy: RestartPolicy::OnFailure,
        max_restarts: Some(3),
        window_secs: Some(60),
        escalate_to: Some("topic://x".to_string()),
    };
    db.set_supervision("abcd0001", &cfg).unwrap();
    let got = db.get_supervision("abcd0001").unwrap().unwrap();
    assert_eq!(got, cfg);
}

#[test]
fn count_restarts_in_window_filters_by_outcome() {
    let db = Database::open_in_memory().unwrap();
    seed_agent(&db, "agt00001", AgentState::Failed);
    let now = Utc::now().timestamp();
    db.insert_restart_history_row("agt00001", now, RestartHistoryOutcome::Scheduled, None)
        .unwrap();
    db.insert_restart_history_row("agt00001", now, RestartHistoryOutcome::FailedAgain, None)
        .unwrap();
    db.insert_restart_history_row("agt00001", now, RestartHistoryOutcome::Succeeded, None)
        .unwrap();
    db.insert_restart_history_row(
        "agt00001",
        now,
        RestartHistoryOutcome::BudgetExhausted,
        None,
    )
    .unwrap();
    let count = db.count_restarts_in_window("agt00001", now - 60).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn count_restarts_in_window_filters_by_time() {
    let db = Database::open_in_memory().unwrap();
    seed_agent(&db, "agt00002", AgentState::Failed);
    let now = Utc::now().timestamp();
    // Two outside window
    db.insert_restart_history_row(
        "agt00002",
        now - 7200,
        RestartHistoryOutcome::Scheduled,
        None,
    )
    .unwrap();
    db.insert_restart_history_row(
        "agt00002",
        now - 7000,
        RestartHistoryOutcome::FailedAgain,
        None,
    )
    .unwrap();
    // Two inside window
    db.insert_restart_history_row(
        "agt00002",
        now - 30,
        RestartHistoryOutcome::Scheduled,
        None,
    )
    .unwrap();
    db.insert_restart_history_row(
        "agt00002",
        now - 10,
        RestartHistoryOutcome::FailedAgain,
        None,
    )
    .unwrap();
    let count = db.count_restarts_in_window("agt00002", now - 60).unwrap();
    assert_eq!(count, 2);
}

#[test]
fn list_failed_with_active_policy_excludes_never() {
    let db = Database::open_in_memory().unwrap();
    seed_agent(&db, "fnvr0001", AgentState::Failed);
    seed_agent(&db, "fonf0001", AgentState::Failed);
    seed_agent(&db, "actv0001", AgentState::Active);
    db.set_supervision(
        "fonf0001",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(3),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    db.set_supervision(
        "actv0001",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(3),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    let ids = db.list_failed_with_active_policy().unwrap();
    assert_eq!(ids, vec!["fonf0001".to_string()]);
}

#[test]
fn mark_torn_restarting_as_failed_returns_ids_and_flips_state() {
    let db = Database::open_in_memory().unwrap();
    seed_agent(&db, "rstr0001", AgentState::Restarting);
    let ids = db.mark_torn_restarting_as_failed().unwrap();
    assert_eq!(ids, vec!["rstr0001".to_string()]);
    let agent = db.get_agent("rstr0001").unwrap().unwrap();
    assert_eq!(agent.state, AgentState::Failed);
}
