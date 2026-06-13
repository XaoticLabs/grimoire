//! Contract tests for the wake_sources / wake_rate_limits schema and the
//! Database CRUD helpers.

use std::path::PathBuf;

use chrono::Utc;
use grimoire::daemon::persistence::Database;
use grimoire::shared::types::{Agent, AgentState, WakeSource, WakeSourceKind, WakeSourceState};

fn fresh() -> Database {
    Database::open_in_memory().unwrap()
}

fn seed_agent(db: &Database, id: &str) {
    let a = Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Dormant,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: Some("sess".into()),
        exit_code: Some(0),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&a).unwrap();
}

fn make_source(id: &str, agent_id: &str, kind: WakeSourceKind) -> WakeSource {
    WakeSource {
        id: id.to_string(),
        agent_id: agent_id.to_string(),
        kind,
        config_json: r#"{"expr":"0 9 * * 1-5"}"#.to_string(),
        state: WakeSourceState::Armed,
        fail_reason: None,
        last_fired_at: None,
        fire_count: 0,
        created_at: 1_700_000_000,
    }
}

#[test]
fn wake_sources_table_exists_after_migrate() {
    let db = fresh();
    let names: Vec<String> = db.with_test_conn(|c| {
        let mut stmt = c
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap();

        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    });
    assert!(names.iter().any(|n| n == "wake_sources"));
    assert!(names.iter().any(|n| n == "wake_rate_limits"));
}

#[test]
fn insert_and_get_wake_source_roundtrip() {
    let db = fresh();
    seed_agent(&db, "agent001");
    let src = make_source("wake_aaaa1111", "agent001", WakeSourceKind::Cron);
    db.insert_wake_source(&src).unwrap();
    let got = db.get_wake_source("wake_aaaa1111").unwrap().unwrap();
    assert_eq!(got, src);
}

#[test]
fn list_wake_sources_for_agent_filters() {
    let db = fresh();
    seed_agent(&db, "agent_a");
    seed_agent(&db, "agent_b");
    db.insert_wake_source(&make_source("wake_a1", "agent_a", WakeSourceKind::Cron))
        .unwrap();
    db.insert_wake_source(&make_source(
        "wake_a2",
        "agent_a",
        WakeSourceKind::FileWatch,
    ))
    .unwrap();
    db.insert_wake_source(&make_source("wake_b1", "agent_b", WakeSourceKind::Cron))
        .unwrap();
    let a = db.list_wake_sources_for_agent("agent_a").unwrap();
    let b = db.list_wake_sources_for_agent("agent_b").unwrap();
    assert_eq!(a.len(), 2);
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].id, "wake_b1");
}

#[test]
fn bump_wake_source_fire_increments_count_and_sets_timestamp() {
    let db = fresh();
    seed_agent(&db, "agent_x");
    let src = make_source("wake_xx", "agent_x", WakeSourceKind::Cron);
    db.insert_wake_source(&src).unwrap();
    db.bump_wake_source_fire("wake_xx", 1_000).unwrap();
    db.bump_wake_source_fire("wake_xx", 2_000).unwrap();
    let got = db.get_wake_source("wake_xx").unwrap().unwrap();
    assert_eq!(got.fire_count, 2);
    assert_eq!(got.last_fired_at, Some(2_000));
}

#[test]
fn delete_wake_source_removes_row() {
    let db = fresh();
    seed_agent(&db, "agent");
    let src = make_source("wake_del1", "agent", WakeSourceKind::Cron);
    db.insert_wake_source(&src).unwrap();
    assert!(db.delete_wake_source("wake_del1").unwrap());
    assert!(db.get_wake_source("wake_del1").unwrap().is_none());
    assert!(!db.delete_wake_source("wake_del1").unwrap()); // second delete is a no-op
}

#[test]
fn delete_wake_sources_for_agent_bulk() {
    let db = fresh();
    seed_agent(&db, "agent_a");
    seed_agent(&db, "agent_b");
    db.insert_wake_source(&make_source("wake_a1", "agent_a", WakeSourceKind::Cron))
        .unwrap();
    db.insert_wake_source(&make_source(
        "wake_a2",
        "agent_a",
        WakeSourceKind::FileWatch,
    ))
    .unwrap();
    db.insert_wake_source(&make_source("wake_b1", "agent_b", WakeSourceKind::Cron))
        .unwrap();
    let n = db.delete_wake_sources_for_agent("agent_a").unwrap();
    assert_eq!(n, 2);
    assert!(
        db.list_wake_sources_for_agent("agent_a")
            .unwrap()
            .is_empty()
    );
    assert_eq!(db.list_wake_sources_for_agent("agent_b").unwrap().len(), 1);
}

#[test]
fn rate_limit_init_creates_row_at_full_capacity() {
    let db = fresh();
    seed_agent(&db, "agent_x");
    let (tokens, last, cap, refill) = db.get_or_init_rate_limit("agent_x", 1_000).unwrap();
    #[allow(clippy::cast_precision_loss)]
    let cap_f = cap as f64;
    assert!((tokens - cap_f).abs() < f64::EPSILON);
    assert_eq!(last, 1_000);
    assert_eq!(cap, 60);
    assert!((refill - 60.0 / 3600.0).abs() < 1e-6);
}

#[test]
fn rate_limit_persists_token_updates() {
    let db = fresh();
    seed_agent(&db, "agent_x");
    db.get_or_init_rate_limit("agent_x", 1_000).unwrap();
    db.update_rate_limit_tokens("agent_x", 12.5, 1_500).unwrap();
    let (tokens, last, _, _) = db.get_or_init_rate_limit("agent_x", 9_999).unwrap();
    assert!((tokens - 12.5).abs() < 1e-6);
    assert_eq!(last, 1_500);
}
