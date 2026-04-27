//! Banish-cascade contract tests for Task 7. Banishing an agent must
//! retire all of its registered wake sources.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use grimoire::daemon::clock::{Clock, SystemClock};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::wake_registry::WakeRegistry;
use grimoire::shared::types::{Agent, AgentState};

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
    };
    db.insert_agent(&a).unwrap();
}

#[tokio::test]
async fn banish_retires_all_agents_wake_sources() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let reg = WakeRegistry::with_default_sender(db.clone(), bus, clock);
    seed_agent(&db, "victim01");
    seed_agent(&db, "bystand1");

    reg.register_cron("victim01", "* * * * *").await.unwrap();
    reg.register_cron("victim01", "0 9 * * *").await.unwrap();
    reg.register_cron("bystand1", "* * * * *").await.unwrap();

    let n = reg.retire_for_agent("victim01").await.unwrap();
    assert_eq!(n, 2);
    assert_eq!(reg.list_for_agent("victim01").await.unwrap().len(), 0);
    assert_eq!(reg.list_for_agent("bystand1").await.unwrap().len(), 1);
}

#[tokio::test]
async fn banish_with_no_wake_sources_succeeds() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let reg = WakeRegistry::with_default_sender(db.clone(), bus, clock);
    seed_agent(&db, "lonely01");
    let n = reg.retire_for_agent("lonely01").await.unwrap();
    assert_eq!(n, 0);
}
