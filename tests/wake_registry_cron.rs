//! Contract tests for the cron wake source through the `WakeRegistry`.
//! Uses `TestClock` for deterministic time travel.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use grimoire::daemon::clock::{Clock, TestClock};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::wake_registry::{WakeMailSender, WakeRegistry};
use grimoire::shared::types::{Agent, AgentState, WakeSourceKind, WakeSourceState};
use tokio::sync::Mutex;

#[derive(Default)]
struct RecordingSender {
    calls: Mutex<Vec<(String, String, String)>>, // (wake_id, agent_id, body)
}

#[async_trait]
impl WakeMailSender for RecordingSender {
    async fn send_wake_mail(&self, wake_id: &str, agent_id: &str, body: &str) -> Result<String> {
        let mut g = self.calls.lock().await;
        let id = format!("mail{:04}", g.len());
        g.push((wake_id.to_string(), agent_id.to_string(), body.to_string()));
        Ok(id)
    }
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

fn make_registry(
    db: Arc<Database>,
    bus: EventBus,
    clock: Arc<dyn Clock>,
) -> (Arc<WakeRegistry>, Arc<RecordingSender>) {
    let sender = Arc::new(RecordingSender::default());
    let reg = WakeRegistry::new(db, bus, clock, sender.clone());
    (reg, sender)
}

#[tokio::test]
async fn register_cron_source_persists_and_arms() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap(),
    ));
    seed_agent(&db, "agent01");

    let (reg, _) = make_registry(db.clone(), bus, clock);
    let wake_id = reg.register_cron("agent01", "* * * * *").await.unwrap();
    let row = db.get_wake_source(&wake_id).unwrap().unwrap();
    assert_eq!(row.state, WakeSourceState::Armed);
    assert_eq!(row.kind, WakeSourceKind::Cron);
    assert_eq!(row.agent_id, "agent01");
}

#[tokio::test]
async fn invalid_cron_expr_fails_registration() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(Utc::now()));
    seed_agent(&db, "agent01");
    let (reg, _) = make_registry(db.clone(), bus, clock);
    let res = reg.register_cron("agent01", "not a cron").await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("invalid_cron"));
}

#[tokio::test]
async fn cron_fires_when_clock_crosses_schedule() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    let test_clock = Arc::new(TestClock::new(t0));
    let clock: Arc<dyn Clock> = test_clock.clone();
    seed_agent(&db, "agent01");
    let (reg, sender) = make_registry(db.clone(), bus, clock);

    let _wake_id = reg.register_cron("agent01", "* * * * *").await.unwrap();
    // Initially no fire.
    reg.tick_cron().await.unwrap();
    assert!(sender.calls.lock().await.is_empty());

    // Advance 90 seconds, crosses at least one minute boundary.
    test_clock.advance(Duration::seconds(90));
    reg.tick_cron().await.unwrap();
    let calls = sender.calls.lock().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1, "agent01");
}

#[tokio::test]
async fn cron_fire_increments_fire_count() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    let test_clock = Arc::new(TestClock::new(t0));
    let clock: Arc<dyn Clock> = test_clock.clone();
    seed_agent(&db, "agent01");
    let (reg, _) = make_registry(db.clone(), bus, clock);

    let wake_id = reg.register_cron("agent01", "* * * * *").await.unwrap();
    test_clock.advance(Duration::seconds(90));
    reg.tick_cron().await.unwrap();
    test_clock.advance(Duration::seconds(90));
    reg.tick_cron().await.unwrap();
    let row = db.get_wake_source(&wake_id).unwrap().unwrap();
    assert_eq!(row.fire_count, 2);
}

#[tokio::test]
async fn test_fire_bypasses_schedule_and_marks_via_test() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(t0));
    seed_agent(&db, "agent01");
    let (reg, sender) = make_registry(db.clone(), bus, clock);

    let wake_id = reg.register_cron("agent01", "0 9 * * 1-5").await.unwrap();
    let mail_id = reg.test_fire(&wake_id).await.unwrap();
    assert!(!mail_id.is_empty());
    let calls = sender.calls.lock().await;
    assert_eq!(calls.len(), 1);
}

#[tokio::test]
async fn remove_drops_handle_and_returns_true_then_false() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(Utc::now()));
    seed_agent(&db, "agent01");
    let (reg, _) = make_registry(db.clone(), bus, clock);

    let wake_id = reg.register_cron("agent01", "* * * * *").await.unwrap();
    assert!(reg.remove(&wake_id).await.unwrap());
    assert!(!reg.remove(&wake_id).await.unwrap());
    assert!(db.get_wake_source(&wake_id).unwrap().is_none());
}

#[tokio::test]
async fn retire_for_agent_removes_all_that_agents_sources() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(Utc::now()));
    seed_agent(&db, "agent_a");
    seed_agent(&db, "agent_b");
    let (reg, _) = make_registry(db.clone(), bus, clock);

    let _ = reg.register_cron("agent_a", "* * * * *").await.unwrap();
    let _ = reg.register_cron("agent_a", "0 9 * * *").await.unwrap();
    let _ = reg.register_cron("agent_b", "* * * * *").await.unwrap();

    let n = reg.retire_for_agent("agent_a").await.unwrap();
    assert_eq!(n, 2);
    assert_eq!(reg.list_for_agent("agent_a").await.unwrap().len(), 0);
    assert_eq!(reg.list_for_agent("agent_b").await.unwrap().len(), 1);
}

#[tokio::test]
async fn replay_on_boot_rearms_armed_sources_with_catchup() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(t0));
    seed_agent(&db, "agent01");

    // Pre-seed a row whose last_fired_at is well in the past. After replay
    // it should fire exactly once (catch-up rule).
    let row = grimoire::shared::types::WakeSource {
        id: "wake_replay".into(),
        agent_id: "agent01".into(),
        kind: WakeSourceKind::Cron,
        config_json: r#"{"expr":"* * * * *"}"#.into(),
        state: WakeSourceState::Armed,
        fail_reason: None,
        last_fired_at: Some(t0.timestamp() - 86_400),
        fire_count: 0,
        created_at: t0.timestamp() - 86_400,
    };
    db.insert_wake_source(&row).unwrap();

    let (reg, sender) = make_registry(db.clone(), bus, clock);
    reg.replay_on_boot().await.unwrap();

    let calls = sender.calls.lock().await;
    assert_eq!(calls.len(), 1, "replay should fire exactly one catch-up");
}

#[tokio::test]
async fn replay_with_invalid_cron_marks_failed() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(Utc::now()));
    seed_agent(&db, "agent01");

    let row = grimoire::shared::types::WakeSource {
        id: "wake_bad".into(),
        agent_id: "agent01".into(),
        kind: WakeSourceKind::Cron,
        config_json: r#"{"expr":"definitely-not-cron"}"#.into(),
        state: WakeSourceState::Armed,
        fail_reason: None,
        last_fired_at: None,
        fire_count: 0,
        created_at: 0,
    };
    db.insert_wake_source(&row).unwrap();

    let (reg, _) = make_registry(db.clone(), bus, clock);
    reg.replay_on_boot().await.unwrap();
    let after = db.get_wake_source("wake_bad").unwrap().unwrap();
    assert_eq!(after.state, WakeSourceState::Failed);
}
