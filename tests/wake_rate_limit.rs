//! Contract tests for the per-agent token-bucket rate limiter on the wake
//! fire path.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{Duration, TimeZone, Utc};
use grimoire::daemon::clock::{Clock, TestClock};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::wake_registry::{WakeMailSender, WakeRegistry};
use grimoire::shared::types::{Agent, AgentState};
use tokio::sync::Mutex;

#[derive(Default)]
struct RecordingSender {
    calls: Mutex<Vec<String>>, // mail bodies
}

#[async_trait]
impl WakeMailSender for RecordingSender {
    async fn send_wake_mail(&self, _wake_id: &str, _agent_id: &str, body: &str) -> Result<String> {
        let mut g = self.calls.lock().await;
        let id = format!("mail{:04}", g.len());
        g.push(body.to_string());
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

async fn setup_with_capacity(
    cap: i64,
    refill_per_sec: f64,
) -> (
    Arc<Database>,
    Arc<TestClock>,
    Arc<WakeRegistry>,
    Arc<RecordingSender>,
    String,
) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(t0));
    let clock_dyn: Arc<dyn Clock> = clock.clone();
    let sender = Arc::new(RecordingSender::default());
    let reg = WakeRegistry::new(db.clone(), bus, clock_dyn, sender.clone());
    seed_agent(&db, "agent01");
    let wake_id = reg.register_cron("agent01", "* * * * *").await.unwrap();
    db.set_rate_limit_capacity("agent01", cap, refill_per_sec, t0.timestamp())
        .unwrap();
    (db, clock, reg, sender, wake_id)
}

#[tokio::test]
async fn first_fire_for_new_agent_allowed() {
    let (_db, _clock, reg, sender, wake_id) = setup_with_capacity(60, 60.0 / 3600.0).await;
    reg.fire(&wake_id, "hi", None).await.unwrap();
    assert_eq!(sender.calls.lock().await.len(), 1);
}

#[tokio::test]
async fn bucket_exhaustion_denies_with_rate_limited() {
    // capacity=2, refill=0 (no recovery), so the third fire is denied
    let (_db, _clock, reg, sender, wake_id) = setup_with_capacity(2, 0.0).await;
    reg.fire(&wake_id, "1", None).await.unwrap();
    reg.fire(&wake_id, "2", None).await.unwrap();
    let third = reg.fire(&wake_id, "3", None).await;
    assert!(third.is_err());
    assert_eq!(third.unwrap_err().to_string(), "rate_limited");
    assert_eq!(sender.calls.lock().await.len(), 2);
}

#[tokio::test]
async fn refill_restores_capacity_over_time() {
    let (_db, clock, reg, sender, wake_id) = setup_with_capacity(2, 1.0).await; // 1 token/sec
    reg.fire(&wake_id, "1", None).await.unwrap();
    reg.fire(&wake_id, "2", None).await.unwrap();
    // bucket now empty; refill restores one token
    clock.advance(Duration::seconds(2));
    reg.fire(&wake_id, "3", None).await.unwrap();
    assert_eq!(sender.calls.lock().await.len(), 3);
}

#[tokio::test]
async fn test_fire_bypasses_rate_limit() {
    let (_db, _clock, reg, sender, wake_id) = setup_with_capacity(0, 0.0).await;
    let res = reg.test_fire(&wake_id).await;
    assert!(res.is_ok(), "test_fire must bypass rate limit: {res:?}");
    assert_eq!(sender.calls.lock().await.len(), 1);
}

#[tokio::test]
async fn capacity_zero_denies_all_regular_fires() {
    let (_db, _clock, reg, sender, wake_id) = setup_with_capacity(0, 0.0).await;
    let res = reg.fire(&wake_id, "x", None).await;
    assert!(res.is_err());
    assert!(sender.calls.lock().await.is_empty());
}

#[tokio::test]
async fn clock_skew_does_not_add_tokens() {
    let (_db, clock, reg, sender, wake_id) = setup_with_capacity(2, 1.0).await;
    reg.fire(&wake_id, "1", None).await.unwrap();
    reg.fire(&wake_id, "2", None).await.unwrap();
    clock.advance(Duration::seconds(-3600)); // clock skew backward
    let res = reg.fire(&wake_id, "3", None).await;
    assert!(res.is_err(), "clock skew must not mint tokens");
    assert_eq!(sender.calls.lock().await.len(), 2);
}

#[tokio::test]
async fn fire_count_increments_on_rate_limit() {
    let (db, _clock, reg, _sender, wake_id) = setup_with_capacity(0, 0.0).await;
    let _ = reg.fire(&wake_id, "x", None).await;
    let _ = reg.fire(&wake_id, "x", None).await;
    let _ = reg.fire(&wake_id, "x", None).await;
    let row = db.get_wake_source(&wake_id).unwrap().unwrap();
    assert_eq!(
        row.fire_count, 3,
        "denied fires still count for observability"
    );
}
