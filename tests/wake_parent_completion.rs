//! Contract tests for the parent-completion wake source.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use grimoire::daemon::clock::{Clock, TestClock};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::wake_registry::{WakeMailSender, WakeRegistry};
use grimoire::daemon::wake_sources::parent_completion::ParentCompletionConfig;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{Agent, AgentState};
use tokio::sync::Mutex;

#[derive(Default)]
struct RecordingSender {
    calls: Mutex<Vec<(String, String, String)>>,
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

async fn setup() -> (
    Arc<Database>,
    EventBus,
    Arc<WakeRegistry>,
    Arc<RecordingSender>,
) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(Utc::now()));
    let sender = Arc::new(RecordingSender::default());
    let reg = WakeRegistry::new(db.clone(), bus.clone(), clock, sender.clone());
    reg.spawn();
    (db, bus, reg, sender)
}

async fn drain() {
    // let the subscriber receive the event and the drain loop process the FireMsg
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
}

#[tokio::test]
async fn fires_on_complete_transition() {
    let (db, bus, reg, sender) = setup().await;
    seed_agent(&db, "child001");
    seed_agent(&db, "parent01");

    let _wake_id = reg
        .register_parent_completion(
            "child001",
            ParentCompletionConfig {
                parent_id: "parent01".into(),
                states: vec![],
            },
        )
        .await
        .unwrap();
    drain().await;

    bus.publish(StreamEvent::StateChange {
        agent_id: "parent01".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });
    drain().await;

    let calls = sender.calls.lock().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1, "child001");
}

#[tokio::test]
async fn does_not_fire_on_non_target_state() {
    let (db, bus, reg, sender) = setup().await;
    seed_agent(&db, "child001");
    seed_agent(&db, "parent01");
    let _ = reg
        .register_parent_completion(
            "child001",
            ParentCompletionConfig {
                parent_id: "parent01".into(),
                states: vec![],
            },
        )
        .await
        .unwrap();
    drain().await;
    bus.publish(StreamEvent::StateChange {
        agent_id: "parent01".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Failed,
    });
    drain().await;
    assert!(sender.calls.lock().await.is_empty());
}

#[tokio::test]
async fn multi_state_filter_fires_for_each_match() {
    let (db, bus, reg, sender) = setup().await;
    seed_agent(&db, "child001");
    seed_agent(&db, "parent01");
    let _ = reg
        .register_parent_completion(
            "child001",
            ParentCompletionConfig {
                parent_id: "parent01".into(),
                states: vec![AgentState::Complete, AgentState::Failed],
            },
        )
        .await
        .unwrap();
    drain().await;

    bus.publish(StreamEvent::StateChange {
        agent_id: "parent01".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });
    bus.publish(StreamEvent::StateChange {
        agent_id: "parent01".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Failed,
    });
    drain().await;
    assert_eq!(sender.calls.lock().await.len(), 2);
}

#[tokio::test]
async fn does_not_fire_for_other_agents() {
    let (db, bus, reg, sender) = setup().await;
    seed_agent(&db, "child001");
    seed_agent(&db, "parent01");
    seed_agent(&db, "stranger");
    let _ = reg
        .register_parent_completion(
            "child001",
            ParentCompletionConfig {
                parent_id: "parent01".into(),
                states: vec![],
            },
        )
        .await
        .unwrap();
    drain().await;
    bus.publish(StreamEvent::StateChange {
        agent_id: "stranger".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });
    drain().await;
    assert!(sender.calls.lock().await.is_empty());
}
