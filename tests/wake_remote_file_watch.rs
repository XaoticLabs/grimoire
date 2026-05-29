//! F4a: contract tests for the `RemoteFileWatch` wake source.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use grimoire::daemon::clock::{Clock, TestClock};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::wake_registry::{WakeMailSender, WakeRegistry};
use grimoire::daemon::wake_sources::remote_file_watch::RemoteFileWatchConfig;
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
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
}

#[tokio::test]
async fn fires_when_matched_path_published_for_target_workspace() {
    let (db, bus, reg, sender) = setup().await;
    seed_agent(&db, "child001");

    let _ = reg
        .register_remote_file_watch(
            "child001",
            RemoteFileWatchConfig {
                workspace_id: "shadow-frontend".into(),
                globs: vec!["src/**/*.rs".into()],
                ignore: vec!["target/**".into()],
            },
        )
        .await
        .unwrap();
    drain().await;

    bus.publish(StreamEvent::WorkspaceFileChanged {
        workspace_id: "shadow-frontend".into(),
        paths: vec!["src/main.rs".into(), "target/build.rs".into()],
        kinds: vec!["modify".into(), "modify".into()],
        truncated_count: 0,
    });
    drain().await;

    let calls = sender.calls.lock().await;
    assert_eq!(calls.len(), 1, "exactly one fire for the matched batch");
    assert_eq!(calls[0].1, "child001");
}

#[tokio::test]
async fn ignores_events_for_other_workspaces() {
    let (db, bus, reg, sender) = setup().await;
    seed_agent(&db, "child001");
    let _ = reg
        .register_remote_file_watch(
            "child001",
            RemoteFileWatchConfig {
                workspace_id: "shadow-frontend".into(),
                globs: vec!["**/*.rs".into()],
                ignore: vec![],
            },
        )
        .await
        .unwrap();
    drain().await;

    bus.publish(StreamEvent::WorkspaceFileChanged {
        workspace_id: "shadow-backend".into(),
        paths: vec!["src/main.rs".into()],
        kinds: vec!["modify".into()],
        truncated_count: 0,
    });
    drain().await;

    assert!(sender.calls.lock().await.is_empty());
}

#[tokio::test]
async fn ignore_globs_suppress_fire() {
    let (db, bus, reg, sender) = setup().await;
    seed_agent(&db, "child001");
    let _ = reg
        .register_remote_file_watch(
            "child001",
            RemoteFileWatchConfig {
                workspace_id: "shadow-frontend".into(),
                globs: vec!["**/*.rs".into()],
                ignore: vec!["target/**".into()],
            },
        )
        .await
        .unwrap();
    drain().await;

    bus.publish(StreamEvent::WorkspaceFileChanged {
        workspace_id: "shadow-frontend".into(),
        paths: vec!["target/build.rs".into()],
        kinds: vec!["modify".into()],
        truncated_count: 0,
    });
    drain().await;

    assert!(sender.calls.lock().await.is_empty());
}
