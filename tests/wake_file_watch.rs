//! Contract tests for the file-watch wake source (T5). Uses tempfile for
//! filesystem isolation. The notify watcher runs on a background thread so
//! tests sleep past the debounce window before asserting.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use grimoire::daemon::clock::{Clock, TestClock};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::wake_registry::{WakeMailSender, WakeRegistry};
use grimoire::daemon::wake_sources::file_watch::FileWatchConfig;
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

async fn setup() -> (Arc<Database>, Arc<WakeRegistry>, Arc<RecordingSender>) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let clock: Arc<dyn Clock> = Arc::new(TestClock::new(Utc::now()));
    let sender = Arc::new(RecordingSender::default());
    let reg = WakeRegistry::new(db.clone(), bus, clock, sender.clone());
    reg.spawn();
    (db, reg, sender)
}

#[tokio::test]
async fn register_with_missing_root_fails() {
    let (db, reg, _) = setup().await;
    seed_agent(&db, "agent01");
    let cfg = FileWatchConfig {
        globs: vec!["*.rs".into()],
        ignore: vec![],
        root: PathBuf::from("/no/such/path/zzz/9999"),
    };
    let res = reg.register_file_watch("agent01", cfg).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("cwd_gone"));
}

#[tokio::test]
async fn single_file_change_fires_after_debounce() {
    let (db, reg, sender) = setup().await;
    seed_agent(&db, "agent01");
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    let cfg = FileWatchConfig {
        globs: vec!["**/*.rs".into()],
        ignore: vec![],
        root: dir.path().to_path_buf(),
    };
    let _wake_id = reg.register_file_watch("agent01", cfg).await.unwrap();

    // Settle: notify watcher needs a moment to attach.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let target = dir.path().join("src/touched.rs");
    std::fs::write(&target, "// hello\n").unwrap();
    // Wait past debounce + drain.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let calls = sender.calls.lock().await;
    assert!(!calls.is_empty(), "expected at least one fire");
}

#[tokio::test]
async fn rapid_changes_coalesce_to_one_fire() {
    let (db, reg, sender) = setup().await;
    seed_agent(&db, "agent01");
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    let cfg = FileWatchConfig {
        globs: vec!["**/*.rs".into()],
        ignore: vec![],
        root: dir.path().to_path_buf(),
    };
    let _ = reg.register_file_watch("agent01", cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let target = dir.path().join("src/x.rs");
    for i in 0..10 {
        std::fs::write(&target, format!("// {}\n", i)).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let calls = sender.calls.lock().await;
    // Coalesce: at most a small number of fires (debounce window). Allow 1-2
    // because some platforms emit a final touch event after the window.
    assert!(
        calls.len() <= 2,
        "expected coalesced fires, got {}",
        calls.len()
    );
    assert!(!calls.is_empty());
}

#[tokio::test]
async fn ignore_glob_excludes_match() {
    let (db, reg, sender) = setup().await;
    seed_agent(&db, "agent01");
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("target")).unwrap();
    std::fs::create_dir_all(dir.path().join("src")).unwrap();
    let cfg = FileWatchConfig {
        globs: vec!["**/*.rs".into()],
        ignore: vec!["target/**".into()],
        root: dir.path().to_path_buf(),
    };
    let _ = reg.register_file_watch("agent01", cfg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    std::fs::write(dir.path().join("target/build.rs"), "// noise\n").unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        sender.calls.lock().await.is_empty(),
        "ignored path must not fire"
    );

    std::fs::write(dir.path().join("src/x.rs"), "// real\n").unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!sender.calls.lock().await.is_empty());
}
