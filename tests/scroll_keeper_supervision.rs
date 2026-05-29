//! Contract tests for scroll-keeper Restarting handling.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use tokio::time::Duration;

use grimoire::daemon::agent_manager::AgentManager;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{
    Agent, AgentState, RestartPolicy, Scroll, ScrollState, SupervisionConfig, Task, TaskState,
};

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
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
}

fn seed_scroll_with_task(db: &Database, scroll_id: &str, task_id: &str, agent_id: &str) {
    let scroll = Scroll {
        id: scroll_id.into(),
        name: "S".into(),
        state: ScrollState::Active,
        source_path: None,
        max_concurrency: 4,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    db.insert_scroll(&scroll).unwrap();
    let task = Task {
        id: task_id.into(),
        scroll_id: scroll_id.into(),
        name: "task1".into(),
        prompt: "do".into(),
        state: TaskState::Active,
        agent_id: Some(agent_id.into()),
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec![],
        order_index: 0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        peer_name: None,
    };
    db.insert_task(&task).unwrap();
    db.update_task_agent(task_id, agent_id).unwrap();
}

#[tokio::test]
async fn restarting_state_does_not_fire_handlers() {
    // Subscribe ScrollKeeper to bus, publish StateChange{Active→Restarting},
    // assert task state stays Active.
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;
    let sk = Arc::new(ScrollKeeper::new(db.clone(), manager));
    sk.clone().start(&bus);

    seed_agent(&db, "skp00001", AgentState::Restarting);
    seed_scroll_with_task(&db, "sk000001", "tk000001", "skp00001");

    bus.publish(StreamEvent::StateChange {
        agent_id: "skp00001".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Restarting,
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let task = db.get_task_by_agent_id("skp00001").unwrap().unwrap();
    assert_eq!(task.state, TaskState::Active);
}

#[tokio::test]
async fn restart_success_fires_completion_handler_once() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;
    let sk = Arc::new(ScrollKeeper::new(db.clone(), manager));
    sk.clone().start(&bus);

    seed_agent(&db, "skp00002", AgentState::Active);
    seed_scroll_with_task(&db, "sk000002", "tk000002", "skp00002");

    // Sequence
    bus.publish(StreamEvent::StateChange {
        agent_id: "skp00002".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Failed,
    });
    bus.publish(StreamEvent::StateChange {
        agent_id: "skp00002".into(),
        old_state: AgentState::Failed,
        new_state: AgentState::Restarting,
    });
    bus.publish(StreamEvent::StateChange {
        agent_id: "skp00002".into(),
        old_state: AgentState::Restarting,
        new_state: AgentState::Active,
    });
    bus.publish(StreamEvent::StateChange {
        agent_id: "skp00002".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    let task = db.get_task_by_agent_id("skp00002").unwrap().unwrap();
    assert_eq!(task.state, TaskState::Complete);
}

#[tokio::test]
async fn dependent_task_blocked_during_restart() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;
    let sk = Arc::new(ScrollKeeper::new(db.clone(), manager));
    sk.clone().start(&bus);

    seed_agent(&db, "skp00003", AgentState::Restarting);
    let scroll = Scroll {
        id: "sk000003".into(),
        name: "S".into(),
        state: ScrollState::Active,
        source_path: None,
        max_concurrency: 4,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    db.insert_scroll(&scroll).unwrap();
    let parent = Task {
        id: "tkparent".into(),
        scroll_id: "sk000003".into(),
        name: "parent".into(),
        prompt: "p".into(),
        state: TaskState::Active,
        agent_id: Some("skp00003".into()),
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec![],
        order_index: 0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        peer_name: None,
    };
    let child = Task {
        id: "tkchild0".into(),
        scroll_id: "sk000003".into(),
        name: "child".into(),
        prompt: "c".into(),
        state: TaskState::Blocked,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec![],
        order_index: 1,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        peer_name: None,
    };
    db.insert_task(&parent).unwrap();
    db.insert_task(&child).unwrap();
    db.insert_task_dependency("tkchild0", "tkparent").unwrap();
    db.update_task_agent("tkparent", "skp00003").unwrap();

    bus.publish(StreamEvent::StateChange {
        agent_id: "skp00003".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Restarting,
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let c = db
        .get_tasks_for_scroll("sk000003")
        .unwrap()
        .into_iter()
        .find(|t| t.id == "tkchild0")
        .unwrap();
    assert_eq!(c.state, TaskState::Blocked);
}

#[tokio::test]
async fn budget_exhausted_fires_failure_handler_once() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;
    let sk = Arc::new(ScrollKeeper::new(db.clone(), manager));
    sk.clone().start(&bus);

    seed_agent(&db, "skp00004", AgentState::Failed);
    // Make sure no supervision so handle_agent_failure isn't deferred.
    db.set_supervision(
        "skp00004",
        &SupervisionConfig {
            policy: RestartPolicy::Never,
            max_restarts: None,
            window_secs: None,
            escalate_to: None,
        },
    )
    .unwrap();
    seed_scroll_with_task(&db, "sk000004", "tk000004", "skp00004");

    bus.publish(StreamEvent::StateChange {
        agent_id: "skp00004".into(),
        old_state: AgentState::Active,
        new_state: AgentState::Failed,
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let task = db.get_task_by_agent_id("skp00004").unwrap().unwrap();
    assert_eq!(task.state, TaskState::Failed);
}
