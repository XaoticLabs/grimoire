//! Task-level retries: `- retries: N` gives up to N re-spawns (fresh agent
//! each) on worker failure before the task fails for good. Driven through the
//! keeper's event-bus path; no scheduler, so re-spawns enqueue without dispatch.

use std::sync::Arc;
use std::time::Duration;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::scroll_parser;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{AgentState, TaskState};

async fn setup() -> (Arc<Database>, Arc<ScrollKeeper>, EventBus) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let event_bus = EventBus::new(db.clone());
    let manager = grimoire::daemon::agent_manager::AgentManager::new(
        db.clone(),
        event_bus.clone(),
        Config::default(),
    )
    .await;
    let keeper = Arc::new(ScrollKeeper::new(db.clone(), manager));
    keeper.clone().start(&event_bus);
    (db, keeper, event_bus)
}

/// Fail the agent currently attached to a task, the way a dying worker
/// would, and let the keeper react.
async fn fail_current_agent(db: &Database, bus: &EventBus, task_id: &str) -> String {
    let task = db.get_task(task_id).unwrap().unwrap();
    let agent_id = task.agent_id.expect("task has an agent");
    bus.publish(StreamEvent::StateChange {
        agent_id: agent_id.clone(),
        old_state: AgentState::Active,
        new_state: AgentState::Failed,
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    agent_id
}

#[tokio::test]
async fn task_retries_then_fails_when_budget_exhausted() {
    let (db, keeper, bus) = setup().await;
    let spec = scroll_parser::parse_scroll(
        "# Scroll: Retried\n\n## Task: Flaky\n- retries: 2\n\nDo flaky work.\n",
    )
    .unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();
    let scroll_id = result.scroll.id.clone();
    let task = db.get_tasks_for_scroll(&scroll_id).unwrap()[0].clone();

    keeper.activate(&scroll_id).await.unwrap();
    let a0 = db.get_task(&task.id).unwrap().unwrap().agent_id.unwrap();

    // first failure → retry 1, fresh agent, still Active
    let failed0 = fail_current_agent(&db, &bus, &task.id).await;
    assert_eq!(failed0, a0);
    let t1 = db.get_task(&task.id).unwrap().unwrap();
    assert_eq!(t1.state, TaskState::Active, "should have re-spawned");
    let a1 = t1.agent_id.clone().unwrap();
    assert_ne!(a1, a0, "retry uses a fresh agent");
    assert_eq!(db.get_task_retry(&task.id).unwrap(), (2, 1));

    // second failure → retry 2
    fail_current_agent(&db, &bus, &task.id).await;
    let t2 = db.get_task(&task.id).unwrap().unwrap();
    assert_eq!(t2.state, TaskState::Active);
    assert_eq!(db.get_task_retry(&task.id).unwrap(), (2, 2));

    // third failure → budget exhausted → task and scroll fail
    fail_current_agent(&db, &bus, &task.id).await;
    let t3 = db.get_task(&task.id).unwrap().unwrap();
    assert_eq!(t3.state, TaskState::Failed);
    let scroll = db.get_scroll(&scroll_id).unwrap().unwrap();
    assert_eq!(scroll.state, grimoire::shared::types::ScrollState::Failed);
}

#[tokio::test]
async fn task_without_retries_fails_on_first_failure() {
    let (db, keeper, bus) = setup().await;
    let spec =
        scroll_parser::parse_scroll("# Scroll: NoRetry\n\n## Task: Once\n\nDo it once.\n").unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();
    let scroll_id = result.scroll.id.clone();
    let task = db.get_tasks_for_scroll(&scroll_id).unwrap()[0].clone();

    keeper.activate(&scroll_id).await.unwrap();
    fail_current_agent(&db, &bus, &task.id).await;
    assert_eq!(
        db.get_task(&task.id).unwrap().unwrap().state,
        TaskState::Failed
    );
}
