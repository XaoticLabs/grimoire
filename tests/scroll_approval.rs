//! HITL approval gates on scroll tasks. A `- approve: true` task is held in
//! `AwaitingApproval` once deps are met; `approve_task` lets the DAG schedule
//! it, `reject_task` fails it and skips downstream. No scheduler runs here, so
//! an approved task reaches Active with an agent assigned but is never dispatched.

use std::sync::Arc;
use std::time::Duration;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::scroll_parser;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{AgentState, ApprovalState, TaskState};

async fn setup() -> (Arc<Database>, Arc<ScrollKeeper>, EventBus) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let event_bus = EventBus::new(db.clone());
    let config = Config::default();
    let manager =
        grimoire::daemon::agent_manager::AgentManager::new(db.clone(), event_bus.clone(), config)
            .await;
    let keeper = Arc::new(ScrollKeeper::new(db.clone(), manager));
    keeper.clone().start(&event_bus); // production completion path

    (db, keeper, event_bus)
}

const GATED_SPEC: &str = r"# Scroll: Gated Pipeline

## Task: Build

Build the thing.

## Task: Deploy
- depends: Build
- approve: true

Deploy the thing.
";

/// Drive a worker completion by publishing the StateChange the keeper
/// subscribes to, using the agent id assigned at enqueue time.
async fn complete_task(db: &Database, bus: &EventBus, task_id: &str) {
    let task = db.get_task(task_id).unwrap().unwrap();
    let agent_id = task.agent_id.expect("task has an enqueued agent");
    bus.publish(StreamEvent::StateChange {
        agent_id,
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn gated_task_is_held_for_approval() {
    let (db, keeper, bus) = setup().await;
    let mut rx = bus.subscribe();

    let spec = scroll_parser::parse_scroll(GATED_SPEC).unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();
    let scroll_id = result.scroll.id.clone();

    let tasks = db.get_tasks_for_scroll(&scroll_id).unwrap();
    let build = tasks.iter().find(|t| t.name == "Build").unwrap().clone();
    let deploy = tasks.iter().find(|t| t.name == "Deploy").unwrap().clone();

    // gate directive landed on Deploy only
    assert!(!db.get_task_approval(&build.id).unwrap().0);
    assert!(db.get_task_approval(&deploy.id).unwrap().0);

    keeper.activate(&scroll_id).await.unwrap();

    assert_eq!(
        db.get_task(&deploy.id).unwrap().unwrap().state,
        TaskState::Blocked
    );

    // complete Build → keeper schedules Deploy, which is gated and held
    complete_task(&db, &bus, &build.id).await;

    let deploy_held = db.get_task(&deploy.id).unwrap().unwrap();
    assert_eq!(
        deploy_held.state,
        TaskState::AwaitingApproval,
        "gated downstream task must be held, not spawned"
    );
    assert!(deploy_held.agent_id.is_none(), "no agent before approval");
    assert_eq!(
        db.get_task_approval(&deploy.id).unwrap().1,
        ApprovalState::Pending
    );

    let mut saw_notification = false;
    while let Ok(ev) = rx.try_recv() {
        if let StreamEvent::Notification { message, .. } = ev
            && message.contains("approval required")
        {
            saw_notification = true;
        }
    }
    assert!(
        saw_notification,
        "expected an approval-required notification"
    );
}

#[tokio::test]
async fn approve_lets_the_task_run() {
    let (db, keeper, bus) = setup().await;
    let spec = scroll_parser::parse_scroll(GATED_SPEC).unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();
    let scroll_id = result.scroll.id.clone();
    let tasks = db.get_tasks_for_scroll(&scroll_id).unwrap();
    let build = tasks.iter().find(|t| t.name == "Build").unwrap().clone();
    let deploy = tasks.iter().find(|t| t.name == "Deploy").unwrap().clone();

    keeper.activate(&scroll_id).await.unwrap();
    complete_task(&db, &bus, &build.id).await;
    assert_eq!(
        db.get_task(&deploy.id).unwrap().unwrap().state,
        TaskState::AwaitingApproval
    );

    // approve → Active with an agent assigned (never dispatched)
    let name = keeper.approve_task(&scroll_id, "Deploy").await.unwrap();
    assert_eq!(name, "Deploy");
    let deploy_after = db.get_task(&deploy.id).unwrap().unwrap();
    assert_eq!(deploy_after.state, TaskState::Active);
    assert!(deploy_after.agent_id.is_some());
    assert_eq!(
        db.get_task_approval(&deploy.id).unwrap().1,
        ApprovalState::Approved
    );
}

#[tokio::test]
async fn reject_fails_the_task_and_downstream() {
    let (db, keeper, bus) = setup().await;
    let spec_text = r"# Scroll: Gated With Tail

## Task: Build

Build.

## Task: Deploy
- depends: Build
- approve: true

Deploy.

## Task: Verify
- depends: Deploy

Verify.
";
    let spec = scroll_parser::parse_scroll(spec_text).unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();
    let scroll_id = result.scroll.id.clone();
    let tasks = db.get_tasks_for_scroll(&scroll_id).unwrap();
    let build = tasks.iter().find(|t| t.name == "Build").unwrap().clone();
    let deploy = tasks.iter().find(|t| t.name == "Deploy").unwrap().clone();
    let verify = tasks.iter().find(|t| t.name == "Verify").unwrap().clone();

    keeper.activate(&scroll_id).await.unwrap();
    complete_task(&db, &bus, &build.id).await;
    assert_eq!(
        db.get_task(&deploy.id).unwrap().unwrap().state,
        TaskState::AwaitingApproval
    );

    let name = keeper.reject_task(&scroll_id, "Deploy").await.unwrap();
    assert_eq!(name, "Deploy");
    assert_eq!(
        db.get_task(&deploy.id).unwrap().unwrap().state,
        TaskState::Failed
    );
    assert_eq!(
        db.get_task(&verify.id).unwrap().unwrap().state,
        TaskState::Skipped
    );
}
