// RED tests for worker-pool spec, Task 6: `RemoteExecutor`.
//
// References `RemoteExecutor` and routing-map APIs not yet implemented.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::timeout;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::executor::{ExecuteRequest, Executor, RemoteExecutor, RoutedInbound};
use grimoire::daemon::persistence::Database;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{Agent, AgentState};
use grimoire::shared::worker_proto::{
    daemon_message, task_event::EventKind, AssignTask, CancelTask, DaemonMessage, TaskEvent,
    TaskFinished, TaskState,
};


fn seed_agent(db: &Database, id: &str) {
    let now = chrono::Utc::now();
    db.insert_agent(&Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Active,
        task: Some("noop".to_string()),
        model: None,
        provider: Some("echo".to_string()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: now,
        updated_at: now,
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    })
    .unwrap();
}

fn build_request(agent_id: &str) -> ExecuteRequest {
    ExecuteRequest {
        agent_id: agent_id.to_string(),
        task: "echo hi".to_string(),
        provider_name: "echo".to_string(),
        cwd: PathBuf::from("/tmp"),
        model: None,
        resume_session_id: None,
    }
}

#[tokio::test]
async fn remote_executor_sends_assign_task_to_worker_channel() {
    let (assign_tx, mut assign_rx) = mpsc::channel::<DaemonMessage>(8);
    let (inbound_tx, inbound_rx) = mpsc::channel::<RoutedInbound>(8);
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());

    let exec = RemoteExecutor::for_test(
        "w-1".to_string(),
        assign_tx,
        inbound_rx,
        bus.clone(),
        db.clone(),
    );
    let handle = exec.start(build_request("a-1")).await.unwrap();
    assert_eq!(handle.worker_id.as_deref(), Some("w-1"));

    let msg = timeout(Duration::from_millis(100), assign_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match msg.kind {
        Some(daemon_message::Kind::AssignTask(AssignTask {
            agent_id, task, ..
        })) => {
            assert_eq!(agent_id, "a-1");
            assert_eq!(task, "echo hi");
        }
        _ => panic!("expected AssignTask"),
    }
    drop(inbound_tx);
}

#[tokio::test]
async fn remote_executor_streams_task_events_into_event_bus() {
    let (assign_tx, _assign_rx) = mpsc::channel::<DaemonMessage>(8);
    let (inbound_tx, inbound_rx) = mpsc::channel::<RoutedInbound>(8);
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let mut sub = bus.subscribe();

    seed_agent(&db, "a-2");
    let exec = RemoteExecutor::for_test(
        "w-1".to_string(),
        assign_tx,
        inbound_rx,
        bus.clone(),
        db.clone(),
    );
    let _handle = exec.start(build_request("a-2")).await.unwrap();

    for line in ["one", "two", "three"] {
        inbound_tx
            .send(RoutedInbound::Event(TaskEvent {
                agent_id: "a-2".to_string(),
                kind: EventKind::Stdout as i32,
                payload: line.to_string(),
            }))
            .await
            .unwrap();
    }

    // Three rows persisted, three events broadcast.
    let mut received = 0;
    while received < 3 {
        match timeout(Duration::from_secs(1), sub.recv()).await {
            Ok(Ok(StreamEvent::Output { agent_id, .. })) if agent_id == "a-2" => {
                received += 1;
            }
            Ok(Ok(_)) => {}
            _ => panic!("did not receive 3 stdout events"),
        }
    }
    let rows = db.get_events("a-2", None).unwrap();
    assert_eq!(rows.len(), 3);
}

#[tokio::test]
async fn remote_executor_completion_resolves_on_task_finished() {
    let (assign_tx, _assign_rx) = mpsc::channel::<DaemonMessage>(8);
    let (inbound_tx, inbound_rx) = mpsc::channel::<RoutedInbound>(8);
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());

    let exec = RemoteExecutor::for_test(
        "w-1".to_string(),
        assign_tx,
        inbound_rx,
        bus,
        db,
    );
    let handle = exec.start(build_request("a-3")).await.unwrap();

    inbound_tx
        .send(RoutedInbound::Finished(TaskFinished {
            agent_id: "a-3".into(),
            state: TaskState::Complete as i32,
            exit_code: Some(0),
            session_id: Some("s-1".into()),
            error_reason: None,
        }))
        .await
        .unwrap();

    let result = timeout(Duration::from_secs(1), handle.completion)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.state, AgentState::Complete);
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.session_id.as_deref(), Some("s-1"));
}

#[tokio::test]
async fn remote_executor_cancel_sends_cancel_task() {
    let (assign_tx, mut assign_rx) = mpsc::channel::<DaemonMessage>(8);
    let (_inbound_tx, inbound_rx) = mpsc::channel::<RoutedInbound>(8);
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());

    let exec = RemoteExecutor::for_test(
        "w-1".to_string(),
        assign_tx,
        inbound_rx,
        bus,
        db,
    );
    let handle = exec.start(build_request("a-4")).await.unwrap();
    let _ = assign_rx.recv().await; // AssignTask

    (handle.cancel)();

    let msg = timeout(Duration::from_millis(200), assign_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match msg.kind {
        Some(daemon_message::Kind::CancelTask(CancelTask { agent_id })) => {
            assert_eq!(agent_id, "a-4");
        }
        _ => panic!("expected CancelTask"),
    }
}

#[tokio::test]
async fn remote_executor_worker_lost_resolves_failed() {
    let (assign_tx, _assign_rx) = mpsc::channel::<DaemonMessage>(8);
    let (inbound_tx, inbound_rx) = mpsc::channel::<RoutedInbound>(8);
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());

    let exec = RemoteExecutor::for_test(
        "w-1".to_string(),
        assign_tx,
        inbound_rx,
        bus,
        db,
    );
    let handle = exec.start(build_request("a-5")).await.unwrap();

    // Simulate eviction by dropping the inbound side; the executor must
    // synthesize TaskFinished{FAILED, "worker_lost"}.
    drop(inbound_tx);

    let result = timeout(Duration::from_secs(1), handle.completion)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.state, AgentState::Failed);
    assert_eq!(
        result.error_reason.as_deref(),
        Some("worker_lost"),
        "executor must mark the agent as worker_lost on eviction"
    );
}
