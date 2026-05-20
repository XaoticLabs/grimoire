// GREEN tests for worker-pool spec, Task 4: `grimw` binary.
//
// Drives the `grimw` library entry against a fake daemon gRPC server
// (in-process, see `tests/support/grimw_fake_daemon.rs`).

#[path = "support/grimw_fake_daemon.rs"]
mod grimw_fake_daemon;

use std::time::{Duration, Instant};

use tokio::time::timeout;

use grimoire::shared::worker_proto::{
    AssignTask, DaemonMessage, TaskEvent, TaskFinished, TaskRejected, TaskState, daemon_message,
    task_event::EventKind, worker_message,
};

use grimw_fake_daemon::FakeDaemon;

#[tokio::test]
async fn grimw_registers_then_heartbeats() {
    let daemon = FakeDaemon::start_with_provider("claude", "1.2.3").await;
    let worker = grimoire::grimw::test_spawn(&daemon.config_path).await;

    let first = timeout(Duration::from_secs(5), daemon.next_message("register"))
        .await
        .expect("Register within 5s");
    assert!(matches!(
        first.kind,
        Some(worker_message::Kind::Register(_))
    ));

    let _ = timeout(Duration::from_secs(8), daemon.next_message("heartbeat"))
        .await
        .expect("first heartbeat");
    let _ = timeout(Duration::from_secs(8), daemon.next_message("heartbeat"))
        .await
        .expect("second heartbeat");

    worker.shutdown().await;
}

#[tokio::test]
async fn grimw_executes_assign_task_and_streams_events() {
    let daemon = FakeDaemon::start_with_provider("echo", "0.0.1").await;
    let worker = grimoire::grimw::test_spawn(&daemon.config_path).await;
    let _ = daemon.next_message("register").await;

    daemon
        .to_worker
        .send(DaemonMessage {
            kind: Some(daemon_message::Kind::AssignTask(AssignTask {
                agent_id: "a-1".into(),
                task: "/bin/echo grim".into(),
                provider_name: "echo".into(),
                provider_constraint: ">=0".into(),
                cwd: "/tmp".into(),
                env: std::collections::HashMap::new(),
                model: None,
                optional_resume_session_id: None,
            })),
        })
        .await
        .unwrap();

    let accepted = daemon.next_message("task_accepted").await;
    assert!(matches!(
        accepted.kind,
        Some(worker_message::Kind::TaskAccepted(_))
    ));

    let event = daemon.next_message("task_event").await;
    if let Some(worker_message::Kind::TaskEvent(TaskEvent { kind, payload, .. })) = event.kind {
        assert_eq!(kind, EventKind::Stdout as i32);
        assert_eq!(payload, "grim");
    } else {
        panic!("expected TaskEvent")
    }

    let finished = daemon.next_message("task_finished").await;
    if let Some(worker_message::Kind::TaskFinished(TaskFinished {
        state, exit_code, ..
    })) = finished.kind
    {
        assert_eq!(state, TaskState::Complete as i32);
        assert_eq!(exit_code, Some(0));
    } else {
        panic!("expected TaskFinished")
    }

    worker.shutdown().await;
}

#[tokio::test]
async fn grimw_rejects_when_cwd_missing() {
    let daemon = FakeDaemon::start_with_provider("echo", "0.0.1").await;
    let worker = grimoire::grimw::test_spawn(&daemon.config_path).await;
    let _ = daemon.next_message("register").await;

    let started = Instant::now();
    daemon
        .to_worker
        .send(DaemonMessage {
            kind: Some(daemon_message::Kind::AssignTask(AssignTask {
                agent_id: "a-2".into(),
                task: "/bin/true".into(),
                provider_name: "echo".into(),
                provider_constraint: ">=0".into(),
                cwd: "/does/not/exist".into(),
                env: std::collections::HashMap::new(),
                model: None,
                optional_resume_session_id: None,
            })),
        })
        .await
        .unwrap();

    let rejected = daemon.next_message("task_rejected").await;
    assert!(started.elapsed() < Duration::from_secs(2));
    if let Some(worker_message::Kind::TaskRejected(TaskRejected { reason, .. })) = rejected.kind {
        assert_eq!(reason, "cwd_unreachable");
    } else {
        panic!("expected TaskRejected")
    }

    worker.shutdown().await;
}

#[tokio::test]
async fn grimw_rejects_when_provider_version_mismatched() {
    let daemon = FakeDaemon::start_with_provider("claude", "1.0.0").await;
    let worker = grimoire::grimw::test_spawn(&daemon.config_path).await;
    let _ = daemon.next_message("register").await;

    daemon
        .to_worker
        .send(DaemonMessage {
            kind: Some(daemon_message::Kind::AssignTask(AssignTask {
                agent_id: "a-3".into(),
                task: "noop".into(),
                provider_name: "claude".into(),
                provider_constraint: ">=2.0".into(),
                cwd: "/tmp".into(),
                env: std::collections::HashMap::new(),
                model: None,
                optional_resume_session_id: None,
            })),
        })
        .await
        .unwrap();

    let rejected = daemon.next_message("task_rejected").await;
    if let Some(worker_message::Kind::TaskRejected(TaskRejected { reason, .. })) = rejected.kind {
        assert_eq!(reason, "version_mismatch");
    } else {
        panic!("expected TaskRejected")
    }

    worker.shutdown().await;
}

#[tokio::test]
async fn grimw_drains_on_sigterm() {
    let daemon = FakeDaemon::start_with_provider("sleep", "0.0.1").await;
    let worker = grimoire::grimw::test_spawn(&daemon.config_path).await;
    let _ = daemon.next_message("register").await;

    daemon
        .to_worker
        .send(DaemonMessage {
            kind: Some(daemon_message::Kind::AssignTask(AssignTask {
                agent_id: "a-4".into(),
                task: "sleep 1".into(),
                provider_name: "sleep".into(),
                provider_constraint: ">=0".into(),
                cwd: "/tmp".into(),
                env: std::collections::HashMap::new(),
                model: None,
                optional_resume_session_id: None,
            })),
        })
        .await
        .unwrap();

    let _ = daemon.next_message("task_accepted").await;
    worker.send_sigterm().await;

    let finished = timeout(Duration::from_secs(5), daemon.next_message("task_finished"))
        .await
        .expect("TaskFinished delivered before exit");
    assert!(matches!(
        finished.kind,
        Some(worker_message::Kind::TaskFinished(_))
    ));

    // Give the worker a moment to settle after delivering finished.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(worker.has_exited().await, "worker should exit after drain");
}
