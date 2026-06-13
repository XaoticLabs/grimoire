//! Contract tests for `agent_manager::enqueue` / `dispatch_internal`.
//!
//! `summon` is split into two halves: `enqueue` writes the agent +
//! `task_queue` row in `Queued` state and returns immediately, and
//! `dispatch_internal` (called via the `Dispatcher` trait by the scheduler)
//! drives `executor.start` and the `Summoning -> Active` transition.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use grimoire::daemon::agent_manager::{AgentManager, Lane};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::executor::{ExecuteRequest, Executor, ExecutorHandle};
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scheduler::Dispatcher;
use grimoire::shared::config::Config;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::AgentState;

// --- Fakes ----------------------------------------------------------------

#[derive(Default)]
struct ExecutorLog {
    calls: Mutex<Vec<ExecuteRequest>>,
    fail_next: Mutex<bool>,
}

struct MockExecutor {
    log: Arc<ExecutorLog>,
}

#[async_trait]
impl Executor for MockExecutor {
    async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle> {
        if std::mem::replace(&mut *self.log.fail_next.lock().unwrap(), false) {
            return Err(anyhow::anyhow!("forced executor failure"));
        }
        self.log.calls.lock().unwrap().push(req);
        let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let completion = tokio::spawn(async {
            grimoire::daemon::process_manager::MonitorResult {
                state: AgentState::Complete,
                exit_code: Some(0),
                ..Default::default()
            }
        });
        Ok(ExecutorHandle {
            worker_id: None,
            pid: Some(0),
            cancel: Box::new(move || {
                let _ = cancel_tx.send(());
            }),
            completion,
        })
    }

    fn name(&self) -> &'static str {
        "mock"
    }
}

async fn fresh_manager() -> (Arc<Database>, EventBus, Arc<AgentManager>, Arc<ExecutorLog>) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let log = Arc::new(ExecutorLog::default());
    let executor: Arc<dyn Executor> = Arc::new(MockExecutor { log: log.clone() });
    let manager =
        AgentManager::new_with_executor(db.clone(), bus.clone(), Config::default(), executor).await;
    (db, bus, manager, log)
}

// --- enqueue contract -----------------------------------------------------

#[tokio::test]
async fn enqueue_returns_agent_in_queued_state() {
    let (_, _, manager, _) = fresh_manager().await;

    let agent = manager
        .enqueue("echo hi", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .expect("enqueue should succeed");

    assert_eq!(
        agent.state,
        AgentState::Queued,
        "enqueue must return state=Queued"
    );
    assert!(agent.pid.is_none(), "queued agents have no pid");
}

#[tokio::test]
async fn enqueue_inserts_into_both_agents_and_task_queue() {
    let (db, _, manager, _) = fresh_manager().await;

    let agent = manager
        .enqueue("ping", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    let stored = db.get_agent(&agent.id).unwrap().expect("agent in DB");
    assert_eq!(stored.state, AgentState::Queued);

    let queue = db.list_queue().unwrap();
    assert_eq!(queue.len(), 1, "row should exist in task_queue");
    assert_eq!(queue[0].id, agent.id);
    assert_eq!(queue[0].lane, "adhoc");
    assert_eq!(queue[0].task_text, "ping");
}

#[tokio::test]
async fn enqueue_publishes_agent_queued_event() {
    let (_, bus, manager, _) = fresh_manager().await;
    let mut rx = bus.subscribe();

    let agent = manager
        .enqueue("hello", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    // Drain events until we see AgentQueued for this id.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut saw_event = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Ok(StreamEvent::AgentQueued { agent_id, lane, .. })) if agent_id == agent.id => {
                assert_eq!(lane, "adhoc");
                saw_event = true;
                break;
            }
            Ok(Ok(_)) | Err(_) => {}
            Ok(Err(_)) => break,
        }
    }
    assert!(saw_event, "AgentQueued event should be published");
}

#[tokio::test]
async fn enqueue_with_lane_scroll_marks_lane_correctly() {
    let (db, _, manager, _) = fresh_manager().await;

    let agent = manager
        .enqueue(
            "scroll work",
            Some("frontend".into()),
            None,
            None,
            Path::new("/tmp"),
            Lane::Scroll,
        )
        .await
        .unwrap();

    let queue = db.list_queue().unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].id, agent.id);
    assert_eq!(queue[0].lane, "scroll");
}

// --- dispatch_internal contract ------------------------------------------

#[tokio::test]
async fn dispatch_drives_executor_exactly_once() {
    let (db, _, manager, log) = fresh_manager().await;

    let agent = manager
        .enqueue("echo hi", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    // Mirror the scheduler's claim phase: fetch the row and atomically claim.
    let row = db.peek_next_dispatch().unwrap().expect("queue has the row");
    assert!(db.claim_for_dispatch(&row.id).unwrap());

    // Dispatch via the public trait method.
    (manager.as_ref() as &dyn Dispatcher)
        .dispatch(row)
        .await
        .expect("dispatch should succeed");

    let calls = log.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "executor.start should be called exactly once"
    );
    assert_eq!(calls[0].agent_id, agent.id);
    assert_eq!(calls[0].task, "echo hi");
}

#[tokio::test]
async fn dispatch_failure_returns_err_without_touching_queue() {
    let (db, _, manager, log) = fresh_manager().await;

    let _agent = manager
        .enqueue("echo hi", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    let row = db.peek_next_dispatch().unwrap().unwrap();
    assert!(db.claim_for_dispatch(&row.id).unwrap());
    *log.fail_next.lock().unwrap() = true;

    let result = (manager.as_ref() as &dyn Dispatcher)
        .dispatch(row.clone())
        .await;
    assert!(result.is_err(), "forced executor failure must propagate");

    // dispatch_internal must NOT mutate queue/state on failure, that's the
    // scheduler's job (it owns the requeue path so fairness is preserved).
    assert_eq!(
        db.count_queued().unwrap(),
        0,
        "dispatch_internal must not re-insert into the queue"
    );
}

// --- banish-on-Queued / invoke-on-Queued ---------------------------------

#[tokio::test]
async fn banish_queued_removes_from_queue() {
    let (db, _, manager, _) = fresh_manager().await;

    let agent = manager
        .enqueue(
            "queued task",
            None,
            None,
            None,
            Path::new("/tmp"),
            Lane::Adhoc,
        )
        .await
        .unwrap();
    assert_eq!(db.count_queued().unwrap(), 1);

    let banished = manager.banish(&agent.id).await.unwrap();
    assert!(banished, "banish on Queued must succeed");

    assert_eq!(
        db.count_queued().unwrap(),
        0,
        "task_queue row must be removed"
    );
}

#[tokio::test]
async fn banish_queued_sets_state_banished() {
    let (db, bus, manager, _) = fresh_manager().await;
    let mut rx = bus.subscribe();

    let agent = manager
        .enqueue("queued", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    manager.banish(&agent.id).await.unwrap();

    let stored = db.get_agent(&agent.id).unwrap().expect("agent in DB");
    assert_eq!(stored.state, AgentState::Banished);

    // Drain events looking for the StateChange Queued -> Banished.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut saw = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Ok(StreamEvent::StateChange {
                agent_id,
                old_state,
                new_state,
            })) if agent_id == agent.id => {
                if old_state == AgentState::Queued && new_state == AgentState::Banished {
                    saw = true;
                    break;
                }
            }
            Ok(Ok(_)) | Err(_) => {}
            Ok(Err(_)) => break,
        }
    }
    assert!(saw, "StateChange Queued -> Banished must be published");
}

#[tokio::test]
async fn banish_queued_does_not_invoke_kill() {
    // For a Queued agent, no executor handle, no pid, and no cancel were
    // registered, banish must take the queue-only path and never reach the
    // process-kill code. Observable: executor was never called.
    let (_, _, manager, log) = fresh_manager().await;

    let agent = manager
        .enqueue("queued", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    assert!(manager.banish(&agent.id).await.unwrap());

    assert_eq!(
        log.calls.lock().unwrap().len(),
        0,
        "executor must never be called for a banish-on-queued"
    );
    let stored = manager.get_agent(&agent.id).await.unwrap().unwrap();
    assert!(
        stored.pid.is_none(),
        "queued agent must never have had a pid"
    );
}

#[tokio::test]
async fn invoke_queued_returns_error_with_clear_message() {
    let (_, _, manager, _) = fresh_manager().await;

    let agent = manager
        .enqueue("queued", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    let err = manager
        .invoke(&agent.id, "ping", None)
        .await
        .expect_err("invoke on Queued must error");
    let msg = err.to_string();
    assert!(
        msg.contains("not dormant") || msg.contains("has not started"),
        "error message must indicate the agent isn't dormant, got: {msg}"
    );
}

#[tokio::test]
async fn invoke_complete_unchanged() {
    // Regression guard: invoke against a Dormant agent (with a real session)
    // continues to drive the executor. The helper seeds Dormant, which is the
    // state Complete-with-session agents land in after the boot migration.
    let (_, _, manager, log) = fresh_manager().await;

    let agent_id = manager
        .seed_agent_for_test_with_session("session-xyz")
        .await
        .unwrap();

    manager.invoke(&agent_id, "follow-up", None).await.unwrap();

    let calls = log.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "invoke on Complete must still call executor.start"
    );
    assert_eq!(calls[0].agent_id, agent_id);
    assert_eq!(calls[0].resume_session_id.as_deref(), Some("session-xyz"));
}

#[tokio::test]
async fn invoke_context_replay_prepends_transcript_and_no_native_resume() {
    use grimoire::shared::config::ProviderConfig;
    use grimoire::shared::types::AgentEvent;
    use std::collections::HashMap;

    // A generic config provider → ContextReplay strategy (no native resume).
    let mut config = Config::default();
    config.providers.insert(
        "aider".to_string(),
        ProviderConfig {
            binary: "true".to_string(),
            args_template: vec!["{task}".to_string()],
            env: HashMap::new(),
            sandbox: None,
            pricing: None,
        },
    );

    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let log = Arc::new(ExecutorLog::default());
    let executor: Arc<dyn Executor> = Arc::new(MockExecutor { log: log.clone() });
    let manager = AgentManager::new_with_executor(db.clone(), bus, config, executor).await;

    // Synthetic session id (as the keep-alive gate would mint for ContextReplay).
    let agent_id = manager
        .seed_agent_for_test_with_session_provider("daemon:abc123", Some("aider"))
        .await
        .unwrap();

    // Prior output the daemon should replay back into the resume prompt.
    for line in ["I reviewed auth.rs", "Found a missing null check"] {
        db.insert_event(&AgentEvent {
            id: None,
            agent_id: agent_id.clone(),
            event_type: "stdout".to_string(),
            payload: line.to_string(),
            created_at: chrono::Utc::now(),
        })
        .unwrap();
    }

    manager
        .invoke(&agent_id, "re-check the diff", None)
        .await
        .unwrap();

    let calls = log.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    // ContextReplay must NOT use native session resume.
    assert_eq!(calls[0].resume_session_id, None);
    // The composed task carries the replayed transcript and the new request.
    let task = &calls[0].task;
    assert!(task.contains("## Prior context"), "task: {task}");
    assert!(task.contains("Found a missing null check"), "task: {task}");
    assert!(task.contains("## Current request"), "task: {task}");
    assert!(task.contains("re-check the diff"), "task: {task}");
}

// --- summon symbol removal contract --------------------------------------

// Compile-time guard: if `AgentManager::summon` is reintroduced, this test
// will not break, but the spec's contract says it is removed. Runtime check:
// build a manager and confirm the new `enqueue` API is the entry point.
#[tokio::test]
async fn enqueue_is_the_entry_point_not_summon() {
    let (_, _, manager, log) = fresh_manager().await;

    manager
        .enqueue("smoke", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    // After enqueue alone (no scheduler tick), the executor must NOT have
    // been called, proving enqueue is non-dispatching.
    assert_eq!(
        log.calls.lock().unwrap().len(),
        0,
        "enqueue alone must not drive the executor"
    );
}
