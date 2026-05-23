// Tests for `Executor` trait + `LocalExecutor`; `AgentManager` is routed
// through it.
//
// Gated with `#![cfg(any())]` until the
// GREEN phase begins; remove the gate to activate.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;

use grimoire::daemon::agent_manager::{AgentManager, Lane};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::executor::{ExecuteRequest, Executor, ExecutorHandle, LocalExecutor};
use grimoire::daemon::persistence::Database;
use grimoire::daemon::provider_registry::ProviderRegistry;
use grimoire::daemon::scheduler::Dispatcher;
use grimoire::shared::config::Config;
use grimoire::shared::types::AgentState;

fn make_request(agent_id: &str, task: &str, cwd: &str) -> ExecuteRequest {
    ExecuteRequest {
        agent_id: agent_id.to_string(),
        task: task.to_string(),
        provider_name: "true_provider".to_string(),
        cwd: PathBuf::from(cwd),
        model: None,
        resume_session_id: None,
    }
}

fn fresh_local_executor() -> LocalExecutor {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let registry = ProviderRegistry::test_with_true_provider();
    LocalExecutor::new(Arc::new(registry), bus, db)
}

#[tokio::test]
async fn local_executor_start_returns_handle_with_pid_and_completion() {
    // The fake provider returns a child running `/bin/true`.
    let exec = fresh_local_executor();
    let handle = exec
        .start(make_request("a-1", "noop", "/tmp"))
        .await
        .expect("LocalExecutor::start should succeed for /bin/true");

    assert!(handle.pid.is_some(), "local executor must report pid");
    assert!(handle.worker_id.is_none(), "local handle has no worker_id");

    let result = handle.completion.await.unwrap();
    assert_eq!(result.state, AgentState::Complete);
    assert_eq!(result.exit_code, Some(0));
}

#[tokio::test]
async fn local_executor_cancel_kills_process() {
    let exec = LocalExecutor::test_with_command("sleep", &["60"]);
    let handle = exec
        .start(make_request("a-2", "sleep", "/tmp"))
        .await
        .unwrap();

    // Wait briefly to ensure the process is running, then cancel.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (handle.cancel)();

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.completion)
        .await
        .expect("completion must resolve within 2s of cancel")
        .unwrap();
    assert_eq!(result.state, AgentState::Failed);
}

#[tokio::test]
async fn agent_manager_summon_uses_executor() {
    #[derive(Default)]
    struct CallLog {
        calls: Mutex<Vec<ExecuteRequest>>,
    }

    struct MockExecutor {
        log: Arc<CallLog>,
    }

    #[async_trait]
    impl Executor for MockExecutor {
        async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle> {
            self.log.calls.lock().unwrap().push(req);
            // Return a handle whose completion resolves immediately.
            let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel::<()>();
            let completion = tokio::spawn(async {
                grimoire::daemon::process_manager::MonitorResult {
                    state: AgentState::Complete,
                    exit_code: Some(0),
                    session_id: None,
                    error_reason: None,
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

    let log = Arc::new(CallLog::default());
    let executor: Arc<dyn Executor> = Arc::new(MockExecutor { log: log.clone() });
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager =
        AgentManager::new_with_executor(db.clone(), bus, Config::default(), executor).await;

    // summon is split into enqueue (queue write) + dispatch
    // (executor call). The scheduler normally drives dispatch; here we mirror
    // its claim+dispatch sequence by hand to keep the test focused on the
    // executor wire-up.
    let agent = manager
        .enqueue(
            "echo hi",
            None,
            None,
            None,
            std::path::Path::new("/tmp"),
            Lane::Adhoc,
        )
        .await
        .unwrap();

    let row = db.peek_next_dispatch().unwrap().unwrap();
    assert!(db.claim_for_dispatch(&row.id).unwrap());
    (manager.as_ref() as &dyn Dispatcher)
        .dispatch(row)
        .await
        .unwrap();

    let calls = log.calls.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "dispatch should drive exactly one start call"
    );
    assert_eq!(calls[0].task, "echo hi");
    assert_eq!(calls[0].agent_id, agent.id);
}

#[tokio::test]
async fn agent_manager_invoke_passes_resume_session_id() {
    #[derive(Default)]
    struct Log {
        last: Mutex<Option<ExecuteRequest>>,
    }
    struct ResumeMock {
        log: Arc<Log>,
    }
    #[async_trait]
    impl Executor for ResumeMock {
        async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle> {
            *self.log.last.lock().unwrap() = Some(req);
            let completion = tokio::spawn(async {
                grimoire::daemon::process_manager::MonitorResult {
                    state: AgentState::Complete,
                    exit_code: Some(0),
                    session_id: None,
                    error_reason: None,
                }
            });
            Ok(ExecutorHandle {
                worker_id: None,
                pid: None,
                cancel: Box::new(|| {}),
                completion,
            })
        }
        fn name(&self) -> &'static str {
            "resume_mock"
        }
    }

    let log = Arc::new(Log::default());
    let executor: Arc<dyn Executor> = Arc::new(ResumeMock { log: log.clone() });
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let manager = AgentManager::new_with_executor(db, bus, Config::default(), executor).await;

    // Pre-seed an agent with a known session_id, then invoke against it.
    let agent_id = manager
        .seed_agent_for_test_with_session("session-abc")
        .await
        .unwrap();
    manager.invoke(&agent_id, "follow up", None).await.unwrap();

    let last = log.last.lock().unwrap().clone().unwrap();
    assert_eq!(last.resume_session_id.as_deref(), Some("session-abc"));
}
