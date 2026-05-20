//! Task 3 contract tests: scheduler tick_supervision + restart_dispatch.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Mutex;

use grimoire::daemon::clock::TestClock;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::{Database, QueueRow};
use grimoire::daemon::scheduler::{Dispatcher, Scheduler};
use grimoire::daemon::supervisor::{
    EscalationMailSender, EscalationOutcome, RestartDispatcher, Supervisor,
};
use grimoire::daemon::worker_registry::WorkerRegistry;
use grimoire::shared::types::{Agent, AgentState, RestartPolicy, SupervisionConfig};

#[derive(Default)]
struct NoopMail;
#[async_trait]
impl EscalationMailSender for NoopMail {
    async fn send_escalation(&self, _: &str, _: &str, _: &str) -> Result<EscalationOutcome> {
        Ok(EscalationOutcome::default())
    }
}

#[derive(Default)]
struct NoopDispatcher;
#[async_trait]
impl Dispatcher for NoopDispatcher {
    async fn dispatch(&self, _row: QueueRow) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct RecordingDispatcher {
    calls: Mutex<Vec<(String, u32)>>,
    fail_next: Mutex<bool>,
}
#[async_trait]
impl RestartDispatcher for RecordingDispatcher {
    async fn restart_dispatch(&self, agent_id: &str, attempt: u32) -> Result<()> {
        if *self.fail_next.lock().await {
            anyhow::bail!("synthetic dispatch failure");
        }
        self.calls
            .lock()
            .await
            .push((agent_id.to_string(), attempt));
        Ok(())
    }
}

fn seed(db: &Database, id: &str, state: AgentState) {
    let agent = Agent {
        id: id.to_string(),
        name: None,
        state,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: Some("sess".into()),
        exit_code: Some(1),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
}

async fn build_supervisor_with_pending(
    db: Arc<Database>,
    bus: EventBus,
    clock: Arc<TestClock>,
    agent_id: &str,
    fire_at: chrono::DateTime<Utc>,
) -> Arc<Supervisor> {
    let mail: Arc<dyn EscalationMailSender> = Arc::new(NoopMail);
    let sup = Supervisor::new(db.clone(), bus, clock, 30, 3, mail);
    db.set_supervision(
        agent_id,
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(3),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    sup.schedule_restart(agent_id, 1, fire_at, false)
        .await
        .unwrap();
    sup
}

fn make_scheduler(
    db: Arc<Database>,
    bus: EventBus,
    cap: u32,
    sup: Arc<Supervisor>,
    rdisp: Arc<dyn RestartDispatcher>,
) -> Arc<Scheduler> {
    let workers = Arc::new(WorkerRegistry::new_with_bus(
        Duration::from_mins(1),
        bus.clone(),
    ));
    let cap = Arc::new(AtomicU32::new(cap));
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(NoopDispatcher);
    let s = Scheduler::new(db, workers, bus, cap, dispatcher).with_supervision(sup, rdisp);
    Arc::new(s)
}

#[tokio::test]
async fn tick_supervision_dispatches_due_restart() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "dis00001", AgentState::Failed);
    let now = Utc::now();
    let clock = Arc::new(TestClock::new(now));
    let sup = build_supervisor_with_pending(db.clone(), bus.clone(), clock, "dis00001", now).await;
    let rec = Arc::new(RecordingDispatcher::default());
    let rdisp: Arc<dyn RestartDispatcher> = rec.clone();
    let sched = make_scheduler(db.clone(), bus, 4, sup, rdisp);
    sched.tick_now().await.unwrap();
    let calls = rec.calls.lock().await.clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "dis00001");
    assert_eq!(calls[0].1, 1);
}

#[tokio::test]
async fn tick_supervision_respects_capacity() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "dis00002", AgentState::Failed);
    // Pre-occupy a slot by inserting an Active agent in DB.
    seed(&db, "actv0001", AgentState::Active);
    let now = Utc::now();
    let clock = Arc::new(TestClock::new(now));
    let sup = build_supervisor_with_pending(db.clone(), bus.clone(), clock, "dis00002", now).await;
    let rec = Arc::new(RecordingDispatcher::default());
    let rdisp: Arc<dyn RestartDispatcher> = rec.clone();
    let sched = make_scheduler(db.clone(), bus, 1, sup.clone(), rdisp);
    sched.tick_now().await.unwrap();
    assert!(rec.calls.lock().await.is_empty());
    // The pending entry should still be there.
    let snap = sup.pending_snapshot().await;
    assert_eq!(snap.len(), 1);
}

#[tokio::test]
async fn restart_dispatch_passes_session_id() {
    use grimoire::daemon::agent_manager::AgentManager;
    use grimoire::daemon::executor::{ExecuteRequest, Executor, ExecutorHandle};
    use grimoire::daemon::process_manager::MonitorResult;
    use grimoire::shared::config::Config;

    #[derive(Default)]
    struct RecExecutor {
        captured: Mutex<Option<ExecuteRequest>>,
    }
    #[async_trait]
    impl Executor for RecExecutor {
        async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle> {
            *self.captured.lock().await = Some(req.clone());
            let (tx, _rx) = tokio::sync::oneshot::channel::<()>();
            let completion = tokio::spawn(async {
                MonitorResult {
                    state: AgentState::Complete,
                    exit_code: Some(0),
                    session_id: None,
                    error_reason: None,
                }
            });
            Ok(ExecutorHandle {
                worker_id: None,
                pid: None,
                cancel: Box::new(move || {
                    let _ = tx.send(());
                }),
                completion,
            })
        }
        fn name(&self) -> &'static str {
            "rec"
        }
    }

    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "dis00004", AgentState::Restarting);
    db.update_agent_session_id("dis00004", "sess-xyz").unwrap();
    let exec = Arc::new(RecExecutor::default());
    let manager =
        AgentManager::new_with_executor(db.clone(), bus.clone(), Config::default(), exec.clone())
            .await;
    manager.restart_dispatch("dis00004", 1).await.unwrap();
    let captured = exec.captured.lock().await.clone().unwrap();
    assert_eq!(captured.resume_session_id.as_deref(), Some("sess-xyz"));
}

#[tokio::test]
async fn restart_dispatch_emits_restarted_event() {
    use grimoire::daemon::agent_manager::AgentManager;
    use grimoire::daemon::executor::{ExecuteRequest, Executor, ExecutorHandle};
    use grimoire::daemon::process_manager::MonitorResult;
    use grimoire::shared::config::Config;
    use grimoire::shared::protocol::StreamEvent;

    #[derive(Default)]
    struct StubExec;
    #[async_trait]
    impl Executor for StubExec {
        async fn start(&self, _req: ExecuteRequest) -> Result<ExecutorHandle> {
            let (tx, _rx) = tokio::sync::oneshot::channel::<()>();
            // Never-completing future so the agent stays Active for the
            // duration of the test.
            let completion = tokio::spawn(async {
                std::future::pending::<()>().await;
                MonitorResult {
                    state: AgentState::Complete,
                    exit_code: Some(0),
                    session_id: None,
                    error_reason: None,
                }
            });
            Ok(ExecutorHandle {
                worker_id: None,
                pid: None,
                cancel: Box::new(move || {
                    let _ = tx.send(());
                }),
                completion,
            })
        }
        fn name(&self) -> &'static str {
            "stub"
        }
    }

    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "dis00005", AgentState::Restarting);
    let exec: Arc<dyn Executor> = Arc::new(StubExec);
    let manager =
        AgentManager::new_with_executor(db.clone(), bus.clone(), Config::default(), exec).await;
    let mut rx = bus.subscribe();
    manager.restart_dispatch("dis00005", 4).await.unwrap();
    let mut count = 0;
    let mut got_attempt = 0;
    while let Ok(ev) = rx.try_recv() {
        if let StreamEvent::Restarted { attempt, .. } = ev {
            count += 1;
            got_attempt = attempt;
        }
    }
    assert_eq!(count, 1);
    assert_eq!(got_attempt, 4);
}

#[tokio::test]
async fn restart_dispatch_rejects_non_restarting_state() {
    use grimoire::daemon::agent_manager::AgentManager;
    use grimoire::shared::config::Config;
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "dis00003", AgentState::Banished);
    let manager = AgentManager::new(db.clone(), bus, Config::default()).await;
    let res = manager.restart_dispatch("dis00003", 1).await;
    assert!(res.is_err());
}
