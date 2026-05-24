//! End-to-end integration tests for the durable work queue: enqueue →
//! scheduler dispatch → AgentManager.dispatch_internal → state transitions,
//! plus restart recovery, banish-while-queued, and ad-hoc/scroll lane order.
//!
//! Tests use the manual-tick scheduler entry point (`Scheduler::tick_now`)
//! plus a controllable mock executor so they are deterministic.

#[path = "support/wait_for_state.rs"]
mod wait_for_state_helper;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use semver::Version;
use tokio::sync::{Mutex, mpsc, oneshot};

use grimoire::daemon::agent_manager::{AgentManager, Lane};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::executor::{ExecuteRequest, Executor, ExecutorHandle};
use grimoire::daemon::persistence::Database;
use grimoire::daemon::process_manager::MonitorResult;
use grimoire::daemon::scheduler::{Dispatcher, Scheduler};
use grimoire::daemon::worker_registry::{RegisterParams, WorkerRegistry};
use grimoire::shared::config::Config;
use grimoire::shared::types::{Agent, AgentId, AgentState};

use wait_for_state_helper::wait_for_state;

// --- Helper-helpers: a controllable executor ------------------------------
//
// Each `start` call returns immediately with a completion future tied to a
// `oneshot::Sender<MonitorResult>` that the test holds. `complete(id, ...)`
// pushes a result through that sender, which the AgentManager's
// `watch_completion` consumes and translates into a `StateChange` event.

#[derive(Default)]
struct ControlledExecutor {
    pending: Mutex<std::collections::HashMap<AgentId, oneshot::Sender<MonitorResult>>>,
    started: Mutex<Vec<AgentId>>,
}

impl ControlledExecutor {
    async fn complete(&self, id: &str, state: AgentState, exit_code: Option<i32>) {
        let sender = {
            let mut p = self.pending.lock().await;
            p.remove(id)
        };
        if let Some(tx) = sender {
            let _ = tx.send(MonitorResult {
                state,
                exit_code,
                session_id: None,
                error_reason: None,
                tokens_used: None,
                token_breakdown: None,
            });
        }
    }

    async fn started_ids(&self) -> Vec<AgentId> {
        self.started.lock().await.clone()
    }
}

#[async_trait]
impl Executor for ControlledExecutor {
    async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle> {
        let (done_tx, done_rx) = oneshot::channel::<MonitorResult>();
        self.pending
            .lock()
            .await
            .insert(req.agent_id.clone(), done_tx);
        self.started.lock().await.push(req.agent_id.clone());
        let (cancel_tx, _cancel_rx) = oneshot::channel::<()>();
        let completion = tokio::spawn(async move {
            done_rx.await.unwrap_or_else(|_| MonitorResult {
                state: AgentState::Failed,
                exit_code: None,
                session_id: None,
                error_reason: Some("test executor dropped".into()),
                tokens_used: None,
                token_breakdown: None,
            })
        });
        Ok(ExecutorHandle {
            worker_id: None,
            pid: Some(1),
            cancel: Box::new(move || {
                let _ = cancel_tx.send(());
            }),
            completion,
        })
    }

    fn name(&self) -> &'static str {
        "controlled"
    }
}

struct Harness {
    db: Arc<Database>,
    bus: EventBus,
    manager: Arc<AgentManager>,
    scheduler: Arc<Scheduler>,
    workers: Arc<WorkerRegistry>,
    executor: Arc<ControlledExecutor>,
    cap: Arc<AtomicU32>,
}

impl Harness {
    async fn build_with_db(db: Arc<Database>, cap_value: u32) -> Self {
        let bus = EventBus::new(db.clone());
        let executor = Arc::new(ControlledExecutor::default());
        let manager = AgentManager::new_with_executor(
            db.clone(),
            bus.clone(),
            Config::default(),
            executor.clone() as Arc<dyn Executor>,
        )
        .await;
        let workers = Arc::new(WorkerRegistry::new_with_bus(
            Duration::from_mins(1),
            bus.clone(),
        ));
        let cap = Arc::new(AtomicU32::new(cap_value));
        let dispatcher: Arc<dyn Dispatcher> = manager.clone();
        let scheduler = Arc::new(Scheduler::new(
            db.clone(),
            workers.clone(),
            bus.clone(),
            cap.clone(),
            dispatcher,
        ));
        Self {
            db,
            bus,
            manager,
            scheduler,
            workers,
            executor,
            cap,
        }
    }

    async fn build(cap_value: u32) -> Self {
        let db = Arc::new(Database::open_in_memory().unwrap());
        Self::build_with_db(db, cap_value).await
    }

    fn register_worker(&self, worker_id: &str, provider: &str) {
        let (tx, _rx) = mpsc::channel(1);
        self.workers
            .register(RegisterParams {
                worker_id: worker_id.to_string(),
                bearer_ok: true,
                worker_version: "0.1.0".to_string(),
                max_concurrent: 4,
                providers: vec![(provider.to_string(), Version::parse("1.0.0").unwrap())],
                tags: vec![],
                assign_tx: tx,
            })
            .unwrap();
    }
}

// --- wait_for_state contract ---------------------------------------------

#[tokio::test]
async fn wait_for_state_returns_when_target_matches() {
    let h = Harness::build(8).await;
    let agent = h
        .manager
        .enqueue("t", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    let got = wait_for_state(
        &h.db,
        &agent.id,
        AgentState::Queued,
        Duration::from_millis(200),
    )
    .await
    .expect("Queued state should match immediately");
    assert_eq!(got.state, AgentState::Queued);
    let _ = h.bus.subscribe(); // keep bus alive
}

#[tokio::test]
async fn wait_for_state_times_out_when_state_never_matches() {
    let h = Harness::build(0).await; // cap=0: agent never dispatches
    let agent = h
        .manager
        .enqueue("t", None, None, None, Path::new("/tmp"), Lane::Adhoc)
        .await
        .unwrap();

    let err = wait_for_state(
        &h.db,
        &agent.id,
        AgentState::Active,
        Duration::from_millis(100),
    )
    .await
    .expect_err("should time out");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("queued"),
        "error must name actual final state; got: {msg}"
    );
}

// --- Integration scenarios -----------------------------------------------

#[tokio::test]
async fn restart_recovery_keeps_queued_loses_active() {
    // Use a temp file path so the second daemon boot sees the same data.
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("grimoire.db");

    // First boot: enqueue 3 with cap=0 so they stay Queued, plus seed one
    // synthetic Active agent to confirm it gets flipped to Failed.
    {
        let db = Arc::new(Database::open(&path).unwrap());
        let h = Harness::build_with_db(db.clone(), 0).await;
        for _ in 0..3 {
            h.manager
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
        }
        // Synthetic Active agent (simulates a daemon that died mid-flight).
        let active = Agent {
            id: "act00001".to_string(),
            name: None,
            state: AgentState::Active,
            task: Some("running".into()),
            model: None,
            provider: None,
            cwd: std::path::PathBuf::from("/tmp"),
            pid: Some(99999),
            session_id: None,
            exit_code: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            worker_id: None,
            restart_policy: grimoire::shared::types::RestartPolicy::Never,
            restart_count: 0,
            workspace_id: None,
        };
        db.insert_agent(&active).unwrap();
        // Drop Harness (and DB connection) to simulate daemon death.
    }

    // Second boot: open fresh DB at the same path. AgentManager::new
    // triggers reload_from_db -> restart_recovery on construction.
    let db2 = Arc::new(Database::open(&path).unwrap());
    let _h2 = Harness::build_with_db(db2.clone(), 0).await;

    // The synthetic Active is now Failed.
    let act = db2.get_agent("act00001").unwrap().expect("present");
    assert_eq!(
        act.state,
        AgentState::Failed,
        "Active across restart must be Failed"
    );

    // Three Queued survived.
    let queued = db2.list_agents(Some("queued")).unwrap();
    assert_eq!(queued.len(), 3, "Queued agents must persist across restart");

    // task_queue rows still match the surviving agents.
    let queue = db2.list_queue().unwrap();
    assert_eq!(queue.len(), 3, "task_queue rows survive restart");
}

#[tokio::test]
async fn capacity_saturation_promotes_on_completion() {
    let h = Harness::build(2).await;
    h.register_worker("w1", "claude");

    let mut ids = Vec::new();
    for _ in 0..3 {
        let a = h
            .manager
            .enqueue(
                "task",
                None,
                None,
                Some("claude".into()),
                Path::new("/tmp"),
                Lane::Adhoc,
            )
            .await
            .unwrap();
        ids.push(a.id);
    }

    // First tick: dispatches up to cap.
    h.scheduler.tick_now().await.unwrap();

    let active_or_summoning = h.db.count_in_flight_agents().unwrap();
    assert_eq!(active_or_summoning, 2, "cap=2 should give 2 in-flight");
    assert_eq!(h.db.count_queued().unwrap(), 1, "third stays queued");

    // Complete the first dispatched agent — frees a slot.
    let started = h.executor.started_ids().await;
    assert_eq!(started.len(), 2);
    h.executor
        .complete(&started[0], AgentState::Complete, Some(0))
        .await;

    // Wait for that agent to settle to Complete (watch_completion is async).
    wait_for_state(
        &h.db,
        &started[0],
        AgentState::Complete,
        Duration::from_secs(2),
    )
    .await
    .expect("completion propagates");

    // Tick again: third should promote.
    h.scheduler.tick_now().await.unwrap();
    let third = ids.iter().find(|id| !started.contains(id)).unwrap();
    wait_for_state(&h.db, third, AgentState::Active, Duration::from_secs(2))
        .await
        .expect("third agent must reach Active after a slot frees");

    assert_eq!(h.db.count_queued().unwrap(), 0);
}

#[tokio::test]
async fn no_eligible_worker_unblocks_on_registration() {
    let h = Harness::build(4).await;
    // No worker yet for "absent".

    let agent = h
        .manager
        .enqueue(
            "needs absent provider",
            None,
            None,
            Some("absent".into()),
            Path::new("/tmp"),
            Lane::Adhoc,
        )
        .await
        .unwrap();

    h.scheduler.tick_now().await.unwrap();

    // Still queued, with the right block reason.
    assert_eq!(h.db.count_queued().unwrap(), 1);
    let rows = h.db.list_queue().unwrap();
    assert_eq!(
        rows[0].block_reason.as_deref(),
        Some("no_eligible_worker"),
        "scheduler must mark block reason when no worker advertises the provider"
    );

    // Register a matching worker; tick again.
    h.register_worker("w-absent", "absent");
    h.scheduler.tick_now().await.unwrap();

    wait_for_state(&h.db, &agent.id, AgentState::Active, Duration::from_secs(2))
        .await
        .expect("agent must dispatch once a worker is available");
    assert_eq!(h.db.count_queued().unwrap(), 0);
}

#[tokio::test]
async fn scroll_and_adhoc_interleave_adhoc_wins() {
    let h = Harness::build(1).await;
    h.register_worker("w1", "claude");

    // Saturate with one in-flight agent. We do this via enqueue + tick so
    // the scheduler's bookkeeping is consistent.
    let occupant = h
        .manager
        .enqueue(
            "occupant",
            None,
            None,
            Some("claude".into()),
            Path::new("/tmp"),
            Lane::Adhoc,
        )
        .await
        .unwrap();
    h.scheduler.tick_now().await.unwrap();
    wait_for_state(
        &h.db,
        &occupant.id,
        AgentState::Active,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    // Now enqueue scroll first, ad-hoc second.
    let scroll_agent = h
        .manager
        .enqueue(
            "scroll work",
            None,
            None,
            Some("claude".into()),
            Path::new("/tmp"),
            Lane::Scroll,
        )
        .await
        .unwrap();
    // Tiny delay so enqueued_at differs deterministically.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let adhoc_agent = h
        .manager
        .enqueue(
            "adhoc work",
            None,
            None,
            Some("claude".into()),
            Path::new("/tmp"),
            Lane::Adhoc,
        )
        .await
        .unwrap();

    // Free the slot.
    h.executor
        .complete(&occupant.id, AgentState::Complete, Some(0))
        .await;
    wait_for_state(
        &h.db,
        &occupant.id,
        AgentState::Complete,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    // Tick: ad-hoc lane drains first even though scroll was enqueued earlier.
    h.scheduler.tick_now().await.unwrap();

    wait_for_state(
        &h.db,
        &adhoc_agent.id,
        AgentState::Active,
        Duration::from_secs(2),
    )
    .await
    .expect("ad-hoc must dispatch first when both lanes wait");

    let scroll_now = h.db.get_agent(&scroll_agent.id).unwrap().unwrap();
    assert_eq!(
        scroll_now.state,
        AgentState::Queued,
        "scroll lane must wait while ad-hoc holds the only slot"
    );
}

#[tokio::test]
async fn banish_while_queued_dequeues() {
    let h = Harness::build(0).await; // cap=0 so nothing dispatches
    h.register_worker("w1", "claude");

    let agent = h
        .manager
        .enqueue(
            "queued",
            None,
            None,
            Some("claude".into()),
            Path::new("/tmp"),
            Lane::Adhoc,
        )
        .await
        .unwrap();
    assert_eq!(h.db.count_queued().unwrap(), 1);

    // Banish while Queued: must remove the row and mark Banished — never
    // touch the executor.
    let ok = h.manager.banish(&agent.id).await.unwrap();
    assert!(ok);

    let stored = h.db.get_agent(&agent.id).unwrap().unwrap();
    assert_eq!(stored.state, AgentState::Banished);
    assert_eq!(h.db.count_queued().unwrap(), 0);

    // Bring the cap up: the scheduler must NOT resurrect a banished agent.
    h.cap.store(4, Ordering::Relaxed);
    h.scheduler.tick_now().await.unwrap();
    let started = h.executor.started_ids().await;
    assert!(
        started.is_empty(),
        "banished agent must never be dispatched"
    );
}
