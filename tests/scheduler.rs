// Contract tests for `daemon::scheduler`, driven through `tick_now()` for
// determinism with a fake `Dispatcher` that records calls and can force errors.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use semver::Version;
use tokio::sync::{Mutex, mpsc};

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::{Database, QueueRow};
use grimoire::daemon::scheduler::{Dispatcher, Scheduler};
use grimoire::daemon::worker_registry::{RegisterParams, WorkerRegistry};
use grimoire::shared::types::{Agent, AgentId, AgentState};

fn make_queued_agent(id: &str, provider: Option<&str>) -> Agent {
    Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Queued,
        task: Some("test".to_string()),
        model: None,
        provider: provider.map(std::string::ToString::to_string),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    }
}

fn make_active_agent(id: &str) -> Agent {
    let mut a = make_queued_agent(id, Some("claude"));
    a.state = AgentState::Active;
    a.pid = Some(1234);
    a
}

fn make_queue_row(id: &str, lane: &str, provider: Option<&str>, t_offset_secs: i64) -> QueueRow {
    QueueRow {
        id: id.to_string(),
        lane: lane.to_string(),
        priority: 0,
        enqueued_at: Utc::now() + chrono::Duration::seconds(t_offset_secs),
        provider_name: provider.map(std::string::ToString::to_string),
        cwd: "/tmp".to_string(),
        model: None,
        task_text: "test task".to_string(),
        block_reason: None,
    }
}

#[derive(Clone, Default)]
struct FakeDispatcher {
    calls: Arc<Mutex<Vec<AgentId>>>,
    fail_next: Arc<AtomicBool>,
}

impl FakeDispatcher {
    fn new() -> Self {
        Self::default()
    }

    fn fail_next(&self) {
        self.fail_next.store(true, Ordering::SeqCst);
    }

    async fn calls(&self) -> Vec<AgentId> {
        self.calls.lock().await.clone()
    }
}

#[async_trait]
impl Dispatcher for FakeDispatcher {
    async fn dispatch(&self, row: QueueRow) -> Result<()> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(anyhow!("fake dispatcher: forced failure"));
        }
        // No DB write needed: tests assert before the next tick, and
        // claim_for_dispatch already moved the agent out of `queued`.
        self.calls.lock().await.push(row.id);
        Ok(())
    }
}

fn build_registry(bus: EventBus) -> Arc<WorkerRegistry> {
    Arc::new(WorkerRegistry::new_with_bus(Duration::from_mins(1), bus))
}

fn register_worker(registry: &WorkerRegistry, worker_id: &str, provider: &str) {
    let (tx, _rx) = mpsc::channel(1);
    registry
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

fn enqueue(db: &Database, agent: &Agent, row: &QueueRow) {
    db.insert_agent(agent).unwrap();
    db.enqueue_task(row).unwrap();
}

fn build_scheduler(
    db: Arc<Database>,
    workers: Arc<WorkerRegistry>,
    bus: EventBus,
    cap: u32,
    dispatcher: Arc<dyn Dispatcher>,
) -> Arc<Scheduler> {
    let cap_atom = Arc::new(AtomicU32::new(cap));
    Arc::new(Scheduler::new(db, workers, bus, cap_atom, dispatcher))
}

#[tokio::test]
async fn scheduler_dispatches_up_to_cap() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let workers = build_registry(bus.clone());
    register_worker(&workers, "w1", "claude");

    for (i, name) in ["a1", "a2", "a3"].iter().enumerate() {
        enqueue(
            &db,
            &make_queued_agent(name, Some("claude")),
            &make_queue_row(name, "adhoc", Some("claude"), i as i64),
        );
    }

    let dispatcher = Arc::new(FakeDispatcher::new());
    let sched = build_scheduler(db.clone(), workers, bus, 2, dispatcher.clone());

    sched.tick_now().await.unwrap();

    let calls = dispatcher.calls().await;
    assert_eq!(calls.len(), 2, "cap=2 should dispatch exactly two");
    assert_eq!(db.count_queued().unwrap(), 1, "third row stays queued");
}

#[tokio::test]
async fn scheduler_respects_inflight_count() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let workers = build_registry(bus.clone());
    register_worker(&workers, "w1", "claude");

    // two running, one queued
    db.insert_agent(&make_active_agent("running1")).unwrap();
    db.insert_agent(&make_active_agent("running2")).unwrap();

    enqueue(
        &db,
        &make_queued_agent("queued1", Some("claude")),
        &make_queue_row("queued1", "adhoc", Some("claude"), 0),
    );

    let dispatcher = Arc::new(FakeDispatcher::new());
    let sched = build_scheduler(db.clone(), workers, bus, 2, dispatcher.clone());

    sched.tick_now().await.unwrap();

    assert_eq!(
        dispatcher.calls().await.len(),
        0,
        "no slots available, no dispatch"
    );
    assert_eq!(db.count_queued().unwrap(), 1);
}

#[tokio::test]
async fn scheduler_blocks_no_eligible_worker() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let workers = build_registry(bus.clone());
    // no worker advertises the "absent" provider
    register_worker(&workers, "w1", "claude");

    enqueue(
        &db,
        &make_queued_agent("a1", Some("absent")),
        &make_queue_row("a1", "adhoc", Some("absent"), 0),
    );

    let dispatcher = Arc::new(FakeDispatcher::new());
    let sched = build_scheduler(db.clone(), workers, bus, 4, dispatcher.clone());

    sched.tick_now().await.unwrap();

    assert!(dispatcher.calls().await.is_empty());
    assert_eq!(db.count_queued().unwrap(), 1);
    let rows = db.list_queue().unwrap();
    assert_eq!(rows[0].block_reason.as_deref(), Some("no_eligible_worker"));
}

#[tokio::test]
async fn scheduler_unblocks_on_worker_registered() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let workers = build_registry(bus.clone());
    register_worker(&workers, "w1", "claude");

    enqueue(
        &db,
        &make_queued_agent("a1", Some("absent")),
        &make_queue_row("a1", "adhoc", Some("absent"), 0),
    );

    let dispatcher = Arc::new(FakeDispatcher::new());
    let sched = build_scheduler(db.clone(), workers.clone(), bus, 4, dispatcher.clone());

    sched.tick_now().await.unwrap();
    assert_eq!(dispatcher.calls().await.len(), 0);

    register_worker(&workers, "w2", "absent");

    sched.tick_now().await.unwrap();
    let calls = dispatcher.calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], "a1");
    assert_eq!(db.count_queued().unwrap(), 0);
}

#[tokio::test]
async fn scheduler_requeues_on_dispatch_failure() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let workers = build_registry(bus.clone());
    register_worker(&workers, "w1", "claude");

    let original = make_queue_row("a1", "adhoc", Some("claude"), 0);
    enqueue(&db, &make_queued_agent("a1", Some("claude")), &original);
    let original_enqueued_at = original.enqueued_at;

    let dispatcher = Arc::new(FakeDispatcher::new());
    dispatcher.fail_next();
    let sched = build_scheduler(db.clone(), workers, bus, 4, dispatcher.clone());

    sched.tick_now().await.unwrap();

    // requeued with the original enqueued_at preserved
    let rows = db.list_queue().unwrap();
    assert_eq!(rows.len(), 1, "row was requeued");
    assert_eq!(rows[0].id, "a1");
    assert_eq!(
        rows[0].enqueued_at.timestamp_millis(),
        original_enqueued_at.timestamp_millis(),
        "enqueued_at preserved"
    );
    let agent = db.get_agent("a1").unwrap().unwrap();
    assert_eq!(agent.state, AgentState::Queued);
}

#[tokio::test]
async fn scheduler_adhoc_lane_drains_first() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let workers = build_registry(bus.clone());
    register_worker(&workers, "w1", "claude");

    // scroll row enqueued first, ad-hoc later — ad-hoc still wins
    enqueue(
        &db,
        &make_queued_agent("scroll1", Some("claude")),
        &make_queue_row("scroll1", "scroll", Some("claude"), 0),
    );
    enqueue(
        &db,
        &make_queued_agent("adhoc1", Some("claude")),
        &make_queue_row("adhoc1", "adhoc", Some("claude"), 1),
    );

    let dispatcher = Arc::new(FakeDispatcher::new());
    let sched = build_scheduler(db.clone(), workers, bus, 1, dispatcher.clone());

    sched.tick_now().await.unwrap();

    let calls = dispatcher.calls().await;
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0], "adhoc1", "ad-hoc lane wins on contention");
    let remaining = db.list_queue().unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, "scroll1");
}

#[tokio::test]
async fn scheduler_idempotent_when_queue_empty() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let workers = build_registry(bus.clone());

    let dispatcher = Arc::new(FakeDispatcher::new());
    let sched = build_scheduler(db.clone(), workers, bus, 4, dispatcher.clone());

    sched.tick_now().await.unwrap();
    sched.tick_now().await.unwrap();

    assert!(dispatcher.calls().await.is_empty());
}
