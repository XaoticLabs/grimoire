//! Scheduler mail-wake branch tests.
//!
//! Drives `Scheduler::tick_now()` after configuring the scheduler with a
//! recording `MailWaker` so we can assert what got invoked, with what
//! prompt, and how many slots were consumed.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Mutex;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::executor::{ExecuteRequest, Executor, ExecutorHandle};
use grimoire::daemon::persistence::Database;
use grimoire::daemon::process_manager::MonitorResult;
use grimoire::daemon::scheduler::{AgentStateLookup, Dispatcher, MailWaker, Scheduler};
use grimoire::daemon::worker_registry::WorkerRegistry;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{Agent, AgentId, AgentState, Mail, MailState};

/// Records every wake() call.
#[derive(Default)]
struct RecordingWaker {
    calls: Mutex<Vec<(String, String)>>,
    fail_next: Mutex<bool>,
}

#[async_trait]
impl MailWaker for RecordingWaker {
    async fn wake(&self, agent_id: &str, prompt: &str) -> Result<()> {
        if *self.fail_next.lock().await {
            anyhow::bail!("synthetic invoke failure");
        }
        self.calls
            .lock()
            .await
            .push((agent_id.to_string(), prompt.to_string()));
        Ok(())
    }
}

/// Reads state directly from the DB.
struct DbLookup(Arc<Database>);

impl AgentStateLookup for DbLookup {
    fn get_state_and_session(&self, id: &str) -> Result<Option<(AgentState, Option<String>)>> {
        Ok(self.0.get_agent(id)?.map(|a| (a.state, a.session_id)))
    }
}

/// No-op dispatcher — mail-wake tests don't exercise the queue path.
#[derive(Default)]
struct NoopDispatcher;

#[async_trait]
impl Dispatcher for NoopDispatcher {
    async fn dispatch(&self, _row: grimoire::daemon::persistence::QueueRow) -> Result<()> {
        Ok(())
    }
}

/// Unused executor stub (Scheduler doesn't talk to one directly).
#[derive(Default)]
#[allow(dead_code)]
struct StubExecutor;

#[async_trait]
impl Executor for StubExecutor {
    async fn start(&self, _req: ExecuteRequest) -> Result<ExecutorHandle> {
        let (cancel_tx, _cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let completion = tokio::spawn(async move {
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
                let _ = cancel_tx.send(());
            }),
            completion,
        })
    }
    fn name(&self) -> &'static str {
        "stub"
    }
}

fn seed_dormant_with_session(db: &Database, id: &str, session_id: Option<&str>) -> AgentId {
    // After T1, the wake-mail filter requires Dormant agents. A Dormant
    // agent always has a session_id in production; for the "no session"
    // negative test we still seed Complete (which is *not* a wake candidate
    // either, exercising the same skip path).
    let state = if session_id.is_some() {
        AgentState::Dormant
    } else {
        AgentState::Complete
    };
    let agent = Agent {
        id: id.to_string(),
        name: None,
        state,
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: session_id.map(std::string::ToString::to_string),
        exit_code: Some(0),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
    if let Some(sid) = session_id {
        db.update_agent_session_id(id, sid).unwrap();
    }
    id.to_string()
}

fn seed_complete_with_session(db: &Database, id: &str, session_id: Option<&str>) -> AgentId {
    seed_dormant_with_session(db, id, session_id)
}

fn make_pending_mail(id: &str, recipient: &str, body: &str, wake_eligible: bool) -> Mail {
    Mail {
        id: id.to_string(),
        recipient_id: recipient.to_string(),
        sender_id: None,
        topic: None,
        body: body.to_string(),
        in_reply_to: None,
        state: MailState::Pending,
        fail_reason: None,
        created_at: Utc::now().timestamp(),
        delivered_at: None,
        seq: 0,
        wake_eligible,
    }
}

fn build_scheduler(
    db: Arc<Database>,
    bus: EventBus,
    waker: Arc<RecordingWaker>,
    cap: u32,
) -> Arc<Scheduler> {
    let workers = Arc::new(WorkerRegistry::new_with_bus(
        Duration::from_mins(1),
        bus.clone(),
    ));
    let cap = Arc::new(AtomicU32::new(cap));
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(NoopDispatcher);
    let s = Scheduler::new(db.clone(), workers, bus, cap, dispatcher)
        .with_mail_wake(waker, Arc::new(DbLookup(db)));
    Arc::new(s)
}

#[tokio::test]
async fn complete_agent_with_pending_mail_is_woken() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let waker = Arc::new(RecordingWaker::default());

    let agent_id = seed_complete_with_session(&db, "wake0001", Some("sess-1"));
    db.insert_mail(&make_pending_mail("mail0001", &agent_id, "hello!", true))
        .unwrap();

    let sched = build_scheduler(db.clone(), bus, waker.clone(), 4);
    sched.tick_now().await.unwrap();

    let calls = waker.calls.lock().await.clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, agent_id);
    assert_eq!(calls[0].1, "hello!");

    let mail = db.get_mail("mail0001").unwrap().unwrap();
    assert_eq!(mail.state, MailState::Delivered);
}

#[tokio::test]
async fn complete_without_session_is_skipped() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let waker = Arc::new(RecordingWaker::default());
    let id = seed_complete_with_session(&db, "noses001", None);
    db.insert_mail(&make_pending_mail("mailns01", &id, "x", true))
        .unwrap();

    let sched = build_scheduler(db.clone(), bus, waker.clone(), 4);
    sched.tick_now().await.unwrap();

    assert!(waker.calls.lock().await.is_empty());
    assert_eq!(
        db.get_mail("mailns01").unwrap().unwrap().state,
        MailState::Pending
    );
}

#[tokio::test]
async fn wake_eligible_zero_does_not_wake() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let waker = Arc::new(RecordingWaker::default());
    let id = seed_complete_with_session(&db, "noeli001", Some("sess-2"));
    db.insert_mail(&make_pending_mail("mailne01", &id, "x", false))
        .unwrap();

    let sched = build_scheduler(db.clone(), bus, waker.clone(), 4);
    sched.tick_now().await.unwrap();

    assert!(waker.calls.lock().await.is_empty());
    assert_eq!(
        db.get_mail("mailne01").unwrap().unwrap().state,
        MailState::Pending
    );
}

#[tokio::test]
async fn multiple_pending_mails_are_folded_into_single_invoke() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let waker = Arc::new(RecordingWaker::default());
    let id = seed_complete_with_session(&db, "fold0001", Some("sess-3"));
    for (i, body) in ["alpha", "beta", "gamma"].iter().enumerate() {
        let mail_id = format!("mfld{i:04}");
        db.insert_mail(&make_pending_mail(&mail_id, &id, body, true))
            .unwrap();
    }

    let sched = build_scheduler(db.clone(), bus, waker.clone(), 4);
    sched.tick_now().await.unwrap();

    let calls = waker.calls.lock().await.clone();
    assert_eq!(calls.len(), 1, "expected a single fused invoke");
    let prompt = &calls[0].1;
    assert!(prompt.contains("alpha"));
    assert!(prompt.contains("beta"));
    assert!(prompt.contains("gamma"));
    assert!(prompt.contains("---"));

    for i in 0..3 {
        let mail_id = format!("mfld{i:04}");
        let m = db.get_mail(&mail_id).unwrap().unwrap();
        assert_eq!(m.state, MailState::Delivered);
    }
}

#[tokio::test]
async fn invoke_failure_leaves_mail_pending() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let waker = Arc::new(RecordingWaker::default());
    *waker.fail_next.lock().await = true;
    let id = seed_complete_with_session(&db, "errs0001", Some("sess-x"));
    db.insert_mail(&make_pending_mail("mailerr1", &id, "x", true))
        .unwrap();

    let sched = build_scheduler(db.clone(), bus, waker.clone(), 4);
    sched.tick_now().await.unwrap();

    assert_eq!(
        db.get_mail("mailerr1").unwrap().unwrap().state,
        MailState::Pending
    );
}

#[tokio::test]
async fn dormant_agent_with_pending_mail_is_woken() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let waker = Arc::new(RecordingWaker::default());

    let agent_id = seed_dormant_with_session(&db, "drmnt001", Some("sess-d"));
    db.insert_mail(&make_pending_mail("maildrm1", &agent_id, "wakey", true))
        .unwrap();

    let sched = build_scheduler(db.clone(), bus, waker.clone(), 4);
    sched.tick_now().await.unwrap();

    let calls = waker.calls.lock().await.clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, agent_id);
    assert_eq!(calls[0].1, "wakey");
}

#[tokio::test]
async fn complete_agent_no_longer_woken_by_mail() {
    // After T1, the scheduler's mail-wake filter requires Dormant. A pure
    // Complete agent (even with session_id) is not a wake candidate.
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let waker = Arc::new(RecordingWaker::default());
    let id = "compl001".to_string();
    let agent = Agent {
        id: id.clone(),
        name: None,
        state: AgentState::Complete, // explicitly NOT Dormant
        task: Some("seed".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: Some("sess-c".into()),
        exit_code: Some(0),
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
    db.update_agent_session_id(&id, "sess-c").unwrap();
    db.insert_mail(&make_pending_mail("compmail1", &id, "x", true))
        .unwrap();

    let sched = build_scheduler(db.clone(), bus, waker.clone(), 4);
    sched.tick_now().await.unwrap();

    assert!(waker.calls.lock().await.is_empty());
    assert_eq!(
        db.get_mail("compmail1").unwrap().unwrap().state,
        MailState::Pending
    );
}

#[tokio::test]
async fn should_wake_returns_true_for_mail_received() {
    let ev = StreamEvent::MailReceived {
        mail_id: "x".into(),
        recipient_id: "r".into(),
        sender_id: None,
        topic: None,
        body_preview: "hi".into(),
        wake_eligible: true,
        origin_daemon_id: None,
    };
    assert!(Scheduler::should_wake(&ev));
}
