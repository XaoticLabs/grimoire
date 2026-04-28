//! Task 4 contract tests: escalation mail + depth propagation.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use tokio::sync::Mutex;

use grimoire::daemon::clock::TestClock;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::{Database, unix_now};
use grimoire::daemon::supervisor::{
    DbEscalationMailSender, EscalationMailSender, EscalationOutcome, Supervisor,
};
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{
    Agent, AgentState, RestartHistoryOutcome, RestartPolicy, Subscription, SupervisionConfig,
};

#[derive(Default)]
struct RecordingMail {
    calls: Mutex<Vec<(String, String, String)>>, // sender, target, body
}

#[async_trait]
impl EscalationMailSender for RecordingMail {
    async fn send_escalation(
        &self,
        sender_id: &str,
        target: &str,
        body: &str,
    ) -> Result<EscalationOutcome> {
        self.calls
            .lock()
            .await
            .push((sender_id.to_string(), target.to_string(), body.to_string()));
        Ok(EscalationOutcome {
            fanout_count: 1,
            recipient_ids: vec!["recipnt1".to_string()],
        })
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
        session_id: None,
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

#[tokio::test]
async fn escalate_to_agent_writes_one_mail_with_supervisor_sender() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "afail001", AgentState::Failed);
    seed(&db, "bbcd0001", AgentState::Active);
    db.set_supervision(
        "afail001",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(2),
            window_secs: Some(60),
            escalate_to: Some("agent://bbcd0001".into()),
        },
    )
    .unwrap();
    // Fill budget so next Failed exhausts.
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    for _ in 0..2 {
        db.insert_restart_history_row(
            "afail001",
            now.timestamp(),
            RestartHistoryOutcome::Scheduled,
            None,
        )
        .unwrap();
    }
    let clock = Arc::new(TestClock::new(now));
    let mail: Arc<dyn EscalationMailSender> = Arc::new(DbEscalationMailSender {
        db: db.clone(),
        bus: bus.clone(),
    });
    let sup = Supervisor::new(db.clone(), bus.clone(), clock, 30, 3, mail);
    sup.on_state_change("afail001", AgentState::Failed)
        .await
        .unwrap();

    // Find the mail row written for recipient bbcd0001
    let mails = db
        .list_mail_by_recipient("bbcd0001", None, None, 100)
        .unwrap();
    assert_eq!(mails.len(), 1);
    assert_eq!(mails[0].sender_id.as_deref(), Some("supervisor://afail001"));
    assert!(
        mails[0]
            .body
            .starts_with("[supervisor] agent afail001 failed")
    );
}

#[tokio::test]
async fn escalate_to_topic_fanout_writes_per_subscriber() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "afail002", AgentState::Failed);
    seed(&db, "subA0001", AgentState::Active);
    seed(&db, "subB0001", AgentState::Active);
    db.insert_subscription(&Subscription {
        id: "sub00001".into(),
        subscriber_id: "subA0001".into(),
        topic: "esc-topic".into(),
        created_at: unix_now(),
    })
    .unwrap();
    db.insert_subscription(&Subscription {
        id: "sub00002".into(),
        subscriber_id: "subB0001".into(),
        topic: "esc-topic".into(),
        created_at: unix_now(),
    })
    .unwrap();
    db.set_supervision(
        "afail002",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(1),
            window_secs: Some(60),
            escalate_to: Some("topic://esc-topic".into()),
        },
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    db.insert_restart_history_row(
        "afail002",
        now.timestamp(),
        RestartHistoryOutcome::Scheduled,
        None,
    )
    .unwrap();
    let clock = Arc::new(TestClock::new(now));
    let mail: Arc<dyn EscalationMailSender> = Arc::new(DbEscalationMailSender {
        db: db.clone(),
        bus: bus.clone(),
    });
    let mut rx = bus.subscribe();
    let sup = Supervisor::new(db.clone(), bus.clone(), clock, 30, 3, mail);
    sup.on_state_change("afail002", AgentState::Failed)
        .await
        .unwrap();
    let mails_a = db
        .list_mail_by_recipient("subA0001", None, None, 100)
        .unwrap();
    let mails_b = db
        .list_mail_by_recipient("subB0001", None, None, 100)
        .unwrap();
    assert_eq!(mails_a.len(), 1);
    assert_eq!(mails_b.len(), 1);
    let mut got = false;
    while let Ok(ev) = rx.try_recv() {
        if let StreamEvent::Escalated { fanout_count, .. } = ev {
            assert_eq!(fanout_count, 2);
            got = true;
        }
    }
    assert!(got);
}

#[tokio::test]
async fn escalate_to_topic_with_no_subscribers_emits_event_with_zero_fanout() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "afail003", AgentState::Failed);
    db.set_supervision(
        "afail003",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(1),
            window_secs: Some(60),
            escalate_to: Some("topic://empty".into()),
        },
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    db.insert_restart_history_row(
        "afail003",
        now.timestamp(),
        RestartHistoryOutcome::Scheduled,
        None,
    )
    .unwrap();
    let clock = Arc::new(TestClock::new(now));
    let mail: Arc<dyn EscalationMailSender> = Arc::new(DbEscalationMailSender {
        db: db.clone(),
        bus: bus.clone(),
    });
    let mut rx = bus.subscribe();
    let sup = Supervisor::new(db.clone(), bus.clone(), clock, 30, 3, mail);
    sup.on_state_change("afail003", AgentState::Failed)
        .await
        .unwrap();
    let mut got = false;
    while let Ok(ev) = rx.try_recv() {
        if let StreamEvent::Escalated {
            fanout_count,
            target,
            ..
        } = ev
        {
            assert_eq!(fanout_count, 0);
            assert_eq!(target, "topic://empty");
            got = true;
        }
    }
    assert!(got);
}

#[tokio::test]
async fn budget_exhausted_without_escalate_to_emits_only_restart_budget_exhausted() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "afail004", AgentState::Failed);
    db.set_supervision(
        "afail004",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(1),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    db.insert_restart_history_row(
        "afail004",
        now.timestamp(),
        RestartHistoryOutcome::Scheduled,
        None,
    )
    .unwrap();
    let clock = Arc::new(TestClock::new(now));
    let mail: Arc<dyn EscalationMailSender> = Arc::new(DbEscalationMailSender {
        db: db.clone(),
        bus: bus.clone(),
    });
    let mut rx = bus.subscribe();
    let sup = Supervisor::new(db.clone(), bus.clone(), clock, 30, 3, mail);
    sup.on_state_change("afail004", AgentState::Failed)
        .await
        .unwrap();
    let mut saw_escalated = false;
    let mut saw_exhausted = false;
    while let Ok(ev) = rx.try_recv() {
        match ev {
            StreamEvent::Escalated { .. } => saw_escalated = true,
            StreamEvent::RestartBudgetExhausted { .. } => saw_exhausted = true,
            _ => {}
        }
    }
    assert!(saw_exhausted);
    assert!(!saw_escalated);
}

#[tokio::test]
async fn tree_depth_exceeded_does_not_escalate() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "afail005", AgentState::Failed);
    seed(&db, "bbcd0005", AgentState::Active);
    db.set_supervision(
        "afail005",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(99),
            window_secs: Some(60),
            escalate_to: Some("agent://bbcd0005".into()),
        },
    )
    .unwrap();
    db.set_escalation_depth("afail005", 3).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(now));
    let mail: Arc<dyn EscalationMailSender> = Arc::new(DbEscalationMailSender {
        db: db.clone(),
        bus: bus.clone(),
    });
    let mut rx = bus.subscribe();
    let sup = Supervisor::new(db.clone(), bus.clone(), clock, 30, 3, mail);
    sup.on_state_change("afail005", AgentState::Failed)
        .await
        .unwrap();
    let mails = db
        .list_mail_by_recipient("bbcd0005", None, None, 100)
        .unwrap();
    assert_eq!(mails.len(), 0);
    while let Ok(ev) = rx.try_recv() {
        if matches!(ev, StreamEvent::Escalated { .. }) {
            panic!("did not expect Escalated event for tree_depth_exceeded");
        }
    }
}

#[tokio::test]
async fn escalation_propagates_depth_plus_one() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "afail006", AgentState::Failed);
    seed(&db, "bbcd0006", AgentState::Active);
    db.set_supervision(
        "afail006",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(1),
            window_secs: Some(60),
            escalate_to: Some("agent://bbcd0006".into()),
        },
    )
    .unwrap();
    db.set_escalation_depth("afail006", 1).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    db.insert_restart_history_row(
        "afail006",
        now.timestamp(),
        RestartHistoryOutcome::Scheduled,
        None,
    )
    .unwrap();
    let clock = Arc::new(TestClock::new(now));
    let mail: Arc<dyn EscalationMailSender> = Arc::new(DbEscalationMailSender {
        db: db.clone(),
        bus: bus.clone(),
    });
    let sup = Supervisor::new(db.clone(), bus.clone(), clock, 30, 3, mail);
    sup.on_state_change("afail006", AgentState::Failed)
        .await
        .unwrap();
    let depth = db.get_escalation_depth("bbcd0006").unwrap();
    assert_eq!(depth, 2);
}
