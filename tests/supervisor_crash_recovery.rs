//! Contract tests for replay_pending_on_boot.

use std::sync::Arc;

use chrono::{TimeZone, Utc};

use grimoire::daemon::clock::{Clock, TestClock};
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::{
    AgentState, RestartHistoryOutcome, RestartPolicy, SupervisionConfig,
};

mod common;
use common::{build, seed};

#[tokio::test]
async fn boot_promotes_restarting_to_failed() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let mut rx = bus.subscribe();
    seed(&db, "crh00001", AgentState::Restarting);
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(now));
    let sup = build(db.clone(), bus.clone(), clock);
    sup.replay_pending_on_boot().await.unwrap();
    let agent = db.get_agent("crh00001").unwrap().unwrap();
    assert_eq!(agent.state, AgentState::Failed);
    let mut saw = false;
    while let Ok(ev) = rx.try_recv() {
        if let StreamEvent::StateChange {
            old_state,
            new_state,
            ..
        } = ev
            && old_state == AgentState::Restarting
            && new_state == AgentState::Failed
        {
            saw = true;
        }
    }
    assert!(saw);
}

#[tokio::test]
async fn boot_replays_pending_for_failed_with_active_policy() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "crh00002", AgentState::Failed);
    db.set_supervision(
        "crh00002",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(3),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(now));
    let sup = build(db.clone(), bus.clone(), clock.clone());
    sup.replay_pending_on_boot().await.unwrap();
    let snap = sup.pending_snapshot().await;
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].agent_id, "crh00002");
    assert!(snap[0].fire_at <= clock.now() + chrono::Duration::seconds(1));
}

#[tokio::test]
async fn boot_skips_replay_for_already_escalated() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "crh00003", AgentState::Failed);
    db.set_supervision(
        "crh00003",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(1),
            window_secs: Some(60),
            escalate_to: Some("topic://x".into()),
        },
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    db.insert_restart_history_row(
        "crh00003",
        now.timestamp() - 60,
        RestartHistoryOutcome::BudgetExhausted,
        None,
    )
    .unwrap();
    // Persist an Escalated event with a later timestamp.
    let _ = db.append_event(&StreamEvent::Escalated {
        agent_id: "crh00003".into(),
        target: "topic://x".into(),
        fanout_count: 0,
    });
    let clock = Arc::new(TestClock::new(now));
    let sup = build(db.clone(), bus.clone(), clock);
    sup.replay_pending_on_boot().await.unwrap();
    let snap = sup.pending_snapshot().await;
    assert!(snap.is_empty());
}

#[tokio::test]
async fn boot_skips_replay_for_policy_never() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "crh00004", AgentState::Failed);
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(now));
    let sup = build(db.clone(), bus.clone(), clock);
    sup.replay_pending_on_boot().await.unwrap();
    assert!(sup.pending_snapshot().await.is_empty());
}

#[tokio::test]
async fn boot_emits_budget_exhausted_when_window_full() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let mut rx = bus.subscribe();
    seed(&db, "crh00005", AgentState::Failed);
    db.set_supervision(
        "crh00005",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(3),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    for _ in 0..3 {
        db.insert_restart_history_row(
            "crh00005",
            now.timestamp(),
            RestartHistoryOutcome::Scheduled,
            None,
        )
        .unwrap();
    }
    let clock = Arc::new(TestClock::new(now));
    let sup = build(db.clone(), bus.clone(), clock);
    sup.replay_pending_on_boot().await.unwrap();
    assert!(sup.pending_snapshot().await.is_empty());
    let mut saw = false;
    while let Ok(ev) = rx.try_recv() {
        if let StreamEvent::RestartBudgetExhausted { reason, .. } = ev {
            assert_eq!(reason, "budget_spent");
            saw = true;
        }
    }
    assert!(saw);
}
