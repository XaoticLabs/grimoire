//! Contract tests for Supervisor::on_state_change, idempotency,
//! cancel_pending, drain_due.

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
async fn failed_event_writes_history_row_and_flips_to_restarting() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let mut rx = bus.subscribe();
    seed(&db, "act00001", AgentState::Failed);
    db.set_supervision(
        "act00001",
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
    let sup = build(db.clone(), bus.clone(), clock);
    sup.on_state_change("act00001", AgentState::Failed)
        .await
        .unwrap();

    let count = db.count_restarts_in_window("act00001", 0).unwrap();
    assert_eq!(count, 1);

    let agent = db.get_agent("act00001").unwrap().unwrap();
    assert_eq!(agent.state, AgentState::Restarting);

    let mut saw_state_change = false;
    let mut saw_scheduled = false;
    while let Ok(ev) = rx.try_recv() {
        match ev {
            StreamEvent::StateChange {
                old_state,
                new_state,
                ..
            } if old_state == AgentState::Failed && new_state == AgentState::Restarting => {
                saw_state_change = true;
            }
            StreamEvent::RestartScheduled { .. } => saw_scheduled = true,
            _ => {}
        }
    }
    assert!(saw_state_change);
    assert!(saw_scheduled);
}

#[tokio::test]
async fn policy_never_is_silent() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let mut rx = bus.subscribe();
    seed(&db, "act00002", AgentState::Failed);
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(now));
    let sup = build(db.clone(), bus.clone(), clock);
    sup.on_state_change("act00002", AgentState::Failed)
        .await
        .unwrap();
    let count = db.count_restarts_in_window("act00002", 0).unwrap();
    assert_eq!(count, 0);
    let agent = db.get_agent("act00002").unwrap().unwrap();
    assert_eq!(agent.state, AgentState::Failed);
    while let Ok(ev) = rx.try_recv() {
        if matches!(
            ev,
            StreamEvent::RestartScheduled { .. } | StreamEvent::RestartBudgetExhausted { .. }
        ) {
            panic!("unexpected supervisor event");
        }
    }
}

#[tokio::test]
async fn duplicate_failed_for_restarting_agent_is_noop() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let mut rx = bus.subscribe();
    seed(&db, "act00003", AgentState::Failed);
    db.set_supervision(
        "act00003",
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
    let sup = build(db.clone(), bus.clone(), clock);

    sup.on_state_change("act00003", AgentState::Failed)
        .await
        .unwrap();
    sup.on_state_change("act00003", AgentState::Failed)
        .await
        .unwrap();

    let mut scheduled_count = 0;
    while let Ok(ev) = rx.try_recv() {
        if matches!(ev, StreamEvent::RestartScheduled { .. }) {
            scheduled_count += 1;
        }
    }
    assert_eq!(scheduled_count, 1);
}

#[tokio::test]
async fn budget_exhausted_publishes_event_and_leaves_agent_failed() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let mut rx = bus.subscribe();
    seed(&db, "act00004", AgentState::Failed);
    db.set_supervision(
        "act00004",
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
            "act00004",
            now.timestamp(),
            RestartHistoryOutcome::Scheduled,
            None,
        )
        .unwrap();
    }
    let clock = Arc::new(TestClock::new(now));
    let sup = build(db.clone(), bus.clone(), clock);
    sup.on_state_change("act00004", AgentState::Failed)
        .await
        .unwrap();

    let agent = db.get_agent("act00004").unwrap().unwrap();
    assert_eq!(agent.state, AgentState::Failed);

    let mut got_exhausted = false;
    while let Ok(ev) = rx.try_recv() {
        if let StreamEvent::RestartBudgetExhausted { reason, .. } = ev {
            assert_eq!(reason, "budget_spent");
            got_exhausted = true;
        }
    }
    assert!(got_exhausted);
}

#[tokio::test]
async fn cancel_pending_removes_entries() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "act00005", AgentState::Failed);
    seed(&db, "act00006", AgentState::Failed);
    db.set_supervision(
        "act00005",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(3),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    db.set_supervision(
        "act00006",
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
    let sup = build(db.clone(), bus.clone(), clock);
    // two pending entries for A, one for B
    sup.schedule_restart("act00005", 1, now + chrono::Duration::seconds(2), false)
        .await
        .unwrap();
    // re-set state so a second schedule for A bypasses the pending guard
    db.update_agent_state("act00005", &AgentState::Failed, None)
        .unwrap();
    sup.schedule_restart("act00005", 2, now + chrono::Duration::seconds(4), false)
        .await
        .unwrap();
    sup.schedule_restart("act00006", 1, now + chrono::Duration::seconds(2), false)
        .await
        .unwrap();
    let removed = sup.cancel_pending("act00005").await.unwrap();
    assert_eq!(removed, 2);
    let snap = sup.pending_snapshot().await;
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].agent_id, "act00006");
}

#[tokio::test]
async fn drain_due_pops_only_due_entries() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    seed(&db, "act00007", AgentState::Failed);
    seed(&db, "act00008", AgentState::Failed);
    db.set_supervision(
        "act00007",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(3),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    db.set_supervision(
        "act00008",
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
    sup.schedule_restart("act00007", 1, now + chrono::Duration::seconds(2), false)
        .await
        .unwrap();
    sup.schedule_restart("act00008", 1, now + chrono::Duration::seconds(10), false)
        .await
        .unwrap();
    clock.advance(chrono::Duration::seconds(5));
    let due = sup.drain_due(clock.now()).await;
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].agent_id, "act00007");
    let snap = sup.pending_snapshot().await;
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].agent_id, "act00008");
}
