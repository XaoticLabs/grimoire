//! Contract tests for Supervisor::evaluate decision tree.

use std::sync::Arc;

use chrono::{TimeZone, Utc};

use grimoire::daemon::clock::TestClock;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::supervisor::{EscalationMailSender, RestartDecision, Supervisor};
use grimoire::shared::types::{
    AgentState, RestartHistoryOutcome, RestartPolicy, SupervisionConfig,
};

mod common;
use common::{NoopMail, seed};

fn seed_failed(db: &Database, id: &str) {
    seed(db, id, AgentState::Failed);
}

fn build_supervisor(
    db: Arc<Database>,
    clock: Arc<TestClock>,
    rate_per_min: u32,
    tree_depth_cap: u32,
) -> Arc<Supervisor> {
    let bus = EventBus::new(db.clone());
    let mail: Arc<dyn EscalationMailSender> = Arc::new(NoopMail);
    Supervisor::new(db, bus, clock, rate_per_min, tree_depth_cap, mail)
}

#[tokio::test]
async fn evaluate_policy_never_returns_not_supervised() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    seed_failed(&db, "abcd0001");
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(now));
    let sup = build_supervisor(db.clone(), clock, 30, 3);
    let d = sup.evaluate("abcd0001").await.unwrap();
    assert!(matches!(d, RestartDecision::NotSupervised));
}

#[tokio::test]
async fn evaluate_with_budget_returns_restart_at_2s() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    seed_failed(&db, "abcd0002");
    db.set_supervision(
        "abcd0002",
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
    let sup = build_supervisor(db.clone(), clock, 30, 3);
    let d = sup.evaluate("abcd0002").await.unwrap();
    match d {
        RestartDecision::Restart {
            attempt,
            fire_at,
            rate_limited,
        } => {
            assert_eq!(attempt, 1);
            assert_eq!(fire_at, now + chrono::Duration::seconds(2));
            assert!(!rate_limited);
        }
        _ => panic!("expected Restart, got {d:?}"),
    }
}

#[tokio::test]
async fn evaluate_at_budget_returns_budget_exhausted() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    seed_failed(&db, "abcd0003");
    db.set_supervision(
        "abcd0003",
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
            "abcd0003",
            now.timestamp(),
            RestartHistoryOutcome::Scheduled,
            None,
        )
        .unwrap();
    }
    let clock = Arc::new(TestClock::new(now));
    let sup = build_supervisor(db.clone(), clock, 30, 3);
    let d = sup.evaluate("abcd0003").await.unwrap();
    match d {
        RestartDecision::BudgetExhausted { reason } => assert_eq!(reason, "budget_spent"),
        _ => panic!("expected BudgetExhausted, got {d:?}"),
    }
}

#[tokio::test]
async fn evaluate_window_slides_per_failure() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    seed_failed(&db, "abcd0004");
    db.set_supervision(
        "abcd0004",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(3),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    // three rows 70s ago, outside the 60s window
    for _ in 0..3 {
        db.insert_restart_history_row(
            "abcd0004",
            (now - chrono::Duration::seconds(70)).timestamp(),
            RestartHistoryOutcome::Scheduled,
            None,
        )
        .unwrap();
    }
    let clock = Arc::new(TestClock::new(now));
    let sup = build_supervisor(db.clone(), clock, 30, 3);
    let d = sup.evaluate("abcd0004").await.unwrap();
    match d {
        RestartDecision::Restart { attempt, .. } => assert_eq!(attempt, 1),
        _ => panic!("expected Restart after window slid"),
    }
}

#[tokio::test]
async fn evaluate_tree_depth_exceeded_takes_precedence() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    seed_failed(&db, "abcd0005");
    db.set_supervision(
        "abcd0005",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(99),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    db.set_escalation_depth("abcd0005", 3).unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(now));
    let sup = build_supervisor(db.clone(), clock, 30, 3);
    let d = sup.evaluate("abcd0005").await.unwrap();
    match d {
        RestartDecision::BudgetExhausted { reason } => {
            assert_eq!(reason, "tree_depth_exceeded");
        }
        _ => panic!("expected BudgetExhausted tree_depth_exceeded"),
    }
}

#[tokio::test]
async fn evaluate_rate_limited_delays_60s() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    seed_failed(&db, "abcd0006");
    db.set_supervision(
        "abcd0006",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(99),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = Arc::new(TestClock::new(now));
    // rate_per_min=1: the first call spends the only token
    let sup = build_supervisor(db.clone(), clock, 1, 3);
    let _ = sup.evaluate("abcd0006").await.unwrap();
    seed_failed(&db, "abcd0007");
    db.set_supervision(
        "abcd0007",
        &SupervisionConfig {
            policy: RestartPolicy::OnFailure,
            max_restarts: Some(99),
            window_secs: Some(60),
            escalate_to: None,
        },
    )
    .unwrap();
    let d = sup.evaluate("abcd0007").await.unwrap();
    match d {
        RestartDecision::Restart {
            fire_at,
            rate_limited,
            ..
        } => {
            assert!(rate_limited);
            assert_eq!(fire_at, now + chrono::Duration::seconds(60));
        }
        _ => panic!("expected rate-limited Restart"),
    }
}
