//! Task 1 contract tests: new StreamEvent variants.

use grimoire::shared::protocol::StreamEvent;

#[test]
fn restart_scheduled_event_kind_and_id() {
    let ev = StreamEvent::RestartScheduled {
        agent_id: "abc12345".into(),
        attempt: 1,
        max: 3,
        fire_at_unix: 0,
        rate_limited: false,
    };
    assert_eq!(ev.kind(), "restart_scheduled");
    assert_eq!(ev.agent_id(), Some("abc12345"));
}

#[test]
fn escalated_event_kind_and_id() {
    let ev = StreamEvent::Escalated {
        agent_id: "abc12345".into(),
        target: "topic://x".into(),
        fanout_count: 1,
    };
    assert_eq!(ev.kind(), "escalated");
    assert_eq!(ev.agent_id(), Some("abc12345"));
}

#[test]
fn restart_budget_exhausted_event_kind_and_id() {
    let ev = StreamEvent::RestartBudgetExhausted {
        agent_id: "abc12345".into(),
        reason: "budget_spent".into(),
    };
    assert_eq!(ev.kind(), "restart_budget_exhausted");
    assert_eq!(ev.agent_id(), Some("abc12345"));
}

#[test]
fn restarted_event_kind_and_id() {
    let ev = StreamEvent::Restarted {
        agent_id: "abc12345".into(),
        attempt: 2,
        mail_id: None,
    };
    assert_eq!(ev.kind(), "restarted");
    assert_eq!(ev.agent_id(), Some("abc12345"));
}
