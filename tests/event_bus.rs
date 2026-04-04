//! Integration tests for the event bus.
//!
//! Verifies publish/subscribe behavior across multiple subscribers,
//! event filtering by agent ID, and behavior when no subscribers exist.

use grimoire::daemon::event_bus::EventBus;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::AgentState;

// ---------------------------------------------------------------------------
// Basic pub/sub: single subscriber receives published events
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_subscriber_receives_events() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe();

    let event = StreamEvent::StateChange {
        agent_id: "agent-1".to_string(),
        old_state: AgentState::Summoning,
        new_state: AgentState::Active,
    };

    bus.publish(event.clone());

    let received = rx.recv().await.unwrap();
    assert_eq!(received.agent_id(), Some("agent-1"));
}

// ---------------------------------------------------------------------------
// Multiple subscribers each receive a copy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_subscribers_receive_copies() {
    let bus = EventBus::new();
    let mut rx1 = bus.subscribe();
    let mut rx2 = bus.subscribe();
    let mut rx3 = bus.subscribe();

    bus.publish(StreamEvent::Output {
        agent_id: "a1".to_string(),
        stream: "stdout".to_string(),
        line: "hello".to_string(),
    });

    // All three should receive the event
    let e1 = rx1.recv().await.unwrap();
    let e2 = rx2.recv().await.unwrap();
    let e3 = rx3.recv().await.unwrap();

    assert_eq!(e1.agent_id(), Some("a1"));
    assert_eq!(e2.agent_id(), Some("a1"));
    assert_eq!(e3.agent_id(), Some("a1"));
}

// ---------------------------------------------------------------------------
// Publishing without subscribers doesn't panic
// ---------------------------------------------------------------------------

#[test]
fn publish_without_subscribers() {
    let bus = EventBus::new();

    // This should not panic
    bus.publish(StreamEvent::Output {
        agent_id: "orphan".to_string(),
        stream: "stdout".to_string(),
        line: "nobody listening".to_string(),
    });
}

// ---------------------------------------------------------------------------
// Events arrive in order
// ---------------------------------------------------------------------------

#[tokio::test]
async fn events_arrive_in_order() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe();

    for i in 0..10 {
        bus.publish(StreamEvent::Output {
            agent_id: "ordered".to_string(),
            stream: "stdout".to_string(),
            line: format!("line-{}", i),
        });
    }

    for i in 0..10 {
        let event = rx.recv().await.unwrap();
        if let StreamEvent::Output { line, .. } = event {
            assert_eq!(line, format!("line-{}", i));
        } else {
            panic!("Expected Output event");
        }
    }
}

// ---------------------------------------------------------------------------
// Mixed event types flow through correctly
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mixed_event_types() {
    let bus = EventBus::new();
    let mut rx = bus.subscribe();

    bus.publish(StreamEvent::Output {
        agent_id: "a1".to_string(),
        stream: "stdout".to_string(),
        line: "working".to_string(),
    });

    bus.publish(StreamEvent::StateChange {
        agent_id: "a1".to_string(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    });

    bus.publish(StreamEvent::ScrollProgress {
        scroll_id: "s1".to_string(),
        total: 5,
        complete: 3,
        active: 1,
        blocked: 1,
        failed: 0,
        skipped: 0,
    });

    let e1 = rx.recv().await.unwrap();
    assert!(matches!(e1, StreamEvent::Output { .. }));

    let e2 = rx.recv().await.unwrap();
    assert!(matches!(e2, StreamEvent::StateChange { .. }));

    let e3 = rx.recv().await.unwrap();
    assert!(matches!(e3, StreamEvent::ScrollProgress { .. }));
    assert_eq!(e3.agent_id(), None); // scroll events have no agent_id
}

// ---------------------------------------------------------------------------
// Late subscriber doesn't receive earlier events
// ---------------------------------------------------------------------------

#[tokio::test]
async fn late_subscriber_misses_earlier_events() {
    let bus = EventBus::new();

    // Publish before subscribing
    bus.publish(StreamEvent::Output {
        agent_id: "early".to_string(),
        stream: "stdout".to_string(),
        line: "you missed this".to_string(),
    });

    let mut rx = bus.subscribe();

    // Publish after subscribing
    bus.publish(StreamEvent::Output {
        agent_id: "late".to_string(),
        stream: "stdout".to_string(),
        line: "you got this".to_string(),
    });

    let event = rx.recv().await.unwrap();
    assert_eq!(event.agent_id(), Some("late"));
}
