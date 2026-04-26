//! Integration tests for the event bus.
//!
//! Verifies publish/subscribe behavior across multiple subscribers,
//! event filtering by agent ID, and behavior when no subscribers exist.

use std::sync::Arc;
use std::time::{Duration, Instant};

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::AgentState;

fn fresh_bus() -> EventBus {
    let db = Arc::new(Database::open_in_memory().unwrap());
    EventBus::new(db)
}

fn fresh_bus_with_db() -> (EventBus, Arc<Database>) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    (EventBus::new(db.clone()), db)
}

/// Poll a closure returning a count until it reaches `target` or `timeout`
/// elapses. Returns the last observed count.
async fn poll_until(target: i64, timeout: Duration, mut probe: impl FnMut() -> i64) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        let n = probe();
        if n >= target || Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Basic pub/sub: single subscriber receives published events
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_subscriber_receives_events() {
    let bus = fresh_bus();
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
    let bus = fresh_bus();
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

#[tokio::test]
async fn publish_without_subscribers() {
    let bus = fresh_bus();

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
    let bus = fresh_bus();
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
    let bus = fresh_bus();
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
    let bus = fresh_bus();

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

// ---------------------------------------------------------------------------
// Durable event log — writer task wired into EventBus (Task 3)
// ---------------------------------------------------------------------------

fn count_events(db: &Database) -> i64 {
    db.with_test_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM events", [], |r| r.get::<_, i64>(0))
            .unwrap()
    })
}

#[tokio::test]
async fn publish_persists_to_database() {
    let (bus, db) = fresh_bus_with_db();

    let events = [
        StreamEvent::Output {
            agent_id: "A".to_string(),
            stream: "stdout".to_string(),
            line: "hi".to_string(),
        },
        StreamEvent::StateChange {
            agent_id: "A".to_string(),
            old_state: AgentState::Summoning,
            new_state: AgentState::Active,
        },
        StreamEvent::ScrollProgress {
            scroll_id: "S".to_string(),
            total: 1, complete: 0, active: 1, blocked: 0, failed: 0, skipped: 0,
        },
    ];
    for ev in &events {
        bus.publish(ev.clone());
    }

    let n = poll_until(events.len() as i64, Duration::from_secs(2), || count_events(&db)).await;
    assert_eq!(n, events.len() as i64);
}

#[tokio::test]
async fn broadcast_subscribers_still_receive_with_persistence_enabled() {
    let bus = fresh_bus();
    let mut rx = bus.subscribe();
    bus.publish(StreamEvent::Output {
        agent_id: "a".into(),
        stream: "stdout".into(),
        line: "x".into(),
    });
    let received = rx.recv().await.unwrap();
    assert_eq!(received.agent_id(), Some("a"));
}

#[tokio::test]
async fn publish_is_non_blocking() {
    let bus = fresh_bus();
    let start = Instant::now();
    for i in 0..1000 {
        bus.publish(StreamEvent::Output {
            agent_id: "burst".into(),
            stream: "stdout".into(),
            line: format!("{}", i),
        });
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "publish loop took {:?}, expected < 100ms",
        elapsed
    );
}

#[tokio::test]
async fn mixed_variants_all_persisted() {
    let (bus, db) = fresh_bus_with_db();

    bus.publish(StreamEvent::Output {
        agent_id: "A".into(),
        stream: "stdout".into(),
        line: "x".into(),
    });
    bus.publish(StreamEvent::StateChange {
        agent_id: "A".into(),
        old_state: AgentState::Summoning,
        new_state: AgentState::Active,
    });
    bus.publish(StreamEvent::ScrollProgress {
        scroll_id: "S".into(),
        total: 1, complete: 0, active: 1, blocked: 0, failed: 0, skipped: 0,
    });
    bus.publish(StreamEvent::TaskStateChange {
        scroll_id: "S".into(),
        task_id: "t".into(),
        task_name: "T".into(),
        old_state: grimoire::shared::types::TaskState::Blocked,
        new_state: grimoire::shared::types::TaskState::Active,
    });

    poll_until(4, Duration::from_secs(2), || count_events(&db)).await;

    let kinds: Vec<String> = db.with_test_conn(|c| {
        let mut stmt = c.prepare("SELECT DISTINCT kind FROM events ORDER BY kind").unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    });
    assert_eq!(
        kinds,
        vec![
            "output".to_string(),
            "scroll_progress".to_string(),
            "state_change".to_string(),
            "task_state_change".to_string(),
        ]
    );
}

// ---------------------------------------------------------------------------
// Restart recovery publishes a StateChange event per failed agent (Task 4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_recovery_publishes_state_change_for_each_failure() {
    use chrono::Utc;
    use grimoire::daemon::agent_manager::AgentManager;
    use grimoire::shared::config::Config;
    use grimoire::shared::types::Agent;
    use std::path::PathBuf;

    let db = Arc::new(Database::open_in_memory().unwrap());

    // Seed two Active agents; AgentManager::new should flip them to Failed
    // during recovery and emit one StateChange event per agent.
    for id in ["recact01", "recact02"] {
        db.insert_agent(&Agent {
            id: id.to_string(),
            name: None,
            state: AgentState::Active,
            task: Some("t".into()),
            model: None,
            provider: None,
            cwd: PathBuf::from("/tmp"),
            pid: None,
            session_id: None,
            exit_code: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            worker_id: None,
        })
        .unwrap();
    }

    let bus = EventBus::new(db.clone());
    let mut rx = bus.subscribe();

    let _manager = AgentManager::new(db.clone(), bus.clone(), Config::default()).await;

    // Drain up to a small budget of events; collect StateChange ones.
    let mut state_changes: Vec<(String, AgentState, AgentState)> = Vec::new();
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline && state_changes.len() < 2 {
        match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Ok(StreamEvent::StateChange { agent_id, old_state, new_state })) => {
                state_changes.push((agent_id, old_state, new_state));
            }
            Ok(Ok(_)) => {}
            Ok(Err(_)) | Err(_) => {}
        }
    }

    assert_eq!(state_changes.len(), 2, "expected one StateChange per failed agent");
    for (_id, old, new) in &state_changes {
        assert_eq!(*old, AgentState::Active);
        assert_eq!(*new, AgentState::Failed);
    }
    let mut ids: Vec<_> = state_changes.iter().map(|(i, ..)| i.clone()).collect();
    ids.sort();
    assert_eq!(ids, vec!["recact01".to_string(), "recact02".to_string()]);
}

#[tokio::test]
async fn dropping_bus_shuts_down_writer_cleanly() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    {
        let bus = EventBus::new(db.clone());
        bus.publish(StreamEvent::Output {
            agent_id: "drop".into(),
            stream: "stdout".into(),
            line: "x".into(),
        });
        // bus and its Clones drop here
    }
    // Yield so the writer can drain and exit; if the task panicked, the
    // tokio runtime would surface it on the next await.
    tokio::time::sleep(Duration::from_millis(50)).await;
}
