//! End-to-end durability test for the event log.
//!
//! Publishes a known sequence through a real `EventBus` + `Database` pair,
//! drops both, reopens the same DB file, and verifies that every event was
//! persisted with contiguous per-stream sequence numbers and id ordering
//! matching publish order.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::Connection;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::shared::protocol::StreamEvent;

struct TempDbPath(PathBuf);

impl TempDbPath {
    fn new(label: &str) -> Self {
        let mut path = std::env::temp_dir();
        let nonce = format!(
            "grimoire-durability-{}-{}-{}.db",
            label,
            std::process::id(),
            uuid::Uuid::new_v4()
        );
        path.push(nonce);
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDbPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_file(self.0.with_extension("db-wal"));
        let _ = std::fs::remove_file(self.0.with_extension("db-shm"));
    }
}

fn count_events(db: &Database) -> i64 {
    db.with_test_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM events", [], |r| r.get::<_, i64>(0))
            .unwrap()
    })
}

async fn poll_count(db: &Database, target: i64, timeout: Duration) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        let n = count_events(db);
        if n >= target || Instant::now() >= deadline {
            return n;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Publish the canonical scenario through `bus` and wait until 8 events
/// have been persisted (or the 2s budget is exhausted).
async fn publish_scenario(bus: &EventBus, db: &Database) {
    // 3 Outputs for "A"
    for i in 0..3 {
        bus.publish(StreamEvent::Output {
            agent_id: "A".to_string(),
            stream: "stdout".to_string(),
            line: format!("a-out-{}", i),
        });
    }
    // 2 ScrollProgress for "S"
    for i in 0..2 {
        bus.publish(StreamEvent::ScrollProgress {
            scroll_id: "S".to_string(),
            total: 5,
            complete: i,
            active: 1,
            blocked: 0,
            failed: 0,
            skipped: 0,
        });
    }
    // 1 AgentCreated for "B"
    let agent = grimoire::shared::types::Agent {
        id: "B".to_string(),
        name: Some("agent-B".to_string()),
        state: grimoire::shared::types::AgentState::Summoning,
        task: None,
        model: None,
        provider: None,
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        worker_id: None,
        restart_policy: grimoire::shared::types::RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    bus.publish(StreamEvent::AgentCreated { agent });
    // 2 more Outputs for "A"
    for i in 3..5 {
        bus.publish(StreamEvent::Output {
            agent_id: "A".to_string(),
            stream: "stdout".to_string(),
            line: format!("a-out-{}", i),
        });
    }
    let n = poll_count(db, 8, Duration::from_secs(2)).await;
    assert_eq!(n, 8, "writer did not drain all 8 events");
}

#[tokio::test]
async fn events_persist_across_database_reopen() {
    let tmp = TempDbPath::new("reopen");

    {
        let db = Arc::new(Database::open(tmp.path()).unwrap());
        let bus = EventBus::new(db.clone());
        publish_scenario(&bus, &db).await;
        // Drop bus first so the writer task drains and exits.
        drop(bus);
        // Yield to let the writer's final loop iteration commit.
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Drop the only Arc by ending the scope.
    }

    // Reopen.
    let db2 = Database::open(tmp.path()).unwrap();
    let count: i64 = db2.with_test_conn(|c| {
        c.query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap()
    });
    assert_eq!(count, 8);

    // Per-stream contiguous seqs.
    let a_seqs: Vec<i64> = db2.with_test_conn(|c| {
        c.prepare("SELECT seq FROM events WHERE agent_id='A' ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    });
    assert_eq!(a_seqs, vec![0, 1, 2, 3, 4]);

    let s_seqs: Vec<i64> = db2.with_test_conn(|c| {
        c.prepare("SELECT seq FROM events WHERE scroll_id='S' ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    });
    assert_eq!(s_seqs, vec![0, 1]);

    let b_seqs: Vec<i64> = db2.with_test_conn(|c| {
        c.prepare("SELECT seq FROM events WHERE agent_id='B' ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    });
    assert_eq!(b_seqs, vec![0]);
}

#[tokio::test]
async fn per_agent_seq_is_contiguous_after_reopen() {
    let tmp = TempDbPath::new("seq-reopen");
    {
        let db = Arc::new(Database::open(tmp.path()).unwrap());
        let bus = EventBus::new(db.clone());
        for i in 0..4 {
            bus.publish(StreamEvent::Output {
                agent_id: "Z".to_string(),
                stream: "stdout".to_string(),
                line: format!("{}", i),
            });
        }
        poll_count(&db, 4, Duration::from_secs(2)).await;
        drop(bus);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let conn = Connection::open(tmp.path()).unwrap();
    let seqs: Vec<i64> = conn
        .prepare("SELECT seq FROM events WHERE agent_id='Z' ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(seqs, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn publish_order_preserved_across_reopen() {
    let tmp = TempDbPath::new("order-reopen");
    let lines = vec!["alpha", "bravo", "charlie", "delta"];
    {
        let db = Arc::new(Database::open(tmp.path()).unwrap());
        let bus = EventBus::new(db.clone());
        for line in &lines {
            bus.publish(StreamEvent::Output {
                agent_id: "ord".to_string(),
                stream: "stdout".to_string(),
                line: line.to_string(),
            });
        }
        poll_count(&db, lines.len() as i64, Duration::from_secs(2)).await;
        drop(bus);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let conn = Connection::open(tmp.path()).unwrap();
    let payloads: Vec<String> = conn
        .prepare("SELECT payload FROM events WHERE agent_id='ord' ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    let observed: Vec<String> = payloads
        .iter()
        .map(|p| {
            let v: serde_json::Value = serde_json::from_str(p).unwrap();
            v.get("line").and_then(|x| x.as_str()).unwrap().to_string()
        })
        .collect();
    assert_eq!(
        observed,
        lines.iter().map(|s| s.to_string()).collect::<Vec<_>>()
    );
}
