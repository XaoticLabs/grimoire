//! Integration tests for the persistence layer.
//!
//! These tests exercise the full lifecycle of agents, events, pacts,
//! scrolls, and tasks through the database, verifying that related
//! operations compose correctly.

use chrono::Utc;
use rusqlite::Connection;
use std::path::PathBuf;

use grimoire::daemon::persistence::{Database, QueueRow};
use grimoire::shared::protocol::StreamEvent;
use grimoire::shared::types::*;

fn test_db() -> Database {
    Database::open_in_memory().unwrap()
}

/// Allocate a fresh on-disk path for tests that need to inspect schema or
/// reopen the same file. Cleanup on drop.
struct TempDbPath(PathBuf);

impl TempDbPath {
    fn new(label: &str) -> Self {
        let mut path = std::env::temp_dir();
        let nonce = format!(
            "grimoire-test-{}-{}-{}.db",
            label,
            std::process::id(),
            uuid::Uuid::new_v4()
        );
        path.push(nonce);
        Self(path)
    }
    fn path(&self) -> &std::path::Path {
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

fn make_agent(id: &str) -> Agent {
    Agent {
        id: id.to_string(),
        name: Some(format!("agent-{}", id)),
        state: AgentState::Summoning,
        task: Some("do something".to_string()),
        model: Some("sonnet".to_string()),
        provider: Some("claude".to_string()),
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

// ---------------------------------------------------------------------------
// Durable event log — schema (Task 1)
// ---------------------------------------------------------------------------

#[test]
fn events_table_exists_after_migration() {
    let tmp = TempDbPath::new("events-table");
    let _db = Database::open(tmp.path()).unwrap();

    let conn = Connection::open(tmp.path()).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='events'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "events table should exist after migration");
}

#[test]
fn events_indexes_exist_after_migration() {
    let tmp = TempDbPath::new("events-indexes");
    let _db = Database::open(tmp.path()).unwrap();

    let conn = Connection::open(tmp.path()).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type='index' AND name IN ('idx_events_agent_seq','idx_events_scroll_seq')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2, "both events indexes should exist");
}

#[test]
fn migrate_is_idempotent() {
    let tmp = TempDbPath::new("idempotent");
    {
        let _db = Database::open(tmp.path()).unwrap();
    }
    // Reopen the same file — must succeed (CREATE TABLE IF NOT EXISTS, etc.)
    let _db = Database::open(tmp.path()).unwrap();
}

// ---------------------------------------------------------------------------
// Durable event log — append_event (Task 2)
// ---------------------------------------------------------------------------

fn sample_agent_for_created(id: &str) -> Agent {
    Agent {
        id: id.to_string(),
        name: Some(format!("agent-{}", id)),
        state: AgentState::Summoning,
        task: Some("hello".to_string()),
        model: None,
        provider: None,
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

fn all_six_variants() -> Vec<(&'static str, StreamEvent)> {
    vec![
        (
            "output",
            StreamEvent::Output {
                agent_id: "A".to_string(),
                stream: "stdout".to_string(),
                line: "hi".to_string(),
            },
        ),
        (
            "state_change",
            StreamEvent::StateChange {
                agent_id: "A".to_string(),
                old_state: AgentState::Summoning,
                new_state: AgentState::Active,
            },
        ),
        (
            "agent_created",
            StreamEvent::AgentCreated {
                agent: sample_agent_for_created("A"),
            },
        ),
        (
            "agent_event",
            StreamEvent::AgentEvent {
                event: AgentEvent {
                    id: None,
                    agent_id: "A".to_string(),
                    event_type: "stdout".to_string(),
                    payload: "p".to_string(),
                    created_at: Utc::now(),
                },
            },
        ),
        (
            "scroll_progress",
            StreamEvent::ScrollProgress {
                scroll_id: "S".to_string(),
                total: 1,
                complete: 0,
                active: 1,
                blocked: 0,
                failed: 0,
                skipped: 0,
            },
        ),
        (
            "task_state_change",
            StreamEvent::TaskStateChange {
                scroll_id: "S".to_string(),
                task_id: "t1".to_string(),
                task_name: "T".to_string(),
                old_state: TaskState::Blocked,
                new_state: TaskState::Active,
            },
        ),
    ]
}

#[test]
fn append_event_round_trips_payload() {
    let tmp = TempDbPath::new("roundtrip");
    let db = Database::open(tmp.path()).unwrap();

    for (_kind, event) in all_six_variants() {
        let id = db.append_event(&event).unwrap();
        let conn = Connection::open(tmp.path()).unwrap();
        let payload: String = conn
            .query_row("SELECT payload FROM events WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .unwrap();
        let decoded: StreamEvent = serde_json::from_str(&payload).unwrap();
        // Payload re-serializes identically — equivalent to original.
        let original_json = serde_json::to_string(&event).unwrap();
        let decoded_json = serde_json::to_string(&decoded).unwrap();
        assert_eq!(original_json, decoded_json);
    }
}

#[test]
fn append_event_sets_kind_per_variant() {
    let tmp = TempDbPath::new("kinds");
    let db = Database::open(tmp.path()).unwrap();

    for (expected_kind, event) in all_six_variants() {
        let id = db.append_event(&event).unwrap();
        let conn = Connection::open(tmp.path()).unwrap();
        let kind: String = conn
            .query_row("SELECT kind FROM events WHERE id = ?1", [id], |r| r.get(0))
            .unwrap();
        assert_eq!(
            kind, expected_kind,
            "kind mismatch for variant {}",
            expected_kind
        );
    }
}

#[test]
fn append_event_seq_monotonic_per_agent() {
    let tmp = TempDbPath::new("seq-agent");
    let db = Database::open(tmp.path()).unwrap();
    for i in 0..3 {
        db.append_event(&StreamEvent::Output {
            agent_id: "A".to_string(),
            stream: "stdout".to_string(),
            line: format!("line-{}", i),
        })
        .unwrap();
    }
    let conn = Connection::open(tmp.path()).unwrap();
    let mut stmt = conn
        .prepare("SELECT seq FROM events WHERE agent_id = 'A' ORDER BY id")
        .unwrap();
    let seqs: Vec<i64> = stmt
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(seqs, vec![0, 1, 2]);
}

#[test]
fn append_event_seq_monotonic_per_scroll() {
    let tmp = TempDbPath::new("seq-scroll");
    let db = Database::open(tmp.path()).unwrap();
    for _ in 0..3 {
        db.append_event(&StreamEvent::ScrollProgress {
            scroll_id: "S".to_string(),
            total: 1,
            complete: 0,
            active: 1,
            blocked: 0,
            failed: 0,
            skipped: 0,
        })
        .unwrap();
    }
    let conn = Connection::open(tmp.path()).unwrap();
    let mut stmt = conn
        .prepare("SELECT seq FROM events WHERE scroll_id = 'S' ORDER BY id")
        .unwrap();
    let seqs: Vec<i64> = stmt
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(seqs, vec![0, 1, 2]);
}

#[test]
fn append_event_scopes_are_independent() {
    let tmp = TempDbPath::new("scopes");
    let db = Database::open(tmp.path()).unwrap();

    // Interleave: A, S, A, S, A
    db.append_event(&StreamEvent::Output {
        agent_id: "A".to_string(),
        stream: "stdout".to_string(),
        line: "1".to_string(),
    })
    .unwrap();
    db.append_event(&StreamEvent::ScrollProgress {
        scroll_id: "S".to_string(),
        total: 1,
        complete: 0,
        active: 1,
        blocked: 0,
        failed: 0,
        skipped: 0,
    })
    .unwrap();
    db.append_event(&StreamEvent::Output {
        agent_id: "A".to_string(),
        stream: "stdout".to_string(),
        line: "2".to_string(),
    })
    .unwrap();
    db.append_event(&StreamEvent::ScrollProgress {
        scroll_id: "S".to_string(),
        total: 1,
        complete: 0,
        active: 1,
        blocked: 0,
        failed: 0,
        skipped: 0,
    })
    .unwrap();
    db.append_event(&StreamEvent::Output {
        agent_id: "A".to_string(),
        stream: "stdout".to_string(),
        line: "3".to_string(),
    })
    .unwrap();

    let conn = Connection::open(tmp.path()).unwrap();
    let agent_seqs: Vec<i64> = conn
        .prepare("SELECT seq FROM events WHERE agent_id='A' ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let scroll_seqs: Vec<i64> = conn
        .prepare("SELECT seq FROM events WHERE scroll_id='S' ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(agent_seqs, vec![0, 1, 2]);
    assert_eq!(scroll_seqs, vec![0, 1]);
}

#[test]
fn append_event_populates_scroll_id_only_for_scroll_variants() {
    let tmp = TempDbPath::new("scroll-id-col");
    let db = Database::open(tmp.path()).unwrap();
    let mut expectations: Vec<(i64, &str, bool, bool)> = Vec::new(); // (id, kind, agent_present, scroll_present)
    for (kind, event) in all_six_variants() {
        let id = db.append_event(&event).unwrap();
        let agent_present = matches!(
            event,
            StreamEvent::Output { .. }
                | StreamEvent::StateChange { .. }
                | StreamEvent::AgentCreated { .. }
                | StreamEvent::AgentEvent { .. }
        );
        let scroll_present = matches!(
            event,
            StreamEvent::ScrollProgress { .. } | StreamEvent::TaskStateChange { .. }
        );
        expectations.push((id, kind, agent_present, scroll_present));
    }
    let conn = Connection::open(tmp.path()).unwrap();
    for (id, kind, agent_present, scroll_present) in expectations {
        let (a, s): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT agent_id, scroll_id FROM events WHERE id = ?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            a.is_some(),
            agent_present,
            "agent_id presence wrong for {}",
            kind
        );
        assert_eq!(
            s.is_some(),
            scroll_present,
            "scroll_id presence wrong for {}",
            kind
        );
    }
}

#[test]
fn append_event_returns_monotonic_id() {
    let tmp = TempDbPath::new("ids");
    let db = Database::open(tmp.path()).unwrap();
    let mut last = 0i64;
    for i in 0..3 {
        let id = db
            .append_event(&StreamEvent::Output {
                agent_id: "A".to_string(),
                stream: "stdout".to_string(),
                line: format!("{}", i),
            })
            .unwrap();
        assert!(
            id > last,
            "id should strictly increase: prev={} new={}",
            last,
            id
        );
        last = id;
    }
}

// ---------------------------------------------------------------------------
// Agent lifecycle: insert → update state → update pid → get → list → delete
// ---------------------------------------------------------------------------

#[test]
fn agent_full_lifecycle() {
    let db = test_db();
    let agent = make_agent("lifecycle-1");

    // Insert
    db.insert_agent(&agent).unwrap();

    // Verify initial state
    let fetched = db.get_agent("lifecycle-1").unwrap().unwrap();
    assert_eq!(fetched.state, AgentState::Summoning);
    assert_eq!(fetched.pid, None);

    // Update PID (simulating process spawn)
    db.update_agent_pid("lifecycle-1", 42).unwrap();
    let fetched = db.get_agent("lifecycle-1").unwrap().unwrap();
    assert_eq!(fetched.pid, Some(42));

    // Transition to Active
    db.update_agent_state("lifecycle-1", &AgentState::Active, None)
        .unwrap();
    let fetched = db.get_agent("lifecycle-1").unwrap().unwrap();
    assert_eq!(fetched.state, AgentState::Active);

    // Set session ID
    db.update_agent_session_id("lifecycle-1", "sess-abc")
        .unwrap();
    let fetched = db.get_agent("lifecycle-1").unwrap().unwrap();
    assert_eq!(fetched.session_id.as_deref(), Some("sess-abc"));

    // Complete with exit code
    db.update_agent_state("lifecycle-1", &AgentState::Complete, Some(0))
        .unwrap();
    let fetched = db.get_agent("lifecycle-1").unwrap().unwrap();
    assert_eq!(fetched.state, AgentState::Complete);
    assert_eq!(fetched.exit_code, Some(0));

    // Delete
    db.delete_agent("lifecycle-1").unwrap();
    assert!(db.get_agent("lifecycle-1").unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Agent listing with state filter
// ---------------------------------------------------------------------------

#[test]
fn agent_list_with_state_filter() {
    let db = test_db();

    let mut a1 = make_agent("filter-1");
    a1.state = AgentState::Active;
    let mut a2 = make_agent("filter-2");
    a2.state = AgentState::Complete;
    let mut a3 = make_agent("filter-3");
    a3.state = AgentState::Active;

    db.insert_agent(&a1).unwrap();
    db.insert_agent(&a2).unwrap();
    db.insert_agent(&a3).unwrap();

    let all = db.list_agents(None).unwrap();
    assert_eq!(all.len(), 3);

    let active = db.list_agents(Some("active")).unwrap();
    assert_eq!(active.len(), 2);

    let complete = db.list_agents(Some("complete")).unwrap();
    assert_eq!(complete.len(), 1);
    assert_eq!(complete[0].id, "filter-2");

    let failed = db.list_agents(Some("failed")).unwrap();
    assert!(failed.is_empty());
}

// ---------------------------------------------------------------------------
// Agent events: insert events, retrieve with and without tail
// ---------------------------------------------------------------------------

#[test]
fn agent_events_lifecycle() {
    let db = test_db();
    let agent = make_agent("events-1");
    db.insert_agent(&agent).unwrap();

    // Insert several events
    for i in 0..5 {
        let event = AgentEvent {
            id: None,
            agent_id: "events-1".to_string(),
            event_type: "stdout".to_string(),
            payload: format!("line {}", i),
            created_at: Utc::now(),
        };
        db.insert_event(&event).unwrap();
    }

    // Get all events (no tail)
    let all = db.get_events("events-1", None).unwrap();
    assert_eq!(all.len(), 5);
    assert_eq!(all[0].payload, "line 0"); // ASC order
    assert_eq!(all[4].payload, "line 4");

    // Get last 2 events (tail)
    let tail = db.get_events("events-1", Some(2)).unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].payload, "line 3"); // reversed back to ASC
    assert_eq!(tail[1].payload, "line 4");
}

// ---------------------------------------------------------------------------
// Pact lifecycle: create → query → fire → verify state
// ---------------------------------------------------------------------------

#[test]
fn pact_lifecycle() {
    let db = test_db();

    // Create source agent
    let agent = make_agent("pact-source");
    db.insert_agent(&agent).unwrap();

    // Create two pacts
    let pact1 = Pact {
        id: "pact-1".to_string(),
        source_id: "pact-source".to_string(),
        task_tpl: "review {output}".to_string(),
        name: Some("review pact".to_string()),
        state: PactState::Pending,
        target_id: None,
        created_at: Utc::now(),
        fired_at: None,
    };
    let pact2 = Pact {
        id: "pact-2".to_string(),
        source_id: "pact-source".to_string(),
        task_tpl: "deploy {output}".to_string(),
        name: None,
        state: PactState::Pending,
        target_id: None,
        created_at: Utc::now(),
        fired_at: None,
    };

    db.insert_pact(&pact1).unwrap();
    db.insert_pact(&pact2).unwrap();

    // Query pending pacts
    let pending = db.get_pending_pacts_for_agent("pact-source").unwrap();
    assert_eq!(pending.len(), 2);

    // Fire pact-1
    let target = make_agent("pact-target");
    db.insert_agent(&target).unwrap();
    db.update_pact_fired("pact-1", "pact-target").unwrap();

    // Only pact-2 should remain pending
    let pending = db.get_pending_pacts_for_agent("pact-source").unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, "pact-2");

    // Verify fired pact state
    let all = db.list_pacts(Some("pact-source")).unwrap();
    let fired = all.iter().find(|p| p.id == "pact-1").unwrap();
    assert_eq!(fired.state, PactState::Fired);
    assert_eq!(fired.target_id.as_deref(), Some("pact-target"));
    assert!(fired.fired_at.is_some());

    // Fail pact-2
    db.update_pact_failed("pact-2").unwrap();
    let pending = db.get_pending_pacts_for_agent("pact-source").unwrap();
    assert!(pending.is_empty());
}

// ---------------------------------------------------------------------------
// Scroll + task lifecycle: insert scroll, tasks, dependencies, query
// ---------------------------------------------------------------------------

#[test]
fn scroll_task_lifecycle() {
    let db = test_db();
    let now = Utc::now();

    let scroll = Scroll {
        id: "scroll-1".to_string(),
        name: "Test Scroll".to_string(),
        state: ScrollState::Inscribed,
        source_path: Some("/tmp/scroll.md".to_string()),
        max_concurrency: 2,
        created_at: now,
        updated_at: now,
    };
    db.insert_scroll(&scroll).unwrap();

    // Create three tasks: A (ready), B (ready), C (depends on A and B)
    let task_a = Task {
        id: "task-a".to_string(),
        scroll_id: "scroll-1".to_string(),
        name: "Setup".to_string(),
        prompt: "set things up".to_string(),
        state: TaskState::Ready,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec!["src/setup.rs".to_string()],
        order_index: 0,
        created_at: now,
        updated_at: now,
    };
    let task_b = Task {
        id: "task-b".to_string(),
        scroll_id: "scroll-1".to_string(),
        name: "Config".to_string(),
        prompt: "configure".to_string(),
        state: TaskState::Ready,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec!["src/config.rs".to_string()],
        order_index: 1,
        created_at: now,
        updated_at: now,
    };
    let task_c = Task {
        id: "task-c".to_string(),
        scroll_id: "scroll-1".to_string(),
        name: "Build".to_string(),
        prompt: "build everything".to_string(),
        state: TaskState::Blocked,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec!["src/main.rs".to_string()],
        order_index: 2,
        created_at: now,
        updated_at: now,
    };

    db.insert_task(&task_a).unwrap();
    db.insert_task(&task_b).unwrap();
    db.insert_task(&task_c).unwrap();

    // C depends on A and B
    db.insert_task_dependency("task-c", "task-a").unwrap();
    db.insert_task_dependency("task-c", "task-b").unwrap();

    // Verify task listing
    let tasks = db.get_tasks_for_scroll("scroll-1").unwrap();
    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[0].name, "Setup"); // order_index 0

    // Verify dependencies
    let deps = db.get_task_dependencies("task-c").unwrap();
    assert_eq!(deps.len(), 2);
    assert!(deps.contains(&"task-a".to_string()));
    assert!(deps.contains(&"task-b".to_string()));

    // Verify dependents (reverse lookup)
    let dependents = db.get_task_dependents("task-a").unwrap();
    assert_eq!(dependents, vec!["task-c"]);

    // C should NOT be ready yet (A and B are Ready, not Complete)
    let ready = db.find_ready_tasks("scroll-1").unwrap();
    assert!(ready.is_empty()); // C is blocked, A and B are Ready (not blocked)

    // Complete A
    db.update_task_state("task-a", &TaskState::Complete)
        .unwrap();

    // C still not ready (B not complete)
    let ready = db.find_ready_tasks("scroll-1").unwrap();
    assert!(ready.is_empty());

    // Complete B
    db.update_task_state("task-b", &TaskState::Complete)
        .unwrap();

    // Now C should be ready (both deps complete)
    let ready = db.find_ready_tasks("scroll-1").unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, "task-c");
}

// ---------------------------------------------------------------------------
// Active task counting and scroll state transitions
// ---------------------------------------------------------------------------

#[test]
fn scroll_state_transitions_and_active_count() {
    let db = test_db();
    let now = Utc::now();

    let scroll = Scroll {
        id: "scroll-st".to_string(),
        name: "State Test".to_string(),
        state: ScrollState::Inscribed,
        source_path: None,
        max_concurrency: 4,
        created_at: now,
        updated_at: now,
    };
    db.insert_scroll(&scroll).unwrap();

    let task = Task {
        id: "task-st-1".to_string(),
        scroll_id: "scroll-st".to_string(),
        name: "Task".to_string(),
        prompt: "do it".to_string(),
        state: TaskState::Ready,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec![],
        order_index: 0,
        created_at: now,
        updated_at: now,
    };
    db.insert_task(&task).unwrap();

    // Activate scroll
    db.update_scroll_state("scroll-st", &ScrollState::Active)
        .unwrap();
    let s = db.get_scroll("scroll-st").unwrap().unwrap();
    assert_eq!(s.state, ScrollState::Active);

    // No active tasks yet
    assert_eq!(db.count_active_tasks("scroll-st").unwrap(), 0);

    // Simulate agent assignment
    db.update_task_agent("task-st-1", "agent-x").unwrap();
    assert_eq!(db.count_active_tasks("scroll-st").unwrap(), 1);

    // Look up task by agent
    let found = db.get_task_by_agent_id("agent-x").unwrap().unwrap();
    assert_eq!(found.id, "task-st-1");

    // Complete the task
    db.update_task_state("task-st-1", &TaskState::Complete)
        .unwrap();
    assert_eq!(db.count_active_tasks("scroll-st").unwrap(), 0);

    // Complete scroll
    db.update_scroll_state("scroll-st", &ScrollState::Complete)
        .unwrap();
    let s = db.get_scroll("scroll-st").unwrap().unwrap();
    assert_eq!(s.state, ScrollState::Complete);
}

// ---------------------------------------------------------------------------
// Agent output extraction (JSON result events)
// ---------------------------------------------------------------------------

#[test]
fn agent_output_extraction() {
    let db = test_db();
    let agent = make_agent("output-1");
    db.insert_agent(&agent).unwrap();

    // Insert some non-result stdout events
    let noise = AgentEvent {
        id: None,
        agent_id: "output-1".to_string(),
        event_type: "stdout".to_string(),
        payload: r#"{"type":"progress","message":"working..."}"#.to_string(),
        created_at: Utc::now(),
    };
    db.insert_event(&noise).unwrap();

    // Insert a result event
    let result_event = AgentEvent {
        id: None,
        agent_id: "output-1".to_string(),
        event_type: "stdout".to_string(),
        payload: r#"{"type":"result","result":"All tasks completed successfully"}"#.to_string(),
        created_at: Utc::now(),
    };
    db.insert_event(&result_event).unwrap();

    // Should extract the result
    let output = db.get_agent_output("output-1").unwrap();
    assert_eq!(output.as_deref(), Some("All tasks completed successfully"));
}

#[test]
fn agent_output_returns_none_when_no_result() {
    let db = test_db();
    let agent = make_agent("output-2");
    db.insert_agent(&agent).unwrap();

    let event = AgentEvent {
        id: None,
        agent_id: "output-2".to_string(),
        event_type: "stdout".to_string(),
        payload: "just plain text".to_string(),
        created_at: Utc::now(),
    };
    db.insert_event(&event).unwrap();

    let output = db.get_agent_output("output-2").unwrap();
    assert!(output.is_none());
}

// ---------------------------------------------------------------------------
// Scroll listing
// ---------------------------------------------------------------------------

#[test]
fn scroll_list() {
    let db = test_db();
    let now = Utc::now();

    for i in 0..3 {
        let scroll = Scroll {
            id: format!("list-{}", i),
            name: format!("Scroll {}", i),
            state: ScrollState::Inscribed,
            source_path: None,
            max_concurrency: 4,
            created_at: now,
            updated_at: now,
        };
        db.insert_scroll(&scroll).unwrap();
    }

    let scrolls = db.list_scrolls().unwrap();
    assert_eq!(scrolls.len(), 3);
}

// ---------------------------------------------------------------------------
// Dependency edges query for cycle detection
// ---------------------------------------------------------------------------

#[test]
fn dependency_edges_for_scroll() {
    let db = test_db();
    let now = Utc::now();

    let scroll = Scroll {
        id: "edges-s".to_string(),
        name: "Edges".to_string(),
        state: ScrollState::Inscribed,
        source_path: None,
        max_concurrency: 4,
        created_at: now,
        updated_at: now,
    };
    db.insert_scroll(&scroll).unwrap();

    for id in ["e-a", "e-b", "e-c"] {
        let task = Task {
            id: id.to_string(),
            scroll_id: "edges-s".to_string(),
            name: id.to_string(),
            prompt: "task".to_string(),
            state: TaskState::Blocked,
            agent_id: None,
            provider: None,
            model: None,
            cwd: None,
            file_patterns: vec![],
            order_index: 0,
            created_at: now,
            updated_at: now,
        };
        db.insert_task(&task).unwrap();
    }

    // e-b depends on e-a, e-c depends on e-b
    db.insert_task_dependency("e-b", "e-a").unwrap();
    db.insert_task_dependency("e-c", "e-b").unwrap();

    let edges = db.get_all_dependencies_for_scroll("edges-s").unwrap();
    assert_eq!(edges.len(), 2);
    assert!(edges.contains(&("e-b".to_string(), "e-a".to_string())));
    assert!(edges.contains(&("e-c".to_string(), "e-b".to_string())));
}

// ---------------------------------------------------------------------------
// task_queue — durable work queue (Task 3)
// ---------------------------------------------------------------------------

fn make_queued_agent(id: &str) -> Agent {
    let mut a = make_agent(id);
    a.state = AgentState::Queued;
    a
}

fn make_queue_row(id: &str, lane: &str, enqueued_at: chrono::DateTime<chrono::Utc>) -> QueueRow {
    QueueRow {
        id: id.to_string(),
        lane: lane.to_string(),
        priority: 0,
        enqueued_at,
        provider_name: Some("anthropic".to_string()),
        cwd: "/tmp".to_string(),
        model: Some("sonnet".to_string()),
        task_text: format!("task for {}", id),
        block_reason: None,
    }
}

#[test]
fn task_queue_table_exists_after_migration() {
    let tmp = TempDbPath::new("task-queue-table");
    let _db = Database::open(tmp.path()).unwrap();

    let conn = Connection::open(tmp.path()).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='task_queue'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "task_queue table should exist after migration");

    let idx: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_task_queue_dispatch'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(idx, 1, "dispatch index should exist");
}

#[test]
fn enqueue_then_list_returns_row() {
    let db = test_db();
    db.insert_agent(&make_queued_agent("eq111111")).unwrap();

    let row = make_queue_row("eq111111", "adhoc", Utc::now());
    db.enqueue_task(&row).unwrap();

    let listed = db.list_queue().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0], row);
}

#[test]
fn peek_next_dispatch_orders_adhoc_before_scroll() {
    let db = test_db();
    db.insert_agent(&make_queued_agent("scroll01")).unwrap();
    db.insert_agent(&make_queued_agent("adhoc001")).unwrap();

    let t0 = Utc::now();
    let t1 = t0 + chrono::Duration::seconds(1);

    // scroll enqueued first (older), then ad-hoc (newer)
    db.enqueue_task(&make_queue_row("scroll01", "scroll", t0))
        .unwrap();
    db.enqueue_task(&make_queue_row("adhoc001", "adhoc", t1))
        .unwrap();

    let next = db.peek_next_dispatch().unwrap().expect("row");
    assert_eq!(
        next.id, "adhoc001",
        "ad-hoc lane should drain before scroll"
    );
}

#[test]
fn peek_next_dispatch_orders_fifo_within_lane() {
    let db = test_db();
    db.insert_agent(&make_queued_agent("scroll01")).unwrap();
    db.insert_agent(&make_queued_agent("scroll02")).unwrap();

    let t0 = Utc::now();
    let t1 = t0 + chrono::Duration::seconds(1);

    db.enqueue_task(&make_queue_row("scroll01", "scroll", t0))
        .unwrap();
    db.enqueue_task(&make_queue_row("scroll02", "scroll", t1))
        .unwrap();

    let next = db.peek_next_dispatch().unwrap().expect("row");
    assert_eq!(next.id, "scroll01", "older row should come first");
}

#[test]
fn claim_for_dispatch_is_atomic() {
    let db = test_db();
    db.insert_agent(&make_queued_agent("claim001")).unwrap();
    db.enqueue_task(&make_queue_row("claim001", "adhoc", Utc::now()))
        .unwrap();

    let claimed = db.claim_for_dispatch(&"claim001".to_string()).unwrap();
    assert!(claimed, "first claim succeeds");

    let again = db.claim_for_dispatch(&"claim001".to_string()).unwrap();
    assert!(!again, "second claim returns false (row gone)");

    let agent = db.get_agent("claim001").unwrap().unwrap();
    assert_eq!(
        agent.state,
        AgentState::Summoning,
        "agent flipped to summoning"
    );
    assert_eq!(db.count_queued().unwrap(), 0);
}

#[test]
fn requeue_preserves_enqueued_at() {
    let db = test_db();
    db.insert_agent(&make_queued_agent("requeue1")).unwrap();

    let original_t = Utc::now() - chrono::Duration::seconds(60);
    let row = make_queue_row("requeue1", "adhoc", original_t);
    db.enqueue_task(&row).unwrap();

    assert!(db.claim_for_dispatch(&"requeue1".to_string()).unwrap());

    // Add a younger competitor in the same lane.
    db.insert_agent(&make_queued_agent("rival001")).unwrap();
    db.enqueue_task(&make_queue_row("rival001", "adhoc", Utc::now()))
        .unwrap();

    db.requeue(&row).unwrap();

    let next = db.peek_next_dispatch().unwrap().expect("row");
    assert_eq!(
        next.id, "requeue1",
        "requeued row keeps original enqueued_at"
    );
    assert_eq!(next.enqueued_at.to_rfc3339(), original_t.to_rfc3339());

    let agent = db.get_agent("requeue1").unwrap().unwrap();
    assert_eq!(agent.state, AgentState::Queued, "agent reverts to queued");
}

#[test]
fn delete_from_queue_is_idempotent() {
    let db = test_db();
    db.insert_agent(&make_queued_agent("delete01")).unwrap();
    db.enqueue_task(&make_queue_row("delete01", "adhoc", Utc::now()))
        .unwrap();

    let first = db.delete_from_queue(&"delete01".to_string()).unwrap();
    assert!(first, "first delete returns true");

    let second = db.delete_from_queue(&"delete01".to_string()).unwrap();
    assert!(!second, "second delete returns false");

    // Calling on a never-existed id is also safe.
    let missing = db.delete_from_queue(&"nosuchid1".to_string()).unwrap();
    assert!(!missing);
}

#[test]
fn count_queued_matches_list_len() {
    let db = test_db();
    for (i, lane) in ["adhoc", "scroll", "adhoc", "scroll", "adhoc"]
        .iter()
        .enumerate()
    {
        let id = format!("cnt{:05}", i);
        db.insert_agent(&make_queued_agent(&id)).unwrap();
        db.enqueue_task(&make_queue_row(&id, lane, Utc::now()))
            .unwrap();
    }

    assert_eq!(db.count_queued().unwrap(), 5);
    assert_eq!(db.list_queue().unwrap().len(), 5);
}

#[test]
fn list_queue_by_lane_filters() {
    let db = test_db();
    for (i, lane) in ["adhoc", "scroll", "adhoc"].iter().enumerate() {
        let id = format!("lan{:05}", i);
        db.insert_agent(&make_queued_agent(&id)).unwrap();
        db.enqueue_task(&make_queue_row(&id, lane, Utc::now()))
            .unwrap();
    }

    assert_eq!(db.list_queue_by_lane("adhoc").unwrap().len(), 2);
    assert_eq!(db.list_queue_by_lane("scroll").unwrap().len(), 1);
}

#[test]
fn set_block_reason_round_trips() {
    let db = test_db();
    db.insert_agent(&make_queued_agent("blkrsn01")).unwrap();
    db.enqueue_task(&make_queue_row("blkrsn01", "adhoc", Utc::now()))
        .unwrap();

    db.set_block_reason(&"blkrsn01".to_string(), Some("capacity"))
        .unwrap();
    let row = &db.list_queue().unwrap()[0];
    assert_eq!(row.block_reason.as_deref(), Some("capacity"));

    db.set_block_reason(&"blkrsn01".to_string(), None).unwrap();
    let cleared = &db.list_queue().unwrap()[0];
    assert_eq!(cleared.block_reason, None);
}

#[test]
fn enqueue_rejects_duplicate_id() {
    let db = test_db();
    db.insert_agent(&make_queued_agent("dup00001")).unwrap();
    let row = make_queue_row("dup00001", "adhoc", Utc::now());
    db.enqueue_task(&row).unwrap();

    let err = db.enqueue_task(&row);
    assert!(err.is_err(), "duplicate id should violate primary key");
}

#[test]
fn enqueue_requires_existing_agent() {
    let db = test_db();
    let row = make_queue_row("orphan01", "adhoc", Utc::now());
    let err = db.enqueue_task(&row);
    assert!(err.is_err(), "FK to agents(id) should reject orphan rows");
}

// ---------------------------------------------------------------------------
// Restart recovery (Task 4)
// ---------------------------------------------------------------------------

fn seed_agent_in_state(db: &Database, id: &str, state: AgentState) {
    let mut a = make_agent(id);
    a.state = state;
    db.insert_agent(&a).unwrap();
}

#[test]
fn restart_recovery_fails_active_and_summoning_only() {
    let db = test_db();
    seed_agent_in_state(&db, "act00001", AgentState::Active);
    seed_agent_in_state(&db, "sum00001", AgentState::Summoning);
    seed_agent_in_state(&db, "que00001", AgentState::Queued);
    seed_agent_in_state(&db, "cmp00001", AgentState::Complete);
    seed_agent_in_state(&db, "fld00001", AgentState::Failed);
    seed_agent_in_state(&db, "ban00001", AgentState::Banished);

    let _ = db.restart_recovery().unwrap();

    assert_eq!(
        db.get_agent("act00001").unwrap().unwrap().state,
        AgentState::Failed
    );
    assert_eq!(
        db.get_agent("sum00001").unwrap().unwrap().state,
        AgentState::Failed
    );
    assert_eq!(
        db.get_agent("que00001").unwrap().unwrap().state,
        AgentState::Queued
    );
    assert_eq!(
        db.get_agent("cmp00001").unwrap().unwrap().state,
        AgentState::Complete
    );
    assert_eq!(
        db.get_agent("fld00001").unwrap().unwrap().state,
        AgentState::Failed
    );
    assert_eq!(
        db.get_agent("ban00001").unwrap().unwrap().state,
        AgentState::Banished
    );
}

#[test]
fn restart_recovery_preserves_queued() {
    let db = test_db();
    for id in ["preq0001", "preq0002", "preq0003"] {
        seed_agent_in_state(&db, id, AgentState::Queued);
    }

    let _ = db.restart_recovery().unwrap();

    for id in ["preq0001", "preq0002", "preq0003"] {
        assert_eq!(db.get_agent(id).unwrap().unwrap().state, AgentState::Queued);
    }
}

#[test]
fn restart_recovery_returns_correct_counts() {
    let db = test_db();
    seed_agent_in_state(&db, "rcact001", AgentState::Active);
    seed_agent_in_state(&db, "rcact002", AgentState::Active);
    seed_agent_in_state(&db, "rcque001", AgentState::Queued);
    seed_agent_in_state(&db, "rcque002", AgentState::Queued);
    seed_agent_in_state(&db, "rcque003", AgentState::Queued);
    seed_agent_in_state(&db, "rccmp001", AgentState::Complete);

    let report = db.restart_recovery().unwrap();
    assert_eq!(report.failed.len(), 2);
    assert_eq!(report.queued_remaining, 3);
}

#[test]
fn restart_recovery_returns_old_states_per_failed_agent() {
    let db = test_db();
    seed_agent_in_state(&db, "oldact01", AgentState::Active);
    seed_agent_in_state(&db, "oldsum01", AgentState::Summoning);

    let report = db.restart_recovery().unwrap();

    let mut by_id: std::collections::HashMap<_, _> = report.failed.into_iter().collect();
    assert_eq!(by_id.remove("oldact01"), Some(AgentState::Active));
    assert_eq!(by_id.remove("oldsum01"), Some(AgentState::Summoning));
    assert!(by_id.is_empty());
}

#[test]
fn restart_recovery_empty_db_is_zero() {
    let db = test_db();
    let report = db.restart_recovery().unwrap();
    assert_eq!(report.failed.len(), 0);
    assert_eq!(report.queued_remaining, 0);
}

// ---------------------------------------------------------------------------
// Mail / subscriptions (agent messaging bus)
// ---------------------------------------------------------------------------

fn make_mail(id: &str, recipient: &str, body: &str) -> Mail {
    Mail {
        id: id.to_string(),
        recipient_id: recipient.to_string(),
        sender_id: None,
        topic: None,
        body: body.to_string(),
        in_reply_to: None,
        state: MailState::Pending,
        fail_reason: None,
        created_at: 1_700_000_000,
        delivered_at: None,
        seq: 0,
        wake_eligible: true,
    }
}

#[test]
fn mail_table_schema_after_migration() {
    let tmp = TempDbPath::new("mail-schema");
    let _db = Database::open(tmp.path()).unwrap();

    let conn = Connection::open(tmp.path()).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='mail'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "mail table must exist");

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='subscriptions'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "subscriptions table must exist");
}

#[test]
fn insert_mail_assigns_per_recipient_seq() {
    let db = test_db();
    db.insert_mail(&make_mail("m000001a", "rcp00001", "hello"))
        .unwrap();
    db.insert_mail(&make_mail("m000001b", "rcp00001", "hello2"))
        .unwrap();
    let listed = db
        .list_mail_by_recipient("rcp00001", None, None, 100)
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].seq, 0);
    assert_eq!(listed[1].seq, 1);
}

#[test]
fn insert_mail_seq_independent_per_recipient() {
    let db = test_db();
    db.insert_mail(&make_mail("m000002a", "rcpA", "x")).unwrap();
    db.insert_mail(&make_mail("m000002b", "rcpB", "y")).unwrap();
    let a = db.list_mail_by_recipient("rcpA", None, None, 100).unwrap();
    let b = db.list_mail_by_recipient("rcpB", None, None, 100).unwrap();
    assert_eq!(a[0].seq, 0);
    assert_eq!(b[0].seq, 0);
}

#[test]
fn list_mail_filters_by_state() {
    let db = test_db();
    db.insert_mail(&make_mail("m1", "r1", "a")).unwrap();
    db.insert_mail(&make_mail("m2", "r1", "b")).unwrap();
    db.set_mail_state("m1", MailState::Delivered, None).unwrap();
    let only_pending = db
        .list_mail_by_recipient("r1", None, Some(MailState::Pending), 100)
        .unwrap();
    assert_eq!(only_pending.len(), 1);
    assert_eq!(only_pending[0].id, "m2");
}

#[test]
fn list_mail_clamps_limit() {
    let db = test_db();
    for i in 0..5 {
        let id = format!("clamp{:03}", i);
        db.insert_mail(&make_mail(&id, "rclamp", "x")).unwrap();
    }
    let huge = db
        .list_mail_by_recipient("rclamp", None, None, 5_000)
        .unwrap();
    assert!(huge.len() <= 1000);
    assert_eq!(huge.len(), 5);
}

#[test]
fn list_mail_after_seq_excludes_cursor() {
    let db = test_db();
    for i in 0..4 {
        let id = format!("aft{:05}", i);
        db.insert_mail(&make_mail(&id, "raft", "x")).unwrap();
    }
    let after_one = db
        .list_mail_by_recipient("raft", Some(1), None, 100)
        .unwrap();
    assert!(after_one.iter().all(|m| m.seq > 1));
    assert_eq!(after_one.len(), 2);
}

#[test]
fn set_mail_state_delivered_sets_delivered_at() {
    let db = test_db();
    db.insert_mail(&make_mail("delv0001", "rdelv", "x"))
        .unwrap();
    db.set_mail_state("delv0001", MailState::Delivered, None)
        .unwrap();
    let m = db.get_mail("delv0001").unwrap().unwrap();
    assert!(m.delivered_at.is_some());
    assert_eq!(m.state, MailState::Delivered);
}

#[test]
fn set_mail_state_failed_records_reason() {
    let db = test_db();
    db.insert_mail(&make_mail("fail0001", "rfail", "x"))
        .unwrap();
    db.set_mail_state("fail0001", MailState::Failed, Some("banished"))
        .unwrap();
    let m = db.get_mail("fail0001").unwrap().unwrap();
    assert_eq!(m.state, MailState::Failed);
    assert_eq!(m.fail_reason.as_deref(), Some("banished"));
}

#[test]
fn list_pending_wake_eligible_filters() {
    let db = test_db();
    let mut m1 = make_mail("wake0001", "rwake", "a");
    m1.wake_eligible = true;
    db.insert_mail(&m1).unwrap();
    let mut m2 = make_mail("wake0002", "rwake", "b");
    m2.wake_eligible = false;
    db.insert_mail(&m2).unwrap();
    let mut m3 = make_mail("wake0003", "rwake", "c");
    m3.wake_eligible = true;
    db.insert_mail(&m3).unwrap();
    db.set_mail_state("wake0003", MailState::Delivered, None)
        .unwrap();

    let pending = db.list_pending_wake_eligible("rwake").unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, "wake0001");
}

#[test]
fn insert_subscription_is_idempotent() {
    let db = test_db();
    let s1 = Subscription {
        id: "sub00001".into(),
        subscriber_id: "rsub".into(),
        topic: "t1".into(),
        created_at: 100,
    };
    let s2 = Subscription {
        id: "sub00002".into(),
        subscriber_id: "rsub".into(),
        topic: "t1".into(),
        created_at: 200,
    };
    let id1 = db.insert_subscription(&s1).unwrap();
    let id2 = db.insert_subscription(&s2).unwrap();
    assert_eq!(id1, id2, "duplicate subscription must return existing id");
}

#[test]
fn delete_subscription_returns_false_for_missing() {
    let db = test_db();
    assert!(!db.delete_subscription("nope").unwrap());
}

#[test]
fn list_subscribers_for_topic_filters() {
    let db = test_db();
    db.insert_subscription(&Subscription {
        id: "lst00001".into(),
        subscriber_id: "a".into(),
        topic: "x".into(),
        created_at: 1,
    })
    .unwrap();
    db.insert_subscription(&Subscription {
        id: "lst00002".into(),
        subscriber_id: "b".into(),
        topic: "y".into(),
        created_at: 2,
    })
    .unwrap();
    let xs = db.list_subscribers_for_topic("x").unwrap();
    assert_eq!(xs.len(), 1);
    assert_eq!(xs[0].subscriber_id, "a");
}

#[test]
fn list_topics_with_counts_sorts_and_counts() {
    let db = test_db();
    db.insert_subscription(&Subscription {
        id: "t1s1".into(),
        subscriber_id: "a".into(),
        topic: "beta".into(),
        created_at: 1,
    })
    .unwrap();
    db.insert_subscription(&Subscription {
        id: "t2s1".into(),
        subscriber_id: "b".into(),
        topic: "alpha".into(),
        created_at: 2,
    })
    .unwrap();
    db.insert_subscription(&Subscription {
        id: "t2s2".into(),
        subscriber_id: "c".into(),
        topic: "alpha".into(),
        created_at: 3,
    })
    .unwrap();
    let topics = db.list_topics_with_counts().unwrap();
    assert_eq!(topics, vec![("alpha".into(), 2), ("beta".into(), 1)]);
}

#[test]
fn mail_migrate_is_idempotent() {
    // Open the same on-disk DB twice. `Database::open` calls `migrate()` on
    // every open; the second open must be a no-op.
    let tmp = TempDbPath::new("mail-migrate");
    let _db1 = Database::open(tmp.path()).unwrap();
    let _db2 = Database::open(tmp.path()).unwrap();
    // (no panic == pass)
}
