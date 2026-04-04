//! Integration tests for the persistence layer.
//!
//! These tests exercise the full lifecycle of agents, events, pacts,
//! scrolls, and runes through the database, verifying that related
//! operations compose correctly.

use chrono::Utc;
use std::path::PathBuf;

use grimoire::daemon::persistence::Database;
use grimoire::shared::types::*;

fn test_db() -> Database {
    Database::open_in_memory().unwrap()
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
// Scroll + rune lifecycle: insert scroll, runes, dependencies, query
// ---------------------------------------------------------------------------

#[test]
fn scroll_rune_lifecycle() {
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

    // Create three runes: A (ready), B (ready), C (depends on A and B)
    let rune_a = Rune {
        id: "rune-a".to_string(),
        scroll_id: "scroll-1".to_string(),
        name: "Setup".to_string(),
        task: "set things up".to_string(),
        state: RuneState::Ready,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec!["src/setup.rs".to_string()],
        order_index: 0,
        created_at: now,
        updated_at: now,
    };
    let rune_b = Rune {
        id: "rune-b".to_string(),
        scroll_id: "scroll-1".to_string(),
        name: "Config".to_string(),
        task: "configure".to_string(),
        state: RuneState::Ready,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec!["src/config.rs".to_string()],
        order_index: 1,
        created_at: now,
        updated_at: now,
    };
    let rune_c = Rune {
        id: "rune-c".to_string(),
        scroll_id: "scroll-1".to_string(),
        name: "Build".to_string(),
        task: "build everything".to_string(),
        state: RuneState::Blocked,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec!["src/main.rs".to_string()],
        order_index: 2,
        created_at: now,
        updated_at: now,
    };

    db.insert_rune(&rune_a).unwrap();
    db.insert_rune(&rune_b).unwrap();
    db.insert_rune(&rune_c).unwrap();

    // C depends on A and B
    db.insert_rune_dependency("rune-c", "rune-a").unwrap();
    db.insert_rune_dependency("rune-c", "rune-b").unwrap();

    // Verify rune listing
    let runes = db.get_runes_for_scroll("scroll-1").unwrap();
    assert_eq!(runes.len(), 3);
    assert_eq!(runes[0].name, "Setup"); // order_index 0

    // Verify dependencies
    let deps = db.get_rune_dependencies("rune-c").unwrap();
    assert_eq!(deps.len(), 2);
    assert!(deps.contains(&"rune-a".to_string()));
    assert!(deps.contains(&"rune-b".to_string()));

    // Verify dependents (reverse lookup)
    let dependents = db.get_rune_dependents("rune-a").unwrap();
    assert_eq!(dependents, vec!["rune-c"]);

    // C should NOT be ready yet (A and B are Ready, not Complete)
    let ready = db.find_ready_runes("scroll-1").unwrap();
    assert!(ready.is_empty()); // C is blocked, A and B are Ready (not blocked)

    // Complete A
    db.update_rune_state("rune-a", &RuneState::Complete).unwrap();

    // C still not ready (B not complete)
    let ready = db.find_ready_runes("scroll-1").unwrap();
    assert!(ready.is_empty());

    // Complete B
    db.update_rune_state("rune-b", &RuneState::Complete).unwrap();

    // Now C should be ready (both deps complete)
    let ready = db.find_ready_runes("scroll-1").unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, "rune-c");
}

// ---------------------------------------------------------------------------
// Active rune counting and scroll state transitions
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

    let rune = Rune {
        id: "rune-st-1".to_string(),
        scroll_id: "scroll-st".to_string(),
        name: "Task".to_string(),
        task: "do it".to_string(),
        state: RuneState::Ready,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec![],
        order_index: 0,
        created_at: now,
        updated_at: now,
    };
    db.insert_rune(&rune).unwrap();

    // Activate scroll
    db.update_scroll_state("scroll-st", &ScrollState::Active).unwrap();
    let s = db.get_scroll("scroll-st").unwrap().unwrap();
    assert_eq!(s.state, ScrollState::Active);

    // No active runes yet
    assert_eq!(db.count_active_runes("scroll-st").unwrap(), 0);

    // Simulate agent assignment
    db.update_rune_agent("rune-st-1", "agent-x").unwrap();
    assert_eq!(db.count_active_runes("scroll-st").unwrap(), 1);

    // Look up rune by agent
    let found = db.get_rune_by_agent_id("agent-x").unwrap().unwrap();
    assert_eq!(found.id, "rune-st-1");

    // Complete the rune
    db.update_rune_state("rune-st-1", &RuneState::Complete).unwrap();
    assert_eq!(db.count_active_runes("scroll-st").unwrap(), 0);

    // Complete scroll
    db.update_scroll_state("scroll-st", &ScrollState::Complete).unwrap();
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
        let rune = Rune {
            id: id.to_string(),
            scroll_id: "edges-s".to_string(),
            name: id.to_string(),
            task: "task".to_string(),
            state: RuneState::Blocked,
            agent_id: None,
            provider: None,
            model: None,
            cwd: None,
            file_patterns: vec![],
            order_index: 0,
            created_at: now,
            updated_at: now,
        };
        db.insert_rune(&rune).unwrap();
    }

    // e-b depends on e-a, e-c depends on e-b
    db.insert_rune_dependency("e-b", "e-a").unwrap();
    db.insert_rune_dependency("e-c", "e-b").unwrap();

    let edges = db.get_all_dependencies_for_scroll("edges-s").unwrap();
    assert_eq!(edges.len(), 2);
    assert!(edges.contains(&("e-b".to_string(), "e-a".to_string())));
    assert!(edges.contains(&("e-c".to_string(), "e-b".to_string())));
}
