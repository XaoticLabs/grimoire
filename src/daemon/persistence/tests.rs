//! Integration tests for the persistence layer, spanning agents, scrolls,
//! tasks, pacts and the durable event stream.

use super::*;
use crate::shared::types::*;
use std::path::PathBuf;

fn test_db() -> Database {
    Database::open_in_memory().unwrap()
}

fn make_agent(id: &str) -> Agent {
    Agent {
        id: id.to_string(),
        name: Some(format!("agent-{id}")),
        state: AgentState::Active,
        task: Some("test task".to_string()),
        model: Some("sonnet".to_string()),
        provider: Some("claude".to_string()),
        cwd: PathBuf::from("/tmp"),
        pid: Some(1234),
        session_id: None,
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    }
}

fn make_scroll(id: &str) -> Scroll {
    Scroll {
        id: id.to_string(),
        name: format!("Scroll {id}"),
        state: ScrollState::Active,
        source_path: None,
        max_concurrency: 4,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

fn make_task(id: &str, scroll_id: &str, state: TaskState) -> Task {
    Task {
        id: id.to_string(),
        scroll_id: scroll_id.to_string(),
        name: format!("Task {id}"),
        prompt: "test".to_string(),
        state,
        agent_id: None,
        provider: None,
        model: None,
        cwd: None,
        file_patterns: vec![],
        order_index: 0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        peer_name: None,
        verify_rubric: None,
        verify_threshold: None,
        verifier_agent_id: None,
    }
}

#[test]
fn agent_insert_and_get() {
    let db = test_db();
    let agent = make_agent("abc12345");
    db.insert_agent(&agent).unwrap();

    let fetched = db.get_agent("abc12345").unwrap().unwrap();
    assert_eq!(fetched.id, "abc12345");
    assert_eq!(fetched.name.as_deref(), Some("agent-abc12345"));
    assert_eq!(fetched.state, AgentState::Active);
    assert_eq!(fetched.provider.as_deref(), Some("claude"));
}

#[test]
fn agent_not_found() {
    let db = test_db();
    assert!(db.get_agent("nonexistent").unwrap().is_none());
}

#[test]
fn agent_list_and_filter() {
    let db = test_db();

    let mut a1 = make_agent("aaaa1111");
    a1.state = AgentState::Active;
    db.insert_agent(&a1).unwrap();

    let mut a2 = make_agent("bbbb2222");
    a2.state = AgentState::Complete;
    db.insert_agent(&a2).unwrap();

    assert_eq!(db.list_agents(None).unwrap().len(), 2);
    assert_eq!(db.list_agents(Some("active")).unwrap().len(), 1);
    assert_eq!(db.list_agents(Some("complete")).unwrap().len(), 1);
    assert_eq!(db.list_agents(Some("banished")).unwrap().len(), 0);
}

#[test]
fn agent_state_transition() {
    let db = test_db();
    db.insert_agent(&make_agent("state111")).unwrap();

    db.update_agent_state("state111", &AgentState::Complete, Some(0))
        .unwrap();

    let fetched = db.get_agent("state111").unwrap().unwrap();
    assert_eq!(fetched.state, AgentState::Complete);
    assert_eq!(fetched.exit_code, Some(0));
}

#[test]
fn agent_session_id_update() {
    let db = test_db();
    db.insert_agent(&make_agent("sess1111")).unwrap();
    db.update_agent_session_id("sess1111", "session-abc")
        .unwrap();

    let fetched = db.get_agent("sess1111").unwrap().unwrap();
    assert_eq!(fetched.session_id.as_deref(), Some("session-abc"));
}

#[test]
fn event_insert_and_tail() {
    let db = test_db();
    db.insert_agent(&make_agent("evt11111")).unwrap();

    for i in 0..5 {
        db.insert_event(&AgentEvent {
            id: None,
            agent_id: "evt11111".to_string(),
            event_type: "stdout".to_string(),
            payload: format!("line {i}"),
            created_at: Utc::now(),
        })
        .unwrap();
    }

    let all = db.get_events("evt11111", None).unwrap();
    assert_eq!(all.len(), 5);

    let tail = db.get_events("evt11111", Some(2)).unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].payload, "line 3");
    assert_eq!(tail[1].payload, "line 4");
}

#[test]
fn read_stream_events_roundtrip_and_ordering() {
    let db = test_db();
    db.insert_agent(&make_agent("rse11111")).unwrap();

    let events = [
        StreamEvent::StateChange {
            agent_id: "rse11111".into(),
            old_state: AgentState::Summoning,
            new_state: AgentState::Active,
        },
        StreamEvent::Output {
            agent_id: "rse11111".into(),
            stream: "stdout".into(),
            line: "hello".into(),
        },
        StreamEvent::Output {
            agent_id: "rse11111".into(),
            stream: "stdout".into(),
            line: "world".into(),
        },
        StreamEvent::Notification {
            agent_id: Some("rse11111".into()),
            message: "ping".into(),
            level: "info".into(),
            source: "agent".into(),
        },
    ];
    for e in &events {
        db.append_event(e).unwrap();
    }

    let stored = db.read_stream_events("rse11111").unwrap();
    assert_eq!(stored.len(), 4);
    // seq is per-agent and dense from 0.
    for (i, s) in stored.iter().enumerate() {
        assert_eq!(s.seq, i as i64);
    }
    assert_eq!(stored[0].kind, "state_change");
    assert_eq!(stored[3].kind, "notification");
    // Unknown agent reads empty, not error.
    assert!(db.read_stream_events("nope0000").unwrap().is_empty());
}

#[test]
fn agent_stdout_lines_in_order() {
    let db = test_db();
    db.insert_agent(&make_agent("out11111")).unwrap();
    for line in ["first", "second", "third"] {
        db.insert_event(&AgentEvent {
            id: None,
            agent_id: "out11111".to_string(),
            event_type: "stdout".to_string(),
            payload: line.to_string(),
            created_at: Utc::now(),
        })
        .unwrap();
    }
    // stderr must not leak into the transcript.
    db.insert_event(&AgentEvent {
        id: None,
        agent_id: "out11111".to_string(),
        event_type: "stderr".to_string(),
        payload: "noise".to_string(),
        created_at: Utc::now(),
    })
    .unwrap();

    assert_eq!(
        db.get_agent_stdout_lines("out11111").unwrap(),
        vec!["first", "second", "third"]
    );
}

#[test]
fn agent_transcript_budget_truncates_oldest() {
    let db = test_db();
    db.insert_agent(&make_agent("trunc111")).unwrap();
    for i in 0..100 {
        db.insert_event(&AgentEvent {
            id: None,
            agent_id: "trunc111".to_string(),
            event_type: "stdout".to_string(),
            payload: format!("line-{i:03}"),
            created_at: Utc::now(),
        })
        .unwrap();
    }
    let t = db.get_agent_transcript("trunc111", 64).unwrap();
    assert!(t.len() <= 64 + "[…earlier output truncated…]\n".len());
    assert!(t.starts_with("[…earlier output truncated…]"));
    assert!(t.ends_with("line-099")); // newest retained
    assert!(!t.contains("line-000")); // oldest dropped
}

#[test]
fn agent_stdout_lines_missing() {
    let db = test_db();
    db.insert_agent(&make_agent("noout111")).unwrap();
    assert!(db.get_agent_stdout_lines("noout111").unwrap().is_empty());
}

#[test]
fn pact_lifecycle() {
    let db = test_db();
    db.insert_agent(&make_agent("pact1111")).unwrap();

    let pact = Pact {
        id: "pact0001".to_string(),
        source_id: "pact1111".to_string(),
        task_tpl: "do {output}".to_string(),
        name: Some("test pact".to_string()),
        state: PactState::Pending,
        target_id: None,
        created_at: Utc::now(),
        fired_at: None,
    };
    db.insert_pact(&pact).unwrap();

    assert_eq!(db.list_pacts(None).unwrap().len(), 1);
    assert_eq!(db.get_pending_pacts_for_agent("pact1111").unwrap().len(), 1);

    db.update_pact_fired("pact0001", "target01").unwrap();

    assert!(
        db.get_pending_pacts_for_agent("pact1111")
            .unwrap()
            .is_empty()
    );
    let fired = db.list_pacts(None).unwrap();
    assert_eq!(fired[0].state, PactState::Fired);
    assert_eq!(fired[0].target_id.as_deref(), Some("target01"));
}

#[test]
fn scroll_crud() {
    let db = test_db();
    let mut scroll = make_scroll("scr11111");
    scroll.state = ScrollState::Inscribed;
    db.insert_scroll(&scroll).unwrap();

    let fetched = db.get_scroll("scr11111").unwrap().unwrap();
    assert_eq!(fetched.state, ScrollState::Inscribed);

    db.update_scroll_state("scr11111", &ScrollState::Active)
        .unwrap();
    assert_eq!(
        db.get_scroll("scr11111").unwrap().unwrap().state,
        ScrollState::Active
    );
    assert_eq!(db.list_scrolls().unwrap().len(), 1);
}

#[test]
fn task_dependencies_and_ready() {
    let db = test_db();
    db.insert_scroll(&make_scroll("scr22222")).unwrap();

    let task_a = make_task("task_a01", "scr22222", TaskState::Complete);
    let task_b = make_task("task_b01", "scr22222", TaskState::Blocked);
    db.insert_task(&task_a).unwrap();
    db.insert_task(&task_b).unwrap();
    db.insert_task_dependency("task_b01", "task_a01").unwrap();

    // A is complete -> B is ready
    let ready = db.find_ready_tasks("scr22222").unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].id, "task_b01");
}

#[test]
fn task_blocked_by_incomplete_dep() {
    let db = test_db();
    db.insert_scroll(&make_scroll("scr33333")).unwrap();

    let task_a = make_task("blk_a001", "scr33333", TaskState::Active);
    let task_b = make_task("blk_b001", "scr33333", TaskState::Blocked);
    db.insert_task(&task_a).unwrap();
    db.insert_task(&task_b).unwrap();
    db.insert_task_dependency("blk_b001", "blk_a001").unwrap();

    assert!(db.find_ready_tasks("scr33333").unwrap().is_empty());
}

#[test]
fn count_active_tasks() {
    let db = test_db();
    db.insert_scroll(&make_scroll("scr44444")).unwrap();

    db.insert_task(&make_task("cnt_a001", "scr44444", TaskState::Active))
        .unwrap();
    db.insert_task(&make_task("cnt_b001", "scr44444", TaskState::Active))
        .unwrap();
    db.insert_task(&make_task("cnt_c001", "scr44444", TaskState::Complete))
        .unwrap();

    assert_eq!(db.count_active_tasks("scr44444").unwrap(), 2);
}

#[test]
fn task_agent_lookup() {
    let db = test_db();
    db.insert_scroll(&make_scroll("scr55555")).unwrap();
    db.insert_task(&make_task("lkp_a001", "scr55555", TaskState::Ready))
        .unwrap();

    db.update_task_agent("lkp_a001", "myagent1").unwrap();

    let found = db.get_task_by_agent_id("myagent1").unwrap().unwrap();
    assert_eq!(found.id, "lkp_a001");
    assert_eq!(found.state, TaskState::Active); // update_task_agent sets active

    assert!(db.get_task_by_agent_id("nonexist").unwrap().is_none());
}

#[test]
fn delete_agent_removes_events() {
    let db = test_db();
    db.insert_agent(&make_agent("del11111")).unwrap();
    db.insert_event(&AgentEvent {
        id: None,
        agent_id: "del11111".to_string(),
        event_type: "stdout".to_string(),
        payload: "hello".to_string(),
        created_at: Utc::now(),
    })
    .unwrap();

    db.delete_agent("del11111").unwrap();
    assert!(db.get_agent("del11111").unwrap().is_none());
    assert!(db.get_events("del11111", None).unwrap().is_empty());
}

#[test]
fn task_dependents() {
    let db = test_db();
    db.insert_scroll(&make_scroll("scr66666")).unwrap();
    db.insert_task(&make_task("dep_a001", "scr66666", TaskState::Complete))
        .unwrap();
    db.insert_task(&make_task("dep_b001", "scr66666", TaskState::Blocked))
        .unwrap();
    db.insert_task(&make_task("dep_c001", "scr66666", TaskState::Blocked))
        .unwrap();
    db.insert_task_dependency("dep_b001", "dep_a001").unwrap();
    db.insert_task_dependency("dep_c001", "dep_a001").unwrap();

    let dependents = db.get_task_dependents("dep_a001").unwrap();
    assert_eq!(dependents.len(), 2);
    assert!(dependents.contains(&"dep_b001".to_string()));
    assert!(dependents.contains(&"dep_c001".to_string()));
}

#[test]
fn all_dependencies_for_scroll() {
    let db = test_db();
    db.insert_scroll(&make_scroll("scr77777")).unwrap();
    db.insert_task(&make_task("edg_a001", "scr77777", TaskState::Complete))
        .unwrap();
    db.insert_task(&make_task("edg_b001", "scr77777", TaskState::Blocked))
        .unwrap();
    db.insert_task_dependency("edg_b001", "edg_a001").unwrap();

    let edges = db.get_all_dependencies_for_scroll("scr77777").unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0], ("edg_b001".to_string(), "edg_a001".to_string()));
}

#[test]
fn pact_failed_state() {
    let db = test_db();
    db.insert_agent(&make_agent("pfail111")).unwrap();
    let pact = Pact {
        id: "pf000001".to_string(),
        source_id: "pfail111".to_string(),
        task_tpl: "do {output}".to_string(),
        name: None,
        state: PactState::Pending,
        target_id: None,
        created_at: Utc::now(),
        fired_at: None,
    };
    db.insert_pact(&pact).unwrap();
    db.update_pact_failed("pf000001").unwrap();

    let pacts = db.list_pacts(None).unwrap();
    assert_eq!(pacts[0].state, PactState::Failed);
    assert!(pacts[0].fired_at.is_some());
}
