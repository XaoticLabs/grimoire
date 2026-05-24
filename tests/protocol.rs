//! Integration tests for the JSON-RPC protocol types.
//!
//! Verifies that RPC requests, responses, and streaming events serialize
//! to the expected JSON shapes and roundtrip correctly. These tests ensure
//! CLI↔daemon wire compatibility.

use chrono::Utc;
use std::path::PathBuf;

use grimoire::shared::protocol::*;
use grimoire::shared::types::*;

// ---------------------------------------------------------------------------
// RpcRequest / RpcResponse roundtrips
// ---------------------------------------------------------------------------

#[test]
fn rpc_request_serialization() {
    let req = RpcRequest {
        method: "agent.summon".to_string(),
        params: serde_json::json!({
            "task": "build the thing",
            "name": "builder",
            "model": "sonnet"
        }),
        id: 42,
        protocol_version: None,
        auth_token: None,
    };

    let json = serde_json::to_string(&req).unwrap();
    let parsed: RpcRequest = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.method, "agent.summon");
    assert_eq!(parsed.id, 42);
    assert_eq!(parsed.params["task"], "build the thing");
}

#[test]
fn rpc_response_success() {
    let resp = RpcResponse::success(1, serde_json::json!({"id": "abc12345", "state": "active"}));

    let json = serde_json::to_string(&resp).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["id"], 1);
    assert!(parsed["result"].is_object());
    assert!(parsed.get("error").is_none());
}

#[test]
fn rpc_response_error() {
    let resp = RpcResponse::error(2, -32601, "Method not found".to_string());

    let json = serde_json::to_string(&resp).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed["id"], 2);
    assert!(parsed.get("result").is_none());
    assert_eq!(parsed["error"]["code"], -32601);
    assert_eq!(parsed["error"]["message"], "Method not found");
}

// ---------------------------------------------------------------------------
// SummonParams / SummonResult
// ---------------------------------------------------------------------------

#[test]
fn summon_params_deserialize() {
    let json = r#"{
        "task": "refactor auth module",
        "name": "auth-refactor",
        "model": "opus",
        "provider": "claude",
        "cwd": "/home/user/project"
    }"#;

    let params: SummonParams = serde_json::from_str(json).unwrap();
    assert_eq!(params.task, "refactor auth module");
    assert_eq!(params.name.as_deref(), Some("auth-refactor"));
    assert_eq!(params.model.as_deref(), Some("opus"));
    assert_eq!(params.provider.as_deref(), Some("claude"));
    assert_eq!(params.cwd, Some(PathBuf::from("/home/user/project")));
}

#[test]
fn summon_params_minimal() {
    let json = r#"{"task": "do stuff"}"#;

    let params: SummonParams = serde_json::from_str(json).unwrap();
    assert_eq!(params.task, "do stuff");
    assert!(params.name.is_none());
    assert!(params.model.is_none());
    assert!(params.provider.is_none());
    assert!(params.cwd.is_none());
}

// ---------------------------------------------------------------------------
// StreamEvent tagged serialization
// ---------------------------------------------------------------------------

#[test]
fn stream_event_output_json_shape() {
    let event = StreamEvent::Output {
        agent_id: "abc123".to_string(),
        stream: "stdout".to_string(),
        line: "hello world".to_string(),
    };

    let json: serde_json::Value = serde_json::to_value(&event).unwrap();
    assert_eq!(json["type"], "output");
    assert_eq!(json["agent_id"], "abc123");
    assert_eq!(json["stream"], "stdout");
    assert_eq!(json["line"], "hello world");
}

#[test]
fn stream_event_state_change_json_shape() {
    let event = StreamEvent::StateChange {
        agent_id: "xyz789".to_string(),
        old_state: AgentState::Active,
        new_state: AgentState::Complete,
    };

    let json: serde_json::Value = serde_json::to_value(&event).unwrap();
    assert_eq!(json["type"], "state_change");
    assert_eq!(json["old_state"], "active");
    assert_eq!(json["new_state"], "complete");
}

#[test]
fn stream_event_agent_created_roundtrip() {
    let agent = Agent {
        id: "test1234".to_string(),
        name: Some("my-agent".to_string()),
        state: AgentState::Summoning,
        task: Some("build things".to_string()),
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
    };

    let event = StreamEvent::AgentCreated { agent };
    let json = serde_json::to_string(&event).unwrap();
    let parsed: StreamEvent = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.agent_id(), Some("test1234"));
}

#[test]
fn stream_event_scroll_progress_json_shape() {
    let event = StreamEvent::ScrollProgress {
        scroll_id: "s1".to_string(),
        total: 10,
        complete: 3,
        active: 2,
        blocked: 4,
        failed: 1,
        skipped: 0,
    };

    let json: serde_json::Value = serde_json::to_value(&event).unwrap();
    assert_eq!(json["type"], "scroll_progress");
    assert_eq!(json["total"], 10);
    assert_eq!(json["complete"], 3);
    assert_eq!(json["failed"], 1);
}

#[test]
fn stream_event_task_state_change_json_shape() {
    let event = StreamEvent::TaskStateChange {
        scroll_id: "s1".to_string(),
        task_id: "r1".to_string(),
        task_name: "Database Setup".to_string(),
        old_state: TaskState::Blocked,
        new_state: TaskState::Ready,
    };

    let json: serde_json::Value = serde_json::to_value(&event).unwrap();
    assert_eq!(json["type"], "task_state_change");
    assert_eq!(json["task_name"], "Database Setup");
    assert_eq!(json["old_state"], "blocked");
    assert_eq!(json["new_state"], "ready");
}

// ---------------------------------------------------------------------------
// Pact params/results
// ---------------------------------------------------------------------------

#[test]
fn pact_create_params_roundtrip() {
    let params = PactCreateParams {
        source_id: "agent-1".to_string(),
        task_tpl: "review {output} and deploy".to_string(),
        name: Some("deploy-pact".to_string()),
    };

    let json = serde_json::to_string(&params).unwrap();
    let parsed: PactCreateParams = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.source_id, "agent-1");
    assert_eq!(parsed.task_tpl, "review {output} and deploy");
    assert_eq!(parsed.name.as_deref(), Some("deploy-pact"));
}

// ---------------------------------------------------------------------------
// Scroll params
// ---------------------------------------------------------------------------

#[test]
fn scroll_inscribe_params_roundtrip() {
    let params = ScrollInscribeParams {
        spec_path: "/home/user/scroll.md".to_string(),
        max_concurrency: Some(8),
    };

    let json = serde_json::to_string(&params).unwrap();
    let parsed: ScrollInscribeParams = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.spec_path, "/home/user/scroll.md");
    assert_eq!(parsed.max_concurrency, Some(8));
}

// ---------------------------------------------------------------------------
// AgentState::Queued serde
// ---------------------------------------------------------------------------

#[test]
fn agent_state_queued_serde_roundtrip() {
    let json = serde_json::to_string(&AgentState::Queued).unwrap();
    assert_eq!(json, "\"queued\"");
    let parsed: AgentState = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, AgentState::Queued);
}

#[test]
fn agent_in_queued_state_roundtrips() {
    let agent = Agent {
        id: "qid12345".to_string(),
        name: None,
        state: AgentState::Queued,
        task: Some("waiting".to_string()),
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
    };

    let json = serde_json::to_string(&agent).unwrap();
    let parsed: Agent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.state, AgentState::Queued);
}

// ---------------------------------------------------------------------------
// All StreamEvent variants deserialize from JSON
// ---------------------------------------------------------------------------

#[test]
fn all_stream_event_variants_from_json() {
    let cases = vec![
        r#"{"type":"output","agent_id":"a","stream":"stdout","line":"hi"}"#,
        r#"{"type":"state_change","agent_id":"a","old_state":"active","new_state":"complete"}"#,
        r#"{"type":"scroll_progress","scroll_id":"s","total":1,"complete":0,"active":0,"blocked":1,"failed":0,"skipped":0}"#,
        r#"{"type":"task_state_change","scroll_id":"s","task_id":"r","task_name":"R","old_state":"blocked","new_state":"ready"}"#,
        r#"{"type":"agent_queued","agent_id":"a","lane":"adhoc","block_reason":null}"#,
        r#"{"type":"worker_registered","worker_id":"w1"}"#,
    ];

    for json in cases {
        let event: StreamEvent = serde_json::from_str(json).unwrap_or_else(|e| {
            panic!("Failed to parse: {json}\nError: {e}");
        });
        // Just verify it parsed without panicking
        let _ = event.agent_id();
    }
}

// ---------------------------------------------------------------------------
// StreamEvent::AgentQueued + StreamEvent::WorkerRegistered (durable work queue)
// ---------------------------------------------------------------------------

#[test]
fn agent_queued_event_serde_roundtrip() {
    let event = StreamEvent::AgentQueued {
        agent_id: "abc12345".to_string(),
        lane: "adhoc".to_string(),
        block_reason: Some("capacity".to_string()),
    };
    let json = serde_json::to_string(&event).unwrap();
    let parsed: StreamEvent = serde_json::from_str(&json).unwrap();
    match parsed {
        StreamEvent::AgentQueued {
            agent_id,
            lane,
            block_reason,
        } => {
            assert_eq!(agent_id, "abc12345");
            assert_eq!(lane, "adhoc");
            assert_eq!(block_reason.as_deref(), Some("capacity"));
        }
        other => panic!("expected AgentQueued, got {other:?}"),
    }

    // None for block_reason should also roundtrip.
    let event = StreamEvent::AgentQueued {
        agent_id: "x".to_string(),
        lane: "scroll".to_string(),
        block_reason: None,
    };
    let json = serde_json::to_string(&event).unwrap();
    let parsed: StreamEvent = serde_json::from_str(&json).unwrap();
    match parsed {
        StreamEvent::AgentQueued {
            lane, block_reason, ..
        } => {
            assert_eq!(lane, "scroll");
            assert!(block_reason.is_none());
        }
        other => panic!("expected AgentQueued, got {other:?}"),
    }
}

#[test]
fn worker_registered_event_serde_roundtrip() {
    let event = StreamEvent::WorkerRegistered {
        worker_id: "worker-1".to_string(),
    };
    let json = serde_json::to_string(&event).unwrap();
    let parsed: StreamEvent = serde_json::from_str(&json).unwrap();
    match parsed {
        StreamEvent::WorkerRegistered { worker_id } => {
            assert_eq!(worker_id, "worker-1");
        }
        other => panic!("expected WorkerRegistered, got {other:?}"),
    }
}

#[test]
fn agent_queued_kind_string() {
    let event = StreamEvent::AgentQueued {
        agent_id: "a".to_string(),
        lane: "adhoc".to_string(),
        block_reason: None,
    };
    assert_eq!(event.kind(), "agent_queued");
}

#[test]
fn worker_registered_kind_string() {
    let event = StreamEvent::WorkerRegistered {
        worker_id: "w".to_string(),
    };
    assert_eq!(event.kind(), "worker_registered");
}

// ---------------------------------------------------------------------------
// StatusResponse / DaemonStatusResult: queue-distinct counts (Task 10)
// ---------------------------------------------------------------------------

#[test]
fn status_response_queued_count_serde() {
    let resp = StatusResponse {
        agents: vec![],
        pacts: vec![],
        workers: vec![],
        uptime_secs: 0,
        active_count: 2,
        queued_count: 3,
        max_concurrent_agents: 8,
        daemon_id: None,
    };
    let json = serde_json::to_value(&resp).unwrap();
    assert_eq!(json["queued_count"].as_u64(), Some(3));
    assert_eq!(json["active_count"].as_u64(), Some(2));
    assert_eq!(json["max_concurrent_agents"].as_u64(), Some(8));

    let parsed: StatusResponse = serde_json::from_value(json).unwrap();
    assert_eq!(parsed.queued_count, 3);
    assert_eq!(parsed.active_count, 2);
    assert_eq!(parsed.max_concurrent_agents, 8);
}

#[test]
fn daemon_status_result_includes_queued_and_cap() {
    let result = DaemonStatusResult {
        uptime_secs: 0,
        agent_count: 5,
        active_count: 2,
        queued_count: 3,
        max_concurrent_agents: 8,
        daemon_id: None,
    };
    let json = serde_json::to_value(&result).unwrap();
    assert_eq!(json["queued_count"].as_u64(), Some(3));
    assert_eq!(json["max_concurrent_agents"].as_u64(), Some(8));
}

// ---------------------------------------------------------------------------
// QueueListResponse / QueueEntry (Task 11)
// ---------------------------------------------------------------------------

#[test]
fn queue_list_response_serde_roundtrip() {
    let resp = QueueListResponse {
        entries: vec![QueueEntry {
            id: "abc12345".to_string(),
            lane: "adhoc".to_string(),
            age_seconds: 12,
            provider: Some("claude".to_string()),
            cwd: "/tmp".to_string(),
            model: None,
            block_reason: Some("capacity".to_string()),
            task_text: "do thing".to_string(),
        }],
    };
    let json = serde_json::to_string(&resp).unwrap();
    let parsed: QueueListResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.entries.len(), 1);
    let e = &parsed.entries[0];
    assert_eq!(e.id, "abc12345");
    assert_eq!(e.lane, "adhoc");
    assert_eq!(e.age_seconds, 12);
    assert_eq!(e.provider.as_deref(), Some("claude"));
    assert_eq!(e.block_reason.as_deref(), Some("capacity"));
}

#[test]
fn queue_entry_block_reason_optional() {
    let entry = QueueEntry {
        id: "x".into(),
        lane: "scroll".into(),
        age_seconds: 0,
        provider: None,
        cwd: "/tmp".into(),
        model: None,
        block_reason: None,
        task_text: "t".into(),
    };
    let json = serde_json::to_value(&entry).unwrap();
    assert!(json["block_reason"].is_null());
    assert!(json["provider"].is_null());
}
