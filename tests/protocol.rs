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
    };

    let json = serde_json::to_string(&req).unwrap();
    let parsed: RpcRequest = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.method, "agent.summon");
    assert_eq!(parsed.id, 42);
    assert_eq!(parsed.params["task"], "build the thing");
}

#[test]
fn rpc_response_success() {
    let resp = RpcResponse::success(
        1,
        serde_json::json!({"id": "abc12345", "state": "active"}),
    );

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
fn stream_event_rune_state_change_json_shape() {
    let event = StreamEvent::RuneStateChange {
        scroll_id: "s1".to_string(),
        rune_id: "r1".to_string(),
        rune_name: "Database Setup".to_string(),
        old_state: RuneState::Blocked,
        new_state: RuneState::Ready,
    };

    let json: serde_json::Value = serde_json::to_value(&event).unwrap();
    assert_eq!(json["type"], "rune_state_change");
    assert_eq!(json["rune_name"], "Database Setup");
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
// All StreamEvent variants deserialize from JSON
// ---------------------------------------------------------------------------

#[test]
fn all_stream_event_variants_from_json() {
    let cases = vec![
        r#"{"type":"output","agent_id":"a","stream":"stdout","line":"hi"}"#,
        r#"{"type":"state_change","agent_id":"a","old_state":"active","new_state":"complete"}"#,
        r#"{"type":"scroll_progress","scroll_id":"s","total":1,"complete":0,"active":0,"blocked":1,"failed":0,"skipped":0}"#,
        r#"{"type":"rune_state_change","scroll_id":"s","rune_id":"r","rune_name":"R","old_state":"blocked","new_state":"ready"}"#,
    ];

    for json in cases {
        let event: StreamEvent = serde_json::from_str(json).unwrap_or_else(|e| {
            panic!("Failed to parse: {}\nError: {}", json, e);
        });
        // Just verify it parsed without panicking
        let _ = event.agent_id();
    }
}
