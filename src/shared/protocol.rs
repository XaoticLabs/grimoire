use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::types::{Agent, AgentEvent, AgentId, AgentState, AgentSummary, RuneConflict, RuneState, ScrollId};

/// JSON-RPC request from CLI to daemon
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub method: String,
    pub params: serde_json::Value,
    pub id: u64,
}

/// JSON-RPC response from daemon to CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcResponse {
    pub fn success(id: u64, result: serde_json::Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: u64, code: i32, message: String) -> Self {
        Self {
            id,
            result: None,
            error: Some(RpcError { code, message }),
        }
    }
}

// --- Method parameters ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummonParams {
    pub task: String,
    pub name: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub cwd: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircleParams {
    pub state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindParams {
    pub id: AgentId,
    #[serde(default)]
    pub tail: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanishParams {
    pub id: AgentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeParams {
    pub id: AgentId,
    pub message: String,
}

// --- Method results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummonResult {
    pub id: AgentId,
    pub name: Option<String>,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircleResult {
    pub agents: Vec<AgentSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanishResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatusResult {
    pub uptime_secs: i64,
    pub agent_count: usize,
    pub active_count: usize,
}

// --- Pact params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PactCreateParams {
    pub source_id: AgentId,
    pub task_tpl: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PactCreateResult {
    pub id: String,
    pub source_id: AgentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PactListParams {
    pub source_id: Option<AgentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PactListResult {
    pub pacts: Vec<super::types::Pact>,
}

// --- Streaming events (sent over bind/SSE) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamEvent {
    #[serde(rename = "output")]
    Output {
        agent_id: AgentId,
        stream: String, // "stdout" or "stderr"
        line: String,
    },
    #[serde(rename = "state_change")]
    StateChange {
        agent_id: AgentId,
        old_state: AgentState,
        new_state: AgentState,
    },
    #[serde(rename = "agent_created")]
    AgentCreated { agent: Agent },
    #[serde(rename = "agent_event")]
    AgentEvent { event: AgentEvent },
    #[serde(rename = "scroll_progress")]
    ScrollProgress {
        scroll_id: ScrollId,
        total: usize,
        complete: usize,
        active: usize,
        blocked: usize,
        failed: usize,
        skipped: usize,
    },
    #[serde(rename = "rune_state_change")]
    RuneStateChange {
        scroll_id: ScrollId,
        rune_id: String,
        rune_name: String,
        old_state: RuneState,
        new_state: RuneState,
    },
}

impl StreamEvent {
    /// Extract the agent ID from any event variant, if applicable.
    pub fn agent_id(&self) -> Option<&str> {
        match self {
            Self::Output { agent_id, .. } => Some(agent_id),
            Self::StateChange { agent_id, .. } => Some(agent_id),
            Self::AgentCreated { agent } => Some(&agent.id),
            Self::AgentEvent { event } => Some(&event.agent_id),
            Self::ScrollProgress { .. } | Self::RuneStateChange { .. } => None,
        }
    }
}

// --- Scroll params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollInscribeParams {
    pub spec_path: String,
    pub max_concurrency: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollInscribeResult {
    pub id: ScrollId,
    pub name: String,
    pub rune_count: usize,
    pub conflicts: Vec<RuneConflict>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollActivateParams {
    pub id: ScrollId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollStatusParams {
    pub id: ScrollId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollAbandonParams {
    pub id: ScrollId,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::types::{Agent, AgentEvent, AgentState, RuneState};
    use chrono::Utc;
    use std::path::PathBuf;

    #[test]
    fn agent_id_extraction() {
        // Output event
        let event = StreamEvent::Output {
            agent_id: "abc".to_string(),
            stream: "stdout".to_string(),
            line: "hello".to_string(),
        };
        assert_eq!(event.agent_id(), Some("abc"));

        // StateChange event
        let event = StreamEvent::StateChange {
            agent_id: "xyz".to_string(),
            old_state: AgentState::Active,
            new_state: AgentState::Complete,
        };
        assert_eq!(event.agent_id(), Some("xyz"));

        // AgentCreated event
        let agent = Agent {
            id: "test1234".to_string(),
            name: None,
            state: AgentState::Summoning,
            task: None,
            model: None,
            provider: None,
            cwd: PathBuf::from("/tmp"),
            pid: None,
            session_id: None,
            exit_code: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        assert_eq!(StreamEvent::AgentCreated { agent }.agent_id(), Some("test1234"));

        // AgentEvent event
        let event = StreamEvent::AgentEvent {
            event: AgentEvent {
                id: None,
                agent_id: "evt12345".to_string(),
                event_type: "stdout".to_string(),
                payload: "data".to_string(),
                created_at: Utc::now(),
            },
        };
        assert_eq!(event.agent_id(), Some("evt12345"));
    }

    #[test]
    fn agent_id_scroll_events_are_none() {
        let event = StreamEvent::ScrollProgress {
            scroll_id: "s1".to_string(),
            total: 4,
            complete: 1,
            active: 2,
            blocked: 1,
            failed: 0,
            skipped: 0,
        };
        assert_eq!(event.agent_id(), None);

        let event = StreamEvent::RuneStateChange {
            scroll_id: "s1".to_string(),
            rune_id: "r1".to_string(),
            rune_name: "Rune 1".to_string(),
            old_state: RuneState::Blocked,
            new_state: RuneState::Active,
        };
        assert_eq!(event.agent_id(), None);
    }
}
