use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::types::{Agent, AgentEvent, AgentId, AgentState, AgentSummary, Mail, MailState, TaskConflict, TaskState, ScrollId, WakeSource};

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
    #[serde(default)]
    pub keep_alive: Option<bool>,
    /// "never" | "on_failure" (default: never)
    #[serde(default)]
    pub restart_policy: Option<String>,
    #[serde(default)]
    pub max_restarts: Option<u32>,
    #[serde(default)]
    pub restart_window_secs: Option<u32>,
    /// Address: `agent://<id>` or `topic://<name>`. Reserved sender prefixes
    /// (`supervisor://`, `wake://`) are rejected at `mail.send`.
    #[serde(default)]
    pub escalate_to: Option<String>,
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
    #[serde(default)]
    pub queued_count: usize,
    #[serde(default)]
    pub max_concurrent_agents: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerStatus {
    pub worker_id: String,
    pub in_flight: u32,
    pub max_concurrent: u32,
    pub last_heartbeat_age_secs: u64,
    pub providers: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusResponse {
    #[serde(default)]
    pub agents: Vec<crate::shared::types::AgentSummary>,
    #[serde(default)]
    pub pacts: Vec<serde_json::Value>,
    #[serde(default)]
    pub workers: Vec<WorkerStatus>,
    #[serde(default)]
    pub uptime_secs: i64,
    #[serde(default)]
    pub active_count: usize,
    #[serde(default)]
    pub queued_count: usize,
    #[serde(default)]
    pub max_concurrent_agents: u32,
}

// --- Pact params/results ---

// --- Queue params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub id: AgentId,
    pub lane: String,
    pub age_seconds: u64,
    pub provider: Option<String>,
    pub cwd: String,
    pub model: Option<String>,
    pub block_reason: Option<String>,
    pub task_text: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueListResponse {
    pub entries: Vec<QueueEntry>,
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

// --- Mail params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSendParams {
    pub to: String,
    pub body: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
    #[serde(default)]
    pub wake_eligible: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSendResult {
    pub delivered: u32,
    pub mail_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailListParams {
    pub agent_id: AgentId,
    #[serde(default)]
    pub after_seq: Option<i64>,
    #[serde(default)]
    pub state: Option<MailState>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailListResult {
    pub mails: Vec<Mail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailAckParams {
    pub mail_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailAckResult {
    pub acked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSubscribeParams {
    pub agent_id: AgentId,
    pub topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSubscribeResult {
    pub subscription_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailUnsubscribeParams {
    pub subscription_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MailUnsubscribeResult {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicCount {
    pub topic: String,
    pub subscriber_count: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MailTopicsResult {
    pub topics: Vec<TopicCount>,
}

// --- Wake-source params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeAddParams {
    pub agent_id: AgentId,
    pub kind: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeAddResult {
    pub wake_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WakeListParams {
    #[serde(default)]
    pub agent_id: Option<AgentId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WakeListResult {
    pub sources: Vec<WakeSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeRemoveParams {
    pub wake_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeRemoveResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeTestParams {
    pub wake_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeTestResult {
    pub success: bool,
    pub mail_id: String,
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
    #[serde(rename = "task_state_change")]
    TaskStateChange {
        scroll_id: ScrollId,
        task_id: String,
        task_name: String,
        old_state: TaskState,
        new_state: TaskState,
    },
    #[serde(rename = "agent_queued")]
    AgentQueued {
        agent_id: AgentId,
        lane: String,
        block_reason: Option<String>,
    },
    #[serde(rename = "worker_registered")]
    WorkerRegistered { worker_id: String },
    #[serde(rename = "mail_sent")]
    MailSent {
        mail_id: String,
        sender_id: Option<AgentId>,
        recipient_id: Option<AgentId>,
        topic: Option<String>,
    },
    #[serde(rename = "mail_received")]
    MailReceived {
        mail_id: String,
        recipient_id: AgentId,
        sender_id: Option<AgentId>,
        topic: Option<String>,
        body_preview: String,
        wake_eligible: bool,
    },
    #[serde(rename = "mail_delivered")]
    MailDelivered {
        mail_id: String,
        recipient_id: AgentId,
    },
    #[serde(rename = "mail_failed")]
    MailFailed {
        mail_id: String,
        recipient_id: AgentId,
        sender_id: Option<AgentId>,
        reason: String,
    },
    #[serde(rename = "wake_source_registered")]
    WakeSourceRegistered {
        wake_id: String,
        agent_id: AgentId,
        kind: String,
    },
    #[serde(rename = "wake_source_fired")]
    WakeSourceFired {
        wake_id: String,
        agent_id: AgentId,
        mail_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        via: Option<String>,
    },
    #[serde(rename = "wake_source_failed")]
    WakeSourceFailed {
        wake_id: String,
        agent_id: AgentId,
        reason: String,
    },
    #[serde(rename = "wake_source_retired")]
    WakeSourceRetired {
        wake_id: String,
        agent_id: AgentId,
        reason: String,
    },
    #[serde(rename = "restart_scheduled")]
    RestartScheduled {
        agent_id: AgentId,
        attempt: u32,
        max: u32,
        fire_at_unix: i64,
        rate_limited: bool,
    },
    #[serde(rename = "restarted")]
    Restarted {
        agent_id: AgentId,
        attempt: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mail_id: Option<String>,
    },
    #[serde(rename = "restart_budget_exhausted")]
    RestartBudgetExhausted {
        agent_id: AgentId,
        /// "budget_spent" | "tree_depth_exceeded"
        reason: String,
    },
    #[serde(rename = "escalated")]
    Escalated {
        agent_id: AgentId,
        target: String,
        fanout_count: u32,
    },
}

impl StreamEvent {
    /// Extract the agent ID from any event variant, if applicable. For mail
    /// events this returns the stream the event should appear on (sender for
    /// `MailSent`/`MailFailed`, recipient for `MailReceived`/`MailDelivered`).
    pub fn agent_id(&self) -> Option<&str> {
        match self {
            Self::Output { agent_id, .. } => Some(agent_id),
            Self::StateChange { agent_id, .. } => Some(agent_id),
            Self::AgentCreated { agent } => Some(&agent.id),
            Self::AgentEvent { event } => Some(&event.agent_id),
            Self::AgentQueued { agent_id, .. } => Some(agent_id),
            Self::MailSent { sender_id, .. } => sender_id.as_deref(),
            Self::MailReceived { recipient_id, .. } => Some(recipient_id),
            Self::MailDelivered { recipient_id, .. } => Some(recipient_id),
            Self::MailFailed {
                sender_id,
                recipient_id,
                ..
            } => sender_id.as_deref().or(Some(recipient_id.as_str())),
            Self::WakeSourceRegistered { agent_id, .. }
            | Self::WakeSourceFired { agent_id, .. }
            | Self::WakeSourceFailed { agent_id, .. }
            | Self::WakeSourceRetired { agent_id, .. } => Some(agent_id),
            Self::RestartScheduled { agent_id, .. }
            | Self::Restarted { agent_id, .. }
            | Self::RestartBudgetExhausted { agent_id, .. }
            | Self::Escalated { agent_id, .. } => Some(agent_id),
            Self::ScrollProgress { .. }
            | Self::TaskStateChange { .. }
            | Self::WorkerRegistered { .. } => None,
        }
    }

    /// Extract the scroll ID, for scroll-scoped events only.
    pub fn scroll_id(&self) -> Option<&str> {
        match self {
            Self::ScrollProgress { scroll_id, .. } | Self::TaskStateChange { scroll_id, .. } => {
                Some(scroll_id)
            }
            _ => None,
        }
    }

    /// Stable kind tag matching the serde rename for this variant.
    /// Used as the durable event log's `kind` column so serialized payload
    /// and kind cannot drift.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Output { .. } => "output",
            Self::StateChange { .. } => "state_change",
            Self::AgentCreated { .. } => "agent_created",
            Self::AgentEvent { .. } => "agent_event",
            Self::ScrollProgress { .. } => "scroll_progress",
            Self::TaskStateChange { .. } => "task_state_change",
            Self::AgentQueued { .. } => "agent_queued",
            Self::WorkerRegistered { .. } => "worker_registered",
            Self::MailSent { .. } => "mail_sent",
            Self::MailReceived { .. } => "mail_received",
            Self::MailDelivered { .. } => "mail_delivered",
            Self::MailFailed { .. } => "mail_failed",
            Self::WakeSourceRegistered { .. } => "wake_source_registered",
            Self::WakeSourceFired { .. } => "wake_source_fired",
            Self::WakeSourceFailed { .. } => "wake_source_failed",
            Self::WakeSourceRetired { .. } => "wake_source_retired",
            Self::RestartScheduled { .. } => "restart_scheduled",
            Self::Restarted { .. } => "restarted",
            Self::RestartBudgetExhausted { .. } => "restart_budget_exhausted",
            Self::Escalated { .. } => "escalated",
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
    pub task_count: usize,
    pub conflicts: Vec<TaskConflict>,
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
    use crate::shared::types::{Agent, AgentEvent, AgentState, TaskState};
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
            worker_id: None,
            restart_policy: crate::shared::types::RestartPolicy::Never,
            restart_count: 0,
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
    fn kind_matches_serde_rename_for_each_variant() {
        let cases: &[(&str, StreamEvent)] = &[
            (
                "output",
                StreamEvent::Output {
                    agent_id: "a".into(),
                    stream: "stdout".into(),
                    line: "x".into(),
                },
            ),
            (
                "state_change",
                StreamEvent::StateChange {
                    agent_id: "a".into(),
                    old_state: AgentState::Active,
                    new_state: AgentState::Complete,
                },
            ),
            (
                "scroll_progress",
                StreamEvent::ScrollProgress {
                    scroll_id: "s".into(),
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
                    scroll_id: "s".into(),
                    task_id: "t".into(),
                    task_name: "T".into(),
                    old_state: TaskState::Blocked,
                    new_state: TaskState::Active,
                },
            ),
        ];
        for (expected, ev) in cases {
            assert_eq!(ev.kind(), *expected);
        }
    }

    #[test]
    fn scroll_id_extraction() {
        let scroll = StreamEvent::ScrollProgress {
            scroll_id: "s1".into(),
            total: 1,
            complete: 0,
            active: 1,
            blocked: 0,
            failed: 0,
            skipped: 0,
        };
        assert_eq!(scroll.scroll_id(), Some("s1"));

        let task = StreamEvent::TaskStateChange {
            scroll_id: "s2".into(),
            task_id: "t".into(),
            task_name: "T".into(),
            old_state: TaskState::Blocked,
            new_state: TaskState::Active,
        };
        assert_eq!(task.scroll_id(), Some("s2"));

        let out = StreamEvent::Output {
            agent_id: "a".into(),
            stream: "stdout".into(),
            line: "x".into(),
        };
        assert_eq!(out.scroll_id(), None);
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

        let event = StreamEvent::TaskStateChange {
            scroll_id: "s1".to_string(),
            task_id: "r1".to_string(),
            task_name: "Task 1".to_string(),
            old_state: TaskState::Blocked,
            new_state: TaskState::Active,
        };
        assert_eq!(event.agent_id(), None);
    }
}
