//! Streaming events sent over `bind`/SSE and persisted to the durable event
//! log. `StreamEvent` is the single tagged enum every subscriber consumes.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::shared::types::{
    Agent, AgentEvent, AgentId, AgentState, DaemonId, ScrollId, TaskState, WorkspaceId,
};

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
        /// `Some(<daemon-id>)` when the mail arrived via a federated peer;
        /// `None` for purely local mail.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_daemon_id: Option<DaemonId>,
    },
    #[serde(rename = "mail_delivered")]
    MailDelivered {
        mail_id: String,
        recipient_id: AgentId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_daemon_id: Option<DaemonId>,
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
    #[serde(rename = "workspace_created")]
    WorkspaceCreated {
        workspace_id: WorkspaceId,
        path: PathBuf,
        branch: String,
    },
    #[serde(rename = "workspace_destroyed")]
    WorkspaceDestroyed { workspace_id: WorkspaceId },
    #[serde(rename = "workspace_orphan_dir_detected")]
    WorkspaceOrphanDirDetected { path: PathBuf },
    #[serde(rename = "memory_written")]
    MemoryWritten {
        workspace_id: WorkspaceId,
        key: String,
        version: u64,
        agent_id: Option<AgentId>,
    },
    #[serde(rename = "memory_deleted")]
    MemoryDeleted {
        workspace_id: WorkspaceId,
        key: String,
        agent_id: Option<AgentId>,
    },
    #[serde(rename = "workspace_file_changed")]
    WorkspaceFileChanged {
        workspace_id: WorkspaceId,
        paths: Vec<String>,
        kinds: Vec<String>,
        truncated_count: u32,
    },
    // --- Federation (peer-link) events ---
    #[serde(rename = "peer_handshake_ok")]
    PeerHandshakeOk {
        peer_id: String,
        peer_daemon_id: DaemonId,
        peer_name: String,
    },
    #[serde(rename = "peer_handshake_failed")]
    PeerHandshakeFailed {
        peer_name: Option<String>,
        reason: String,
    },
    #[serde(rename = "peer_stream_connected")]
    PeerStreamConnected { peer_id: String },
    #[serde(rename = "peer_stream_disconnected")]
    PeerStreamDisconnected { peer_id: String, reason: String },
    #[serde(rename = "peer_mail_forwarded")]
    PeerMailForwarded {
        peer_id: String,
        mail_id: String,
        sender_seq: u64,
    },
    #[serde(rename = "peer_mail_forward_failed")]
    PeerMailForwardFailed {
        peer_id: String,
        mail_id: String,
        reason: String,
    },
    #[serde(rename = "peer_mail_received")]
    PeerMailReceived {
        peer_id: String,
        mail_id: String,
        sender_daemon_id: DaemonId,
    },
    #[serde(rename = "topic_federation_added")]
    TopicFederationAdded {
        peer_id: String,
        topic: String,
        direction: String,
    },
    #[serde(rename = "topic_federation_removed")]
    TopicFederationRemoved { peer_id: String, topic: String },
    /// A notification destined for the human operator. Emitted by the `notify`
    /// RPC (`source = "agent"`, an agent calling `grim notify`) or internally
    /// (`source = "system"`). The `Notifier` subscriber forwards matching ones
    /// to the configured webhook; it also lands in the durable event log.
    #[serde(rename = "notification")]
    Notification {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_id: Option<AgentId>,
        message: String,
        level: String,
        source: String,
    },
    /// F4b: republished agent state-change from a federated peer.
    /// Produced by the inbound `AgentLifecycleDeliver` handler after
    /// dedupe; consumed by `RemoteAgentCompletion` wake sources and
    /// the dashboard's federated-agents view.
    #[serde(rename = "remote_agent_state_changed")]
    RemoteAgentStateChanged {
        sender_daemon_id: DaemonId,
        agent_id: AgentId,
        old_state: AgentState,
        new_state: AgentState,
        /// Optional snapshot fields the producer ships alongside the
        /// state — `None` when the producer didn't include them
        /// (older daemons / minimal payloads).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
    },
}

impl StreamEvent {
    /// Extract the agent ID from any event variant, if applicable. For mail
    /// events this returns the stream the event should appear on (sender for
    /// `MailSent`/`MailFailed`, recipient for `MailReceived`/`MailDelivered`).
    pub fn agent_id(&self) -> Option<&str> {
        // Each variant binds a differently-named field (agent_id vs
        // recipient_id vs sender_id); collapsing the arms would obscure
        // which field is being read for each event type.
        #[allow(clippy::match_same_arms)]
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
            Self::MemoryWritten { agent_id, .. } | Self::MemoryDeleted { agent_id, .. } => {
                agent_id.as_deref()
            }
            Self::Notification { agent_id, .. } => agent_id.as_deref(),
            // Federated state changes report the remote agent's id so
            // local wake sources can match against it.
            Self::RemoteAgentStateChanged { agent_id, .. } => Some(agent_id),
            Self::ScrollProgress { .. }
            | Self::TaskStateChange { .. }
            | Self::WorkerRegistered { .. }
            | Self::WorkspaceCreated { .. }
            | Self::WorkspaceDestroyed { .. }
            | Self::WorkspaceOrphanDirDetected { .. }
            | Self::WorkspaceFileChanged { .. }
            | Self::PeerHandshakeOk { .. }
            | Self::PeerHandshakeFailed { .. }
            | Self::PeerStreamConnected { .. }
            | Self::PeerStreamDisconnected { .. }
            | Self::PeerMailForwarded { .. }
            | Self::PeerMailForwardFailed { .. }
            | Self::PeerMailReceived { .. }
            | Self::TopicFederationAdded { .. }
            | Self::TopicFederationRemoved { .. } => None,
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
    /// Wire tag for this event (the `type` field on the JSON envelope) and
    /// the SQL `events.kind` column value. The drift-detection test
    /// `kind_matches_serde_rename_for_each_variant` keeps these in sync with
    /// each variant's `#[serde(rename = "…")]`. Hand-maintained because
    /// `serde_variant::to_variant_name` does not support internally-tagged
    /// enums.
    pub const fn kind(&self) -> &'static str {
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
            Self::WorkspaceCreated { .. } => "workspace_created",
            Self::WorkspaceDestroyed { .. } => "workspace_destroyed",
            Self::WorkspaceOrphanDirDetected { .. } => "workspace_orphan_dir_detected",
            Self::MemoryWritten { .. } => "memory_written",
            Self::MemoryDeleted { .. } => "memory_deleted",
            Self::WorkspaceFileChanged { .. } => "workspace_file_changed",
            Self::PeerHandshakeOk { .. } => "peer_handshake_ok",
            Self::PeerHandshakeFailed { .. } => "peer_handshake_failed",
            Self::PeerStreamConnected { .. } => "peer_stream_connected",
            Self::PeerStreamDisconnected { .. } => "peer_stream_disconnected",
            Self::PeerMailForwarded { .. } => "peer_mail_forwarded",
            Self::PeerMailForwardFailed { .. } => "peer_mail_forward_failed",
            Self::PeerMailReceived { .. } => "peer_mail_received",
            Self::TopicFederationAdded { .. } => "topic_federation_added",
            Self::TopicFederationRemoved { .. } => "topic_federation_removed",
            Self::Notification { .. } => "notification",
            Self::RemoteAgentStateChanged { .. } => "remote_agent_state_changed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::types::{Agent, AgentEvent, AgentState, TaskState};
    use chrono::Utc;
    use std::path::PathBuf;

    #[test]
    fn agent_id_extraction() {
        let event = StreamEvent::Output {
            agent_id: "abc".to_string(),
            stream: "stdout".to_string(),
            line: "hello".to_string(),
        };
        assert_eq!(event.agent_id(), Some("abc"));

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
            workspace_id: None,
        };
        assert_eq!(
            StreamEvent::AgentCreated { agent }.agent_id(),
            Some("test1234")
        );

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
