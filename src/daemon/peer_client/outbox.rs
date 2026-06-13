//! Outbox backends: one `OutboxBackend` impl per durable outbox table
//! (mail, namespace memory, workspace events, agent lifecycle, scroll
//! dispatch). Each maps a stored row onto its `PeerOutbound` wire message.

use crate::shared::peer_proto::{MemoryDeliver, PeerOutbound, ScrollTaskDispatch, peer_outbound};

use super::super::peer_outbox::OutboxBackend;
use super::super::persistence::Database;

/// Mail outbox backend. Drives `peer_outbox` rows over the mail channel.
pub(super) struct MailOutbox<'a> {
    pub(super) db: &'a Database,
}

impl OutboxBackend for MailOutbox<'_> {
    type Row = crate::shared::types::PeerOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.next_outbox_row(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.mark_outbox_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.mark_outbox_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db.mark_outbox_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.mail_id.clone()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        PeerOutbound {
            msg: Some(peer_outbound::Msg::MailDeliver(
                super::super::peer_outbox::row_to_mail_deliver(row),
            )),
        }
    }
}

/// F3b: workspace-file-event federation backend. Drives
/// `workspace_event_outbox` rows over the workspace channel. The
/// payload is already JSON-serialized at enqueue time, so the backend
/// is just a passthrough.
pub(super) struct WorkspaceEventOutbox<'a> {
    pub(super) db: &'a Database,
}

impl OutboxBackend for WorkspaceEventOutbox<'_> {
    type Row = crate::daemon::workspace_db::WsEventOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.workspace_event_next_outbox(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.workspace_event_mark_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.workspace_event_mark_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db
            .workspace_event_mark_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.sender_seq.to_string()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        PeerOutbound {
            msg: Some(peer_outbound::Msg::WorkspaceEventDeliver(
                crate::shared::peer_proto::WorkspaceEventDeliver {
                    workspace_id: row.workspace_id.clone(),
                    sender_seq: row.sender_seq,
                    payload_json: String::from_utf8_lossy(&row.payload).into_owned(),
                },
            )),
        }
    }
}

/// F5a: scroll-dispatch outbox backend.
pub(super) struct ScrollDispatchOutbox<'a> {
    pub(super) db: &'a Database,
}

impl OutboxBackend for ScrollDispatchOutbox<'_> {
    type Row = crate::daemon::persistence::scroll_dispatch::ScrollDispatchOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.scroll_dispatch_next_outbox(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.scroll_dispatch_mark_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.scroll_dispatch_mark_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db
            .scroll_dispatch_mark_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.sender_seq.to_string()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        // Payload was serialized at enqueue time as a JSON envelope
        // carrying the same fields the proto message holds. Decode
        // here so the over-the-wire proto stays the source of truth.
        let parsed: ScrollDispatchPayload =
            serde_json::from_slice(&row.payload).unwrap_or_default();
        PeerOutbound {
            msg: Some(peer_outbound::Msg::ScrollTaskDispatch(ScrollTaskDispatch {
                sender_seq: row.sender_seq,
                scroll_id: parsed.scroll_id,
                task_id: parsed.task_id,
                task_name: parsed.task_name,
                prompt: parsed.prompt,
                provider: parsed.provider,
                model: parsed.model,
                cwd: parsed.cwd,
                file_patterns: parsed.file_patterns,
            })),
        }
    }
}

/// Wire shape of the dispatch outbox payload (JSON in the BLOB column).
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct ScrollDispatchPayload {
    pub scroll_id: String,
    pub task_id: String,
    pub task_name: String,
    pub prompt: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub file_patterns: Vec<String>,
}

/// F4b: agent-lifecycle federation backend. Drives
/// `agent_lifecycle_outbox` rows over the lifecycle channel.
pub(super) struct AgentLifecycleOutbox<'a> {
    pub(super) db: &'a Database,
}

impl OutboxBackend for AgentLifecycleOutbox<'_> {
    type Row = crate::daemon::persistence::agent_lifecycle::AgentLifecycleOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.agent_lifecycle_next_outbox(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.agent_lifecycle_mark_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.agent_lifecycle_mark_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db
            .agent_lifecycle_mark_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.sender_seq.to_string()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        PeerOutbound {
            msg: Some(peer_outbound::Msg::AgentLifecycleDeliver(
                crate::shared::peer_proto::AgentLifecycleDeliver {
                    sender_seq: row.sender_seq,
                    payload_json: String::from_utf8_lossy(&row.payload).into_owned(),
                },
            )),
        }
    }
}

/// Namespace replication backend. Drives `namespace_outbox` rows over
/// the memory channel.
pub(super) struct MemoryOutbox<'a> {
    pub(super) db: &'a Database,
}

impl OutboxBackend for MemoryOutbox<'_> {
    type Row = crate::daemon::namespace_db::NsOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.namespace_next_outbox(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.namespace_mark_outbox_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.namespace_mark_outbox_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db
            .namespace_mark_outbox_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.op_id.clone()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        PeerOutbound {
            msg: Some(peer_outbound::Msg::MemoryDeliver(MemoryDeliver {
                op_id: row.op_id.clone(),
                namespace: row.namespace.clone(),
                key: row.key.clone(),
                value: row.value.clone(),
                lamport: row.lamport,
                origin_daemon_id: row.origin_daemon_id.clone(),
                deleted: row.deleted,
                updated_by: row.updated_by.clone(),
            })),
        }
    }
}
