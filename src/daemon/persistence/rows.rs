//! Positional `rusqlite::Row` → domain-type mapping helpers shared across
//! the persistence submodules.

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::shared::types::{
    Agent, AgentState, Mail, MailState, Pact, PactState, RestartPolicy, Scroll, ScrollState,
    Subscription, Task, TaskState, WakeSource, WakeSourceKind, WakeSourceState,
};

use super::QueueRow;

/// Parse an RFC3339 timestamp from a DB column, returning a proper error instead of panicking.
pub(super) fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| anyhow::anyhow!("invalid timestamp '{s}': {e}"))
}

pub(super) fn row_to_subscription(row: &rusqlite::Row) -> Result<Subscription> {
    Ok(Subscription {
        id: row.get(0)?,
        subscriber_id: row.get(1)?,
        topic: row.get(2)?,
        created_at: row.get(3)?,
    })
}

pub(super) fn row_to_peer(row: &rusqlite::Row) -> Result<crate::shared::types::Peer> {
    use crate::shared::types::{Peer, PeerState};
    let state_str: String = row.get(7)?;
    Ok(Peer {
        id: row.get(0)?,
        daemon_id: row.get(1)?,
        name: row.get(2)?,
        url: row.get(3)?,
        bearer_token_hash: row.get(4)?,
        bearer_token: row.get(5)?,
        public_key: row.get(6)?,
        state: state_str
            .parse::<PeerState>()
            .map_err(|e| anyhow::anyhow!("peer state: {e}"))?,
        last_seen: row.get(8)?,
        registered_at: row.get(9)?,
    })
}

pub(super) fn row_to_outbox(row: &rusqlite::Row) -> Result<crate::shared::types::PeerOutboxRow> {
    use crate::shared::types::{PeerOutboxRow, PeerOutboxState};
    let state_str: String = row.get(11)?;
    let attempts: i64 = row.get(9)?;
    let sender_seq: i64 = row.get(3)?;
    Ok(PeerOutboxRow {
        id: row.get(0)?,
        peer_id: row.get(1)?,
        mail_id: row.get(2)?,
        sender_seq: sender_seq as u64,
        recipient: row.get(4)?,
        sender: row.get(5)?,
        topic: row.get(6)?,
        body: row.get(7)?,
        created_at: row.get(8)?,
        attempts: attempts as u32,
        next_attempt_at: row.get(10)?,
        state: state_str
            .parse::<PeerOutboxState>()
            .map_err(|e| anyhow::anyhow!("outbox state: {e}"))?,
    })
}

pub(super) fn row_to_topic_federation(
    row: &rusqlite::Row,
) -> Result<crate::shared::types::TopicFederation> {
    use crate::shared::types::{FederationDirection, TopicFederation};
    let dir: String = row.get(3)?;
    Ok(TopicFederation {
        id: row.get(0)?,
        peer_id: row.get(1)?,
        topic: row.get(2)?,
        direction: dir
            .parse::<FederationDirection>()
            .map_err(|e| anyhow::anyhow!("direction: {e}"))?,
        created_at: row.get(4)?,
    })
}

pub(super) fn row_to_mail(row: &rusqlite::Row) -> Result<Mail> {
    let state_str: String = row.get(6)?;
    let wake_eligible: i64 = row.get(11)?;
    Ok(Mail {
        id: row.get(0)?,
        recipient_id: row.get(1)?,
        sender_id: row.get(2)?,
        topic: row.get(3)?,
        body: row.get(4)?,
        in_reply_to: row.get(5)?,
        state: state_str.parse().unwrap_or(MailState::Failed),
        fail_reason: row.get(7)?,
        created_at: row.get(8)?,
        delivered_at: row.get(9)?,
        seq: row.get(10)?,
        wake_eligible: wake_eligible != 0,
    })
}

pub(super) fn row_to_scroll(row: &rusqlite::Row) -> Result<Scroll> {
    let state_str: String = row.get(2)?;
    let created_str: String = row.get(5)?;
    let updated_str: String = row.get(6)?;

    Ok(Scroll {
        id: row.get(0)?,
        name: row.get(1)?,
        state: state_str.parse().unwrap_or(ScrollState::Failed),
        source_path: row.get(3)?,
        max_concurrency: row.get(4)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
    })
}

pub(super) fn row_to_task(row: &rusqlite::Row) -> Result<Task> {
    let state_str: String = row.get(4)?;
    let file_patterns_json: String = row.get(9)?;
    let created_str: String = row.get(11)?;
    let updated_str: String = row.get(12)?;

    let peer_name: Option<String> = row.get(13).unwrap_or(None);
    let verify_rubric: Option<String> = row.get(14).unwrap_or(None);
    let verify_threshold: Option<f64> = row.get(15).unwrap_or(None);
    let verifier_agent_id: Option<String> = row.get(16).unwrap_or(None);
    Ok(Task {
        id: row.get(0)?,
        scroll_id: row.get(1)?,
        name: row.get(2)?,
        prompt: row.get(3)?,
        state: state_str.parse().unwrap_or(TaskState::Failed),
        agent_id: row.get(5)?,
        provider: row.get(6)?,
        model: row.get(7)?,
        cwd: row.get(8)?,
        file_patterns: serde_json::from_str(&file_patterns_json).unwrap_or_default(),
        order_index: row.get(10)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
        peer_name,
        verify_rubric,
        verify_threshold,
        verifier_agent_id,
    })
}

pub(super) fn row_to_pact(row: &rusqlite::Row) -> Result<Pact> {
    let state_str: String = row.get(4)?;
    let created_str: String = row.get(6)?;
    let fired_str: Option<String> = row.get(7)?;

    Ok(Pact {
        id: row.get(0)?,
        source_id: row.get(1)?,
        task_tpl: row.get(2)?,
        name: row.get(3)?,
        state: state_str.parse().unwrap_or(PactState::Failed),
        target_id: row.get(5)?,
        created_at: parse_timestamp(&created_str)?,
        fired_at: fired_str.as_deref().map(parse_timestamp).transpose()?,
    })
}

pub(super) fn row_to_queue_row(row: &rusqlite::Row) -> Result<QueueRow> {
    let enqueued_str: String = row.get(3)?;
    Ok(QueueRow {
        id: row.get(0)?,
        lane: row.get(1)?,
        priority: row.get(2)?,
        enqueued_at: parse_timestamp(&enqueued_str)?,
        provider_name: row.get(4)?,
        cwd: row.get(5)?,
        model: row.get(6)?,
        task_text: row.get(7)?,
        block_reason: row.get(8)?,
    })
}

pub(super) fn row_to_wake_source(row: &rusqlite::Row) -> Result<WakeSource> {
    let kind_str: String = row.get(2)?;
    let state_str: String = row.get(4)?;
    Ok(WakeSource {
        id: row.get(0)?,
        agent_id: row.get(1)?,
        kind: kind_str.parse().unwrap_or(WakeSourceKind::Cron),
        config_json: row.get(3)?,
        state: state_str.parse().unwrap_or(WakeSourceState::Failed),
        fail_reason: row.get(5)?,
        last_fired_at: row.get(6)?,
        fire_count: row.get(7)?,
        created_at: row.get(8)?,
    })
}

pub(super) fn row_to_agent(row: &rusqlite::Row) -> Result<Agent> {
    let state_str: String = row.get(2)?;
    let cwd_str: String = row.get(6)?;
    let created_str: String = row.get(10)?;
    let updated_str: String = row.get(11)?;
    let policy_str: Option<String> = row.get(13)?;
    let restart_policy: RestartPolicy = policy_str
        .as_deref()
        .unwrap_or("never")
        .parse()
        .unwrap_or(RestartPolicy::Never);
    let restart_count: i64 = row.get::<_, Option<i64>>(14)?.unwrap_or(0);

    Ok(Agent {
        id: row.get(0)?,
        name: row.get(1)?,
        state: state_str.parse().unwrap_or(AgentState::Failed),
        task: row.get(3)?,
        model: row.get(4)?,
        provider: row.get(5)?,
        cwd: std::path::PathBuf::from(cwd_str),
        pid: row.get::<_, Option<u32>>(7)?,
        session_id: row.get(8)?,
        exit_code: row.get(9)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
        worker_id: row.get::<_, Option<String>>(12)?,
        restart_policy,
        restart_count: restart_count.max(0) as u32,
        workspace_id: row.get::<_, Option<String>>(15)?,
    })
}
