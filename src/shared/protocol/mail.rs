//! Mailbox RPC: send/ask/tender, list/ack, topic subscribe/unsubscribe.
use serde::{Deserialize, Serialize};

use super::EmptyResult;
use crate::shared::types::{AgentId, Mail, MailState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSendParams {
    pub to: String,
    pub body: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
    #[serde(default)]
    pub wake_eligible: Option<bool>,
    /// Correlation id of the mail this replies to, echoed back unchanged so
    /// request/reply (see `mail.ask`) can match.
    #[serde(default)]
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailAskParams {
    pub to: String,
    pub body: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
    /// Max time to wait for a reply, in milliseconds. Defaults to 30 000.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailAskResult {
    /// The full reply mail row.
    pub reply: Mail,
}

/// Post a task to a topic (or single agent) and collect replies for a fixed
/// window — fan out a job to `topic://workers`, then take the best bid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailTenderParams {
    pub to: String,
    pub body: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
    /// How long to wait for bids, in milliseconds. Defaults to 30 000.
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailTenderResult {
    /// Mail ids of the original tender posts (one per topic subscriber when
    /// `to` was a topic, otherwise one).
    pub request_mail_ids: Vec<String>,
    /// Bids collected during the window, in arrival order.
    pub bids: Vec<Mail>,
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

pub type MailUnsubscribeResult = EmptyResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicCount {
    pub topic: String,
    pub subscriber_count: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MailTopicsResult {
    pub topics: Vec<TopicCount>,
}
