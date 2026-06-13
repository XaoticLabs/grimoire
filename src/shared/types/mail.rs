//! Inter-agent mail and topic subscriptions.

use super::AgentId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum MailState {
    Pending,
    Delivered,
    Failed,
}

impl_state_enum!(MailState {
    Pending => "Pending",
    Delivered => "Delivered",
    Failed => "Failed",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mail {
    pub id: String,
    pub recipient_id: AgentId,
    pub sender_id: Option<AgentId>,
    pub topic: Option<String>,
    pub body: String,
    pub in_reply_to: Option<String>,
    pub state: MailState,
    pub fail_reason: Option<String>,
    pub created_at: i64,
    pub delivered_at: Option<i64>,
    pub seq: i64,
    pub wake_eligible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: String,
    pub subscriber_id: AgentId,
    pub topic: String,
    pub created_at: i64,
}
