//! Operator notification RPC. The emitted event is `StreamEvent::Notification`
//! (see the `event` module); the `Notifier` subscriber forwards matching ones
//! to the configured webhook.
use serde::{Deserialize, Serialize};

use crate::shared::types::AgentId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyParams {
    pub message: String,
    #[serde(default)]
    pub agent_id: Option<AgentId>,
    /// Severity hint passed through to the webhook payload. Defaults to
    /// `"info"` when absent.
    #[serde(default)]
    pub level: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotifyResult {
    pub published: bool,
}
