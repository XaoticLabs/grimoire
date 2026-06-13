//! Wake-source RPC: register/list/remove/test the triggers that re-summon an
//! agent (timers, file watches, mail topics, …).
use serde::{Deserialize, Serialize};

use crate::shared::types::{AgentId, WakeSource};

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
