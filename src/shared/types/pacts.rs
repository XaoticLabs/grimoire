//! Pacts: deferred task templates that fire from a source agent.

use super::AgentId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PactState {
    Pending,
    Fired,
    Failed,
}

impl_state_enum!(PactState {
    Pending => "pending",
    Fired => "fired",
    Failed => "failed",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pact {
    pub id: String,
    pub source_id: AgentId,
    pub task_tpl: String,
    pub name: Option<String>,
    pub state: PactState,
    pub target_id: Option<AgentId>,
    pub created_at: DateTime<Utc>,
    pub fired_at: Option<DateTime<Utc>>,
}
