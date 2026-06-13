//! Supervisor tree view plus the manual override actions and history drawer
//! the dashboard exposes per agent row.
use serde::{Deserialize, Serialize};

use crate::shared::types::AgentId;

// One row per agent with supervision metadata in a shape the dashboard can
// group by `parent_id` to render parent → child trees. `last_restart_*` is
// the most recent `restart_history` row, summarising the agent's tail.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorNode {
    pub agent_id: AgentId,
    pub name: Option<String>,
    pub state: String,
    pub task: Option<String>,
    pub parent_id: Option<AgentId>,
    pub restart_policy: String,
    pub restart_count: u32,
    pub max_restarts: Option<u32>,
    pub window_secs: Option<u32>,
    pub escalate_to: Option<String>,
    pub escalation_depth: u32,
    pub last_restart_at: Option<i64>,
    pub last_restart_outcome: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorTreeResult {
    pub nodes: Vec<SupervisorNode>,
}

// `supervisor.restart-now` / `supervisor.clear-escalation` are the manual
// overrides the dashboard exposes per row. `supervisor.history` backs the
// expandable restart-history drawer.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRestartNowParams {
    pub agent_id: AgentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorRestartNowResult {
    pub agent_id: AgentId,
    /// `scheduled` when a restart entry was placed on the heap; `rejected`
    /// with a `reason` (`not_supervised`, `already_pending`, `bad_state`)
    /// when the manual override declined.
    pub outcome: String,
    pub reason: Option<String>,
    pub attempt: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorClearEscalationParams {
    pub agent_id: AgentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorClearEscalationResult {
    pub agent_id: AgentId,
    pub previous_depth: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorHistoryParams {
    pub agent_id: AgentId,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorHistoryRow {
    pub id: i64,
    pub attempted_at: i64,
    pub outcome: String,
    pub error_summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupervisorHistoryResult {
    pub agent_id: AgentId,
    pub rows: Vec<SupervisorHistoryRow>,
}
