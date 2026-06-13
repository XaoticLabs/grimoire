//! Wake sources: triggers that revive a dormant agent.

use super::AgentId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WakeSourceKind {
    Cron,
    FileWatch,
    ParentCompletion,
    /// F4a: file events for a shadow workspace, delivered via federation
    /// (`WorkspaceEventDeliver`) and republished onto the local bus. The
    /// matcher runs over the rel-paths the home daemon reported — there
    /// is no on-disk worktree on this side to canonicalize against.
    RemoteFileWatch,
    /// F4b: lifecycle events from a federated agent, delivered via
    /// `AgentLifecycleDeliver` and republished as
    /// `RemoteAgentStateChanged`. Filters on (sender_daemon_id,
    /// remote_agent_id, target states).
    RemoteAgentCompletion,
}

impl_state_enum!(WakeSourceKind {
    Cron => "cron",
    FileWatch => "file_watch",
    ParentCompletion => "parent_completion",
    RemoteFileWatch => "remote_file_watch",
    RemoteAgentCompletion => "remote_agent_completion",
});

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WakeSourceState {
    Armed,
    Failed,
    Disabled,
}

impl_state_enum!(WakeSourceState {
    Armed => "armed",
    Failed => "failed",
    Disabled => "disabled",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WakeSource {
    pub id: String,
    pub agent_id: AgentId,
    pub kind: WakeSourceKind,
    pub config_json: String,
    pub state: WakeSourceState,
    pub fail_reason: Option<String>,
    pub last_fired_at: Option<i64>,
    pub fire_count: i64,
    pub created_at: i64,
}
