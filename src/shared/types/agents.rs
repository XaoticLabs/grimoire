//! Agent lifecycle state, supervision config, and per-run artifact records.

use super::WorkspaceId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::AgentId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Queued,
    Summoning,
    Active,
    Complete,
    Failed,
    Banished,
    /// Parked: last run finished but the agent has a session_id and a wake
    /// source (or `--keep-alive`). Slot free, lifecycle not final; wakes → `Active`.
    Dormant,
    /// Restart queued by the supervisor. Slot free (no live process), lifecycle
    /// not final; → `Active` via `restart_dispatch`.
    Restarting,
}

impl_state_enum!(AgentState {
    Queued => "queued",
    Summoning => "summoning",
    Active => "active",
    Complete => "complete",
    Failed => "failed",
    Banished => "banished",
    Dormant => "dormant",
    Restarting => "restarting",
});

impl AgentState {
    /// Slot-accounting predicate: `true` when not consuming a scheduler slot.
    /// Includes `Dormant` and `Restarting` (neither has a live process).
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Failed | Self::Banished | Self::Dormant | Self::Restarting
        )
    }

    /// Lifecycle predicate: `true` only when finished for good. Excludes
    /// `Dormant`/`Restarting`, which can still transition back to `Active`.
    pub const fn is_final(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Banished)
    }

    /// Whether the supervisor evaluates restart policy at this state. Only `Failed`.
    pub const fn is_supervisable(&self) -> bool {
        matches!(self, Self::Failed)
    }
}

// --- Supervision config ---

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    #[default]
    Never,
    OnFailure,
}

impl_state_enum!(RestartPolicy {
    Never => "never",
    OnFailure => "on_failure",
});

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestartHistoryOutcome {
    Scheduled,
    Succeeded,
    FailedAgain,
    BudgetExhausted,
}

impl_state_enum!(RestartHistoryOutcome {
    Scheduled => "scheduled",
    Succeeded => "succeeded",
    FailedAgain => "failed_again",
    BudgetExhausted => "budget_exhausted",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupervisionConfig {
    pub policy: RestartPolicy,
    pub max_restarts: Option<u32>,
    pub window_secs: Option<u32>,
    pub escalate_to: Option<String>,
}

impl SupervisionConfig {
    pub const fn never() -> Self {
        Self {
            policy: RestartPolicy::Never,
            max_restarts: None,
            window_secs: None,
            escalate_to: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub name: Option<String>,
    pub state: AgentState,
    pub task: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub cwd: PathBuf,
    pub pid: Option<u32>,
    pub session_id: Option<String>,
    pub exit_code: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    #[serde(default)]
    pub restart_count: u32,
    #[serde(default)]
    pub workspace_id: Option<WorkspaceId>,
}

/// One file an agent touched, per git. `status` is the porcelain code
/// (`M`, `A`, `D`, `??`, …) or `"M"` from a numstat row. Line counts are
/// git's; binary/untracked files report 0.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChange {
    pub path: String,
    pub status: String,
    pub insertions: u64,
    pub deletions: u64,
}

/// Structured record of one agent run: what it changed on disk (vs the commit
/// at dispatch) and what it cost. Captured best-effort at completion; a non-git
/// cwd yields a cost-only record (empty `files_changed`, `diff = None`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentArtifact {
    pub agent_id: AgentId,
    /// `HEAD` at dispatch the diff is taken against, if cwd was a git tree then.
    pub base_commit: Option<String>,
    pub files_changed: Vec<FileChange>,
    /// Unified diff (tail-truncated past the cap), or `None` when no tracked
    /// changes / no git.
    pub diff: Option<String>,
    pub insertions: u64,
    pub deletions: u64,
    pub tokens_used: u64,
    pub usd_spent: f64,
    /// Unix seconds at capture.
    pub captured_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    pub id: Option<i64>,
    pub agent_id: AgentId,
    pub event_type: String,
    pub payload: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSummary {
    pub id: AgentId,
    pub name: Option<String>,
    pub state: AgentState,
    pub task: Option<String>,
    pub age_secs: i64,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub restart_policy: RestartPolicy,
    #[serde(default)]
    pub restart_count: u32,
    #[serde(default)]
    pub max_restarts: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_state_is_terminal() {
        assert!(!AgentState::Queued.is_terminal());
        assert!(!AgentState::Summoning.is_terminal());
        assert!(!AgentState::Active.is_terminal());
        assert!(AgentState::Complete.is_terminal());
        assert!(AgentState::Failed.is_terminal());
        assert!(AgentState::Banished.is_terminal());
        assert!(AgentState::Dormant.is_terminal());
        assert!(AgentState::Restarting.is_terminal());
    }

    #[test]
    fn agent_state_is_final_excludes_dormant() {
        assert!(!AgentState::Dormant.is_final());
        assert!(AgentState::Complete.is_final());
        assert!(AgentState::Failed.is_final());
        assert!(AgentState::Banished.is_final());
        assert!(!AgentState::Queued.is_final());
        assert!(!AgentState::Summoning.is_final());
        assert!(!AgentState::Active.is_final());
    }

    #[test]
    fn agent_state_dormant_serde_roundtrip() {
        let json = serde_json::to_string(&AgentState::Dormant).unwrap();
        assert_eq!(json, "\"dormant\"");
        let parsed: AgentState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, AgentState::Dormant);
        let by_str: AgentState = "dormant".parse().unwrap();
        assert_eq!(by_str, AgentState::Dormant);
        assert_eq!(AgentState::Dormant.to_string(), "dormant");
    }

    #[test]
    fn agent_state_queued_is_not_terminal() {
        assert!(!AgentState::Queued.is_terminal());
    }

    #[test]
    fn agent_state_queued_string_roundtrip() {
        assert_eq!(AgentState::Queued.to_string(), "queued");
        assert_eq!("queued".parse::<AgentState>().unwrap(), AgentState::Queued);
    }

    #[test]
    fn agent_state_queued_serde_roundtrip() {
        let json = serde_json::to_string(&AgentState::Queued).unwrap();
        assert_eq!(json, "\"queued\"");
        let parsed: AgentState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, AgentState::Queued);
    }
}
