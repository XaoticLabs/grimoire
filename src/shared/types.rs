use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

pub type AgentId = String;

// --- State Enums with consistent FromStr + Display ---

macro_rules! impl_state_enum {
    ($name:ident { $($variant:ident => $str:literal),+ $(,)? }) => {
        impl $name {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$variant => $str),+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl std::str::FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($str => Ok(Self::$variant)),+,
                    _ => Err(anyhow::anyhow!("invalid {} value: '{}'", stringify!($name), s)),
                }
            }
        }
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Queued,
    Summoning,
    Active,
    Complete,
    Failed,
    Banished,
    /// Parked: the agent's last run finished, but it has a session_id and
    /// at least one wake source (or was opted in via `--keep-alive`). Slot
    /// is free; lifecycle is not final. Wakes back to `Active`.
    Dormant,
    /// Transient: the supervisor has decided to restart this agent and a
    /// `PendingRestart` is queued. Slot is free (no live process), lifecycle
    /// is not final. Transitions back to `Active` via `restart_dispatch`.
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
    /// Slot accounting predicate: `true` when the agent is not consuming a
    /// scheduler slot. Includes `Dormant` (parked, no live process) and
    /// `Restarting` (mid-lifecycle, no live process).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Complete
                | Self::Failed
                | Self::Banished
                | Self::Dormant
                | Self::Restarting
        )
    }

    /// Lifecycle predicate: `true` only when the agent is truly finished
    /// and will not transition again. Excludes `Dormant` and `Restarting`,
    /// which can still transition back to `Active`.
    pub fn is_final(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Banished)
    }

    /// Whether the supervisor should evaluate restart policy when an agent
    /// reaches this state. Only `Failed` is considered supervisable.
    pub fn is_supervisable(&self) -> bool {
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
    pub fn never() -> Self {
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

// --- Mail ---

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

// --- Pacts ---

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

// --- Wake sources ---

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WakeSourceKind {
    Cron,
    FileWatch,
    ParentCompletion,
}

impl_state_enum!(WakeSourceKind {
    Cron => "cron",
    FileWatch => "file_watch",
    ParentCompletion => "parent_completion",
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

// --- Scrolls (Spec-based DAG Orchestration) ---

pub type ScrollId = String;
pub type TaskId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScrollState {
    Inscribed,
    Active,
    Complete,
    Failed,
    Abandoned,
}

impl_state_enum!(ScrollState {
    Inscribed => "inscribed",
    Active => "active",
    Complete => "complete",
    Failed => "failed",
    Abandoned => "abandoned",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Blocked,
    Ready,
    Active,
    Complete,
    Failed,
    Skipped,
}

impl_state_enum!(TaskState {
    Blocked => "blocked",
    Ready => "ready",
    Active => "active",
    Complete => "complete",
    Failed => "failed",
    Skipped => "skipped",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scroll {
    pub id: ScrollId,
    pub name: String,
    pub state: ScrollState,
    pub source_path: Option<String>,
    pub max_concurrency: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub scroll_id: ScrollId,
    pub name: String,
    pub prompt: String,
    pub state: TaskState,
    pub agent_id: Option<AgentId>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub file_patterns: Vec<String>,
    pub order_index: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConflict {
    pub task_a: TaskId,
    pub task_a_name: String,
    pub task_b: TaskId,
    pub task_b_name: String,
    pub overlapping_patterns: Vec<String>,
}

impl TaskConflict {
    pub fn detect(a: &Task, b: &Task) -> Option<Self> {
        let a_patterns: HashSet<&str> = a.file_patterns.iter().map(|s| s.as_str()).collect();
        let b_patterns: HashSet<&str> = b.file_patterns.iter().map(|s| s.as_str()).collect();
        let overlap: Vec<String> = a_patterns
            .intersection(&b_patterns)
            .map(|s| s.to_string())
            .collect();
        if overlap.is_empty() {
            None
        } else {
            Some(Self {
                task_a: a.id.clone(),
                task_a_name: a.name.clone(),
                task_b: b.id.clone(),
                task_b_name: b.name.clone(),
                overlapping_patterns: overlap,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_enum_roundtrips() {
        // AgentState
        for (s, expected) in [
            ("queued", AgentState::Queued),
            ("summoning", AgentState::Summoning),
            ("active", AgentState::Active),
            ("complete", AgentState::Complete),
            ("failed", AgentState::Failed),
            ("banished", AgentState::Banished),
        ] {
            let parsed: AgentState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }

        // PactState
        for (s, expected) in [
            ("pending", PactState::Pending),
            ("fired", PactState::Fired),
            ("failed", PactState::Failed),
        ] {
            let parsed: PactState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }

        // ScrollState
        for (s, expected) in [
            ("inscribed", ScrollState::Inscribed),
            ("active", ScrollState::Active),
            ("complete", ScrollState::Complete),
            ("failed", ScrollState::Failed),
            ("abandoned", ScrollState::Abandoned),
        ] {
            let parsed: ScrollState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }

        // TaskState
        for (s, expected) in [
            ("blocked", TaskState::Blocked),
            ("ready", TaskState::Ready),
            ("active", TaskState::Active),
            ("complete", TaskState::Complete),
            ("failed", TaskState::Failed),
            ("skipped", TaskState::Skipped),
        ] {
            let parsed: TaskState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }
    }

    #[test]
    fn state_enum_invalid_strings() {
        assert!("bogus".parse::<AgentState>().is_err());
        assert!("bogus".parse::<PactState>().is_err());
        assert!("bogus".parse::<ScrollState>().is_err());
        assert!("bogus".parse::<TaskState>().is_err());
    }

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
        assert_eq!(
            "queued".parse::<AgentState>().unwrap(),
            AgentState::Queued
        );
    }

    #[test]
    fn agent_state_queued_serde_roundtrip() {
        let json = serde_json::to_string(&AgentState::Queued).unwrap();
        assert_eq!(json, "\"queued\"");
        let parsed: AgentState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, AgentState::Queued);
    }

    #[test]
    fn task_conflict_detect_overlap() {
        let a = make_task("a", vec!["src/foo.rs", "src/bar.rs"]);
        let b = make_task("b", vec!["src/bar.rs", "src/baz.rs"]);
        let conflict = TaskConflict::detect(&a, &b).unwrap();
        assert_eq!(conflict.overlapping_patterns, vec!["src/bar.rs"]);
    }

    #[test]
    fn task_conflict_detect_no_overlap() {
        let a = make_task("a", vec!["src/foo.rs"]);
        let b = make_task("b", vec!["src/bar.rs"]);
        assert!(TaskConflict::detect(&a, &b).is_none());
    }

    #[test]
    fn task_conflict_detect_empty_patterns() {
        let a = make_task("a", vec![]);
        let b = make_task("b", vec!["src/bar.rs"]);
        assert!(TaskConflict::detect(&a, &b).is_none());

        // Both empty
        let c = make_task("c", vec![]);
        let d = make_task("d", vec![]);
        assert!(TaskConflict::detect(&c, &d).is_none());
    }

    fn make_task(id: &str, file_patterns: Vec<&str>) -> Task {
        Task {
            id: id.to_string(),
            scroll_id: "scroll1".to_string(),
            name: format!("Task {}", id),
            prompt: "test task".to_string(),
            state: TaskState::Ready,
            agent_id: None,
            provider: None,
            model: None,
            cwd: None,
            file_patterns: file_patterns.into_iter().map(String::from).collect(),
            order_index: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}
