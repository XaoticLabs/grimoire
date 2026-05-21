#![allow(missing_docs)] // Shared value types; documentation pass pending.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

pub type AgentId = String;

/// Stable 8-hex identifier minted on first `grimd` boot. Two daemons in a
/// federation use distinct DaemonIds to disambiguate agent addresses.
/// Display form prefixes `grimd-`; storage is the bare 8-hex string.
pub type DaemonId = String;

/// `^[0-9a-f]{8}$`.
pub fn validate_daemon_id(s: &str) -> bool {
    if s.len() != 8 {
        return false;
    }
    s.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub type PeerId = String;

// --- State Enums with consistent FromStr + Display ---

macro_rules! impl_state_enum {
    ($name:ident { $($variant:ident => $str:literal),+ $(,)? }) => {
        impl $name {
            pub const fn as_str(&self) -> &'static str {
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
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Complete | Self::Failed | Self::Banished | Self::Dormant | Self::Restarting
        )
    }

    /// Lifecycle predicate: `true` only when the agent is truly finished
    /// and will not transition again. Excludes `Dormant` and `Restarting`,
    /// which can still transition back to `Active`.
    pub const fn is_final(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Banished)
    }

    /// Whether the supervisor should evaluate restart policy when an agent
    /// reaches this state. Only `Failed` is considered supervisable.
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

// --- Workspaces ---

pub type WorkspaceId = String;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum WorkspaceState {
    Active,
    Destroying,
}

impl_state_enum!(WorkspaceState {
    Active => "Active",
    Destroying => "Destroying",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Workspace {
    pub id: WorkspaceId,
    pub path: PathBuf,
    pub repo_path: PathBuf,
    pub branch: String,
    pub state: WorkspaceState,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceListEntry {
    pub id: WorkspaceId,
    pub path: PathBuf,
    pub branch: String,
    pub state: WorkspaceState,
    pub agent_count: u32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryEntry {
    pub workspace_id: WorkspaceId,
    pub key: String,
    pub value: serde_json::Value,
    pub version: u64,
    pub updated_at: i64,
    pub updated_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoryListItem {
    pub key: String,
    pub version: u64,
    pub updated_at: i64,
    pub value_size: u64,
}

/// Validation error returned by [`validate_workspace_id`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameError {
    Empty,
    TooLong,
    InvalidChar,
    LeadingNonAlphanumeric,
}

impl std::fmt::Display for NameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("empty"),
            Self::TooLong => f.write_str("too_long"),
            Self::InvalidChar => f.write_str("invalid_char"),
            Self::LeadingNonAlphanumeric => f.write_str("leading_non_alphanumeric"),
        }
    }
}

/// Workspace ID validator: `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`.
pub fn validate_workspace_id(s: &str) -> Result<(), NameError> {
    if s.is_empty() {
        return Err(NameError::Empty);
    }
    if s.len() > super::constants::MAX_WORKSPACE_NAME_LEN {
        return Err(NameError::TooLong);
    }
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return Err(NameError::Empty);
    };
    if !first.is_ascii_alphanumeric() {
        return Err(NameError::LeadingNonAlphanumeric);
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-') {
            return Err(NameError::InvalidChar);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryKeyError {
    Empty,
    TooLong,
    InvalidChar,
    LeadingSlash,
    TrailingSlash,
    DoubleSlash,
}

impl std::fmt::Display for MemoryKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("empty"),
            Self::TooLong => f.write_str("too_long"),
            Self::InvalidChar => f.write_str("invalid_char"),
            Self::LeadingSlash => f.write_str("leading_slash"),
            Self::TrailingSlash => f.write_str("trailing_slash"),
            Self::DoubleSlash => f.write_str("double_slash"),
        }
    }
}

/// Memory key validator: `^[a-zA-Z0-9._-]+(/[a-zA-Z0-9._-]+)*$`, ≤ 256 chars,
/// no leading/trailing slash, no double slash.
pub fn validate_memory_key(s: &str) -> Result<(), MemoryKeyError> {
    if s.is_empty() {
        return Err(MemoryKeyError::Empty);
    }
    if s.len() > super::constants::MAX_MEMORY_KEY_LEN {
        return Err(MemoryKeyError::TooLong);
    }
    if s.starts_with('/') {
        return Err(MemoryKeyError::LeadingSlash);
    }
    if s.ends_with('/') {
        return Err(MemoryKeyError::TrailingSlash);
    }
    if s.contains("//") {
        return Err(MemoryKeyError::DoubleSlash);
    }
    for c in s.chars() {
        if !(c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' || c == '/') {
            return Err(MemoryKeyError::InvalidChar);
        }
    }
    Ok(())
}

/// Split a memory key into segments (for topic emission).
pub fn memory_key_segments(s: &str) -> Vec<&str> {
    s.split('/').collect()
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
        let a_patterns: HashSet<&str> = a
            .file_patterns
            .iter()
            .map(std::string::String::as_str)
            .collect();
        let b_patterns: HashSet<&str> = b
            .file_patterns
            .iter()
            .map(std::string::String::as_str)
            .collect();
        let overlap: Vec<String> = a_patterns
            .intersection(&b_patterns)
            .map(std::string::ToString::to_string)
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
        assert_eq!("queued".parse::<AgentState>().unwrap(), AgentState::Queued);
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
            name: format!("Task {id}"),
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

// --- Federation peer types (see plan/federation.md) ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerState {
    Pending,
    Active,
    Down,
    Removing,
}

impl_state_enum!(PeerState {
    Pending => "pending",
    Active => "active",
    Down => "down",
    Removing => "removing",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerOutboxState {
    Pending,
    InFlight,
    Delivered,
    Failed,
}

impl_state_enum!(PeerOutboxState {
    Pending => "pending",
    InFlight => "in_flight",
    Delivered => "delivered",
    Failed => "failed",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FederationDirection {
    Inbound,
    Outbound,
    Both,
}

impl_state_enum!(FederationDirection {
    Inbound => "inbound",
    Outbound => "outbound",
    Both => "both",
});

impl FederationDirection {
    /// Merge two direction declarations into the most-permissive form.
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        if self == other {
            return self;
        }
        Self::Both
    }

    pub const fn allows_outbound(&self) -> bool {
        matches!(self, Self::Outbound | Self::Both)
    }

    pub const fn allows_inbound(&self) -> bool {
        matches!(self, Self::Inbound | Self::Both)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Peer {
    pub id: PeerId,
    /// Remote DaemonId. Empty until the handshake completes for `Pending` rows.
    pub daemon_id: String,
    pub name: String,
    pub url: String,
    pub bearer_token_hash: Vec<u8>,
    /// Plaintext bearer token. Stored alongside the hash so the outbound
    /// client task can re-issue `Hello` after daemon restarts. Spec calls
    /// for hash-only storage; v1 keeps plaintext for ergonomics until
    /// a token-rotation UX lands.
    pub bearer_token: String,
    pub public_key: Option<Vec<u8>>,
    pub state: PeerState,
    pub last_seen: Option<i64>,
    pub registered_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerOutboxRow {
    pub id: String,
    pub peer_id: PeerId,
    pub mail_id: String,
    pub sender_seq: u64,
    pub recipient: String,
    pub sender: Option<String>,
    pub topic: Option<String>,
    pub body: String,
    pub created_at: i64,
    pub attempts: u32,
    pub next_attempt_at: i64,
    pub state: PeerOutboxState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerInboxRow {
    pub sender_daemon_id: String,
    pub sender_seq: u64,
    pub mail_id: String,
    pub received_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicFederation {
    pub id: String,
    pub peer_id: PeerId,
    pub topic: String,
    pub direction: FederationDirection,
    pub created_at: i64,
}

#[cfg(test)]
mod federation_type_tests {
    use super::*;

    #[test]
    fn validate_daemon_id_accepts_lowercase_hex() {
        assert!(validate_daemon_id("abcd1234"));
        assert!(validate_daemon_id("00000000"));
        assert!(validate_daemon_id("ffffffff"));
    }

    #[test]
    fn validate_daemon_id_rejects_other() {
        assert!(!validate_daemon_id(""));
        assert!(!validate_daemon_id("abcd123"));
        assert!(!validate_daemon_id("abcd1234x"));
        assert!(!validate_daemon_id("ABCD1234"));
        assert!(!validate_daemon_id("ghij1234"));
    }

    #[test]
    fn federation_direction_merge_idempotent_to_both() {
        assert_eq!(
            FederationDirection::Outbound.merge(FederationDirection::Inbound),
            FederationDirection::Both
        );
        assert_eq!(
            FederationDirection::Outbound.merge(FederationDirection::Outbound),
            FederationDirection::Outbound
        );
        assert_eq!(
            FederationDirection::Outbound.merge(FederationDirection::Both),
            FederationDirection::Both
        );
    }
}
