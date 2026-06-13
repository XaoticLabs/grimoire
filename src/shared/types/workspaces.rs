//! Workspaces, federated memory entries, and name/key validators.

use super::{DaemonId, FederationDirection, PeerId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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

/// `Local` = real on-disk worktree; `Shadow` = thin mirror row of a workspace
/// homed on another daemon (`home_*` populated, no real `path`). Existing rows
/// default to `Local` per the migration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum WorkspaceKind {
    Local,
    Shadow,
}

impl_state_enum!(WorkspaceKind {
    Local => "Local",
    Shadow => "Shadow",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Workspace {
    pub id: WorkspaceId,
    pub path: PathBuf,
    pub repo_path: PathBuf,
    pub branch: String,
    pub state: WorkspaceState,
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_local_kind")]
    pub kind: WorkspaceKind,
    /// Set iff `Shadow`: the daemon owning the canonical workspace.
    #[serde(default)]
    pub home_daemon_id: Option<DaemonId>,
    /// Set iff `Shadow`: the workspace id on the home daemon.
    #[serde(default)]
    pub home_workspace_id: Option<WorkspaceId>,
}

const fn default_local_kind() -> WorkspaceKind {
    WorkspaceKind::Local
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceListEntry {
    pub id: WorkspaceId,
    pub path: PathBuf,
    pub branch: String,
    pub state: WorkspaceState,
    pub agent_count: u32,
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_local_kind")]
    pub kind: WorkspaceKind,
    #[serde(default)]
    pub home_daemon_id: Option<DaemonId>,
    #[serde(default)]
    pub home_workspace_id: Option<WorkspaceId>,
}

/// One (peer, workspace) opt-in for cross-daemon file-event federation. Lives
/// on the home daemon for `Outbound`/`Both`, on the consumer for `Inbound`/`Both`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceFederation {
    pub id: String,
    pub peer_id: PeerId,
    pub workspace_id: WorkspaceId,
    pub direction: FederationDirection,
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
    if s.len() > crate::shared::constants::MAX_WORKSPACE_NAME_LEN {
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
    if s.len() > crate::shared::constants::MAX_MEMORY_KEY_LEN {
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
