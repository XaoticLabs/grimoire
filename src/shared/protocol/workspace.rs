//! Workspace lifecycle (create/list/destroy/assign) and per-workspace memory
//! KV (put/get/list/delete). Cross-daemon workspace federation lives in the
//! `federation` module.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::{EmptyResult, IdParams};
use crate::shared::types::{AgentId, MemoryListItem, WorkspaceId, WorkspaceListEntry};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceCreateParams {
    pub name: String,
    pub repo_path: String,
    pub branch: String,
    /// If set, copy every `workspace_memory` row from this source into the new
    /// workspace (seeding swarm children). Source must exist; a copy failure
    /// leaves the workspace empty but does not abort creation.
    #[serde(default)]
    pub copy_memory_from: Option<WorkspaceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceCreateResult {
    pub id: WorkspaceId,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkspaceListParams {
    #[serde(default)]
    pub include_orphans: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkspaceListResult {
    pub workspaces: Vec<WorkspaceListEntry>,
    #[serde(default)]
    pub orphans: Vec<String>,
}

pub type WorkspaceDestroyParams = IdParams;

pub type WorkspaceDestroyResult = EmptyResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceAssignParams {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
}

pub type WorkspaceAssignResult = EmptyResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPutParams {
    pub workspace_id: WorkspaceId,
    pub key: String,
    pub value: serde_json::Value,
    #[serde(default)]
    pub expected_version: Option<u64>,
    #[serde(default)]
    pub sender: Option<AgentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPutResult {
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryGetParams {
    pub workspace_id: WorkspaceId,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryGetResult {
    pub value: serde_json::Value,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryListParams {
    pub workspace_id: WorkspaceId,
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryListResult {
    pub entries: Vec<MemoryListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryDeleteParams {
    pub workspace_id: WorkspaceId,
    pub key: String,
    #[serde(default)]
    pub expected_version: Option<u64>,
    #[serde(default)]
    pub sender: Option<AgentId>,
}

pub type MemoryDeleteResult = EmptyResult;
