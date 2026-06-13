//! Federated namespace memory RPC. Values are treated as UTF-8 strings at the
//! RPC boundary (the store keeps raw bytes). Versioning is the LWW tuple
//! (lamport, origin_daemon_id).
use serde::{Deserialize, Serialize};

use super::EmptyResult;
use crate::shared::types::AgentId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsPutParams {
    pub namespace: String,
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsPutResult {
    pub lamport: u64,
    pub origin_daemon_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsGetParams {
    pub namespace: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsGetResult {
    pub value: String,
    pub lamport: u64,
    pub origin_daemon_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsListParams {
    pub namespace: String,
    #[serde(default)]
    pub prefix: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NsListResult {
    pub entries: Vec<NsListItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsListItem {
    pub key: String,
    pub lamport: u64,
    pub origin_daemon_id: String,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsDeleteParams {
    pub namespace: String,
    pub key: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
}

pub type NsDeleteResult = EmptyResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsFederateParams {
    pub namespace: String,
    pub peer: String,
    /// "inbound" | "outbound" | "both"
    pub direction: String,
}

pub type NsFederateResult = EmptyResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsUnfederateParams {
    pub namespace: String,
    pub peer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsUnfederateResult {
    pub removed: bool,
}
