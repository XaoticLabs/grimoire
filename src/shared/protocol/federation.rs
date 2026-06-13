//! Cross-daemon federation RPC: peer add/list/remove/ping, topic and
//! workspace federation, agent-lifecycle replication, and cross-peer scroll
//! task dispatch. The namespace-memory federation calls live in the
//! `namespace` module.
use serde::{Deserialize, Serialize};

use crate::shared::types::{DaemonId, NamespaceFederation, TopicFederation, WorkspaceFederation};

// One row per (peer, scope) federation. `created_at` is unix seconds for the
// topic/ns shape and ISO-8601 for workspaces (matches the underlying types).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFederationsResult {
    pub federations: Vec<TopicFederation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsFederationsResult {
    pub federations: Vec<NamespaceFederation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceFederationsResult {
    pub federations: Vec<WorkspaceFederation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerAddParams {
    pub name: String,
    pub url: String,
    pub bearer_token: String,
    /// PEM cert of the remote daemon, exchanged out-of-band. Pinned as the
    /// sole TLS trust anchor for this peer.
    pub cert_pem: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerAddResult {
    pub peer_id: String,
    pub daemon_id: DaemonId,
}

/// Result of `peer.local-cert`: this daemon's own transport identity, to be
/// handed to a remote operator so they can pin it when adding us as a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerLocalCertResult {
    pub cert_pem: String,
    pub fingerprint_sha256: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerListParams {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerListResult {
    pub peers: Vec<PeerSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSummary {
    pub peer_id: String,
    pub name: String,
    pub daemon_id: String,
    pub url: String,
    pub state: String,
    pub last_seen: Option<i64>,
    pub outbox_depth: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRemoveParams {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRemoveResult {
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerPingParams {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerPingResult {
    pub rtt_ms: u64,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFederateParams {
    pub topic: String,
    pub peer: String,
    /// `inbound` | `outbound` | `both`
    pub direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFederateResult {
    pub topic: String,
    pub direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicUnfederateParams {
    pub topic: String,
    pub peer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicUnfederateResult {
    pub removed: bool,
}

/// Home-daemon side: opt a local workspace into cross-daemon file-event
/// federation with a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceFederateParams {
    pub workspace: String,
    pub peer: String,
    /// `inbound` | `outbound` | `both`
    pub direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceFederateResult {
    pub workspace: String,
    pub direction: String,
}

/// Consumer-daemon side: create a local shadow workspace pointing at a remote
/// home, and pre-record the inbound federation row that authorizes its events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceFederateSubscribeParams {
    /// `<home-daemon-id>/<home-workspace-id>`: address of the tracked remote workspace.
    pub home: String,
    pub peer: String,
    /// Optional local alias. Defaults to `<home-workspace>-shadow`.
    #[serde(default)]
    pub alias: Option<String>,
    /// Optional branch label (display only; no worktree on disk).
    #[serde(default = "default_shadow_branch")]
    pub branch: String,
}

fn default_shadow_branch() -> String {
    "main".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::struct_field_names)] // all three fields legitimately end in `_id`
pub struct WorkspaceFederateSubscribeResult {
    pub local_workspace_id: String,
    pub home_daemon_id: String,
    pub home_workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceUnfederateParams {
    pub workspace: String,
    pub peer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceUnfederateResult {
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLifecycleFederateParams {
    pub peer: String,
    pub direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLifecycleFederateResult {
    pub peer: String,
    pub direction: String,
    /// Count of existing agents snapshotted into the outbox as a replay so the
    /// receiver gets current state without waiting. `0` if direction is inbound-only.
    pub replayed: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLifecycleUnfederateParams {
    pub peer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentLifecycleUnfederateResult {
    pub removed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSetAcceptDispatchParams {
    pub peer: String,
    pub accept: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSetAcceptDispatchResult {
    pub peer: String,
    pub accept: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollDispatchTaskParams {
    pub scroll_id: String,
    pub task_id: String,
    pub peer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollDispatchTaskResult {
    pub scroll_id: String,
    pub task_id: String,
    pub peer: String,
    /// Outbox sender_seq of the queued delivery, for operators verifying the row landed.
    pub sender_seq: u64,
}
