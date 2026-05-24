#![allow(missing_docs)] // RPC wire types; one-line docs per message pending.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::types::{
    Agent, AgentEvent, AgentId, AgentState, AgentSummary, DaemonId, Mail, MailState,
    MemoryListItem, ScrollId, TaskConflict, TaskState, WakeSource, WorkspaceId, WorkspaceListEntry,
};

/// Empty `{}` result body for RPC methods that just report success.
/// Serializes to `{}` so the wire format matches per-method empty result types.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyResult {}

/// Single-`id` params shape for RPC methods whose only argument is an id
/// (agent, scroll, workspace, …). Aliases share the wire shape `{"id": "..."}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdParams {
    pub id: String,
}

/// JSON-RPC request from CLI to daemon
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RpcRequest {
    pub method: String,
    pub params: serde_json::Value,
    pub id: u64,
    /// RPC protocol version. Existing callers omit this; the dispatcher
    /// defaults to `1`. Unknown versions are rejected with
    /// `unsupported_protocol_version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<u32>,
    /// Bearer token. Required when the daemon cannot identify the caller
    /// via `SO_PEERCRED` (i.e. UDS connections from a different UID, or
    /// any future non-UDS transport that reuses `RpcRequest`). UDS
    /// connections from the daemon's own UID may omit the token; the
    /// kernel's peer-credential check substitutes for authentication.
    /// Sent on every request; the server caches `authed=true` per
    /// connection after the first successful check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

/// JSON-RPC response from daemon to CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcResponse {
    pub const fn success(id: u64, result: serde_json::Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Build a success response from any serializable payload. Panics only if
    /// serialization fails, which for the plain `derive(Serialize)` result
    /// structs used here is a programmer error, not a runtime condition.
    pub fn success_json<T: Serialize>(id: u64, value: &T) -> Self {
        let result = serde_json::to_value(value)
            .expect("RPC result payloads are plain derive(Serialize) structs");
        Self::success(id, result)
    }

    pub const fn error(id: u64, code: i32, message: String) -> Self {
        Self {
            id,
            result: None,
            error: Some(RpcError { code, message }),
        }
    }
}

// --- Method parameters ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummonParams {
    pub task: String,
    pub name: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub keep_alive: Option<bool>,
    /// "never" | "on_failure" (default: never)
    #[serde(default)]
    pub restart_policy: Option<String>,
    #[serde(default)]
    pub max_restarts: Option<u32>,
    #[serde(default)]
    pub restart_window_secs: Option<u32>,
    /// Address: `agent://<id>` or `topic://<name>`. Reserved sender prefixes
    /// (`supervisor://`, `wake://`) are rejected at `mail.send`.
    #[serde(default)]
    pub escalate_to: Option<String>,
    /// Workspace name to summon into. Mutually exclusive with `cwd`.
    #[serde(default)]
    pub workspace: Option<String>,
    /// Supervision-tree parent. When set, banishing the parent cascades:
    /// every live child of `parent_agent_id` is also banished. Lets a
    /// coordinator agent spawn helpers and know they'll all die together.
    #[serde(default)]
    pub parent_agent_id: Option<AgentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircleParams {
    pub state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindParams {
    pub id: AgentId,
    #[serde(default)]
    pub tail: Option<usize>,
}

pub type BanishParams = IdParams;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvokeParams {
    pub id: AgentId,
    pub message: String,
}

// --- Method results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummonResult {
    pub id: AgentId,
    pub name: Option<String>,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircleResult {
    pub agents: Vec<AgentSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanishResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatusResult {
    pub uptime_secs: i64,
    pub agent_count: usize,
    pub active_count: usize,
    #[serde(default)]
    pub queued_count: usize,
    #[serde(default)]
    pub max_concurrent_agents: u32,
    /// Stable 8-hex DaemonId of this daemon. Display form: `grimd-<id>`.
    #[serde(default)]
    pub daemon_id: Option<DaemonId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerStatus {
    pub worker_id: String,
    pub in_flight: u32,
    pub max_concurrent: u32,
    pub last_heartbeat_age_secs: u64,
    pub providers: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StatusResponse {
    #[serde(default)]
    pub agents: Vec<crate::shared::types::AgentSummary>,
    #[serde(default)]
    pub pacts: Vec<serde_json::Value>,
    #[serde(default)]
    pub workers: Vec<WorkerStatus>,
    #[serde(default)]
    pub uptime_secs: i64,
    #[serde(default)]
    pub active_count: usize,
    #[serde(default)]
    pub queued_count: usize,
    #[serde(default)]
    pub max_concurrent_agents: u32,
    #[serde(default)]
    pub daemon_id: Option<DaemonId>,
}

// --- Pact params/results ---

// --- Queue params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub id: AgentId,
    pub lane: String,
    pub age_seconds: u64,
    pub provider: Option<String>,
    pub cwd: String,
    pub model: Option<String>,
    pub block_reason: Option<String>,
    pub task_text: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueListResponse {
    pub entries: Vec<QueueEntry>,
}

// --- Replay / chronicle params/results ---

pub type ReplayParams = IdParams;

/// One entry of an agent's reconstructed life: the durable per-agent `seq`,
/// the event `kind` tag, the stored timestamp, and the full event payload.
/// The daemon returns the whole timeline; windowing (`--from`/`--until`),
/// kind filtering, and state-at-point reconstruction happen client-side so
/// the RPC stays a dumb read and the reconstruction logic stays testable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayEntry {
    pub seq: i64,
    pub kind: String,
    pub ts: String,
    pub event: StreamEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResponse {
    pub agent_id: AgentId,
    pub entries: Vec<ReplayEntry>,
}

// --- Pact params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PactCreateParams {
    pub source_id: AgentId,
    pub task_tpl: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PactCreateResult {
    pub id: String,
    pub source_id: AgentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PactListParams {
    pub source_id: Option<AgentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PactListResult {
    pub pacts: Vec<super::types::Pact>,
}

// --- Mail params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSendParams {
    pub to: String,
    pub body: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
    #[serde(default)]
    pub wake_eligible: Option<bool>,
    /// Correlation id of an earlier mail this one is a reply to. The id is
    /// echoed back unchanged so request/reply (see `mail.ask`) can match.
    #[serde(default)]
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailAskParams {
    pub to: String,
    pub body: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
    /// Max time to wait for a reply, in milliseconds. Defaults to 30 000.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailAskResult {
    /// The full reply mail row.
    pub reply: Mail,
}

/// Post a task to a topic (or single agent) and collect replies for a
/// fixed window. Used to gather bids from a fleet; the typical pattern is
/// "fan out a job to `topic://workers`, take the first/best bid."
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailTenderParams {
    pub to: String,
    pub body: String,
    #[serde(default)]
    pub sender: Option<AgentId>,
    /// How long to wait for bids, in milliseconds. Defaults to 30 000.
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalRecord {
    pub id: String,
    pub target_id: AgentId,
    pub evaluator_id: AgentId,
    pub score: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalRecordParams {
    pub target_id: AgentId,
    pub evaluator_id: AgentId,
    pub score: f64,
    #[serde(default)]
    pub verdict: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalRecordResult {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalListParams {
    pub target_id: AgentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalListResult {
    pub results: Vec<EvalRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResultParams {
    pub id: AgentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResultResponse {
    /// Provider-extracted final result text, or `None` if the agent has no
    /// usable result yet (still running, no stdout, provider declined).
    pub result: Option<String>,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetStatus {
    pub name: String,
    pub daily_usd: f64,
    pub spent_usd: f64,
    pub providers: Vec<String>,
    pub hard: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetListResult {
    pub day: String,
    pub budgets: Vec<BudgetStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailTenderResult {
    /// Mail ids of the original tender posts (one per topic subscriber when
    /// `to` was a topic, otherwise one).
    pub request_mail_ids: Vec<String>,
    /// Bids collected during the window, in arrival order.
    pub bids: Vec<Mail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSendResult {
    pub delivered: u32,
    pub mail_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailListParams {
    pub agent_id: AgentId,
    #[serde(default)]
    pub after_seq: Option<i64>,
    #[serde(default)]
    pub state: Option<MailState>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailListResult {
    pub mails: Vec<Mail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailAckParams {
    pub mail_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailAckResult {
    pub acked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSubscribeParams {
    pub agent_id: AgentId,
    pub topic: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailSubscribeResult {
    pub subscription_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailUnsubscribeParams {
    pub subscription_id: String,
}

pub type MailUnsubscribeResult = EmptyResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicCount {
    pub topic: String,
    pub subscriber_count: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MailTopicsResult {
    pub topics: Vec<TopicCount>,
}

// --- Wake-source params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeAddParams {
    pub agent_id: AgentId,
    pub kind: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeAddResult {
    pub wake_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WakeListParams {
    #[serde(default)]
    pub agent_id: Option<AgentId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WakeListResult {
    pub sources: Vec<WakeSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeRemoveParams {
    pub wake_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeRemoveResult {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeTestParams {
    pub wake_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WakeTestResult {
    pub success: bool,
    pub mail_id: String,
}

// --- Workspace params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceCreateParams {
    pub name: String,
    pub repo_path: String,
    pub branch: String,
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

// --- Memory params/results ---

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

// --- Federated namespace memory params/results ---
// Values are treated as UTF-8 strings at the RPC boundary (the store keeps
// raw bytes). Versioning is the LWW tuple (lamport, origin_daemon_id).

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

// --- Process inventory ---
// Per-agent OS-process visibility for the dashboard's "Processes" panel.
// `alive` is a kill(pid, 0) check at request time; a "stuck" row is one
// where the agent is in a terminal state but the OS process is still alive.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProcess {
    pub agent_id: AgentId,
    pub state: String,
    pub task: Option<String>,
    pub pid: u32,
    pub alive: bool,
    /// `true` iff the agent's state is terminal (Complete/Failed/Banished)
    /// but `alive` is also true (i.e. the OS process outlived its agent).
    pub stuck: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProcessesResult {
    pub processes: Vec<AgentProcess>,
}

// --- Federation listing ---
// One row per (peer, scope) federation. `created_at` is unix seconds for the
// topic/ns shape and ISO-8601 for workspaces (matches the underlying types).

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicFederationsResult {
    pub federations: Vec<super::types::TopicFederation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NsFederationsResult {
    pub federations: Vec<super::types::NamespaceFederation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceFederationsResult {
    pub federations: Vec<super::types::WorkspaceFederation>,
}

// --- Notify params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyParams {
    pub message: String,
    #[serde(default)]
    pub agent_id: Option<AgentId>,
    /// Severity hint passed through to the webhook payload. Defaults to
    /// `"info"` when absent.
    #[serde(default)]
    pub level: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotifyResult {
    pub published: bool,
}

// --- Streaming events (sent over bind/SSE) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StreamEvent {
    #[serde(rename = "output")]
    Output {
        agent_id: AgentId,
        stream: String, // "stdout" or "stderr"
        line: String,
    },
    #[serde(rename = "state_change")]
    StateChange {
        agent_id: AgentId,
        old_state: AgentState,
        new_state: AgentState,
    },
    #[serde(rename = "agent_created")]
    AgentCreated { agent: Agent },
    #[serde(rename = "agent_event")]
    AgentEvent { event: AgentEvent },
    #[serde(rename = "scroll_progress")]
    ScrollProgress {
        scroll_id: ScrollId,
        total: usize,
        complete: usize,
        active: usize,
        blocked: usize,
        failed: usize,
        skipped: usize,
    },
    #[serde(rename = "task_state_change")]
    TaskStateChange {
        scroll_id: ScrollId,
        task_id: String,
        task_name: String,
        old_state: TaskState,
        new_state: TaskState,
    },
    #[serde(rename = "agent_queued")]
    AgentQueued {
        agent_id: AgentId,
        lane: String,
        block_reason: Option<String>,
    },
    #[serde(rename = "worker_registered")]
    WorkerRegistered { worker_id: String },
    #[serde(rename = "mail_sent")]
    MailSent {
        mail_id: String,
        sender_id: Option<AgentId>,
        recipient_id: Option<AgentId>,
        topic: Option<String>,
    },
    #[serde(rename = "mail_received")]
    MailReceived {
        mail_id: String,
        recipient_id: AgentId,
        sender_id: Option<AgentId>,
        topic: Option<String>,
        body_preview: String,
        wake_eligible: bool,
        /// `Some(<daemon-id>)` when the mail arrived via a federated peer;
        /// `None` for purely local mail.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_daemon_id: Option<DaemonId>,
    },
    #[serde(rename = "mail_delivered")]
    MailDelivered {
        mail_id: String,
        recipient_id: AgentId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_daemon_id: Option<DaemonId>,
    },
    #[serde(rename = "mail_failed")]
    MailFailed {
        mail_id: String,
        recipient_id: AgentId,
        sender_id: Option<AgentId>,
        reason: String,
    },
    #[serde(rename = "wake_source_registered")]
    WakeSourceRegistered {
        wake_id: String,
        agent_id: AgentId,
        kind: String,
    },
    #[serde(rename = "wake_source_fired")]
    WakeSourceFired {
        wake_id: String,
        agent_id: AgentId,
        mail_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        via: Option<String>,
    },
    #[serde(rename = "wake_source_failed")]
    WakeSourceFailed {
        wake_id: String,
        agent_id: AgentId,
        reason: String,
    },
    #[serde(rename = "wake_source_retired")]
    WakeSourceRetired {
        wake_id: String,
        agent_id: AgentId,
        reason: String,
    },
    #[serde(rename = "restart_scheduled")]
    RestartScheduled {
        agent_id: AgentId,
        attempt: u32,
        max: u32,
        fire_at_unix: i64,
        rate_limited: bool,
    },
    #[serde(rename = "restarted")]
    Restarted {
        agent_id: AgentId,
        attempt: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mail_id: Option<String>,
    },
    #[serde(rename = "restart_budget_exhausted")]
    RestartBudgetExhausted {
        agent_id: AgentId,
        /// "budget_spent" | "tree_depth_exceeded"
        reason: String,
    },
    #[serde(rename = "escalated")]
    Escalated {
        agent_id: AgentId,
        target: String,
        fanout_count: u32,
    },
    #[serde(rename = "workspace_created")]
    WorkspaceCreated {
        workspace_id: WorkspaceId,
        path: PathBuf,
        branch: String,
    },
    #[serde(rename = "workspace_destroyed")]
    WorkspaceDestroyed { workspace_id: WorkspaceId },
    #[serde(rename = "workspace_orphan_dir_detected")]
    WorkspaceOrphanDirDetected { path: PathBuf },
    #[serde(rename = "memory_written")]
    MemoryWritten {
        workspace_id: WorkspaceId,
        key: String,
        version: u64,
        agent_id: Option<AgentId>,
    },
    #[serde(rename = "memory_deleted")]
    MemoryDeleted {
        workspace_id: WorkspaceId,
        key: String,
        agent_id: Option<AgentId>,
    },
    #[serde(rename = "workspace_file_changed")]
    WorkspaceFileChanged {
        workspace_id: WorkspaceId,
        paths: Vec<String>,
        kinds: Vec<String>,
        truncated_count: u32,
    },
    // --- Federation (peer-link) events ---
    #[serde(rename = "peer_handshake_ok")]
    PeerHandshakeOk {
        peer_id: String,
        peer_daemon_id: DaemonId,
        peer_name: String,
    },
    #[serde(rename = "peer_handshake_failed")]
    PeerHandshakeFailed {
        peer_name: Option<String>,
        reason: String,
    },
    #[serde(rename = "peer_stream_connected")]
    PeerStreamConnected { peer_id: String },
    #[serde(rename = "peer_stream_disconnected")]
    PeerStreamDisconnected { peer_id: String, reason: String },
    #[serde(rename = "peer_mail_forwarded")]
    PeerMailForwarded {
        peer_id: String,
        mail_id: String,
        sender_seq: u64,
    },
    #[serde(rename = "peer_mail_forward_failed")]
    PeerMailForwardFailed {
        peer_id: String,
        mail_id: String,
        reason: String,
    },
    #[serde(rename = "peer_mail_received")]
    PeerMailReceived {
        peer_id: String,
        mail_id: String,
        sender_daemon_id: DaemonId,
    },
    #[serde(rename = "topic_federation_added")]
    TopicFederationAdded {
        peer_id: String,
        topic: String,
        direction: String,
    },
    #[serde(rename = "topic_federation_removed")]
    TopicFederationRemoved { peer_id: String, topic: String },
    /// A notification destined for the human operator. Emitted by the `notify`
    /// RPC (`source = "agent"`, an agent calling `grim notify`) or internally
    /// (`source = "system"`). The `Notifier` subscriber forwards matching ones
    /// to the configured webhook; it also lands in the durable event log.
    #[serde(rename = "notification")]
    Notification {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_id: Option<AgentId>,
        message: String,
        level: String,
        source: String,
    },
}

impl StreamEvent {
    /// Extract the agent ID from any event variant, if applicable. For mail
    /// events this returns the stream the event should appear on (sender for
    /// `MailSent`/`MailFailed`, recipient for `MailReceived`/`MailDelivered`).
    pub fn agent_id(&self) -> Option<&str> {
        // Each variant binds a differently-named field (agent_id vs
        // recipient_id vs sender_id); collapsing the arms would obscure
        // which field is being read for each event type.
        #[allow(clippy::match_same_arms)]
        match self {
            Self::Output { agent_id, .. } => Some(agent_id),
            Self::StateChange { agent_id, .. } => Some(agent_id),
            Self::AgentCreated { agent } => Some(&agent.id),
            Self::AgentEvent { event } => Some(&event.agent_id),
            Self::AgentQueued { agent_id, .. } => Some(agent_id),
            Self::MailSent { sender_id, .. } => sender_id.as_deref(),
            Self::MailReceived { recipient_id, .. } => Some(recipient_id),
            Self::MailDelivered { recipient_id, .. } => Some(recipient_id),
            Self::MailFailed {
                sender_id,
                recipient_id,
                ..
            } => sender_id.as_deref().or(Some(recipient_id.as_str())),
            Self::WakeSourceRegistered { agent_id, .. }
            | Self::WakeSourceFired { agent_id, .. }
            | Self::WakeSourceFailed { agent_id, .. }
            | Self::WakeSourceRetired { agent_id, .. } => Some(agent_id),
            Self::RestartScheduled { agent_id, .. }
            | Self::Restarted { agent_id, .. }
            | Self::RestartBudgetExhausted { agent_id, .. }
            | Self::Escalated { agent_id, .. } => Some(agent_id),
            Self::MemoryWritten { agent_id, .. } | Self::MemoryDeleted { agent_id, .. } => {
                agent_id.as_deref()
            }
            Self::Notification { agent_id, .. } => agent_id.as_deref(),
            Self::ScrollProgress { .. }
            | Self::TaskStateChange { .. }
            | Self::WorkerRegistered { .. }
            | Self::WorkspaceCreated { .. }
            | Self::WorkspaceDestroyed { .. }
            | Self::WorkspaceOrphanDirDetected { .. }
            | Self::WorkspaceFileChanged { .. }
            | Self::PeerHandshakeOk { .. }
            | Self::PeerHandshakeFailed { .. }
            | Self::PeerStreamConnected { .. }
            | Self::PeerStreamDisconnected { .. }
            | Self::PeerMailForwarded { .. }
            | Self::PeerMailForwardFailed { .. }
            | Self::PeerMailReceived { .. }
            | Self::TopicFederationAdded { .. }
            | Self::TopicFederationRemoved { .. } => None,
        }
    }

    /// Extract the scroll ID, for scroll-scoped events only.
    pub fn scroll_id(&self) -> Option<&str> {
        match self {
            Self::ScrollProgress { scroll_id, .. } | Self::TaskStateChange { scroll_id, .. } => {
                Some(scroll_id)
            }
            _ => None,
        }
    }

    /// Stable kind tag matching the serde rename for this variant.
    /// Used as the durable event log's `kind` column so serialized payload
    /// Wire tag for this event (the `type` field on the JSON envelope) and
    /// the SQL `events.kind` column value. The drift-detection test
    /// `kind_matches_serde_rename_for_each_variant` keeps these in sync with
    /// each variant's `#[serde(rename = "…")]`. Hand-maintained because
    /// `serde_variant::to_variant_name` does not support internally-tagged
    /// enums.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Output { .. } => "output",
            Self::StateChange { .. } => "state_change",
            Self::AgentCreated { .. } => "agent_created",
            Self::AgentEvent { .. } => "agent_event",
            Self::ScrollProgress { .. } => "scroll_progress",
            Self::TaskStateChange { .. } => "task_state_change",
            Self::AgentQueued { .. } => "agent_queued",
            Self::WorkerRegistered { .. } => "worker_registered",
            Self::MailSent { .. } => "mail_sent",
            Self::MailReceived { .. } => "mail_received",
            Self::MailDelivered { .. } => "mail_delivered",
            Self::MailFailed { .. } => "mail_failed",
            Self::WakeSourceRegistered { .. } => "wake_source_registered",
            Self::WakeSourceFired { .. } => "wake_source_fired",
            Self::WakeSourceFailed { .. } => "wake_source_failed",
            Self::WakeSourceRetired { .. } => "wake_source_retired",
            Self::RestartScheduled { .. } => "restart_scheduled",
            Self::Restarted { .. } => "restarted",
            Self::RestartBudgetExhausted { .. } => "restart_budget_exhausted",
            Self::Escalated { .. } => "escalated",
            Self::WorkspaceCreated { .. } => "workspace_created",
            Self::WorkspaceDestroyed { .. } => "workspace_destroyed",
            Self::WorkspaceOrphanDirDetected { .. } => "workspace_orphan_dir_detected",
            Self::MemoryWritten { .. } => "memory_written",
            Self::MemoryDeleted { .. } => "memory_deleted",
            Self::WorkspaceFileChanged { .. } => "workspace_file_changed",
            Self::PeerHandshakeOk { .. } => "peer_handshake_ok",
            Self::PeerHandshakeFailed { .. } => "peer_handshake_failed",
            Self::PeerStreamConnected { .. } => "peer_stream_connected",
            Self::PeerStreamDisconnected { .. } => "peer_stream_disconnected",
            Self::PeerMailForwarded { .. } => "peer_mail_forwarded",
            Self::PeerMailForwardFailed { .. } => "peer_mail_forward_failed",
            Self::PeerMailReceived { .. } => "peer_mail_received",
            Self::TopicFederationAdded { .. } => "topic_federation_added",
            Self::TopicFederationRemoved { .. } => "topic_federation_removed",
            Self::Notification { .. } => "notification",
        }
    }
}

// --- Scroll params/results ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollInscribeParams {
    pub spec_path: String,
    pub max_concurrency: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollInscribeResult {
    pub id: ScrollId,
    pub name: String,
    pub task_count: usize,
    pub conflicts: Vec<TaskConflict>,
}

pub type ScrollActivateParams = IdParams;
pub type ScrollStatusParams = IdParams;
pub type ScrollAbandonParams = IdParams;

// --- Federation peer / topic params ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerAddParams {
    pub name: String,
    pub url: String,
    pub bearer_token: String,
    /// PEM-encoded certificate of the remote daemon, exchanged out-of-band
    /// alongside the token. Pinned as the sole TLS trust anchor for this peer.
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

// --- Workspace federation ---

/// Home-daemon side: opt a local workspace into cross-daemon file event
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

/// Consumer-daemon side: create a local shadow workspace pointing at a
/// remote home, and pre-record the inbound federation row so events
/// from that peer are authorized when the producer/consumer paths land.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceFederateSubscribeParams {
    /// `<home-daemon-id>/<home-workspace-id>`. The canonical address of
    /// the remote workspace this shadow tracks.
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
    /// Local workspace id assigned to the new shadow row.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::types::{Agent, AgentEvent, AgentState, TaskState};
    use chrono::Utc;
    use std::path::PathBuf;

    #[test]
    fn agent_id_extraction() {
        let event = StreamEvent::Output {
            agent_id: "abc".to_string(),
            stream: "stdout".to_string(),
            line: "hello".to_string(),
        };
        assert_eq!(event.agent_id(), Some("abc"));

        let event = StreamEvent::StateChange {
            agent_id: "xyz".to_string(),
            old_state: AgentState::Active,
            new_state: AgentState::Complete,
        };
        assert_eq!(event.agent_id(), Some("xyz"));

        // AgentCreated event
        let agent = Agent {
            id: "test1234".to_string(),
            name: None,
            state: AgentState::Summoning,
            task: None,
            model: None,
            provider: None,
            cwd: PathBuf::from("/tmp"),
            pid: None,
            session_id: None,
            exit_code: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            worker_id: None,
            restart_policy: crate::shared::types::RestartPolicy::Never,
            restart_count: 0,
            workspace_id: None,
        };
        assert_eq!(
            StreamEvent::AgentCreated { agent }.agent_id(),
            Some("test1234")
        );

        let event = StreamEvent::AgentEvent {
            event: AgentEvent {
                id: None,
                agent_id: "evt12345".to_string(),
                event_type: "stdout".to_string(),
                payload: "data".to_string(),
                created_at: Utc::now(),
            },
        };
        assert_eq!(event.agent_id(), Some("evt12345"));
    }

    #[test]
    fn kind_matches_serde_rename_for_each_variant() {
        let cases: &[(&str, StreamEvent)] = &[
            (
                "output",
                StreamEvent::Output {
                    agent_id: "a".into(),
                    stream: "stdout".into(),
                    line: "x".into(),
                },
            ),
            (
                "state_change",
                StreamEvent::StateChange {
                    agent_id: "a".into(),
                    old_state: AgentState::Active,
                    new_state: AgentState::Complete,
                },
            ),
            (
                "scroll_progress",
                StreamEvent::ScrollProgress {
                    scroll_id: "s".into(),
                    total: 1,
                    complete: 0,
                    active: 1,
                    blocked: 0,
                    failed: 0,
                    skipped: 0,
                },
            ),
            (
                "task_state_change",
                StreamEvent::TaskStateChange {
                    scroll_id: "s".into(),
                    task_id: "t".into(),
                    task_name: "T".into(),
                    old_state: TaskState::Blocked,
                    new_state: TaskState::Active,
                },
            ),
        ];
        for (expected, ev) in cases {
            assert_eq!(ev.kind(), *expected);
        }
    }

    #[test]
    fn scroll_id_extraction() {
        let scroll = StreamEvent::ScrollProgress {
            scroll_id: "s1".into(),
            total: 1,
            complete: 0,
            active: 1,
            blocked: 0,
            failed: 0,
            skipped: 0,
        };
        assert_eq!(scroll.scroll_id(), Some("s1"));

        let task = StreamEvent::TaskStateChange {
            scroll_id: "s2".into(),
            task_id: "t".into(),
            task_name: "T".into(),
            old_state: TaskState::Blocked,
            new_state: TaskState::Active,
        };
        assert_eq!(task.scroll_id(), Some("s2"));

        let out = StreamEvent::Output {
            agent_id: "a".into(),
            stream: "stdout".into(),
            line: "x".into(),
        };
        assert_eq!(out.scroll_id(), None);
    }

    #[test]
    fn agent_id_scroll_events_are_none() {
        let event = StreamEvent::ScrollProgress {
            scroll_id: "s1".to_string(),
            total: 4,
            complete: 1,
            active: 2,
            blocked: 1,
            failed: 0,
            skipped: 0,
        };
        assert_eq!(event.agent_id(), None);

        let event = StreamEvent::TaskStateChange {
            scroll_id: "s1".to_string(),
            task_id: "r1".to_string(),
            task_name: "Task 1".to_string(),
            old_state: TaskState::Blocked,
            new_state: TaskState::Active,
        };
        assert_eq!(event.agent_id(), None);
    }
}
