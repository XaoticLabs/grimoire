//! Agent lifecycle (summon/circle/bind/banish/invoke), daemon status, queue,
//! replay, pacts, artifacts, per-provider budgets, and process inventory.
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::{IdParams, StreamEvent};
use crate::shared::types::{AgentArtifact, AgentId, AgentSummary, DaemonId, Pact};

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
    /// Supervision-tree parent. Banishing the parent cascades to every live child.
    #[serde(default)]
    pub parent_agent_id: Option<AgentId>,
    /// USD ceiling for the supervision tree rooted here. Once summed tree spend
    /// reaches the cap, no member may start another run (queue dispatch, mail
    /// wake, manual invoke all blocked) and the operator is notified once.
    #[serde(default)]
    pub tree_budget_usd: Option<f64>,
    /// Idempotency key: a repeat summon with this key returns the existing agent
    /// instead of spawning a duplicate, so `summon` is retry-safe.
    #[serde(default)]
    pub idempotency_key: Option<String>,
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
    pub agents: Vec<AgentSummary>,
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

pub type ReplayParams = IdParams;

/// One entry of an agent's reconstructed life. The daemon returns the whole
/// timeline; windowing, kind filtering, and state-at-point reconstruction
/// happen client-side so the RPC stays a dumb read.
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
    pub pacts: Vec<Pact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResultParams {
    pub id: AgentId,
}

pub type AgentArtifactParams = IdParams;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentArtifactResult {
    /// The captured artifact, or `None` if the agent has not produced one yet.
    pub artifact: Option<AgentArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentResultResponse {
    /// Provider-extracted final result text, or `None` if none is available yet.
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

// Per-agent OS-process inventory. `alive` is a kill(pid, 0) check at request time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProcess {
    pub agent_id: AgentId,
    pub state: String,
    pub task: Option<String>,
    pub pid: u32,
    pub alive: bool,
    /// `true` iff the agent's state is terminal but the OS process is still alive.
    pub stuck: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProcessesResult {
    pub processes: Vec<AgentProcess>,
}
