#![allow(missing_docs)] // Config schema; documentation pass pending.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use super::constants;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub daemon: DaemonConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub providers: HashMap<String, ProviderConfig>,
    #[serde(default)]
    pub worker: Option<WorkerConfig>,
    #[serde(default)]
    pub notifications: NotificationsConfig,
    /// Inbound webhooks → mail bus, keyed by the `POST /webhooks/<name>`
    /// segment. Empty (default) keeps the webhook surface closed.
    #[serde(default)]
    pub webhooks: HashMap<String, WebhookConfig>,
    /// Per-budget daily USD caps gated at agent dispatch, keyed by budget
    /// name. Empty (default) leaves spend unbounded.
    #[serde(default)]
    pub budgets: HashMap<String, BudgetConfig>,
    /// Allow/deny rules enforced at `agent.summon`. None (default) permits
    /// every provider and cwd; a missing field means no restriction on that axis.
    #[serde(default)]
    pub policy: Option<PolicyConfig>,
}

/// One inbound webhook → mail bridge at `/webhooks/<name>`: the raw request
/// body is delivered to `topic` (or `recipient`). We deliberately *don't*
/// implement provider-specific HMAC schemes (GitHub `X-Hub-Signature-256`,
/// Slack signing-secret); the expected deploy is reverse-proxy → daemon, where
/// the proxy unwraps any such header and applies the daemon-side `secret`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Destination topic (no `topic://` prefix). Exclusive with `recipient`.
    #[serde(default)]
    pub topic: Option<String>,
    /// Direct agent recipient ID (no `agent://` prefix). Exclusive with `topic`.
    #[serde(default)]
    pub recipient: Option<String>,
    /// If present, the request must carry it in `X-Grimoire-Webhook-Token`.
    /// Unset = open to anyone who can reach the HTTP port; operators exposing
    /// the (default 127.0.0.1) port to the network should always set this.
    #[serde(default)]
    pub secret: Option<String>,
}

/// Outbound notifications. Matched events fan out to each configured sink:
/// HTTPS POST (`webhook_url`), append-only log (`log_file`), desktop toast
/// (`desktop`). The notifier task spawns only when at least one sink is set.
// Each bool is an independent TOML toggle mapping 1:1 to a config key; the
// clippy-suggested enum refactor would obscure that mapping.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationsConfig {
    #[serde(default)]
    pub webhook_url: Option<String>,
    /// Append a JSON line per matched event. Created on first write; the
    /// parent directory must already exist.
    #[serde(default)]
    pub log_file: Option<PathBuf>,
    /// Desktop toast via `notify-send`. Linux-only (no-ops if not on PATH).
    #[serde(default)]
    pub desktop: bool,
    /// Notify when an agent reaches `Complete`.
    #[serde(default = "default_true")]
    pub on_completion: bool,
    /// Notify on `Failed` / `Banished` and restart-budget exhaustion.
    #[serde(default = "default_true")]
    pub on_failure: bool,
    /// Notify when a standing agent's wake source fires.
    #[serde(default = "default_true")]
    pub on_wake: bool,
    /// Notify when an agent emits its own message via `grim notify`.
    #[serde(default = "default_true")]
    pub on_agent_decided: bool,
    /// Per-request timeout for the webhook POST.
    #[serde(default = "default_notify_timeout")]
    pub timeout_secs: u64,
}

impl NotificationsConfig {
    /// True when at least one delivery sink is configured.
    pub const fn has_sink(&self) -> bool {
        self.webhook_url.is_some() || self.log_file.is_some() || self.desktop
    }
}

impl Default for NotificationsConfig {
    fn default() -> Self {
        Self {
            webhook_url: None,
            log_file: None,
            desktop: false,
            on_completion: true,
            on_failure: true,
            on_wake: true,
            on_agent_decided: true,
            timeout_secs: default_notify_timeout(),
        }
    }
}

const fn default_true() -> bool {
    true
}

const fn default_notify_timeout() -> u64 {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Required. Bearer token validated on every Register.
    pub secret: String,
    #[serde(default = "default_worker_listen_addr")]
    pub listen_addr: std::net::SocketAddr,
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,
    #[serde(default = "default_heartbeat_interval_hint")]
    pub heartbeat_interval_hint_secs: u64,
    /// Worker-listener transport identity (server cert/key). Unset →
    /// auto-generated + pinned under `<grimoire_dir>/tls/worker.{crt,key}`.
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,
    /// PEM certs of workers allowed to connect (mTLS client-cert pinning).
    /// Empty → listener binds but trusts no client cert, so no worker can
    /// register: the bearer secret alone is not accepted without a pinned cert.
    #[serde(default)]
    pub trusted_worker_certs: Vec<PathBuf>,
}

fn default_worker_listen_addr() -> std::net::SocketAddr {
    use std::net::{Ipv4Addr, SocketAddr};
    SocketAddr::from((Ipv4Addr::LOCALHOST, 7878))
}

const fn default_heartbeat_timeout() -> u64 {
    30
}

const fn default_heartbeat_interval_hint() -> u64 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub binary: String,
    #[serde(default)]
    pub args_template: Vec<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Per-provider confinement. `None` (default) = un-sandboxed; setting any
    /// field opts in. Enforcement layers apply at spawn when host tools are
    /// present, and warn rather than fail when they are not.
    #[serde(default)]
    pub sandbox: Option<SandboxConfig>,
    /// Token → USD pricing used to attribute spend to `[budgets.*]`. Unset →
    /// runs record token usage but charge no budget (e.g. free local models).
    #[serde(default)]
    pub pricing: Option<ProviderPricing>,
}

/// Price list for one provider, USD per million tokens, mirroring vendor
/// pricing pages so operators can paste them in.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderPricing {
    pub input_per_mtok: f64,
    pub output_per_mtok: f64,
    /// Defaults to `input_per_mtok / 10` (typical 90% cache-read discount).
    #[serde(default)]
    pub cache_read_per_mtok: Option<f64>,
    /// Defaults to `input_per_mtok * 1.25` (typical creation premium).
    #[serde(default)]
    pub cache_creation_per_mtok: Option<f64>,
}

impl ProviderPricing {
    pub fn cache_read(&self) -> f64 {
        self.cache_read_per_mtok
            .unwrap_or(self.input_per_mtok / 10.0)
    }
    pub fn cache_creation(&self) -> f64 {
        self.cache_creation_per_mtok
            .unwrap_or(self.input_per_mtok * 1.25)
    }
}

/// One UTC-day-windowed USD spend cap shared across a set of providers. A
/// completed agent attributes its cost to *every* budget whose `providers`
/// list includes the run's provider; at dispatch a matching budget at/over
/// `daily_usd` refuses new work (`hard`) or just warns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BudgetConfig {
    /// Max USD across this budget's providers in one UTC day.
    pub daily_usd: f64,
    /// Providers this budget governs. Empty = every provider.
    #[serde(default)]
    pub providers: Vec<String>,
    /// `true` (default) → dispatch refused once spend ≥ cap; `false` → WARN only.
    #[serde(default = "default_true")]
    pub hard: bool,
}

/// Allow/deny rules enforced at `agent.summon`. Empty `allow` = no allowlist
/// (everything passes); empty `deny` = nothing denied; `deny` wins on conflict.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// If non-empty, the agent's provider must be in this set.
    #[serde(default)]
    pub provider_allow: Vec<String>,
    /// Forbidden providers, even if also allow-listed.
    #[serde(default)]
    pub provider_deny: Vec<String>,
    /// If non-empty, `cwd` must lie under one of these prefixes (canonical
    /// absolute paths).
    #[serde(default)]
    pub cwd_allow_prefixes: Vec<PathBuf>,
    /// `cwd` prefixes that may never host an agent. Deny wins on conflict.
    #[serde(default)]
    pub cwd_deny_prefixes: Vec<PathBuf>,
}

/// Per-agent resource/capability limits across three independent axes:
/// resource (`memory_max`, `cpu_quota` via cgroup v2 / systemd-run), filesystem
/// + network (`fs_jail`, `allow_network`, `ro_paths`, `rw_paths` via `bwrap`),
/// and economic (`token_budget`, enforced in-process). Any subset may be set;
/// missing host tools degrade with a one-time warning rather than failing spawn.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SandboxConfig {
    /// Hard memory cap (bytes). Maps to systemd `MemoryMax=`.
    #[serde(default)]
    pub memory_max: Option<u64>,
    /// Percent of one core (100 = one full core). Maps to systemd `CPUQuota=`.
    #[serde(default)]
    pub cpu_quota_percent: Option<u32>,
    /// bwrap filesystem isolation: `ro_paths` read-only, `rw_paths` read-write,
    /// the rest of `/` hidden.
    #[serde(default)]
    pub fs_jail: bool,
    /// Only consulted when `fs_jail` (bwrap `--share-net`/`--unshare-net`).
    /// Cgroup egress isn't modelled; for network-deny without an fs jail run
    /// the daemon under an outer namespace.
    #[serde(default = "default_true")]
    pub allow_network: bool,
    /// Paths to mount read-only inside the jail.
    #[serde(default)]
    pub ro_paths: Vec<PathBuf>,
    /// Paths to mount read-write inside the jail. The agent's CWD is added
    /// automatically.
    #[serde(default)]
    pub rw_paths: Vec<PathBuf>,
    /// Max total tokens (input+output) over the agent's lifetime before the
    /// supervisor suspends it. `None` = unlimited.
    #[serde(default)]
    pub token_budget: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub socket_path: Option<PathBuf>,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_agents: u32,
    /// Daemon-wide cap on supervisor restarts per minute (token bucket).
    #[serde(default = "default_restart_rate_per_min")]
    pub restart_rate_per_min: u32,
    /// Tree-depth cap for supervisor escalation chains. Not user-configurable
    /// in v1; exposed for tests.
    #[serde(default = "default_tree_depth_cap")]
    pub tree_depth_cap: u32,
    /// Per-value cap on workspace memory entries (bytes after JSON canonicalization).
    #[serde(default = "default_workspace_value_cap")]
    pub workspace_value_cap_bytes: u64,
    /// Per-workspace total cap on memory KV bytes.
    #[serde(default = "default_workspace_total_cap")]
    pub workspace_total_cap_bytes: u64,
    /// Federation: max queued peer_outbox rows (Pending+InFlight) per peer.
    #[serde(default = "default_peer_outbox_max_depth")]
    pub peer_outbox_max_depth: u64,
    /// Federation: handshake timeout for `peer add`.
    #[serde(default = "default_peer_handshake_timeout_secs")]
    pub peer_handshake_timeout_secs: u64,
    /// Federation: heartbeat interval over the peer channel.
    #[serde(default = "default_peer_heartbeat_interval_secs")]
    pub peer_heartbeat_interval_secs: u64,
    /// Federation: `host:port` to bind the peer gRPC listener. `None`
    /// (default) skips it: federation can't arrive, but `mail.send` still
    /// routes locally.
    #[serde(default)]
    pub peer_listen_addr: Option<String>,
    /// Federation mTLS transport identity. Unset → auto-generated + pinned
    /// self-signed pair under `<grimoire_dir>/tls/daemon.{crt,key}`.
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,
    /// Auth for the local UDS RPC and HTTP dashboard only. Workers and peers
    /// carry their own per-link tokens (`WorkerConfig.secret`, `peer add --token`).
    #[serde(default)]
    pub auth: AuthConfig,
}

/// CLI/HTTP token configuration. See `src/shared/auth.rs` for resolution
/// order (env → this field → auto-generated `~/.grimoire/auth.token`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// Explicit token override. Empty/absent → fall through to the
    /// auto-generated file.
    #[serde(default)]
    pub token: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            socket_path: None,
            log_level: default_log_level(),
            max_concurrent_agents: default_max_concurrent(),
            restart_rate_per_min: default_restart_rate_per_min(),
            tree_depth_cap: default_tree_depth_cap(),
            workspace_value_cap_bytes: default_workspace_value_cap(),
            workspace_total_cap_bytes: default_workspace_total_cap(),
            peer_outbox_max_depth: default_peer_outbox_max_depth(),
            peer_handshake_timeout_secs: default_peer_handshake_timeout_secs(),
            peer_heartbeat_interval_secs: default_peer_heartbeat_interval_secs(),
            peer_listen_addr: None,
            tls_cert_path: None,
            tls_key_path: None,
            auth: AuthConfig::default(),
        }
    }
}

const fn default_peer_outbox_max_depth() -> u64 {
    10_000
}

const fn default_peer_handshake_timeout_secs() -> u64 {
    10
}

const fn default_peer_heartbeat_interval_secs() -> u64 {
    5
}

const fn default_max_concurrent() -> u32 {
    8
}

const fn default_restart_rate_per_min() -> u32 {
    30
}

const fn default_tree_depth_cap() -> u32 {
    3
}

const fn default_workspace_value_cap() -> u64 {
    262_144 // 256 KiB
}

const fn default_workspace_total_cap() -> u64 {
    67_108_864 // 64 MiB
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentConfig {
    #[serde(default)]
    pub default_model: Option<String>,
    #[serde(default)]
    pub default_cwd: Option<PathBuf>,
    #[serde(default)]
    pub claude_binary: Option<String>,
    #[serde(default)]
    pub pi_binary: Option<String>,
    #[serde(default)]
    pub default_provider: Option<String>,
}

const fn default_port() -> u16 {
    constants::DAEMON_PORT
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Config {
    pub fn config_path() -> PathBuf {
        constants::grimoire_dir().join("config.toml")
    }

    pub fn load() -> Result<Self> {
        let path = Self::config_path();
        if path.exists() {
            let content = std::fs::read_to_string(&path)?;
            let config: Self = toml::from_str(&content)?;
            Ok(config)
        } else {
            Ok(Self::default())
        }
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::config_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)?;
        std::fs::write(&path, content)?;
        Ok(())
    }

    pub const fn port(&self) -> u16 {
        self.daemon.port
    }

    pub fn socket_path(&self) -> PathBuf {
        self.daemon
            .socket_path
            .clone()
            .unwrap_or_else(constants::socket_path)
    }

    pub fn claude_binary(&self) -> String {
        self.agent
            .claude_binary
            .clone()
            .unwrap_or_else(|| "claude".to_string())
    }

    pub fn pi_binary(&self) -> String {
        self.agent
            .pi_binary
            .clone()
            .unwrap_or_else(|| "pi".to_string())
    }

    /// Shared handle over `daemon.max_concurrent_agents`, cloned to the
    /// scheduler; reloads call [`Config::apply_max_concurrent`] to update the
    /// live value without restart.
    pub fn max_concurrent_atomic(&self) -> Arc<AtomicU32> {
        Arc::new(AtomicU32::new(self.daemon.max_concurrent_agents))
    }

    /// Update an existing shared atomic to reflect the current config value.
    pub fn apply_max_concurrent(&self, atomic: &AtomicU32) {
        atomic.store(self.daemon.max_concurrent_agents, Ordering::Relaxed);
    }
}
