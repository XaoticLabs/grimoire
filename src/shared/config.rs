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
    /// Inbound webhooks that publish into the mail bus. Keyed by the URL
    /// segment used in `POST /webhooks/<name>`. Empty (the default) keeps
    /// the webhook surface closed.
    #[serde(default)]
    pub webhooks: HashMap<String, WebhookConfig>,
}

/// One inbound webhook → mail bridge. Each entry exposes one endpoint at
/// `/webhooks/<name>` whose raw request body is delivered to `topic` (or to
/// `recipient` for direct mail). If `secret` is set, callers must present it
/// in the `X-Grimoire-Webhook-Token` header — the only auth on the otherwise-
/// public webhook surface. We deliberately *don't* implement provider-specific
/// HMAC schemes (GitHub's `X-Hub-Signature-256`, Slack's signing-secret); the
/// expected deploy is "reverse proxy → daemon," where the proxy unwraps any
/// such header and applies a daemon-side token instead.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Destination topic (without the `topic://` prefix). Mutually exclusive
    /// with `recipient`.
    #[serde(default)]
    pub topic: Option<String>,
    /// Direct agent recipient ID (without the `agent://` prefix). Mutually
    /// exclusive with `topic`.
    #[serde(default)]
    pub recipient: Option<String>,
    /// If present, the request must carry it in `X-Grimoire-Webhook-Token`.
    /// Unset = the endpoint is open to anyone who can reach the HTTP port.
    /// The daemon binds 127.0.0.1 by default, but operators exposing it to
    /// the network should always set this.
    #[serde(default)]
    pub secret: Option<String>,
}

/// Outbound notification webhook. When `webhook_url` is set the daemon POSTs
/// a JSON payload to it for each enabled trigger. Channel-agnostic: point it
/// at a Slack/Discord incoming-webhook, a relay, or any HTTP endpoint.
// Each bool is an independent TOML toggle mapping 1:1 to a config key; the
// clippy-suggested state-machine/enum refactor would obscure that mapping.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationsConfig {
    /// Webhook endpoint. `None` (default) disables outbound notifications
    /// entirely — the notifier task is never spawned.
    #[serde(default)]
    pub webhook_url: Option<String>,
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

impl Default for NotificationsConfig {
    fn default() -> Self {
        Self {
            webhook_url: None,
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
    /// The daemon's worker-listener transport identity (server cert/key).
    /// Unset → auto-generated + pinned under `<grimoire_dir>/tls/worker.{crt,key}`.
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,
    /// Paths to the PEM certs of workers allowed to connect (mTLS client-cert
    /// pinning). Each worker exposes its cert at `<grimoire_dir>/tls/worker.crt`.
    /// Empty → the listener binds but trusts no client cert, so no worker can
    /// register (the bearer secret alone is not accepted without a pinned cert).
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
    /// Federation: optional `host:port` to bind the peer gRPC listener.
    /// `None` (default) skips the listener — federation traffic cannot
    /// arrive but `mail.send` still routes locally.
    #[serde(default)]
    pub peer_listen_addr: Option<String>,
    /// Federation mTLS: explicit transport-identity cert/key. When unset
    /// (default) the daemon auto-generates and pins a self-signed pair under
    /// `<grimoire_dir>/tls/daemon.{crt,key}`. Set both for org PKI.
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,
    /// Authentication for the local UDS RPC and HTTP dashboard. Workers
    /// and peers carry their own per-link tokens (see `WorkerConfig.secret`
    /// and `peer add --token`); this section only controls the CLI/HTTP
    /// trust domain.
    #[serde(default)]
    pub auth: AuthConfig,
}

/// CLI/HTTP token configuration. See `src/shared/auth.rs` for resolution
/// order (env → this field → auto-generated `~/.grimoire/auth.token`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthConfig {
    /// Explicit token override. Empty/absent means "fall through to the
    /// auto-generated file."
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

    /// Build a fresh `Arc<AtomicU32>` initialized from the current
    /// `daemon.max_concurrent_agents` value. The daemon clones this handle
    /// to share with the scheduler; reloads call [`Config::apply_max_concurrent`]
    /// to update the live value without restart.
    pub fn max_concurrent_atomic(&self) -> Arc<AtomicU32> {
        Arc::new(AtomicU32::new(self.daemon.max_concurrent_agents))
    }

    /// Update an existing shared atomic to reflect the current config value.
    pub fn apply_max_concurrent(&self, atomic: &AtomicU32) {
        atomic.store(self.daemon.max_concurrent_agents, Ordering::Relaxed);
    }
}
