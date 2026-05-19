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
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,
}

fn default_worker_listen_addr() -> std::net::SocketAddr {
    "127.0.0.1:7878".parse().unwrap()
}

fn default_heartbeat_timeout() -> u64 {
    30
}

fn default_heartbeat_interval_hint() -> u64 {
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
            auth: AuthConfig::default(),
        }
    }
}

fn default_peer_outbox_max_depth() -> u64 {
    10_000
}

fn default_peer_handshake_timeout_secs() -> u64 {
    10
}

fn default_peer_heartbeat_interval_secs() -> u64 {
    5
}

fn default_max_concurrent() -> u32 {
    8
}

fn default_restart_rate_per_min() -> u32 {
    30
}

fn default_tree_depth_cap() -> u32 {
    3
}

fn default_workspace_value_cap() -> u64 {
    262_144 // 256 KiB
}

fn default_workspace_total_cap() -> u64 {
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
    pub default_provider: Option<String>,
}

fn default_port() -> u16 {
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
            let config: Config = toml::from_str(&content)?;
            Ok(config)
        } else {
            Ok(Config::default())
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

    pub fn port(&self) -> u16 {
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
