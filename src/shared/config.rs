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
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            socket_path: None,
            log_level: default_log_level(),
            max_concurrent_agents: default_max_concurrent(),
        }
    }
}

fn default_max_concurrent() -> u32 {
    8
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
