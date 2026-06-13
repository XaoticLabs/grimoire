use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrimwConfig {
    /// `https://host:port` of the daemon's worker listener. Plaintext rejected; link is mTLS.
    pub daemon_url: String,
    #[serde(default)]
    pub secret: String,
    /// Daemon worker-listener cert (PEM), pinned as the TLS trust anchor.
    pub daemon_cert_path: PathBuf,
    /// Optional SHA-256 of the pinned daemon cert; cross-checked against
    /// `daemon_cert_path` to catch a stale/wrong cert file.
    #[serde(default)]
    pub daemon_cert_sha256: String,
    /// This worker's transport-identity cert/key (the client cert). Unset →
    /// auto-generated under `<grimoire_dir>/tls/worker.{crt,key}`.
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,
    pub worker_id: Option<String>,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: u32,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub providers: HashMap<String, ProviderConfig>,
}

const fn default_max_concurrent() -> u32 {
    4
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub binary: String,
    #[serde(default)]
    pub args_template: Vec<String>,
    #[serde(default = "default_version")]
    pub version: String,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

fn default_version() -> String {
    "0.0.0".to_string()
}

impl GrimwConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let cfg: Self =
            toml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
        Ok(cfg)
    }
}
