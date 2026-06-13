#![allow(missing_docs)] // Path and version constants; self-explanatory.

use std::path::PathBuf;

pub const DAEMON_PORT: u16 = 6660;
pub const SOCKET_FILENAME: &str = "grimd.sock";
pub const DB_FILENAME: &str = "grimoire.db";
pub const DAEMON_ID_FILENAME: &str = "daemon.id";

/// Federation peer-protocol version (`proto/peer.proto`). Negotiated via
/// `Hello { protocol_version }`; bump on wire-incompatible changes.
pub const PEER_PROTOCOL_VERSION: u32 = 1;

/// Highest RPC protocol version this build supports.
pub const RPC_PROTOCOL_VERSION: u32 = 1;

/// Worker control-protocol version (`proto/worker.proto`). Sent on `Register`,
/// rejected by the daemon on mismatch; bump on wire-incompatible changes.
pub const WORKER_PROTOCOL_VERSION: u32 = 1;

pub const MAX_WORKSPACE_NAME_LEN: usize = 64;
pub const MAX_MEMORY_KEY_LEN: usize = 256;
pub const WORKSPACE_WATCH_DEBOUNCE_MS: u64 = 200;
pub const WORKSPACE_WATCH_BATCH_MAX: usize = 64;
pub const WORKSPACE_WATCH_DEFAULT_IGNORES: &[&str] = &[
    ".git/**",
    "target/**",
    "node_modules/**",
    ".DS_Store",
    "*.swp",
];

/// Root state directory (config, socket, db, pid, auth token, TLS certs,
/// workspaces). `~/.grimoire` by default; `GRIMOIRE_DIR` overrides it so
/// demos and multi-daemon-on-one-host setups can run fully isolated.
pub fn grimoire_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("GRIMOIRE_DIR")
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    let home = dirs_home();
    home.join(".grimoire")
}

pub fn socket_path() -> PathBuf {
    grimoire_dir().join(SOCKET_FILENAME)
}

pub fn db_path() -> PathBuf {
    grimoire_dir().join(DB_FILENAME)
}

pub fn pid_path() -> PathBuf {
    grimoire_dir().join("grimd.pid")
}

pub fn workspaces_root() -> PathBuf {
    grimoire_dir().join("workspaces")
}

/// Path to the persisted DaemonId file. Honours `GRIMOIRE_DAEMON_ID_PATH` for
/// tests so two-daemon harnesses can mint distinct IDs.
pub fn daemon_id_path() -> PathBuf {
    if let Ok(s) = std::env::var("GRIMOIRE_DAEMON_ID_PATH") {
        return PathBuf::from(s);
    }
    grimoire_dir().join(DAEMON_ID_FILENAME)
}

pub fn generate_short_id() -> String {
    uuid::Uuid::new_v4().to_string()[..8].to_string()
}

fn dirs_home() -> PathBuf {
    directories::BaseDirs::new().map_or_else(|| PathBuf::from("~"), |d| d.home_dir().to_path_buf())
}
