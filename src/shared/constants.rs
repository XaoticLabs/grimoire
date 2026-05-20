use std::path::PathBuf;

pub const DAEMON_PORT: u16 = 6660;
pub const SOCKET_FILENAME: &str = "grimd.sock";
pub const DB_FILENAME: &str = "grimoire.db";
pub const DAEMON_ID_FILENAME: &str = "daemon.id";

/// Federation peer-protocol version. Bumped whenever wire-incompatible
/// changes ship in `proto/peer.proto`. Negotiated via `Hello { protocol_version }`.
pub const PEER_PROTOCOL_VERSION: u32 = 1;

/// Highest RPC protocol version this build supports.
pub const RPC_PROTOCOL_VERSION: u32 = 1;

/// Wire-format version of the worker control protocol. Bumped whenever
/// `proto/worker.proto` ships wire-incompatible changes. Sent by the
/// worker on `Register` and rejected by the daemon if it doesn't match.
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

pub fn grimoire_dir() -> PathBuf {
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

/// Generate a short 8-character ID from a UUID.
pub fn generate_short_id() -> String {
    uuid::Uuid::new_v4().to_string()[..8].to_string()
}

fn dirs_home() -> PathBuf {
    directories::BaseDirs::new().map_or_else(|| PathBuf::from("~"), |d| d.home_dir().to_path_buf())
}
