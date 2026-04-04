use std::path::PathBuf;

pub const DAEMON_PORT: u16 = 6660;
pub const SOCKET_FILENAME: &str = "grimd.sock";
pub const DB_FILENAME: &str = "grimoire.db";

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

fn dirs_home() -> PathBuf {
    directories::BaseDirs::new()
        .map(|d| d.home_dir().to_path_buf())
        .unwrap_or_else(|| PathBuf::from("~"))
}
