//! DaemonId minting + persistence.
//!
//! On first daemon boot we mint an 8-hex `DaemonId` and persist it atomically
//! to `~/.grimoire/daemon.id` (override via `GRIMOIRE_DAEMON_ID_PATH`). On
//! subsequent boots we load and validate the existing file. The file holds
//! the bare 8-hex string; the `grimd-` display prefix is constructed at
//! render time only.

use anyhow::{Result, anyhow};
use std::fs;
use std::io::Write;
use std::path::Path;

use crate::shared::constants::generate_short_id;
use crate::shared::types::{DaemonId, validate_daemon_id};

/// Read or mint the local DaemonId for `path`.
///
/// * If the file is absent, generate a fresh ID, write atomically (tempfile
///   + rename), and return it.
/// * If the file is present, trim whitespace and validate; reject corruption
///   with `invalid_daemon_id_file` rather than overwriting silently.
pub fn load_or_mint(path: &Path) -> Result<DaemonId> {
    if path.exists() {
        let contents = fs::read_to_string(path)?;
        let trimmed = contents.trim().to_string();
        if !validate_daemon_id(&trimmed) {
            return Err(anyhow!(
                "invalid_daemon_id_file: contents at {} are not 8 lowercase hex chars",
                path.display()
            ));
        }
        return Ok(trimmed);
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let id = generate_short_id();
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("daemon_id path has no parent"))?;

    // Atomic write: tempfile in same dir, set perms, persist.
    let mut tmp = tempfile::NamedTempFile::new_in(parent)?;
    tmp.write_all(id.as_bytes())?;
    tmp.flush()?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        fs::set_permissions(tmp.path(), perms)?;
    }

    // `persist_noclobber` would error if another racer wrote first; we want
    // to read theirs in that case.
    if tmp.persist_noclobber(path).is_ok() {
        Ok(id)
    } else {
        // Lost the race; load the winner's value.
        let contents = fs::read_to_string(path)?;
        let trimmed = contents.trim().to_string();
        if !validate_daemon_id(&trimmed) {
            return Err(anyhow!(
                "invalid_daemon_id_file: contents at {} are not 8 lowercase hex chars",
                path.display()
            ));
        }
        Ok(trimmed)
    }
}

/// Display form: `grimd-<id>`.
pub fn display(id: &str) -> String {
    format!("grimd-{id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_or_mint_creates_file_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.id");
        let id = load_or_mint(&path).unwrap();
        assert!(validate_daemon_id(&id));
        let from_disk = fs::read_to_string(&path).unwrap().trim().to_string();
        assert_eq!(id, from_disk);
    }

    #[test]
    fn load_or_mint_returns_existing_value_when_present() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.id");
        fs::write(&path, "abcd1234\n").unwrap();
        let id = load_or_mint(&path).unwrap();
        assert_eq!(id, "abcd1234");
    }

    #[test]
    fn load_or_mint_rejects_invalid_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.id");
        fs::write(&path, "NOTHEX").unwrap();
        let err = load_or_mint(&path).unwrap_err();
        assert!(format!("{err}").contains("invalid_daemon_id_file"));
    }

    #[cfg(unix)]
    #[test]
    fn persisted_file_is_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.id");
        load_or_mint(&path).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
