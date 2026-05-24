//! Filesystem helpers shared across the daemon and CLI.
//!
//! Currently just the owner-only permission helper used wherever we persist
//! a secret (auth token, TLS key, daemon id). Centralising it means a future
//! audit can grep one symbol instead of tracking down every `set_mode`.

use std::path::Path;

use anyhow::Result;

/// Set the file at `path` to mode `0600` on Unix. No-op on other platforms
/// (best effort; ACLs are out of scope).
#[cfg(unix)]
pub fn set_owner_only(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(0o600);
    std::fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
pub fn set_owner_only(_path: &Path) -> Result<()> {
    Ok(())
}
