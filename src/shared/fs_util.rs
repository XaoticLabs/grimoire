//! Filesystem helpers shared across daemon and CLI. Centralising the
//! owner-only secret-write path means an audit greps one symbol, not every
//! `set_mode` call site.

use std::path::Path;

use anyhow::Result;

/// Set `path` to mode `0600` on Unix; no-op elsewhere.
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

/// Write `contents` to `path` (creating parent dirs) then tighten to `0600`.
/// Used for every on-disk secret so the permission step can't be forgotten.
pub fn write_secret(path: &Path, contents: impl AsRef<[u8]>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, contents)?;
    set_owner_only(path)?;
    Ok(())
}
