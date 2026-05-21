//! Shared authentication token used by the CLI (over UDS) and dashboard
//! (over HTTP). Peer-federation and worker-pool links use their own
//! per-link bearer tokens — those are orthogonal trust domains and live
//! elsewhere (`peer_registry`, `WorkerConfig.secret`).
//!
//! ## Resolution order
//!
//! On daemon boot, the active token is resolved in this order:
//!
//! 1. `GRIMOIRE_AUTH_TOKEN` environment variable (operator override / CI).
//! 2. `[daemon.auth] token = "..."` in `config.toml` (explicit config).
//! 3. `~/.grimoire/auth.token` (auto-generated on first start, mode 0600).
//!
//! The CLI uses the same resolution order. With no config and no env, both
//! sides converge on the auto-generated file — zero-config for the solo
//! developer, explicit override for shared/CI machines.
//!
//! ## Verification
//!
//! All comparisons go through [`AuthToken::verify`], which uses
//! `subtle::ConstantTimeEq` to avoid leaking timing information.

#![warn(missing_docs)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use subtle::ConstantTimeEq;

use super::constants;

/// Filename of the auto-generated token under [`constants::grimoire_dir`].
pub const AUTH_TOKEN_FILENAME: &str = "auth.token";
/// Environment variable that overrides every other token source.
pub const AUTH_TOKEN_ENV: &str = "GRIMOIRE_AUTH_TOKEN";

/// Length of an auto-generated token, in hex characters (= 32 random bytes).
const GENERATED_TOKEN_HEX_LEN: usize = 64;

/// Opaque holder for the daemon's bearer token. Constructable from any
/// resolution source; comparisons must go through [`AuthToken::verify`].
#[derive(Clone)]
pub struct AuthToken(String);

impl AuthToken {
    /// Wrap a raw token string. The input is stored verbatim — callers are
    /// responsible for trimming whitespace before construction.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Returns the raw token bytes as a `&str`. Avoid logging this value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Constant-time equality check.
    #[must_use]
    pub fn verify(&self, provided: &str) -> bool {
        let a = self.0.as_bytes();
        let b = provided.as_bytes();
        if a.len() != b.len() {
            // ConstantTimeEq still does work to avoid early-exit, but inputs
            // of different lengths can't match by definition. Use a fixed
            // dummy compare so timing reveals only that the lengths differ
            // (which is unavoidable).
            let _ = a.ct_eq(a);
            return false;
        }
        a.ct_eq(b).into()
    }
}

impl std::fmt::Debug for AuthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never log the token itself.
        write!(f, "AuthToken(<redacted>)")
    }
}

/// Default path: `~/.grimoire/auth.token`. Honours `GRIMOIRE_AUTH_TOKEN_PATH`
/// for tests so harnesses can isolate the file.
#[must_use]
pub fn token_path() -> PathBuf {
    if let Ok(s) = std::env::var("GRIMOIRE_AUTH_TOKEN_PATH") {
        return PathBuf::from(s);
    }
    constants::grimoire_dir().join(AUTH_TOKEN_FILENAME)
}

/// Daemon-side: resolve the active token, generating a fresh one and
/// persisting it if no source supplied one.
///
/// `config_override` is the value of `[daemon.auth].token` from the
/// config file, if set.
pub fn load_or_init_daemon(config_override: Option<&str>) -> Result<AuthToken> {
    if let Some(tok) = env_token() {
        return Ok(AuthToken::new(tok));
    }
    if let Some(tok) = config_override {
        let tok = tok.trim();
        if !tok.is_empty() {
            return Ok(AuthToken::new(tok));
        }
    }
    let path = token_path();
    if let Some(existing) = read_token_file(&path)? {
        return Ok(AuthToken::new(existing));
    }
    let generated = generate_token();
    write_token_file(&path, &generated)
        .with_context(|| format!("writing auth token to {}", path.display()))?;
    Ok(AuthToken::new(generated))
}

/// CLI-side: resolve the active token *without* generating one. Returns an
/// error if no source has a token (typical cause: daemon hasn't started).
pub fn load_for_client() -> Result<AuthToken> {
    if let Some(tok) = env_token() {
        return Ok(AuthToken::new(tok));
    }
    let path = token_path();
    match read_token_file(&path)? {
        Some(tok) => Ok(AuthToken::new(tok)),
        None => Err(anyhow::anyhow!(
            "no auth token found: set {} or start the daemon (which writes {})",
            AUTH_TOKEN_ENV,
            path.display()
        )),
    }
}

fn env_token() -> Option<String> {
    std::env::var(AUTH_TOKEN_ENV).ok().and_then(|s| {
        let t = s.trim().to_string();
        if t.is_empty() { None } else { Some(t) }
    })
}

fn read_token_file(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading auth token from {}", path.display()))?;
    let t = raw.trim().to_string();
    if t.is_empty() { Ok(None) } else { Ok(Some(t)) }
}

fn write_token_file(path: &Path, token: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, token)?;
    set_owner_only_permissions(path)?;
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(0o600);
    std::fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_owner_only_permissions(_path: &Path) -> Result<()> {
    // Non-Unix platforms: best effort; ACLs are out of scope here.
    Ok(())
}

/// 32 random bytes rendered as 64 lowercase hex chars. Sourced from
/// two UUIDv4s (16 bytes each) to avoid pulling in `rand` directly.
fn generate_token() -> String {
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let mut out = String::with_capacity(GENERATED_TOKEN_HEX_LEN);
    for byte in a.as_bytes().iter().chain(b.as_bytes().iter()) {
        let _ = write!(out, "{byte:02x}");
    }
    debug_assert_eq!(out.len(), GENERATED_TOKEN_HEX_LEN);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Tests in this module mutate process-global env vars (`GRIMOIRE_AUTH_TOKEN`,
    /// `GRIMOIRE_AUTH_TOKEN_PATH`). Cargo runs tests in parallel by default,
    /// so without serialization one test's env changes leak into another's
    /// `load_or_init_daemon` call. This mutex serializes the env-touching
    /// tests; it lives in test code only.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// # Safety
    ///
    /// `std::env::set_var` / `remove_var` became `unsafe` in Rust 1.80
    /// because concurrent reads from other threads can observe a torn
    /// `environ` pointer. The caller must hold [`ENV_LOCK`] for the entire
    /// scope in which any env-touching code may run; the lock provides
    /// the required exclusion across all tests in this module.
    #[allow(unsafe_code, clippy::disallowed_methods)]
    unsafe fn env_set(key: &str, value: impl AsRef<std::ffi::OsStr>) {
        unsafe { std::env::set_var(key, value) };
    }

    /// # Safety
    ///
    /// Same contract as [`env_set`].
    #[allow(unsafe_code, clippy::disallowed_methods)]
    unsafe fn env_remove(key: &str) {
        unsafe { std::env::remove_var(key) };
    }

    #[test]
    fn verify_matches_self() {
        let t = AuthToken::new("abc123");
        assert!(t.verify("abc123"));
    }

    #[test]
    fn verify_rejects_wrong_token() {
        let t = AuthToken::new("abc123");
        assert!(!t.verify("abc124"));
        assert!(!t.verify(""));
        assert!(!t.verify("abc1234"));
    }

    #[test]
    fn generated_token_is_64_hex_chars() {
        let t = generate_token();
        assert_eq!(t.len(), GENERATED_TOKEN_HEX_LEN);
        assert!(
            t.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        // Two consecutive generations should never collide.
        assert_ne!(t, generate_token());
    }

    #[test]
    fn auto_gen_writes_0600_file() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.token");
        // SAFETY: ENV_LOCK held for the scope of this test.
        unsafe {
            env_set("GRIMOIRE_AUTH_TOKEN_PATH", &path);
            env_remove(AUTH_TOKEN_ENV);
        }

        let tok = load_or_init_daemon(None).unwrap();
        assert_eq!(tok.as_str().len(), GENERATED_TOKEN_HEX_LEN);
        assert!(path.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "expected 0600, got {mode:o}");
        }

        // Second call returns the same token (no regeneration).
        let tok2 = load_or_init_daemon(None).unwrap();
        assert_eq!(tok.as_str(), tok2.as_str());

        // SAFETY: ENV_LOCK still held.
        unsafe { env_remove("GRIMOIRE_AUTH_TOKEN_PATH") };
    }

    #[test]
    fn env_overrides_file_and_config() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.token");
        std::fs::write(&path, "from-file").unwrap();
        // SAFETY: ENV_LOCK held for the scope of this test.
        unsafe {
            env_set("GRIMOIRE_AUTH_TOKEN_PATH", &path);
            env_set(AUTH_TOKEN_ENV, "from-env");
        }

        let tok = load_or_init_daemon(Some("from-config")).unwrap();
        assert_eq!(tok.as_str(), "from-env");

        // SAFETY: ENV_LOCK still held.
        unsafe {
            env_remove(AUTH_TOKEN_ENV);
            env_remove("GRIMOIRE_AUTH_TOKEN_PATH");
        }
    }

    #[test]
    fn config_overrides_file() {
        let _guard = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.token");
        std::fs::write(&path, "from-file").unwrap();
        // SAFETY: ENV_LOCK held for the scope of this test.
        unsafe {
            env_set("GRIMOIRE_AUTH_TOKEN_PATH", &path);
            env_remove(AUTH_TOKEN_ENV);
        }

        let tok = load_or_init_daemon(Some("from-config")).unwrap();
        assert_eq!(tok.as_str(), "from-config");

        // SAFETY: ENV_LOCK still held.
        unsafe { env_remove("GRIMOIRE_AUTH_TOKEN_PATH") };
    }
}
