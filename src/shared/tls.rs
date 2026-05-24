//! Self-signed identity certificates for peer-federation and worker-pool
//! mutual TLS. Each daemon and each worker owns a long-lived self-signed
//! cert+key pair (its transport identity); trust between two endpoints is
//! established by **pinning** the other side's certificate out-of-band,
//! exactly the way the per-link bearer tokens already are (see
//! [`super::auth`], `peer add --token`, `WorkerConfig.secret`).
//!
//! ## Why self-signed + pinning rather than a CA
//!
//! Grimoire links are point-to-point and few: an operator runs `grim peer
//! add <name> <url> --token T --cert peer.crt`, exchanging the peer's cert
//! alongside its token. Each self-signed cert is its own trust anchor, so
//! pinning that exact cert as the TLS root verifies the remote's identity
//! with no PKI to provision. The bearer token rides *inside* the now-encrypted
//! channel as a second factor.
//!
//! ## Resolution order (daemon/worker side)
//!
//! 1. Explicit `tls_cert_path` / `tls_key_path` from config, if both set.
//! 2. `<grimoire_dir>/tls/<name>.crt` + `.key` (auto-generated on first
//!    start, mode 0600), where `<name>` is `daemon` or `worker`.
//!
//! With no config, the daemon converges on the auto-generated file — zero
//! config for the solo operator, explicit override for org PKI.

#![warn(missing_docs)]

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::constants;

/// Constant DNS SAN minted into every identity cert. Because trust is pinned
/// to the exact certificate, the SAN match performed by rustls is not the
/// security boundary — a fixed name lets the client always set a matching
/// `domain_name` without knowing the remote's id ahead of time. `.invalid`
/// is reserved (RFC 6761) so it can never resolve on a real network.
pub const TLS_SAN: &str = "grimoire.invalid";

/// A loaded transport identity: the PEM-encoded cert (also used as the pinned
/// trust anchor by the remote) and its private key.
#[derive(Clone)]
pub struct Identity {
    cert_pem: String,
    key_pem: String,
}

impl Identity {
    /// The certificate in PEM form. Safe to share — this is what a remote
    /// pins to trust this endpoint.
    #[must_use]
    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    /// The private key in PEM form. Never log or transmit this.
    #[must_use]
    pub fn key_pem(&self) -> &str {
        &self.key_pem
    }

    /// Build a tonic [`tonic::transport::Identity`] for use in a server or
    /// client TLS config.
    #[must_use]
    pub fn to_tonic(&self) -> tonic::transport::Identity {
        tonic::transport::Identity::from_pem(self.cert_pem.as_bytes(), self.key_pem.as_bytes())
    }

    /// Lowercase hex SHA-256 fingerprint of the certificate's DER bytes — the
    /// value an operator pins (e.g. `WorkerConfig.daemon_cert_sha256`).
    #[must_use]
    pub fn fingerprint(&self) -> String {
        cert_fingerprint(&self.cert_pem)
    }
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never log the private key.
        write!(f, "Identity(fingerprint={})", self.fingerprint())
    }
}

/// Lowercase hex SHA-256 over the DER of the first certificate in `cert_pem`.
/// Returns an empty string if the PEM contains no certificate.
#[must_use]
pub fn cert_fingerprint(cert_pem: &str) -> String {
    let Some(der) = first_cert_der(cert_pem) else {
        return String::new();
    };
    let digest = ring::digest::digest(&ring::digest::SHA256, &der);
    let mut out = String::with_capacity(digest.as_ref().len() * 2);
    for b in digest.as_ref() {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Decode the first PEM `CERTIFICATE` block in `pem` to its DER bytes.
fn first_cert_der(pem: &str) -> Option<Vec<u8>> {
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";
    let start = pem.find(BEGIN)? + BEGIN.len();
    let end = pem[start..].find(END)? + start;
    let b64: String = pem[start..end].split_whitespace().collect();
    base64_decode(&b64)
}

/// Minimal standard-alphabet base64 decoder (no padding requirement). Avoids
/// pulling in a base64 crate just for PEM body decoding.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0;
    for &c in s.as_bytes() {
        if c == b'=' {
            break;
        }
        let v = val(c)?;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Default identity directory: `<grimoire_dir>/tls`. Honours
/// `GRIMOIRE_TLS_DIR` so test harnesses can isolate cert files.
#[must_use]
pub fn tls_dir() -> PathBuf {
    if let Ok(s) = std::env::var("GRIMOIRE_TLS_DIR") {
        return PathBuf::from(s);
    }
    constants::grimoire_dir().join("tls")
}

/// Load an identity from explicit cert+key paths if both are provided,
/// otherwise load-or-generate the auto-managed pair at
/// `<tls_dir>/<name>.crt` + `.key`.
///
/// `name` is a short stable label (`"daemon"` / `"worker"`) and is also
/// embedded as the cert's Common Name.
pub fn load_or_init(
    name: &str,
    cert_path: Option<&Path>,
    key_path: Option<&Path>,
) -> Result<Identity> {
    if let (Some(cp), Some(kp)) = (cert_path, key_path) {
        let cert_pem = std::fs::read_to_string(cp)
            .with_context(|| format!("reading tls cert from {}", cp.display()))?;
        let key_pem = std::fs::read_to_string(kp)
            .with_context(|| format!("reading tls key from {}", kp.display()))?;
        return Ok(Identity { cert_pem, key_pem });
    }

    let dir = tls_dir();
    let cp = dir.join(format!("{name}.crt"));
    let kp = dir.join(format!("{name}.key"));
    if cp.exists() && kp.exists() {
        let cert_pem = std::fs::read_to_string(&cp)
            .with_context(|| format!("reading tls cert from {}", cp.display()))?;
        let key_pem = std::fs::read_to_string(&kp)
            .with_context(|| format!("reading tls key from {}", kp.display()))?;
        return Ok(Identity { cert_pem, key_pem });
    }

    let id = generate(name)?;
    std::fs::create_dir_all(&dir).with_context(|| format!("creating tls dir {}", dir.display()))?;
    std::fs::write(&cp, &id.cert_pem)
        .with_context(|| format!("writing tls cert to {}", cp.display()))?;
    std::fs::write(&kp, &id.key_pem)
        .with_context(|| format!("writing tls key to {}", kp.display()))?;
    super::fs_util::set_owner_only(&cp)?;
    super::fs_util::set_owner_only(&kp)?;
    Ok(id)
}

/// Generate a fresh self-signed cert+key with CN=`name` and the constant
/// [`TLS_SAN`] (plus `name` as a DNS SAN) so a client can verify against
/// either. Not persisted — callers decide where it lives.
pub fn generate(name: &str) -> Result<Identity> {
    let cert_key =
        rcgen::generate_simple_self_signed(vec![TLS_SAN.to_string(), sanitize_san(name)])
            .context("generating self-signed identity cert")?;
    Ok(Identity {
        cert_pem: cert_key.cert.pem(),
        key_pem: cert_key.signing_key.serialize_pem(),
    })
}

/// rcgen requires DNS SAN entries to be valid hostnames; reduce an arbitrary
/// label to a safe form (alnum and `-`), falling back to a placeholder.
fn sanitize_san(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "node".to_string()
    } else {
        format!("{cleaned}.{TLS_SAN}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_produces_pinnable_pair() {
        let id = generate("daemon").unwrap();
        assert!(id.cert_pem().contains("BEGIN CERTIFICATE"));
        assert!(id.key_pem().contains("PRIVATE KEY"));
        // Fingerprint is a 64-char lowercase hex SHA-256.
        let fp = id.fingerprint();
        assert_eq!(fp.len(), 64, "fingerprint: {fp}");
        assert!(
            fp.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn fingerprint_is_stable_for_same_cert() {
        let id = generate("daemon").unwrap();
        assert_eq!(id.fingerprint(), cert_fingerprint(id.cert_pem()));
    }

    #[test]
    fn distinct_certs_have_distinct_fingerprints() {
        let a = generate("daemon").unwrap();
        let b = generate("daemon").unwrap();
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    // Mutates a process-global env var; `set_var`/`remove_var` are `unsafe`
    // since Rust 1.80 and on the clippy disallowed list. This test runs the
    // env-touching code single-threaded within its own scope.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn load_or_init_generates_then_reuses_0600() {
        let dir = tempfile::tempdir().unwrap();
        // SAFETY: single-threaded test; no other code reads GRIMOIRE_TLS_DIR here.
        unsafe { std::env::set_var("GRIMOIRE_TLS_DIR", dir.path()) };

        let first = load_or_init("daemon", None, None).unwrap();
        let cert = dir.path().join("daemon.crt");
        assert!(cert.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&cert).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "expected 0600, got {mode:o}");
        }

        // Second call reuses the persisted pair (same fingerprint).
        let second = load_or_init("daemon", None, None).unwrap();
        assert_eq!(first.fingerprint(), second.fingerprint());

        unsafe { std::env::remove_var("GRIMOIRE_TLS_DIR") };
    }

    #[test]
    fn explicit_paths_take_precedence() {
        let id = generate("daemon").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cp = dir.path().join("custom.crt");
        let kp = dir.path().join("custom.key");
        std::fs::write(&cp, id.cert_pem()).unwrap();
        std::fs::write(&kp, id.key_pem()).unwrap();

        let loaded = load_or_init("daemon", Some(&cp), Some(&kp)).unwrap();
        assert_eq!(loaded.fingerprint(), id.fingerprint());
    }
}
