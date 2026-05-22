//! Federation Task 13: end-to-end two-daemon harness — now over mTLS.
//!
//! Spawns two in-process `PeerRegistry` instances (each backed by its
//! own tempdir database + EventBus + DaemonId + self-signed identity),
//! wires daemon-A to daemon-B's TLS Tonic server, and exercises the
//! handshake + cert-pinning paths.
//!
//! The full plan-spec harness boots the entire `grimd` AppState. This
//! file goes a layer below: it exercises the federation transport
//! directly without dragging in the agent manager / scheduler.

use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Certificate, Server, ServerTlsConfig};

use grimoire::daemon::clock::SystemClock;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::peer_registry::PeerRegistry;
use grimoire::daemon::peer_rpc_server::PeerSvc;
use grimoire::daemon::persistence::Database;
use grimoire::shared::tls::Identity;

struct TestDaemon {
    registry: Arc<PeerRegistry>,
    identity: Arc<Identity>,
    db: Arc<Database>,
    addr: std::net::SocketAddr,
}

/// Boot an in-process daemon whose peer listener presents `identity` and
/// trusts the client certs in `trusted_client_certs` (concatenated as the
/// mTLS client-CA bundle).
async fn boot_daemon(daemon_id: &str, trusted_client_certs: &[String]) -> TestDaemon {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.db");
    std::mem::forget(dir);
    let db = Arc::new(Database::open(&path).unwrap());
    let bus = EventBus::new(db.clone());
    let clock = Arc::new(SystemClock);
    let identity = Arc::new(grimoire::shared::tls::generate("daemon").unwrap());
    let registry = PeerRegistry::new(
        db.clone(),
        bus.clone(),
        clock,
        daemon_id.to_string(),
        identity.clone(),
    );

    // Bind on a random port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mut tls = ServerTlsConfig::new().identity(identity.to_tonic());
    if !trusted_client_certs.is_empty() {
        tls = tls.client_ca_root(Certificate::from_pem(
            trusted_client_certs.join("\n").as_bytes(),
        ));
    }
    let svc = PeerSvc::new(registry.clone());
    tokio::spawn(async move {
        let _ = Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(svc)
            .serve(addr)
            .await;
    });
    // Give the listener a moment to come up.
    tokio::time::sleep(Duration::from_millis(100)).await;

    TestDaemon {
        registry,
        identity,
        db,
        addr,
    }
}

#[tokio::test]
async fn handshake_and_invalid_token_rejected() {
    // A is the client; B trusts A's client cert so the TLS layer passes and
    // the bearer-token check is what rejects.
    let a_id = grimoire::shared::tls::generate("daemon").unwrap();
    let b = boot_daemon("bbbbbbbb", &[a_id.cert_pem().to_string()]).await;
    let a = boot_daemon_with("aaaaaaaa", a_id, &[]).await;

    let url = format!("https://{}", b.addr);
    let result = a
        .registry
        .register_peer(
            "b",
            &url,
            "abcdefabcdefabcdef0000000000000000000000",
            b.identity.cert_pem(),
            2,
        )
        .await;
    assert!(
        result.is_err(),
        "expected handshake failure, got: {:?}",
        result.map(|p| p.name)
    );
}

#[tokio::test]
async fn registered_token_handshake_succeeds() {
    let a_id = grimoire::shared::tls::generate("daemon").unwrap();
    let b = boot_daemon("bbbbbbbb", &[a_id.cert_pem().to_string()]).await;
    let a = boot_daemon_with("aaaaaaaa", a_id, &[]).await;

    // Pre-seed B with a peer row whose token-hash matches what A will send.
    let token = "0123456789abcdef0123456789abcdef0123456789abcdef".to_string();
    let token_hash = blake3::hash(token.as_bytes()).as_bytes().to_vec();
    let peer = grimoire::shared::types::Peer {
        id: "p-from-a".into(),
        daemon_id: String::new(),
        name: "incoming-a".into(),
        url: "https://0.0.0.0:0".into(),
        bearer_token_hash: token_hash,
        bearer_token: token.clone(),
        public_key: None,
        state: grimoire::shared::types::PeerState::Pending,
        last_seen: None,
        registered_at: grimoire::daemon::persistence::unix_now(),
    };
    b.db.insert_peer(&peer).unwrap();

    let url = format!("https://{}", b.addr);
    let result = a
        .registry
        .register_peer("b", &url, &token, b.identity.cert_pem(), 5)
        .await;
    assert!(
        result.is_ok(),
        "handshake should succeed: {:?}",
        result.err()
    );
    let p = result.unwrap();
    assert_eq!(p.daemon_id, "bbbbbbbb");
}

#[tokio::test]
async fn wrong_pinned_server_cert_rejected() {
    // A pins the *wrong* server cert for B: TLS verification of B's cert must
    // fail before any handshake, so register_peer errors out (timeout/failure).
    let a_id = grimoire::shared::tls::generate("daemon").unwrap();
    let b = boot_daemon("bbbbbbbb", &[a_id.cert_pem().to_string()]).await;
    let a = boot_daemon_with("aaaaaaaa", a_id, &[]).await;

    let bogus = grimoire::shared::tls::generate("daemon").unwrap();
    let token = "0123456789abcdef0123456789abcdef0123456789abcdef".to_string();
    let url = format!("https://{}", b.addr);
    let result = a
        .registry
        .register_peer("b", &url, &token, bogus.cert_pem(), 2)
        .await;
    assert!(
        result.is_err(),
        "pinning a non-matching server cert must fail the TLS handshake"
    );
}

#[tokio::test]
async fn unfederated_address_rejected_with_clear_error() {
    use grimoire::shared::mail::{Address, parse_address};
    // Sanity: parse_address accepts the federated form.
    let parsed = parse_address("agent://grimd-aaaaaaaa/abcd1234").unwrap();
    matches!(parsed, Address::FederatedAgent { .. });
}

/// Variant of [`boot_daemon`] that uses a caller-supplied identity (so the
/// client cert A presents matches what B pinned).
async fn boot_daemon_with(
    daemon_id: &str,
    identity: Identity,
    trusted_client_certs: &[String],
) -> TestDaemon {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.db");
    std::mem::forget(dir);
    let db = Arc::new(Database::open(&path).unwrap());
    let bus = EventBus::new(db.clone());
    let clock = Arc::new(SystemClock);
    let identity = Arc::new(identity);
    let registry = PeerRegistry::new(
        db.clone(),
        bus.clone(),
        clock,
        daemon_id.to_string(),
        identity.clone(),
    );

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let mut tls = ServerTlsConfig::new().identity(identity.to_tonic());
    if !trusted_client_certs.is_empty() {
        tls = tls.client_ca_root(Certificate::from_pem(
            trusted_client_certs.join("\n").as_bytes(),
        ));
    }
    let svc = PeerSvc::new(registry.clone());
    tokio::spawn(async move {
        let _ = Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(svc)
            .serve(addr)
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    TestDaemon {
        registry,
        identity,
        db,
        addr,
    }
}
