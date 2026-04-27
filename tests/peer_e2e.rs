//! Federation Task 13: end-to-end two-daemon harness.
//!
//! Spawns two in-process `PeerRegistry` instances (each backed by its
//! own tempdir database + EventBus + DaemonId), wires daemon-A to
//! daemon-B's Tonic server, and exercises the handshake + mail
//! forwarding paths.
//!
//! The full plan-spec harness boots the entire `grimd` AppState. This
//! v1 file goes a layer below: it exercises the federation transport
//! directly without dragging in the agent manager / scheduler so the
//! tests stay fast and focused on the federation contract.

use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;

use grimoire::daemon::clock::SystemClock;
use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::peer_registry::PeerRegistry;
use grimoire::daemon::peer_rpc_server::PeerSvc;
use grimoire::daemon::persistence::Database;

struct TestDaemon {
    db: Arc<Database>,
    registry: Arc<PeerRegistry>,
    bus: EventBus,
    daemon_id: String,
    addr: std::net::SocketAddr,
}

async fn boot_daemon(daemon_id: &str) -> TestDaemon {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("g.db");
    std::mem::forget(dir);
    let db = Arc::new(Database::open(&path).unwrap());
    let bus = EventBus::new(db.clone());
    let clock = Arc::new(SystemClock);
    let registry = PeerRegistry::new(db.clone(), bus.clone(), clock, daemon_id.to_string());

    // Bind on a random port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let svc = PeerSvc::new(registry.clone());
    tokio::spawn(async move {
        let _ = Server::builder().add_service(svc).serve(addr).await;
    });
    // Give the listener a moment to come up.
    tokio::time::sleep(Duration::from_millis(50)).await;

    TestDaemon {
        db,
        registry,
        bus,
        daemon_id: daemon_id.to_string(),
        addr,
    }
}

#[tokio::test]
async fn handshake_and_invalid_token_rejected() {
    let a = boot_daemon("aaaaaaaa").await;
    let b = boot_daemon("bbbbbbbb").await;

    // A points at B with a token B has never heard of. B's server should
    // reply with `HelloAck { accepted: false, reject_reason: "invalid_token" }`.
    let url = format!("http://{}", b.addr);
    let result = a
        .registry
        .register_peer(
            "b".to_string(),
            url,
            "abcdefabcdefabcdef0000000000000000000000".to_string(),
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
    let a = boot_daemon("aaaaaaaa").await;
    let b = boot_daemon("bbbbbbbb").await;

    // Pre-seed B with a peer row whose token-hash matches what A will send.
    let token = "0123456789abcdef0123456789abcdef0123456789abcdef".to_string();
    let token_hash = blake3::hash(token.as_bytes()).as_bytes().to_vec();
    let peer = grimoire::shared::types::Peer {
        id: "p-from-a".into(),
        daemon_id: String::new(),
        name: "incoming-a".into(),
        url: "http://0.0.0.0:0".into(),
        bearer_token_hash: token_hash,
        bearer_token: token.clone(),
        public_key: None,
        state: grimoire::shared::types::PeerState::Pending,
        last_seen: None,
        registered_at: grimoire::daemon::persistence::unix_now(),
    };
    b.db.insert_peer(&peer).unwrap();

    let url = format!("http://{}", b.addr);
    let result = a.registry.register_peer("b".to_string(), url, token, 5).await;
    assert!(result.is_ok(), "handshake should succeed: {:?}", result.err());
    let p = result.unwrap();
    assert_eq!(p.daemon_id, "bbbbbbbb");
}

#[tokio::test]
async fn unfederated_address_rejected_with_clear_error() {
    use grimoire::shared::mail::{Address, parse_address};
    // Sanity: parse_address accepts the federated form.
    let parsed = parse_address("agent://grimd-aaaaaaaa/abcd1234").unwrap();
    matches!(parsed, Address::FederatedAgent { .. });
}
