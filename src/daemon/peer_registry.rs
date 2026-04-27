//! Federation peer registry (Tasks 6, 11). Owns the per-peer outbound
//! client task + outbox drainer + inbox handler dispatch.
//!
//! State machine:
//!   `Pending` (handshake not yet completed) →
//!   `Active` (stream up) ↔ `Down` (stream lost, retry pending) →
//!   `Removing` (cascade in progress) → row deleted.

use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;

use crate::shared::constants::generate_short_id;
use crate::shared::protocol::{PeerSummary, StreamEvent};
use crate::shared::types::{DaemonId, Peer, PeerState};

use super::clock::Clock;
use super::event_bus::EventBus;
use super::peer_client::PeerClientHandle;
use super::peer_inbox::InboxHandler;
use super::persistence::{Database, unix_now};

pub struct PeerHandle {
    pub state: PeerState,
    pub notify_outbox: Arc<Notify>,
    pub client: Option<PeerClientHandle>,
}

pub struct PeerRegistry {
    pub db: Arc<Database>,
    pub bus: EventBus,
    pub clock: Arc<dyn Clock>,
    pub daemon_id: DaemonId,
    pub handles: Mutex<HashMap<String, PeerHandle>>,
    pub inbox: Arc<InboxHandler>,
}

impl PeerRegistry {
    pub fn new(
        db: Arc<Database>,
        bus: EventBus,
        clock: Arc<dyn Clock>,
        daemon_id: DaemonId,
    ) -> Arc<Self> {
        let inbox = Arc::new(InboxHandler::new(db.clone(), bus.clone(), daemon_id.clone()));
        Arc::new(Self {
            db,
            bus,
            clock,
            daemon_id,
            handles: Mutex::new(HashMap::new()),
            inbox,
        })
    }

    /// On boot: reset any `in_flight` outbox rows back to `pending`, and
    /// flip `Active` peer rows to `Down` until the client task reconnects.
    pub async fn reconcile_on_boot(&self) -> Result<()> {
        let _ = self.db.reset_outbox_in_flight()?;
        for p in self.db.list_peers()? {
            if p.state == PeerState::Active {
                self.db.set_peer_state(&p.id, PeerState::Down)?;
            }
        }
        Ok(())
    }

    /// Spawn outbound client tasks for all non-Removing peers on boot.
    pub async fn spawn_all_active(self: &Arc<Self>) {
        let peers = match self.db.list_peers() {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, "list_peers failed during spawn_all_active");
                return;
            }
        };
        for peer in peers {
            if peer.state == PeerState::Removing {
                continue;
            }
            if let Err(e) = self.ensure_connected(&peer).await {
                tracing::warn!(peer = %peer.name, error = %e, "ensure_connected failed");
            }
        }
    }

    /// Ensure a per-peer outbound client task is running; idempotent.
    pub async fn ensure_connected(self: &Arc<Self>, peer: &Peer) -> Result<()> {
        let mut guard = self.handles.lock().await;
        if guard.contains_key(&peer.id) {
            return Ok(());
        }
        let notify = Arc::new(Notify::new());
        let client = super::peer_client::spawn(
            self.clone(),
            peer.clone(),
            notify.clone(),
        );
        guard.insert(
            peer.id.clone(),
            PeerHandle {
                state: peer.state.clone(),
                notify_outbox: notify,
                client: Some(client),
            },
        );
        Ok(())
    }

    /// Look up a peer by daemon-id (for outbound mail routing).
    pub async fn peer_for_daemon_id(&self, daemon_id: &str) -> Result<Option<Peer>> {
        self.db.get_peer_by_daemon_id(daemon_id)
    }

    /// Notify the per-peer drainer that a new outbox row is queued.
    pub async fn notify_outbox(&self, peer_id: &str) {
        let guard = self.handles.lock().await;
        if let Some(h) = guard.get(peer_id) {
            h.notify_outbox.notify_one();
        }
    }

    /// Register a new peer: insert row (Pending), spawn the client task,
    /// wait up to `timeout_secs` for handshake to flip state to Active.
    pub async fn register_peer(
        self: &Arc<Self>,
        name: String,
        url: String,
        bearer_token: String,
        timeout_secs: u64,
    ) -> Result<Peer> {
        // Validate name + token shapes.
        if !is_valid_peer_name(&name) {
            return Err(anyhow!("invalid_peer_name"));
        }
        if !is_valid_bearer_token(&bearer_token) {
            return Err(anyhow!("invalid_bearer_token"));
        }
        if url.starts_with("https://") {
            return Err(anyhow!("peer_tls_not_supported_yet"));
        }
        if !url.starts_with("http://") {
            return Err(anyhow!("invalid_peer_url"));
        }
        if self.db.get_peer_by_name(&name)?.is_some() {
            return Err(anyhow!("peer_name_exists"));
        }

        let token_hash = blake3::hash(bearer_token.as_bytes()).as_bytes().to_vec();
        let peer = Peer {
            id: generate_short_id(),
            daemon_id: String::new(),
            name: name.clone(),
            url: url.clone(),
            bearer_token_hash: token_hash,
            bearer_token: bearer_token.clone(),
            public_key: None,
            state: PeerState::Pending,
            last_seen: None,
            registered_at: unix_now(),
        };
        self.db.insert_peer(&peer)?;
        self.ensure_connected(&peer).await?;

        // Wait for handshake to complete (Active) or timeout.
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            match self.db.get_peer_by_name(&name)? {
                Some(p) if p.state == PeerState::Active => return Ok(p),
                Some(p) if p.state == PeerState::Down => {
                    // Handshake explicitly rejected — surface to caller and
                    // tear down the row so a retry can choose a new token.
                    let _ = self.db.delete_peer(&p.id);
                    self.handles.lock().await.remove(&p.id);
                    return Err(anyhow!("peer_handshake_failed"));
                }
                _ => {}
            }
            if std::time::Instant::now() >= deadline {
                // Timeout: roll back the row so operator can `peer add` cleanly.
                if let Some(p) = self.db.get_peer_by_name(&name)? {
                    let _ = self.db.delete_peer(&p.id);
                    self.handles.lock().await.remove(&p.id);
                }
                return Err(anyhow!("peer_handshake_timeout"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    pub async fn list_with_outbox_depth(&self) -> Result<Vec<PeerSummary>> {
        let peers = self.db.list_peers()?;
        let mut out = Vec::with_capacity(peers.len());
        for p in peers {
            let depth = self.db.outbox_depth(&p.id).unwrap_or(0);
            out.push(PeerSummary {
                peer_id: p.id,
                name: p.name,
                daemon_id: p.daemon_id,
                url: p.url,
                state: p.state.as_str().to_string(),
                last_seen: p.last_seen,
                outbox_depth: depth,
            });
        }
        Ok(out)
    }

    pub async fn remove_peer(&self, name: &str) -> Result<bool> {
        let peer = match self.db.get_peer_by_name(name)? {
            Some(p) => p,
            None => return Ok(false),
        };
        // Mark Removing so any in-flight drainer halts cleanly.
        self.db.set_peer_state(&peer.id, PeerState::Removing)?;
        // Tear down client task.
        if let Some(handle) = self.handles.lock().await.remove(&peer.id) {
            if let Some(client) = handle.client {
                client.shutdown().await;
            }
        }
        // Cascade-delete via FK; mail rows are retained by design.
        self.db.delete_peer(&peer.id)?;
        Ok(true)
    }

    pub async fn ping_peer(&self, name: &str) -> Result<(u64, String)> {
        let peer = match self.db.get_peer_by_name(name)? {
            Some(p) => p,
            None => return Err(anyhow!("peer_not_found")),
        };
        // Minimal ping: report current state + zero RTT for now (full
        // request/response over the channel is layered on Heartbeat / HeartbeatAck
        // — for v1, returning state suffices).
        Ok((0, peer.state.as_str().to_string()))
    }
}

pub fn is_valid_peer_name(s: &str) -> bool {
    if s.is_empty() || s.len() > 64 {
        return false;
    }
    let bytes = s.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return false;
    }
    bytes.iter().all(|&b| {
        b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-'
    })
}

pub fn is_valid_bearer_token(s: &str) -> bool {
    if s.len() < 16 || s.len() > 256 {
        return false;
    }
    s.bytes().all(|b| (b'!'..=b'~').contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_name_shape() {
        assert!(is_valid_peer_name("foo"));
        assert!(is_valid_peer_name("foo-bar.baz_1"));
        assert!(!is_valid_peer_name(""));
        assert!(!is_valid_peer_name("-foo"));
        assert!(!is_valid_peer_name("a b"));
    }

    #[test]
    fn token_shape() {
        assert!(is_valid_bearer_token("0123456789abcdef!"));
        assert!(!is_valid_bearer_token("short"));
        assert!(!is_valid_bearer_token("with spaces                                "));
    }
}
