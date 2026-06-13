//! Connection lifecycle: mTLS handshake, the heartbeat/select loop, and
//! exponential-backoff reconnect for one outbound peer link.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tonic::Request;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tracing::debug;

use crate::shared::peer_proto::peer_client::PeerClient as TonicPeerClient;
use crate::shared::peer_proto::{Hello, PeerOutbound, peer_inbound, peer_outbound};
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Peer, PeerState};

use super::super::peer_outbox::{InFlight, pump_one_row};
use super::super::peer_registry::PeerRegistry;
use super::super::persistence::unix_now;
use super::inbound::handle_inbound;
use super::outbox::{
    AgentLifecycleOutbox, MailOutbox, MemoryOutbox, ScrollDispatchOutbox, WorkspaceEventOutbox,
};

/// Outbound gRPC connect timeout; on elapse the task falls back to backoff.
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct PeerClientHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<()>,
}

impl PeerClientHandle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join.await;
    }
}

pub fn spawn(
    registry: Arc<PeerRegistry>,
    peer: Peer,
    notify_outbox: Arc<Notify>,
) -> PeerClientHandle {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let mut backoff: u64 = 1;
        loop {
            // Re-load each iteration to observe a `Removing` transition.
            let Ok(Some(cur)) = registry.db.get_peer(&peer.id) else {
                return;
            };
            if cur.state == PeerState::Removing {
                return;
            }

            match run_once(&registry, &cur, &notify_outbox, &mut shutdown_rx).await {
                Ok(()) => {
                    // Clean shutdown; exit the reconnect loop.
                    return;
                }
                Err(e) => {
                    debug!(peer = %peer.name, error = %e, "peer client iteration ended");
                    let _ = registry.db.set_peer_state(&peer.id, PeerState::Down);
                    registry.bus.publish(StreamEvent::PeerStreamDisconnected {
                        peer_id: peer.id.clone(),
                        reason: e.to_string(),
                    });
                }
            }

            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(backoff)) => {}
                _ = &mut shutdown_rx => return,
            }
            backoff = (backoff * 2).min(60);
        }
    });
    PeerClientHandle {
        shutdown_tx: Some(shutdown_tx),
        join,
    }
}

async fn run_once(
    registry: &Arc<PeerRegistry>,
    peer: &Peer,
    notify_outbox: &Arc<Notify>,
    shutdown_rx: &mut oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    // mTLS: present our client cert, pin the peer's cert as the sole trust
    // anchor. `domain_name` matches the constant SAN in every identity cert.
    let Some(peer_cert) = peer.pinned_cert_pem() else {
        return Err(anyhow::anyhow!("peer_missing_pinned_cert"));
    };
    let tls = ClientTlsConfig::new()
        .identity(registry.tls_identity.to_tonic())
        .ca_certificate(Certificate::from_pem(peer_cert.as_bytes()))
        .domain_name(crate::shared::tls::TLS_SAN);
    let endpoint = Endpoint::from_shared(peer.url.clone())?
        .tls_config(tls)?
        .connect_timeout(PEER_CONNECT_TIMEOUT);
    let channel = endpoint.connect().await?;
    let mut client = TonicPeerClient::new(channel);

    let (out_tx, out_rx) = mpsc::channel::<PeerOutbound>(64);
    let outbound_stream = tokio_stream::wrappers::ReceiverStream::new(out_rx);

    let hello = PeerOutbound {
        msg: Some(peer_outbound::Msg::Hello(Hello {
            daemon_id: registry.daemon_id.clone(),
            protocol_version: crate::shared::constants::PEER_PROTOCOL_VERSION,
            bearer_token: peer.bearer_token.clone(),
            advertised_topics: vec![],
        })),
    };
    out_tx
        .send(hello)
        .await
        .map_err(|_| anyhow::anyhow!("failed to send Hello"))?;

    let mut inbound = client
        .channel(Request::new(outbound_stream))
        .await?
        .into_inner();

    let Some(ack_msg) = inbound.message().await? else {
        return Err(anyhow::anyhow!("stream closed before HelloAck"));
    };
    let accepted = match ack_msg.msg.as_ref() {
        Some(peer_inbound::Msg::HelloAck(a)) => a.clone(),
        _ => return Err(anyhow::anyhow!("expected HelloAck")),
    };
    if !accepted.accepted {
        registry.bus.publish(StreamEvent::PeerHandshakeFailed {
            peer_name: Some(peer.name.clone()),
            reason: accepted.reject_reason.clone(),
        });
        return Err(anyhow::anyhow!(
            "hello_rejected: {}",
            accepted.reject_reason
        ));
    }
    registry.db.set_peer_state(&peer.id, PeerState::Active)?;
    registry.db.set_peer_last_seen(&peer.id, unix_now())?;
    if peer.daemon_id.is_empty() {
        registry
            .db
            .update_peer_daemon_id(&peer.id, &accepted.daemon_id)?;
    }
    registry.bus.publish(StreamEvent::PeerHandshakeOk {
        peer_id: peer.id.clone(),
        peer_daemon_id: accepted.daemon_id.clone(),
        peer_name: peer.name.clone(),
    });
    registry.bus.publish(StreamEvent::PeerStreamConnected {
        peer_id: peer.id.clone(),
    });

    // One in-flight slot per outbox table; helpers stash/clear as rows ship
    // and ack. ack_key is `mail_id` for mail, `op_id` for memory.
    let mut in_flight_mail: Option<InFlight> = None;
    let mut in_flight_memory: Option<InFlight> = None;
    let mut in_flight_workspace: Option<InFlight> = None;
    let mut in_flight_lifecycle: Option<InFlight> = None;
    let mut in_flight_dispatch: Option<InFlight> = None;

    let mail_backend = MailOutbox { db: &registry.db };
    let memory_backend = MemoryOutbox { db: &registry.db };
    let workspace_backend = WorkspaceEventOutbox { db: &registry.db };
    let lifecycle_backend = AgentLifecycleOutbox { db: &registry.db };
    let dispatch_backend = ScrollDispatchOutbox { db: &registry.db };

    // Initial pump in case rows were queued while disconnected.
    let removing = peer_removing(registry, &peer.id);
    pump_one_row(
        &mail_backend,
        &peer.id,
        removing,
        &out_tx,
        &mut in_flight_mail,
    )
    .await?;
    pump_one_row(
        &memory_backend,
        &peer.id,
        removing,
        &out_tx,
        &mut in_flight_memory,
    )
    .await?;
    pump_one_row(
        &workspace_backend,
        &peer.id,
        removing,
        &out_tx,
        &mut in_flight_workspace,
    )
    .await?;
    pump_one_row(
        &lifecycle_backend,
        &peer.id,
        removing,
        &out_tx,
        &mut in_flight_lifecycle,
    )
    .await?;
    pump_one_row(
        &dispatch_backend,
        &peer.id,
        removing,
        &out_tx,
        &mut in_flight_dispatch,
    )
    .await?;

    loop {
        tokio::select! {
            biased;
            _ = &mut *shutdown_rx => {
                let _ = out_tx.send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::Goodbye(
                        crate::shared::peer_proto::Goodbye { reason: "shutdown".into() },
                    )),
                }).await;
                return Ok(());
            }
            msg = inbound.message() => {
                let msg = match msg {
                    Ok(Some(m)) => m,
                    Ok(None) => return Err(anyhow::anyhow!("stream closed")),
                    Err(e) => return Err(e.into()),
                };
                handle_inbound(registry, peer, msg, &out_tx, &mail_backend, &memory_backend, &workspace_backend, &lifecycle_backend, &dispatch_backend, &mut in_flight_mail, &mut in_flight_memory, &mut in_flight_workspace, &mut in_flight_lifecycle, &mut in_flight_dispatch).await?;
                let _ = registry.db.set_peer_last_seen(&peer.id, unix_now());
                let removing = peer_removing(registry, &peer.id);
                pump_one_row(&mail_backend, &peer.id, removing, &out_tx, &mut in_flight_mail).await?;
                pump_one_row(&memory_backend, &peer.id, removing, &out_tx, &mut in_flight_memory).await?;
                pump_one_row(&workspace_backend, &peer.id, removing, &out_tx, &mut in_flight_workspace).await?;
                pump_one_row(&lifecycle_backend, &peer.id, removing, &out_tx, &mut in_flight_lifecycle).await?;
                pump_one_row(&dispatch_backend, &peer.id, removing, &out_tx, &mut in_flight_dispatch).await?;
            }
            () = notify_outbox.notified() => {
                let removing = peer_removing(registry, &peer.id);
                pump_one_row(&mail_backend, &peer.id, removing, &out_tx, &mut in_flight_mail).await?;
                pump_one_row(&memory_backend, &peer.id, removing, &out_tx, &mut in_flight_memory).await?;
                pump_one_row(&workspace_backend, &peer.id, removing, &out_tx, &mut in_flight_workspace).await?;
                pump_one_row(&lifecycle_backend, &peer.id, removing, &out_tx, &mut in_flight_lifecycle).await?;
                pump_one_row(&dispatch_backend, &peer.id, removing, &out_tx, &mut in_flight_dispatch).await?;
            }
        }
    }
}

/// True iff the peer is `Removing`, so the drainer halts rather than race
/// teardown. A missing/errored lookup counts as "not removing".
fn peer_removing(registry: &Arc<PeerRegistry>, peer_id: &str) -> bool {
    matches!(
        registry.db.get_peer(peer_id),
        Ok(Some(p)) if p.state == PeerState::Removing
    )
}
