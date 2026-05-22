//! Outbound peer client (Tasks 6, 7). One task per peer link:
//! connects, performs the `Hello`/`HelloAck` handshake, runs the
//! heartbeat + select loop, and reconnects with exponential backoff
//! on disconnect.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tonic::Request;
use tonic::transport::Endpoint;
use tracing::debug;

use crate::shared::peer_proto::peer_client::PeerClient as TonicPeerClient;
use crate::shared::peer_proto::{
    Hello, MailAck, MailDeliver, PeerInbound, PeerOutbound, peer_inbound, peer_outbound,
};
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Peer, PeerState};

use super::peer_outbox::backoff_secs;
use super::peer_registry::PeerRegistry;
use super::persistence::unix_now;

/// Connection timeout for outbound gRPC to a federation peer. After this
/// elapses the client task falls back to its exponential backoff.
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
            // Re-load peer state from DB each iteration.
            let Ok(Some(cur)) = registry.db.get_peer(&peer.id) else {
                return;
            };
            if cur.state == PeerState::Removing {
                return;
            }

            match run_once(&registry, &cur, &notify_outbox, &mut shutdown_rx).await {
                Ok(()) => {
                    // Clean shutdown — `backoff` is no longer relevant since
                    // we're exiting the reconnect loop entirely.
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
    if peer.url.starts_with("https://") {
        return Err(anyhow::anyhow!("peer_tls_not_supported_yet"));
    }
    let endpoint = Endpoint::from_shared(peer.url.clone())?.connect_timeout(PEER_CONNECT_TIMEOUT);
    let channel = endpoint.connect().await?;
    let mut client = TonicPeerClient::new(channel);

    let (out_tx, out_rx) = mpsc::channel::<PeerOutbound>(64);
    let outbound_stream = tokio_stream::wrappers::ReceiverStream::new(out_rx);

    // Hello.
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

    // Wait for HelloAck.
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

    // Track currently-in-flight mail to match against MailAcks. The u32 is the
    // outbox row's prior-failure count, carried so a failed ack can grow the
    // retry backoff off the real attempt number.
    let mut in_flight_outbox_id: Option<(String, u32)> = None;
    let mut in_flight_mail_id: Option<String> = None;

    // Initial pump in case rows were queued while disconnected.
    pump_one(
        registry,
        peer,
        &out_tx,
        &mut in_flight_outbox_id,
        &mut in_flight_mail_id,
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
                handle_inbound(registry, peer, msg, &out_tx, &mut in_flight_outbox_id, &mut in_flight_mail_id).await?;
                let _ = registry.db.set_peer_last_seen(&peer.id, unix_now());
                if in_flight_outbox_id.is_none() {
                    pump_one(registry, peer, &out_tx, &mut in_flight_outbox_id, &mut in_flight_mail_id).await?;
                }
            }
            () = notify_outbox.notified() => {
                if in_flight_outbox_id.is_none() {
                    pump_one(registry, peer, &out_tx, &mut in_flight_outbox_id, &mut in_flight_mail_id).await?;
                }
            }
        }
    }
}

async fn handle_inbound(
    registry: &Arc<PeerRegistry>,
    peer: &Peer,
    msg: PeerInbound,
    out_tx: &mpsc::Sender<PeerOutbound>,
    in_flight_outbox_id: &mut Option<(String, u32)>,
    in_flight_mail_id: &mut Option<String>,
) -> anyhow::Result<()> {
    // Each arm is enumerated explicitly so dispatch for a new variant is a
    // compile error rather than silently routed to a wildcard.
    #[allow(clippy::match_same_arms)]
    match msg.msg {
        Some(peer_inbound::Msg::HelloAck(_)) => Ok(()),
        Some(peer_inbound::Msg::Heartbeat(h)) => {
            let _ = out_tx
                .send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::HeartbeatAck(
                        crate::shared::peer_proto::HeartbeatAck { nonce: h.nonce },
                    )),
                })
                .await;
            Ok(())
        }
        Some(peer_inbound::Msg::HeartbeatAck(_)) => Ok(()),
        Some(peer_inbound::Msg::MailDeliver(d)) => {
            // Server pushed mail at us — route through inbox handler.
            let ack = registry.inbox.handle_mail_deliver(peer, &d).await?;
            let _ = out_tx
                .send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::MailAck(ack)),
                })
                .await;
            Ok(())
        }
        Some(peer_inbound::Msg::MailAck(ack)) => {
            if Some(&ack.mail_id) == in_flight_mail_id.as_ref() {
                if let Some((id, attempts)) = in_flight_outbox_id.take() {
                    handle_outbox_ack(registry, peer, &id, attempts, &ack);
                }
                *in_flight_mail_id = None;
            }
            Ok(())
        }
        Some(peer_inbound::Msg::TopicSubscribe(_) | peer_inbound::Msg::TopicUnsubscribe(_)) => {
            Ok(())
        }
        Some(peer_inbound::Msg::Goodbye(_)) => Err(anyhow::anyhow!("peer goodbye")),
        None => Ok(()),
    }
}

fn handle_outbox_ack(
    registry: &Arc<PeerRegistry>,
    peer: &Peer,
    outbox_id: &str,
    attempts: u32,
    ack: &MailAck,
) {
    let now = unix_now();
    if ack.ok {
        let _ = registry.db.mark_outbox_delivered(outbox_id);
        registry.bus.publish(StreamEvent::PeerMailForwarded {
            peer_id: peer.id.clone(),
            mail_id: ack.mail_id.clone(),
            sender_seq: 0,
        });
    } else {
        // `attempts` is the row's prior-failure count, so this delivery was
        // attempt `attempts + 1`. Grow the backoff off that real number, the
        // same way the local-send-failure path in `pump_one` does, so a
        // persistently-unreachable peer is retried progressively less often.
        let backoff = backoff_secs(attempts + 1);
        let _ = registry
            .db
            .mark_outbox_failed_retry(outbox_id, now + backoff as i64);
        registry.bus.publish(StreamEvent::PeerMailForwardFailed {
            peer_id: peer.id.clone(),
            mail_id: ack.mail_id.clone(),
            reason: ack.reason.clone(),
        });
    }
}

async fn pump_one(
    registry: &Arc<PeerRegistry>,
    peer: &Peer,
    out_tx: &mpsc::Sender<PeerOutbound>,
    in_flight_outbox_id: &mut Option<(String, u32)>,
    in_flight_mail_id: &mut Option<String>,
) -> anyhow::Result<()> {
    if in_flight_outbox_id.is_some() {
        return Ok(());
    }
    if let Ok(Some(p)) = registry.db.get_peer(&peer.id)
        && p.state == PeerState::Removing
    {
        return Ok(());
    }
    let now = unix_now();
    let Some(row) = registry.db.next_outbox_row(&peer.id, now)? else {
        return Ok(());
    };
    registry.db.mark_outbox_in_flight(&row.id)?;
    let deliver = MailDeliver {
        mail_id: row.mail_id.clone(),
        sender: row.sender.clone().unwrap_or_default(),
        recipient: row.recipient.clone(),
        body: row.body.clone(),
        topic: row.topic.clone(),
        sender_seq: row.sender_seq,
    };
    if let Err(e) = out_tx
        .send(PeerOutbound {
            msg: Some(peer_outbound::Msg::MailDeliver(deliver)),
        })
        .await
    {
        let backoff = backoff_secs(row.attempts + 1);
        let _ = registry
            .db
            .mark_outbox_failed_retry(&row.id, now + backoff as i64);
        return Err(anyhow::anyhow!("send: {e}"));
    }
    *in_flight_outbox_id = Some((row.id, row.attempts));
    *in_flight_mail_id = Some(row.mail_id);
    Ok(())
}
