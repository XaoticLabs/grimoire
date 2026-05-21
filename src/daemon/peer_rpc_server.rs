//! Inbound peer gRPC service (Tasks 5, 7). Mirrors `WorkerControlService`:
//! reads `Hello`, validates token + version + daemon-id, replies with
//! `HelloAck`, then relays subsequent `MailDeliver` traffic into the
//! `InboxHandler`.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::shared::peer_proto::peer_server::{Peer as PeerService, PeerServer};
use crate::shared::peer_proto::{
    HelloAck, MailAck, PeerInbound, PeerOutbound, peer_inbound, peer_outbound,
};
use crate::shared::protocol::StreamEvent;
use crate::shared::types::PeerState;

use super::event_bus::EventBus;
use super::peer_inbox::InboxHandler;
use super::peer_registry::PeerRegistry;
use super::persistence::{Database, unix_now};

pub struct PeerSvc {
    db: Arc<Database>,
    bus: EventBus,
    daemon_id: String,
    inbox: Arc<InboxHandler>,
}

impl PeerSvc {
    /// Build a `PeerSvc` by cloning the handles the service actually uses
    /// out of the registry. The registry itself is not retained — each of
    /// `db`, `bus`, `daemon_id`, and `inbox` already owns an independent
    /// `Arc` to its underlying data, so dropping the registry reference
    /// after construction doesn't affect lifetimes.
    pub fn new(registry: Arc<PeerRegistry>) -> PeerServer<Self> {
        let svc = Self {
            db: registry.db.clone(),
            bus: registry.bus.clone(),
            daemon_id: registry.daemon_id.clone(),
            inbox: registry.inbox.clone(),
        };
        PeerServer::new(svc)
    }
}

type InboundStream = Pin<Box<dyn Stream<Item = Result<PeerInbound, Status>> + Send>>;

#[tonic::async_trait]
impl PeerService for PeerSvc {
    type ChannelStream = InboundStream;

    async fn channel(
        &self,
        req: Request<Streaming<PeerOutbound>>,
    ) -> Result<Response<Self::ChannelStream>, Status> {
        let mut inbound = req.into_inner();

        // First message must be Hello.
        let Some(first) = inbound.message().await? else {
            return Err(Status::invalid_argument("stream closed before Hello"));
        };
        let Some(peer_outbound::Msg::Hello(hello)) = first.msg else {
            return Err(Status::invalid_argument("first message must be Hello"));
        };

        // --- Handshake validation ---
        let token_hash = blake3::hash(hello.bearer_token.as_bytes())
            .as_bytes()
            .to_vec();
        let Ok(Some(peer)) = self.db.lookup_peer_by_token_hash(&token_hash) else {
            return single_helloack_stream(self.daemon_id.clone(), false, "invalid_token");
        };
        if hello.protocol_version != crate::shared::constants::PEER_PROTOCOL_VERSION {
            self.bus.publish(StreamEvent::PeerHandshakeFailed {
                peer_name: Some(peer.name.clone()),
                reason: "unsupported_protocol_version".to_string(),
            });
            return single_helloack_stream(
                self.daemon_id.clone(),
                false,
                "unsupported_protocol_version",
            );
        }
        // Daemon-id check: if the row already has a daemon_id and it
        // disagrees with hello.daemon_id, reject.
        if !peer.daemon_id.is_empty() && peer.daemon_id != hello.daemon_id {
            self.bus.publish(StreamEvent::PeerHandshakeFailed {
                peer_name: Some(peer.name.clone()),
                reason: "peer_daemon_id_collision".to_string(),
            });
            return single_helloack_stream(
                self.daemon_id.clone(),
                false,
                "peer_daemon_id_collision",
            );
        }
        if peer.daemon_id.is_empty() {
            let _ = self.db.update_peer_daemon_id(&peer.id, &hello.daemon_id);
        }
        let _ = self.db.set_peer_state(&peer.id, PeerState::Active);
        let _ = self.db.set_peer_last_seen(&peer.id, unix_now());
        self.bus.publish(StreamEvent::PeerHandshakeOk {
            peer_id: peer.id.clone(),
            peer_daemon_id: hello.daemon_id.clone(),
            peer_name: peer.name.clone(),
        });

        // Set up the inbound→client → outbound→server pipe.
        let (out_tx, out_rx) = mpsc::channel::<Result<PeerInbound, Status>>(64);
        // Send HelloAck first.
        let _ = out_tx
            .send(Ok(PeerInbound {
                msg: Some(peer_inbound::Msg::HelloAck(HelloAck {
                    daemon_id: self.daemon_id.clone(),
                    protocol_version: crate::shared::constants::PEER_PROTOCOL_VERSION,
                    accepted: true,
                    reject_reason: String::new(),
                })),
            }))
            .await;

        let inbox = self.inbox.clone();
        let bus = self.bus.clone();
        let peer_for_loop = peer.clone();
        let peer_id_for_loop = peer.id.clone();
        let db = self.db.clone();
        tokio::spawn(async move {
            while let Some(msg) = inbound.next().await {
                let Ok(msg) = msg else { break };
                let _ = db.set_peer_last_seen(&peer_id_for_loop, unix_now());
                match msg.msg {
                    Some(peer_outbound::Msg::Heartbeat(h)) => {
                        let _ = out_tx
                            .send(Ok(PeerInbound {
                                msg: Some(peer_inbound::Msg::HeartbeatAck(
                                    crate::shared::peer_proto::HeartbeatAck { nonce: h.nonce },
                                )),
                            }))
                            .await;
                    }
                    Some(peer_outbound::Msg::MailDeliver(d)) => {
                        let ack: MailAck = match inbox.handle_mail_deliver(&peer_for_loop, &d).await
                        {
                            Ok(a) => a,
                            Err(e) => MailAck {
                                mail_id: d.mail_id.clone(),
                                ok: false,
                                reason: format!("inbox_error: {e}"),
                            },
                        };
                        let _ = out_tx
                            .send(Ok(PeerInbound {
                                msg: Some(peer_inbound::Msg::MailAck(ack)),
                            }))
                            .await;
                    }
                    Some(peer_outbound::Msg::Goodbye(_)) => break,
                    _ => {} // ignore Hello/HelloAck/etc
                }
            }
            bus.publish(StreamEvent::PeerStreamDisconnected {
                peer_id: peer_for_loop.id.clone(),
                reason: "stream_closed".to_string(),
            });
            let _ = db.set_peer_state(&peer_for_loop.id, PeerState::Down);
        });

        let stream: InboundStream = Box::pin(tokio_stream::wrappers::ReceiverStream::new(out_rx));
        Ok(Response::new(stream))
    }
}

// `tonic::Status` is large (≈176B), but the streaming-RPC return type is fixed by
// the generated tonic trait — we can't box it without changing the public API.
// The `Result` wrapper is likewise dictated by the tonic trait signature.
#[allow(clippy::result_large_err, clippy::unnecessary_wraps)]
fn single_helloack_stream(
    daemon_id: String,
    accepted: bool,
    reason: &str,
) -> Result<Response<InboundStream>, Status> {
    let ack = HelloAck {
        daemon_id,
        protocol_version: crate::shared::constants::PEER_PROTOCOL_VERSION,
        accepted,
        reject_reason: reason.to_string(),
    };
    let stream = futures::stream::once(async move {
        Ok(PeerInbound {
            msg: Some(peer_inbound::Msg::HelloAck(ack)),
        })
    });
    Ok(Response::new(Box::pin(stream)))
}
