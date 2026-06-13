//! Inbound peer gRPC service: reads `Hello`, validates token + version +
//! daemon-id, replies `HelloAck`, then relays traffic into the `InboxHandler`.

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
    /// Clone out the handles the service uses; the registry itself isn't
    /// retained (each handle owns its own `Arc`).
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
        let Ok(Some(mut peer)) = self.db.lookup_peer_by_token_hash(&token_hash) else {
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
        // Reject if the row's daemon_id disagrees with hello's.
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
            // Sync the in-session snapshot too: inbound handlers key dedupe,
            // shadow lookups, and republished events off `peer.daemon_id`.
            // Skipping this would make the first session after `peer add` drop
            // federated workspace events and misattribute lifecycle deliveries.
            peer.daemon_id = hello.daemon_id.clone();
        }
        let _ = self.db.set_peer_state(&peer.id, PeerState::Active);
        let _ = self.db.set_peer_last_seen(&peer.id, unix_now());
        self.bus.publish(StreamEvent::PeerHandshakeOk {
            peer_id: peer.id.clone(),
            peer_daemon_id: hello.daemon_id.clone(),
            peer_name: peer.name.clone(),
        });

        let (out_tx, out_rx) = mpsc::channel::<Result<PeerInbound, Status>>(64);
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
                    Some(peer_outbound::Msg::MemoryDeliver(d)) => {
                        // Namespace replication: apply via LWW (idempotent), ack.
                        let ack =
                            super::peer_client::apply_memory_deliver(&db, &peer_id_for_loop, &d);
                        let _ = out_tx
                            .send(Ok(PeerInbound {
                                msg: Some(peer_inbound::Msg::MemoryAck(ack)),
                            }))
                            .await;
                    }
                    Some(peer_outbound::Msg::WorkspaceEventDeliver(d)) => {
                        // Republish onto local shadow workspace.
                        let ack = super::peer_client::apply_workspace_event_deliver(
                            &db,
                            &bus,
                            &peer_for_loop,
                            &d,
                        );
                        let _ = out_tx
                            .send(Ok(PeerInbound {
                                msg: Some(peer_inbound::Msg::WorkspaceEventAck(ack)),
                            }))
                            .await;
                    }
                    Some(peer_outbound::Msg::AgentLifecycleDeliver(d)) => {
                        // Republish as local RemoteAgentStateChanged.
                        let ack = super::peer_client::apply_agent_lifecycle_deliver(
                            &db,
                            &bus,
                            &peer_for_loop,
                            &d,
                        );
                        let _ = out_tx
                            .send(Ok(PeerInbound {
                                msg: Some(peer_inbound::Msg::AgentLifecycleAck(ack)),
                            }))
                            .await;
                    }
                    Some(peer_outbound::Msg::ScrollTaskDispatch(d)) => {
                        let ack = super::peer_client::apply_scroll_task_dispatch(
                            &db,
                            &bus,
                            &peer_for_loop,
                            &d,
                        )
                        .await;
                        let _ = out_tx
                            .send(Ok(PeerInbound {
                                msg: Some(peer_inbound::Msg::ScrollTaskDispatchAck(ack)),
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

// Return type + `Result` wrapper are fixed by the generated tonic trait, so
// the large `Status` can't be boxed away.
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
