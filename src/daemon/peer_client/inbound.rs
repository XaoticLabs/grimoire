//! Inbound message dispatch for the outbound link's reverse stream:
//! routes each `PeerInbound` variant to its handler and reconciles acks
//! against the in-flight outbox slots.

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::shared::peer_proto::{MailAck, PeerInbound, PeerOutbound, peer_inbound, peer_outbound};
use crate::shared::protocol::StreamEvent;
use crate::shared::types::Peer;

use super::super::peer_outbox::{InFlight, handle_ack_outcome};
use super::super::peer_registry::PeerRegistry;
use super::appliers::{
    apply_agent_lifecycle_deliver, apply_memory_deliver, apply_scroll_task_dispatch,
    apply_workspace_event_deliver,
};
use super::outbox::{
    AgentLifecycleOutbox, MailOutbox, MemoryOutbox, ScrollDispatchOutbox, WorkspaceEventOutbox,
};

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_inbound(
    registry: &Arc<PeerRegistry>,
    peer: &Peer,
    msg: PeerInbound,
    out_tx: &mpsc::Sender<PeerOutbound>,
    mail_backend: &MailOutbox<'_>,
    memory_backend: &MemoryOutbox<'_>,
    workspace_backend: &WorkspaceEventOutbox<'_>,
    lifecycle_backend: &AgentLifecycleOutbox<'_>,
    dispatch_backend: &ScrollDispatchOutbox<'_>,
    in_flight_mail: &mut Option<InFlight>,
    in_flight_memory: &mut Option<InFlight>,
    in_flight_workspace: &mut Option<InFlight>,
    in_flight_lifecycle: &mut Option<InFlight>,
    in_flight_dispatch: &mut Option<InFlight>,
) -> anyhow::Result<()> {
    // Arms enumerated explicitly so a new variant is a compile error, not a
    // silent wildcard route.
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
            let ack = registry.inbox.handle_mail_deliver(peer, &d).await?;
            let _ = out_tx
                .send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::MailAck(ack)),
                })
                .await;
            Ok(())
        }
        Some(peer_inbound::Msg::MailAck(ack)) => {
            // Match ack-key before clearing: a delayed ack must not resolve the
            // wrong follow-up row.
            if matches!(in_flight_mail.as_ref(), Some(f) if f.ack_key == ack.mail_id) {
                let slot = in_flight_mail.take().expect("just matched");
                handle_ack_outcome(mail_backend, &slot, ack.ok);
                emit_mail_ack_event(registry, peer, &ack);
            }
            Ok(())
        }
        Some(peer_inbound::Msg::TopicSubscribe(_) | peer_inbound::Msg::TopicUnsubscribe(_)) => {
            Ok(())
        }
        Some(peer_inbound::Msg::MemoryDeliver(d)) => {
            let ack = apply_memory_deliver(&registry.db, &peer.id, &d);
            let _ = out_tx
                .send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::MemoryAck(ack)),
                })
                .await;
            Ok(())
        }
        Some(peer_inbound::Msg::MemoryAck(ack)) => {
            if matches!(in_flight_memory.as_ref(), Some(f) if f.ack_key == ack.op_id) {
                let slot = in_flight_memory.take().expect("just matched");
                handle_ack_outcome(memory_backend, &slot, ack.ok);
                if !ack.ok {
                    tracing::warn!(peer = %peer.name, op = %ack.op_id, reason = %ack.reason,
                        "namespace replication rejected");
                }
            }
            // Unknown op: drop; the tracked row's real ack will arrive later.
            Ok(())
        }
        Some(peer_inbound::Msg::WorkspaceEventDeliver(d)) => {
            let ack = apply_workspace_event_deliver(&registry.db, &registry.bus, peer, &d);
            let _ = out_tx
                .send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::WorkspaceEventAck(ack)),
                })
                .await;
            Ok(())
        }
        Some(peer_inbound::Msg::WorkspaceEventAck(ack)) => {
            let key = ack.sender_seq.to_string();
            if matches!(in_flight_workspace.as_ref(), Some(f) if f.ack_key == key) {
                let slot = in_flight_workspace.take().expect("just matched");
                handle_ack_outcome(workspace_backend, &slot, ack.ok);
                if !ack.ok {
                    tracing::warn!(peer = %peer.name, seq = ack.sender_seq, reason = %ack.reason,
                        "workspace event delivery rejected");
                }
            }
            Ok(())
        }
        Some(peer_inbound::Msg::AgentLifecycleDeliver(d)) => {
            let ack = apply_agent_lifecycle_deliver(&registry.db, &registry.bus, peer, &d);
            let _ = out_tx
                .send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::AgentLifecycleAck(ack)),
                })
                .await;
            Ok(())
        }
        Some(peer_inbound::Msg::AgentLifecycleAck(ack)) => {
            let key = ack.sender_seq.to_string();
            if matches!(in_flight_lifecycle.as_ref(), Some(f) if f.ack_key == key) {
                let slot = in_flight_lifecycle.take().expect("just matched");
                handle_ack_outcome(lifecycle_backend, &slot, ack.ok);
                if !ack.ok {
                    tracing::warn!(peer = %peer.name, seq = ack.sender_seq, reason = %ack.reason,
                        "agent lifecycle delivery rejected");
                }
            }
            Ok(())
        }
        Some(peer_inbound::Msg::ScrollTaskDispatch(d)) => {
            let ack = apply_scroll_task_dispatch(&registry.db, &registry.bus, peer, &d).await;
            let _ = out_tx
                .send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::ScrollTaskDispatchAck(ack)),
                })
                .await;
            Ok(())
        }
        Some(peer_inbound::Msg::ScrollTaskDispatchAck(ack)) => {
            let key = ack.sender_seq.to_string();
            if matches!(in_flight_dispatch.as_ref(), Some(f) if f.ack_key == key) {
                let slot = in_flight_dispatch.take().expect("just matched");
                if ack.ok
                    && !ack.local_agent_id.is_empty()
                    && !ack.scroll_id.is_empty()
                    && !ack.task_id.is_empty()
                {
                    let _ = registry.db.scroll_dispatch_set_remote_agent(
                        &ack.scroll_id,
                        &ack.task_id,
                        &peer.id,
                        &ack.local_agent_id,
                    );
                }
                handle_ack_outcome(dispatch_backend, &slot, ack.ok);
                if !ack.ok {
                    tracing::warn!(peer = %peer.name, seq = ack.sender_seq, reason = %ack.reason,
                        "scroll task dispatch rejected");
                }
            }
            Ok(())
        }
        Some(peer_inbound::Msg::Goodbye(_)) => Err(anyhow::anyhow!("peer goodbye")),
        None => Ok(()),
    }
}

/// Mail forwarding success/rejection events. Mail-only (the memory LWW path is
/// silent by design), so it's not on the trait.
fn emit_mail_ack_event(registry: &Arc<PeerRegistry>, peer: &Peer, ack: &MailAck) {
    if ack.ok {
        registry.bus.publish(StreamEvent::PeerMailForwarded {
            peer_id: peer.id.clone(),
            mail_id: ack.mail_id.clone(),
            sender_seq: 0,
        });
    } else {
        registry.bus.publish(StreamEvent::PeerMailForwardFailed {
            peer_id: peer.id.clone(),
            mail_id: ack.mail_id.clone(),
            reason: ack.reason.clone(),
        });
    }
}
