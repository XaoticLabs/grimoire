//! Outbound peer client (Tasks 6, 7). One task per peer link:
//! connects, performs the `Hello`/`HelloAck` handshake, runs the
//! heartbeat + select loop, and reconnects with exponential backoff
//! on disconnect.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tonic::Request;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};
use tracing::debug;

use crate::shared::peer_proto::peer_client::PeerClient as TonicPeerClient;
use crate::shared::peer_proto::{
    AgentLifecycleAck, AgentLifecycleDeliver, Hello, MailAck, MemoryAck, MemoryDeliver,
    PeerInbound, PeerOutbound, ScrollTaskDispatch, ScrollTaskDispatchAck, WorkspaceEventAck,
    WorkspaceEventDeliver, peer_inbound, peer_outbound,
};
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{AgentState, Peer, PeerState};

use super::event_bus::EventBus;
use super::peer_outbox::{InFlight, OutboxBackend, handle_ack_outcome, pump_one_row};
use super::peer_registry::PeerRegistry;
use super::persistence::{Database, unix_now};

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
                    // Clean shutdown. `backoff` is no longer relevant since
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
    // mTLS: present our identity as the client cert and pin the peer's cert
    // as the sole trust anchor for the server side. `domain_name` matches the
    // constant SAN minted into every grimoire identity cert.
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

    // One in-flight slot per outbox table. Mail's ack_key is the `mail_id`;
    // memory's is the `op_id`. The generic helpers stash + clear these as
    // each row ships and acks.
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

/// True iff the peer row is in `Removing`. Drainer halts in that case so
/// teardown isn't racing fresh sends. A missing/errored lookup is treated
/// as "not removing" (matching the pre-refactor behavior, where the
/// `if let Ok(Some(_)) && removing` guard simply fell through).
fn peer_removing(registry: &Arc<PeerRegistry>, peer_id: &str) -> bool {
    matches!(
        registry.db.get_peer(peer_id),
        Ok(Some(p)) if p.state == PeerState::Removing
    )
}

#[allow(clippy::too_many_arguments)]
async fn handle_inbound(
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
            // Server pushed mail at us; route through inbox handler.
            let ack = registry.inbox.handle_mail_deliver(peer, &d).await?;
            let _ = out_tx
                .send(PeerOutbound {
                    msg: Some(peer_outbound::Msg::MailAck(ack)),
                })
                .await;
            Ok(())
        }
        Some(peer_inbound::Msg::MailAck(ack)) => {
            // Ack-key match before clearing the slot: a delayed ack for an
            // already-resolved row must not resolve the wrong follow-up.
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
            // Server pushed a namespace write at us; apply via LWW and ack.
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
            // Ack for an unknown op; drop it. The sender will retry the
            // tracked row when the real ack arrives.
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

/// Mail-side observability: forwarding success / rejection events.
/// Memory has no equivalent (the LWW path is silent by design), so this
/// stays in the mail-only branch rather than on the trait.
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

/// Apply an inbound namespace write if the peer is authorized to replicate
/// into that namespace, then build the ack. Authorization failures ack with
/// `ok=false` so the sender stops retrying a namespace we don't accept.
/// Shared by the outbound client and the inbound server.
pub fn apply_memory_deliver(
    db: &crate::daemon::persistence::Database,
    peer_id: &str,
    d: &MemoryDeliver,
) -> MemoryAck {
    use crate::daemon::namespace_db::NamespaceWrite;
    match db.namespace_inbound_authorized(peer_id, &d.namespace) {
        Ok(true) => {}
        Ok(false) => {
            return MemoryAck {
                op_id: d.op_id.clone(),
                ok: false,
                reason: "namespace_not_federated_inbound".into(),
            };
        }
        Err(e) => {
            return MemoryAck {
                op_id: d.op_id.clone(),
                ok: false,
                reason: format!("authz_error: {e}"),
            };
        }
    }
    let write = NamespaceWrite {
        namespace: d.namespace.clone(),
        key: d.key.clone(),
        value: d.value.clone(),
        lamport: d.lamport,
        origin_daemon_id: d.origin_daemon_id.clone(),
        deleted: d.deleted,
        updated_by: d.updated_by.clone(),
    };
    match db.namespace_apply_write(&write) {
        Ok(_) => MemoryAck {
            op_id: d.op_id.clone(),
            ok: true,
            reason: String::new(),
        },
        Err(e) => MemoryAck {
            op_id: d.op_id.clone(),
            ok: false,
            reason: format!("apply_error: {e}"),
        },
    }
}

/// Wire shape of the `AgentLifecycleDeliver.payload_json` field.
/// Matches the snapshot the producer's bus-subscriber emits.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AgentLifecyclePayload {
    pub agent_id: String,
    pub old_state: AgentState,
    pub new_state: AgentState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

/// F4b: republish an inbound `AgentLifecycleDeliver` as a local
/// `RemoteAgentStateChanged` stream event. Shared between the outbound
/// client's reverse stream and the inbound server.
pub fn apply_agent_lifecycle_deliver(
    db: &Database,
    bus: &EventBus,
    peer: &Peer,
    d: &AgentLifecycleDeliver,
) -> AgentLifecycleAck {
    match db.agent_lifecycle_inbound_authorized(&peer.id) {
        Ok(true) => {}
        Ok(false) => {
            return AgentLifecycleAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: "lifecycle_not_federated_inbound".into(),
            };
        }
        Err(e) => {
            return AgentLifecycleAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: format!("authz_error: {e}"),
            };
        }
    }

    match db.agent_lifecycle_inbox_record(&peer.daemon_id, d.sender_seq) {
        Ok(true) => {}
        Ok(false) => {
            return AgentLifecycleAck {
                sender_seq: d.sender_seq,
                ok: true,
                reason: String::new(),
            };
        }
        Err(e) => {
            return AgentLifecycleAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: format!("inbox_error: {e}"),
            };
        }
    }

    let parsed: AgentLifecyclePayload = match serde_json::from_str(&d.payload_json) {
        Ok(p) => p,
        Err(e) => {
            return AgentLifecycleAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: format!("bad_payload: {e}"),
            };
        }
    };

    bus.publish(StreamEvent::RemoteAgentStateChanged {
        sender_daemon_id: peer.daemon_id.clone(),
        agent_id: parsed.agent_id,
        old_state: parsed.old_state,
        new_state: parsed.new_state,
        name: parsed.name,
        task: parsed.task,
        exit_code: parsed.exit_code,
    });

    AgentLifecycleAck {
        sender_seq: d.sender_seq,
        ok: true,
        reason: String::new(),
    }
}

/// Wire shape of the `WorkspaceEventDeliver.payload_json` field.
/// Matches what the watcher serializes in `fanout_to_federated_peers`.
#[derive(serde::Deserialize)]
struct WorkspaceEventPayload {
    #[serde(default)]
    paths: Vec<String>,
    #[serde(default)]
    kinds: Vec<String>,
    #[serde(default)]
    truncated: u32,
}

/// F3c: republish an inbound `WorkspaceEventDeliver` onto the local
/// shadow workspace. Shared between the outbound client's reverse
/// stream (peer_client) and the inbound server (peer_rpc_server).
///
/// Ack semantics:
/// - `ok: true` — applied OR a known terminal state (no shadow
///   configured, already-seen). The sender drops the row.
/// - `ok: false` — authz failure or payload error. The sender stops
///   retrying (the row exits via the same `mark_delivered` ack path,
///   intentionally — workspace events are time-sensitive, retrying
///   stale fs events forever is worse than dropping them).
pub fn apply_workspace_event_deliver(
    db: &Database,
    bus: &EventBus,
    peer: &Peer,
    d: &WorkspaceEventDeliver,
) -> WorkspaceEventAck {
    // Resolve the local shadow workspace. The sender ships its own
    // (home) workspace id; we look up which of our shadows points at
    // (peer.daemon_id, home_workspace_id).
    let shadow_id = match db.find_shadow_workspace(&peer.daemon_id, &d.workspace_id) {
        Ok(Some(id)) => id,
        Ok(None) => {
            // No shadow configured locally — drop with positive ack so
            // the sender doesn't retry forever.
            tracing::debug!(
                peer = %peer.name,
                home_workspace = %d.workspace_id,
                "no local shadow for inbound workspace event; dropping",
            );
            return WorkspaceEventAck {
                sender_seq: d.sender_seq,
                ok: true,
                reason: String::new(),
            };
        }
        Err(e) => {
            return WorkspaceEventAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: format!("shadow_lookup_error: {e}"),
            };
        }
    };

    match db.workspace_federation_inbound_authorized(&peer.id, &shadow_id) {
        Ok(true) => {}
        Ok(false) => {
            return WorkspaceEventAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: "workspace_not_federated_inbound".into(),
            };
        }
        Err(e) => {
            return WorkspaceEventAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: format!("authz_error: {e}"),
            };
        }
    }

    // Dedupe by (sender_daemon_id, sender_seq). Already-seen → drop
    // with positive ack; replay is the sender's normal retry path.
    match db.workspace_event_inbox_record(&peer.daemon_id, d.sender_seq, &shadow_id) {
        Ok(true) => {}
        Ok(false) => {
            return WorkspaceEventAck {
                sender_seq: d.sender_seq,
                ok: true,
                reason: String::new(),
            };
        }
        Err(e) => {
            return WorkspaceEventAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: format!("inbox_error: {e}"),
            };
        }
    }

    let parsed: WorkspaceEventPayload = match serde_json::from_str(&d.payload_json) {
        Ok(p) => p,
        Err(e) => {
            return WorkspaceEventAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: format!("bad_payload: {e}"),
            };
        }
    };

    super::workspace_watcher::publish_workspace_file_change(
        &shadow_id,
        db,
        bus,
        &parsed.paths,
        &parsed.kinds,
        parsed.truncated,
    );

    WorkspaceEventAck {
        sender_seq: d.sender_seq,
        ok: true,
        reason: String::new(),
    }
}

/// Mail outbox backend. Drives `peer_outbox` rows over the mail channel.
struct MailOutbox<'a> {
    db: &'a Database,
}

impl OutboxBackend for MailOutbox<'_> {
    type Row = crate::shared::types::PeerOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.next_outbox_row(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.mark_outbox_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.mark_outbox_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db.mark_outbox_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.mail_id.clone()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        PeerOutbound {
            msg: Some(peer_outbound::Msg::MailDeliver(
                super::peer_outbox::row_to_mail_deliver(row),
            )),
        }
    }
}

/// F3b: workspace-file-event federation backend. Drives
/// `workspace_event_outbox` rows over the workspace channel. The
/// payload is already JSON-serialized at enqueue time, so the backend
/// is just a passthrough.
struct WorkspaceEventOutbox<'a> {
    db: &'a Database,
}

impl OutboxBackend for WorkspaceEventOutbox<'_> {
    type Row = crate::daemon::workspace_db::WsEventOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.workspace_event_next_outbox(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.workspace_event_mark_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.workspace_event_mark_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db
            .workspace_event_mark_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.sender_seq.to_string()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        PeerOutbound {
            msg: Some(peer_outbound::Msg::WorkspaceEventDeliver(
                crate::shared::peer_proto::WorkspaceEventDeliver {
                    workspace_id: row.workspace_id.clone(),
                    sender_seq: row.sender_seq,
                    payload_json: String::from_utf8_lossy(&row.payload).into_owned(),
                },
            )),
        }
    }
}

/// F5a: receive a `ScrollTaskDispatch` from a coordinator peer and
/// queue a local agent for it.
///
/// Gates:
/// - Peer must have `accept_scroll_dispatch = 1` (opt-in).
/// - Inbox dedupe: replays return the previously-assigned
///   `local_agent_id` instead of spawning a duplicate.
///
/// The receiver does NOT acquire any scroll DB rows on its side —
/// scrolls are coordinator-owned. The dispatched agent is a plain
/// queued agent; it shows up in `grim ps` like anything else and is
/// surfaced to the coordinator only via F4b lifecycle federation.
pub async fn apply_scroll_task_dispatch(
    db: &Database,
    bus: &crate::daemon::event_bus::EventBus,
    peer: &Peer,
    d: &ScrollTaskDispatch,
) -> ScrollTaskDispatchAck {
    use crate::daemon::persistence::QueueRow;
    use crate::shared::types::{Agent, AgentState, RestartPolicy};
    use chrono::Utc;

    match db.peer_accept_scroll_dispatch(&peer.id) {
        Ok(true) => {}
        Ok(false) => {
            return ScrollTaskDispatchAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: "peer_not_accepting_scroll_dispatch".into(),
                local_agent_id: String::new(),
                scroll_id: d.scroll_id.clone(),
                task_id: d.task_id.clone(),
            };
        }
        Err(e) => {
            return ScrollTaskDispatchAck {
                sender_seq: d.sender_seq,
                ok: false,
                reason: format!("authz_error: {e}"),
                local_agent_id: String::new(),
                scroll_id: d.scroll_id.clone(),
                task_id: d.task_id.clone(),
            };
        }
    }

    if let Ok(Some(existing)) = db.scroll_dispatch_inbox_lookup(&peer.daemon_id, d.sender_seq) {
        return ScrollTaskDispatchAck {
            sender_seq: d.sender_seq,
            ok: true,
            reason: String::new(),
            local_agent_id: existing,
            scroll_id: d.scroll_id.clone(),
            task_id: d.task_id.clone(),
        };
    }

    let agent_id = crate::shared::constants::generate_short_id();
    let now = Utc::now();
    let cwd_str = if d.cwd.is_empty() { "." } else { &d.cwd };
    let cwd = std::path::PathBuf::from(cwd_str);
    let task_text = if d.prompt.is_empty() {
        d.task_name.clone()
    } else {
        d.prompt.clone()
    };
    let provider_opt = (!d.provider.is_empty()).then(|| d.provider.clone());
    let model_opt = (!d.model.is_empty()).then(|| d.model.clone());

    let agent = Agent {
        id: agent_id.clone(),
        name: Some(format!("dispatched:{}", d.task_name)),
        state: AgentState::Queued,
        task: Some(task_text.clone()),
        model: model_opt.clone(),
        provider: provider_opt.clone(),
        cwd: cwd.clone(),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: now,
        updated_at: now,
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    if let Err(e) = db.insert_agent(&agent) {
        return ScrollTaskDispatchAck {
            sender_seq: d.sender_seq,
            ok: false,
            reason: format!("insert_agent: {e}"),
            local_agent_id: String::new(),
            scroll_id: d.scroll_id.clone(),
            task_id: d.task_id.clone(),
        };
    }
    let queue = QueueRow {
        id: agent_id.clone(),
        lane: "default".to_string(),
        priority: 0,
        enqueued_at: now,
        provider_name: provider_opt,
        cwd: cwd.to_string_lossy().to_string(),
        model: model_opt,
        task_text,
        block_reason: None,
    };
    if let Err(e) = db.enqueue_task(&queue) {
        return ScrollTaskDispatchAck {
            sender_seq: d.sender_seq,
            ok: false,
            reason: format!("enqueue: {e}"),
            local_agent_id: String::new(),
            scroll_id: d.scroll_id.clone(),
            task_id: d.task_id.clone(),
        };
    }
    let _ = db.scroll_dispatch_inbox_record(&peer.daemon_id, d.sender_seq, &agent_id);

    bus.publish(crate::shared::protocol::StreamEvent::AgentCreated { agent });
    bus.publish(crate::shared::protocol::StreamEvent::AgentQueued {
        agent_id: agent_id.clone(),
        lane: "default".to_string(),
        block_reason: None,
    });

    ScrollTaskDispatchAck {
        sender_seq: d.sender_seq,
        ok: true,
        reason: String::new(),
        local_agent_id: agent_id,
        scroll_id: d.scroll_id.clone(),
        task_id: d.task_id.clone(),
    }
}

/// F5a: scroll-dispatch outbox backend.
struct ScrollDispatchOutbox<'a> {
    db: &'a Database,
}

impl OutboxBackend for ScrollDispatchOutbox<'_> {
    type Row = crate::daemon::persistence::scroll_dispatch::ScrollDispatchOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.scroll_dispatch_next_outbox(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.scroll_dispatch_mark_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.scroll_dispatch_mark_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db
            .scroll_dispatch_mark_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.sender_seq.to_string()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        // Payload was serialized at enqueue time as a JSON envelope
        // carrying the same fields the proto message holds. Decode
        // here so the over-the-wire proto stays the source of truth.
        let parsed: ScrollDispatchPayload =
            serde_json::from_slice(&row.payload).unwrap_or_default();
        PeerOutbound {
            msg: Some(peer_outbound::Msg::ScrollTaskDispatch(ScrollTaskDispatch {
                sender_seq: row.sender_seq,
                scroll_id: parsed.scroll_id,
                task_id: parsed.task_id,
                task_name: parsed.task_name,
                prompt: parsed.prompt,
                provider: parsed.provider,
                model: parsed.model,
                cwd: parsed.cwd,
                file_patterns: parsed.file_patterns,
            })),
        }
    }
}

/// Wire shape of the dispatch outbox payload (JSON in the BLOB column).
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct ScrollDispatchPayload {
    pub scroll_id: String,
    pub task_id: String,
    pub task_name: String,
    pub prompt: String,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub file_patterns: Vec<String>,
}

/// F4b: agent-lifecycle federation backend. Drives
/// `agent_lifecycle_outbox` rows over the lifecycle channel.
struct AgentLifecycleOutbox<'a> {
    db: &'a Database,
}

impl OutboxBackend for AgentLifecycleOutbox<'_> {
    type Row = crate::daemon::persistence::agent_lifecycle::AgentLifecycleOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.agent_lifecycle_next_outbox(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.agent_lifecycle_mark_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.agent_lifecycle_mark_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db
            .agent_lifecycle_mark_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.sender_seq.to_string()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        PeerOutbound {
            msg: Some(peer_outbound::Msg::AgentLifecycleDeliver(
                crate::shared::peer_proto::AgentLifecycleDeliver {
                    sender_seq: row.sender_seq,
                    payload_json: String::from_utf8_lossy(&row.payload).into_owned(),
                },
            )),
        }
    }
}

/// Namespace replication backend. Drives `namespace_outbox` rows over
/// the memory channel.
struct MemoryOutbox<'a> {
    db: &'a Database,
}

impl OutboxBackend for MemoryOutbox<'_> {
    type Row = crate::daemon::namespace_db::NsOutboxRow;

    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>> {
        self.db.namespace_next_outbox(peer_id, now)
    }
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.namespace_mark_outbox_in_flight(row_id)
    }
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()> {
        self.db.namespace_mark_outbox_delivered(row_id)
    }
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()> {
        self.db
            .namespace_mark_outbox_failed_retry(row_id, next_attempt_at)
    }
    fn row_id(row: &Self::Row) -> &str {
        &row.id
    }
    fn row_attempts(row: &Self::Row) -> u32 {
        row.attempts
    }
    fn row_ack_key(row: &Self::Row) -> String {
        row.op_id.clone()
    }
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound {
        PeerOutbound {
            msg: Some(peer_outbound::Msg::MemoryDeliver(MemoryDeliver {
                op_id: row.op_id.clone(),
                namespace: row.namespace.clone(),
                key: row.key.clone(),
                value: row.value.clone(),
                lamport: row.lamport,
                origin_daemon_id: row.origin_daemon_id.clone(),
                deleted: row.deleted,
                updated_by: row.updated_by.clone(),
            })),
        }
    }
}
