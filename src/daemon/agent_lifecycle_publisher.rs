//! Bus subscriber fanning local `StateChange` events into the
//! `agent_lifecycle_outbox` for each outbound-lifecycle peer (shipped by the
//! `peer_client::run_once` drainer).
//!
//! Only local `StateChange` feeds it; federated `RemoteAgentStateChanged`
//! arrivals are skipped to prevent re-fanout loops in A → B → C topologies.

use std::sync::Arc;

use crate::daemon::event_bus::EventBus;
use crate::daemon::peer_client::AgentLifecyclePayload;
use crate::daemon::peer_registry::PeerRegistry;
use crate::daemon::persistence::Database;
use crate::shared::protocol::StreamEvent;

/// Spawn the subscriber (ends when the broadcast channel closes). Enqueue
/// failures are warn-logged, never fatal — lifecycle federation is best-effort.
pub fn spawn(db: Arc<Database>, bus: EventBus, registry: Arc<PeerRegistry>) {
    let mut rx = bus.subscribe();
    tokio::spawn(async move {
        while let Ok(ev) = rx.recv().await {
            let StreamEvent::StateChange {
                agent_id,
                old_state,
                new_state,
            } = ev
            else {
                continue;
            };

            let peers = match db.agent_lifecycle_outbound_peers() {
                Ok(p) if !p.is_empty() => p,
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!(error = %e, "agent_lifecycle_outbound_peers failed");
                    continue;
                }
            };

            // Ship the agent metadata so the receiver renders rich
            // notifications without re-querying. A missing row (raced destroy)
            // ships a bare payload; the receiver tolerates None.
            let snap = db.get_agent(&agent_id).ok().flatten();
            let payload = AgentLifecyclePayload {
                agent_id: agent_id.clone(),
                old_state,
                new_state,
                name: snap.as_ref().and_then(|a| a.name.clone()),
                task: snap.as_ref().and_then(|a| a.task.clone()),
                exit_code: snap.as_ref().and_then(|a| a.exit_code),
            };
            let bytes = match serde_json::to_vec(&payload) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(error = %e, "agent lifecycle payload encode failed");
                    continue;
                }
            };

            let mut woke: Vec<String> = Vec::with_capacity(peers.len());
            for peer_id in peers {
                match db.agent_lifecycle_enqueue(&peer_id, &bytes) {
                    Ok(_) => woke.push(peer_id),
                    Err(e) => {
                        tracing::warn!(error = %e, peer = %peer_id,
                            "agent_lifecycle_enqueue failed");
                    }
                }
            }
            if woke.is_empty() {
                continue;
            }
            let registry = registry.clone();
            tokio::spawn(async move {
                for peer_id in woke {
                    registry.notify_outbox(&peer_id).await;
                }
            });
        }
    });
}
