//! Inbound-delivery appliers shared between the outbound client's reverse
//! stream and the inbound peer RPC server: namespace writes, workspace
//! file events, agent-lifecycle snapshots, and scroll-task dispatch.

use crate::shared::peer_proto::{
    AgentLifecycleAck, AgentLifecycleDeliver, MemoryAck, MemoryDeliver, ScrollTaskDispatch,
    ScrollTaskDispatchAck, WorkspaceEventAck, WorkspaceEventDeliver,
};
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{AgentState, Peer};

use super::super::event_bus::EventBus;
use super::super::persistence::Database;

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

    super::super::workspace_watcher::publish_workspace_file_change(
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
