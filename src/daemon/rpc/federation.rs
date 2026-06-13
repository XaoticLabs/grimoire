//! Federation RPC handlers: peer registry management, topic / namespace /
//! workspace federation, federated namespace KV, agent-lifecycle federation,
//! and scroll-dispatch acceptance.

use std::sync::Arc;

use crate::shared::mail::is_valid_topic_name;
use crate::shared::protocol::*;

use crate::daemon::peer_registry::PeerRegistry;
use crate::daemon::persistence::{Database, unix_now};

use super::{parse_params, resolve_peer, resolve_workspace, rpc_err, try_op, try_params, try_rpc};

// --- Federated namespace memory handlers ---

/// Namespaces and keys: non-empty, printable, bounded. Keys may contain `/`
/// for hierarchy; namespaces are flat labels.
fn valid_ns_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| (b'!'..=b'~').contains(&b))
}

/// Fan a just-applied local write out to every peer the namespace federates
/// to: enqueue an outbox row and wake that peer's drainer. Best-effort;
/// enqueue failures are logged, not surfaced to the writer (the write itself
/// already succeeded locally, and replication retries on its own schedule).
async fn namespace_replicate(
    peer_registry: &Arc<PeerRegistry>,
    write: &crate::daemon::namespace_db::NamespaceWrite,
) {
    let namespace = write.namespace.clone();
    let write_owned = write.clone();
    // Look up outbound peers and enqueue in one trip; tail just fires the
    // notify-outbox calls on the async runtime.
    let enqueued: Vec<String> = peer_registry
        .db
        .run(move |db| -> Vec<String> {
            let peers = match db.namespace_outbound_peers(&namespace) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, ns = %namespace, "ns outbound_peers lookup failed");
                    return Vec::new();
                }
            };
            let mut ok = Vec::with_capacity(peers.len());
            for peer_id in peers {
                let op_id = crate::shared::constants::generate_short_id();
                if let Err(e) = db.namespace_enqueue(&peer_id, &op_id, &write_owned) {
                    tracing::warn!(error = %e, peer_id = %peer_id, "ns enqueue failed");
                    continue;
                }
                ok.push(peer_id);
            }
            ok
        })
        .await;
    for peer_id in &enqueued {
        peer_registry.notify_outbox(peer_id).await;
    }
}

pub(super) async fn handle_ns_put(
    db: &Arc<Database>,
    peer_registry: &Arc<PeerRegistry>,
    daemon_id: &str,
    req: RpcRequest,
) -> RpcResponse {
    let params: crate::shared::protocol::NsPutParams = try_params!(req);
    if !valid_ns_name(&params.namespace) || !valid_ns_name(&params.key) {
        return rpc_err(req.id, "invalid_namespace_or_key");
    }
    let namespace = params.namespace.clone();
    let key = params.key.clone();
    let value = params.value.clone();
    let sender = params.sender.clone();
    let daemon_id_owned = daemon_id.to_string();
    let write = match db
        .run(move |db| {
            let updated_by = sender.as_deref().unwrap_or("system");
            db.namespace_put(
                &namespace,
                &key,
                value.as_bytes(),
                &daemon_id_owned,
                updated_by,
            )
        })
        .await
    {
        Ok(w) => w,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("ns put: {e}")),
    };
    namespace_replicate(peer_registry, &write).await;
    RpcResponse::success_json(
        req.id,
        &crate::shared::protocol::NsPutResult {
            lamport: write.lamport,
            origin_daemon_id: write.origin_daemon_id,
        },
    )
}

pub(super) async fn handle_ns_get(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: crate::shared::protocol::NsGetParams = try_params!(req);
    let namespace = params.namespace;
    let key = params.key;
    match db.run(move |db| db.namespace_get(&namespace, &key)).await {
        Ok(Some(e)) => RpcResponse::success_json(
            req.id,
            &crate::shared::protocol::NsGetResult {
                value: String::from_utf8_lossy(&e.value).into_owned(),
                lamport: e.lamport,
                origin_daemon_id: e.origin_daemon_id,
            },
        ),
        Ok(None) => rpc_err(req.id, "ns_key_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("ns get: {e}")),
    }
}

pub(super) async fn handle_ns_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: crate::shared::protocol::NsListParams = try_params!(req);
    let namespace = params.namespace;
    let prefix = params.prefix;
    try_op(
        req.id,
        "ns list",
        db.run(move |db| db.namespace_list(&namespace, prefix.as_deref()))
            .await
            .map(|entries| {
                let entries = entries
                    .into_iter()
                    .map(|e| crate::shared::protocol::NsListItem {
                        key: e.key,
                        lamport: e.lamport,
                        origin_daemon_id: e.origin_daemon_id,
                        updated_at: e.updated_at,
                    })
                    .collect();
                crate::shared::protocol::NsListResult { entries }
            }),
    )
}

pub(super) async fn handle_ns_delete(
    db: &Arc<Database>,
    peer_registry: &Arc<PeerRegistry>,
    daemon_id: &str,
    req: RpcRequest,
) -> RpcResponse {
    let params: crate::shared::protocol::NsDeleteParams = try_params!(req);
    if !valid_ns_name(&params.namespace) || !valid_ns_name(&params.key) {
        return rpc_err(req.id, "invalid_namespace_or_key");
    }
    let namespace = params.namespace.clone();
    let key = params.key.clone();
    let sender = params.sender.clone();
    let daemon_id_owned = daemon_id.to_string();
    let write = match db
        .run(move |db| {
            let updated_by = sender.as_deref().unwrap_or("system");
            db.namespace_delete(&namespace, &key, &daemon_id_owned, updated_by)
        })
        .await
    {
        Ok(w) => w,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("ns delete: {e}")),
    };
    namespace_replicate(peer_registry, &write).await;
    RpcResponse::success_json(req.id, &crate::shared::protocol::NsDeleteResult::default())
}

pub(super) async fn handle_ns_unfederate(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: crate::shared::protocol::NsUnfederateParams = try_params!(req);
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let peer_id = peer.id.clone();
    let ns = params.namespace.clone();
    match peer_registry
        .db
        .run(move |db| db.delete_namespace_federation(&peer_id, &ns))
        .await
    {
        Ok(removed) => RpcResponse::success_json(
            req.id,
            &crate::shared::protocol::NsUnfederateResult { removed },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("ns.unfederate: {e}")),
    }
}

pub(super) async fn handle_ns_federate(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::shared::types::FederationDirection;
    let params: crate::shared::protocol::NsFederateParams = try_params!(req);
    if !valid_ns_name(&params.namespace) {
        return rpc_err(req.id, "invalid_namespace");
    }
    let direction: FederationDirection = match params.direction.parse() {
        Ok(d) => d,
        Err(_) => return rpc_err(req.id, "invalid_direction"),
    };
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let id = crate::shared::constants::generate_short_id();
    let namespace = params.namespace.clone();
    let peer_id = peer.id.clone();
    let now = unix_now();
    match peer_registry
        .db
        .run(move |db| db.namespace_upsert_federation(&id, &peer_id, &namespace, direction, now))
        .await
    {
        Ok(_) => RpcResponse::success_json(
            req.id,
            &crate::shared::protocol::NsFederateResult::default(),
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("ns federate: {e}")),
    }
}

// --- Federation handlers ---

pub(super) async fn handle_peer_add(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: PeerAddParams = try_params!(req);
    match peer_registry
        .register_peer(
            &params.name,
            &params.url,
            &params.bearer_token,
            &params.cert_pem,
            10,
        )
        .await
    {
        Ok(peer) => RpcResponse::success_json(
            req.id,
            &PeerAddResult {
                peer_id: peer.id,
                daemon_id: peer.daemon_id,
            },
        ),
        Err(e) => rpc_err(req.id, &e.to_string()),
    }
}

/// Return this daemon's transport identity (cert PEM + SHA-256 fingerprint) so
/// an operator can hand it to a remote daemon for pinning at `peer add`.
pub(super) fn handle_peer_local_cert(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let id = &peer_registry.tls_identity;
    RpcResponse::success_json(
        req.id,
        &crate::shared::protocol::PeerLocalCertResult {
            cert_pem: id.cert_pem().to_string(),
            fingerprint_sha256: id.fingerprint(),
        },
    )
}

pub(super) async fn handle_peer_list(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    try_op(
        req.id,
        "list_peers",
        peer_registry
            .list_with_outbox_depth()
            .await
            .map(|peers| PeerListResult { peers }),
    )
}

pub(super) async fn handle_peer_remove(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: PeerRemoveParams = try_params!(req);
    match peer_registry.remove_peer(&params.name).await {
        Ok(removed) => RpcResponse::success_json(req.id, &PeerRemoveResult { removed }),
        Err(e) => rpc_err(req.id, &e.to_string()),
    }
}

pub(super) async fn handle_peer_ping(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: PeerPingParams = try_params!(req);
    match peer_registry.ping_peer(&params.name).await {
        Ok((rtt, state)) => {
            RpcResponse::success_json(req.id, &PeerPingResult { rtt_ms: rtt, state })
        }
        Err(e) => rpc_err(req.id, &e.to_string()),
    }
}

pub(super) async fn handle_topic_federate(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::shared::types::FederationDirection;
    let params: TopicFederateParams = try_params!(req);
    if !is_valid_topic_name(&params.topic) {
        return rpc_err(req.id, "invalid_topic_name");
    }
    if params.topic.starts_with("workspace/")
        || params.topic.starts_with("supervisor/")
        || params.topic.starts_with("wake/")
    {
        return rpc_err(req.id, "topic_federation_reserved");
    }
    let direction: FederationDirection = match params.direction.parse() {
        Ok(d) => d,
        Err(_) => return rpc_err(req.id, "invalid_direction"),
    };
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let id = crate::shared::constants::generate_short_id();
    let now = unix_now();
    let peer_id = peer.id.clone();
    let topic_for_db = params.topic.clone();
    let final_dir = match peer_registry
        .db
        .run(move |db| db.upsert_topic_federation(&id, &peer_id, &topic_for_db, direction, now))
        .await
    {
        Ok(d) => d,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("federate: {e}")),
    };
    peer_registry
        .bus
        .publish(StreamEvent::TopicFederationAdded {
            peer_id: peer.id,
            topic: params.topic.clone(),
            direction: final_dir.as_str().to_string(),
        });
    RpcResponse::success_json(
        req.id,
        &TopicFederateResult {
            topic: params.topic,
            direction: final_dir.as_str().to_string(),
        },
    )
}

pub(super) async fn handle_topic_unfederate(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: TopicUnfederateParams = try_params!(req);
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let peer_id = peer.id.clone();
    let topic_for_db = params.topic.clone();
    match peer_registry
        .db
        .run(move |db| db.delete_topic_federation(&peer_id, &topic_for_db))
        .await
    {
        Ok(removed) => {
            if removed {
                peer_registry
                    .bus
                    .publish(StreamEvent::TopicFederationRemoved {
                        peer_id: peer.id.clone(),
                        topic: params.topic.clone(),
                    });
            }
            RpcResponse::success_json(req.id, &TopicUnfederateResult { removed })
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("unfederate: {e}")),
    }
}

// --- Federation listings ---
// Read-only enumeration of `*_federations` rows. Used by `grim topic
// federations` / `ns federations` / `workspace federations` and the
// dashboard's Federation page.

pub(super) async fn handle_topic_federations(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    match peer_registry
        .db
        .run(crate::daemon::persistence::Database::list_topic_federations)
        .await
    {
        Ok(federations) => RpcResponse::success_json(
            req.id,
            &crate::shared::protocol::TopicFederationsResult { federations },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("topic.federations: {e}")),
    }
}

pub(super) async fn handle_ns_federations(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    match peer_registry
        .db
        .run(crate::daemon::persistence::Database::list_namespace_federations)
        .await
    {
        Ok(federations) => RpcResponse::success_json(
            req.id,
            &crate::shared::protocol::NsFederationsResult { federations },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("ns.federations: {e}")),
    }
}

pub(super) async fn handle_workspace_federations(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    match peer_registry
        .db
        .run(crate::daemon::persistence::Database::list_workspace_federations)
        .await
    {
        Ok(federations) => RpcResponse::success_json(
            req.id,
            &crate::shared::protocol::WorkspaceFederationsResult { federations },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("workspace.federations: {e}")),
    }
}

// --- Workspace federation ---

/// Home-daemon-side opt-in: this workspace's file events will fan out
/// to `peer` per `direction`. The drainer + producer is not yet wired;
/// this records the intent so cross-machine subscribe can be set up ahead
/// of the event flow landing.
pub(super) async fn handle_workspace_federate(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::shared::types::{FederationDirection, WorkspaceKind};
    let params: WorkspaceFederateParams = try_params!(req);
    let direction: FederationDirection = match params.direction.parse() {
        Ok(d) => d,
        Err(_) => return rpc_err(req.id, "invalid_direction"),
    };
    let ws = try_rpc!(resolve_workspace(req.id, &peer_registry.db, &params.workspace).await);
    // Only the *home* of a workspace can opt it into outbound federation.
    // Shadows already point at a remote home and would re-export events
    // they don't originate.
    if matches!(ws.kind, WorkspaceKind::Shadow) {
        return rpc_err(req.id, "workspace_is_shadow_cannot_federate");
    }
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let id = crate::shared::constants::generate_short_id();
    let now = unix_now();
    let peer_id = peer.id.clone();
    let workspace_for_db = params.workspace.clone();
    let final_dir = match peer_registry
        .db
        .run(move |db| {
            db.upsert_workspace_federation(&id, &peer_id, &workspace_for_db, direction, now)
        })
        .await
    {
        Ok(d) => d,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("federate: {e}")),
    };
    RpcResponse::success_json(
        req.id,
        &WorkspaceFederateResult {
            workspace: params.workspace,
            direction: final_dir.as_str().to_string(),
        },
    )
}

/// Consumer-daemon-side: create a local shadow workspace pointing at a
/// remote home, and pre-record an Inbound federation row so events from
/// that peer are authorized on arrival. Caller supplies the
/// `<home-daemon-id>/<home-ws-id>` pair as a single `home` field
/// matching the `agent://`-style address shape.
pub(super) async fn handle_workspace_federate_subscribe(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::shared::types::FederationDirection;
    let params: WorkspaceFederateSubscribeParams = try_params!(req);
    let Some((home_daemon_id, home_workspace_id)) = params.home.split_once('/') else {
        return rpc_err(req.id, "invalid_home_address_expected_daemon/workspace");
    };
    if !crate::shared::types::validate_daemon_id(home_daemon_id) {
        return rpc_err(req.id, "invalid_home_daemon_id");
    }
    if home_workspace_id.is_empty() {
        return rpc_err(req.id, "invalid_home_workspace_id");
    }
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);

    let local_id = params
        .alias
        .unwrap_or_else(|| format!("{home_workspace_id}-shadow"));

    let local_id_for_db = local_id.clone();
    let home_daemon_owned = home_daemon_id.to_string();
    let home_ws_owned = home_workspace_id.to_string();
    let branch = params.branch.clone();
    let peer_id = peer.id.clone();
    let fed_id = crate::shared::constants::generate_short_id();
    let now_ts = chrono::Utc::now();
    let now_secs = unix_now();
    // Insert shadow + federation in one trip; rollback the shadow row
    // best-effort if the federation upsert fails. Keeps the original
    // sequence so existing-id collisions still fail before federations.
    let outcome: Result<Result<(), String>, anyhow::Error> = peer_registry
        .db
        .run(move |db| {
            if let Err(e) = db.insert_shadow_workspace(
                &local_id_for_db,
                &home_daemon_owned,
                &home_ws_owned,
                &branch,
                now_ts,
            ) {
                return Ok(Err(format!("insert_shadow: {e}")));
            }
            if let Err(e) = db.upsert_workspace_federation(
                &fed_id,
                &peer_id,
                &local_id_for_db,
                FederationDirection::Inbound,
                now_secs,
            ) {
                let _ = db.delete_workspace_row(&local_id_for_db);
                return Ok(Err(format!("federate_subscribe: {e}")));
            }
            Ok(Ok(()))
        })
        .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(msg)) => return RpcResponse::error(req.id, -32000, msg),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db: {e}")),
    }
    RpcResponse::success_json(
        req.id,
        &WorkspaceFederateSubscribeResult {
            local_workspace_id: local_id,
            home_daemon_id: home_daemon_id.to_string(),
            home_workspace_id: home_workspace_id.to_string(),
        },
    )
}

/// Symmetric to `topic.unfederate`: run on each side independently to
/// drop the federation row. Does *not* delete the shadow workspace row;
/// that's an explicit `workspace destroy`, so an operator doesn't lose
/// historical chronicle attribution by accident.
pub(super) async fn handle_workspace_unfederate(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: WorkspaceUnfederateParams = try_params!(req);
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let peer_id = peer.id.clone();
    let workspace_for_db = params.workspace.clone();
    match peer_registry
        .db
        .run(move |db| db.delete_workspace_federation(&peer_id, &workspace_for_db))
        .await
    {
        Ok(n) => RpcResponse::success_json(req.id, &WorkspaceUnfederateResult { removed: n > 0 }),
        Err(e) => RpcResponse::error(req.id, -32000, format!("unfederate: {e}")),
    }
}

/// Opt this daemon into agent-lifecycle federation with one peer.
/// Direction follows the existing federation convention
/// (`outbound`/`inbound`/`both`); identical directions on both sides
/// merge to `both`.
///
/// When the resulting direction includes `outbound`, snapshot every
/// active agent's current state into the outbox so the receiver gets
/// a current view without waiting for the next transition. The agents
/// `Dormant`/`Active`/`Failed`/`Complete`/`Restarting` are all
/// snapshotted; `Queued` ones are not (they have no meaningful state
/// for a remote observer yet).
pub(super) async fn handle_agent_lifecycle_federate(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::daemon::peer_client::AgentLifecyclePayload;
    use crate::shared::protocol::{AgentLifecycleFederateParams, AgentLifecycleFederateResult};
    use crate::shared::types::{AgentState, FederationDirection};
    let params: AgentLifecycleFederateParams = try_params!(req);
    let direction: FederationDirection = match params.direction.parse() {
        Ok(d) => d,
        Err(_) => return rpc_err(req.id, "invalid_direction"),
    };
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let peer_id = peer.id.clone();
    let fed_id = crate::shared::constants::generate_short_id();
    let now = unix_now();

    let outcome: Result<(FederationDirection, u32), String> = peer_registry
        .db
        .run(move |db| -> Result<(FederationDirection, u32), String> {
            let final_dir = db
                .upsert_agent_lifecycle_federation(&fed_id, &peer_id, direction, now)
                .map_err(|e| format!("federate: {e}"))?;

            let mut replayed = 0u32;
            if matches!(
                final_dir,
                FederationDirection::Outbound | FederationDirection::Both
            ) {
                let agents = db.list_agents(None).map_err(|e| format!("snapshot: {e}"))?;
                for a in agents {
                    if matches!(a.state, AgentState::Queued) {
                        continue;
                    }
                    let payload = AgentLifecyclePayload {
                        agent_id: a.id.clone(),
                        // No prior state for a synthetic snapshot — use
                        // `new == old` so receivers can detect "this is
                        // a snapshot, not a transition" if they ever care.
                        old_state: a.state.clone(),
                        new_state: a.state,
                        name: a.name,
                        task: a.task,
                        exit_code: a.exit_code,
                    };
                    let bytes = match serde_json::to_vec(&payload) {
                        Ok(b) => b,
                        Err(e) => return Err(format!("encode: {e}")),
                    };
                    if let Err(e) = db.agent_lifecycle_enqueue(&peer_id, &bytes) {
                        return Err(format!("enqueue: {e}"));
                    }
                    replayed += 1;
                }
            }
            Ok((final_dir, replayed))
        })
        .await;

    match outcome {
        Ok((dir, replayed)) => {
            // Poke the drainer so any replayed rows ship immediately.
            if replayed > 0 {
                peer_registry.notify_outbox(&peer.id).await;
            }
            RpcResponse::success_json(
                req.id,
                &AgentLifecycleFederateResult {
                    peer: params.peer,
                    direction: dir.as_str().to_string(),
                    replayed,
                },
            )
        }
        Err(msg) => RpcResponse::error(req.id, -32000, msg),
    }
}

pub(super) async fn handle_agent_lifecycle_unfederate(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::shared::protocol::{AgentLifecycleUnfederateParams, AgentLifecycleUnfederateResult};
    let params: AgentLifecycleUnfederateParams = try_params!(req);
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let peer_id = peer.id.clone();
    match peer_registry
        .db
        .run(move |db| db.delete_agent_lifecycle_federation(&peer_id))
        .await
    {
        Ok(n) => {
            RpcResponse::success_json(req.id, &AgentLifecycleUnfederateResult { removed: n > 0 })
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("lifecycle_unfederate: {e}")),
    }
}

pub(super) async fn handle_peer_set_accept_dispatch(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::shared::protocol::{PeerSetAcceptDispatchParams, PeerSetAcceptDispatchResult};
    let params: PeerSetAcceptDispatchParams = try_params!(req);
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let peer_id = peer.id.clone();
    let accept = params.accept;
    match peer_registry
        .db
        .run(move |db| db.set_peer_accept_scroll_dispatch(&peer_id, accept))
        .await
    {
        Ok(()) => RpcResponse::success_json(
            req.id,
            &PeerSetAcceptDispatchResult {
                peer: params.peer,
                accept,
            },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("set_accept_dispatch: {e}")),
    }
}
