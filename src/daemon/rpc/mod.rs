//! JSON-RPC dispatch for the daemon's UDS/HTTP control plane.
//!
//! `handle_rpc` validates the protocol version and routes each method to its
//! domain module (`agents`, `mail`, `federation`, …). The shared parse/error
//! helpers and the `try_params!` / `try_rpc!` early-return macros live here
//! because every domain module uses them.

use serde::de::DeserializeOwned;
use std::sync::Arc;

use crate::shared::constants::RPC_PROTOCOL_VERSION;
use crate::shared::protocol::*;

use super::agent_manager::AgentManager;
use super::event_bus::EventBus;
use super::peer_registry::PeerRegistry;
use super::persistence::Database;
use super::scroll_keeper::ScrollKeeper;
use super::wake_registry::WakeRegistry;
use super::workspace_registry::WorkspaceRegistry;

mod agents;
mod federation;
mod mail;
mod misc;
mod scrolls;
mod supervision;
mod wake;
mod workspace;

pub use self::mail::{handle_mail_ask, handle_mail_send, handle_mail_tender};

pub const MAX_MAIL_BODY_BYTES: usize = 65_536;

fn parse_params<T: DeserializeOwned>(req: &RpcRequest) -> Result<T, RpcResponse> {
    serde_json::from_value(req.params.clone())
        .map_err(|e| RpcResponse::error(req.id, -32602, format!("Invalid params: {e}")))
}

/// Standard `"Failed to {action}: {err}"` RPC error response (code -32000).
fn rpc_fail(req_id: u64, action: &str, err: impl std::fmt::Display) -> RpcResponse {
    RpcResponse::error(req_id, -32000, format!("Failed to {action}: {err}"))
}

/// `Ok` → success_json with the value serialized; `Err` → `Failed to {action}: {err}`.
/// Use `.map(|v| WrapperResult { ... })` upstream when the success type needs
/// to be a wire wrapper.
fn try_op<T: serde::Serialize, E: std::fmt::Display>(
    req_id: u64,
    action: &str,
    r: Result<T, E>,
) -> RpcResponse {
    match r {
        Ok(v) => RpcResponse::success_json(req_id, &v),
        Err(e) => rpc_fail(req_id, action, e),
    }
}

/// Resolve a peer name to a `Peer`, mapping the standard tri-state outcome
/// (`Ok(Some)` / `Ok(None)` / `Err`) into an `RpcResponse` error suitable for
/// early return via `let peer = resolve_peer(req_id, &reg, name).await?;`.
/// Async so the underlying `Database::get_peer_by_name` runs on the blocking
/// pool instead of stalling the Tokio worker.
async fn resolve_peer(
    req_id: u64,
    peer_registry: &PeerRegistry,
    name: &str,
) -> Result<crate::shared::types::Peer, RpcResponse> {
    let name = name.to_string();
    match peer_registry
        .db
        .run(move |db| db.get_peer_by_name(&name))
        .await
    {
        Ok(Some(p)) => Ok(p),
        Ok(None) => Err(rpc_err(req_id, "peer_not_found")),
        Err(e) => Err(RpcResponse::error(req_id, -32000, format!("db: {e}"))),
    }
}

/// Resolve a workspace id to a `Workspace`, same shape as `resolve_peer`.
async fn resolve_workspace(
    req_id: u64,
    db: &Arc<Database>,
    id: &str,
) -> Result<crate::shared::types::Workspace, RpcResponse> {
    let id = id.to_string();
    match db.run(move |db| db.get_workspace(&id)).await {
        Ok(Some(w)) => Ok(w),
        Ok(None) => Err(rpc_err(req_id, "workspace_not_found")),
        Err(e) => Err(RpcResponse::error(req_id, -32000, format!("db: {e}"))),
    }
}

/// RPC errors use code -32000 plus a string in the message; the mail layer
/// uses the symbolic codes documented in the spec (`body_too_large`,
/// `unknown_recipient`, …) as the message text so callers can match on it.
fn rpc_err(req_id: u64, code: &str) -> RpcResponse {
    RpcResponse::error(req_id, -32000, code.to_string())
}

/// Parse `req.params` into the inferred type, returning early with the
/// invalid-params `RpcResponse` on failure. Use in handlers that return
/// `RpcResponse`:
///
/// ```ignore
/// let params: BanishParams = try_params!(req);
/// ```
macro_rules! try_params {
    ($req:expr) => {
        match parse_params(&$req) {
            Ok(p) => p,
            Err(e) => return e,
        }
    };
}
pub(crate) use try_params;

/// Unwrap a `Result<T, RpcResponse>`, returning early with the error
/// response on `Err`. Pairs with helpers like `resolve_peer` /
/// `resolve_workspace` that already produce an `RpcResponse` on failure.
macro_rules! try_rpc {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(resp) => return resp,
        }
    };
}
pub(crate) use try_rpc;

/// Test-only wrapper that fabricates a `PeerRegistry` and a synthetic
/// `daemon_id`. Production code goes through `handle_rpc` with the real
/// per-daemon registry from `AppState`.
pub async fn handle_rpc_test(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    scroll_keeper: &Arc<ScrollKeeper>,
    wake_registry: &Arc<WakeRegistry>,
    workspace_registry: &Arc<WorkspaceRegistry>,
    bus: &EventBus,
    req: RpcRequest,
) -> RpcResponse {
    use super::clock::SystemClock;
    let clock: Arc<dyn super::clock::Clock> = Arc::new(SystemClock);
    let tls_identity = Arc::new(crate::shared::tls::generate("daemon").expect("gen test identity"));
    let peer_registry = super::peer_registry::PeerRegistry::new(
        db.clone(),
        bus.clone(),
        clock,
        "00000000".to_string(),
        tls_identity,
    );
    handle_rpc(
        manager,
        db,
        scroll_keeper,
        wake_registry,
        workspace_registry,
        &peer_registry,
        bus,
        "00000000",
        req,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_rpc(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    scroll_keeper: &Arc<ScrollKeeper>,
    wake_registry: &Arc<WakeRegistry>,
    workspace_registry: &Arc<WorkspaceRegistry>,
    peer_registry: &Arc<PeerRegistry>,
    bus: &EventBus,
    daemon_id: &str,
    req: RpcRequest,
) -> RpcResponse {
    // Validate RPC protocol_version. Existing callers omit the field and
    // default to v1.
    let pv = req.protocol_version.unwrap_or(RPC_PROTOCOL_VERSION);
    if pv != RPC_PROTOCOL_VERSION {
        return rpc_err(req.id, "unsupported_protocol_version");
    }
    match req.method.as_str() {
        "agent.summon" => agents::handle_summon(manager, db, workspace_registry, req).await,
        "agent.circle" => agents::handle_circle(manager, req).await,
        "agent.banish" => agents::handle_banish(manager, req).await,
        "agent.invoke" => agents::handle_invoke(manager, req).await,
        "pact.create" => scrolls::handle_pact_create(db, req).await,
        "pact.list" => scrolls::handle_pact_list(db, req).await,
        "scroll.inscribe" => scrolls::handle_scroll_inscribe(scroll_keeper, req).await,
        "scroll.activate" => scrolls::handle_scroll_activate(scroll_keeper, req).await,
        "scroll.status" => scrolls::handle_scroll_status(scroll_keeper, req),
        "scroll.list" => scrolls::handle_scroll_list(db, req).await,
        "scroll.abandon" => scrolls::handle_scroll_abandon(scroll_keeper, req).await,
        "scroll.approve" => scrolls::handle_scroll_approve(scroll_keeper, req).await,
        "scroll.reject" => scrolls::handle_scroll_reject(scroll_keeper, req).await,
        "daemon.status" => agents::handle_status(manager, daemon_id, req).await,
        "agent.queue.list" => agents::handle_queue_list(db, req).await,
        "agent.processes" => agents::handle_agent_processes(db, req).await,
        "agent.supervisor-tree" => supervision::handle_supervisor_tree(db, req).await,
        "supervisor.restart-now" => supervision::handle_supervisor_restart_now(manager, req).await,
        "supervisor.clear-escalation" => {
            supervision::handle_supervisor_clear_escalation(manager, req).await
        }
        "supervisor.history" => supervision::handle_supervisor_history(db, req).await,
        "agent.result" => agents::handle_agent_result(manager, db, req).await,
        "agent.artifact" => agents::handle_agent_artifact(db, req).await,
        "budget.list" => misc::handle_budget_list(manager, db, req).await,
        "eval.record" => misc::handle_eval_record(db, req).await,
        "eval.list" => misc::handle_eval_list(db, req).await,
        "eval.scores" => misc::handle_eval_scores(db, req).await,
        "mail.send" => mail::handle_mail_send(db, bus, peer_registry, daemon_id, req).await,
        "mail.ask" => mail::handle_mail_ask(db, bus, peer_registry, daemon_id, req).await,
        "mail.tender" => mail::handle_mail_tender(db, bus, peer_registry, daemon_id, req).await,
        "mail.list" => mail::handle_mail_list(db, req).await,
        "mail.ack" => mail::handle_mail_ack(db, bus, req).await,
        "mail.subscribe" => mail::handle_mail_subscribe(db, req).await,
        "mail.unsubscribe" => mail::handle_mail_unsubscribe(db, req).await,
        "mail.topics" => mail::handle_mail_topics(db, req).await,
        "wake.add" => wake::handle_wake_add(db, wake_registry, req).await,
        "wake.list" => wake::handle_wake_list(wake_registry, req).await,
        "wake.remove" => wake::handle_wake_remove(wake_registry, req).await,
        "wake.test" => wake::handle_wake_test(wake_registry, req).await,
        "workspace.create" => workspace::handle_workspace_create(workspace_registry, req).await,
        "workspace.list" => workspace::handle_workspace_list(workspace_registry, req),
        "workspace.destroy" => workspace::handle_workspace_destroy(workspace_registry, req).await,
        "workspace.assign" => workspace::handle_workspace_assign(workspace_registry, req).await,
        "workspace.federate" => federation::handle_workspace_federate(peer_registry, req).await,
        "workspace.federate-subscribe" => {
            federation::handle_workspace_federate_subscribe(peer_registry, req).await
        }
        "workspace.unfederate" => federation::handle_workspace_unfederate(peer_registry, req).await,
        "agent.lifecycle-federate" => {
            federation::handle_agent_lifecycle_federate(peer_registry, req).await
        }
        "agent.lifecycle-unfederate" => {
            federation::handle_agent_lifecycle_unfederate(peer_registry, req).await
        }
        "peer.set-accept-dispatch" => {
            federation::handle_peer_set_accept_dispatch(peer_registry, req).await
        }
        "scroll.dispatch-task" => scrolls::handle_scroll_dispatch_task(peer_registry, req).await,
        "memory.put" => workspace::handle_memory_put(db, bus, req).await,
        "memory.get" => workspace::handle_memory_get(db, req).await,
        "memory.list" => workspace::handle_memory_list(db, req).await,
        "memory.delete" => workspace::handle_memory_delete(db, bus, req).await,
        "ns.put" => federation::handle_ns_put(db, peer_registry, daemon_id, req).await,
        "ns.get" => federation::handle_ns_get(db, req).await,
        "ns.list" => federation::handle_ns_list(db, req).await,
        "ns.delete" => federation::handle_ns_delete(db, peer_registry, daemon_id, req).await,
        "ns.federate" => federation::handle_ns_federate(peer_registry, req).await,
        "ns.unfederate" => federation::handle_ns_unfederate(peer_registry, req).await,
        "peer.add" => federation::handle_peer_add(peer_registry, req).await,
        "peer.local-cert" => federation::handle_peer_local_cert(peer_registry, req),
        "peer.list" => federation::handle_peer_list(peer_registry, req).await,
        "peer.remove" => federation::handle_peer_remove(peer_registry, req).await,
        "peer.ping" => federation::handle_peer_ping(peer_registry, req).await,
        "topic.federate" => federation::handle_topic_federate(peer_registry, req).await,
        "topic.unfederate" => federation::handle_topic_unfederate(peer_registry, req).await,
        "topic.federations" => federation::handle_topic_federations(peer_registry, req).await,
        "ns.federations" => federation::handle_ns_federations(peer_registry, req).await,
        "workspace.federations" => {
            federation::handle_workspace_federations(peer_registry, req).await
        }
        "notify" => misc::handle_notify(bus, req),
        "agent.replay" => agents::handle_replay(db, req).await,
        _ => RpcResponse::error(req.id, -32601, format!("Unknown method: {}", req.method)),
    }
}
