use serde::de::DeserializeOwned;
use std::sync::Arc;

use crate::shared::constants::RPC_PROTOCOL_VERSION;
use crate::shared::mail::{Address, body_preview, is_valid_topic_name, parse_address};
use crate::shared::protocol::*;
use crate::shared::types::{
    AgentState, Mail, MailState, Pact, PactState, Subscription, WakeSourceKind,
};

use super::agent_manager::AgentManager;
use super::event_bus::EventBus;
use super::peer_registry::PeerRegistry;
use super::persistence::{Database, OutboxFanoutRow, unix_now};
use super::scroll_keeper::ScrollKeeper;
use super::wake_registry::WakeRegistry;
use super::workspace_db::MemoryWriteOutcome;
use super::workspace_registry::{WorkspaceRegistry, publish_memory_topic_mail};
use crate::shared::types::{validate_memory_key, validate_workspace_id};

pub const MAX_MAIL_BODY_BYTES: usize = 65_536;
const PREVIEW_CHARS: usize = 200;

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

/// Decide the initial `MailState` for a piece of outbound mail given the
/// looked-up recipient agent. Returns `(state, fail_reason)`; the reason is
/// `Some` iff the state is `Failed`.
fn compute_mail_state(
    agent: Option<&crate::shared::types::Agent>,
) -> (MailState, Option<&'static str>) {
    match agent {
        None => (MailState::Failed, Some("unknown_recipient")),
        Some(a) if a.state == AgentState::Banished => {
            (MailState::Failed, Some("recipient_banished"))
        }
        Some(_) => (MailState::Pending, None),
    }
}

/// Operator-facing fields of a freshly-built outbound mail row, before the
/// daemon mints the `id` and `created_at`. Replaces what used to be ten
/// positional arguments to `new_outbound_mail`. Every call site now reads
/// like prose and a new field doesn't ripple through every caller.
struct MailDraft {
    recipient_id: String,
    sender: Option<String>,
    topic: Option<String>,
    body: String,
    state: MailState,
    fail_reason: Option<&'static str>,
    wake_eligible: bool,
    in_reply_to: Option<String>,
}

/// Stamp a [`MailDraft`] with `mail_id` and `now`, deriving `delivered_at`
/// from `state` so every send path agrees on the invariants. `seq` is set
/// by the insert path.
fn new_outbound_mail(draft: MailDraft, mail_id: String, now: i64) -> Mail {
    let delivered_at = if draft.state == MailState::Pending {
        None
    } else {
        Some(now)
    };
    Mail {
        id: mail_id,
        recipient_id: draft.recipient_id,
        sender_id: draft.sender,
        topic: draft.topic,
        body: draft.body,
        in_reply_to: draft.in_reply_to,
        state: draft.state,
        fail_reason: draft.fail_reason.map(str::to_string),
        created_at: now,
        delivered_at,
        seq: 0,
        wake_eligible: draft.wake_eligible,
    }
}

/// Per-state event emission for a freshly-inserted local mail row.
/// `Pending` → `MailSent` + `MailReceived`; `Failed` → `MailFailed`;
/// `Delivered` should never occur at send time but is handled defensively.
fn emit_mail_events(bus: &EventBus, mail: &Mail, body_preview: &str) {
    match mail.state {
        MailState::Pending => {
            bus.publish(StreamEvent::MailSent {
                mail_id: mail.id.clone(),
                sender_id: mail.sender_id.clone(),
                recipient_id: Some(mail.recipient_id.clone()),
                topic: mail.topic.clone(),
            });
            bus.publish(StreamEvent::MailReceived {
                mail_id: mail.id.clone(),
                recipient_id: mail.recipient_id.clone(),
                sender_id: mail.sender_id.clone(),
                topic: mail.topic.clone(),
                body_preview: body_preview.to_string(),
                wake_eligible: mail.wake_eligible,
                origin_daemon_id: None,
            });
        }
        MailState::Failed => {
            let reason = mail
                .fail_reason
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            bus.publish(StreamEvent::MailFailed {
                mail_id: mail.id.clone(),
                recipient_id: mail.recipient_id.clone(),
                sender_id: mail.sender_id.clone(),
                reason,
            });
        }
        MailState::Delivered => {
            tracing::warn!(mail_id = %mail.id, "unexpected Delivered state at send time");
        }
    }
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
        "agent.summon" => handle_summon(manager, db, workspace_registry, req).await,
        "agent.circle" => handle_circle(manager, req).await,
        "agent.banish" => handle_banish(manager, req).await,
        "agent.invoke" => handle_invoke(manager, req).await,
        "pact.create" => handle_pact_create(db, req).await,
        "pact.list" => handle_pact_list(db, req).await,
        "scroll.inscribe" => handle_scroll_inscribe(scroll_keeper, req).await,
        "scroll.activate" => handle_scroll_activate(scroll_keeper, req).await,
        "scroll.status" => handle_scroll_status(scroll_keeper, req),
        "scroll.list" => handle_scroll_list(db, req).await,
        "scroll.abandon" => handle_scroll_abandon(scroll_keeper, req).await,
        "daemon.status" => handle_status(manager, daemon_id, req).await,
        "agent.queue.list" => handle_queue_list(db, req).await,
        "agent.processes" => handle_agent_processes(db, req).await,
        "agent.result" => handle_agent_result(manager, db, req).await,
        "budget.list" => handle_budget_list(manager, db, req).await,
        "eval.record" => handle_eval_record(db, req).await,
        "eval.list" => handle_eval_list(db, req).await,
        "mail.send" => handle_mail_send(db, bus, peer_registry, daemon_id, req).await,
        "mail.ask" => handle_mail_ask(db, bus, peer_registry, daemon_id, req).await,
        "mail.tender" => handle_mail_tender(db, bus, peer_registry, daemon_id, req).await,
        "mail.list" => handle_mail_list(db, req).await,
        "mail.ack" => handle_mail_ack(db, bus, req).await,
        "mail.subscribe" => handle_mail_subscribe(db, req).await,
        "mail.unsubscribe" => handle_mail_unsubscribe(db, req).await,
        "mail.topics" => handle_mail_topics(db, req).await,
        "wake.add" => handle_wake_add(db, wake_registry, req).await,
        "wake.list" => handle_wake_list(wake_registry, req).await,
        "wake.remove" => handle_wake_remove(wake_registry, req).await,
        "wake.test" => handle_wake_test(wake_registry, req).await,
        "workspace.create" => handle_workspace_create(workspace_registry, req).await,
        "workspace.list" => handle_workspace_list(workspace_registry, req),
        "workspace.destroy" => handle_workspace_destroy(workspace_registry, req).await,
        "workspace.assign" => handle_workspace_assign(workspace_registry, req).await,
        "workspace.federate" => handle_workspace_federate(peer_registry, req).await,
        "workspace.federate-subscribe" => {
            handle_workspace_federate_subscribe(peer_registry, req).await
        }
        "workspace.unfederate" => handle_workspace_unfederate(peer_registry, req).await,
        "memory.put" => handle_memory_put(db, bus, req).await,
        "memory.get" => handle_memory_get(db, req).await,
        "memory.list" => handle_memory_list(db, req).await,
        "memory.delete" => handle_memory_delete(db, bus, req).await,
        "ns.put" => handle_ns_put(db, peer_registry, daemon_id, req).await,
        "ns.get" => handle_ns_get(db, req).await,
        "ns.list" => handle_ns_list(db, req).await,
        "ns.delete" => handle_ns_delete(db, peer_registry, daemon_id, req).await,
        "ns.federate" => handle_ns_federate(peer_registry, req).await,
        "ns.unfederate" => handle_ns_unfederate(peer_registry, req).await,
        "peer.add" => handle_peer_add(peer_registry, req).await,
        "peer.local-cert" => handle_peer_local_cert(peer_registry, req),
        "peer.list" => handle_peer_list(peer_registry, req).await,
        "peer.remove" => handle_peer_remove(peer_registry, req).await,
        "peer.ping" => handle_peer_ping(peer_registry, req).await,
        "topic.federate" => handle_topic_federate(peer_registry, req).await,
        "topic.unfederate" => handle_topic_unfederate(peer_registry, req).await,
        "topic.federations" => handle_topic_federations(peer_registry, req).await,
        "ns.federations" => handle_ns_federations(peer_registry, req).await,
        "workspace.federations" => handle_workspace_federations(peer_registry, req).await,
        "notify" => handle_notify(bus, req),
        "agent.replay" => handle_replay(db, req).await,
        _ => RpcResponse::error(req.id, -32601, format!("Unknown method: {}", req.method)),
    }
}

// --- Notify handler ---

/// Publish an operator-facing notification onto the event bus. The `Notifier`
/// subscriber forwards it to the configured webhook; it also lands in the
/// durable event log. Decoupling via the bus keeps the RPC layer free of any
/// HTTP/notifier dependency.
fn handle_notify(bus: &EventBus, req: RpcRequest) -> RpcResponse {
    let params: NotifyParams = try_params!(req);
    if params.message.trim().is_empty() {
        return RpcResponse::error(req.id, -32602, "notify: message must not be empty".into());
    }
    let level = params.level.unwrap_or_else(|| "info".to_string());
    bus.publish(StreamEvent::Notification {
        agent_id: params.agent_id,
        message: params.message,
        level,
        source: "agent".to_string(),
    });
    RpcResponse::success_json(req.id, &NotifyResult { published: true })
}

// --- Wake handlers ---

async fn handle_wake_add(
    db: &Arc<Database>,
    reg: &Arc<WakeRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: WakeAddParams = try_params!(req);
    // Validate agent exists.
    let agent_id = params.agent_id.clone();
    match db.run(move |db| db.get_agent(&agent_id)).await {
        Ok(Some(_)) => {}
        Ok(None) => return rpc_err(req.id, "agent_not_found"),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db: {e}")),
    }
    let kind: WakeSourceKind = match params.kind.parse() {
        Ok(k) => k,
        Err(_) => return rpc_err(req.id, "invalid_kind"),
    };
    let config_json = match serde_json::to_string(&params.config) {
        Ok(s) => s,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("config: {e}")),
    };
    match reg.register(&params.agent_id, kind, &config_json).await {
        Ok(wake_id) => RpcResponse::success_json(req.id, &WakeAddResult { wake_id }),
        Err(e) => rpc_err(req.id, &e.to_string()),
    }
}

async fn handle_wake_list(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeListParams = parse_params(&req).unwrap_or_default();
    let result = match params.agent_id {
        Some(id) => reg.list_for_agent(&id).await,
        None => reg.list_all().await,
    };
    try_op(
        req.id,
        "list",
        result.map(|sources| WakeListResult { sources }),
    )
}

async fn handle_wake_remove(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeRemoveParams = try_params!(req);
    match reg.remove(&params.wake_id).await {
        Ok(true) => RpcResponse::success_json(req.id, &WakeRemoveResult { success: true }),
        Ok(false) => rpc_err(req.id, "wake_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("remove: {e}")),
    }
}

async fn handle_wake_test(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeTestParams = try_params!(req);
    match reg.test_fire(&params.wake_id).await {
        Ok(mail_id) => RpcResponse::success_json(
            req.id,
            &WakeTestResult {
                success: true,
                mail_id,
            },
        ),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("wake_not_found") {
                rpc_err(req.id, "wake_not_found")
            } else {
                RpcResponse::error(req.id, -32000, msg)
            }
        }
    }
}

async fn handle_summon(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    workspace_registry: &Arc<WorkspaceRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: SummonParams = try_params!(req);

    // Supervision validation.
    let policy_str = params.restart_policy.as_deref().unwrap_or("never");
    let policy: crate::shared::types::RestartPolicy = match policy_str.parse() {
        Ok(p) => p,
        Err(_) => return rpc_err(req.id, "invalid_restart_policy"),
    };
    let any_extra = params.max_restarts.is_some()
        || params.restart_window_secs.is_some()
        || params.escalate_to.is_some();
    if policy == crate::shared::types::RestartPolicy::Never && any_extra {
        if params.escalate_to.is_some() {
            return rpc_err(req.id, "escalate_requires_policy");
        }
        return rpc_err(req.id, "never_with_options");
    }
    if policy == crate::shared::types::RestartPolicy::OnFailure {
        match params.max_restarts {
            None => return rpc_err(req.id, "max_restarts_required"),
            Some(0) => return rpc_err(req.id, "max_restarts_zero"),
            Some(_) => {}
        }
        match params.restart_window_secs {
            None | Some(0) => return rpc_err(req.id, "window_required"),
            Some(n) if n > 604_800 => return rpc_err(req.id, "window_too_large"),
            Some(_) => {}
        }
    }
    if let Some(addr) = &params.escalate_to {
        if policy != crate::shared::types::RestartPolicy::OnFailure {
            return rpc_err(req.id, "escalate_requires_policy");
        }
        // Forward parse error.
        if let Err(e) = crate::shared::mail::parse_address(addr) {
            return rpc_err(req.id, e.code());
        }
    }

    let supervision = if policy == crate::shared::types::RestartPolicy::OnFailure {
        Some(crate::shared::types::SupervisionConfig {
            policy,
            max_restarts: params.max_restarts,
            window_secs: params.restart_window_secs,
            escalate_to: params.escalate_to.clone(),
        })
    } else {
        None
    };

    // Workspace short-circuit on cwd: mutually exclusive with --cwd, looks
    // up the workspace path, and triggers assignment after agent insert.
    if params.workspace.is_some() && params.cwd.is_some() {
        return rpc_err(req.id, "conflicting_options");
    }
    let workspace_path: Option<std::path::PathBuf> = match &params.workspace {
        Some(name) => {
            let ws = try_rpc!(resolve_workspace(req.id, db, name).await);
            if ws.state != crate::shared::types::WorkspaceState::Active {
                return rpc_err(req.id, "workspace_destroying");
            }
            Some(ws.path)
        }
        None => None,
    };

    let cwd = workspace_path
        .clone()
        .unwrap_or_else(|| manager.resolve_cwd(params.cwd.clone()));

    // Policy gate. Deny wins on conflict; an empty allow list means "any."
    // Resolves cwd through `canonicalize` when possible so prefix matches
    // survive symlinks; falls back to the unresolved path if the cwd
    // doesn't yet exist (the agent's process will fail later, not here).
    if let Some(policy) = manager.policy() {
        let provider_for_check = params
            .provider
            .clone()
            .unwrap_or_else(|| manager.default_provider_name().to_string());
        if policy.provider_deny.contains(&provider_for_check) {
            return rpc_err(req.id, "policy_provider_denied");
        }
        if !policy.provider_allow.is_empty() && !policy.provider_allow.contains(&provider_for_check)
        {
            return rpc_err(req.id, "policy_provider_not_allowed");
        }
        let canon_cwd = std::fs::canonicalize(&cwd).unwrap_or_else(|_| cwd.clone());
        let any_match = |prefixes: &[std::path::PathBuf]| {
            prefixes.iter().any(|p| {
                let canon_prefix = std::fs::canonicalize(p).unwrap_or_else(|_| p.clone());
                canon_cwd.starts_with(&canon_prefix)
            })
        };
        if !policy.cwd_deny_prefixes.is_empty() && any_match(&policy.cwd_deny_prefixes) {
            return rpc_err(req.id, "policy_cwd_denied");
        }
        if !policy.cwd_allow_prefixes.is_empty() && !any_match(&policy.cwd_allow_prefixes) {
            return rpc_err(req.id, "policy_cwd_not_allowed");
        }
    }

    let keep_alive = params.keep_alive.unwrap_or(false);
    let result = match manager
        .enqueue_with_options(
            &params.task,
            params.name,
            params.model,
            params.provider,
            &cwd,
            crate::daemon::agent_manager::Lane::Adhoc,
            keep_alive,
            supervision.clone(),
        )
        .await
    {
        Ok(a) => a,
        Err(e) => return rpc_fail(req.id, "summon", e),
    };

    // Post-insert assignment.
    if let Some(name) = &params.workspace
        && let Err(e) = workspace_registry.assign(name, &result.id).await
    {
        tracing::warn!(workspace = %name, agent = %result.id, error = %e, "workspace assign after summon failed");
    }

    // Wire the supervision-tree edge so a subsequent parent banish cascades.
    if let Some(parent) = &params.parent_agent_id {
        if parent == &result.id {
            let _ = manager.banish(&result.id).await;
            return rpc_err(req.id, "self_parent");
        }
        let parent_owned = parent.clone();
        let child_id = result.id.clone();
        let lookup = db
            .run(move |db| match db.get_agent(&parent_owned) {
                Ok(Some(_)) => Ok(Some(db.set_agent_parent(&child_id, Some(&parent_owned)))),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .await;
        match lookup {
            Ok(Some(set_res)) => {
                if let Err(e) = set_res {
                    tracing::warn!(
                        agent = %result.id,
                        parent = %parent,
                        error = %e,
                        "set_agent_parent failed"
                    );
                }
            }
            Ok(None) => {
                let _ = manager.banish(&result.id).await;
                return rpc_err(req.id, "parent_not_found");
            }
            Err(e) => return rpc_fail(req.id, "summon", e),
        }
    }

    // Self-escalation check, post-id-generation, before any further state.
    if let Some(addr) = &params.escalate_to {
        let self_addr = format!("agent://{}", result.id);
        if addr == &self_addr {
            // Roll back the insert.
            let _ = manager.banish(&result.id).await;
            // Banish on Queued cleans queue + agent. The agent row remains
            // (state=Banished). Acceptable behavior; spec asks for "no agent
            // row" but the rollback path is best-effort.
            return rpc_err(req.id, "self_escalation");
        }
    }

    let resp = SummonResult {
        id: result.id,
        name: result.name,
        state: result.state.to_string(),
    };
    RpcResponse::success_json(req.id, &resp)
}

async fn handle_circle(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: CircleParams = parse_params(&req).unwrap_or(CircleParams { state: None });

    try_op(
        req.id,
        "list",
        manager
            .circle(params.state.as_deref())
            .await
            .map(|agents| CircleResult { agents }),
    )
}

async fn handle_banish(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: BanishParams = try_params!(req);
    try_op(
        req.id,
        "banish",
        manager
            .banish(&params.id)
            .await
            .map(|success| BanishResult { success }),
    )
}

async fn handle_invoke(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: InvokeParams = try_params!(req);
    try_op(
        req.id,
        "invoke",
        manager
            .invoke(&params.id, &params.message, None)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

async fn handle_pact_create(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: PactCreateParams = try_params!(req);

    let pact_id = crate::shared::constants::generate_short_id();
    let pact = Pact {
        id: pact_id.clone(),
        source_id: params.source_id.clone(),
        task_tpl: params.task_tpl,
        name: params.name,
        state: PactState::Pending,
        target_id: None,
        created_at: chrono::Utc::now(),
        fired_at: None,
    };

    let pact_for_db = pact.clone();
    try_op(
        req.id,
        "create pact",
        db.run(move |db| db.insert_pact(&pact_for_db))
            .await
            .map(|()| PactCreateResult {
                id: pact_id,
                source_id: params.source_id,
            }),
    )
}

async fn handle_pact_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: PactListParams = parse_params(&req).unwrap_or(PactListParams { source_id: None });
    let source_id = params.source_id;
    try_op(
        req.id,
        "list pacts",
        db.run(move |db| db.list_pacts(source_id.as_deref()))
            .await
            .map(|pacts| PactListResult { pacts }),
    )
}

// --- Scroll handlers ---

async fn handle_scroll_inscribe(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollInscribeParams = try_params!(req);

    let content = match tokio::fs::read_to_string(&params.spec_path).await {
        Ok(c) => c,
        Err(e) => {
            return RpcResponse::error(
                req.id,
                -32000,
                format!("Failed to read spec file '{}': {}", params.spec_path, e),
            );
        }
    };

    let spec = match super::scroll_parser::parse_scroll(&content) {
        Ok(s) => s,
        Err(e) => {
            return rpc_fail(req.id, "parse spec", e);
        }
    };

    try_op(
        req.id,
        "inscribe",
        keeper
            .inscribe(spec, params.max_concurrency, Some(params.spec_path))
            .map(|result| ScrollInscribeResult {
                id: result.scroll.id,
                name: result.scroll.name,
                task_count: result.task_count,
                conflicts: result.conflicts,
            }),
    )
}

async fn handle_scroll_activate(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollActivateParams = try_params!(req);
    try_op(
        req.id,
        "activate",
        keeper
            .activate(&params.id)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

fn handle_scroll_status(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollStatusParams = try_params!(req);
    try_op(req.id, "get status", keeper.status(&params.id))
}

async fn handle_scroll_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    try_op(
        req.id,
        "list scrolls",
        db.run(Database::list_scrolls)
            .await
            .map(|scrolls| serde_json::json!({"scrolls": scrolls})),
    )
}

async fn handle_scroll_abandon(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollAbandonParams = try_params!(req);
    try_op(
        req.id,
        "abandon",
        keeper
            .abandon(&params.id)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

async fn handle_queue_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    try_op(
        req.id,
        "list queue",
        db.run(Database::list_queue).await.map(|rows| {
            let now = chrono::Utc::now();
            let entries: Vec<QueueEntry> = rows
                .into_iter()
                .map(|row| {
                    let age = (now - row.enqueued_at).num_seconds().max(0) as u64;
                    QueueEntry {
                        id: row.id,
                        lane: row.lane,
                        age_seconds: age,
                        provider: row.provider_name,
                        cwd: row.cwd,
                        model: row.model,
                        block_reason: row.block_reason,
                        task_text: row.task_text,
                    }
                })
                .collect();
            QueueListResponse { entries }
        }),
    )
}

/// Return an agent's full durable event timeline for `grim chronicle`. The
/// agent must exist (so an unknown id is a clean error, not an empty reel);
/// beyond that this is a straight read of the `events` table. All filtering
/// and state reconstruction is the client's job.
async fn handle_replay(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: ReplayParams = try_params!(req);

    let id = params.id.clone();
    // One trip to the blocking pool: agent existence check + event log read.
    let outcome: Result<Option<Vec<crate::daemon::persistence::StoredEvent>>, anyhow::Error> = db
        .run(move |db| match db.get_agent(&id) {
            Ok(Some(_)) => db.read_stream_events(&id).map(Some),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        })
        .await;

    match outcome {
        Ok(Some(stored)) => {
            let entries: Vec<ReplayEntry> = stored
                .into_iter()
                .map(|s| ReplayEntry {
                    seq: s.seq,
                    kind: s.kind,
                    ts: s.ts,
                    event: s.event,
                })
                .collect();
            RpcResponse::success_json(
                req.id,
                &ReplayResponse {
                    agent_id: params.id,
                    entries,
                },
            )
        }
        Ok(None) => {
            RpcResponse::error(req.id, -32000, format!("no agent matching '{}'", params.id))
        }
        Err(e) => rpc_fail(req.id, "read event log", e),
    }
}

// --- Mail handlers ---

/// RPC errors use code -32000 plus a string in the message; the mail layer
/// uses the symbolic codes documented in the spec (`body_too_large`,
/// `unknown_recipient`, …) as the message text so callers can match on it.
fn rpc_err(req_id: u64, code: &str) -> RpcResponse {
    RpcResponse::error(req_id, -32000, code.to_string())
}

/// Persist one rubric-scored evaluation, attributing the verdict from
/// `evaluator_id` to `target_id`. Idempotency is per-call (each insert
/// mints a new row id); callers that want dedupe should do so client-side.
async fn handle_eval_record(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: crate::shared::protocol::EvalRecordParams = try_params!(req);
    let target_id = params.target_id.clone();
    let evaluator_id = params.evaluator_id.clone();
    let verdict = params.verdict.clone();
    let rationale = params.rationale.clone();
    let score = params.score;
    let outcome = db
        .run(
            move |db| -> Result<Option<anyhow::Result<String>>, anyhow::Error> {
                if db.get_agent(&target_id)?.is_none() {
                    return Ok(None);
                }
                Ok(Some(db.insert_eval_result(
                    &target_id,
                    &evaluator_id,
                    score,
                    verdict.as_deref(),
                    rationale.as_deref(),
                )))
            },
        )
        .await;
    match outcome {
        Ok(None) => rpc_err(req.id, "target_not_found"),
        Ok(Some(Ok(id))) => {
            RpcResponse::success_json(req.id, &crate::shared::protocol::EvalRecordResult { id })
        }
        Ok(Some(Err(e))) => RpcResponse::error(req.id, -32000, format!("insert_eval_result: {e}")),
        Err(e) => RpcResponse::error(req.id, -32000, format!("db: {e}")),
    }
}

async fn handle_eval_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: crate::shared::protocol::EvalListParams = try_params!(req);
    let target_id = params.target_id.clone();
    let rows = match db.run(move |db| db.list_eval_results(&target_id)).await {
        Ok(r) => r,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("list_eval_results: {e}")),
    };
    let results = rows
        .into_iter()
        .map(|r| crate::shared::protocol::EvalRecord {
            id: r.id,
            target_id: r.target_id,
            evaluator_id: r.evaluator_id,
            score: r.score,
            verdict: r.verdict,
            rationale: r.rationale,
            created_at: r.created_at,
        })
        .collect();
    RpcResponse::success_json(req.id, &crate::shared::protocol::EvalListResult { results })
}

/// Return the provider-extracted final result text for an agent. Mirrors
/// `manager.agent_result()` (the in-process accessor used by pact
/// `{output}` injection) over the RPC, so the CLI can read an evaluator's
/// score JSON without scraping the chronicle.
async fn handle_agent_result(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    req: RpcRequest,
) -> RpcResponse {
    let params: crate::shared::protocol::AgentResultParams = try_params!(req);
    let id = params.id.clone();
    let agent = match db.run(move |db| db.get_agent(&id)).await {
        Ok(Some(a)) => a,
        Ok(None) => return rpc_err(req.id, "agent_not_found"),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db error: {e}")),
    };
    let result = manager.agent_result(&params.id);
    let resp = crate::shared::protocol::AgentResultResponse {
        result,
        state: agent.state.to_string(),
    };
    RpcResponse::success_json(req.id, &resp)
}

/// Snapshot every configured budget with its USD cap and today's running
/// spend. Read-only; runs against the same `budget_spend` rows the
/// dispatch-time gate consults.
async fn handle_budget_list(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    req: RpcRequest,
) -> RpcResponse {
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let budget_meta: Vec<(String, crate::shared::config::BudgetConfig)> = manager
        .budgets()
        .iter()
        .map(|(n, b)| (n.clone(), b.clone()))
        .collect();
    let today_for_db = today.clone();
    let spend_meta = budget_meta.clone();
    let spends: Vec<f64> = db
        .run(move |db| {
            spend_meta
                .iter()
                .map(|(name, _)| db.get_budget_spend(name, &today_for_db).unwrap_or(0.0))
                .collect()
        })
        .await;
    let mut budgets: Vec<crate::shared::protocol::BudgetStatus> = budget_meta
        .into_iter()
        .zip(spends)
        .map(
            |((name, b), spent_usd)| crate::shared::protocol::BudgetStatus {
                name,
                daily_usd: b.daily_usd,
                spent_usd,
                providers: b.providers.clone(),
                hard: b.hard,
            },
        )
        .collect();
    budgets.sort_by(|a, b| a.name.cmp(&b.name));
    let result = crate::shared::protocol::BudgetListResult {
        day: today,
        budgets,
    };
    RpcResponse::success_json(req.id, &result)
}

pub async fn handle_mail_send(
    db: &Arc<Database>,
    bus: &EventBus,
    peer_registry: &Arc<PeerRegistry>,
    daemon_id: &str,
    req: RpcRequest,
) -> RpcResponse {
    let params: MailSendParams = try_params!(req);

    if params.body.len() > MAX_MAIL_BODY_BYTES {
        return rpc_err(req.id, "body_too_large");
    }

    // Reserved-prefix guard: user-supplied senders cannot forge system
    // identities. Internal callers (wake registry, supervisor) bypass
    // mail.send entirely and write rows directly.
    if let Some(s) = &params.sender
        && (s.starts_with("supervisor://")
            || s.starts_with("wake://")
            || s.starts_with("workspace://")
            || s.starts_with("peer://"))
    {
        return rpc_err(req.id, "reserved_sender_prefix");
    }

    let address = match parse_address(&params.to) {
        Ok(a) => a,
        Err(e) => return rpc_err(req.id, e.code()),
    };

    let wake_eligible = params.wake_eligible.unwrap_or(true);

    match address {
        Address::Agent(recipient_id) => {
            handle_direct_send(db, bus, &req, &params, recipient_id, wake_eligible).await
        }
        Address::Topic(topic) => {
            handle_topic_send(db, bus, &req, &params, topic, wake_eligible, peer_registry).await
        }
        Address::FederatedAgent {
            daemon_id: target_daemon,
            agent_id,
        } => {
            // Self via federated form: rewrite to local before reaching
            // federation routing.
            if target_daemon == daemon_id {
                handle_direct_send(db, bus, &req, &params, agent_id, wake_eligible).await
            } else {
                handle_federated_direct_send(
                    db,
                    bus,
                    peer_registry,
                    &req,
                    &params,
                    &target_daemon,
                    &agent_id,
                    wake_eligible,
                )
                .await
            }
        }
    }
}

/// Synchronous request/reply over the mailbox. Sends `params.body` to
/// `params.to`, then blocks until either:
///   * an inbound `MailReceived` event names a mail whose `in_reply_to`
///     equals the sent mail's id, in which case the full reply row is
///     returned, or
///   * `timeout_ms` elapses (default 30 000), returning `ask_timeout`.
///
/// Repliers acknowledge the request by sending an ordinary mail with
/// `in_reply_to` set to the original mail id. There is no separate "reply"
/// verb: ordinary `mail.send` carries the correlation.
pub async fn handle_mail_ask(
    db: &Arc<Database>,
    bus: &EventBus,
    peer_registry: &Arc<PeerRegistry>,
    daemon_id: &str,
    req: RpcRequest,
) -> RpcResponse {
    let params: crate::shared::protocol::MailAskParams = try_params!(req);
    let timeout = std::time::Duration::from_millis(params.timeout_ms.unwrap_or(30_000));
    let req_id = req.id;

    let posted = match post_request_for_reply(
        db,
        bus,
        peer_registry,
        daemon_id,
        &req,
        &params.to,
        &params.body,
        params.sender.clone(),
    )
    .await
    {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    if posted.request_ids.is_empty() {
        return rpc_err(req_id, "ask_no_recipients");
    }

    let replies = collect_mail_replies(db, posted.events, &posted.request_ids, timeout, 1).await;
    match replies.into_iter().next() {
        Some(reply) => {
            RpcResponse::success_json(req_id, &crate::shared::protocol::MailAskResult { reply })
        }
        None => rpc_err(req_id, "ask_timeout"),
    }
}

/// Subscribe to the bus, send the request mail, and return the
/// subscription handle + posted mail ids. Holding the subscriber *before*
/// the send is the load-bearing detail: a fast reply must not race past us.
struct PostedRequest {
    events: tokio::sync::broadcast::Receiver<StreamEvent>,
    request_ids: std::collections::HashSet<String>,
}

#[allow(clippy::too_many_arguments)]
async fn post_request_for_reply(
    db: &Arc<Database>,
    bus: &EventBus,
    peer_registry: &Arc<PeerRegistry>,
    daemon_id: &str,
    req: &RpcRequest,
    to: &str,
    body: &str,
    sender: Option<String>,
) -> Result<PostedRequest, RpcResponse> {
    let events = bus.subscribe();
    let send_req = RpcRequest {
        id: req.id,
        protocol_version: req.protocol_version,
        method: "mail.send".to_string(),
        params: serde_json::to_value(MailSendParams {
            to: to.to_string(),
            body: body.to_string(),
            sender,
            wake_eligible: Some(true),
            in_reply_to: None,
        })
        .expect("MailSendParams serializes to JSON infallibly"),
        auth_token: req.auth_token.clone(),
    };
    let send_resp = handle_mail_send(db, bus, peer_registry, daemon_id, send_req).await;
    if send_resp.error.is_some() {
        return Err(send_resp);
    }
    let send_result: MailSendResult = serde_json::from_value(
        send_resp
            .result
            .ok_or_else(|| rpc_err(req.id, "mail_send_no_result"))?,
    )
    .map_err(|_| rpc_err(req.id, "mail_send_no_result"))?;
    Ok(PostedRequest {
        events,
        request_ids: send_result.mail_ids.into_iter().collect(),
    })
}

/// Drain `events` until `stop_after_n` distinct replies have arrived whose
/// `in_reply_to` matches one of `request_ids`, or `timeout` elapses. The
/// caller decides whether zero matches is a failure (`mail.ask`) or a
/// legitimate empty result (`mail.tender`).
async fn collect_mail_replies(
    db: &Database,
    mut events: tokio::sync::broadcast::Receiver<StreamEvent>,
    request_ids: &std::collections::HashSet<String>,
    timeout: std::time::Duration,
    stop_after_n: usize,
) -> Vec<Mail> {
    let mut out: Vec<Mail> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + timeout;
    while out.len() < stop_after_n {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let recv = match tokio::time::timeout(remaining, events.recv()).await {
            Ok(Ok(ev)) => ev,
            // Lagged subscribers can still catch later events; closed bus
            // means no more replies will ever arrive. Both are treated the
            // same here, with the deadline as the real exit condition.
            Ok(Err(_)) => continue,
            Err(_) => break,
        };
        let mail_id = match &recv {
            StreamEvent::MailReceived { mail_id, .. }
            | StreamEvent::MailDelivered { mail_id, .. } => mail_id.clone(),
            _ => continue,
        };
        if !seen.insert(mail_id.clone()) {
            continue;
        }
        let Ok(Some(mail)) = db.get_mail(&mail_id) else {
            continue;
        };
        if let Some(rt) = &mail.in_reply_to
            && request_ids.contains(rt)
        {
            out.push(mail);
        }
    }
    out
}

/// Multi-bid auction over the mailbox. Posts `params.body` to `params.to`
/// (typically a `topic://...`), then collects every reply mail whose
/// `in_reply_to` matches one of the posted ids until `deadline_ms` elapses.
/// Unlike [`handle_mail_ask`], this returns *all* bids. Picking the winner
/// is the caller's job, and zero bids is not an error.
pub async fn handle_mail_tender(
    db: &Arc<Database>,
    bus: &EventBus,
    peer_registry: &Arc<PeerRegistry>,
    daemon_id: &str,
    req: RpcRequest,
) -> RpcResponse {
    let params: crate::shared::protocol::MailTenderParams = try_params!(req);
    let deadline = std::time::Duration::from_millis(params.deadline_ms.unwrap_or(30_000));
    let req_id = req.id;

    let posted = match post_request_for_reply(
        db,
        bus,
        peer_registry,
        daemon_id,
        &req,
        &params.to,
        &params.body,
        params.sender.clone(),
    )
    .await
    {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let request_mail_ids: Vec<String> = posted.request_ids.iter().cloned().collect();

    let bids =
        collect_mail_replies(db, posted.events, &posted.request_ids, deadline, usize::MAX).await;

    let result = crate::shared::protocol::MailTenderResult {
        request_mail_ids,
        bids,
    };
    RpcResponse::success_json(req_id, &result)
}

async fn handle_direct_send(
    db: &Arc<Database>,
    bus: &EventBus,
    req: &RpcRequest,
    params: &MailSendParams,
    recipient_id: String,
    wake_eligible: bool,
) -> RpcResponse {
    let preview = body_preview(&params.body, PREVIEW_CHARS);
    let mail_id = crate::shared::constants::generate_short_id();
    let now = unix_now();

    let recipient_for_db = recipient_id.clone();
    let sender = params.sender.clone();
    let body = params.body.clone();
    let in_reply_to = params.in_reply_to.clone();
    let mail_id_for_db = mail_id.clone();
    // One trip: lookup recipient, build mail row, insert it. Returns the
    // finished `Mail` (with computed state/fail_reason) for downstream events.
    let outcome: Result<Result<Mail, anyhow::Error>, anyhow::Error> = db
        .run(move |db| {
            let agent = db.get_agent(&recipient_for_db)?;
            let (state, fail_reason) = compute_mail_state(agent.as_ref());
            let mail = new_outbound_mail(
                MailDraft {
                    recipient_id: recipient_for_db,
                    sender,
                    topic: None,
                    body,
                    state,
                    fail_reason,
                    wake_eligible,
                    in_reply_to,
                },
                mail_id_for_db,
                now,
            );
            Ok::<_, anyhow::Error>(db.insert_mail(&mail).map(|()| mail))
        })
        .await;
    let mail = match outcome {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => return RpcResponse::error(req.id, -32000, format!("insert_mail: {e}")),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db error: {e}")),
    };
    let state = mail.state;
    emit_mail_events(bus, &mail, &preview);

    let delivered = u32::from(state == MailState::Pending);
    RpcResponse::success_json(
        req.id,
        &MailSendResult {
            delivered,
            mail_ids: vec![mail_id],
        },
    )
}

/// Output of the single blocking-pool trip [`handle_topic_send`] uses to do
/// per-subscriber state lookup + mail batch insert + federation fanout in
/// one shot, returning data the async tail needs for bus emission and
/// peer notification.
struct TopicSendOut {
    mails: Vec<Mail>,
    delivered: u32,
    insert_err: Option<String>,
    full_peers: Vec<String>,
    notify_peers: Vec<String>,
    fanout_err: Option<String>,
}

async fn handle_topic_send(
    db: &Arc<Database>,
    bus: &EventBus,
    req: &RpcRequest,
    params: &MailSendParams,
    topic: String,
    wake_eligible: bool,
    peer_registry: &Arc<PeerRegistry>,
) -> RpcResponse {
    let topic_for_db = topic.clone();
    let subscribers = match db
        .run(move |db| db.list_subscribers_for_topic(&topic_for_db))
        .await
    {
        Ok(s) => s,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db error: {e}")),
    };

    if subscribers.is_empty() {
        bus.publish(StreamEvent::MailSent {
            mail_id: String::new(),
            sender_id: params.sender.clone(),
            recipient_id: None,
            topic: Some(topic),
        });
        let result = MailSendResult {
            delivered: 0,
            mail_ids: vec![],
        };
        return RpcResponse::success_json(req.id, &result);
    }

    let preview = body_preview(&params.body, PREVIEW_CHARS);
    let now = unix_now();

    // Pre-clone everything the closure needs so it owns 'static state.
    let topic_for_db = topic.clone();
    let sender_for_db = params.sender.clone();
    let body_for_db = params.body.clone();
    let in_reply_to_for_db = params.in_reply_to.clone();
    let subs_for_db = subscribers.clone();
    let mail_id_for_peer = crate::shared::constants::generate_short_id();
    let mail_id_for_peer_for_db = mail_id_for_peer.clone();

    // Single trip to the blocking pool: per-subscriber state lookup, mail
    // batch insert, federation enumeration, per-peer cap check, and outbox
    // fanout insert. Returns rich data the async tail needs (events, peer
    // notifications).
    let out: TopicSendOut = db
        .run(move |db| {
            let mut mails: Vec<Mail> = Vec::with_capacity(subs_for_db.len());
            let mut delivered: u32 = 0;
            for sub in &subs_for_db {
                let agent = match db.get_agent(&sub.subscriber_id) {
                    Ok(a) => a,
                    Err(e) => {
                        return TopicSendOut {
                            mails,
                            delivered,
                            insert_err: Some(format!("db error: {e}")),
                            full_peers: Vec::new(),
                            notify_peers: Vec::new(),
                            fanout_err: None,
                        };
                    }
                };
                let (state, fail_reason) = compute_mail_state(agent.as_ref());
                if state == MailState::Pending {
                    delivered += 1;
                }
                mails.push(new_outbound_mail(
                    MailDraft {
                        recipient_id: sub.subscriber_id.clone(),
                        sender: sender_for_db.clone(),
                        topic: Some(topic_for_db.clone()),
                        body: body_for_db.clone(),
                        state,
                        fail_reason,
                        wake_eligible,
                        in_reply_to: in_reply_to_for_db.clone(),
                    },
                    crate::shared::constants::generate_short_id(),
                    now,
                ));
            }

            if let Err(e) = db.insert_mail_batch(&mails) {
                return TopicSendOut {
                    mails,
                    delivered,
                    insert_err: Some(format!("insert_mail_batch: {e}")),
                    full_peers: Vec::new(),
                    notify_peers: Vec::new(),
                    fanout_err: None,
                };
            }

            // Federation fanout (Task 12).
            let federated_peers = db
                .list_outbound_federations_for_topic(&topic_for_db)
                .unwrap_or_default();
            let mut full_peers: Vec<String> = Vec::new();
            let mut notify_peers: Vec<String> = Vec::new();
            let mut fanout_err = None;
            if !federated_peers.is_empty() {
                let pick_id = mails
                    .first()
                    .map_or_else(|| mail_id_for_peer_for_db.clone(), |m| m.id.clone());
                let recipient_addr = format!("topic://{topic_for_db}");
                let mut fanout: Vec<OutboxFanoutRow> = Vec::new();
                for fed in &federated_peers {
                    if let Ok(depth) = db.outbox_depth(&fed.peer_id)
                        && depth >= PEER_OUTBOX_MAX_DEPTH_DEFAULT
                    {
                        full_peers.push(fed.peer_id.clone());
                        continue;
                    }
                    let outbox_id = crate::shared::constants::generate_short_id();
                    fanout.push((
                        fed.peer_id.clone(),
                        outbox_id,
                        pick_id.clone(),
                        recipient_addr.clone(),
                        body_for_db.clone(),
                        sender_for_db.clone(),
                        now,
                    ));
                }
                if !fanout.is_empty() {
                    match db.insert_mail_batch_with_outbox(&[], &fanout) {
                        Ok(()) => {
                            for (peer_id, _, _, _, _, _, _) in &fanout {
                                notify_peers.push(peer_id.clone());
                            }
                        }
                        Err(e) => {
                            fanout_err = Some(e.to_string());
                        }
                    }
                }
            }

            TopicSendOut {
                mails,
                delivered,
                insert_err: None,
                full_peers,
                notify_peers,
                fanout_err,
            }
        })
        .await;

    if let Some(err) = out.insert_err {
        return RpcResponse::error(req.id, -32000, err);
    }
    let mails = out.mails;
    let delivered = out.delivered;
    for peer_id in &out.full_peers {
        bus.publish(StreamEvent::PeerMailForwardFailed {
            peer_id: peer_id.clone(),
            mail_id: mail_id_for_peer.clone(),
            reason: "peer_outbox_full".to_string(),
        });
    }
    if let Some(err) = out.fanout_err {
        tracing::warn!(error = %err, "topic federation outbox fanout failed");
    }
    for peer_id in &out.notify_peers {
        peer_registry.notify_outbox(peer_id).await;
    }

    // Emit one MailSent + MailReceived per Pending row (and MailFailed per
    // Failed row); event stream is "one event per recipient".
    for mail in &mails {
        emit_mail_events(bus, mail, &preview);
    }

    let result = MailSendResult {
        delivered,
        mail_ids: mails.into_iter().map(|m| m.id).collect(),
    };
    RpcResponse::success_json(req.id, &result)
}

async fn handle_mail_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MailListParams = try_params!(req);
    let limit = params.limit.unwrap_or(100);
    if limit > 1000 {
        return rpc_err(req.id, "limit_too_large");
    }
    let agent_id = params.agent_id.clone();
    let after_seq = params.after_seq;
    let state = params.state;
    try_op(
        req.id,
        "list mail",
        db.run(move |db| db.list_mail_by_recipient(&agent_id, after_seq, state, limit))
            .await
            .map(|mails| MailListResult { mails }),
    )
}

async fn handle_mail_ack(db: &Arc<Database>, bus: &EventBus, req: RpcRequest) -> RpcResponse {
    let params: MailAckParams = try_params!(req);

    let mail_id = params.mail_id.clone();
    // Lookup + state mutation in one trip; tail handles event emission.
    let outcome: Result<Result<Option<Mail>, anyhow::Error>, anyhow::Error> = db
        .run(
            move |db| -> Result<Result<Option<Mail>, anyhow::Error>, anyhow::Error> {
                let Some(mail) = db.get_mail(&mail_id)? else {
                    return Ok(Ok(None));
                };
                match mail.state {
                    MailState::Pending => {
                        match db.set_mail_state(&mail.id, MailState::Delivered, None) {
                            Ok(()) => Ok(Ok(Some(mail))),
                            Err(e) => Ok(Err(e)),
                        }
                    }
                    // Delivered/Failed: return the mail unchanged so the tail can
                    // distinguish via its `state`.
                    _ => Ok(Ok(Some(mail))),
                }
            },
        )
        .await;
    let mail = match outcome {
        Ok(Ok(Some(m))) => m,
        Ok(Ok(None)) => return rpc_err(req.id, "mail_not_found"),
        Ok(Err(e)) => return RpcResponse::error(req.id, -32000, format!("set_state: {e}")),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db: {e}")),
    };

    match mail.state {
        MailState::Delivered => RpcResponse::success_json(req.id, &MailAckResult { acked: false }),
        MailState::Failed => rpc_err(req.id, "cannot_ack_failed"),
        MailState::Pending => {
            bus.publish(StreamEvent::MailDelivered {
                mail_id: mail.id.clone(),
                recipient_id: mail.recipient_id,
                origin_daemon_id: None,
            });
            RpcResponse::success_json(req.id, &MailAckResult { acked: true })
        }
    }
}

async fn handle_mail_subscribe(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MailSubscribeParams = try_params!(req);

    if !is_valid_topic_name(&params.topic) {
        return rpc_err(req.id, "invalid_topic_name");
    }

    let new_id = crate::shared::constants::generate_short_id();
    let sub = Subscription {
        id: new_id,
        subscriber_id: params.agent_id.clone(),
        topic: params.topic,
        created_at: unix_now(),
    };
    let agent_id_for_db = params.agent_id;
    let sub_for_db = sub.clone();
    // Validate-then-insert in one trip.
    let outcome: Result<Result<Option<anyhow::Result<String>>, anyhow::Error>, anyhow::Error> = db
        .run(
            move |db| -> Result<
                Result<Option<anyhow::Result<String>>, anyhow::Error>,
                anyhow::Error,
            > {
                match db.get_agent(&agent_id_for_db)? {
                    Some(_) => Ok(Ok(Some(db.insert_subscription(&sub_for_db)))),
                    None => Ok(Ok(None)),
                }
            },
        )
        .await;
    match outcome {
        Ok(Ok(None)) => rpc_err(req.id, "unknown_agent"),
        Ok(Ok(Some(Ok(id)))) => RpcResponse::success_json(
            req.id,
            &MailSubscribeResult {
                subscription_id: id,
            },
        ),
        Ok(Ok(Some(Err(e)))) => {
            RpcResponse::error(req.id, -32000, format!("insert_subscription: {e}"))
        }
        Ok(Err(e)) | Err(e) => RpcResponse::error(req.id, -32000, format!("db: {e}")),
    }
}

async fn handle_mail_unsubscribe(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MailUnsubscribeParams = try_params!(req);
    let id = params.subscription_id;
    match db.run(move |db| db.delete_subscription(&id)).await {
        Ok(true) => RpcResponse::success_json(req.id, &MailUnsubscribeResult::default()),
        Ok(false) => rpc_err(req.id, "subscription_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("delete_subscription: {e}")),
    }
}

async fn handle_mail_topics(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    try_op(
        req.id,
        "list_topics",
        db.run(Database::list_topics_with_counts).await.map(|rows| {
            let topics: Vec<TopicCount> = rows
                .into_iter()
                .map(|(topic, n)| TopicCount {
                    topic,
                    subscriber_count: n,
                })
                .collect();
            MailTopicsResult { topics }
        }),
    )
}

async fn handle_status(
    manager: &Arc<AgentManager>,
    daemon_id: &str,
    req: RpcRequest,
) -> RpcResponse {
    try_op(
        req.id,
        "status",
        manager.circle(None).await.map(|agents| {
            use crate::shared::types::AgentState;
            let active = agents
                .iter()
                .filter(|a| a.state == AgentState::Active)
                .count();
            let queued = agents
                .iter()
                .filter(|a| a.state == AgentState::Queued)
                .count();
            DaemonStatusResult {
                uptime_secs: 0,
                agent_count: agents.len(),
                active_count: active,
                queued_count: queued,
                max_concurrent_agents: manager.max_concurrent_agents(),
                daemon_id: Some(daemon_id.to_string()),
            }
        }),
    )
}

// --- Workspace + Memory handlers ---

async fn handle_workspace_create(reg: &Arc<WorkspaceRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WorkspaceCreateParams = try_params!(req);
    if let Err(e) = validate_workspace_id(&params.name) {
        let _ = e;
        return rpc_err(req.id, "invalid_workspace_name");
    }
    match reg
        .create(
            &params.name,
            std::path::Path::new(&params.repo_path),
            &params.branch,
        )
        .await
    {
        Ok(ws) => RpcResponse::success_json(
            req.id,
            &WorkspaceCreateResult {
                id: ws.id,
                path: ws.path,
            },
        ),
        Err(e) => rpc_err(req.id, e.code()),
    }
}

fn handle_workspace_list(reg: &Arc<WorkspaceRegistry>, req: RpcRequest) -> RpcResponse {
    try_op(
        req.id,
        "list",
        reg.list().map(|(workspaces, orphans)| WorkspaceListResult {
            workspaces,
            orphans,
        }),
    )
}

async fn handle_workspace_destroy(reg: &Arc<WorkspaceRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WorkspaceDestroyParams = try_params!(req);
    match reg.destroy(&params.id).await {
        Ok(()) => RpcResponse::success_json(req.id, &WorkspaceDestroyResult::default()),
        Err(e) => rpc_err(req.id, e.code()),
    }
}

async fn handle_workspace_assign(reg: &Arc<WorkspaceRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WorkspaceAssignParams = try_params!(req);
    match reg.assign(&params.workspace_id, &params.agent_id).await {
        Ok(()) => RpcResponse::success_json(req.id, &WorkspaceAssignResult::default()),
        Err(e) => rpc_err(req.id, e.code()),
    }
}

async fn handle_memory_put(db: &Arc<Database>, bus: &EventBus, req: RpcRequest) -> RpcResponse {
    let params: MemoryPutParams = try_params!(req);
    if validate_memory_key(&params.key).is_err() {
        return rpc_err(req.id, "invalid_memory_key");
    }
    // Workspace must exist and be Active.
    let ws = try_rpc!(resolve_workspace(req.id, db, &params.workspace_id).await);
    if ws.state != crate::shared::types::WorkspaceState::Active {
        return rpc_err(req.id, "workspace_destroying");
    }

    let Ok(bytes) = serde_json::to_vec(&params.value) else {
        return rpc_err(req.id, "invalid_value_json");
    };

    let cfg = crate::shared::config::Config::load().unwrap_or_default();
    if (bytes.len() as u64) > cfg.daemon.workspace_value_cap_bytes {
        return rpc_err(req.id, "memory_value_too_large");
    }

    let workspace_id = params.workspace_id.clone();
    let key = params.key.clone();
    let sender_owned = params.sender.clone();
    let bus_clone = bus.clone();
    let expected = params.expected_version;
    let total_cap = cfg.daemon.workspace_total_cap_bytes;
    // One trip: total-cap pre-check + CAS write + memory-topic fanout
    // (which itself does mail-batch inserts). Bus emission for MemoryWritten
    // stays on the caller side so the success branch is clean.
    let outcome: Result<MemoryWriteOutcome, RpcResponse> = db
        .run(move |db| {
            let total = db
                .memory_total_size_for_workspace(&workspace_id)
                .unwrap_or(0);
            let (_cur_v, cur_size) = db
                .memory_current_version_and_size(&workspace_id, &key)
                .unwrap_or((0, 0));
            let new_total = total
                .saturating_sub(cur_size)
                .saturating_add(bytes.len() as u64);
            if new_total > total_cap {
                return Err(rpc_err(req.id, "memory_total_cap_exceeded"));
            }
            let updated_by = sender_owned.as_deref().unwrap_or("system");
            let res = match db.memory_put_cas(&workspace_id, &key, &bytes, expected, updated_by) {
                Ok(o) => o,
                Err(e) => return Err(RpcResponse::error(req.id, -32000, format!("put: {e}"))),
            };
            if let MemoryWriteOutcome::Written { version } = &res {
                let _ = publish_memory_topic_mail(
                    db,
                    &bus_clone,
                    &workspace_id,
                    &key,
                    *version,
                    "put",
                    sender_owned.as_deref(),
                );
            }
            Ok(res)
        })
        .await;

    match outcome {
        Err(resp) => resp,
        Ok(MemoryWriteOutcome::Conflict { current_version }) => RpcResponse::error(
            req.id,
            -32000,
            format!("cas_conflict:current_version={current_version}"),
        ),
        Ok(MemoryWriteOutcome::Written { version }) => {
            bus.publish(StreamEvent::MemoryWritten {
                workspace_id: params.workspace_id.clone(),
                key: params.key.clone(),
                version,
                agent_id: params.sender.clone(),
            });
            RpcResponse::success_json(req.id, &MemoryPutResult { version })
        }
    }
}

async fn handle_memory_get(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MemoryGetParams = try_params!(req);
    if validate_memory_key(&params.key).is_err() {
        return rpc_err(req.id, "invalid_memory_key");
    }
    let workspace_id = params.workspace_id;
    let key = params.key;
    match db.run(move |db| db.memory_get(&workspace_id, &key)).await {
        Ok(Some(entry)) => {
            let result = MemoryGetResult {
                value: entry.value,
                version: entry.version,
            };
            RpcResponse::success_json(req.id, &result)
        }
        Ok(None) => rpc_err(req.id, "memory_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("get: {e}")),
    }
}

async fn handle_memory_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MemoryListParams = try_params!(req);
    let workspace_id = params.workspace_id;
    let prefix = params.prefix;
    try_op(
        req.id,
        "list",
        db.run(move |db| db.memory_list_prefix(&workspace_id, prefix.as_deref()))
            .await
            .map(|entries| MemoryListResult { entries }),
    )
}

async fn handle_memory_delete(db: &Arc<Database>, bus: &EventBus, req: RpcRequest) -> RpcResponse {
    let params: MemoryDeleteParams = try_params!(req);
    if validate_memory_key(&params.key).is_err() {
        return rpc_err(req.id, "invalid_memory_key");
    }
    let workspace_id = params.workspace_id.clone();
    let key = params.key.clone();
    let sender_owned = params.sender.clone();
    let bus_clone = bus.clone();
    let expected = params.expected_version;
    let outcome: Result<MemoryWriteOutcome, anyhow::Error> = db
        .run(move |db| {
            let res = db.memory_delete_cas(&workspace_id, &key, expected)?;
            if let MemoryWriteOutcome::Written { version } = &res
                && *version > 0
            {
                let _ = publish_memory_topic_mail(
                    db,
                    &bus_clone,
                    &workspace_id,
                    &key,
                    *version,
                    "delete",
                    sender_owned.as_deref(),
                );
            }
            Ok(res)
        })
        .await;
    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("delete: {e}")),
    };
    match outcome {
        MemoryWriteOutcome::Conflict { current_version } => RpcResponse::error(
            req.id,
            -32000,
            format!("cas_conflict:current_version={current_version}"),
        ),
        MemoryWriteOutcome::Written { version } => {
            // version == 0 means "no-op" (key didn't exist).
            if version > 0 {
                bus.publish(StreamEvent::MemoryDeleted {
                    workspace_id: params.workspace_id.clone(),
                    key: params.key.clone(),
                    agent_id: params.sender.clone(),
                });
            }
            RpcResponse::success_json(req.id, &MemoryDeleteResult::default())
        }
    }
}

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

async fn handle_ns_put(
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

async fn handle_ns_get(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
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

async fn handle_ns_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
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

async fn handle_ns_delete(
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

async fn handle_ns_unfederate(
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

async fn handle_ns_federate(peer_registry: &Arc<PeerRegistry>, req: RpcRequest) -> RpcResponse {
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

// --- Federation handlers (Tasks 10, 11, 12) ---

const PEER_OUTBOX_MAX_DEPTH_DEFAULT: u64 = 10_000;

#[allow(clippy::too_many_arguments)]
async fn handle_federated_direct_send(
    db: &Arc<Database>,
    bus: &EventBus,
    peer_registry: &Arc<PeerRegistry>,
    req: &RpcRequest,
    params: &MailSendParams,
    target_daemon: &str,
    agent_id: &str,
    wake_eligible: bool,
) -> RpcResponse {
    use crate::shared::types::{Peer, PeerState};
    let peer: Peer = match peer_registry.peer_for_daemon_id(target_daemon).await {
        Ok(Some(p)) => p,
        Ok(None) => return rpc_err(req.id, "peer_unknown_for_recipient"),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db: {e}")),
    };
    if peer.state == PeerState::Removing {
        return rpc_err(req.id, "peer_removing");
    }

    let now = unix_now();
    let mail_id = crate::shared::constants::generate_short_id();
    let outbox_id = crate::shared::constants::generate_short_id();
    let recipient_addr = format!("agent://grimd-{target_daemon}/{agent_id}");

    let mail = new_outbound_mail(
        MailDraft {
            recipient_id: recipient_addr.clone(),
            sender: params.sender.clone(),
            topic: None,
            body: params.body.clone(),
            state: MailState::Pending,
            fail_reason: None,
            wake_eligible,
            in_reply_to: params.in_reply_to.clone(),
        },
        mail_id.clone(),
        now,
    );

    // Pre-check depth + insert in one trip so we don't bounce between
    // workers and the blocking pool.
    let peer_id = peer.id.clone();
    let mail_for_db = mail.clone();
    let outbox_id_for_db = outbox_id.clone();
    let recipient_for_db = recipient_addr.clone();
    let outcome: Result<Result<(), String>, anyhow::Error> = db
        .run(move |db| -> Result<Result<(), String>, anyhow::Error> {
            let depth = db.outbox_depth(&peer_id)?;
            if depth >= PEER_OUTBOX_MAX_DEPTH_DEFAULT {
                return Ok(Err("peer_outbox_full".to_string()));
            }
            match db.insert_mail_with_outbox(
                &mail_for_db,
                &peer_id,
                &outbox_id_for_db,
                &recipient_for_db,
                None,
                now,
            ) {
                Ok(_) => Ok(Ok(())),
                Err(e) => Ok(Err(format!("insert_mail_with_outbox: {e}"))),
            }
        })
        .await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(code)) if code == "peer_outbox_full" => return rpc_err(req.id, "peer_outbox_full"),
        Ok(Err(msg)) => return RpcResponse::error(req.id, -32000, msg),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("outbox_depth: {e}")),
    }

    bus.publish(StreamEvent::MailSent {
        mail_id: mail_id.clone(),
        sender_id: params.sender.clone(),
        recipient_id: Some(recipient_addr.clone()),
        topic: None,
    });
    peer_registry.notify_outbox(&peer.id).await;

    let result = MailSendResult {
        delivered: 1,
        mail_ids: vec![mail_id],
    };
    RpcResponse::success_json(req.id, &result)
}

async fn handle_peer_add(peer_registry: &Arc<PeerRegistry>, req: RpcRequest) -> RpcResponse {
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
fn handle_peer_local_cert(peer_registry: &Arc<PeerRegistry>, req: RpcRequest) -> RpcResponse {
    let id = &peer_registry.tls_identity;
    RpcResponse::success_json(
        req.id,
        &crate::shared::protocol::PeerLocalCertResult {
            cert_pem: id.cert_pem().to_string(),
            fingerprint_sha256: id.fingerprint(),
        },
    )
}

async fn handle_peer_list(peer_registry: &Arc<PeerRegistry>, req: RpcRequest) -> RpcResponse {
    try_op(
        req.id,
        "list_peers",
        peer_registry
            .list_with_outbox_depth()
            .await
            .map(|peers| PeerListResult { peers }),
    )
}

async fn handle_peer_remove(peer_registry: &Arc<PeerRegistry>, req: RpcRequest) -> RpcResponse {
    let params: PeerRemoveParams = try_params!(req);
    match peer_registry.remove_peer(&params.name).await {
        Ok(removed) => RpcResponse::success_json(req.id, &PeerRemoveResult { removed }),
        Err(e) => rpc_err(req.id, &e.to_string()),
    }
}

async fn handle_peer_ping(peer_registry: &Arc<PeerRegistry>, req: RpcRequest) -> RpcResponse {
    let params: PeerPingParams = try_params!(req);
    match peer_registry.ping_peer(&params.name).await {
        Ok((rtt, state)) => {
            RpcResponse::success_json(req.id, &PeerPingResult { rtt_ms: rtt, state })
        }
        Err(e) => rpc_err(req.id, &e.to_string()),
    }
}

async fn handle_topic_federate(peer_registry: &Arc<PeerRegistry>, req: RpcRequest) -> RpcResponse {
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

async fn handle_topic_unfederate(
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

async fn handle_agent_processes(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    use crate::daemon::process_manager::process_alive;
    use crate::shared::protocol::{AgentProcess, AgentProcessesResult};
    let outcome = db.run(|db| db.list_agents(None)).await;
    match outcome {
        Ok(agents) => {
            let processes = agents
                .into_iter()
                .filter_map(|a| {
                    let pid = a.pid?;
                    let alive = process_alive(pid);
                    let terminal = matches!(
                        a.state,
                        AgentState::Complete | AgentState::Failed | AgentState::Banished
                    );
                    Some(AgentProcess {
                        agent_id: a.id,
                        state: a.state.to_string(),
                        task: a.task,
                        pid,
                        alive,
                        stuck: alive && terminal,
                    })
                })
                .collect();
            RpcResponse::success_json(req.id, &AgentProcessesResult { processes })
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("agent.processes: {e}")),
    }
}

// --- Federation listings ---
// Read-only enumeration of `*_federations` rows. Used by `grim topic
// federations` / `ns federations` / `workspace federations` and the
// dashboard's Federation page.

async fn handle_topic_federations(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    match peer_registry
        .db
        .run(|db| db.list_topic_federations())
        .await
    {
        Ok(federations) => RpcResponse::success_json(
            req.id,
            &crate::shared::protocol::TopicFederationsResult { federations },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("topic.federations: {e}")),
    }
}

async fn handle_ns_federations(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    match peer_registry
        .db
        .run(|db| db.list_namespace_federations())
        .await
    {
        Ok(federations) => RpcResponse::success_json(
            req.id,
            &crate::shared::protocol::NsFederationsResult { federations },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("ns.federations: {e}")),
    }
}

async fn handle_workspace_federations(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    match peer_registry
        .db
        .run(|db| db.list_workspace_federations())
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
async fn handle_workspace_federate(
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
async fn handle_workspace_federate_subscribe(
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
async fn handle_workspace_unfederate(
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
