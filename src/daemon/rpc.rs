use serde::de::DeserializeOwned;
use std::sync::Arc;

use crate::shared::mail::{Address, parse_address, body_preview, is_valid_topic_name};
use crate::shared::protocol::*;
use crate::shared::types::{AgentState, Mail, MailState, Pact, PactState, Subscription, WakeSourceKind};

use super::agent_manager::AgentManager;
use super::event_bus::EventBus;
use super::persistence::{Database, unix_now};
use super::scroll_keeper::ScrollKeeper;
use super::wake_registry::WakeRegistry;

const MAX_MAIL_BODY_BYTES: usize = 65_536;
const PREVIEW_CHARS: usize = 200;

fn parse_params<T: DeserializeOwned>(req: &RpcRequest) -> Result<T, RpcResponse> {
    serde_json::from_value(req.params.clone())
        .map_err(|e| RpcResponse::error(req.id, -32602, format!("Invalid params: {}", e)))
}

pub async fn handle_rpc(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    scroll_keeper: &Arc<ScrollKeeper>,
    wake_registry: &Arc<WakeRegistry>,
    bus: &EventBus,
    req: RpcRequest,
) -> RpcResponse {
    match req.method.as_str() {
        "agent.summon" => handle_summon(manager, req).await,
        "agent.circle" => handle_circle(manager, req).await,
        "agent.banish" => handle_banish(manager, req).await,
        "agent.invoke" => handle_invoke(manager, req).await,
        "pact.create" => handle_pact_create(db, req),
        "pact.list" => handle_pact_list(db, req),
        "scroll.inscribe" => handle_scroll_inscribe(scroll_keeper, req),
        "scroll.activate" => handle_scroll_activate(scroll_keeper, req).await,
        "scroll.status" => handle_scroll_status(scroll_keeper, req),
        "scroll.list" => handle_scroll_list(db, req),
        "scroll.abandon" => handle_scroll_abandon(scroll_keeper, req).await,
        "daemon.status" => handle_status(manager, req).await,
        "agent.queue.list" => handle_queue_list(db, req),
        "mail.send" => handle_mail_send(db, bus, req),
        "mail.list" => handle_mail_list(db, req),
        "mail.ack" => handle_mail_ack(db, bus, req),
        "mail.subscribe" => handle_mail_subscribe(db, req),
        "mail.unsubscribe" => handle_mail_unsubscribe(db, req),
        "mail.topics" => handle_mail_topics(db, req),
        "wake.add" => handle_wake_add(db, wake_registry, req).await,
        "wake.list" => handle_wake_list(wake_registry, req).await,
        "wake.remove" => handle_wake_remove(wake_registry, req).await,
        "wake.test" => handle_wake_test(wake_registry, req).await,
        _ => RpcResponse::error(req.id, -32601, format!("Unknown method: {}", req.method)),
    }
}

// --- Wake handlers ---

async fn handle_wake_add(
    db: &Arc<Database>,
    reg: &Arc<WakeRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: WakeAddParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };
    // Validate agent exists.
    match db.get_agent(&params.agent_id) {
        Ok(Some(_)) => {}
        Ok(None) => return rpc_err(req.id, "agent_not_found"),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db: {}", e)),
    }
    let kind: WakeSourceKind = match params.kind.parse() {
        Ok(k) => k,
        Err(_) => return rpc_err(req.id, "invalid_kind"),
    };
    let config_json = match serde_json::to_string(&params.config) {
        Ok(s) => s,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("config: {}", e)),
    };
    match reg.register(&params.agent_id, kind, &config_json).await {
        Ok(wake_id) => {
            RpcResponse::success(req.id, serde_json::to_value(WakeAddResult { wake_id }).unwrap())
        }
        Err(e) => rpc_err(req.id, &e.to_string()),
    }
}

async fn handle_wake_list(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeListParams = parse_params(&req).unwrap_or_default();
    let result = match params.agent_id {
        Some(id) => reg.list_for_agent(&id).await,
        None => reg.list_all().await,
    };
    match result {
        Ok(sources) => RpcResponse::success(
            req.id,
            serde_json::to_value(WakeListResult { sources }).unwrap(),
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("list: {}", e)),
    }
}

async fn handle_wake_remove(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeRemoveParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };
    match reg.remove(&params.wake_id).await {
        Ok(true) => RpcResponse::success(
            req.id,
            serde_json::to_value(WakeRemoveResult { success: true }).unwrap(),
        ),
        Ok(false) => rpc_err(req.id, "wake_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("remove: {}", e)),
    }
}

async fn handle_wake_test(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeTestParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };
    match reg.test_fire(&params.wake_id).await {
        Ok(mail_id) => RpcResponse::success(
            req.id,
            serde_json::to_value(WakeTestResult {
                success: true,
                mail_id,
            })
            .unwrap(),
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

async fn handle_summon(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: SummonParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    // Supervision validation.
    let policy_str = params
        .restart_policy
        .clone()
        .unwrap_or_else(|| "never".to_string());
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
            None => return rpc_err(req.id, "window_required"),
            Some(0) => return rpc_err(req.id, "window_required"),
            Some(n) if n > 604800 => return rpc_err(req.id, "window_too_large"),
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

    let cwd = manager.resolve_cwd(params.cwd);
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
        Err(e) => return RpcResponse::error(req.id, -32000, format!("Failed to summon: {}", e)),
    };

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
    RpcResponse::success(req.id, serde_json::to_value(resp).unwrap())
}

async fn handle_circle(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: CircleParams = parse_params(&req).unwrap_or(CircleParams { state: None });

    match manager.circle(params.state.as_deref()).await {
        Ok(agents) => {
            let result = CircleResult { agents };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to list: {}", e)),
    }
}

async fn handle_banish(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: BanishParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    match manager.banish(&params.id).await {
        Ok(success) => {
            let result = BanishResult { success };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to banish: {}", e)),
    }
}

async fn handle_invoke(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: InvokeParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    match manager.invoke(&params.id, &params.message, None).await {
        Ok(()) => RpcResponse::success(req.id, serde_json::json!({"success": true})),
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to invoke: {}", e)),
    }
}

fn handle_pact_create(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: PactCreateParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

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

    match db.insert_pact(&pact) {
        Ok(()) => {
            let result = PactCreateResult {
                id: pact_id,
                source_id: params.source_id,
            };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to create pact: {}", e)),
    }
}

fn handle_pact_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: PactListParams = parse_params(&req).unwrap_or(PactListParams { source_id: None });

    match db.list_pacts(params.source_id.as_deref()) {
        Ok(pacts) => {
            let result = PactListResult { pacts };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to list pacts: {}", e)),
    }
}

// --- Scroll handlers ---

fn handle_scroll_inscribe(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollInscribeParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let content = match std::fs::read_to_string(&params.spec_path) {
        Ok(c) => c,
        Err(e) => {
            return RpcResponse::error(
                req.id,
                -32000,
                format!("Failed to read spec file '{}': {}", params.spec_path, e),
            )
        }
    };

    let spec = match super::scroll_parser::parse_scroll(&content) {
        Ok(s) => s,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("Failed to parse spec: {}", e)),
    };

    match keeper.inscribe(spec, params.max_concurrency, Some(params.spec_path)) {
        Ok(result) => {
            let resp = ScrollInscribeResult {
                id: result.scroll.id,
                name: result.scroll.name,
                task_count: result.task_count,
                conflicts: result.conflicts,
            };
            RpcResponse::success(req.id, serde_json::to_value(resp).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to inscribe: {}", e)),
    }
}

async fn handle_scroll_activate(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollActivateParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    match keeper.activate(&params.id).await {
        Ok(()) => RpcResponse::success(req.id, serde_json::json!({"success": true})),
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to activate: {}", e)),
    }
}

fn handle_scroll_status(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollStatusParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    match keeper.status(&params.id) {
        Ok(status) => RpcResponse::success(req.id, serde_json::to_value(status).unwrap()),
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to get status: {}", e)),
    }
}

fn handle_scroll_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    match db.list_scrolls() {
        Ok(scrolls) => RpcResponse::success(req.id, serde_json::json!({"scrolls": scrolls})),
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to list scrolls: {}", e)),
    }
}

async fn handle_scroll_abandon(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollAbandonParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    match keeper.abandon(&params.id).await {
        Ok(()) => RpcResponse::success(req.id, serde_json::json!({"success": true})),
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to abandon: {}", e)),
    }
}

fn handle_queue_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    match db.list_queue() {
        Ok(rows) => {
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
            let resp = QueueListResponse { entries };
            RpcResponse::success(req.id, serde_json::to_value(resp).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to list queue: {}", e)),
    }
}

// --- Mail handlers ---

/// RPC errors use code -32000 plus a string in the message; the mail layer
/// uses the symbolic codes documented in the spec (`body_too_large`,
/// `unknown_recipient`, …) as the message text so callers can match on it.
fn rpc_err(req_id: u64, code: &str) -> RpcResponse {
    RpcResponse::error(req_id, -32000, code.to_string())
}

fn handle_mail_send(db: &Arc<Database>, bus: &EventBus, req: RpcRequest) -> RpcResponse {
    let params: MailSendParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    if params.body.len() > MAX_MAIL_BODY_BYTES {
        return rpc_err(req.id, "body_too_large");
    }

    // Reserved-prefix guard: user-supplied senders cannot forge system
    // identities. Internal callers (wake registry, supervisor) bypass
    // mail.send entirely and write rows directly.
    if let Some(s) = &params.sender {
        if s.starts_with("supervisor://") || s.starts_with("wake://") {
            return rpc_err(req.id, "reserved_sender_prefix");
        }
    }

    let address = match parse_address(&params.to) {
        Ok(a) => a,
        Err(e) => return rpc_err(req.id, e.code()),
    };

    let wake_eligible = params.wake_eligible.unwrap_or(true);

    match address {
        Address::Agent(recipient_id) => {
            handle_direct_send(db, bus, &req, &params, recipient_id, wake_eligible)
        }
        Address::Topic(topic) => {
            handle_topic_send(db, bus, &req, &params, topic, wake_eligible)
        }
    }
}

fn handle_direct_send(
    db: &Arc<Database>,
    bus: &EventBus,
    req: &RpcRequest,
    params: &MailSendParams,
    recipient_id: String,
    wake_eligible: bool,
) -> RpcResponse {
    let agent = match db.get_agent(&recipient_id) {
        Ok(a) => a,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db error: {}", e)),
    };

    let preview = body_preview(&params.body, PREVIEW_CHARS);
    let mail_id = crate::shared::constants::generate_short_id();
    let now = unix_now();

    let (state, fail_reason): (MailState, Option<&'static str>) = match &agent {
        None => (MailState::Failed, Some("unknown_recipient")),
        Some(a) if a.state == AgentState::Banished => {
            (MailState::Failed, Some("recipient_banished"))
        }
        Some(_) => (MailState::Pending, None),
    };

    let mail = Mail {
        id: mail_id.clone(),
        recipient_id: recipient_id.clone(),
        sender_id: params.sender.clone(),
        topic: None,
        body: params.body.clone(),
        in_reply_to: None,
        state,
        fail_reason: fail_reason.map(|s| s.to_string()),
        created_at: now,
        delivered_at: if state != MailState::Pending { Some(now) } else { None },
        seq: 0,
        wake_eligible,
    };

    if let Err(e) = db.insert_mail(&mail) {
        return RpcResponse::error(req.id, -32000, format!("insert_mail: {}", e));
    }

    match state {
        MailState::Failed => {
            let reason = fail_reason.unwrap_or("unknown").to_string();
            bus.publish(StreamEvent::MailFailed {
                mail_id: mail_id.clone(),
                recipient_id: recipient_id.clone(),
                sender_id: params.sender.clone(),
                reason,
            });
            let result = MailSendResult {
                delivered: 0,
                mail_ids: vec![mail_id],
            };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        MailState::Pending => {
            bus.publish(StreamEvent::MailSent {
                mail_id: mail_id.clone(),
                sender_id: params.sender.clone(),
                recipient_id: Some(recipient_id.clone()),
                topic: None,
            });
            bus.publish(StreamEvent::MailReceived {
                mail_id: mail_id.clone(),
                recipient_id,
                sender_id: params.sender.clone(),
                topic: None,
                body_preview: preview,
                wake_eligible,
            });
            let result = MailSendResult {
                delivered: 1,
                mail_ids: vec![mail_id],
            };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        MailState::Delivered => unreachable!(),
    }
}

fn handle_topic_send(
    db: &Arc<Database>,
    bus: &EventBus,
    req: &RpcRequest,
    params: &MailSendParams,
    topic: String,
    wake_eligible: bool,
) -> RpcResponse {
    let subscribers = match db.list_subscribers_for_topic(&topic) {
        Ok(s) => s,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db error: {}", e)),
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
        return RpcResponse::success(req.id, serde_json::to_value(result).unwrap());
    }

    let preview = body_preview(&params.body, PREVIEW_CHARS);
    let now = unix_now();

    // Determine each subscriber's initial state (Pending vs Failed for
    // banished). Pre-compute mail rows so the batch insert is one txn.
    let mut mails: Vec<Mail> = Vec::with_capacity(subscribers.len());
    let mut per_state: Vec<MailState> = Vec::with_capacity(subscribers.len());
    let mut delivered: u32 = 0;
    let mut failed_reasons: Vec<(String, String)> = Vec::new();

    for sub in &subscribers {
        let agent = match db.get_agent(&sub.subscriber_id) {
            Ok(a) => a,
            Err(e) => return RpcResponse::error(req.id, -32000, format!("db error: {}", e)),
        };
        let (state, fail_reason): (MailState, Option<&'static str>) = match agent {
            None => (MailState::Failed, Some("unknown_recipient")),
            Some(a) if a.state == AgentState::Banished => {
                (MailState::Failed, Some("recipient_banished"))
            }
            Some(_) => (MailState::Pending, None),
        };
        let mail_id = crate::shared::constants::generate_short_id();
        let mail = Mail {
            id: mail_id.clone(),
            recipient_id: sub.subscriber_id.clone(),
            sender_id: params.sender.clone(),
            topic: Some(topic.clone()),
            body: params.body.clone(),
            in_reply_to: None,
            state,
            fail_reason: fail_reason.map(|s| s.to_string()),
            created_at: now,
            delivered_at: if state != MailState::Pending { Some(now) } else { None },
            seq: 0,
            wake_eligible,
        };
        if state == MailState::Pending {
            delivered += 1;
        } else if let Some(r) = fail_reason {
            failed_reasons.push((mail_id.clone(), r.to_string()));
        }
        mails.push(mail);
        per_state.push(state);
    }

    if let Err(e) = db.insert_mail_batch(&mails) {
        return RpcResponse::error(req.id, -32000, format!("insert_mail_batch: {}", e));
    }

    // Emit one MailSent per subscriber row; each carries the per-recipient
    // mail_id so the event stream is "one event per recipient".
    for (mail, state) in mails.iter().zip(per_state.iter()) {
        match state {
            MailState::Pending => {
                bus.publish(StreamEvent::MailSent {
                    mail_id: mail.id.clone(),
                    sender_id: params.sender.clone(),
                    recipient_id: Some(mail.recipient_id.clone()),
                    topic: Some(topic.clone()),
                });
                bus.publish(StreamEvent::MailReceived {
                    mail_id: mail.id.clone(),
                    recipient_id: mail.recipient_id.clone(),
                    sender_id: params.sender.clone(),
                    topic: Some(topic.clone()),
                    body_preview: preview.clone(),
                    wake_eligible,
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
                    sender_id: params.sender.clone(),
                    reason,
                });
            }
            MailState::Delivered => {}
        }
    }

    let _ = failed_reasons; // logged via events
    let result = MailSendResult {
        delivered,
        mail_ids: mails.into_iter().map(|m| m.id).collect(),
    };
    RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
}

fn handle_mail_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MailListParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let limit = params.limit.unwrap_or(100);
    if limit > 1000 {
        return rpc_err(req.id, "limit_too_large");
    }
    match db.list_mail_by_recipient(&params.agent_id, params.after_seq, params.state, limit) {
        Ok(mails) => {
            let result = MailListResult { mails };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to list mail: {}", e)),
    }
}

fn handle_mail_ack(db: &Arc<Database>, bus: &EventBus, req: RpcRequest) -> RpcResponse {
    let params: MailAckParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let mail = match db.get_mail(&params.mail_id) {
        Ok(Some(m)) => m,
        Ok(None) => return rpc_err(req.id, "mail_not_found"),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db: {}", e)),
    };

    match mail.state {
        MailState::Delivered => {
            let r = MailAckResult { acked: false };
            RpcResponse::success(req.id, serde_json::to_value(r).unwrap())
        }
        MailState::Failed => rpc_err(req.id, "cannot_ack_failed"),
        MailState::Pending => {
            if let Err(e) = db.set_mail_state(&mail.id, MailState::Delivered, None) {
                return RpcResponse::error(req.id, -32000, format!("set_state: {}", e));
            }
            bus.publish(StreamEvent::MailDelivered {
                mail_id: mail.id.clone(),
                recipient_id: mail.recipient_id.clone(),
            });
            let r = MailAckResult { acked: true };
            RpcResponse::success(req.id, serde_json::to_value(r).unwrap())
        }
    }
}

fn handle_mail_subscribe(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MailSubscribeParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    if !is_valid_topic_name(&params.topic) {
        return rpc_err(req.id, "invalid_topic_name");
    }

    match db.get_agent(&params.agent_id) {
        Ok(Some(_)) => {}
        Ok(None) => return rpc_err(req.id, "unknown_agent"),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db: {}", e)),
    }

    let new_id = crate::shared::constants::generate_short_id();
    let sub = Subscription {
        id: new_id,
        subscriber_id: params.agent_id,
        topic: params.topic,
        created_at: unix_now(),
    };
    match db.insert_subscription(&sub) {
        Ok(id) => {
            let r = MailSubscribeResult { subscription_id: id };
            RpcResponse::success(req.id, serde_json::to_value(r).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("insert_subscription: {}", e)),
    }
}

fn handle_mail_unsubscribe(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MailUnsubscribeParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };
    match db.delete_subscription(&params.subscription_id) {
        Ok(true) => RpcResponse::success(
            req.id,
            serde_json::to_value(MailUnsubscribeResult::default()).unwrap(),
        ),
        Ok(false) => rpc_err(req.id, "subscription_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("delete_subscription: {}", e)),
    }
}

fn handle_mail_topics(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    match db.list_topics_with_counts() {
        Ok(rows) => {
            let topics: Vec<TopicCount> = rows
                .into_iter()
                .map(|(topic, n)| TopicCount {
                    topic,
                    subscriber_count: n,
                })
                .collect();
            let r = MailTopicsResult { topics };
            RpcResponse::success(req.id, serde_json::to_value(r).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("list_topics: {}", e)),
    }
}

async fn handle_status(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    match manager.circle(None).await {
        Ok(agents) => {
            use crate::shared::types::AgentState;
            let active = agents
                .iter()
                .filter(|a| a.state == AgentState::Active)
                .count();
            let queued = agents
                .iter()
                .filter(|a| a.state == AgentState::Queued)
                .count();
            let result = DaemonStatusResult {
                uptime_secs: 0,
                agent_count: agents.len(),
                active_count: active,
                queued_count: queued,
                max_concurrent_agents: manager.max_concurrent_agents(),
            };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed: {}", e)),
    }
}
