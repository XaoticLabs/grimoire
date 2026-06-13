//! Mail RPC handlers: direct/topic/federated sends, ask/tender request-reply,
//! list/ack, and topic subscription management, plus the mail-row builders and
//! event emission shared by every send path.

use std::sync::Arc;

use crate::shared::mail::{Address, body_preview, is_valid_topic_name, parse_address};
use crate::shared::protocol::*;
use crate::shared::types::{AgentState, Mail, MailState, Subscription};

use crate::daemon::event_bus::EventBus;
use crate::daemon::peer_registry::PeerRegistry;
use crate::daemon::persistence::{Database, OutboxFanoutRow, unix_now};

use super::{MAX_MAIL_BODY_BYTES, parse_params, rpc_err, try_op, try_params};

const PREVIEW_CHARS: usize = 200;

const PEER_OUTBOX_MAX_DEPTH_DEFAULT: u64 = 10_000;

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

pub(super) async fn handle_mail_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
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

pub(super) async fn handle_mail_ack(
    db: &Arc<Database>,
    bus: &EventBus,
    req: RpcRequest,
) -> RpcResponse {
    let params: MailAckParams = try_params!(req);

    let mail_id = params.mail_id.clone();
    // Lookup + state mutation in one trip; tail handles event emission.
    // Accepts short prefixes; ambiguous prefix surfaces as `ambiguous_mail_prefix`.
    let outcome: Result<Result<Option<Mail>, anyhow::Error>, anyhow::Error> = db
        .run(
            move |db| -> Result<Result<Option<Mail>, anyhow::Error>, anyhow::Error> {
                let mail = match db.get_mail_by_prefix(&mail_id) {
                    Ok(Some(m)) => m,
                    Ok(None) => return Ok(Ok(None)),
                    Err(e) => return Ok(Err(e)),
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
        Ok(Err(e)) => {
            let msg = e.to_string();
            if msg.starts_with("Ambiguous mail prefix") {
                return rpc_err(req.id, "ambiguous_mail_prefix");
            }
            return RpcResponse::error(req.id, -32000, format!("set_state: {e}"));
        }
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

pub(super) async fn handle_mail_subscribe(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
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

pub(super) async fn handle_mail_unsubscribe(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MailUnsubscribeParams = try_params!(req);
    let id = params.subscription_id;
    match db.run(move |db| db.delete_subscription(&id)).await {
        Ok(true) => RpcResponse::success_json(req.id, &MailUnsubscribeResult::default()),
        Ok(false) => rpc_err(req.id, "subscription_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("delete_subscription: {e}")),
    }
}

pub(super) async fn handle_mail_topics(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
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
