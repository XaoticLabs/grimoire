//! The HTTP request handlers behind the router: REST CRUD over agents and
//! scrolls, the SSE event streams, the generic JSON-RPC bridge, inbound
//! webhooks, the Prometheus metrics endpoint, and the dashboard HTML.

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use std::convert::Infallible;

use crate::shared::protocol::*;

use super::super::rpc;
use super::AppState;
use super::http::{json_ok, json_result};

pub(super) async fn http_list_agents(
    State(state): State<AppState>,
) -> axum::Json<serde_json::Value> {
    json_result(
        state
            .manager
            .circle(None)
            .await
            .map(|agents| CircleResult { agents }),
    )
}

pub(super) async fn http_summon_agent(
    State(state): State<AppState>,
    axum::Json(params): axum::Json<SummonParams>,
) -> axum::Json<serde_json::Value> {
    let cwd = state.manager.resolve_cwd(params.cwd);
    let _ = params.restart_policy;
    let _ = params.max_restarts;
    let _ = params.restart_window_secs;
    let _ = params.escalate_to;
    json_result(
        state
            .manager
            .enqueue(
                &params.task,
                params.name,
                params.model,
                params.provider,
                &cwd,
                crate::daemon::agent_manager::Lane::Adhoc,
            )
            .await
            .map(|agent| SummonResult {
                id: agent.id,
                name: agent.name,
                state: agent.state.as_str().to_string(),
            }),
    )
}

pub(super) async fn http_get_agent(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    match state.manager.get_agent(&id).await {
        Ok(Some(agent)) => json_ok(agent),
        Ok(None) => axum::Json(serde_json::json!({"error": "Agent not found"})),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

pub(super) async fn http_banish_agent(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    json_result(
        state
            .manager
            .banish(&id)
            .await
            .map(|success| BanishResult { success }),
    )
}

pub(super) async fn http_invoke_agent(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    let message = body
        .get("message")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();

    if message.is_empty() {
        return axum::Json(serde_json::json!({"error": "message is required"}));
    }

    json_result(
        state
            .manager
            .invoke(&id, &message, None)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

pub(super) async fn http_agent_events_sse(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.manager.subscribe();
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if event.agent_id() == Some(id.as_str())
                        && let Ok(json) = serde_json::to_string(&event)
                    {
                        yield Ok(Event::default().data(json));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

pub(super) async fn http_all_events_sse(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.manager.subscribe();
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if let Ok(json) = serde_json::to_string(&event) {
                        yield Ok(Event::default().data(json));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

pub(super) async fn http_agent_history(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    json_result(state.manager.get_events(&id, None))
}

pub(super) async fn http_list_scrolls(
    State(state): State<AppState>,
) -> axum::Json<serde_json::Value> {
    json_result(
        state
            .db
            .run(super::super::persistence::Database::list_scrolls)
            .await
            .map(|scrolls| serde_json::json!({"scrolls": scrolls})),
    )
}

pub(super) async fn http_inscribe_scroll(
    State(state): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    let spec_path = match body.get("spec_path").and_then(|v| v.as_str()) {
        Some(p) => p.to_string(),
        None => return axum::Json(serde_json::json!({"error": "spec_path is required"})),
    };
    let max_concurrency = body
        .get("max_concurrency")
        .and_then(serde_json::Value::as_u64)
        .map(|v| v as u32);

    let content = match tokio::fs::read_to_string(&spec_path).await {
        Ok(c) => c,
        Err(e) => {
            return axum::Json(serde_json::json!({"error": format!("Failed to read spec: {}", e)}));
        }
    };

    let spec = match super::super::scroll_parser::parse_scroll(&content) {
        Ok(s) => s,
        Err(e) => return axum::Json(serde_json::json!({"error": format!("Parse error: {}", e)})),
    };

    json_result(
        state
            .scroll_keeper
            .inscribe(spec, max_concurrency, Some(spec_path))
            .map(|result| {
                serde_json::json!({
                    "id": result.scroll.id,
                    "name": result.scroll.name,
                    "task_count": result.task_count,
                    "conflicts": result.conflicts,
                })
            }),
    )
}

pub(super) async fn http_scroll_status(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    json_result(state.scroll_keeper.status(&id))
}

pub(super) async fn http_activate_scroll(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    json_result(
        state
            .scroll_keeper
            .activate(&id)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

pub(super) async fn http_abandon_scroll(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    json_result(
        state
            .scroll_keeper
            .abandon(&id)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

/// Generic JSON-RPC bridge for the dashboard. Forwards `{method, params}` to
/// the same `handle_rpc` dispatcher the UDS server uses, so every CLI-equivalent
/// method becomes reachable from the browser without bespoke per-resource HTTP
/// handlers. Bearer-auth-gated like the rest of `/api/*`.
pub(super) async fn http_rpc_bridge(
    State(state): State<AppState>,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> axum::Json<serde_json::Value> {
    let method = match body.get("method").and_then(|m| m.as_str()) {
        Some(m) => m.to_string(),
        None => return axum::Json(serde_json::json!({"error": "method is required"})),
    };
    let params = body
        .get("params")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let id = body
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);

    // `webhook.list` is HTTP-only: webhook config lives in this server's
    // `AppState`, not the UDS dispatcher. Short-circuit before handing
    // off to handle_rpc.
    if method == "webhook.list" {
        let entries: Vec<_> = state
            .webhooks
            .iter()
            .map(|(name, cfg)| {
                serde_json::json!({
                    "name": name,
                    "topic": cfg.topic,
                    "recipient": cfg.recipient,
                    "auth": cfg.secret.is_some(),
                })
            })
            .collect();
        return axum::Json(serde_json::json!({
            "id": id,
            "result": { "webhooks": entries },
        }));
    }

    let req = RpcRequest {
        method,
        params,
        id,
        protocol_version: None,
        auth_token: None,
    };
    let bus = state.manager.event_bus();
    let resp = rpc::handle_rpc(
        &state.manager,
        &state.db,
        &state.scroll_keeper,
        &state.wake_registry,
        &state.workspace_registry,
        &state.peer_registry,
        &bus,
        &state.daemon_id,
        req,
    )
    .await;
    match serde_json::to_value(&resp) {
        Ok(v) => axum::Json(v),
        Err(e) => axum::Json(serde_json::json!({"error": format!("serialize: {e}")})),
    }
}

/// Inbound webhook endpoint. The raw request body becomes the mail body;
/// no provider-specific decoding here, since subscriber agents know the shape
/// of whatever they signed up for. Goes through the same `mail.send` path
/// every other mail does, so subscriber wake-on-mail Just Works.
pub(super) async fn http_webhook(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use super::super::webhook::{WEBHOOK_TOKEN_HEADER, WebhookAuth, check_token, resolve_target};

    let Some(cfg) = state.webhooks.get(&name) else {
        return webhook_err(axum::http::StatusCode::NOT_FOUND, "unknown_webhook");
    };

    let presented = headers
        .get(WEBHOOK_TOKEN_HEADER)
        .and_then(|h| h.to_str().ok());
    match check_token(presented, cfg.secret.as_deref()) {
        WebhookAuth::Open | WebhookAuth::Match => {}
        WebhookAuth::Missing => {
            return webhook_err(axum::http::StatusCode::UNAUTHORIZED, "missing_token");
        }
        WebhookAuth::Mismatch => {
            return webhook_err(axum::http::StatusCode::UNAUTHORIZED, "bad_token");
        }
    }

    let to = match resolve_target(cfg) {
        Ok(addr) => addr,
        Err(code) => return webhook_err(axum::http::StatusCode::INTERNAL_SERVER_ERROR, code),
    };

    if body.len() > super::super::rpc::MAX_MAIL_BODY_BYTES {
        return webhook_err(axum::http::StatusCode::PAYLOAD_TOO_LARGE, "body_too_large");
    }

    // Bodies are passed through verbatim. UTF-8 is required (mail body is a
    // String); binary webhook payloads aren't a use case for v1. Operators
    // can base64 ahead of the daemon if they need one.
    let Ok(body_str) = String::from_utf8(body.to_vec()) else {
        return webhook_err(axum::http::StatusCode::BAD_REQUEST, "body_not_utf8");
    };

    // Reuse the canonical mail-send path so the topic fan-out, federation
    // routing, body-size guard, and StreamEvent emission all stay
    // single-sourced. The synthetic RpcRequest is the price for not
    // refactoring the handler. The id is 0 because we throw it away.
    let req = crate::shared::protocol::RpcRequest {
        method: "mail.send".to_string(),
        params: serde_json::json!({
            "to": to,
            "body": body_str,
            // Sender stays None: a `webhook://` prefix would be the audit-friendly
            // choice but is currently rejected by the reserved-prefix guard.
            // Operators trace via the webhook name in logs + the topic in mail.
            "sender": null,
            "wake_eligible": true,
        }),
        id: 0,
        protocol_version: None,
        auth_token: None,
    };
    let bus = state.manager.event_bus();
    let resp = super::super::rpc::handle_mail_send(
        &state.db,
        &bus,
        &state.peer_registry,
        &state.daemon_id,
        req,
    )
    .await;

    if let Some(err) = resp.error {
        // Map the symbolic mail-layer code to an HTTP status. Anything we
        // don't recognize gets a 500 so the operator sees the message.
        let status = match err.message.as_str() {
            "body_too_large" => axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            "unknown_recipient" | "unknown_topic" => axum::http::StatusCode::NOT_FOUND,
            _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        };
        return webhook_err(status, &err.message);
    }

    let payload = resp.result.unwrap_or_else(|| serde_json::json!({}));
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(payload.to_string()))
        .unwrap_or_else(|_| axum::response::Response::new(axum::body::Body::from("{}")))
}

fn webhook_err(status: axum::http::StatusCode, code: &str) -> axum::response::Response {
    let body = serde_json::json!({ "error": code }).to_string();
    axum::response::Response::builder()
        .status(status)
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| axum::response::Response::new(axum::body::Body::from("{}")))
}

/// Prometheus text-exposition endpoint. Behind the same bearer-auth wall as
/// `/api/*`: within the daemon's trust boundary a scrape needs the token,
/// which keeps the metrics surface out of the casual-browser attack surface
/// without making metrics second-class.
pub(super) async fn http_metrics(State(state): State<AppState>) -> axum::response::Response {
    let db = state.db.clone();
    let started_at = state.started_at;
    let body = db
        .run(move |db| super::super::metrics::render(db, started_at, env!("CARGO_PKG_VERSION")))
        .await;
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        // Prometheus expects this exact content-type for parsers to attach.
        .header(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| {
            axum::response::Response::new(axum::body::Body::from("metrics encode failure"))
        })
}

pub(super) async fn http_dashboard() -> axum::response::Html<String> {
    axum::response::Html(include_str!("../dashboard.html").to_string())
}
