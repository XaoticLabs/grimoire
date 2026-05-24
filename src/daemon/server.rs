use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, post};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tracing::{error, info, warn};

use crate::shared::auth::AuthToken;
use crate::shared::constants;
use crate::shared::protocol::*;

use super::agent_manager::AgentManager;
use super::rpc;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<AgentManager>,
    pub db: Arc<super::persistence::Database>,
    pub scroll_keeper: Arc<super::scroll_keeper::ScrollKeeper>,
    pub wake_registry: Arc<super::wake_registry::WakeRegistry>,
    pub workspace_registry: Arc<super::workspace_registry::WorkspaceRegistry>,
    pub supervisor: Arc<super::supervisor::Supervisor>,
    pub peer_registry: Arc<super::peer_registry::PeerRegistry>,
    pub daemon_id: String,
    pub auth_token: Arc<AuthToken>,
    /// Captured at server startup. Drives the `grimoire_uptime_seconds`
    /// metric; an `Instant` (not a wall-clock time) so clock skew doesn't
    /// confuse the value across daemon restarts on the same host.
    pub started_at: std::time::Instant,
    /// Inbound webhook configuration keyed by `name` (the URL segment in
    /// `POST /webhooks/<name>`). Empty map = the webhook surface is closed.
    pub webhooks: Arc<std::collections::HashMap<String, crate::shared::config::WebhookConfig>>,
}

/// UID the daemon process is running as. Cached at boot; used by the UDS
/// peer-credentials check to decide whether a connecting client is the
/// owning user (trusted, no token required) or a different UID (must
/// present a valid bearer token on the first RPC).
#[cfg(unix)]
fn daemon_uid() -> u32 {
    // Safety: getuid is always safe; the libc wrapper returns the value.
    nix::unistd::Uid::current().as_raw()
}

#[cfg(not(unix))]
fn daemon_uid() -> u32 {
    0
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::implicit_hasher)]
pub async fn run(
    manager: Arc<AgentManager>,
    db: Arc<super::persistence::Database>,
    scroll_keeper: Arc<super::scroll_keeper::ScrollKeeper>,
    wake_registry: Arc<super::wake_registry::WakeRegistry>,
    workspace_registry: Arc<super::workspace_registry::WorkspaceRegistry>,
    supervisor: Arc<super::supervisor::Supervisor>,
    peer_registry: Arc<super::peer_registry::PeerRegistry>,
    daemon_id: String,
    auth_token: Arc<AuthToken>,
    webhooks: Arc<std::collections::HashMap<String, crate::shared::config::WebhookConfig>>,
) -> Result<()> {
    let state = AppState {
        manager: manager.clone(),
        db,
        scroll_keeper,
        wake_registry,
        workspace_registry,
        supervisor,
        peer_registry,
        daemon_id,
        auth_token,
        started_at: std::time::Instant::now(),
        webhooks,
    };

    let uds_state = state.clone();
    let uds_handle = tokio::spawn(async move {
        if let Err(e) = run_uds_server(uds_state).await {
            error!("UDS server error: {}", e);
        }
    });

    let http_handle = tokio::spawn(async move {
        if let Err(e) = run_http_server(state).await {
            error!("HTTP server error: {}", e);
        }
    });

    tokio::select! {
        _ = uds_handle => {},
        _ = http_handle => {},
    }

    Ok(())
}

async fn run_uds_server(state: AppState) -> Result<()> {
    let socket_path = constants::socket_path();

    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    // Lock the socket file down to the owning user. Combined with the
    // per-connection peercred check below this gives belt-and-braces
    // protection: other UIDs can't open the socket *and* couldn't pass
    // the in-band check if they did.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) =
            std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))
        {
            warn!(error = %e, path = %socket_path.display(), "failed to set socket permissions");
        }
    }
    let owner_uid = daemon_uid();
    info!(path = %socket_path.display(), uid = owner_uid, "UDS server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();

        // Determine whether this connection is from the owning user. If so
        // the kernel-supplied peer credentials substitute for a bearer
        // token; otherwise the first RPC must carry a valid `auth_token`.
        let peercred_trusted = match stream.peer_cred() {
            Ok(cred) => cred.uid() == owner_uid,
            Err(e) => {
                warn!(error = %e, "could not read SO_PEERCRED; falling back to token auth");
                false
            }
        };

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let reader = BufReader::new(reader);
            let mut lines = reader.lines();
            // Per-connection auth state. Cached so the token check only
            // runs once per connection, not per RPC.
            let mut authed = peercred_trusted;

            while let Ok(Some(line)) = lines.next_line().await {
                let req: RpcRequest = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = RpcResponse::error(0, -32700, format!("Parse error: {e}"));
                        let _ = write_response(&mut writer, &err).await;
                        continue;
                    }
                };

                // Auth gate. Trusted peercred connections skip this; others
                // must present a matching token on the first RPC. Once the
                // token has matched, the connection is sticky-authed for
                // the remainder of its lifetime (no re-check per RPC).
                match check_uds_auth(authed, req.auth_token.as_deref(), &state.auth_token) {
                    UdsAuthDecision::Pass => {
                        authed = true;
                    }
                    UdsAuthDecision::Reject => {
                        let err = RpcResponse::error(req.id, -32000, "unauthenticated".to_string());
                        let _ = write_response(&mut writer, &err).await;
                        // Close the connection on failed auth — repeated
                        // attempts on the same socket would just be a
                        // (very slow) brute-force vector.
                        return;
                    }
                }

                if req.method == "agent.bind" {
                    let params: BindParams = match serde_json::from_value(req.params.clone()) {
                        Ok(p) => p,
                        Err(e) => {
                            let err =
                                RpcResponse::error(req.id, -32602, format!("Invalid params: {e}"));
                            let _ = write_response(&mut writer, &err).await;
                            continue;
                        }
                    };

                    if let Ok(events) = state.manager.get_events(&params.id, params.tail) {
                        for event in events {
                            let stream_event = StreamEvent::AgentEvent { event };
                            let Ok(json) = serde_json::to_string(&stream_event) else {
                                continue;
                            };
                            if writer.write_all(json.as_bytes()).await.is_err() {
                                return;
                            }
                            if writer.write_all(b"\n").await.is_err() {
                                return;
                            }
                        }
                    }

                    let mut rx = state.manager.subscribe();
                    loop {
                        match rx.recv().await {
                            Ok(event) => {
                                if event.agent_id() == Some(params.id.as_str()) {
                                    let Ok(json) = serde_json::to_string(&event) else {
                                        continue;
                                    };
                                    if writer.write_all(json.as_bytes()).await.is_err() {
                                        return;
                                    }
                                    if writer.write_all(b"\n").await.is_err() {
                                        return;
                                    }
                                    let _ = writer.flush().await;
                                }

                                if let StreamEvent::StateChange { new_state, .. } = &event
                                    && new_state.is_terminal()
                                {
                                    return;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(_) => return,
                        }
                    }
                }

                let bus = state.manager.event_bus();
                let response = rpc::handle_rpc(
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
                if write_response(&mut writer, &response).await.is_err() {
                    return;
                }
            }
        });
    }
}

/// Outcome of evaluating a single RPC's auth state. Extracted into a pure
/// function for unit-testability; the caller is responsible for caching
/// `Pass` results across subsequent RPCs on the same connection.
#[derive(Debug, PartialEq, Eq)]
pub enum UdsAuthDecision {
    Pass,
    Reject,
}

pub fn check_uds_auth(
    already_authed: bool,
    presented: Option<&str>,
    daemon_token: &AuthToken,
) -> UdsAuthDecision {
    if already_authed {
        return UdsAuthDecision::Pass;
    }
    match presented {
        Some(tok) if daemon_token.verify(tok) => UdsAuthDecision::Pass,
        _ => UdsAuthDecision::Reject,
    }
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &RpcResponse,
) -> Result<()> {
    let json = serde_json::to_string(response)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

// --- HTTP Server ---
//
// Auth model:
//
// * `/api/*` and the dashboard HTML at `/` require a valid bearer token.
//   The middleware accepts either:
//     - `Authorization: Bearer <token>` header (for programmatic clients
//       and the SPA's `fetch` calls that read the token from localStorage),
//     - a `grim_auth=<token>` cookie set by the login flow.
// * `/auth/login` is the only unauthenticated route. It takes a token via
//   `?t=<token>` query (used by `grim dashboard --open`) or form POST,
//   sets an HttpOnly cookie, and redirects to `/`.
// * `/auth/logout` clears the cookie.
//
// No loopback exception: a daemon listening on 127.0.0.1 still requires
// auth, which closes the "any process on this machine can drive my
// agents" gap.

const AUTH_COOKIE_NAME: &str = "grim_auth";

async fn run_http_server(state: AppState) -> Result<()> {
    // Protected routes — everything that touches state, plus the dashboard
    // HTML itself (the SPA leaks no useful surface unauthenticated, but
    // shielding `/` means a stray browser tab can't even render the chrome).
    let protected = Router::new()
        .route("/api/agents", get(http_list_agents))
        .route("/api/agents", post(http_summon_agent))
        .route("/api/agents/{id}", get(http_get_agent))
        .route("/api/agents/{id}", delete(http_banish_agent))
        .route("/api/agents/{id}/invoke", post(http_invoke_agent))
        .route("/api/agents/{id}/events", get(http_agent_events_sse))
        .route("/api/agents/{id}/history", get(http_agent_history))
        .route("/api/events", get(http_all_events_sse))
        .route("/api/scrolls", get(http_list_scrolls))
        .route("/api/scrolls", post(http_inscribe_scroll))
        .route("/api/scrolls/{id}", get(http_scroll_status))
        .route("/api/scrolls/{id}/activate", post(http_activate_scroll))
        .route("/api/scrolls/{id}/abandon", post(http_abandon_scroll))
        .route("/metrics", get(http_metrics))
        .route("/", get(http_dashboard))
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth_token.clone(),
            http_auth_middleware,
        ));

    // Unauthenticated routes — the login endpoints accept the token
    // explicitly and set the cookie on success. Webhooks live here because
    // external services (GitHub, Slack, …) won't carry the daemon's bearer
    // token; per-webhook auth is the optional shared secret in the config.
    let public = Router::new()
        .route("/auth/login", get(http_login_get).post(http_login_post))
        .route("/auth/logout", post(http_logout))
        .route("/webhooks/{name}", post(http_webhook));

    let app = Router::new()
        .merge(protected)
        .merge(public)
        .with_state(state);

    let port = constants::DAEMON_PORT;
    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "HTTP server listening");

    axum::serve(listener, app).await?;
    Ok(())
}

/// Serialize a result payload into an `axum::Json` response. On the (for plain
/// `derive(Serialize)` payloads, unreachable) serialization error, returns an
/// error envelope rather than panicking.
fn json_ok<T: serde::Serialize>(value: T) -> axum::Json<serde_json::Value> {
    match serde_json::to_value(&value) {
        Ok(v) => axum::Json(v),
        Err(e) => axum::Json(serde_json::json!({"error": format!("serialize: {e}")})),
    }
}

/// `Ok` → `json_ok(value)`, `Err` → `{"error": err.to_string()}`. Standard tail
/// for HTTP handlers wrapping a manager/db call.
fn json_result<T: serde::Serialize, E: std::fmt::Display>(
    r: Result<T, E>,
) -> axum::Json<serde_json::Value> {
    match r {
        Ok(v) => json_ok(v),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_list_agents(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    json_result(
        state
            .manager
            .circle(None)
            .await
            .map(|agents| CircleResult { agents }),
    )
}

async fn http_summon_agent(
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

async fn http_get_agent(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    match state.manager.get_agent(&id).await {
        Ok(Some(agent)) => json_ok(agent),
        Ok(None) => axum::Json(serde_json::json!({"error": "Agent not found"})),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_banish_agent(
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

async fn http_invoke_agent(
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

async fn http_agent_events_sse(
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

async fn http_all_events_sse(
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

async fn http_agent_history(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    json_result(state.manager.get_events(&id, None))
}

async fn http_list_scrolls(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    json_result(
        state
            .db
            .run(super::persistence::Database::list_scrolls)
            .await
            .map(|scrolls| serde_json::json!({"scrolls": scrolls})),
    )
}

async fn http_inscribe_scroll(
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

    let spec = match super::scroll_parser::parse_scroll(&content) {
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

async fn http_scroll_status(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    json_result(state.scroll_keeper.status(&id))
}

async fn http_activate_scroll(
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

async fn http_abandon_scroll(
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

/// Inbound webhook endpoint. The raw request body becomes the mail body —
/// no provider-specific decoding here; subscriber agents know the shape of
/// whatever they signed up for. Goes through the same `mail.send` path
/// every other mail does, so subscriber wake-on-mail Just Works.
async fn http_webhook(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use super::webhook::{WEBHOOK_TOKEN_HEADER, WebhookAuth, check_token, resolve_target};

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

    if body.len() > super::rpc::MAX_MAIL_BODY_BYTES {
        return webhook_err(axum::http::StatusCode::PAYLOAD_TOO_LARGE, "body_too_large");
    }

    // Bodies are passed through verbatim. UTF-8 is required (mail body is a
    // String); binary webhook payloads aren't a use case for v1 — operators
    // can base64 ahead of the daemon if they need one.
    let Ok(body_str) = String::from_utf8(body.to_vec()) else {
        return webhook_err(axum::http::StatusCode::BAD_REQUEST, "body_not_utf8");
    };

    // Reuse the canonical mail-send path so the topic fan-out, federation
    // routing, body-size guard, and StreamEvent emission all stay
    // single-sourced. The synthetic RpcRequest is the price for not
    // refactoring the handler — id=0 is fine because we throw it away.
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
    let resp =
        super::rpc::handle_mail_send(&state.db, &bus, &state.peer_registry, &state.daemon_id, req)
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
/// `/api/*` — within the daemon's trust boundary a scrape needs the token,
/// which keeps the metrics surface out of the casual-browser attack surface
/// without making metrics second-class.
async fn http_metrics(State(state): State<AppState>) -> axum::response::Response {
    let db = state.db.clone();
    let started_at = state.started_at;
    let body = db
        .run(move |db| super::metrics::render(db, started_at, env!("CARGO_PKG_VERSION")))
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

async fn http_dashboard() -> axum::response::Html<String> {
    axum::response::Html(include_str!("../dashboard/templates/index.html").to_string())
}

// --- HTTP auth middleware + login flow ---

/// Extract the auth token from the request. Order of precedence matches the
/// CLI: `Authorization: Bearer …` header, then `grim_auth` cookie.
fn extract_request_token(headers: &axum::http::HeaderMap) -> Option<String> {
    if let Some(h) = headers.get(axum::http::header::AUTHORIZATION)
        && let Ok(s) = h.to_str()
        && let Some(rest) = s.strip_prefix("Bearer ")
    {
        return Some(rest.trim().to_string());
    }
    if let Some(cookie) = headers.get(axum::http::header::COOKIE)
        && let Ok(s) = cookie.to_str()
    {
        for part in s.split(';') {
            let kv = part.trim();
            if let Some(v) = kv.strip_prefix(&format!("{AUTH_COOKIE_NAME}=")) {
                return Some(v.to_string());
            }
        }
    }
    None
}

async fn http_auth_middleware(
    State(token): State<Arc<AuthToken>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let presented = extract_request_token(req.headers());
    match presented {
        Some(tok) if token.verify(&tok) => next.run(req).await,
        _ => unauthorized_response(req.uri().path()),
    }
}

/// Test-only helper: build a tiny router that wraps a single protected
/// route with the same auth middleware shape `/api/*` uses in production.
/// Exposed via `#[cfg(any(test, feature = "test-helpers"))]` so the HTTP
/// auth integration tests can hit the middleware without dragging in the
/// rest of `AppState`.
#[cfg(test)]
pub fn test_auth_router(token: Arc<AuthToken>) -> Router {
    let protected = Router::new()
        .route(
            "/api/ping",
            get(|| async { axum::Json(serde_json::json!({"pong": true})) }),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            token,
            http_auth_middleware,
        ));
    let public = Router::new().route("/auth/ping-open", get(|| async { "open" }));
    Router::new().merge(protected).merge(public)
}

/// 401 with a small JSON body for `/api/*` and an HTML pointer to
/// `/auth/login` for everything else. Either way the body is constant —
/// the auth check is constant-time on its hot path.
fn unauthorized_response(path: &str) -> axum::response::Response {
    use axum::http::StatusCode;
    if path.starts_with("/api/") {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            r#"{"error":"unauthenticated"}"#,
        )
            .into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
            LOGIN_PAGE_HTML,
        )
            .into_response()
    }
}

// Trait import scoped to the auth section so the rest of the file isn't
// affected by the `IntoResponse` glob.
use axum::response::IntoResponse;

/// `GET /auth/login` — also accepts `?t=<token>` so `grim dashboard --open`
/// can produce a single-shot URL the user clicks once. If `?t=` validates,
/// we set the cookie and redirect to `/`; otherwise we render the login
/// form and let the user paste the token by hand.
async fn http_login_get(
    State(state): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> axum::response::Response {
    if let Some(tok) = q.get("t")
        && state.auth_token.verify(tok)
    {
        return login_success_response(tok);
    }
    (
        axum::http::StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        LOGIN_PAGE_HTML,
    )
        .into_response()
}

/// `POST /auth/login` — form-encoded `token=…` from the login page.
async fn http_login_post(
    State(state): State<AppState>,
    axum::Form(form): axum::Form<LoginForm>,
) -> axum::response::Response {
    if state.auth_token.verify(&form.token) {
        login_success_response(&form.token)
    } else {
        (
            axum::http::StatusCode::UNAUTHORIZED,
            [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
            LOGIN_PAGE_HTML,
        )
            .into_response()
    }
}

async fn http_logout() -> axum::response::Response {
    (
        axum::http::StatusCode::OK,
        [
            (
                axum::http::header::SET_COOKIE,
                format!("{AUTH_COOKIE_NAME}=; Max-Age=0; Path=/; HttpOnly; SameSite=Strict"),
            ),
            (axum::http::header::CONTENT_TYPE, "text/plain".to_string()),
        ],
        "logged out",
    )
        .into_response()
}

#[derive(serde::Deserialize)]
struct LoginForm {
    token: String,
}

fn login_success_response(token: &str) -> axum::response::Response {
    // HttpOnly + SameSite=Strict: no JS access, no cross-site CSRF.
    // No `Secure` flag because the daemon listens on plain HTTP loopback.
    let cookie =
        format!("{AUTH_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400");
    (
        axum::http::StatusCode::SEE_OTHER,
        [
            (axum::http::header::SET_COOKIE, cookie),
            (axum::http::header::LOCATION, "/".to_string()),
        ],
        "",
    )
        .into_response()
}

const LOGIN_PAGE_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>grimoire — sign in</title>
<style>
  body { font-family: -apple-system, system-ui, sans-serif; background: #0e0e10;
         color: #d8d8d8; display: flex; min-height: 100vh; align-items: center;
         justify-content: center; margin: 0; }
  form { background: #1a1a1d; padding: 2rem; border-radius: 0.5rem;
         border: 1px solid #2a2a2e; min-width: 320px; }
  h1 { font-size: 1rem; font-weight: 500; letter-spacing: 0.05em;
       text-transform: uppercase; margin: 0 0 1rem; color: #888; }
  input { width: 100%; padding: 0.6rem; box-sizing: border-box;
          background: #0e0e10; border: 1px solid #2a2a2e; color: #d8d8d8;
          font-family: ui-monospace, monospace; font-size: 0.9rem;
          border-radius: 0.25rem; }
  button { margin-top: 0.75rem; width: 100%; padding: 0.6rem;
           background: #6b46c1; color: white; border: 0; border-radius: 0.25rem;
           cursor: pointer; font-weight: 500; }
  button:hover { background: #7c54d6; }
  p { color: #666; font-size: 0.8rem; margin: 0.75rem 0 0; }
  code { background: #0e0e10; padding: 0.1rem 0.35rem; border-radius: 0.2rem;
         font-size: 0.8rem; }
</style>
</head>
<body>
<form method="post" action="/auth/login">
  <h1>◆ grimoire</h1>
  <input type="password" name="token" placeholder="auth token" autofocus autocomplete="off">
  <button type="submit">sign in</button>
  <p>token lives in <code>~/.grimoire/auth.token</code></p>
</form>
</body>
</html>"#;

#[cfg(test)]
mod auth_tests {
    use super::*;

    fn tok(s: &str) -> AuthToken {
        AuthToken::new(s)
    }

    // --- UDS auth decision matrix ---

    #[test]
    fn peercred_trusted_bypasses_token() {
        // already_authed=true models a peercred-trusted connection on its
        // first RPC. The decision must pass regardless of what the client
        // sent (including nothing).
        assert_eq!(
            check_uds_auth(true, None, &tok("secret")),
            UdsAuthDecision::Pass
        );
        assert_eq!(
            check_uds_auth(true, Some("wrong"), &tok("secret")),
            UdsAuthDecision::Pass
        );
    }

    #[test]
    fn untrusted_with_matching_token_passes() {
        assert_eq!(
            check_uds_auth(false, Some("secret"), &tok("secret")),
            UdsAuthDecision::Pass
        );
    }

    #[test]
    fn untrusted_with_wrong_token_rejects() {
        assert_eq!(
            check_uds_auth(false, Some("nope"), &tok("secret")),
            UdsAuthDecision::Reject
        );
    }

    #[test]
    fn untrusted_with_missing_token_rejects() {
        assert_eq!(
            check_uds_auth(false, None, &tok("secret")),
            UdsAuthDecision::Reject
        );
    }

    #[test]
    fn untrusted_with_empty_token_rejects() {
        assert_eq!(
            check_uds_auth(false, Some(""), &tok("secret")),
            UdsAuthDecision::Reject
        );
    }

    // --- HTTP header / cookie extraction ---

    fn headers_bearer(t: &str) -> axum::http::HeaderMap {
        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {t}").parse().unwrap(),
        );
        h
    }

    fn headers_cookie(raw: &str) -> axum::http::HeaderMap {
        let mut h = axum::http::HeaderMap::new();
        h.insert(axum::http::header::COOKIE, raw.parse().unwrap());
        h
    }

    #[test]
    fn extract_bearer_header() {
        let h = headers_bearer("xyz");
        assert_eq!(extract_request_token(&h).as_deref(), Some("xyz"));
    }

    #[test]
    fn extract_bearer_ignores_other_schemes() {
        let mut h = axum::http::HeaderMap::new();
        h.insert(
            axum::http::header::AUTHORIZATION,
            "Basic dXNlcjpwYXNz".parse().unwrap(),
        );
        assert_eq!(extract_request_token(&h), None);
    }

    #[test]
    fn extract_cookie_alone() {
        let h = headers_cookie(&format!("{AUTH_COOKIE_NAME}=tok1"));
        assert_eq!(extract_request_token(&h).as_deref(), Some("tok1"));
    }

    #[test]
    fn extract_cookie_among_others() {
        let h = headers_cookie(&format!("other=foo; {AUTH_COOKIE_NAME}=tok2; trailing=bar"));
        assert_eq!(extract_request_token(&h).as_deref(), Some("tok2"));
    }

    #[test]
    fn extract_bearer_beats_cookie() {
        let mut h = headers_bearer("from-header");
        h.insert(
            axum::http::header::COOKIE,
            format!("{AUTH_COOKIE_NAME}=from-cookie").parse().unwrap(),
        );
        assert_eq!(extract_request_token(&h).as_deref(), Some("from-header"));
    }

    #[test]
    fn extract_no_credentials() {
        let h = axum::http::HeaderMap::new();
        assert_eq!(extract_request_token(&h), None);
    }

    // --- HTTP middleware end-to-end (against test_auth_router) ---

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn router_with(token: &str) -> axum::Router {
        test_auth_router(Arc::new(AuthToken::new(token)))
    }

    async fn status_of(router: axum::Router, req: Request<Body>) -> StatusCode {
        router.oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn protected_route_rejects_missing_credentials() {
        let req = Request::builder()
            .uri("/api/ping")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(router_with("secret"), req).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn protected_route_rejects_wrong_bearer() {
        let req = Request::builder()
            .uri("/api/ping")
            .header("authorization", "Bearer nope")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(router_with("secret"), req).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn protected_route_accepts_correct_bearer() {
        let req = Request::builder()
            .uri("/api/ping")
            .header("authorization", "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status_of(router_with("secret"), req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_route_accepts_cookie() {
        let req = Request::builder()
            .uri("/api/ping")
            .header("cookie", format!("{AUTH_COOKIE_NAME}=secret"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(status_of(router_with("secret"), req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn protected_route_rejects_cookie_with_wrong_value() {
        let req = Request::builder()
            .uri("/api/ping")
            .header("cookie", format!("{AUTH_COOKIE_NAME}=wrong"))
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            status_of(router_with("secret"), req).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn public_route_does_not_require_auth() {
        let req = Request::builder()
            .uri("/auth/ping-open")
            .body(Body::empty())
            .unwrap();
        assert_eq!(status_of(router_with("secret"), req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn api_path_unauthorized_responds_with_json_body() {
        let req = Request::builder()
            .uri("/api/ping")
            .body(Body::empty())
            .unwrap();
        let resp = router_with("secret").oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("application/json"), "got {ct}");
    }
}
