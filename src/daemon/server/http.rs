//! HTTP server bootstrap: the axum router wiring (protected vs. public route
//! tables, auth middleware layer, TCP bind) plus the shared JSON-response
//! helpers used by the REST handlers.

use anyhow::Result;
use axum::Router;
use axum::routing::{delete, get, post};
use tracing::info;

use super::AppState;
use super::auth::{http_auth_middleware, http_login_get, http_login_post, http_logout};
use super::handlers::{
    http_abandon_scroll, http_activate_scroll, http_agent_events_sse, http_agent_history,
    http_all_events_sse, http_banish_agent, http_dashboard, http_get_agent, http_inscribe_scroll,
    http_invoke_agent, http_list_agents, http_list_scrolls, http_metrics, http_rpc_bridge,
    http_scroll_status, http_summon_agent, http_webhook,
};

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

pub(super) async fn run_http_server(state: AppState, port: u16) -> Result<()> {
    // Protected routes: everything that touches state, plus the dashboard
    // HTML itself. The SPA leaks no useful surface unauthenticated, but
    // shielding `/` means a stray browser tab can't even render the chrome.
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
        .route("/api/rpc", post(http_rpc_bridge))
        .route("/metrics", get(http_metrics))
        .route("/", get(http_dashboard))
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth_token.clone(),
            http_auth_middleware,
        ));

    // Unauthenticated routes. The login endpoints accept the token
    // explicitly and set the cookie on success. Webhooks live here because
    // external services (GitHub, Slack, etc.) won't carry the daemon's bearer
    // token; per-webhook auth is the optional shared secret in the config.
    let public = Router::new()
        .route("/auth/login", get(http_login_get).post(http_login_post))
        .route("/auth/logout", post(http_logout))
        .route("/webhooks/{name}", post(http_webhook));

    let app = Router::new()
        .merge(protected)
        .merge(public)
        .with_state(state);

    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "HTTP server listening");

    axum::serve(listener, app).await?;
    Ok(())
}

/// Serialize a result payload into an `axum::Json` response. On the (for plain
/// `derive(Serialize)` payloads, unreachable) serialization error, returns an
/// error envelope rather than panicking.
pub(super) fn json_ok<T: serde::Serialize>(value: T) -> axum::Json<serde_json::Value> {
    match serde_json::to_value(&value) {
        Ok(v) => axum::Json(v),
        Err(e) => axum::Json(serde_json::json!({"error": format!("serialize: {e}")})),
    }
}

/// `Ok` → `json_ok(value)`, `Err` → `{"error": err.to_string()}`. Standard tail
/// for HTTP handlers wrapping a manager/db call.
pub(super) fn json_result<T: serde::Serialize, E: std::fmt::Display>(
    r: Result<T, E>,
) -> axum::Json<serde_json::Value> {
    match r {
        Ok(v) => json_ok(v),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}
