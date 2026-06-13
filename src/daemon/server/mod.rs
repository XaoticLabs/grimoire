//! Daemon front-end servers: shared `AppState`, the UDS JSON-RPC socket, and
//! the HTTP bridge (REST + SSE + dashboard + auth) that fan out from `run`.

use anyhow::Result;
use std::sync::Arc;
use tracing::error;

use crate::shared::auth::AuthToken;

use super::agent_manager::AgentManager;

mod auth;
mod handlers;
mod http;
mod uds;

use http::run_http_server;
use uds::run_uds_server;
pub use uds::{UdsAuthDecision, check_uds_auth};

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
    /// Drives `grimoire_uptime_seconds`; an `Instant` so clock skew can't
    /// confuse the value.
    pub started_at: std::time::Instant,
    /// Inbound webhook config keyed by the `name` URL segment in
    /// `POST /webhooks/<name>`. Empty map = webhook surface closed.
    pub webhooks: Arc<std::collections::HashMap<String, crate::shared::config::WebhookConfig>>,
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
    http_port: u16,
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
        if let Err(e) = run_http_server(state, http_port).await {
            error!("HTTP server error: {}", e);
        }
    });

    tokio::select! {
        _ = uds_handle => {},
        _ = http_handle => {},
    }

    Ok(())
}
