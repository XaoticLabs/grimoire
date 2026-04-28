use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, post};
use std::convert::Infallible;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tracing::{error, info};

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
}

/// Start both UDS and HTTP servers
#[allow(clippy::too_many_arguments)]
pub async fn run(
    manager: Arc<AgentManager>,
    db: Arc<super::persistence::Database>,
    scroll_keeper: Arc<super::scroll_keeper::ScrollKeeper>,
    wake_registry: Arc<super::wake_registry::WakeRegistry>,
    workspace_registry: Arc<super::workspace_registry::WorkspaceRegistry>,
    supervisor: Arc<super::supervisor::Supervisor>,
    peer_registry: Arc<super::peer_registry::PeerRegistry>,
    daemon_id: String,
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
    };

    // Start UDS listener
    let uds_state = state.clone();
    let uds_handle = tokio::spawn(async move {
        if let Err(e) = run_uds_server(uds_state).await {
            error!("UDS server error: {}", e);
        }
    });

    // Start HTTP server
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

    // Remove stale socket
    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    info!(path = %socket_path.display(), "UDS server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let reader = BufReader::new(reader);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let req: RpcRequest = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = RpcResponse::error(0, -32700, format!("Parse error: {}", e));
                        let _ = write_response(&mut writer, &err).await;
                        continue;
                    }
                };

                // Special case: bind streams events
                if req.method == "agent.bind" {
                    let params: BindParams = match serde_json::from_value(req.params.clone()) {
                        Ok(p) => p,
                        Err(e) => {
                            let err = RpcResponse::error(
                                req.id,
                                -32602,
                                format!("Invalid params: {}", e),
                            );
                            let _ = write_response(&mut writer, &err).await;
                            continue;
                        }
                    };

                    // Send historical events first
                    if let Ok(events) = state.manager.get_events(&params.id, params.tail) {
                        for event in events {
                            let stream_event = StreamEvent::AgentEvent { event };
                            let json = serde_json::to_string(&stream_event).unwrap();
                            if writer.write_all(json.as_bytes()).await.is_err() {
                                return;
                            }
                            if writer.write_all(b"\n").await.is_err() {
                                return;
                            }
                        }
                    }

                    // Then stream live events
                    let mut rx = state.manager.subscribe();
                    loop {
                        match rx.recv().await {
                            Ok(event) => {
                                // Filter to this agent
                                if event.agent_id() == Some(params.id.as_str()) {
                                    let json = serde_json::to_string(&event).unwrap();
                                    if writer.write_all(json.as_bytes()).await.is_err() {
                                        return;
                                    }
                                    if writer.write_all(b"\n").await.is_err() {
                                        return;
                                    }
                                    let _ = writer.flush().await;
                                }

                                // Stop streaming if agent finished
                                if let StreamEvent::StateChange { new_state, .. } = &event
                                    && new_state.is_terminal()
                                {
                                    return;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
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

async fn run_http_server(state: AppState) -> Result<()> {
    let app = Router::new()
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
        .route("/", get(http_dashboard))
        .with_state(state);

    let port = constants::DAEMON_PORT;
    let addr = format!("127.0.0.1:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(addr = %addr, "HTTP server listening");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn http_list_agents(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    match state.manager.circle(None).await {
        Ok(agents) => axum::Json(serde_json::to_value(CircleResult { agents }).unwrap()),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
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
    match state
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
    {
        Ok(agent) => axum::Json(
            serde_json::to_value(SummonResult {
                id: agent.id,
                name: agent.name,
                state: agent.state.as_str().to_string(),
            })
            .unwrap(),
        ),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_get_agent(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    match state.manager.get_agent(&id).await {
        Ok(Some(agent)) => axum::Json(serde_json::to_value(agent).unwrap()),
        Ok(None) => axum::Json(serde_json::json!({"error": "Agent not found"})),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_banish_agent(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    match state.manager.banish(&id).await {
        Ok(success) => axum::Json(serde_json::to_value(BanishResult { success }).unwrap()),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
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

    match state.manager.invoke(&id, &message, None).await {
        Ok(()) => axum::Json(serde_json::json!({"success": true})),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
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
                    if event.agent_id() == Some(id.as_str()) {
                        let json = serde_json::to_string(&event).unwrap();
                        yield Ok(Event::default().data(json));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
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
                    let json = serde_json::to_string(&event).unwrap();
                    yield Ok(Event::default().data(json));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
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
    match state.manager.get_events(&id, None) {
        Ok(events) => axum::Json(serde_json::to_value(events).unwrap()),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_list_scrolls(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    match state.db.list_scrolls() {
        Ok(scrolls) => axum::Json(serde_json::json!({"scrolls": scrolls})),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
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
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);

    let content = match std::fs::read_to_string(&spec_path) {
        Ok(c) => c,
        Err(e) => {
            return axum::Json(serde_json::json!({"error": format!("Failed to read spec: {}", e)}));
        }
    };

    let spec = match super::scroll_parser::parse_scroll(&content) {
        Ok(s) => s,
        Err(e) => return axum::Json(serde_json::json!({"error": format!("Parse error: {}", e)})),
    };

    match state
        .scroll_keeper
        .inscribe(spec, max_concurrency, Some(spec_path))
    {
        Ok(result) => axum::Json(serde_json::json!({
            "id": result.scroll.id,
            "name": result.scroll.name,
            "task_count": result.task_count,
            "conflicts": result.conflicts,
        })),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_scroll_status(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    match state.scroll_keeper.status(&id) {
        Ok(status) => axum::Json(serde_json::to_value(status).unwrap()),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_activate_scroll(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    match state.scroll_keeper.activate(&id).await {
        Ok(()) => axum::Json(serde_json::json!({"success": true})),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_abandon_scroll(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    match state.scroll_keeper.abandon(&id).await {
        Ok(()) => axum::Json(serde_json::json!({"success": true})),
        Err(e) => axum::Json(serde_json::json!({"error": e.to_string()})),
    }
}

async fn http_dashboard() -> axum::response::Html<String> {
    axum::response::Html(include_str!("../dashboard/templates/index.html").to_string())
}
