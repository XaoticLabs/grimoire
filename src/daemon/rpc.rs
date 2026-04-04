use serde::de::DeserializeOwned;
use std::sync::Arc;

use crate::shared::protocol::*;
use crate::shared::types::{Pact, PactState};

use super::agent_manager::AgentManager;
use super::persistence::Database;
use super::scroll_keeper::ScrollKeeper;

fn parse_params<T: DeserializeOwned>(req: &RpcRequest) -> Result<T, RpcResponse> {
    serde_json::from_value(req.params.clone())
        .map_err(|e| RpcResponse::error(req.id, -32602, format!("Invalid params: {}", e)))
}

pub async fn handle_rpc(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    scroll_keeper: &Arc<ScrollKeeper>,
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
        _ => RpcResponse::error(req.id, -32601, format!("Unknown method: {}", req.method)),
    }
}

async fn handle_summon(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: SummonParams = match parse_params(&req) {
        Ok(p) => p,
        Err(e) => return e,
    };

    match manager
        .summon(params.task, params.name, params.model, params.cwd, params.provider)
        .await
    {
        Ok(agent) => {
            let result = SummonResult {
                id: agent.id,
                name: agent.name,
                state: agent.state.to_string(),
            };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to summon: {}", e)),
    }
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

    match manager.invoke(&params.id, params.message).await {
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
                rune_count: result.rune_count,
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

async fn handle_status(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    match manager.circle(None).await {
        Ok(agents) => {
            let active = agents
                .iter()
                .filter(|a| a.state == crate::shared::types::AgentState::Active)
                .count();
            let result = DaemonStatusResult {
                uptime_secs: 0,
                agent_count: agents.len(),
                active_count: active,
            };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed: {}", e)),
    }
}
