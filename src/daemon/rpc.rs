use std::sync::Arc;

use crate::shared::protocol::*;

use super::agent_manager::AgentManager;

pub async fn handle_rpc(
    manager: &Arc<AgentManager>,
    req: RpcRequest,
) -> RpcResponse {
    match req.method.as_str() {
        "agent.summon" => handle_summon(manager, req).await,
        "agent.circle" => handle_circle(manager, req).await,
        "agent.banish" => handle_banish(manager, req).await,
        "daemon.status" => handle_status(manager, req).await,
        _ => RpcResponse::error(req.id, -32601, format!("Unknown method: {}", req.method)),
    }
}

async fn handle_summon(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: SummonParams = match serde_json::from_value(req.params) {
        Ok(p) => p,
        Err(e) => return RpcResponse::error(req.id, -32602, format!("Invalid params: {}", e)),
    };

    match manager
        .summon(params.task, params.name, params.model, params.cwd)
        .await
    {
        Ok(agent) => {
            let result = SummonResult {
                id: agent.id,
                name: agent.name,
                state: agent.state.as_str().to_string(),
            };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to summon: {}", e)),
    }
}

async fn handle_circle(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: CircleParams = serde_json::from_value(req.params).unwrap_or(CircleParams { state: None });

    match manager.circle(params.state.as_deref()).await {
        Ok(agents) => {
            let result = CircleResult { agents };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to list: {}", e)),
    }
}

async fn handle_banish(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: BanishParams = match serde_json::from_value(req.params) {
        Ok(p) => p,
        Err(e) => return RpcResponse::error(req.id, -32602, format!("Invalid params: {}", e)),
    };

    match manager.banish(&params.id).await {
        Ok(success) => {
            let result = BanishResult { success };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to banish: {}", e)),
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
                uptime_secs: 0, // TODO: track daemon start time
                agent_count: agents.len(),
                active_count: active,
            };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed: {}", e)),
    }
}
