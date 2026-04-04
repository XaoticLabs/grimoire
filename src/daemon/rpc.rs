use std::sync::Arc;

use crate::shared::protocol::*;
use crate::shared::types::{Pact, PactState};

use super::agent_manager::AgentManager;
use super::persistence::Database;

pub async fn handle_rpc(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    req: RpcRequest,
) -> RpcResponse {
    match req.method.as_str() {
        "agent.summon" => handle_summon(manager, req).await,
        "agent.circle" => handle_circle(manager, req).await,
        "agent.banish" => handle_banish(manager, req).await,
        "agent.invoke" => handle_invoke(manager, req).await,
        "pact.create" => handle_pact_create(db, req),
        "pact.list" => handle_pact_list(db, req),
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
    let params: CircleParams =
        serde_json::from_value(req.params).unwrap_or(CircleParams { state: None });

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

async fn handle_invoke(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: InvokeParams = match serde_json::from_value(req.params) {
        Ok(p) => p,
        Err(e) => return RpcResponse::error(req.id, -32602, format!("Invalid params: {}", e)),
    };

    match manager.invoke(&params.id, params.message).await {
        Ok(()) => RpcResponse::success(req.id, serde_json::json!({"success": true})),
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to invoke: {}", e)),
    }
}

fn handle_pact_create(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: PactCreateParams = match serde_json::from_value(req.params) {
        Ok(p) => p,
        Err(e) => return RpcResponse::error(req.id, -32602, format!("Invalid params: {}", e)),
    };

    let pact_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
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
    let params: PactListParams =
        serde_json::from_value(req.params).unwrap_or(PactListParams { source_id: None });

    match db.list_pacts(params.source_id.as_deref()) {
        Ok(pacts) => {
            let result = PactListResult { pacts };
            RpcResponse::success(req.id, serde_json::to_value(result).unwrap())
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("Failed to list pacts: {}", e)),
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
