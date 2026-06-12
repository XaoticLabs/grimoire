//! Supervision RPC handlers: supervisor tree inspection, manual restart,
//! escalation clearing, and restart history.

use std::sync::Arc;

use crate::shared::protocol::*;

use crate::daemon::agent_manager::AgentManager;
use crate::daemon::persistence::Database;

use super::{parse_params, try_params};

pub(super) async fn handle_supervisor_tree(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    use crate::shared::protocol::SupervisorTreeResult;
    let outcome = db.run(Database::list_supervisor_nodes).await;
    match outcome {
        Ok(nodes) => RpcResponse::success_json(req.id, &SupervisorTreeResult { nodes }),
        Err(e) => RpcResponse::error(req.id, -32000, format!("agent.supervisor-tree: {e}")),
    }
}

pub(super) async fn handle_supervisor_restart_now(
    manager: &Arc<AgentManager>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::daemon::supervisor::ManualRestartOutcome;
    use crate::shared::protocol::{SupervisorRestartNowParams, SupervisorRestartNowResult};
    let params: SupervisorRestartNowParams = try_params!(req);
    let Some(sup) = manager.supervisor().await else {
        return RpcResponse::error(req.id, -32000, "supervisor_not_ready".into());
    };
    match sup.manual_restart(&params.agent_id).await {
        Ok(ManualRestartOutcome::Scheduled { attempt }) => RpcResponse::success_json(
            req.id,
            &SupervisorRestartNowResult {
                agent_id: params.agent_id,
                outcome: "scheduled".into(),
                reason: None,
                attempt: Some(attempt),
            },
        ),
        Ok(ManualRestartOutcome::Rejected { reason }) => RpcResponse::success_json(
            req.id,
            &SupervisorRestartNowResult {
                agent_id: params.agent_id,
                outcome: "rejected".into(),
                reason: Some(reason.to_string()),
                attempt: None,
            },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("supervisor.restart-now: {e}")),
    }
}

pub(super) async fn handle_supervisor_clear_escalation(
    manager: &Arc<AgentManager>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::shared::protocol::{
        SupervisorClearEscalationParams, SupervisorClearEscalationResult,
    };
    let params: SupervisorClearEscalationParams = try_params!(req);
    let Some(sup) = manager.supervisor().await else {
        return RpcResponse::error(req.id, -32000, "supervisor_not_ready".into());
    };
    match sup.clear_escalation(&params.agent_id) {
        Ok(prev) => RpcResponse::success_json(
            req.id,
            &SupervisorClearEscalationResult {
                agent_id: params.agent_id,
                previous_depth: prev,
            },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("supervisor.clear-escalation: {e}")),
    }
}

pub(super) async fn handle_supervisor_history(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    use crate::shared::protocol::{SupervisorHistoryParams, SupervisorHistoryResult};
    let params: SupervisorHistoryParams = try_params!(req);
    let agent_id = params.agent_id.clone();
    let limit = params.limit.unwrap_or(50);
    let outcome = db
        .run(move |db| db.list_restart_history(&agent_id, limit))
        .await;
    match outcome {
        Ok(rows) => RpcResponse::success_json(
            req.id,
            &SupervisorHistoryResult {
                agent_id: params.agent_id,
                rows,
            },
        ),
        Err(e) => RpcResponse::error(req.id, -32000, format!("supervisor.history: {e}")),
    }
}
