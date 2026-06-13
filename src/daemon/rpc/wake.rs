//! Wake-source RPC handlers: add/list/remove/test against the `WakeRegistry`.

use std::sync::Arc;

use crate::shared::protocol::*;
use crate::shared::types::WakeSourceKind;

use crate::daemon::persistence::Database;
use crate::daemon::wake_registry::WakeRegistry;

use super::{parse_params, rpc_err, try_op, try_params};

pub(super) async fn handle_wake_add(
    db: &Arc<Database>,
    reg: &Arc<WakeRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: WakeAddParams = try_params!(req);
    // Capture the agent: file-watch roots resolve against its cwd below.
    let agent_id = params.agent_id.clone();
    let agent = match db.run(move |db| db.get_agent(&agent_id)).await {
        Ok(Some(a)) => a,
        Ok(None) => return rpc_err(req.id, "agent_not_found"),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db: {e}")),
    };
    let kind: WakeSourceKind = match params.kind.parse() {
        Ok(k) => k,
        Err(_) => return rpc_err(req.id, "invalid_kind"),
    };
    // Globs watch under the agent's cwd: override any client-sent root, since
    // older CLIs sent their own current_dir (wrong unless it matched the cwd).
    let mut config = params.config.clone();
    if kind == WakeSourceKind::FileWatch
        && let Some(obj) = config.as_object_mut()
    {
        obj.insert("root".into(), serde_json::json!(agent.cwd));
    }
    let config_json = match serde_json::to_string(&config) {
        Ok(s) => s,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("config: {e}")),
    };
    match reg.register(&params.agent_id, kind, &config_json).await {
        Ok(wake_id) => RpcResponse::success_json(req.id, &WakeAddResult { wake_id }),
        Err(e) => rpc_err(req.id, &e.to_string()),
    }
}

pub(super) async fn handle_wake_list(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeListParams = parse_params(&req).unwrap_or_default();
    let result = match params.agent_id {
        Some(id) => reg.list_for_agent(&id).await,
        None => reg.list_all().await,
    };
    try_op(
        req.id,
        "list",
        result.map(|sources| WakeListResult { sources }),
    )
}

pub(super) async fn handle_wake_remove(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeRemoveParams = try_params!(req);
    match reg.remove(&params.wake_id).await {
        Ok(true) => RpcResponse::success_json(req.id, &WakeRemoveResult { success: true }),
        Ok(false) => rpc_err(req.id, "wake_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("remove: {e}")),
    }
}

pub(super) async fn handle_wake_test(reg: &Arc<WakeRegistry>, req: RpcRequest) -> RpcResponse {
    let params: WakeTestParams = try_params!(req);
    match reg.test_fire(&params.wake_id).await {
        Ok(mail_id) => RpcResponse::success_json(
            req.id,
            &WakeTestResult {
                success: true,
                mail_id,
            },
        ),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("wake_not_found") {
                rpc_err(req.id, "wake_not_found")
            } else {
                RpcResponse::error(req.id, -32000, msg)
            }
        }
    }
}
