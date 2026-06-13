//! Workspace and workspace-memory RPC handlers: create/list/destroy/assign
//! plus the memory KV (put/get/list/delete) scoped to a workspace.

use std::sync::Arc;

use crate::shared::protocol::*;
use crate::shared::types::{validate_memory_key, validate_workspace_id};

use crate::daemon::event_bus::EventBus;
use crate::daemon::persistence::Database;
use crate::daemon::workspace_db::MemoryWriteOutcome;
use crate::daemon::workspace_registry::{WorkspaceRegistry, publish_memory_topic_mail};

use super::{parse_params, resolve_workspace, rpc_err, try_op, try_params, try_rpc};

pub(super) async fn handle_workspace_create(
    reg: &Arc<WorkspaceRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: WorkspaceCreateParams = try_params!(req);
    if let Err(e) = validate_workspace_id(&params.name) {
        let _ = e;
        return rpc_err(req.id, "invalid_workspace_name");
    }
    let ws = match reg
        .create(
            &params.name,
            std::path::Path::new(&params.repo_path),
            &params.branch,
        )
        .await
    {
        Ok(ws) => ws,
        Err(e) => return rpc_err(req.id, e.code()),
    };
    if let Some(src) = params.copy_memory_from {
        // Best-effort: log on failure but don't unwind workspace creation.
        if let Err(e) = reg.db().memory_copy_workspace(&src, &ws.id) {
            tracing::warn!(
                target = %ws.id,
                source = %src,
                error = %e,
                "workspace.create: copy_memory_from failed; new workspace is empty"
            );
        }
    }
    RpcResponse::success_json(
        req.id,
        &WorkspaceCreateResult {
            id: ws.id,
            path: ws.path,
        },
    )
}

pub(super) fn handle_workspace_list(reg: &Arc<WorkspaceRegistry>, req: RpcRequest) -> RpcResponse {
    try_op(
        req.id,
        "list",
        reg.list().map(|(workspaces, orphans)| WorkspaceListResult {
            workspaces,
            orphans,
        }),
    )
}

pub(super) async fn handle_workspace_destroy(
    reg: &Arc<WorkspaceRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: WorkspaceDestroyParams = try_params!(req);
    match reg.destroy(&params.id).await {
        Ok(()) => RpcResponse::success_json(req.id, &WorkspaceDestroyResult::default()),
        Err(e) => rpc_err(req.id, e.code()),
    }
}

pub(super) async fn handle_workspace_assign(
    reg: &Arc<WorkspaceRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: WorkspaceAssignParams = try_params!(req);
    match reg.assign(&params.workspace_id, &params.agent_id).await {
        Ok(()) => RpcResponse::success_json(req.id, &WorkspaceAssignResult::default()),
        Err(e) => rpc_err(req.id, e.code()),
    }
}

pub(super) async fn handle_memory_put(
    db: &Arc<Database>,
    bus: &EventBus,
    req: RpcRequest,
) -> RpcResponse {
    let params: MemoryPutParams = try_params!(req);
    if validate_memory_key(&params.key).is_err() {
        return rpc_err(req.id, "invalid_memory_key");
    }
    let ws = try_rpc!(resolve_workspace(req.id, db, &params.workspace_id).await);
    if ws.state != crate::shared::types::WorkspaceState::Active {
        return rpc_err(req.id, "workspace_destroying");
    }

    let Ok(bytes) = serde_json::to_vec(&params.value) else {
        return rpc_err(req.id, "invalid_value_json");
    };

    let cfg = crate::shared::config::Config::load().unwrap_or_default();
    if (bytes.len() as u64) > cfg.daemon.workspace_value_cap_bytes {
        return rpc_err(req.id, "memory_value_too_large");
    }

    let workspace_id = params.workspace_id.clone();
    let key = params.key.clone();
    let sender_owned = params.sender.clone();
    let bus_clone = bus.clone();
    let expected = params.expected_version;
    let total_cap = cfg.daemon.workspace_total_cap_bytes;
    // One trip: total-cap pre-check + CAS write + memory-topic fanout.
    // MemoryWritten emission stays caller-side to keep the success branch clean.
    let outcome: Result<MemoryWriteOutcome, RpcResponse> = db
        .run(move |db| {
            let total = db
                .memory_total_size_for_workspace(&workspace_id)
                .unwrap_or(0);
            let (_cur_v, cur_size) = db
                .memory_current_version_and_size(&workspace_id, &key)
                .unwrap_or((0, 0));
            let new_total = total
                .saturating_sub(cur_size)
                .saturating_add(bytes.len() as u64);
            if new_total > total_cap {
                return Err(rpc_err(req.id, "memory_total_cap_exceeded"));
            }
            let updated_by = sender_owned.as_deref().unwrap_or("system");
            let res = match db.memory_put_cas(&workspace_id, &key, &bytes, expected, updated_by) {
                Ok(o) => o,
                Err(e) => return Err(RpcResponse::error(req.id, -32000, format!("put: {e}"))),
            };
            if let MemoryWriteOutcome::Written { version } = &res {
                let _ = publish_memory_topic_mail(
                    db,
                    &bus_clone,
                    &workspace_id,
                    &key,
                    *version,
                    "put",
                    sender_owned.as_deref(),
                );
            }
            Ok(res)
        })
        .await;

    match outcome {
        Err(resp) => resp,
        Ok(MemoryWriteOutcome::Conflict { current_version }) => RpcResponse::error(
            req.id,
            -32000,
            format!("cas_conflict:current_version={current_version}"),
        ),
        Ok(MemoryWriteOutcome::Written { version }) => {
            bus.publish(StreamEvent::MemoryWritten {
                workspace_id: params.workspace_id.clone(),
                key: params.key.clone(),
                version,
                agent_id: params.sender.clone(),
            });
            RpcResponse::success_json(req.id, &MemoryPutResult { version })
        }
    }
}

pub(super) async fn handle_memory_get(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MemoryGetParams = try_params!(req);
    if validate_memory_key(&params.key).is_err() {
        return rpc_err(req.id, "invalid_memory_key");
    }
    let workspace_id = params.workspace_id;
    let key = params.key;
    match db.run(move |db| db.memory_get(&workspace_id, &key)).await {
        Ok(Some(entry)) => {
            let result = MemoryGetResult {
                value: entry.value,
                version: entry.version,
            };
            RpcResponse::success_json(req.id, &result)
        }
        Ok(None) => rpc_err(req.id, "memory_not_found"),
        Err(e) => RpcResponse::error(req.id, -32000, format!("get: {e}")),
    }
}

pub(super) async fn handle_memory_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: MemoryListParams = try_params!(req);
    let workspace_id = params.workspace_id;
    let prefix = params.prefix;
    try_op(
        req.id,
        "list",
        db.run(move |db| db.memory_list_prefix(&workspace_id, prefix.as_deref()))
            .await
            .map(|entries| MemoryListResult { entries }),
    )
}

pub(super) async fn handle_memory_delete(
    db: &Arc<Database>,
    bus: &EventBus,
    req: RpcRequest,
) -> RpcResponse {
    let params: MemoryDeleteParams = try_params!(req);
    if validate_memory_key(&params.key).is_err() {
        return rpc_err(req.id, "invalid_memory_key");
    }
    let workspace_id = params.workspace_id.clone();
    let key = params.key.clone();
    let sender_owned = params.sender.clone();
    let bus_clone = bus.clone();
    let expected = params.expected_version;
    let outcome: Result<MemoryWriteOutcome, anyhow::Error> = db
        .run(move |db| {
            let res = db.memory_delete_cas(&workspace_id, &key, expected)?;
            if let MemoryWriteOutcome::Written { version } = &res
                && *version > 0
            {
                let _ = publish_memory_topic_mail(
                    db,
                    &bus_clone,
                    &workspace_id,
                    &key,
                    *version,
                    "delete",
                    sender_owned.as_deref(),
                );
            }
            Ok(res)
        })
        .await;
    let outcome = match outcome {
        Ok(o) => o,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("delete: {e}")),
    };
    match outcome {
        MemoryWriteOutcome::Conflict { current_version } => RpcResponse::error(
            req.id,
            -32000,
            format!("cas_conflict:current_version={current_version}"),
        ),
        MemoryWriteOutcome::Written { version } => {
            // version == 0 means "no-op" (key didn't exist).
            if version > 0 {
                bus.publish(StreamEvent::MemoryDeleted {
                    workspace_id: params.workspace_id.clone(),
                    key: params.key.clone(),
                    agent_id: params.sender.clone(),
                });
            }
            RpcResponse::success_json(req.id, &MemoryDeleteResult::default())
        }
    }
}
