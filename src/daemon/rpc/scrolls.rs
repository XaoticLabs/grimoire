//! Pact and scroll RPC handlers: pact create/list, scroll lifecycle
//! (inscribe/activate/status/list/abandon), and cross-peer task dispatch.

use std::sync::Arc;

use crate::shared::protocol::*;
use crate::shared::types::{Pact, PactState};

use crate::daemon::peer_registry::PeerRegistry;
use crate::daemon::persistence::Database;
use crate::daemon::scroll_keeper::ScrollKeeper;

use super::{parse_params, resolve_peer, rpc_fail, try_op, try_params, try_rpc};

pub(super) async fn handle_pact_create(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: PactCreateParams = try_params!(req);

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

    let pact_for_db = pact.clone();
    try_op(
        req.id,
        "create pact",
        db.run(move |db| db.insert_pact(&pact_for_db))
            .await
            .map(|()| PactCreateResult {
                id: pact_id,
                source_id: params.source_id,
            }),
    )
}

pub(super) async fn handle_pact_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: PactListParams = parse_params(&req).unwrap_or(PactListParams { source_id: None });
    let source_id = params.source_id;
    try_op(
        req.id,
        "list pacts",
        db.run(move |db| db.list_pacts(source_id.as_deref()))
            .await
            .map(|pacts| PactListResult { pacts }),
    )
}

pub(super) async fn handle_scroll_inscribe(
    keeper: &Arc<ScrollKeeper>,
    req: RpcRequest,
) -> RpcResponse {
    let params: ScrollInscribeParams = try_params!(req);

    let content = match tokio::fs::read_to_string(&params.spec_path).await {
        Ok(c) => c,
        Err(e) => {
            return RpcResponse::error(
                req.id,
                -32000,
                format!("Failed to read spec file '{}': {}", params.spec_path, e),
            );
        }
    };

    let spec = match crate::daemon::scroll_parser::parse_scroll(&content) {
        Ok(s) => s,
        Err(e) => {
            return rpc_fail(req.id, "parse spec", e);
        }
    };

    try_op(
        req.id,
        "inscribe",
        keeper
            .inscribe(spec, params.max_concurrency, Some(params.spec_path))
            .map(|result| ScrollInscribeResult {
                id: result.scroll.id,
                name: result.scroll.name,
                task_count: result.task_count,
                conflicts: result.conflicts,
            }),
    )
}

pub(super) async fn handle_scroll_activate(
    keeper: &Arc<ScrollKeeper>,
    req: RpcRequest,
) -> RpcResponse {
    let params: ScrollActivateParams = try_params!(req);
    try_op(
        req.id,
        "activate",
        keeper
            .activate(&params.id)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

pub(super) fn handle_scroll_status(keeper: &Arc<ScrollKeeper>, req: RpcRequest) -> RpcResponse {
    let params: ScrollStatusParams = try_params!(req);
    try_op(req.id, "get status", keeper.status(&params.id))
}

pub(super) async fn handle_scroll_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    try_op(
        req.id,
        "list scrolls",
        db.run(Database::list_scrolls)
            .await
            .map(|scrolls| serde_json::json!({"scrolls": scrolls})),
    )
}

pub(super) async fn handle_scroll_abandon(
    keeper: &Arc<ScrollKeeper>,
    req: RpcRequest,
) -> RpcResponse {
    let params: ScrollAbandonParams = try_params!(req);
    try_op(
        req.id,
        "abandon",
        keeper
            .abandon(&params.id)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

/// F5a: dispatch one scroll task to a peer.
///
/// The coordinator looks up the task, serializes the payload, writes
/// the durable `scroll_task_dispatches` row, and enqueues the wire
/// outbox row. The receiver's local agent id flows back via the
/// `ScrollTaskDispatchAck` ack handler, which patches the row.
pub(super) async fn handle_scroll_dispatch_task(
    peer_registry: &Arc<PeerRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    use crate::daemon::peer_client::ScrollDispatchPayload;
    use crate::shared::protocol::{ScrollDispatchTaskParams, ScrollDispatchTaskResult};
    let params: ScrollDispatchTaskParams = try_params!(req);
    let peer = try_rpc!(resolve_peer(req.id, peer_registry, &params.peer).await);
    let peer_id = peer.id.clone();
    let scroll_id = params.scroll_id.clone();
    let task_id = params.task_id.clone();

    let outcome: Result<u64, String> = peer_registry
        .db
        .run(move |db| -> Result<u64, String> {
            let task = match db.get_task(&task_id) {
                Ok(Some(t)) => t,
                Ok(None) => return Err("task_not_found".into()),
                Err(e) => return Err(format!("get_task: {e}")),
            };
            if task.scroll_id != scroll_id {
                return Err("task_scroll_mismatch".into());
            }
            let payload = ScrollDispatchPayload {
                scroll_id: scroll_id.clone(),
                task_id: task_id.clone(),
                task_name: task.name,
                prompt: task.prompt,
                provider: task.provider.unwrap_or_default(),
                model: task.model.unwrap_or_default(),
                cwd: task.cwd.unwrap_or_default(),
                file_patterns: task.file_patterns,
            };
            let bytes = serde_json::to_vec(&payload).map_err(|e| format!("encode: {e}"))?;
            let dispatch_id = crate::shared::constants::generate_short_id();
            if let Err(e) = db.scroll_dispatch_insert(&dispatch_id, &scroll_id, &task_id, &peer_id)
            {
                return Err(format!("insert_dispatch: {e}"));
            }
            db.scroll_dispatch_enqueue(&peer_id, &bytes)
                .map_err(|e| format!("enqueue: {e}"))
        })
        .await;

    match outcome {
        Ok(seq) => {
            peer_registry.notify_outbox(&peer.id).await;
            RpcResponse::success_json(
                req.id,
                &ScrollDispatchTaskResult {
                    scroll_id: params.scroll_id,
                    task_id: params.task_id,
                    peer: params.peer,
                    sender_seq: seq,
                },
            )
        }
        Err(msg) => RpcResponse::error(req.id, -32000, msg),
    }
}
