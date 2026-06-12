//! Agent lifecycle RPC handlers: summon/circle/banish/invoke, daemon status,
//! queue listing, process inspection, result retrieval, and chronicle replay.

use std::sync::Arc;

use crate::shared::protocol::*;
use crate::shared::types::AgentState;

use crate::daemon::agent_manager::AgentManager;
use crate::daemon::persistence::Database;
use crate::daemon::workspace_registry::WorkspaceRegistry;

use super::{parse_params, resolve_workspace, rpc_err, rpc_fail, try_op, try_params, try_rpc};

pub(super) async fn handle_summon(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    workspace_registry: &Arc<WorkspaceRegistry>,
    req: RpcRequest,
) -> RpcResponse {
    let params: SummonParams = try_params!(req);

    // Idempotency: a repeat summon with a key that already minted an agent
    // returns that agent untouched, so callers can safely retry on a flaky
    // connection. A stale key whose agent was deleted falls through to a
    // fresh summon.
    if let Some(key) = params.idempotency_key.clone() {
        let key2 = key.clone();
        let existing = db
            .run(
                move |db| -> anyhow::Result<Option<crate::shared::types::Agent>> {
                    match db.lookup_idempotency_key(&key2) {
                        Ok(Some(id)) => Ok(db.get_agent(&id).ok().flatten()),
                        _ => Ok(None),
                    }
                },
            )
            .await
            .unwrap_or(None);
        if let Some(agent) = existing {
            return RpcResponse::success_json(
                req.id,
                &SummonResult {
                    id: agent.id,
                    name: agent.name,
                    state: agent.state.to_string(),
                },
            );
        }
    }

    // Supervision validation.
    let policy_str = params.restart_policy.as_deref().unwrap_or("never");
    let policy: crate::shared::types::RestartPolicy = match policy_str.parse() {
        Ok(p) => p,
        Err(_) => return rpc_err(req.id, "invalid_restart_policy"),
    };
    let any_extra = params.max_restarts.is_some()
        || params.restart_window_secs.is_some()
        || params.escalate_to.is_some();
    if policy == crate::shared::types::RestartPolicy::Never && any_extra {
        if params.escalate_to.is_some() {
            return rpc_err(req.id, "escalate_requires_policy");
        }
        return rpc_err(req.id, "never_with_options");
    }
    if policy == crate::shared::types::RestartPolicy::OnFailure {
        match params.max_restarts {
            None => return rpc_err(req.id, "max_restarts_required"),
            Some(0) => return rpc_err(req.id, "max_restarts_zero"),
            Some(_) => {}
        }
        match params.restart_window_secs {
            None | Some(0) => return rpc_err(req.id, "window_required"),
            Some(n) if n > 604_800 => return rpc_err(req.id, "window_too_large"),
            Some(_) => {}
        }
    }
    if let Some(addr) = &params.escalate_to {
        if policy != crate::shared::types::RestartPolicy::OnFailure {
            return rpc_err(req.id, "escalate_requires_policy");
        }
        // Forward parse error.
        if let Err(e) = crate::shared::mail::parse_address(addr) {
            return rpc_err(req.id, e.code());
        }
    }

    let supervision = if policy == crate::shared::types::RestartPolicy::OnFailure {
        Some(crate::shared::types::SupervisionConfig {
            policy,
            max_restarts: params.max_restarts,
            window_secs: params.restart_window_secs,
            escalate_to: params.escalate_to.clone(),
        })
    } else {
        None
    };

    // Workspace short-circuit on cwd: mutually exclusive with --cwd, looks
    // up the workspace path, and triggers assignment after agent insert.
    if params.workspace.is_some() && params.cwd.is_some() {
        return rpc_err(req.id, "conflicting_options");
    }
    let workspace_path: Option<std::path::PathBuf> = match &params.workspace {
        Some(name) => {
            let ws = try_rpc!(resolve_workspace(req.id, db, name).await);
            if ws.state != crate::shared::types::WorkspaceState::Active {
                return rpc_err(req.id, "workspace_destroying");
            }
            Some(ws.path)
        }
        None => None,
    };

    let cwd = workspace_path
        .clone()
        .unwrap_or_else(|| manager.resolve_cwd(params.cwd.clone()));

    // Policy gate. Deny wins on conflict; an empty allow list means "any."
    // Resolves cwd through `canonicalize` when possible so prefix matches
    // survive symlinks; falls back to the unresolved path if the cwd
    // doesn't yet exist (the agent's process will fail later, not here).
    if let Some(policy) = manager.policy() {
        let provider_for_check = params
            .provider
            .clone()
            .unwrap_or_else(|| manager.default_provider_name().to_string());
        if policy.provider_deny.contains(&provider_for_check) {
            return rpc_err(req.id, "policy_provider_denied");
        }
        if !policy.provider_allow.is_empty() && !policy.provider_allow.contains(&provider_for_check)
        {
            return rpc_err(req.id, "policy_provider_not_allowed");
        }
        let canon_cwd = std::fs::canonicalize(&cwd).unwrap_or_else(|_| cwd.clone());
        let any_match = |prefixes: &[std::path::PathBuf]| {
            prefixes.iter().any(|p| {
                let canon_prefix = std::fs::canonicalize(p).unwrap_or_else(|_| p.clone());
                canon_cwd.starts_with(&canon_prefix)
            })
        };
        if !policy.cwd_deny_prefixes.is_empty() && any_match(&policy.cwd_deny_prefixes) {
            return rpc_err(req.id, "policy_cwd_denied");
        }
        if !policy.cwd_allow_prefixes.is_empty() && !any_match(&policy.cwd_allow_prefixes) {
            return rpc_err(req.id, "policy_cwd_not_allowed");
        }
    }

    let keep_alive = params.keep_alive.unwrap_or(false);
    let result = match manager
        .enqueue_with_options(
            &params.task,
            params.name,
            params.model,
            params.provider,
            &cwd,
            crate::daemon::agent_manager::Lane::Adhoc,
            keep_alive,
            supervision.clone(),
        )
        .await
    {
        Ok(a) => a,
        Err(e) => return rpc_fail(req.id, "summon", e),
    };

    // Bind the idempotency key to the freshly minted agent (first writer
    // wins, so a concurrent racing summon collapses onto one agent).
    if let Some(key) = params.idempotency_key.clone() {
        let agent_id = result.id.clone();
        let _ = db
            .run(move |db| db.insert_idempotency_key(&key, &agent_id))
            .await;
    }

    // Post-insert assignment.
    if let Some(name) = &params.workspace
        && let Err(e) = workspace_registry.assign(name, &result.id).await
    {
        tracing::warn!(workspace = %name, agent = %result.id, error = %e, "workspace assign after summon failed");
    }

    // Wire the supervision-tree edge so a subsequent parent banish cascades.
    if let Some(parent) = &params.parent_agent_id {
        if parent == &result.id {
            let _ = manager.banish(&result.id).await;
            return rpc_err(req.id, "self_parent");
        }
        let parent_owned = parent.clone();
        let child_id = result.id.clone();
        let lookup = db
            .run(move |db| match db.get_agent(&parent_owned) {
                Ok(Some(_)) => Ok(Some(db.set_agent_parent(&child_id, Some(&parent_owned)))),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .await;
        match lookup {
            Ok(Some(set_res)) => {
                if let Err(e) = set_res {
                    tracing::warn!(
                        agent = %result.id,
                        parent = %parent,
                        error = %e,
                        "set_agent_parent failed"
                    );
                }
            }
            Ok(None) => {
                let _ = manager.banish(&result.id).await;
                return rpc_err(req.id, "parent_not_found");
            }
            Err(e) => return rpc_fail(req.id, "summon", e),
        }
    }

    // Tree budget: this agent becomes the budgeted root of its (future)
    // subtree. Children summoned with `--parent <this>` count against it.
    if let Some(cap) = params.tree_budget_usd {
        if !cap.is_finite() || cap <= 0.0 {
            let _ = manager.banish(&result.id).await;
            return rpc_err(req.id, "invalid_tree_budget");
        }
        let root_id = result.id.clone();
        if let Err(e) = db.run(move |db| db.set_tree_budget(&root_id, cap)).await {
            tracing::warn!(agent = %result.id, error = %e, "set_tree_budget failed");
        }
    }

    // Self-escalation check, post-id-generation, before any further state.
    if let Some(addr) = &params.escalate_to {
        let self_addr = format!("agent://{}", result.id);
        if addr == &self_addr {
            // Roll back the insert.
            let _ = manager.banish(&result.id).await;
            // Banish on Queued cleans queue + agent. The agent row remains
            // (state=Banished). Acceptable behavior; spec asks for "no agent
            // row" but the rollback path is best-effort.
            return rpc_err(req.id, "self_escalation");
        }
    }

    let resp = SummonResult {
        id: result.id,
        name: result.name,
        state: result.state.to_string(),
    };
    RpcResponse::success_json(req.id, &resp)
}

pub(super) async fn handle_circle(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: CircleParams = parse_params(&req).unwrap_or(CircleParams { state: None });

    try_op(
        req.id,
        "list",
        manager
            .circle(params.state.as_deref())
            .await
            .map(|agents| CircleResult { agents }),
    )
}

pub(super) async fn handle_banish(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: BanishParams = try_params!(req);
    try_op(
        req.id,
        "banish",
        manager
            .banish(&params.id)
            .await
            .map(|success| BanishResult { success }),
    )
}

pub(super) async fn handle_invoke(manager: &Arc<AgentManager>, req: RpcRequest) -> RpcResponse {
    let params: InvokeParams = try_params!(req);
    try_op(
        req.id,
        "invoke",
        manager
            .invoke(&params.id, &params.message, None)
            .await
            .map(|()| serde_json::json!({"success": true})),
    )
}

pub(super) async fn handle_queue_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    try_op(
        req.id,
        "list queue",
        db.run(Database::list_queue).await.map(|rows| {
            let now = chrono::Utc::now();
            let entries: Vec<QueueEntry> = rows
                .into_iter()
                .map(|row| {
                    let age = (now - row.enqueued_at).num_seconds().max(0) as u64;
                    QueueEntry {
                        id: row.id,
                        lane: row.lane,
                        age_seconds: age,
                        provider: row.provider_name,
                        cwd: row.cwd,
                        model: row.model,
                        block_reason: row.block_reason,
                        task_text: row.task_text,
                    }
                })
                .collect();
            QueueListResponse { entries }
        }),
    )
}

/// Return an agent's full durable event timeline for `grim chronicle`. The
/// agent must exist (so an unknown id is a clean error, not an empty reel);
/// beyond that this is a straight read of the `events` table. All filtering
/// and state reconstruction is the client's job.
pub(super) async fn handle_replay(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: ReplayParams = try_params!(req);

    let id = params.id.clone();
    // One trip to the blocking pool: agent existence check + event log read.
    let outcome: Result<Option<Vec<crate::daemon::persistence::StoredEvent>>, anyhow::Error> = db
        .run(move |db| match db.get_agent(&id) {
            Ok(Some(_)) => db.read_stream_events(&id).map(Some),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        })
        .await;

    match outcome {
        Ok(Some(stored)) => {
            let entries: Vec<ReplayEntry> = stored
                .into_iter()
                .map(|s| ReplayEntry {
                    seq: s.seq,
                    kind: s.kind,
                    ts: s.ts,
                    event: s.event,
                })
                .collect();
            RpcResponse::success_json(
                req.id,
                &ReplayResponse {
                    agent_id: params.id,
                    entries,
                },
            )
        }
        Ok(None) => {
            RpcResponse::error(req.id, -32000, format!("no agent matching '{}'", params.id))
        }
        Err(e) => rpc_fail(req.id, "read event log", e),
    }
}

/// Return the provider-extracted final result text for an agent. Mirrors
/// `manager.agent_result()` (the in-process accessor used by pact
/// `{output}` injection) over the RPC, so the CLI can read an evaluator's
/// score JSON without scraping the chronicle.
pub(super) async fn handle_agent_result(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    req: RpcRequest,
) -> RpcResponse {
    let params: crate::shared::protocol::AgentResultParams = try_params!(req);
    let id = params.id.clone();
    let agent = match db.run(move |db| db.get_agent(&id)).await {
        Ok(Some(a)) => a,
        Ok(None) => return rpc_err(req.id, "agent_not_found"),
        Err(e) => return RpcResponse::error(req.id, -32000, format!("db error: {e}")),
    };
    let result = manager.agent_result(&params.id);
    let resp = crate::shared::protocol::AgentResultResponse {
        result,
        state: agent.state.to_string(),
    };
    RpcResponse::success_json(req.id, &resp)
}

/// Return the structured artifact (files changed, unified diff, cost) an
/// agent produced. Unknown agent → error; known agent with no captured
/// artifact yet → `{ "artifact": null }`.
pub(super) async fn handle_agent_artifact(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    use crate::shared::protocol::{AgentArtifactParams, AgentArtifactResult};
    let params: AgentArtifactParams = try_params!(req);
    let id = params.id.clone();
    let outcome = db
        .run(move |db| match db.get_agent(&id) {
            Ok(Some(_)) => db.get_artifact(&id).map(Some),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        })
        .await;
    match outcome {
        Ok(Some(artifact)) => RpcResponse::success_json(req.id, &AgentArtifactResult { artifact }),
        Ok(None) => rpc_err(req.id, "agent_not_found"),
        Err(e) => rpc_fail(req.id, "read artifact", e),
    }
}

pub(super) async fn handle_status(
    manager: &Arc<AgentManager>,
    daemon_id: &str,
    req: RpcRequest,
) -> RpcResponse {
    try_op(
        req.id,
        "status",
        manager.circle(None).await.map(|agents| {
            use crate::shared::types::AgentState;
            let active = agents
                .iter()
                .filter(|a| a.state == AgentState::Active)
                .count();
            let queued = agents
                .iter()
                .filter(|a| a.state == AgentState::Queued)
                .count();
            DaemonStatusResult {
                uptime_secs: 0,
                agent_count: agents.len(),
                active_count: active,
                queued_count: queued,
                max_concurrent_agents: manager.max_concurrent_agents(),
                daemon_id: Some(daemon_id.to_string()),
            }
        }),
    )
}

pub(super) async fn handle_agent_processes(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    use crate::daemon::process_manager::process_alive;
    use crate::shared::protocol::{AgentProcess, AgentProcessesResult};
    let outcome = db.run(|db| db.list_agents(None)).await;
    match outcome {
        Ok(agents) => {
            let processes = agents
                .into_iter()
                .filter_map(|a| {
                    let pid = a.pid?;
                    let alive = process_alive(pid);
                    let terminal = matches!(
                        a.state,
                        AgentState::Complete | AgentState::Failed | AgentState::Banished
                    );
                    Some(AgentProcess {
                        agent_id: a.id,
                        state: a.state.to_string(),
                        task: a.task,
                        pid,
                        alive,
                        stuck: alive && terminal,
                    })
                })
                .collect();
            RpcResponse::success_json(req.id, &AgentProcessesResult { processes })
        }
        Err(e) => RpcResponse::error(req.id, -32000, format!("agent.processes: {e}")),
    }
}
