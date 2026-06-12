//! Miscellaneous RPC handlers: operator notifications, budget listing, and
//! rubric-eval record/list/scores.

use std::sync::Arc;

use crate::shared::protocol::*;

use crate::daemon::agent_manager::AgentManager;
use crate::daemon::event_bus::EventBus;
use crate::daemon::persistence::Database;

use super::{parse_params, rpc_err, try_params};

// --- Notify handler ---

/// Publish an operator-facing notification onto the event bus. The `Notifier`
/// subscriber forwards it to the configured webhook; it also lands in the
/// durable event log. Decoupling via the bus keeps the RPC layer free of any
/// HTTP/notifier dependency.
pub(super) fn handle_notify(bus: &EventBus, req: RpcRequest) -> RpcResponse {
    let params: NotifyParams = try_params!(req);
    if params.message.trim().is_empty() {
        return RpcResponse::error(req.id, -32602, "notify: message must not be empty".into());
    }
    let level = params.level.unwrap_or_else(|| "info".to_string());
    bus.publish(StreamEvent::Notification {
        agent_id: params.agent_id,
        message: params.message,
        level,
        source: "agent".to_string(),
    });
    RpcResponse::success_json(req.id, &NotifyResult { published: true })
}

/// Persist one rubric-scored evaluation, attributing the verdict from
/// `evaluator_id` to `target_id`. Idempotency is per-call (each insert
/// mints a new row id); callers that want dedupe should do so client-side.
pub(super) async fn handle_eval_record(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: crate::shared::protocol::EvalRecordParams = try_params!(req);
    let target_id = params.target_id.clone();
    let evaluator_id = params.evaluator_id.clone();
    let verdict = params.verdict.clone();
    let rationale = params.rationale.clone();
    let score = params.score;
    let outcome = db
        .run(
            move |db| -> Result<Option<anyhow::Result<String>>, anyhow::Error> {
                if db.get_agent(&target_id)?.is_none() {
                    return Ok(None);
                }
                Ok(Some(db.insert_eval_result(
                    &target_id,
                    &evaluator_id,
                    score,
                    verdict.as_deref(),
                    rationale.as_deref(),
                )))
            },
        )
        .await;
    match outcome {
        Ok(None) => rpc_err(req.id, "target_not_found"),
        Ok(Some(Ok(id))) => {
            RpcResponse::success_json(req.id, &crate::shared::protocol::EvalRecordResult { id })
        }
        Ok(Some(Err(e))) => RpcResponse::error(req.id, -32000, format!("insert_eval_result: {e}")),
        Err(e) => RpcResponse::error(req.id, -32000, format!("db: {e}")),
    }
}

pub(super) async fn handle_eval_list(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let params: crate::shared::protocol::EvalListParams = try_params!(req);
    let target_id = params.target_id.clone();
    let rows = match db.run(move |db| db.list_eval_results(&target_id)).await {
        Ok(r) => r,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("list_eval_results: {e}")),
    };
    let results = rows
        .into_iter()
        .map(|r| crate::shared::protocol::EvalRecord {
            id: r.id,
            target_id: r.target_id,
            evaluator_id: r.evaluator_id,
            score: r.score,
            verdict: r.verdict,
            rationale: r.rationale,
            created_at: r.created_at,
        })
        .collect();
    RpcResponse::success_json(req.id, &crate::shared::protocol::EvalListResult { results })
}

/// Latest score per evaluated target across the whole circle. Lets the CLI
/// (`grim circle --eval-score-lt`) filter and decorate without an N+1 fanout.
pub(super) async fn handle_eval_scores(db: &Arc<Database>, req: RpcRequest) -> RpcResponse {
    let rows = match db.run(Database::latest_eval_scores_all).await {
        Ok(r) => r,
        Err(e) => return RpcResponse::error(req.id, -32000, format!("latest_eval_scores: {e}")),
    };
    let scores = rows
        .into_iter()
        .map(|(target_id, score)| crate::shared::protocol::EvalScoreEntry { target_id, score })
        .collect();
    RpcResponse::success_json(
        req.id,
        &crate::shared::protocol::EvalScoresResult { scores },
    )
}

/// Snapshot every configured budget with its USD cap and today's running
/// spend. Read-only; runs against the same `budget_spend` rows the
/// dispatch-time gate consults.
pub(super) async fn handle_budget_list(
    manager: &Arc<AgentManager>,
    db: &Arc<Database>,
    req: RpcRequest,
) -> RpcResponse {
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let budget_meta: Vec<(String, crate::shared::config::BudgetConfig)> = manager
        .budgets()
        .iter()
        .map(|(n, b)| (n.clone(), b.clone()))
        .collect();
    let today_for_db = today.clone();
    let spend_meta = budget_meta.clone();
    let spends: Vec<f64> = db
        .run(move |db| {
            spend_meta
                .iter()
                .map(|(name, _)| db.get_budget_spend(name, &today_for_db).unwrap_or(0.0))
                .collect()
        })
        .await;
    let mut budgets: Vec<crate::shared::protocol::BudgetStatus> = budget_meta
        .into_iter()
        .zip(spends)
        .map(
            |((name, b), spent_usd)| crate::shared::protocol::BudgetStatus {
                name,
                daily_usd: b.daily_usd,
                spent_usd,
                providers: b.providers.clone(),
                hard: b.hard,
            },
        )
        .collect();
    budgets.sort_by(|a, b| a.name.cmp(&b.name));
    let result = crate::shared::protocol::BudgetListResult {
        day: today,
        budgets,
    };
    RpcResponse::success_json(req.id, &result)
}
