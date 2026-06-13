//! Run lifecycle: completion monitoring + token/USD attribution, artifact
//! capture, dormant resume (`invoke`), and banish (with wake/supervisor/child
//! cascades).

use anyhow::{Result, anyhow};
use chrono::Utc;
use std::sync::Arc;
use tracing::{error, info};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{AgentId, AgentState, RestartHistoryOutcome};

use super::super::executor::{ExecuteRequest, ExecutorHandle};
use super::super::process_manager;
use super::super::provider::ResumeStrategy;
use super::{AgentManager, CONTEXT_REPLAY_BUDGET_BYTES};

impl AgentManager {
    /// Wire an executor's completion future into agent state updates and the
    /// event bus, mirroring the previous spawn_monitor body.
    pub(crate) fn watch_completion(
        self: &Arc<Self>,
        agent_id: AgentId,
        completion: tokio::task::JoinHandle<process_manager::MonitorResult>,
    ) -> tokio::task::JoinHandle<()> {
        let db = self.db.clone();
        let bus = self.event_bus.clone();
        let manager = self.clone();
        // One span per agent run, following the OTel GenAI semantic
        // conventions (`gen_ai.*`) so OTLP consumers (Langfuse, Phoenix,
        // Jaeger dashboards keyed on the semconv) can read runs without a
        // translation layer. Usage fields are recorded at completion.
        let span = tracing::info_span!(
            "invoke_agent",
            gen_ai.operation.name = "invoke_agent",
            gen_ai.agent.id = %agent_id,
            gen_ai.agent.name = tracing::field::Empty,
            gen_ai.provider.name = tracing::field::Empty,
            gen_ai.request.model = tracing::field::Empty,
            gen_ai.conversation.id = tracing::field::Empty,
            gen_ai.usage.input_tokens = tracing::field::Empty,
            gen_ai.usage.output_tokens = tracing::field::Empty,
        );
        if let Ok(Some(row)) = self.db.get_agent(&agent_id) {
            if let Some(name) = &row.name {
                span.record("gen_ai.agent.name", tracing::field::display(name));
            }
            if let Some(provider) = &row.provider {
                span.record("gen_ai.provider.name", tracing::field::display(provider));
            }
            if let Some(model) = &row.model {
                span.record("gen_ai.request.model", tracing::field::display(model));
            }
        }
        let run_span = span.clone();
        tokio::spawn(tracing::Instrument::instrument(
            async move {
                let mut result = match completion.await {
                    Ok(r) => r,
                    Err(e) => {
                        error!(agent_id = %agent_id, error = %e, "executor completion task panicked");
                        process_manager::MonitorResult {
                            error_reason: Some(format!("completion_panicked: {e}")),
                            ..Default::default()
                        }
                    }
                };

                if let Some(b) = result.token_breakdown {
                    run_span.record("gen_ai.usage.input_tokens", b.input);
                    run_span.record("gen_ai.usage.output_tokens", b.output);
                }
                if let Some(ref sid) = result.session_id {
                    run_span.record("gen_ai.conversation.id", tracing::field::display(sid));
                }

                if let Some(ref sid) = result.session_id
                    && let Err(e) = db.update_agent_session_id(&agent_id, sid)
                {
                    error!(agent_id = %agent_id, error = %e, "Failed to store session_id");
                }

                // keep-alive agents that complete normally with a session land
                // in Dormant instead of Complete.
                if matches!(result.state, AgentState::Complete) {
                    let keep_alive = db.get_keep_alive(&agent_id).unwrap_or(false);
                    if keep_alive {
                        if result.session_id.is_some() {
                            result.state = AgentState::Dormant;
                        } else if manager.provider_resume_strategy(&agent_id)
                            == Some(ResumeStrategy::ContextReplay)
                        {
                            // No native session, but the provider supports
                            // daemon-managed continuity: mint a synthetic session id
                            // so the agent goes Dormant and the scheduler will wake it.
                            // Continuity is reconstructed from the event log on resume.
                            let sid = format!("daemon:{}", uuid::Uuid::new_v4());
                            if let Err(e) = db.update_agent_session_id(&agent_id, &sid) {
                                error!(agent_id = %agent_id, error = %e, "Failed to store synthetic session_id");
                            } else {
                                result.session_id = Some(sid);
                                result.state = AgentState::Dormant;
                            }
                        } else {
                            tracing::warn!(
                                agent_id = %agent_id,
                                "keep_alive set but no session_id; completing as Complete"
                            );
                        }
                    }
                }

                if let Some(tokens) = result.tokens_used
                    && tokens > 0
                {
                    match db.add_agent_tokens(&agent_id, tokens) {
                        Ok(total) => {
                            tracing::info!(
                                agent_id = %agent_id,
                                tokens_this_run = tokens,
                                tokens_total = total,
                                "Recorded token usage"
                            );
                        }
                        Err(e) => {
                            error!(agent_id = %agent_id, error = %e, "Failed to record token usage");
                        }
                    }
                }

                // USD attribution. Compute spend from the breakdown × provider
                // pricing, then charge the agent's lifetime spend AND every
                // budget whose `providers` list matches this run's provider.
                // No pricing or no breakdown → no charge, by design (free models
                // and providers without usage telemetry are silently
                // un-budget-able).
                if let Some(provider_name) = db
                    .get_agent(&agent_id)
                    .ok()
                    .flatten()
                    .and_then(|a| a.provider)
                    && let Some(pricing) = manager.registry.pricing_for(&provider_name)
                {
                    let breakdown = result.token_breakdown.unwrap_or_else(|| {
                        // No breakdown but we may still have a total. Charge it
                        // as input tokens (conservative; vendors price input
                        // cheaper than output, so this *under*-bills slightly).
                        crate::daemon::provider::TokenBreakdown {
                            input: result.tokens_used.unwrap_or(0),
                            ..Default::default()
                        }
                    });
                    let usd = breakdown.cost_usd(&pricing);
                    if usd > 0.0 {
                        if let Err(e) = db.add_agent_usd(&agent_id, usd) {
                            error!(agent_id = %agent_id, error = %e, "Failed to record USD spend");
                        }
                        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
                        for (name, b) in &manager.config.budgets {
                            let matches =
                                b.providers.is_empty() || b.providers.contains(&provider_name);
                            if !matches {
                                continue;
                            }
                            match db.add_budget_spend(name, &today, usd) {
                                Ok(total) => tracing::info!(
                                    budget = %name,
                                    provider = %provider_name,
                                    charged_usd = usd,
                                    day_total_usd = total,
                                    daily_cap_usd = b.daily_usd,
                                    "Charged budget"
                                ),
                                Err(e) => {
                                    error!(budget = %name, error = %e, "Failed to charge budget");
                                }
                            }
                        }
                    }
                }

                // Detect tree-budget exhaustion at attribution time (not just
                // at the next dispatch attempt) so the operator notification
                // fires as soon as spend crosses the cap. Return value
                // ignored: this run already happened.
                let _ = manager.tree_budget_block(&agent_id);

                if let Err(e) = db.update_agent_state(&agent_id, &result.state, result.exit_code) {
                    error!(agent_id = %agent_id, error = %e, "Failed to update agent state");
                }

                // Supervision history reconciliation: if there's a scheduled
                // history row for this agent, flip it based on the final state.
                // Only Complete bumps restart_count; Failed just records outcome.
                let outcome = match result.state {
                    AgentState::Complete => Some(RestartHistoryOutcome::Succeeded),
                    AgentState::Failed => Some(RestartHistoryOutcome::FailedAgain),
                    _ => None,
                };
                if let Some(outcome) = outcome {
                    let updated = db
                        .update_latest_scheduled_outcome(&agent_id, outcome)
                        .unwrap_or(0);
                    if updated > 0 && result.state == AgentState::Complete {
                        let _ = db.bump_restart_count(&agent_id);
                    }
                }

                let mut agents = manager.agents.lock().await;
                if let Some(managed) = agents.get_mut(&agent_id) {
                    managed.agent.state = result.state.clone();
                    managed.agent.exit_code = result.exit_code;
                    if let Some(ref sid) = result.session_id {
                        managed.agent.session_id = Some(sid.clone());
                    }
                    managed.cancel = None;
                }

                // Capture the diff/cost artifact before announcing the
                // terminal state, so consumers reacting to the StateChange
                // (scroll verification, fork-and-race, approval review) find
                // the artifact already on disk.
                manager.capture_artifact(&agent_id).await;

                bus.publish(StreamEvent::StateChange {
                    agent_id,
                    old_state: AgentState::Active,
                    new_state: result.state.clone(),
                });
            },
            span,
        ))
    }

    /// Look up an agent's provider and report its resume strategy, if both the
    /// agent and its provider are known.
    pub(crate) fn provider_resume_strategy(&self, agent_id: &str) -> Option<ResumeStrategy> {
        let provider_name = self.db.get_agent(agent_id).ok().flatten()?.provider?;
        Some(self.registry.get(&provider_name)?.resume_strategy())
    }

    /// Capture the per-agent artifact (git diff + cost) after a run. Reads
    /// the agent's cwd and the base commit recorded at dispatch, computes
    /// the diff on the blocking pool (git shellouts), and upserts the row.
    /// Best-effort: any failure is logged and swallowed — a missing artifact
    /// must never disturb the agent's lifecycle.
    pub(crate) async fn capture_artifact(&self, agent_id: &str) {
        let Ok(Some(agent)) = self.db.get_agent(agent_id) else {
            return;
        };
        let cwd = agent.cwd.clone();
        let base = self.db.get_artifact_base(agent_id).unwrap_or(None);
        let tokens = self.db.get_agent_tokens(agent_id).unwrap_or(0);
        let usd = self.db.get_agent_usd(agent_id).unwrap_or(0.0);
        let id = agent_id.to_string();
        let captured_at = Utc::now().timestamp();
        let artifact = tokio::task::spawn_blocking(move || {
            super::super::artifacts::compute(&id, &cwd, base.as_deref(), tokens, usd, captured_at)
        })
        .await;
        match artifact {
            Ok(a) => {
                if let Err(e) = self.db.upsert_artifact(&a) {
                    tracing::warn!(agent_id = %agent_id, error = %e, "failed to persist artifact");
                }
            }
            Err(e) => {
                tracing::warn!(agent_id = %agent_id, error = %e, "artifact capture task failed");
            }
        }
    }

    pub async fn invoke(
        self: &Arc<Self>,
        id: &str,
        message: &str,
        model: Option<String>,
    ) -> Result<()> {
        // Every resume path (mail wake, wake sources, manual `grim invoke`)
        // funnels through here, so this one gate pauses a whole exhausted
        // tree: dormant members stay dormant, their pending mail stays
        // pending, and a re-budget (`set_tree_budget`) lets it all flow again.
        if let Some(reason) = self.tree_budget_block(id) {
            return Err(anyhow!(reason));
        }
        let (session_id, cwd, provider_name) = {
            let agents = self.agents.lock().await;
            let managed = agents
                .get(id)
                .ok_or_else(|| anyhow!("Agent not found: {id}"))?;

            if managed.agent.state != AgentState::Dormant {
                return Err(anyhow!(
                    "Agent {} is not dormant (state: {})",
                    id,
                    managed.agent.state
                ));
            }

            let session_id = managed
                .agent
                .session_id
                .clone()
                .ok_or_else(|| anyhow!("Agent {id} has no session to resume"))?;

            (
                session_id,
                managed.agent.cwd.clone(),
                managed
                    .agent
                    .provider
                    .clone()
                    .unwrap_or_else(|| "claude".to_string()),
            )
        };

        let provider = self
            .registry
            .get(&provider_name)
            .ok_or_else(|| anyhow!("Unknown provider: {provider_name}"))?;

        // How we resume depends on the provider:
        //   Native        → hand the message to the CLI's own session resume.
        //   ContextReplay  → reconstruct prior output from the event log, prepend
        //                    it to the message, and start a fresh process.
        let (task, resume_session_id) = match provider.resume_strategy() {
            ResumeStrategy::Native => (message.to_string(), Some(session_id)),
            ResumeStrategy::ContextReplay => {
                let transcript = self
                    .db
                    .get_agent_transcript(id, CONTEXT_REPLAY_BUDGET_BYTES)
                    .unwrap_or_default();
                let task = if transcript.trim().is_empty() {
                    message.to_string()
                } else {
                    format!(
                        "## Prior context\n\nYou are a standing agent resuming. This is your \
                         earlier output in this working directory:\n\n{transcript}\n\n\
                         ## Current request\n\n{message}"
                    )
                };
                (task, None)
            }
        };

        self.db.update_agent_state(id, &AgentState::Active, None)?;

        self.event_bus.publish(StreamEvent::StateChange {
            agent_id: id.to_string(),
            old_state: AgentState::Dormant,
            new_state: AgentState::Active,
        });

        let req = ExecuteRequest {
            agent_id: id.to_string(),
            task,
            provider_name,
            cwd,
            model,
            resume_session_id,
        };

        let handle = self.executor.start(req).await?;
        let ExecutorHandle {
            pid,
            cancel,
            completion,
            worker_id: _,
        } = handle;

        if let Some(p) = pid {
            self.db.update_agent_pid(id, p)?;
        }

        {
            let mut agents = self.agents.lock().await;
            if let Some(managed) = agents.get_mut(id) {
                managed.agent.state = AgentState::Active;
                managed.agent.pid = pid;
                managed.cancel = Some(cancel);
            }
        }

        let completion_handle = self.watch_completion(id.to_string(), completion);

        {
            let mut agents = self.agents.lock().await;
            if let Some(managed) = agents.get_mut(id) {
                managed.completion_handle = Some(completion_handle);
            }
        }

        info!(id = %id, message = %message, "Agent invoked");
        Ok(())
    }

    pub async fn banish(&self, id: &str) -> Result<bool> {
        let result = self.banish_inner(id).await?;
        if result {
            // Cascade: retire wake sources after the state flip. Errors are
            // logged but don't fail the banish (banish must always succeed).
            if let Some(reg) = self.wake_registry.lock().await.clone()
                && let Err(e) = reg.retire_for_agent(id).await
            {
                tracing::warn!(agent_id = %id, error = %e, "wake source retire on banish failed");
            }
            // Cascade: cancel any pending restart and clear supervision.
            if let Some(sup) = self.supervisor.lock().await.clone()
                && let Err(e) = sup.cancel_pending(id).await
            {
                tracing::warn!(agent_id = %id, error = %e, "supervisor cancel_pending on banish failed");
            }
            if let Err(e) = self.db.clear_supervision(id) {
                tracing::warn!(agent_id = %id, error = %e, "clear_supervision on banish failed");
            }
            // Cascade: supervision-tree children die with their parent.
            // Recursive: each child's `banish` likewise cascades to its own
            // children, so the whole subtree collapses in one call.
            match self.db.list_live_children(id) {
                Ok(children) => {
                    for child in children {
                        if let Err(e) = Box::pin(self.banish(&child)).await {
                            tracing::warn!(
                                parent = %id,
                                child = %child,
                                error = %e,
                                "cascade banish of child failed"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(parent = %id, error = %e, "list_live_children failed");
                }
            }
        }
        Ok(result)
    }

    async fn banish_inner(&self, id: &str) -> Result<bool> {
        let mut agents = self.agents.lock().await;
        let Some(managed) = agents.get_mut(id) else {
            return Ok(false);
        };

        match managed.agent.state {
            AgentState::Queued => {
                // Queued agents have no process and no executor handle. Just
                // dequeue and flip to Banished. `delete_from_queue` is
                // idempotent: if the scheduler already claimed the row (state
                // would have moved to Summoning, so we wouldn't be here), the
                // false return is harmless.
                self.db.delete_from_queue(&id.to_string())?;
                managed.agent.state = AgentState::Banished;
                self.db
                    .update_agent_state(id, &AgentState::Banished, None)?;

                self.event_bus.publish(StreamEvent::StateChange {
                    agent_id: id.to_string(),
                    old_state: AgentState::Queued,
                    new_state: AgentState::Banished,
                });

                info!(id = %id, "Queued agent banished");
                Ok(true)
            }
            AgentState::Active | AgentState::Summoning => {
                let old_state = managed.agent.state.clone();
                if let Some(cancel) = managed.cancel.take() {
                    cancel();
                } else if let Some(pid) = managed.agent.pid
                    && let Err(e) = process_manager::kill_process(pid)
                {
                    error!(agent_id = %id, error = %e, "Failed to kill agent process");
                }

                managed.agent.state = AgentState::Banished;
                self.db
                    .update_agent_state(id, &AgentState::Banished, None)?;

                self.event_bus.publish(StreamEvent::StateChange {
                    agent_id: id.to_string(),
                    old_state,
                    new_state: AgentState::Banished,
                });

                info!(id = %id, "Agent banished");
                Ok(true)
            }
            AgentState::Dormant => {
                // No live process; just retire the agent.
                managed.agent.state = AgentState::Banished;
                self.db
                    .update_agent_state(id, &AgentState::Banished, None)?;
                self.event_bus.publish(StreamEvent::StateChange {
                    agent_id: id.to_string(),
                    old_state: AgentState::Dormant,
                    new_state: AgentState::Banished,
                });
                info!(id = %id, "Dormant agent banished");
                Ok(true)
            }
            AgentState::Restarting => {
                // No live process and no executor handle. The supervisor's
                // pending heap is cancelled by the outer cascade.
                managed.agent.state = AgentState::Banished;
                self.db
                    .update_agent_state(id, &AgentState::Banished, None)?;
                self.event_bus.publish(StreamEvent::StateChange {
                    agent_id: id.to_string(),
                    old_state: AgentState::Restarting,
                    new_state: AgentState::Banished,
                });
                info!(id = %id, "Restarting agent banished");
                Ok(true)
            }
            AgentState::Complete | AgentState::Failed | AgentState::Banished => Ok(false),
        }
    }
}
