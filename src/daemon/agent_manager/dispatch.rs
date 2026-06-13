//! Queue enqueue, scheduler-driven dispatch, budget gating, restart dispatch,
//! and the scheduler/supervisor trait impls (`Dispatcher`, `MailWaker`,
//! `RestartDispatcher`).

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Agent, AgentState, RestartPolicy};

use super::super::executor::{ExecuteRequest, ExecutorHandle};
use super::super::persistence::QueueRow;
use super::super::scheduler::{Dispatcher, MailWaker};
use super::super::supervisor::RestartDispatcher;
use super::{AgentManager, Lane, ManagedAgent};

impl AgentManager {
    /// Enqueue an agent for the scheduler to pick up. Inserts the agent in
    /// `Queued` state and writes a row into `task_queue`; does NOT start the
    /// executor (that is the scheduler's job, handled by
    /// `AgentManager::dispatch_internal`).
    pub async fn enqueue(
        self: &Arc<Self>,
        task: &str,
        name: Option<String>,
        model: Option<String>,
        provider_name: Option<String>,
        cwd: &Path,
        lane: Lane,
    ) -> Result<Agent> {
        self.enqueue_with_options(task, name, model, provider_name, cwd, lane, false, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_with_options(
        self: &Arc<Self>,
        task: &str,
        name: Option<String>,
        model: Option<String>,
        provider_name: Option<String>,
        cwd: &Path,
        lane: Lane,
        keep_alive: bool,
        supervision: Option<crate::shared::types::SupervisionConfig>,
    ) -> Result<Agent> {
        let provider_name =
            provider_name.unwrap_or_else(|| self.registry.default_name().to_string());

        let agent_id = crate::shared::constants::generate_short_id();
        let cwd_path = cwd.to_path_buf();
        let model = model.or_else(|| self.config.agent.default_model.clone());
        let now = Utc::now();

        let agent = Agent {
            id: agent_id.clone(),
            name: name.clone(),
            state: AgentState::Queued,
            task: Some(task.to_string()),
            model: model.clone(),
            provider: Some(provider_name.clone()),
            cwd: cwd_path.clone(),
            pid: None,
            session_id: None,
            exit_code: None,
            created_at: now,
            updated_at: now,
            worker_id: None,
            restart_policy: RestartPolicy::Never,
            restart_count: 0,
            workspace_id: None,
        };

        self.db.insert_agent(&agent)?;
        if keep_alive {
            let _ = self.db.set_keep_alive(&agent.id, true);
        }
        if let Some(cfg) = &supervision {
            let _ = self.db.set_supervision(&agent.id, cfg);
        }

        let row = QueueRow {
            id: agent_id.clone(),
            lane: lane.as_str().to_string(),
            priority: 0,
            enqueued_at: now,
            provider_name: Some(provider_name.clone()),
            cwd: cwd_path.to_string_lossy().to_string(),
            model: model.clone(),
            task_text: task.to_string(),
            block_reason: None,
        };
        self.db.enqueue_task(&row)?;

        self.event_bus.publish(StreamEvent::AgentCreated {
            agent: agent.clone(),
        });
        self.event_bus.publish(StreamEvent::AgentQueued {
            agent_id: agent_id.clone(),
            lane: lane.as_str().to_string(),
            block_reason: None,
        });

        let mut agents = self.agents.lock().await;
        agents.insert(
            agent_id,
            ManagedAgent {
                agent: agent.clone(),
                cancel: None,
                completion_handle: None,
            },
        );

        info!(id = %agent.id, task = %task, lane = %lane.as_str(), "Agent enqueued");
        Ok(agent)
    }

    /// Drive a claimed queue row through `executor.start` and the
    /// `Summoning -> Active` transition. Called only by the scheduler after a
    /// Tree-budget gate. `None` = proceed; `Some(reason)` = the supervision
    /// tree this agent belongs to has spent its USD cap, so no further run
    /// may start anywhere in the tree (queue dispatch, mail wake, manual
    /// invoke — every path funnels through here). The operator notification
    /// fires exactly once per exhaustion, on whichever check observes it
    /// first. DB errors fail open: a broken budget lookup must not stop the
    /// fleet.
    pub(crate) fn tree_budget_block(&self, agent_id: &str) -> Option<String> {
        let root = self.db.find_tree_root(agent_id).ok()?;
        let (cap, _) = self.db.get_tree_budget(&root).ok().flatten()?;
        let spent = self.db.tree_spend_usd(&root).unwrap_or(0.0);
        if spent < cap {
            return None;
        }
        if self.db.mark_tree_budget_exhausted(&root).unwrap_or(false) {
            self.event_bus.publish(StreamEvent::Notification {
                agent_id: Some(root.clone()),
                message: format!(
                    "tree budget exhausted: tree {root} spent ${spent:.4} >= cap ${cap:.4}; \
                     dispatches and wakes in this tree are blocked"
                ),
                level: "error".to_string(),
                source: "system".to_string(),
            });
        }
        Some(format!(
            "tree_budget_exhausted: tree {root} spent ${spent:.4} >= ${cap:.4}"
        ))
    }

    /// successful `claim_for_dispatch`. On failure, returns `Err` *without*
    /// mutating queue state, since the scheduler owns the requeue path so the
    /// row's original `enqueued_at` (and therefore lane fairness) is preserved.
    pub(crate) async fn dispatch_internal(self: &Arc<Self>, row: QueueRow) -> Result<()> {
        let agent_id = row.id.clone();
        let provider_name = row
            .provider_name
            .clone()
            .unwrap_or_else(|| self.registry.default_name().to_string());

        // Token-budget gate: if the provider sandbox caps lifetime token
        // spend and we're already past it, refuse to start another turn and
        // banish with a clear reason instead of silently re-running.
        if let Some(sb) = self.registry.sandbox_for(&provider_name)
            && let Some(budget) = sb.token_budget
        {
            let used = self.db.get_agent_tokens(&agent_id).unwrap_or(0);
            if used >= budget {
                let reason = format!("token_budget_exceeded: used {used} >= budget {budget}");
                tracing::warn!(agent_id = %agent_id, %reason, "Refusing dispatch");
                let _ = self
                    .db
                    .update_agent_state(&agent_id, &AgentState::Banished, None);
                return Err(anyhow::anyhow!(reason));
            }
        }

        // Daily USD budget gate. For every budget that includes this
        // provider, check today's running spend. Hard budgets refuse
        // dispatch; soft budgets only log. Unlike the token gate above,
        // this does NOT banish. The next day will let work through, so we
        // requeue-via-Err and the scheduler retries naturally.
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        for (name, b) in &self.config.budgets {
            let matches = b.providers.is_empty() || b.providers.contains(&provider_name);
            if !matches {
                continue;
            }
            let spent = self.db.get_budget_spend(name, &today).unwrap_or(0.0);
            if spent < b.daily_usd {
                continue;
            }
            if b.hard {
                let reason = format!(
                    "budget_exhausted: '{name}' spent ${spent:.4} >= ${:.4} today",
                    b.daily_usd
                );
                tracing::warn!(
                    agent_id = %agent_id,
                    budget = %name,
                    %reason,
                    "Refusing dispatch"
                );
                return Err(anyhow::anyhow!(reason));
            }
            tracing::warn!(
                agent_id = %agent_id,
                budget = %name,
                spent_usd = spent,
                daily_cap_usd = b.daily_usd,
                "Soft budget exceeded; dispatching anyway"
            );
        }

        // Tree-budget gate: refuses dispatch anywhere in a tree whose USD
        // cap is spent. Requeue-via-Err like the daily gate (the block is
        // visible via `grim queue`'s block_reason).
        if let Some(reason) = self.tree_budget_block(&agent_id) {
            tracing::warn!(agent_id = %agent_id, %reason, "Refusing dispatch");
            return Err(anyhow::anyhow!(reason));
        }

        // Record the baseline commit the completion-time artifact diffs
        // against. Best-effort: a non-git cwd records `None`.
        let base = super::super::artifacts::head_commit(&PathBuf::from(&row.cwd));
        let _ = self.db.set_artifact_base(&agent_id, base.as_deref());

        let req = ExecuteRequest {
            agent_id: agent_id.clone(),
            task: row.task_text.clone(),
            provider_name,
            cwd: PathBuf::from(&row.cwd),
            model: row.model.clone(),
            resume_session_id: None,
        };

        let handle = self.executor.start(req).await?;
        let ExecutorHandle {
            pid,
            cancel,
            completion,
            worker_id: _,
        } = handle;

        if let Some(p) = pid {
            self.db.update_agent_pid(&agent_id, p)?;
        }
        self.db
            .update_agent_state(&agent_id, &AgentState::Active, None)?;

        self.event_bus.publish(StreamEvent::StateChange {
            agent_id: agent_id.clone(),
            old_state: AgentState::Summoning,
            new_state: AgentState::Active,
        });

        let completion_handle = self.watch_completion(agent_id.clone(), completion);

        let mut agents = self.agents.lock().await;
        if !agents.contains_key(&agent_id) {
            // Rare path: scheduler ticked before `reload_from_db` populated
            // the in-memory map (e.g., a `Queued` row recovered after
            // restart). Backfill the entry from the queue row + DB state.
            agents.insert(
                agent_id.clone(),
                ManagedAgent {
                    agent: Agent {
                        id: agent_id.clone(),
                        name: None,
                        state: AgentState::Active,
                        task: Some(row.task_text.clone()),
                        model: row.model.clone(),
                        provider: row.provider_name.clone(),
                        cwd: PathBuf::from(&row.cwd),
                        pid,
                        session_id: None,
                        exit_code: None,
                        created_at: row.enqueued_at,
                        updated_at: Utc::now(),
                        worker_id: None,
                        restart_policy: RestartPolicy::Never,
                        restart_count: 0,
                        workspace_id: None,
                    },
                    cancel: None,
                    completion_handle: None,
                },
            );
        }
        let managed = agents
            .get_mut(&agent_id)
            .expect("just inserted or pre-existing");
        managed.agent.state = AgentState::Active;
        managed.agent.pid = pid;
        managed.cancel = Some(cancel);
        managed.completion_handle = Some(completion_handle);

        info!(id = %agent_id, "Agent dispatched");
        Ok(())
    }

    /// Restart-dispatch path. Resumes the agent's session under the same
    /// `Active` state used by ad-hoc dispatch. Caller (the supervisor +
    /// scheduler) is responsible for ensuring `agent.state == Restarting`
    /// before invoking this.
    pub(crate) async fn restart_dispatch(
        self: &Arc<Self>,
        agent_id: &str,
        attempt: u32,
    ) -> Result<()> {
        let agent = self
            .db
            .get_agent(agent_id)?
            .ok_or_else(|| anyhow!("agent not found: {agent_id}"))?;
        if agent.state != AgentState::Restarting {
            return Err(anyhow!(
                "restart_dispatch: agent {} not in Restarting (state: {})",
                agent_id,
                agent.state
            ));
        }
        let task = agent.task.clone().unwrap_or_default();
        let provider_name = agent
            .provider
            .clone()
            .unwrap_or_else(|| self.registry.default_name().to_string());
        let cwd = agent.cwd.clone();
        let model = agent.model.clone();
        let resume_session_id = agent.session_id.clone();

        let req = ExecuteRequest {
            agent_id: agent_id.to_string(),
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
            self.db.update_agent_pid(agent_id, p)?;
        }
        self.db
            .update_agent_state(agent_id, &AgentState::Active, None)?;
        self.event_bus.publish(StreamEvent::StateChange {
            agent_id: agent_id.to_string(),
            old_state: AgentState::Restarting,
            new_state: AgentState::Active,
        });
        let completion_handle = self.watch_completion(agent_id.to_string(), completion);
        {
            let mut agents = self.agents.lock().await;
            let managed = agents
                .entry(agent_id.to_string())
                .or_insert_with(|| ManagedAgent {
                    agent: agent.clone(),
                    cancel: None,
                    completion_handle: None,
                });
            managed.agent.state = AgentState::Active;
            managed.agent.pid = pid;
            managed.cancel = Some(cancel);
            managed.completion_handle = Some(completion_handle);
        }
        self.event_bus.publish(StreamEvent::Restarted {
            agent_id: agent_id.to_string(),
            attempt,
            mail_id: None,
        });
        info!(id = %agent_id, attempt = attempt, "Agent restarted");
        Ok(())
    }
}

#[async_trait]
impl Dispatcher for AgentManager {
    async fn dispatch(&self, row: QueueRow) -> Result<()> {
        let arc = self
            .weak_self
            .upgrade()
            .ok_or_else(|| anyhow!("agent manager has been dropped"))?;
        arc.dispatch_internal(row).await
    }
}

#[async_trait]
impl MailWaker for AgentManager {
    async fn wake(&self, agent_id: &str, prompt: &str) -> Result<()> {
        let arc = self
            .weak_self
            .upgrade()
            .ok_or_else(|| anyhow!("agent manager has been dropped"))?;
        arc.invoke(agent_id, prompt, None).await
    }
}

#[async_trait]
impl RestartDispatcher for AgentManager {
    async fn restart_dispatch(&self, agent_id: &str, attempt: u32) -> Result<()> {
        let arc = self
            .weak_self
            .upgrade()
            .ok_or_else(|| anyhow!("agent manager has been dropped"))?;
        arc.restart_dispatch(agent_id, attempt).await
    }
}
