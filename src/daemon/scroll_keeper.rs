use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Scroll, ScrollState, Task, TaskConflict, TaskState};

use super::agent_manager::AgentManager;
use super::event_bus::EventBus;
use super::peer_registry::PeerRegistry;
use super::persistence::Database;
use super::scroll_parser::ScrollSpec;
use tokio::sync::Mutex;

/// Pass bar for verification-gated tasks whose spec sets a rubric but
/// no explicit `verify_threshold`.
const DEFAULT_VERIFY_THRESHOLD: f64 = 0.7;

pub struct ScrollKeeper {
    db: Arc<Database>,
    manager: Arc<AgentManager>,
    /// Late-bound: set after `peer_registry` is created in
    /// `daemon::start`. Required only for peer-dispatched tasks; local
    /// scrolls work without it.
    peer_registry: Mutex<Option<Arc<PeerRegistry>>>,
}

/// Status snapshot for a scroll
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScrollStatus {
    pub scroll: Scroll,
    pub tasks: Vec<TaskStatus>,
    pub total: usize,
    pub complete: usize,
    pub active: usize,
    pub blocked: usize,
    pub ready: usize,
    pub failed: usize,
    pub skipped: usize,
    #[serde(default)]
    pub awaiting_approval: usize,
    pub conflicts: Vec<TaskConflict>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskStatus {
    pub task: Task,
    pub depends_on_names: Vec<String>,
}

impl ScrollKeeper {
    pub fn new(db: Arc<Database>, manager: Arc<AgentManager>) -> Self {
        Self {
            db,
            manager,
            peer_registry: Mutex::new(None),
        }
    }

    /// Late-bind the peer registry so scrolls can dispatch tasks with
    /// `peer:` directives. Called from `daemon::start` after
    /// `PeerRegistry::new`. Idempotent for repeated calls (the latest
    /// wins).
    pub async fn set_peer_registry(&self, registry: Arc<PeerRegistry>) {
        *self.peer_registry.lock().await = Some(registry);
    }

    /// Start listening to the event bus for agent completions
    pub fn start(self: Arc<Self>, event_bus: &EventBus) {
        let mut rx = event_bus.subscribe();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(StreamEvent::StateChange {
                        ref agent_id,
                        ref new_state,
                        ..
                    }) => {
                        use crate::shared::types::AgentState;
                        match new_state {
                            AgentState::Complete => self.handle_agent_completion(agent_id).await,
                            AgentState::Failed | AgentState::Banished => {
                                self.handle_agent_failure(agent_id).await;
                            }
                            AgentState::Restarting => {
                                debug!(agent_id = %agent_id, "scroll-keeper: ignoring transient Restarting state");
                            }
                            AgentState::Queued
                            | AgentState::Summoning
                            | AgentState::Active
                            | AgentState::Dormant => {}
                        }
                    }
                    Ok(StreamEvent::RemoteAgentStateChanged {
                        ref sender_daemon_id,
                        ref agent_id,
                        ref new_state,
                        ..
                    }) => {
                        // F5b: a federated peer is reporting that one
                        // of our dispatched tasks just transitioned.
                        // Resolve `(sender_daemon_id, remote_agent_id)`
                        // to the local dispatch row and update the
                        // task state to match.
                        self.handle_remote_state_change(sender_daemon_id, agent_id, new_state)
                            .await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "ScrollKeeper lagged, some events missed");
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        });
    }

    /// Inscribe a scroll from a parsed spec
    pub fn inscribe(
        &self,
        spec: ScrollSpec,
        max_concurrency: Option<u32>,
        source_path: Option<String>,
    ) -> anyhow::Result<InscribeResult> {
        let scroll_id = crate::shared::constants::generate_short_id();
        let now = chrono::Utc::now();

        let scroll = Scroll {
            id: scroll_id.clone(),
            name: spec.name.clone(),
            state: ScrollState::Inscribed,
            source_path,
            max_concurrency: max_concurrency.unwrap_or(4),
            created_at: now,
            updated_at: now,
        };

        self.db.insert_scroll(&scroll)?;

        // Create tasks and build name->id map
        let mut name_to_id: HashMap<String, String> = HashMap::new();
        let mut tasks = Vec::new();

        for (idx, task_spec) in spec.tasks.iter().enumerate() {
            let task_id = crate::shared::constants::generate_short_id();
            name_to_id.insert(task_spec.name.clone(), task_id.clone());

            let has_deps = !task_spec.depends_on.is_empty();
            let task = Task {
                id: task_id,
                scroll_id: scroll_id.clone(),
                name: task_spec.name.clone(),
                prompt: task_spec.prompt.clone(),
                state: if has_deps {
                    TaskState::Blocked
                } else {
                    TaskState::Ready
                },
                agent_id: None,
                provider: task_spec.provider.clone(),
                model: task_spec.model.clone(),
                cwd: task_spec.cwd.clone(),
                file_patterns: task_spec.file_patterns.clone(),
                order_index: idx as u32,
                created_at: now,
                updated_at: now,
                peer_name: task_spec.peer.clone(),
                verify_rubric: task_spec.verify.clone(),
                verify_threshold: task_spec.verify_threshold,
                verifier_agent_id: None,
            };

            self.db.insert_task(&task)?;
            self.db
                .set_task_directives(&task.id, task_spec.approve, task_spec.retries)?;
            tasks.push(task);
        }

        for task_spec in &spec.tasks {
            let task_id = &name_to_id[&task_spec.name];
            for dep_name in &task_spec.depends_on {
                let dep_id = &name_to_id[dep_name];
                self.db.insert_task_dependency(task_id, dep_id)?;
            }
        }

        self.validate_dag(&scroll_id)?;

        let conflicts = Self::detect_all_conflicts(&tasks);

        info!(scroll_id = %scroll_id, name = %spec.name, tasks = tasks.len(), "Scroll inscribed");

        Ok(InscribeResult {
            scroll,
            task_count: tasks.len(),
            conflicts,
        })
    }

    /// Activate a scroll. Starts scheduling ready tasks.
    pub async fn activate(&self, scroll_id: &str) -> anyhow::Result<()> {
        let scroll = self
            .db
            .get_scroll(scroll_id)?
            .ok_or_else(|| anyhow::anyhow!("Scroll not found: {scroll_id}"))?;

        if scroll.state != ScrollState::Inscribed {
            return Err(anyhow::anyhow!(
                "Scroll {} is in state '{}', can only activate from 'inscribed'",
                scroll_id,
                scroll.state
            ));
        }

        self.db
            .update_scroll_state(scroll_id, &ScrollState::Active)?;

        info!(scroll_id = %scroll_id, "Scroll activated");

        self.schedule_tasks(scroll_id).await?;

        Ok(())
    }

    /// Abandon a scroll. Banishes active agents and marks incomplete tasks as skipped.
    pub async fn abandon(&self, scroll_id: &str) -> anyhow::Result<()> {
        let tasks = self.db.get_tasks_for_scroll(scroll_id)?;

        for task in &tasks {
            match task.state {
                TaskState::Active => {
                    if let Some(ref agent_id) = task.agent_id {
                        let _ = self.manager.banish(agent_id).await;
                    }
                    self.db.update_task_state(&task.id, &TaskState::Skipped)?;
                }
                TaskState::Blocked | TaskState::Ready | TaskState::AwaitingApproval => {
                    self.db.update_task_state(&task.id, &TaskState::Skipped)?;
                }
                _ => {}
            }
        }

        self.db
            .update_scroll_state(scroll_id, &ScrollState::Abandoned)?;

        info!(scroll_id = %scroll_id, "Scroll abandoned");
        Ok(())
    }

    /// Get full status of a scroll
    pub fn status(&self, scroll_id: &str) -> anyhow::Result<ScrollStatus> {
        let scroll = self
            .db
            .get_scroll(scroll_id)?
            .ok_or_else(|| anyhow::anyhow!("Scroll not found: {scroll_id}"))?;

        let tasks = self.db.get_tasks_for_scroll(scroll_id)?;

        let id_to_name: HashMap<String, String> = tasks
            .iter()
            .map(|r| (r.id.clone(), r.name.clone()))
            .collect();

        let mut task_statuses = Vec::new();
        for task in &tasks {
            let deps = self.db.get_task_dependencies(&task.id)?;
            let depends_on_names: Vec<String> = deps
                .iter()
                .filter_map(|id| id_to_name.get(id).cloned())
                .collect();
            task_statuses.push(TaskStatus {
                task: task.clone(),
                depends_on_names,
            });
        }

        let total = tasks.len();
        let complete = tasks
            .iter()
            .filter(|r| r.state == TaskState::Complete)
            .count();
        let active = tasks
            .iter()
            .filter(|r| r.state == TaskState::Active)
            .count();
        let blocked = tasks
            .iter()
            .filter(|r| r.state == TaskState::Blocked)
            .count();
        let ready = tasks.iter().filter(|r| r.state == TaskState::Ready).count();
        let failed = tasks
            .iter()
            .filter(|r| r.state == TaskState::Failed)
            .count();
        let skipped = tasks
            .iter()
            .filter(|r| r.state == TaskState::Skipped)
            .count();
        let awaiting_approval = tasks
            .iter()
            .filter(|r| r.state == TaskState::AwaitingApproval)
            .count();

        // Detect conflicts among active + ready tasks
        let conflictable: Vec<Task> = tasks
            .iter()
            .filter(|r| r.state == TaskState::Active || r.state == TaskState::Ready)
            .cloned()
            .collect();
        let conflicts = Self::detect_all_conflicts(&conflictable);

        Ok(ScrollStatus {
            scroll,
            tasks: task_statuses,
            total,
            complete,
            active,
            blocked,
            ready,
            failed,
            skipped,
            awaiting_approval,
            conflicts,
        })
    }

    // --- Internal ---

    async fn handle_agent_completion(&self, agent_id: &str) {
        // A completing agent may be an evaluator verifying another
        // task's worker, not a worker itself. Check that link first:
        // its completion carries a verdict, not task output.
        match self.db.get_task_by_verifier_agent_id(agent_id) {
            Ok(Some(task)) => {
                self.finish_verification(&task, agent_id).await;
                return;
            }
            Ok(None) => {}
            Err(e) => {
                error!(agent_id = %agent_id, error = %e, "Failed to look up verifier task");
                return;
            }
        }

        let task = match self.db.get_task_by_agent_id(agent_id) {
            Ok(Some(r)) => r,
            Ok(None) => return, // Not a scroll-managed agent
            Err(e) => {
                error!(agent_id = %agent_id, error = %e, "Failed to look up task");
                return;
            }
        };

        // Verification gate: a rubric-bearing task is not complete when
        // its worker finishes; an evaluator scores the transcript first.
        if task.verify_rubric.is_some() {
            if task.verifier_agent_id.is_none() {
                self.start_verification(&task, agent_id).await;
            } else {
                debug!(
                    scroll_id = %task.scroll_id,
                    task = %task.name,
                    agent_id = %agent_id,
                    "Worker completed again while verification is pending; ignoring"
                );
            }
            return;
        }

        info!(
            scroll_id = %task.scroll_id,
            task = %task.name,
            agent_id = %agent_id,
            "Task completed"
        );

        self.complete_task_and_advance(&task).await;
    }

    /// Mark `task` complete and move the scroll forward: finish the
    /// scroll if every task is terminal, otherwise schedule the next
    /// batch. Shared by the plain completion path and a passed
    /// verification.
    async fn complete_task_and_advance(&self, task: &Task) {
        if let Err(e) = self.db.update_task_state(&task.id, &TaskState::Complete) {
            error!(task_id = %task.id, error = %e, "Failed to update task state");
            return;
        }

        let tasks = match self.db.get_tasks_for_scroll(&task.scroll_id) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "Failed to get tasks for scroll");
                return;
            }
        };

        let all_done = tasks.iter().all(|r| {
            matches!(
                r.state,
                TaskState::Complete | TaskState::Skipped | TaskState::Failed
            )
        });

        if all_done {
            let any_failed = tasks.iter().any(|r| r.state == TaskState::Failed);
            let new_state = if any_failed {
                ScrollState::Failed
            } else {
                ScrollState::Complete
            };
            let _ = self.db.update_scroll_state(&task.scroll_id, &new_state);
            info!(scroll_id = %task.scroll_id, state = %new_state, "Scroll finished");
        } else {
            // Schedule next batch
            if let Err(e) = self.schedule_tasks(&task.scroll_id).await {
                error!(scroll_id = %task.scroll_id, error = %e, "Failed to schedule tasks");
            }
        }
    }

    /// A task run failed (worker died, or verification scored below the
    /// bar). If the task has retry budget left, re-spawn a fresh agent for
    /// it instead of failing; otherwise fall through to the terminal
    /// failure path. Retrying clears any verifier link so a re-run's
    /// completion re-triggers verification from scratch, and flips the task
    /// back to `Ready` so the scheduler re-enqueues it (an approved gate
    /// stays approved — no second approval is asked for).
    ///
    /// Note this is the DAG-level retry (a new agent per attempt), distinct
    /// from agent-level `--restart` (which resumes the same agent's
    /// session). A supervised agent's failure is handled by the supervisor
    /// before reaching here, so the two never double-fire.
    async fn retry_or_fail(&self, task: &Task) {
        let (max, count) = self.db.get_task_retry(&task.id).unwrap_or((0, 0));
        if count >= max {
            self.fail_task_and_advance(task).await;
            return;
        }
        let attempt = self.db.bump_task_retry(&task.id).unwrap_or(count + 1);
        let _ = self.db.clear_task_verifier(&task.id);
        if let Err(e) = self.db.update_task_state(&task.id, &TaskState::Ready) {
            error!(task = %task.name, error = %e, "retry: failed to reset task; failing instead");
            self.fail_task_and_advance(task).await;
            return;
        }
        self.manager.event_bus().publish(StreamEvent::Notification {
            agent_id: None,
            message: format!(
                "retrying scroll {} task '{}' (attempt {attempt}/{max})",
                task.scroll_id, task.name
            ),
            level: "warn".to_string(),
            source: "system".to_string(),
        });
        info!(
            scroll_id = %task.scroll_id,
            task = %task.name,
            attempt,
            max,
            "Task failed; retrying with a fresh agent"
        );
        if let Err(e) = self.schedule_tasks(&task.scroll_id).await {
            error!(scroll_id = %task.scroll_id, error = %e, "retry: schedule_tasks failed");
        }
    }

    /// Mark `task` failed, skip everything downstream of it, and either
    /// finish the scroll (all terminal) or keep scheduling independent
    /// tasks. Shared by worker failure and a failed verification.
    async fn fail_task_and_advance(&self, task: &Task) {
        let _ = self.db.update_task_state(&task.id, &TaskState::Failed);

        self.skip_downstream(&task.id);

        let Ok(tasks) = self.db.get_tasks_for_scroll(&task.scroll_id) else {
            return;
        };

        let all_terminal = tasks.iter().all(|r| {
            matches!(
                r.state,
                TaskState::Complete | TaskState::Failed | TaskState::Skipped
            )
        });

        if all_terminal {
            let _ = self
                .db
                .update_scroll_state(&task.scroll_id, &ScrollState::Failed);
            info!(scroll_id = %task.scroll_id, "Scroll failed");
        } else {
            // There may still be independent tasks that can run
            let _ = self.schedule_tasks(&task.scroll_id).await;
        }
    }

    /// Park a gated task in `AwaitingApproval` and signal the operator. The
    /// notification is the human-facing HITL channel (it fans out to the
    /// configured webhook / log / desktop via the notifier); the
    /// `TaskStateChange` keeps the dashboard and `grim scroll` in sync.
    /// Idempotent: re-holding an already-pending task only re-affirms state.
    async fn hold_for_approval(&self, task: &Task) {
        use crate::shared::types::ApprovalState;
        let already_pending = matches!(
            self.db.get_task_approval(&task.id),
            Ok((_, ApprovalState::Pending))
        ) && task.state == TaskState::AwaitingApproval;
        if let Err(e) = self
            .db
            .update_task_state(&task.id, &TaskState::AwaitingApproval)
        {
            error!(task = %task.name, error = %e, "failed to hold task for approval");
            return;
        }
        let _ = self
            .db
            .set_task_approval_state(&task.id, ApprovalState::Pending);

        if already_pending {
            return;
        }

        let bus = self.manager.event_bus();
        bus.publish(StreamEvent::TaskStateChange {
            scroll_id: task.scroll_id.clone(),
            task_id: task.id.clone(),
            task_name: task.name.clone(),
            old_state: TaskState::Ready,
            new_state: TaskState::AwaitingApproval,
        });
        bus.publish(StreamEvent::Notification {
            agent_id: None,
            message: format!(
                "approval required: scroll {} task '{}' ({}) is held for review. \
                 Approve with `grim scroll approve {} {}` or reject with \
                 `grim scroll reject {} {}`.",
                task.scroll_id,
                task.name,
                task.id,
                task.scroll_id,
                task.name,
                task.scroll_id,
                task.name,
            ),
            level: "warn".to_string(),
            source: "system".to_string(),
        });
        info!(
            scroll_id = %task.scroll_id,
            task = %task.name,
            "Task held for human approval"
        );
    }

    /// Resolve a task reference (exact id, else exact name) within a scroll.
    fn resolve_task_in_scroll(&self, scroll_id: &str, task_ref: &str) -> anyhow::Result<Task> {
        let tasks = self.db.get_tasks_for_scroll(scroll_id)?;
        tasks
            .iter()
            .find(|t| t.id == task_ref)
            .or_else(|| tasks.iter().find(|t| t.name == task_ref))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("task '{task_ref}' not found in scroll {scroll_id}"))
    }

    /// HITL approve: clear a task's gate and let the DAG schedule it. Only
    /// valid for a task currently `AwaitingApproval`. Returns the task name.
    pub async fn approve_task(&self, scroll_id: &str, task_ref: &str) -> anyhow::Result<String> {
        use crate::shared::types::ApprovalState;
        let task = self.resolve_task_in_scroll(scroll_id, task_ref)?;
        if task.state != TaskState::AwaitingApproval {
            return Err(anyhow::anyhow!(
                "task '{}' is not awaiting approval (state: {})",
                task.name,
                task.state
            ));
        }
        self.db
            .set_task_approval_state(&task.id, ApprovalState::Approved)?;
        // Flip back to a schedulable state; the gate check now passes.
        self.db.update_task_state(&task.id, &TaskState::Ready)?;
        self.manager.event_bus().publish(StreamEvent::TaskStateChange {
            scroll_id: task.scroll_id.clone(),
            task_id: task.id.clone(),
            task_name: task.name.clone(),
            old_state: TaskState::AwaitingApproval,
            new_state: TaskState::Ready,
        });
        info!(scroll_id = %scroll_id, task = %task.name, "Task approved");
        self.schedule_tasks(scroll_id).await?;
        Ok(task.name)
    }

    /// HITL reject: fail a held task and skip everything downstream of it.
    /// Returns the task name.
    pub async fn reject_task(&self, scroll_id: &str, task_ref: &str) -> anyhow::Result<String> {
        use crate::shared::types::ApprovalState;
        let task = self.resolve_task_in_scroll(scroll_id, task_ref)?;
        if task.state != TaskState::AwaitingApproval {
            return Err(anyhow::anyhow!(
                "task '{}' is not awaiting approval (state: {})",
                task.name,
                task.state
            ));
        }
        self.db
            .set_task_approval_state(&task.id, ApprovalState::Rejected)?;
        info!(scroll_id = %scroll_id, task = %task.name, "Task rejected");
        self.fail_task_and_advance(&task).await;
        Ok(task.name)
    }

    /// The worker for a rubric-bearing task just completed: summon an
    /// evaluator agent to score the worker's transcript. The task stays
    /// in its current (non-terminal) state until the verdict arrives.
    async fn start_verification(&self, task: &Task, worker_agent_id: &str) {
        let rubric = task.verify_rubric.clone().unwrap_or_default();

        let events = self
            .db
            .read_stream_events(worker_agent_id)
            .unwrap_or_else(|e| {
                warn!(agent_id = %worker_agent_id, error = %e,
                    "Failed to read worker events; verifying against an empty transcript");
                Vec::new()
            });
        let max_seq = events.last().map_or(0, |e| e.seq);
        let transcript = crate::shared::eval::fold_stdout_output(events.iter().map(|e| &e.event));
        let prompt =
            crate::shared::eval::build_eval_prompt(worker_agent_id, max_seq, &rubric, &transcript);

        let cwd = self
            .manager
            .resolve_cwd(task.cwd.as_ref().map(PathBuf::from));
        match self
            .manager
            .enqueue(
                &prompt,
                Some(format!("verify:{}", task.name)),
                task.model.clone(),
                task.provider.clone(),
                &cwd,
                crate::daemon::agent_manager::Lane::Adhoc,
            )
            .await
        {
            Ok(evaluator) => {
                if let Err(e) = self.db.set_task_verifier(&task.id, &evaluator.id) {
                    error!(task_id = %task.id, error = %e,
                        "Failed to record verifier agent; failing task instead of leaving it hung");
                    self.fail_task_and_advance(task).await;
                    return;
                }
                info!(
                    scroll_id = %task.scroll_id,
                    task = %task.name,
                    worker = %worker_agent_id,
                    verifier = %evaluator.id,
                    "Worker finished; evaluator summoned to verify against rubric"
                );
            }
            Err(e) => {
                warn!(
                    scroll_id = %task.scroll_id,
                    task = %task.name,
                    error = %e,
                    "Failed to enqueue verifier agent; treating verification as failed"
                );
                self.fail_task_and_advance(task).await;
            }
        }
    }

    /// An evaluator agent finished: parse its verdict and settle the
    /// task it was verifying. A missing or unparsable verdict counts
    /// as a failed verification — the gate must never silently pass.
    async fn finish_verification(&self, task: &Task, evaluator_id: &str) {
        let threshold = task.verify_threshold.unwrap_or(DEFAULT_VERIFY_THRESHOLD);

        let verdict = self
            .manager
            .agent_result(evaluator_id)
            .ok_or_else(|| anyhow::anyhow!("verifier produced no result text"))
            .and_then(|text| crate::shared::eval::parse_verdict(&text));

        let verdict = match verdict {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    task = %task.name,
                    verifier = %evaluator_id,
                    error = %e,
                    "No usable verdict from verifier; verification fails"
                );
                self.fail_task_and_advance(task).await;
                return;
            }
        };

        // Record the verdict so `grim eval <worker> --list` shows scroll
        // verifications alongside manual evals.
        let target_id = task.agent_id.clone().unwrap_or_else(|| task.id.clone());
        if let Err(e) = self.db.insert_eval_result(
            &target_id,
            evaluator_id,
            verdict.score,
            verdict.verdict.as_deref(),
            verdict.rationale.as_deref(),
        ) {
            warn!(task = %task.name, error = %e, "Failed to persist verification verdict");
        }

        if verdict.score >= threshold {
            info!(
                scroll_id = %task.scroll_id,
                task = %task.name,
                verifier = %evaluator_id,
                score = verdict.score,
                threshold,
                "Verification passed"
            );
            self.complete_task_and_advance(task).await;
        } else {
            warn!(
                scroll_id = %task.scroll_id,
                task = %task.name,
                verifier = %evaluator_id,
                score = verdict.score,
                threshold,
                "Verification failed: score below threshold"
            );
            self.retry_or_fail(task).await;
        }
    }

    async fn handle_agent_failure(&self, agent_id: &str) {
        // If the agent has an active restart policy, defer the failure
        // handling to the supervisor, which will transition the agent to
        // Restarting and eventually to terminal Failed if the budget is
        // exhausted. Without this gate, scroll-keeper could mark the task
        // failed before the supervisor flips state.
        if let Ok(Some(cfg)) = self.db.get_supervision(agent_id) {
            use crate::shared::types::RestartPolicy;
            if cfg.policy != RestartPolicy::Never {
                debug!(agent_id = %agent_id, "scroll-keeper: deferring failure handling to supervisor");
                return;
            }
        }

        // A dying evaluator means the verdict will never arrive: the
        // verification (and therefore the task) fails rather than hangs.
        if let Ok(Some(task)) = self.db.get_task_by_verifier_agent_id(agent_id) {
            warn!(
                scroll_id = %task.scroll_id,
                task = %task.name,
                verifier = %agent_id,
                "Verifier agent failed; treating verification as failed"
            );
            self.retry_or_fail(&task).await;
            return;
        }

        let task = match self.db.get_task_by_agent_id(agent_id) {
            Ok(Some(r)) => r,
            Ok(None) => return,
            Err(e) => {
                error!(agent_id = %agent_id, error = %e, "Failed to look up task");
                return;
            }
        };

        warn!(
            scroll_id = %task.scroll_id,
            task = %task.name,
            agent_id = %agent_id,
            "Task failed"
        );

        self.retry_or_fail(&task).await;
    }

    fn skip_downstream(&self, task_id: &str) {
        let Ok(dependents) = self.db.get_task_dependents(task_id) else {
            return;
        };

        for dep_id in dependents {
            let _ = self.db.update_task_state(&dep_id, &TaskState::Skipped);
            self.skip_downstream(&dep_id);
        }
    }

    async fn schedule_tasks(&self, scroll_id: &str) -> anyhow::Result<()> {
        let scroll = self
            .db
            .get_scroll(scroll_id)?
            .ok_or_else(|| anyhow::anyhow!("Scroll not found"))?;

        if scroll.state != ScrollState::Active {
            return Ok(());
        }

        let active_count = self.db.count_active_tasks(scroll_id)?;
        let available_slots = (scroll.max_concurrency as usize).saturating_sub(active_count);

        if available_slots == 0 {
            return Ok(());
        }

        let ready_tasks = self.db.find_ready_tasks(scroll_id)?;
        if ready_tasks.is_empty() {
            return Ok(());
        }

        // Get currently active tasks for conflict checking
        let all_tasks = self.db.get_tasks_for_scroll(scroll_id)?;
        let active_tasks: Vec<&Task> = all_tasks
            .iter()
            .filter(|r| r.state == TaskState::Active)
            .collect();

        let mut spawned = 0;
        for task in &ready_tasks {
            if spawned >= available_slots {
                break;
            }

            // HITL gate: a task that requires approval is held the first
            // time it becomes runnable. A human reviews the upstream work
            // (artifacts) and runs `scroll.approve`, which flips the gate
            // to `approved` and re-schedules. An unapproved gate parks the
            // task in `AwaitingApproval` and skips it — it does not consume
            // a slot and is not re-found by `find_ready_tasks`.
            match self.db.get_task_approval(&task.id) {
                Ok((true, approval_state)) => {
                    use crate::shared::types::ApprovalState;
                    match approval_state {
                        ApprovalState::Approved => { /* cleared; fall through to spawn */ }
                        ApprovalState::None | ApprovalState::Pending => {
                            self.hold_for_approval(task).await;
                            continue;
                        }
                        ApprovalState::Rejected => {
                            // Defensive: a rejected gate should already be
                            // Failed. Settle it inline (not via
                            // fail_task_and_advance, which re-enters
                            // schedule_tasks and would recurse).
                            let _ = self.db.update_task_state(&task.id, &TaskState::Failed);
                            self.skip_downstream(&task.id);
                            continue;
                        }
                    }
                }
                Ok((false, _)) => { /* no gate */ }
                Err(e) => {
                    error!(task = %task.name, error = %e, "approval lookup failed; spawning ungated");
                }
            }

            let has_conflict = active_tasks
                .iter()
                .any(|active| TaskConflict::detect(active, task).is_some());

            if has_conflict {
                info!(
                    task = %task.name,
                    "Delaying task due to file conflict with active task"
                );
                continue;
            }

            // F5b: peer-targeted tasks are dispatched, not spawned.
            // The receiver enqueues an agent of its own and federates
            // lifecycle back; the task's local `agent_id` is filled in
            // by the dispatch ack handler.
            if let Some(peer_name) = task.peer_name.clone() {
                match self.dispatch_to_peer(task, &peer_name).await {
                    Ok(()) => {
                        info!(
                            scroll_id = %scroll_id,
                            task = %task.name,
                            peer = %peer_name,
                            "Task dispatched to peer"
                        );
                        spawned += 1;
                    }
                    Err(e) => {
                        error!(task = %task.name, peer = %peer_name, error = %e,
                            "Failed to dispatch task to peer");
                        self.db.update_task_state(&task.id, &TaskState::Failed)?;
                        self.skip_downstream(&task.id);
                    }
                }
                continue;
            }

            let cwd_opt = task.cwd.as_ref().map(PathBuf::from);
            let cwd = self.manager.resolve_cwd(cwd_opt);

            match self
                .manager
                .enqueue(
                    &task.prompt,
                    Some(task.name.clone()),
                    task.model.clone(),
                    task.provider.clone(),
                    &cwd,
                    crate::daemon::agent_manager::Lane::Scroll,
                )
                .await
            {
                Ok(agent) => {
                    self.db.update_task_agent(&task.id, &agent.id)?;
                    info!(
                        scroll_id = %scroll_id,
                        task = %task.name,
                        agent_id = %agent.id,
                        "Task spawned"
                    );
                    spawned += 1;
                }
                Err(e) => {
                    error!(task = %task.name, error = %e, "Failed to spawn task");
                    self.db.update_task_state(&task.id, &TaskState::Failed)?;
                    self.skip_downstream(&task.id);
                }
            }
        }

        Ok(())
    }

    /// F5b: a `RemoteAgentStateChanged` arrived. Look up the
    /// dispatch row and, on a terminal remote state, mirror it onto
    /// the local task. Non-terminal transitions are ignored — the
    /// task already sits in `active` once `update_task_agent` runs at
    /// dispatch time.
    async fn handle_remote_state_change(
        &self,
        sender_daemon_id: &str,
        remote_agent_id: &str,
        new_state: &crate::shared::types::AgentState,
    ) {
        use crate::shared::types::AgentState;
        if !matches!(
            new_state,
            AgentState::Complete | AgentState::Failed | AgentState::Banished
        ) {
            return;
        }
        let Ok(Some(peer)) = self.db.get_peer_by_daemon_id(sender_daemon_id) else {
            return;
        };
        let peer_id = peer.id;
        let Ok(Some(dispatch)) = self
            .db
            .scroll_dispatch_find_by_remote(&peer_id, remote_agent_id)
        else {
            return;
        };
        let task_state = if matches!(new_state, AgentState::Complete) {
            TaskState::Complete
        } else {
            TaskState::Failed
        };
        if let Err(e) = self.db.update_task_state(&dispatch.task_id, &task_state) {
            warn!(error = %e, task = %dispatch.task_id, "remote dispatch task_state update failed");
            return;
        }
        let dispatch_state = if matches!(task_state, TaskState::Complete) {
            "complete"
        } else {
            "failed"
        };
        let _ = self.db.scroll_dispatch_set_state(
            &dispatch.scroll_id,
            &dispatch.task_id,
            &peer_id,
            dispatch_state,
        );
        info!(
            scroll = %dispatch.scroll_id,
            task = %dispatch.task_id,
            remote = %remote_agent_id,
            state = ?task_state,
            "remote dispatched task settled",
        );
        if task_state == TaskState::Failed {
            self.skip_downstream(&dispatch.task_id);
        }
        // Kick the scroll's DAG so anything waiting on this task can
        // move forward.
        if let Err(e) = self.schedule_tasks(&dispatch.scroll_id).await {
            warn!(error = %e, "schedule_tasks after remote completion failed");
        }
    }

    /// Dispatch a single peer-targeted task to its named peer. Writes
    /// the durable `scroll_task_dispatches` row, enqueues the wire
    /// outbox row, and pokes the drainer. Marks the local task
    /// `Active` so DAG accounting sees it as in-flight.
    async fn dispatch_to_peer(&self, task: &Task, peer_name: &str) -> anyhow::Result<()> {
        use crate::daemon::peer_client::ScrollDispatchPayload;
        let Some(registry) = self.peer_registry.lock().await.clone() else {
            return Err(anyhow::anyhow!("peer_registry_not_bound"));
        };
        let Some(peer) = self.db.get_peer_by_name(peer_name)? else {
            return Err(anyhow::anyhow!("peer_not_found: {peer_name}"));
        };
        let payload = ScrollDispatchPayload {
            scroll_id: task.scroll_id.clone(),
            task_id: task.id.clone(),
            task_name: task.name.clone(),
            prompt: task.prompt.clone(),
            provider: task.provider.clone().unwrap_or_default(),
            model: task.model.clone().unwrap_or_default(),
            cwd: task.cwd.clone().unwrap_or_default(),
            file_patterns: task.file_patterns.clone(),
        };
        let bytes = serde_json::to_vec(&payload)?;
        let dispatch_id = crate::shared::constants::generate_short_id();
        self.db
            .scroll_dispatch_insert(&dispatch_id, &task.scroll_id, &task.id, &peer.id)?;
        self.db.scroll_dispatch_enqueue(&peer.id, &bytes)?;
        // The task is now considered in-flight; the receiver's local
        // agent id will be patched in by the ack handler.
        self.db.update_task_state(&task.id, &TaskState::Active)?;
        registry.notify_outbox(&peer.id).await;
        Ok(())
    }

    fn validate_dag(&self, scroll_id: &str) -> anyhow::Result<()> {
        let edges = self.db.get_all_dependencies_for_scroll(scroll_id)?;
        let tasks = self.db.get_tasks_for_scroll(scroll_id)?;

        let mut adj: HashMap<String, Vec<String>> = HashMap::new();
        let all_ids: HashSet<String> = tasks.iter().map(|r| r.id.clone()).collect();

        for id in &all_ids {
            adj.entry(id.clone()).or_default();
        }
        for (from, to) in &edges {
            adj.entry(from.clone()).or_default().push(to.clone());
        }

        // Topological sort via DFS to detect cycles
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();

        for id in &all_ids {
            if dag_has_cycle(id, &adj, &mut visited, &mut in_stack) {
                return Err(anyhow::anyhow!(
                    "Dependency cycle detected in scroll {scroll_id}"
                ));
            }
        }

        Ok(())
    }

    fn detect_all_conflicts(tasks: &[Task]) -> Vec<TaskConflict> {
        let mut conflicts = Vec::new();
        for i in 0..tasks.len() {
            for j in (i + 1)..tasks.len() {
                if let Some(c) = TaskConflict::detect(&tasks[i], &tasks[j]) {
                    conflicts.push(c);
                }
            }
        }
        conflicts
    }
}

/// Cycle-detecting DFS helper for `validate_dag`. Returns `true` if a
/// back-edge is found from `node`.
fn dag_has_cycle(
    node: &str,
    adj: &HashMap<String, Vec<String>>,
    visited: &mut HashSet<String>,
    in_stack: &mut HashSet<String>,
) -> bool {
    if in_stack.contains(node) {
        return true;
    }
    if visited.contains(node) {
        return false;
    }
    visited.insert(node.to_string());
    in_stack.insert(node.to_string());

    if let Some(neighbors) = adj.get(node) {
        for neighbor in neighbors {
            if dag_has_cycle(neighbor, adj, visited, in_stack) {
                return true;
            }
        }
    }

    in_stack.remove(node);
    false
}

pub struct InscribeResult {
    pub scroll: Scroll,
    pub task_count: usize,
    pub conflicts: Vec<TaskConflict>,
}
