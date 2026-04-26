use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{
    Task, TaskConflict, TaskState, Scroll, ScrollState,
};

use super::agent_manager::AgentManager;
use super::event_bus::EventBus;
use super::persistence::Database;
use super::scroll_parser::ScrollSpec;

pub struct ScrollKeeper {
    db: Arc<Database>,
    manager: Arc<AgentManager>,
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
    pub conflicts: Vec<TaskConflict>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskStatus {
    pub task: Task,
    pub depends_on_names: Vec<String>,
}

impl ScrollKeeper {
    pub fn new(db: Arc<Database>, manager: Arc<AgentManager>) -> Self {
        Self { db, manager }
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
                            AgentState::Failed | AgentState::Banished => self.handle_agent_failure(agent_id).await,
                            _ => {}
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "ScrollKeeper lagged, some events missed");
                        continue;
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
            };

            self.db.insert_task(&task)?;
            tasks.push(task);
        }

        // Insert dependency edges
        for task_spec in &spec.tasks {
            let task_id = &name_to_id[&task_spec.name];
            for dep_name in &task_spec.depends_on {
                let dep_id = &name_to_id[dep_name];
                self.db.insert_task_dependency(task_id, dep_id)?;
            }
        }

        // Validate: no cycles
        self.validate_dag(&scroll_id)?;

        // Detect file conflicts
        let conflicts = self.detect_all_conflicts(&tasks);

        info!(scroll_id = %scroll_id, name = %spec.name, tasks = tasks.len(), "Scroll inscribed");

        Ok(InscribeResult {
            scroll,
            task_count: tasks.len(),
            conflicts,
        })
    }

    /// Activate a scroll — start scheduling ready tasks
    pub async fn activate(&self, scroll_id: &str) -> anyhow::Result<()> {
        let scroll = self
            .db
            .get_scroll(scroll_id)?
            .ok_or_else(|| anyhow::anyhow!("Scroll not found: {}", scroll_id))?;

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

    /// Abandon a scroll — banish active agents, mark incomplete tasks as skipped
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
                TaskState::Blocked | TaskState::Ready => {
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
            .ok_or_else(|| anyhow::anyhow!("Scroll not found: {}", scroll_id))?;

        let tasks = self.db.get_tasks_for_scroll(scroll_id)?;

        // Build name map for dependency display
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
        let complete = tasks.iter().filter(|r| r.state == TaskState::Complete).count();
        let active = tasks.iter().filter(|r| r.state == TaskState::Active).count();
        let blocked = tasks.iter().filter(|r| r.state == TaskState::Blocked).count();
        let ready = tasks.iter().filter(|r| r.state == TaskState::Ready).count();
        let failed = tasks.iter().filter(|r| r.state == TaskState::Failed).count();
        let skipped = tasks.iter().filter(|r| r.state == TaskState::Skipped).count();

        // Detect conflicts among active + ready tasks
        let conflictable: Vec<Task> = tasks
            .iter()
            .filter(|r| r.state == TaskState::Active || r.state == TaskState::Ready)
            .cloned()
            .collect();
        let conflicts = self.detect_all_conflicts(&conflictable);

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
            conflicts,
        })
    }

    // --- Internal ---

    async fn handle_agent_completion(&self, agent_id: &str) {
        let task = match self.db.get_task_by_agent_id(agent_id) {
            Ok(Some(r)) => r,
            Ok(None) => return, // Not a scroll-managed agent
            Err(e) => {
                error!(agent_id = %agent_id, error = %e, "Failed to look up task");
                return;
            }
        };

        info!(
            scroll_id = %task.scroll_id,
            task = %task.name,
            agent_id = %agent_id,
            "Task completed"
        );

        if let Err(e) = self.db.update_task_state(&task.id, &TaskState::Complete) {
            error!(task_id = %task.id, error = %e, "Failed to update task state");
            return;
        }

        // Check if scroll is done
        let tasks = match self.db.get_tasks_for_scroll(&task.scroll_id) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "Failed to get tasks for scroll");
                return;
            }
        };

        let all_done = tasks
            .iter()
            .all(|r| matches!(r.state, TaskState::Complete | TaskState::Skipped | TaskState::Failed));

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

    async fn handle_agent_failure(&self, agent_id: &str) {
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

        let _ = self.db.update_task_state(&task.id, &TaskState::Failed);

        // Skip all downstream tasks
        self.skip_downstream(&task.id);

        // Check if scroll is done
        let tasks = match self.db.get_tasks_for_scroll(&task.scroll_id) {
            Ok(r) => r,
            Err(_) => return,
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

    fn skip_downstream(&self, task_id: &str) {
        let dependents = match self.db.get_task_dependents(task_id) {
            Ok(d) => d,
            Err(_) => return,
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

            // Check for file conflicts with active tasks
            let has_conflict = active_tasks.iter().any(|active| {
                TaskConflict::detect(active, task).is_some()
            });

            if has_conflict {
                info!(
                    task = %task.name,
                    "Delaying task due to file conflict with active task"
                );
                continue;
            }

            // Spawn agent for this task
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

    fn validate_dag(&self, scroll_id: &str) -> anyhow::Result<()> {
        let edges = self.db.get_all_dependencies_for_scroll(scroll_id)?;
        let tasks = self.db.get_tasks_for_scroll(scroll_id)?;

        // Build adjacency list
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

        fn dfs(
            node: &str,
            adj: &HashMap<String, Vec<String>>,
            visited: &mut HashSet<String>,
            in_stack: &mut HashSet<String>,
        ) -> bool {
            if in_stack.contains(node) {
                return true; // cycle
            }
            if visited.contains(node) {
                return false;
            }
            visited.insert(node.to_string());
            in_stack.insert(node.to_string());

            if let Some(neighbors) = adj.get(node) {
                for neighbor in neighbors {
                    if dfs(neighbor, adj, visited, in_stack) {
                        return true;
                    }
                }
            }

            in_stack.remove(node);
            false
        }

        for id in &all_ids {
            if dfs(id, &adj, &mut visited, &mut in_stack) {
                return Err(anyhow::anyhow!(
                    "Dependency cycle detected in scroll {}",
                    scroll_id
                ));
            }
        }

        Ok(())
    }

    fn detect_all_conflicts(&self, tasks: &[Task]) -> Vec<TaskConflict> {
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

pub struct InscribeResult {
    pub scroll: Scroll,
    pub task_count: usize,
    pub conflicts: Vec<TaskConflict>,
}
