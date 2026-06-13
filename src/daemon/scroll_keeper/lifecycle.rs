//! Scroll lifecycle: inscribe, activate, abandon, and status snapshots.

use std::collections::HashMap;
use tracing::info;

use crate::shared::types::{Scroll, ScrollState, Task, TaskState};

use crate::daemon::scroll_parser::ScrollSpec;

use super::{InscribeResult, ScrollKeeper, ScrollStatus, TaskStatus};

impl ScrollKeeper {
    /// Inscribe a scroll from a parsed spec.
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

    /// Activate a scroll and start scheduling ready tasks.
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

    /// Abandon a scroll: banish active agents, skip incomplete tasks.
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

    /// Full status snapshot of a scroll.
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
}
