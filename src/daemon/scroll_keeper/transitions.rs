//! Task-state transitions: complete, retry, fail, and downstream-skip
//! propagation that drive a scroll's DAG forward.

use tracing::{error, info};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{ScrollState, Task, TaskState};

use super::ScrollKeeper;

impl ScrollKeeper {
    /// Mark `task` complete and advance the scroll: finish if every task is
    /// terminal, else schedule the next batch.
    pub(super) async fn complete_task_and_advance(&self, task: &Task) {
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
            if let Err(e) = self.schedule_tasks(&task.scroll_id).await {
                error!(scroll_id = %task.scroll_id, error = %e, "Failed to schedule tasks");
            }
        }
    }

    /// A task run failed (worker died, or verification scored below the bar).
    /// With retry budget left, re-spawn a fresh agent; else fall through to
    /// terminal failure. Clears the verifier link so a re-run re-verifies from
    /// scratch, and resets to `Ready` to be re-enqueued (an approved gate stays
    /// approved — no second approval).
    ///
    /// This is the DAG-level retry (new agent per attempt), distinct from
    /// agent-level `--restart` (resumes the same session). A supervised agent's
    /// failure is handled by the supervisor before reaching here, so the two
    /// never double-fire.
    pub(super) async fn retry_or_fail(&self, task: &Task) {
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

    /// Mark `task` failed, skip everything downstream, then finish the scroll
    /// (all terminal) or keep scheduling independent tasks.
    pub(super) async fn fail_task_and_advance(&self, task: &Task) {
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
            let _ = self.schedule_tasks(&task.scroll_id).await;
        }
    }

    pub(super) fn skip_downstream(&self, task_id: &str) {
        let Ok(dependents) = self.db.get_task_dependents(task_id) else {
            return;
        };

        for dep_id in dependents {
            let _ = self.db.update_task_state(&dep_id, &TaskState::Skipped);
            self.skip_downstream(&dep_id);
        }
    }
}
