//! Human-in-the-loop approval gating: parking gated tasks, and the
//! operator-facing approve / reject transitions.

use tracing::{error, info};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Task, TaskState};

use super::ScrollKeeper;

impl ScrollKeeper {
    /// Park a gated task in `AwaitingApproval` and signal the operator (the
    /// notification is the human-facing HITL channel; `TaskStateChange` keeps
    /// the dashboard/`grim scroll` in sync). Idempotent for already-pending tasks.
    pub(super) async fn hold_for_approval(&self, task: &Task) {
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

    /// HITL approve: clear the gate and let the DAG schedule it. Only valid for
    /// an `AwaitingApproval` task. Returns the task name.
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
        self.db.update_task_state(&task.id, &TaskState::Ready)?;
        self.manager
            .event_bus()
            .publish(StreamEvent::TaskStateChange {
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
}
