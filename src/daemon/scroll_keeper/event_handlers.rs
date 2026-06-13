//! Event-bus reactions: settle tasks when worker, verifier, or remote
//! dispatched agents change state.

use tracing::{debug, error, info, warn};

use crate::shared::types::TaskState;

use super::ScrollKeeper;

impl ScrollKeeper {
    pub(super) async fn handle_agent_completion(&self, agent_id: &str) {
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

    pub(super) async fn handle_agent_failure(&self, agent_id: &str) {
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

    /// A `RemoteAgentStateChanged` arrived. Look up the
    /// dispatch row and, on a terminal remote state, mirror it onto
    /// the local task. Non-terminal transitions are ignored — the
    /// task already sits in `active` once `update_task_agent` runs at
    /// dispatch time.
    pub(super) async fn handle_remote_state_change(
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
}
