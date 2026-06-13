//! DAG scheduling: pick ready tasks within the concurrency budget, apply
//! HITL gating and conflict checks, then spawn locally or dispatch to a peer.

use std::path::PathBuf;
use tracing::{error, info};

use crate::shared::types::{ScrollState, Task, TaskConflict, TaskState};

use super::ScrollKeeper;

impl ScrollKeeper {
    pub(super) async fn schedule_tasks(&self, scroll_id: &str) -> anyhow::Result<()> {
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

        // Active tasks, for conflict checking against ready ones.
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

            // Peer-targeted tasks are dispatched, not spawned.
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
}
