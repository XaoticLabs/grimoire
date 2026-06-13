//! Scroll keeper: owns the lifecycle, DAG scheduling, verification, and HITL
//! gating of inscribed scrolls and their tasks.

use std::sync::Arc;
use tracing::{debug, warn};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Scroll, Task, TaskConflict};

use super::agent_manager::AgentManager;
use super::event_bus::EventBus;
use super::peer_registry::PeerRegistry;
use super::persistence::Database;
use tokio::sync::Mutex;

mod approval;
mod dag;
mod event_handlers;
mod lifecycle;
mod scheduling;
mod transitions;
mod verification;

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

pub struct InscribeResult {
    pub scroll: Scroll,
    pub task_count: usize,
    pub conflicts: Vec<TaskConflict>,
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
                        // A federated peer is reporting that one of our
                        // dispatched tasks just transitioned. Resolve
                        // `(sender_daemon_id, remote_agent_id)` to the local
                        // dispatch row and update the task state to match.
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
}
