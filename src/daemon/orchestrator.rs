use std::sync::Arc;
use tracing::{error, info};

use crate::shared::protocol::StreamEvent;

use super::agent_manager::AgentManager;
use super::event_bus::EventBus;
use super::persistence::Database;

pub struct Orchestrator {
    db: Arc<Database>,
    manager: Arc<AgentManager>,
}

impl Orchestrator {
    pub fn new(db: Arc<Database>, manager: Arc<AgentManager>) -> Self {
        Self { db, manager }
    }

    /// Start listening for agent completions and firing pacts.
    pub fn start(self, event_bus: &EventBus) {
        let mut rx = event_bus.subscribe();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(StreamEvent::StateChange {
                        ref agent_id,
                        ref new_state,
                        ..
                    }) if *new_state == crate::shared::types::AgentState::Complete => {
                        self.handle_completion(agent_id).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "Orchestrator lagged, some events missed");
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        });
    }

    async fn handle_completion(&self, agent_id: &str) {
        let pacts = match self.db.get_pending_pacts_for_agent(agent_id) {
            Ok(p) => p,
            Err(e) => {
                error!(agent_id = %agent_id, error = %e, "Failed to query pacts");
                return;
            }
        };

        if pacts.is_empty() {
            return;
        }

        // Extract the completed agent's result text
        let output = self
            .db
            .get_agent_output(agent_id)
            .unwrap_or(None)
            .unwrap_or_default();

        for pact in pacts {
            let task = pact.task_tpl.replace("{output}", &output);

            info!(
                pact_id = %pact.id,
                source = %agent_id,
                "Firing pact"
            );

            let cwd = self.manager.resolve_cwd(None);
            match self
                .manager
                .enqueue(
                    &task,
                    pact.name.clone(),
                    None,
                    None,
                    &cwd,
                    crate::daemon::agent_manager::Lane::Scroll,
                )
                .await
            {
                Ok(agent) => {
                    if let Err(e) = self.db.update_pact_fired(&pact.id, &agent.id) {
                        error!(pact_id = %pact.id, error = %e, "Failed to update pact state");
                    }
                    info!(
                        pact_id = %pact.id,
                        source = %agent_id,
                        target = %agent.id,
                        "Pact fired successfully"
                    );
                }
                Err(e) => {
                    let _ = self.db.update_pact_failed(&pact.id);
                    error!(
                        pact_id = %pact.id,
                        source = %agent_id,
                        error = %e,
                        "Pact failed to fire"
                    );
                }
            }
        }
    }
}
