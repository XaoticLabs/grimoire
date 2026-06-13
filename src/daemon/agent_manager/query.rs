//! Read-side surface: agent result extraction, roster listing, single-agent
//! and event lookups, event-bus subscription, plus the test-only seed helpers.

use anyhow::Result;
use chrono::Utc;
use std::path::PathBuf;
use std::sync::Arc;

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Agent, AgentId, AgentState, AgentSummary, RestartPolicy};

use super::super::event_bus::EventBus;
use super::{AgentManager, ManagedAgent};

impl AgentManager {
    /// The agent's result text, extracted per its provider's `extract_result`
    /// (for pact `{output}` injection). `None` if nothing usable.
    pub fn agent_result(&self, agent_id: &str) -> Option<String> {
        let provider_name = self
            .db
            .get_agent(agent_id)
            .ok()
            .flatten()
            .and_then(|a| a.provider)
            .unwrap_or_else(|| self.registry.default_name().to_string());
        let provider = self.registry.get(&provider_name)?;
        let lines = self.db.get_agent_stdout_lines(agent_id).ok()?;
        provider.extract_result(&lines)
    }

    pub async fn circle(&self, state_filter: Option<&str>) -> Result<Vec<AgentSummary>> {
        let agents = self.db.list_agents(state_filter)?;
        let now = Utc::now();
        let mut out = Vec::with_capacity(agents.len());
        for a in agents {
            let age = now.signed_duration_since(a.created_at).num_seconds();
            let max_restarts = self
                .db
                .get_supervision(&a.id)
                .ok()
                .flatten()
                .and_then(|c| c.max_restarts);
            out.push(AgentSummary {
                id: a.id,
                name: a.name,
                state: a.state,
                task: a.task,
                age_secs: age,
                worker_id: a.worker_id,
                restart_policy: a.restart_policy,
                restart_count: a.restart_count,
                max_restarts,
            });
        }
        Ok(out)
    }

    pub async fn get_agent(&self, id: &str) -> Result<Option<Agent>> {
        self.db.get_agent(id)
    }

    pub fn get_events(
        &self,
        agent_id: &str,
        tail: Option<usize>,
    ) -> Result<Vec<crate::shared::types::AgentEvent>> {
        self.db.get_events(agent_id, tail)
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<StreamEvent> {
        self.event_bus.subscribe()
    }

    /// Clone of the shared EventBus, for handlers that emit events directly.
    pub fn event_bus(&self) -> EventBus {
        self.event_bus.clone()
    }

    /// Test helper: seed a `Dormant` agent with a known session_id so `invoke`
    /// can be driven without a real `summon`.
    pub async fn seed_agent_for_test_with_session(
        self: &Arc<Self>,
        session_id: &str,
    ) -> Result<AgentId> {
        self.seed_agent_for_test_with_session_provider(session_id, None)
            .await
    }

    /// Like [`Self::seed_agent_for_test_with_session`] but pins the provider,
    /// so tests can exercise both resume strategies.
    pub async fn seed_agent_for_test_with_session_provider(
        self: &Arc<Self>,
        session_id: &str,
        provider: Option<&str>,
    ) -> Result<AgentId> {
        let agent_id = crate::shared::constants::generate_short_id();
        let now = Utc::now();
        let provider =
            provider.map_or_else(|| self.registry.default_name().to_string(), str::to_string);
        let agent = Agent {
            id: agent_id.clone(),
            name: None,
            state: AgentState::Dormant,
            task: Some("seed".to_string()),
            model: None,
            provider: Some(provider),
            cwd: PathBuf::from("/tmp"),
            pid: None,
            session_id: Some(session_id.to_string()),
            exit_code: Some(0),
            created_at: now,
            updated_at: now,
            worker_id: None,
            restart_policy: RestartPolicy::Never,
            restart_count: 0,
            workspace_id: None,
        };
        self.db.insert_agent(&agent)?;
        self.db.update_agent_session_id(&agent_id, session_id)?;
        let mut map = self.agents.lock().await;
        map.insert(
            agent_id.clone(),
            ManagedAgent {
                agent,
                cancel: None,
                completion_handle: None,
            },
        );
        Ok(agent_id)
    }
}
