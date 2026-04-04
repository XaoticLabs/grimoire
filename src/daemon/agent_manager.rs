use anyhow::{Result, anyhow};
use chrono::Utc;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::shared::config::Config;
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Agent, AgentId, AgentState, AgentSummary};

use super::event_bus::EventBus;
use super::persistence::Database;
use super::process_manager;
use super::provider_registry::ProviderRegistry;

struct ManagedAgent {
    agent: Agent,
    #[allow(dead_code)] // handle kept alive to avoid aborting the monitor task
    monitor_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct AgentManager {
    agents: Mutex<HashMap<AgentId, ManagedAgent>>,
    db: Arc<Database>,
    event_bus: EventBus,
    config: Config,
    registry: ProviderRegistry,
}

impl AgentManager {
    pub async fn new(db: Arc<Database>, event_bus: EventBus, config: Config) -> Arc<Self> {
        let registry = ProviderRegistry::from_config(&config);
        let manager = Arc::new(Self {
            agents: Mutex::new(HashMap::new()),
            db,
            event_bus,
            config,
            registry,
        });

        if let Err(e) = manager.reload_from_db().await {
            error!("Failed to reload agents from DB: {}", e);
        }

        manager
    }

    async fn reload_from_db(&self) -> Result<()> {
        let agents = self.db.list_agents(None)?;
        let mut map = self.agents.lock().await;
        for mut agent in agents {
            if agent.state == AgentState::Summoning || agent.state == AgentState::Active {
                agent.state = AgentState::Failed;
                let _ = self
                    .db
                    .update_agent_state(&agent.id, &AgentState::Failed, None);
            }
            map.insert(
                agent.id.clone(),
                ManagedAgent {
                    agent,
                    monitor_handle: None,
                },
            );
        }
        Ok(())
    }

    /// Spawn a monitor task for a process, updating state when it completes
    fn spawn_monitor(
        self: &Arc<Self>,
        agent_id: AgentId,
        child: tokio::process::Child,
        provider: Arc<dyn super::provider::Provider>,
    ) -> tokio::task::JoinHandle<()> {
        let db = self.db.clone();
        let bus = self.event_bus.clone();
        let id = agent_id.clone();
        let manager = self.clone();

        tokio::spawn(async move {
            let result =
                process_manager::monitor_agent(id.clone(), child, bus.clone(), db.clone(), provider)
                    .await;

            // Store session_id if we captured one
            if let Some(ref sid) = result.session_id {
                if let Err(e) = db.update_agent_session_id(&id, sid) {
                    error!(agent_id = %id, error = %e, "Failed to store session_id");
                }
            }

            // Update DB state
            if let Err(e) = db.update_agent_state(&id, &result.state, result.exit_code) {
                error!(agent_id = %id, error = %e, "Failed to update agent state");
            }

            // Update in-memory state
            let mut agents = manager.agents.lock().await;
            if let Some(managed) = agents.get_mut(&id) {
                managed.agent.state = result.state.clone();
                managed.agent.exit_code = result.exit_code;
                if let Some(ref sid) = result.session_id {
                    managed.agent.session_id = Some(sid.clone());
                }
            }

            bus.publish(StreamEvent::StateChange {
                agent_id: id,
                old_state: AgentState::Active,
                new_state: result.state.clone(),
            });
        })
    }

    pub async fn summon(
        self: &Arc<Self>,
        task: String,
        name: Option<String>,
        model: Option<String>,
        cwd: Option<PathBuf>,
        provider_name: Option<String>,
    ) -> Result<Agent> {
        let provider_name = provider_name.unwrap_or_else(|| self.registry.default_name().to_string());
        let provider = self
            .registry
            .get(&provider_name)
            .ok_or_else(|| anyhow!("Unknown provider: {}", provider_name))?;

        let agent_id = crate::shared::constants::generate_short_id();
        let cwd = cwd
            .or_else(|| self.config.agent.default_cwd.clone())
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")));
        let model = model.or_else(|| self.config.agent.default_model.clone());
        let now = Utc::now();

        let agent = Agent {
            id: agent_id.clone(),
            name: name.clone(),
            state: AgentState::Summoning,
            task: Some(task.clone()),
            model: model.clone(),
            provider: Some(provider_name),
            cwd: cwd.clone(),
            pid: None,
            session_id: None,
            exit_code: None,
            created_at: now,
            updated_at: now,
        };

        self.db.insert_agent(&agent)?;

        self.event_bus
            .publish(StreamEvent::AgentCreated { agent: agent.clone() });

        let spawned = provider.spawn(&task, &cwd, model.as_deref())?;
        let pid = spawned.pid;

        self.db.update_agent_pid(&agent_id, pid)?;
        self.db
            .update_agent_state(&agent_id, &AgentState::Active, None)?;

        let mut agent = agent;
        agent.pid = Some(pid);
        agent.state = AgentState::Active;

        self.event_bus.publish(StreamEvent::StateChange {
            agent_id: agent_id.clone(),
            old_state: AgentState::Summoning,
            new_state: AgentState::Active,
        });

        let handle = self.spawn_monitor(agent_id.clone(), spawned.child, provider);

        let mut agents = self.agents.lock().await;
        agents.insert(
            agent_id,
            ManagedAgent {
                agent: agent.clone(),
                monitor_handle: Some(handle),
            },
        );

        info!(id = %agent.id, task = %task, "Agent summoned");
        Ok(agent)
    }

    pub async fn invoke(
        self: &Arc<Self>,
        id: &str,
        message: String,
    ) -> Result<()> {
        // Look up the agent, its session_id, and provider
        let (session_id, cwd, provider_name) = {
            let agents = self.agents.lock().await;
            let managed = agents
                .get(id)
                .ok_or_else(|| anyhow!("Agent not found: {}", id))?;

            let session_id = managed
                .agent
                .session_id
                .clone()
                .ok_or_else(|| anyhow!("Agent {} has no session to resume", id))?;

            (
                session_id,
                managed.agent.cwd.clone(),
                managed.agent.provider.clone().unwrap_or_else(|| "claude".to_string()),
            )
        };

        let provider = self
            .registry
            .get(&provider_name)
            .ok_or_else(|| anyhow!("Unknown provider: {}", provider_name))?;

        if !provider.capabilities().supports_resume {
            return Err(anyhow!(
                "Provider '{}' does not support session resume",
                provider_name
            ));
        }

        // Update state to active
        self.db
            .update_agent_state(id, &AgentState::Active, None)?;

        self.event_bus.publish(StreamEvent::StateChange {
            agent_id: id.to_string(),
            old_state: AgentState::Complete,
            new_state: AgentState::Active,
        });

        // Spawn resumed process
        let spawned = provider.spawn_resume(&session_id, &message, &cwd)?;
        let pid = spawned.pid;

        self.db.update_agent_pid(id, pid)?;

        // Update in-memory
        {
            let mut agents = self.agents.lock().await;
            if let Some(managed) = agents.get_mut(id) {
                managed.agent.state = AgentState::Active;
                managed.agent.pid = Some(pid);
            }
        }

        let handle = self.spawn_monitor(id.to_string(), spawned.child, provider);

        {
            let mut agents = self.agents.lock().await;
            if let Some(managed) = agents.get_mut(id) {
                managed.monitor_handle = Some(handle);
            }
        }

        info!(id = %id, message = %message, "Agent invoked");
        Ok(())
    }

    pub async fn banish(&self, id: &str) -> Result<bool> {
        let mut agents = self.agents.lock().await;
        if let Some(managed) = agents.get_mut(id) {
            if managed.agent.state == AgentState::Active
                || managed.agent.state == AgentState::Summoning
            {
                if let Some(pid) = managed.agent.pid {
                    if let Err(e) = process_manager::kill_process(pid) {
                        error!(agent_id = %id, error = %e, "Failed to kill agent process");
                    }
                }

                managed.agent.state = AgentState::Banished;
                self.db
                    .update_agent_state(id, &AgentState::Banished, None)?;

                self.event_bus.publish(StreamEvent::StateChange {
                    agent_id: id.to_string(),
                    old_state: AgentState::Active,
                    new_state: AgentState::Banished,
                });

                info!(id = %id, "Agent banished");
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn circle(&self, state_filter: Option<&str>) -> Result<Vec<AgentSummary>> {
        let agents = self.db.list_agents(state_filter)?;
        let now = Utc::now();
        Ok(agents
            .into_iter()
            .map(|a| {
                let age = now.signed_duration_since(a.created_at).num_seconds();
                AgentSummary {
                    id: a.id,
                    name: a.name,
                    state: a.state,
                    task: a.task,
                    age_secs: age,
                }
            })
            .collect())
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
}
