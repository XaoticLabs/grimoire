use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Agent, AgentId, AgentState, AgentSummary};

use super::event_bus::EventBus;
use super::persistence::Database;
use super::process_manager;

struct ManagedAgent {
    agent: Agent,
    monitor_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct AgentManager {
    agents: Mutex<HashMap<AgentId, ManagedAgent>>,
    db: Arc<Database>,
    event_bus: EventBus,
}

impl AgentManager {
    pub async fn new(db: Arc<Database>, event_bus: EventBus) -> Arc<Self> {
        let manager = Arc::new(Self {
            agents: Mutex::new(HashMap::new()),
            db,
            event_bus,
        });

        // Reload agents from DB on startup
        if let Err(e) = manager.reload_from_db().await {
            error!("Failed to reload agents from DB: {}", e);
        }

        manager
    }

    async fn reload_from_db(&self) -> Result<()> {
        let agents = self.db.list_agents(None)?;
        let mut map = self.agents.lock().await;
        for mut agent in agents {
            // Mark any previously-active agents as failed (daemon restarted)
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

    pub async fn summon(
        self: &Arc<Self>,
        task: String,
        name: Option<String>,
        model: Option<String>,
        cwd: Option<PathBuf>,
    ) -> Result<Agent> {
        let agent_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cwd = cwd.unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")));
        let now = Utc::now();

        let agent = Agent {
            id: agent_id.clone(),
            name: name.clone(),
            state: AgentState::Summoning,
            task: Some(task.clone()),
            model: model.clone(),
            cwd: cwd.clone(),
            pid: None,
            session_id: None,
            exit_code: None,
            created_at: now,
            updated_at: now,
        };

        // Persist
        self.db.insert_agent(&agent)?;

        // Broadcast creation
        self.event_bus
            .publish(StreamEvent::AgentCreated { agent: agent.clone() });

        // Spawn process
        let spawned = process_manager::spawn_claude(&task, &cwd, model.as_deref())?;
        let pid = spawned.pid;

        // Update state to active
        self.db.update_agent_pid(&agent_id, pid)?;
        self.db
            .update_agent_state(&agent_id, &AgentState::Active, None)?;

        let mut agent = agent;
        agent.pid = Some(pid);
        agent.state = AgentState::Active;

        self.event_bus.publish(StreamEvent::StateChange {
            agent_id: agent_id.clone(),
            old_state: "summoning".to_string(),
            new_state: "active".to_string(),
        });

        // Start monitoring in background
        let db = self.db.clone();
        let bus = self.event_bus.clone();
        let id = agent_id.clone();
        let manager = self.clone();

        let handle = tokio::spawn(async move {
            let (final_state, exit_code) =
                process_manager::monitor_agent(id.clone(), spawned.child, bus.clone(), db.clone())
                    .await;

            // Update DB
            if let Err(e) = db.update_agent_state(&id, &final_state, exit_code) {
                error!(agent_id = %id, error = %e, "Failed to update agent state");
            }

            // Update in-memory state
            let mut agents = manager.agents.lock().await;
            if let Some(managed) = agents.get_mut(&id) {
                managed.agent.state = final_state.clone();
                managed.agent.exit_code = exit_code;
            }

            bus.publish(StreamEvent::StateChange {
                agent_id: id,
                old_state: "active".to_string(),
                new_state: final_state.as_str().to_string(),
            });
        });

        // Store in memory
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

    pub async fn banish(&self, id: &str) -> Result<bool> {
        let mut agents = self.agents.lock().await;
        if let Some(managed) = agents.get_mut(id) {
            if managed.agent.state == AgentState::Active
                || managed.agent.state == AgentState::Summoning
            {
                // Kill the process
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
                    old_state: "active".to_string(),
                    new_state: "banished".to_string(),
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
