use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::shared::config::Config;
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Agent, AgentId, AgentState, AgentSummary};

use super::event_bus::EventBus;
use super::executor::{ExecuteRequest, Executor, ExecutorHandle, LocalExecutor};
use super::persistence::{Database, QueueRow};
use super::process_manager;
use super::provider_registry::ProviderRegistry;
use super::scheduler::{Dispatcher, MailWaker};

/// Which queue lane an enqueued agent belongs to. Drives dispatch ordering:
/// the scheduler drains `Adhoc` before `Scroll` on each tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Adhoc,
    Scroll,
}

impl Lane {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Adhoc => "adhoc",
            Self::Scroll => "scroll",
        }
    }
}

struct ManagedAgent {
    agent: Agent,
    cancel: Option<Box<dyn FnOnce() + Send>>,
    #[allow(dead_code)] // handle kept alive to avoid aborting the monitor task
    completion_handle: Option<tokio::task::JoinHandle<()>>,
}

pub struct AgentManager {
    agents: Mutex<HashMap<AgentId, ManagedAgent>>,
    db: Arc<Database>,
    event_bus: EventBus,
    config: Config,
    registry: Arc<ProviderRegistry>,
    executor: Arc<dyn Executor>,
    /// Self-reference used by `Dispatcher::dispatch` to call into the
    /// `Arc<Self>`-flavored `dispatch_internal`. Set at construction via
    /// `Arc::new_cyclic`.
    weak_self: Weak<Self>,
}

impl AgentManager {
    pub async fn new(db: Arc<Database>, event_bus: EventBus, config: Config) -> Arc<Self> {
        let registry = Arc::new(ProviderRegistry::from_config(&config));
        let executor: Arc<dyn Executor> = Arc::new(LocalExecutor::new(
            registry.clone(),
            event_bus.clone(),
            db.clone(),
        ));
        Self::new_inner(db, event_bus, config, registry, executor).await
    }

    pub async fn new_with_executor(
        db: Arc<Database>,
        event_bus: EventBus,
        config: Config,
        executor: Arc<dyn Executor>,
    ) -> Arc<Self> {
        let registry = Arc::new(ProviderRegistry::from_config(&config));
        Self::new_inner(db, event_bus, config, registry, executor).await
    }

    async fn new_inner(
        db: Arc<Database>,
        event_bus: EventBus,
        config: Config,
        registry: Arc<ProviderRegistry>,
        executor: Arc<dyn Executor>,
    ) -> Arc<Self> {
        let manager = Arc::new_cyclic(|weak| Self {
            agents: Mutex::new(HashMap::new()),
            db,
            event_bus,
            config,
            registry,
            executor,
            weak_self: weak.clone(),
        });

        if let Err(e) = manager.reload_from_db().await {
            error!("Failed to reload agents from DB: {}", e);
        }

        manager
    }

    async fn reload_from_db(&self) -> Result<()> {
        let report = self.db.restart_recovery()?;

        for (agent_id, old_state) in &report.failed {
            self.event_bus.publish(StreamEvent::StateChange {
                agent_id: agent_id.clone(),
                old_state: old_state.clone(),
                new_state: AgentState::Failed,
            });
        }

        let agents = self.db.list_agents(None)?;
        let mut map = self.agents.lock().await;
        for agent in agents {
            map.insert(
                agent.id.clone(),
                ManagedAgent {
                    agent,
                    cancel: None,
                    completion_handle: None,
                },
            );
        }
        Ok(())
    }

    /// Resolve an optional caller-provided cwd to a concrete path, falling back
    /// to config defaults and then process cwd.
    pub fn resolve_cwd(&self, cwd: Option<PathBuf>) -> PathBuf {
        cwd.or_else(|| self.config.agent.default_cwd.clone())
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")))
    }

    pub fn max_concurrent_agents(&self) -> u32 {
        self.config.daemon.max_concurrent_agents
    }

    /// Wire an executor's completion future into agent state updates and the
    /// event bus, mirroring the previous spawn_monitor body.
    fn watch_completion(
        self: &Arc<Self>,
        agent_id: AgentId,
        completion: tokio::task::JoinHandle<process_manager::MonitorResult>,
    ) -> tokio::task::JoinHandle<()> {
        let db = self.db.clone();
        let bus = self.event_bus.clone();
        let manager = self.clone();
        tokio::spawn(async move {
            let result = match completion.await {
                Ok(r) => r,
                Err(e) => {
                    error!(agent_id = %agent_id, error = %e, "executor completion task panicked");
                    process_manager::MonitorResult {
                        state: AgentState::Failed,
                        exit_code: None,
                        session_id: None,
                        error_reason: Some(format!("completion_panicked: {}", e)),
                    }
                }
            };

            if let Some(ref sid) = result.session_id {
                if let Err(e) = db.update_agent_session_id(&agent_id, sid) {
                    error!(agent_id = %agent_id, error = %e, "Failed to store session_id");
                }
            }

            if let Err(e) = db.update_agent_state(&agent_id, &result.state, result.exit_code) {
                error!(agent_id = %agent_id, error = %e, "Failed to update agent state");
            }

            let mut agents = manager.agents.lock().await;
            if let Some(managed) = agents.get_mut(&agent_id) {
                managed.agent.state = result.state.clone();
                managed.agent.exit_code = result.exit_code;
                if let Some(ref sid) = result.session_id {
                    managed.agent.session_id = Some(sid.clone());
                }
                managed.cancel = None;
            }

            bus.publish(StreamEvent::StateChange {
                agent_id,
                old_state: AgentState::Active,
                new_state: result.state.clone(),
            });
        })
    }

    /// Enqueue an agent for the scheduler to pick up. Inserts the agent in
    /// `Queued` state and writes a row into `task_queue`; does NOT start the
    /// executor — that is the scheduler's job (see [`AgentManager::dispatch_internal`]).
    pub async fn enqueue(
        self: &Arc<Self>,
        task: &str,
        name: Option<String>,
        model: Option<String>,
        provider_name: Option<String>,
        cwd: &Path,
        lane: Lane,
    ) -> Result<Agent> {
        let provider_name =
            provider_name.unwrap_or_else(|| self.registry.default_name().to_string());

        let agent_id = crate::shared::constants::generate_short_id();
        let cwd_path = cwd.to_path_buf();
        let model = model.or_else(|| self.config.agent.default_model.clone());
        let now = Utc::now();

        let agent = Agent {
            id: agent_id.clone(),
            name: name.clone(),
            state: AgentState::Queued,
            task: Some(task.to_string()),
            model: model.clone(),
            provider: Some(provider_name.clone()),
            cwd: cwd_path.clone(),
            pid: None,
            session_id: None,
            exit_code: None,
            created_at: now,
            updated_at: now,
            worker_id: None,
        };

        self.db.insert_agent(&agent)?;

        let row = QueueRow {
            id: agent_id.clone(),
            lane: lane.as_str().to_string(),
            priority: 0,
            enqueued_at: now,
            provider_name: Some(provider_name.clone()),
            cwd: cwd_path.to_string_lossy().to_string(),
            model: model.clone(),
            task_text: task.to_string(),
            block_reason: None,
        };
        self.db.enqueue_task(&row)?;

        self.event_bus
            .publish(StreamEvent::AgentCreated { agent: agent.clone() });
        self.event_bus.publish(StreamEvent::AgentQueued {
            agent_id: agent_id.clone(),
            lane: lane.as_str().to_string(),
            block_reason: None,
        });

        let mut agents = self.agents.lock().await;
        agents.insert(
            agent_id,
            ManagedAgent {
                agent: agent.clone(),
                cancel: None,
                completion_handle: None,
            },
        );

        info!(id = %agent.id, task = %task, lane = %lane.as_str(), "Agent enqueued");
        Ok(agent)
    }

    /// Drive a claimed queue row through `executor.start` and the
    /// `Summoning -> Active` transition. Called only by the scheduler after a
    /// successful `claim_for_dispatch`. On failure, returns `Err` *without*
    /// mutating queue state — the scheduler owns the requeue path so the
    /// row's original `enqueued_at` (and therefore lane fairness) is preserved.
    pub(crate) async fn dispatch_internal(self: &Arc<Self>, row: QueueRow) -> Result<()> {
        let agent_id = row.id.clone();
        let provider_name = row
            .provider_name
            .clone()
            .unwrap_or_else(|| self.registry.default_name().to_string());

        let req = ExecuteRequest {
            agent_id: agent_id.clone(),
            task: row.task_text.clone(),
            provider_name,
            cwd: PathBuf::from(&row.cwd),
            model: row.model.clone(),
            resume_session_id: None,
        };

        let handle = self.executor.start(req).await?;
        let ExecutorHandle {
            pid,
            cancel,
            completion,
            worker_id: _,
        } = handle;

        if let Some(p) = pid {
            self.db.update_agent_pid(&agent_id, p)?;
        }
        self.db
            .update_agent_state(&agent_id, &AgentState::Active, None)?;

        self.event_bus.publish(StreamEvent::StateChange {
            agent_id: agent_id.clone(),
            old_state: AgentState::Summoning,
            new_state: AgentState::Active,
        });

        let completion_handle = self.watch_completion(agent_id.clone(), completion);

        let mut agents = self.agents.lock().await;
        if !agents.contains_key(&agent_id) {
            // Rare path: scheduler ticked before `reload_from_db` populated
            // the in-memory map (e.g., a `Queued` row recovered after
            // restart). Backfill the entry from the queue row + DB state.
            agents.insert(
                agent_id.clone(),
                ManagedAgent {
                    agent: Agent {
                        id: agent_id.clone(),
                        name: None,
                        state: AgentState::Active,
                        task: Some(row.task_text.clone()),
                        model: row.model.clone(),
                        provider: row.provider_name.clone(),
                        cwd: PathBuf::from(&row.cwd),
                        pid,
                        session_id: None,
                        exit_code: None,
                        created_at: row.enqueued_at,
                        updated_at: Utc::now(),
                        worker_id: None,
                    },
                    cancel: None,
                    completion_handle: None,
                },
            );
        }
        let managed = agents.get_mut(&agent_id).expect("just inserted or pre-existing");
        managed.agent.state = AgentState::Active;
        managed.agent.pid = pid;
        managed.cancel = Some(cancel);
        managed.completion_handle = Some(completion_handle);

        info!(id = %agent_id, "Agent dispatched");
        Ok(())
    }

    pub async fn invoke(
        self: &Arc<Self>,
        id: &str,
        message: &str,
        model: Option<String>,
    ) -> Result<()> {
        let (session_id, cwd, provider_name) = {
            let agents = self.agents.lock().await;
            let managed = agents
                .get(id)
                .ok_or_else(|| anyhow!("Agent not found: {}", id))?;

            if managed.agent.state == AgentState::Queued {
                return Err(anyhow!(
                    "Agent {} has not started yet (state: queued)",
                    id
                ));
            }

            let session_id = managed
                .agent
                .session_id
                .clone()
                .ok_or_else(|| anyhow!("Agent {} has no session to resume", id))?;

            (
                session_id,
                managed.agent.cwd.clone(),
                managed
                    .agent
                    .provider
                    .clone()
                    .unwrap_or_else(|| "claude".to_string()),
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

        self.db
            .update_agent_state(id, &AgentState::Active, None)?;

        self.event_bus.publish(StreamEvent::StateChange {
            agent_id: id.to_string(),
            old_state: AgentState::Complete,
            new_state: AgentState::Active,
        });

        let req = ExecuteRequest {
            agent_id: id.to_string(),
            task: message.to_string(),
            provider_name,
            cwd,
            model,
            resume_session_id: Some(session_id),
        };

        let handle = self.executor.start(req).await?;
        let ExecutorHandle {
            pid,
            cancel,
            completion,
            worker_id: _,
        } = handle;

        if let Some(p) = pid {
            self.db.update_agent_pid(id, p)?;
        }

        {
            let mut agents = self.agents.lock().await;
            if let Some(managed) = agents.get_mut(id) {
                managed.agent.state = AgentState::Active;
                managed.agent.pid = pid;
                managed.cancel = Some(cancel);
            }
        }

        let completion_handle = self.watch_completion(id.to_string(), completion);

        {
            let mut agents = self.agents.lock().await;
            if let Some(managed) = agents.get_mut(id) {
                managed.completion_handle = Some(completion_handle);
            }
        }

        info!(id = %id, message = %message, "Agent invoked");
        Ok(())
    }

    pub async fn banish(&self, id: &str) -> Result<bool> {
        let mut agents = self.agents.lock().await;
        let Some(managed) = agents.get_mut(id) else {
            return Ok(false);
        };

        match managed.agent.state {
            AgentState::Queued => {
                // Queued agents have no process and no executor handle — just
                // dequeue and flip to Banished. `delete_from_queue` is
                // idempotent: if the scheduler already claimed the row (state
                // would have moved to Summoning, so we wouldn't be here), the
                // false return is harmless.
                self.db.delete_from_queue(&id.to_string())?;
                managed.agent.state = AgentState::Banished;
                self.db
                    .update_agent_state(id, &AgentState::Banished, None)?;

                self.event_bus.publish(StreamEvent::StateChange {
                    agent_id: id.to_string(),
                    old_state: AgentState::Queued,
                    new_state: AgentState::Banished,
                });

                info!(id = %id, "Queued agent banished");
                Ok(true)
            }
            AgentState::Active | AgentState::Summoning => {
                let old_state = managed.agent.state.clone();
                if let Some(cancel) = managed.cancel.take() {
                    cancel();
                } else if let Some(pid) = managed.agent.pid {
                    if let Err(e) = process_manager::kill_process(pid) {
                        error!(agent_id = %id, error = %e, "Failed to kill agent process");
                    }
                }

                managed.agent.state = AgentState::Banished;
                self.db
                    .update_agent_state(id, &AgentState::Banished, None)?;

                self.event_bus.publish(StreamEvent::StateChange {
                    agent_id: id.to_string(),
                    old_state,
                    new_state: AgentState::Banished,
                });

                info!(id = %id, "Agent banished");
                Ok(true)
            }
            _ => Ok(false),
        }
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
                    worker_id: a.worker_id,
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

    /// Clone of the EventBus shared by this manager. Used by RPC handlers
    /// that emit events without going through the manager (e.g. mail.send).
    pub fn event_bus(&self) -> EventBus {
        self.event_bus.clone()
    }

    /// Test helper: insert an agent with a known session_id so `invoke` can be
    /// driven without a prior real `summon`.
    pub async fn seed_agent_for_test_with_session(
        self: &Arc<Self>,
        session_id: &str,
    ) -> Result<AgentId> {
        let agent_id = crate::shared::constants::generate_short_id();
        let now = Utc::now();
        let agent = Agent {
            id: agent_id.clone(),
            name: None,
            state: AgentState::Complete,
            task: Some("seed".to_string()),
            model: None,
            provider: Some(self.registry.default_name().to_string()),
            cwd: PathBuf::from("/tmp"),
            pid: None,
            session_id: Some(session_id.to_string()),
            exit_code: Some(0),
            created_at: now,
            updated_at: now,
            worker_id: None,
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

#[async_trait]
impl Dispatcher for AgentManager {
    async fn dispatch(&self, row: QueueRow) -> Result<()> {
        let arc = self
            .weak_self
            .upgrade()
            .ok_or_else(|| anyhow!("agent manager has been dropped"))?;
        arc.dispatch_internal(row).await
    }
}

#[async_trait]
impl MailWaker for AgentManager {
    async fn wake(&self, agent_id: &str, prompt: &str) -> Result<()> {
        let arc = self
            .weak_self
            .upgrade()
            .ok_or_else(|| anyhow!("agent manager has been dropped"))?;
        arc.invoke(agent_id, prompt, None).await
    }
}
