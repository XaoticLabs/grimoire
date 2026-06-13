//! Agent lifecycle manager: in-memory `ManagedAgent` registry plus the
//! constructor/accessor surface. Dispatch, lifecycle, and queries extend
//! `AgentManager` from sibling submodules.

use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;
use tracing::error;

use crate::shared::config::Config;
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Agent, AgentId, AgentState};

use super::event_bus::EventBus;
use super::executor::{Executor, LocalExecutor};
use super::persistence::Database;
use super::provider_registry::ProviderRegistry;
use super::supervisor::Supervisor;
use super::wake_registry::WakeRegistry;

// These submodules only add `impl AgentManager` / trait-impl blocks.
mod dispatch;
mod lifecycle;
mod query;

/// Byte budget for the `ContextReplay` transcript prepended on resume.
const CONTEXT_REPLAY_BUDGET_BYTES: usize = 16 * 1024;

/// Queue lane. The scheduler drains `Adhoc` before `Scroll` each tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    Adhoc,
    Scroll,
}

impl Lane {
    pub const fn as_str(self) -> &'static str {
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
    /// Self-reference for the `Arc<Self>`-flavored `dispatch_internal`, set via
    /// `Arc::new_cyclic`.
    weak_self: Weak<Self>,
    /// Set by daemon boot; banish cascades retire the agent's wake sources.
    wake_registry: Mutex<Option<Arc<WakeRegistry>>>,
    /// Set by daemon boot; banish cascades cancel pending restarts.
    supervisor: Mutex<Option<Arc<Supervisor>>>,
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
            wake_registry: Mutex::new(None),
            supervisor: Mutex::new(None),
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

    /// Inject the wake registry (once, at boot).
    pub async fn set_wake_registry(&self, registry: Arc<WakeRegistry>) {
        *self.wake_registry.lock().await = Some(registry);
    }

    /// Inject the supervisor (once, at boot).
    pub async fn set_supervisor(&self, supervisor: Arc<Supervisor>) {
        *self.supervisor.lock().await = Some(supervisor);
    }

    /// The registered supervisor, or `None` before boot wiring completes.
    pub async fn supervisor(&self) -> Option<Arc<Supervisor>> {
        self.supervisor.lock().await.clone()
    }

    /// Resolve an optional caller-provided cwd to a concrete path, falling back
    /// to config defaults and then process cwd.
    /// Read-only accessor on the loaded `[policy]` block, used by
    /// `handle_summon` to gate by provider / cwd allow-deny lists.
    pub const fn policy(&self) -> Option<&crate::shared::config::PolicyConfig> {
        self.config.policy.as_ref()
    }

    /// The loaded `[budgets.*]` blocks.
    pub const fn budgets(
        &self,
    ) -> &std::collections::HashMap<String, crate::shared::config::BudgetConfig> {
        &self.config.budgets
    }

    /// Provider a summon without `--provider` defaults to; lets the policy
    /// gate match the resolved name rather than `None`.
    pub fn default_provider_name(&self) -> &str {
        self.registry.default_name()
    }

    pub fn resolve_cwd(&self, cwd: Option<PathBuf>) -> PathBuf {
        cwd.or_else(|| self.config.agent.default_cwd.clone())
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/")))
    }

    pub const fn max_concurrent_agents(&self) -> u32 {
        self.config.daemon.max_concurrent_agents
    }
}
