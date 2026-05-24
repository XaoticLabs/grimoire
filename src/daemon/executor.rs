use parking_lot::Mutex;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use semver::VersionReq;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::shared::types::{AgentId, AgentState};
use crate::shared::worker_proto::{
    AssignTask, CancelTask, DaemonMessage, TaskEvent, TaskFinished, TaskState, daemon_message,
    task_event::EventKind,
};

use super::event_bus::EventBus;
use super::persistence::Database;
use super::process_manager::{self, LineSource, MonitorResult, SpawnedAgent};
use super::provider_registry::ProviderRegistry;
use super::worker_registry::WorkerRegistry;

#[derive(Debug, Clone)]
pub struct ExecuteRequest {
    pub agent_id: AgentId,
    pub task: String,
    pub provider_name: String,
    pub cwd: PathBuf,
    pub model: Option<String>,
    pub resume_session_id: Option<String>,
}

pub struct ExecutorHandle {
    pub worker_id: Option<String>,
    pub pid: Option<u32>,
    pub cancel: Box<dyn FnOnce() + Send>,
    pub completion: JoinHandle<MonitorResult>,
}

#[async_trait]
pub trait Executor: Send + Sync {
    async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle>;
    fn name(&self) -> &str;
}

// --- LocalExecutor ---------------------------------------------------------

pub struct LocalExecutor {
    registry: Arc<ProviderRegistry>,
    bus: EventBus,
    db: Arc<Database>,
    test_command: Option<(String, Vec<String>)>,
}

impl LocalExecutor {
    pub const fn new(registry: Arc<ProviderRegistry>, bus: EventBus, db: Arc<Database>) -> Self {
        Self {
            registry,
            bus,
            db,
            test_command: None,
        }
    }

    pub fn test_with_command(cmd: &str, args: &[&str]) -> Self {
        let db = Arc::new(Database::open_in_memory().unwrap());
        let bus = EventBus::new(db.clone());
        let registry = Arc::new(ProviderRegistry::test_with_true_provider());
        Self {
            registry,
            bus,
            db,
            test_command: Some((
                cmd.to_string(),
                args.iter().map(std::string::ToString::to_string).collect(),
            )),
        }
    }
}

#[async_trait]
impl Executor for LocalExecutor {
    async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle> {
        let (spawned, provider) = if let Some((cmd, args)) = &self.test_command {
            let mut command = Command::new(cmd);
            command
                .args(args)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let child = command.spawn()?;
            let pid = child.id().unwrap_or(0);
            (SpawnedAgent { child, pid }, None)
        } else {
            let provider = self
                .registry
                .get(&req.provider_name)
                .ok_or_else(|| anyhow!("Unknown provider: {}", req.provider_name))?;
            let ctx = crate::daemon::provider::AgentContext {
                agent_id: req.agent_id.clone(),
                sandbox: self.registry.sandbox_for(&req.provider_name),
            };
            let spawned = if let Some(sid) = &req.resume_session_id {
                provider.spawn_resume(sid, &req.task, &req.cwd, &ctx)?
            } else {
                provider.spawn(&req.task, &req.cwd, req.model.as_deref(), &ctx)?
            };
            (spawned, Some(provider))
        };

        let pid = spawned.pid;
        let bus = self.bus.clone();
        let db = self.db.clone();
        let agent_id = req.agent_id.clone();

        let completion = tokio::spawn(async move {
            process_manager::monitor_agent(agent_id, spawned.child, bus, db, provider).await
        });

        let cancel: Box<dyn FnOnce() + Send> = Box::new(move || {
            let _ = process_manager::kill_process(pid);
        });

        Ok(ExecutorHandle {
            worker_id: None,
            pid: Some(pid),
            cancel,
            completion,
        })
    }

    fn name(&self) -> &'static str {
        "local"
    }
}

// --- RemoteExecutor --------------------------------------------------------

#[derive(Debug)]
pub enum RoutedInbound {
    Event(TaskEvent),
    Finished(TaskFinished),
}

pub struct RemoteExecutor {
    worker_id: String,
    assign_tx: mpsc::Sender<DaemonMessage>,
    inbound_rx: Mutex<Option<mpsc::Receiver<RoutedInbound>>>,
    bus: Option<EventBus>,
    db: Option<Arc<Database>>,
}

impl RemoteExecutor {
    pub const fn for_test(
        worker_id: String,
        assign_tx: mpsc::Sender<DaemonMessage>,
        inbound_rx: mpsc::Receiver<RoutedInbound>,
        bus: EventBus,
        db: Arc<Database>,
    ) -> Self {
        Self {
            worker_id,
            assign_tx,
            inbound_rx: Mutex::new(Some(inbound_rx)),
            bus: Some(bus),
            db: Some(db),
        }
    }

    /// Stub used purely by `Placement` tests — never started.
    pub fn stub_for_test(worker_id: String) -> Self {
        let (tx, _) = mpsc::channel(1);
        let (_, rx) = mpsc::channel(1);
        Self {
            worker_id,
            assign_tx: tx,
            inbound_rx: Mutex::new(Some(rx)),
            bus: None,
            db: None,
        }
    }
}

async fn run_remote_completion(
    agent_id: AgentId,
    mut inbound: mpsc::Receiver<RoutedInbound>,
    bus: EventBus,
    db: Arc<Database>,
) -> MonitorResult {
    while let Some(msg) = inbound.recv().await {
        match msg {
            RoutedInbound::Event(ev) => {
                let source = if ev.kind == EventKind::Stderr as i32 {
                    LineSource::Stderr
                } else {
                    LineSource::Stdout
                };
                let _ = process_manager::persist_event(&db, &agent_id, source, &ev.payload);
                process_manager::publish_output(&bus, &agent_id, source, &ev.payload);
            }
            RoutedInbound::Finished(fin) => {
                let state = match TaskState::try_from(fin.state).unwrap_or(TaskState::Failed) {
                    TaskState::Complete => AgentState::Complete,
                    TaskState::Failed => AgentState::Failed,
                    TaskState::Banished => AgentState::Banished,
                };
                return MonitorResult {
                    state,
                    exit_code: fin.exit_code,
                    session_id: fin.session_id,
                    error_reason: fin.error_reason,
                    tokens_used: None,
                };
            }
        }
    }
    // Stream closed without a TaskFinished — treat as worker_lost.
    MonitorResult {
        state: AgentState::Failed,
        exit_code: None,
        session_id: None,
        error_reason: Some("worker_lost".to_string()),
        tokens_used: None,
    }
}

#[async_trait]
impl Executor for RemoteExecutor {
    async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle> {
        let inbound_rx = self
            .inbound_rx
            .lock()
            .take()
            .ok_or_else(|| anyhow!("RemoteExecutor inbound already consumed"))?;

        let assign = AssignTask {
            agent_id: req.agent_id.clone(),
            task: req.task.clone(),
            provider_constraint: String::new(),
            provider_name: req.provider_name.clone(),
            cwd: req.cwd.to_string_lossy().to_string(),
            model: req.model.clone(),
            env: std::collections::HashMap::default(),
            optional_resume_session_id: req.resume_session_id.clone().map(
                crate::shared::worker_proto::assign_task::OptionalResumeSessionId::ResumeSessionId,
            ),
        };
        self.assign_tx
            .send(DaemonMessage {
                kind: Some(daemon_message::Kind::AssignTask(assign)),
            })
            .await
            .map_err(|e| anyhow!("send AssignTask: {e}"))?;

        let bus = self
            .bus
            .clone()
            .ok_or_else(|| anyhow!("RemoteExecutor missing bus (stub)"))?;
        let db = self
            .db
            .clone()
            .ok_or_else(|| anyhow!("RemoteExecutor missing db (stub)"))?;
        let agent_id_for_complete = req.agent_id.clone();
        let completion = tokio::spawn(async move {
            run_remote_completion(agent_id_for_complete, inbound_rx, bus, db).await
        });

        let cancel_tx = self.assign_tx.clone();
        let cancel_agent = req.agent_id.clone();
        let cancel: Box<dyn FnOnce() + Send> = Box::new(move || {
            tokio::spawn(async move {
                let _ = cancel_tx
                    .send(DaemonMessage {
                        kind: Some(daemon_message::Kind::CancelTask(CancelTask {
                            agent_id: cancel_agent,
                        })),
                    })
                    .await;
            });
        });

        Ok(ExecutorHandle {
            worker_id: Some(self.worker_id.clone()),
            pid: None,
            cancel,
            completion,
        })
    }

    fn name(&self) -> &'static str {
        "remote"
    }
}

// --- Placement -------------------------------------------------------------

pub trait Placement: Send + Sync {
    fn pick(&self, req: &ExecuteRequest) -> Arc<dyn Executor>;
}

pub type RemoteFactory = Arc<dyn Fn(String) -> Arc<dyn Executor> + Send + Sync>;

pub struct LeastLoadedPlacement {
    registry: Arc<WorkerRegistry>,
    local: Arc<LocalExecutor>,
    remote_factory: RemoteFactory,
}

impl LeastLoadedPlacement {
    pub fn new(
        registry: Arc<WorkerRegistry>,
        local: Arc<LocalExecutor>,
        remote_factory: RemoteFactory,
    ) -> Self {
        Self {
            registry,
            local,
            remote_factory,
        }
    }
}

impl Placement for LeastLoadedPlacement {
    fn pick(&self, req: &ExecuteRequest) -> Arc<dyn Executor> {
        let constraint = VersionReq::STAR;
        match self
            .registry
            .pick_least_loaded(&req.provider_name, &constraint)
        {
            Some(worker_id) => (self.remote_factory)(worker_id),
            None => self.local.clone(),
        }
    }
}
