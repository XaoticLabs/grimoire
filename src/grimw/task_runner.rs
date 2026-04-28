use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::shared::worker_proto::{
    AssignTask, TaskAccepted, TaskEvent, TaskFinished, TaskRejected, WorkerMessage,
    task_event::EventKind, worker_message,
};

use super::config::ProviderConfig;

/// Per-task slot holding the spawned child process (None once finished/cancelled).
pub type ChildSlot = Arc<Mutex<Option<Child>>>;
/// Map of task_id → child slot for all in-flight tasks.
pub type RunningTasks = Arc<Mutex<HashMap<String, ChildSlot>>>;

#[derive(Clone)]
pub struct TaskDispatcher {
    pub providers: HashMap<String, ProviderConfig>,
    pub in_flight: Arc<AtomicU32>,
    pub max_concurrent: u32,
    pub running: RunningTasks,
    pub draining: Arc<std::sync::atomic::AtomicBool>,
}

impl TaskDispatcher {
    pub fn new(providers: HashMap<String, ProviderConfig>, max_concurrent: u32) -> Self {
        Self {
            providers,
            in_flight: Arc::new(AtomicU32::new(0)),
            max_concurrent,
            running: Arc::new(Mutex::new(HashMap::new())),
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn in_flight(&self) -> u32 {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// Validate an assignment: returns Err with a rejection reason if it
    /// cannot be accepted.
    fn validate(&self, assign: &AssignTask) -> Result<&ProviderConfig, &'static str> {
        if self.draining.load(Ordering::SeqCst) {
            return Err("draining");
        }
        if self.in_flight() >= self.max_concurrent {
            return Err("at_capacity");
        }
        let provider = self
            .providers
            .get(&assign.provider_name)
            .ok_or("provider_missing")?;
        if !PathBuf::from(&assign.cwd).is_dir() {
            return Err("cwd_unreachable");
        }
        if !assign.provider_constraint.is_empty() && assign.provider_constraint != "*" {
            let req = semver::VersionReq::parse(&assign.provider_constraint)
                .map_err(|_| "version_mismatch")?;
            let ver = semver::Version::parse(&provider.version).map_err(|_| "version_mismatch")?;
            if !req.matches(&ver) {
                return Err("version_mismatch");
            }
        }
        Ok(provider)
    }

    pub async fn handle_assign(
        &self,
        assign: AssignTask,
        outbound: tokio::sync::mpsc::Sender<WorkerMessage>,
    ) {
        let provider = match self.validate(&assign) {
            Ok(p) => p.clone(),
            Err(reason) => {
                let _ = outbound
                    .send(WorkerMessage {
                        kind: Some(worker_message::Kind::TaskRejected(TaskRejected {
                            agent_id: assign.agent_id.clone(),
                            reason: reason.to_string(),
                        })),
                    })
                    .await;
                return;
            }
        };

        // Spawn the provider command.
        let mut cmd = Command::new(&provider.binary);
        for arg in provider.args_template.iter() {
            cmd.arg(arg.replace("{task}", &assign.task));
        }
        for (k, v) in provider.env.iter() {
            cmd.env(k, v);
        }
        cmd.current_dir(&assign.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                let _ = outbound
                    .send(WorkerMessage {
                        kind: Some(worker_message::Kind::TaskRejected(TaskRejected {
                            agent_id: assign.agent_id.clone(),
                            reason: format!("spawn_failed: {}", e),
                        })),
                    })
                    .await;
                return;
            }
        };

        let pid = child.id().unwrap_or(0);
        self.in_flight.fetch_add(1, Ordering::SeqCst);

        let _ = outbound
            .send(WorkerMessage {
                kind: Some(worker_message::Kind::TaskAccepted(TaskAccepted {
                    agent_id: assign.agent_id.clone(),
                    pid,
                })),
            })
            .await;

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let agent_id = assign.agent_id.clone();
        let dispatcher = self.clone();

        // Track running so cancel/drain can find it.
        let child_slot = Arc::new(Mutex::new(Some(child)));
        {
            let mut running = self.running.lock().await;
            running.insert(agent_id.clone(), child_slot.clone());
        }

        tokio::spawn(async move {
            // Stream stdout
            if let Some(stdout) = stdout {
                let outbound = outbound.clone();
                let agent_id = agent_id.clone();
                tokio::spawn(async move {
                    let mut lines = BufReader::new(stdout).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let _ = outbound
                            .send(WorkerMessage {
                                kind: Some(worker_message::Kind::TaskEvent(TaskEvent {
                                    agent_id: agent_id.clone(),
                                    kind: EventKind::Stdout as i32,
                                    payload: line,
                                })),
                            })
                            .await;
                    }
                });
            }
            if let Some(stderr) = stderr {
                let outbound = outbound.clone();
                let agent_id = agent_id.clone();
                tokio::spawn(async move {
                    let mut lines = BufReader::new(stderr).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let _ = outbound
                            .send(WorkerMessage {
                                kind: Some(worker_message::Kind::TaskEvent(TaskEvent {
                                    agent_id: agent_id.clone(),
                                    kind: EventKind::Stderr as i32,
                                    payload: line,
                                })),
                            })
                            .await;
                    }
                });
            }

            // Wait for exit
            let exit_status = {
                let mut guard = child_slot.lock().await;
                if let Some(child) = guard.as_mut() {
                    child.wait().await
                } else {
                    return;
                }
            };

            let (state, exit_code) = match exit_status {
                Ok(status) => {
                    let code = status.code();
                    let st = if status.success() {
                        crate::shared::worker_proto::TaskState::Complete
                    } else {
                        crate::shared::worker_proto::TaskState::Failed
                    };
                    (st as i32, code)
                }
                Err(e) => {
                    error!(error = %e, "wait failed");
                    (crate::shared::worker_proto::TaskState::Failed as i32, None)
                }
            };

            // Allow stdout/stderr drainers to flush.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;

            let _ = outbound
                .send(WorkerMessage {
                    kind: Some(worker_message::Kind::TaskFinished(TaskFinished {
                        agent_id: agent_id.clone(),
                        state,
                        exit_code,
                        session_id: None,
                        error_reason: None,
                    })),
                })
                .await;

            dispatcher.in_flight.fetch_sub(1, Ordering::SeqCst);
            let mut running = dispatcher.running.lock().await;
            running.remove(&agent_id);
        });

        debug!(agent_id = %assign.agent_id, "task accepted");
    }

    pub async fn cancel(&self, agent_id: &str) {
        let running = self.running.lock().await;
        if let Some(slot) = running.get(agent_id) {
            let mut guard = slot.lock().await;
            if let Some(child) = guard.as_mut() {
                let _ = child.start_kill();
            }
        }
    }

    pub fn drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }
}
