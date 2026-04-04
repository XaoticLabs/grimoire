use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::Child;
use tracing::{error, info, warn};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{AgentEvent, AgentId, AgentState};

use super::event_bus::EventBus;
use super::provider::Provider;

pub struct SpawnedAgent {
    pub child: Child,
    pub pid: u32,
}

pub struct MonitorResult {
    pub state: AgentState,
    pub exit_code: Option<i32>,
    pub session_id: Option<String>,
}

async fn monitor_stream(
    stream: Option<impl AsyncRead + Unpin>,
    stream_name: &'static str,
    agent_id: AgentId,
    event_bus: EventBus,
    db: Arc<crate::daemon::persistence::Database>,
    session_extractor: Option<(Arc<dyn Provider>, Arc<tokio::sync::Mutex<Option<String>>>)>,
) {
    let Some(stream) = stream else { return };
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some((ref provider, ref session_id)) = session_extractor {
            if let Some(sid) = provider.extract_session_id(&line) {
                *session_id.lock().await = Some(sid);
            }
        }

        let event = AgentEvent {
            id: None,
            agent_id: agent_id.clone(),
            event_type: stream_name.to_string(),
            payload: line.clone(),
            created_at: chrono::Utc::now(),
        };
        if let Err(e) = db.insert_event(&event) {
            error!("Failed to persist {} event: {}", stream_name, e);
        }

        event_bus.publish(StreamEvent::Output {
            agent_id: agent_id.clone(),
            stream: stream_name.to_string(),
            line,
        });
    }
}

/// Monitor a running agent's stdout/stderr, emitting events and persisting them.
pub async fn monitor_agent(
    agent_id: AgentId,
    mut child: Child,
    event_bus: EventBus,
    db: Arc<crate::daemon::persistence::Database>,
    provider: Arc<dyn Provider>,
) -> MonitorResult {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let session_id = Arc::new(tokio::sync::Mutex::new(None::<String>));

    let stdout_handle = tokio::spawn(monitor_stream(
        stdout,
        "stdout",
        agent_id.clone(),
        event_bus.clone(),
        db.clone(),
        Some((provider.clone(), session_id.clone())),
    ));

    let stderr_handle = tokio::spawn(monitor_stream(
        stderr,
        "stderr",
        agent_id.clone(),
        event_bus.clone(),
        db.clone(),
        None,
    ));

    // Wait for process to exit
    let exit_status = child.wait().await;

    // Wait for output readers to finish
    let _ = stdout_handle.await;
    let _ = stderr_handle.await;

    let captured_session_id = session_id.lock().await.clone();

    match exit_status {
        Ok(status) => {
            let code = status.code();
            if status.success() {
                info!(agent_id = %agent_id, "Agent completed successfully");
                MonitorResult {
                    state: AgentState::Complete,
                    exit_code: code,
                    session_id: captured_session_id,
                }
            } else {
                warn!(agent_id = %agent_id, code = ?code, "Agent failed");
                MonitorResult {
                    state: AgentState::Failed,
                    exit_code: code,
                    session_id: captured_session_id,
                }
            }
        }
        Err(e) => {
            error!(agent_id = %agent_id, error = %e, "Failed to wait on agent process");
            MonitorResult {
                state: AgentState::Failed,
                exit_code: None,
                session_id: captured_session_id,
            }
        }
    }
}

/// Kill an agent process by PID
pub fn kill_process(pid: u32) -> Result<()> {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let nix_pid = Pid::from_raw(pid as i32);
    kill(nix_pid, Signal::SIGTERM)?;
    Ok(())
}
