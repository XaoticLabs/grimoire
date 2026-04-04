use anyhow::Result;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tracing::{error, info, warn};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{AgentEvent, AgentId, AgentState};

use super::event_bus::EventBus;

pub struct SpawnedAgent {
    pub child: Child,
    pub pid: u32,
}

/// Spawn a Claude Code process with structured JSON output
pub fn spawn_claude(task: &str, cwd: &Path, model: Option<&str>) -> Result<SpawnedAgent> {
    let mut cmd = Command::new("claude");
    cmd.arg("--print")
        .arg("--output-format")
        .arg("stream-json")
        .arg("--verbose")
        .arg("-p")
        .arg(task)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(m) = model {
        cmd.arg("--model").arg(m);
    }

    let child = cmd.spawn()?;
    let pid = child.id().unwrap_or(0);

    Ok(SpawnedAgent { child, pid })
}

/// Monitor a running agent's stdout/stderr, emitting events and persisting them.
/// Returns the exit code when the process completes.
pub async fn monitor_agent(
    agent_id: AgentId,
    mut child: Child,
    event_bus: EventBus,
    db: std::sync::Arc<crate::daemon::persistence::Database>,
) -> (AgentState, Option<i32>) {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let bus_stdout = event_bus.clone();
    let db_stdout = db.clone();
    let id_stdout = agent_id.clone();

    let stdout_handle = tokio::spawn(async move {
        if let Some(stdout) = stdout {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                // Persist event
                let event = AgentEvent {
                    id: None,
                    agent_id: id_stdout.clone(),
                    event_type: "stdout".to_string(),
                    payload: line.clone(),
                    created_at: chrono::Utc::now(),
                };
                if let Err(e) = db_stdout.insert_event(&event) {
                    error!("Failed to persist event: {}", e);
                }

                // Broadcast
                bus_stdout.publish(StreamEvent::Output {
                    agent_id: id_stdout.clone(),
                    stream: "stdout".to_string(),
                    line,
                });
            }
        }
    });

    let bus_stderr = event_bus.clone();
    let db_stderr = db.clone();
    let id_stderr = agent_id.clone();

    let stderr_handle = tokio::spawn(async move {
        if let Some(stderr) = stderr {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let event = AgentEvent {
                    id: None,
                    agent_id: id_stderr.clone(),
                    event_type: "stderr".to_string(),
                    payload: line.clone(),
                    created_at: chrono::Utc::now(),
                };
                if let Err(e) = db_stderr.insert_event(&event) {
                    error!("Failed to persist stderr event: {}", e);
                }

                bus_stderr.publish(StreamEvent::Output {
                    agent_id: id_stderr.clone(),
                    stream: "stderr".to_string(),
                    line,
                });
            }
        }
    });

    // Wait for process to exit
    let exit_status = child.wait().await;

    // Wait for output readers to finish
    let _ = stdout_handle.await;
    let _ = stderr_handle.await;

    match exit_status {
        Ok(status) => {
            let code = status.code();
            if status.success() {
                info!(agent_id = %agent_id, "Agent completed successfully");
                (AgentState::Complete, code)
            } else {
                warn!(agent_id = %agent_id, code = ?code, "Agent failed");
                (AgentState::Failed, code)
            }
        }
        Err(e) => {
            error!(agent_id = %agent_id, error = %e, "Failed to wait on agent process");
            (AgentState::Failed, None)
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
