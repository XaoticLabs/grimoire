use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_stream::stream;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;
use tokio_stream::{Stream, StreamExt};
use tracing::{error, info, warn};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{AgentEvent, AgentId, AgentState};

use super::event_bus::EventBus;
use super::persistence::Database;
use super::provider::Provider;

pub struct SpawnedAgent {
    pub child: Child,
    pub pid: u32,
}

pub struct MonitorResult {
    pub state: AgentState,
    pub exit_code: Option<i32>,
    pub session_id: Option<String>,
    pub error_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineSource {
    Stdout,
    Stderr,
}

impl LineSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LineEvent {
    pub source: LineSource,
    pub line: String,
}

pub type CapturedSessionId = Option<String>;

/// Persist a single output line as an `AgentEvent` row. Shared between the
/// local consume_lines path and the future RemoteExecutor.
pub fn persist_event(
    db: &Database,
    agent_id: &str,
    source: LineSource,
    line: &str,
) -> Result<()> {
    let event = AgentEvent {
        id: None,
        agent_id: agent_id.to_string(),
        event_type: source.as_str().to_string(),
        payload: line.to_string(),
        created_at: chrono::Utc::now(),
    };
    db.insert_event(&event).map(|_| ())
}

/// Publish a single output line as a `StreamEvent::Output`. Shared between the
/// local consume_lines path and the future RemoteExecutor.
pub fn publish_output(event_bus: &EventBus, agent_id: &str, source: LineSource, line: &str) {
    event_bus.publish(StreamEvent::Output {
        agent_id: agent_id.to_string(),
        stream: source.as_str().to_string(),
        line: line.to_string(),
    });
}

/// Drain a stream of `LineEvent`s, persisting each line and publishing it on
/// the bus. Optionally extracts the agent's session id via the provider.
pub async fn consume_lines<S>(
    agent_id: AgentId,
    mut lines: S,
    event_bus: EventBus,
    db: Arc<Database>,
    provider: Option<Arc<dyn Provider>>,
) -> CapturedSessionId
where
    S: Stream<Item = LineEvent> + Unpin,
{
    let mut session_id: Option<String> = None;
    while let Some(LineEvent { source, line }) = lines.next().await {
        if let Some(p) = &provider {
            if let Some(sid) = p.extract_session_id(&line) {
                session_id = Some(sid);
            }
        }
        if let Err(e) = persist_event(&db, &agent_id, source, &line) {
            error!(?source, error = %e, "failed to persist event");
        }
        publish_output(&event_bus, &agent_id, source, &line);
    }
    session_id
}

fn line_stream<R>(
    reader: Option<R>,
    source: LineSource,
) -> Pin<Box<dyn Stream<Item = LineEvent> + Send>>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    match reader {
        None => Box::pin(tokio_stream::empty()),
        Some(reader) => Box::pin(stream! {
            let buf = BufReader::new(reader);
            let mut lines = buf.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                yield LineEvent { source, line };
            }
        }),
    }
}

/// Monitor a running agent's stdout/stderr, emitting events and persisting them.
pub async fn monitor_agent(
    agent_id: AgentId,
    mut child: Child,
    event_bus: EventBus,
    db: Arc<Database>,
    provider: Option<Arc<dyn Provider>>,
) -> MonitorResult {
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let merged = line_stream(stdout, LineSource::Stdout)
        .merge(line_stream(stderr, LineSource::Stderr));

    let consume = tokio::spawn(consume_lines(
        agent_id.clone(),
        merged,
        event_bus,
        db,
        provider,
    ));

    let exit_status = child.wait().await;
    let captured_session_id = consume.await.unwrap_or(None);

    match exit_status {
        Ok(status) => {
            let code = status.code();
            if status.success() {
                info!(agent_id = %agent_id, "Agent completed successfully");
                MonitorResult {
                    state: AgentState::Complete,
                    exit_code: code,
                    session_id: captured_session_id,
                    error_reason: None,
                }
            } else {
                warn!(agent_id = %agent_id, code = ?code, "Agent failed");
                MonitorResult {
                    state: AgentState::Failed,
                    exit_code: code,
                    session_id: captured_session_id,
                    error_reason: None,
                }
            }
        }
        Err(e) => {
            error!(agent_id = %agent_id, error = %e, "Failed to wait on agent process");
            MonitorResult {
                state: AgentState::Failed,
                exit_code: None,
                session_id: captured_session_id,
                error_reason: Some(format!("wait_failed: {}", e)),
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
