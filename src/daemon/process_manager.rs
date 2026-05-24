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
    /// Tokens consumed by this run, when the provider reports usage. `None`
    /// for providers that don't (the agent is then unbillable against
    /// `SandboxConfig.token_budget`).
    pub tokens_used: Option<u64>,
    /// Per-bucket breakdown used to attribute USD spend through
    /// `[providers.<name>.pricing]`. `None` for providers that only report
    /// a total — budgets then attribute the total at `input_per_mtok`.
    pub token_breakdown: Option<super::provider::TokenBreakdown>,
}

impl Default for MonitorResult {
    /// `Failed` with no exit code is the "no information" baseline used by
    /// every fabricated/error-path `MonitorResult` in this codebase — tests
    /// override one or two fields with `..Default::default()` so new
    /// fields don't ripple through every init site.
    fn default() -> Self {
        Self {
            state: AgentState::Failed,
            exit_code: None,
            session_id: None,
            error_reason: None,
            tokens_used: None,
            token_breakdown: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineSource {
    Stdout,
    Stderr,
}

impl LineSource {
    pub const fn as_str(self) -> &'static str {
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

/// Output of [`consume_lines`]: the resumable session id (if the provider
/// reports one) plus the agent's full stdout buffer, used post-exit to
/// extract token usage and the final result text.
pub struct ConsumedRun {
    pub session_id: CapturedSessionId,
    pub stdout_lines: Vec<String>,
}

/// Persist a single output line as an `AgentEvent` row. Shared between the
/// local consume_lines path and the future RemoteExecutor.
pub fn persist_event(db: &Database, agent_id: &str, source: LineSource, line: &str) -> Result<()> {
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
) -> ConsumedRun
where
    S: Stream<Item = LineEvent> + Unpin,
{
    let mut session_id: Option<String> = None;
    let mut stdout_lines: Vec<String> = Vec::new();
    while let Some(LineEvent { source, line }) = lines.next().await {
        if let Some(p) = &provider
            && let Some(sid) = p.extract_session_id(&line)
        {
            session_id = Some(sid);
        }
        if let Err(e) = persist_event(&db, &agent_id, source, &line) {
            error!(?source, error = %e, "failed to persist event");
        }
        publish_output(&event_bus, &agent_id, source, &line);
        if matches!(source, LineSource::Stdout) {
            stdout_lines.push(line);
        }
    }
    ConsumedRun {
        session_id,
        stdout_lines,
    }
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

    let merged =
        line_stream(stdout, LineSource::Stdout).merge(line_stream(stderr, LineSource::Stderr));

    let provider_for_usage = provider.clone();
    let consume = tokio::spawn(consume_lines(
        agent_id.clone(),
        merged,
        event_bus,
        db,
        provider,
    ));

    let exit_status = child.wait().await;
    let consumed = consume.await.unwrap_or(ConsumedRun {
        session_id: None,
        stdout_lines: Vec::new(),
    });
    let captured_session_id = consumed.session_id;
    let token_breakdown = provider_for_usage
        .as_ref()
        .and_then(|p| p.extract_token_breakdown(&consumed.stdout_lines));
    let tokens_used = token_breakdown.map(|b| b.total()).filter(|t| *t > 0);

    match exit_status {
        Ok(status) => {
            let code = status.code();
            let state = if status.success() {
                info!(agent_id = %agent_id, "Agent completed successfully");
                AgentState::Complete
            } else {
                warn!(agent_id = %agent_id, code = ?code, "Agent failed");
                AgentState::Failed
            };
            MonitorResult {
                state,
                exit_code: code,
                session_id: captured_session_id,
                tokens_used,
                token_breakdown,
                ..Default::default()
            }
        }
        Err(e) => {
            error!(agent_id = %agent_id, error = %e, "Failed to wait on agent process");
            MonitorResult {
                session_id: captured_session_id,
                error_reason: Some(format!("wait_failed: {e}")),
                tokens_used,
                token_breakdown,
                ..Default::default()
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
