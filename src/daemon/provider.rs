use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::process::Command;

use super::process_manager::SpawnedAgent;
use crate::shared::config::SandboxConfig;

/// Identity injected into a spawned agent's environment so the agent can call
/// back into `grim` (mail, memory, notify) knowing who it is — without the
/// agent having to be told its own id. Provider-neutral: applied as
/// `GRIMOIRE_*` env vars on the child process regardless of which CLI runs,
/// so claude / `pi` / opencode / aider all see the same contract.
#[derive(Debug, Clone, Default)]
pub struct AgentContext {
    pub agent_id: String,
    /// Optional per-spawn confinement (cgroup limits, fs jail, budgets).
    /// Providers should call [`crate::daemon::sandbox::apply_resource_limits`]
    /// on the built `Command` so any resource caps are enforced.
    pub sandbox: Option<SandboxConfig>,
}

impl AgentContext {
    /// Set the `GRIMOIRE_*` identity env vars on a command about to be spawned.
    /// Future fields (workspace, session) extend here without touching the
    /// `Provider` trait signature.
    pub fn apply_env(&self, cmd: &mut Command) {
        cmd.env("GRIMOIRE_AGENT_ID", &self.agent_id);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputFormat {
    StreamJson,
    PlainText,
}

#[derive(Debug, Clone)]
pub struct ProviderCapabilities {
    pub supports_resume: bool,
    pub supports_model_selection: bool,
    pub output_format: OutputFormat,
}

/// How an agent's continuity is restored when a dormant agent is woken.
///
/// - `Native`: the underlying CLI owns the session; the daemon resumes it by id
///   via [`Provider::spawn_resume`] (Claude `--resume`, pi `--session`).
///   Full-fidelity — the CLI keeps the transcript and its own state.
/// - `ContextReplay`: the CLI is stateless; the daemon reconstructs a context
///   preamble from the durable event log and prepends it to a fresh
///   [`Provider::spawn`]. The universal fallback for generic config providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeStrategy {
    Native,
    ContextReplay,
}

pub trait Provider: Send + Sync {
    /// Unique identifier (e.g. "claude", "pi", "aider")
    fn name(&self) -> &str;

    fn capabilities(&self) -> ProviderCapabilities;

    /// Continuity strategy used to wake a dormant agent. Defaults from
    /// `supports_resume`; native-session adapters keep the default, generic
    /// config providers fall through to `ContextReplay`.
    fn resume_strategy(&self) -> ResumeStrategy {
        if self.capabilities().supports_resume {
            ResumeStrategy::Native
        } else {
            ResumeStrategy::ContextReplay
        }
    }

    /// Spawn a new agent process
    fn spawn(
        &self,
        task: &str,
        cwd: &Path,
        model: Option<&str>,
        ctx: &AgentContext,
    ) -> Result<SpawnedAgent>;

    /// Resume an existing session (returns error if unsupported)
    fn spawn_resume(
        &self,
        session_id: &str,
        message: &str,
        cwd: &Path,
        ctx: &AgentContext,
    ) -> Result<SpawnedAgent>;

    /// Extract session ID from a stdout line (called per-line during monitoring)
    fn extract_session_id(&self, line: &str) -> Option<String>;

    /// Extract the final result text from collected stdout lines
    fn extract_result(&self, stdout_lines: &[String]) -> Option<String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_env_sets_agent_id() {
        let ctx = AgentContext {
            agent_id: "abc12345".to_string(),
            ..AgentContext::default()
        };
        let mut cmd = Command::new("true");
        ctx.apply_env(&mut cmd);
        let found = cmd
            .as_std()
            .get_envs()
            .any(|(k, v)| k == "GRIMOIRE_AGENT_ID" && v == Some("abc12345".as_ref()));
        assert!(found, "GRIMOIRE_AGENT_ID should be set on the command");
    }
}
