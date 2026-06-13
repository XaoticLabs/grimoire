use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::process::Command;

use super::process_manager::SpawnedAgent;
use crate::shared::config::SandboxConfig;

/// Identity injected as provider-neutral `GRIMOIRE_*` env vars so a spawned
/// agent can call back into `grim` (mail, memory, notify) knowing its own id,
/// uniformly across claude / pi / opencode / aider.
#[derive(Debug, Clone, Default)]
pub struct AgentContext {
    pub agent_id: String,
    /// Per-spawn confinement. Providers must call
    /// [`crate::daemon::sandbox::apply_resource_limits`] on the built `Command`.
    pub sandbox: Option<SandboxConfig>,
}

impl AgentContext {
    /// Set the `GRIMOIRE_*` identity env vars on a command about to spawn.
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

/// How a dormant agent's continuity is restored on wake.
///
/// - `Native`: the CLI owns the session; resumed by id via
///   [`Provider::spawn_resume`] (Claude `--resume`, pi `--session`).
/// - `ContextReplay`: stateless CLI; the daemon rebuilds a context preamble
///   from the event log and prepends it to a fresh [`Provider::spawn`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeStrategy {
    Native,
    ContextReplay,
}

pub trait Provider: Send + Sync {
    /// Unique identifier (e.g. "claude", "pi", "aider")
    fn name(&self) -> &str;

    fn capabilities(&self) -> ProviderCapabilities;

    /// Wake strategy; defaults from `supports_resume`.
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

    /// Per-bucket token counts for USD attribution and budget checks. `None`
    /// (default) when the provider has no usage telemetry — such runs are
    /// unbillable and unbudgetable by design.
    fn extract_token_breakdown(&self, _stdout_lines: &[String]) -> Option<TokenBreakdown> {
        None
    }
}

/// Per-bucket token usage for one agent run. Fields match the pricing axes
/// vendors publish; missing buckets stay at zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenBreakdown {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_creation: u64,
}

impl TokenBreakdown {
    pub const fn total(&self) -> u64 {
        self.input + self.output + self.cache_read + self.cache_creation
    }

    /// Apply `pricing` (USD per 1 000 000 tokens) and return the spend in USD.
    pub fn cost_usd(&self, pricing: &crate::shared::config::ProviderPricing) -> f64 {
        let m = 1_000_000.0_f64;
        (self.input as f64) * pricing.input_per_mtok / m
            + (self.output as f64) * pricing.output_per_mtok / m
            + (self.cache_read as f64) * pricing.cache_read() / m
            + (self.cache_creation as f64) * pricing.cache_creation() / m
    }
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
