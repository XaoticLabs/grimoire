use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

use super::process_manager::SpawnedAgent;

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

pub trait Provider: Send + Sync {
    /// Unique identifier (e.g. "claude", "codex", "aider")
    fn name(&self) -> &str;

    fn capabilities(&self) -> ProviderCapabilities;

    /// Spawn a new agent process
    fn spawn(&self, task: &str, cwd: &Path, model: Option<&str>) -> Result<SpawnedAgent>;

    /// Resume an existing session (returns error if unsupported)
    fn spawn_resume(
        &self,
        session_id: &str,
        message: &str,
        cwd: &Path,
    ) -> Result<SpawnedAgent>;

    /// Extract session ID from a stdout line (called per-line during monitoring)
    fn extract_session_id(&self, line: &str) -> Option<String>;

    /// Extract the final result text from collected stdout lines
    fn extract_result(&self, stdout_lines: &[String]) -> Option<String>;
}
