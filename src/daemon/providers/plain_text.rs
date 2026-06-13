use anyhow::{Result, anyhow};
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

use crate::daemon::process_manager::SpawnedAgent;
use crate::daemon::provider::{AgentContext, OutputFormat, Provider, ProviderCapabilities};
use crate::shared::config::ProviderConfig;

/// Byte cap on AGENTS.md content prepended to a generic provider's prompt
/// (native-session CLIs read instruction files themselves; generic ones don't).
const AGENTS_MD_BUDGET_BYTES: usize = 8 * 1024;

pub struct PlainTextProvider {
    pub provider_name: String,
    pub config: ProviderConfig,
}

impl PlainTextProvider {
    pub const fn new(name: String, config: ProviderConfig) -> Self {
        Self {
            provider_name: name,
            config,
        }
    }

    fn build_args(&self, task: &str) -> Vec<String> {
        self.config
            .args_template
            .iter()
            .map(|arg| arg.replace("{task}", task))
            .collect()
    }

    /// Prepend the cwd's `AGENTS.md` (tail-truncated to
    /// [`AGENTS_MD_BUDGET_BYTES`]) so generic CLIs see project instructions.
    fn compose_task(cwd: &Path, task: &str) -> String {
        let path = cwd.join("AGENTS.md");
        let Ok(content) = std::fs::read_to_string(&path) else {
            return task.to_string();
        };
        let content = content.trim();
        if content.is_empty() {
            return task.to_string();
        }
        let (shown, note) = if content.len() > AGENTS_MD_BUDGET_BYTES {
            let mut cut = AGENTS_MD_BUDGET_BYTES;
            while !content.is_char_boundary(cut) {
                cut -= 1;
            }
            (&content[..cut], "\n[truncated]")
        } else {
            (content, "")
        };
        format!("## Project instructions (AGENTS.md)\n{shown}{note}\n\n## Task\n{task}")
    }
}

impl Provider for PlainTextProvider {
    fn name(&self) -> &str {
        &self.provider_name
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_resume: false,
            supports_model_selection: false,
            output_format: OutputFormat::PlainText,
        }
    }

    fn spawn(
        &self,
        task: &str,
        cwd: &Path,
        _model: Option<&str>,
        ctx: &AgentContext,
    ) -> Result<SpawnedAgent> {
        let mut cmd = Command::new(&self.config.binary);

        let task = Self::compose_task(cwd, task);
        for arg in self.build_args(&task) {
            cmd.arg(arg);
        }

        for (k, v) in &self.config.env {
            cmd.env(k, v);
        }

        ctx.apply_env(&mut cmd);

        cmd.current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut cmd = crate::daemon::sandbox::apply(cmd, ctx.sandbox.as_ref());
        let child = cmd.spawn()?;
        let pid = child.id().unwrap_or(0);
        Ok(SpawnedAgent { child, pid })
    }

    fn spawn_resume(
        &self,
        _session_id: &str,
        _message: &str,
        _cwd: &Path,
        _ctx: &AgentContext,
    ) -> Result<SpawnedAgent> {
        Err(anyhow!(
            "Provider '{}' does not support session resume",
            self.provider_name
        ))
    }

    fn extract_session_id(&self, _line: &str) -> Option<String> {
        None
    }

    fn extract_result(&self, stdout_lines: &[String]) -> Option<String> {
        if stdout_lines.is_empty() {
            return None;
        }
        let skip = stdout_lines.len().saturating_sub(50);
        Some(stdout_lines[skip..].join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_provider() -> PlainTextProvider {
        PlainTextProvider::new(
            "test".to_string(),
            ProviderConfig {
                binary: "echo".to_string(),
                args_template: vec!["--msg".to_string(), "{task}".to_string()],
                env: HashMap::new(),
                sandbox: None,
                pricing: None,
            },
        )
    }

    #[test]
    fn capabilities() {
        let p = test_provider();
        let caps = p.capabilities();
        assert!(!caps.supports_resume);
        assert!(!caps.supports_model_selection);
        assert_eq!(caps.output_format, OutputFormat::PlainText);
    }

    #[test]
    fn build_args_substitution() {
        let p = test_provider();
        let args = p.build_args("hello world");
        assert_eq!(args, vec!["--msg", "hello world"]);
    }

    #[test]
    fn extract_result_returns_last_lines() {
        let p = test_provider();
        let lines: Vec<String> = (0..3).map(|i| format!("line {i}")).collect();
        let result = p.extract_result(&lines).unwrap();
        assert_eq!(result, "line 0\nline 1\nline 2");
    }

    #[test]
    fn extract_result_empty() {
        let p = test_provider();
        assert!(p.extract_result(&[]).is_none());
    }

    #[test]
    fn extract_session_id_always_none() {
        let p = test_provider();
        assert!(p.extract_session_id("anything").is_none());
    }

    #[test]
    fn compose_task_without_agents_md_is_identity() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(PlainTextProvider::compose_task(dir.path(), "do x"), "do x");
    }

    #[test]
    fn compose_task_prepends_agents_md() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "Use tabs.\n").unwrap();
        let composed = PlainTextProvider::compose_task(dir.path(), "do x");
        assert!(composed.starts_with("## Project instructions (AGENTS.md)\nUse tabs."));
        assert!(composed.ends_with("## Task\ndo x"));
    }

    #[test]
    fn compose_task_truncates_oversized_agents_md() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "x".repeat(20_000)).unwrap();
        let composed = PlainTextProvider::compose_task(dir.path(), "do x");
        assert!(composed.contains("[truncated]"));
        assert!(composed.len() < 10_000);
    }

    #[test]
    fn compose_task_ignores_empty_agents_md() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("AGENTS.md"), "  \n").unwrap();
        assert_eq!(PlainTextProvider::compose_task(dir.path(), "do x"), "do x");
    }

    #[test]
    fn resume_not_supported() {
        let p = test_provider();
        assert!(
            p.spawn_resume("sid", "msg", Path::new("/tmp"), &AgentContext::default())
                .is_err()
        );
    }
}
