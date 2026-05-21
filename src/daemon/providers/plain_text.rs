use anyhow::{Result, anyhow};
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

use crate::daemon::process_manager::SpawnedAgent;
use crate::daemon::provider::{OutputFormat, Provider, ProviderCapabilities};
use crate::shared::config::ProviderConfig;

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

    fn spawn(&self, task: &str, cwd: &Path, _model: Option<&str>) -> Result<SpawnedAgent> {
        let mut cmd = Command::new(&self.config.binary);

        for arg in self.build_args(task) {
            cmd.arg(arg);
        }

        for (k, v) in &self.config.env {
            cmd.env(k, v);
        }

        cmd.current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let child = cmd.spawn()?;
        let pid = child.id().unwrap_or(0);
        Ok(SpawnedAgent { child, pid })
    }

    fn spawn_resume(&self, _session_id: &str, _message: &str, _cwd: &Path) -> Result<SpawnedAgent> {
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
    fn resume_not_supported() {
        let p = test_provider();
        assert!(p.spawn_resume("sid", "msg", Path::new("/tmp")).is_err());
    }
}
