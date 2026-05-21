use anyhow::Result;
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

use crate::daemon::process_manager::SpawnedAgent;
use crate::daemon::provider::{AgentContext, OutputFormat, Provider, ProviderCapabilities};

pub struct ClaudeProvider {
    pub binary: String,
}

/// Scoped tool-allow rules so a spawned/woken agent can call back into Grimoire
/// (notify the operator, message peers, read/write shared memory) without a
/// permission prompt it can't answer in headless `--print` mode. Deliberately
/// narrow: it grants the three coordination verbs and NOT arbitrary shell or
/// destructive grim verbs (`banish`, `summon`, `daemon`). Comma-separated per
/// `claude --allowedTools` syntax (`Bash(cmd *)`).
const GRIM_CALLBACK_TOOLS: &str = "Bash(grim notify *),Bash(grim mail *),Bash(grim memory *)";

impl ClaudeProvider {
    pub const fn new(binary: String) -> Self {
        Self { binary }
    }
}

impl Provider for ClaudeProvider {
    fn name(&self) -> &'static str {
        "claude"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_resume: true,
            supports_model_selection: true,
            output_format: OutputFormat::StreamJson,
        }
    }

    fn spawn(
        &self,
        task: &str,
        cwd: &Path,
        model: Option<&str>,
        ctx: &AgentContext,
    ) -> Result<SpawnedAgent> {
        let mut cmd = Command::new(&self.binary);
        cmd.arg("--print")
            .arg("--output-format")
            .arg("stream-json")
            .arg("--verbose")
            .arg("--allowedTools")
            .arg(GRIM_CALLBACK_TOOLS)
            .arg("-p")
            .arg(task)
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(m) = model {
            cmd.arg("--model").arg(m);
        }

        ctx.apply_env(&mut cmd);

        let child = cmd.spawn()?;
        let pid = child.id().unwrap_or(0);
        Ok(SpawnedAgent { child, pid })
    }

    fn spawn_resume(
        &self,
        session_id: &str,
        message: &str,
        cwd: &Path,
        ctx: &AgentContext,
    ) -> Result<SpawnedAgent> {
        let mut cmd = Command::new(&self.binary);
        cmd.arg("--print")
            .arg("--output-format")
            .arg("stream-json")
            .arg("--verbose")
            .arg("--allowedTools")
            .arg(GRIM_CALLBACK_TOOLS)
            .arg("--resume")
            .arg(session_id)
            .arg("-p")
            .arg(message)
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        ctx.apply_env(&mut cmd);

        let child = cmd.spawn()?;
        let pid = child.id().unwrap_or(0);
        Ok(SpawnedAgent { child, pid })
    }

    fn extract_session_id(&self, line: &str) -> Option<String> {
        let v: serde_json::Value = serde_json::from_str(line).ok()?;
        if v.get("type")?.as_str()? == "system" {
            v.get("session_id")?
                .as_str()
                .map(std::string::ToString::to_string)
        } else {
            None
        }
    }

    fn extract_result(&self, stdout_lines: &[String]) -> Option<String> {
        for line in stdout_lines.iter().rev() {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(line)
                && v.get("type").and_then(|t| t.as_str()) == Some("result")
                && let Some(result) = v.get("result").and_then(|r| r.as_str())
            {
                return Some(result.to_string());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claude() -> ClaudeProvider {
        ClaudeProvider::new("claude".to_string())
    }

    #[test]
    fn capabilities() {
        let p = claude();
        let caps = p.capabilities();
        assert!(caps.supports_resume);
        assert!(caps.supports_model_selection);
        assert_eq!(caps.output_format, OutputFormat::StreamJson);
    }

    #[test]
    fn extract_session_id_valid() {
        let p = claude();
        let line = r#"{"type":"system","session_id":"abc123","model":"sonnet"}"#;
        assert_eq!(p.extract_session_id(line), Some("abc123".to_string()));
    }

    #[test]
    fn extract_session_id_non_system() {
        let p = claude();
        let line = r#"{"type":"assistant","message":"hello"}"#;
        assert_eq!(p.extract_session_id(line), None);
    }

    #[test]
    fn extract_session_id_malformed() {
        let p = claude();
        assert_eq!(p.extract_session_id("not json"), None);
        assert_eq!(p.extract_session_id(""), None);
    }

    #[test]
    fn extract_result_valid() {
        let p = claude();
        let lines = vec![
            r#"{"type":"assistant","message":"thinking"}"#.to_string(),
            r#"{"type":"result","result":"the answer"}"#.to_string(),
        ];
        assert_eq!(p.extract_result(&lines), Some("the answer".to_string()));
    }

    #[test]
    fn extract_result_empty() {
        let p = claude();
        assert_eq!(p.extract_result(&[]), None);
    }

    #[test]
    fn extract_result_no_result_event() {
        let p = claude();
        let lines = vec![r#"{"type":"assistant","message":"hello"}"#.to_string()];
        assert_eq!(p.extract_result(&lines), None);
    }
}
