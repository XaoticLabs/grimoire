//! Tier-1 adapter for Earendil Works' `pi` coding agent CLI.
//!
//! pi has a native session model with a JSONL session tree
//! (`~/.pi/agent/sessions/`) and resume by id/path, so this is a `Native`
//! resume adapter. We deliberately use pi's **print/JSON one-shot mode**, not
//! its persistent `--mode rpc` server: dormancy in Grimoire means the process
//! exits, and a resident RPC subprocess per dormant agent would break the
//! "sleeps when idle / scales to many" property (see the spec's
//! "Considered & Deferred" section).
//!
//! Verified live against pi 0.75.4 (2026-05-22): `pi -p --mode json` runs a
//! headless turn and exits cleanly after `agent_end`. The first stdout event is
//! `{"type":"session","id":"<uuid>","cwd":...}` — that `id` is the resumable
//! session id (it also names the persisted file under
//! `~/.pi/agent/sessions/<cwd-scope>/`). Resume is `pi --session <id> -p`.
//! pi enables its read/bash/edit/write tools by default, so an agent can shell
//! out to `grim notify/mail/memory` without an allowlist (unlike Claude's
//! headless mode).

use anyhow::Result;
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

use crate::daemon::process_manager::SpawnedAgent;
use crate::daemon::provider::{AgentContext, OutputFormat, Provider, ProviderCapabilities};

pub struct PiProvider {
    pub binary: String,
}

impl PiProvider {
    pub const fn new(binary: String) -> Self {
        Self { binary }
    }
}

impl Provider for PiProvider {
    fn name(&self) -> &'static str {
        "pi"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_resume: true,
            supports_model_selection: true,
            // pi's JSON mode is "print mode with structured events".
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
        // Headless single-shot (`-p`) with structured JSON events (`--mode json`).
        // The session is persisted by default (no `--no-session`) so the agent can
        // be resumed on the next wake.
        cmd.arg("-p").arg("--mode").arg("json");

        if let Some(m) = model {
            cmd.arg("--model").arg(m);
        }

        cmd.arg(task)
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

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
        // `pi --session <id>` resumes a specific stored session (partial ids are
        // accepted per pi's docs); `-p --mode json` keeps it headless.
        cmd.arg("--session")
            .arg(session_id)
            .arg("-p")
            .arg("--mode")
            .arg("json")
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
        // pi emits `{"type":"session","id":"<uuid>",...}` as the first event of a
        // headless run (verified against pi 0.75.4). That `id` is the resume key.
        let v: serde_json::Value = serde_json::from_str(line).ok()?;
        if v.get("type").and_then(serde_json::Value::as_str) == Some("session") {
            return v
                .get("id")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string);
        }
        None
    }

    fn extract_result(&self, stdout_lines: &[String]) -> Option<String> {
        // pi emits JSON events; the final assistant text lives in the last
        // `message_end`/`turn_end` event with `message.role == "assistant"`.
        // Pull that out so pact `{output}` injection gets clean prose, not JSON.
        for line in stdout_lines.iter().rev() {
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            let ty = v.get("type").and_then(serde_json::Value::as_str);
            if !matches!(ty, Some("message_end" | "turn_end")) {
                continue;
            }
            let msg = v.get("message")?;
            if msg.get("role").and_then(serde_json::Value::as_str) != Some("assistant") {
                continue;
            }
            let text: String = msg
                .get("content")?
                .as_array()?
                .iter()
                .filter(|p| p.get("type").and_then(serde_json::Value::as_str) == Some("text"))
                .filter_map(|p| p.get("text").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .join("");
            if !text.is_empty() {
                return Some(text);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_are_native_resume() {
        let p = PiProvider::new("pi".into());
        assert!(p.capabilities().supports_resume);
        assert_eq!(
            p.resume_strategy(),
            crate::daemon::provider::ResumeStrategy::Native
        );
    }

    #[test]
    fn extracts_assistant_text_from_message_end() {
        let p = PiProvider::new("pi".into());
        let lines = vec![
            r#"{"type":"session","id":"x"}"#.to_string(),
            r#"{"type":"message_start","message":{"role":"assistant","content":[]}}"#.to_string(),
            r#"{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"hello world"}]}}"#.to_string(),
        ];
        assert_eq!(p.extract_result(&lines).as_deref(), Some("hello world"));
    }

    #[test]
    fn extract_result_none_without_assistant_message() {
        let p = PiProvider::new("pi".into());
        let lines = vec![r#"{"type":"agent_start"}"#.to_string()];
        assert_eq!(p.extract_result(&lines), None);
    }

    #[test]
    fn extracts_session_id_from_real_session_event() {
        let p = PiProvider::new("pi".into());
        // Real first stdout line from `pi -p --mode json` (pi 0.75.4).
        let line = r#"{"type":"session","version":3,"id":"019e50f1-9a84-74b3-843e-8b435cbd9059","timestamp":"2026-05-22T18:27:51.556Z","cwd":"/tmp/x"}"#;
        assert_eq!(
            p.extract_session_id(line).as_deref(),
            Some("019e50f1-9a84-74b3-843e-8b435cbd9059")
        );
        // Other event types carry no session id.
        assert_eq!(p.extract_session_id(r#"{"type":"agent_start"}"#), None);
    }
}
