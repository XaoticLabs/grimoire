use std::fmt::Write as _;

use colored::Colorize;

use crate::shared::protocol::StreamEvent;

/// Truncate a string to `max` chars, appending `…` if it was cut. Used to keep
/// timeline detail lines (tasks, mail previews) to a single readable row.
fn truncate(s: &str, max: usize) -> String {
    let s = s.replace('\n', " ");
    if s.chars().count() <= max {
        s
    } else {
        let head: String = s.chars().take(max).collect();
        format!("{head}…")
    }
}

/// Format a non-`Output` lifecycle event as one compact, colored detail line
/// (no seq/timestamp columns — the caller prepends those). Returns `None` for
/// events that carry no agent-scoped signal worth a row (e.g. `MailDelivered`,
/// which is implied by the preceding `MailReceived`). The catch-all renders
/// the bare kind tag so a newly added variant is never silently invisible.
pub fn format_lifecycle(event: &StreamEvent) -> Option<String> {
    let line = match event {
        StreamEvent::StateChange {
            old_state,
            new_state,
            ..
        } => format!(
            "{} {} → {}",
            "→".yellow(),
            old_state.to_string().dimmed(),
            new_state.to_string().bold()
        ),
        StreamEvent::AgentCreated { agent } => {
            let provider = agent.provider.as_deref().unwrap_or("default");
            let task = agent.task.as_deref().unwrap_or("");
            format!(
                "{} created  provider={} task=\"{}\"",
                "✦".cyan(),
                provider.dimmed(),
                truncate(task, 60)
            )
        }
        StreamEvent::AgentQueued {
            lane, block_reason, ..
        } => {
            let mut s = format!("{} queued  lane={}", "⋯".dimmed(), lane.dimmed());
            if let Some(reason) = block_reason {
                let _ = write!(s, "  blocked: {}", reason.yellow());
            }
            s
        }
        StreamEvent::WakeSourceRegistered { kind, .. } => {
            format!("{} wake source +{}", "+".green(), kind.bold())
        }
        StreamEvent::WakeSourceFired { via, .. } => {
            let via = via
                .as_deref()
                .map(|v| format!(" via {v}"))
                .unwrap_or_default();
            format!("{} wake fired{}", "⏰".yellow(), via.dimmed())
        }
        StreamEvent::WakeSourceFailed { reason, .. } => {
            format!("{} wake failed: {}", "⏰".red(), reason.red())
        }
        StreamEvent::WakeSourceRetired { reason, .. } => {
            format!("{} wake retired: {}", "⏰".dimmed(), reason.dimmed())
        }
        StreamEvent::RestartScheduled {
            attempt,
            max,
            rate_limited,
            ..
        } => {
            let rl = if *rate_limited {
                " (rate-limited)".red().to_string()
            } else {
                String::new()
            };
            format!("{} restart scheduled {attempt}/{max}{rl}", "↻".yellow())
        }
        StreamEvent::Restarted { attempt, .. } => {
            format!("{} restarted (attempt {attempt})", "↻".yellow().bold())
        }
        StreamEvent::RestartBudgetExhausted { reason, .. } => {
            format!("{} restart budget exhausted: {}", "✗".red(), reason.red())
        }
        StreamEvent::Escalated {
            target,
            fanout_count,
            ..
        } => format!(
            "{} escalated → {} (fanout {fanout_count})",
            "⚠".red().bold(),
            target.bold()
        ),
        StreamEvent::MailSent {
            topic,
            recipient_id,
            ..
        } => {
            let dest = topic
                .as_deref()
                .map(|t| format!("topic://{t}"))
                .or_else(|| recipient_id.as_deref().map(|r| format!("agent://{r}")))
                .unwrap_or_else(|| "?".to_string());
            format!("{} mail → {}", "✉".blue(), dest.dimmed())
        }
        StreamEvent::MailReceived {
            sender_id,
            topic,
            body_preview,
            origin_daemon_id,
            ..
        } => {
            let from = topic
                .as_deref()
                .map(|t| format!("topic://{t}"))
                .or_else(|| sender_id.as_deref().map(|s| format!("agent://{s}")))
                .unwrap_or_else(|| "?".to_string());
            let peer = origin_daemon_id
                .as_deref()
                .map(|d| format!(" [peer {}]", &d[..8.min(d.len())]))
                .unwrap_or_default();
            format!(
                "{} mail ← {} \"{}\"{}",
                "✉".blue().bold(),
                from.dimmed(),
                truncate(body_preview, 50),
                peer.dimmed()
            )
        }
        StreamEvent::MailFailed { reason, .. } => {
            format!("{} mail failed: {}", "✉".red(), reason.red())
        }
        StreamEvent::MemoryWritten { key, version, .. } => {
            format!("{} mem put {} v{version}", "▪".magenta(), key.bold())
        }
        StreamEvent::MemoryDeleted { key, .. } => {
            format!("{} mem del {}", "▪".magenta(), key.bold())
        }
        StreamEvent::Notification {
            message,
            level,
            source,
            ..
        } => format!(
            "{} [{}] {} {}",
            "🔔".yellow(),
            level.bold(),
            truncate(message, 70),
            format!("({source})").dimmed()
        ),
        // MailDelivered is implied by MailReceived; suppress to cut noise.
        StreamEvent::MailDelivered { .. } => return None,
        // Everything else (scroll/workspace/peer events that don't ride an
        // agent stream, plus any future variant): show the bare kind tag so
        // it is visible but unobtrusive.
        other => other.kind().dimmed().to_string(),
    };
    Some(line)
}

/// Parse and format a Claude Code stream-json line into human-readable output.
/// Returns None if the event should be suppressed (e.g. rate_limit_event).
pub fn format_stream_json(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;

    let event_type = v.get("type")?.as_str()?;

    // `rate_limit_event` is split from the catch-all `_` so the explicit
    // suppression of that known event type stays self-documenting.
    #[allow(clippy::match_same_arms)]
    match event_type {
        "system" => Some(format_system(&v)),
        "assistant" => format_assistant(&v),
        "tool_use" => Some(format_tool_use(&v)),
        "tool_result" => Some(format_tool_result(&v)),
        "result" => Some(format_result(&v)),
        "rate_limit_event" => None, // suppress
        _ => None,
    }
}

fn format_system(v: &serde_json::Value) -> String {
    let model = v.get("model").and_then(|m| m.as_str()).unwrap_or("unknown");
    let session_id = v.get("session_id").and_then(|s| s.as_str()).unwrap_or("?");
    let short_session = &session_id[..8.min(session_id.len())];

    format!(
        "{} {} session {}",
        "◆".cyan(),
        model.bold(),
        short_session.dimmed()
    )
}

fn format_assistant(v: &serde_json::Value) -> Option<String> {
    let message = v.get("message")?;
    let content = message.get("content")?.as_array()?;

    let mut output = String::new();
    for block in content {
        match block.get("type").and_then(|t| t.as_str()) {
            Some("text") => {
                if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        output.push_str(trimmed);
                    }
                }
            }
            Some("tool_use") => {
                let name = block
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown");
                let input = block
                    .get("input")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                output.push_str(&format_tool_call(name, &input));
            }
            _ => {}
        }
    }

    if output.is_empty() {
        None
    } else {
        Some(output)
    }
}

fn format_tool_call(name: &str, input: &serde_json::Value) -> String {
    let mut out = format!("\n{} {}", "▸".yellow(), name.yellow().bold());

    match name {
        "Bash" => {
            if let Some(cmd) = input.get("command").and_then(|c| c.as_str()) {
                let _ = write!(out, "\n  {}", cmd.dimmed());
            }
        }
        "Read" | "Write" | "Edit" => {
            if let Some(path) = input.get("file_path").and_then(|p| p.as_str()) {
                let _ = write!(out, " {}", path.dimmed());
            }
        }
        "Glob" | "Grep" => {
            if let Some(pattern) = input.get("pattern").and_then(|p| p.as_str()) {
                let _ = write!(out, " {}", pattern.dimmed());
            }
        }
        _ => {
            // For unknown tools, show a compact summary of the input
            if let Some(obj) = input.as_object() {
                let keys: Vec<&str> = obj.keys().map(std::string::String::as_str).collect();
                if !keys.is_empty() {
                    let _ = write!(out, " ({})", keys.join(", ").dimmed());
                }
            }
        }
    }

    out.push('\n');
    out
}

fn format_tool_use(v: &serde_json::Value) -> String {
    let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
    let input = v.get("input").cloned().unwrap_or(serde_json::Value::Null);
    format_tool_call(name, &input)
}

fn format_tool_result(v: &serde_json::Value) -> String {
    let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("tool");
    let is_error = v
        .get("is_error")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    if is_error {
        let error_text = v
            .get("content")
            .and_then(|c| c.as_str())
            .or_else(|| v.get("output").and_then(|o| o.as_str()))
            .unwrap_or("(error)");
        format!("  {} {} {}\n", "✗".red(), name.dimmed(), error_text.red())
    } else {
        // Show a brief summary for successful tool results
        let output = v
            .get("content")
            .and_then(|c| c.as_str())
            .or_else(|| v.get("output").and_then(|o| o.as_str()));

        if let Some(text) = output {
            let lines: Vec<&str> = text.lines().collect();
            if lines.len() <= 5 {
                format!("  {} {}\n{}\n", "✓".green(), name.dimmed(), text.dimmed())
            } else {
                // Truncate long output
                let preview: String = lines[..3].join("\n");
                format!(
                    "  {} {} ({} lines)\n{}\n  {}\n",
                    "✓".green(),
                    name.dimmed(),
                    lines.len(),
                    preview.dimmed(),
                    "...".dimmed()
                )
            }
        } else {
            format!("  {} {}\n", "✓".green(), name.dimmed())
        }
    }
}

fn format_result(v: &serde_json::Value) -> String {
    let subtype = v
        .get("subtype")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");
    let duration_ms = v
        .get("duration_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let cost = v.get("total_cost_usd").and_then(serde_json::Value::as_f64);
    let turns = v
        .get("num_turns")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);

    let duration_str = if duration_ms > 60_000 {
        format!("{:.1}m", duration_ms as f64 / 60_000.0)
    } else {
        format!("{:.1}s", duration_ms as f64 / 1000.0)
    };

    let status = if subtype == "success" {
        "✓".green()
    } else {
        "✗".red()
    };

    let mut line = format!(
        "\n{} {} in {} ({} turn{})",
        status,
        subtype.bold(),
        duration_str.dimmed(),
        turns,
        if turns == 1 { "" } else { "s" }
    );

    if let Some(c) = cost {
        let _ = write!(line, " ${c:.4}");
    }

    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_event() {
        let line = r#"{"type":"system","model":"claude-sonnet","session_id":"abcdef1234567890"}"#;
        let result = format_stream_json(line).unwrap();
        assert!(result.contains("claude-sonnet"));
        assert!(result.contains("abcdef12"));
    }

    #[test]
    fn result_success_event() {
        let line = r#"{"type":"result","subtype":"success","result":"done","duration_ms":5000,"num_turns":3,"total_cost_usd":0.05}"#;
        let result = format_stream_json(line).unwrap();
        assert!(result.contains("success"));
        assert!(result.contains("5.0s"));
        assert!(result.contains("3 turns"));
    }

    #[test]
    fn tool_use_bash() {
        let line = r#"{"type":"tool_use","name":"Bash","input":{"command":"ls -la"}}"#;
        let result = format_stream_json(line).unwrap();
        assert!(result.contains("Bash"));
        assert!(result.contains("ls -la"));
    }

    #[test]
    fn tool_result_error() {
        let line =
            r#"{"type":"tool_result","name":"Bash","is_error":true,"content":"command not found"}"#;
        let result = format_stream_json(line).unwrap();
        assert!(result.contains("command not found"));
    }

    #[test]
    fn rate_limit_suppressed() {
        let line = r#"{"type":"rate_limit_event"}"#;
        assert!(format_stream_json(line).is_none());
    }

    #[test]
    fn unknown_event_suppressed() {
        let line = r#"{"type":"unknown_thing"}"#;
        assert!(format_stream_json(line).is_none());
    }

    #[test]
    fn lifecycle_state_change_renders() {
        use crate::shared::types::AgentState;
        let ev = StreamEvent::StateChange {
            agent_id: "abc".into(),
            old_state: AgentState::Active,
            new_state: AgentState::Dormant,
        };
        let out = format_lifecycle(&ev).unwrap();
        assert!(out.contains("active"));
        assert!(out.contains("dormant"));
    }

    #[test]
    fn lifecycle_mail_delivered_suppressed() {
        // MailDelivered is implied by the preceding MailReceived; it must
        // not produce a row so timelines aren't doubled up.
        let ev = StreamEvent::MailDelivered {
            mail_id: "m1".into(),
            recipient_id: "abc".into(),
            origin_daemon_id: None,
        };
        assert!(format_lifecycle(&ev).is_none());
    }

    #[test]
    fn malformed_json() {
        assert!(format_stream_json("not json").is_none());
        assert!(format_stream_json("").is_none());
        assert!(format_stream_json("{}").is_none()); // no type field
    }
}
