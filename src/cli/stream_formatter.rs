use colored::Colorize;

/// Parse and format a Claude Code stream-json line into human-readable output.
/// Returns None if the event should be suppressed (e.g. rate_limit_event).
pub fn format_stream_json(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;

    let event_type = v.get("type")?.as_str()?;

    match event_type {
        "system" => format_system(&v),
        "assistant" => format_assistant(&v),
        "tool_use" => format_tool_use(&v),
        "tool_result" => format_tool_result(&v),
        "result" => format_result(&v),
        "rate_limit_event" => None, // suppress
        _ => None,
    }
}

fn format_system(v: &serde_json::Value) -> Option<String> {
    let model = v.get("model").and_then(|m| m.as_str()).unwrap_or("unknown");
    let session_id = v.get("session_id").and_then(|s| s.as_str()).unwrap_or("?");
    let short_session = &session_id[..8.min(session_id.len())];

    Some(format!(
        "{} {} session {}",
        "◆".cyan(),
        model.bold(),
        short_session.dimmed()
    ))
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
                out.push_str(&format!("\n  {}", cmd.dimmed()));
            }
        }
        "Read" | "Write" | "Edit" => {
            if let Some(path) = input.get("file_path").and_then(|p| p.as_str()) {
                out.push_str(&format!(" {}", path.dimmed()));
            }
        }
        "Glob" | "Grep" => {
            if let Some(pattern) = input.get("pattern").and_then(|p| p.as_str()) {
                out.push_str(&format!(" {}", pattern.dimmed()));
            }
        }
        _ => {
            // For unknown tools, show a compact summary of the input
            if let Some(obj) = input.as_object() {
                let keys: Vec<&str> = obj.keys().map(|k| k.as_str()).collect();
                if !keys.is_empty() {
                    out.push_str(&format!(" ({})", keys.join(", ").dimmed()));
                }
            }
        }
    }

    out.push('\n');
    out
}

fn format_tool_use(v: &serde_json::Value) -> Option<String> {
    let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
    let input = v.get("input").cloned().unwrap_or(serde_json::Value::Null);
    Some(format_tool_call(name, &input))
}

fn format_tool_result(v: &serde_json::Value) -> Option<String> {
    let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("tool");
    let is_error = v.get("is_error").and_then(|e| e.as_bool()).unwrap_or(false);

    if is_error {
        let error_text = v
            .get("content")
            .and_then(|c| c.as_str())
            .or_else(|| v.get("output").and_then(|o| o.as_str()))
            .unwrap_or("(error)");
        Some(format!(
            "  {} {} {}\n",
            "✗".red(),
            name.dimmed(),
            error_text.red()
        ))
    } else {
        // Show a brief summary for successful tool results
        let output = v
            .get("content")
            .and_then(|c| c.as_str())
            .or_else(|| v.get("output").and_then(|o| o.as_str()));

        if let Some(text) = output {
            let lines: Vec<&str> = text.lines().collect();
            if lines.len() <= 5 {
                Some(format!(
                    "  {} {}\n{}\n",
                    "✓".green(),
                    name.dimmed(),
                    text.dimmed()
                ))
            } else {
                // Truncate long output
                let preview: String = lines[..3].join("\n");
                Some(format!(
                    "  {} {} ({} lines)\n{}\n  {}\n",
                    "✓".green(),
                    name.dimmed(),
                    lines.len(),
                    preview.dimmed(),
                    "...".dimmed()
                ))
            }
        } else {
            Some(format!("  {} {}\n", "✓".green(), name.dimmed()))
        }
    }
}

fn format_result(v: &serde_json::Value) -> Option<String> {
    let subtype = v
        .get("subtype")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");
    let duration_ms = v.get("duration_ms").and_then(|d| d.as_u64()).unwrap_or(0);
    let cost = v.get("total_cost_usd").and_then(|c| c.as_f64());
    let turns = v.get("num_turns").and_then(|t| t.as_u64()).unwrap_or(0);

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
        line.push_str(&format!(" ${:.4}", c));
    }

    Some(line)
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
    fn malformed_json() {
        assert!(format_stream_json("not json").is_none());
        assert!(format_stream_json("").is_none());
        assert!(format_stream_json("{}").is_none()); // no type field
    }
}
