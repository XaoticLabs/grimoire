use anyhow::{Context, Result, anyhow};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{ReplayResponse, StreamEvent, SummonResult};

/// Maximum bytes of folded transcript we send to the evaluator. Tail-keep
/// the most recent output (latest behavior dominates most rubric calls) and
/// mark the truncation explicitly so the evaluator can choose to weight or
/// caveat its score. Mirrors `fork`'s fold cap deliberately — anywhere the
/// daemon reasons about an agent's "context", 16 KiB is the line.
const MAX_FOLDED_TRANSCRIPT: usize = 16 * 1024;

/// Build the prompt sent to the evaluator agent. Inputs are intentionally
/// concatenated rather than templated through a schema so the rubric author
/// can put anything they want in the rubric file — including stricter
/// JSON-output instructions that override the default suggestion.
fn build_eval_prompt(target_id: &str, max_seq: i64, rubric: &str, transcript: &str) -> String {
    let (transcript_block, note) = if transcript.len() > MAX_FOLDED_TRANSCRIPT {
        let start = transcript.len() - MAX_FOLDED_TRANSCRIPT;
        (
            transcript[start..].to_string(),
            format!("[…earlier {start} bytes truncated]\n"),
        )
    } else {
        (transcript.to_string(), String::new())
    };

    format!(
        "You are an evaluator agent. Score the following agent's transcript \
         against the rubric. Apply the rubric strictly; do not invent criteria.\n\
         \n\
         === Rubric ===\n\
         {rubric}\n\
         \n\
         === Transcript (agent {target_id}, seq 0..={max_seq}) ===\n\
         {note}{transcript_block}\n\
         === End transcript ===\n\
         \n\
         === Output spec ===\n\
         Reply with a single JSON object on its own line, then exit. Schema:\n\
         {{\"score\": <number 0.0-1.0>, \"verdict\": \"pass\" | \"fail\" | \"partial\", \"rationale\": \"<one paragraph>\"}}\n\
         No commentary outside the JSON object."
    )
}

/// Fold the target agent's `Output` stdout events into a single transcript
/// string. Same shape as `fork::build_fork_prompt` uses — keep the two in
/// sync if one changes how it extracts output.
fn fold_transcript(entries: &[crate::shared::protocol::ReplayEntry]) -> String {
    let mut buf = String::new();
    for entry in entries {
        if let StreamEvent::Output { stream, line, .. } = &entry.event
            && stream == "stdout"
        {
            buf.push_str(line);
            if !line.ends_with('\n') {
                buf.push('\n');
            }
        }
    }
    buf
}

pub async fn run(
    target_id: &str,
    rubric_path: &str,
    provider: Option<String>,
    model: Option<String>,
    name: Option<String>,
) -> Result<()> {
    let rubric = std::fs::read_to_string(rubric_path)
        .with_context(|| format!("reading rubric file {rubric_path}"))?;
    if rubric.trim().is_empty() {
        return Err(anyhow!("rubric file {rubric_path} is empty"));
    }

    let mut client = DaemonClient::connect().await?;
    let replay: ReplayResponse = client
        .call_typed("agent.replay", serde_json::json!({ "id": target_id }))
        .await?;
    if replay.entries.is_empty() {
        return Err(anyhow!("target agent {target_id} has no recorded events"));
    }

    let max_seq = replay.entries.last().map_or(0, |e| e.seq);
    let transcript = fold_transcript(&replay.entries);
    let prompt = build_eval_prompt(target_id, max_seq, &rubric, &transcript);

    // Deterministic name encodes the link target→evaluator so `grim circle`
    // can find them without a new schema. The short id is enough since this
    // is just operator UX, not a foreign key.
    let short = &target_id[..8.min(target_id.len())];
    let evaluator_name = name.unwrap_or_else(|| format!("eval:{short}"));

    let response = client
        .call(
            "agent.summon",
            serde_json::json!({
                "task": prompt,
                "name": evaluator_name,
                "model": model,
                "provider": provider,
            }),
        )
        .await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: SummonResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;

    println!(
        "{} Evaluator {} summoned to score {} (state: {})",
        "✓".green(),
        result.id.bold(),
        target_id.dimmed(),
        result.state
    );
    println!(
        "  {} grim chronicle {} {}",
        "→".dimmed(),
        result.id,
        "# JSON verdict lands as the evaluator's last output".dimmed()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::protocol::ReplayEntry;
    use chrono::Utc;

    fn output_entry(id: &str, seq: i64, line: &str) -> ReplayEntry {
        ReplayEntry {
            seq,
            kind: "output".into(),
            ts: Utc::now().to_rfc3339(),
            event: StreamEvent::Output {
                agent_id: id.into(),
                stream: "stdout".into(),
                line: line.into(),
            },
        }
    }

    #[test]
    fn fold_transcript_concatenates_stdout_only() {
        let entries = vec![
            output_entry("a", 0, "first"),
            ReplayEntry {
                seq: 1,
                kind: "state_change".into(),
                ts: Utc::now().to_rfc3339(),
                event: StreamEvent::StateChange {
                    agent_id: "a".into(),
                    old_state: crate::shared::types::AgentState::Active,
                    new_state: crate::shared::types::AgentState::Complete,
                },
            },
            output_entry("a", 2, "second"),
        ];
        let s = fold_transcript(&entries);
        assert!(s.contains("first"));
        assert!(s.contains("second"));
        // The state-change line shouldn't appear in the folded transcript.
        assert!(!s.contains("state_change"));
    }

    #[test]
    fn eval_prompt_includes_rubric_transcript_and_schema() {
        let prompt = build_eval_prompt(
            "abc12345",
            7,
            "Did the agent identify the bug?",
            "agent said: there is a null deref on line 42\n",
        );
        assert!(prompt.contains("Did the agent identify the bug?"));
        assert!(prompt.contains("null deref on line 42"));
        assert!(prompt.contains("seq 0..=7"));
        assert!(prompt.contains("\"score\""));
        assert!(prompt.contains("\"verdict\""));
        assert!(prompt.contains("\"pass\""));
    }

    #[test]
    fn eval_prompt_tail_truncates_long_transcript() {
        let big = "x".repeat(MAX_FOLDED_TRANSCRIPT + 200);
        let prompt = build_eval_prompt("a", 1, "rubric", &big);
        assert!(prompt.contains("earlier"));
        assert!(prompt.contains("truncated"));
        // The output spec at the bottom must still survive truncation.
        assert!(prompt.contains("\"score\""));
    }
}
