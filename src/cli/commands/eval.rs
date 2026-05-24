use anyhow::{Context, Result, anyhow};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{
    EvalListResult, EvalRecordResult, ReplayResponse, StreamEvent, SummonResult,
};

/// Maximum bytes of folded transcript we send to the evaluator. Tail-keep
/// the most recent output (latest behavior dominates most rubric calls) and
/// mark the truncation explicitly so the evaluator can choose to weight or
/// caveat its score. Mirrors `fork`'s fold cap deliberately: anywhere the
/// daemon reasons about an agent's "context", 16 KiB is the line.
const MAX_FOLDED_TRANSCRIPT: usize = 16 * 1024;

/// Build the prompt sent to the evaluator agent. Inputs are intentionally
/// concatenated rather than templated through a schema so the rubric author
/// can put anything they want in the rubric file, including stricter
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
/// string. Same shape as `fork::build_fork_prompt` uses. Keep the two in
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
    wait: bool,
    timeout_secs: u64,
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

    if !wait {
        println!(
            "  {} grim chronicle {} {}",
            "→".dimmed(),
            result.id,
            "# JSON verdict lands as the evaluator's last output".dimmed()
        );
        return Ok(());
    }

    // Block until the evaluator reaches a terminal state, then parse its
    // last-result JSON. Polling, not subscribing, keeps the CLI hops
    // identical to the rest of `grim`; eval runs are short-lived enough
    // that a 1 s tick is fine.
    let parsed = wait_for_verdict(&mut client, &result.id, timeout_secs).await?;
    let record_id = record_verdict(&mut client, target_id, &result.id, &parsed).await?;
    print_verdict(&result.id, &parsed);
    println!("  {} recorded as eval {}", "↳".dimmed(), record_id.dimmed());
    Ok(())
}

/// `grim eval <id> --list`: print every recorded evaluation for the
/// target, newest first. Mirrors `grim mail list` in shape so operators
/// can scan an agent's full review history at a glance.
pub async fn run_list(target_id: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let result: EvalListResult = client
        .call_typed("eval.list", serde_json::json!({ "target_id": target_id }))
        .await?;
    if result.results.is_empty() {
        println!("no evaluations recorded for {target_id}");
        return Ok(());
    }
    println!(
        "{:<10} {:>6}  {:<8}  {:<10}  RATIONALE",
        "EVAL".bold(),
        "SCORE".bold(),
        "VERDICT".bold(),
        "EVALUATOR".bold(),
    );
    for r in &result.results {
        let short_id = &r.id[..8.min(r.id.len())];
        let short_eval = &r.evaluator_id[..8.min(r.evaluator_id.len())];
        let verdict = r.verdict.as_deref().unwrap_or("-");
        let rationale = r.rationale.as_deref().unwrap_or("").replace('\n', " ");
        let trimmed: String = rationale.chars().take(80).collect();
        println!(
            "{:<10} {:>6.2}  {:<8}  {:<10}  {}",
            short_id, r.score, verdict, short_eval, trimmed
        );
    }
    Ok(())
}

async fn record_verdict(
    client: &mut DaemonClient,
    target_id: &str,
    evaluator_id: &str,
    v: &EvalVerdict,
) -> Result<String> {
    let resp: EvalRecordResult = client
        .call_typed(
            "eval.record",
            serde_json::json!({
                "target_id": target_id,
                "evaluator_id": evaluator_id,
                "score": v.score,
                "verdict": v.verdict,
                "rationale": v.rationale,
            }),
        )
        .await?;
    Ok(resp.id)
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct EvalVerdict {
    score: f64,
    #[serde(default)]
    verdict: Option<String>,
    #[serde(default)]
    rationale: Option<String>,
}

async fn wait_for_verdict(
    client: &mut DaemonClient,
    evaluator_id: &str,
    timeout_secs: u64,
) -> Result<EvalVerdict> {
    use crate::shared::protocol::AgentResultResponse;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        let resp: AgentResultResponse = client
            .call_typed("agent.result", serde_json::json!({ "id": evaluator_id }))
            .await?;
        let terminal = matches!(
            resp.state.as_str(),
            "Complete" | "Failed" | "Banished" | "Dormant"
        );
        if terminal {
            let text = resp.result.ok_or_else(|| {
                anyhow!(
                    "evaluator {evaluator_id} finished as {} with no result",
                    resp.state
                )
            })?;
            return parse_verdict(&text);
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "evaluator {evaluator_id} did not finish within {timeout_secs}s (last state {})",
                resp.state
            ));
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// Pull the score JSON out of the evaluator's free-form reply. We accept
/// either a bare JSON object (the strict path requested by the prompt) or
/// a JSON object embedded anywhere in the result, because agents reliably drift
/// from "JSON only" and rejecting that drift would make eval brittle.
fn parse_verdict(text: &str) -> Result<EvalVerdict> {
    if let Ok(v) = serde_json::from_str::<EvalVerdict>(text.trim()) {
        return Ok(v);
    }
    let start = text
        .find('{')
        .ok_or_else(|| anyhow!("no JSON object in evaluator reply"))?;
    let end = text
        .rfind('}')
        .ok_or_else(|| anyhow!("unterminated JSON in evaluator reply"))?;
    if end <= start {
        return Err(anyhow!("malformed JSON range in evaluator reply"));
    }
    serde_json::from_str::<EvalVerdict>(&text[start..=end])
        .with_context(|| "parsing evaluator score JSON")
}

fn print_verdict(evaluator_id: &str, v: &EvalVerdict) {
    let score_str = format!("{:.2}", v.score);
    let colored_score = if v.score >= 0.8 {
        score_str.green().to_string()
    } else if v.score >= 0.5 {
        score_str.yellow().to_string()
    } else {
        score_str.red().to_string()
    };
    let verdict_str = v.verdict.as_deref().unwrap_or("(none)");
    println!(
        "{} score: {}  verdict: {}  (evaluator {})",
        "★".bold(),
        colored_score,
        verdict_str.bold(),
        evaluator_id.dimmed()
    );
    if let Some(r) = &v.rationale {
        println!("  {}", r.trim());
    }
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
    fn parse_verdict_accepts_bare_json() {
        let v = parse_verdict(r#"{"score":0.9,"verdict":"pass","rationale":"good"}"#).unwrap();
        assert!((v.score - 0.9).abs() < 1e-6);
        assert_eq!(v.verdict.as_deref(), Some("pass"));
    }

    #[test]
    fn parse_verdict_extracts_embedded_json() {
        let v = parse_verdict(
            "Here is my evaluation:\n{\"score\":0.4,\"verdict\":\"partial\"}\nThank you.",
        )
        .unwrap();
        assert!((v.score - 0.4).abs() < 1e-6);
        assert_eq!(v.verdict.as_deref(), Some("partial"));
    }

    #[test]
    fn parse_verdict_rejects_no_object() {
        assert!(parse_verdict("nothing useful here").is_err());
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
