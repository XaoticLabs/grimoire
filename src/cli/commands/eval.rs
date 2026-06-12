use anyhow::{Context, Result, anyhow};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::eval::{EvalVerdict, build_eval_prompt, fold_stdout_output, parse_verdict};
use crate::shared::protocol::{EvalListResult, EvalRecordResult, ReplayResponse, SummonResult};

/// Fold the target agent's `Output` stdout events into a single transcript
/// string. Same shape as `fork::build_fork_prompt` uses. Keep the two in
/// sync if one changes how it extracts output.
fn fold_transcript(entries: &[crate::shared::protocol::ReplayEntry]) -> String {
    fold_stdout_output(entries.iter().map(|e| &e.event))
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
    use crate::shared::protocol::{ReplayEntry, StreamEvent};
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
}
