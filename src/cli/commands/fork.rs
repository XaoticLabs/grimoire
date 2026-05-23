use anyhow::{Context, Result, anyhow};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{ReplayEntry, ReplayResponse, StreamEvent, SummonResult};

/// Maximum bytes of folded parent output we'll prepend to a fork's prompt.
/// Set to be friendly to context windows: long enough to capture a meaningful
/// session, short enough to leave room for the actual task and the new
/// agent's working memory. Mirrors the wake-on-mail fold cap.
const MAX_FOLDED_OUTPUT: usize = 16 * 1024;

/// Build the fork's seed prompt from the parent's chronicle, truncated to
/// `MAX_FOLDED_OUTPUT` from the tail (the most recent events are the most
/// relevant). The header is intentionally explicit so a smart agent can
/// reason about its own provenance and decide what to keep / discard.
fn build_fork_prompt(
    parent_id: &str,
    cut_seq: i64,
    parent_task: &str,
    events: &[ReplayEntry],
) -> String {
    let mut buf = String::new();
    for entry in events {
        if let StreamEvent::Output { stream, line, .. } = &entry.event
            && stream == "stdout"
        {
            buf.push_str(line);
            if !line.ends_with('\n') {
                buf.push('\n');
            }
        }
    }

    // Tail-truncate: the latest output is usually the most relevant context
    // for "what was the agent thinking right before the cut?"
    if buf.len() > MAX_FOLDED_OUTPUT {
        let start = buf.len() - MAX_FOLDED_OUTPUT;
        buf = format!("[…truncated {} bytes]\n{}", start, &buf[start..]);
    }

    format!(
        "You are a fork of agent {parent_id} branched at event seq {cut_seq}.\n\
         Below is the parent agent's output stream up to the fork point — your provenance.\n\
         The original task follows after the divider.\n\
         \n\
         === parent transcript (seq 0..={cut_seq}) ===\n\
         {buf}\
         === end transcript ===\n\
         \n\
         {parent_task}"
    )
}

/// Walk the replay to find the cut index for `--at`. Integer = inclusive seq;
/// any other string = a kind name, stop after its first match. None = end of
/// life. Mirrors `chronicle`'s `--until` semantics so the two commands feel
/// like one tool.
fn resolve_cut(entries: &[ReplayEntry], at: Option<&str>) -> usize {
    match at {
        None => entries.len(),
        Some(a) => match a.parse::<i64>() {
            Ok(seq) => entries
                .iter()
                .position(|e| e.seq > seq)
                .unwrap_or(entries.len()),
            Err(_) => entries
                .iter()
                .position(|e| e.kind == a)
                .map_or(entries.len(), |i| i + 1),
        },
    }
}

pub async fn run(
    parent_id: &str,
    at: Option<String>,
    task_override: Option<String>,
    provider_override: Option<String>,
    model_override: Option<String>,
    name: Option<String>,
) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    // 1) Read the parent's full chronicle.
    let replay: ReplayResponse = client
        .call_typed("agent.replay", serde_json::json!({ "id": parent_id }))
        .await?;
    if replay.entries.is_empty() {
        return Err(anyhow!("parent agent {parent_id} has no recorded events"));
    }

    // 2) Resolve the cut point.
    let cut = resolve_cut(&replay.entries, at.as_deref());
    let considered = &replay.entries[..cut];
    let cut_seq = considered.last().map_or(0, |e| e.seq);

    // 3) Extract parent metadata from the AgentCreated event — first entry,
    // by construction. We need its task/provider/model so the fork inherits
    // them unless the caller overrides.
    let (parent_task, parent_provider, parent_model) = considered
        .iter()
        .find_map(|e| match &e.event {
            StreamEvent::AgentCreated { agent } => Some((
                agent.task.clone().unwrap_or_default(),
                agent.provider.clone(),
                agent.model.clone(),
            )),
            _ => None,
        })
        .ok_or_else(|| anyhow!("no AgentCreated event in parent's chronicle"))?;

    let task = task_override.unwrap_or_else(|| parent_task.clone());
    let provider = provider_override.or(parent_provider);
    let model = model_override.or(parent_model);

    // 4) Compose the fork prompt and summon.
    let prompt = build_fork_prompt(parent_id, cut_seq, &task, considered);

    let params = serde_json::json!({
        "task": prompt,
        "name": name,
        "model": model,
        "provider": provider,
    });
    let response = client.call("agent.summon", params).await?;
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
        "{} Forked {} from {}@seq{} (state: {})",
        "✓".green(),
        result.id.bold(),
        parent_id.dimmed(),
        cut_seq,
        result.state
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::types::{Agent, AgentState, RestartPolicy};
    use chrono::Utc;
    use std::path::PathBuf;

    fn agent_created_entry(id: &str, task: &str, seq: i64) -> ReplayEntry {
        ReplayEntry {
            seq,
            kind: "agent_created".into(),
            ts: Utc::now().to_rfc3339(),
            event: StreamEvent::AgentCreated {
                agent: Agent {
                    id: id.into(),
                    name: None,
                    state: AgentState::Active,
                    task: Some(task.into()),
                    model: None,
                    provider: Some("claude".into()),
                    cwd: PathBuf::from("/tmp"),
                    pid: None,
                    session_id: None,
                    exit_code: None,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    worker_id: None,
                    restart_policy: RestartPolicy::default(),
                    restart_count: 0,
                    workspace_id: None,
                },
            },
        }
    }

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
    fn cut_at_seq_is_inclusive() {
        let entries = vec![
            agent_created_entry("p0000001", "t", 0),
            output_entry("p0000001", 1, "a"),
            output_entry("p0000001", 2, "b"),
            output_entry("p0000001", 3, "c"),
        ];
        // seq=2 ⇒ keep [0, 1, 2]
        assert_eq!(resolve_cut(&entries, Some("2")), 3);
    }

    #[test]
    fn cut_at_kind_stops_after_first_match() {
        let entries = vec![
            agent_created_entry("p0000001", "t", 0),
            output_entry("p0000001", 1, "a"),
            output_entry("p0000001", 2, "b"),
        ];
        // First Output is at index 1; stop after it ⇒ keep [0, 1]
        assert_eq!(resolve_cut(&entries, Some("output")), 2);
    }

    #[test]
    fn cut_default_is_whole_life() {
        let entries = vec![agent_created_entry("p0000001", "t", 0)];
        assert_eq!(resolve_cut(&entries, None), 1);
    }

    #[test]
    fn fork_prompt_includes_provenance_and_task() {
        let entries = vec![
            agent_created_entry("p0000001", "original task", 0),
            output_entry("p0000001", 1, "step one"),
            output_entry("p0000001", 2, "step two"),
        ];
        let prompt = build_fork_prompt("p0000001", 2, "original task", &entries);
        assert!(prompt.contains("fork of agent p0000001"));
        assert!(prompt.contains("seq 0..=2"));
        assert!(prompt.contains("step one"));
        assert!(prompt.contains("step two"));
        assert!(prompt.contains("original task"));
    }

    #[test]
    fn fork_prompt_tail_truncates_long_output() {
        let big = "x".repeat(MAX_FOLDED_OUTPUT + 100);
        let entries = vec![
            agent_created_entry("p0000001", "t", 0),
            output_entry("p0000001", 1, &big),
        ];
        let prompt = build_fork_prompt("p0000001", 1, "t", &entries);
        assert!(prompt.contains("…truncated"));
        // The original task still survives below the transcript.
        assert!(prompt.contains("\nt"));
    }
}
