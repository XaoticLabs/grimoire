//! `grim artifact <id>`: show the structured record of what an agent
//! changed (files, diff, line counts) and what it cost (tokens, USD).

use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::AgentArtifactResult;

/// Print an agent's artifact. `--json` emits the raw record; `--diff`
/// prints the full unified diff after the summary.
pub async fn run(id: &str, json: bool, show_diff: bool) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let result: AgentArtifactResult = client
        .call_typed("agent.artifact", serde_json::json!({ "id": id }))
        .await?;

    let Some(artifact) = result.artifact else {
        if json {
            println!("null");
        } else {
            println!("no artifact captured for {id} (still running or never completed)");
        }
        return Ok(());
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&artifact)?);
        return Ok(());
    }

    println!("{} {}", "Artifact for".bold(), artifact.agent_id.bold());
    if let Some(base) = &artifact.base_commit {
        println!("  {} {}", "base:".dimmed(), &base[..12.min(base.len())]);
    }
    println!(
        "  {} {} files, {} / {} lines",
        "changes:".dimmed(),
        artifact.files_changed.len(),
        format!("+{}", artifact.insertions).green(),
        format!("-{}", artifact.deletions).red(),
    );
    println!(
        "  {} {} tokens, ${:.4}",
        "cost:".dimmed(),
        artifact.tokens_used,
        artifact.usd_spent,
    );

    if !artifact.files_changed.is_empty() {
        println!();
        for f in &artifact.files_changed {
            println!(
                "  {:<4} {:>5} {:>5}  {}",
                f.status,
                format!("+{}", f.insertions).green(),
                format!("-{}", f.deletions).red(),
                f.path,
            );
        }
    }

    if show_diff {
        match &artifact.diff {
            Some(diff) => {
                println!("\n{}", "─── diff ───".dimmed());
                print!("{diff}");
            }
            None => println!("\n{}", "(no tracked diff)".dimmed()),
        }
    } else if artifact.diff.is_some() {
        println!("\n{}", "run with --diff to see the full unified diff".dimmed());
    }

    Ok(())
}
