use anyhow::{Context, Result, anyhow};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::daemon::scroll_keeper::ScrollStatus;
use crate::shared::protocol::ScrollDispatchTaskResult;
use crate::shared::types::Scroll;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    id: Option<String>,
    activate: bool,
    abandon: bool,
    dispatch_task: Option<String>,
    to: Option<String>,
    approve: Option<String>,
    reject: Option<String>,
) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    // HITL approve / reject of a held task.
    if let Some(ref scroll_id) = id
        && (approve.is_some() || reject.is_some())
    {
        let (method, task) = match (&approve, &reject) {
            (Some(t), _) => ("scroll.approve", t.clone()),
            (_, Some(t)) => ("scroll.reject", t.clone()),
            _ => unreachable!(),
        };
        let resp = client
            .call(
                method,
                serde_json::json!({ "scroll_id": scroll_id, "task": task }),
            )
            .await?;
        if let Some(err) = resp.error {
            return Err(anyhow!("{}", err.message));
        }
        let result: crate::shared::protocol::ScrollApproveResult =
            serde_json::from_value(resp.result.unwrap_or_default())?;
        let verb = if result.decision == "approved" {
            "Approved".green()
        } else {
            "Rejected".red()
        };
        println!(
            "{} task {} in scroll {}",
            verb,
            result.task_name.bold(),
            result.scroll_id.dimmed(),
        );
        return Ok(());
    }

    if let (Some(scroll_id), Some(task_id), Some(peer)) = (id.as_ref(), dispatch_task, to) {
        let resp = client
            .call(
                "scroll.dispatch-task",
                serde_json::json!({
                    "scroll_id": scroll_id,
                    "task_id": task_id,
                    "peer": peer,
                }),
            )
            .await?;
        if let Some(err) = resp.error {
            return Err(anyhow!("scroll dispatch-task failed: {}", err.message));
        }
        let result: ScrollDispatchTaskResult =
            serde_json::from_value(resp.result.unwrap_or_default())?;
        println!(
            "Dispatched task {} (scroll {}) to peer {} (seq {}).",
            result.task_id.dimmed(),
            result.scroll_id.dimmed(),
            result.peer.bold(),
            result.sender_seq,
        );
        return Ok(());
    }

    if let Some(ref scroll_id) = id {
        if activate {
            let params = serde_json::json!({"id": scroll_id});
            let response = client.call("scroll.activate", params).await?;
            if let Some(error) = response.error {
                eprintln!("{} {}", "Error:".red(), error.message);
                std::process::exit(1);
            }
            println!("{} Scroll {} activated", "✓".green(), scroll_id);
        } else if abandon {
            let params = serde_json::json!({"id": scroll_id});
            let response = client.call("scroll.abandon", params).await?;
            if let Some(error) = response.error {
                eprintln!("{} {}", "Error:".red(), error.message);
                std::process::exit(1);
            }
            println!("{} Scroll {} abandoned", "✓".green(), scroll_id);
        } else {
            let params = serde_json::json!({"id": scroll_id});
            let response = client.call("scroll.status", params).await?;
            if let Some(error) = response.error {
                eprintln!("{} {}", "Error:".red(), error.message);
                std::process::exit(1);
            }

            let status: ScrollStatus = serde_json::from_value(
                response
                    .result
                    .context("daemon returned `ok` with empty result payload")?,
            )?;
            print_scroll_status(&status);
        }
    } else {
        let response = client.call("scroll.list", serde_json::json!({})).await?;
        if let Some(error) = response.error {
            eprintln!("{} {}", "Error:".red(), error.message);
            std::process::exit(1);
        }

        let result: serde_json::Value = response
            .result
            .context("daemon returned `ok` with empty result payload")?;
        let scrolls: Vec<Scroll> = serde_json::from_value(result["scrolls"].clone())?;

        if scrolls.is_empty() {
            println!(
                "No scrolls. Use {} to create one.",
                "grim inscribe <spec.md>".dimmed()
            );
            return Ok(());
        }

        for scroll in &scrolls {
            let state_colored = match scroll.state.as_str() {
                "inscribed" => scroll.state.to_string().blue(),
                "active" => scroll.state.to_string().yellow(),
                "complete" => scroll.state.to_string().green(),
                "failed" => scroll.state.to_string().red(),
                "abandoned" => scroll.state.to_string().dimmed(),
                _ => scroll.state.to_string().normal(),
            };
            println!(
                "  {} {} [{}]",
                scroll.id.dimmed(),
                scroll.name.bold(),
                state_colored,
            );
        }
    }

    Ok(())
}

fn print_scroll_status(status: &ScrollStatus) {
    let state_colored = match status.scroll.state.as_str() {
        "inscribed" => "inscribed".blue(),
        "active" => "active".yellow(),
        "complete" => "complete".green(),
        "failed" => "failed".red(),
        "abandoned" => "abandoned".dimmed(),
        _ => status.scroll.state.to_string().normal(),
    };

    println!(
        "{} Scroll: {}  [{}]",
        "◆".bold(),
        status.scroll.name.bold(),
        state_colored,
    );
    println!(
        "  {} tasks: {} complete, {} active, {} blocked, {} ready, {} failed, {} skipped",
        status.total,
        status.complete.to_string().green(),
        status.active.to_string().yellow(),
        status.blocked,
        status.ready.to_string().cyan(),
        status.failed.to_string().red(),
        status.skipped,
    );
    if status.awaiting_approval > 0 {
        println!(
            "  {} {} awaiting approval — review then `grim scroll {} --approve <task>`",
            "⏸".magenta(),
            status.awaiting_approval.to_string().magenta(),
            status.scroll.id,
        );
    }
    println!();

    for rs in &status.tasks {
        let (icon, state_str) = match rs.task.state.as_str() {
            "complete" => ("✓".green(), "done".green()),
            "active" => ("◆".yellow(), "active".yellow()),
            "blocked" => ("◇".dimmed(), "blocked".dimmed()),
            "awaiting_approval" => ("⏸".magenta(), "approve?".magenta()),
            "ready" => ("○".cyan(), "ready".cyan()),
            "failed" => ("✗".red(), "failed".red()),
            "skipped" => ("–".dimmed(), "skipped".dimmed()),
            s => ("?".normal(), s.to_string().normal()),
        };

        let agent_info = rs
            .task
            .agent_id
            .as_ref()
            .map(|id| format!("agent:{id}"))
            .unwrap_or_default();

        let dep_info = if !rs.depends_on_names.is_empty()
            && (rs.task.state.as_str() == "blocked" || rs.task.state.as_str() == "ready")
        {
            format!("  waiting on: {}", rs.depends_on_names.join(", "))
        } else {
            String::new()
        };

        println!(
            "  [{}] {:<8} {:<30} {}{}",
            icon,
            state_str,
            rs.task.name,
            agent_info.dimmed(),
            dep_info.dimmed(),
        );
    }

    if !status.conflicts.is_empty() {
        println!();
        println!("{} Active conflicts:", "⚠".yellow());
        for c in &status.conflicts {
            println!(
                "  {} <-> {} ({})",
                c.task_a_name,
                c.task_b_name,
                c.overlapping_patterns.join(", ")
            );
        }
    }
}
