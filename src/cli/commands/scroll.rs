use anyhow::{Context, Result};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::daemon::scroll_keeper::ScrollStatus;
use crate::shared::types::Scroll;

pub async fn run(id: Option<String>, activate: bool, abandon: bool) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

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
            // Show status
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
    println!();

    for rs in &status.tasks {
        let (icon, state_str) = match rs.task.state.as_str() {
            "complete" => ("✓".green(), "done".green()),
            "active" => ("◆".yellow(), "active".yellow()),
            "blocked" => ("◇".dimmed(), "blocked".dimmed()),
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
