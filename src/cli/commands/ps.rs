//! `grim ps` — OS-process inventory for live agents. The dashboard's
//! "Processes" panel reads the same `agent.processes` RPC.

use anyhow::{Context, Result};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::AgentProcessesResult;

pub async fn run(json: bool) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let response = client
        .call("agent.processes", serde_json::json!({}))
        .await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: AgentProcessesResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    if result.processes.is_empty() {
        println!("no tracked processes");
        return Ok(());
    }

    println!(
        "{:<14} {:<8} {:<10} {:<6} {}",
        "AGENT", "PID", "STATE", "ALIVE", "TASK"
    );
    for p in result.processes {
        let agent = p.agent_id.chars().take(12).collect::<String>();
        let alive = if p.alive { "yes" } else { "no" };
        let row = format!(
            "{:<14} {:<8} {:<10} {:<6} {}",
            agent,
            p.pid,
            p.state,
            alive,
            p.task.as_deref().unwrap_or("").chars().take(60).collect::<String>(),
        );
        if p.stuck {
            println!("{} {}", row.yellow(), "STUCK".red().bold());
        } else {
            println!("{row}");
        }
    }
    Ok(())
}
