use anyhow::{Context, Result};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{PactCreateResult, PactListResult};

pub async fn run(
    source_id: Option<String>,
    task: Option<String>,
    name: Option<String>,
    list: bool,
) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    if list || (source_id.is_none() && task.is_none()) {
        // List pacts
        let params = serde_json::json!({ "source_id": source_id });
        let response = client.call("pact.list", params).await?;

        if let Some(error) = response.error {
            eprintln!("{} {}", "Error:".red(), error.message);
            std::process::exit(1);
        }

        let result: PactListResult = serde_json::from_value(
            response
                .result
                .context("daemon returned `ok` with empty result payload")?,
        )?;

        if result.pacts.is_empty() {
            println!("{}", "No pacts defined.".dimmed());
            return Ok(());
        }

        println!(
            "{:<10} {:<10} {:<8} {:<10} {}",
            "PACT".bold(),
            "SOURCE".bold(),
            "STATE".bold(),
            "TARGET".bold(),
            "TASK TEMPLATE".bold(),
        );
        println!("{}", "─".repeat(70).dimmed());

        for pact in &result.pacts {
            let state = match pact.state.as_str() {
                "pending" => "pending".yellow().to_string(),
                "fired" => "fired".green().to_string(),
                "failed" => "failed".red().to_string(),
                s => s.to_string(),
            };
            let target = pact.target_id.as_deref().unwrap_or("-");
            let tpl: String = pact.task_tpl.chars().take(35).collect();

            println!(
                "{:<10} {:<10} {:<8} {:<10} {}",
                pact.id.dimmed(),
                pact.source_id.dimmed(),
                state,
                target.dimmed(),
                tpl,
            );
        }
    } else {
        // Create pact
        let source_id = source_id.expect("source_id required");
        let task = task.expect("--task required");

        let params = serde_json::json!({
            "source_id": source_id,
            "task_tpl": task,
            "name": name,
        });

        let response = client.call("pact.create", params).await?;

        if let Some(error) = response.error {
            eprintln!("{} {}", "Error:".red(), error.message);
            std::process::exit(1);
        }

        let result: PactCreateResult = serde_json::from_value(
            response
                .result
                .context("daemon returned `ok` with empty result payload")?,
        )?;
        println!(
            "{} Pact {} created: when {} completes, fire new agent",
            "✓".green(),
            result.id.bold(),
            result.source_id.bold(),
        );
    }

    Ok(())
}
