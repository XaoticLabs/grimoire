use anyhow::{Context, Result};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::cli::formatters;
use crate::shared::protocol::QueueListResponse;

pub async fn run(json: bool) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let response = client
        .call("agent.queue.list", serde_json::json!({}))
        .await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: QueueListResponse = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        print!("{}", formatters::format_queue(&result.entries));
    }

    Ok(())
}
