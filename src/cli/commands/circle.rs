use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::cli::formatters;
use crate::shared::protocol::CircleResult;

pub async fn run(state: Option<String>) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let params = serde_json::json!({ "state": state });
    let response = client.call("agent.circle", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: CircleResult = serde_json::from_value(response.result.unwrap())?;
    formatters::format_circle(&result.agents);

    Ok(())
}
