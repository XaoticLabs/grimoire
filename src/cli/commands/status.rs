use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::DaemonStatusResult;

pub async fn run() -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let response = client.call("daemon.status", serde_json::json!({})).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: DaemonStatusResult = serde_json::from_value(response.result.unwrap())?;

    println!(
        "{} Daemon running  ·  {} agent{}  ·  {} active",
        "◆".cyan(),
        result.agent_count,
        if result.agent_count == 1 { "" } else { "s" },
        result.active_count,
    );

    Ok(())
}
