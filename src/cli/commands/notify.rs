use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;

/// `grim notify`: emit an operator-facing notification, for agents that decide
/// something is worth surfacing. The agent id is read from the daemon-injected
/// `GRIMOIRE_AGENT_ID` so the agent need not know its own id.
pub async fn run(message: &str, level: Option<String>) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let agent_id = std::env::var("GRIMOIRE_AGENT_ID").ok();
    let params = serde_json::json!({
        "message": message,
        "agent_id": agent_id,
        "level": level,
    });
    let response = client.call("notify", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    println!("{} notification published", "✓".green());
    Ok(())
}
