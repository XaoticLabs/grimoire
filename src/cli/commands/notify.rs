use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;

/// `grim notify`: emit an operator-facing notification. Intended for spawned
/// agents to call when *they* decide something is worth surfacing ("ping me
/// only if interesting"). Provider-neutral: any agent CLI that can run a shell
/// command can use it. The agent's own id is read from `GRIMOIRE_AGENT_ID`
/// (injected by the daemon) so the agent need not know it.
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
