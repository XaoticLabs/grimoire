use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::BanishResult;

pub async fn run(id: String) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let params = serde_json::json!({ "id": id });
    let response = client.call("agent.banish", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: BanishResult = serde_json::from_value(response.result.unwrap())?;
    if result.success {
        println!("{} Agent {} has been banished", "✓".green(), id.bold());
    } else {
        println!(
            "{} Agent {} was not active (already finished or not found)",
            "!".yellow(),
            id.bold()
        );
    }

    Ok(())
}
