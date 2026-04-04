use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::SummonResult;

pub async fn run(task: String, name: Option<String>, model: Option<String>) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let params = serde_json::json!({
        "task": task,
        "name": name,
        "model": model,
    });

    let response = client.call("agent.summon", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: SummonResult = serde_json::from_value(response.result.unwrap())?;
    println!(
        "{} Agent {} summoned (state: {})",
        "✓".green(),
        result.id.bold(),
        result.state,
    );

    Ok(())
}
