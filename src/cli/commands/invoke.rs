use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;

pub async fn run(id: String, message: String) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let params = serde_json::json!({ "id": id, "message": message });
    let response = client.call("agent.invoke", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    println!(
        "{} Invoked agent {} with new message",
        "✓".green(),
        id.bold(),
    );
    println!("{}", "Use `grim bind` to watch the output".dimmed());

    Ok(())
}
