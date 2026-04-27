use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;

/// `grim invoke` is now a thin wrapper over `mail.send --wake-eligible`. The
/// scheduler's mail-wake path (T1) picks the message up and resumes any
/// Dormant agent at the recipient address.
pub async fn run(id: String, message: String) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let to = format!("agent://{}", id);
    let params = serde_json::json!({
        "to": to,
        "body": message,
        "wake_eligible": true,
    });
    let response = client.call("mail.send", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    println!(
        "{} Invoked agent {} (queued via mail-wake)",
        "✓".green(),
        id.bold(),
    );
    println!("{}", "Use `grim bind` to watch the output".dimmed());

    Ok(())
}
