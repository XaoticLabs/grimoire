use anyhow::Result;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::ScrollInscribeResult;

pub async fn run(spec: String, concurrency: u32, activate: bool) -> Result<()> {
    // Resolve to absolute path
    let spec_path = std::fs::canonicalize(&spec)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or(spec);

    let mut client = DaemonClient::connect().await?;

    let params = serde_json::json!({
        "spec_path": spec_path,
        "max_concurrency": concurrency,
    });

    let response = client.call("scroll.inscribe", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: ScrollInscribeResult = serde_json::from_value(response.result.unwrap())?;

    println!(
        "{} Scroll '{}' inscribed (id: {}, {} tasks)",
        "✓".green(),
        result.name.bold(),
        result.id,
        result.task_count,
    );

    if !result.conflicts.is_empty() {
        println!();
        println!("{} File conflicts detected:", "⚠".yellow());
        for conflict in &result.conflicts {
            println!(
                "  {} <-> {} ({})",
                conflict.task_a_name,
                conflict.task_b_name,
                conflict.overlapping_patterns.join(", ")
            );
        }
        println!(
            "  Conflicting tasks will be serialized (not run in parallel)."
        );
    }

    if activate {
        let params = serde_json::json!({"id": result.id});
        let response = client.call("scroll.activate", params).await?;

        if let Some(error) = response.error {
            eprintln!("{} {}", "Error:".red(), error.message);
            std::process::exit(1);
        }

        println!("{} Scroll activated", "✓".green());
    } else {
        println!(
            "  Run {} to start execution",
            format!("grim scroll {} --activate", result.id).dimmed()
        );
    }

    Ok(())
}
