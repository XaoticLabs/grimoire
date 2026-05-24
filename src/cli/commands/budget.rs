//! `grim budget` — read-only visibility into configured `[budgets.*]`
//! caps and today's running spend. There is no `create` / `delete` here on
//! purpose: budgets live in `config.toml` so they're versionable and
//! reproducible across daemon restarts.

use anyhow::{Context, Result};
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{BudgetListResult, BudgetStatus};

#[derive(Debug, Subcommand)]
pub enum BudgetCommand {
    /// Show every configured budget with today's USD spend.
    List,
    /// Show one budget by name.
    Show { name: String },
}

pub async fn run(cmd: BudgetCommand) -> Result<()> {
    match cmd {
        BudgetCommand::List => run_list().await,
        BudgetCommand::Show { name } => run_show(&name).await,
    }
}

async fn run_list() -> Result<()> {
    let result = fetch().await?;
    if result.budgets.is_empty() {
        println!("no budgets configured");
        return Ok(());
    }
    println!(
        "{:<24} {:>10} {:>10} {:>10}  {}",
        "NAME".bold(),
        "SPENT".bold(),
        "CAP".bold(),
        "LEFT".bold(),
        "PROVIDERS".bold()
    );
    for b in &result.budgets {
        print_row(b);
    }
    println!("\nday: {}", result.day);
    Ok(())
}

async fn run_show(name: &str) -> Result<()> {
    let result = fetch().await?;
    let Some(b) = result.budgets.iter().find(|b| b.name == name) else {
        eprintln!("{} no such budget: {}", "✗".red(), name);
        std::process::exit(1);
    };
    let providers = if b.providers.is_empty() {
        "(any)".to_string()
    } else {
        b.providers.join(",")
    };
    let cap_kind = if b.hard { "hard" } else { "soft" };
    println!("name:       {}", b.name);
    println!("day:        {}", result.day);
    println!("spent_usd:  {:.4}", b.spent_usd);
    println!("daily_usd:  {:.4}", b.daily_usd);
    println!(
        "remaining:  {:.4}",
        (b.daily_usd - b.spent_usd).max(0.0)
    );
    println!("kind:       {cap_kind}");
    println!("providers:  {providers}");
    Ok(())
}

async fn fetch() -> Result<BudgetListResult> {
    let mut client = DaemonClient::connect().await?;
    let response = client.call("budget.list", serde_json::json!({})).await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: BudgetListResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    Ok(result)
}

fn print_row(b: &BudgetStatus) {
    let remaining = (b.daily_usd - b.spent_usd).max(0.0);
    let providers = if b.providers.is_empty() {
        "(any)".to_string()
    } else {
        b.providers.join(",")
    };
    let pct = if b.daily_usd > 0.0 {
        (b.spent_usd / b.daily_usd) * 100.0
    } else {
        0.0
    };
    let spent_str = format!("${:.2}", b.spent_usd);
    let spent = if pct >= 100.0 && b.hard {
        spent_str.red().to_string()
    } else if pct >= 80.0 {
        spent_str.yellow().to_string()
    } else {
        spent_str
    };
    println!(
        "{:<24} {:>10} {:>10} {:>10}  {}",
        b.name,
        spent,
        format!("${:.2}", b.daily_usd),
        format!("${:.2}", remaining),
        providers
    );
}
