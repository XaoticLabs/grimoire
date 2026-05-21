use anyhow::{Context, Result, anyhow};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::SummonResult;

/// Parse `--max-restarts <N>/<T>s` into `(max, window_secs)`.
fn parse_max_restarts(s: &str) -> Result<(u32, u32)> {
    let trimmed = s.trim();
    if let Some(rest) = trimmed.strip_suffix('s')
        && let Some((n, t)) = rest.split_once('/')
    {
        let n: u32 = n
            .parse()
            .map_err(|_| anyhow!("expected format <N>/<T>s, e.g. 3/60s"))?;
        let t: u32 = t
            .parse()
            .map_err(|_| anyhow!("expected format <N>/<T>s, e.g. 3/60s"))?;
        return Ok((n, t));
    }
    Err(anyhow!("expected format <N>/<T>s, e.g. 3/60s"))
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    task: String,
    name: Option<String>,
    model: Option<String>,
    provider: Option<String>,
    keep_alive: bool,
    restart: String,
    max_restarts: Option<String>,
    escalate_to: Option<String>,
    workspace: Option<String>,
) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let (max_n, window_t) = match max_restarts {
        Some(s) => {
            let (n, t) = parse_max_restarts(&s)?;
            (Some(n), Some(t))
        }
        None => (None, None),
    };

    let params = serde_json::json!({
        "task": task,
        "name": name,
        "model": model,
        "provider": provider,
        "keep_alive": keep_alive,
        "restart_policy": restart,
        "max_restarts": max_n,
        "restart_window_secs": window_t,
        "escalate_to": escalate_to,
        "workspace": workspace,
    });

    let response = client.call("agent.summon", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: SummonResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    let mut tail = format!("(state: {})", result.state);
    if restart != "never" {
        let mr = max_n
            .zip(window_t)
            .map_or_else(|| "?".into(), |(n, t)| format!("{n}/{t}s"));
        tail = format!("(state: {}, restart: {} {})", result.state, restart, mr);
        if let Some(addr) = escalate_to {
            tail.pop(); // remove ')'
            tail = format!("{tail}, escalate-to: {addr})");
        }
    }
    println!(
        "{} Agent {} summoned {}",
        "✓".green(),
        result.id.bold(),
        tail,
    );

    Ok(())
}
