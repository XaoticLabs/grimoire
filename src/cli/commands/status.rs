use std::fmt::Write as _;

use anyhow::{Context, Result};
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{DaemonStatusResult, StatusResponse};

pub fn format_text(resp: &StatusResponse) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Daemon running  ·  {} agent{}  ·  {} active, {} queued (cap {})  ·  {} workers",
        resp.agents.len(),
        if resp.agents.len() == 1 { "" } else { "s" },
        resp.active_count,
        resp.queued_count,
        resp.max_concurrent_agents,
        resp.workers.len(),
    );
    if resp.workers.is_empty() {
        out.push_str("Workers (0) — running local-only.\n");
    } else {
        let _ = writeln!(out, "Workers ({})", resp.workers.len());
        for w in &resp.workers {
            let id_short: String = w.worker_id.chars().take(6).collect();
            let _ = writeln!(
                out,
                "  {}  in_flight={}/{}  ↻ {}s  [{}]",
                id_short,
                w.in_flight,
                w.max_concurrent,
                w.last_heartbeat_age_secs,
                w.providers.join(", "),
            );
        }
    }
    out
}

pub async fn run(json: bool) -> Result<()> {
    let mut client = DaemonClient::connect().await?;

    let response = client.call("daemon.status", serde_json::json!({})).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let raw = response
        .result
        .context("daemon returned `ok` with empty result payload")?;
    let result: DaemonStatusResult = serde_json::from_value(raw)?;

    if json {
        let resp = StatusResponse {
            active_count: result.active_count,
            queued_count: result.queued_count,
            max_concurrent_agents: result.max_concurrent_agents,
            uptime_secs: result.uptime_secs,
            daemon_id: result.daemon_id,
            ..Default::default()
        };
        println!("{}", serde_json::to_string_pretty(&resp)?);
    } else {
        println!(
            "{} Daemon running  ·  {} agent{}  ·  {} active, {} queued (cap {})",
            "◆".cyan(),
            result.agent_count,
            if result.agent_count == 1 { "" } else { "s" },
            result.active_count,
            result.queued_count,
            result.max_concurrent_agents,
        );
        if let Some(id) = &result.daemon_id {
            println!("Daemon ID: grimd-{id}");
        }
    }

    Ok(())
}
