//! `grim wake`: manage agent wake sources.

use anyhow::{Context, Result};
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::{DaemonClient, resolve_agent_id};
use crate::shared::protocol::{WakeAddResult, WakeListResult, WakeTestResult};

#[derive(Debug, Subcommand)]
pub enum WakeCommand {
    /// Register a wake source for an agent. Exactly one of --cron / --watch /
    /// --on-parent must be provided. --watch may repeat; --ignore as well.
    Add {
        /// Agent id (or short prefix).
        agent_id: String,
        /// Cron expression (5 fields). Times are evaluated in UTC.
        #[arg(long, conflicts_with_all = ["watch", "on_parent"])]
        cron: Option<String>,
        /// Glob to watch under the agent's cwd. May repeat.
        #[arg(long, conflicts_with_all = ["cron", "on_parent"])]
        watch: Vec<String>,
        /// Glob to ignore. May repeat. Used with --watch.
        #[arg(long)]
        ignore: Vec<String>,
        /// Parent agent id whose state changes wake this agent.
        #[arg(long = "on-parent", conflicts_with_all = ["cron", "watch", "remote_watch"])]
        on_parent: Option<String>,
        /// Comma-separated parent target states (default: complete).
        /// Used with `--on-parent` or `--on-remote-parent`.
        #[arg(long)]
        states: Option<String>,
        /// Shadow workspace id to watch for federated file events.
        /// Pair with --watch globs (and optionally --ignore). When set,
        /// the wake fires on inbound `WorkspaceEventDeliver` payloads
        /// rather than local notify events.
        #[arg(long = "remote-watch", conflicts_with_all = ["cron", "on_parent"])]
        remote_watch: Option<String>,
        /// Remote agent id (federated parent). Requires
        /// `--sender-daemon` to identify which peer's lifecycle stream
        /// to subscribe to. Optional `--states` filters target states.
        #[arg(
            long = "on-remote-parent",
            conflicts_with_all = ["cron", "watch", "on_parent", "remote_watch"]
        )]
        on_remote_parent: Option<String>,
        /// `grimd-...` daemon id of the home daemon for the remote
        /// parent. Required with `--on-remote-parent`.
        #[arg(long, requires = "on_remote_parent")]
        sender_daemon: Option<String>,
    },
    /// List wake sources. Without an agent id, lists all.
    List {
        /// Agent id (or short prefix). Omit for all.
        agent_id: Option<String>,
    },
    /// Remove a wake source.
    Remove { wake_id: String },
    /// Manually fire a wake source bypassing rate limits.
    Test { wake_id: String },
}

pub async fn run(cmd: WakeCommand) -> Result<()> {
    match cmd {
        WakeCommand::Add {
            agent_id,
            cron,
            watch,
            ignore,
            on_parent,
            states,
            remote_watch,
            on_remote_parent,
            sender_daemon,
        } => {
            run_add(
                &agent_id,
                cron,
                watch,
                ignore,
                on_parent,
                states,
                remote_watch,
                on_remote_parent,
                sender_daemon,
            )
            .await
        }
        WakeCommand::List { agent_id } => run_list(agent_id).await,
        WakeCommand::Remove { wake_id } => run_remove(&wake_id).await,
        WakeCommand::Test { wake_id } => run_test(&wake_id).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_add(
    agent_prefix: &str,
    cron: Option<String>,
    watch: Vec<String>,
    ignore: Vec<String>,
    on_parent: Option<String>,
    states: Option<String>,
    remote_watch: Option<String>,
    on_remote_parent: Option<String>,
    sender_daemon: Option<String>,
) -> Result<()> {
    let agent_id = resolve_agent_id(agent_prefix).await?;
    let mut client = DaemonClient::connect().await?;

    let (kind, config) = if let Some(expr) = cron {
        ("cron".to_string(), serde_json::json!({ "expr": expr }))
    } else if let Some(remote_agent_id) = on_remote_parent {
        let Some(daemon) = sender_daemon else {
            eprintln!(
                "{} --on-remote-parent requires --sender-daemon",
                "Error:".red()
            );
            std::process::exit(2);
        };
        let states_vec: Vec<String> = states
            .map(|s| {
                s.split(',')
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        (
            "remote_agent_completion".to_string(),
            serde_json::json!({
                "sender_daemon_id": daemon,
                "remote_agent_id": remote_agent_id,
                "states": states_vec,
            }),
        )
    } else if let Some(workspace_id) = remote_watch {
        if watch.is_empty() {
            eprintln!(
                "{} --remote-watch requires at least one --watch glob",
                "Error:".red()
            );
            std::process::exit(2);
        }
        (
            "remote_file_watch".to_string(),
            serde_json::json!({
                "workspace_id": workspace_id,
                "globs": watch,
                "ignore": ignore,
            }),
        )
    } else if !watch.is_empty() {
        let cwd = std::env::current_dir()?;
        (
            "file_watch".to_string(),
            serde_json::json!({
                "globs": watch,
                "ignore": ignore,
                "root": cwd,
            }),
        )
    } else if let Some(parent_prefix) = on_parent {
        let parent_id = resolve_agent_id(&parent_prefix).await?;
        let states_vec: Vec<String> = match states {
            Some(s) => s
                .split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
            None => vec!["complete".into()],
        };
        (
            "parent_completion".to_string(),
            serde_json::json!({
                "parent_id": parent_id,
                "states": states_vec,
            }),
        )
    } else {
        eprintln!(
            "{} one of --cron / --watch / --on-parent / --remote-watch / --on-remote-parent is required",
            "Error:".red()
        );
        std::process::exit(2);
    };

    let response = client
        .call(
            "wake.add",
            serde_json::json!({
                "agent_id": agent_id,
                "kind": kind,
                "config": config,
            }),
        )
        .await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: WakeAddResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!(
        "Wake source {} registered ({}) for agent {}.",
        result.wake_id.bold(),
        kind,
        agent_id,
    );
    Ok(())
}

async fn run_list(agent_prefix: Option<String>) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let params = match agent_prefix {
        Some(p) => {
            let id = resolve_agent_id(&p).await?;
            serde_json::json!({ "agent_id": id })
        }
        None => serde_json::json!({}),
    };
    let response = client.call("wake.list", params).await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: WakeListResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    if result.sources.is_empty() {
        println!("no wake sources");
        return Ok(());
    }
    println!(
        "{:<14} {:<18} {:<8} {:<22} {:<8} FIRES",
        "ID", "KIND", "AGENT", "CONFIG", "STATE"
    );
    for s in &result.sources {
        let agent_short: String = s.agent_id.chars().take(8).collect();
        let cfg_short: String = s.config_json.chars().take(22).collect();
        println!(
            "{:<14} {:<18} {:<8} {:<22} {:<8} {}",
            s.id,
            s.kind.as_str(),
            agent_short,
            cfg_short,
            s.state.as_str(),
            s.fire_count,
        );
    }
    Ok(())
}

async fn run_remove(wake_id: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let response = client
        .call("wake.remove", serde_json::json!({ "wake_id": wake_id }))
        .await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    println!("removed: {wake_id}");
    Ok(())
}

async fn run_test(wake_id: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let response = client
        .call("wake.test", serde_json::json!({ "wake_id": wake_id }))
        .await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: WakeTestResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("fired: mail {}", result.mail_id);
    Ok(())
}
