//! `grim demo`: one-command scaffolds that wire existing primitives into a
//! working standing-agent flow. Each step prints the underlying `grim` action
//! it performs, so the demo doubles as a legibility aid: nothing here is magic,
//! it's just `summon --keep-alive` + a file-watch wake source + `grim notify`.

use anyhow::Result;
use colored::Colorize;
use std::path::PathBuf;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{SummonResult, WakeAddResult};

/// The standing reviewer's brief. Provider-neutral: any agent CLI that can run
/// shell commands can act on it. Relies on `GRIMOIRE_AGENT_ID` (injected by the
/// daemon) so `grim notify` is auto-attributed.
const REVIEWER_PROMPT: &str = "You are a standing code reviewer running under Grimoire. \
You wake whenever a file changes in this repository. On each wake: inspect the most recent \
changes (e.g. run `git diff` and `git status`), and IF you find something a human should \
know about (a likely bug, a risky change, a failing test, a security issue), surface it by \
running `grim notify \"<short finding>\" --level warn`. If nothing is noteworthy, stay quiet \
and do nothing. Be terse. Do NOT modify any files.";

pub async fn run(name: &str, repo: Option<PathBuf>, provider: Option<String>) -> Result<()> {
    match name {
        "standing-review" => standing_review(repo, provider).await,
        other => {
            eprintln!(
                "{} unknown demo '{other}'. Available: standing-review",
                "Error:".red()
            );
            std::process::exit(2);
        }
    }
}

async fn standing_review(repo: Option<PathBuf>, provider: Option<String>) -> Result<()> {
    let repo = match repo {
        Some(p) => p,
        None => std::env::current_dir()?,
    };
    let repo = repo.canonicalize().unwrap_or_else(|_| repo.clone());
    let repo_str = repo.to_string_lossy().to_string();

    println!("{}", "Setting up a standing code-review agent.".bold());
    println!(
        "{}",
        "It wakes on file changes, reviews them, and notifies you if something's worth a look.\n"
            .dimmed()
    );

    let mut client = DaemonClient::connect().await?;

    let provider_note = provider
        .as_deref()
        .map_or(String::new(), |p| format!(" --provider {p}"));
    println!(
        "{}",
        format!("→ grim summon --keep-alive{provider_note} --cwd {repo_str} \"<reviewer brief>\"")
            .dimmed()
    );
    let summon: SummonResult = client
        .call_typed(
            "agent.summon",
            serde_json::json!({
                "task": REVIEWER_PROMPT,
                "name": "standing-reviewer",
                "provider": provider,
                "cwd": repo,
                "keep_alive": true,
            }),
        )
        .await?;
    let agent_id = summon.id;
    println!("  {} reviewer summoned: {}\n", "✓".green(), agent_id.bold());

    let ignore = vec![
        "**/.git/**".to_string(),
        "**/target/**".to_string(),
        "**/node_modules/**".to_string(),
    ];
    println!(
        "{}",
        format!("→ grim wake add {agent_id} --watch '**/*' (ignoring .git, target, node_modules)")
            .dimmed()
    );
    let wake: WakeAddResult = client
        .call_typed(
            "wake.add",
            serde_json::json!({
                "agent_id": agent_id,
                "kind": "file_watch",
                "config": {
                    "globs": ["**/*"],
                    "ignore": ignore,
                    "root": repo,
                },
            }),
        )
        .await?;
    println!(
        "  {} file-watch wake source registered: {}\n",
        "✓".green(),
        wake.wake_id.bold()
    );

    println!("{}", "Done. Your standing reviewer is live.".bold().green());
    println!("  • Edit a file in {repo_str} to wake it.");
    println!(
        "  • Set {} in ~/.grimoire/config.toml to receive the pings.",
        "[notifications].webhook_url".cyan()
    );
    println!(
        "  • Watch it live:   {}   or   {}",
        format!("grim bind {agent_id}").cyan(),
        "grim scry".cyan()
    );
    println!(
        "  • Tear it down:    {}",
        format!("grim banish {agent_id}").cyan(),
    );

    Ok(())
}
