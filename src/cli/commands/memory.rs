use anyhow::{Context, Result, anyhow};
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{MemoryGetResult, MemoryListResult, MemoryPutResult};

#[derive(Debug, Subcommand)]
pub enum MemoryCommand {
    /// Write a JSON value to a workspace memory key.
    Put {
        workspace: String,
        key: String,
        /// Inline value. JSON parsed by default; use `@<file>` to read from disk.
        value: String,
        #[arg(long)]
        expected_version: Option<u64>,
    },
    /// Read a workspace memory value.
    Get { workspace: String, key: String },
    /// List keys under an optional segment-aligned prefix.
    List {
        workspace: String,
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Delete a key with optional CAS expectation.
    Delete {
        workspace: String,
        key: String,
        #[arg(long)]
        expected_version: Option<u64>,
    },
}

pub async fn run(cmd: MemoryCommand) -> Result<()> {
    match cmd {
        MemoryCommand::Put {
            workspace,
            key,
            value,
            expected_version,
        } => run_put(&workspace, &key, &value, expected_version).await,
        MemoryCommand::Get { workspace, key } => run_get(&workspace, &key).await,
        MemoryCommand::List { workspace, prefix } => run_list(&workspace, prefix.as_deref()).await,
        MemoryCommand::Delete {
            workspace,
            key,
            expected_version,
        } => run_delete(&workspace, &key, expected_version).await,
    }
}

fn parse_value(input: &str) -> Result<serde_json::Value> {
    if let Some(path) = input.strip_prefix('@') {
        let data = std::fs::read(path).map_err(|e| anyhow!("cannot read {path}: {e}"))?;
        let s = String::from_utf8(data).map_err(|e| anyhow!("file not utf8: {e}"))?;
        serde_json::from_str(&s).map_err(|e| anyhow!("invalid JSON in {path}: {e}"))
    } else {
        serde_json::from_str(input).map_err(|e| anyhow!("invalid JSON: {e}"))
    }
}

async fn run_put(
    workspace: &str,
    key: &str,
    value: &str,
    expected_version: Option<u64>,
) -> Result<()> {
    let parsed = match parse_value(value) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{} {}", "Error:".red(), e);
            std::process::exit(1);
        }
    };
    let mut client = DaemonClient::connect().await?;
    let mut params = serde_json::json!({
        "workspace_id": workspace,
        "key": key,
        "value": parsed,
    });
    if let Some(v) = expected_version {
        params["expected_version"] = serde_json::json!(v);
    }
    let resp = client.call("memory.put", params).await?;
    if let Some(err) = resp.error {
        if err.message.starts_with("cas_conflict") {
            eprintln!("{} {}", "✗".red(), err.message);
            std::process::exit(3);
        }
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: MemoryPutResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("version: {}", r.version);
    Ok(())
}

async fn run_get(workspace: &str, key: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "memory.get",
            serde_json::json!({ "workspace_id": workspace, "key": key }),
        )
        .await?;
    if let Some(err) = resp.error {
        if err.message == "memory_not_found" {
            eprintln!("not found");
            std::process::exit(1);
        }
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: MemoryGetResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("{}", serde_json::to_string_pretty(&r.value)?);
    Ok(())
}

async fn run_list(workspace: &str, prefix: Option<&str>) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let mut params = serde_json::json!({ "workspace_id": workspace });
    if let Some(p) = prefix {
        params["prefix"] = serde_json::json!(p);
    }
    let resp = client.call("memory.list", params).await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: MemoryListResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    if r.entries.is_empty() {
        println!("no entries");
    } else {
        println!("KEY\tVERSION\tSIZE\tUPDATED_AT");
        for e in &r.entries {
            println!(
                "{}\t{}\t{}\t{}",
                e.key, e.version, e.value_size, e.updated_at
            );
        }
    }
    Ok(())
}

async fn run_delete(workspace: &str, key: &str, expected_version: Option<u64>) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let mut params = serde_json::json!({
        "workspace_id": workspace,
        "key": key,
    });
    if let Some(v) = expected_version {
        params["expected_version"] = serde_json::json!(v);
    }
    let resp = client.call("memory.delete", params).await?;
    if let Some(err) = resp.error {
        if err.message.starts_with("cas_conflict") {
            eprintln!("{} {}", "✗".red(), err.message);
            std::process::exit(3);
        }
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    println!("deleted: {key}");
    Ok(())
}
