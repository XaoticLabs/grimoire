//! `grim ns` — federated namespace memory. A string-named KV store that can
//! replicate across daemons (see `ns federate`). Values are UTF-8 strings;
//! conflicts resolve last-write-wins on a Lamport tuple.

use anyhow::{Context, Result};
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{NsGetResult, NsListResult, NsPutResult};

#[derive(Debug, Subcommand)]
pub enum NsCommand {
    /// Write a value to a namespace key.
    Put {
        namespace: String,
        key: String,
        /// Inline value, or `@<file>` to read from disk.
        value: String,
    },
    /// Read a namespace value.
    Get { namespace: String, key: String },
    /// List keys under an optional prefix.
    List {
        namespace: String,
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Delete a key (replicates as a tombstone).
    Delete { namespace: String, key: String },
    /// Replicate a namespace to/from a peer. Direction: inbound | outbound | both.
    Federate {
        namespace: String,
        peer: String,
        #[arg(long, default_value = "both")]
        direction: String,
    },
}

pub async fn run(cmd: NsCommand) -> Result<()> {
    match cmd {
        NsCommand::Put {
            namespace,
            key,
            value,
        } => run_put(&namespace, &key, &value).await,
        NsCommand::Get { namespace, key } => run_get(&namespace, &key).await,
        NsCommand::List { namespace, prefix } => run_list(&namespace, prefix.as_deref()).await,
        NsCommand::Delete { namespace, key } => run_delete(&namespace, &key).await,
        NsCommand::Federate {
            namespace,
            peer,
            direction,
        } => run_federate(&namespace, &peer, &direction).await,
    }
}

fn read_value(input: &str) -> Result<String> {
    if let Some(path) = input.strip_prefix('@') {
        let data = std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
        Ok(data)
    } else {
        Ok(input.to_string())
    }
}

async fn run_put(namespace: &str, key: &str, value: &str) -> Result<()> {
    let value = read_value(value)?;
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "ns.put",
            serde_json::json!({ "namespace": namespace, "key": key, "value": value }),
        )
        .await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: NsPutResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!(
        "lamport: {} (origin grimd-{})",
        r.lamport, r.origin_daemon_id
    );
    Ok(())
}

async fn run_get(namespace: &str, key: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "ns.get",
            serde_json::json!({ "namespace": namespace, "key": key }),
        )
        .await?;
    if let Some(err) = resp.error {
        if err.message == "ns_key_not_found" {
            eprintln!("not found");
            std::process::exit(1);
        }
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: NsGetResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("{}", r.value);
    Ok(())
}

async fn run_list(namespace: &str, prefix: Option<&str>) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let mut params = serde_json::json!({ "namespace": namespace });
    if let Some(p) = prefix {
        params["prefix"] = serde_json::json!(p);
    }
    let resp = client.call("ns.list", params).await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: NsListResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    if r.entries.is_empty() {
        println!("no entries");
    } else {
        println!("KEY\tLAMPORT\tORIGIN\tUPDATED_AT");
        for e in &r.entries {
            println!(
                "{}\t{}\tgrimd-{}\t{}",
                e.key, e.lamport, e.origin_daemon_id, e.updated_at
            );
        }
    }
    Ok(())
}

async fn run_delete(namespace: &str, key: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "ns.delete",
            serde_json::json!({ "namespace": namespace, "key": key }),
        )
        .await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    println!("deleted: {key}");
    Ok(())
}

async fn run_federate(namespace: &str, peer: &str, direction: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "ns.federate",
            serde_json::json!({ "namespace": namespace, "peer": peer, "direction": direction }),
        )
        .await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    println!(
        "{} namespace {} {} peer {}",
        "Federated".green(),
        namespace,
        direction,
        peer
    );
    Ok(())
}
