//! `grim peer` — federation peer management (Task 11).

use anyhow::{Result, anyhow};
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{PeerAddResult, PeerListResult, PeerPingResult, PeerRemoveResult};

#[derive(Debug, Subcommand)]
pub enum PeerCommand {
    /// Register a peer and attempt the initial handshake.
    Add {
        name: String,
        #[arg(long)]
        url: String,
        #[arg(long)]
        token: String,
    },
    /// List configured peers.
    List,
    /// Remove a peer (cascades outbox/topic_federations; mail rows retained).
    Remove { name: String },
    /// Ping a peer (returns RTT and stream state).
    Ping { name: String },
}

pub async fn run(cmd: PeerCommand) -> Result<()> {
    match cmd {
        PeerCommand::Add { name, url, token } => run_add(name, url, token).await,
        PeerCommand::List => run_list().await,
        PeerCommand::Remove { name } => run_remove(name).await,
        PeerCommand::Ping { name } => run_ping(name).await,
    }
}

async fn run_add(name: String, url: String, token: String) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "peer.add",
            serde_json::json!({ "name": name, "url": url, "bearer_token": token }),
        )
        .await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("peer add failed: {}", err.message));
    }
    let result: PeerAddResult = serde_json::from_value(resp.result.unwrap_or_default())?;
    println!(
        "{} {} (peer_id={}, daemon_id=grimd-{})",
        "Added peer".green(),
        name,
        result.peer_id,
        result.daemon_id
    );
    Ok(())
}

async fn run_list() -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client.call("peer.list", serde_json::json!({})).await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("peer list failed: {}", err.message));
    }
    let result: PeerListResult = serde_json::from_value(resp.result.unwrap_or_default())?;
    if result.peers.is_empty() {
        println!("(no peers)");
        return Ok(());
    }
    println!(
        "{:<20} {:<14} {:<10} {:<28} OUTBOX",
        "NAME", "DAEMON_ID", "STATE", "URL"
    );
    for p in &result.peers {
        let dimm: &str = if p.daemon_id.is_empty() {
            "—"
        } else {
            &p.daemon_id
        };
        println!(
            "{:<20} grimd-{:<8} {:<10} {:<28} {}",
            p.name, dimm, p.state, p.url, p.outbox_depth
        );
    }
    Ok(())
}

async fn run_remove(name: String) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call("peer.remove", serde_json::json!({ "name": name }))
        .await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("peer remove failed: {}", err.message));
    }
    let result: PeerRemoveResult = serde_json::from_value(resp.result.unwrap_or_default())?;
    if result.removed {
        println!("Removed peer {name}");
    } else {
        println!("Peer {name} not found");
    }
    Ok(())
}

async fn run_ping(name: String) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call("peer.ping", serde_json::json!({ "name": name }))
        .await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("peer ping failed: {}", err.message));
    }
    let result: PeerPingResult = serde_json::from_value(resp.result.unwrap_or_default())?;
    println!("{} state={} rtt={}ms", name, result.state, result.rtt_ms);
    Ok(())
}
