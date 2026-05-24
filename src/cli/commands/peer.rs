//! `grim peer`: federation peer management (Task 11).

use anyhow::{Result, anyhow};
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{
    PeerAddResult, PeerListResult, PeerLocalCertResult, PeerPingResult, PeerRemoveResult,
};

#[derive(Debug, Subcommand)]
pub enum PeerCommand {
    /// Register a peer and attempt the initial handshake. Requires the peer's
    /// TLS cert (from `grim peer cert` on the remote daemon) for mTLS pinning.
    Add {
        name: String,
        /// `https://host:port` of the peer's federation listener.
        #[arg(long)]
        url: String,
        #[arg(long)]
        token: String,
        /// Path to the remote daemon's PEM cert (`grim peer cert > peer.crt`
        /// on the other side), pinned as the TLS trust anchor.
        #[arg(long)]
        cert: std::path::PathBuf,
    },
    /// Print this daemon's federation cert (PEM) + fingerprint, to hand to a
    /// remote operator for pinning when they add us as a peer.
    Cert,
    /// List configured peers.
    List,
    /// Remove a peer (cascades outbox/topic_federations; mail rows retained).
    Remove { name: String },
    /// Ping a peer (returns RTT and stream state).
    Ping { name: String },
}

pub async fn run(cmd: PeerCommand) -> Result<()> {
    match cmd {
        PeerCommand::Add {
            name,
            url,
            token,
            cert,
        } => run_add(&name, &url, &token, &cert).await,
        PeerCommand::Cert => run_cert().await,
        PeerCommand::List => run_list().await,
        PeerCommand::Remove { name } => run_remove(&name).await,
        PeerCommand::Ping { name } => run_ping(&name).await,
    }
}

async fn run_add(name: &str, url: &str, token: &str, cert: &std::path::Path) -> Result<()> {
    let cert_pem = std::fs::read_to_string(cert)
        .map_err(|e| anyhow!("reading peer cert {}: {e}", cert.display()))?;
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "peer.add",
            serde_json::json!({
                "name": name,
                "url": url,
                "bearer_token": token,
                "cert_pem": cert_pem,
            }),
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

async fn run_cert() -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call("peer.local-cert", serde_json::json!({}))
        .await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("peer cert failed: {}", err.message));
    }
    let result: PeerLocalCertResult = serde_json::from_value(resp.result.unwrap_or_default())?;
    // PEM to stdout (pipeable to a file); fingerprint to stderr so it doesn't
    // pollute a redirected cert file.
    eprintln!(
        "{} {}",
        "fingerprint (sha256):".green(),
        result.fingerprint_sha256
    );
    print!("{}", result.cert_pem);
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
            "-"
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

async fn run_remove(name: &str) -> Result<()> {
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

async fn run_ping(name: &str) -> Result<()> {
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
