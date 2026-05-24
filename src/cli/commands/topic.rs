//! `grim topic` — federate / unfederate topics across peers (Task 12).

use anyhow::{Result, anyhow};
use clap::Subcommand;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{TopicFederateResult, TopicFederationsResult, TopicUnfederateResult};

#[derive(Debug, Subcommand)]
pub enum TopicCommand {
    /// Federate a topic with a peer. Direction: inbound | outbound | both.
    Federate {
        topic: String,
        #[arg(long)]
        peer: String,
        #[arg(long, default_value = "outbound")]
        direction: String,
    },
    /// Remove a topic federation row. Run on each side independently.
    Unfederate {
        topic: String,
        #[arg(long)]
        peer: String,
    },
    /// List every active topic federation on this daemon.
    Federations,
}

pub async fn run(cmd: TopicCommand) -> Result<()> {
    match cmd {
        TopicCommand::Federate {
            topic,
            peer,
            direction,
        } => run_federate(&topic, &peer, &direction).await,
        TopicCommand::Unfederate { topic, peer } => run_unfederate(&topic, &peer).await,
        TopicCommand::Federations => run_federations().await,
    }
}

async fn run_federations() -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client.call("topic.federations", serde_json::json!({})).await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("topic federations failed: {}", err.message));
    }
    let result: TopicFederationsResult = serde_json::from_value(resp.result.unwrap_or_default())?;
    if result.federations.is_empty() {
        println!("no topic federations");
        return Ok(());
    }
    println!("{:<22} {:<24} {:<10} CREATED", "TOPIC", "PEER", "DIRECTION");
    for f in result.federations {
        println!(
            "{:<22} {:<24} {:<10} {}",
            f.topic,
            f.peer_id,
            f.direction.as_str(),
            f.created_at
        );
    }
    Ok(())
}

async fn run_federate(topic: &str, peer: &str, direction: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "topic.federate",
            serde_json::json!({ "topic": topic, "peer": peer, "direction": direction }),
        )
        .await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("topic federate failed: {}", err.message));
    }
    let result: TopicFederateResult = serde_json::from_value(resp.result.unwrap_or_default())?;
    println!(
        "Federated topic {} with peer {} (direction={}). Run on both daemons to make traffic flow both ways.",
        result.topic, peer, result.direction
    );
    Ok(())
}

async fn run_unfederate(topic: &str, peer: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "topic.unfederate",
            serde_json::json!({ "topic": topic, "peer": peer }),
        )
        .await?;
    if let Some(err) = resp.error {
        return Err(anyhow!("topic unfederate failed: {}", err.message));
    }
    let result: TopicUnfederateResult = serde_json::from_value(resp.result.unwrap_or_default())?;
    if result.removed {
        println!("Unfederated topic {topic} from peer {peer}");
    } else {
        println!("No federation row found for {topic}/{peer}");
    }
    Ok(())
}
