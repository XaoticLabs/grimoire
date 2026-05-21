#![cfg_attr(not(test), forbid(unsafe_code))]

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser)]
#[command(name = "grimw", about = "Grimoire worker")]
struct Cli {
    /// Path to grimw.toml.
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();
    let config = grimoire::grimw::config::GrimwConfig::load(&args.config)
        .with_context(|| format!("load config from {}", args.config.display()))?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let signal_task = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(());
    });

    grimoire::grimw::rpc_client::run(config, shutdown_rx).await?;
    signal_task.abort();
    Ok(())
}
