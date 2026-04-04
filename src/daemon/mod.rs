pub mod agent_manager;
pub mod event_bus;
pub mod persistence;
pub mod process_manager;
pub mod rpc;
pub mod server;

use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use crate::shared::constants;

pub async fn start() -> Result<()> {
    // Ensure grimoire directory exists
    let dir = constants::grimoire_dir();
    std::fs::create_dir_all(&dir)?;

    // Write PID file
    let pid = std::process::id();
    std::fs::write(constants::pid_path(), pid.to_string())?;

    info!(pid = pid, dir = %dir.display(), "Grimoire daemon starting");

    // Open database
    let db = Arc::new(persistence::Database::open(&constants::db_path())?);

    // Create event bus
    let event_bus = event_bus::EventBus::new();

    // Create agent manager
    let manager = agent_manager::AgentManager::new(db, event_bus).await;

    // Start servers (UDS + HTTP)
    server::run(manager).await?;

    // Cleanup
    let _ = std::fs::remove_file(constants::socket_path());
    let _ = std::fs::remove_file(constants::pid_path());

    Ok(())
}
