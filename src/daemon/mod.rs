pub mod agent_manager;
pub mod event_bus;
pub mod executor;
pub mod worker_registry;
pub mod worker_rpc_server;
pub mod orchestrator;
pub mod persistence;
pub mod process_manager;
pub mod provider;
pub mod provider_registry;
pub mod providers;
pub mod rpc;
pub mod scheduler;
pub mod scroll_keeper;
pub mod scroll_parser;
pub mod server;

use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use crate::shared::config::Config;
use crate::shared::constants;

pub async fn start() -> Result<()> {
    let config = Config::load()?;

    let dir = constants::grimoire_dir();
    std::fs::create_dir_all(&dir)?;

    let pid = std::process::id();
    std::fs::write(constants::pid_path(), pid.to_string())?;

    let socket = config.socket_path();
    let port = config.port();

    eprintln!();
    eprintln!("  ◆ grimoire daemon v{}", env!("CARGO_PKG_VERSION"));
    eprintln!(
        "    pid {}  ·  socket {}  ·  http 127.0.0.1:{}",
        pid,
        socket.display(),
        port
    );
    eprintln!("    db {}", constants::db_path().display());
    eprintln!();

    info!(pid = pid, dir = %dir.display(), "Grimoire daemon starting");

    let db = Arc::new(persistence::Database::open(&constants::db_path())?);
    let event_bus = event_bus::EventBus::new(db.clone());
    let manager = agent_manager::AgentManager::new(db.clone(), event_bus.clone(), config).await;

    // Start orchestrator (listens for agent completions, fires pacts)
    let orch = orchestrator::Orchestrator::new(db.clone(), manager.clone());
    orch.start(&event_bus);

    // Start scroll keeper (listens for agent completions, manages scroll DAGs)
    let scroll_keeper = Arc::new(scroll_keeper::ScrollKeeper::new(db.clone(), manager.clone()));
    scroll_keeper.clone().start(&event_bus);

    // Start servers (UDS + HTTP)
    server::run(manager, db, scroll_keeper).await?;

    let _ = std::fs::remove_file(constants::socket_path());
    let _ = std::fs::remove_file(constants::pid_path());

    Ok(())
}
