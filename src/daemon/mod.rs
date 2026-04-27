pub mod agent_manager;
pub mod clock;
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
pub mod supervisor;
pub mod wake_registry;
pub mod wake_sources;

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

    // Promote Complete-with-session agents to Dormant before any subsystem
    // (scheduler, wake registry) starts looking at agent state. Idempotent.
    let migrated_ids = match db.migrate_dormant_agents() {
        Ok(ids) => {
            if !ids.is_empty() {
                info!(count = ids.len(), "migrated agents from complete to dormant");
            }
            ids
        }
        Err(e) => {
            tracing::error!(error = %e, "dormant migration failed");
            Vec::new()
        }
    };

    let event_bus = event_bus::EventBus::new(db.clone());

    // Publish a StateChange for each migration on the live bus so the event
    // log captures the transition and any subscriber attached during boot
    // sees it. The bus's writer task persists each event to the events table.
    for id in &migrated_ids {
        event_bus.publish(crate::shared::protocol::StreamEvent::StateChange {
            agent_id: id.clone(),
            old_state: crate::shared::types::AgentState::Complete,
            new_state: crate::shared::types::AgentState::Dormant,
        });
    }

    let manager = agent_manager::AgentManager::new(db.clone(), event_bus.clone(), config.clone()).await;

    // Wake registry (cron / file-watch / parent-completion sources).
    let clock: Arc<dyn clock::Clock> = Arc::new(clock::SystemClock);
    let wake_registry = wake_registry::WakeRegistry::with_default_sender(
        db.clone(),
        event_bus.clone(),
        clock.clone(),
    );
    if let Err(e) = wake_registry.replay_on_boot().await {
        tracing::warn!(error = %e, "wake registry replay_on_boot failed");
    }
    wake_registry.spawn();
    manager.set_wake_registry(wake_registry.clone()).await;

    // Supervisor (restart policy + escalation).
    let supervisor = supervisor::Supervisor::with_default_sender(
        db.clone(),
        event_bus.clone(),
        clock.clone(),
        config.daemon.restart_rate_per_min,
        config.daemon.tree_depth_cap,
    );
    if let Err(e) = supervisor.replay_pending_on_boot().await {
        tracing::warn!(error = %e, "supervisor replay_pending_on_boot failed");
    }
    manager.set_supervisor(supervisor.clone()).await;
    let _supervisor_task = supervisor.spawn();

    // Start orchestrator (listens for agent completions, fires pacts)
    let orch = orchestrator::Orchestrator::new(db.clone(), manager.clone());
    orch.start(&event_bus);

    // Start scroll keeper (listens for agent completions, manages scroll DAGs)
    let scroll_keeper = Arc::new(scroll_keeper::ScrollKeeper::new(db.clone(), manager.clone()));
    scroll_keeper.clone().start(&event_bus);

    // Start servers (UDS + HTTP)
    server::run(manager, db, scroll_keeper, wake_registry, supervisor).await?;

    let _ = std::fs::remove_file(constants::socket_path());
    let _ = std::fs::remove_file(constants::pid_path());

    Ok(())
}
