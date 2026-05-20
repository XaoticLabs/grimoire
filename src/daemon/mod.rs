pub mod agent_manager;
pub mod clock;
pub mod daemon_id;
pub mod event_bus;
pub mod executor;
pub mod orchestrator;
pub mod peer_client;
pub mod peer_inbox;
pub mod peer_outbox;
pub mod peer_registry;
pub mod peer_rpc_server;
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
pub mod worker_registry;
pub mod worker_rpc_server;
pub mod workspace_db;
pub mod workspace_registry;
pub mod workspace_watcher;

use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use crate::shared::auth;
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

    let daemon_id = daemon_id::load_or_mint(&constants::daemon_id_path())?;
    info!(daemon_id = %daemon_id, "daemon id loaded");

    let db = Arc::new(persistence::Database::open(&constants::db_path())?);

    // Promote Complete-with-session agents to Dormant before any subsystem
    // (scheduler, wake registry) starts looking at agent state. Idempotent.
    let migrated_ids = match db.migrate_dormant_agents() {
        Ok(ids) => {
            if !ids.is_empty() {
                info!(
                    count = ids.len(),
                    "migrated agents from complete to dormant"
                );
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

    let manager =
        agent_manager::AgentManager::new(db.clone(), event_bus.clone(), config.clone()).await;

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

    // Scheduler — promotes Queued agents to Active per global capacity and
    // dispatches supervised restarts when they come due. The daemon's
    // single load-bearing tick loop: without it, `grim summon` would queue
    // a row that never starts, and `Failed` agents with `--restart on_failure`
    // would never resurface. Wired here (rather than per-test) because all
    // its collaborators (AgentManager as Dispatcher/MailWaker/RestartDispatcher,
    // WorkerRegistry, Supervisor) are now constructed. Handle is held in the
    // local binding so the background task lives for the daemon's lifetime.
    let workers = Arc::new(worker_registry::WorkerRegistry::new_with_bus(
        std::time::Duration::from_mins(1),
        event_bus.clone(),
    ));
    let cap = config.max_concurrent_atomic();
    let state_lookup: Arc<dyn scheduler::AgentStateLookup> =
        Arc::new(scheduler::DbStateLookup { db: db.clone() });
    let local_providers = provider_registry::ProviderRegistry::from_config(&config).list();
    let scheduler_obj = scheduler::Scheduler::new(
        db.clone(),
        workers.clone(),
        event_bus.clone(),
        cap,
        manager.clone() as Arc<dyn scheduler::Dispatcher>,
    )
    .with_local_providers(local_providers)
    .with_mail_wake(
        manager.clone() as Arc<dyn scheduler::MailWaker>,
        state_lookup,
    )
    .with_supervision(
        supervisor.clone(),
        manager.clone() as Arc<dyn supervisor::RestartDispatcher>,
    );
    let _scheduler_handle = Arc::new(scheduler_obj).spawn();

    // Start orchestrator (listens for agent completions, fires pacts)
    let orch = orchestrator::Orchestrator::new(db.clone(), manager.clone());
    orch.start(&event_bus);

    // Start scroll keeper (listens for agent completions, manages scroll DAGs)
    let scroll_keeper = Arc::new(scroll_keeper::ScrollKeeper::new(
        db.clone(),
        manager.clone(),
    ));
    scroll_keeper.clone().start(&event_bus);

    // Workspace registry (workspaces + memory KV + filewatch).
    let workspace_registry =
        workspace_registry::WorkspaceRegistry::with_default_git(db.clone(), event_bus.clone());
    if let Err(e) = workspace_registry.reconcile_on_boot().await {
        tracing::warn!(error = %e, "workspace registry reconcile failed");
    }

    // Federation peer registry. Per spec Slice 2: this hosts outbound peer
    // clients + outbox drainers + dispatch into the inbox handler.
    let peer_registry = peer_registry::PeerRegistry::new(
        db.clone(),
        event_bus.clone(),
        clock.clone(),
        daemon_id.clone(),
    );
    if let Err(e) = peer_registry.reconcile_on_boot().await {
        tracing::warn!(error = %e, "peer registry reconcile_on_boot failed");
    }
    peer_registry.spawn_all_active().await;

    // Spawn the peer gRPC listener if configured.
    if let Some(addr) = config.daemon.peer_listen_addr.clone() {
        let registry_clone = peer_registry.clone();
        tokio::spawn(async move {
            match addr.parse::<std::net::SocketAddr>() {
                Ok(sa) => {
                    let svc = peer_rpc_server::PeerSvc::new(registry_clone);
                    if let Err(e) = tonic::transport::Server::builder()
                        .add_service(svc)
                        .serve(sa)
                        .await
                    {
                        tracing::error!(error = %e, "peer gRPC listener exited");
                    }
                }
                Err(e) => tracing::warn!(addr = %addr, error = %e, "invalid peer_listen_addr"),
            }
        });
    }

    // Resolve (and on first boot mint) the CLI/HTTP bearer token. Workers
    // and peers carry their own per-link tokens; this one only governs the
    // local UDS RPC and the dashboard.
    let auth_token = Arc::new(auth::load_or_init_daemon(
        config.daemon.auth.token.as_deref(),
    )?);
    info!(
        path = %auth::token_path().display(),
        "auth token resolved (file is canonical source for the CLI)"
    );

    // Start servers (UDS + HTTP)
    server::run(
        manager,
        db,
        scroll_keeper,
        wake_registry,
        workspace_registry,
        supervisor,
        peer_registry,
        daemon_id,
        auth_token,
    )
    .await?;

    let _ = std::fs::remove_file(constants::socket_path());
    let _ = std::fs::remove_file(constants::pid_path());

    Ok(())
}
