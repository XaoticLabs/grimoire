#![allow(missing_docs)] // Daemon internals; documentation pass pending per-module.

pub mod agent_lifecycle_publisher;
pub mod agent_manager;
pub mod artifacts;
pub mod clock;
pub mod daemon_id;
pub mod event_bus;
pub mod executor;
pub mod metrics;
pub mod namespace_db;
pub mod notifier;
pub mod observability;
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
pub mod sandbox;
pub mod scheduler;
pub mod scroll_keeper;
pub mod scroll_parser;
pub mod server;
pub mod supervisor;
pub mod wake_registry;
pub mod wake_sources;
pub mod webhook;
pub mod worker_registry;
pub mod worker_rpc_server;
pub mod workspace_db;
pub mod workspace_registry;
pub mod workspace_watcher;

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Heartbeat interval used by the worker registry to probe outbound worker
/// liveness. Workers that miss two consecutive probes are marked stale.
const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_mins(1);

use crate::shared::auth;
use crate::shared::config::Config;
use crate::shared::constants;

pub async fn start() -> Result<()> {
    let config = Config::load()?;
    bootstrap_runtime(&config)?;

    let daemon_id = daemon_id::load_or_mint(&constants::daemon_id_path())?;
    info!(daemon_id = %daemon_id, "daemon id loaded");

    let db = Arc::new(persistence::Database::open(&constants::db_path())?);
    let event_bus = event_bus::EventBus::new(db.clone());
    replay_dormant_migration(&db, &event_bus);

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

    // Scheduler: the daemon's load-bearing tick loop. Without it queued agents
    // never start and Failed agents with a restart policy never resurface.
    // Wired here because all its collaborators are now constructed; the handle
    // binding keeps the task alive for the daemon's lifetime.
    let workers = Arc::new(worker_registry::WorkerRegistry::new_with_bus(
        WORKER_HEARTBEAT_INTERVAL,
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

    // Outbound notifications: bus subscriber fanning matched events to
    // configured sinks. Only spawned when at least one sink is set.
    if config.notifications.has_sink() {
        let cfg = &config.notifications;
        let sinks = [
            cfg.webhook_url.as_ref().map(|_| "webhook"),
            cfg.log_file.as_ref().map(|_| "log"),
            cfg.desktop.then_some("desktop"),
        ];
        let active: Vec<&str> = sinks.into_iter().flatten().collect();
        match notifier::Notifier::new(config.notifications.clone()) {
            Ok(n) => {
                n.start(&event_bus);
                info!(sinks = ?active, "outbound notifications enabled");
            }
            Err(e) => tracing::warn!(error = %e, "notifier init failed; notifications disabled"),
        }
    }

    // Orchestrator: fires pacts on agent completion.
    let orch = orchestrator::Orchestrator::new(db.clone(), manager.clone());
    orch.start(&event_bus);

    // Scroll keeper: advances scroll DAGs on agent completion.
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

    // Federation mTLS identity, shared by outbound peer clients (client cert)
    // and the inbound listener (server cert). Auto-minted + pinned on first
    // boot under <grimoire_dir>/tls/daemon.{crt,key}, or loaded from config.
    let tls_identity = Arc::new(crate::shared::tls::load_or_init(
        "daemon",
        config.daemon.tls_cert_path.as_deref(),
        config.daemon.tls_key_path.as_deref(),
    )?);
    info!(
        fingerprint = %tls_identity.fingerprint(),
        "federation transport identity resolved"
    );

    // Peer registry: outbound peer clients + outbox drainers + inbox dispatch.
    let peer_registry = peer_registry::PeerRegistry::new(
        db.clone(),
        event_bus.clone(),
        clock.clone(),
        daemon_id.clone(),
        tls_identity.clone(),
    );
    if let Err(e) = peer_registry.reconcile_on_boot().await {
        tracing::warn!(error = %e, "peer registry reconcile_on_boot failed");
    }
    peer_registry.spawn_all_active().await;

    // Lets the watcher wake outbox drainers right after enqueueing a federated
    // event instead of waiting on the peer's next heartbeat tick.
    workspace_watcher::set_peer_registry(peer_registry.clone());

    // Lifecycle producer: fans local `StateChange` events into each peer's
    // outbox. Boot recovery: revert in-flight rows so the drainer reships.
    let _ = db.agent_lifecycle_reset_in_flight();
    agent_lifecycle_publisher::spawn(db.clone(), event_bus.clone(), peer_registry.clone());

    // Hand peer_registry to scroll_keeper for outbox-path dispatch. Reset
    // in-flight scroll-dispatch rows so the drainer reships them.
    let _ = db.scroll_dispatch_reset_in_flight();
    scroll_keeper.set_peer_registry(peer_registry.clone()).await;

    if let Some(addr) = config.daemon.peer_listen_addr.clone() {
        spawn_peer_listener(addr, peer_registry.clone(), tls_identity.clone());
    }

    // Worker-pool control plane, bound only when `[worker]` is configured.
    // mTLS with a dedicated "worker" identity, trusting only certs pinned in
    // `trusted_worker_certs`.
    if let Some(worker_cfg) = config.worker.clone() {
        spawn_worker_listener(worker_cfg, workers.clone());
    }

    // CLI/HTTP bearer token (minted on first boot). Governs only local UDS RPC
    // and the dashboard; workers and peers carry their own per-link tokens.
    let auth_token = Arc::new(auth::load_or_init_daemon(
        config.daemon.auth.token.as_deref(),
    )?);
    info!(
        path = %auth::token_path().display(),
        "auth token resolved (file is canonical source for the CLI)"
    );

    let webhooks = Arc::new(config.webhooks.clone());
    let http_port = config.daemon.port;
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
        webhooks,
        http_port,
    )
    .await?;

    cleanup_runtime_files();
    Ok(())
}

/// Create the grimoire dir, write the pid file, and print the boot banner.
fn bootstrap_runtime(config: &Config) -> Result<()> {
    let dir = constants::grimoire_dir();
    std::fs::create_dir_all(&dir)?;

    let pid = std::process::id();
    std::fs::write(constants::pid_path(), pid.to_string())?;

    let socket = config.socket_path();
    let port = config.port();

    // Banner on stderr (not tracing) so it shows in interactive
    // `grim daemon start` regardless of the env filter.
    #[allow(clippy::print_stderr)]
    {
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
    }

    info!(pid = pid, dir = %dir.display(), "Grimoire daemon starting");
    Ok(())
}

/// Promote Complete-with-session agents to Dormant, publishing `StateChange`
/// so the event log and boot-time subscribers see it. Idempotent. MUST run
/// before any subsystem reads agent state, or the boot view is inconsistent.
fn replay_dormant_migration(db: &Arc<persistence::Database>, event_bus: &event_bus::EventBus) {
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
    for id in &migrated_ids {
        event_bus.publish(crate::shared::protocol::StreamEvent::StateChange {
            agent_id: id.clone(),
            old_state: crate::shared::types::AgentState::Complete,
            new_state: crate::shared::types::AgentState::Dormant,
        });
    }
}

/// Spawn the peer-federation gRPC listener. Address/server errors are logged
/// and the daemon continues without federation rather than failing boot.
fn spawn_peer_listener(
    addr: String,
    peer_registry: Arc<peer_registry::PeerRegistry>,
    tls_identity: Arc<crate::shared::tls::Identity>,
) {
    tokio::spawn(async move {
        let Ok(sa) = addr.parse::<std::net::SocketAddr>() else {
            tracing::warn!(addr = %addr, "invalid peer_listen_addr");
            return;
        };

        // mTLS: present our server cert; require inbound peers to present a
        // client cert pinned at `peer add` (self-signed, so trust == equality).
        // With none pinned the listener still binds but rejects every client.
        // Newly-added peers enter the trust bundle only on daemon restart.
        let mut tls = tonic::transport::ServerTlsConfig::new().identity(tls_identity.to_tonic());
        let bundle = peer_registry
            .db
            .list_peers()
            .unwrap_or_default()
            .iter()
            .filter_map(crate::shared::types::Peer::pinned_cert_pem)
            .collect::<Vec<_>>()
            .join("\n");
        if bundle.is_empty() {
            tracing::info!(
                "peer listener: no pinned peer certs yet; inbound federation \
                 stays closed until a peer is added and the daemon restarts"
            );
        } else {
            tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(bundle.as_bytes()));
        }

        let svc = peer_rpc_server::PeerSvc::new(peer_registry);
        let mut server = match tonic::transport::Server::builder().tls_config(tls) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "peer listener TLS config invalid");
                return;
            }
        };
        if let Err(e) = server.add_service(svc).serve(sa).await {
            tracing::error!(error = %e, "peer gRPC listener exited");
        }
    });
}

/// Spawn the mTLS worker control listener. Cert/load failures are logged and
/// the worker pool stays down rather than failing daemon boot.
fn spawn_worker_listener(
    cfg: crate::shared::config::WorkerConfig,
    workers: Arc<worker_registry::WorkerRegistry>,
) {
    let identity = match crate::shared::tls::load_or_init(
        "worker",
        cfg.tls_cert_path.as_deref(),
        cfg.tls_key_path.as_deref(),
    ) {
        Ok(id) => Arc::new(id),
        Err(e) => {
            tracing::error!(error = %e, "worker listener disabled: identity load failed");
            return;
        }
    };

    // Pinned worker certs into one client-CA bundle; unreadable paths skipped.
    let mut certs = Vec::new();
    for path in &cfg.trusted_worker_certs {
        match std::fs::read_to_string(path) {
            Ok(pem) => certs.push(pem),
            Err(e) => tracing::warn!(path = %path.display(), error = %e,
                "skipping unreadable trusted_worker_cert"),
        }
    }

    info!(
        addr = %cfg.listen_addr,
        fingerprint = %identity.fingerprint(),
        trusted_workers = certs.len(),
        "worker control listener starting (mTLS)"
    );
    worker_rpc_server::spawn(
        cfg.listen_addr,
        workers,
        cfg.secret,
        identity,
        certs.join("\n"),
    );
}

/// Remove the socket + pid files. Best-effort: failures here mean the next
/// daemon start will overwrite them anyway.
fn cleanup_runtime_files() {
    let _ = std::fs::remove_file(constants::socket_path());
    let _ = std::fs::remove_file(constants::pid_path());
}
