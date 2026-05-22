use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn};

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use crate::shared::worker_proto::{
    Heartbeat, ProviderCap, Register, WorkerMessage, daemon_message,
    worker_control_client::WorkerControlClient, worker_message,
};

/// Build the mTLS channel to the daemon's worker listener: present this
/// worker's identity as the client cert and pin the daemon's cert as the sole
/// trust anchor. The `domain_name` matches the constant SAN in every grimoire
/// identity cert.
async fn connect_daemon(config: &GrimwConfig) -> Result<Channel> {
    if !config.daemon_url.starts_with("https://") {
        return Err(anyhow::anyhow!(
            "daemon_url must be https:// (worker link is mTLS); got {}",
            config.daemon_url
        ));
    }

    let daemon_cert = std::fs::read_to_string(&config.daemon_cert_path)
        .with_context(|| format!("reading daemon cert {}", config.daemon_cert_path.display()))?;
    if !config.daemon_cert_sha256.is_empty() {
        let actual = crate::shared::tls::cert_fingerprint(&daemon_cert);
        if actual != config.daemon_cert_sha256.to_lowercase() {
            return Err(anyhow::anyhow!(
                "pinned daemon cert fingerprint mismatch: config has {}, file is {actual}",
                config.daemon_cert_sha256
            ));
        }
    }

    let identity = crate::shared::tls::load_or_init(
        "worker",
        config.tls_cert_path.as_deref(),
        config.tls_key_path.as_deref(),
    )?;

    let tls = ClientTlsConfig::new()
        .identity(identity.to_tonic())
        .ca_certificate(Certificate::from_pem(daemon_cert.as_bytes()))
        .domain_name(crate::shared::tls::TLS_SAN);

    let channel = Endpoint::from_shared(config.daemon_url.clone())?
        .tls_config(tls)?
        .connect()
        .await
        .with_context(|| format!("connect daemon at {}", config.daemon_url))?;
    Ok(channel)
}

/// How long the worker waits for its inbound stream to wake before
/// declaring the daemon link broken and entering reconnect.
const STREAM_DEADLINE: Duration = Duration::from_secs(5);

/// Backoff yield between stream-recv retry attempts inside the deadline.
const STREAM_RETRY_YIELD: Duration = Duration::from_millis(50);

use super::config::GrimwConfig;
use super::task_runner::TaskDispatcher;

pub const HEARTBEAT_INTERVAL_SECS: u64 = 5;
pub const WORKER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn run(
    config: GrimwConfig,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    let worker_id = config
        .worker_id
        .clone()
        .unwrap_or_else(|| format!("w-{}", uuid::Uuid::new_v4().simple()));

    let providers: Vec<ProviderCap> = config
        .providers
        .iter()
        .map(|(name, cfg)| ProviderCap {
            name: name.clone(),
            version: cfg.version.clone(),
        })
        .collect();

    let dispatcher = TaskDispatcher::new(config.providers.clone(), config.max_concurrent);

    let channel = connect_daemon(&config).await?;
    let mut client = WorkerControlClient::new(channel);

    let (tx, rx) = mpsc::channel::<WorkerMessage>(64);

    // Send Register first.
    tx.send(WorkerMessage {
        kind: Some(worker_message::Kind::Register(Register {
            worker_id: worker_id.clone(),
            bearer_token: config.secret.clone(),
            worker_version: WORKER_VERSION.to_string(),
            max_concurrent: config.max_concurrent,
            providers,
            tags: config.tags.clone(),
            protocol_version: crate::shared::constants::WORKER_PROTOCOL_VERSION,
        })),
    })
    .await
    .ok();

    let outbound = ReceiverStream::new(rx);
    let response = client
        .channel(outbound)
        .await
        .context("open WorkerControl channel")?;
    let mut inbound = response.into_inner();

    info!(worker_id = %worker_id, "worker registered");

    // Heartbeat loop
    let hb_tx = tx.clone();
    let hb_dispatcher = dispatcher.clone();
    let hb_handle = tokio::spawn(async move {
        let mut seq: u64 = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
        // First tick fires immediately; skip it so the daemon receives the
        // first Register before any Heartbeat.
        interval.tick().await;
        loop {
            interval.tick().await;
            seq += 1;
            if hb_tx
                .send(WorkerMessage {
                    kind: Some(worker_message::Kind::Heartbeat(Heartbeat {
                        in_flight: hb_dispatcher.in_flight(),
                        seq,
                    })),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Inbound loop
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                info!("shutdown requested; draining");
                dispatcher.drain();
                // Wait briefly for in-flight tasks to finish; bound to 5s.
                let deadline = tokio::time::Instant::now() + STREAM_DEADLINE;
                while dispatcher.in_flight() > 0 && tokio::time::Instant::now() < deadline {
                    tokio::time::sleep(STREAM_RETRY_YIELD).await;
                }
                break;
            }
            msg = inbound.message() => {
                match msg {
                    Ok(Some(daemon_msg)) => {
                        match daemon_msg.kind {
                            Some(daemon_message::Kind::AssignTask(assign)) => {
                                let dispatcher = dispatcher.clone();
                                let outbound = tx.clone();
                                tokio::spawn(async move {
                                    dispatcher.handle_assign(assign, outbound).await;
                                });
                            }
                            Some(daemon_message::Kind::CancelTask(cancel)) => {
                                dispatcher.cancel(&cancel.agent_id).await;
                            }
                            Some(daemon_message::Kind::Ping(_) | daemon_message::Kind::Ack(_))
                            | None => {}
                        }
                    }
                    Ok(None) => {
                        warn!("daemon closed channel");
                        break;
                    }
                    Err(e) => {
                        warn!(error = %e, "daemon channel error");
                        break;
                    }
                }
            }
        }
    }

    hb_handle.abort();
    Ok(())
}
