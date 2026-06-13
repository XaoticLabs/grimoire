use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use semver::Version;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use crate::shared::worker_proto::{
    DaemonMessage, WorkerMessage, worker_control_server::WorkerControl, worker_message,
};

use super::worker_registry::{RegisterParams, WorkerRegistry, worker_version_meets_minimum};

/// Settle delay so the registry observes a just-registered worker.
const REGISTRY_SETTLE_YIELD: Duration = Duration::from_millis(50);

/// Routes inbound TaskEvent/TaskFinished/TaskAccepted/TaskRejected back to a
/// per-agent channel registered by RemoteExecutor.
pub type RoutingMap = Arc<Mutex<std::collections::HashMap<String, mpsc::Sender<WorkerMessage>>>>;

pub struct WorkerControlService {
    registry: Arc<WorkerRegistry>,
    bearer_secret: String,
    routing: RoutingMap,
}

#[tonic::async_trait]
impl WorkerControl for WorkerControlService {
    type ChannelStream =
        Pin<Box<dyn Stream<Item = Result<DaemonMessage, Status>> + Send + 'static>>;

    // Err type is fixed by the generated trait; the large `Status` can't be boxed.
    #[allow(clippy::result_large_err)]
    async fn channel(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::ChannelStream>, Status> {
        let mut inbound = request.into_inner();

        // First message must be Register.
        let first = inbound
            .message()
            .await
            .map_err(|e| Status::aborted(format!("read register: {e}")))?;
        let Some(WorkerMessage {
            kind: Some(worker_message::Kind::Register(register)),
        }) = first
        else {
            return Err(Status::invalid_argument("first message must be Register"));
        };

        if register.protocol_version != crate::shared::constants::WORKER_PROTOCOL_VERSION {
            return Err(Status::failed_precondition(format!(
                "unsupported_protocol_version: worker sent {}, daemon supports {}",
                register.protocol_version,
                crate::shared::constants::WORKER_PROTOCOL_VERSION,
            )));
        }
        if register.bearer_token != self.bearer_secret {
            return Err(Status::unauthenticated("invalid bearer token"));
        }
        if !worker_version_meets_minimum(&register.worker_version) {
            return Err(Status::failed_precondition(format!(
                "worker version {} below minimum",
                register.worker_version
            )));
        }

        let providers: Vec<(String, Version)> = register
            .providers
            .iter()
            .filter_map(|p| Version::parse(&p.version).ok().map(|v| (p.name.clone(), v)))
            .collect();

        let (assign_tx, assign_rx) = mpsc::channel::<DaemonMessage>(64);

        self.registry
            .register(RegisterParams {
                worker_id: register.worker_id.clone(),
                bearer_ok: true,
                worker_version: register.worker_version,
                max_concurrent: register.max_concurrent,
                providers,
                tags: register.tags,
                assign_tx,
            })
            .map_err(|e| Status::already_exists(format!("{e}")))?;

        info!(worker_id = %register.worker_id, "worker registered");

        let registry_for_drop = self.registry.clone();
        let worker_id_for_drop = register.worker_id;
        let routing = self.routing.clone();

        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.kind {
                    Some(worker_message::Kind::Heartbeat(hb)) => {
                        registry_for_drop.record_heartbeat(&worker_id_for_drop, hb.in_flight);
                    }
                    Some(worker_message::Kind::TaskAccepted(ref t)) => {
                        Self::route(&routing, &t.agent_id, msg.clone()).await;
                    }
                    Some(worker_message::Kind::TaskRejected(ref t)) => {
                        Self::route(&routing, &t.agent_id, msg.clone()).await;
                    }
                    Some(worker_message::Kind::TaskEvent(ref t)) => {
                        Self::route(&routing, &t.agent_id, msg.clone()).await;
                    }
                    Some(worker_message::Kind::TaskFinished(ref t)) => {
                        Self::route(&routing, &t.agent_id, msg.clone()).await;
                    }
                    _ => {}
                }
            }
            warn!(worker_id = %worker_id_for_drop, "worker stream closed; evicting");
            registry_for_drop.evict(&worker_id_for_drop);
        });

        let outbound = ReceiverStream::new(assign_rx).map(Ok);
        let stream: Self::ChannelStream = Box::pin(tokio_stream::StreamExt::map(
            outbound,
            |r: Result<DaemonMessage, Status>| r,
        ));
        Ok(Response::new(stream))
    }
}

impl WorkerControlService {
    /// `routing` is shared with `RemoteExecutor` to route task events back to
    /// the originating agent; the register/heartbeat/eviction path is independent.
    pub const fn new(
        registry: Arc<WorkerRegistry>,
        bearer_secret: String,
        routing: RoutingMap,
    ) -> Self {
        Self {
            registry,
            bearer_secret,
            routing,
        }
    }

    async fn route(routing: &RoutingMap, agent_id: &str, msg: WorkerMessage) {
        let map = routing.lock().await;
        if let Some(tx) = map.get(agent_id) {
            let _ = tx.send(msg).await;
        }
    }
}

/// Spawn the production worker-control gRPC listener with mTLS. The daemon
/// presents `identity` as its server cert; only workers presenting a client
/// cert in `trusted_certs_bundle` (concatenated PEM) complete the handshake.
/// With an empty bundle the listener binds but no worker can register.
pub fn spawn(
    addr: std::net::SocketAddr,
    registry: Arc<WorkerRegistry>,
    bearer_secret: String,
    identity: Arc<crate::shared::tls::Identity>,
    trusted_certs_bundle: String,
) {
    use tonic::transport::{Certificate, Server, ServerTlsConfig};

    let routing: RoutingMap = Arc::new(Mutex::new(std::collections::HashMap::default()));
    let svc = WorkerControlService::new(registry, bearer_secret, routing);

    let mut tls = ServerTlsConfig::new().identity(identity.to_tonic());
    if trusted_certs_bundle.is_empty() {
        warn!(
            "worker listener: no trusted_worker_certs configured; no worker can \
             register until one is pinned and the daemon restarts"
        );
    } else {
        tls = tls.client_ca_root(Certificate::from_pem(trusted_certs_bundle.as_bytes()));
    }

    tokio::spawn(async move {
        let mut server = match Server::builder().tls_config(tls) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "worker listener TLS config invalid");
                return;
            }
        };
        if let Err(e) = server
            .add_service(
                crate::shared::worker_proto::worker_control_server::WorkerControlServer::new(svc),
            )
            .serve(addr)
            .await
        {
            warn!(error = %e, "worker gRPC listener exited");
        }
    });
}

pub mod test_helpers {
    use super::*;
    use tonic::transport::Server;

    pub struct TestServerHandle {
        pub addr: std::net::SocketAddr,
        pub registry: Arc<WorkerRegistry>,
        pub routing: RoutingMap,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl TestServerHandle {
        pub async fn shutdown(mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
        }
    }

    pub async fn spawn_test_server(
        registry: Arc<WorkerRegistry>,
        bearer_secret: &str,
    ) -> TestServerHandle {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test setup: bind ephemeral port");
        let addr = listener.local_addr().expect("test setup: query bound addr");
        let routing: RoutingMap = Arc::new(Mutex::new(std::collections::HashMap::default()));
        let svc = WorkerControlService {
            registry: registry.clone(),
            bearer_secret: bearer_secret.to_string(),
            routing: routing.clone(),
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(
                    crate::shared::worker_proto::worker_control_server::WorkerControlServer::new(
                        svc,
                    ),
                )
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });
        tokio::time::sleep(REGISTRY_SETTLE_YIELD).await;
        TestServerHandle {
            addr,
            registry,
            routing,
            shutdown: Some(shutdown_tx),
        }
    }
}
