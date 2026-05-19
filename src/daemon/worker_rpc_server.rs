use std::pin::Pin;
use std::sync::Arc;

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

/// Routes inbound TaskEvent/TaskFinished/TaskAccepted/TaskRejected back to a
/// per-agent channel registered by RemoteExecutor (Task 6).
pub type RoutingMap = Arc<Mutex<std::collections::HashMap<String, mpsc::Sender<WorkerMessage>>>>;

pub struct WorkerControlService {
    pub registry: Arc<WorkerRegistry>,
    pub bearer_secret: String,
    pub routing: RoutingMap,
}

#[tonic::async_trait]
impl WorkerControl for WorkerControlService {
    type ChannelStream =
        Pin<Box<dyn Stream<Item = Result<DaemonMessage, Status>> + Send + 'static>>;

    // `tonic::Status` is large but the trait fixes the Err type — boxing here would
    // change the API generated from the proto definition.
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
            .map_err(|e| Status::aborted(format!("read register: {}", e)))?;
        let register = match first {
            Some(WorkerMessage {
                kind: Some(worker_message::Kind::Register(r)),
            }) => r,
            _ => return Err(Status::invalid_argument("first message must be Register")),
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
            .map_err(|e| Status::already_exists(format!("{}", e)))?;

        info!(worker_id = %register.worker_id, "worker registered");

        let registry_for_drop = self.registry.clone();
        let worker_id_for_drop = register.worker_id.clone();
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
    async fn route(routing: &RoutingMap, agent_id: &str, msg: WorkerMessage) {
        let map = routing.lock().await;
        if let Some(tx) = map.get(agent_id) {
            let _ = tx.send(msg).await;
        }
    }
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let routing: RoutingMap = Arc::new(Mutex::new(Default::default()));
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
        // Brief settle.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        TestServerHandle {
            addr,
            registry,
            routing,
            shutdown: Some(shutdown_tx),
        }
    }
}
