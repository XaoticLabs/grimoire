// Test-only helper. Spins a tonic gRPC server implementing the WorkerControl
// service against `127.0.0.1:0`, captures the worker's bidi stream, and
// exposes a queue of received `WorkerMessage`s plus a sender for outbound
// `DaemonMessage`s. Used by `tests/grimw_integration.rs`.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use grimoire::shared::worker_proto::{
    DaemonMessage, WorkerMessage,
    worker_control_server::{WorkerControl, WorkerControlServer},
};
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};

pub struct FakeDaemon {
    pub received: Arc<Mutex<Vec<WorkerMessage>>>,
    pub to_worker: mpsc::Sender<DaemonMessage>,
    pub addr: String,
    pub config_path: PathBuf,
    _shutdown_tx: tokio::sync::oneshot::Sender<()>,
    _tempdir: tempfile::TempDir,
}

struct Service {
    received: Arc<Mutex<Vec<WorkerMessage>>>,
    to_worker_rx: Arc<Mutex<Option<mpsc::Receiver<DaemonMessage>>>>,
}

#[tonic::async_trait]
impl WorkerControl for Service {
    type ChannelStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<DaemonMessage, Status>> + Send + 'static>>;

    async fn channel(
        &self,
        request: Request<Streaming<WorkerMessage>>,
    ) -> Result<Response<Self::ChannelStream>, Status> {
        let mut inbound = request.into_inner();
        let received = self.received.clone();
        let to_worker_rx = self.to_worker_rx.lock().await.take();

        // Reader task: stuff every WorkerMessage we get into `received`.
        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                received.lock().await.push(msg);
            }
        });

        // Writer stream: forward DaemonMessages from the channel.
        let (tx, rx) = mpsc::channel::<Result<DaemonMessage, Status>>(64);
        if let Some(mut rx_in) = to_worker_rx {
            tokio::spawn(async move {
                while let Some(m) = rx_in.recv().await {
                    if tx.send(Ok(m)).await.is_err() {
                        break;
                    }
                }
            });
        }

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::ChannelStream))
    }
}

impl FakeDaemon {
    pub async fn start_with_provider(provider: &str, version: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let addr_str = format!("http://{local_addr}");

        let received = Arc::new(Mutex::new(Vec::new()));
        let (to_worker, to_worker_rx) = mpsc::channel::<DaemonMessage>(64);
        let to_worker_rx = Arc::new(Mutex::new(Some(to_worker_rx)));

        let svc = Service {
            received: received.clone(),
            to_worker_rx,
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(WorkerControlServer::new(svc))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Write a temp grimw.toml.
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("grimw.toml");
        let toml = format!(
            r#"
daemon_url = "{addr_str}"
secret = "test-secret"
worker_id = "w-test"
max_concurrent = 4
tags = []

[providers.{provider}]
binary = "sh"
args_template = ["-c", "{{task}}"]
version = "{version}"
"#,
        );
        std::fs::write(&config_path, toml).unwrap();

        // Brief settle so the server is ready to accept.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        Self {
            received,
            to_worker,
            addr: addr_str,
            config_path,
            _shutdown_tx: shutdown_tx,
            _tempdir: tempdir,
        }
    }

    pub async fn next_message(&self, kind: &'static str) -> WorkerMessage {
        use grimoire::shared::worker_proto::worker_message;
        use std::time::{Duration, Instant};
        let started = Instant::now();
        let mut last_seen_idx = 0usize;
        loop {
            {
                let received = self.received.lock().await;
                for (i, m) in received.iter().enumerate().skip(last_seen_idx) {
                    let matches = matches!(
                        (&m.kind, kind),
                        (Some(worker_message::Kind::Register(_)), "register")
                            | (Some(worker_message::Kind::Heartbeat(_)), "heartbeat")
                            | (Some(worker_message::Kind::TaskAccepted(_)), "task_accepted")
                            | (Some(worker_message::Kind::TaskRejected(_)), "task_rejected")
                            | (Some(worker_message::Kind::TaskEvent(_)), "task_event")
                            | (Some(worker_message::Kind::TaskFinished(_)), "task_finished")
                    );
                    if matches {
                        return m.clone();
                    }
                    last_seen_idx = i + 1;
                }
            }
            assert!(
                started.elapsed() <= Duration::from_secs(10),
                "timed out waiting for {kind}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
