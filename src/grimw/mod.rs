pub mod config;
pub mod rpc_client;
pub mod task_runner;

use std::path::Path;
use std::sync::Arc;

use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;

use config::GrimwConfig;

pub struct TestWorker {
    shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl TestWorker {
    pub async fn shutdown(self) {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.handle.lock().await.take() {
            let _ = h.await;
        }
    }

    pub async fn send_sigterm(&self) {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
    }

    pub async fn has_exited(&self) -> bool {
        let guard = self.handle.lock().await;
        match guard.as_ref() {
            Some(h) => h.is_finished(),
            None => true,
        }
    }
}

pub async fn test_spawn(config_path: &Path) -> TestWorker {
    let config = GrimwConfig::load(config_path).expect("load grimw config");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        if let Err(e) = rpc_client::run(config, shutdown_rx).await {
            tracing::error!(error = %e, "grimw run failed");
        }
    });
    TestWorker {
        shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
        handle: Arc::new(Mutex::new(Some(handle))),
    }
}
