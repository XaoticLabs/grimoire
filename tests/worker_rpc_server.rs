// RED tests for worker-pool spec, Task 5: daemon-side worker RPC server.
//
// References `worker_rpc_server` startup helpers and proto types from Task 3.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;
use tonic::Code;

use grimoire::daemon::worker_registry::WorkerRegistry;
use grimoire::daemon::worker_rpc_server::test_helpers::{TestServerHandle, spawn_test_server};
use grimoire::shared::worker_proto::{
    ProviderCap, Register, WorkerMessage, worker_control_client::WorkerControlClient,
    worker_message,
};

async fn connect(handle: &TestServerHandle) -> WorkerControlClient<tonic::transport::Channel> {
    let endpoint = format!("http://{}", handle.addr);
    WorkerControlClient::connect(endpoint).await.unwrap()
}

#[tokio::test]
async fn register_with_bad_token_returns_unauthenticated() {
    let registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
    let handle = spawn_test_server(registry.clone(), "correct-secret").await;
    let mut client = connect(&handle).await;

    let (tx, rx) = tokio::sync::mpsc::channel::<WorkerMessage>(8);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
    tx.send(WorkerMessage {
        kind: Some(worker_message::Kind::Register(Register {
            worker_id: "w-1".into(),
            bearer_token: "wrong".into(),
            worker_version: "1.0.0".into(),
            max_concurrent: 1,
            providers: vec![],
            tags: vec![],
        })),
    })
    .await
    .unwrap();

    let resp = client.channel(outbound).await;
    let err = resp.expect_err("server must reject");
    assert_eq!(err.code(), Code::Unauthenticated);
    assert_eq!(registry.count(), 0);
}

#[tokio::test]
async fn register_with_old_version_returns_failed_precondition() {
    let registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
    let handle = spawn_test_server(registry.clone(), "secret").await;
    let mut client = connect(&handle).await;

    let (tx, rx) = tokio::sync::mpsc::channel::<WorkerMessage>(8);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
    tx.send(WorkerMessage {
        kind: Some(worker_message::Kind::Register(Register {
            worker_id: "w-2".into(),
            bearer_token: "secret".into(),
            worker_version: "0.0.1".into(),
            max_concurrent: 1,
            providers: vec![ProviderCap {
                name: "claude".into(),
                version: "1.0.0".into(),
            }],
            tags: vec![],
        })),
    })
    .await
    .unwrap();

    let resp = client.channel(outbound).await;
    let err = resp.expect_err("server must reject");
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn worker_disconnect_evicts_immediately() {
    let registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
    let handle = spawn_test_server(registry.clone(), "secret").await;
    let mut client = connect(&handle).await;

    let (tx, rx) = tokio::sync::mpsc::channel::<WorkerMessage>(8);
    let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
    tx.send(WorkerMessage {
        kind: Some(worker_message::Kind::Register(Register {
            worker_id: "w-3".into(),
            bearer_token: "secret".into(),
            worker_version: "1.0.0".into(),
            max_concurrent: 1,
            providers: vec![],
            tags: vec![],
        })),
    })
    .await
    .unwrap();

    let _stream = client.channel(outbound).await.unwrap();
    // Wait for the registry to reflect the registration.
    let started = std::time::Instant::now();
    while registry.count() == 0 {
        if started.elapsed() > Duration::from_secs(2) {
            panic!("worker never registered");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(registry.count(), 1);

    // Drop the outbound sender to close the worker side of the bidi stream.
    drop(tx);

    let started = std::time::Instant::now();
    while registry.count() != 0 {
        if started.elapsed() > Duration::from_secs(1) {
            panic!("worker not evicted within 1s");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(registry.count(), 0);

    let _ = timeout(Duration::from_secs(1), handle.shutdown()).await;
}
