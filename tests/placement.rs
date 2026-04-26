// RED tests for worker-pool spec, Task 6: `LeastLoadedPlacement`.
//
// References `Placement` and `LeastLoadedPlacement` not yet implemented.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use semver::Version;
use tokio::sync::mpsc;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::executor::{
    ExecuteRequest, LeastLoadedPlacement, LocalExecutor, Placement, RemoteExecutor,
};
use grimoire::daemon::persistence::Database;
use grimoire::daemon::provider_registry::ProviderRegistry;
use grimoire::daemon::worker_registry::{RegisterParams, WorkerRegistry};

fn req(provider: &str) -> ExecuteRequest {
    ExecuteRequest {
        agent_id: "a-1".to_string(),
        task: "noop".to_string(),
        provider_name: provider.to_string(),
        cwd: PathBuf::from("/tmp"),
        model: None,
        resume_session_id: None,
    }
}

fn fresh_local() -> Arc<LocalExecutor> {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let registry = ProviderRegistry::test_with_true_provider();
    Arc::new(LocalExecutor::new(Arc::new(registry), bus, db))
}

#[tokio::test]
async fn placement_picks_remote_when_worker_available() {
    let registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
    let (tx, _rx) = mpsc::channel(8);
    registry
        .register(RegisterParams {
            worker_id: "w-1".to_string(),
            bearer_ok: true,
            worker_version: "1.0.0".into(),
            max_concurrent: 4,
            providers: vec![("claude".to_string(), Version::parse("1.0.0").unwrap())],
            tags: vec![],
            assign_tx: tx,
        })
        .unwrap();

    let local = fresh_local();
    let remote_factory: Arc<dyn Fn(String) -> Arc<dyn grimoire::daemon::executor::Executor>
            + Send
            + Sync> = Arc::new(|wid| {
        // Construct a stub RemoteExecutor that records the worker id.
        Arc::new(RemoteExecutor::stub_for_test(wid))
            as Arc<dyn grimoire::daemon::executor::Executor>
    });

    let placement = LeastLoadedPlacement::new(registry, local.clone(), remote_factory);
    let chosen = placement.pick(&req("claude"));
    assert_eq!(
        chosen.name(),
        "remote",
        "should pick remote when a worker matches"
    );
}

#[tokio::test]
async fn placement_falls_back_to_local_when_no_worker() {
    let registry = Arc::new(WorkerRegistry::new(Duration::from_secs(30)));
    let local = fresh_local();
    let remote_factory: Arc<dyn Fn(String) -> Arc<dyn grimoire::daemon::executor::Executor>
            + Send
            + Sync> = Arc::new(|_| panic!("remote_factory should not be invoked"));

    let placement = LeastLoadedPlacement::new(registry, local.clone(), remote_factory);
    let chosen = placement.pick(&req("claude"));
    assert_eq!(
        chosen.name(),
        "local",
        "should fall back to local when registry empty"
    );
}
