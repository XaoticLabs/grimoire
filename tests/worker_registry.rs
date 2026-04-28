// RED tests for worker-pool spec, Task 5: `WorkerRegistry`.
//
// References types that do not yet exist (`WorkerRegistry`, `Worker`,
// `WorkerId`, `RegisterRequest`, etc.).

use std::sync::Arc;
use std::time::Duration;

use semver::{Version, VersionReq};
use tokio::sync::mpsc;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::worker_registry::{RegisterParams, WorkerRegistry};
use grimoire::shared::protocol::StreamEvent;

fn register(reg: &WorkerRegistry, id: &str, in_flight: u32, providers: &[(&str, &str)]) {
    let (tx, _rx) = mpsc::channel(8);
    reg.register(RegisterParams {
        worker_id: id.to_string(),
        bearer_ok: true,
        worker_version: "1.0.0".to_string(),
        max_concurrent: 8,
        providers: providers
            .iter()
            .map(|(n, v)| (n.to_string(), Version::parse(v).unwrap()))
            .collect(),
        tags: vec![],
        assign_tx: tx,
    })
    .unwrap();
    if in_flight > 0 {
        reg.set_in_flight_for_test(id, in_flight);
    }
}

#[tokio::test]
async fn pick_least_loaded_picks_lowest_in_flight() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    register(&reg, "A", 2, &[("claude", "1.0.0")]);
    register(&reg, "B", 0, &[("claude", "1.0.0")]);
    register(&reg, "C", 1, &[("claude", "1.0.0")]);

    let pick = reg.pick_least_loaded("claude", &VersionReq::parse(">=1").unwrap());
    assert_eq!(pick.as_deref(), Some("B"));
}

#[tokio::test]
async fn pick_least_loaded_filters_by_constraint() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    register(&reg, "A", 0, &[("claude", "1.0.0")]);
    register(&reg, "B", 0, &[("claude", "2.1.0")]);

    let pick = reg.pick_least_loaded("claude", &VersionReq::parse(">=2").unwrap());
    assert_eq!(pick.as_deref(), Some("B"));
}

#[tokio::test]
async fn pick_least_loaded_returns_none_when_no_match() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    register(&reg, "A", 0, &[("claude", "1.0.0")]);
    let pick = reg.pick_least_loaded("claude", &VersionReq::parse(">=3").unwrap());
    assert!(pick.is_none());
}

#[tokio::test]
async fn pick_least_loaded_breaks_ties_by_worker_id() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    register(&reg, "bbb", 0, &[("claude", "1.0.0")]);
    register(&reg, "aaa", 0, &[("claude", "1.0.0")]);
    let pick = reg.pick_least_loaded("claude", &VersionReq::parse("*").unwrap());
    assert_eq!(pick.as_deref(), Some("aaa"));
}

#[tokio::test]
async fn register_publishes_worker_registered_event() {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let mut sub = bus.subscribe();

    let reg = WorkerRegistry::new_with_bus(Duration::from_secs(30), bus);
    register(&reg, "worker-xyz", 0, &[("claude", "1.0.0")]);

    let event = tokio::time::timeout(Duration::from_secs(1), sub.recv())
        .await
        .expect("event should arrive within timeout")
        .expect("event channel should still be open");
    match event {
        StreamEvent::WorkerRegistered { worker_id } => {
            assert_eq!(worker_id, "worker-xyz");
        }
        other => panic!("expected WorkerRegistered, got {other:?}"),
    }
}

#[tokio::test]
async fn has_eligible_worker_empty_returns_false() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    assert!(!reg.has_eligible_worker("claude", &VersionReq::parse("*").unwrap()));
}

#[tokio::test]
async fn has_eligible_worker_provider_match_returns_true() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    register(&reg, "A", 0, &[("claude", "1.0.0")]);
    assert!(reg.has_eligible_worker("claude", &VersionReq::parse(">=1").unwrap()));
}

#[tokio::test]
async fn has_eligible_worker_provider_mismatch_returns_false() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    register(&reg, "A", 0, &[("claude", "1.0.0")]);
    assert!(!reg.has_eligible_worker("openai", &VersionReq::parse("*").unwrap()));
}

#[tokio::test]
async fn has_eligible_worker_version_mismatch_returns_false() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    register(&reg, "A", 0, &[("claude", "1.0.0")]);
    assert!(!reg.has_eligible_worker("claude", &VersionReq::parse(">=2").unwrap()));
}

#[tokio::test]
async fn has_eligible_worker_ignores_capacity() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    // Register and saturate the worker (in_flight == max_concurrent).
    register(&reg, "A", 8, &[("claude", "1.0.0")]);
    // pick_least_loaded would skip this worker; has_eligible_worker must not.
    assert!(
        reg.pick_least_loaded("claude", &VersionReq::parse("*").unwrap())
            .is_none()
    );
    assert!(reg.has_eligible_worker("claude", &VersionReq::parse("*").unwrap()));
}

#[tokio::test]
async fn has_eligible_worker_is_non_mutating() {
    let reg = WorkerRegistry::new(Duration::from_secs(30));
    register(&reg, "A", 0, &[("claude", "1.0.0")]);
    let before = reg.count();
    let _ = reg.has_eligible_worker("claude", &VersionReq::parse("*").unwrap());
    let _ = reg.has_eligible_worker("absent", &VersionReq::parse("*").unwrap());
    assert_eq!(reg.count(), before);
}

#[tokio::test]
async fn eviction_removes_stale_worker() {
    // Test relies on a fake clock injected into the registry.
    let reg = Arc::new(WorkerRegistry::new_with_clock_for_test(
        Duration::from_secs(30),
    ));
    register(&reg, "A", 0, &[("claude", "1.0.0")]);
    assert_eq!(reg.count(), 1);

    reg.advance_clock_for_test(Duration::from_secs(31));
    reg.run_eviction_pass_for_test().await;
    assert_eq!(reg.count(), 0);
}
