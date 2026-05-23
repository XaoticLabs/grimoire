// Tests for the `grim status` worker count.

use grimoire::cli::commands::status;
use grimoire::shared::protocol::{StatusResponse, WorkerStatus};

fn fixture_status(workers: Vec<WorkerStatus>) -> StatusResponse {
    StatusResponse {
        agents: vec![],
        pacts: vec![],
        workers,
        ..Default::default()
    }
}

#[test]
fn status_json_lists_workers() {
    let resp = fixture_status(vec![WorkerStatus {
        worker_id: "w-1".into(),
        in_flight: 0,
        max_concurrent: 4,
        last_heartbeat_age_secs: 1,
        providers: vec!["claude@1.2.3".into()],
    }]);
    let json = serde_json::to_value(&resp).unwrap();
    let arr = json.get("workers").and_then(|v| v.as_array()).unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0].get("worker_id").unwrap().as_str(), Some("w-1"));
}

#[test]
fn status_text_zero_workers_message() {
    let resp = fixture_status(vec![]);
    let text = status::format_text(&resp);
    assert!(
        text.contains("Workers (0)"),
        "zero-worker text output must include `Workers (0)`; got:\n{text}"
    );
}

#[test]
fn status_text_lists_one_worker() {
    let resp = fixture_status(vec![WorkerStatus {
        worker_id: "w-abc1234".into(),
        in_flight: 1,
        max_concurrent: 4,
        last_heartbeat_age_secs: 3,
        providers: vec!["claude@1.2.3".into()],
    }]);
    let text = status::format_text(&resp);
    assert!(text.contains("Workers (1)"));
    assert!(text.contains("claude@1.2.3"));
    assert!(text.contains("w-abc1") || text.contains("w-abc1234"));
}

// --- Task 10: queue-distinct counts in status ----------------------------

#[test]
fn status_reports_queued_count() {
    let resp = StatusResponse {
        agents: vec![],
        pacts: vec![],
        workers: vec![],
        uptime_secs: 0,
        active_count: 2,
        queued_count: 3,
        max_concurrent_agents: 8,
        daemon_id: None,
    };
    let text = status::format_text(&resp);
    assert!(
        text.contains("2 active"),
        "status text should mention `2 active`; got:\n{text}"
    );
    assert!(
        text.contains("3 queued"),
        "status text should mention `3 queued`; got:\n{text}"
    );
    assert!(
        text.contains("cap 8"),
        "status text should mention `cap 8`; got:\n{text}"
    );
}

#[test]
fn status_json_includes_queued_count() {
    let resp = StatusResponse {
        agents: vec![],
        pacts: vec![],
        workers: vec![],
        uptime_secs: 0,
        active_count: 2,
        queued_count: 3,
        max_concurrent_agents: 8,
        daemon_id: None,
    };
    let json = serde_json::to_value(&resp).unwrap();
    assert_eq!(json["queued_count"].as_u64(), Some(3));
    assert_eq!(json["active_count"].as_u64(), Some(2));
    assert_eq!(json["max_concurrent_agents"].as_u64(), Some(8));
}
