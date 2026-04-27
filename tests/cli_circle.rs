// RED tests for worker-pool spec, Task 8a: `grim circle` worker annotation.
//
// References `AgentSummary::worker_id` (added in this task) and a CLI
// formatter helper not yet implemented.

use grimoire::cli::formatters;
use grimoire::shared::types::{AgentState, AgentSummary, RestartPolicy};

fn summary(id: &str, worker_id: Option<&str>) -> AgentSummary {
    AgentSummary {
        id: id.to_string(),
        name: None,
        state: AgentState::Complete,
        task: Some("noop".into()),
        age_secs: 0,
        worker_id: worker_id.map(|s| s.to_string()),
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        max_restarts: None,
    }
}

#[test]
fn circle_json_includes_worker_id() {
    let s = summary("a-1", Some("w-abc1234"));
    let json = serde_json::to_value(&s).unwrap();
    assert_eq!(
        json.get("worker_id").and_then(|v| v.as_str()),
        Some("w-abc1234"),
        "AgentSummary JSON must include worker_id"
    );
}

#[test]
fn circle_text_shows_worker_column() {
    let rows = vec![summary("a-1", Some("w-abc1234")), summary("a-2", None)];
    let rendered = formatters::circle_text(&rows);
    assert!(
        rendered.contains("WORKER"),
        "text output must include WORKER column header; got:\n{rendered}"
    );
    // Truncated worker id (first 6 chars) and `local` label both appear.
    assert!(rendered.contains("w-abc1"));
    assert!(rendered.contains("local"));
}
