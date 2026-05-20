//! Task 6 contract tests: circle/status rendering with supervision.

use std::path::PathBuf;

use chrono::Utc;

use grimoire::cli::formatters::{format_circle_text, format_status_supervision_block};
use grimoire::shared::types::{Agent, AgentState, AgentSummary, RestartPolicy, SupervisionConfig};

fn agent(id: &str, policy: RestartPolicy, count: u32) -> Agent {
    Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Active,
        task: Some("seed".into()),
        model: None,
        provider: None,
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: policy,
        restart_count: count,
        workspace_id: None,
    }
}

fn summary(id: &str, policy: RestartPolicy, count: u32, max: Option<u32>) -> AgentSummary {
    AgentSummary {
        id: id.to_string(),
        name: None,
        state: AgentState::Active,
        task: Some("seed".into()),
        age_secs: 5,
        worker_id: None,
        restart_policy: policy,
        restart_count: count,
        max_restarts: max,
    }
}

#[test]
fn circle_renders_restart_column() {
    let agents = vec![summary("aaaa0001", RestartPolicy::OnFailure, 2, Some(3))];
    let out = format_circle_text(&agents);
    assert!(out.contains("RESTART"));
    assert!(out.contains("2/3"));
}

#[test]
fn circle_renders_dash_for_never_policy() {
    let agents = vec![summary("aaaa0002", RestartPolicy::Never, 0, None)];
    let out = format_circle_text(&agents);
    let lines: Vec<&str> = out.lines().collect();
    let body = lines.last().unwrap();
    assert!(body.contains(" - "), "expected dash in column: {body}");
}

#[test]
fn status_renders_supervision_block_for_on_failure() {
    let a = agent("aaaa0003", RestartPolicy::OnFailure, 1);
    let cfg = SupervisionConfig {
        policy: RestartPolicy::OnFailure,
        max_restarts: Some(3),
        window_secs: Some(60),
        escalate_to: Some("topic://human-review".into()),
    };
    let out = format_status_supervision_block(&a, Some(&cfg), 0);
    assert!(out.contains("restart-policy: on_failure (3/60s)"));
    assert!(out.contains("restart-count: 1"));
    assert!(out.contains("escalate-to: topic://human-review"));
    assert!(out.contains("escalation-depth: 0"));
}

#[test]
fn status_omits_supervision_block_for_never() {
    let a = agent("aaaa0004", RestartPolicy::Never, 0);
    let out = format_status_supervision_block(&a, None, 0);
    assert!(out.is_empty());
}
