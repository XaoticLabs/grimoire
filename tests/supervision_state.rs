//! Task 1 contract tests: AgentState::Restarting + is_supervisable.

use grimoire::shared::types::AgentState;

#[test]
fn restarting_is_terminal_not_final() {
    assert!(AgentState::Restarting.is_terminal());
    assert!(!AgentState::Restarting.is_final());
}

#[test]
fn is_supervisable_only_failed() {
    assert!(AgentState::Failed.is_supervisable());
    for s in [
        AgentState::Queued,
        AgentState::Summoning,
        AgentState::Active,
        AgentState::Complete,
        AgentState::Banished,
        AgentState::Dormant,
        AgentState::Restarting,
    ] {
        assert!(!s.is_supervisable(), "{} should not be supervisable", s);
    }
}

#[test]
fn restarting_serde_roundtrip() {
    let json = serde_json::to_string(&AgentState::Restarting).unwrap();
    assert_eq!(json, "\"restarting\"");
    let parsed: AgentState = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, AgentState::Restarting);
    let from_str: AgentState = "restarting".parse().unwrap();
    assert_eq!(from_str, AgentState::Restarting);
    assert_eq!(AgentState::Restarting.to_string(), "restarting");
}
