//! Contract tests for the `AgentState::Dormant` variant and the
//! `is_terminal` / `is_final` split.

use grimoire::shared::types::AgentState;

#[test]
fn is_terminal_includes_dormant() {
    assert!(AgentState::Dormant.is_terminal());
    assert!(AgentState::Complete.is_terminal());
    assert!(AgentState::Failed.is_terminal());
    assert!(AgentState::Banished.is_terminal());
    assert!(!AgentState::Active.is_terminal());
    assert!(!AgentState::Queued.is_terminal());
    assert!(!AgentState::Summoning.is_terminal());
}

#[test]
fn is_final_excludes_dormant() {
    assert!(!AgentState::Dormant.is_final());
    assert!(AgentState::Complete.is_final());
    assert!(AgentState::Failed.is_final());
    assert!(AgentState::Banished.is_final());
    assert!(!AgentState::Active.is_final());
    assert!(!AgentState::Queued.is_final());
    assert!(!AgentState::Summoning.is_final());
}

#[test]
fn dormant_serde_roundtrip() {
    let json = serde_json::to_string(&AgentState::Dormant).unwrap();
    assert_eq!(json, "\"dormant\"");
    let parsed: AgentState = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, AgentState::Dormant);

    let parsed: AgentState = "dormant".parse().unwrap();
    assert_eq!(parsed, AgentState::Dormant);
    assert_eq!(AgentState::Dormant.to_string(), "dormant");
}
