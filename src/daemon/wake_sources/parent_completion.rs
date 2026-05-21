//! Parent-completion wake source: fires when a configured parent agent
//! transitions into one of the configured target states.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::shared::types::AgentState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParentCompletionConfig {
    pub parent_id: String,
    /// Target states. If empty, defaults to `[Complete]`.
    #[serde(default)]
    pub states: Vec<AgentState>,
}

impl ParentCompletionConfig {
    pub fn target_states(&self) -> Vec<AgentState> {
        if self.states.is_empty() {
            vec![AgentState::Complete]
        } else {
            self.states.clone()
        }
    }
}

pub struct ParentCompletionSource {
    pub config: ParentCompletionConfig,
}

impl ParentCompletionSource {
    pub const fn new(config: ParentCompletionConfig) -> Result<Self> {
        Ok(Self { config })
    }

    /// Determines whether a `StateChange` event should fire this source.
    /// The registry's bus listener uses this to filter incoming events.
    pub fn should_fire(&self, agent_id: &str, new_state: &AgentState) -> bool {
        if agent_id != self.config.parent_id {
            return false;
        }
        self.config.target_states().iter().any(|s| s == new_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_only_for_matching_agent_and_state() {
        let s = ParentCompletionSource::new(ParentCompletionConfig {
            parent_id: "parent01".into(),
            states: vec![],
        })
        .unwrap();
        assert!(s.should_fire("parent01", &AgentState::Complete));
        assert!(!s.should_fire("parent01", &AgentState::Failed));
        assert!(!s.should_fire("other", &AgentState::Complete));
    }

    #[test]
    fn multi_state_filter() {
        let s = ParentCompletionSource::new(ParentCompletionConfig {
            parent_id: "parent01".into(),
            states: vec![AgentState::Complete, AgentState::Failed],
        })
        .unwrap();
        assert!(s.should_fire("parent01", &AgentState::Complete));
        assert!(s.should_fire("parent01", &AgentState::Failed));
        assert!(!s.should_fire("parent01", &AgentState::Banished));
    }
}
