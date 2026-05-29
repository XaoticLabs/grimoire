//! F4b: remote agent-completion wake source. Sister to
//! `parent_completion` for federated parents — fires on a
//! `RemoteAgentStateChanged` bus event whose `(sender_daemon_id,
//! agent_id)` match this source's config and whose `new_state` is in
//! the target set.
//!
//! `states` empty ⇒ defaults to `[Complete]`, matching the local
//! `ParentCompletionSource` ergonomics.

use serde::{Deserialize, Serialize};

use crate::shared::types::AgentState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteAgentCompletionConfig {
    pub sender_daemon_id: String,
    pub remote_agent_id: String,
    #[serde(default)]
    pub states: Vec<AgentState>,
}

pub struct RemoteAgentCompletionSource {
    pub config: RemoteAgentCompletionConfig,
    target_states: Vec<AgentState>,
}

impl RemoteAgentCompletionSource {
    pub fn new(config: RemoteAgentCompletionConfig) -> anyhow::Result<Self> {
        if config.sender_daemon_id.is_empty() {
            return Err(anyhow::anyhow!("sender_daemon_id_required"));
        }
        if config.remote_agent_id.is_empty() {
            return Err(anyhow::anyhow!("remote_agent_id_required"));
        }
        let target_states = if config.states.is_empty() {
            vec![AgentState::Complete]
        } else {
            config.states.clone()
        };
        Ok(Self {
            config,
            target_states,
        })
    }

    pub fn should_fire(
        &self,
        sender_daemon_id: &str,
        agent_id: &str,
        new_state: &AgentState,
    ) -> bool {
        if sender_daemon_id != self.config.sender_daemon_id {
            return false;
        }
        if agent_id != self.config.remote_agent_id {
            return false;
        }
        self.target_states.iter().any(|s| s == new_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> RemoteAgentCompletionConfig {
        RemoteAgentCompletionConfig {
            sender_daemon_id: "homeD".into(),
            remote_agent_id: "agent01".into(),
            states: vec![],
        }
    }

    #[test]
    fn default_target_state_is_complete() {
        let s = RemoteAgentCompletionSource::new(cfg()).unwrap();
        assert!(s.should_fire("homeD", "agent01", &AgentState::Complete));
        assert!(!s.should_fire("homeD", "agent01", &AgentState::Failed));
    }

    #[test]
    fn other_daemon_or_agent_does_not_fire() {
        let s = RemoteAgentCompletionSource::new(cfg()).unwrap();
        assert!(!s.should_fire("otherD", "agent01", &AgentState::Complete));
        assert!(!s.should_fire("homeD", "agent02", &AgentState::Complete));
    }

    #[test]
    fn multi_state_filter() {
        let mut c = cfg();
        c.states = vec![AgentState::Complete, AgentState::Failed];
        let s = RemoteAgentCompletionSource::new(c).unwrap();
        assert!(s.should_fire("homeD", "agent01", &AgentState::Complete));
        assert!(s.should_fire("homeD", "agent01", &AgentState::Failed));
        assert!(!s.should_fire("homeD", "agent01", &AgentState::Active));
    }

    #[test]
    fn empty_ids_rejected() {
        let mut c = cfg();
        c.sender_daemon_id = String::new();
        assert!(RemoteAgentCompletionSource::new(c).is_err());
        let mut c = cfg();
        c.remote_agent_id = String::new();
        assert!(RemoteAgentCompletionSource::new(c).is_err());
    }
}
