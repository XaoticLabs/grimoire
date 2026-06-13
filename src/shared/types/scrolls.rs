//! Scrolls: spec-based DAG orchestration — scrolls, tasks, and conflicts.

use super::AgentId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub type ScrollId = String;
pub type TaskId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScrollState {
    Inscribed,
    Active,
    Complete,
    Failed,
    Abandoned,
}

impl_state_enum!(ScrollState {
    Inscribed => "inscribed",
    Active => "active",
    Complete => "complete",
    Failed => "failed",
    Abandoned => "abandoned",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Blocked,
    Ready,
    /// HITL gate: runnable but held for human approval before spawning. Not
    /// terminal (scroll stays Active); → Ready on approval, Failed on rejection.
    AwaitingApproval,
    Active,
    Complete,
    Failed,
    Skipped,
}

impl_state_enum!(TaskState {
    Blocked => "blocked",
    Ready => "ready",
    AwaitingApproval => "awaiting_approval",
    Active => "active",
    Complete => "complete",
    Failed => "failed",
    Skipped => "skipped",
});

/// HITL approval state, a DB-only column on `tasks`; settled by
/// `scroll.approve` / `scroll.reject`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    /// No gate, or the gate hasn't been reached yet.
    #[default]
    None,
    /// Held, waiting for a human decision.
    Pending,
    Approved,
    Rejected,
}

impl_state_enum!(ApprovalState {
    None => "none",
    Pending => "pending",
    Approved => "approved",
    Rejected => "rejected",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scroll {
    pub id: ScrollId,
    pub name: String,
    pub state: ScrollState,
    pub source_path: Option<String>,
    pub max_concurrency: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub scroll_id: ScrollId,
    pub name: String,
    pub prompt: String,
    pub state: TaskState,
    pub agent_id: Option<AgentId>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub file_patterns: Vec<String>,
    pub order_index: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Dispatch to this peer instead of spawning locally. The receiver queues
    /// a local agent and federates lifecycle back; `agent_id` is set to the
    /// receiver's local id once the dispatch ack arrives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_name: Option<String>,
    /// Verification gate: when set, worker completion doesn't finish the task;
    /// an evaluator scores the transcript against this rubric and the task
    /// completes only if the score clears `verify_threshold`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_rubric: Option<String>,
    /// Min evaluator score (0.0–1.0) for a verified task to complete. `None` =
    /// keeper's default (0.7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_threshold: Option<f64>,
    /// Evaluator agent scoring this task's transcript; set when verification
    /// is summoned so the keeper can route its completion back here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verifier_agent_id: Option<AgentId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConflict {
    pub task_a: TaskId,
    pub task_a_name: String,
    pub task_b: TaskId,
    pub task_b_name: String,
    pub overlapping_patterns: Vec<String>,
}

impl TaskConflict {
    pub fn detect(a: &Task, b: &Task) -> Option<Self> {
        let a_patterns: HashSet<&str> = a
            .file_patterns
            .iter()
            .map(std::string::String::as_str)
            .collect();
        let b_patterns: HashSet<&str> = b
            .file_patterns
            .iter()
            .map(std::string::String::as_str)
            .collect();
        let overlap: Vec<String> = a_patterns
            .intersection(&b_patterns)
            .map(std::string::ToString::to_string)
            .collect();
        if overlap.is_empty() {
            None
        } else {
            Some(Self {
                task_a: a.id.clone(),
                task_a_name: a.name.clone(),
                task_b: b.id.clone(),
                task_b_name: b.name.clone(),
                overlapping_patterns: overlap,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::types::{AgentState, PactState};

    #[test]
    fn state_enum_roundtrips() {
        for (s, expected) in [
            ("queued", AgentState::Queued),
            ("summoning", AgentState::Summoning),
            ("active", AgentState::Active),
            ("complete", AgentState::Complete),
            ("failed", AgentState::Failed),
            ("banished", AgentState::Banished),
        ] {
            let parsed: AgentState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }

        for (s, expected) in [
            ("pending", PactState::Pending),
            ("fired", PactState::Fired),
            ("failed", PactState::Failed),
        ] {
            let parsed: PactState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }

        for (s, expected) in [
            ("inscribed", ScrollState::Inscribed),
            ("active", ScrollState::Active),
            ("complete", ScrollState::Complete),
            ("failed", ScrollState::Failed),
            ("abandoned", ScrollState::Abandoned),
        ] {
            let parsed: ScrollState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }

        for (s, expected) in [
            ("blocked", TaskState::Blocked),
            ("ready", TaskState::Ready),
            ("awaiting_approval", TaskState::AwaitingApproval),
            ("active", TaskState::Active),
            ("complete", TaskState::Complete),
            ("failed", TaskState::Failed),
            ("skipped", TaskState::Skipped),
        ] {
            let parsed: TaskState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }

        for (s, expected) in [
            ("none", ApprovalState::None),
            ("pending", ApprovalState::Pending),
            ("approved", ApprovalState::Approved),
            ("rejected", ApprovalState::Rejected),
        ] {
            let parsed: ApprovalState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }
    }

    #[test]
    fn state_enum_invalid_strings() {
        assert!("bogus".parse::<AgentState>().is_err());
        assert!("bogus".parse::<PactState>().is_err());
        assert!("bogus".parse::<ScrollState>().is_err());
        assert!("bogus".parse::<TaskState>().is_err());
    }

    #[test]
    fn task_conflict_detect_overlap() {
        let a = make_task("a", vec!["src/foo.rs", "src/bar.rs"]);
        let b = make_task("b", vec!["src/bar.rs", "src/baz.rs"]);
        let conflict = TaskConflict::detect(&a, &b).unwrap();
        assert_eq!(conflict.overlapping_patterns, vec!["src/bar.rs"]);
    }

    #[test]
    fn task_conflict_detect_no_overlap() {
        let a = make_task("a", vec!["src/foo.rs"]);
        let b = make_task("b", vec!["src/bar.rs"]);
        assert!(TaskConflict::detect(&a, &b).is_none());
    }

    #[test]
    fn task_conflict_detect_empty_patterns() {
        let a = make_task("a", vec![]);
        let b = make_task("b", vec!["src/bar.rs"]);
        assert!(TaskConflict::detect(&a, &b).is_none());

        // Both empty
        let c = make_task("c", vec![]);
        let d = make_task("d", vec![]);
        assert!(TaskConflict::detect(&c, &d).is_none());
    }

    fn make_task(id: &str, file_patterns: Vec<&str>) -> Task {
        Task {
            id: id.to_string(),
            scroll_id: "scroll1".to_string(),
            name: format!("Task {id}"),
            prompt: "test task".to_string(),
            state: TaskState::Ready,
            agent_id: None,
            provider: None,
            model: None,
            cwd: None,
            file_patterns: file_patterns.into_iter().map(String::from).collect(),
            order_index: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            peer_name: None,
            verify_rubric: None,
            verify_threshold: None,
            verifier_agent_id: None,
        }
    }
}
