use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

pub type AgentId = String;

// --- State Enums with consistent FromStr + Display ---

macro_rules! impl_state_enum {
    ($name:ident { $($variant:ident => $str:literal),+ $(,)? }) => {
        impl $name {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$variant => $str),+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl std::str::FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($str => Ok(Self::$variant)),+,
                    _ => Err(anyhow::anyhow!("invalid {} value: '{}'", stringify!($name), s)),
                }
            }
        }
    };
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Summoning,
    Active,
    Complete,
    Failed,
    Banished,
}

impl_state_enum!(AgentState {
    Summoning => "summoning",
    Active => "active",
    Complete => "complete",
    Failed => "failed",
    Banished => "banished",
});

impl AgentState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Banished)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub name: Option<String>,
    pub state: AgentState,
    pub task: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub cwd: PathBuf,
    pub pid: Option<u32>,
    pub session_id: Option<String>,
    pub exit_code: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    pub id: Option<i64>,
    pub agent_id: AgentId,
    pub event_type: String,
    pub payload: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSummary {
    pub id: AgentId,
    pub name: Option<String>,
    pub state: AgentState,
    pub task: Option<String>,
    pub age_secs: i64,
}

// --- Pacts ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PactState {
    Pending,
    Fired,
    Failed,
}

impl_state_enum!(PactState {
    Pending => "pending",
    Fired => "fired",
    Failed => "failed",
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pact {
    pub id: String,
    pub source_id: AgentId,
    pub task_tpl: String,
    pub name: Option<String>,
    pub state: PactState,
    pub target_id: Option<AgentId>,
    pub created_at: DateTime<Utc>,
    pub fired_at: Option<DateTime<Utc>>,
}

// --- Scrolls (Spec-based DAG Orchestration) ---

pub type ScrollId = String;
pub type RuneId = String;

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
pub enum RuneState {
    Blocked,
    Ready,
    Active,
    Complete,
    Failed,
    Skipped,
}

impl_state_enum!(RuneState {
    Blocked => "blocked",
    Ready => "ready",
    Active => "active",
    Complete => "complete",
    Failed => "failed",
    Skipped => "skipped",
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
pub struct Rune {
    pub id: RuneId,
    pub scroll_id: ScrollId,
    pub name: String,
    pub task: String,
    pub state: RuneState,
    pub agent_id: Option<AgentId>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub cwd: Option<String>,
    pub file_patterns: Vec<String>,
    pub order_index: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuneConflict {
    pub rune_a: RuneId,
    pub rune_a_name: String,
    pub rune_b: RuneId,
    pub rune_b_name: String,
    pub overlapping_patterns: Vec<String>,
}

impl RuneConflict {
    pub fn detect(a: &Rune, b: &Rune) -> Option<Self> {
        let a_patterns: HashSet<&str> = a.file_patterns.iter().map(|s| s.as_str()).collect();
        let b_patterns: HashSet<&str> = b.file_patterns.iter().map(|s| s.as_str()).collect();
        let overlap: Vec<String> = a_patterns
            .intersection(&b_patterns)
            .map(|s| s.to_string())
            .collect();
        if overlap.is_empty() {
            None
        } else {
            Some(Self {
                rune_a: a.id.clone(),
                rune_a_name: a.name.clone(),
                rune_b: b.id.clone(),
                rune_b_name: b.name.clone(),
                overlapping_patterns: overlap,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_enum_roundtrips() {
        // AgentState
        for (s, expected) in [
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

        // PactState
        for (s, expected) in [
            ("pending", PactState::Pending),
            ("fired", PactState::Fired),
            ("failed", PactState::Failed),
        ] {
            let parsed: PactState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }

        // ScrollState
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

        // RuneState
        for (s, expected) in [
            ("blocked", RuneState::Blocked),
            ("ready", RuneState::Ready),
            ("active", RuneState::Active),
            ("complete", RuneState::Complete),
            ("failed", RuneState::Failed),
            ("skipped", RuneState::Skipped),
        ] {
            let parsed: RuneState = s.parse().unwrap();
            assert_eq!(parsed, expected);
            assert_eq!(parsed.to_string(), s);
        }
    }

    #[test]
    fn state_enum_invalid_strings() {
        assert!("bogus".parse::<AgentState>().is_err());
        assert!("bogus".parse::<PactState>().is_err());
        assert!("bogus".parse::<ScrollState>().is_err());
        assert!("bogus".parse::<RuneState>().is_err());
    }

    #[test]
    fn agent_state_is_terminal() {
        assert!(!AgentState::Summoning.is_terminal());
        assert!(!AgentState::Active.is_terminal());
        assert!(AgentState::Complete.is_terminal());
        assert!(AgentState::Failed.is_terminal());
        assert!(AgentState::Banished.is_terminal());
    }

    #[test]
    fn rune_conflict_detect_overlap() {
        let a = make_rune("a", vec!["src/foo.rs", "src/bar.rs"]);
        let b = make_rune("b", vec!["src/bar.rs", "src/baz.rs"]);
        let conflict = RuneConflict::detect(&a, &b).unwrap();
        assert_eq!(conflict.overlapping_patterns, vec!["src/bar.rs"]);
    }

    #[test]
    fn rune_conflict_detect_no_overlap() {
        let a = make_rune("a", vec!["src/foo.rs"]);
        let b = make_rune("b", vec!["src/bar.rs"]);
        assert!(RuneConflict::detect(&a, &b).is_none());
    }

    #[test]
    fn rune_conflict_detect_empty_patterns() {
        let a = make_rune("a", vec![]);
        let b = make_rune("b", vec!["src/bar.rs"]);
        assert!(RuneConflict::detect(&a, &b).is_none());

        // Both empty
        let c = make_rune("c", vec![]);
        let d = make_rune("d", vec![]);
        assert!(RuneConflict::detect(&c, &d).is_none());
    }

    fn make_rune(id: &str, file_patterns: Vec<&str>) -> Rune {
        Rune {
            id: id.to_string(),
            scroll_id: "scroll1".to_string(),
            name: format!("Rune {}", id),
            task: "test task".to_string(),
            state: RuneState::Ready,
            agent_id: None,
            provider: None,
            model: None,
            cwd: None,
            file_patterns: file_patterns.into_iter().map(String::from).collect(),
            order_index: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}
