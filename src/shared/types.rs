use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub type AgentId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Summoning,
    Active,
    Complete,
    Failed,
    Banished,
}

impl AgentState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Summoning => "summoning",
            Self::Active => "active",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Banished => "banished",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "summoning" => Some(Self::Summoning),
            "active" => Some(Self::Active),
            "complete" => Some(Self::Complete),
            "failed" => Some(Self::Failed),
            "banished" => Some(Self::Banished),
            _ => None,
        }
    }
}

impl std::fmt::Display for AgentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub name: Option<String>,
    pub state: AgentState,
    pub task: Option<String>,
    pub model: Option<String>,
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
