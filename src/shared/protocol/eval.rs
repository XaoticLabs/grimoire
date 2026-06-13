//! Evaluation records: a scored verdict by an evaluator agent against a
//! target agent, used by verification-gated scrolls.
use serde::{Deserialize, Serialize};

use crate::shared::types::AgentId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalRecord {
    pub id: String,
    pub target_id: AgentId,
    pub evaluator_id: AgentId,
    pub score: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verdict: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalRecordParams {
    pub target_id: AgentId,
    pub evaluator_id: AgentId,
    pub score: f64,
    #[serde(default)]
    pub verdict: Option<String>,
    #[serde(default)]
    pub rationale: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalRecordResult {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalListParams {
    pub target_id: AgentId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalListResult {
    pub results: Vec<EvalRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalScoreEntry {
    pub target_id: AgentId,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalScoresResult {
    pub scores: Vec<EvalScoreEntry>,
}
