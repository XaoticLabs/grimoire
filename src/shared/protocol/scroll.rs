//! Scroll (multi-task DAG) RPC: inscribe/activate/status/abandon plus the
//! HITL approve/reject of held tasks.
use serde::{Deserialize, Serialize};

use super::IdParams;
use crate::shared::types::{ScrollId, TaskConflict};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollInscribeParams {
    pub spec_path: String,
    pub max_concurrency: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollInscribeResult {
    pub id: ScrollId,
    pub name: String,
    pub task_count: usize,
    pub conflicts: Vec<TaskConflict>,
}

pub type ScrollActivateParams = IdParams;
pub type ScrollStatusParams = IdParams;
pub type ScrollAbandonParams = IdParams;

/// HITL approve/reject of a held task. `task` is an exact task id or name
/// within the scroll.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollApproveParams {
    pub scroll_id: ScrollId,
    pub task: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrollApproveResult {
    pub scroll_id: ScrollId,
    pub task_name: String,
    /// `approved` or `rejected`.
    pub decision: String,
}
