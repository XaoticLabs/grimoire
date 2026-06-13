//! DAG validation (cycle detection) and file-conflict detection over a
//! scroll's task graph.

use std::collections::{HashMap, HashSet};

use crate::shared::types::{Task, TaskConflict};

use super::ScrollKeeper;

impl ScrollKeeper {
    pub(super) fn validate_dag(&self, scroll_id: &str) -> anyhow::Result<()> {
        let edges = self.db.get_all_dependencies_for_scroll(scroll_id)?;
        let tasks = self.db.get_tasks_for_scroll(scroll_id)?;

        let mut adj: HashMap<String, Vec<String>> = HashMap::new();
        let all_ids: HashSet<String> = tasks.iter().map(|r| r.id.clone()).collect();

        for id in &all_ids {
            adj.entry(id.clone()).or_default();
        }
        for (from, to) in &edges {
            adj.entry(from.clone()).or_default().push(to.clone());
        }

        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();

        for id in &all_ids {
            if dag_has_cycle(id, &adj, &mut visited, &mut in_stack) {
                return Err(anyhow::anyhow!(
                    "Dependency cycle detected in scroll {scroll_id}"
                ));
            }
        }

        Ok(())
    }

    pub(super) fn detect_all_conflicts(tasks: &[Task]) -> Vec<TaskConflict> {
        let mut conflicts = Vec::new();
        for i in 0..tasks.len() {
            for j in (i + 1)..tasks.len() {
                if let Some(c) = TaskConflict::detect(&tasks[i], &tasks[j]) {
                    conflicts.push(c);
                }
            }
        }
        conflicts
    }
}

/// Cycle-detecting DFS for `validate_dag`: `true` if a back-edge exists.
fn dag_has_cycle(
    node: &str,
    adj: &HashMap<String, Vec<String>>,
    visited: &mut HashSet<String>,
    in_stack: &mut HashSet<String>,
) -> bool {
    if in_stack.contains(node) {
        return true;
    }
    if visited.contains(node) {
        return false;
    }
    visited.insert(node.to_string());
    in_stack.insert(node.to_string());

    if let Some(neighbors) = adj.get(node) {
        for neighbor in neighbors {
            if dag_has_cycle(neighbor, adj, visited, in_stack) {
                return true;
            }
        }
    }

    in_stack.remove(node);
    false
}
