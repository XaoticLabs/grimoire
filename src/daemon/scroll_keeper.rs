use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{
    Rune, RuneConflict, RuneState, Scroll, ScrollState,
};

use super::agent_manager::AgentManager;
use super::event_bus::EventBus;
use super::persistence::Database;
use super::scroll_parser::ScrollSpec;

pub struct ScrollKeeper {
    db: Arc<Database>,
    manager: Arc<AgentManager>,
}

/// Status snapshot for a scroll
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScrollStatus {
    pub scroll: Scroll,
    pub runes: Vec<RuneStatus>,
    pub total: usize,
    pub complete: usize,
    pub active: usize,
    pub blocked: usize,
    pub ready: usize,
    pub failed: usize,
    pub skipped: usize,
    pub conflicts: Vec<RuneConflict>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RuneStatus {
    pub rune: Rune,
    pub depends_on_names: Vec<String>,
}

impl ScrollKeeper {
    pub fn new(db: Arc<Database>, manager: Arc<AgentManager>) -> Self {
        Self { db, manager }
    }

    /// Start listening to the event bus for agent completions
    pub fn start(self: Arc<Self>, event_bus: &EventBus) {
        let mut rx = event_bus.subscribe();

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(StreamEvent::StateChange {
                        ref agent_id,
                        ref new_state,
                        ..
                    }) => {
                        use crate::shared::types::AgentState;
                        match new_state {
                            AgentState::Complete => self.handle_agent_completion(agent_id).await,
                            AgentState::Failed | AgentState::Banished => self.handle_agent_failure(agent_id).await,
                            _ => {}
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "ScrollKeeper lagged, some events missed");
                        continue;
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        });
    }

    /// Inscribe a scroll from a parsed spec
    pub fn inscribe(
        &self,
        spec: ScrollSpec,
        max_concurrency: Option<u32>,
        source_path: Option<String>,
    ) -> anyhow::Result<InscribeResult> {
        let scroll_id = crate::shared::constants::generate_short_id();
        let now = chrono::Utc::now();

        let scroll = Scroll {
            id: scroll_id.clone(),
            name: spec.name.clone(),
            state: ScrollState::Inscribed,
            source_path,
            max_concurrency: max_concurrency.unwrap_or(4),
            created_at: now,
            updated_at: now,
        };

        self.db.insert_scroll(&scroll)?;

        // Create runes and build name->id map
        let mut name_to_id: HashMap<String, String> = HashMap::new();
        let mut runes = Vec::new();

        for (idx, rune_spec) in spec.runes.iter().enumerate() {
            let rune_id = crate::shared::constants::generate_short_id();
            name_to_id.insert(rune_spec.name.clone(), rune_id.clone());

            let has_deps = !rune_spec.depends_on.is_empty();
            let rune = Rune {
                id: rune_id,
                scroll_id: scroll_id.clone(),
                name: rune_spec.name.clone(),
                task: rune_spec.task.clone(),
                state: if has_deps {
                    RuneState::Blocked
                } else {
                    RuneState::Ready
                },
                agent_id: None,
                provider: rune_spec.provider.clone(),
                model: rune_spec.model.clone(),
                cwd: rune_spec.cwd.clone(),
                file_patterns: rune_spec.file_patterns.clone(),
                order_index: idx as u32,
                created_at: now,
                updated_at: now,
            };

            self.db.insert_rune(&rune)?;
            runes.push(rune);
        }

        // Insert dependency edges
        for rune_spec in &spec.runes {
            let rune_id = &name_to_id[&rune_spec.name];
            for dep_name in &rune_spec.depends_on {
                let dep_id = &name_to_id[dep_name];
                self.db.insert_rune_dependency(rune_id, dep_id)?;
            }
        }

        // Validate: no cycles
        self.validate_dag(&scroll_id)?;

        // Detect file conflicts
        let conflicts = self.detect_all_conflicts(&runes);

        info!(scroll_id = %scroll_id, name = %spec.name, runes = runes.len(), "Scroll inscribed");

        Ok(InscribeResult {
            scroll,
            rune_count: runes.len(),
            conflicts,
        })
    }

    /// Activate a scroll — start scheduling ready runes
    pub async fn activate(&self, scroll_id: &str) -> anyhow::Result<()> {
        let scroll = self
            .db
            .get_scroll(scroll_id)?
            .ok_or_else(|| anyhow::anyhow!("Scroll not found: {}", scroll_id))?;

        if scroll.state != ScrollState::Inscribed {
            return Err(anyhow::anyhow!(
                "Scroll {} is in state '{}', can only activate from 'inscribed'",
                scroll_id,
                scroll.state
            ));
        }

        self.db
            .update_scroll_state(scroll_id, &ScrollState::Active)?;

        info!(scroll_id = %scroll_id, "Scroll activated");

        self.schedule_runes(scroll_id).await?;

        Ok(())
    }

    /// Abandon a scroll — banish active agents, mark incomplete runes as skipped
    pub async fn abandon(&self, scroll_id: &str) -> anyhow::Result<()> {
        let runes = self.db.get_runes_for_scroll(scroll_id)?;

        for rune in &runes {
            match rune.state {
                RuneState::Active => {
                    if let Some(ref agent_id) = rune.agent_id {
                        let _ = self.manager.banish(agent_id).await;
                    }
                    self.db.update_rune_state(&rune.id, &RuneState::Skipped)?;
                }
                RuneState::Blocked | RuneState::Ready => {
                    self.db.update_rune_state(&rune.id, &RuneState::Skipped)?;
                }
                _ => {}
            }
        }

        self.db
            .update_scroll_state(scroll_id, &ScrollState::Abandoned)?;

        info!(scroll_id = %scroll_id, "Scroll abandoned");
        Ok(())
    }

    /// Get full status of a scroll
    pub fn status(&self, scroll_id: &str) -> anyhow::Result<ScrollStatus> {
        let scroll = self
            .db
            .get_scroll(scroll_id)?
            .ok_or_else(|| anyhow::anyhow!("Scroll not found: {}", scroll_id))?;

        let runes = self.db.get_runes_for_scroll(scroll_id)?;

        // Build name map for dependency display
        let id_to_name: HashMap<String, String> = runes
            .iter()
            .map(|r| (r.id.clone(), r.name.clone()))
            .collect();

        let mut rune_statuses = Vec::new();
        for rune in &runes {
            let deps = self.db.get_rune_dependencies(&rune.id)?;
            let depends_on_names: Vec<String> = deps
                .iter()
                .filter_map(|id| id_to_name.get(id).cloned())
                .collect();
            rune_statuses.push(RuneStatus {
                rune: rune.clone(),
                depends_on_names,
            });
        }

        let total = runes.len();
        let complete = runes.iter().filter(|r| r.state == RuneState::Complete).count();
        let active = runes.iter().filter(|r| r.state == RuneState::Active).count();
        let blocked = runes.iter().filter(|r| r.state == RuneState::Blocked).count();
        let ready = runes.iter().filter(|r| r.state == RuneState::Ready).count();
        let failed = runes.iter().filter(|r| r.state == RuneState::Failed).count();
        let skipped = runes.iter().filter(|r| r.state == RuneState::Skipped).count();

        // Detect conflicts among active + ready runes
        let conflictable: Vec<Rune> = runes
            .iter()
            .filter(|r| r.state == RuneState::Active || r.state == RuneState::Ready)
            .cloned()
            .collect();
        let conflicts = self.detect_all_conflicts(&conflictable);

        Ok(ScrollStatus {
            scroll,
            runes: rune_statuses,
            total,
            complete,
            active,
            blocked,
            ready,
            failed,
            skipped,
            conflicts,
        })
    }

    // --- Internal ---

    async fn handle_agent_completion(&self, agent_id: &str) {
        let rune = match self.db.get_rune_by_agent_id(agent_id) {
            Ok(Some(r)) => r,
            Ok(None) => return, // Not a scroll-managed agent
            Err(e) => {
                error!(agent_id = %agent_id, error = %e, "Failed to look up rune");
                return;
            }
        };

        info!(
            scroll_id = %rune.scroll_id,
            rune = %rune.name,
            agent_id = %agent_id,
            "Rune completed"
        );

        if let Err(e) = self.db.update_rune_state(&rune.id, &RuneState::Complete) {
            error!(rune_id = %rune.id, error = %e, "Failed to update rune state");
            return;
        }

        // Check if scroll is done
        let runes = match self.db.get_runes_for_scroll(&rune.scroll_id) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "Failed to get runes for scroll");
                return;
            }
        };

        let all_done = runes
            .iter()
            .all(|r| matches!(r.state, RuneState::Complete | RuneState::Skipped | RuneState::Failed));

        if all_done {
            let any_failed = runes.iter().any(|r| r.state == RuneState::Failed);
            let new_state = if any_failed {
                ScrollState::Failed
            } else {
                ScrollState::Complete
            };
            let _ = self.db.update_scroll_state(&rune.scroll_id, &new_state);
            info!(scroll_id = %rune.scroll_id, state = %new_state, "Scroll finished");
        } else {
            // Schedule next batch
            if let Err(e) = self.schedule_runes(&rune.scroll_id).await {
                error!(scroll_id = %rune.scroll_id, error = %e, "Failed to schedule runes");
            }
        }
    }

    async fn handle_agent_failure(&self, agent_id: &str) {
        let rune = match self.db.get_rune_by_agent_id(agent_id) {
            Ok(Some(r)) => r,
            Ok(None) => return,
            Err(e) => {
                error!(agent_id = %agent_id, error = %e, "Failed to look up rune");
                return;
            }
        };

        warn!(
            scroll_id = %rune.scroll_id,
            rune = %rune.name,
            agent_id = %agent_id,
            "Rune failed"
        );

        let _ = self.db.update_rune_state(&rune.id, &RuneState::Failed);

        // Skip all downstream runes
        self.skip_downstream(&rune.id);

        // Check if scroll is done
        let runes = match self.db.get_runes_for_scroll(&rune.scroll_id) {
            Ok(r) => r,
            Err(_) => return,
        };

        let all_terminal = runes.iter().all(|r| {
            matches!(
                r.state,
                RuneState::Complete | RuneState::Failed | RuneState::Skipped
            )
        });

        if all_terminal {
            let _ = self
                .db
                .update_scroll_state(&rune.scroll_id, &ScrollState::Failed);
            info!(scroll_id = %rune.scroll_id, "Scroll failed");
        } else {
            // There may still be independent runes that can run
            let _ = self.schedule_runes(&rune.scroll_id).await;
        }
    }

    fn skip_downstream(&self, rune_id: &str) {
        let dependents = match self.db.get_rune_dependents(rune_id) {
            Ok(d) => d,
            Err(_) => return,
        };

        for dep_id in dependents {
            let _ = self.db.update_rune_state(&dep_id, &RuneState::Skipped);
            self.skip_downstream(&dep_id);
        }
    }

    async fn schedule_runes(&self, scroll_id: &str) -> anyhow::Result<()> {
        let scroll = self
            .db
            .get_scroll(scroll_id)?
            .ok_or_else(|| anyhow::anyhow!("Scroll not found"))?;

        if scroll.state != ScrollState::Active {
            return Ok(());
        }

        let active_count = self.db.count_active_runes(scroll_id)?;
        let available_slots = (scroll.max_concurrency as usize).saturating_sub(active_count);

        if available_slots == 0 {
            return Ok(());
        }

        let ready_runes = self.db.find_ready_runes(scroll_id)?;
        if ready_runes.is_empty() {
            return Ok(());
        }

        // Get currently active runes for conflict checking
        let all_runes = self.db.get_runes_for_scroll(scroll_id)?;
        let active_runes: Vec<&Rune> = all_runes
            .iter()
            .filter(|r| r.state == RuneState::Active)
            .collect();

        let mut spawned = 0;
        for rune in &ready_runes {
            if spawned >= available_slots {
                break;
            }

            // Check for file conflicts with active runes
            let has_conflict = active_runes.iter().any(|active| {
                RuneConflict::detect(active, rune).is_some()
            });

            if has_conflict {
                info!(
                    rune = %rune.name,
                    "Delaying rune due to file conflict with active rune"
                );
                continue;
            }

            // Spawn agent for this rune
            let cwd = rune
                .cwd
                .as_ref()
                .map(PathBuf::from);

            match self
                .manager
                .summon(
                    rune.task.clone(),
                    Some(rune.name.clone()),
                    rune.model.clone(),
                    cwd,
                    rune.provider.clone(),
                )
                .await
            {
                Ok(agent) => {
                    self.db.update_rune_agent(&rune.id, &agent.id)?;
                    info!(
                        scroll_id = %scroll_id,
                        rune = %rune.name,
                        agent_id = %agent.id,
                        "Rune spawned"
                    );
                    spawned += 1;
                }
                Err(e) => {
                    error!(rune = %rune.name, error = %e, "Failed to spawn rune");
                    self.db.update_rune_state(&rune.id, &RuneState::Failed)?;
                    self.skip_downstream(&rune.id);
                }
            }
        }

        Ok(())
    }

    fn validate_dag(&self, scroll_id: &str) -> anyhow::Result<()> {
        let edges = self.db.get_all_dependencies_for_scroll(scroll_id)?;
        let runes = self.db.get_runes_for_scroll(scroll_id)?;

        // Build adjacency list
        let mut adj: HashMap<String, Vec<String>> = HashMap::new();
        let all_ids: HashSet<String> = runes.iter().map(|r| r.id.clone()).collect();

        for id in &all_ids {
            adj.entry(id.clone()).or_default();
        }
        for (from, to) in &edges {
            adj.entry(from.clone()).or_default().push(to.clone());
        }

        // Topological sort via DFS to detect cycles
        let mut visited = HashSet::new();
        let mut in_stack = HashSet::new();

        fn dfs(
            node: &str,
            adj: &HashMap<String, Vec<String>>,
            visited: &mut HashSet<String>,
            in_stack: &mut HashSet<String>,
        ) -> bool {
            if in_stack.contains(node) {
                return true; // cycle
            }
            if visited.contains(node) {
                return false;
            }
            visited.insert(node.to_string());
            in_stack.insert(node.to_string());

            if let Some(neighbors) = adj.get(node) {
                for neighbor in neighbors {
                    if dfs(neighbor, adj, visited, in_stack) {
                        return true;
                    }
                }
            }

            in_stack.remove(node);
            false
        }

        for id in &all_ids {
            if dfs(id, &adj, &mut visited, &mut in_stack) {
                return Err(anyhow::anyhow!(
                    "Dependency cycle detected in scroll {}",
                    scroll_id
                ));
            }
        }

        Ok(())
    }

    fn detect_all_conflicts(&self, runes: &[Rune]) -> Vec<RuneConflict> {
        let mut conflicts = Vec::new();
        for i in 0..runes.len() {
            for j in (i + 1)..runes.len() {
                if let Some(c) = RuneConflict::detect(&runes[i], &runes[j]) {
                    conflicts.push(c);
                }
            }
        }
        conflicts
    }
}

pub struct InscribeResult {
    pub scroll: Scroll,
    pub rune_count: usize,
    pub conflicts: Vec<RuneConflict>,
}
