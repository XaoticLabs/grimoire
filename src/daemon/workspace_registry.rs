//! `WorkspaceRegistry` — daemon-internal owner of workspace lifecycle.
//!
//! Mirrors the `WakeRegistry` shape (`Arc<Self>`, mutex-protected handle map,
//! shells out via a `GitRunner` seam so tests can swap the git binary). Owns
//! the `WorkspaceWatcher` handles for active workspaces; lazy-starts a watcher
//! on first assign, stops on destroy.

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::shared::constants;
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Workspace, WorkspaceListEntry, WorkspaceState, validate_workspace_id};

use super::event_bus::EventBus;
use super::persistence::Database;
use super::workspace_watcher::WorkspaceWatcherHandle;

/// Errors enumerated as RPC code strings — keep stable for client matching.
#[derive(Debug)]
pub struct WorkspaceError(pub String);

impl std::fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for WorkspaceError {}

#[async_trait]
pub trait GitRunner: Send + Sync {
    async fn worktree_add(&self, repo: &Path, target: &Path, branch: &str) -> Result<(), GitError>;
    async fn worktree_remove(&self, repo: &Path, target: &Path) -> Result<(), GitError>;
}

#[derive(Debug, Clone)]
pub struct GitError {
    pub stderr: String,
    pub exit_code: Option<i32>,
}

impl std::fmt::Display for GitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "git failed (exit {:?}): {}", self.exit_code, self.stderr)
    }
}

/// Production `GitRunner` shelling out to the system `git` binary.
pub struct SystemGitRunner;

#[async_trait]
impl GitRunner for SystemGitRunner {
    async fn worktree_add(&self, repo: &Path, target: &Path, branch: &str) -> Result<(), GitError> {
        let output = tokio::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .arg("worktree")
            .arg("add")
            .arg(target)
            .arg(branch)
            .output()
            .await
            .map_err(|e| GitError {
                stderr: e.to_string(),
                exit_code: None,
            })?;
        if !output.status.success() {
            return Err(GitError {
                stderr: String::from_utf8_lossy(&output.stderr)
                    .chars()
                    .take(4096)
                    .collect(),
                exit_code: output.status.code(),
            });
        }
        Ok(())
    }

    async fn worktree_remove(&self, repo: &Path, target: &Path) -> Result<(), GitError> {
        let output = tokio::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .arg("worktree")
            .arg("remove")
            .arg("--force")
            .arg(target)
            .output()
            .await
            .map_err(|e| GitError {
                stderr: e.to_string(),
                exit_code: None,
            })?;
        if !output.status.success() {
            return Err(GitError {
                stderr: String::from_utf8_lossy(&output.stderr)
                    .chars()
                    .take(4096)
                    .collect(),
                exit_code: output.status.code(),
            });
        }
        Ok(())
    }
}

pub struct WorkspaceRegistry {
    db: Arc<Database>,
    bus: EventBus,
    git: Arc<dyn GitRunner>,
    watchers: Mutex<HashMap<String, WorkspaceWatcherHandle>>,
    /// Override workspaces root (tests substitute a tempdir).
    root_override: Mutex<Option<PathBuf>>,
}

impl WorkspaceRegistry {
    pub fn new(db: Arc<Database>, bus: EventBus, git: Arc<dyn GitRunner>) -> Arc<Self> {
        Arc::new(Self {
            db,
            bus,
            git,
            watchers: Mutex::new(HashMap::new()),
            root_override: Mutex::new(None),
        })
    }

    pub fn with_default_git(db: Arc<Database>, bus: EventBus) -> Arc<Self> {
        Self::new(db, bus, Arc::new(SystemGitRunner))
    }

    /// Override the workspaces-root directory. Test-only seam.
    pub async fn set_root_override(&self, root: PathBuf) {
        *self.root_override.lock().await = Some(root);
    }

    async fn workspaces_root(&self) -> PathBuf {
        if let Some(root) = self.root_override.lock().await.as_ref() {
            return root.clone();
        }
        constants::workspaces_root()
    }

    /// Provision a new workspace. Returns the inserted row.
    ///
    /// Order of operations:
    /// 1. Name validation (rejects with `invalid_workspace_name`).
    /// 2. DB pre-check on uniqueness (rejects with `workspace_already_exists`).
    /// 3. Filesystem pre-check on path occupancy (rejects with `workspace_path_occupied`).
    /// 4. `git worktree add` shellout (rejects with `git_worktree_add_failed`).
    /// 5. DB insert. On race, rolls back via `git worktree remove --force`.
    pub async fn create(
        &self,
        name: &str,
        repo_path: &Path,
        branch: &str,
    ) -> Result<Workspace, WorkspaceError> {
        validate_workspace_id(name)
            .map_err(|e| WorkspaceError(format!("invalid_workspace_name:{e}")))?;

        // Repo validation.
        let repo_canonical = std::fs::canonicalize(repo_path)
            .map_err(|_| WorkspaceError("invalid_repo_path".into()))?;
        if !repo_canonical.is_dir() {
            return Err(WorkspaceError("invalid_repo_path".into()));
        }
        if !repo_canonical.join(".git").exists() {
            return Err(WorkspaceError("not_a_git_repo".into()));
        }

        let root = self.workspaces_root().await;
        let _ = std::fs::create_dir_all(&root);
        let target = root.join(name);

        // DB pre-check.
        if self
            .db
            .get_workspace(name)
            .map_err(|e| WorkspaceError(format!("db:{e}")))?
            .is_some()
        {
            return Err(WorkspaceError("workspace_already_exists".into()));
        }
        if target.exists() {
            return Err(WorkspaceError("workspace_path_occupied".into()));
        }

        // Shell out.
        if let Err(ge) = self
            .git
            .worktree_add(&repo_canonical, &target, branch)
            .await
        {
            return Err(WorkspaceError(format!(
                "git_worktree_add_failed:{}",
                ge.stderr
            )));
        }

        let ws = Workspace {
            id: name.to_string(),
            path: target.clone(),
            repo_path: repo_canonical.clone(),
            branch: branch.to_string(),
            state: WorkspaceState::Active,
            created_at: Utc::now(),
            kind: crate::shared::types::WorkspaceKind::Local,
            home_daemon_id: None,
            home_workspace_id: None,
        };

        if let Err(e) = self.db.insert_workspace(&ws) {
            // Best-effort rollback: leave the worktree in place if removal
            // fails. Reconciliation on next boot will catch it.
            let _ = self.git.worktree_remove(&repo_canonical, &target).await;
            return Err(WorkspaceError(format!("db_insert_failed:{e}")));
        }

        self.bus.publish(StreamEvent::WorkspaceCreated {
            workspace_id: ws.id.clone(),
            path: ws.path.clone(),
            branch: ws.branch.clone(),
        });
        Ok(ws)
    }

    pub fn list(&self) -> Result<(Vec<WorkspaceListEntry>, Vec<String>)> {
        let entries = self.db.list_workspaces_with_counts()?;
        // Orphans are computed by reconcile; we expose them via a side cache
        // when reconcile runs. For v1, expose empty here and let `--orphans`
        // hit a dedicated reconcile-style scan.
        Ok((entries, Vec::new()))
    }

    /// Refuse-if-in-use destroy. Transitions Active → Destroying → gone.
    pub async fn destroy(&self, id: &str) -> Result<(), WorkspaceError> {
        let ws = match self.db.get_workspace(id) {
            Ok(Some(w)) => w,
            Ok(None) => return Err(WorkspaceError("workspace_not_found".into())),
            Err(e) => return Err(WorkspaceError(format!("db:{e}"))),
        };

        // In-use precheck: any non-terminal agent assigned blocks destroy.
        let assigned = self
            .db
            .list_active_assigned_agents(id)
            .map_err(|e| WorkspaceError(format!("db:{e}")))?;
        let busy: Vec<String> = assigned
            .into_iter()
            .filter_map(|(agent_id, state_str)| {
                let st: crate::shared::types::AgentState = state_str.parse().ok()?;
                if st.is_terminal() {
                    None
                } else {
                    Some(agent_id)
                }
            })
            .collect();
        if !busy.is_empty() {
            return Err(WorkspaceError(format!(
                "workspace_in_use:{}",
                busy.join(",")
            )));
        }

        // Active → Destroying.
        if ws.state == WorkspaceState::Destroying {
            return Err(WorkspaceError("workspace_destroying".into()));
        }
        self.db
            .update_workspace_state(id, WorkspaceState::Destroying)
            .map_err(|e| WorkspaceError(format!("db:{e}")))?;

        // Stop watcher.
        if let Some(handle) = self.watchers.lock().await.remove(id) {
            handle.shutdown();
        }

        // Shell out remove (tolerate failure).
        let _ = self.git.worktree_remove(&ws.repo_path, &ws.path).await;

        self.db
            .delete_workspace_row(id)
            .map_err(|e| WorkspaceError(format!("db:{e}")))?;

        self.bus.publish(StreamEvent::WorkspaceDestroyed {
            workspace_id: id.to_string(),
        });
        Ok(())
    }

    /// Assign an agent to a workspace. Idempotent for the same `(workspace, agent)`.
    /// Rejects if agent is already assigned to a different workspace.
    pub async fn assign(&self, workspace_id: &str, agent_id: &str) -> Result<(), WorkspaceError> {
        let ws = match self.db.get_workspace(workspace_id) {
            Ok(Some(w)) => w,
            Ok(None) => return Err(WorkspaceError("workspace_not_found".into())),
            Err(e) => return Err(WorkspaceError(format!("db:{e}"))),
        };
        if ws.state != WorkspaceState::Active {
            return Err(WorkspaceError("workspace_destroying".into()));
        }

        if let Ok(Some(existing)) = self.db.agent_workspace_id(agent_id)
            && existing != workspace_id
        {
            return Err(WorkspaceError("agent_already_assigned".into()));
        }

        self.db
            .insert_workspace_assignment(workspace_id, agent_id)
            .map_err(|e| WorkspaceError(format!("db:{e}")))?;

        self.ensure_watcher_started(&ws).await;
        Ok(())
    }

    async fn ensure_watcher_started(&self, ws: &Workspace) {
        let mut handles = self.watchers.lock().await;
        if handles.contains_key(&ws.id) {
            return;
        }
        match super::workspace_watcher::WorkspaceWatcher::start(
            ws.id.clone(),
            ws.path.clone(),
            self.db.clone(),
            self.bus.clone(),
        ) {
            Ok(handle) => {
                handles.insert(ws.id.clone(), handle);
            }
            Err(e) => {
                tracing::warn!(workspace = %ws.id, error = %e, "failed to start workspace watcher");
            }
        }
    }

    /// Boot reconciliation: cross-check on-disk dirs with DB rows.
    /// - Orphan dir (no row): emit `WorkspaceOrphanDirDetected`, leave on disk.
    /// - Orphan row (no dir): mark Destroying, delete (cascade), emit destroyed.
    pub async fn reconcile_on_boot(&self) -> Result<()> {
        let root = self.workspaces_root().await;
        let _ = std::fs::create_dir_all(&root);

        let db_rows = self.db.list_workspace_paths().unwrap_or_default();
        let db_paths: HashMap<PathBuf, (String, WorkspaceState)> = db_rows
            .iter()
            .map(|(id, p, s)| (p.clone(), (id.clone(), *s)))
            .collect();

        // Orphan dirs.
        if let Ok(read) = std::fs::read_dir(&root) {
            for entry in read.flatten() {
                let p = entry.path();
                if !p.is_dir() {
                    continue;
                }
                if !db_paths.contains_key(&p) {
                    self.bus
                        .publish(StreamEvent::WorkspaceOrphanDirDetected { path: p.clone() });
                    tracing::warn!(path = %p.display(), "workspace orphan dir detected; leaving in place");
                }
            }
        }

        // Orphan rows.
        for (id, path, _state) in &db_rows {
            if !path.exists() {
                let _ = self
                    .db
                    .update_workspace_state(id, WorkspaceState::Destroying);
                if let Err(e) = self.db.delete_workspace_row(id) {
                    tracing::warn!(workspace = %id, error = %e, "delete orphan row failed");
                    continue;
                }
                self.bus.publish(StreamEvent::WorkspaceDestroyed {
                    workspace_id: id.clone(),
                });
            }
        }

        Ok(())
    }
}

// --- Helper: emit topic mail for memory writes ---

/// Publish topic-mail to every segment-prefix and the wildcard for a memory
/// write. Caller already wrote the row and emitted the `MemoryWritten`/Deleted
/// stream event; this fans out to subscribers via the existing mail plumbing.
pub fn publish_memory_topic_mail(
    db: &Database,
    bus: &EventBus,
    workspace_id: &str,
    key: &str,
    version: u64,
    op: &str, // "put" | "delete"
    sender_id: Option<&str>,
) -> Result<()> {
    use crate::shared::constants::generate_short_id;
    use crate::shared::types::{Mail, MailState};

    let body = serde_json::json!({
        "key": key,
        "version": version,
        "op": op,
    })
    .to_string();
    let _ = sender_id;

    let segments: Vec<&str> = key.split('/').collect();
    let mut topics: Vec<String> = Vec::with_capacity(segments.len() + 1);
    let mut prefix = String::new();
    for seg in &segments {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(seg);
        topics.push(format!("workspace/{workspace_id}/memory/{prefix}"));
    }
    topics.push(format!("workspace/{workspace_id}/memory/*"));

    let now = chrono::Utc::now().timestamp();
    let sender = format!("workspace://{workspace_id}");

    for topic in topics {
        let Ok(subscribers) = db.list_subscribers_for_topic(&topic) else {
            continue;
        };
        if subscribers.is_empty() {
            continue;
        }
        let mut mails: Vec<Mail> = Vec::with_capacity(subscribers.len());
        for sub in subscribers {
            let mail = Mail {
                id: generate_short_id(),
                recipient_id: sub.subscriber_id.clone(),
                sender_id: Some(sender.clone()),
                topic: Some(topic.clone()),
                body: body.clone(),
                in_reply_to: None,
                state: MailState::Pending,
                fail_reason: None,
                created_at: now,
                delivered_at: None,
                seq: 0,
                wake_eligible: true,
            };
            mails.push(mail);
        }
        if mails.is_empty() {
            continue;
        }
        if let Err(e) = db.insert_mail_batch(&mails) {
            tracing::warn!(error = %e, "memory topic mail insert failed");
            continue;
        }
        for mail in &mails {
            bus.publish(StreamEvent::MailReceived {
                mail_id: mail.id.clone(),
                recipient_id: mail.recipient_id.clone(),
                sender_id: Some(sender.clone()),
                topic: Some(topic.clone()),
                body_preview: body.chars().take(200).collect(),
                wake_eligible: true,
                origin_daemon_id: None,
            });
        }
    }
    Ok(())
}
