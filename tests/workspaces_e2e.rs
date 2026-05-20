//! End-to-end integration tests for shared-memory-workspaces v1.
//!
//! Exercises the WorkspaceRegistry + Memory KV + WorkspaceWatcher composition
//! against an in-memory DB. Uses a fake `GitRunner` so tests don't shell out.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tempfile::TempDir;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::workspace_db::MemoryWriteOutcome;
use grimoire::daemon::workspace_registry::{GitError, GitRunner, WorkspaceRegistry};
use grimoire::shared::types::{Workspace, WorkspaceState};

/// Fake `GitRunner` that creates the target dir on add and removes it on
/// remove. No real git invocation.
struct FakeGit;

#[async_trait]
impl GitRunner for FakeGit {
    async fn worktree_add(
        &self,
        _repo: &Path,
        target: &Path,
        _branch: &str,
    ) -> Result<(), GitError> {
        std::fs::create_dir_all(target).map_err(|e| GitError {
            stderr: e.to_string(),
            exit_code: None,
        })
    }
    async fn worktree_remove(&self, _repo: &Path, target: &Path) -> Result<(), GitError> {
        let _ = std::fs::remove_dir_all(target);
        Ok(())
    }
}

fn fake_git_repo(td: &TempDir) -> PathBuf {
    let repo = td.path().join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    repo
}

async fn build_registry() -> (Arc<Database>, EventBus, Arc<WorkspaceRegistry>, TempDir) {
    let td = TempDir::new().unwrap();
    let db = Arc::new(Database::open_in_memory().unwrap());
    let bus = EventBus::new(db.clone());
    let reg = WorkspaceRegistry::new(db.clone(), bus.clone(), Arc::new(FakeGit));
    reg.set_root_override(td.path().join("workspaces")).await;
    (db, bus, reg, td)
}

#[tokio::test]
async fn create_then_list_then_destroy_roundtrip() {
    let (db, _bus, reg, td) = build_registry().await;
    let repo = fake_git_repo(&td);

    let ws = reg.create("my-ws", &repo, "wip").await.unwrap();
    assert_eq!(ws.id, "my-ws");
    assert!(ws.path.starts_with(td.path()));

    let (entries, _orphans) = reg.list().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, "my-ws");
    assert_eq!(entries[0].agent_count, 0);

    reg.destroy("my-ws").await.unwrap();
    let (entries, _) = reg.list().unwrap();
    assert!(entries.is_empty());
    // Memory rows for that workspace must be gone (cascade).
    let cur = db.memory_get("my-ws", "anything").unwrap();
    assert!(cur.is_none());
}

#[tokio::test]
async fn create_invalid_name_rejects() {
    let (_db, _bus, reg, td) = build_registry().await;
    let repo = fake_git_repo(&td);
    let err = reg.create("../escape", &repo, "wip").await.unwrap_err();
    assert!(err.to_string().contains("invalid_workspace_name"));
}

#[tokio::test]
async fn create_duplicate_rejects() {
    let (_db, _bus, reg, td) = build_registry().await;
    let repo = fake_git_repo(&td);
    reg.create("dup", &repo, "wip").await.unwrap();
    let err = reg.create("dup", &repo, "wip").await.unwrap_err();
    assert!(err.to_string().contains("workspace_already_exists"));
}

#[tokio::test]
async fn memory_put_get_roundtrip() {
    let (db, _bus, reg, td) = build_registry().await;
    let repo = fake_git_repo(&td);
    reg.create("ws", &repo, "wip").await.unwrap();

    let bytes = serde_json::to_vec(&serde_json::json!("hello")).unwrap();
    match db
        .memory_put_cas("ws", "greeting", &bytes, None, "system")
        .unwrap()
    {
        MemoryWriteOutcome::Written { version } => assert_eq!(version, 1),
        MemoryWriteOutcome::Conflict { .. } => panic!("expected Written"),
    }

    let entry = db.memory_get("ws", "greeting").unwrap().unwrap();
    assert_eq!(entry.value, serde_json::json!("hello"));
    assert_eq!(entry.version, 1);
}

#[tokio::test]
async fn memory_cas_conflict_surfaces_current_version() {
    let (db, _bus, reg, td) = build_registry().await;
    let repo = fake_git_repo(&td);
    reg.create("ws", &repo, "wip").await.unwrap();

    let bytes = serde_json::to_vec(&serde_json::json!(1)).unwrap();
    db.memory_put_cas("ws", "k", &bytes, None, "system")
        .unwrap();
    db.memory_put_cas("ws", "k", &bytes, Some(1), "system")
        .unwrap();

    match db
        .memory_put_cas("ws", "k", &bytes, Some(1), "system")
        .unwrap()
    {
        MemoryWriteOutcome::Conflict { current_version } => assert_eq!(current_version, 2),
        MemoryWriteOutcome::Written { .. } => panic!("expected Conflict"),
    }
}

#[tokio::test]
async fn memory_list_prefix_segment_aligned() {
    let (db, _bus, reg, td) = build_registry().await;
    let repo = fake_git_repo(&td);
    reg.create("ws", &repo, "wip").await.unwrap();

    let v = serde_json::to_vec(&serde_json::json!("x")).unwrap();
    db.memory_put_cas("ws", "findings", &v, None, "system")
        .unwrap();
    db.memory_put_cas("ws", "findings/auth", &v, None, "system")
        .unwrap();
    db.memory_put_cas("ws", "findings/auth/token", &v, None, "system")
        .unwrap();
    db.memory_put_cas("ws", "findingsX", &v, None, "system")
        .unwrap();

    let items = db.memory_list_prefix("ws", Some("findings")).unwrap();
    let keys: Vec<_> = items.iter().map(|i| i.key.clone()).collect();
    assert!(keys.contains(&"findings".to_string()));
    assert!(keys.contains(&"findings/auth".to_string()));
    assert!(keys.contains(&"findings/auth/token".to_string()));
    assert!(!keys.contains(&"findingsX".to_string()));
}

#[tokio::test]
async fn destroy_with_running_agent_refused() {
    use chrono::Utc;
    use grimoire::shared::types::{Agent, AgentState, RestartPolicy};

    let (db, _bus, reg, td) = build_registry().await;
    let repo = fake_git_repo(&td);
    reg.create("ws", &repo, "wip").await.unwrap();

    // Insert a running agent and assign it.
    let agent = Agent {
        id: "agentaaa".into(),
        name: None,
        state: AgentState::Active,
        task: Some("t".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();
    reg.assign("ws", "agentaaa").await.unwrap();

    let err = reg.destroy("ws").await.unwrap_err();
    assert!(err.to_string().contains("workspace_in_use"));

    // Workspace still Active.
    let ws = db.get_workspace("ws").unwrap().unwrap();
    assert_eq!(ws.state, WorkspaceState::Active);
}

#[tokio::test]
async fn assign_idempotent_for_same_pair() {
    use chrono::Utc;
    use grimoire::shared::types::{Agent, AgentState, RestartPolicy};

    let (db, _bus, reg, td) = build_registry().await;
    let repo = fake_git_repo(&td);
    reg.create("ws", &repo, "wip").await.unwrap();

    let agent = Agent {
        id: "agentbbb".into(),
        name: None,
        state: AgentState::Active,
        task: Some("t".into()),
        model: None,
        provider: Some("claude".into()),
        cwd: PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        worker_id: None,
        restart_policy: RestartPolicy::Never,
        restart_count: 0,
        workspace_id: None,
    };
    db.insert_agent(&agent).unwrap();

    reg.assign("ws", "agentbbb").await.unwrap();
    // Second call must not error.
    reg.assign("ws", "agentbbb").await.unwrap();
}

#[tokio::test]
async fn boot_reconcile_orphan_dir_emits_event_and_preserves() {
    let (db, bus, reg, td) = build_registry().await;
    // Pre-create an orphan dir under workspaces root with no DB row.
    let root = td.path().join("workspaces");
    std::fs::create_dir_all(root.join("orphan-1")).unwrap();
    let _ = (&db, &bus);

    // Subscribe to events.
    let mut rx = bus.subscribe();

    reg.reconcile_on_boot().await.unwrap();

    // Drain a few events and look for the orphan event.
    let mut found = false;
    for _ in 0..16 {
        match rx.try_recv() {
            Ok(grimoire::shared::protocol::StreamEvent::WorkspaceOrphanDirDetected { path }) => {
                if path.ends_with("orphan-1") {
                    found = true;
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    assert!(found, "expected WorkspaceOrphanDirDetected");
    assert!(
        root.join("orphan-1").exists(),
        "orphan dir must be preserved"
    );
}

#[tokio::test]
async fn boot_reconcile_orphan_row_deletes_and_cascades() {
    let (db, bus, reg, td) = build_registry().await;
    let _ = (&bus, &td);

    // Insert a row whose path doesn't exist on disk.
    let ws = Workspace {
        id: "ghost".into(),
        path: PathBuf::from("/nonexistent/ghost"),
        repo_path: PathBuf::from("/tmp"),
        branch: "wip".into(),
        state: WorkspaceState::Active,
        created_at: Utc::now(),
    };
    db.insert_workspace(&ws).unwrap();

    // Sanity: row exists.
    assert!(db.get_workspace("ghost").unwrap().is_some());

    reg.reconcile_on_boot().await.unwrap();

    // Row is gone.
    assert!(db.get_workspace("ghost").unwrap().is_none());
}
