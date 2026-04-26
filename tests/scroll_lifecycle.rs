//! Integration tests for the scroll lifecycle.
//!
//! Tests the end-to-end flow: parse a markdown scroll spec, inscribe it
//! into the database via ScrollKeeper, and verify the resulting DAG
//! structure, state assignments, and conflict detection.

use std::sync::Arc;

use grimoire::daemon::event_bus::EventBus;
use grimoire::daemon::persistence::Database;
use grimoire::daemon::scroll_keeper::ScrollKeeper;
use grimoire::daemon::scroll_parser;
use grimoire::shared::config::Config;
use grimoire::shared::types::*;

/// Create a ScrollKeeper backed by an in-memory DB.
/// The AgentManager is required but we won't activate any scrolls in these
/// tests (that would require a real provider), so we create one with defaults.
async fn setup() -> (Arc<Database>, ScrollKeeper) {
    let db = Arc::new(Database::open_in_memory().unwrap());
    let event_bus = EventBus::new(db.clone());
    let config = Config::default();
    let manager =
        grimoire::daemon::agent_manager::AgentManager::new(db.clone(), event_bus, config).await;
    let keeper = ScrollKeeper::new(db.clone(), manager);
    (db, keeper)
}

// ---------------------------------------------------------------------------
// Parse → inscribe → verify DB state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inscribe_basic_scroll() {
    let (db, keeper) = setup().await;

    let spec_content = r#"# Scroll: Integration Test

## Task: Database Setup
- files: migrations/, src/db.rs

Create the database schema.

## Task: API Layer
- files: src/api.rs
- depends: Database Setup

Build REST endpoints.

## Task: Frontend
- files: src/ui.tsx
- depends: API Layer
- provider: aider

Build the frontend.
"#;

    let spec = scroll_parser::parse_scroll(spec_content).unwrap();
    assert_eq!(spec.tasks.len(), 3);

    let result = keeper.inscribe(spec, Some(2), Some("/tmp/test.md".to_string())).unwrap();

    assert_eq!(result.scroll.name, "Integration Test");
    assert_eq!(result.scroll.state, ScrollState::Inscribed);
    assert_eq!(result.scroll.max_concurrency, 2);
    assert_eq!(result.task_count, 3);
    assert!(result.conflicts.is_empty()); // no overlapping files

    // Verify tasks in DB
    let tasks = db.get_tasks_for_scroll(&result.scroll.id).unwrap();
    assert_eq!(tasks.len(), 3);

    // First task has no deps → Ready
    let db_setup = tasks.iter().find(|r| r.name == "Database Setup").unwrap();
    assert_eq!(db_setup.state, TaskState::Ready);
    assert_eq!(db_setup.file_patterns, vec!["migrations/", "src/db.rs"]);

    // Second task depends on first → Blocked
    let api = tasks.iter().find(|r| r.name == "API Layer").unwrap();
    assert_eq!(api.state, TaskState::Blocked);
    let api_deps = db.get_task_dependencies(&api.id).unwrap();
    assert_eq!(api_deps.len(), 1);
    assert_eq!(api_deps[0], db_setup.id);

    // Third task has provider override
    let frontend = tasks.iter().find(|r| r.name == "Frontend").unwrap();
    assert_eq!(frontend.provider.as_deref(), Some("aider"));
    assert_eq!(frontend.state, TaskState::Blocked);
}

// ---------------------------------------------------------------------------
// Conflict detection: overlapping file patterns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inscribe_detects_file_conflicts() {
    let (_, keeper) = setup().await;

    let spec_content = r#"# Scroll: Conflict Test

## Task: Task A
- files: src/shared.rs, src/a.rs

Work on A.

## Task: Task B
- files: src/shared.rs, src/b.rs

Work on B.
"#;

    let spec = scroll_parser::parse_scroll(spec_content).unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();

    assert_eq!(result.conflicts.len(), 1);
    assert_eq!(result.conflicts[0].overlapping_patterns, vec!["src/shared.rs"]);
}

// ---------------------------------------------------------------------------
// Multiple independent tasks start as Ready
// ---------------------------------------------------------------------------

#[tokio::test]
async fn independent_tasks_all_ready() {
    let (db, keeper) = setup().await;

    let spec_content = r#"# Scroll: Parallel

## Task: Alpha

Do alpha.

## Task: Beta

Do beta.

## Task: Gamma

Do gamma.
"#;

    let spec = scroll_parser::parse_scroll(spec_content).unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();

    let tasks = db.get_tasks_for_scroll(&result.scroll.id).unwrap();
    assert_eq!(tasks.len(), 3);
    for task in &tasks {
        assert_eq!(task.state, TaskState::Ready, "Task '{}' should be Ready", task.name);
    }
}

// ---------------------------------------------------------------------------
// Diamond dependency: A → B, A → C, B+C → D
// ---------------------------------------------------------------------------

#[tokio::test]
async fn diamond_dependency_graph() {
    let (db, keeper) = setup().await;

    let spec_content = r#"# Scroll: Diamond

## Task: A

Root task.

## Task: B
- depends: A

B after A.

## Task: C
- depends: A

C after A.

## Task: D
- depends: B, C

D after both B and C.
"#;

    let spec = scroll_parser::parse_scroll(spec_content).unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();

    let tasks = db.get_tasks_for_scroll(&result.scroll.id).unwrap();
    assert_eq!(tasks.len(), 4);

    let a = tasks.iter().find(|r| r.name == "A").unwrap();
    let b = tasks.iter().find(|r| r.name == "B").unwrap();
    let c = tasks.iter().find(|r| r.name == "C").unwrap();
    let d = tasks.iter().find(|r| r.name == "D").unwrap();

    assert_eq!(a.state, TaskState::Ready);
    assert_eq!(b.state, TaskState::Blocked);
    assert_eq!(c.state, TaskState::Blocked);
    assert_eq!(d.state, TaskState::Blocked);

    // D depends on both B and C
    let d_deps = db.get_task_dependencies(&d.id).unwrap();
    assert_eq!(d_deps.len(), 2);
    assert!(d_deps.contains(&b.id));
    assert!(d_deps.contains(&c.id));

    // A has two dependents: B and C
    let a_dependents = db.get_task_dependents(&a.id).unwrap();
    assert_eq!(a_dependents.len(), 2);

    // Simulate A completes → B and C should become ready
    db.update_task_state(&a.id, &TaskState::Complete).unwrap();
    let ready = db.find_ready_tasks(&result.scroll.id).unwrap();
    assert_eq!(ready.len(), 2);
    let ready_names: Vec<&str> = ready.iter().map(|r| r.name.as_str()).collect();
    assert!(ready_names.contains(&"B"));
    assert!(ready_names.contains(&"C"));

    // D still blocked (B and C not complete)
    assert!(!ready_names.contains(&"D"));

    // Complete B and C → D becomes ready
    db.update_task_state(&b.id, &TaskState::Complete).unwrap();
    db.update_task_state(&c.id, &TaskState::Complete).unwrap();
    let ready = db.find_ready_tasks(&result.scroll.id).unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].name, "D");
}

// ---------------------------------------------------------------------------
// Scroll status reporting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scroll_status_counts() {
    let (db, keeper) = setup().await;

    let spec_content = r#"# Scroll: Status Test

## Task: A

Do A.

## Task: B
- depends: A

Do B.

## Task: C
- depends: A

Do C.
"#;

    let spec = scroll_parser::parse_scroll(spec_content).unwrap();
    let result = keeper.inscribe(spec, None, None).unwrap();
    let scroll_id = &result.scroll.id;

    let status = keeper.status(scroll_id).unwrap();
    assert_eq!(status.total, 3);
    assert_eq!(status.ready, 1);  // A
    assert_eq!(status.blocked, 2); // B, C
    assert_eq!(status.complete, 0);
    assert_eq!(status.active, 0);
    assert_eq!(status.failed, 0);

    // Complete A
    let tasks = db.get_tasks_for_scroll(scroll_id).unwrap();
    let a = tasks.iter().find(|r| r.name == "A").unwrap();
    db.update_task_state(&a.id, &TaskState::Complete).unwrap();

    let status = keeper.status(scroll_id).unwrap();
    assert_eq!(status.complete, 1);
    // B and C are still blocked in state but their deps are met
    // (find_ready_tasks would find them, but their TaskState is still Blocked)
    assert_eq!(status.blocked, 2);
}
