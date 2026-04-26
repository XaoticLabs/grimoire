// RED tests for worker-pool spec, Task 7: `agents.worker_id` column.
//
// Spec lists this as `tests/database.rs (extend)`, but extending the existing
// passing file with non-compiling code would break it. These tests live in
// their own file and merge into `database.rs` once the column lands.

use grimoire::daemon::persistence::Database;
use grimoire::shared::types::{Agent, AgentState};
use rusqlite::Connection;

fn fresh_agent(id: &str, worker_id: Option<&str>) -> Agent {
    Agent {
        id: id.to_string(),
        name: None,
        state: AgentState::Active,
        task: Some("noop".to_string()),
        model: None,
        provider: Some("claude".to_string()),
        cwd: std::path::PathBuf::from("/tmp"),
        pid: None,
        session_id: None,
        exit_code: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
        worker_id: worker_id.map(|s| s.to_string()),
    }
}

#[test]
fn db_migration_adds_worker_id_column() {
    // Arrange: open a DB at a path created with a hand-written pre-migration
    // schema that lacks `worker_id`. Then open via `Database::open` and
    // assert the migration ran.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("grimoire.db");

    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE agents (
                id TEXT PRIMARY KEY,
                name TEXT,
                state TEXT NOT NULL,
                task TEXT,
                model TEXT,
                provider TEXT,
                cwd TEXT NOT NULL,
                pid INTEGER,
                session_id TEXT,
                exit_code INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );",
        )
        .unwrap();
    }

    let _db = Database::open(&path).unwrap();

    let conn = Connection::open(&path).unwrap();
    let cols: Vec<String> = conn
        .prepare("PRAGMA table_info(agents)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(
        cols.iter().any(|c| c == "worker_id"),
        "worker_id column should exist after migration; cols={cols:?}"
    );
}

#[test]
fn db_update_agent_worker_id_persists() {
    let db = Database::open_in_memory().unwrap();
    let agent = fresh_agent("a-1", None);
    db.insert_agent(&agent).unwrap();

    db.update_agent_worker_id("a-1", Some("w-42")).unwrap();
    let reloaded = db.get_agent("a-1").unwrap().expect("agent exists");
    assert_eq!(reloaded.worker_id.as_deref(), Some("w-42"));
}

#[test]
fn db_agent_with_null_worker_id_loads_as_none() {
    let db = Database::open_in_memory().unwrap();
    let agent = fresh_agent("a-2", None);
    db.insert_agent(&agent).unwrap();

    let reloaded = db.get_agent("a-2").unwrap().unwrap();
    assert!(reloaded.worker_id.is_none(), "default worker_id is None");
}
