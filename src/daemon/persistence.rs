use anyhow::Result;
use chrono::Utc;
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Mutex;

use crate::shared::types::{Agent, AgentEvent, AgentState};

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS agents (
                id          TEXT PRIMARY KEY,
                name        TEXT,
                state       TEXT NOT NULL,
                task        TEXT,
                model       TEXT,
                cwd         TEXT NOT NULL,
                pid         INTEGER,
                session_id  TEXT,
                exit_code   INTEGER,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS agent_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id    TEXT NOT NULL REFERENCES agents(id),
                event_type  TEXT NOT NULL,
                payload     TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_agent_events_agent_id
                ON agent_events(agent_id);
            ",
        )?;
        Ok(())
    }

    pub fn insert_agent(&self, agent: &Agent) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO agents (id, name, state, task, model, cwd, pid, session_id, exit_code, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                agent.id,
                agent.name,
                agent.state.as_str(),
                agent.task,
                agent.model,
                agent.cwd.to_string_lossy().to_string(),
                agent.pid,
                agent.session_id,
                agent.exit_code,
                agent.created_at.to_rfc3339(),
                agent.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn update_agent_state(
        &self,
        id: &str,
        state: &AgentState,
        exit_code: Option<i32>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE agents SET state = ?1, exit_code = ?2, updated_at = ?3 WHERE id = ?4",
            params![state.as_str(), exit_code, now, id],
        )?;
        Ok(())
    }

    pub fn update_agent_pid(&self, id: &str, pid: u32) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE agents SET pid = ?1, updated_at = ?2 WHERE id = ?3",
            params![pid, now, id],
        )?;
        Ok(())
    }

    pub fn get_agent(&self, id: &str) -> Result<Option<Agent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, state, task, model, cwd, pid, session_id, exit_code, created_at, updated_at
             FROM agents WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_agent(row)?)),
            None => Ok(None),
        }
    }

    pub fn list_agents(&self, state_filter: Option<&str>) -> Result<Vec<Agent>> {
        let conn = self.conn.lock().unwrap();
        let mut agents = Vec::new();
        if let Some(state) = state_filter {
            let mut stmt = conn.prepare(
                "SELECT id, name, state, task, model, cwd, pid, session_id, exit_code, created_at, updated_at
                 FROM agents WHERE state = ?1 ORDER BY created_at DESC",
            )?;
            let mut rows = stmt.query(params![state])?;
            while let Some(row) = rows.next()? {
                agents.push(row_to_agent(row)?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT id, name, state, task, model, cwd, pid, session_id, exit_code, created_at, updated_at
                 FROM agents ORDER BY created_at DESC",
            )?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                agents.push(row_to_agent(row)?);
            }
        }
        Ok(agents)
    }

    pub fn insert_event(&self, event: &AgentEvent) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO agent_events (agent_id, event_type, payload, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                event.agent_id,
                event.event_type,
                event.payload,
                event.created_at.to_rfc3339(),
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get_events(&self, agent_id: &str, tail: Option<usize>) -> Result<Vec<AgentEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut events = Vec::new();
        let query = if let Some(limit) = tail {
            format!(
                "SELECT id, agent_id, event_type, payload, created_at
                 FROM agent_events WHERE agent_id = ?1
                 ORDER BY id DESC LIMIT {}",
                limit
            )
        } else {
            "SELECT id, agent_id, event_type, payload, created_at
             FROM agent_events WHERE agent_id = ?1
             ORDER BY id ASC"
                .to_string()
        };
        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query(params![agent_id])?;
        while let Some(row) = rows.next()? {
            events.push(AgentEvent {
                id: Some(row.get(0)?),
                agent_id: row.get(1)?,
                event_type: row.get(2)?,
                payload: row.get(3)?,
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)
                    .unwrap()
                    .with_timezone(&chrono::Utc),
            });
        }
        if tail.is_some() {
            events.reverse();
        }
        Ok(events)
    }

    pub fn delete_agent(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM agent_events WHERE agent_id = ?1", params![id])?;
        conn.execute("DELETE FROM agents WHERE id = ?1", params![id])?;
        Ok(())
    }
}

fn row_to_agent(row: &rusqlite::Row) -> Result<Agent> {
    let state_str: String = row.get(2)?;
    let state = AgentState::from_str(&state_str).unwrap_or(AgentState::Failed);
    let cwd_str: String = row.get(5)?;
    let created_str: String = row.get(9)?;
    let updated_str: String = row.get(10)?;

    Ok(Agent {
        id: row.get(0)?,
        name: row.get(1)?,
        state,
        task: row.get(3)?,
        model: row.get(4)?,
        cwd: std::path::PathBuf::from(cwd_str),
        pid: row.get::<_, Option<u32>>(6)?,
        session_id: row.get(7)?,
        exit_code: row.get(8)?,
        created_at: chrono::DateTime::parse_from_rfc3339(&created_str)
            .unwrap()
            .with_timezone(&chrono::Utc),
        updated_at: chrono::DateTime::parse_from_rfc3339(&updated_str)
            .unwrap()
            .with_timezone(&chrono::Utc),
    })
}
