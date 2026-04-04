use anyhow::Result;
use chrono::Utc;
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Mutex;

use crate::shared::types::{Agent, AgentEvent, AgentState, Pact, PactState};

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

            CREATE TABLE IF NOT EXISTS pacts (
                id          TEXT PRIMARY KEY,
                source_id   TEXT NOT NULL REFERENCES agents(id),
                task_tpl    TEXT NOT NULL,
                name        TEXT,
                state       TEXT NOT NULL DEFAULT 'pending',
                target_id   TEXT,
                created_at  TEXT NOT NULL,
                fired_at    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_pacts_source_id
                ON pacts(source_id);
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

    pub fn update_agent_session_id(&self, id: &str, session_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE agents SET session_id = ?1, updated_at = ?2 WHERE id = ?3",
            params![session_id, now, id],
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

    #[allow(dead_code)]
    pub fn delete_agent(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM agent_events WHERE agent_id = ?1", params![id])?;
        conn.execute("DELETE FROM agents WHERE id = ?1", params![id])?;
        Ok(())
    }

    // --- Pact methods ---

    pub fn insert_pact(&self, pact: &Pact) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO pacts (id, source_id, task_tpl, name, state, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                pact.id,
                pact.source_id,
                pact.task_tpl,
                pact.name,
                pact.state.as_str(),
                pact.created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn list_pacts(&self, source_id: Option<&str>) -> Result<Vec<Pact>> {
        let conn = self.conn.lock().unwrap();
        let mut pacts = Vec::new();
        if let Some(sid) = source_id {
            let mut stmt = conn.prepare(
                "SELECT id, source_id, task_tpl, name, state, target_id, created_at, fired_at
                 FROM pacts WHERE source_id = ?1 ORDER BY created_at DESC",
            )?;
            let mut rows = stmt.query(params![sid])?;
            while let Some(row) = rows.next()? {
                pacts.push(row_to_pact(row)?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT id, source_id, task_tpl, name, state, target_id, created_at, fired_at
                 FROM pacts ORDER BY created_at DESC",
            )?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                pacts.push(row_to_pact(row)?);
            }
        }
        Ok(pacts)
    }

    pub fn get_pending_pacts_for_agent(&self, agent_id: &str) -> Result<Vec<Pact>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, source_id, task_tpl, name, state, target_id, created_at, fired_at
             FROM pacts WHERE source_id = ?1 AND state = 'pending'",
        )?;
        let mut rows = stmt.query(params![agent_id])?;
        let mut pacts = Vec::new();
        while let Some(row) = rows.next()? {
            pacts.push(row_to_pact(row)?);
        }
        Ok(pacts)
    }

    pub fn update_pact_fired(&self, id: &str, target_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE pacts SET state = 'fired', target_id = ?1, fired_at = ?2 WHERE id = ?3",
            params![target_id, now, id],
        )?;
        Ok(())
    }

    pub fn update_pact_failed(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE pacts SET state = 'failed', fired_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        Ok(())
    }

    /// Extract the final result text from an agent's output events.
    pub fn get_agent_output(&self, agent_id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT payload FROM agent_events
             WHERE agent_id = ?1 AND event_type = 'stdout'
             ORDER BY id DESC",
        )?;
        let mut rows = stmt.query(params![agent_id])?;
        while let Some(row) = rows.next()? {
            let payload: String = row.get(0)?;
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&payload) {
                if v.get("type").and_then(|t| t.as_str()) == Some("result") {
                    if let Some(result) = v.get("result").and_then(|r| r.as_str()) {
                        return Ok(Some(result.to_string()));
                    }
                }
            }
        }
        Ok(None)
    }
}

fn row_to_pact(row: &rusqlite::Row) -> Result<Pact> {
    let state_str: String = row.get(4)?;
    let state = PactState::from_str(&state_str).unwrap_or(PactState::Failed);
    let created_str: String = row.get(6)?;
    let fired_str: Option<String> = row.get(7)?;

    Ok(Pact {
        id: row.get(0)?,
        source_id: row.get(1)?,
        task_tpl: row.get(2)?,
        name: row.get(3)?,
        state,
        target_id: row.get(5)?,
        created_at: chrono::DateTime::parse_from_rfc3339(&created_str)
            .unwrap()
            .with_timezone(&chrono::Utc),
        fired_at: fired_str
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .map(|d| d.with_timezone(&chrono::Utc)),
    })
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
