use anyhow::Result;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use std::path::Path;
use std::sync::Mutex;

use crate::shared::types::{
    Agent, AgentEvent, AgentState, Pact, PactState, Rune, RuneId, RuneState, Scroll, ScrollState,
};

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

    /// Open an in-memory database (for tests).
    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
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
                provider    TEXT,
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

        // Migration: add provider column if missing (for existing DBs)
        let has_provider: bool = conn
            .prepare("SELECT provider FROM agents LIMIT 0")
            .is_ok();
        if !has_provider {
            conn.execute_batch("ALTER TABLE agents ADD COLUMN provider TEXT;")?;
        }

        // Scroll tables
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS scrolls (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                state           TEXT NOT NULL DEFAULT 'inscribed',
                source_path     TEXT,
                max_concurrency INTEGER NOT NULL DEFAULT 4,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runes (
                id              TEXT PRIMARY KEY,
                scroll_id       TEXT NOT NULL REFERENCES scrolls(id),
                name            TEXT NOT NULL,
                task            TEXT NOT NULL,
                state           TEXT NOT NULL DEFAULT 'blocked',
                agent_id        TEXT,
                provider        TEXT,
                model           TEXT,
                cwd             TEXT,
                file_patterns   TEXT NOT NULL DEFAULT '[]',
                order_index     INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_runes_scroll_id ON runes(scroll_id);
            CREATE INDEX IF NOT EXISTS idx_runes_agent_id ON runes(agent_id);

            CREATE TABLE IF NOT EXISTS rune_dependencies (
                rune_id         TEXT NOT NULL REFERENCES runes(id),
                depends_on_id   TEXT NOT NULL REFERENCES runes(id),
                PRIMARY KEY (rune_id, depends_on_id)
            );
            ",
        )?;

        Ok(())
    }

    pub fn insert_agent(&self, agent: &Agent) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO agents (id, name, state, task, model, provider, cwd, pid, session_id, exit_code, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                agent.id,
                agent.name,
                agent.state.as_str(),
                agent.task,
                agent.model,
                agent.provider,
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
            "SELECT id, name, state, task, model, provider, cwd, pid, session_id, exit_code, created_at, updated_at
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
        let query = match state_filter {
            Some(_) => "SELECT id, name, state, task, model, provider, cwd, pid, session_id, exit_code, created_at, updated_at
                        FROM agents WHERE state = ?1 ORDER BY created_at DESC",
            None => "SELECT id, name, state, task, model, provider, cwd, pid, session_id, exit_code, created_at, updated_at
                     FROM agents ORDER BY created_at DESC",
        };
        let mut stmt = conn.prepare(query)?;
        let mut rows = match state_filter {
            Some(state) => stmt.query(params![state])?,
            None => stmt.query([])?,
        };
        let mut agents = Vec::new();
        while let Some(row) = rows.next()? {
            agents.push(row_to_agent(row)?);
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

    // --- Scroll methods ---

    pub fn insert_scroll(&self, scroll: &Scroll) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO scrolls (id, name, state, source_path, max_concurrency, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                scroll.id,
                scroll.name,
                scroll.state.as_str(),
                scroll.source_path,
                scroll.max_concurrency,
                scroll.created_at.to_rfc3339(),
                scroll.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_scroll(&self, id: &str) -> Result<Option<Scroll>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, state, source_path, max_concurrency, created_at, updated_at
             FROM scrolls WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_scroll(row)?)),
            None => Ok(None),
        }
    }

    pub fn list_scrolls(&self) -> Result<Vec<Scroll>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, state, source_path, max_concurrency, created_at, updated_at
             FROM scrolls ORDER BY created_at DESC",
        )?;
        let mut rows = stmt.query([])?;
        let mut scrolls = Vec::new();
        while let Some(row) = rows.next()? {
            scrolls.push(row_to_scroll(row)?);
        }
        Ok(scrolls)
    }

    pub fn update_scroll_state(&self, id: &str, state: &ScrollState) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE scrolls SET state = ?1, updated_at = ?2 WHERE id = ?3",
            params![state.as_str(), now, id],
        )?;
        Ok(())
    }

    // --- Rune methods ---

    pub fn insert_rune(&self, rune: &Rune) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let file_patterns_json = serde_json::to_string(&rune.file_patterns)?;
        conn.execute(
            "INSERT INTO runes (id, scroll_id, name, task, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                rune.id,
                rune.scroll_id,
                rune.name,
                rune.task,
                rune.state.as_str(),
                rune.agent_id,
                rune.provider,
                rune.model,
                rune.cwd,
                file_patterns_json,
                rune.order_index,
                rune.created_at.to_rfc3339(),
                rune.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn insert_rune_dependency(&self, rune_id: &str, depends_on_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO rune_dependencies (rune_id, depends_on_id) VALUES (?1, ?2)",
            params![rune_id, depends_on_id],
        )?;
        Ok(())
    }

    pub fn get_runes_for_scroll(&self, scroll_id: &str) -> Result<Vec<Rune>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, scroll_id, name, task, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at
             FROM runes WHERE scroll_id = ?1 ORDER BY order_index ASC",
        )?;
        let mut rows = stmt.query(params![scroll_id])?;
        let mut runes = Vec::new();
        while let Some(row) = rows.next()? {
            runes.push(row_to_rune(row)?);
        }
        Ok(runes)
    }

    pub fn get_rune_by_agent_id(&self, agent_id: &str) -> Result<Option<Rune>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, scroll_id, name, task, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at
             FROM runes WHERE agent_id = ?1",
        )?;
        let mut rows = stmt.query(params![agent_id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_rune(row)?)),
            None => Ok(None),
        }
    }

    pub fn update_rune_state(&self, id: &str, state: &RuneState) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE runes SET state = ?1, updated_at = ?2 WHERE id = ?3",
            params![state.as_str(), now, id],
        )?;
        Ok(())
    }

    pub fn update_rune_agent(&self, rune_id: &str, agent_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE runes SET agent_id = ?1, state = 'active', updated_at = ?2 WHERE id = ?3",
            params![agent_id, now, rune_id],
        )?;
        Ok(())
    }

    /// Get rune IDs that a rune depends on
    pub fn get_rune_dependencies(&self, rune_id: &str) -> Result<Vec<RuneId>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT depends_on_id FROM rune_dependencies WHERE rune_id = ?1",
        )?;
        let mut rows = stmt.query(params![rune_id])?;
        let mut deps = Vec::new();
        while let Some(row) = rows.next()? {
            deps.push(row.get(0)?);
        }
        Ok(deps)
    }

    /// Get rune IDs that depend on a given rune (downstream)
    pub fn get_rune_dependents(&self, rune_id: &str) -> Result<Vec<RuneId>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT rune_id FROM rune_dependencies WHERE depends_on_id = ?1",
        )?;
        let mut rows = stmt.query(params![rune_id])?;
        let mut deps = Vec::new();
        while let Some(row) = rows.next()? {
            deps.push(row.get(0)?);
        }
        Ok(deps)
    }

    /// Find blocked runes in a scroll where all dependencies are complete
    pub fn find_ready_runes(&self, scroll_id: &str) -> Result<Vec<Rune>> {
        let conn = self.conn.lock().unwrap();
        // Find runes that are blocked and have all dependencies in 'complete' state
        let mut stmt = conn.prepare(
            "SELECT r.id, r.scroll_id, r.name, r.task, r.state, r.agent_id, r.provider, r.model, r.cwd, r.file_patterns, r.order_index, r.created_at, r.updated_at
             FROM runes r
             WHERE r.scroll_id = ?1 AND r.state = 'blocked'
             AND NOT EXISTS (
                 SELECT 1 FROM rune_dependencies rd
                 JOIN runes dep ON dep.id = rd.depends_on_id
                 WHERE rd.rune_id = r.id AND dep.state != 'complete'
             )",
        )?;
        let mut rows = stmt.query(params![scroll_id])?;
        let mut runes = Vec::new();
        while let Some(row) = rows.next()? {
            runes.push(row_to_rune(row)?);
        }
        Ok(runes)
    }

    /// Count active runes in a scroll
    pub fn count_active_runes(&self, scroll_id: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM runes WHERE scroll_id = ?1 AND state = 'active'",
            params![scroll_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Get all dependency edges for a scroll (for cycle detection)
    pub fn get_all_dependencies_for_scroll(&self, scroll_id: &str) -> Result<Vec<(RuneId, RuneId)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT rd.rune_id, rd.depends_on_id
             FROM rune_dependencies rd
             JOIN runes r ON r.id = rd.rune_id
             WHERE r.scroll_id = ?1",
        )?;
        let mut rows = stmt.query(params![scroll_id])?;
        let mut edges = Vec::new();
        while let Some(row) = rows.next()? {
            edges.push((row.get(0)?, row.get(1)?));
        }
        Ok(edges)
    }
}

/// Parse an RFC3339 timestamp from a DB column, returning a proper error instead of panicking.
fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| anyhow::anyhow!("invalid timestamp '{}': {}", s, e))
}

fn row_to_scroll(row: &rusqlite::Row) -> Result<Scroll> {
    let state_str: String = row.get(2)?;
    let created_str: String = row.get(5)?;
    let updated_str: String = row.get(6)?;

    Ok(Scroll {
        id: row.get(0)?,
        name: row.get(1)?,
        state: state_str.parse().unwrap_or(ScrollState::Failed),
        source_path: row.get(3)?,
        max_concurrency: row.get(4)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
    })
}

fn row_to_rune(row: &rusqlite::Row) -> Result<Rune> {
    let state_str: String = row.get(4)?;
    let file_patterns_json: String = row.get(9)?;
    let created_str: String = row.get(11)?;
    let updated_str: String = row.get(12)?;

    Ok(Rune {
        id: row.get(0)?,
        scroll_id: row.get(1)?,
        name: row.get(2)?,
        task: row.get(3)?,
        state: state_str.parse().unwrap_or(RuneState::Failed),
        agent_id: row.get(5)?,
        provider: row.get(6)?,
        model: row.get(7)?,
        cwd: row.get(8)?,
        file_patterns: serde_json::from_str(&file_patterns_json).unwrap_or_default(),
        order_index: row.get(10)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
    })
}

fn row_to_pact(row: &rusqlite::Row) -> Result<Pact> {
    let state_str: String = row.get(4)?;
    let created_str: String = row.get(6)?;
    let fired_str: Option<String> = row.get(7)?;

    Ok(Pact {
        id: row.get(0)?,
        source_id: row.get(1)?,
        task_tpl: row.get(2)?,
        name: row.get(3)?,
        state: state_str.parse().unwrap_or(PactState::Failed),
        target_id: row.get(5)?,
        created_at: parse_timestamp(&created_str)?,
        fired_at: fired_str.as_deref().map(parse_timestamp).transpose()?,
    })
}

fn row_to_agent(row: &rusqlite::Row) -> Result<Agent> {
    let state_str: String = row.get(2)?;
    let cwd_str: String = row.get(6)?;
    let created_str: String = row.get(10)?;
    let updated_str: String = row.get(11)?;

    Ok(Agent {
        id: row.get(0)?,
        name: row.get(1)?,
        state: state_str.parse().unwrap_or(AgentState::Failed),
        task: row.get(3)?,
        model: row.get(4)?,
        provider: row.get(5)?,
        cwd: std::path::PathBuf::from(cwd_str),
        pid: row.get::<_, Option<u32>>(7)?,
        session_id: row.get(8)?,
        exit_code: row.get(9)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::types::*;
    use std::path::PathBuf;

    fn test_db() -> Database {
        Database::open_in_memory().unwrap()
    }

    fn make_agent(id: &str) -> Agent {
        Agent {
            id: id.to_string(),
            name: Some(format!("agent-{}", id)),
            state: AgentState::Active,
            task: Some("test task".to_string()),
            model: Some("sonnet".to_string()),
            provider: Some("claude".to_string()),
            cwd: PathBuf::from("/tmp"),
            pid: Some(1234),
            session_id: None,
            exit_code: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn make_scroll(id: &str) -> Scroll {
        Scroll {
            id: id.to_string(),
            name: format!("Scroll {}", id),
            state: ScrollState::Active,
            source_path: None,
            max_concurrency: 4,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn make_rune(id: &str, scroll_id: &str, state: RuneState) -> Rune {
        Rune {
            id: id.to_string(),
            scroll_id: scroll_id.to_string(),
            name: format!("Rune {}", id),
            task: "test".to_string(),
            state,
            agent_id: None,
            provider: None,
            model: None,
            cwd: None,
            file_patterns: vec![],
            order_index: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn agent_insert_and_get() {
        let db = test_db();
        let agent = make_agent("abc12345");
        db.insert_agent(&agent).unwrap();

        let fetched = db.get_agent("abc12345").unwrap().unwrap();
        assert_eq!(fetched.id, "abc12345");
        assert_eq!(fetched.name.as_deref(), Some("agent-abc12345"));
        assert_eq!(fetched.state, AgentState::Active);
        assert_eq!(fetched.provider.as_deref(), Some("claude"));
    }

    #[test]
    fn agent_not_found() {
        let db = test_db();
        assert!(db.get_agent("nonexistent").unwrap().is_none());
    }

    #[test]
    fn agent_list_and_filter() {
        let db = test_db();

        let mut a1 = make_agent("aaaa1111");
        a1.state = AgentState::Active;
        db.insert_agent(&a1).unwrap();

        let mut a2 = make_agent("bbbb2222");
        a2.state = AgentState::Complete;
        db.insert_agent(&a2).unwrap();

        assert_eq!(db.list_agents(None).unwrap().len(), 2);
        assert_eq!(db.list_agents(Some("active")).unwrap().len(), 1);
        assert_eq!(db.list_agents(Some("complete")).unwrap().len(), 1);
        assert_eq!(db.list_agents(Some("banished")).unwrap().len(), 0);
    }

    #[test]
    fn agent_state_transition() {
        let db = test_db();
        db.insert_agent(&make_agent("state111")).unwrap();

        db.update_agent_state("state111", &AgentState::Complete, Some(0))
            .unwrap();

        let fetched = db.get_agent("state111").unwrap().unwrap();
        assert_eq!(fetched.state, AgentState::Complete);
        assert_eq!(fetched.exit_code, Some(0));
    }

    #[test]
    fn agent_session_id_update() {
        let db = test_db();
        db.insert_agent(&make_agent("sess1111")).unwrap();
        db.update_agent_session_id("sess1111", "session-abc").unwrap();

        let fetched = db.get_agent("sess1111").unwrap().unwrap();
        assert_eq!(fetched.session_id.as_deref(), Some("session-abc"));
    }

    #[test]
    fn event_insert_and_tail() {
        let db = test_db();
        db.insert_agent(&make_agent("evt11111")).unwrap();

        for i in 0..5 {
            db.insert_event(&AgentEvent {
                id: None,
                agent_id: "evt11111".to_string(),
                event_type: "stdout".to_string(),
                payload: format!("line {}", i),
                created_at: Utc::now(),
            })
            .unwrap();
        }

        let all = db.get_events("evt11111", None).unwrap();
        assert_eq!(all.len(), 5);

        let tail = db.get_events("evt11111", Some(2)).unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].payload, "line 3");
        assert_eq!(tail[1].payload, "line 4");
    }

    #[test]
    fn agent_output_extraction() {
        let db = test_db();
        db.insert_agent(&make_agent("out11111")).unwrap();

        db.insert_event(&AgentEvent {
            id: None,
            agent_id: "out11111".to_string(),
            event_type: "stdout".to_string(),
            payload: r#"{"type":"result","result":"the answer is 42"}"#.to_string(),
            created_at: Utc::now(),
        })
        .unwrap();

        assert_eq!(
            db.get_agent_output("out11111").unwrap().as_deref(),
            Some("the answer is 42")
        );
    }

    #[test]
    fn agent_output_missing() {
        let db = test_db();
        db.insert_agent(&make_agent("noout111")).unwrap();
        assert!(db.get_agent_output("noout111").unwrap().is_none());
    }

    #[test]
    fn pact_lifecycle() {
        let db = test_db();
        db.insert_agent(&make_agent("pact1111")).unwrap();

        let pact = Pact {
            id: "pact0001".to_string(),
            source_id: "pact1111".to_string(),
            task_tpl: "do {output}".to_string(),
            name: Some("test pact".to_string()),
            state: PactState::Pending,
            target_id: None,
            created_at: Utc::now(),
            fired_at: None,
        };
        db.insert_pact(&pact).unwrap();

        assert_eq!(db.list_pacts(None).unwrap().len(), 1);
        assert_eq!(db.get_pending_pacts_for_agent("pact1111").unwrap().len(), 1);

        db.update_pact_fired("pact0001", "target01").unwrap();

        assert!(db.get_pending_pacts_for_agent("pact1111").unwrap().is_empty());
        let fired = db.list_pacts(None).unwrap();
        assert_eq!(fired[0].state, PactState::Fired);
        assert_eq!(fired[0].target_id.as_deref(), Some("target01"));
    }

    #[test]
    fn scroll_crud() {
        let db = test_db();
        let mut scroll = make_scroll("scr11111");
        scroll.state = ScrollState::Inscribed;
        db.insert_scroll(&scroll).unwrap();

        let fetched = db.get_scroll("scr11111").unwrap().unwrap();
        assert_eq!(fetched.state, ScrollState::Inscribed);

        db.update_scroll_state("scr11111", &ScrollState::Active).unwrap();
        assert_eq!(db.get_scroll("scr11111").unwrap().unwrap().state, ScrollState::Active);
        assert_eq!(db.list_scrolls().unwrap().len(), 1);
    }

    #[test]
    fn rune_dependencies_and_ready() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr22222")).unwrap();

        let rune_a = make_rune("rune_a01", "scr22222", RuneState::Complete);
        let rune_b = make_rune("rune_b01", "scr22222", RuneState::Blocked);
        db.insert_rune(&rune_a).unwrap();
        db.insert_rune(&rune_b).unwrap();
        db.insert_rune_dependency("rune_b01", "rune_a01").unwrap();

        // A is complete -> B is ready
        let ready = db.find_ready_runes("scr22222").unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "rune_b01");
    }

    #[test]
    fn rune_blocked_by_incomplete_dep() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr33333")).unwrap();

        let rune_a = make_rune("blk_a001", "scr33333", RuneState::Active);
        let rune_b = make_rune("blk_b001", "scr33333", RuneState::Blocked);
        db.insert_rune(&rune_a).unwrap();
        db.insert_rune(&rune_b).unwrap();
        db.insert_rune_dependency("blk_b001", "blk_a001").unwrap();

        assert!(db.find_ready_runes("scr33333").unwrap().is_empty());
    }

    #[test]
    fn count_active_runes() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr44444")).unwrap();

        db.insert_rune(&make_rune("cnt_a001", "scr44444", RuneState::Active)).unwrap();
        db.insert_rune(&make_rune("cnt_b001", "scr44444", RuneState::Active)).unwrap();
        db.insert_rune(&make_rune("cnt_c001", "scr44444", RuneState::Complete)).unwrap();

        assert_eq!(db.count_active_runes("scr44444").unwrap(), 2);
    }

    #[test]
    fn rune_agent_lookup() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr55555")).unwrap();
        db.insert_rune(&make_rune("lkp_a001", "scr55555", RuneState::Ready)).unwrap();

        db.update_rune_agent("lkp_a001", "myagent1").unwrap();

        let found = db.get_rune_by_agent_id("myagent1").unwrap().unwrap();
        assert_eq!(found.id, "lkp_a001");
        assert_eq!(found.state, RuneState::Active); // update_rune_agent sets active

        assert!(db.get_rune_by_agent_id("nonexist").unwrap().is_none());
    }

    #[test]
    fn delete_agent_removes_events() {
        let db = test_db();
        db.insert_agent(&make_agent("del11111")).unwrap();
        db.insert_event(&AgentEvent {
            id: None,
            agent_id: "del11111".to_string(),
            event_type: "stdout".to_string(),
            payload: "hello".to_string(),
            created_at: Utc::now(),
        })
        .unwrap();

        db.delete_agent("del11111").unwrap();
        assert!(db.get_agent("del11111").unwrap().is_none());
        assert!(db.get_events("del11111", None).unwrap().is_empty());
    }

    #[test]
    fn rune_dependents() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr66666")).unwrap();
        db.insert_rune(&make_rune("dep_a001", "scr66666", RuneState::Complete)).unwrap();
        db.insert_rune(&make_rune("dep_b001", "scr66666", RuneState::Blocked)).unwrap();
        db.insert_rune(&make_rune("dep_c001", "scr66666", RuneState::Blocked)).unwrap();
        db.insert_rune_dependency("dep_b001", "dep_a001").unwrap();
        db.insert_rune_dependency("dep_c001", "dep_a001").unwrap();

        let dependents = db.get_rune_dependents("dep_a001").unwrap();
        assert_eq!(dependents.len(), 2);
        assert!(dependents.contains(&"dep_b001".to_string()));
        assert!(dependents.contains(&"dep_c001".to_string()));
    }

    #[test]
    fn all_dependencies_for_scroll() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr77777")).unwrap();
        db.insert_rune(&make_rune("edg_a001", "scr77777", RuneState::Complete)).unwrap();
        db.insert_rune(&make_rune("edg_b001", "scr77777", RuneState::Blocked)).unwrap();
        db.insert_rune_dependency("edg_b001", "edg_a001").unwrap();

        let edges = db.get_all_dependencies_for_scroll("scr77777").unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0], ("edg_b001".to_string(), "edg_a001".to_string()));
    }

    #[test]
    fn pact_failed_state() {
        let db = test_db();
        db.insert_agent(&make_agent("pfail111")).unwrap();
        let pact = Pact {
            id: "pf000001".to_string(),
            source_id: "pfail111".to_string(),
            task_tpl: "do {output}".to_string(),
            name: None,
            state: PactState::Pending,
            target_id: None,
            created_at: Utc::now(),
            fired_at: None,
        };
        db.insert_pact(&pact).unwrap();
        db.update_pact_failed("pf000001").unwrap();

        let pacts = db.list_pacts(None).unwrap();
        assert_eq!(pacts[0].state, PactState::Failed);
        assert!(pacts[0].fired_at.is_some());
    }
}
