use anyhow::Result;
use chrono::Utc;
use rusqlite::params;

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Agent, AgentEvent, AgentId, AgentState};

use super::{RecoveryReport, StoredEvent, row_to_agent};

/// Column list for `SELECT … FROM agents` queries. Order must match
/// `row_to_agent`'s positional column reads.
const AGENT_COLS: &str = "id, name, state, task, model, provider, cwd, pid, session_id, exit_code, created_at, updated_at, worker_id, restart_policy, restart_count, workspace_id";

impl super::Database {
    /// Append a stream event to the durable log. Returns the new row's id.
    /// Computes `seq` per (agent_id) when present, else per (scroll_id), else 0.
    pub fn append_event(&self, event: &StreamEvent) -> Result<i64> {
        let agent_id = event.agent_id();
        let scroll_id = event.scroll_id();
        let kind = event.kind();
        let payload = serde_json::to_string(event)?;
        let ts = Utc::now().to_rfc3339();

        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let seq: i64 = if let Some(aid) = agent_id {
            tx.query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0) FROM events WHERE agent_id = ?1",
                params![aid],
                |r| r.get(0),
            )?
        } else if let Some(sid) = scroll_id {
            tx.query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0) FROM events WHERE scroll_id = ?1",
                params![sid],
                |r| r.get(0),
            )?
        } else {
            0
        };

        tx.execute(
            "INSERT INTO events (agent_id, scroll_id, seq, kind, payload, ts) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![agent_id, scroll_id, seq, kind, payload, ts],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    pub fn insert_agent(&self, agent: &Agent) -> Result<()> {
        self.exec(
            "INSERT INTO agents (id, name, state, task, model, provider, cwd, pid, session_id, exit_code, created_at, updated_at, worker_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
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
                agent.worker_id,
            ],
        )?;
        Ok(())
    }

    /// Update a single `agents` column plus `updated_at`. Caller supplies the
    /// column name; SQL uses a fixed template so no injection surface exists.
    fn update_agent_field(
        &self,
        id: &str,
        column: &str,
        value: &dyn rusqlite::ToSql,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            &format!("UPDATE agents SET {column} = ?1, updated_at = ?2 WHERE id = ?3"),
            params![value, now, id],
        )?;
        Ok(())
    }

    pub fn update_agent_state(
        &self,
        id: &str,
        state: &AgentState,
        exit_code: Option<i32>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            "UPDATE agents SET state = ?1, exit_code = ?2, updated_at = ?3 WHERE id = ?4",
            params![state.as_str(), exit_code, now, id],
        )?;
        Ok(())
    }

    pub fn update_agent_session_id(&self, id: &str, session_id: &str) -> Result<()> {
        self.update_agent_field(id, "session_id", &session_id)
    }

    pub fn update_agent_worker_id(&self, id: &str, worker_id: Option<&str>) -> Result<()> {
        self.update_agent_field(id, "worker_id", &worker_id)
    }

    pub fn update_agent_pid(&self, id: &str, pid: u32) -> Result<()> {
        self.update_agent_field(id, "pid", &pid)
    }

    /// Atomically add `tokens` to `agents.tokens_used` for `id`. Returns the
    /// new running total. A `0` increment is a no-op fast path.
    pub fn add_agent_tokens(&self, id: &str, tokens: u64) -> Result<u64> {
        if tokens == 0 {
            return self.get_agent_tokens(id);
        }
        let conn = self.conn_lock();
        conn.execute(
            "UPDATE agents SET tokens_used = tokens_used + ?1, updated_at = ?2 WHERE id = ?3",
            params![tokens as i64, chrono::Utc::now().to_rfc3339(), id],
        )?;
        let total: i64 = conn.query_row(
            "SELECT tokens_used FROM agents WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )?;
        Ok(total.max(0) as u64)
    }

    /// Set or clear the parent of an agent. Used by `agent.summon --parent`
    /// to wire the supervision tree at creation time.
    pub fn set_agent_parent(&self, id: &str, parent_id: Option<&str>) -> Result<()> {
        self.update_agent_field(id, "parent_agent_id", &parent_id)
    }

    /// Children of `parent_id` whose state is still in-flight (Queued,
    /// Summoning, Active, Dormant). Completed / Banished / Failed children
    /// are excluded; there's nothing to cascade onto.
    pub fn list_live_children(&self, parent_id: &str) -> Result<Vec<String>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare(
            "SELECT id FROM agents \
             WHERE parent_agent_id = ?1 \
               AND state IN ('Queued','Summoning','Active','Dormant')",
        )?;
        let ids = stmt
            .query_map(params![parent_id], |r| r.get::<_, String>(0))?
            .filter_map(Result::ok)
            .collect();
        Ok(ids)
    }

    /// Add `usd` to the agent's lifetime spend, returning the new total.
    pub fn add_agent_usd(&self, id: &str, usd: f64) -> Result<f64> {
        if usd <= 0.0 {
            return self.get_agent_usd(id);
        }
        let conn = self.conn_lock();
        conn.execute(
            "UPDATE agents SET usd_spent = usd_spent + ?1, updated_at = ?2 WHERE id = ?3",
            params![usd, chrono::Utc::now().to_rfc3339(), id],
        )?;
        let total: f64 = conn.query_row(
            "SELECT usd_spent FROM agents WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )?;
        Ok(total.max(0.0))
    }

    pub fn get_agent_usd(&self, id: &str) -> Result<f64> {
        let conn = self.conn_lock();
        let total: f64 = conn
            .query_row(
                "SELECT usd_spent FROM agents WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap_or(0.0);
        Ok(total.max(0.0))
    }

    pub fn get_agent_tokens(&self, id: &str) -> Result<u64> {
        let conn = self.conn_lock();
        let total: i64 = conn
            .query_row(
                "SELECT tokens_used FROM agents WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(total.max(0) as u64)
    }

    pub fn get_agent(&self, id: &str) -> Result<Option<Agent>> {
        self.query_opt(
            &format!("SELECT {AGENT_COLS} FROM agents WHERE id = ?1"),
            params![id],
            row_to_agent,
        )
    }

    pub fn list_agents(&self, state_filter: Option<&str>) -> Result<Vec<Agent>> {
        match state_filter {
            Some(state) => self.query_vec(
                &format!(
                    "SELECT {AGENT_COLS} FROM agents WHERE state = ?1 ORDER BY created_at DESC"
                ),
                params![state],
                row_to_agent,
            ),
            None => self.query_vec(
                &format!("SELECT {AGENT_COLS} FROM agents ORDER BY created_at DESC"),
                [],
                row_to_agent,
            ),
        }
    }

    pub fn insert_event(&self, event: &AgentEvent) -> Result<i64> {
        let conn = self.conn_lock();
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
        let conn = self.conn_lock();
        let mut events = Vec::new();
        let query = if let Some(limit) = tail {
            format!(
                "SELECT id, agent_id, event_type, payload, created_at
                 FROM agent_events WHERE agent_id = ?1
                 ORDER BY id DESC LIMIT {limit}"
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
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)?
                    .with_timezone(&chrono::Utc),
            });
        }
        if tail.is_some() {
            events.reverse();
        }
        Ok(events)
    }

    /// Count agents grouped by state string (matches `AgentState::as_str`).
    /// Returned in no particular order; the metrics renderer fans missing
    /// states out to zero so the exposition is shape-stable across scrapes.
    pub fn count_agents_by_state(&self) -> Result<Vec<(String, i64)>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare("SELECT state, COUNT(*) FROM agents GROUP BY state")?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push((row.get::<_, String>(0)?, row.get::<_, i64>(1)?));
        }
        Ok(out)
    }

    /// Total rows in the durable `events` stream log. Cheap (table has a
    /// rowid index) but not O(1); fine at metrics-scrape rates.
    pub fn count_events_total(&self) -> Result<i64> {
        let conn = self.conn_lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))?;
        Ok(n)
    }

    /// Count durable events of a single `kind` (the per-variant tag from
    /// `StreamEvent::kind`). Backs the per-event-type counter metrics.
    pub fn count_events_by_kind(&self, kind: &str) -> Result<i64> {
        let conn = self.conn_lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events WHERE kind = ?1",
            params![kind],
            |r| r.get(0),
        )?;
        Ok(n)
    }

    /// Count notification events grouped by their `level` payload field.
    /// Used to label the operator-facing notifications counter so warn/error
    /// rates show up distinctly in dashboards.
    pub fn count_notifications_by_level(&self) -> Result<Vec<(String, i64)>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare(
            "SELECT json_extract(payload, '$.level') AS lvl, COUNT(*) \
             FROM events WHERE kind = 'notification' GROUP BY lvl",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let level: String = row
                .get::<_, Option<String>>(0)?
                .unwrap_or_else(|| "unknown".to_string());
            out.push((level, row.get::<_, i64>(1)?));
        }
        Ok(out)
    }

    /// Read the full durable stream-event log for one agent, oldest first.
    /// This is the rich `events` table (every `StreamEvent` variant), not the
    /// legacy `agent_events` stdout/stderr stream that `get_events` serves.
    /// Rows whose payload fails to deserialize (a schema that predates a
    /// variant rename, say) are skipped rather than failing the whole read;
    /// a partial timeline beats no timeline.
    pub fn read_stream_events(&self, agent_id: &str) -> Result<Vec<StoredEvent>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare(
            "SELECT seq, kind, payload, ts FROM events \
             WHERE agent_id = ?1 ORDER BY seq ASC",
        )?;
        let mut rows = stmt.query(params![agent_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let seq: i64 = row.get(0)?;
            let kind: String = row.get(1)?;
            let payload: String = row.get(2)?;
            let ts: String = row.get(3)?;
            let Ok(event) = serde_json::from_str::<StreamEvent>(&payload) else {
                continue;
            };
            out.push(StoredEvent {
                seq,
                kind,
                ts,
                event,
            });
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn delete_agent(&self, id: &str) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute("DELETE FROM agent_events WHERE agent_id = ?1", params![id])?;
        conn.execute("DELETE FROM agents WHERE id = ?1", params![id])?;
        Ok(())
    }

    /// An agent's stdout lines in emission order. The raw material for a
    /// provider's `extract_result` (pact `{output}` injection) and the
    /// `ContextReplay` transcript. Provider-neutral; no format assumed here.
    pub fn get_agent_stdout_lines(&self, agent_id: &str) -> Result<Vec<String>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare(
            "SELECT payload FROM agent_events
             WHERE agent_id = ?1 AND event_type = 'stdout'
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query(params![agent_id])?;
        let mut lines = Vec::new();
        while let Some(row) = rows.next()? {
            lines.push(row.get(0)?);
        }
        Ok(lines)
    }

    /// Reconstruct an agent's prior stdout as a single string, for the
    /// `ContextReplay` resume strategy (providers with no native session). Capped
    /// to the last `budget_bytes` (oldest output truncated with a note),
    /// mirroring the scheduler's mail-fold budgeting. Returns the empty string if the agent
    /// produced no output.
    pub fn get_agent_transcript(&self, agent_id: &str, budget_bytes: usize) -> Result<String> {
        let full = self.get_agent_stdout_lines(agent_id)?.join("\n");
        if full.len() <= budget_bytes {
            return Ok(full);
        }
        // Keep the tail; align the cut to a UTF-8 char boundary.
        let mut start = full.len() - budget_bytes;
        while start < full.len() && !full.is_char_boundary(start) {
            start += 1;
        }
        Ok(format!("[…earlier output truncated…]\n{}", &full[start..]))
    }

    pub fn get_keep_alive(&self, agent_id: &str) -> Result<bool> {
        let conn = self.conn_lock();
        let v: i64 = conn.query_row(
            "SELECT keep_alive FROM agents WHERE id = ?1",
            params![agent_id],
            |r| r.get(0),
        )?;
        Ok(v != 0)
    }

    pub fn set_keep_alive(&self, agent_id: &str, keep_alive: bool) -> Result<()> {
        self.exec(
            "UPDATE agents SET keep_alive = ?1 WHERE id = ?2",
            params![i64::from(keep_alive), agent_id],
        )?;
        Ok(())
    }

    /// Promote `Complete` agents that still have a `session_id` to `Dormant`.
    /// Idempotent: replays are no-ops because the WHERE clause filters
    /// already-Dormant rows. Returns the IDs that flipped, so the caller can
    /// emit `StateChange { Complete -> Dormant }` events for each.
    pub fn migrate_dormant_agents(&self) -> Result<Vec<AgentId>> {
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let ids: Vec<AgentId> = {
            let mut stmt = tx.prepare(
                "SELECT id FROM agents \
                 WHERE state = 'complete' AND session_id IS NOT NULL",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.filter_map(std::result::Result::ok).collect()
        };

        if !ids.is_empty() {
            let now = Utc::now().to_rfc3339();
            tx.execute(
                "UPDATE agents SET state = 'dormant', updated_at = ?1 \
                 WHERE state = 'complete' AND session_id IS NOT NULL",
                params![now],
            )?;
        }

        tx.commit()?;
        Ok(ids)
    }

    /// On daemon startup, mark every agent that was mid-flight (`Active` or
    /// `Summoning`) as `Failed` (their child processes are gone), then report
    /// what was changed plus how many `Queued` agents survived for the
    /// scheduler to pick up. `Complete`/`Failed`/`Banished` rows and `Queued`
    /// rows are left untouched.
    pub fn restart_recovery(&self) -> Result<RecoveryReport> {
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let failed: Vec<(AgentId, AgentState)> = {
            let mut stmt =
                tx.prepare("SELECT id, state FROM agents WHERE state IN ('active', 'summoning')")?;
            let rows = stmt.query_map([], |row| {
                let id: String = row.get(0)?;
                let state: String = row.get(1)?;
                Ok((id, state))
            })?;
            rows.filter_map(std::result::Result::ok)
                .map(|(id, s)| {
                    let parsed = s.parse().unwrap_or(AgentState::Failed);
                    (id, parsed)
                })
                .collect()
        };

        let now = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE agents SET state = 'failed', updated_at = ?1 \
             WHERE state IN ('active', 'summoning')",
            params![now],
        )?;

        let queued_remaining: i64 = tx.query_row(
            "SELECT COUNT(*) FROM agents WHERE state = 'queued'",
            [],
            |r| r.get(0),
        )?;

        tx.commit()?;
        Ok(RecoveryReport {
            failed,
            queued_remaining: queued_remaining as usize,
        })
    }
}
