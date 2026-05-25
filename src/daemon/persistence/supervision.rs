use anyhow::Result;
use chrono::Utc;
use rusqlite::params;

use crate::shared::protocol::SupervisorNode;
use crate::shared::types::{AgentId, RestartHistoryOutcome, RestartPolicy, SupervisionConfig};

impl super::Database {
    pub fn set_supervision(&self, agent_id: &str, cfg: &SupervisionConfig) -> Result<()> {
        self.exec(
            "UPDATE agents SET restart_policy = ?1, max_restarts = ?2, \
             restart_window_secs = ?3, escalate_to = ?4 WHERE id = ?5",
            params![
                cfg.policy.as_str(),
                cfg.max_restarts,
                cfg.window_secs,
                cfg.escalate_to,
                agent_id,
            ],
        )?;
        Ok(())
    }

    pub fn get_supervision(&self, agent_id: &str) -> Result<Option<SupervisionConfig>> {
        type SupervisionRow = (String, Option<u32>, Option<u32>, Option<String>);
        let conn = self.conn_lock();
        let row: Option<SupervisionRow> = conn
            .query_row(
                "SELECT restart_policy, max_restarts, restart_window_secs, escalate_to \
                 FROM agents WHERE id = ?1",
                params![agent_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .ok();
        Ok(row.map(|(policy, max, window, esc)| SupervisionConfig {
            policy: policy.parse().unwrap_or(RestartPolicy::Never),
            max_restarts: max,
            window_secs: window,
            escalate_to: esc,
        }))
    }

    pub fn clear_supervision(&self, agent_id: &str) -> Result<()> {
        self.exec(
            "UPDATE agents SET restart_policy = 'never', max_restarts = NULL, \
             restart_window_secs = NULL, escalate_to = NULL WHERE id = ?1",
            params![agent_id],
        )?;
        Ok(())
    }

    pub fn bump_restart_count(&self, agent_id: &str) -> Result<()> {
        self.exec(
            "UPDATE agents SET restart_count = restart_count + 1 WHERE id = ?1",
            params![agent_id],
        )?;
        Ok(())
    }

    pub fn get_escalation_depth(&self, agent_id: &str) -> Result<u32> {
        let conn = self.conn_lock();
        let v: i64 = conn
            .query_row(
                "SELECT escalation_depth FROM agents WHERE id = ?1",
                params![agent_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(v.max(0) as u32)
    }

    pub fn set_escalation_depth(&self, agent_id: &str, depth: u32) -> Result<()> {
        self.exec(
            "UPDATE agents SET escalation_depth = ?1 WHERE id = ?2",
            params![i64::from(depth), agent_id],
        )?;
        Ok(())
    }

    pub fn insert_restart_history_row(
        &self,
        agent_id: &str,
        attempted_at: i64,
        outcome: RestartHistoryOutcome,
        error_summary: Option<&str>,
    ) -> Result<i64> {
        let conn = self.conn_lock();
        conn.execute(
            "INSERT INTO restart_history (agent_id, attempted_at, outcome, error_summary) \
             VALUES (?1, ?2, ?3, ?4)",
            params![agent_id, attempted_at, outcome.as_str(), error_summary],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn count_restarts_in_window(&self, agent_id: &str, window_start: i64) -> Result<u32> {
        let conn = self.conn_lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM restart_history \
             WHERE agent_id = ?1 AND attempted_at >= ?2 \
             AND outcome IN ('scheduled','failed_again')",
            params![agent_id, window_start],
            |r| r.get(0),
        )?;
        Ok(n.max(0) as u32)
    }

    /// Update the most recent `restart_history` row for `agent_id` whose
    /// `outcome = 'scheduled'`. Returns the number of rows updated.
    pub fn update_latest_scheduled_outcome(
        &self,
        agent_id: &str,
        new_outcome: RestartHistoryOutcome,
    ) -> Result<usize> {
        self.exec(
            "UPDATE restart_history SET outcome = ?1 \
             WHERE id = (SELECT id FROM restart_history \
                         WHERE agent_id = ?2 AND outcome = 'scheduled' \
                         ORDER BY attempted_at DESC, id DESC LIMIT 1)",
            params![new_outcome.as_str(), agent_id],
        )
    }

    /// Time of the most recent `restart_history` row for `agent_id`, or `None`.
    pub fn latest_restart_history_attempted_at(&self, agent_id: &str) -> Result<Option<i64>> {
        let conn = self.conn_lock();
        let v: Option<i64> = conn
            .query_row(
                "SELECT MAX(attempted_at) FROM restart_history WHERE agent_id = ?1",
                params![agent_id],
                |r| r.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten();
        Ok(v)
    }

    /// One row per agent with everything the supervisor dashboard needs to
    /// render the tree. Single query — no N+1.
    pub fn list_supervisor_nodes(&self) -> Result<Vec<SupervisorNode>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare(
            "SELECT a.id, a.name, a.state, a.task, a.parent_agent_id, \
                    a.restart_policy, a.restart_count, a.max_restarts, \
                    a.restart_window_secs, a.escalate_to, a.escalation_depth, \
                    (SELECT MAX(attempted_at) FROM restart_history h \
                     WHERE h.agent_id = a.id), \
                    (SELECT outcome FROM restart_history h \
                     WHERE h.agent_id = a.id \
                     ORDER BY attempted_at DESC, id DESC LIMIT 1) \
             FROM agents a \
             ORDER BY a.created_at DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            let restart_count: i64 = r.get::<_, Option<i64>>(6)?.unwrap_or(0);
            let escalation_depth: i64 = r.get::<_, Option<i64>>(10)?.unwrap_or(0);
            Ok(SupervisorNode {
                agent_id: r.get(0)?,
                name: r.get(1)?,
                state: r.get(2)?,
                task: r.get(3)?,
                parent_id: r.get(4)?,
                restart_policy: r
                    .get::<_, Option<String>>(5)?
                    .unwrap_or_else(|| "never".to_string()),
                restart_count: restart_count.max(0) as u32,
                max_restarts: r.get::<_, Option<u32>>(7)?,
                window_secs: r.get::<_, Option<u32>>(8)?,
                escalate_to: r.get(9)?,
                escalation_depth: escalation_depth.max(0) as u32,
                last_restart_at: r.get::<_, Option<i64>>(11)?,
                last_restart_outcome: r.get(12)?,
            })
        })?;
        Ok(rows.filter_map(std::result::Result::ok).collect())
    }

    pub fn list_failed_with_active_policy(&self) -> Result<Vec<AgentId>> {
        self.query_vec(
            "SELECT id FROM agents \
             WHERE state = 'failed' AND restart_policy != 'never'",
            [],
            |row| Ok(row.get(0)?),
        )
    }

    pub fn mark_torn_restarting_as_failed(&self) -> Result<Vec<AgentId>> {
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let ids: Vec<AgentId> = {
            let mut stmt = tx.prepare("SELECT id FROM agents WHERE state = 'restarting'")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.filter_map(std::result::Result::ok).collect()
        };
        if !ids.is_empty() {
            let now = Utc::now().to_rfc3339();
            tx.execute(
                "UPDATE agents SET state = 'failed', updated_at = ?1 WHERE state = 'restarting'",
                params![now],
            )?;
        }
        tx.commit()?;
        Ok(ids)
    }

    /// Return `true` if there is an `Escalated` event for `agent_id` whose
    /// row id is later than the latest `restart_history` row for the agent.
    /// Used by boot replay to skip re-escalation.
    pub fn has_escalated_event_after_latest_history(&self, agent_id: &str) -> Result<bool> {
        let conn = self.conn_lock();
        let latest_history_ts: Option<String> = conn
            .query_row(
                "SELECT MAX(attempted_at) FROM restart_history WHERE agent_id = ?1",
                params![agent_id],
                |r| r.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten()
            .map(|t| {
                chrono::DateTime::<Utc>::from_timestamp(t, 0)
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339()
            });
        let n: i64 = match latest_history_ts {
            Some(ts) => conn
                .query_row(
                    "SELECT COUNT(*) FROM events \
                     WHERE agent_id = ?1 AND kind = 'escalated' AND ts > ?2",
                    params![agent_id, ts],
                    |r| r.get(0),
                )
                .unwrap_or(0),
            None => conn
                .query_row(
                    "SELECT COUNT(*) FROM events \
                     WHERE agent_id = ?1 AND kind = 'escalated'",
                    params![agent_id],
                    |r| r.get(0),
                )
                .unwrap_or(0),
        };
        Ok(n > 0)
    }
}
