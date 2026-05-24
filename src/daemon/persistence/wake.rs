use anyhow::Result;
use rusqlite::params;

use crate::shared::types::{WakeSource, WakeSourceState};

use super::row_to_wake_source;

/// Column list for `SELECT … FROM wake_sources`. Matches `row_to_wake_source`.
const WAKE_SRC_COLS: &str =
    "id, agent_id, kind, config_json, state, fail_reason, last_fired_at, fire_count, created_at";

impl super::Database {
    pub fn insert_wake_source(&self, src: &WakeSource) -> Result<()> {
        self.exec(
            "INSERT INTO wake_sources \
                (id, agent_id, kind, config_json, state, fail_reason, last_fired_at, fire_count, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                src.id,
                src.agent_id,
                src.kind.as_str(),
                src.config_json,
                src.state.as_str(),
                src.fail_reason,
                src.last_fired_at,
                src.fire_count,
                src.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn get_wake_source(&self, id: &str) -> Result<Option<WakeSource>> {
        self.query_opt(
            &format!("SELECT {WAKE_SRC_COLS} FROM wake_sources WHERE id = ?1"),
            params![id],
            row_to_wake_source,
        )
    }

    pub fn list_wake_sources_for_agent(&self, agent_id: &str) -> Result<Vec<WakeSource>> {
        self.query_vec(
            &format!("SELECT {WAKE_SRC_COLS} FROM wake_sources WHERE agent_id = ?1 ORDER BY created_at DESC, id ASC"),
            params![agent_id],
            row_to_wake_source,
        )
    }

    pub fn list_all_wake_sources(&self) -> Result<Vec<WakeSource>> {
        self.query_vec(
            &format!("SELECT {WAKE_SRC_COLS} FROM wake_sources ORDER BY created_at DESC, agent_id ASC, id ASC"),
            [],
            row_to_wake_source,
        )
    }

    pub fn list_armed_wake_sources(&self) -> Result<Vec<WakeSource>> {
        self.query_vec(
            &format!("SELECT {WAKE_SRC_COLS} FROM wake_sources WHERE state = 'armed' ORDER BY created_at ASC"),
            [],
            row_to_wake_source,
        )
    }

    pub fn delete_wake_source(&self, id: &str) -> Result<bool> {
        Ok(self.exec("DELETE FROM wake_sources WHERE id = ?1", params![id])? > 0)
    }

    pub fn delete_wake_sources_for_agent(&self, agent_id: &str) -> Result<usize> {
        self.exec(
            "DELETE FROM wake_sources WHERE agent_id = ?1",
            params![agent_id],
        )
    }

    pub fn update_wake_source_state(
        &self,
        id: &str,
        state: WakeSourceState,
        fail_reason: Option<&str>,
    ) -> Result<()> {
        self.exec(
            "UPDATE wake_sources SET state = ?1, fail_reason = ?2 WHERE id = ?3",
            params![state.as_str(), fail_reason, id],
        )?;
        Ok(())
    }

    pub fn bump_wake_source_fire(&self, id: &str, last_fired_at: i64) -> Result<()> {
        self.exec(
            "UPDATE wake_sources \
             SET fire_count = fire_count + 1, last_fired_at = ?1 \
             WHERE id = ?2",
            params![last_fired_at, id],
        )?;
        Ok(())
    }

    /// Per-agent token-bucket row used by the rate limiter. Returns
    /// `(tokens, last_refill_at, capacity, refill_per_sec)`. If the row
    /// doesn't exist yet, it is created at full capacity.
    pub fn get_or_init_rate_limit(&self, agent_id: &str, now: i64) -> Result<(f64, i64, i64, f64)> {
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row: Option<(f64, i64, i64, f64)> = tx
            .query_row(
                "SELECT tokens, last_refill_at, capacity, refill_per_sec \
                 FROM wake_rate_limits WHERE agent_id = ?1",
                params![agent_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .ok();
        let result = if let Some(r) = row {
            r
        } else {
            // Defaults: 60 tokens, 60-per-hour refill.
            let capacity: i64 = 60;
            let refill: f64 = 60.0 / 3600.0;
            tx.execute(
                "INSERT INTO wake_rate_limits (agent_id, tokens, last_refill_at, capacity, refill_per_sec) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![agent_id, capacity as f64, now, capacity, refill],
            )?;
            (capacity as f64, now, capacity, refill)
        };
        tx.commit()?;
        Ok(result)
    }

    pub fn update_rate_limit_tokens(
        &self,
        agent_id: &str,
        tokens: f64,
        last_refill_at: i64,
    ) -> Result<()> {
        self.exec(
            "UPDATE wake_rate_limits SET tokens = ?1, last_refill_at = ?2 WHERE agent_id = ?3",
            params![tokens, last_refill_at, agent_id],
        )?;
        Ok(())
    }

    pub fn set_rate_limit_capacity(
        &self,
        agent_id: &str,
        capacity: i64,
        refill_per_sec: f64,
        now: i64,
    ) -> Result<()> {
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let exists: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM wake_rate_limits WHERE agent_id = ?1",
                params![agent_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if exists == 0 {
            tx.execute(
                "INSERT INTO wake_rate_limits (agent_id, tokens, last_refill_at, capacity, refill_per_sec) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![agent_id, capacity as f64, now, capacity, refill_per_sec],
            )?;
        } else {
            tx.execute(
                "UPDATE wake_rate_limits SET capacity = ?1, refill_per_sec = ?2 WHERE agent_id = ?3",
                params![capacity, refill_per_sec, agent_id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}
