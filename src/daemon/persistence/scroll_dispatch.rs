//! Persistence helpers for cross-peer scroll task dispatch.
//!
//! - `scroll_task_dispatches` is the durable record of "task T of
//!   scroll S has been handed to peer P, who acked with
//!   `remote_agent_id`." Created on dispatch enqueue, updated on
//!   ack, terminal on lifecycle completion.
//! - `scroll_dispatch_outbox` is the wire-level at-least-once queue,
//!   identical state machine to the other federation outboxes.
//! - `scroll_dispatch_inbox` dedupes inbound by
//!   `(sender_daemon_id, sender_seq)`. The `local_agent_id` column
//!   is stashed for replay debugging — on a replayed delivery we
//!   re-ack with the same agent id rather than spawning a duplicate.

use anyhow::Result;
use chrono::Utc;
use rusqlite::params;

#[derive(Debug, Clone)]
pub struct ScrollDispatchOutboxRow {
    pub id: String,
    pub sender_seq: u64,
    pub payload: Vec<u8>,
    pub attempts: u32,
}

#[derive(Debug, Clone)]
pub struct ScrollDispatchRow {
    pub id: String,
    pub scroll_id: String,
    pub task_id: String,
    pub peer_id: String,
    pub remote_agent_id: Option<String>,
    pub state: String,
}

impl super::Database {
    pub fn scroll_dispatch_insert(
        &self,
        id: &str,
        scroll_id: &str,
        task_id: &str,
        peer_id: &str,
    ) -> Result<()> {
        let conn = self.conn_lock();
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO scroll_task_dispatches
                (id, scroll_id, task_id, peer_id, remote_agent_id, state, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, NULL, 'pending', ?5, ?5)",
            params![id, scroll_id, task_id, peer_id, now],
        )?;
        Ok(())
    }

    pub fn scroll_dispatch_set_remote_agent(
        &self,
        scroll_id: &str,
        task_id: &str,
        peer_id: &str,
        remote_agent_id: &str,
    ) -> Result<()> {
        let conn = self.conn_lock();
        let now = Utc::now().timestamp();
        conn.execute(
            "UPDATE scroll_task_dispatches
             SET remote_agent_id = ?1, state = 'dispatched', updated_at = ?2
             WHERE scroll_id = ?3 AND task_id = ?4 AND peer_id = ?5",
            params![remote_agent_id, now, scroll_id, task_id, peer_id],
        )?;
        Ok(())
    }

    pub fn scroll_dispatch_set_state(
        &self,
        scroll_id: &str,
        task_id: &str,
        peer_id: &str,
        new_state: &str,
    ) -> Result<()> {
        let conn = self.conn_lock();
        let now = Utc::now().timestamp();
        conn.execute(
            "UPDATE scroll_task_dispatches
             SET state = ?1, updated_at = ?2
             WHERE scroll_id = ?3 AND task_id = ?4 AND peer_id = ?5",
            params![new_state, now, scroll_id, task_id, peer_id],
        )?;
        Ok(())
    }

    /// Look up the dispatch row driven by an inbound remote-agent
    /// transition. Coordinator's lifecycle subscriber uses this to map
    /// `(sender_daemon_id, remote_agent_id) -> (scroll, task)` and
    /// then update the task state.
    pub fn scroll_dispatch_find_by_remote(
        &self,
        peer_id: &str,
        remote_agent_id: &str,
    ) -> Result<Option<ScrollDispatchRow>> {
        let conn = self.conn_lock();
        let row = conn
            .query_row(
                "SELECT id, scroll_id, task_id, peer_id, remote_agent_id, state
                 FROM scroll_task_dispatches
                 WHERE peer_id = ?1 AND remote_agent_id = ?2 LIMIT 1",
                params![peer_id, remote_agent_id],
                |r| {
                    Ok(ScrollDispatchRow {
                        id: r.get(0)?,
                        scroll_id: r.get(1)?,
                        task_id: r.get(2)?,
                        peer_id: r.get(3)?,
                        remote_agent_id: r.get(4)?,
                        state: r.get(5)?,
                    })
                },
            )
            .ok();
        Ok(row)
    }

    pub fn scroll_dispatch_enqueue(&self, peer_id: &str, payload: &[u8]) -> Result<u64> {
        let mut conn = self.conn_lock();
        let now = Utc::now().timestamp();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let next_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(sender_seq), 0) + 1 FROM scroll_dispatch_outbox
             WHERE peer_id = ?1",
            params![peer_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO scroll_dispatch_outbox
                (id, peer_id, sender_seq, payload, created_at,
                 attempts, next_attempt_at, state)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?5, 'pending')",
            params![
                crate::shared::constants::generate_short_id(),
                peer_id,
                next_seq,
                payload,
                now,
            ],
        )?;
        tx.commit()?;
        Ok(u64::try_from(next_seq).unwrap_or(0))
    }

    pub fn scroll_dispatch_next_outbox(
        &self,
        peer_id: &str,
        now: i64,
    ) -> Result<Option<ScrollDispatchOutboxRow>> {
        let conn = self.conn_lock();
        let row = conn
            .query_row(
                "SELECT id, sender_seq, payload, attempts
                 FROM scroll_dispatch_outbox
                 WHERE peer_id = ?1 AND state = 'pending' AND next_attempt_at <= ?2
                 ORDER BY created_at ASC LIMIT 1",
                params![peer_id, now],
                |r| {
                    Ok(ScrollDispatchOutboxRow {
                        id: r.get(0)?,
                        sender_seq: u64::try_from(r.get::<_, i64>(1)?).unwrap_or(0),
                        payload: r.get(2)?,
                        attempts: u32::try_from(r.get::<_, i64>(3)?).unwrap_or(0),
                    })
                },
            )
            .ok();
        Ok(row)
    }

    pub fn scroll_dispatch_mark_in_flight(&self, id: &str) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "UPDATE scroll_dispatch_outbox SET state = 'in_flight' WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn scroll_dispatch_mark_delivered(&self, id: &str) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "DELETE FROM scroll_dispatch_outbox WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn scroll_dispatch_mark_failed_retry(&self, id: &str, next_attempt_at: i64) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "UPDATE scroll_dispatch_outbox
             SET state = 'pending', attempts = attempts + 1, next_attempt_at = ?2
             WHERE id = ?1",
            params![id, next_attempt_at],
        )?;
        Ok(())
    }

    pub fn scroll_dispatch_reset_in_flight(&self) -> Result<usize> {
        let conn = self.conn_lock();
        Ok(conn.execute(
            "UPDATE scroll_dispatch_outbox SET state = 'pending' WHERE state = 'in_flight'",
            [],
        )?)
    }

    /// Record an inbound dispatch. On first sighting, returns
    /// `Ok(None)` and the caller spawns the agent + writes
    /// `scroll_dispatch_inbox_set_agent`. On replay, returns
    /// `Ok(Some(local_agent_id))` so the receiver re-acks with the
    /// same id (no duplicate spawn).
    pub fn scroll_dispatch_inbox_lookup(
        &self,
        sender_daemon_id: &str,
        sender_seq: u64,
    ) -> Result<Option<String>> {
        let conn = self.conn_lock();
        let seq_i = i64::try_from(sender_seq).unwrap_or(i64::MAX);
        let row: Option<String> = conn
            .query_row(
                "SELECT local_agent_id FROM scroll_dispatch_inbox
                 WHERE sender_daemon_id = ?1 AND sender_seq = ?2",
                params![sender_daemon_id, seq_i],
                |r| r.get(0),
            )
            .ok();
        Ok(row)
    }

    pub fn scroll_dispatch_inbox_record(
        &self,
        sender_daemon_id: &str,
        sender_seq: u64,
        local_agent_id: &str,
    ) -> Result<()> {
        let conn = self.conn_lock();
        let seq_i = i64::try_from(sender_seq).unwrap_or(i64::MAX);
        conn.execute(
            "INSERT OR IGNORE INTO scroll_dispatch_inbox
                (sender_daemon_id, sender_seq, local_agent_id, received_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                sender_daemon_id,
                seq_i,
                local_agent_id,
                Utc::now().timestamp()
            ],
        )?;
        Ok(())
    }

    /// Update the `accept_scroll_dispatch` opt-in flag for a peer.
    pub fn set_peer_accept_scroll_dispatch(&self, peer_id: &str, accept: bool) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "UPDATE peers SET accept_scroll_dispatch = ?1 WHERE id = ?2",
            params![i64::from(accept), peer_id],
        )?;
        Ok(())
    }

    /// Read the opt-in flag. Defaults to `false` for unknown peers.
    pub fn peer_accept_scroll_dispatch(&self, peer_id: &str) -> Result<bool> {
        let conn = self.conn_lock();
        let flag: i64 = conn
            .query_row(
                "SELECT accept_scroll_dispatch FROM peers WHERE id = ?1",
                params![peer_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(flag != 0)
    }
}
