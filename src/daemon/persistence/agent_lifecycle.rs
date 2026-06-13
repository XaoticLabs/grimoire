//! Persistence helpers for agent-lifecycle federation.
//!
//! - `agent_lifecycle_federations` — per-peer subscription rows.
//! - `agent_lifecycle_outbox` — durable per-peer queue (monotonic `sender_seq`,
//!   retry backoff); one row per wire delivery.
//! - `agent_lifecycle_inbox` — receiver-side dedupe by `(sender_daemon_id,
//!   sender_seq)`.

use anyhow::Result;
use chrono::Utc;
use rusqlite::params;

use crate::shared::types::FederationDirection;

/// One pending outbox row ready to ship.
#[derive(Debug, Clone)]
pub struct AgentLifecycleOutboxRow {
    pub id: String,
    pub sender_seq: u64,
    pub payload: Vec<u8>,
    pub attempts: u32,
}

impl super::Database {
    /// Upsert a peer's lifecycle federation, merging directions
    /// (`Outbound + Inbound -> Both`).
    pub fn upsert_agent_lifecycle_federation(
        &self,
        id: &str,
        peer_id: &str,
        direction: FederationDirection,
        created_at: i64,
    ) -> Result<FederationDirection> {
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT direction FROM agent_lifecycle_federations WHERE peer_id = ?1",
                params![peer_id],
                |r| r.get::<_, String>(0),
            )
            .ok();
        let final_dir = if let Some(s) = existing {
            let cur: FederationDirection = s.parse().unwrap_or(FederationDirection::Both);
            cur.merge(direction)
        } else {
            direction
        };
        tx.execute(
            "INSERT INTO agent_lifecycle_federations (id, peer_id, direction, created_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(peer_id) DO UPDATE SET direction = excluded.direction",
            params![id, peer_id, final_dir.as_str(), created_at],
        )?;
        tx.commit()?;
        Ok(final_dir)
    }

    /// Delete a peer's lifecycle federation, returning rows affected.
    pub fn delete_agent_lifecycle_federation(&self, peer_id: &str) -> Result<usize> {
        let conn = self.conn_lock();
        Ok(conn.execute(
            "DELETE FROM agent_lifecycle_federations WHERE peer_id = ?1",
            params![peer_id],
        )?)
    }

    /// Peer ids subscribed for outbound fanout, read on every `StateChange`.
    pub fn agent_lifecycle_outbound_peers(&self) -> Result<Vec<String>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare(
            "SELECT peer_id FROM agent_lifecycle_federations
             WHERE direction IN ('outbound', 'both')",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Whether `peer_id` may deliver agent-lifecycle events into our local bus.
    pub fn agent_lifecycle_inbound_authorized(&self, peer_id: &str) -> Result<bool> {
        let conn = self.conn_lock();
        let dir: Option<String> = conn
            .query_row(
                "SELECT direction FROM agent_lifecycle_federations WHERE peer_id = ?1",
                params![peer_id],
                |r| r.get::<_, String>(0),
            )
            .ok();
        let Some(d) = dir else { return Ok(false) };
        let parsed: FederationDirection = d.parse().unwrap_or(FederationDirection::Both);
        Ok(matches!(
            parsed,
            FederationDirection::Inbound | FederationDirection::Both
        ))
    }

    /// Enqueue a serialized lifecycle event, allocating `sender_seq` atomically per peer.
    pub fn agent_lifecycle_enqueue(&self, peer_id: &str, payload: &[u8]) -> Result<u64> {
        let mut conn = self.conn_lock();
        let now = Utc::now().timestamp();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let next_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(sender_seq), 0) + 1 FROM agent_lifecycle_outbox
             WHERE peer_id = ?1",
            params![peer_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO agent_lifecycle_outbox
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

    pub fn agent_lifecycle_next_outbox(
        &self,
        peer_id: &str,
        now: i64,
    ) -> Result<Option<AgentLifecycleOutboxRow>> {
        let conn = self.conn_lock();
        let row = conn
            .query_row(
                "SELECT id, sender_seq, payload, attempts
                 FROM agent_lifecycle_outbox
                 WHERE peer_id = ?1 AND state = 'pending' AND next_attempt_at <= ?2
                 ORDER BY created_at ASC LIMIT 1",
                params![peer_id, now],
                |r| {
                    Ok(AgentLifecycleOutboxRow {
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

    pub fn agent_lifecycle_mark_in_flight(&self, id: &str) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "UPDATE agent_lifecycle_outbox SET state = 'in_flight' WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn agent_lifecycle_mark_delivered(&self, id: &str) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "DELETE FROM agent_lifecycle_outbox WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn agent_lifecycle_mark_failed_retry(&self, id: &str, next_attempt_at: i64) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "UPDATE agent_lifecycle_outbox
             SET state = 'pending', attempts = attempts + 1, next_attempt_at = ?2
             WHERE id = ?1",
            params![id, next_attempt_at],
        )?;
        Ok(())
    }

    /// Boot recovery: revert `in_flight` to `pending` for reship; receiver
    /// dedupe makes the resend idempotent.
    pub fn agent_lifecycle_reset_in_flight(&self) -> Result<usize> {
        let conn = self.conn_lock();
        Ok(conn.execute(
            "UPDATE agent_lifecycle_outbox SET state = 'pending' WHERE state = 'in_flight'",
            [],
        )?)
    }

    /// Inbox dedupe. Returns `true` on first sighting (caller
    /// republishes), `false` on replay (caller drops with positive ack).
    pub fn agent_lifecycle_inbox_record(
        &self,
        sender_daemon_id: &str,
        sender_seq: u64,
    ) -> Result<bool> {
        let conn = self.conn_lock();
        let seq_i = i64::try_from(sender_seq).unwrap_or(i64::MAX);
        let n = conn.execute(
            "INSERT OR IGNORE INTO agent_lifecycle_inbox
                (sender_daemon_id, sender_seq, received_at)
             VALUES (?1, ?2, ?3)",
            params![sender_daemon_id, seq_i, Utc::now().timestamp()],
        )?;
        Ok(n > 0)
    }
}
