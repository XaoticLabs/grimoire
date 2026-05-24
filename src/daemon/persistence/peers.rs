use anyhow::Result;
use rusqlite::params;

use crate::shared::types::Mail;

use super::{row_to_outbox, row_to_peer, row_to_topic_federation};

/// Column list for `SELECT … FROM peers`. Matches `row_to_peer`.
const PEER_COLS: &str = "id, daemon_id, name, url, bearer_token_hash, bearer_token, public_key, state, last_seen, registered_at";

impl super::Database {
    pub fn insert_peer(&self, peer: &crate::shared::types::Peer) -> Result<()> {
        self.exec(
            "INSERT INTO peers (id, daemon_id, name, url, bearer_token_hash, bearer_token, public_key, state, last_seen, registered_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                peer.id,
                peer.daemon_id,
                peer.name,
                peer.url,
                peer.bearer_token_hash,
                peer.bearer_token,
                peer.public_key,
                peer.state.as_str(),
                peer.last_seen,
                peer.registered_at,
            ],
        )?;
        Ok(())
    }

    pub fn delete_peer(&self, peer_id: &str) -> Result<bool> {
        Ok(self.exec("DELETE FROM peers WHERE id = ?1", params![peer_id])? > 0)
    }

    pub fn get_peer_by_name(&self, name: &str) -> Result<Option<crate::shared::types::Peer>> {
        self.query_opt(
            &format!("SELECT {PEER_COLS} FROM peers WHERE name = ?1"),
            params![name],
            row_to_peer,
        )
    }

    pub fn get_peer_by_daemon_id(
        &self,
        daemon_id: &str,
    ) -> Result<Option<crate::shared::types::Peer>> {
        self.query_opt(
            &format!("SELECT {PEER_COLS} FROM peers WHERE daemon_id = ?1"),
            params![daemon_id],
            row_to_peer,
        )
    }

    pub fn get_peer(&self, peer_id: &str) -> Result<Option<crate::shared::types::Peer>> {
        self.query_opt(
            &format!("SELECT {PEER_COLS} FROM peers WHERE id = ?1"),
            params![peer_id],
            row_to_peer,
        )
    }

    pub fn lookup_peer_by_token_hash(
        &self,
        hash: &[u8],
    ) -> Result<Option<crate::shared::types::Peer>> {
        self.query_opt(
            &format!("SELECT {PEER_COLS} FROM peers WHERE bearer_token_hash = ?1"),
            params![hash],
            row_to_peer,
        )
    }

    pub fn list_peers(&self) -> Result<Vec<crate::shared::types::Peer>> {
        self.query_vec(
            &format!("SELECT {PEER_COLS} FROM peers ORDER BY registered_at"),
            [],
            row_to_peer,
        )
    }

    pub fn set_peer_state(
        &self,
        peer_id: &str,
        state: crate::shared::types::PeerState,
    ) -> Result<()> {
        self.exec(
            "UPDATE peers SET state = ?1 WHERE id = ?2",
            params![state.as_str(), peer_id],
        )?;
        Ok(())
    }

    pub fn set_peer_last_seen(&self, peer_id: &str, ts: i64) -> Result<()> {
        self.exec(
            "UPDATE peers SET last_seen = ?1 WHERE id = ?2",
            params![ts, peer_id],
        )?;
        Ok(())
    }

    pub fn update_peer_daemon_id(&self, peer_id: &str, daemon_id: &str) -> Result<()> {
        self.exec(
            "UPDATE peers SET daemon_id = ?1 WHERE id = ?2",
            params![daemon_id, peer_id],
        )?;
        Ok(())
    }

    pub fn outbox_depth(&self, peer_id: &str) -> Result<u64> {
        let conn = self.conn_lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM peer_outbox WHERE peer_id = ?1 AND state IN ('pending','in_flight')",
            params![peer_id],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    /// Atomic: insert a `mail` row + `peer_outbox` row in a single
    /// IMMEDIATE transaction. `mail.seq` is computed per recipient as
    /// usual; `peer_outbox.sender_seq` is computed per `peer_id`.
    pub fn insert_mail_with_outbox(
        &self,
        mail: &Mail,
        peer_id: &str,
        outbox_id: &str,
        recipient: &str,
        topic: Option<&str>,
        next_attempt_at: i64,
    ) -> Result<u64> {
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mail_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?1",
            params![mail.recipient_id],
            |r| r.get(0),
        )?;
        Self::insert_mail_with_seq_in_tx(&tx, mail, mail_seq)?;
        let sender_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(sender_seq) + 1, 1) FROM peer_outbox WHERE peer_id = ?1",
            params![peer_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO peer_outbox (id, peer_id, mail_id, sender_seq, recipient, sender, topic, body, created_at, attempts, next_attempt_at, state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, 'pending')",
            params![
                outbox_id,
                peer_id,
                mail.id,
                sender_seq,
                recipient,
                mail.sender_id,
                topic,
                mail.body,
                mail.created_at,
                next_attempt_at,
            ],
        )?;
        tx.commit()?;
        Ok(sender_seq as u64)
    }

    /// Pop the next `Pending` outbox row whose `next_attempt_at <= now`.
    pub fn next_outbox_row(
        &self,
        peer_id: &str,
        now: i64,
    ) -> Result<Option<crate::shared::types::PeerOutboxRow>> {
        self.query_opt(
            "SELECT id, peer_id, mail_id, sender_seq, recipient, sender, topic, body, created_at, attempts, next_attempt_at, state \
             FROM peer_outbox WHERE peer_id = ?1 AND state = 'pending' AND next_attempt_at <= ?2 \
             ORDER BY sender_seq ASC LIMIT 1",
            params![peer_id, now],
            row_to_outbox,
        )
    }

    pub fn mark_outbox_in_flight(&self, id: &str) -> Result<()> {
        self.exec(
            "UPDATE peer_outbox SET state = 'in_flight' WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn mark_outbox_delivered(&self, id: &str) -> Result<()> {
        self.exec(
            "UPDATE peer_outbox SET state = 'delivered', attempts = attempts + 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn mark_outbox_failed_retry(&self, id: &str, next_attempt_at: i64) -> Result<()> {
        self.exec(
            "UPDATE peer_outbox SET state = 'pending', attempts = attempts + 1, next_attempt_at = ?2 WHERE id = ?1",
            params![id, next_attempt_at],
        )?;
        Ok(())
    }

    /// On boot, flip any `in_flight` outbox rows back to `pending` so the
    /// drainer re-sends them. Idempotency on the receiver dedupes any
    /// already-delivered messages.
    pub fn reset_outbox_in_flight(&self) -> Result<u32> {
        let conn = self.conn_lock();
        let n = conn.execute(
            "UPDATE peer_outbox SET state = 'pending' WHERE state = 'in_flight'",
            [],
        )?;
        Ok(n as u32)
    }

    /// Idempotency-keyed inbox insert. Returns `true` if this is a new
    /// delivery (insertion happened); `false` if the (daemon, seq) pair
    /// already existed (replay).
    pub fn insert_peer_inbox_if_absent(
        &self,
        sender_daemon_id: &str,
        sender_seq: u64,
        mail_id: &str,
        received_at: i64,
    ) -> Result<bool> {
        let conn = self.conn_lock();
        let n = conn.execute(
            "INSERT OR IGNORE INTO peer_inbox (sender_daemon_id, sender_seq, mail_id, received_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![sender_daemon_id, sender_seq as i64, mail_id, received_at],
        )?;
        Ok(n > 0)
    }

    pub fn insert_topic_federation(
        &self,
        id: &str,
        peer_id: &str,
        topic: &str,
        direction: crate::shared::types::FederationDirection,
        created_at: i64,
    ) -> Result<()> {
        self.exec(
            "INSERT INTO topic_federations (id, peer_id, topic, direction, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, peer_id, topic, direction.as_str(), created_at],
        )?;
        Ok(())
    }

    pub fn upsert_topic_federation(
        &self,
        id: &str,
        peer_id: &str,
        topic: &str,
        direction: crate::shared::types::FederationDirection,
        created_at: i64,
    ) -> Result<crate::shared::types::FederationDirection> {
        use crate::shared::types::FederationDirection;
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT direction FROM topic_federations WHERE peer_id = ?1 AND topic = ?2",
                params![peer_id, topic],
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
            "INSERT INTO topic_federations (id, peer_id, topic, direction, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(peer_id, topic) DO UPDATE SET direction = excluded.direction",
            params![id, peer_id, topic, final_dir.as_str(), created_at],
        )?;
        tx.commit()?;
        Ok(final_dir)
    }

    pub fn delete_topic_federation(&self, peer_id: &str, topic: &str) -> Result<bool> {
        Ok(self.exec(
            "DELETE FROM topic_federations WHERE peer_id = ?1 AND topic = ?2",
            params![peer_id, topic],
        )? > 0)
    }

    pub fn list_outbound_federations_for_topic(
        &self,
        topic: &str,
    ) -> Result<Vec<crate::shared::types::TopicFederation>> {
        self.query_vec(
            "SELECT id, peer_id, topic, direction, created_at FROM topic_federations WHERE topic = ?1 AND direction IN ('outbound','both')",
            params![topic],
            row_to_topic_federation,
        )
    }

    pub fn list_topic_federations(&self) -> Result<Vec<crate::shared::types::TopicFederation>> {
        self.query_vec(
            "SELECT id, peer_id, topic, direction, created_at FROM topic_federations ORDER BY topic, peer_id",
            params![],
            row_to_topic_federation,
        )
    }

    pub fn topic_federation_inbound_authorized(&self, peer_id: &str, topic: &str) -> Result<bool> {
        let conn = self.conn_lock();
        let dir: Option<String> = conn
            .query_row(
                "SELECT direction FROM topic_federations WHERE peer_id = ?1 AND topic = ?2",
                params![peer_id, topic],
                |r| r.get::<_, String>(0),
            )
            .ok();
        Ok(matches!(dir.as_deref(), Some("inbound" | "both")))
    }
}
