use anyhow::Result;
use rusqlite::params;

use crate::shared::types::{AgentId, Mail, MailState, Subscription};

use super::{MAIL_COLS, OutboxFanoutRow, row_to_mail, row_to_subscription, unix_now};

impl super::Database {
    /// `seq` is computed per `recipient_id` inside an IMMEDIATE transaction so
    /// concurrent inserts to the same recipient serialize.
    pub fn insert_mail(&self, mail: &Mail) -> Result<()> {
        if mail.recipient_id.is_empty() {
            anyhow::bail!("recipient_id must not be empty");
        }
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?1",
            params![mail.recipient_id],
            |r| r.get(0),
        )?;
        tx.execute(
            &format!("INSERT INTO mail ({MAIL_COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"),
            params![
                mail.id,
                mail.recipient_id,
                mail.sender_id,
                mail.topic,
                mail.body,
                mail.in_reply_to,
                mail.state.as_str(),
                mail.fail_reason,
                mail.created_at,
                mail.delivered_at,
                seq,
                i64::from(mail.wake_eligible),
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Insert with a caller-provided `seq`, for multi-row fanout in one tx.
    pub(super) fn insert_mail_with_seq_in_tx(
        tx: &rusqlite::Transaction<'_>,
        mail: &Mail,
        seq: i64,
    ) -> Result<()> {
        tx.execute(
            &format!("INSERT INTO mail ({MAIL_COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"),
            params![
                mail.id,
                mail.recipient_id,
                mail.sender_id,
                mail.topic,
                mail.body,
                mail.in_reply_to,
                mail.state.as_str(),
                mail.fail_reason,
                mail.created_at,
                mail.delivered_at,
                seq,
                i64::from(mail.wake_eligible),
            ],
        )?;
        Ok(())
    }

    /// Insert mail rows + per-peer `peer_outbox` fanout rows in one IMMEDIATE
    /// transaction. `mail.seq` is per recipient; outbox `sender_seq` is per peer.
    pub fn insert_mail_batch_with_outbox(
        &self,
        mails: &[Mail],
        outbox_fanout: &[OutboxFanoutRow],
    ) -> Result<()> {
        if mails.is_empty() && outbox_fanout.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for m in mails {
            if m.recipient_id.is_empty() {
                anyhow::bail!("recipient_id must not be empty");
            }
            let seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?1",
                params![m.recipient_id],
                |r| r.get(0),
            )?;
            Self::insert_mail_with_seq_in_tx(&tx, m, seq)?;
        }
        for (peer_id, outbox_id, mail_id, recipient, body, sender, created_at) in outbox_fanout {
            let sender_seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(sender_seq) + 1, 1) FROM peer_outbox WHERE peer_id = ?1",
                params![peer_id],
                |r| r.get(0),
            )?;
            // For topic fanout, `recipient` carries the remote topic address
            // (`topic://<name>`); receivers fan out per `topic_federations`.
            tx.execute(
                "INSERT INTO peer_outbox (id, peer_id, mail_id, sender_seq, recipient, sender, topic, body, created_at, attempts, next_attempt_at, state)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?9, 'pending')",
                params![
                    outbox_id,
                    peer_id,
                    mail_id,
                    sender_seq,
                    recipient,
                    sender,
                    Some(recipient.strip_prefix("topic://").unwrap_or("")),
                    body,
                    created_at,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Insert mail rows in one IMMEDIATE transaction so a partial topic fanout
    /// cannot be observed. Each `seq` is computed per recipient.
    pub fn insert_mail_batch(&self, mails: &[Mail]) -> Result<()> {
        if mails.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for m in mails {
            if m.recipient_id.is_empty() {
                anyhow::bail!("recipient_id must not be empty");
            }
            let seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?1",
                params![m.recipient_id],
                |r| r.get(0),
            )?;
            Self::insert_mail_with_seq_in_tx(&tx, m, seq)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_mail_by_recipient(
        &self,
        recipient_id: &str,
        after_seq: Option<i64>,
        state_filter: Option<MailState>,
        limit: u32,
    ) -> Result<Vec<Mail>> {
        use std::fmt::Write;
        let limit = i64::from(limit.clamp(1, 1000));
        let mut sql = format!("SELECT {MAIL_COLS} FROM mail WHERE recipient_id = ?1");
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(recipient_id.to_string())];
        if let Some(s) = after_seq {
            let _ = write!(sql, " AND seq > ?{}", args.len() + 1);
            args.push(Box::new(s));
        }
        if let Some(st) = state_filter {
            let _ = write!(sql, " AND state = ?{}", args.len() + 1);
            args.push(Box::new(st.as_str().to_string()));
        }
        let _ = write!(sql, " ORDER BY seq ASC LIMIT {limit}");

        let conn = self.conn_lock();
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(args.iter().map(|b| &**b)))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row_to_mail(row)?);
        }
        Ok(out)
    }

    pub fn get_mail(&self, id: &str) -> Result<Option<Mail>> {
        self.query_opt(
            &format!("SELECT {MAIL_COLS} FROM mail WHERE id = ?1"),
            params![id],
            row_to_mail,
        )
    }

    /// Find a mail row by short id prefix. `Err` if the prefix is ambiguous.
    pub fn get_mail_by_prefix(&self, prefix: &str) -> Result<Option<Mail>> {
        let conn = self.conn_lock();
        let sql = format!("SELECT {MAIL_COLS} FROM mail WHERE id LIKE ?1 || '%' LIMIT 2");
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(params![prefix])?;
        let first = match rows.next()? {
            Some(r) => row_to_mail(r)?,
            None => return Ok(None),
        };
        if rows.next()?.is_some() {
            anyhow::bail!("Ambiguous mail prefix '{prefix}'");
        }
        Ok(Some(first))
    }

    pub fn set_mail_state(
        &self,
        id: &str,
        new_state: MailState,
        fail_reason: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn_lock();
        let now = unix_now();
        let delivered_at: Option<i64> = match new_state {
            MailState::Delivered | MailState::Failed => Some(now),
            MailState::Pending => None,
        };
        let n = conn.execute(
            "UPDATE mail SET state = ?1, fail_reason = COALESCE(?2, fail_reason), delivered_at = COALESCE(?3, delivered_at) WHERE id = ?4",
            params![new_state.as_str(), fail_reason, delivered_at, id],
        )?;
        if n == 0 {
            anyhow::bail!("mail not found: {id}");
        }
        Ok(())
    }

    pub fn list_pending_wake_eligible(&self, recipient_id: &str) -> Result<Vec<Mail>> {
        self.query_vec(
            &format!("SELECT {MAIL_COLS} FROM mail WHERE recipient_id = ?1 AND state = 'Pending' AND wake_eligible = 1 ORDER BY seq ASC"),
            params![recipient_id],
            row_to_mail,
        )
    }

    /// Distinct recipient ids with at least one Pending, wake-eligible mail row.
    pub fn list_recipients_with_pending_wake_eligible_mail(&self) -> Result<Vec<AgentId>> {
        self.query_vec(
            "SELECT recipient_id, MIN(seq) FROM mail \
             WHERE state = 'Pending' AND wake_eligible = 1 \
             GROUP BY recipient_id ORDER BY MIN(seq) ASC",
            [],
            |row| Ok(row.get(0)?),
        )
    }

    /// Insert a subscription, returning the existing id on UNIQUE
    /// (subscriber_id, topic) conflict rather than erroring.
    pub fn insert_subscription(&self, sub: &Subscription) -> Result<String> {
        let mut conn = self.conn_lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT id FROM subscriptions WHERE subscriber_id = ?1 AND topic = ?2",
                params![sub.subscriber_id, sub.topic],
                |r| r.get(0),
            )
            .ok();
        if let Some(id) = existing {
            tx.commit()?;
            return Ok(id);
        }
        tx.execute(
            "INSERT INTO subscriptions (id, subscriber_id, topic, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![sub.id, sub.subscriber_id, sub.topic, sub.created_at],
        )?;
        tx.commit()?;
        Ok(sub.id.clone())
    }

    pub fn delete_subscription(&self, id: &str) -> Result<bool> {
        Ok(self.exec("DELETE FROM subscriptions WHERE id = ?1", params![id])? > 0)
    }

    pub fn list_subscribers_for_topic(&self, topic: &str) -> Result<Vec<Subscription>> {
        self.query_vec(
            "SELECT id, subscriber_id, topic, created_at FROM subscriptions WHERE topic = ?1 ORDER BY created_at ASC, id ASC",
            params![topic],
            row_to_subscription,
        )
    }

    pub fn list_subscriptions_by_subscriber(&self, agent_id: &str) -> Result<Vec<Subscription>> {
        self.query_vec(
            "SELECT id, subscriber_id, topic, created_at FROM subscriptions WHERE subscriber_id = ?1 ORDER BY topic ASC",
            params![agent_id],
            row_to_subscription,
        )
    }

    pub fn list_topics_with_counts(&self) -> Result<Vec<(String, u32)>> {
        self.query_vec(
            "SELECT topic, COUNT(*) FROM subscriptions GROUP BY topic ORDER BY topic ASC",
            [],
            |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u32)),
        )
    }
}
