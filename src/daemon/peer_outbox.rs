//! Generic per-peer outbox drainer.
//!
//! Both the federation mail path and the namespace replication path follow
//! the same shape: a `pending` row is claimed by flipping it to `in_flight`,
//! shipped as a single `PeerOutbound` frame, and resolved on the matching
//! ack (`delivered` on success, `pending` with bumped `attempts` +
//! `next_attempt_at` on failure). The per-table specifics live behind
//! [`OutboxBackend`] and the common loop body lives in [`pump_one_row`] /
//! [`handle_ack_outcome`].
//!
//! Bus-event emission (mail's `PeerMailForwarded` / `PeerMailForwardFailed`)
//! is intentionally kept at the call site, not on the trait. Memory has
//! no equivalent and other drainers each carry their own observability
//! needs.

use tokio::sync::mpsc;

use crate::shared::peer_proto::PeerOutbound;
use crate::shared::types::PeerOutboxRow;

use super::persistence::unix_now;

/// Computed retry delay for the `attempts`-th failure (1-based). Caps at 60s.
pub fn backoff_secs(attempts: u32) -> u64 {
    let exp = 1u64 << attempts.saturating_sub(1).min(6);
    exp.min(60)
}

/// Predicate used by the drainer to decide whether to halt without
/// writing further acks (peer is being torn down).
pub const fn should_halt(state: &crate::shared::types::PeerState) -> bool {
    matches!(state, crate::shared::types::PeerState::Removing)
}

/// Convert an outbox row into the wire `MailDeliver` message.
pub fn row_to_mail_deliver(row: &PeerOutboxRow) -> crate::shared::peer_proto::MailDeliver {
    crate::shared::peer_proto::MailDeliver {
        mail_id: row.mail_id.clone(),
        sender: row.sender.clone().unwrap_or_default(),
        recipient: row.recipient.clone(),
        body: row.body.clone(),
        topic: row.topic.clone(),
        sender_seq: row.sender_seq,
    }
}

/// What the per-peer client task remembers about a row it has shipped
/// and is waiting on an ack for. Single in-flight slot per drainer keeps
/// the wire serial; the receiving side only needs to ack one outstanding
/// row at a time.
#[derive(Debug, Clone)]
pub struct InFlight {
    /// Outbox row PK (used to drive `mark_delivered` / `mark_failed_retry`).
    pub row_id: String,
    /// Prior failure count carried from the row, so a failed ack grows
    /// the retry backoff off the real attempt number rather than always
    /// reapplying the base delay.
    pub attempts: u32,
    /// Correlation key the receiver echoes back in its ack
    /// (`MailAck.mail_id`, `MemoryAck.op_id`, …). The caller compares it
    /// against the inbound ack before invoking [`handle_ack_outcome`] so
    /// we don't resolve the wrong row on a delayed/reordered ack.
    pub ack_key: String,
}

/// Per-table glue: how to read the next pending row, transition row
/// state, and translate a row into the wire message that ships it. One
/// impl per outbox table.
pub trait OutboxBackend: Sync {
    /// Row type returned by [`Self::next_row`]. Carries everything the
    /// drainer needs to build the outbound frame plus track the in-flight
    /// slot. Must be `Send` because [`pump_one_row`] holds the value
    /// briefly while awaiting `out_tx.send`.
    type Row: Send;

    /// Pop the next `pending` row for `peer_id` whose `next_attempt_at`
    /// has elapsed. Returns `None` when the queue is drained.
    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>>;

    /// Flip the row to `in_flight` so a concurrent pump (e.g. after
    /// reconnect) won't double-send it.
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()>;

    /// Terminal-success transition. Implementations may delete the row
    /// (memory) or move it to `delivered` (mail); both are correct.
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()>;

    /// Failed-ack / send-error transition: row returns to `pending` with
    /// `attempts` bumped and `next_attempt_at` set to the backoff target.
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()>;

    fn row_id(row: &Self::Row) -> &str;
    fn row_attempts(row: &Self::Row) -> u32;
    /// Ack-correlation key (mail_id, op_id, …) the receiver echoes back.
    fn row_ack_key(row: &Self::Row) -> String;
    /// Build the `PeerOutbound` frame that ships this row over the wire.
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound;
}

/// Drain a single pending row for `peer_id`, transition it to
/// `in_flight`, and ship it. No-op if `in_flight` is already occupied or
/// the peer is being torn down (both cases are routine, not errors).
///
/// On send failure the row is bumped back to `pending` with backoff
/// applied before the error propagates, so a stuck channel won't pin a
/// row at `in_flight` forever.
pub async fn pump_one_row<B: OutboxBackend>(
    backend: &B,
    peer_id: &str,
    peer_removing: bool,
    out_tx: &mpsc::Sender<PeerOutbound>,
    in_flight: &mut Option<InFlight>,
) -> anyhow::Result<()> {
    if in_flight.is_some() {
        return Ok(());
    }
    if peer_removing {
        return Ok(());
    }
    let now = unix_now();
    let Some(row) = backend.next_row(peer_id, now)? else {
        return Ok(());
    };
    let row_id = B::row_id(&row).to_string();
    let attempts = B::row_attempts(&row);
    let ack_key = B::row_ack_key(&row);
    backend.mark_in_flight(&row_id)?;
    let outbound = B::row_to_outbound(&row);
    if let Err(e) = out_tx.send(outbound).await {
        let backoff = backoff_secs(attempts + 1);
        let _ = backend.mark_failed_retry(&row_id, now + backoff as i64);
        return Err(anyhow::anyhow!("send: {e}"));
    }
    *in_flight = Some(InFlight {
        row_id,
        attempts,
        ack_key,
    });
    Ok(())
}

/// Resolve the in-flight slot against an ack outcome. `ok=true` →
/// terminal-success; otherwise the row is rescheduled with backoff
/// computed off `in_flight.attempts + 1` (the just-completed delivery's
/// real attempt number).
///
/// Caller's responsibility: ack-key match (compare `ack.<key>` to
/// `in_flight.ack_key`) and emitting any bus events (kept out of this
/// function so memory's no-event policy stays clean).
pub fn handle_ack_outcome<B: OutboxBackend>(backend: &B, in_flight: &InFlight, ok: bool) {
    let now = unix_now();
    if ok {
        let _ = backend.mark_delivered(&in_flight.row_id);
    } else {
        let backoff = backoff_secs(in_flight.attempts + 1);
        let _ = backend.mark_failed_retry(&in_flight.row_id, now + backoff as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_caps_at_60s() {
        assert_eq!(backoff_secs(1), 1);
        assert_eq!(backoff_secs(2), 2);
        assert_eq!(backoff_secs(3), 4);
        assert_eq!(backoff_secs(7), 60);
        assert_eq!(backoff_secs(20), 60);
    }
}
