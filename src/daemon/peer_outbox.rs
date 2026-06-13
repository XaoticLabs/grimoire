//! Generic per-peer outbox drainer.
//!
//! Both the federation mail path and namespace replication share one shape: a
//! `pending` row is claimed (→ `in_flight`), shipped as one `PeerOutbound`
//! frame, and resolved on the matching ack (`delivered`, or back to `pending`
//! with bumped `attempts`/`next_attempt_at` on failure). Per-table specifics
//! live behind [`OutboxBackend`]; the loop body is [`pump_one_row`] /
//! [`handle_ack_outcome`].
//!
//! Bus-event emission stays at the call site, not on the trait, since memory
//! has no equivalent and drainers vary in observability needs.

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

/// A shipped row awaiting its ack. A single in-flight slot per drainer keeps
/// the wire serial — the receiver only acks one outstanding row at a time.
#[derive(Debug, Clone)]
pub struct InFlight {
    pub row_id: String,
    /// Prior failure count, so a failed ack grows backoff off the real attempt
    /// number rather than the base delay.
    pub attempts: u32,
    /// Key the receiver echoes back (`MailAck.mail_id`, `MemoryAck.op_id`, …).
    /// The caller matches it before [`handle_ack_outcome`] so a delayed or
    /// reordered ack can't resolve the wrong row.
    pub ack_key: String,
}

/// Per-table glue for the drainer. One impl per outbox table.
pub trait OutboxBackend: Sync {
    /// Must be `Send` because [`pump_one_row`] holds it across `out_tx.send`.
    type Row: Send;

    /// Next due `pending` row for `peer_id` (`next_attempt_at <= now`); `None`
    /// when drained.
    fn next_row(&self, peer_id: &str, now: i64) -> anyhow::Result<Option<Self::Row>>;

    /// Flip to `in_flight` so a concurrent pump won't double-send it.
    fn mark_in_flight(&self, row_id: &str) -> anyhow::Result<()>;

    /// Terminal success: delete (memory) or move to `delivered` (mail).
    fn mark_delivered(&self, row_id: &str) -> anyhow::Result<()>;

    /// Return to `pending` with `attempts` bumped and `next_attempt_at` at the
    /// backoff target.
    fn mark_failed_retry(&self, row_id: &str, next_attempt_at: i64) -> anyhow::Result<()>;

    fn row_id(row: &Self::Row) -> &str;
    fn row_attempts(row: &Self::Row) -> u32;
    /// Ack-correlation key (mail_id, op_id, …) the receiver echoes back.
    fn row_ack_key(row: &Self::Row) -> String;
    /// Build the `PeerOutbound` frame that ships this row over the wire.
    fn row_to_outbound(row: &Self::Row) -> PeerOutbound;
}

/// Claim, transition to `in_flight`, and ship one pending row. No-op (not an
/// error) if the slot is occupied or the peer is being torn down. On send
/// failure the row is bumped back to `pending` with backoff before the error
/// propagates, so a stuck channel can't pin a row at `in_flight` forever.
#[tracing::instrument(name = "outbox.pump_one_row", skip(backend, out_tx, in_flight), fields(peer_id = %peer_id, peer_removing))]
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

/// Resolve the in-flight slot. `ok` → delivered; else reschedule with backoff
/// off `in_flight.attempts + 1`. Caller must first match the ack key and emit
/// any bus events (kept out here so memory's no-event policy stays clean).
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
