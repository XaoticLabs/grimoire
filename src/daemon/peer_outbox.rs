//! Per-peer outbox drainer (Task 8). The drainer logic is currently
//! co-located with `peer_client::run_once` for v1 — this module exists
//! as a placeholder for future extraction (e.g. when the drainer needs
//! its own task to support pipelined acks).

use crate::shared::types::PeerOutboxRow;

/// Computed retry delay for the `attempts`-th failure (1-based). Caps at
/// 60s (federation-spec ambiguity table).
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
