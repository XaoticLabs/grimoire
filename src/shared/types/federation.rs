//! Federation peer types: peers, outbox/inbox rows, and topic/namespace
//! federation declarations.

use super::PeerId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerState {
    Pending,
    Active,
    Down,
    Removing,
}

impl_state_enum!(PeerState {
    Pending => "pending",
    Active => "active",
    Down => "down",
    Removing => "removing",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerOutboxState {
    Pending,
    InFlight,
    Delivered,
    Failed,
}

impl_state_enum!(PeerOutboxState {
    Pending => "pending",
    InFlight => "in_flight",
    Delivered => "delivered",
    Failed => "failed",
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FederationDirection {
    Inbound,
    Outbound,
    Both,
}

impl_state_enum!(FederationDirection {
    Inbound => "inbound",
    Outbound => "outbound",
    Both => "both",
});

impl FederationDirection {
    /// Merge two direction declarations into the most-permissive form.
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        if self == other {
            return self;
        }
        Self::Both
    }

    pub const fn allows_outbound(&self) -> bool {
        matches!(self, Self::Outbound | Self::Both)
    }

    pub const fn allows_inbound(&self) -> bool {
        matches!(self, Self::Inbound | Self::Both)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Peer {
    pub id: PeerId,
    /// Remote DaemonId. Empty until the handshake completes for `Pending` rows.
    pub daemon_id: String,
    pub name: String,
    pub url: String,
    pub bearer_token_hash: Vec<u8>,
    /// Plaintext bearer token. Stored alongside the hash so the outbound
    /// client task can re-issue `Hello` after daemon restarts. Kept in
    /// plaintext (rather than hash-only) until a token-rotation UX lands.
    pub bearer_token: String,
    /// PEM-encoded certificate of the remote daemon, pinned out-of-band at
    /// `peer add` time and used as the sole TLS trust anchor for both the
    /// outbound client (server-cert verification) and the inbound listener
    /// (client-cert verification). The `peers.public_key` column carries it.
    pub public_key: Option<Vec<u8>>,
    pub state: PeerState,
    pub last_seen: Option<i64>,
    pub registered_at: i64,
}

impl Peer {
    /// The pinned remote certificate as a PEM string, if present.
    #[must_use]
    pub fn pinned_cert_pem(&self) -> Option<String> {
        self.public_key
            .as_ref()
            .and_then(|b| String::from_utf8(b.clone()).ok())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerOutboxRow {
    pub id: String,
    pub peer_id: PeerId,
    pub mail_id: String,
    pub sender_seq: u64,
    pub recipient: String,
    pub sender: Option<String>,
    pub topic: Option<String>,
    pub body: String,
    pub created_at: i64,
    pub attempts: u32,
    pub next_attempt_at: i64,
    pub state: PeerOutboxState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerInboxRow {
    pub sender_daemon_id: String,
    pub sender_seq: u64,
    pub mail_id: String,
    pub received_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopicFederation {
    pub id: String,
    pub peer_id: PeerId,
    pub topic: String,
    pub direction: FederationDirection,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceFederation {
    pub id: String,
    pub peer_id: PeerId,
    pub namespace: String,
    pub direction: FederationDirection,
    pub created_at: i64,
}

#[cfg(test)]
mod federation_type_tests {
    use super::*;
    use crate::shared::types::validate_daemon_id;

    #[test]
    fn validate_daemon_id_accepts_lowercase_hex() {
        assert!(validate_daemon_id("abcd1234"));
        assert!(validate_daemon_id("00000000"));
        assert!(validate_daemon_id("ffffffff"));
    }

    #[test]
    fn validate_daemon_id_rejects_other() {
        assert!(!validate_daemon_id(""));
        assert!(!validate_daemon_id("abcd123"));
        assert!(!validate_daemon_id("abcd1234x"));
        assert!(!validate_daemon_id("ABCD1234"));
        assert!(!validate_daemon_id("ghij1234"));
    }

    #[test]
    fn federation_direction_merge_idempotent_to_both() {
        assert_eq!(
            FederationDirection::Outbound.merge(FederationDirection::Inbound),
            FederationDirection::Both
        );
        assert_eq!(
            FederationDirection::Outbound.merge(FederationDirection::Outbound),
            FederationDirection::Outbound
        );
        assert_eq!(
            FederationDirection::Outbound.merge(FederationDirection::Both),
            FederationDirection::Both
        );
    }
}
