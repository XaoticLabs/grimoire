#![allow(missing_docs)] // Module-level doc covers the model; per-item docs pending.

//! Mail address parsing.
//!
//! Two schemes are supported:
//!
//!   * `agent://<id>`, where `<id>` is the 8-char short id (`[0-9a-f]{8}`).
//!   * `topic://<name>`, where `<name>` is one or more slash-separated
//!     segments each matching `[a-zA-Z0-9][a-zA-Z0-9._:-]*`, total length
//!     capped at 128.
//!
//! Anything else, including a bare string with no `://`, is rejected.

use crate::shared::types::{AgentId, DaemonId, validate_daemon_id};

/// A parsed destination for `mail.send`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    Agent(AgentId),
    Topic(String),
    /// `agent://grimd-<daemon-id>/<agent-id>` (federation form).
    FederatedAgent {
        daemon_id: DaemonId,
        agent_id: AgentId,
    },
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Agent(id) => write!(f, "agent://{id}"),
            Self::Topic(name) => write!(f, "topic://{name}"),
            Self::FederatedAgent {
                daemon_id,
                agent_id,
            } => {
                write!(f, "agent://grimd-{daemon_id}/{agent_id}")
            }
        }
    }
}

/// Reason a string failed to parse as an `Address`. The error code (`code()`)
/// is the value returned to RPC callers via `RpcError`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum AddressParseError {
    InvalidAgentId,
    InvalidTopicName,
    InvalidAddress,
    InvalidFederatedAgentId,
}

impl AddressParseError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidAgentId => "invalid_agent_id",
            Self::InvalidTopicName => "invalid_topic_name",
            Self::InvalidAddress => "invalid_address",
            Self::InvalidFederatedAgentId => "invalid_federated_agent_id",
        }
    }
}

impl std::fmt::Display for AddressParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for AddressParseError {}

pub fn parse_address(s: &str) -> Result<Address, AddressParseError> {
    if let Some(rest) = s.strip_prefix("agent://") {
        // Try the federated form first (`grimd-<daemon-id>/<agent-id>`).
        // Falls back to bare 8-hex.
        if let Some(stripped) = rest.strip_prefix("grimd-") {
            // Strict shape: <8hex>/<8hex>, no extra path segments.
            return match parse_federated_agent_tail(stripped) {
                Some((daemon_id, agent_id)) => Ok(Address::FederatedAgent {
                    daemon_id,
                    agent_id,
                }),
                None => Err(AddressParseError::InvalidFederatedAgentId),
            };
        }
        if is_valid_agent_id(rest) {
            Ok(Address::Agent(rest.to_string()))
        } else {
            Err(AddressParseError::InvalidAgentId)
        }
    } else if let Some(rest) = s.strip_prefix("topic://") {
        if is_valid_topic_name(rest) {
            Ok(Address::Topic(rest.to_string()))
        } else {
            Err(AddressParseError::InvalidTopicName)
        }
    } else {
        Err(AddressParseError::InvalidAddress)
    }
}

/// Parse the tail of `agent://grimd-<rest>` into `(daemon_id, agent_id)`.
/// Both must be 8 lowercase hex characters; no extra segments allowed.
fn parse_federated_agent_tail(s: &str) -> Option<(DaemonId, AgentId)> {
    let (daemon, agent) = s.split_once('/')?;

    if !validate_daemon_id(daemon) {
        return None;
    }
    if !is_valid_agent_id(agent) {
        return None;
    }
    Some((daemon.to_string(), agent.to_string()))
}

/// `^[0-9a-f]{8}$`. The existing short-id shape; reject anything else,
/// including segments after a slash.
pub fn is_valid_agent_id(s: &str) -> bool {
    if s.len() != 8 {
        return false;
    }
    s.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Topic names are slash-separated segments. Each segment matches
/// `[a-zA-Z0-9][a-zA-Z0-9._:-]*` and the full string is capped at 128
/// chars. Slashes carry no semantics to the bus — they're just a naming
/// convention — but the daemon already publishes to multi-segment topics
/// (e.g. `workspace/<id>/files`, `workspace/<id>/memory/<prefix>`), so
/// subscribers must be able to name them too.
pub fn is_valid_topic_name(s: &str) -> bool {
    if s.is_empty() || s.len() > 128 {
        return false;
    }
    // Reject leading, trailing, or consecutive slashes — there's no valid
    // interpretation, and accepting them invites collisions in any
    // subscriber matching scheme.
    if s.starts_with('/') || s.ends_with('/') || s.contains("//") {
        return false;
    }
    for segment in s.split('/') {
        let mut bytes = segment.bytes();
        let Some(first) = bytes.next() else {
            return false;
        };
        if !first.is_ascii_alphanumeric() {
            return false;
        }
        for b in bytes {
            let ok = b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b':' || b == b'-';
            if !ok {
                return false;
            }
        }
    }
    true
}

/// Char-truncate a body to at most `max_chars` characters, returning a new
/// `String`. Used for the `body_preview` field in `MailReceived` events so
/// multi-byte codepoints aren't split mid-character.
pub fn body_preview(body: &str, max_chars: usize) -> String {
    body.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_address_accepts_agent_scheme() {
        assert_eq!(
            parse_address("agent://abcd1234").unwrap(),
            Address::Agent("abcd1234".into())
        );
    }

    #[test]
    fn parse_address_accepts_topic_scheme() {
        assert_eq!(
            parse_address("topic://pr-opened").unwrap(),
            Address::Topic("pr-opened".into())
        );
    }

    #[test]
    fn parse_address_rejects_uppercase_agent_id() {
        assert_eq!(
            parse_address("agent://ABCD1234").unwrap_err(),
            AddressParseError::InvalidAgentId
        );
    }

    #[test]
    fn parse_address_rejects_topic_with_space() {
        assert_eq!(
            parse_address("topic://has space").unwrap_err(),
            AddressParseError::InvalidTopicName
        );
    }

    #[test]
    fn parse_address_rejects_unknown_scheme() {
        assert_eq!(
            parse_address("grimd://host/x").unwrap_err(),
            AddressParseError::InvalidAddress
        );
    }

    #[test]
    fn parse_address_rejects_empty_string() {
        assert_eq!(
            parse_address("").unwrap_err(),
            AddressParseError::InvalidAddress
        );
    }

    #[test]
    fn parse_address_rejects_extra_path_segments() {
        assert_eq!(
            parse_address("agent://abcd1234/def").unwrap_err(),
            AddressParseError::InvalidAgentId
        );
    }

    #[test]
    fn body_preview_truncates_by_chars_not_bytes() {
        // 4-byte UTF-8 character (an emoji); preview must not split it.
        let body = "🎉".repeat(10);
        let preview = body_preview(&body, 5);
        assert_eq!(preview.chars().count(), 5);
    }

    // --- Federated address parser ---

    #[test]
    fn parse_address_accepts_federated_form() {
        let out = parse_address("agent://grimd-1a2b3c4d/deadbeef").unwrap();
        match out {
            Address::FederatedAgent {
                daemon_id,
                agent_id,
            } => {
                assert_eq!(daemon_id, "1a2b3c4d");
                assert_eq!(agent_id, "deadbeef");
            }
            _ => panic!("expected FederatedAgent"),
        }
    }

    #[test]
    fn parse_address_rejects_uppercase_federated_daemon_id() {
        assert_eq!(
            parse_address("agent://grimd-1A2B3C4D/abcd1234").unwrap_err(),
            AddressParseError::InvalidFederatedAgentId
        );
    }

    #[test]
    fn parse_address_rejects_federated_with_extra_segment() {
        assert_eq!(
            parse_address("agent://grimd-1a2b3c4d/deadbeef/x").unwrap_err(),
            AddressParseError::InvalidFederatedAgentId
        );
    }

    #[test]
    fn parse_address_bare_agent_form_still_works() {
        assert_eq!(
            parse_address("agent://abcd1234").unwrap(),
            Address::Agent("abcd1234".into())
        );
    }

    #[test]
    fn federated_address_round_trips_via_display() {
        let s = "agent://grimd-1a2b3c4d/deadbeef";
        let a = parse_address(s).unwrap();
        assert_eq!(a.to_string(), s);
        assert_eq!(parse_address(&a.to_string()).unwrap(), a);
    }

    #[test]
    fn parse_address_rejects_federated_trailing_slash() {
        assert_eq!(
            parse_address("agent://grimd-1a2b3c4d/").unwrap_err(),
            AddressParseError::InvalidFederatedAgentId
        );
    }

    // --- Topic name validator ---

    #[test]
    fn topic_name_accepts_single_segment() {
        assert!(is_valid_topic_name("pr-opened"));
        assert!(is_valid_topic_name("alerts.build"));
        assert!(is_valid_topic_name("foo_bar:baz"));
    }

    #[test]
    fn topic_name_accepts_slash_separated_segments() {
        // The daemon publishes workspace/<id>/files internally; subscribers
        // must be able to name them too.
        assert!(is_valid_topic_name("workspace/abc123/files"));
        assert!(is_valid_topic_name("workspace/abc123/memory/findings"));
        assert!(is_valid_topic_name("qa/escalations"));
    }

    #[test]
    fn topic_name_rejects_leading_trailing_or_double_slash() {
        assert!(!is_valid_topic_name("/foo"));
        assert!(!is_valid_topic_name("foo/"));
        assert!(!is_valid_topic_name("foo//bar"));
    }

    #[test]
    fn topic_name_rejects_segment_with_non_alnum_first_char() {
        assert!(!is_valid_topic_name("foo/.hidden"));
        assert!(!is_valid_topic_name("foo/-leading"));
    }

    #[test]
    fn topic_name_rejects_disallowed_chars() {
        assert!(!is_valid_topic_name("foo bar"));
        assert!(!is_valid_topic_name("foo$bar"));
        assert!(!is_valid_topic_name("foo/bar baz"));
    }

    #[test]
    fn topic_name_enforces_length_cap() {
        let long = "a".repeat(129);
        assert!(!is_valid_topic_name(&long));
        let max = "a".repeat(128);
        assert!(is_valid_topic_name(&max));
    }
}
