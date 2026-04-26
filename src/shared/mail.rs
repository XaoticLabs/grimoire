//! Mail address parsing.
//!
//! Two schemes are supported:
//!   * `agent://<id>` — `<id>` is the 8-char short id (`[0-9a-f]{8}`).
//!   * `topic://<name>` — `<name>` matches `^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$`.
//!
//! Anything else, including a bare string with no `://`, is rejected.

use crate::shared::types::AgentId;

/// A parsed destination for `mail.send`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    Agent(AgentId),
    Topic(String),
}

/// Reason a string failed to parse as an `Address`. The error code (`code()`)
/// is the value returned to RPC callers via `RpcError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressParseError {
    InvalidAgentId,
    InvalidTopicName,
    InvalidAddress,
}

impl AddressParseError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidAgentId => "invalid_agent_id",
            Self::InvalidTopicName => "invalid_topic_name",
            Self::InvalidAddress => "invalid_address",
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

/// `^[0-9a-f]{8}$` — the existing short-id shape. Reject anything else,
/// including segments after a slash.
pub fn is_valid_agent_id(s: &str) -> bool {
    if s.len() != 8 {
        return false;
    }
    s.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$`.
pub fn is_valid_topic_name(s: &str) -> bool {
    if s.is_empty() || s.len() > 128 {
        return false;
    }
    let mut bytes = s.bytes();
    let first = bytes.next().unwrap();
    if !(first.is_ascii_alphanumeric()) {
        return false;
    }
    for b in bytes {
        let ok = b.is_ascii_alphanumeric()
            || b == b'.'
            || b == b'_'
            || b == b':'
            || b == b'-';
        if !ok {
            return false;
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
        // 4-byte UTF-8 character (an emoji) — preview must not split it.
        let body = "🎉".repeat(10);
        let preview = body_preview(&body, 5);
        assert_eq!(preview.chars().count(), 5);
    }
}
