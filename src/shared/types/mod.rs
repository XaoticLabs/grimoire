//! Shared domain value types, grouped by concern and re-exported flat so
//! `crate::shared::types::X` resolves for every public type.
#![allow(missing_docs)] // Shared value types; documentation pass pending.

pub type AgentId = String;

/// Stable 8-hex identifier minted on first `grimd` boot. Two daemons in a
/// federation use distinct DaemonIds to disambiguate agent addresses.
/// Display form prefixes `grimd-`; storage is the bare 8-hex string.
pub type DaemonId = String;

/// `^[0-9a-f]{8}$`.
pub fn validate_daemon_id(s: &str) -> bool {
    if s.len() != 8 {
        return false;
    }
    s.bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub type PeerId = String;

// --- State Enums with consistent FromStr + Display ---

macro_rules! impl_state_enum {
    ($name:ident { $($variant:ident => $str:literal),+ $(,)? }) => {
        impl $name {
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$variant => $str),+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl std::str::FromStr for $name {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($str => Ok(Self::$variant)),+,
                    _ => Err(anyhow::anyhow!("invalid {} value: '{}'", stringify!($name), s)),
                }
            }
        }
    };
}
// `impl_state_enum!` is in textual macro scope for every submodule declared
// below, so no `use`/re-export is needed.

mod agents;
mod federation;
mod mail;
mod pacts;
mod scrolls;
mod wake;
mod workspaces;

pub use agents::*;
pub use federation::*;
pub use mail::*;
pub use pacts::*;
pub use scrolls::*;
pub use wake::*;
pub use workspaces::*;
