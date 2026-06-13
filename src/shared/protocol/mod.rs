#![allow(missing_docs)] // RPC wire types; one-line docs per message pending.

//! JSON-RPC wire types shared between the CLI and the daemon, split by domain.
//! Every type is re-exported flat, so callers use `protocol::Foo` regardless of submodule.

mod agent;
mod envelope;
mod eval;
mod event;
mod federation;
mod mail;
mod namespace;
mod notify;
mod scroll;
mod supervisor;
mod wake;
mod workspace;

pub use agent::*;
pub use envelope::*;
pub use eval::*;
pub use event::*;
pub use federation::*;
pub use mail::*;
pub use namespace::*;
pub use notify::*;
pub use scroll::*;
pub use supervisor::*;
pub use wake::*;
pub use workspace::*;
