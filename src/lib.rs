#![cfg_attr(not(test), forbid(unsafe_code))]
// Production code: no `.unwrap()`. Tests freely use it.
#![cfg_attr(not(test), warn(clippy::unwrap_used))]
#![warn(missing_docs)]
//! # Grimoire — agent orchestration daemon
//!
//! A daemon-based orchestrator for AI coding agents. Agents are modeled as
//! supervised processes, not function calls: they survive the death of any
//! controlling shell, wake on schedules and file changes, message each
//! other over a typed mail bus, and self-heal under a restart policy.
//!
//! ## Crate layout
//!
//! * [`daemon`] — the long-running supervisor process (`grim daemon`).
//!   Owns the SQLite event log, the scheduler, the wake-source registry,
//!   and the gRPC servers exposed over UDS, HTTP, and federated peer
//!   links.
//! * [`cli`] — the `grim` command-line client. Speaks the daemon's
//!   protocol over the UDS socket; everything the dashboard does is
//!   reachable here too.
//! * [`grimw`] — the worker-side process (`grimw`). Connects outbound
//!   to a daemon and executes work the daemon assigns it.
//! * [`shared`] — wire-format types ([`shared::protocol`]), auth tokens
//!   ([`shared::auth`]), config schema ([`shared::config`]), and other
//!   types reused across the three binaries above.
//!
//! ## Stability
//!
//! Pre-1.0. Public items are subject to change between minor versions.
//! The library is published primarily so the binaries can share a
//! protocol vocabulary; embed at your own risk.

/// `grim` command-line client.
pub mod cli;
/// Daemon process: scheduler, supervisor, RPC servers, persistence.
pub mod daemon;
/// `grimw` worker process.
pub mod grimw;
/// Wire types, auth, config, and other cross-binary glue.
pub mod shared;
