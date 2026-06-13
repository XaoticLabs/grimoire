#![cfg_attr(not(test), forbid(unsafe_code))]
// Production code: no `.unwrap()`. Tests freely use it.
#![cfg_attr(not(test), warn(clippy::unwrap_used))]
#![warn(missing_docs)]
//! # Grimoire: agent orchestration daemon
//!
//! A daemon-based orchestrator for AI coding agents, modeled as supervised
//! processes: they survive their controlling shell, wake on schedules and
//! file changes, message over a typed mail bus, and self-heal.
//!
//! Pre-1.0; public items may change between minor versions.

/// `grim` command-line client.
pub mod cli;
/// Daemon process: scheduler, supervisor, RPC servers, persistence.
pub mod daemon;
/// `grimw` worker process.
pub mod grimw;
/// Wire types, auth, config, and other cross-binary glue.
pub mod shared;
