//! Concrete `WakeSource` implementations and their config shapes.
//!
//! Each kind is a small struct that knows how to (a) produce a
//! `FireDecision` when its trigger condition is met, and (b) arm any
//! background plumbing (file watcher, event-bus subscriber, etc.) needed
//! to detect that condition. The actual fire path (sending wake mail,
//! bumping counters, applying the rate limit) lives in the registry.

pub mod cron;
pub mod file_watch;
pub mod parent_completion;
