//! Concrete `WakeSource` implementations and their config shapes. Each decides
//! whether its trigger fired and arms any background plumbing; the actual fire
//! path (wake mail, counters, rate limit) lives in the registry.

pub mod cron;
pub mod file_watch;
pub mod parent_completion;
pub mod remote_agent_completion;
pub mod remote_file_watch;
