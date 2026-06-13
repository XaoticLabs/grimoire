// CLI surface; user-facing, so direct stdout/stderr output is its job.
#![allow(missing_docs)]
#![allow(clippy::print_stdout, clippy::print_stderr)]

pub mod client;
pub mod commands;
pub mod formatters;
pub mod stream_formatter;
