// CLI surface; not part of the library's documented API. Also: this is the
// user-facing boundary, so direct stdout/stderr output is its job — the
// global `print_stdout`/`print_stderr` lints stay on so stray prints in
// library/daemon code still get caught.
#![allow(missing_docs)]
#![allow(clippy::print_stdout, clippy::print_stderr)]

pub mod client;
pub mod commands;
pub mod formatters;
pub mod stream_formatter;
