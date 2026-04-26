# Handoff — worker-pool-protocol implementation

> Resume here in a new conversation. Last touched: 2026-04-25.

## What we're doing

Implementing `.claude/specs/worker-pool-protocol-spec.md` — 9 atomic tasks
adding a worker-pool runtime (`grimw` binary + `Executor`/`Placement` seam in
the daemon) so agents can run on any registered worker. CLI surface and
`StreamEvent` shape are unchanged; `LocalExecutor` is the fallback when no
worker is registered.

## Working agreements

- **Branch:** working directly on `main` — no worktree, no branch.
- **Commits:** none in this session by user direction. Everything is
  unstaged. Don't commit unless the user says so.
- **Pre-existing WIP:** the working tree carried ~1200 lines of unrelated
  changes when this session started (durable-event-log work + cleanups in
  `scroll_keeper`, `scroll_parser`, `types.rs`, etc.). Leave alone — not
  ours to land.

## State

### Done

- **All 9 tasks have RED test files** in `tests/`, gated with
  `#![cfg(any())]` at the top so they don't break the build until each
  task's GREEN phase begins. Removing that line activates the file.
- **Task 1 GREEN.** `tests/process_monitor.rs` cfg gate removed; 5 tests
  passing. Refactor landed in `src/daemon/process_manager.rs`:
  - New `LineSource`, `LineEvent`, `CapturedSessionId` types.
  - `consume_lines(agent_id, stream, bus, db, provider)` extracted.
  - `persist_event(...)` and `publish_output(...)` are now `pub` helpers
    callable from the future `RemoteExecutor` (Task 6).
  - `monitor_agent` is a thin adapter: it merges stdout/stderr into a
    single `Stream<LineEvent>` via `tokio_stream::StreamExt::merge` and
    delegates to `consume_lines`. Exit-code derivation stays here.
  - No call site outside `process_manager` changed. Full test suite still
    green (67 unit + the integration tests).

### Remaining tasks (in dependency order)

| Task | What | RED file(s) | Notes |
|------|------|-------------|-------|
| 2 | `Executor` trait + `LocalExecutor`; route `AgentManager` through it | `tests/executor_local.rs` | Medium, in-tree only |
| 3 | `worker.proto` + tonic build wiring | `tests/worker_proto.rs` | Adds tonic/prost/tonic-build deps; build-time cost +30–60s |
| 4 | `grimw` binary crate | `tests/grimw_integration.rs` | New bin entry; biggest single task |
| 5 | `WorkerRegistry` + worker RPC server | `tests/worker_registry.rs`, `tests/worker_rpc_server.rs` | Depends on 3 |
| 6 | `LeastLoadedPlacement` + `RemoteExecutor` | `tests/executor_remote.rs`, `tests/placement.rs` | Depends on 2, 4, 5 |
| 7 | `agents.worker_id` column + reload behavior | `tests/database_worker_id.rs` | Depends on 5 (semantically) |
| 8 | `grim circle` worker col + `grim status` worker count | `tests/cli_circle.rs`, `tests/cli_status.rs` | Depends on 5, 6 |
| 9 | Config additions + README docs | `tests/config_worker.rs` | Depends on 4, 5 |

Critical path: 1 → 2 → 6 → 8;  3 → 4, 5 → 6;  5 → 7;  4, 5 → 9.

### Modified files in this session (all unstaged)

- `src/daemon/process_manager.rs` — Task 1 refactor.
- `tests/process_monitor.rs` — RED tests, cfg gate removed, `seed_agent`
  helper added (FK constraint requires the agent row to exist).
- `tests/executor_local.rs`, `tests/worker_proto.rs`,
  `tests/grimw_integration.rs`, `tests/worker_registry.rs`,
  `tests/worker_rpc_server.rs`, `tests/executor_remote.rs`,
  `tests/placement.rs`, `tests/database_worker_id.rs`,
  `tests/cli_circle.rs`, `tests/cli_status.rs`,
  `tests/config_worker.rs` — RED tests, all gated.

## Caveats baked into the RED tests

The RED tests assume some test-only constructors and library-mode entries
that the GREEN phase needs to provide alongside the production code:

- `LocalExecutor::new(registry, bus, db)`, `LocalExecutor::test_with_command(...)`.
- `AgentManager::new_with_executor(...)` and a
  `seed_agent_for_test_with_session(...)` helper.
- `ProviderRegistry::test_with_true_provider()` (returns a registry that
  spawns `/bin/true` for any task).
- `WorkerRegistry::new_with_clock_for_test(...)`,
  `set_in_flight_for_test`, `advance_clock_for_test`,
  `run_eviction_pass_for_test`.
- `RemoteExecutor::for_test(worker_id, assign_tx, inbound_rx, bus, db)`,
  `RemoteExecutor::stub_for_test(worker_id)`.
- `worker_rpc_server::test_helpers::spawn_test_server(...)`.
- `grimoire::grimw::test_spawn(&config_path)` — exposes the worker as a
  library entry. If `grimw` ships as a pure binary, switch the test to
  invoke it via `Command`.
- Task 7's tests live in `tests/database_worker_id.rs` (not in
  `database.rs` as the spec said) so RED doesn't break the existing
  passing file. Merge in once the column lands.
- Task 8's `tests/cli_status.rs` assumes `StatusResponse: Default`; if not
  today, either derive `Default` or replace `..Default::default()` with an
  explicit constructor.

## How to resume

1. Read `.claude/specs/worker-pool-protocol-spec.md` (the source of truth)
   and this file.
2. Pick the next task (2). Open `tests/executor_local.rs`, remove the
   `#![cfg(any())]` line, run `cargo test --test executor_local` to see it
   fail to compile.
3. Implement the production types (Executor trait, LocalExecutor,
   ExecuteRequest, ExecutorHandle in `src/daemon/executor.rs`; wire
   `AgentManager` to use it). Add the test-only helpers listed above as
   needed.
4. `cargo test --test executor_local` until green, then `cargo test` to
   confirm no regressions.
5. Repeat for Tasks 3–9. Don't commit unless the user asks.

## Things to verify before continuing

- The user has not yet decided whether to land the pre-existing WIP
  (durable-event-log + cleanups). When they do, this branch may need a
  rebase. Until then, the existing test suite is green and Task 1's
  refactor doesn't conflict with the WIP files.
- Confirm the user still wants the full 9-task sequence, vs. stopping
  partway. Each task after 2 adds significant build/runtime infrastructure.
