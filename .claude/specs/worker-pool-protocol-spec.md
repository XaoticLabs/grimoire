# Implementation Spec: Worker Pool Protocol (grimd ↔ grimw)

> Generated from: `.claude/plans/worker-pool-protocol.md`
> Generated on: 2026-04-25

## Overview

Today every agent process spawned by Grimoire runs on the same machine as `grimd`: `AgentManager::summon` calls `Provider::spawn(...)` directly and gets back a local `tokio::process::Child` that `process_manager::monitor_agent` reads line-by-line. This spec introduces an `Executor` abstraction at the `AgentManager` level and a new `grimw` worker binary so agents can run on any registered worker, with the daemon as the control plane.

The CLI surface (`grim summon`, `grim bind`, `grim circle`, `grim scroll`) and the existing `StreamEvent` shape do **not** change. A worker-pool `RemoteExecutor` and a worker-side process supervisor produce the same event stream the local path produces today. With zero workers registered, `LocalExecutor` is the default and behavior is byte-identical to the current daemon.

## Technical Context

### Relevant Codebase Areas

- `src/daemon/agent_manager.rs` (`AgentManager::summon` lines 118-189, `invoke` lines 191-264, `spawn_monitor` lines 72-116) — the single seam where `provider.spawn(...)` is called and a monitor task is launched. This is where the `Executor` indirection lands.
- `src/daemon/process_manager.rs` (entire file, 139 lines) — `monitor_agent` is tightly coupled to `tokio::process::Child` (takes stdout/stderr handles, `wait()`s the child). Its core "produce a sequence of (stream, line) tuples → persist + publish" pipeline must be factored so the same persistence + EventBus logic can also consume an async stream of remote `TaskEvent` messages.
- `src/daemon/provider.rs` and `src/daemon/provider_registry.rs` — the trait + registry pattern the new `Executor` trait + `WorkerRegistry` mirror.
- `src/shared/protocol.rs` — existing CLI JSON-RPC; unchanged. `StreamEvent` shape unchanged.
- `src/shared/types.rs` — `Agent` struct gains an optional `worker_id: Option<String>`.
- `src/shared/config.rs` — `DaemonConfig` gains `worker_listen_addr` and `worker_secret`. New `grimw.toml` schema lives in the new `grimw` crate.
- `src/daemon/persistence.rs` — schema migration to add nullable `worker_id` column on `agents`; new query helper `update_agent_worker_id`.
- `src/daemon/event_bus.rs` — unchanged; `RemoteExecutor` calls the same `publish` API the local path uses.
- `src/daemon/scroll_keeper.rs` — unchanged structurally; it schedules through `AgentManager::summon`, so cross-worker fan-out falls out for free.
- `src/cli/commands/` — `grim circle` formatter annotates each agent with its worker; `grim status` reports worker count. Read-only additions.
- `Cargo.toml` — adds `tonic`, `prost`, `tonic-build`, `rustls`/`tonic` TLS feature, and possibly `semver` to the daemon workspace; new `grimw` binary entry.

### Existing Patterns to Follow

- **Provider trait + registry pair** (`src/daemon/provider.rs`, `provider_registry.rs`): the new `Executor` + `WorkerRegistry` mirror this exactly. `Executor::start(...)` returns a handle whose monitor task funnels into `EventBus`, mirroring `Provider::spawn` returning `SpawnedAgent`.
- **Single mutex-wrapped HashMap of managed entities** (`AgentManager::agents`): `WorkerRegistry` uses the same `Mutex<HashMap<WorkerId, Worker>>` shape so heartbeat eviction and load lookups don't introduce a new concurrency story.
- **EventBus broadcast publish** (`src/daemon/event_bus.rs`): both executors emit the same `StreamEvent` variants. No new variant types except a small annotation field on `AgentCreated`.
- **Persistence migration discipline** (recent commits show schema migrations applied during `Database::open`): the `worker_id` column is added through the same migration path used for prior columns.
- **`reload_from_db` recovery on daemon start** (`AgentManager::reload_from_db`): on daemon restart, agents that were `Active` on a worker are marked `Failed` with reason `daemon_restart` — same coarse-but-safe recovery as today.

### Key Dependencies

- **tonic** (new): gRPC server + client with bidi streaming. Adds 30-60s to a clean build; accepted cost.
- **prost / tonic-build**: code generation from `worker.proto`. Build script lives in `grimw` crate and the daemon crate (or a shared `grimoire-proto` crate — see Ambiguity Resolutions).
- **rustls + tokio-rustls** (via tonic TLS feature): TLS termination on both daemon and worker. Self-signed cert acceptable per plan.
- **semver** (new): provider version constraint parsing/matching (e.g. `claude@>=1.2`).
- Existing: `tokio`, `serde`, `tracing`, `chrono`, `anyhow`, `rusqlite`.

### Ambiguity Resolutions

| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| Crate layout | Where do generated proto types live? | New shared module `src/shared/worker_proto/` with `tonic-build` invoked by a top-level `build.rs`. Daemon and `grimw` both consume from the same module. Avoids a separate sub-crate to keep build complexity low. | Assumed default; revisit if compile time becomes painful. |
| Embedded grimw | Does `grimd` always run an embedded grimw, or is local spawn a separate fallback? | **Keep separate `LocalExecutor` for MVP.** `LocalExecutor` is the fallback when `WorkerRegistry` is empty. Embedded-grimw can replace it later as a pure refactor. | Plan open question; choose lower-MVP-risk option. |
| `session_id` extraction | Worker-side or daemon-side? | **Worker-side.** Worker has the provider impl; sends extracted `session_id` in `TaskFinished`. Daemon does not re-parse. | Plan open question; cleaner contract, no raw-bytes streaming. |
| Heartbeat tuning | Default interval / timeout? | **5s heartbeat, 30s eviction**. Both surfaced in daemon config under `[worker]` so they can be tuned without recompile. | Plan suggested defaults. |
| Drain RPC | Need a `DrainWorker` for graceful shutdown? | **No for MVP.** SIGTERM on the worker stops accepting new assignments and lets in-flight tasks finish; daemon detects via heartbeat absence and marks new assignments unassigned. Add later if pain emerges. | Plan open question; keep MVP small. |
| Worker port binding | Default bind address? | **`127.0.0.1:7878`** for the worker-RPC port. Documented as "set to your Tailscale interface for cross-machine." Never `0.0.0.0` by default. | Plan security note. |
| Capabilities advertisement | When does a worker re-advertise after provider install/uninstall? | **At `Register` only.** Worker re-registration (restart) is the supported path for capability change in MVP. No live capability mutation RPC. | Assumed default; matches "single-tenant, restart is fine" mood. |
| Auth failure mode | What does the daemon do when a worker presents a bad token? | Reject the bidi stream with gRPC status `Unauthenticated`; log at warn; do not record the worker. No partial-trust state. | Plan security model. |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | Refactor `monitor_agent` to split source-of-lines from persistence + publish | None | Medium |
| 2 | Define `Executor` trait + `LocalExecutor`; route `AgentManager` through it | 1 | Medium |
| 3 | Define worker RPC protocol (`worker.proto`) + tonic build wiring | None (parallel with 1, 2) | Low |
| 4 | `grimw` binary crate: bootstrap, RPC client, local provider spawn, event forwarding | 3 | High |
| 5 | `WorkerRegistry` + daemon-side worker RPC server (register/heartbeat/event ingest) | 3 | Medium |
| 6 | `LeastLoadedPlacement` + `RemoteExecutor`; wire into `AgentManager` | 2, 4, 5 | Medium |
| 7 | Persistence: add `agents.worker_id` column + reload behavior | 5 | Low |
| 8 | CLI surfacing: `grim circle` worker annotation + `grim status` worker count | 5, 6 | Low |
| 9 | Config additions (daemon `[worker]` block + `grimw.toml`) and README docs | 4, 5 | Low |

### Critical Path

```
1 ──► 2 ──┐
          ├──► 6 ──► 8
3 ──► 4 ──┤        │
3 ──► 5 ──┘        │
        │          │
        └──► 7 ────┘
                   │
        4, 5 ──► 9
```

- Tasks 1, 3 can start immediately and run in parallel.
- Task 2 unblocks the moment 1 lands.
- Tasks 4 and 5 unblock once 3 lands (the proto is the contract between them).
- Task 6 is the integration step and requires 2, 4, 5 to be complete.
- Tasks 7, 8, 9 are tail visibility / persistence work after the runtime path is correct.

---

### Task 1: Refactor `monitor_agent` to split source-of-lines from persistence + publish

**Summary:** Factor `process_manager::monitor_agent` so its DB-write + EventBus-publish core accepts any async stream of `(stream_name, line)` items, keeping local `tokio::process::Child` as one producer.

**Dependencies:** None

**Files to create/modify:**
- `src/daemon/process_manager.rs` — split `monitor_stream` body into a generic consumer; introduce `MonitorSource` trait or `Stream<Item = LineEvent>` input.
- `tests/process_monitor.rs` (new) — fixture-driven test of the line consumer using an `mpsc::channel` of synthetic lines, asserting DB rows + EventBus events match today's output for a recorded fixture.

**Detailed specification:**

Introduce a new internal type:

```rust
pub enum LineSource {
    Stdout,
    Stderr,
}

pub struct LineEvent {
    pub source: LineSource,
    pub line: String,
}
```

Extract a function `consume_lines(agent_id, lines: impl Stream<Item = LineEvent>, event_bus, db, provider) -> MonitorResult`. Its body is the existing `monitor_stream` loop unified across stdout/stderr (the old `stream_name: &'static str` becomes a per-event field), plus the existing exit handling — except the exit signal arrives as `Stream::None` rather than `child.wait()`.

`monitor_agent` becomes a thin adapter that:
1. Splits `child.stdout` and `child.stderr` into two `LinesStream`s.
2. Merges them (`tokio_stream::StreamExt::merge`) into a single `Stream<Item = LineEvent>`.
3. Awaits `child.wait()` in a `select!` to capture exit code.
4. Calls `consume_lines` with the merged stream.

`MonitorResult.exit_code` and `state` derivation logic moves into `monitor_agent` (local-only knows the exit code shape); for remote, the `RemoteExecutor` will produce a `MonitorResult` from a `TaskFinished` message in Task 6.

Session-id extraction (`provider.extract_session_id(&line)`) stays in `consume_lines` for the local path. **For remote, Task 6 documents that workers extract session_id locally and forward it in `TaskFinished` — `consume_lines` is not used on the daemon side for remote agents; `RemoteExecutor` writes directly to DB + EventBus using the same helpers.** To enable that, also extract two pure helpers `persist_event(db, agent_id, source, line)` and `publish_output(event_bus, agent_id, source, line)` from `consume_lines` so both paths share the exact same write shape.

**Edge cases to handle:**
- Partial-line at EOF: `tokio::io::Lines` yields the trailing partial line as a final `Some(...)`; behavior preserved by reusing `BufReader::lines()`.
- Stdout/stderr interleaving order: today's code spawns two tasks and writes events as they arrive (no global ordering). Preserve by using `merge` (round-robin polling); document in code that ordering is not stable across stdout vs stderr.
- A line whose `extract_session_id` returns `Some(_)` after a previous one already set it: keep current behavior (last-wins).

**Acceptance criteria:**
- [ ] `consume_lines` exists with signature `(AgentId, impl Stream<Item = LineEvent>, EventBus, Arc<Database>, Option<Arc<dyn Provider>>) -> CapturedSessionId`.
- [ ] `monitor_agent` produces the same DB rows and EventBus events as the pre-refactor function for a recorded stdout/stderr fixture (assert via snapshot on `events` table contents and broadcast receiver capture).
- [ ] `persist_event` and `publish_output` are public-in-crate helpers callable from a future `RemoteExecutor`.
- [ ] No call site outside `process_manager` is changed in this task (callers still see `monitor_agent(child, …) -> MonitorResult`).
- [ ] `cargo test` and `cargo clippy --all-targets` pass.

**Contract tests (RED phase):**
- Test file: `tests/process_monitor.rs`
- Tests to write before implementing:
  - `consume_lines_persists_each_line_as_event` — feed a `mpsc` of 5 lines, assert 5 rows in `events` table with correct `event_type` (`stdout`/`stderr`).
  - `consume_lines_publishes_streamevent_output_per_line` — same fixture, assert broadcast receiver yields 5 `StreamEvent::Output` with matching `stream` field.
  - `consume_lines_captures_session_id_from_provider` — feed lines with one matching the provider's extractor, assert returned `CapturedSessionId` is `Some(...)`.
  - `monitor_agent_local_path_matches_pre_refactor_fixture` — drive a fake `Child` (or shell `printf`) producing a known sequence; assert DB rows == golden snapshot.

**Notes/Warnings:**
- This is the riskiest refactor. Land it behind tests *before* introducing `Executor` indirection so regressions are isolated.
- Do not change `MonitorResult` field shape; downstream callers in `agent_manager` rely on `state`, `exit_code`, `session_id`.

---

### Task 2: Define `Executor` trait + `LocalExecutor`; route `AgentManager` through it

**Summary:** Introduce an `Executor` trait at the `AgentManager` level with a single `LocalExecutor` implementation; move `provider.spawn(...)` + monitor-task spawn into the executor.

**Dependencies:** Task 1

**Files to create/modify:**
- `src/daemon/executor.rs` (new) — `Executor` trait, `ExecutorHandle` type, `LocalExecutor` impl.
- `src/daemon/agent_manager.rs` — `summon` and `invoke` route through the active executor instead of calling `provider.spawn` directly.
- `src/daemon/mod.rs` — register the new module.
- `tests/executor_local.rs` (new) — verifies `LocalExecutor` produces identical DB + EventBus output to current `summon`.

**Detailed specification:**

```rust
pub struct ExecuteRequest {
    pub agent_id: AgentId,
    pub task: String,
    pub provider_name: String,
    pub cwd: PathBuf,
    pub model: Option<String>,
    pub resume_session_id: Option<String>, // for invoke path
}

pub struct ExecutorHandle {
    pub worker_id: Option<String>, // None = local
    pub pid: Option<u32>,          // Some for local; may be None for remote
    pub cancel: Box<dyn FnOnce() + Send>,
    pub completion: tokio::task::JoinHandle<MonitorResult>,
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    async fn start(&self, req: ExecuteRequest) -> Result<ExecutorHandle>;
    fn name(&self) -> &str;
}
```

`LocalExecutor` wraps `ProviderRegistry`, `EventBus`, `Database`. Its `start`:
1. Looks up provider; calls `provider.spawn(...)` or `provider.spawn_resume(...)`.
2. Spawns a monitor task that runs `monitor_agent(...)` and returns `MonitorResult` via the `completion` `JoinHandle`.
3. Constructs `cancel` to call `process_manager::kill_process(pid)`.

`AgentManager` gains a single `executor: Arc<dyn Executor>` field, defaulted to `LocalExecutor` constructed in `AgentManager::new`. `summon` and `invoke` build an `ExecuteRequest` and call `self.executor.start(req).await?`. The state-update tail (DB writes, in-memory map updates, `StateChange` event) consumes `ExecutorHandle` and is **shared** between local and remote paths — no `match` on executor type in `agent_manager`.

`banish` consumes `ExecutorHandle.cancel` to terminate.

**Edge cases to handle:**
- Resume with `resume_session_id` set: `LocalExecutor` calls `provider.spawn_resume`. Remote handling deferred to Task 6.
- Cancel called after completion: must be a no-op (close over an `AtomicBool`).
- Provider unknown: returns `Err` from `start`; `AgentManager::summon` propagates and rolls back the DB row to `Failed` (matches today's behavior of returning early).

**Acceptance criteria:**
- [ ] `Executor` trait and `LocalExecutor` exist in `src/daemon/executor.rs`.
- [ ] `AgentManager::summon` calls `executor.start(...)` and contains no direct `provider.spawn(` reference.
- [ ] `AgentManager::invoke` routes through the same executor with `resume_session_id` set.
- [ ] `grim summon "echo hi"` end-to-end test produces the same `agents` row, `events` rows, and `StreamEvent` sequence as before the refactor.
- [ ] `cargo test`, `cargo clippy --all-targets -- -D warnings` pass.

**Contract tests (RED phase):**
- Test file: `tests/executor_local.rs`
- Tests to write before implementing:
  - `local_executor_start_returns_handle_with_pid_and_completion` — start a fake provider that runs `/bin/true`; assert `handle.pid.is_some()` and `handle.completion.await` yields `state == Complete`.
  - `local_executor_cancel_kills_process` — spawn `/bin/sleep 60`, call `cancel()`, assert completion resolves with `state == Failed` within 2s.
  - `agent_manager_summon_uses_executor` — inject a `MockExecutor` that records calls; assert one `start` call with the expected `ExecuteRequest` per `summon`.
  - `agent_manager_invoke_passes_resume_session_id` — same mock; assert `resume_session_id == Some(...)` matches the agent's session.

**Non-testable items:**
- `mod.rs` registration line.

**Notes/Warnings:**
- Do **not** introduce `RemoteExecutor` here. Land local-through-trait first; the trait is the unblocker for the remote work.

---

### Task 3: Define worker RPC protocol (`worker.proto`) + tonic build wiring

**Summary:** Add `worker.proto`, wire `tonic-build`, generate types into a shared module consumable by both daemon and `grimw`.

**Dependencies:** None

**Files to create/modify:**
- `proto/worker.proto` (new) — service + message definitions.
- `build.rs` (new at workspace root) — invokes `tonic_build::compile_protos`.
- `Cargo.toml` — `[build-dependencies] tonic-build = "0.12"`; runtime deps `tonic = { version = "0.12", features = ["tls"] }`, `prost = "0.13"`, `semver = "1"`.
- `src/shared/worker_proto.rs` (new, generated include) — `tonic::include_proto!("grimoire.worker");`.
- `src/shared/mod.rs` — pub mod worker_proto.

**Detailed specification:**

```proto
syntax = "proto3";
package grimoire.worker;

service WorkerControl {
  // Long-lived bidi: worker sends WorkerMessage, daemon sends DaemonMessage.
  rpc Channel(stream WorkerMessage) returns (stream DaemonMessage);
}

message WorkerMessage {
  oneof kind {
    Register register = 1;
    Heartbeat heartbeat = 2;
    TaskAccepted task_accepted = 3;
    TaskRejected task_rejected = 4;
    TaskEvent task_event = 5;
    TaskFinished task_finished = 6;
  }
}

message DaemonMessage {
  oneof kind {
    Ack ack = 1;
    AssignTask assign_task = 2;
    CancelTask cancel_task = 3;
    Ping ping = 4;
  }
}

message Register {
  string worker_id = 1;            // worker-generated UUID; persists across restarts in grimw.toml
  string bearer_token = 2;
  string worker_version = 3;       // semver of the grimw binary
  uint32 max_concurrent = 4;
  repeated ProviderCap providers = 5;
  repeated string tags = 6;
}

message ProviderCap {
  string name = 1;                 // e.g. "claude"
  string version = 2;              // semver, e.g. "1.2.3"
}

message Heartbeat {
  uint32 in_flight = 1;
  // monotonic counter to detect duplicate/stale heartbeats
  uint64 seq = 2;
}

message AssignTask {
  string agent_id = 1;
  string task = 2;
  string provider_constraint = 3;  // semver req, e.g. ">=1.2"
  string provider_name = 4;
  string cwd = 5;
  optional string model = 6;
  map<string, string> env = 7;
  optional string resume_session_id = 8;
}

message CancelTask { string agent_id = 1; }
message Ping {}
message Ack { string ref_id = 1; }

message TaskAccepted { string agent_id = 1; uint32 pid = 2; }
message TaskRejected { string agent_id = 1; string reason = 2; } // cwd_unreachable, provider_missing, version_mismatch

message TaskEvent {
  string agent_id = 1;
  EventKind kind = 2;
  string payload = 3;              // raw line for stdout/stderr
}
enum EventKind { STDOUT = 0; STDERR = 1; }

message TaskFinished {
  string agent_id = 1;
  TaskState state = 2;
  optional int32 exit_code = 3;
  optional string session_id = 4;
  optional string error_reason = 5;
}
enum TaskState { COMPLETE = 0; FAILED = 1; BANISHED = 2; }
```

`build.rs` runs `tonic_build::configure().build_server(true).build_client(true).compile_protos(&["proto/worker.proto"], &["proto"])?;`.

**Edge cases to handle:**
- Schema evolution: every message uses proto3 with `optional` markers on nullable fields so older workers don't crash on missing fields. `Register.worker_version` allows daemon to refuse incompatible versions.
- `bearer_token` lives only on `Register` (not on every subsequent message); the server validates once and pins the stream.

**Acceptance criteria:**
- [ ] `proto/worker.proto` exists and compiles via `cargo build` (driven by `build.rs`).
- [ ] `crate::shared::worker_proto` exposes the generated `worker_control_client::WorkerControlClient` and `worker_control_server::WorkerControlServer`.
- [ ] `Register`, `Heartbeat`, `AssignTask`, `CancelTask`, `TaskAccepted`, `TaskRejected`, `TaskEvent`, `TaskFinished` messages exist with the fields enumerated above (verified by referencing them in a doc-test).
- [ ] No runtime call sites yet — wire-types only.

**Contract tests (RED phase):**
- Test file: `tests/worker_proto.rs`
- Tests to write before implementing:
  - `worker_proto_register_message_roundtrip` — construct `Register`, serialize via `prost::Message::encode`, decode, assert equal.
  - `worker_proto_assign_task_optional_fields_default_to_none` — decode an `AssignTask` encoded without `model` / `resume_session_id`, assert `None`.
  - `worker_proto_compiles` — type-only test that references each generated client/server stub.

**Non-testable items:**
- `build.rs` configuration (verified by build success).
- `Cargo.toml` dep additions.

**Notes/Warnings:**
- Pin `tonic`/`prost` versions; the proto code-gen surface changes between minor versions.
- Keep `grimoire.worker` package name — future `grimoire.cli`-like packages can live alongside.

---

### Task 4: `grimw` binary crate

**Summary:** New `grimw` binary that registers with the daemon, heartbeats, accepts `AssignTask` messages, spawns the provider locally, and forwards events back over the bidi stream.

**Dependencies:** Task 3

**Files to create/modify:**
- `src/grimw/main.rs` (new bin entry) — `[[bin]] name = "grimw" path = "src/grimw/main.rs"` in `Cargo.toml`.
- `src/grimw/config.rs` (new) — `GrimwConfig { daemon_url, secret, worker_id, max_concurrent, tags, providers }`; loaded from `~/.grimoire/grimw.toml` (path overridable via `--config`).
- `src/grimw/rpc_client.rs` (new) — bidi stream lifecycle: connect, register, heartbeat loop, dispatch incoming.
- `src/grimw/task_runner.rs` (new) — accepts an `AssignTask`, calls into a thin local provider spawner (a trimmed copy or re-export of `provider_registry`), runs `consume_lines`-equivalent that forwards each line as `TaskEvent` and sends `TaskFinished` on exit.
- `tests/grimw_integration.rs` (new) — spins a fake daemon gRPC server, runs `grimw` against it, asserts the message sequence.

**Detailed specification:**

Lifecycle:
1. Load `GrimwConfig`. If `worker_id` absent, generate a UUID, write back to file.
2. Probe locally available providers (re-use `ProviderRegistry::from_config` from `grimoire`) and capture each as `ProviderCap { name, version }`. Provider version comes from running the configured binary with a known version flag; on parse failure, version is `0.0.0` with a warning log (worker still registers).
3. Open TLS gRPC channel to `daemon_url`. Pin the daemon's cert fingerprint from `daemon_cert_sha256` in config.
4. Open the bidi `Channel` stream. Send `Register` first.
5. Spawn a heartbeat task: every 5s send `Heartbeat { in_flight, seq }`.
6. Read loop: dispatch `AssignTask` to a worker pool that respects `max_concurrent`. Reject with `TaskRejected { reason }` if at capacity, cwd doesn't exist, no provider matches the constraint, or provider version fails the semver req.
7. For each accepted task, spawn a per-agent task that:
   - Calls `provider.spawn(...)` (or `spawn_resume` if `resume_session_id` is `Some`).
   - Sends `TaskAccepted { agent_id, pid }`.
   - Streams stdout/stderr lines as `TaskEvent` messages (one per line; `EventKind::STDOUT` or `STDERR`), running `provider.extract_session_id(&line)` locally to capture the session id.
   - On exit, sends `TaskFinished { state, exit_code, session_id }`.
8. On `CancelTask`, calls `process_manager::kill_process(pid)`; the per-agent task naturally completes and sends `TaskFinished` with `state = BANISHED`.
9. On SIGTERM: stop accepting new `AssignTask` (respond `TaskRejected { reason: "draining" }`), wait for in-flight to finish, send a final `Heartbeat`, close stream.

**Edge cases to handle:**
- Daemon unreachable on startup: retry connect with capped exponential backoff (1s, 2s, 4s, max 30s); log at warn each attempt.
- Stream broken mid-task: in-flight tasks continue running; reconnect, re-register with the **same** `worker_id`, send a `TaskFinished` for any task that completed during the disconnect (buffered up to N=64 events per task; overflow → drop oldest with warn).
- `cwd` path missing: respond `TaskRejected { reason: "cwd_unreachable" }` immediately.
- Provider absent or version mismatch: `TaskRejected { reason: "provider_missing" | "version_mismatch" }`.
- Concurrent assignment exceeding `max_concurrent`: `TaskRejected { reason: "at_capacity" }`.

**Acceptance criteria:**
- [ ] `cargo build --bin grimw` produces a binary.
- [ ] `grimw --config <path>` loads `GrimwConfig`; missing file yields a clear error referencing the expected path.
- [ ] Against a fake daemon, `grimw` sends `Register` within 2s of startup and `Heartbeat` every 5±1s.
- [ ] An `AssignTask` for `/bin/echo hello` produces: one `TaskAccepted`, ≥1 `TaskEvent { kind: STDOUT, payload: "hello" }`, one `TaskFinished { state: COMPLETE, exit_code: Some(0) }`.
- [ ] `cwd_unreachable` `TaskRejected` is sent within 200ms when an `AssignTask` references a missing directory.
- [ ] On SIGTERM with one in-flight task, `grimw` exits only after that task's `TaskFinished` has been sent.

**Contract tests (RED phase):**
- Test file: `tests/grimw_integration.rs`
- Tests to write before implementing:
  - `grimw_registers_then_heartbeats` — fake server captures the first 3 messages; assert types `[Register, Heartbeat, Heartbeat]`.
  - `grimw_executes_assign_task_and_streams_events` — fake server sends `AssignTask { task: "/bin/echo grim" }`; assert `[TaskAccepted, TaskEvent(stdout="grim"), TaskFinished{COMPLETE, exit=0}]`.
  - `grimw_rejects_when_cwd_missing` — `AssignTask { cwd: "/does/not/exist" }`; assert `TaskRejected { reason: "cwd_unreachable" }`.
  - `grimw_rejects_when_provider_version_mismatched` — register with `claude@1.0.0`, assign `provider_constraint: ">=2.0"`; assert `TaskRejected { reason: "version_mismatch" }`.
  - `grimw_drains_on_sigterm` — send SIGTERM with a long-running task; assert `TaskFinished` arrives before process exit.

**Notes/Warnings:**
- Do **not** import `daemon::*` modules — `grimw` is a peer crate-level concern, not a daemon submodule. Provider trait and registry must move to `src/shared/` or be re-exported via a `lib.rs` boundary so both binaries can use them. (Recommend: create `src/shared/provider/` containing trait + registry + concrete provider impls; daemon and `grimw` both consume from there.)
- Buffer for events during disconnect must be bounded; document the "drop oldest" policy in code.

---

### Task 5: `WorkerRegistry` + daemon-side worker RPC server

**Summary:** Daemon-side gRPC server that accepts worker bidi streams, maintains a `WorkerRegistry`, and routes incoming `TaskEvent`/`TaskFinished` messages.

**Dependencies:** Task 3

**Files to create/modify:**
- `src/daemon/worker_registry.rs` (new) — `WorkerRegistry { workers: Mutex<HashMap<WorkerId, Worker>> }` with API: `register`, `evict`, `record_heartbeat`, `pick_least_loaded(constraint, provider_name)`, `assign_tx(worker_id) -> Sender<DaemonMessage>`.
- `src/daemon/worker_rpc_server.rs` (new) — implements `WorkerControl::channel`. Validates bearer token, registers, fans `DaemonMessage`s out, receives `WorkerMessage`s and routes them.
- `src/daemon/server.rs` — start the worker gRPC server on `worker_listen_addr` alongside the existing CLI server when configured.
- `src/daemon/mod.rs` — module registration.
- `tests/worker_registry.rs` (new).
- `tests/worker_rpc_server.rs` (new).

**Detailed specification:**

```rust
pub struct Worker {
    pub worker_id: String,
    pub address: SocketAddr,
    pub providers: Vec<(String, semver::Version)>,
    pub tags: Vec<String>,
    pub max_concurrent: u32,
    pub in_flight: u32,
    pub last_heartbeat: Instant, // monotonic
    pub registered_at: DateTime<Utc>,
    pub assign_tx: mpsc::Sender<DaemonMessage>,
}
```

`WorkerRegistry::pick_least_loaded(provider_name, constraint: semver::VersionReq) -> Option<WorkerId>`:
1. Filter workers whose providers contain `(name, ver)` where `ver` matches `constraint`.
2. Filter workers with `in_flight < max_concurrent`.
3. Sort by `(in_flight asc, worker_id asc)`; return first.

Eviction task: every 5s, scan `last_heartbeat`; if `now - last_heartbeat > 30s`, evict. On eviction, find all in-flight agents on that worker (by querying `AgentManager` via a callback or a routing table held in the registry) and emit a synthetic `TaskFinished { state: FAILED, error_reason: "worker_lost" }` for each, then drop the entry.

The RPC server's `channel` impl:
1. Reads first message; rejects with `Unauthenticated` if not a `Register` with the configured `worker_secret`.
2. Adds to `WorkerRegistry`.
3. Spawns a forwarder task that pulls `DaemonMessage`s from the worker's `assign_tx` channel into the response stream.
4. In the request loop: dispatches `Heartbeat` (update `last_heartbeat`, `in_flight`), `TaskAccepted/Rejected/Event/Finished` (route to the agent's `RemoteExecutorHandle`, defined in Task 6).
5. On stream close (worker disconnect): mark worker for eviction immediately (don't wait the 30s).

**Edge cases to handle:**
- Two workers register with the same `worker_id`: reject the second with `AlreadyExists`. (No "rolling restart" support in MVP — restart finishes the old stream before starting the new one.)
- Worker version below `MIN_WORKER_VERSION` constant: reject with `FailedPrecondition` and a clear message. Constant defined in `src/shared/constants.rs`.
- Heartbeat seq goes backwards: log warn, ignore (don't update `last_heartbeat`).
- Worker process dies between `TaskAccepted` and `TaskFinished`: stream closes → eviction → in-flight agent transitions to Failed with `worker_lost`.

**Acceptance criteria:**
- [ ] `WorkerRegistry::register` adds a worker and is observable via a count getter.
- [ ] `WorkerRegistry::pick_least_loaded` returns the worker with fewest `in_flight` matching the provider constraint; ties broken by `worker_id` sort.
- [ ] `WorkerRegistry::pick_least_loaded` returns `None` when no worker matches the provider constraint.
- [ ] Eviction task removes workers whose `last_heartbeat` is older than 30s.
- [ ] gRPC server rejects a `Register` with wrong bearer token (`Unauthenticated`).
- [ ] gRPC server rejects a `Register` with `worker_version < MIN_WORKER_VERSION` (`FailedPrecondition`).
- [ ] Worker disconnect triggers immediate eviction (verified by registry count drop within 1s).

**Contract tests (RED phase):**
- Test file: `tests/worker_registry.rs`
  - `pick_least_loaded_picks_lowest_in_flight` — register A(in_flight=2), B(in_flight=0), C(in_flight=1); assert pick == B.
  - `pick_least_loaded_filters_by_constraint` — register A with `claude@1.0.0`, B with `claude@2.1.0`; constraint `>=2`; assert pick == B.
  - `pick_least_loaded_returns_none_when_no_match` — only A `claude@1.0.0`, constraint `>=3`; assert `None`.
  - `pick_least_loaded_breaks_ties_by_worker_id` — two workers with `in_flight=0`, ids `aaa`, `bbb`; assert pick == `aaa`.
  - `eviction_removes_stale_worker` — fake clock; advance 31s; assert registry length 0.
- Test file: `tests/worker_rpc_server.rs`
  - `register_with_bad_token_returns_unauthenticated` — gRPC client sends `Register { bearer_token: "wrong" }`; assert `Status::unauthenticated`.
  - `register_with_old_version_returns_failed_precondition` — `worker_version: "0.0.1"`; assert `Status::failed_precondition`.
  - `worker_disconnect_evicts_immediately` — connect, register, drop client; assert registry empty within 1s.

**Non-testable items:**
- `server.rs` wiring of the gRPC server alongside CLI server.

**Notes/Warnings:**
- The eviction task and the RPC server share `Arc<WorkerRegistry>`. Use `tokio::sync::Mutex` consistently with the existing daemon style.
- `MIN_WORKER_VERSION` is the gate for proto-schema breaks; bump it deliberately.

---

### Task 6: `LeastLoadedPlacement` + `RemoteExecutor`; wire into `AgentManager`

**Summary:** Implement `RemoteExecutor` that issues `AssignTask` over a chosen worker's channel and translates incoming `TaskEvent`/`TaskFinished` into `EventBus` writes; add a `Placement` policy that picks `RemoteExecutor` when a worker matches and falls back to `LocalExecutor` otherwise.

**Dependencies:** Tasks 2, 4, 5

**Files to create/modify:**
- `src/daemon/executor.rs` — add `RemoteExecutor`, `Placement` trait, `LeastLoadedPlacement`.
- `src/daemon/agent_manager.rs` — replace single `Arc<dyn Executor>` field with `placement: Arc<dyn Placement>` whose `pick(req) -> Arc<dyn Executor>` is called per-summon.
- `src/daemon/worker_rpc_server.rs` — register a `RemoteExecutorHandle` per accepted task so incoming `TaskEvent`s land on the right channel.
- `tests/executor_remote.rs` (new) — drive a `RemoteExecutor` against a fake worker channel.
- `tests/placement.rs` (new).

**Detailed specification:**

```rust
pub trait Placement: Send + Sync {
    fn pick(&self, req: &ExecuteRequest) -> Arc<dyn Executor>;
}

pub struct LeastLoadedPlacement {
    registry: Arc<WorkerRegistry>,
    local: Arc<LocalExecutor>,
    remote_factory: Arc<dyn Fn(WorkerId) -> Arc<dyn Executor> + Send + Sync>,
}
```

`pick`:
1. Build a `VersionReq` from the request's provider name + the configured per-provider constraint (default `*`).
2. Call `registry.pick_least_loaded(provider_name, constraint)`.
3. If `Some(worker_id)`, return `remote_factory(worker_id)`.
4. Otherwise return `local`.

`RemoteExecutor::start(req)`:
1. Reserve an entry in a routing map: `agent_id → mpsc::Sender<TaskEvent | TaskFinished>`.
2. Bump `in_flight` on the chosen worker.
3. Send `AssignTask` on the worker's `assign_tx`.
4. Spawn a "completion task" that consumes the routing-map receiver, calls `process_manager::persist_event` and `publish_output` for each `TaskEvent`, and on `TaskFinished` returns a `MonitorResult { state, exit_code, session_id }`.
5. Return `ExecutorHandle { worker_id: Some(...), pid: None initially (filled by `TaskAccepted`), cancel, completion }`.

`cancel` sends `CancelTask { agent_id }` on the worker's `assign_tx`.

`worker_rpc_server` upon receiving `TaskAccepted`/`TaskRejected`/`TaskEvent`/`TaskFinished` for an `agent_id` looks up the routing map and forwards. `TaskRejected` produces an immediate `TaskFinished { state: FAILED, error_reason: <rejection reason> }` synthesized internally — same downstream behavior.

`AgentManager::summon` keeps its existing tail logic; the only change is that `executor` is now `placement.pick(&req)`. The persisted `agent.worker_id` is set from `ExecutorHandle.worker_id` after `start`.

**Edge cases to handle:**
- Worker evicted while task in flight: routing map's receiver gets a synthetic `TaskFinished { state: FAILED, error_reason: "worker_lost" }`; completion resolves accordingly.
- `pick_least_loaded` returns `None` and no local fallback wanted (config-flagged future feature, not in MVP): MVP always falls back to local. Document.
- Two summons race for the same single-capacity worker: first wins, second sees `in_flight == max_concurrent`, falls back to next least-loaded or local.
- `resume_session_id` on a remote agent whose original worker is gone: `pick` falls back to local execution; `LocalExecutor` will fail (no session); agent transitions to Failed with reason `worker_lost` in `summon`'s tail. Document this behavior.

**Acceptance criteria:**
- [ ] `LeastLoadedPlacement::pick` returns a `RemoteExecutor` when a matching worker exists and a `LocalExecutor` when none does.
- [ ] `RemoteExecutor::start` sends an `AssignTask` to the chosen worker's channel within 100ms.
- [ ] Incoming `TaskEvent { kind: STDOUT, payload }` for an active remote agent results in one `events` row and one `StreamEvent::Output` published.
- [ ] Incoming `TaskFinished { state: COMPLETE, exit_code: 0 }` resolves the executor's `completion` JoinHandle with matching `MonitorResult`.
- [ ] Worker eviction during a remote task results in a synthesized `TaskFinished { state: FAILED, error_reason: "worker_lost" }` reaching the completion handle.
- [ ] `agents.worker_id` is persisted as `Some(worker_id)` for remote, `None` for local.
- [ ] End-to-end: with one fake `grimw` registered, `grim summon "echo hi"` produces the same CLI-visible event sequence as the all-local path.

**Contract tests (RED phase):**
- Test file: `tests/executor_remote.rs`
  - `remote_executor_sends_assign_task_to_worker_channel` — fake worker channel; assert `AssignTask` received with matching fields.
  - `remote_executor_streams_task_events_into_event_bus` — feed 3 fake `TaskEvent`s into the routing map; assert 3 `events` rows + 3 `StreamEvent::Output`.
  - `remote_executor_completion_resolves_on_task_finished` — feed `TaskFinished{COMPLETE, exit=0, session_id=Some("s1")}`; assert `MonitorResult{state: Complete, exit_code: Some(0), session_id: Some("s1")}`.
  - `remote_executor_cancel_sends_cancel_task` — call `cancel`; fake worker channel observes a `CancelTask { agent_id }`.
  - `remote_executor_worker_lost_resolves_failed` — drop the routing map's tx (simulating eviction); completion resolves with `state == Failed`, `error_reason == "worker_lost"`.
- Test file: `tests/placement.rs`
  - `placement_picks_remote_when_worker_available` — registry has one worker with `claude@1.0.0`; pick returns remote.
  - `placement_falls_back_to_local_when_no_worker` — empty registry; pick returns local.

**Notes/Warnings:**
- `RemoteExecutor` shares the `persist_event` and `publish_output` helpers with `LocalExecutor` (extracted in Task 1). **Do not duplicate the DB-write or EventBus shape.**
- The routing map lives in the worker RPC server, not in `RemoteExecutor`, because the server is the only thing that sees incoming `TaskEvent` messages. `RemoteExecutor::start` calls a server-provided `register_route(agent_id) -> Receiver<...>` to wire the two sides.

---

### Task 7: Persistence: add `agents.worker_id` column + reload behavior

**Summary:** Add nullable `worker_id` column to the `agents` table, surface it through `Agent`, and update `reload_from_db` to mark in-flight remote agents as Failed on daemon restart.

**Dependencies:** Task 5 (so `worker_id` is meaningful) — but the schema migration itself can land earlier as an empty column.

**Files to create/modify:**
- `src/daemon/persistence.rs` — add migration step `ALTER TABLE agents ADD COLUMN worker_id TEXT`; update `Agent` row mapping; new helper `update_agent_worker_id`.
- `src/shared/types.rs` — `Agent { ..., pub worker_id: Option<String> }`.
- `src/daemon/agent_manager.rs` — `summon` calls `update_agent_worker_id` after `executor.start` returns.
- `tests/database.rs` — extend.

**Detailed specification:**

Migration registered in the existing migration path (look at how prior migrations were applied around the `agents`/`events` tables). On open, run `PRAGMA user_version`, branch, apply ALTER, bump version. Default for existing rows is `NULL` — meaning "ran on the daemon's local executor."

`reload_from_db`: same logic as today (Active/Summoning → Failed on daemon restart). The only addition is that the failure reason recorded in the events table (if we record one — today the code does not) should be `daemon_restart` for clarity. **If we don't record a reason today, do not add one in this task** — keep this scoped to the column.

**Edge cases to handle:**
- Database created before this migration: ALTER applies cleanly (SQLite supports adding nullable columns).
- Repeated migration on already-migrated DB: detected via `user_version`; no-op.

**Acceptance criteria:**
- [ ] `agents` table has a `worker_id` TEXT column nullable.
- [ ] `Agent::worker_id` field exists; serializes to JSON as `worker_id`.
- [ ] `update_agent_worker_id(agent_id, worker_id)` updates the row.
- [ ] Loading an existing pre-migration DB succeeds; existing rows have `worker_id == None`.
- [ ] `reload_from_db` behavior is unchanged for `worker_id == None` agents.

**Contract tests (RED phase):**
- Test file: `tests/database.rs` (extend)
  - `db_migration_adds_worker_id_column` — open against a fixture DB lacking the column; assert column exists after open.
  - `db_update_agent_worker_id_persists` — insert agent, call `update_agent_worker_id(id, "w1")`, reload, assert `Some("w1")`.
  - `db_agent_with_null_worker_id_loads_as_none` — insert with `NULL`; assert `worker_id == None` on read.

**Notes/Warnings:**
- The schema migration is the only "irreversible" change in this spec — once persisted, downgrade requires manual `ALTER TABLE DROP COLUMN`. Acceptable; document in the rollout section.

---

### Task 8: CLI surfacing: `grim circle` worker annotation + `grim status` worker count

**Summary:** Show the assigned worker on each agent in `grim circle`; show worker count and per-worker health in `grim status`.

**Dependencies:** Tasks 5, 6

**Files to create/modify:**
- `src/cli/formatters.rs` — `circle` formatter renders `worker_id` (or `local`) column; truncate to 6 chars.
- `src/cli/commands/` — `status.rs` reports worker count, names, in-flight per worker, last-heartbeat-age.
- `src/shared/protocol.rs` — `StatusResponse` gains `workers: Vec<WorkerStatus>`; `AgentSummary` gains `worker_id: Option<String>`.
- `src/daemon/rpc.rs` — populate the new fields.
- `src/daemon/agent_manager.rs::circle` — read `worker_id` from `Agent` and include it in `AgentSummary`.

**Detailed specification:**

`WorkerStatus { worker_id, in_flight, max_concurrent, last_heartbeat_age_secs, providers: Vec<String> }`.

`grim circle` output gains a `WORKER` column showing the first 6 chars of `worker_id`, or `local`. Width-bounded; existing column layout preserved.

`grim status` adds a section:
```
Workers (2)
  w1abc234  in_flight=1/4   ↻ 3s   [claude@1.2.3, codex@1.0.0]
  w2def567  in_flight=0/8   ↻ 1s   [claude@1.2.3]
```

If zero workers registered: print `Workers (0) — running local-only.`

**Edge cases to handle:**
- A worker_id that's truncated in `circle` collides with another's prefix: full id remains in JSON for scripting; the prefix is human-only.
- Heartbeat age > 30s but not yet evicted (race): show with a `!` marker.

**Acceptance criteria:**
- [ ] `grim circle` JSON output includes `worker_id` field per agent.
- [ ] `grim circle` text output shows a `WORKER` column.
- [ ] `grim status` JSON output includes `workers: [WorkerStatus, ...]`.
- [ ] With zero workers registered, `grim status` text output contains `Workers (0)`.
- [ ] With one worker registered with claude@1.2.3, `grim status` text output lists it.

**Contract tests (RED phase):**
- Test file: `tests/cli_circle.rs` (new)
  - `circle_json_includes_worker_id` — summon agent against fake remote worker; `grim circle --json`; assert `worker_id` field present and matches.
  - `circle_text_shows_worker_column` — same, text output; assert column header `WORKER` and a non-empty value row.
- Test file: `tests/cli_status.rs` (new)
  - `status_json_lists_workers` — register one fake worker; `grim status --json`; assert `workers` length 1 with matching `worker_id`.
  - `status_text_zero_workers_message` — no workers; text output contains `Workers (0)`.

**Non-testable items:**
- Exact column-width formatting — verified manually.

**Notes/Warnings:**
- Keep the existing JSON field set in `AgentSummary` — only **add** `worker_id`. No removals.

---

### Task 9: Config + docs

**Summary:** Add `[worker]` block to daemon config, the `grimw.toml` schema, and a README section explaining the worker-pool model.

**Dependencies:** Tasks 4, 5

**Files to create/modify:**
- `src/shared/config.rs` — add `WorkerConfig { listen_addr: SocketAddr, secret: String, heartbeat_timeout_secs: u64 = 30, heartbeat_interval_hint_secs: u64 = 5, tls_cert_path: PathBuf, tls_key_path: PathBuf }` field on `DaemonConfig`. Optional — if absent, daemon does not start the worker RPC server.
- `src/grimw/config.rs` — `GrimwConfig { daemon_url, secret, daemon_cert_sha256, worker_id, max_concurrent, tags, providers: HashMap<String, ProviderConfig> }`.
- `README.md` — new "Worker Pool" section after "Commands": one-machine vs multi-machine quickstart, `grim daemon` vs `grimw` startup, security note about Tailscale-only by default.

**Detailed specification:**

Daemon `[worker]` is fully optional. If present, `worker.listen_addr` defaults to `127.0.0.1:7878` if not specified. `worker.secret` is required when `[worker]` is present; absence is an error at daemon startup.

`grimw.toml` minimum example:
```toml
daemon_url = "https://daemon.tailnet.ts.net:7878"
secret = "shared-bearer-token-here"
daemon_cert_sha256 = "abcd1234..."
max_concurrent = 4
tags = ["beefy"]

[providers.claude]
binary = "/usr/local/bin/claude"
```

**Edge cases to handle:**
- `[worker]` block present but `secret` absent: clear error at startup, exit nonzero.
- Worker binds 0.0.0.0 (user override): log a warn, continue.

**Acceptance criteria:**
- [ ] `Config` parses a TOML with `[worker]` block populated.
- [ ] `Config` parses a TOML without `[worker]` (legacy single-machine setup) — `worker` field is `None`.
- [ ] `Config` rejects `[worker]` without `secret` with a clear error message naming the field.
- [ ] `GrimwConfig` parses the example above.
- [ ] README contains a `## Worker Pool` section.

**Contract tests (RED phase):**
- Test file: `tests/config.rs` (extend or new)
  - `daemon_config_parses_with_worker_block` — TOML fixture with `[worker]`; asserts fields.
  - `daemon_config_parses_without_worker_block` — TOML without; assert `worker.is_none()`.
  - `daemon_config_rejects_worker_without_secret` — TOML with `[worker] listen_addr=...`; assert parse error mentions `secret`.
  - `grimw_config_parses_minimum_example` — fixture; assert all required fields present.

**Non-testable items:**
- README prose.
- Tailscale recommendation (documentation-only).

**Notes/Warnings:**
- Default `listen_addr` is `127.0.0.1` — never `0.0.0.0`. This is enforced in the constructor's defaulting logic, not just by documentation.

---

## Testing Strategy (TDD)

Implementation follows red-green TDD: for each task, the implementer writes failing contract tests from the acceptance criteria (RED), then implements the minimum code to make them pass (GREEN). Contract tests are immutable once committed.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `tests/process_monitor.rs` | 4 tests | none |
| 2 | `tests/executor_local.rs` | 4 tests | mod registration |
| 3 | `tests/worker_proto.rs` | 3 tests | `build.rs`, Cargo deps |
| 4 | `tests/grimw_integration.rs` | 5 tests | bin entry config |
| 5 | `tests/worker_registry.rs`, `tests/worker_rpc_server.rs` | 5 + 3 tests | `server.rs` wiring |
| 6 | `tests/executor_remote.rs`, `tests/placement.rs` | 5 + 2 tests | none |
| 7 | `tests/database.rs` (extend) | 3 tests | none |
| 8 | `tests/cli_circle.rs`, `tests/cli_status.rs` | 2 + 2 tests | column formatting |
| 9 | `tests/config.rs` | 4 tests | README, Tailscale doc |

### Integration Testing

A tail integration test, `tests/end_to_end_remote.rs`, brings up:
1. An ephemeral daemon (in-process).
2. A fake `grimw` (in-process gRPC client wrapping a stub provider that runs `/bin/echo`).
3. Issues a `summon` via the CLI's JSON-RPC client.
4. Asserts the produced `events`, `StreamEvent`s, and final `agents` row are byte-identical to those produced by the same task on the local path (golden snapshot).

This is the proof-of-correct-seam test referenced in the plan's "Riskiest Part" section.

### Manual Testing Checklist

- [ ] Start `grimd` on machine A with `[worker]` configured. Start `grimw` on machine B pointing at A. `grim status` on A shows one worker.
- [ ] `grim summon "echo hello"` on A — observe the agent runs on B (`grim circle` shows B's worker_id).
- [ ] `grim bind <id>` streams output identically to a local-only summon.
- [ ] `grim banish <id>` mid-run kills the process on B within 1s.
- [ ] Kill `grimw` on B mid-task; agent transitions to Failed within 30s on A.
- [ ] Restart `grimw` on B; new summons land on B again.
- [ ] Stop daemon mid-task; restart; agent shows Failed (matches today's behavior).
- [ ] With zero workers, `grim summon` works exactly as today (LocalExecutor fallback).

## Rollout Considerations

### Feature Flags

No runtime feature flag. Behavior is fully gated by config: absent `[worker]` block → no worker RPC server, only `LocalExecutor` ever picked. Existing users see zero behavior change.

### Migration Strategy

- **DB migration:** one ALTER TABLE adding nullable `worker_id`. Forward-compatible: pre-migration daemons cannot read post-migration DB (the `Agent` deserialization would fail on the new column if read by an older binary). Acceptable for a single-user MVP; documented.
- **Proto schema:** `MIN_WORKER_VERSION` constant gates worker compatibility. Bump deliberately on breaking proto changes.

### Rollback Plan

- Revert PR. `agents.worker_id` column remains in the DB but is harmless (older code ignores unknown columns? — verify; if not, ship a follow-up migration that drops the column or rebuild the DB from scratch given it's a personal tool).
- For a partial revert (keep Task 1's refactor, drop the rest): all subsequent tasks are additive — removing them returns the daemon to local-only behavior.

## Open Items

- [ ] Confirm whether the existing `rusqlite` row reader fails on unknown columns or tolerates them. If it fails, the rollback strategy needs a "drop column" migration step recorded.
- [ ] Decide TLS cert distribution: self-signed cert generated by `grimd` on first start and printed to stdout for the user to paste into `grimw.toml`, or pre-generated externally? (Leaning: generate-and-print, friction-minimizing.)
- [ ] Confirm that moving `Provider` trait + `ProviderRegistry` to `src/shared/` doesn't break any daemon-internal-only assumption (e.g., access to `Database` from inside a provider, which would create a circular module ownership).

---

*This spec is implementation-ready. Each task is designed for red-green TDD. Tasks can be picked up independently (respecting dependencies) and completed in a single iteration.*
