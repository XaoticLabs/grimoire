# Implementation Spec: Durable Work Queue with Admission Control

> Generated from: `.claude/plans/durable-work-queue.md`
> Generated on: 2026-04-25

## Overview

Today every path that wants to start an agent — `grim summon`, `scroll_keeper::schedule_tasks`, and pact firing in the orchestrator — calls `agent_manager.summon()`, which synchronously calls `executor.start()`. There is no daemon-wide concurrency ceiling, no scheduler, and no durable record of work that has been *requested* but not yet *started*. Once `grimw` workers exist, this means the daemon can either fail-fast on capability mismatch or oversubscribe one worker; it has no way to wait.

This spec introduces a single daemon-owned scheduler that becomes the only path from "we want this agent to run" to `executor.start()`. A new `AgentState::Queued` sits in front of `Summoning`. Submissions are recorded in a new `task_queue` SQLite table that survives daemon restart. A reactor wired to the event bus (with a periodic tick as a safety net) promotes queued tasks when a global capacity slot is free *and* an eligible worker exists. A new `grim queue` command surfaces pending work and block reasons. The wire contract for `summon` becomes "enqueue and acknowledge" rather than "start synchronously."

## Technical Context

### Relevant Codebase Areas

- `src/shared/types.rs` (lines 39–61) — `AgentState` enum and `is_terminal()`. New `Queued` variant added here is the foundation; `impl_state_enum!` macro auto-derives serialization.
- `src/shared/protocol.rs` (lines 156–193) — `StreamEvent` enum. New `AgentQueued` variant added here.
- `src/daemon/agent_manager.rs` (lines 160–244) — `summon()` is the load-bearing function that this spec splits into `enqueue()` and `dispatch_internal()`. Also `banish()` (lines 339–368), `invoke()` (lines 246–337), and `reload_from_db()` (lines 79–99) need state-aware guards.
- `src/daemon/scroll_keeper.rs` (lines 377–459) — `schedule_tasks()` currently calls `manager.summon()` directly; will be rerouted to `manager.enqueue()`. Existing reactor pattern (lines 48–75) is the model for the new scheduler.
- `src/daemon/orchestrator.rs` (lines 47–104, esp. line 78) — pact firing calls `manager.summon()`; will be rerouted to `manager.enqueue()`.
- `src/daemon/persistence.rs` — schema lives at lines 51–166 with a `CREATE TABLE IF NOT EXISTS` pattern. New `task_queue` table goes here alongside helpers.
- `src/daemon/event_bus.rs` — broadcast-plus-MPSC pattern for publish-and-persist. No changes other than carrying the new event variant.
- `src/daemon/worker_registry.rs` (lines 132–153) — `pick_least_loaded()` exists; new non-mutating `has_eligible_worker()` peek added alongside.
- `src/daemon/executor.rs` — unchanged; the scheduler becomes the new caller of `executor.start()`.
- `src/daemon/rpc.rs` (lines 39–60, 224–240) — `handle_summon()` returns `Queued` as the post-call state; `handle_status()` reports both queued and active counts. New `agent.queue.list` RPC added.
- `src/cli/formatters.rs` (lines 5–12, 128–134) — `format_state()` is exhaustive and must learn the `Queued` variant.
- `src/shared/config.rs` (`DaemonConfig`, lines 58–75) — new `max_concurrent_agents` key with `serde(default)`.
- `tests/` — existing post-summon `Active` assertions in `database.rs`, `executor_local.rs`, `executor_remote.rs`, `event_bus.rs`, `cli_circle.rs` need a `wait_for_state` helper; new integration tests cover restart recovery, capacity saturation, no-eligible-worker, scroll/ad-hoc interleave, and banish-while-queued.

### Existing Patterns to Follow

- **Reactor + tick.** `scroll_keeper` already runs as a Tokio task that subscribes to event-bus completions and periodically wakes. The scheduler mirrors this shape and runs in parallel — they compose by pipelining (scroll_keeper enqueues; scheduler dispatches), not by merging.
- **`CREATE TABLE IF NOT EXISTS` + `ALTER ... ADD COLUMN`.** No migration framework. The new `task_queue` table follows this pattern.
- **`impl_state_enum!` macro.** Adding a variant to `AgentState` automatically updates `FromStr`, `Display`, and serde mappings.
- **Event publish + DB persistence.** Every `StreamEvent` is broadcast for live subscribers and written to the `events` table. Adding `AgentQueued` requires no new plumbing.
- **`#[serde(default = "fn")]` for config defaults.** New `max_concurrent_agents` follows the existing `default_port` / `default_log_level` pattern in `DaemonConfig`.
- **`MockClock` / event-bus subscription test pattern.** Existing event-bus tests subscribe and assert on broadcast deliveries; new tests reuse this and add a manual-tick handle on the scheduler.

### Key Dependencies

- `EventBus` (broadcast subscribe) — scheduler subscribes to `StateChange` (to wake on completions) and `WorkerRegistered` (new event the registry must already publish, or that this spec adds; see ambiguity table).
- `WorkerRegistry::has_eligible_worker()` (new) — non-mutating peek used by the scheduler before claim.
- `Persistence` — the durable layer; `task_queue` table + helpers; restart-recovery query.
- `Executor` (placement abstraction) — unchanged interface, but the scheduler is now its sole caller.

### Ambiguity Resolutions

| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| Default `max_concurrent_agents` | Plan says "a sensible number like 8" | Default `8`. Operator overrides via `daemon.max_concurrent_agents` in config. | Plan body / assumed default |
| `summon` RPC return shape | Plan says "honest wire contract once daemon owns placement" | `SummonResult` structure unchanged; `state` field can now be `"queued"`. CLI prints `<id> (queued)`. No new RPC method added. | Plan + minimal-change principle |
| `block_reason` enum values | Plan body lists `capacity \| no_eligible_worker \| scroll_conflict`, but edge-case table says scroll-conflict tasks stay in `tasks` (not enqueued) | `block_reason ∈ {capacity, no_eligible_worker}`, NULL when not yet evaluated or unblocked. Scroll-conflict tasks remain in `tasks` and are *not* enqueued. | Plan edge-case table (authoritative over body) |
| Scheduler tick interval | Plan says "~50ms promotes" but doesn't pin the safety-net interval | 100ms periodic tick. Event-bus signals drive normal-case dispatch; tick is the safety net for worker registrations and TOCTOU retries. | Assumed default |
| Test determinism | Plan calls out flakiness risk but doesn't specify mechanism | `Scheduler` exposes `tick_now()` (test-only via `pub(crate)` + `#[cfg(test)]` flag) and a constructor variant that disables the periodic tick. Tests drive ticks explicitly. | Plan risk-mitigation section |
| Lane tie-break | Plan commits to ad-hoc-wins | Confirmed: ad-hoc lane drains before scroll lane on each tick. Within a lane, FIFO by `enqueued_at` then `id`. | Plan |
| Worker-registered event | Plan says scheduler "wakes on registration" but `worker_registry.rs` does not yet publish a registration event | Add `StreamEvent::WorkerRegistered { worker_id }` published from `WorkerRegistry::register`. Scheduler subscribes. | Assumed default |
| Banish while Queued | Plan edge-case row is clear: dequeue + mark Banished | Implemented as part of state-guard task. `banish()` accepts `Queued`. | Plan |
| Invoke on Queued | Plan: reject with clear error | `invoke()` returns `Err("agent has not started yet")` when state is `Queued`. | Plan |
| `dispatch failed → return to queue` semantics | Plan says "task returns to Queued, scheduler retries on next tick" | Scheduler's claim is two-phase: (a) `UPDATE agents SET state='Summoning'` + `DELETE FROM task_queue` in one transaction; (b) on `executor.start()` failure, *re-insert* into `task_queue` with original `enqueued_at` and set `agents.state='Queued'`. Same row identity (by agent id). | Plan + atomicity requirement |
| Default `daemon.max_concurrent_agents` reload | Plan: "honored on next tick (no restart needed)" | Config-reload path (existing `Config::load`) updates an `AtomicU32` shared with the scheduler; tick reads the latest value. | Plan |
| `--ralph` flag in input | User invoked `/hatch:write-spec durable-work-queue` (no `--ralph`) | Generate monolithic spec only. Skip Phase 8. | User input |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | `AgentState::Queued` variant + exhaustive match fixes | None | Low |
| 2 | `StreamEvent::AgentQueued` + `WorkerRegistered` variants | 1 | Low |
| 3 | `task_queue` table + persistence helpers | 1 | Medium |
| 4 | Restart-recovery semantics | 1, 3 | Low |
| 5 | `WorkerRegistry::has_eligible_worker` capability peek | None | Low |
| 6 | `daemon.max_concurrent_agents` config + atomic reload | None | Low |
| 7 | `Scheduler` module — reactor, tick loop, test mode | 2, 3, 5, 6 | High |
| 8 | Split `agent_manager.summon`; reroute scroll + orchestrator | 1, 2, 3, 7 | High |
| 9 | Banish-on-Queued + invoke-on-Queued guards | 1, 3 | Low |
| 10 | `grim status` queue-distinct counts | 1 | Low |
| 11 | `grim queue` CLI + `agent.queue.list` RPC | 3, 8 | Medium |
| 12 | `wait_for_state` test helper + integration test surface | 8 | Medium |

### Critical Path

```
   1 ──┬── 2 ──┐
       ├── 3 ──┼── 7 ── 8 ──┬── 11
       ├── 4   │            ├── 12
       └── 9   │            
   5 ──────────┤
   6 ──────────┘
   1 ── 10
```

- **Parallelizable:** 5 and 6 are independent of 1–4 and can land first. After 1, work on 2/3/4/9/10 can fan out.
- **Serial spine:** 7 → 8 is the hottest path. 8 is the riskiest task (split of `summon`); 7 must land first so 8 has a scheduler to call.
- **Tail:** 11 (CLI) and 12 (test surface) wait on 8.

---

### Task 1: `AgentState::Queued` variant + exhaustive match fixes

**Summary:** Add a new `AgentState::Queued` variant and fix every exhaustive match expression that breaks at compile time. No behavior change yet — agents can be inserted in `Queued` and stay there indefinitely.

**Dependencies:** None

**Files to create/modify:**
- `src/shared/types.rs` — add `Queued` to the `AgentState` enum and to `impl_state_enum!` mapping. Confirm `is_terminal()` returns `false` for `Queued`. Update test data at lines 238–243 and 299–303.
- `src/cli/formatters.rs` — add `AgentState::Queued => "queued".cyan().to_string()` arm at lines 6–12. Add `AgentState::Queued` to the test vec at lines 128–134.
- `tests/protocol.rs` — add a serde round-trip case for `Queued` (see Task 2 for the `AgentQueued` event itself; this task only ensures `AgentState::Queued` round-trips through `to_string`/`from_str`/serde).

**Detailed specification:**

Add the variant in lexical order at the top of the enum so it sits before `Summoning`:

```rust
pub enum AgentState {
    Queued,
    Summoning,
    Active,
    Complete,
    Failed,
    Banished,
}
```

Add `("queued", AgentState::Queued)` to whatever string-mapping array `impl_state_enum!` consumes in `types.rs`. Verify that `is_terminal()` still uses the `matches!(...)` form and does **not** include `Queued`.

In `formatters.rs::format_state`, render `Queued` as cyan to distinguish from `Summoning` (yellow) and `Active` (green).

This task is purely additive and must compile cleanly with `cargo build` and `cargo test --no-run` before merge.

**Edge cases to handle:**
- `is_terminal()` must return `false` for `Queued`.
- `AgentState::Queued.to_string()` must produce `"queued"` (snake_case, matches existing convention).

**Acceptance criteria:**
- [ ] `AgentState::Queued` exists as an enum variant in `src/shared/types.rs`.
- [ ] `cargo build --all-targets` succeeds with no warnings about non-exhaustive matches on `AgentState`.
- [ ] `AgentState::Queued.is_terminal()` returns `false`.
- [ ] `AgentState::Queued.to_string()` returns `"queued"`.
- [ ] `AgentState::from_str("queued")` returns `Ok(AgentState::Queued)`.
- [ ] `serde_json::to_string(&AgentState::Queued)` returns `"\"queued\""`.
- [ ] `format_state(&AgentState::Queued)` contains the substring `"queued"`.

**Contract tests (RED phase):**
- Test file: `src/shared/types.rs` (existing `#[cfg(test)] mod tests`)
- Tests to write before implementing:
  - `agent_state_queued_is_not_terminal` — asserts `!AgentState::Queued.is_terminal()`.
  - `agent_state_queued_string_roundtrip` — asserts `AgentState::from_str("queued").unwrap() == AgentState::Queued` and `AgentState::Queued.to_string() == "queued"`.
  - `agent_state_queued_serde_roundtrip` — asserts JSON serialize/deserialize fidelity.
- Test file: `src/cli/formatters.rs` (existing test module)
- Tests to write:
  - `format_state_handles_queued` — asserts `format_state(&AgentState::Queued)` contains `"queued"`.

**Notes/Warnings:**
- The Rust compiler will flag every exhaustive `match` site on `AgentState` once the variant is added. Use those compile errors as the to-do list — do not grep manually.
- Do not yet update `agent_manager::reload_from_db` or `banish()` guards; that is Task 4 / Task 9 territory.

---

### Task 2: `StreamEvent::AgentQueued` + `WorkerRegistered` variants

**Summary:** Add two new `StreamEvent` variants — `AgentQueued` (published when work is enqueued) and `WorkerRegistered` (published from `WorkerRegistry::register` so the scheduler can wake).

**Dependencies:** Task 1

**Files to create/modify:**
- `src/shared/protocol.rs` — add two variants to the `StreamEvent` enum (lines 156–193) with serde renames `agent_queued` and `worker_registered`. Update `kind()` (lines 220–229) to return matching strings.
- `src/daemon/worker_registry.rs` — at the end of `register()` (around line 78–102), publish `StreamEvent::WorkerRegistered { worker_id }` via the event bus the registry already holds (or accept an `Arc<EventBus>` if it does not — check current constructor).
- `tests/protocol.rs` — extend serialization tests (lines 270–417) to cover both new variants.

**Detailed specification:**

```rust
#[serde(rename = "agent_queued")]
AgentQueued {
    agent_id: AgentId,
    lane: String,            // "adhoc" | "scroll"
    block_reason: Option<String>,  // "capacity" | "no_eligible_worker" | None
},
#[serde(rename = "worker_registered")]
WorkerRegistered {
    worker_id: WorkerId,
},
```

`AgentQueued` is published from `agent_manager::enqueue()` in Task 8. `WorkerRegistered` is published from `worker_registry::register` in this task (so the scheduler in Task 7 has something to subscribe to).

**Edge cases to handle:**
- `block_reason` is `None` when the task was enqueued before any placement check ran (rare; happens if the scheduler is not yet ticking).
- Re-registering an existing worker (re-connection) republishes `WorkerRegistered`; the scheduler must treat the event as idempotent.

**Acceptance criteria:**
- [ ] `StreamEvent::AgentQueued` exists with fields `agent_id`, `lane`, `block_reason`.
- [ ] `StreamEvent::WorkerRegistered` exists with field `worker_id`.
- [ ] `StreamEvent::AgentQueued { ... }.kind()` returns `"agent_queued"`.
- [ ] `StreamEvent::WorkerRegistered { ... }.kind()` returns `"worker_registered"`.
- [ ] Both variants round-trip through `serde_json` losslessly.
- [ ] `WorkerRegistry::register` publishes `WorkerRegistered` after a successful insert; verifiable by an event-bus subscriber.

**Contract tests (RED phase):**
- Test file: `tests/protocol.rs`
- Tests to write:
  - `agent_queued_event_serde_roundtrip` — JSON-encode then decode an `AgentQueued` and assert equality.
  - `worker_registered_event_serde_roundtrip` — same for `WorkerRegistered`.
  - `agent_queued_kind_string` — asserts `kind()` returns `"agent_queued"`.
  - `worker_registered_kind_string` — asserts `kind()` returns `"worker_registered"`.
- Test file: `tests/worker_registry.rs`
- Tests to write:
  - `register_publishes_worker_registered_event` — subscribes to the event bus, calls `register()`, asserts the event is delivered with the correct `worker_id`.

**Notes/Warnings:**
- If `WorkerRegistry::register` does not currently hold an `Arc<EventBus>`, threading it through is part of this task — keep the surface narrow.

---

### Task 3: `task_queue` table + persistence helpers

**Summary:** Add a `task_queue` SQLite table with insert / claim / requeue / list / delete / restart-recovery helpers in `persistence.rs`. No reactor logic yet.

**Dependencies:** Task 1

**Files to create/modify:**
- `src/daemon/persistence.rs` — add `CREATE TABLE IF NOT EXISTS task_queue (...)` to the schema bootstrap (alongside existing tables at lines 51–166). Add helper methods on the persistence struct.
- `tests/database.rs` — extend with cases for the new table.

**Detailed specification:**

Schema:

```sql
CREATE TABLE IF NOT EXISTS task_queue (
    id              TEXT PRIMARY KEY,        -- matches agents.id
    lane            TEXT NOT NULL,           -- 'adhoc' | 'scroll'
    priority        INTEGER NOT NULL DEFAULT 0,
    enqueued_at     TEXT NOT NULL,           -- RFC3339 UTC
    provider_name   TEXT,                    -- nullable; null = "any"
    cwd             TEXT NOT NULL,
    model           TEXT,
    task_text       TEXT NOT NULL,
    block_reason    TEXT,                    -- 'capacity' | 'no_eligible_worker' | NULL
    FOREIGN KEY (id) REFERENCES agents(id)
);
CREATE INDEX IF NOT EXISTS idx_task_queue_dispatch
    ON task_queue (lane, priority DESC, enqueued_at, id);
```

Helpers (all on the existing `Persistence` struct):

```rust
pub fn enqueue_task(&self, row: &QueueRow) -> Result<()>;
pub fn list_queue(&self) -> Result<Vec<QueueRow>>;
pub fn list_queue_by_lane(&self, lane: &str) -> Result<Vec<QueueRow>>;
pub fn peek_next_dispatch(&self) -> Result<Option<QueueRow>>;        // honors lane order: adhoc before scroll
pub fn claim_for_dispatch(&self, id: &AgentId) -> Result<bool>;       // DELETE + sets agents.state='Summoning' atomically; false if row vanished
pub fn requeue(&self, row: &QueueRow) -> Result<()>;                  // re-insert preserving original enqueued_at; sets agents.state='Queued'
pub fn delete_from_queue(&self, id: &AgentId) -> Result<bool>;        // for banish-while-queued
pub fn set_block_reason(&self, id: &AgentId, reason: Option<&str>) -> Result<()>;
pub fn count_queued(&self) -> Result<usize>;
pub fn restart_recovery(&self) -> Result<RecoveryReport>;             // see Task 4
```

`QueueRow` lives in `persistence.rs` (or `shared/types.rs` if it crosses the daemon boundary; default to `persistence.rs` since the queue is internal).

`peek_next_dispatch` ordering: `ORDER BY CASE lane WHEN 'adhoc' THEN 0 ELSE 1 END, priority DESC, enqueued_at ASC, id ASC LIMIT 1`.

`claim_for_dispatch` must use a single transaction: `BEGIN; DELETE FROM task_queue WHERE id = ?; UPDATE agents SET state = 'summoning' WHERE id = ?; COMMIT;`. Return `Ok(true)` if both rows were affected, `Ok(false)` if the queue row was already gone (someone else claimed it).

**Edge cases to handle:**
- Re-insert via `requeue` must preserve the original `enqueued_at` (ordering fairness).
- `delete_from_queue` returns `false` (not error) if the row no longer exists — banish is idempotent.
- `enqueue_task` rejects duplicate ids (foreign-key + primary-key constraint).

**Acceptance criteria:**
- [ ] `task_queue` table is created on first daemon start with `CREATE TABLE IF NOT EXISTS`.
- [ ] `enqueue_task` inserts a row that `list_queue` then returns.
- [ ] `peek_next_dispatch` returns rows in (lane=adhoc first, then priority DESC, then enqueued_at ASC) order — verified with at least one ad-hoc and one scroll row at different timestamps.
- [ ] `claim_for_dispatch(id)` returns `true` on first call and `false` on second, *and* atomically updates `agents.state` from `queued` to `summoning`.
- [ ] `requeue(row)` re-inserts with the original `enqueued_at` and sets `agents.state` back to `queued`.
- [ ] `delete_from_queue(id)` removes the row and returns `true`; calling it twice returns `false` the second time.
- [ ] `count_queued()` matches `list_queue().len()`.

**Contract tests (RED phase):**
- Test file: `tests/database.rs` (extend; alongside existing tests at lines 404–428).
- Tests to write:
  - `enqueue_then_list_returns_row` — insert one row, `list_queue` returns vec of length 1 with matching fields.
  - `peek_next_dispatch_orders_adhoc_before_scroll` — insert a scroll row at T, then an ad-hoc row at T+1; peek returns the ad-hoc row.
  - `peek_next_dispatch_orders_fifo_within_lane` — insert two scroll rows at T and T+1; peek returns T.
  - `claim_for_dispatch_is_atomic` — insert + claim once returns true; claim again returns false; `agents.state` is `summoning` after claim.
  - `requeue_preserves_enqueued_at` — claim, then requeue with original `enqueued_at`; a subsequent peek still returns this row first.
  - `delete_from_queue_is_idempotent` — first delete returns true, second returns false.
  - `count_queued_matches_list_len` — property: insert N, expect `count_queued() == N`.

**Notes/Warnings:**
- Use SQLite's `BEGIN IMMEDIATE` for `claim_for_dispatch` to avoid SQLITE_BUSY under contention; the daemon is single-process so contention is bounded but the scheduler may race with `banish`.
- `task_queue.id` foreign-key requires the `agents` row to exist first — `enqueue_task` is called after `insert_agent(..., state='queued', ...)`.

---

### Task 4: Restart-recovery semantics

**Summary:** On daemon startup, mark any agent in `Active` or `Summoning` state as `Failed` (their child processes are gone), but leave `Queued` agents untouched so the scheduler picks them up.

**Dependencies:** Tasks 1, 3

**Files to create/modify:**
- `src/daemon/persistence.rs` — implement `restart_recovery()` returning a `RecoveryReport { failed: usize, queued_remaining: usize }`.
- `src/daemon/agent_manager.rs` — replace the current `reload_from_db` logic at lines 79–99 with a call to `Persistence::restart_recovery()`. Publish a `StateChange` event for each agent flipped to `Failed`.

**Detailed specification:**

`restart_recovery()` runs:

```sql
UPDATE agents SET state = 'failed' WHERE state IN ('active', 'summoning');
SELECT COUNT(*) FROM agents WHERE state = 'queued';
```

The SQL for the failed set must return the list of affected ids so `agent_manager` can publish `StateChange { old_state, new_state: Failed }` events. Use SQLite's `RETURNING id, state` clause (rusqlite supports it on recent versions; if not, do a `SELECT ... WHERE state IN (...)` *first*, then `UPDATE`, in one transaction).

`Queued` rows are untouched — the scheduler will discover them on first tick.

**Edge cases to handle:**
- Empty database: `restart_recovery` returns `RecoveryReport { failed: 0, queued_remaining: 0 }` without error.
- `Complete` / `Failed` / `Banished` agents are untouched.
- An agent in `Queued` whose corresponding `task_queue` row is missing (orphan) — log a warning and leave the agent in `Queued`. Do not auto-fail; this is a bug-detection signal.

**Acceptance criteria:**
- [ ] After daemon restart, every agent that was `Active` is now `Failed`.
- [ ] After daemon restart, every agent that was `Summoning` is now `Failed`.
- [ ] After daemon restart, every agent that was `Queued` is still `Queued`.
- [ ] After daemon restart, every agent that was `Complete`/`Failed`/`Banished` is unchanged.
- [ ] For each agent flipped to `Failed`, a `StateChange` event is published with the correct `old_state`.
- [ ] `restart_recovery` returns counts matching the actual transitions.

**Contract tests (RED phase):**
- Test file: `tests/database.rs`
- Tests to write:
  - `restart_recovery_fails_active_and_summoning_only` — seed agents in all 6 states, call `restart_recovery`, assert resulting states.
  - `restart_recovery_preserves_queued` — seed three `Queued` agents, call `restart_recovery`, assert all three remain `Queued`.
  - `restart_recovery_returns_correct_counts` — seed 2 active + 3 queued + 1 complete; assert `RecoveryReport { failed: 2, queued_remaining: 3 }`.
- Test file: `tests/event_bus.rs`
- Tests to write:
  - `restart_recovery_publishes_state_change_for_each_failure` — subscribe, run recovery on a DB with 2 active agents, assert 2 `StateChange` events with `new_state: Failed`.

**Notes/Warnings:**
- Recovery happens *before* the scheduler starts ticking. Order in `daemon/mod.rs` boot sequence: persistence → recovery → scheduler.
- Replacing `reload_from_db` is the riskiest line — it currently fails *all* `Summoning|Active` rows. Make sure the new behavior doesn't accidentally fail `Queued` rows.

---

### Task 5: `WorkerRegistry::has_eligible_worker` capability peek

**Summary:** Add a non-mutating method on `WorkerRegistry` that returns whether at least one registered worker advertises the given provider+version. Used by the scheduler before it claims a queue row.

**Dependencies:** None

**Files to create/modify:**
- `src/daemon/worker_registry.rs` — add `has_eligible_worker(provider_name: &str, constraint: &VersionReq) -> bool`. Implementation mirrors the filter clauses in `pick_least_loaded` (lines 132–153) but without the `in_flight < max_concurrent` check (capacity is the *daemon's* concern; the registry only answers "is there a worker that *could* run this if it had capacity").
- `tests/worker_registry.rs` — add coverage.

**Detailed specification:**

```rust
pub fn has_eligible_worker(&self, provider_name: &str, constraint: &VersionReq) -> bool {
    let workers = self.workers.lock().unwrap();
    workers.values().any(|w| {
        w.providers.iter().any(|(n, v)| n == provider_name && constraint.matches(v))
    })
}
```

This intentionally does *not* check `in_flight < max_concurrent`. The semantic is: "could anyone in principle take this work?" The scheduler combines this peek with its own daemon-wide capacity check.

**Edge cases to handle:**
- Empty registry: returns `false`.
- Provider name match but version mismatch: returns `false`.
- Multiple workers, only one matching: returns `true`.

**Acceptance criteria:**
- [ ] `has_eligible_worker` returns `false` when no workers are registered.
- [ ] `has_eligible_worker("anthropic", any_version_req)` returns `true` after registering a worker advertising `anthropic` at a matching version.
- [ ] `has_eligible_worker` ignores the `in_flight` / `max_concurrent` fields (proven by registering a worker with `in_flight == max_concurrent` and asserting the method still returns `true`).
- [ ] `has_eligible_worker` does not mutate registry state (call twice, assert registry contents unchanged).

**Contract tests (RED phase):**
- Test file: `tests/worker_registry.rs`
- Tests to write:
  - `has_eligible_worker_empty_returns_false`
  - `has_eligible_worker_provider_match_returns_true`
  - `has_eligible_worker_provider_mismatch_returns_false`
  - `has_eligible_worker_version_mismatch_returns_false`
  - `has_eligible_worker_ignores_capacity` — register a saturated worker; method still returns `true`.
  - `has_eligible_worker_is_non_mutating` — assert `worker_count()` is unchanged before/after.

**Notes/Warnings:**
- If the codebase passes provider as a different type (e.g., `&ProviderName`), match the existing `pick_least_loaded` signature exactly.

---

### Task 6: `daemon.max_concurrent_agents` config + atomic reload

**Summary:** Add a new `max_concurrent_agents` key to `DaemonConfig` with `serde(default = "default_max_concurrent")` returning `8`. Expose it as an `Arc<AtomicU32>` so the scheduler reads the latest value on every tick without restart.

**Dependencies:** None

**Files to create/modify:**
- `src/shared/config.rs` — add field at lines 58–75:
  ```rust
  #[serde(default = "default_max_concurrent")]
  pub max_concurrent_agents: u32,
  
  fn default_max_concurrent() -> u32 { 8 }
  ```
- `src/daemon/mod.rs` (or wherever the daemon assembles its components) — wrap the value in `Arc<AtomicU32>` and pass it to the scheduler.
- `tests/config_worker.rs` (extend) or new test in an existing config test file.

**Detailed specification:**

The atomic is owned by the daemon and shared with the scheduler. Config reload (existing path: `Config::load`) writes the new value via `cap.store(new_value, Ordering::Relaxed)`. The scheduler reads it via `cap.load(Ordering::Relaxed)` at the top of each tick.

**Edge cases to handle:**
- Config file missing the key entirely: defaults to `8`.
- Config value of `0`: scheduler never dispatches (effective freeze). This is a valid operator override; do not error.
- Config reload to a smaller value than current `Active` count: in-flight agents continue to run; only *new* dispatches are gated. (No preemption.)

**Acceptance criteria:**
- [ ] `DaemonConfig` has a public field `max_concurrent_agents: u32`.
- [ ] Default value when key is absent from TOML is `8`.
- [ ] A config TOML with `[daemon]\nmax_concurrent_agents = 16` parses to `16`.
- [ ] A config TOML with `[daemon]\nmax_concurrent_agents = 0` parses to `0` (no error).
- [ ] The shared `AtomicU32` reflects updates after a `Config::load`-driven reload.

**Contract tests (RED phase):**
- Test file: `tests/config_worker.rs` (extend) or `src/shared/config.rs` test module.
- Tests to write:
  - `daemon_config_max_concurrent_default_is_eight` — empty `[daemon]` table parses to `8`.
  - `daemon_config_max_concurrent_explicit` — `max_concurrent_agents = 16` parses to `16`.
  - `daemon_config_max_concurrent_zero_is_valid` — `max_concurrent_agents = 0` parses without error.

**Notes/Warnings:**
- This task does not add reload semantics for *all* config keys — just the wiring for this one. Other reload behavior is out of scope.

---

### Task 7: `Scheduler` module — reactor, tick loop, test mode

**Summary:** Create a new `src/daemon/scheduler.rs` module with a Tokio-task reactor that subscribes to `StateChange`/`AgentQueued`/`WorkerRegistered` events and additionally wakes on a 100ms periodic tick. On each tick it dispatches eligible queued tasks while global capacity allows.

**Dependencies:** Tasks 2, 3, 5, 6

**Files to create/modify:**
- `src/daemon/scheduler.rs` — new file.
- `src/daemon/mod.rs` — register the module and wire its construction into the daemon boot sequence (after persistence + recovery, before serving RPC).
- `src/lib.rs` — re-export if needed.
- `tests/scheduler.rs` — new integration test file using the test-mode constructor.

**Detailed specification:**

```rust
pub struct Scheduler {
    persistence: Arc<Persistence>,
    workers: Arc<WorkerRegistry>,
    bus: Arc<EventBus>,
    cap: Arc<AtomicU32>,
    manager: Arc<AgentManager>,        // for dispatch_internal callback
    test_manual_tick: bool,            // disables periodic tick when true
    tick_signal: tokio::sync::Notify,  // manual ticks pulse this
}

impl Scheduler {
    pub fn spawn(...) -> SchedulerHandle { /* normal mode: runs a Tokio task with both bus + interval */ }
    
    #[cfg(test)]
    pub fn spawn_for_test(...) -> SchedulerHandle { /* manual tick only */ }
    
    pub async fn tick_now(&self) { /* drives one full tick synchronously and returns when settled */ }
}
```

Tick loop:

```
1. Count current Active+Summoning agents → in_flight.
2. Read cap.load(Ordering::Relaxed) → cap.
3. While (in_flight < cap):
   a. row = persistence.peek_next_dispatch() — break if None.
   b. If row.provider_name set: if !workers.has_eligible_worker(row.provider_name, ..) — set block_reason='no_eligible_worker', skip this row, continue with the *next* row (do NOT break).
      Note: peek_next_dispatch must support a "skip blocked" mode, OR the scheduler iterates through list_queue() ordered, filtering blocked rows in memory. Choose the latter for simplicity in v1.
   c. If !persistence.claim_for_dispatch(row.id) — row vanished (raced with banish), continue.
   d. Call manager.dispatch_internal(row). On success, in_flight += 1. On failure, persistence.requeue(row) and break (to avoid a tight failure loop on this tick; next tick will retry).
4. For all rows still in queue with block_reason=capacity that would now fit: clear block_reason. (Cosmetic — scheduler will revisit them anyway.)
```

The reactor subscribes to:
- `StateChange { new_state }` where `new_state` is terminal (`Complete`, `Failed`, `Banished`) → tick.
- `AgentQueued` → tick.
- `WorkerRegistered` → tick (re-evaluate `no_eligible_worker` blocks).

Plus a `tokio::time::interval(Duration::from_millis(100))` tick as the safety net.

In test mode, the periodic interval is disabled; tests call `tick_now()` explicitly.

**Edge cases to handle:**
- Cap is `0`: tick loop's outer while-condition is immediately false; nothing dispatches.
- `peek_next_dispatch` returns `None`: tick exits cleanly.
- `claim_for_dispatch` returns `false` (raced banish): skip and continue.
- `dispatch_internal` returns `Err`: `requeue` and break to avoid livelock; next tick re-tries (any of the wakeup signals).
- Scheduler shutdown: handle owns a `CancellationToken`; the daemon cancels it on shutdown.

**Acceptance criteria:**
- [ ] `Scheduler::spawn_for_test` exists and returns a handle whose periodic tick is disabled.
- [ ] `tick_now()` is async and returns only after the tick has completed (no fire-and-forget).
- [ ] In test mode, calling `tick_now` with cap=2, two Queued rows, and an eligible worker results in exactly two `dispatch_internal` calls (verifiable via a fake `AgentManager`).
- [ ] In test mode, calling `tick_now` with cap=2 and two Active agents already running results in zero dispatches.
- [ ] In test mode, a row with `provider_name="missing"` and no eligible worker is left in the queue with `block_reason='no_eligible_worker'` and is *not* dispatched.
- [ ] On `WorkerRegistered` for the missing provider, the next `tick_now` clears the block and dispatches.
- [ ] Failed `dispatch_internal` requeues the row and breaks the inner loop; next tick re-attempts.

**Contract tests (RED phase):**
- Test file: `tests/scheduler.rs`
- Tests to write:
  - `scheduler_dispatches_up_to_cap` — cap=2, three Queued, one eligible worker, single tick → 2 dispatches.
  - `scheduler_respects_inflight_count` — cap=2, two Active already, one Queued, single tick → 0 dispatches.
  - `scheduler_blocks_no_eligible_worker` — Queued with provider='absent', tick → row remains queued, `block_reason='no_eligible_worker'`.
  - `scheduler_unblocks_on_worker_registered` — start with above scenario, register matching worker, tick → row is dispatched.
  - `scheduler_requeues_on_dispatch_failure` — fake manager returns Err, tick → row reappears in queue with original `enqueued_at`.
  - `scheduler_adhoc_lane_drains_first` — one ad-hoc + one scroll Queued, cap=1, tick → ad-hoc dispatched, scroll remains queued.
  - `scheduler_idempotent_when_queue_empty` — empty queue, ticks complete without panic.

**Non-testable items:**
- The 100ms periodic tick interval value itself (config wiring); covered implicitly by integration tests in Task 12.

**Notes/Warnings:**
- The trait boundary for `manager.dispatch_internal` should be a small trait the scheduler holds (`trait Dispatcher { async fn dispatch(&self, row: QueueRow) -> Result<()>; }`) so tests can substitute a fake. Wire `AgentManager` to implement it in Task 8.
- Avoid holding the `WorkerRegistry` lock across `dispatch_internal` — peek and release before claiming.
- Test the "dispatch break + next tick recovers" property carefully; this is the TOCTOU window the plan calls out.

---

### Task 8: Split `agent_manager.summon`; reroute scroll + orchestrator

**Summary:** Decompose `agent_manager::summon()` into two functions: `enqueue()` (insert agent in `Queued` state, write to `task_queue`, publish `AgentQueued`, return) and `dispatch_internal()` (the existing post-insert `executor.start()` + state transitions, called only by the scheduler). Reroute `scroll_keeper::schedule_tasks` and `orchestrator::handle_completion` (pact firing) through `enqueue`.

**Dependencies:** Tasks 1, 2, 3, 7

**Files to create/modify:**
- `src/daemon/agent_manager.rs` — split `summon` (lines 160–244):
  - New `pub async fn enqueue(&self, ...) -> Result<Agent>` — does steps 1–4 of current `summon` but inserts state as `Queued`, additionally writes `task_queue` row, publishes `StreamEvent::AgentQueued`. Returns the agent immediately.
  - New `pub(crate) async fn dispatch_internal(&self, row: QueueRow) -> Result<()>` — does steps 5–8 of current `summon` (executor.start, state transitions, watch_completion registration). On failure, returns Err so the scheduler can requeue.
  - The old `pub async fn summon` is removed; the RPC handler calls `enqueue` directly.
  - Implement the `Dispatcher` trait (from Task 7) for `AgentManager` so the scheduler can call `dispatch_internal` polymorphically.
- `src/daemon/rpc.rs` (lines 39–60) — `handle_summon` calls `manager.enqueue`. The returned `SummonResult.state` is now `"queued"`. CLI prints `<id> (queued)`.
- `src/daemon/scroll_keeper.rs` (lines 429–437) — replace `manager.summon(...)` with `manager.enqueue(...)`. Scroll-level conflict and `max_concurrency` checks remain *before* enqueue (those tasks stay in `tasks` table, not in `task_queue`).
- `src/daemon/orchestrator.rs` (line 78) — replace `manager.summon(...)` with `manager.enqueue(...)`.
- `src/cli/commands/...` (the summon command) — accept that the printed state is `(queued)`. No flag changes.

**Detailed specification:**

`enqueue` flow:
1. Build `Agent { state: Queued, ... }`.
2. `persistence.insert_agent(&agent)`.
3. `persistence.enqueue_task(&QueueRow { id: agent.id, lane, ... })` — `lane` is `"adhoc"` for RPC summon and `"scroll"` for scroll/orchestrator callers (add a `lane: Lane` parameter to `enqueue`; default the public RPC path to `Lane::Adhoc`).
4. Publish `StreamEvent::AgentQueued { agent_id, lane: lane.to_string(), block_reason: None }`.
5. Notify the scheduler (via the existing event-bus subscription — no extra plumbing).
6. Return the agent.

`dispatch_internal` flow:
1. `executor.start(request)` — same logic as current `summon` lines 197–206.
2. Update agent: `state = Active`, set `pid`, persist.
3. Publish `StateChange { Summoning → Active }`.
4. Register cancel handle and completion watcher (same as current lines 232–240).
5. Returns `Ok(())` on success. On any error, return `Err(e)` *without* mutating queue state — the scheduler is responsible for `requeue` + state rollback to `Queued`.

Note the order: by the time `dispatch_internal` runs, `claim_for_dispatch` has already moved `agents.state` from `Queued` to `Summoning`. On error, the scheduler's `requeue` flips it back to `Queued`.

**Edge cases to handle:**
- `enqueue` called before scheduler is started: row sits in `task_queue` until first tick; this is fine.
- `dispatch_internal` succeeds but the agent immediately fails inside `executor.start`'s monitor loop: the existing `watch_completion` path handles this with a `StateChange` to `Failed`. No new code needed.
- `enqueue` for a scroll task: `lane = scroll`. `scroll_keeper` continues to enforce `max_concurrency` *before* calling `enqueue` so the queue is not flooded.
- Pact-fired enqueue: pact's spawned task gets `lane = scroll` (so interactive ad-hoc summons still beat it on contention).

**Acceptance criteria:**
- [ ] `agent_manager.enqueue(...)` returns an `Agent` whose `state == AgentState::Queued` and whose `pid` is `None`.
- [ ] After `enqueue`, the agent exists in both the `agents` table (state=queued) and the `task_queue` table.
- [ ] After `enqueue`, a `StreamEvent::AgentQueued` event has been published with the matching `agent_id` and `lane`.
- [ ] `agent_manager.dispatch_internal(row)` is `pub(crate)` (not exposed beyond the daemon crate).
- [ ] `dispatch_internal` calls `executor.start` exactly once per call.
- [ ] On `dispatch_internal` success, `agents.state` is `Active` and `pid` is set.
- [ ] On `dispatch_internal` error, the function returns `Err` *without* writing to `task_queue` (scheduler owns requeue).
- [ ] `rpc.handle_summon` returns a `SummonResult` whose `state` field equals `"queued"`.
- [ ] `scroll_keeper::schedule_tasks` no longer calls `manager.summon`; grep for the symbol returns zero hits in that file.
- [ ] `orchestrator::handle_completion` no longer calls `manager.summon`; pact firing routes through `manager.enqueue`.
- [ ] No call to `agent_manager.summon` exists anywhere in the codebase (the symbol is removed).

**Contract tests (RED phase):**
- Test file: `tests/agent_manager.rs` (new)
- Tests to write:
  - `enqueue_returns_agent_in_queued_state`
  - `enqueue_inserts_into_both_agents_and_task_queue`
  - `enqueue_publishes_agent_queued_event`
  - `enqueue_with_lane_scroll_marks_lane_correctly`
- Test file: `tests/scroll_lifecycle.rs` (extend)
- Tests to write:
  - `scroll_task_dispatch_routes_through_enqueue` — start a scroll, observe that tasks appear in `task_queue` with `lane=scroll` before being dispatched.
- Test file: `tests/cli_circle.rs` or new `tests/cli_summon.rs`
- Tests to write:
  - `summon_cli_returns_queued_state` — invoke `grim summon "..."` against a fake daemon; assert the printed state contains `"queued"`.

**Notes/Warnings:**
- This is the riskiest task. The plan explicitly flags that **every** existing test that asserted post-summon `Active` state will break. Task 12 builds the `wait_for_state` helper — but landing this task requires either (a) updating those tests in this PR or (b) using `wait_for_state` from a stub merged earlier. Recommendation: include the helper in this task (small) and update the broken tests in Task 12.
- The `Dispatcher` trait (from Task 7) is the seam for fake-manager testing. Make sure `AgentManager: Dispatcher` is implemented here.
- Do not pre-populate the cancel-handle map until `dispatch_internal` succeeds; otherwise `banish` of a Queued agent (Task 9) would think there's a process to kill.

---

### Task 9: Banish-on-Queued + invoke-on-Queued guards

**Summary:** Update `banish()` to accept `Queued` agents (delete the queue row, mark agent `Banished`). Update `invoke()` to reject `Queued` agents with a clear error.

**Dependencies:** Tasks 1, 3

**Files to create/modify:**
- `src/daemon/agent_manager.rs` — modify `banish()` (lines 339–368) and `invoke()` (lines 246–337).

**Detailed specification:**

`banish` — extend the state guard at lines 342–343 to include `Queued`:

```rust
match state {
    AgentState::Queued => {
        persistence.delete_from_queue(&agent_id)?;
        persistence.update_agent_state(&agent_id, &AgentState::Banished)?;
        bus.publish(StateChange { old_state: Queued, new_state: Banished, .. });
        Ok(())
    }
    AgentState::Active | AgentState::Summoning => { /* existing kill-process path */ }
    other => Err(format!("cannot banish agent in state {other:?}")),
}
```

`invoke` — early-return when state is `Queued`:

```rust
if agent.state == AgentState::Queued {
    return Err(anyhow!("agent has not started yet (state: queued)"));
}
```

This check goes *before* the existing `Complete` check at line 287.

**Edge cases to handle:**
- Banish on a `Queued` agent whose `task_queue` row is somehow already gone (race with scheduler claim): `delete_from_queue` returns `false`, but we still update `agents.state` to `Banished`. The scheduler will see the state-change event and skip.
- Banish on a `Queued` agent that was just claimed (state moved to `Summoning`): the match falls through to the `Active|Summoning` arm, which already handles it.
- Invoke on `Queued` returns Err; CLI displays the error string.

**Acceptance criteria:**
- [ ] `banish(id)` on a `Queued` agent removes the row from `task_queue`, sets `agents.state` to `Banished`, and publishes a `StateChange` event.
- [ ] `banish(id)` on a `Queued` agent does **not** call `executor.kill` or any process-kill code path.
- [ ] `invoke(id, msg)` on a `Queued` agent returns an `Err` whose message contains `"has not started"`.
- [ ] `invoke(id, msg)` on a `Complete` agent still works (unchanged behavior).
- [ ] Banishing a `Queued` agent that was already claimed (state=`Summoning`) takes the `Active|Summoning` branch and kills the process.

**Contract tests (RED phase):**
- Test file: `tests/agent_manager.rs` (extend)
- Tests to write:
  - `banish_queued_removes_from_queue`
  - `banish_queued_sets_state_banished`
  - `banish_queued_does_not_invoke_kill` — fake executor's `kill` count remains 0.
  - `invoke_queued_returns_error_with_clear_message` — assert error message contains `"has not started"`.
  - `invoke_complete_unchanged` — regression guard.

**Notes/Warnings:**
- The error message string is part of the contract — implementers must keep `"has not started"` in the message.

---

### Task 10: `grim status` queue-distinct counts

**Summary:** Update `daemon.status` RPC and the CLI's status formatter to report `queued_count` distinctly from `active_count`.

**Dependencies:** Task 1

**Files to create/modify:**
- `src/daemon/rpc.rs` (lines 224–240) — add `queued_count: usize` to `StatusResponse`. Compute via `agents.iter().filter(state == Queued).count()`.
- `src/shared/protocol.rs` — add the field to the `StatusResponse` struct definition.
- `src/cli/commands/status.rs` — render the queued count alongside active count.
- `src/cli/formatters.rs` — add a status-line variant if the formatter is centralized.

**Detailed specification:**

`StatusResponse` shape:

```rust
pub struct StatusResponse {
    pub active_count: usize,
    pub queued_count: usize,
    pub max_concurrent_agents: u32,
    /* existing fields preserved */
}
```

CLI rendering: `Status: 3 active, 5 queued (cap 8)`.

**Edge cases to handle:**
- Empty database: `active_count = 0`, `queued_count = 0`.
- All agents `Queued`: `active_count = 0`, `queued_count = N`.
- Cap = 0: still render `(cap 0)` without error.

**Acceptance criteria:**
- [ ] `StatusResponse` struct contains `queued_count: usize` and `max_concurrent_agents: u32` fields.
- [ ] `rpc.handle_status` populates both new fields correctly when queried against a DB with mixed states.
- [ ] `grim status` CLI output contains both `active` and `queued` counts.
- [ ] `grim status --json` includes both fields in the JSON object.

**Contract tests (RED phase):**
- Test file: `tests/protocol.rs` (extend)
- Tests to write:
  - `status_response_queued_count_serde`
- Test file: `tests/cli_status.rs` (already exists per git status)
- Tests to write:
  - `status_reports_queued_count` — seed DB with 2 active + 3 queued; assert CLI text output contains both counts.
  - `status_json_includes_queued_count` — `--json` output has `queued_count: 3`.

---

### Task 11: `grim queue` CLI + `agent.queue.list` RPC

**Summary:** Add a new `agent.queue.list` RPC and a `grim queue` CLI command that lists pending work, age, lane, target provider, and block reason.

**Dependencies:** Tasks 3, 8

**Files to create/modify:**
- `src/shared/protocol.rs` — add `QueueListRequest`, `QueueListResponse { entries: Vec<QueueEntry> }`, `QueueEntry { id, lane, age_seconds, provider, model, cwd, block_reason }`.
- `src/daemon/rpc.rs` — add `handle_queue_list`. Reads from `task_queue` joined with `agents`; computes `age_seconds = now - enqueued_at`.
- `src/cli/commands/queue.rs` — new file. Command struct with optional `--json` flag.
- `src/cli/mod.rs` — register the new command.
- `src/cli/formatters.rs` — add `format_queue` for the table layout.

**Detailed specification:**

CLI default (text) output:

```
ID         LANE     AGE   PROVIDER     BLOCK
42a8f1c3   adhoc    12s   anthropic    capacity
9b3c4d2e   scroll   4s    -            -
```

`--json` returns the raw `QueueListResponse`.

`age_seconds` is computed at RPC handle time (server-side) from `enqueued_at` to keep the CLI dumb.

`block_reason` rendering: `capacity` → `"capacity"`, `no_eligible_worker` → `"no worker"`, `None` → `"-"`.

ID rendering: 8-char prefix, consistent with `grim circle`.

**Edge cases to handle:**
- Empty queue: print `"No queued work."` (or empty JSON array).
- Very old queue entry (>1h): render age as `1h12m` (use existing humanize helper if present, otherwise just seconds).

**Acceptance criteria:**
- [ ] `agent.queue.list` RPC returns a `QueueListResponse` with entries matching `task_queue` content.
- [ ] Each entry includes `id`, `lane`, `age_seconds`, `provider`, `cwd`, `model`, `block_reason`.
- [ ] `grim queue` prints a table with one row per queued task.
- [ ] `grim queue --json` emits valid JSON parseable by `serde_json::from_str::<QueueListResponse>`.
- [ ] `grim queue` against an empty queue prints a one-line "no queued work" message and exits 0.
- [ ] `grim queue` with an entry blocked by capacity shows `"capacity"` in the block column.

**Contract tests (RED phase):**
- Test file: `tests/protocol.rs` (extend)
- Tests to write:
  - `queue_list_response_serde_roundtrip`
- Test file: `tests/agent_manager.rs` or new `tests/cli_queue.rs`
- Tests to write:
  - `queue_list_returns_pending_entries` — enqueue 2, call RPC, assert 2 entries in response.
  - `queue_list_age_increases_over_time` — enqueue, wait via mock clock, assert age increases.
  - `queue_list_includes_block_reason` — enqueue with `block_reason='capacity'`, assert response carries the value.
  - `cli_queue_text_format_has_columns` — assert output contains `"LANE"` and `"BLOCK"` headers.
  - `cli_queue_json_emits_parseable_response`.
  - `cli_queue_empty_prints_message`.

**Notes/Warnings:**
- Reuse the short-prefix ID matching from `grim circle` (mentioned in README) to be consistent.
- Do not include the full `task_text` in the default text output (privacy + width); include it in `--json`.

---

### Task 12: `wait_for_state` test helper + integration test surface

**Summary:** Add a `wait_for_state(id, target_state, timeout)` helper to `tests/support/`. Update existing tests that asserted post-summon `Active` state to use it. Add new integration tests for restart recovery, capacity saturation, no-eligible-worker, scroll/ad-hoc interleave, and banish-while-queued.

**Dependencies:** Task 8 (the rest of the system must be wired)

**Files to create/modify:**
- `tests/support/mod.rs` (or extend existing `tests/support/`) — add `pub async fn wait_for_state(client: &Client, id: &str, target: AgentState, timeout: Duration) -> Result<Agent>`.
- `tests/scroll_lifecycle.rs`, `tests/executor_local.rs`, `tests/executor_remote.rs`, `tests/cli_circle.rs`, `tests/event_bus.rs` — replace `assert_eq!(a.state, AgentState::Active)` patterns with `wait_for_state(.., Active, 2s)`.
- `tests/scheduler_integration.rs` (new) — end-to-end tests below.

**Detailed specification:**

`wait_for_state` implementation: poll `agent.get(id)` every 25ms until state matches or timeout; return `Err` on timeout with the actual final state in the message.

Integration tests (each is its own `#[tokio::test]`):

1. **`restart_recovery_keeps_queued_loses_active`**
   - Boot daemon, enqueue 3 ad-hoc summons (cap=0 to keep them Queued).
   - Force-kill daemon (drop the daemon handle without graceful shutdown).
   - Boot a new daemon over the same SQLite file with cap=0.
   - Assert: 3 agents in `Queued`, 0 in `Failed`. (Additionally, seed a synthetic `Active` agent in the DB before the second boot to verify it is flipped to `Failed`.)

2. **`capacity_saturation_promotes_on_completion`**
   - cap=2. Enqueue 3 tasks, all dispatchable.
   - Assert: 2 dispatched (Active), 1 Queued.
   - Complete one Active agent (drive its executor to finish).
   - `wait_for_state` on the third — must reach Active within 1s.

3. **`no_eligible_worker_unblocks_on_registration`**
   - cap=4. Enqueue an ad-hoc summon for provider `"absent"` (no worker registered).
   - Assert: agent stays Queued; `grim queue` shows `block_reason=no_eligible_worker`.
   - Register a worker advertising `"absent"`.
   - `wait_for_state` — agent reaches Active within 1s.

4. **`scroll_and_adhoc_interleave_adhoc_wins`**
   - cap=1, one Active agent.
   - Enqueue a scroll task at T, then an ad-hoc summon at T+1.
   - Complete the Active agent.
   - Assert: ad-hoc agent reaches Active before the scroll agent.

5. **`banish_while_queued_dequeues`**
   - cap=0. Enqueue an ad-hoc summon.
   - `banish` it.
   - Assert: agent state is `Banished`; `task_queue` row is gone; `grim queue` shows zero entries.
   - Bonus: bring cap up to 1; assert no dispatch happens (the agent is gone, not Queued).

**Edge cases to handle:**
- `wait_for_state` against a non-existent id: return Err immediately (don't loop).
- Mock clock vs real time: integration tests use real time with short timeouts (2s); flakiness mitigated by manual-tick scheduler when possible.

**Acceptance criteria:**
- [ ] `wait_for_state(id, target, timeout)` returns `Ok(Agent)` once `agent.state == target`.
- [ ] `wait_for_state` returns `Err` after `timeout` with a message containing the actual final state.
- [ ] All 5 integration tests above pass.
- [ ] No existing test asserts `agent.state == AgentState::Active` immediately after `summon` — every such site uses `wait_for_state` instead. (Verifiable with `rg "AgentState::Active" tests/ | grep -v wait_for_state` returning zero hits where the surrounding code is a direct post-summon assertion.)

**Contract tests (RED phase):**
- Test file: `tests/support/mod.rs` (or wherever the helper lives)
- Tests to write:
  - `wait_for_state_returns_when_target_matches`
  - `wait_for_state_times_out_when_state_never_matches`
- Test file: `tests/scheduler_integration.rs`
- Tests to write: the five named above.

**Non-testable items:**
- The mechanical edits to existing test files (replacing assertions with the helper). These are wiring changes; they pass when the suite still passes after Task 8.

**Notes/Warnings:**
- These tests are the ones the plan flags as "flakiness magnet." If a test is intermittent, prefer extending the timeout marginally first; if that doesn't help, switch to the manual-tick scheduler from Task 7 and drive ticks explicitly.

---

## Testing Strategy (TDD)

Implementation follows red-green TDD: for each task, the implementer writes failing contract tests from the acceptance criteria (RED), then implements the minimum code to make them pass (GREEN). Contract tests are immutable once committed.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `src/shared/types.rs`, `src/cli/formatters.rs` | 4 | None |
| 2 | `tests/protocol.rs`, `tests/worker_registry.rs` | 5 | None |
| 3 | `tests/database.rs` | 7 | None |
| 4 | `tests/database.rs`, `tests/event_bus.rs` | 4 | Boot ordering (covered by integration in Task 12) |
| 5 | `tests/worker_registry.rs` | 6 | None |
| 6 | `tests/config_worker.rs` | 3 | Reload mechanics (integration in Task 12) |
| 7 | `tests/scheduler.rs` | 7 | Real-time periodic tick interval value |
| 8 | `tests/agent_manager.rs`, `tests/scroll_lifecycle.rs`, `tests/cli_summon.rs` | 6 | Removal of old `summon` symbol (grep guard) |
| 9 | `tests/agent_manager.rs` | 5 | None |
| 10 | `tests/protocol.rs`, `tests/cli_status.rs` | 3 | None |
| 11 | `tests/protocol.rs`, `tests/cli_queue.rs` | 6 | Humanized age rendering edge cases |
| 12 | `tests/support/mod.rs`, `tests/scheduler_integration.rs` | 7 | Mechanical migration of existing assertions |

### Integration Testing

The five end-to-end tests in Task 12 cover the cross-task contract:

- **Restart recovery** crosses Tasks 1, 3, 4, 7, 8.
- **Capacity saturation** crosses Tasks 6, 7, 8.
- **No eligible worker** crosses Tasks 2, 5, 7, 11.
- **Scroll + ad-hoc interleave** crosses Tasks 3, 7, 8.
- **Banish while queued** crosses Tasks 8, 9, 11.

### Manual Testing Checklist

- [ ] `grim daemon` starts cleanly with no `max_concurrent_agents` in config (default 8 applied).
- [ ] `grim summon "..."` returns `<id> (queued)` and the agent transitions to active within ~100ms when capacity is free.
- [ ] `grim summon` 12 times against a daemon with `cap=4`: 4 go Active, 8 stay Queued; `grim queue` shows the backlog with `block_reason=capacity`.
- [ ] `grim banish <queued-id>` removes the row from `grim queue`.
- [ ] Kill the daemon with Active + Queued agents; restart; `grim circle` shows Active ones as Failed and Queued ones still queued; the scheduler picks them up.
- [ ] `grim summon` for a provider with no worker; `grim queue` shows `block_reason=no worker`; start a matching `grimw`; agent dispatches.
- [ ] `grim status` shows both `queued` and `active` counts.

## Rollout Considerations

### Feature Flags

None. The feature is a coordinated change to a wire contract (`summon` returns `Queued`); a flag would create two divergent code paths that re-converge nowhere useful. The default `max_concurrent_agents=8` acts as a soft de-facto rollout knob — operators can set it very high to approximate the old "always start immediately" behavior if they need to.

### Migration Strategy

- **Schema:** `task_queue` is created via `CREATE TABLE IF NOT EXISTS` on first boot post-upgrade. No backfill needed — existing `Active`/`Complete`/`Failed` agents are unaffected.
- **In-flight agents at upgrade time:** When the new daemon boots, restart-recovery (Task 4) flips any `Active`/`Summoning` rows to `Failed`. This is identical to the existing pre-upgrade behavior on daemon restart, so no operator surprise.
- **CLI clients on older code:** A `grim` CLI built before this change calls `agent.summon` and ignores the new `state="queued"` value (it just prints what the daemon returned). No CLI break.
- **Workers (`grimw`):** Unchanged. They receive assignments via the existing executor → worker RPC path. No protocol-version bump needed.

### Rollback Plan

- The previous version of the daemon will start cleanly against a database that contains a `task_queue` table (it just ignores the table) and `Queued` agents (the `AgentState` enum will fail to parse — see caveat below).
- **Caveat:** Rolling back across a `Queued`-state agent in the DB will cause the old daemon's `AgentState::from_str` to error on those rows. Mitigation: before rolling back, run a one-shot `UPDATE agents SET state='failed' WHERE state='queued'; DELETE FROM task_queue;` against the daemon's SQLite file (operator-run, document in the README's "Operations" section as part of this feature's PR).
- The rollback is a manual, operator-driven action; no in-product flag toggles it.

## Open Items

- [ ] Lane tie-break direction (ad-hoc-wins) is a guess. After dogfooding for a release, we may find scroll-tasks-first is better. Revisit at next planning cycle.
- [ ] Dashboard rendering of queued agents is not in this spec. Once `Queued` exists as a state, the dashboard's existing state-badge code will pick it up automatically; explicit design is deferred.
- [ ] `grim summon --wait` sugar flag for "block until Active or N seconds" — defer until someone asks.
- [ ] Per-tenant / per-cwd / per-provider concurrency caps are out of scope (v2). The `priority` column in `task_queue` is provisioned in the schema so they can land without migration.

---

*This spec is implementation-ready. Each task is designed for red-green TDD. Tasks can be picked up independently (respecting the dependency graph in the Critical Path) and completed in a single iteration.*
