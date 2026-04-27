# Implementation Spec: Supervision Trees — Restart Policy & Escalation

> Generated from: `.claude/plans/supervision-trees.md`
> Generated on: 2026-04-27

## Overview

Today, an agent that crashes or hits a transient provider error transitions to `Failed` and stays there. Overnight scrolls fail on a single 502; standing review agents stay dead until a human intervenes. There is no policy layer the daemon can enforce — no retry budget, no escalation path, no concept of a "task that recovered after two retries."

This spec lands the daemon-internal `Supervisor` actor, a new transient `AgentState::Restarting`, a `restart_history` table, summon-time CLI surface (`--restart on_failure`, `--max-restarts N/Ts`, `--escalate-to <addr>`), and four `StreamEvent` variants for audit. Restarts ride the existing scheduler dispatch path; escalation rides the existing mail bus. The implementation is the same shape as the `WakeRegistry` work that just shipped: a peer actor with an injected `Clock` seam, a per-agent rate counter, and a banish cascade.

## Technical Context

### Relevant Codebase Areas

- `src/shared/types.rs:39-80` — `AgentState` enum with `is_terminal()` and `is_final()`. `Restarting` lands here as a non-terminal, non-final variant (slot-free, mid-lifecycle). The macro at `:54` and the `is_*` helpers must include it.
- `src/daemon/agent_manager.rs:152-217` — `watch_completion()` is where `Active → {Complete,Failed,Dormant}` transitions happen and `StateChange` is published. The supervisor subscribes to that bus event; no agent_manager change is required for entry.
- `src/daemon/agent_manager.rs:391-490` — `invoke()` resumes a `Dormant` agent. Restart dispatch reuses the queue lane, not `invoke()` — the supervisor re-enqueues the failed agent's session via a new `restart_dispatch()` helper that mirrors `dispatch_internal`.
- `src/daemon/agent_manager.rs:492-571` — `banish()` cascade: queues retire, processes die, wake sources retire (`:497-501`). Add `supervisor.cancel_pending(id)` in the same cascade.
- `src/daemon/scheduler.rs:120-214` — `tick_now()` runs `tick_mail_wake()` then queue dispatch. New `tick_supervision()` step lands between them, gated by capacity.
- `src/daemon/wake_registry.rs:106-191` — `WakeRegistry` is the structural template. Same shape: `Arc<Self>`, `Mutex<HashMap<...>>`, `mpsc` channel + drain loop, `replay_on_boot()`, integration with `Clock`.
- `src/daemon/clock.rs` — `Clock` trait + `SystemClock` + `TestClock`. Reused unchanged; the `Supervisor` takes `Arc<dyn Clock>`.
- `src/daemon/event_bus.rs` — Subscribe via `EventBus::subscribe()`. The supervisor's actor task is a long-lived `tokio::spawn` consuming a `broadcast::Receiver<StreamEvent>` filtered for `StateChange { new_state: Failed }`.
- `src/daemon/persistence.rs:79-273` — Migration pattern (`CREATE TABLE IF NOT EXISTS` + column-existence-probed `ALTER TABLE`). New `restart_history` table and five new `agents` columns land in `migrate()`.
- `src/daemon/persistence.rs:1193-…` — `migrate_dormant_agents()` is the reference for boot-time event-emitting migrations. The supervisor's `replay_pending_on_boot()` follows the same shape.
- `src/shared/protocol.rs:309-472` — `StreamEvent` enum. Four new variants land here with matching `kind()` / `agent_id()` / `serde rename`.
- `src/shared/protocol.rs:50-91` — `SummonParams` / `SummonResult` and `MailSendParams`. New optional supervision fields on `SummonParams`; `MailSendParams` reserved-prefix guard.
- `src/shared/mail.rs:45-94` — `parse_address()` accepts `agent://` and `topic://`. The reserved-prefix guard for `supervisor://` lives at the `mail.send` RPC boundary, not in the parser (`supervisor://` is a sender, not a destination).
- `src/daemon/scroll_keeper.rs:53-75` — `StateChange` match. Add an explicit `AgentState::Restarting` no-op arm so dependent tasks do not fire while a task is mid-retry.
- `src/daemon/server.rs` / `src/daemon/mod.rs:53-107` — Daemon boot wiring. The supervisor instantiates after the wake registry and before `server::run`, and `manager.set_supervisor(supervisor)` mirrors `set_wake_registry`.
- `src/cli/commands/summon.rs`, `src/main.rs:19-40,195-196` — CLI surface for `grim summon`. New flags follow the `--keep-alive` pattern.
- `src/cli/formatters.rs` — `circle` / `status` table rendering. Add a `restart_count` / `policy` column.
- `tests/scheduler_mail_wake.rs` — Test seam pattern (`RecordingWaker`, `DbLookup`, `NoopDispatcher`). New supervision tests follow this shape.

### Existing Patterns to Follow

- **Peer-actor pattern** (`wake_registry.rs:106-150`) — `Arc<Self>` constructor, `Mutex<HashMap>` for in-memory handles, `mpsc::Sender<FireMsg>` for cross-task fire requests, `spawn()` returns the drain `JoinHandle`. Apply directly to `Supervisor`.
- **Boot-time replay** (`wake_registry.rs:407-454`) — `replay_on_boot(&Arc<Self>)` reads persisted rows, re-arms in-memory state, fires once for any source whose deadline elapsed during downtime. Apply directly to supervisor pending-restart replay.
- **Banish cascade** (`agent_manager.rs:492-504`) — `banish()` calls inner first, then on success cascades to subsystems. Errors logged but do not fail the banish.
- **Migration shape** (`persistence.rs:79-273`) — `CREATE TABLE IF NOT EXISTS` + index creation in the main `migrate()` block; column probes (`SELECT col FROM table LIMIT 0`) gate `ALTER TABLE`. Both reach the same final schema for fresh DBs and upgraded DBs.
- **Event-emitting boot migration** (`mod.rs:55-81`) — Capture migrated IDs before `EventBus` is constructed so `StateChange` events publish on the live bus and persist via the writer task. Restart catch-up at boot follows this pattern: collect, then publish.
- **`Clock` injection** (`wake_registry.rs:106-141`) — `Arc<dyn Clock>` constructor parameter; production wires `SystemClock`, tests wire `TestClock`. Both restart-window evaluation and the daemon-wide rate counter consume the clock.
- **Recording test seams** (`tests/scheduler_mail_wake.rs:31-72`) — `Mutex<Vec<...>>` mocks for inspecting calls. Supervision tests substitute a `RecordingDispatcher` and a `RecordingMailSender`.
- **`is_*` predicates split** (`types.rs:64-80`) — `is_terminal` for slot accounting, `is_final` for lifecycle. Add `is_supervisable` (Failed only) so the supervisor's `on_state_change` filter is centralized.

### Key Dependencies

- **`chrono`** — already in `Cargo.toml`; `Duration::seconds(2)` for the restart delay, `Utc::now()` via `Clock` for the window evaluator.
- **`tokio::sync::broadcast`** — existing `EventBus` channel. Supervisor subscribes once in `spawn()`.
- **`tokio::sync::mpsc`** — for the pending-restart fire channel (mirrors `WakeRegistry::fire_tx`).
- **`rusqlite`** — bundled SQLite. New `restart_history` table + five new `agents` columns; both add inside the existing `migrate()` block.
- **`anyhow`** — existing error type for the supervisor's public surface.

### Ambiguity Resolutions

| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| Daemon-wide restart rate cap default | Plan flagged as open question | 30 restarts/min, configurable via `tome` (`daemon.restart_rate_per_min`); applies across all agents | User |
| Tree-depth cap default | Plan flagged as open question | 3 levels (matches plan's working number); not configurable in v1 | User |
| Behavior when daemon-wide rate cap exceeded | Plan flagged as open question | Delay individual restart by 60s and re-check; supervisor re-queues into pending heap with new fire deadline; emits `RestartScheduled { rate_limited: true }` | User |
| Restart fixed delay | Plan stated 2s | Exactly `Duration::seconds(2)` between `Failed → dispatch`; not configurable in v1 | Plan |
| `restart_count` field semantics | Plan said "lifetime; restart_history is the windowed truth" | `agents.restart_count` is a denormalized lifetime counter (UI convenience). The window evaluator queries `restart_history WHERE agent_id = ? AND attempted_at >= now - window_secs` for the budget gate; `restart_count` never gates a decision | Plan |
| `escalation_depth` propagation | Plan said "incremented when this agent's escalation triggers a restart on the recipient" | When supervisor sends escalation mail, the resulting wake/dispatch on the recipient (if any) inherits the failed agent's `escalation_depth + 1` and writes that value to the recipient's `agents.escalation_depth` column. Cap test: `if escalation_depth + 1 > 3 → refuse, fire RestartBudgetExhausted{reason: tree_depth_exceeded}` instead of escalating | Plan |
| Escalation sender format | Plan said `supervisor://<failed-agent-id>` | Mail row `sender_id` literally `supervisor://<failed-agent-id>`; not parsed via `parse_address` (it's a synthetic sender, like `wake://<wake-id>`); the `mail.send` RPC handler rejects user-supplied senders matching `^(supervisor|wake)://` (`reserved_sender_prefix` error) | Plan + parity with wake |
| Escalation payload | Plan said "agent id + last error message" | Body format: `[supervisor] agent <id> failed (budget exhausted): <error_summary>` truncated at the existing 16 KiB wake-fold cap; `error_summary` sourced from the agent's `exit_code`/last `MonitorResult.error_reason` if present, else `"unknown"` | Plan + scheduler.rs:34 |
| `max_restarts = 0` validation | Plan said reject at summon | RPC `agent.summon` rejects with `invalid_supervision: max_restarts_zero` if `restart_policy != "never"` and `max_restarts == 0` | Plan |
| `restart_policy = never` with `--escalate-to` | Plan said reject at summon | RPC rejects with `invalid_supervision: escalate_requires_policy` | Plan |
| `--escalate-to agent://<self>` | Plan flagged as edge | RPC rejects with `invalid_supervision: self_escalation` (compare to `params.id` after enqueue assigns the agent_id; rejected before insert) | Plan |
| Restart on a transient `Restarting` agent | Plan said idempotent | `Supervisor::on_state_change` gates on `state == Failed && agent.is_supervisable()`; a duplicate `Failed` event for an already-`Restarting` agent is dropped (no-op log at debug) | Plan |
| Crash-mid-dispatch recovery | Plan said "promote `Restarting` to `Failed` on boot" | At boot, before the supervisor spawns: `UPDATE agents SET state = 'failed' WHERE state = 'restarting'` (idempotent), capturing IDs to publish `StateChange { Restarting → Failed }` events. Then `replay_pending_on_boot` reconciles | Plan |
| Crash-after-escalation | Plan said "do not re-escalate" | `restart_history` row with `outcome = 'budget_exhausted'` AND `Escalated` event in `events` table for the same `agent_id` since the row's `attempted_at` → boot replay treats budget as spent, no fresh restart queued | Plan |
| Catch-up restart delay on boot | Plan said "delay = 0 (window has elapsed)" | If a pending restart's original deadline (`attempted_at + 2s`) is already past at boot, fire immediately on the first scheduler tick. Otherwise honor remaining delay | Plan |
| Two `Failed` events <2s apart for same agent | Plan said no-op the second | Second `Failed` finds agent in `Restarting` (set when first decision queued); `is_supervisable()` returns false; supervisor logs and drops | Plan |
| Topic with zero subscribers | Plan said "Escalated event still fires" | `mail.send` writes 0 mail rows; supervisor still publishes `Escalated { agent_id, target, fanout_count: 0 }` | Plan |
| Banish during `Restarting` | Plan said banish wins | `banish_inner` adds a `Restarting` arm: cancel pending via `supervisor.cancel_pending(id)`, flip state to `Banished`, publish `StateChange { Restarting → Banished }`. Cascade then retires wake sources | Plan |
| Restart preserves session | Plan said "same agent id, same session" | Restart dispatch passes `resume_session_id = agent.session_id` to `ExecuteRequest`; if session is None (agent never produced one before failing), restart enqueues a fresh-task dispatch using `agent.task` | Plan + agent_manager.rs:322-329 |
| Slot accounting for `Restarting` | Plan said "non-terminal, slot-free" | `is_terminal()` returns `true` for `Restarting` (slot-free); `is_final()` returns `false` (lifecycle in flight). Mirrors the `Dormant` decision at `types.rs:64-80` | Plan |
| `--max-restarts` flag format | Plan showed `3/60s` | CLI parses as `<N>/<T>s` (regex `^(\d+)/(\d+)s$`); RPC accepts `max_restarts: u32` and `restart_window_secs: u32` as separate fields. CLI rejects malformed input before RPC | Plan |
| `restart_window_secs` upper bound | Plan said document a recommended max of 7d | CLI / RPC accepts up to 604800s (7 days); rejects anything larger with `invalid_supervision: window_too_large` | Plan |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | `Restarting` state + schema + `is_supervisable` + `StreamEvent` variants | None | High |
| 2 | `Supervisor` actor: policy evaluation, budget, rate counter, depth cap | 1 | High |
| 3 | Scheduler integration: `tick_supervision()` + restart dispatch path | 2 | Medium |
| 4 | Escalation: mail send + reserved-prefix guard + depth propagation | 2 | Medium |
| 5 | Banish cascade + crash recovery (`replay_pending_on_boot`) | 2, 3 | Medium |
| 6 | CLI surface: `--restart`, `--max-restarts`, `--escalate-to` + circle/status display | 1, 4 | Medium |
| 7 | Scroll-keeper integration: `Restarting` no-op arm + dependent-task gating | 1 | Low |

### Critical Path

```
T1 (state + schema + events) ──► T2 (Supervisor actor) ──┬──► T3 (scheduler tick) ──► T5 (banish + boot replay)
                                                          ├──► T4 (escalation mail)
                                                          │       │
                                                          │       └──► T6 (CLI summon flags + display)
                                                          │
                                                          └──► T7 (scroll-keeper arm) [parallel; needs T1 only]
```

T1 is the foundation — every match on `AgentState` widens. T2 unblocks T3 / T4 / T5; T7 only needs T1 and can land in parallel. T6 needs T4 (`--escalate-to` validation requires the address-resolution code from T4 to live).

---

### Task 1: `Restarting` state + schema + `is_supervisable` + `StreamEvent` variants

**Summary:** Add `AgentState::Restarting` (slot-free, non-final), add `is_supervisable()`, add the `restart_history` table and five new `agents` columns via the existing migration pattern, and add four new `StreamEvent` variants.

**Dependencies:** None

**Files to create/modify:**
- `src/shared/types.rs` — Add `Restarting` to `AgentState`; extend the `impl_state_enum!` invocation; update `is_terminal()` (returns `true` for `Restarting`); update `is_final()` (returns `false`); add `is_supervisable()` (returns `true` for `Failed` only). Add `RestartPolicy` enum (`Never | OnFailure`) with `impl_state_enum!`. Add `RestartHistoryOutcome` enum (`Scheduled | Succeeded | FailedAgain | BudgetExhausted`). Add `SupervisionConfig` struct (policy, max_restarts, window_secs, escalate_to).
- `src/daemon/persistence.rs` — In `migrate()`, append `CREATE TABLE IF NOT EXISTS restart_history (...)` plus indexes; add five `ALTER TABLE agents ADD COLUMN` migrations gated on column-existence probes (matches the `keep_alive` block at `:260-270`). Add CRUD methods: `insert_restart_history_row`, `count_restarts_in_window(agent_id, window_start)`, `get_supervision(agent_id) -> Option<SupervisionConfig>`, `set_supervision(agent_id, config)`, `bump_restart_count(agent_id)`, `get_escalation_depth(agent_id) -> u32`, `set_escalation_depth(agent_id, u32)`, `list_failed_with_active_policy() -> Vec<AgentId>` (boot replay helper), `mark_torn_restarting_as_failed() -> Vec<AgentId>` (boot crash recovery).
- `src/shared/protocol.rs` — Add `StreamEvent::RestartScheduled { agent_id, attempt, max, fire_at_unix, rate_limited }`, `Restarted { agent_id, attempt, mail_id: Option<String> }`, `RestartBudgetExhausted { agent_id, reason }`, `Escalated { agent_id, target, fanout_count }`. Add matching arms to `kind()` and `agent_id()`. Add the four serde renames.
- `src/cli/formatters.rs` — Render `Restarting` as `"restarting"` in `circle` / `status` output. (Display column added in T6.)

**Detailed specification:**

1. **`AgentState::Restarting`**
   - Serde rename: `"restarting"`. Display: `"restarting"`. `FromStr` accepts `"restarting"`.
   - `is_terminal(&self) -> bool` — `true` for `Complete | Failed | Banished | Dormant | Restarting`. (Same slot accounting as `Dormant`.)
   - `is_final(&self) -> bool` — unchanged: `Complete | Failed | Banished` only.
   - `is_supervisable(&self) -> bool` — new — `matches!(self, Self::Failed)`.

2. **`restart_history` schema** (lives inside `migrate()` `execute_batch`):
   ```sql
   CREATE TABLE IF NOT EXISTS restart_history (
       id              INTEGER PRIMARY KEY AUTOINCREMENT,
       agent_id        TEXT NOT NULL REFERENCES agents(id),
       attempted_at    INTEGER NOT NULL,             -- unix seconds
       outcome         TEXT NOT NULL,                -- 'scheduled'|'succeeded'|'failed_again'|'budget_exhausted'
       error_summary   TEXT
   );
   CREATE INDEX IF NOT EXISTS restart_history_by_agent_window
       ON restart_history(agent_id, attempted_at);
   CREATE INDEX IF NOT EXISTS restart_history_by_time
       ON restart_history(attempted_at);
   ```

3. **`agents` column additions** (each gated on a `prepare("SELECT <col> FROM agents LIMIT 0")` probe, matching `keep_alive` at `persistence.rs:263-270`):
   - `restart_policy TEXT NOT NULL DEFAULT 'never'`
   - `max_restarts INTEGER`
   - `restart_window_secs INTEGER`
   - `escalate_to TEXT`
   - `restart_count INTEGER NOT NULL DEFAULT 0`
   - `escalation_depth INTEGER NOT NULL DEFAULT 0`

4. **`StreamEvent` variants:**
   ```rust
   #[serde(rename = "restart_scheduled")]
   RestartScheduled { agent_id: AgentId, attempt: u32, max: u32, fire_at_unix: i64, rate_limited: bool },
   #[serde(rename = "restarted")]
   Restarted { agent_id: AgentId, attempt: u32, mail_id: Option<String> },
   #[serde(rename = "restart_budget_exhausted")]
   RestartBudgetExhausted { agent_id: AgentId, reason: String },  // "budget_spent" | "tree_depth_exceeded"
   #[serde(rename = "escalated")]
   Escalated { agent_id: AgentId, target: String, fanout_count: u32 },
   ```
   Each variant returns the agent's id from `agent_id()` and a stable kind in `kind()`.

5. **Window-count query:**
   ```sql
   SELECT COUNT(*) FROM restart_history
   WHERE agent_id = ?1 AND attempted_at >= ?2
   ```
   Called by `Supervisor::evaluate` with `?2 = now - restart_window_secs`. Counts `outcome IN ('scheduled', 'failed_again')` only — `succeeded` and `budget_exhausted` rows are excluded so the window measures *active* retries, not history.

**Edge cases to handle:**
- Existing DBs without the new columns: idempotent `ALTER TABLE` migration. Defaults make every existing agent equivalent to `restart_policy = never`.
- `SupervisionConfig` round-trip: `policy = Never` AND any of (`max_restarts`, `escalate_to`) set is invalid; the type's constructor rejects it.
- `match` on `AgentState` with no `_` arm — every callsite in `agent_manager.rs`, `scheduler.rs`, `scroll_keeper.rs`, `rpc.rs`, `formatters.rs` adds an explicit `Restarting` arm. Run `rg "match.*AgentState" src/ tests/` and audit.

**Acceptance criteria:**
- [ ] `AgentState::Restarting` exists, serializes as `"restarting"`, parses from `"restarting"`, displays as `"restarting"`.
- [ ] `AgentState::Restarting.is_terminal() == true`; `AgentState::Restarting.is_final() == false`.
- [ ] `AgentState::Failed.is_supervisable() == true`; every other variant returns `false`.
- [ ] `AgentState` `is_terminal` returns `true` for the full set `{Complete, Failed, Banished, Dormant, Restarting}`.
- [ ] `Database::migrate()` creates the `restart_history` table with both indexes; running it twice is a no-op.
- [ ] After migration, the `agents` table has columns: `restart_policy`, `max_restarts`, `restart_window_secs`, `escalate_to`, `restart_count`, `escalation_depth`. Defaults match the spec (policy `'never'`, ints `0` for the non-nullable counters).
- [ ] `Database::set_supervision(agent_id, cfg)` followed by `get_supervision(agent_id)` returns the same config (round-trip).
- [ ] `Database::count_restarts_in_window(id, window_start)` returns the count of rows with `attempted_at >= window_start` and `outcome IN ('scheduled','failed_again')`.
- [ ] `Database::insert_restart_history_row` writes with `attempted_at` = caller-supplied unix seconds.
- [ ] `Database::list_failed_with_active_policy()` returns IDs where `state = 'failed' AND restart_policy != 'never'`.
- [ ] `Database::mark_torn_restarting_as_failed()` flips any `state = 'restarting'` rows to `'failed'` and returns their IDs.
- [ ] `StreamEvent::RestartScheduled`, `Restarted`, `RestartBudgetExhausted`, `Escalated` exist; `kind()` returns `"restart_scheduled"`, `"restarted"`, `"restart_budget_exhausted"`, `"escalated"` respectively; `agent_id()` returns `Some(<id>)` for each.
- [ ] `cargo check` and `cargo clippy` pass with no non-exhaustive-match warnings across the workspace.

**Contract tests (RED phase):**
- Test file: `tests/supervision_state.rs` (new)
- Tests to write before implementing:
  - `restarting_is_terminal_not_final` — asserts `is_terminal == true` and `is_final == false` for `Restarting`.
  - `is_supervisable_only_failed` — every variant returns `false` except `Failed`.
  - `restarting_serde_roundtrip` — `serde_json` round-trip via `"restarting"` string.
- Test file: `tests/supervision_schema.rs` (new)
- Tests to write before implementing:
  - `migration_adds_restart_history_table` — open in-memory DB, verify `restart_history` table and both indexes exist via `pragma_table_info`.
  - `migration_adds_supervision_columns` — verify all six `agents` columns exist with stated defaults.
  - `migration_is_idempotent` — call `migrate()` twice; second call is a no-op (no error, no schema change).
  - `set_get_supervision_round_trips` — write a `SupervisionConfig`, read it back, assert equality.
  - `count_restarts_in_window_filters_by_outcome` — insert four rows (one per outcome) all in window; assert count returns 2 (`scheduled` + `failed_again` only).
  - `count_restarts_in_window_filters_by_time` — insert two rows with `attempted_at` outside window, two inside; assert count returns 2.
  - `list_failed_with_active_policy_excludes_never` — seed three agents (Failed+Never, Failed+OnFailure, Active+OnFailure); assert only the second is returned.
  - `mark_torn_restarting_as_failed_returns_ids_and_flips_state` — seed one Restarting agent; call helper; assert returned `[id]` and DB state == `'failed'`.
- Test file: `tests/supervision_events.rs` (new)
- Tests to write before implementing:
  - `restart_scheduled_event_kind_and_id` — instantiate variant; assert `kind()` and `agent_id()`.
  - `escalated_event_kind_and_id` — same shape.
  - `restart_budget_exhausted_event_kind_and_id` — same shape.
  - `restarted_event_kind_and_id` — same shape.

**Non-testable items:**
- The audit of every `match AgentState` callsite is enforced by `cargo check`, not contract tests. Specifically: `formatters.rs`, `scheduler.rs:265`, `agent_manager.rs:512-571`, `scroll_keeper.rs:60-65`, `rpc.rs` guards.

**Notes/Warnings:**
- Existing `tests/scheduler_mail_wake.rs` and `tests/dormant_state.rs` exhaustively match `AgentState`; they need a `Restarting => panic!("unexpected")` arm or similar in their fixtures.
- The `RestartPolicy` and `RestartHistoryOutcome` enums use `impl_state_enum!`; both must be in `types.rs` for the macro to apply.

---

### Task 2: `Supervisor` actor: policy evaluation, budget, rate counter, depth cap

**Summary:** Add the daemon-internal `Supervisor` actor — peer of `WakeRegistry` and `Scheduler` — that subscribes to `StateChange { Restarting | Failed }`, evaluates restart policy + windowed budget + global rate cap + tree-depth cap, persists decisions to `restart_history`, and either queues a delayed restart or fires `RestartBudgetExhausted`.

**Dependencies:** Task 1

**Files to create/modify:**
- `src/daemon/supervisor.rs` (new) — `Supervisor` struct + `RestartDecision` enum + `PendingRestart` struct + `RateCounter` helper. `pub fn new(db, bus, clock, config) -> Arc<Self>`; `pub fn spawn(self: &Arc<Self>) -> JoinHandle<()>` (subscribes to bus, kicks off drain loop). `pub async fn on_state_change(&self, agent_id: &str, new: AgentState)`. `pub async fn evaluate(&self, agent_id: &str) -> RestartDecision`. `pub async fn schedule_restart(&self, agent_id, attempt, fire_at)`. `pub async fn cancel_pending(&self, agent_id) -> usize`. `pub fn drain_due(&self, now: DateTime<Utc>) -> Vec<PendingRestart>` (called by scheduler in T3).
- `src/daemon/mod.rs` — `pub mod supervisor;` and the boot wiring (instantiate after wake registry, call `replay_pending_on_boot()` then `spawn()`).
- `src/shared/config.rs` — Add `daemon.restart_rate_per_min: u32` (default 30) and `daemon.tree_depth_cap: u32` (default 3, not user-configurable in v1 — read-only constant exposed via config for tests).

**Detailed specification:**

1. **Struct shape** (mirrors `WakeRegistry`):
   ```rust
   pub struct Supervisor {
       db: Arc<Database>,
       bus: EventBus,
       clock: Arc<dyn Clock>,
       pending: Mutex<BinaryHeap<PendingRestart>>,    // min-heap by fire_at
       global_rate: Mutex<RateCounter>,               // 30/min default
       tree_depth_cap: u32,                           // 3
       restart_delay: chrono::Duration,               // 2s
   }

   pub struct PendingRestart { pub agent_id: AgentId, pub attempt: u32, pub fire_at: DateTime<Utc> }

   pub enum RestartDecision {
       Restart { attempt: u32, fire_at: DateTime<Utc>, rate_limited: bool },
       BudgetExhausted { reason: &'static str },     // "budget_spent" | "tree_depth_exceeded"
       NotSupervised,                                // policy = never, or no policy
   }
   ```

2. **`on_state_change` filter:**
   - Skips events where `!new_state.is_supervisable()` (i.e. anything but `Failed`).
   - Skips agents already in the pending heap (idempotency for double-`Failed` events).
   - Skips agents whose policy is `Never` (explicit no-op).
   - For supervisable agents, calls `evaluate()` and routes the decision.

3. **`evaluate` policy:**
   - Reads `SupervisionConfig` via `db.get_supervision(agent_id)`.
   - If `policy == Never` → `NotSupervised`.
   - Reads `escalation_depth`. If `escalation_depth + 1 > tree_depth_cap` → `BudgetExhausted { reason: "tree_depth_exceeded" }`.
   - Reads window count: `count_restarts_in_window(id, now - window_secs)`. If `count >= max_restarts` → `BudgetExhausted { reason: "budget_spent" }`.
   - Else `attempt = count + 1`; consult `global_rate` (token bucket: `30 / 60` per second, capacity 30). On accept: `fire_at = now + 2s`, `rate_limited = false`. On deny: `fire_at = now + 60s`, `rate_limited = true`. Either way return `Restart`.

4. **`schedule_restart`:**
   - Inserts `restart_history` row with `outcome = 'scheduled'`, `error_summary = <agent's last error>`.
   - Pushes `PendingRestart` onto the heap.
   - Updates `agents.state` to `Restarting`, publishes `StateChange { Failed → Restarting }`.
   - Publishes `StreamEvent::RestartScheduled { agent_id, attempt, max, fire_at_unix, rate_limited }`.

5. **`cancel_pending(agent_id)` → `usize`** — removes all entries for this agent from the heap (rebuild from filtered iterator) and returns the count cancelled.

6. **`drain_due(now)`** — pops every entry with `fire_at <= now`, returns them. The scheduler T3 dispatches them one by one under capacity.

7. **Bus subscription loop** — `spawn()` task subscribes to `EventBus` via `subscribe()`; on each `StateChange`, calls `on_state_change`. Lagged-receiver branch: drain best-effort, log warn, do not panic. Closed-receiver branch: exit task.

**Edge cases to handle:**
- Two `Failed` events for the same agent <2s apart: first transitions agent to `Restarting`; second's `is_supervisable` filter excludes it.
- Agent fails outside its restart window: `count_restarts_in_window` returns rows from the *new* window starting at `now - window_secs`, so prior restarts naturally age out.
- `agents.escalation_depth + 1 > cap` AND no remaining budget: tree-depth check runs *first*; emit `RestartBudgetExhausted { reason: "tree_depth_exceeded" }` (single reason, not two).
- Clock skew: `RateCounter` uses the same `Clock` seam; backwards jumps clamp to `elapsed = 0` (matches `wake_registry.rs:387-403`).
- `evaluate` called after `cancel_pending` on a banished agent: `db.get_supervision` returns `None` (banish cleared policy); decision is `NotSupervised`.

**Acceptance criteria:**
- [ ] `Supervisor::new` returns `Arc<Self>` and accepts `db`, `bus`, `clock`, `restart_rate_per_min`, `tree_depth_cap` parameters.
- [ ] On `StateChange { new_state: Failed }` for an agent with `policy = OnFailure` and budget remaining, `Supervisor` writes a `restart_history` row with `outcome = 'scheduled'`, transitions the agent to `Restarting`, and pushes a `PendingRestart` onto the heap.
- [ ] On `StateChange { new_state: Failed }` for an agent with `policy = Never`, supervisor is a no-op (no row, no state change, no event).
- [ ] On `StateChange { new_state: Failed }` when window count `>= max_restarts`, supervisor writes a `restart_history` row with `outcome = 'budget_exhausted'`, leaves agent in `Failed`, and publishes `StreamEvent::RestartBudgetExhausted { reason: "budget_spent" }`.
- [ ] On `StateChange { new_state: Failed }` for an agent with `escalation_depth >= tree_depth_cap`, supervisor publishes `RestartBudgetExhausted { reason: "tree_depth_exceeded" }`. The tree-depth check fires before the budget check.
- [ ] When the global rate cap is full, `evaluate` returns `Restart { rate_limited: true, fire_at = now + 60s }` and the published `RestartScheduled` event has `rate_limited: true`.
- [ ] When the global rate cap has tokens, `evaluate` returns `Restart { rate_limited: false, fire_at = now + 2s }`.
- [ ] `Supervisor::cancel_pending(agent_id)` removes all pending entries for that agent and returns the number removed.
- [ ] `Supervisor::drain_due(now)` returns and removes every `PendingRestart` with `fire_at <= now`; entries with future `fire_at` remain.
- [ ] Two `Failed` events <2s apart for the same agent produce exactly one `RestartScheduled` event (idempotency).
- [ ] `Supervisor::evaluate` is deterministic under `TestClock` — advancing the clock by `window_secs` fully resets the budget.

**Contract tests (RED phase):**
- Test file: `tests/supervisor_evaluate.rs` (new)
- Tests to write before implementing:
  - `evaluate_policy_never_returns_not_supervised` — seed agent with `policy = Never`; assert decision is `NotSupervised`.
  - `evaluate_with_budget_returns_restart_at_2s` — seed `OnFailure, max 3, window 60`; under `TestClock`; assert `Restart { fire_at = now + 2s, rate_limited = false }`.
  - `evaluate_at_budget_returns_budget_exhausted` — seed three `scheduled` history rows; assert `BudgetExhausted { "budget_spent" }`.
  - `evaluate_window_slides_per_failure` — seed history rows with `attempted_at` 70s ago; advance clock; assert budget reset.
  - `evaluate_tree_depth_exceeded_takes_precedence` — seed `escalation_depth = 3`, no history; assert `BudgetExhausted { "tree_depth_exceeded" }` regardless of remaining budget.
  - `evaluate_rate_limited_delays_60s` — fill global bucket; assert `Restart { fire_at = now + 60s, rate_limited = true }`.
- Test file: `tests/supervisor_actor.rs` (new)
- Tests to write before implementing:
  - `failed_event_writes_history_row_and_flips_to_restarting` — publish `StateChange{Active→Failed}`; assert row with `outcome=scheduled`, agent state == `Restarting`, event `RestartScheduled` published.
  - `policy_never_is_silent` — same scenario but `policy=Never`; assert no row, no state change, no event.
  - `duplicate_failed_for_restarting_agent_is_noop` — agent already `Restarting`; publish another `Failed`; assert exactly one `RestartScheduled` event total.
  - `budget_exhausted_publishes_event_and_leaves_agent_failed` — fill window with 3 prior scheduled rows; publish new `Failed`; assert state stays `Failed`, event `RestartBudgetExhausted{budget_spent}`.
  - `cancel_pending_removes_entries` — schedule 2 restarts for agent A and 1 for agent B; cancel A; assert 1 remains and the count returned is 2.
  - `drain_due_pops_only_due_entries` — schedule restarts at `now+2s` and `now+10s`; advance clock 5s; `drain_due` returns the first only.

**Non-testable items:**
- The bus-subscription `spawn` loop is wired in T2 but exercised by integration tests in T3 (real scheduler tick).

**Notes/Warnings:**
- The `BinaryHeap<PendingRestart>` needs a `Reverse`-wrapped comparator so the heap acts as a min-heap by `fire_at`.
- `RateCounter` mirrors `WakeRegistry::consume_token` (`wake_registry.rs:387-403`) but lives entirely in-memory — restart rate is ephemeral, no DB persistence.
- `Supervisor` does NOT spawn agent processes itself. It only marks state, persists decisions, and signals the scheduler. T3 connects it to dispatch.

---

### Task 3: Scheduler integration: `tick_supervision()` + restart dispatch path

**Summary:** Add `tick_supervision()` between `tick_mail_wake()` and the queue dispatch loop in `Scheduler::tick_now`. Pull due restarts via `supervisor.drain_due(now)`, dispatch each through a new `restart_dispatch()` agent-manager method that resumes the agent's session under the same capacity cap.

**Dependencies:** Task 2

**Files to create/modify:**
- `src/daemon/scheduler.rs` — Add `Option<Arc<Supervisor>>` field; add `with_supervision(self, sup) -> Self` builder; add `async fn tick_supervision(&self, mut in_flight: usize, cap: usize) -> Result<usize>` (mirrors `tick_mail_wake` shape at `:273-333`); call it between line 135 (`tick_mail_wake`) and the cap check at line 136. Update `should_wake` to wake on `RestartScheduled` events.
- `src/daemon/agent_manager.rs` — Add `pub(crate) async fn restart_dispatch(self: &Arc<Self>, agent_id: &str, attempt: u32) -> Result<()>` that mirrors `dispatch_internal` (`:315-389`) but reads the agent's persisted task + session_id, builds an `ExecuteRequest` with `resume_session_id = agent.session_id`, transitions `Restarting → Active`, publishes `StateChange { Restarting → Active }` and `StreamEvent::Restarted { agent_id, attempt, mail_id: None }`, and writes a `restart_history` row update (`outcome` flips `scheduled → succeeded` only after the agent reaches `Complete` — see watch_completion changes below).
- `src/daemon/agent_manager.rs:152-217` — Update `watch_completion`: on `Active → Complete` for an agent with a `scheduled` history row whose `outcome` is still `scheduled`, set the latest row's outcome to `succeeded`. On `Active → Failed` with the same row, set outcome to `failed_again` (which counts toward the next window evaluation).
- `src/daemon/server.rs` — Wire `scheduler = scheduler.with_supervision(supervisor.clone())` at the existing scheduler-construction site.
- `src/daemon/supervisor.rs` — Add a `pub trait RestartDispatcher: Send + Sync { async fn restart_dispatch(&self, agent_id: &str, attempt: u32) -> Result<()>; }` for the scheduler's seam. `AgentManager` implements it.

**Detailed specification:**

1. **`tick_supervision` flow:**
   ```
   if supervisor is None or in_flight >= cap: return Ok(in_flight)
   let due = supervisor.drain_due(now)
   for entry in due:
       if in_flight >= cap:
           supervisor.requeue(entry)         // push back; will fire next tick
           break
       agent_manager.restart_dispatch(entry.agent_id, entry.attempt).await?
       in_flight += 1
   ```

2. **Restart dispatch (`AgentManager::restart_dispatch`):**
   - Reads agent row; rejects if state is not `Restarting` (raced with banish).
   - Reads `agent.task` and `agent.session_id`.
   - Builds `ExecuteRequest { agent_id, task, provider_name, cwd, model, resume_session_id: agent.session_id }`. If `session_id` is `None`, `resume_session_id` is `None` and the request runs as a fresh dispatch (rare path: agent failed before producing a session).
   - `executor.start(req).await?` returns a handle; agent state flips `Restarting → Active`; `StateChange` published.
   - `bus.publish(StreamEvent::Restarted { agent_id, attempt, mail_id: None })`.

3. **`watch_completion` history reconciliation:**
   - On `MonitorResult { state: Complete }` for an agent whose latest `restart_history` row has `outcome = 'scheduled'`, update that row to `succeeded` and bump `agents.restart_count`.
   - On `MonitorResult { state: Failed }` for the same scenario, update to `failed_again`. The supervisor's bus subscription will then re-evaluate via the same `Failed` path; the next scheduling decision sees the prior `failed_again` row counted in the window.

4. **`should_wake` extension** (`scheduler.rs:261-269`): also returns `true` for `StreamEvent::RestartScheduled { .. }`. This guarantees `tick_now()` runs immediately after a supervisor decision, even if no other event interleaves.

**Edge cases to handle:**
- Capacity full at `tick_supervision`: due entries are pushed back into the supervisor's heap (preserving `fire_at`); next signal re-attempts.
- Agent state changed from `Restarting` to `Banished` between `drain_due` and `restart_dispatch`: dispatch returns an error; supervisor logs warn; nothing else.
- `executor.start` fails (worker disappeared): same shape as `dispatch_internal` failure — log error, leave agent in `Restarting`. The drift will be reconciled on next supervisor tick (still sees `Failed` is no longer the state, treats as no-op). Acceptable for v1; refinement deferred.
- Capacity is 0: `tick_now` returns early before `tick_mail_wake` or `tick_supervision` runs; pending restarts wait for capacity to open.

**Acceptance criteria:**
- [ ] `Scheduler::with_supervision(Arc<Supervisor>)` returns a builder; without it, `tick_supervision` is silently skipped.
- [ ] On `tick_now()` with one due restart and free capacity, `AgentManager::restart_dispatch` is invoked exactly once.
- [ ] After `restart_dispatch`, the agent's state is `Active` and a `StateChange { Restarting → Active }` event is published.
- [ ] After `restart_dispatch`, exactly one `StreamEvent::Restarted` event is published with the supplied `attempt`.
- [ ] If capacity is full, `tick_supervision` requeues the popped entry without dispatching; `supervisor.drain_due(now)` on the next call returns the same entry.
- [ ] `watch_completion` updates the latest `restart_history` row's `outcome` from `scheduled` to `succeeded` when the agent reaches `Complete`, and bumps `agents.restart_count` by 1.
- [ ] `watch_completion` updates the same row's `outcome` to `failed_again` when the agent reaches `Failed` (not `Complete`).
- [ ] `Scheduler::should_wake(StreamEvent::RestartScheduled { .. })` returns `true`.
- [ ] Restart dispatch passes `resume_session_id = agent.session_id` to `ExecuteRequest` (verified via a fake executor that records the request).
- [ ] If the agent is in state other than `Restarting` at `restart_dispatch` entry, the call returns `Err` and no executor is started.

**Contract tests (RED phase):**
- Test file: `tests/supervisor_dispatch.rs` (new)
- Tests to write before implementing:
  - `tick_supervision_dispatches_due_restart` — seed Restarting agent + pending heap entry with `fire_at = now`; tick scheduler; assert `restart_dispatch` invoked once via `RecordingDispatcher`.
  - `tick_supervision_respects_capacity` — set `cap = 1`, in-flight `1`; assert no dispatch and entry remains in heap.
  - `restart_dispatch_passes_session_id` — fake executor records `ExecuteRequest`; assert `resume_session_id == Some(<seeded>)`.
  - `restart_dispatch_emits_restarted_event` — record bus events; assert exactly one `Restarted` with the given `attempt`.
  - `restart_dispatch_rejects_non_restarting_state` — set agent to `Banished`; call `restart_dispatch`; assert error; assert no executor invocation.
- Test file: `tests/supervisor_history_reconcile.rs` (new)
- Tests to write before implementing:
  - `complete_after_restart_marks_succeeded` — seed scheduled history row; simulate `MonitorResult{Complete}`; assert row outcome == `succeeded` and `agents.restart_count == 1`.
  - `failed_again_after_restart_marks_failed_again` — same but `MonitorResult{Failed}`; assert row outcome == `failed_again`.
  - `should_wake_includes_restart_scheduled` — assert `Scheduler::should_wake(RestartScheduled{..}) == true`.

**Non-testable items:**
- Daemon-boot wiring of `scheduler.with_supervision(...)` is verified by integration tests in T5 and end-to-end runs.

**Notes/Warnings:**
- `restart_dispatch` is intentionally separate from `dispatch_internal` (which consumes a `QueueRow`). They share the executor-start + watch_completion plumbing but differ on how they assemble `ExecuteRequest`.
- The `RestartDispatcher` trait keeps `Scheduler` testable without the full `AgentManager`. Production wires `manager.clone()`.

---

### Task 4: Escalation: mail send + reserved-prefix guard + depth propagation

**Summary:** When the supervisor decides `BudgetExhausted` and the agent has `escalate_to`, send a wake-eligible mail with `sender_id = "supervisor://<failed-agent-id>"` to the configured target (agent or topic), publish `Escalated`, and propagate `escalation_depth + 1` to any restart triggered on the recipient. Reject user-sent mail with reserved sender prefixes.

**Dependencies:** Task 2

**Files to create/modify:**
- `src/daemon/supervisor.rs` — Add `mail_sender: Arc<dyn EscalationMailSender>` field. Add `pub trait EscalationMailSender: Send + Sync { async fn send_escalation(&self, sender_id: &str, target: &str, body: &str) -> Result<u32>; }` (returns `fanout_count`). Default impl writes mail rows directly via `Database::insert_mail` (mirroring `DbWakeMailSender` at `wake_registry.rs:50-91`). On `BudgetExhausted` with `escalate_to.is_some()`, supervisor calls `send_escalation`, then publishes `StreamEvent::Escalated { agent_id, target, fanout_count }`.
- `src/daemon/supervisor.rs` — Add `propagate_escalation_depth(recipient_id, source_depth)`: when escalation lands on a recipient, the recipient's `agents.escalation_depth` is set to `max(current, source_depth + 1)` so the next failure on the recipient sees the inherited depth.
- `src/daemon/rpc.rs:376-…` (`handle_mail_send`) — Add a guard: reject `params.sender` matching `^(supervisor|wake)://` with `rpc_err(req.id, "reserved_sender_prefix")` *before* `parse_address`.
- `src/shared/protocol.rs` — Document `MailSendParams.sender` constraint in a doc comment. (No new struct field.)
- `src/daemon/persistence.rs` — Add `Database::set_escalation_depth(agent_id, u32)` and `get_escalation_depth(agent_id) -> u32`. (Both already declared in T1.)
- `src/daemon/wake_registry.rs:55-91` (`DbWakeMailSender::send_wake_mail`) — Refactor: extract a private `write_mail_row(sender_id, recipient_id, body, wake_eligible) -> Result<String>` helper. The supervisor's escalation sender uses the same helper to keep both paths identical.

**Detailed specification:**

1. **Escalation body format:**
   ```
   [supervisor] agent <failed-id> failed (budget exhausted): <error_summary>
   ```
   Truncated at 16 KiB (existing `WAKE_FOLD_MAX_BYTES`). `error_summary` comes from the agent's last `MonitorResult.error_reason`, fallback `"unknown"`.

2. **Address resolution:**
   - `escalate_to` is parsed via `parse_address` (T0 unchanged).
   - `Address::Agent(id)` — single mail row written, `recipient_id = id`. `fanout_count = 1`.
   - `Address::Topic(name)` — fan out via existing `db.list_subscribers_for_topic(name)`; one mail row per subscriber; `fanout_count = subscribers.len()`. Zero subscribers is valid: 0 mail rows, `fanout_count = 0`, `Escalated` still fires.

3. **Sender prefix guard at `mail.send`:**
   - In `handle_mail_send`, after parsing params: `if let Some(s) = &params.sender { if s.starts_with("supervisor://") || s.starts_with("wake://") { return rpc_err(req.id, "reserved_sender_prefix"); } }`.
   - Internal callers (the supervisor's `EscalationMailSender`) bypass `mail.send` entirely; they call `Database::insert_mail` directly with the synthetic sender. Same pattern as `DbWakeMailSender`.

4. **Depth propagation:**
   - When `send_escalation` writes the mail row(s), the supervisor reads the failed agent's `escalation_depth` and, for each recipient, calls `propagate_escalation_depth(recipient_id, failed_depth)`.
   - The recipient's restart-triggered-by-mail flow doesn't need changes — the next `Failed` event on the recipient reads the now-bumped `escalation_depth` via `evaluate`.

5. **`Escalated` event:**
   - Always emitted on the `BudgetExhausted` branch when `escalate_to` is set.
   - `target` field is the literal `escalate_to` string (`agent://abc12345` or `topic://human-review`), preserving the user's input.
   - `fanout_count` reflects how many mail rows were written.

**Edge cases to handle:**
- `escalate_to = topic://...` with zero subscribers: 0 mail rows, `Escalated { fanout_count: 0 }`.
- `escalate_to = agent://<banished>`: mail row inserted; existing mail subsystem flips it to `Failed` per current semantics. Supervisor still publishes `Escalated`.
- `BudgetExhausted` with `escalate_to = None`: no mail, no `Escalated` event. The agent stays `Failed` and `RestartBudgetExhausted` is the only terminal event (this is the "Alternative C" — pure retry without escalation).
- `BudgetExhausted { reason: "tree_depth_exceeded" }`: `Escalated` is *not* fired (the whole point is to stop pushing deeper). Only `RestartBudgetExhausted { reason: "tree_depth_exceeded" }` lands.
- User submits `mail.send sender = "supervisor://xyz"`: rejected at RPC with `reserved_sender_prefix`. Preexisting wake mail-write path is unaffected (it bypasses `handle_mail_send`).

**Acceptance criteria:**
- [ ] On `BudgetExhausted` with `escalate_to = Some("agent://<id>")`, supervisor writes one mail row with `sender_id = "supervisor://<failed-agent-id>"`, `recipient_id = <id>`, `wake_eligible = true`.
- [ ] On `BudgetExhausted` with `escalate_to = Some("topic://<name>")`, supervisor writes one mail row per topic subscriber with the same `sender_id`. Zero subscribers → 0 rows.
- [ ] After every escalation send (including 0-subscriber topic), supervisor publishes one `StreamEvent::Escalated { agent_id, target, fanout_count }` with `target` echoing the user's `escalate_to` string verbatim.
- [ ] On `BudgetExhausted` with `escalate_to = None`, no mail row, no `Escalated` event, and exactly one `RestartBudgetExhausted` event.
- [ ] On `BudgetExhausted { reason: "tree_depth_exceeded" }`, no `Escalated` event regardless of `escalate_to`.
- [ ] When escalation is sent, every recipient's `agents.escalation_depth` is updated to `max(current, failed_agent_depth + 1)`.
- [ ] `mail.send` RPC with `sender = "supervisor://abc12345"` returns error code `reserved_sender_prefix` and writes no mail row.
- [ ] `mail.send` RPC with `sender = "wake://abc12345"` returns the same error.
- [ ] `mail.send` RPC with `sender = "agent://abc12345"` (existing valid form) is unaffected.
- [ ] Escalation body matches the format `[supervisor] agent <id> failed (budget exhausted): <reason>` and is truncated at 16 KiB.

**Contract tests (RED phase):**
- Test file: `tests/supervisor_escalation.rs` (new)
- Tests to write before implementing:
  - `escalate_to_agent_writes_one_mail_with_supervisor_sender` — seed agent `A` with `escalate_to = agent://B`, exhaust budget; assert mail row with `sender_id == "supervisor://A"`, `recipient_id == B`.
  - `escalate_to_topic_fanout_writes_per_subscriber` — seed two subscribers on `topic://t`; assert 2 mail rows + `Escalated { fanout_count: 2 }`.
  - `escalate_to_topic_with_no_subscribers_emits_event_with_zero_fanout` — empty topic; assert 0 mail rows + `Escalated { fanout_count: 0 }`.
  - `budget_exhausted_without_escalate_to_emits_only_restart_budget_exhausted` — `escalate_to = None`; assert no `Escalated` event.
  - `tree_depth_exceeded_does_not_escalate` — seed `escalation_depth = 3`, `escalate_to = agent://X`; assert no mail, no `Escalated` event.
  - `escalation_propagates_depth_plus_one` — seed `failed.escalation_depth = 1`; escalate to `recipient`; assert `recipient.escalation_depth == 2`.
- Test file: `tests/mail_reserved_prefix.rs` (new)
- Tests to write before implementing:
  - `mail_send_rejects_supervisor_prefix` — RPC `mail.send` with `sender = "supervisor://xyz"`; assert `RpcError.code == "reserved_sender_prefix"`; assert no row in `mail`.
  - `mail_send_rejects_wake_prefix` — same with `wake://xyz`.
  - `mail_send_accepts_agent_prefix` — same with `agent://abcd1234` and a valid recipient; assert success.

**Non-testable items:**
- The body-format string is matched textually in tests; the format is part of the contract.
- Documenting the reserved-prefix list in `mail.send` help is a doc-only follow-up.

**Notes/Warnings:**
- The supervisor's escalation mail path bypasses `handle_mail_send` entirely (same as the wake registry). The reserved-prefix guard exists only to stop *user-supplied* senders from forging system identities.
- `propagate_escalation_depth` writes the recipient's column even if the recipient is `Active` or `Dormant`; the value only matters when the recipient enters `Failed` later.

---

### Task 5: Banish cascade + crash recovery (`replay_pending_on_boot`)

**Summary:** Add a `supervisor.cancel_pending(id)` call to the existing `agent_manager.banish()` cascade and clear the agent's supervision config. On daemon boot, before the supervisor spawns its bus loop, promote any torn `Restarting` rows to `Failed`, then re-evaluate every `Failed` agent with active policy and unspent budget so pending restarts survive a daemon flap.

**Dependencies:** Tasks 2, 3

**Files to create/modify:**
- `src/daemon/agent_manager.rs:492-571` — In `banish()`, after `banish_inner` succeeds, after the existing `wake_registry.retire_for_agent(id)` cascade, call `if let Some(sup) = self.supervisor.lock().await.clone() { sup.cancel_pending(id).await; db.clear_supervision(id); }`. Add `supervisor: Mutex<Option<Arc<Supervisor>>>` field and `set_supervisor` setter (mirrors `set_wake_registry` at `:136-138`). Add a new `Restarting` arm in `banish_inner` (`:512-570`): cancel pending, flip state to `Banished`, publish `StateChange { Restarting → Banished }`.
- `src/daemon/persistence.rs` — Add `Database::clear_supervision(agent_id)`: `UPDATE agents SET restart_policy='never', max_restarts=NULL, restart_window_secs=NULL, escalate_to=NULL WHERE id=?1`.
- `src/daemon/supervisor.rs` — Add `pub async fn replay_pending_on_boot(self: &Arc<Self>) -> Result<()>`: (a) call `db.mark_torn_restarting_as_failed()` and publish `StateChange { Restarting → Failed }` for each returned id; (b) for each `Failed` agent with active policy, call `evaluate(id)`; if `Restart`, call `schedule_restart` with `fire_at = now` (delay-zero, since downtime exceeded the original window); if `BudgetExhausted`, publish the corresponding event but skip if an `Escalated` event for the same agent exists in the events table since the latest `restart_history` row's `attempted_at`.
- `src/daemon/mod.rs:55-107` — Wire the supervisor: after `wake_registry.replay_on_boot()`, instantiate `supervisor = Supervisor::new(...)`, call `supervisor.replay_pending_on_boot().await`, then `manager.set_supervisor(supervisor.clone())` and `scheduler = scheduler.with_supervision(supervisor.clone())` and `supervisor.spawn()`.

**Detailed specification:**

1. **Banish cascade ordering:**
   - `banish_inner` first (state flip, process kill).
   - Then `wake_registry.retire_for_agent` (existing).
   - Then `supervisor.cancel_pending(id)` + `db.clear_supervision(id)`.
   - Cascade is fire-and-forget: errors logged at warn, banish always succeeds. Same as wake-registry cascade.

2. **`Restarting` → `Banished` arm in `banish_inner`:**
   ```rust
   AgentState::Restarting => {
       self.db.update_agent_state(id, &AgentState::Banished, None)?;
       managed.agent.state = AgentState::Banished;
       self.event_bus.publish(StreamEvent::StateChange {
           agent_id: id.to_string(),
           old_state: AgentState::Restarting,
           new_state: AgentState::Banished,
       });
       info!(id = %id, "Restarting agent banished");
       Ok(true)
   }
   ```
   Pending-restart cancellation happens in the outer `banish` cascade.

3. **Boot replay sequence** (in `replay_pending_on_boot`, called once before `spawn()`):
   - Step 1: `let torn = db.mark_torn_restarting_as_failed()?` — flips any leftover `Restarting` rows. For each id, publish `StateChange { Restarting → Failed }`.
   - Step 2: `let candidates = db.list_failed_with_active_policy()?` — agents in `Failed` with `restart_policy != 'never'`.
   - Step 3: For each candidate, run `evaluate(id)`. Branch:
     - `Restart { attempt, .. }`: check the events table for an `Escalated { agent_id }` row with `seq > <last restart_history row's seq>`. If present, treat budget as already escalated (skip — this is the "crash after escalation" case). Else `schedule_restart(id, attempt, fire_at: now)`.
     - `BudgetExhausted { reason }`: publish `RestartBudgetExhausted` (idempotent — events table de-dups via the `seq` ordering, so a re-published event appears as a new event row but downstream consumers tolerate that, matching dormant-migration semantics).
     - `NotSupervised`: skip.

4. **Set ordering:** `replay_pending_on_boot` must run before `supervisor.spawn()` so the in-memory heap is rebuilt before the bus loop starts consuming new `StateChange` events.

**Edge cases to handle:**
- Boot with zero `Restarting` rows: `mark_torn_restarting_as_failed` returns empty Vec; no events.
- Boot with one `Restarting` row that crashed mid-dispatch: flip to `Failed`, then `evaluate` runs the failed-agent path; the prior `restart_history` row is still `outcome = scheduled` from before the crash. Subsequent `succeeded` reconciliation never happens, so the row stays `scheduled`. Acceptable — the window count interprets `scheduled` as "in flight," and the next failure-driven `evaluate` either schedules or exhausts the budget per current rules.
- Banish during `Restarting`: the cascade cancels the pending heap entry. The flow is: cancel pending → state flip → wake registry retire → supervisor cancel_pending (idempotent if cascade ran in either order). Test verifies both orderings.
- Banish during the 2-second restart delay window: same as above.
- Banish of an agent with no policy: `clear_supervision` is a no-op (the row already has defaults).
- Boot replay with a million `Failed` rows: bounded to N evaluations × constant DB work each. Acceptable for v1; if it becomes an issue, add a `LIMIT 10000` to `list_failed_with_active_policy` and document the cap.

**Acceptance criteria:**
- [ ] `agent_manager.banish(id)` on a `Restarting` agent: agent transitions to `Banished`, `StateChange { Restarting → Banished }` published, pending heap entry for `id` removed.
- [ ] `agent_manager.banish(id)` on a `Failed` agent with pending restart: pending heap entry removed; supervision config cleared (`restart_policy = 'never'`, all other fields `NULL`).
- [ ] `agent_manager.banish(id)` on an agent with no supervision config: succeeds, no error, supervision-clear is a no-op.
- [ ] Banish cascade is fire-and-forget: a transient `cancel_pending` failure logs a warn but does not fail `banish` (verified by injecting a fake supervisor whose `cancel_pending` returns `Err`).
- [ ] `Database::mark_torn_restarting_as_failed()` is called by `replay_pending_on_boot` before any other DB read; it returns the IDs and the supervisor publishes one `StateChange { Restarting → Failed }` per ID.
- [ ] After boot replay with one `Failed`+`OnFailure` agent and unspent budget, the supervisor's pending heap contains exactly one entry for that agent with `fire_at = now` (immediate fire).
- [ ] After boot replay with a `Failed` agent that has an `Escalated` event newer than its latest `restart_history` row, the supervisor does *not* schedule a restart and does *not* re-emit `Escalated`.
- [ ] After boot replay with a `Failed`+`OnFailure` agent whose budget is spent, the supervisor publishes `RestartBudgetExhausted` (no schedule).
- [ ] Boot replay runs before `supervisor.spawn()` (verified by ordering — the spawned task does not see the bus events emitted during replay because they're persisted via the writer task; new bus subscribers from `spawn()` don't replay history).
- [ ] `Database::clear_supervision(id)` resets `restart_policy` to `'never'` and nulls the other four supervision columns.

**Contract tests (RED phase):**
- Test file: `tests/supervisor_banish.rs` (new)
- Tests to write before implementing:
  - `banish_restarting_transitions_to_banished` — agent in `Restarting`; banish; assert state `Banished` and `StateChange{Restarting→Banished}`.
  - `banish_cancels_pending_restart` — schedule restart; banish; assert `supervisor.pending` no longer contains the agent.
  - `banish_clears_supervision_config` — seed config; banish; assert `db.get_supervision(id)` returns `None` (or `policy = Never` with all other fields cleared).
  - `banish_continues_when_supervisor_cancel_fails` — fake supervisor returning Err; banish still flips state; warn logged.
- Test file: `tests/supervisor_crash_recovery.rs` (new)
- Tests to write before implementing:
  - `boot_promotes_restarting_to_failed` — seed agent in `Restarting`; boot replay; assert state `Failed` and `StateChange` event published.
  - `boot_replays_pending_for_failed_with_active_policy` — seed Failed agent with policy + budget; boot replay; assert one entry in pending heap with `fire_at <= now`.
  - `boot_skips_replay_for_already_escalated` — seed Failed agent + `restart_history{budget_exhausted}` row + `Escalated` event newer than the row; boot replay; assert no schedule, no fresh `Escalated`.
  - `boot_skips_replay_for_policy_never` — seed Failed agent with `policy = Never`; boot replay; assert empty heap.
  - `boot_emits_budget_exhausted_when_window_full` — seed Failed agent with `max_restarts = 3` and 3 history rows; boot replay; assert `RestartBudgetExhausted` event and no schedule.

**Non-testable items:**
- The boot wiring sequence in `mod.rs` (replay-before-spawn) is verified by integration tests at the daemon level (see Testing Strategy).

**Notes/Warnings:**
- The `Escalated`-event check in the replay path is the single defense against re-escalation after a crash. Test it explicitly.
- `clear_supervision` does NOT clear `restart_count` or `escalation_depth` — those are historical records. Banish ends an agent's life; bumping its lifetime counter to zero would lose audit info.
- Banish ordering must be consistent: outer `banish` calls `banish_inner` first (state flip) so subsequent cascade steps see a banished state. This matches the wake-registry cascade ordering at `agent_manager.rs:494-503`.

---

### Task 6: CLI surface: `--restart`, `--max-restarts`, `--escalate-to` + circle/status display

**Summary:** Add the three supervision flags to `grim summon`, plumb them through `SummonParams` and the RPC handler with validation, and surface restart count + policy in `grim circle` / `grim status` output.

**Dependencies:** Tasks 1, 4

**Files to create/modify:**
- `src/main.rs:19-40` — Add three new flags to the `Summon` clap variant: `--restart <never|on_failure>` (default `never`), `--max-restarts <N/Ts>` (parsed via a custom value parser), `--escalate-to <addr>`.
- `src/cli/commands/summon.rs` — Pass the new fields through to the RPC params; render `Agent <id> summoned (state: queued, restart: on_failure 3/60s, escalate-to: topic://x)` when applicable.
- `src/shared/protocol.rs:50-60` — Extend `SummonParams`: `pub restart_policy: Option<String>`, `pub max_restarts: Option<u32>`, `pub restart_window_secs: Option<u32>`, `pub escalate_to: Option<String>`.
- `src/daemon/rpc.rs:146-176` — In `handle_summon`, validate the new fields *before* `enqueue_with_options`. Reject:
  - `policy = "never"` AND any of the other three fields set → `invalid_supervision: never_with_options`.
  - `policy = "on_failure"` AND `max_restarts` is `None` or `0` → `invalid_supervision: max_restarts_required` / `max_restarts_zero`.
  - `policy = "on_failure"` AND `restart_window_secs` is `None` or `0` → `invalid_supervision: window_required`.
  - `restart_window_secs > 604800` → `invalid_supervision: window_too_large`.
  - `escalate_to.is_some()` AND `policy != "on_failure"` → `invalid_supervision: escalate_requires_policy`.
  - `escalate_to == Some(format!("agent://{}", new_agent_id))` (post-enqueue check; or accept the rare ID collision and rely on summon-time uniqueness) → `invalid_supervision: self_escalation`.
  - `escalate_to` that fails `parse_address` → forward the parse error code.
- `src/daemon/agent_manager.rs:235-308` — Extend `enqueue_with_options` to accept and persist the supervision fields via `db.set_supervision`. After insert, call `db.set_supervision(agent_id, config)`.
- `src/cli/formatters.rs` — Add `RESTART` column to `grim circle` showing `<count>/<max>` for `OnFailure` agents and `-` for `Never`. Add `restart_policy` and `restart_count` to the `grim status <id>` block.
- `src/daemon/persistence.rs` — Extend `list_agents` / `get_agent` to project the new columns (already added in T1) into `Agent`. Add `restart_policy: RestartPolicy` and `restart_count: u32` to `Agent` struct in `types.rs`.

**Detailed specification:**

1. **`--max-restarts` parsing:** clap value parser regex `^(\d+)/(\d+)s$`. Yields `(max: u32, window_secs: u32)`. Malformed input fails clap parse with help text: `expected format <N>/<T>s, e.g. 3/60s`.

2. **CLI examples (must match acceptance criteria):**
   - `grim summon "task" --restart on_failure --max-restarts 3/60s` — pure retry, no escalation.
   - `grim summon "task" --restart on_failure --max-restarts 5/3600s --escalate-to topic://human-review` — full supervision.
   - `grim summon "task" --restart never` — explicit no-restart (same as omitting the flag).
   - `grim summon "task" --escalate-to topic://x` — REJECT (`escalate_requires_policy`).
   - `grim summon "task" --restart on_failure` — REJECT (`max_restarts_required`).

3. **Display format:**
   ```
   $ grim circle
   ID         STATE       RESTART   AGE   TASK
   abc12345   active      0/3       2m    overnight scroll task 4
   def67890   restarting  1/3       3m    overnight scroll task 6
   ghi23456   complete    -         5m    one-shot
   ```
   `RESTART` column shows `<used>/<max>` for `OnFailure`, `-` for `Never`. `<used>` = `agents.restart_count` (lifetime).

4. **`grim status <id>` adds:**
   ```
   restart-policy: on_failure (3/60s)
   restart-count: 1
   escalate-to: topic://human-review
   escalation-depth: 0
   ```
   Omit the lines entirely when policy is `Never`.

5. **Reject ordering:** validation runs before `enqueue_with_options` so a rejection leaves no DB row.

**Edge cases to handle:**
- `--max-restarts 0/60s` — clap accepts the parse, but `handle_summon` rejects with `max_restarts_zero`.
- `--max-restarts 3/0s` — same, `window_required` (or `window_zero`; pick `window_required` to match the "missing" semantics).
- `--max-restarts 3/700000s` — rejected with `window_too_large` (max 7d = 604800s).
- `--escalate-to bogus://x` — clap accepts; RPC rejects with the parse-address error code (`invalid_address`).
- `--escalate-to topic://` (empty topic) — RPC rejects with `invalid_topic_name`.
- `--restart on_failure --max-restarts 3/60s` with no `--escalate-to`: pure retry. Persisted with `escalate_to = NULL`. Valid.
- Existing summon without supervision flags: behaves identically to current code (`restart_policy = 'never'` default).

**Acceptance criteria:**
- [ ] `grim summon "task" --restart on_failure --max-restarts 3/60s` succeeds and the resulting agent's `agents` row has `restart_policy = 'on_failure'`, `max_restarts = 3`, `restart_window_secs = 60`, `escalate_to = NULL`.
- [ ] `grim summon "task" --restart on_failure --max-restarts 3/60s --escalate-to topic://x` succeeds; `escalate_to = 'topic://x'`.
- [ ] `grim summon "task" --restart never` succeeds with `restart_policy = 'never'` and the other supervision columns NULL/0 (default).
- [ ] `grim summon "task" --escalate-to topic://x` fails with exit code != 0 and `RpcError.code == "escalate_requires_policy"`.
- [ ] `grim summon "task" --restart on_failure` fails with `max_restarts_required`.
- [ ] `grim summon "task" --restart on_failure --max-restarts 0/60s` fails with `max_restarts_zero`.
- [ ] `grim summon "task" --restart on_failure --max-restarts 3/700000s` fails with `window_too_large`.
- [ ] `grim summon "task" --restart on_failure --max-restarts 3/60s --escalate-to bogus://x` fails with `invalid_address`.
- [ ] `grim summon "task" --restart on_failure --max-restarts 3/60s --escalate-to agent://<self>` (self-loop): RPC rejects with `self_escalation` (the new agent's id is checked after id generation, before insert).
- [ ] After three successful restarts, `grim circle` shows the agent's `RESTART` column as `3/3`.
- [ ] `grim status <id>` for an `OnFailure` agent shows `restart-policy: on_failure (<max>/<window>s)` and `restart-count: <n>` lines.
- [ ] `grim status <id>` for a `Never` agent shows neither line.
- [ ] `grim summon` without supervision flags persists `restart_policy = 'never'` and `restart_count = 0` (default behavior unchanged).
- [ ] Validation rejections leave no `agents` row (verified via `db.get_agent(id)` → None after a rejected summon).

**Contract tests (RED phase):**
- Test file: `tests/cli_summon_supervision.rs` (new)
- Tests to write before implementing:
  - `summon_on_failure_persists_full_config` — RPC call; assert all four columns set as expected.
  - `summon_never_persists_defaults` — RPC; assert default state.
  - `summon_escalate_without_policy_rejects` — assert error code `escalate_requires_policy`; assert no agent row.
  - `summon_on_failure_without_max_restarts_rejects` — error `max_restarts_required`.
  - `summon_max_restarts_zero_rejects` — error `max_restarts_zero`.
  - `summon_window_too_large_rejects` — error `window_too_large`.
  - `summon_self_escalation_rejects` — error `self_escalation`.
  - `summon_invalid_escalate_address_rejects` — error code from `parse_address`.
- Test file: `tests/cli_circle_supervision.rs` (new)
- Tests to write before implementing:
  - `circle_renders_restart_column` — seed agent with `restart_count = 2, max_restarts = 3`; capture `circle` output; assert it contains `2/3`.
  - `circle_renders_dash_for_never_policy` — seed agent with `policy = Never`; assert `RESTART` column is `-`.
  - `status_renders_supervision_block_for_on_failure` — capture `status <id>` output; assert it contains `restart-policy:` and `restart-count:` lines.
  - `status_omits_supervision_block_for_never` — assert the lines are absent.

**Non-testable items:**
- clap value-parser construction for `--max-restarts` is verified by `cargo check` and a single CLI smoke test (a malformed value exits the binary with help text).
- Help-text wording for the new flags is doc-only.

**Notes/Warnings:**
- `Agent` struct gains two new public fields (`restart_policy`, `restart_count`); update every `Agent { ... }` literal in tests and `seed_agent_for_test_with_session` (`agent_manager.rs:618-650`).
- The `RESTART` column appears even when no agent has supervision configured (rendered as `-` for all rows). Cosmetic — accepts a follow-up to hide it conditionally.

---

### Task 7: Scroll-keeper integration: `Restarting` no-op arm + dependent-task gating

**Summary:** Add an explicit `Restarting` arm to `ScrollKeeper`'s `StateChange` matcher so dependent tasks do not fire while a supervised task is mid-retry. The arm is a no-op + debug log; the existing `Complete` / `Failed` arms run unchanged on retry success / budget exhaustion.

**Dependencies:** Task 1

**Files to create/modify:**
- `src/daemon/scroll_keeper.rs:53-75` — Add `AgentState::Restarting => debug!(agent_id = %agent_id, "scroll-keeper: ignoring transient Restarting state")` arm. Verify the existing `_ => {}` branch is removed in favor of explicit enumeration.

**Detailed specification:**

1. **Existing matcher** (lines 53-65):
   ```rust
   match new_state {
       AgentState::Complete => self.handle_agent_completion(agent_id).await,
       AgentState::Failed | AgentState::Banished => self.handle_agent_failure(agent_id).await,
       _ => {}
   }
   ```

2. **New matcher:**
   ```rust
   match new_state {
       AgentState::Complete => self.handle_agent_completion(agent_id).await,
       AgentState::Failed | AgentState::Banished => self.handle_agent_failure(agent_id).await,
       AgentState::Restarting => {
           debug!(agent_id = %agent_id, "scroll-keeper: ignoring transient Restarting state");
       }
       AgentState::Queued | AgentState::Summoning | AgentState::Active | AgentState::Dormant => {}
   }
   ```
   The wildcard `_ => {}` is replaced by explicit enumeration so `cargo check` flags any future state addition.

3. **Behavioral implication:**
   - A scroll task that fails and gets retried lands in `Restarting` first; scroll-keeper does not call `handle_agent_failure`. Dependent tasks stay `Blocked`.
   - When the retry succeeds (`Restarting → Active → Complete`), `handle_agent_completion` fires once and dependents proceed.
   - When the retry exhausts the budget (`Restarting → Failed` via `restart_dispatch` → `watch_completion`), `handle_agent_failure` fires once and dependents are skipped per existing logic.

**Edge cases to handle:**
- Scroll abandonment cascade (`scroll abandon`): unchanged. The cascade flips agents to `Banished` regardless of their prior state, which scroll-keeper already handles.
- Budget-exhausted scroll task: the second `Failed` event (after the retries) hits the existing `Failed` arm; dependents skip. No special path.
- Two-level supervisor tree where parent escalation fires inside a scroll: orthogonal — escalation is mail, scrolls don't subscribe to mail events.

**Acceptance criteria:**
- [ ] In `ScrollKeeper::start`, the `match new_state` arm for `AgentState::Restarting` is present and is a no-op (does not call `handle_agent_completion` or `handle_agent_failure`).
- [ ] When a scroll task transitions `Failed → Restarting → Active → Complete`, exactly one `handle_agent_completion` is invoked (after `Complete`).
- [ ] When a scroll task transitions `Failed → Restarting → Failed` (budget exhausted), exactly one `handle_agent_failure` is invoked (after the second `Failed`); the first `Failed` does not fire it because the supervisor catches it first and transitions to `Restarting`.
- [ ] A scroll dependent task does NOT enter `Active` while its parent is in `Restarting`.
- [ ] The `match new_state` block has no `_ => {}` wildcard; every `AgentState` variant is enumerated explicitly.

**Contract tests (RED phase):**
- Test file: `tests/scroll_keeper_supervision.rs` (new)
- Tests to write before implementing:
  - `restarting_state_does_not_fire_handlers` — instrument scroll-keeper with mock `handle_agent_completion` / `handle_agent_failure`; publish `StateChange{Active→Restarting}`; assert neither handler invoked.
  - `restart_success_fires_completion_handler_once` — publish sequence `Active→Failed`, `Failed→Restarting`, `Restarting→Active`, `Active→Complete`; assert `handle_agent_completion` called exactly once.
  - `dependent_task_blocked_during_restart` — seed scroll with parent task and one dependent; transition parent to `Restarting`; tick scroll-keeper; assert dependent stays `Blocked`.
  - `budget_exhausted_fires_failure_handler_once` — sequence `Active→Failed`, `Failed→Restarting` (supervisor schedules, exhausts, flips back), `Restarting→Failed`; assert `handle_agent_failure` called exactly once.

**Non-testable items:**
- The exhaustive-match check is enforced by `cargo check`, not a contract test.

**Notes/Warnings:**
- The first `Failed` in the supervised flow goes to `Restarting` via the supervisor; scroll-keeper's existing `Failed` arm only fires for terminal failures (budget exhausted). Verify ordering: the supervisor's bus subscription must transition the agent to `Restarting` *before* scroll-keeper's bus subscriber processes the `Failed` event. Both use `tokio::sync::broadcast`, which is FIFO per receiver — so the supervisor's `on_state_change` runs in its own task and the actual state flip in the DB happens before the next bus tick. **However**, scroll-keeper's `handle_agent_failure` reads the agent's current DB state before acting; if there's a race window where it reads `Failed` before the supervisor flips to `Restarting`, the dependent might prematurely skip. **Mitigation:** scroll-keeper checks `agents.restart_policy != 'never'` before acting on `Failed`; if a policy is set, treat the event as transient and defer to a later `StateChange` event. This is a small fix in `handle_agent_failure` and is part of T7.
- Update `handle_agent_failure` (`scroll_keeper.rs`) to skip the failure path when `db.get_supervision(agent_id)?.is_some_and(|c| c.policy != Never)` — let the supervisor's `Restarting` or terminal `Failed` event drive scroll-keeper's response.

---

## Testing Strategy (TDD)

Implementation follows red-green TDD: for each task, the implementer writes failing contract tests from the acceptance criteria (RED), then implements the minimum code to make them pass (GREEN). Contract tests are immutable once committed.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `tests/supervision_state.rs`, `tests/supervision_schema.rs`, `tests/supervision_events.rs` | 15 tests | Match-arm audits (`cargo check`) |
| 2 | `tests/supervisor_evaluate.rs`, `tests/supervisor_actor.rs` | 12 tests | Bus-loop wiring (covered by T3 integration) |
| 3 | `tests/supervisor_dispatch.rs`, `tests/supervisor_history_reconcile.rs` | 8 tests | Daemon-boot wiring |
| 4 | `tests/supervisor_escalation.rs`, `tests/mail_reserved_prefix.rs` | 9 tests | Body-format documentation |
| 5 | `tests/supervisor_banish.rs`, `tests/supervisor_crash_recovery.rs` | 9 tests | Boot-sequence ordering (covered by integration) |
| 6 | `tests/cli_summon_supervision.rs`, `tests/cli_circle_supervision.rs` | 12 tests | clap parser construction; help text |
| 7 | `tests/scroll_keeper_supervision.rs` | 4 tests | `cargo check` exhaustive-match enforcement |

### Integration Testing

- **End-to-end retry success:** summon agent with `--restart on_failure --max-restarts 3/60s`; inject a flaky executor that fails once then succeeds; assert agent transitions `Active → Failed → Restarting → Active → Complete` and `restart_count == 1`. `tests/supervision_e2e_retry_success.rs`.
- **End-to-end budget exhausted + escalation to topic:** summon two agents; one subscribes to `topic://escalations`; first agent has `--restart on_failure --max-restarts 2/60s --escalate-to topic://escalations` and a permafailing executor; assert after 2 retries, `RestartBudgetExhausted` and `Escalated{fanout_count:1}` events fire and the subscriber receives the escalation mail. `tests/supervision_e2e_escalation.rs`.
- **End-to-end three-level tree-depth cap:** summon a chain A→B→C where each escalates to the next; configure C to fail; assert tree-depth cap fires at depth 3 and no infinite mail loop. `tests/supervision_e2e_tree_depth.rs`.
- **Crash recovery clean shutdown:** schedule a restart, drop the supervisor (simulate clean shutdown), boot a fresh one, assert pending heap rebuilt and the next tick dispatches. `tests/supervision_e2e_crash_clean.rs`.
- **Crash recovery mid-dispatch:** seed agent in `Restarting` (torn write); boot supervisor; assert state flips to `Failed`, then to `Restarting` again via re-evaluation, then dispatches once. `tests/supervision_e2e_crash_torn.rs`.
- **Banish during retry window:** schedule restart; banish agent before the 2-second deadline; assert no dispatch occurs and the heap entry is gone. `tests/supervision_e2e_banish_during_window.rs`.
- **Scroll integration:** inscribe a scroll with a flaky task and a dependent; assert dependent waits through the retry and only fires after success. `tests/supervision_e2e_scroll.rs`.

### Manual Testing Checklist

- [ ] `grim daemon`; `grim summon "echo hi" --restart on_failure --max-restarts 3/60s`; verify `grim circle` shows `RESTART 0/3`.
- [ ] Summon a known-failing task with `--restart on_failure --max-restarts 2/60s`; verify `grim circle` advances `0/2 → 1/2 → 2/2` and final state is `Failed`.
- [ ] Summon a flaky-then-succeeding task with `--restart on_failure --max-restarts 3/60s`; verify success after retry and `restart_count == 1`.
- [ ] Summon with `--escalate-to topic://human-review` (no subscribers); verify `Escalated { fanout_count: 0 }` in the event log.
- [ ] Summon a parent supervisor agent subscribed to `topic://human-review`; trigger a failed escalation; verify the parent wakes from the topic mail.
- [ ] Banish a `Restarting` agent; verify state flips to `Banished` and the pending restart does not fire.
- [ ] Restart the daemon while a `Restarting` agent has a pending restart; verify boot replay fires it on the first tick.
- [ ] Try `mail.send --sender supervisor://abc12345`; verify `reserved_sender_prefix` error.
- [ ] Try `grim summon "x" --restart on_failure --max-restarts 0/60s`; verify CLI exits non-zero with `max_restarts_zero`.
- [ ] Inscribe a scroll with one task at `--restart on_failure --max-restarts 2/60s`; force the task to fail twice then succeed; verify dependents fire only after the final success.

## Rollout Considerations

### Feature Flags

No feature flags. The work is additive at the data layer (new table, new columns with defaults), and the CLI surface is opt-in via the new flags. Existing agents and existing CLI usage are unaffected.

### Migration Strategy

- **Schema migrations** are additive and idempotent (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE` gated on column existence — same pattern used for `keep_alive` at `persistence.rs:260-270`).
- **No data migration required** — existing agents default to `restart_policy = 'never'`, which preserves current behavior exactly.
- **Boot replay** (`replay_pending_on_boot`) runs on every boot; it is a no-op on a fresh DB and idempotent on an existing one.
- **Reserved-prefix guard at `mail.send`** is a behavioral break for any user who was sending mail with `sender = "supervisor://..."`. Likelihood: zero (supervisor didn't exist). Document the new error in the `mail.send` help text.

### Rollback Plan

If supervision misbehaves at runtime:
1. `grim banish <flapping-agent-id>` — cancels pending restart and clears policy.
2. For a stuck pending heap, restart the daemon — the heap is rebuilt from `restart_history` + agent state, idempotent.
3. To disable supervision daemon-wide, set `daemon.restart_rate_per_min = 0` in the config — every restart will be rate-limited indefinitely, effectively suspending the feature without code rollback.

If a binary rollback is required:
1. Stop the daemon.
2. `sqlite3 grimoire.db "UPDATE agents SET state = 'failed' WHERE state = 'restarting'"` — collapses the new state into the old enum.
3. Restart with the prior binary. The `restart_history` table and new agents columns are ignored by the older code (additive, not breaking).
4. New columns and the table linger; they can be dropped later or left in place.

## Open Items

- [ ] Confirm `tokio::sync::broadcast` ordering semantics: scroll-keeper and supervisor both subscribe to the same bus; we rely on the supervisor's `Failed → Restarting` flip happening before scroll-keeper's failure handler queries the DB. Mitigation in T7 (supervision-aware skip in `handle_agent_failure`) makes this deterministic regardless of receiver scheduling. Verify via the integration test in `tests/supervision_e2e_scroll.rs`.
- [ ] Confirm `Agent` struct field additions (`restart_policy`, `restart_count`) don't break any external `serde_json` consumers. The HTTP dashboard and `grimw` worker may parse `Agent`; backward-compat: new fields default-deserialize.
- [ ] Validate the `daemon.restart_rate_per_min = 30` default against an actual long-running scroll workload before declaring v1 done. Plan recommends a sanity check; defer to first integration test run.

---

*This spec is implementation-ready. Each task is designed for red-green TDD. Tasks can be picked up independently (respecting dependencies) and completed in a single iteration.*
