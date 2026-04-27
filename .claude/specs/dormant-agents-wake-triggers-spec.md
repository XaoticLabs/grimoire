# Implementation Spec: Dormant Agents with First-Class Wake Triggers

> Generated from: `.claude/plans/dormant-agents-wake-triggers.md`
> Generated on: 2026-04-26

## Overview

Today, `AgentState::Complete` does double duty for "this work is done forever" and "this agent is parked, waiting for something to happen." The mail-wake path proves the daemon can resurrect a finished agent, but it's the only wake source and it special-cases `Complete` agents with a `session_id`. This spec promotes `Dormant` to a real `AgentState`, lands a `WakeRegistry` abstraction, and ships three working wake sources (`cron`, `file-watch`, `parent-completion`) — all firing through the existing mail bus so the resume path is unchanged.

The implementation extends the existing scheduler's seam pattern (`Dispatcher`, `MailWaker`, `AgentStateLookup`) with a new `Clock` seam and a `WakeSource` trait. New CLI surface — the `grim wake` group — mirrors `grim mail` and `grim pact`. The mail-wake scheduler filter widens from `Complete` to `Dormant`, a boot-time migration auto-promotes existing Complete-with-session agents, and `grim invoke` collapses into a thin wrapper over `mail.send --wake-eligible`.

## Technical Context

### Relevant Codebase Areas

- `src/shared/types.rs:39-63` — `AgentState` enum and `is_terminal()`. Extending this enum is the foundation; every match arm in the codebase needs auditing.
- `src/daemon/scheduler.rs:261-330` — `Scheduler::should_wake` and `tick_mail_wake`. Mail-wake filter (`scheduler.rs:290`) widens from `Complete` to `Dormant`.
- `src/daemon/agent_manager.rs:346-498` — `invoke()` (Complete→Active session resume) and `banish()` (cascade cleanup) live here. `AgentManager` already implements `MailWaker`.
- `src/daemon/persistence.rs:78-234` — Migration pattern: `CREATE TABLE IF NOT EXISTS` + `ALTER TABLE` with idempotent existence checks; `events` table lives at lines 179-189.
- `src/shared/protocol.rs:263-390` — `StreamEvent` enum; new `WakeSource*` variants land here.
- `src/shared/types.rs:119-133`, `src/daemon/persistence.rs:206-221` — `Mail` struct and `mail` table; `wake_eligible` flag and `mail_pending_wake` index already in place.
- `src/daemon/event_bus.rs` — `EventBus::publish()` and the background persister; subscribe via `EventBus::subscribe()` returning `tokio::sync::broadcast::Receiver`.
- `src/daemon/rpc.rs:280-305` — `handle_mail_send` reference shape for `wake.*` RPC handlers.
- `src/cli/commands/mail.rs:13-44`, `src/main.rs:141-145` — CLI subcommand-group pattern to mirror in `src/cli/commands/wake.rs`.
- `tests/scheduler_mail_wake.rs:145-283` — Test harness pattern (`RecordingWaker`, `DbLookup`, `NoopDispatcher`) to extend for wake-trigger tests.
- `Cargo.toml` — New deps: `cron = "0.12"` and `notify = "6"`. `chrono`, `rusqlite`, `tokio`, `clap` already present.

### Existing Patterns to Follow

- **Scheduler seam pattern** (`scheduler.rs:41-71`) — daemon-owned actor with injected traits (`Dispatcher`, `MailWaker`, `AgentStateLookup`). `WakeRegistry` is a peer of `Scheduler` and follows the exact same pattern.
- **Migration shape** (`persistence.rs:78-234`) — additive migrations using `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, and existence-checked `ALTER TABLE`. Boot-time agent migration emits `StateChange` events through `EventBus`.
- **CLI subcommand group** (`mail.rs:13-44` + `main.rs:141-145`) — clap derive `#[derive(Subcommand)]` with `Wake { #[command(subcommand)] cmd }` in the top-level enum.
- **JSON-RPC method** (`rpc.rs:280-305`) — `handle_<name>` pattern: parse params, dispatch to manager/registry, return `RpcResponse::success` or `rpc_err`.
- **Recording test seams** (`tests/scheduler_mail_wake.rs:31-72`) — `Mutex<Vec<...>>`-backed mocks for inspecting calls.
- **`is_terminal()` callsites** that distinguish slot accounting from lifecycle finality drive the new `is_final()` method.

### Key Dependencies

- **`cron` crate (0.12+)** — 5-field standard cron parser; produces a `Schedule` with `upcoming(Utc)` iterator. We own the loop; the crate has no embedded scheduler.
- **`notify` crate (6.x)** — recommended-watcher with debounced events. Used in callback mode that pushes events into a tokio mpsc channel.
- **`chrono::Utc`** — already used across the codebase; cron evaluation runs in UTC.
- **`tokio::sync::broadcast`** — already used by `EventBus`; parent-completion source subscribes via `EventBus::subscribe()`.
- **`rusqlite` 0.32** — bundled SQLite; `wake_sources` and `wake_rate_limits` tables added with the existing migration pattern.

### Ambiguity Resolutions

| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| Cron timezone | Plan says "5-field standard cron" but does not state timezone | UTC for cron evaluation (matches `chrono::Utc` usage everywhere else; document in `grim wake add` help text) | Assumed default |
| Wake ID surfaced in mail body | Plan open question | Yes — wake-fire mail body is prefixed with `[wake_<id> (cron|file-watch|parent-completion)]\n` so the resume prompt sees who woke it | Plan favored yes |
| `--keep-alive` flag name | Plan open question | Keep `--keep-alive` for v1 — cosmetic; can rename in a follow-up without breaking persisted state | Plan defer |
| File-watch exclusion patterns | Plan open question | Support `!pattern` exclusion globs in v1; multiple `--watch`/`--ignore` flags accepted on `grim wake add` | Plan favored yes |
| Default rate-limit values | Plan open question | Capacity 60, refill 60/3600 tokens/sec (60 wakes/hr) per agent; per-source override via `--capacity` and `--refill-per-hour` on `grim wake add` | Plan default |
| Max watched paths per file-watch source | Plan flagged inotify limit risk | Cap at 1000 unique paths per source (after glob expansion); over-cap surfaces `WakeSourceFailed { reason: "watch_limit_exceeded" }` | Assumed default |
| `wake list` (no agent_id) ordering | Not specified | Order by `created_at DESC` then `agent_id` for stable output | Assumed default |
| Wake-source ID format | Plan says `wake_<8-hex>` | `wake_` + 8 lowercase hex chars from a UUIDv4 (matches `agent_id` style) | Plan |
| Cron catch-up "missed at least one" definition | Not precise | At boot: if `last_fired_at IS NULL` or `now > next_after(last_fired_at)`, fire once; advance `last_fired_at` to `now` | Assumed default |
| File-watch reconciliation on boot | Plan says "watched paths' mtime newer than last_fired_at" | At boot: walk root once, compare any matched-path mtime vs `last_fired_at`; if any newer, fire once with body `[reconcile] paths newer than last_fired_at` | Assumed default |
| Parent-completion default target states | Plan says "Complete (default; states configurable)" | Default fires only on `Complete`. `--states complete,failed` accepted; `banished` accepted but warned in CLI help | Plan |
| `wake test` rate-limit interaction | Not specified | `wake test` bypasses the token bucket (debug action) but still emits `WakeSourceFired` event with `via: "test"` annotation | Assumed default |
| Migration: agent has session_id but state isn't Complete | Not specified | Migration is gated strictly on `state = 'complete' AND session_id IS NOT NULL`; nothing else is touched | Plan implies |
| Wake mail `sender_id` format | Plan says `wake://<wake-id>` | Stored verbatim in `mail.sender_id`; `parse_address` not extended (synthetic senders bypass address parsing in the send path) | Plan |
| Wake mail `wake_eligible` value | Plan says `wake_eligible = true` | Always `true` for wake-fire mails | Plan |
| Two sources fire near-simultaneously | Plan says "scheduler folds via existing fold logic" | No new logic — both mails are written; existing `build_wake_prompt` joins them | Plan |
| `grim wake remove` while fire is in flight | Not specified | Remove is best-effort: row deleted from `wake_sources`, in-memory handle dropped; any mail already enqueued still fires (consistent with mail bus semantics) | Assumed default |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | `Dormant` state + `is_final` split + scheduler filter + boot migration | None | High |
| 2 | Wake-source schema + `Clock` seam + `StreamEvent` variants | 1 | Medium |
| 3 | `WakeRegistry` actor + `WakeSource` trait + cron source | 2 | High |
| 4 | Parent-completion wake source | 3 | Medium |
| 5 | File-watch wake source | 3 | High |
| 6 | Per-agent token-bucket rate limiter | 3 | Low |
| 7 | `grim wake` CLI + RPC methods + banish cascade | 3 | Medium |
| 8 | `--keep-alive` flag on summon + `invoke` reconciliation | 1, 7 | Low |

### Critical Path

```
T1 (state machine) ──► T2 (schema + clock + events) ──► T3 (registry + cron) ──┬──► T4 (parent-complete)
                                                                                 ├──► T5 (file-watch)
                                                                                 ├──► T6 (rate limiter)
                                                                                 └──► T7 (CLI/RPC) ──► T8 (summon/invoke)
```

T1 is the foundation and must land first. T2 unblocks T3. Tasks T4, T5, T6, and T7 can be parallelized once T3 is in. T8 needs both T1 (Dormant exists) and T7 (`mail.send --wake-eligible` plumbing) to land.

---

### Task 1: `Dormant` state + `is_final` split + scheduler filter + boot migration

**Summary:** Add `AgentState::Dormant`, split `is_terminal()` (slot-free) from `is_final()` (lifecycle done), update every match callsite, widen the mail-wake scheduler filter from `Complete` to `Dormant`, and ship a boot-time migration that promotes Complete-with-session agents to `Dormant`.

**Dependencies:** None

**Files to create/modify:**
- `src/shared/types.rs` — Add `Dormant` variant to `AgentState`; add `is_final()`; update `is_terminal()` to include `Dormant`; update `impl_state_enum!` macro invocation.
- `src/daemon/scheduler.rs` — Change `tick_mail_wake` candidate filter from `state == AgentState::Complete` to `state == AgentState::Dormant` (line 290). Update `should_wake` if needed.
- `src/daemon/agent_manager.rs` — Update `invoke()` (line 346-444) to accept `Dormant` agents instead of `Complete` (transition `Dormant → Active`). Update `banish()` match (line 452-497) to handle `Dormant`. Update any other `match` on `AgentState`.
- `src/daemon/rpc.rs` — Update guards at lines 326 and 435 (banished checks) and any state-based dispatch.
- `src/daemon/scroll_keeper.rs` — Update match at lines 53-66 to use `is_final()` (not `is_terminal()`) for pact/task-chain triggering, since pacts should fire only on truly final states.
- `src/daemon/persistence.rs` — Add `migrate_dormant_agents()` method called from boot path: `UPDATE agents SET state = 'dormant' WHERE state = 'complete' AND session_id IS NOT NULL`. Capture pre-update IDs to emit `StateChange` events.
- `src/daemon/server.rs` (or wherever daemon boot wiring happens) — Call `migrate_dormant_agents()` after `Database::new()` returns; for each migrated id, publish `StreamEvent::StateChange { agent_id, old_state: Complete, new_state: Dormant }` through `EventBus`.
- `src/cli/formatters.rs` (and any human-readable state output) — Render `Dormant` (e.g., as `dormant` in `grim circle` output).

**Detailed specification:**

1. **`AgentState::Dormant`** is added as a non-final, non-active variant. Serde rename: `"dormant"`. Display: `"dormant"`. `FromStr` accepts `"dormant"`.

2. **Method semantics:**
   - `is_terminal(&self) -> bool` — `true` for `Complete | Failed | Banished | Dormant`. Used by the scheduler for slot accounting (`scheduler.rs:265`, `scheduler.rs:281`).
   - `is_final(&self) -> bool` — `true` for `Complete | Failed | Banished` only. Used by lifecycle code (pacts firing, scroll keeper, "agent finished forever" UI labels).

3. **Scheduler filter change:** `scheduler.rs:290` becomes `if state_session.0 != AgentState::Dormant { continue; }`. The `session_id` requirement at line 293-296 stays.

4. **`invoke()` flow change:** `agent_manager.rs:346-444` currently transitions `Complete → Active`. Change to require `Dormant → Active`. The session-resume code path is unchanged. Behavioral effect: callers see `invoke <complete-id>` fail before T8 lands; the boot migration ensures real-world Complete-with-session agents are already Dormant by then.

5. **Boot migration semantics:**
   - Idempotent: repeated runs are no-ops (the WHERE clause filters re-runs).
   - Gated strictly on `state = 'complete' AND session_id IS NOT NULL`. No other states touched.
   - Emits one `StreamEvent::StateChange { agent_id, old_state: Complete, new_state: Dormant }` per migrated agent.
   - Logs `migrated N agents from complete to dormant` at info level.

6. **Match-callsite audit:** Every `match` on `AgentState` and every `is_terminal()` callsite in the codebase must be reviewed. Use `cargo check` + grep `match.*AgentState\|is_terminal\|is_final` to find them. Specific known callsites: `agent_manager.rs:358`, `agent_manager.rs:452-497`, `rpc.rs:326`, `rpc.rs:435`, `scheduler.rs:265`, `scheduler.rs:290`, `scroll_keeper.rs:53-66`.

**Edge cases to handle:**

- Migration runs on a corrupted DB or partial Complete row — gated WHERE clause means malformed rows (e.g., null `state`) are simply skipped.
- Banished agent with a session_id — never migrates (`state != 'complete'`).
- `match` on `AgentState` that uses `_` catch-all — explicitly enumerate the new variant in any non-`_` match; use `#[deny(non_exhaustive_omitted_patterns)]` locally where the original code used exhaustive matching.
- `grim circle` and `grim status` output — render `dormant` as a distinct state, not folded into `complete`.

**Acceptance criteria:**
- [ ] `AgentState::Dormant` is defined in `src/shared/types.rs`, serializes as `"dormant"`, and round-trips through `FromStr`/`Display`.
- [ ] `AgentState::is_terminal()` returns `true` for `Dormant`; `AgentState::is_final()` returns `false` for `Dormant`; both methods compile and `is_terminal` returns `true` for the full set `{Complete, Failed, Banished, Dormant}`.
- [ ] In `src/daemon/scheduler.rs`, `tick_mail_wake` selects only agents with `state == AgentState::Dormant` as wake candidates (verified by reading the source).
- [ ] `Database::migrate_dormant_agents()` updates rows with `state = 'complete' AND session_id IS NOT NULL` to `state = 'dormant'` and returns the list of migrated agent IDs.
- [ ] On daemon boot, for each migrated agent id, exactly one `StreamEvent::StateChange { old_state: Complete, new_state: Dormant }` is published through `EventBus`.
- [ ] Running `migrate_dormant_agents()` twice in a row is a no-op the second time (returns empty list).
- [ ] An agent with `state = 'complete' AND session_id IS NULL` is NOT migrated.
- [ ] An agent with `state = 'failed' AND session_id IS NOT NULL` is NOT migrated.
- [ ] `agent_manager.rs:invoke()` returns `Ok(())` when called on a `Dormant` agent with a valid session_id, and the agent transitions to `Active`.
- [ ] `scroll_keeper.rs` pact-triggering uses `is_final()`, so a `StateChange { new_state: Dormant }` does NOT fire pacts.
- [ ] `cargo check` and `cargo clippy` pass with the new variant — no non-exhaustive match warnings.

**Contract tests (RED phase):**
- Test file: `tests/dormant_state.rs` (new)
- Tests to write before implementing:
  - `is_terminal_includes_dormant` — asserts `AgentState::Dormant.is_terminal() == true`.
  - `is_final_excludes_dormant` — asserts `AgentState::Dormant.is_final() == false`; asserts `Complete/Failed/Banished` all return `true`.
  - `dormant_serde_roundtrip` — serialize Dormant → JSON `"dormant"`; parse `"dormant"` back to `AgentState::Dormant`.
- Test file: `tests/dormant_migration.rs` (new)
- Tests to write before implementing:
  - `migration_promotes_complete_with_session` — seed two agents (one Complete+session, one Complete-no-session); call `migrate_dormant_agents()`; assert only the first is now `Dormant`.
  - `migration_is_idempotent` — call twice; second call returns empty Vec.
  - `migration_emits_state_change_events` — wire a recording `EventBus` subscriber; after migration, exactly one `StateChange{old=Complete,new=Dormant}` per migrated id.
  - `migration_skips_failed_with_session` — Failed-with-session agent stays Failed.
- Test file: `tests/scheduler_mail_wake.rs` (extend existing)
- Tests to write/update:
  - `dormant_agent_with_pending_mail_is_woken` — same as existing `complete_agent_with_pending_mail_is_woken` but agent state is `Dormant`. Asserts wake fires.
  - `complete_agent_no_longer_woken_by_mail` — seed a Complete (not Dormant) agent with pending mail; assert wake does NOT fire.
  - `pacts_do_not_fire_on_dormant_transition` — extend `tests/scroll_lifecycle.rs` or create `tests/pacts_dormant.rs`: a parent that transitions `Active → Dormant` should NOT fire any registered pact.

**Non-testable items:**
- Updating `match AgentState` arms across the codebase is enforced by `cargo check`, not contract tests.
- `grim circle` rendering of `dormant` is best verified manually; a CLI integration test in `tests/cli_circle.rs` may extend coverage.

**Notes/Warnings:**
- This is the riskiest task — every callsite that branched on `Complete` must be audited. Run `rg "AgentState::Complete" src/ tests/` and review every result.
- The boot-migration `StateChange` event ordering matters: emit AFTER the DB update is committed and BEFORE the scheduler's first tick, so dashboards see the transition before any wake activity.
- Do NOT break `grim invoke <complete-id>` for users who run an old DB until T8 lands — the migration takes care of it on first boot.

---

### Task 2: Wake-source schema + `Clock` seam + `StreamEvent` variants

**Summary:** Add the `wake_sources` and `wake_rate_limits` tables, the `Clock` trait + `SystemClock` + `TestClock`, and four new `StreamEvent` variants (`WakeSourceRegistered`, `WakeSourceFired`, `WakeSourceFailed`, `WakeSourceRetired`).

**Dependencies:** Task 1

**Files to create/modify:**
- `src/daemon/persistence.rs` — Append migrations for `wake_sources` and `wake_rate_limits` tables. Add `Database::insert_wake_source`, `update_wake_source`, `delete_wake_source`, `list_wake_sources_for_agent`, `list_all_wake_sources`, `get_wake_source`, `bump_wake_source_fire(&self, id, last_fired_at)`. Add `Database::get_or_init_rate_limit(&self, agent_id)`, `update_rate_limit_tokens(&self, agent_id, tokens, last_refill_at)`.
- `src/daemon/clock.rs` (new) — Define `pub trait Clock: Send + Sync { fn now(&self) -> chrono::DateTime<Utc>; }`. Implement `SystemClock` (calls `Utc::now()`) and `TestClock` (`Mutex<DateTime<Utc>>`, with `advance(Duration)` and `set(DateTime<Utc>)` methods).
- `src/daemon/mod.rs` — `pub mod clock;`.
- `src/shared/protocol.rs` — Add four `StreamEvent` variants.
- `src/shared/types.rs` — Add `WakeSource` struct (id, agent_id, kind, config_json, state, fail_reason, last_fired_at, fire_count, created_at) and `WakeSourceKind` enum (`Cron | FileWatch | ParentCompletion`) and `WakeSourceState` enum (`Armed | Failed | Disabled`).

**Detailed specification:**

1. **`wake_sources` schema:**
```sql
CREATE TABLE IF NOT EXISTS wake_sources (
    id              TEXT PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    kind            TEXT NOT NULL,           -- 'cron' | 'file_watch' | 'parent_completion'
    config_json     TEXT NOT NULL,           -- kind-specific config blob
    state           TEXT NOT NULL,           -- 'armed' | 'failed' | 'disabled'
    fail_reason     TEXT,
    last_fired_at   INTEGER,                 -- unix seconds, nullable
    fire_count      INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    FOREIGN KEY(agent_id) REFERENCES agents(id)
);
CREATE INDEX IF NOT EXISTS wake_sources_by_agent ON wake_sources(agent_id);
CREATE INDEX IF NOT EXISTS wake_sources_armed ON wake_sources(state) WHERE state = 'armed';
```

2. **`wake_rate_limits` schema:**
```sql
CREATE TABLE IF NOT EXISTS wake_rate_limits (
    agent_id        TEXT PRIMARY KEY,
    tokens          REAL NOT NULL,
    last_refill_at  INTEGER NOT NULL,
    capacity        INTEGER NOT NULL DEFAULT 60,
    refill_per_sec  REAL NOT NULL DEFAULT 0.01666666,  -- 60 per hour
    FOREIGN KEY(agent_id) REFERENCES agents(id)
);
```

3. **`Clock` trait:**
```rust
pub trait Clock: Send + Sync {
    fn now(&self) -> chrono::DateTime<chrono::Utc>;
}
pub struct SystemClock;
impl Clock for SystemClock { fn now(&self) -> DateTime<Utc> { Utc::now() } }
pub struct TestClock { inner: std::sync::Mutex<DateTime<Utc>> }
impl TestClock {
    pub fn new(t: DateTime<Utc>) -> Self { /* ... */ }
    pub fn advance(&self, d: chrono::Duration) { /* ... */ }
    pub fn set(&self, t: DateTime<Utc>) { /* ... */ }
}
impl Clock for TestClock { /* ... */ }
```

4. **`StreamEvent` variants** (added to `src/shared/protocol.rs:263-390`):
```rust
WakeSourceRegistered { wake_id: String, agent_id: String, kind: String },
WakeSourceFired      { wake_id: String, agent_id: String, mail_id: String, via: Option<String> },  // via = "test" for grim wake test, else None
WakeSourceFailed     { wake_id: String, agent_id: String, reason: String },                         // "rate_limited" | "cwd_gone" | "invalid_cron" | "watch_limit_exceeded" | ...
WakeSourceRetired    { wake_id: String, agent_id: String, reason: String },                         // "user_removed" | "agent_banished"
```
Use serde tag `kind` consistent with existing variants (snake_case rename).

5. **`WakeSourceKind` config_json shapes** (documented as comments next to the enum):
- `Cron`: `{"expr": "0 9 * * 1-5"}`
- `FileWatch`: `{"globs": ["src/api/**/*.rs"], "ignore": ["target/**"], "root": "/abs/path"}` (root is the agent's cwd resolved at registration time)
- `ParentCompletion`: `{"parent_id": "abcd1234", "states": ["complete"]}` (states is one or more of `complete`, `failed`, `banished`)

**Edge cases to handle:**
- Migration runs on a fresh DB — `CREATE TABLE IF NOT EXISTS` is a no-op. No data needed.
- Migration runs on a DB already at this version — idempotent.
- `TestClock` shared across threads — use `Mutex<DateTime<Utc>>`, not `RefCell`.

**Acceptance criteria:**
- [ ] `wake_sources` and `wake_rate_limits` tables exist after `Database::new()` runs on a fresh sqlite file (verified by `SELECT name FROM sqlite_master WHERE type='table'`).
- [ ] `Database::insert_wake_source`, `get_wake_source`, `list_wake_sources_for_agent`, `list_all_wake_sources`, `delete_wake_source`, `update_wake_source` round-trip a `WakeSource` row with all fields preserved.
- [ ] `Database::bump_wake_source_fire(id, ts)` increments `fire_count` by 1 and sets `last_fired_at = ts`.
- [ ] `Clock` trait, `SystemClock`, and `TestClock` are defined; `SystemClock::now()` returns `chrono::Utc::now()` (within 1s of test invocation); `TestClock::new(t).now()` returns `t` exactly; `advance(d)` shifts `now()` by `d`.
- [ ] `TestClock` is `Send + Sync` (compile check that it can be wrapped in `Arc<dyn Clock>`).
- [ ] Four `StreamEvent` variants exist with the specified field shapes; each round-trips through serde `to_value` / `from_value`.
- [ ] `WakeSourceKind` and `WakeSourceState` enums round-trip through `FromStr`/`Display` for DB persistence.

**Contract tests (RED phase):**
- Test file: `tests/wake_schema.rs` (new)
- Tests to write before implementing:
  - `wake_sources_table_exists_after_migrate` — open a fresh DB; query `sqlite_master`; assert both tables present.
  - `insert_and_get_wake_source_roundtrip` — insert a Cron source; `get_wake_source` returns equal row.
  - `list_wake_sources_for_agent_returns_only_that_agents_sources` — seed 3 sources for two agents; assert filter.
  - `bump_wake_source_fire_increments_count_and_sets_timestamp` — insert source; bump twice with different timestamps; assert `fire_count == 2` and `last_fired_at == last_call_ts`.
  - `delete_wake_source_removes_row` — insert, delete, get returns `None`.
- Test file: `tests/clock_seam.rs` (new)
- Tests to write before implementing:
  - `system_clock_now_within_one_second_of_utc_now` — call `SystemClock.now()`; diff vs `Utc::now()` < 1s.
  - `test_clock_advance` — `TestClock::new(t)`, `.advance(1h)`, `.now()` equals `t + 1h`.
  - `test_clock_set` — `.set(t2)` overwrites time.
- Test file: `tests/wake_events.rs` (new)
- Tests to write before implementing:
  - `wake_source_registered_serde_roundtrip` — serialize → JSON → deserialize, assert equal.
  - `wake_source_fired_with_via_test` — `via: Some("test")` round-trips; `via: None` round-trips.
  - `wake_source_failed_with_reason` — variant carries `reason: "rate_limited"` correctly.
  - `wake_source_retired` — variant carries `reason: "agent_banished"` correctly.

**Non-testable items:**
- Wiring `mod clock;` in `daemon/mod.rs` is verified by `cargo check`.

**Notes/Warnings:**
- The `wake_rate_limits` row is created lazily on first fire (Task 6), not on agent creation. The `get_or_init_rate_limit` method handles that.
- Don't add a `wake_id` index on the `mail` table; wake-fire mails are identified by `sender_id LIKE 'wake://%'` and that's adequate.

---

### Task 3: `WakeRegistry` actor + `WakeSource` trait + cron source

**Summary:** Implement the daemon-internal `WakeRegistry` actor with register / list / remove / test paths and a `WakeSource` trait abstraction; ship the cron source as the first concrete implementation, with boot-time replay and missed-fire catch-up.

**Dependencies:** Task 2

**Files to create/modify:**
- `src/daemon/wake_registry.rs` (new) — `WakeRegistry` struct, `WakeSource` trait, `ArmedHandle` enum, in-memory `HashMap<wake_id, ArmedHandle>`, public methods `register`, `remove`, `test_fire`, `list_for_agent`, `list_all`, `replay_on_boot`, `retire_for_agent`.
- `src/daemon/wake_sources/mod.rs` (new) — submodule.
- `src/daemon/wake_sources/cron.rs` (new) — `CronSource` impl of `WakeSource` trait, parses cron expr at construction, evaluates next fire vs `Clock::now()`.
- `src/daemon/mod.rs` — `pub mod wake_registry;` and `pub mod wake_sources;`.
- `src/daemon/server.rs` (or daemon boot wiring) — Construct `WakeRegistry` with `Arc<Database>`, `EventBus`, `Arc<dyn Clock>`, and a `MailSender` seam (initially backed by the daemon's mail-send code path); call `replay_on_boot()` after the agent migration from T1.
- `Cargo.toml` — Add `cron = "0.12"`.

**Detailed specification:**

1. **`WakeSource` trait:**
```rust
#[async_trait::async_trait]
pub trait WakeSource: Send + Sync {
    /// Arm the source. Called on register and on boot replay. May spawn watcher
    /// threads or set up timers. Returns an opaque handle the registry stores.
    fn arm(&self, ctx: &ArmCtx) -> Result<ArmedHandle>;

    /// For time-driven sources (cron): synchronously evaluate whether to fire,
    /// given current clock. Event-driven sources return Vec::new() and rely on
    /// callbacks pushed through `ArmCtx::fire_tx`.
    fn evaluate(&self, ctx: &EvalCtx) -> Vec<FireDecision>;
}

pub struct FireDecision {
    pub body: String,        // wake mail body
    pub now: DateTime<Utc>,  // wall time of fire (used to update last_fired_at)
}

pub enum ArmedHandle {
    Cron,                                          // stateless; registry calls evaluate() on tick
    FileWatch(notify::RecommendedWatcher),         // owns the watcher, dropped on retire
    ParentCompletion(tokio::task::JoinHandle<()>), // owns the subscription task
}
```

2. **`WakeRegistry` shape:**
```rust
pub struct WakeRegistry {
    db: Arc<Database>,
    bus: EventBus,
    clock: Arc<dyn Clock>,
    mail_sender: Arc<dyn WakeMailSender>,  // seam — see below
    handles: tokio::sync::Mutex<HashMap<String, ArmedHandle>>,
    fire_tx: tokio::sync::mpsc::Sender<FireMsg>,
    fire_rx: tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<FireMsg>>>,
}

#[async_trait::async_trait]
pub trait WakeMailSender: Send + Sync {
    async fn send_wake_mail(&self, wake_id: &str, agent_id: &str, body: &str) -> Result<String>; // returns mail_id
}
```

The default `WakeMailSender` impl writes a mail row directly with `sender_id = format!("wake://{}", wake_id)`, `recipient_id = agent_id`, `wake_eligible = true`, `state = Pending`, and publishes `StreamEvent::MailReceived`. This reuses `Database::insert_mail` and the existing event flow.

3. **`register(source: WakeSource, agent_id, kind, config_json) -> Result<wake_id>`**:
   - Generate `wake_<8-hex>` id.
   - Persist row with `state = 'armed'`, `last_fired_at = NULL`, `fire_count = 0`.
   - Call `source.arm(ctx)`; on failure, set `state = 'failed'`, emit `WakeSourceFailed`, return error.
   - Store handle in in-memory map.
   - Publish `WakeSourceRegistered` event.
   - Return `wake_id`.

4. **`remove(wake_id) -> Result<()>`**:
   - Delete from `wake_sources` table.
   - Drop the handle from the map (this drops watchers, aborts tasks).
   - Publish `WakeSourceRetired { reason: "user_removed" }`.

5. **`retire_for_agent(agent_id) -> Result<()>`**: bulk path used by `grim banish`. Deletes all rows; drops all handles; emits `WakeSourceRetired { reason: "agent_banished" }` per source.

6. **`test_fire(wake_id) -> Result<()>`**: bypasses rate limit; calls `mail_sender.send_wake_mail`; bumps fire counter; emits `WakeSourceFired { via: Some("test") }`.

7. **`replay_on_boot()`**:
   - Read all rows where `state = 'armed'`.
   - For each: re-construct the source from `config_json`, call `arm()`, store handle.
   - For cron sources: invoke catch-up — if `last_fired_at` is `NULL` OR `now > next_after(last_fired_at)`, fire once with body `"[catch-up] cron <expr> last fired <ts>"`. Cap at one fire per source.

8. **Cron evaluation loop:**
   - The registry spawns a tokio task that ticks every 30s (configurable later). On each tick, iterates cron sources, calls `evaluate()`, fires any that returned `FireDecision`.
   - `CronSource::evaluate` checks `clock.now() >= next_after(last_fired_at)`; if yes, returns one `FireDecision` with body `"[cron] <expr> fired at <iso8601>"`.
   - Catch-up rule: at most one fire per source per tick (no replay of multiple missed fires within a tick).

9. **Fire path (common to all sources):**
   - Cron: registry tick calls `evaluate()` then fires synchronously.
   - Event-driven: source pushes `FireMsg { wake_id, body }` through `fire_tx`; registry's drain task consumes and fires.
   - `fire(wake_id, body)`:
     - (T6 will inject rate-limit gate here; for T3, skip the gate.)
     - Call `mail_sender.send_wake_mail(wake_id, agent_id, body)` → `mail_id`.
     - `db.bump_wake_source_fire(wake_id, now)`.
     - Publish `WakeSourceFired { wake_id, agent_id, mail_id, via: None }`.

**Edge cases to handle:**
- Cron expr invalid at registration — `arm()` returns Err; row persisted with `state = 'failed'`, `fail_reason = "invalid_cron: <msg>"`; CLI surfaces error.
- Cron expr invalid at boot replay — same: row flipped to `failed`, `WakeSourceFailed` emitted, daemon continues.
- Agent banished after registration — handles get dropped via `retire_for_agent` (T7 wiring); any in-flight fire's mail is left in place (banish-mail handling already exists in mail subsystem).
- Two cron sources on the same agent — both fire independently; mail bus folds bodies via `build_wake_prompt`.
- `register` race with `remove` — `Mutex<HashMap>` serializes; if remove wins, register returns Err (row insert fails on duplicate id; never happens since IDs are random) or the source is created and immediately removed (handle drop is safe).

**Acceptance criteria:**
- [ ] `WakeRegistry::register(CronSource::new("0 * * * *"), "agent_x", ...)` returns `Ok(wake_id)`, persists a row with `state = 'armed'`, and emits `WakeSourceRegistered`.
- [ ] `register` with an invalid cron expression returns `Err`, persists `state = 'failed'`, and emits `WakeSourceFailed { reason: "invalid_cron" }`.
- [ ] When `Clock::now()` crosses a cron schedule boundary, the next registry tick produces a `WakeSourceFired` event and a row in `mail` with `sender_id = "wake://<id>"`, `recipient_id = "<agent_id>"`, `wake_eligible = 1`, `state = 'Pending'`.
- [ ] After a fire, `wake_sources.fire_count` increments by 1 and `last_fired_at` is set to the fire timestamp.
- [ ] `WakeRegistry::test_fire(wake_id)` produces exactly one wake mail and emits `WakeSourceFired { via: Some("test") }`, even if the cron schedule is not due.
- [ ] `WakeRegistry::remove(wake_id)` deletes the row, drops the handle, and emits `WakeSourceRetired { reason: "user_removed" }`.
- [ ] `WakeRegistry::retire_for_agent(agent_id)` removes all that agent's sources and emits one `WakeSourceRetired { reason: "agent_banished" }` per source.
- [ ] `WakeRegistry::replay_on_boot()` re-arms all `state = 'armed'` rows. On replay, a cron source whose `last_fired_at` is older than its previous expected tick fires exactly once (catch-up), regardless of how many ticks were missed.
- [ ] On replay, a cron source with an invalid expression flips to `failed` rather than crashing the daemon.
- [ ] `list_for_agent` and `list_all` return rows with stable field ordering matching the `WakeSource` struct shape.

**Contract tests (RED phase):**
- Test file: `tests/wake_registry_cron.rs` (new)
- Tests to write before implementing:
  - `register_cron_source_persists_and_arms` — register; assert DB row present with `state = 'armed'` and `WakeSourceRegistered` event published.
  - `invalid_cron_expr_fails_registration` — register with `"not a cron"`; assert `Err` and DB row has `state = 'failed'`.
  - `cron_fires_when_clock_crosses_schedule` — `TestClock` set to T0, register `"* * * * *"` (every minute), advance to T0+1min, run registry tick, assert one wake mail row written and one `WakeSourceFired` event.
  - `cron_fire_increments_fire_count` — fire twice, assert `fire_count == 2`.
  - `cron_fire_writes_mail_with_synthetic_sender` — assert `mail.sender_id == "wake://<wake_id>"` and `wake_eligible == 1`.
  - `test_fire_bypasses_schedule` — register cron that's not due; call `test_fire`; assert one wake mail produced and `WakeSourceFired { via: Some("test") }` emitted.
  - `remove_drops_handle_and_emits_retired` — register, remove, assert row gone and `WakeSourceRetired { reason: "user_removed" }` emitted.
  - `retire_for_agent_removes_all_that_agents_sources` — register 3 sources for agent_a, 1 for agent_b; retire agent_a; assert agent_b's source remains, agent_a's are gone.
  - `replay_on_boot_rearms_armed_sources` — pre-seed two `armed` rows in DB, construct registry, call `replay_on_boot`, assert handles map contains both.
  - `cron_catchup_fires_once_after_long_downtime` — seed cron row with `last_fired_at` 24h before `TestClock::now()`; replay; assert exactly one fire (not 24).
  - `replay_with_invalid_cron_marks_failed` — pre-seed armed row with broken expr; replay; assert row flipped to `failed`, daemon continues.
- Test file: `tests/wake_e2e_cron.rs` (new)
- Tests to write before implementing:
  - `cron_fire_through_scheduler_wakes_dormant_agent` — full integration: agent in `Dormant` with session_id, register cron, advance clock, run scheduler tick, assert `MailWaker::wake` is called (extend `RecordingWaker` from `tests/scheduler_mail_wake.rs`).

**Non-testable items:**
- Wiring `WakeRegistry` into `server.rs` boot is verified by daemon-startup integration test (covered by T7's CLI tests).

**Notes/Warnings:**
- Use `Arc<tokio::sync::Mutex<HashMap<...>>>` for `handles`, not `std::sync::Mutex`, because some methods are async.
- The 30s tick interval is configurable later; v1 ships with 30s.
- `cron::Schedule::after(t).next()` is the correct API for "next fire after t". Docs: <https://docs.rs/cron/0.12>.

---

### Task 4: Parent-completion wake source

**Summary:** Implement a `WakeSource` that subscribes to `EventBus` `StateChange` events and fires when a configured parent agent transitions to one of the configured target states.

**Dependencies:** Task 3

**Files to create/modify:**
- `src/daemon/wake_sources/parent_completion.rs` (new) — `ParentCompletionSource` struct; `arm()` spawns a tokio task that subscribes to `bus.subscribe()`, filters for matching `StateChange`, sends `FireMsg` to registry; `evaluate()` returns empty (event-driven).
- `src/daemon/wake_sources/mod.rs` — `pub mod parent_completion;`.

**Detailed specification:**

1. **Config:** `{"parent_id": "abcd1234", "states": ["complete"]}`. `states` is a `Vec<AgentState>` serialized as snake_case strings.

2. **`arm(ctx) -> Result<ArmedHandle>`**:
   - `let mut rx = ctx.bus.subscribe();`
   - Spawn `tokio::task::spawn(async move { while let Ok(ev) = rx.recv().await { ... } })`.
   - Inside the loop: match `StreamEvent::StateChange { agent_id, new_state, .. }` where `agent_id == parent_id` and `new_state ∈ target_states`; send `FireMsg { wake_id, body: format!("[parent {} -> {}]", agent_id, new_state) }` through `ctx.fire_tx`.
   - Return `ArmedHandle::ParentCompletion(join_handle)`.

3. **Default behavior:** If `states` is empty (legacy / not specified), default to `["complete"]`.

4. **Banished parent edge case:** If `parent_id` agent's state has already transitioned to a target state before this source is registered, the source does NOT fire retroactively. The user is expected to register the source before the parent's terminal transition. Document this in CLI help.

5. **Reconciliation on boot:** On `replay_on_boot`, query the parent's current state. If it's already in `target_states` AND `last_fired_at IS NULL`, fire once (so a wake source registered just before a daemon crash isn't lost). Otherwise, just resubscribe.

**Edge cases to handle:**
- Parent ID does not exist — `arm()` succeeds (subscription is on a bus, not a specific agent); the source simply never fires. CLI may warn at register time, but registration still proceeds.
- Parent transitions multiple times (e.g., Active → Dormant → Active → Complete) — fires once per matching transition. If `target_states = ["complete"]`, only the final transition fires.
- Source dropped while task is running — `JoinHandle` aborted on drop; `rx` drops; loop exits cleanly.

**Acceptance criteria:**
- [ ] Registering a `ParentCompletion` source with `states=["complete"]` and a parent that subsequently transitions to `Complete` produces exactly one wake fire (one mail row, one `WakeSourceFired` event).
- [ ] If the parent transitions to `Failed` and `states=["complete"]`, no fire occurs.
- [ ] If `states=["complete","failed"]`, both transitions fire.
- [ ] Banished parent does NOT fire when `states=["complete"]` (default).
- [ ] On replay_on_boot: a registered source whose parent is already in a target state with `last_fired_at IS NULL` fires once.
- [ ] On replay_on_boot: a registered source whose parent is in a target state but `last_fired_at IS NOT NULL` does NOT fire.

**Contract tests (RED phase):**
- Test file: `tests/wake_parent_completion.rs` (new)
- Tests to write before implementing:
  - `fires_on_complete_transition` — register source for parent_id, simulate `StateChange{parent_id, _, Complete}`, assert one wake fire.
  - `does_not_fire_on_non_target_state` — register with states=[complete], emit `StateChange{parent_id,_,Failed}`, assert no fire.
  - `multi_state_filter` — register with states=[complete,failed], emit both, assert two fires.
  - `does_not_fire_for_other_agents` — emit `StateChange{other_id,_,Complete}`, assert no fire.
  - `boot_replay_fires_if_parent_already_terminal_and_never_fired` — pre-seed parent in Complete, source row with `last_fired_at = NULL`; replay; assert one fire.
  - `boot_replay_does_not_double_fire_if_already_fired` — pre-seed parent in Complete, source row with `last_fired_at = some_ts`; replay; assert no new fire.

**Non-testable items:** None.

**Notes/Warnings:**
- Subscribing to `EventBus` from inside a wake source must use `tokio::sync::broadcast::Receiver`. Lagged events (broadcast slow consumer) should log a warn but not crash.

---

### Task 5: File-watch wake source

**Summary:** Implement a `WakeSource` backed by the `notify` crate that fires when files matching the configured glob change. Includes 200ms debouncing, cwd containment, exclusion globs, and self-disable on cwd-gone or watch-limit.

**Dependencies:** Task 3

**Files to create/modify:**
- `src/daemon/wake_sources/file_watch.rs` (new) — `FileWatchSource` struct; `arm()` constructs `notify::RecommendedWatcher`, expands globs to root paths, sets up a debounce timer; on event, sends `FireMsg`.
- `src/daemon/wake_sources/mod.rs` — `pub mod file_watch;`.
- `Cargo.toml` — Add `notify = "6"` and `globset = "0.4"` (for glob matching).

**Detailed specification:**

1. **Config:** `{"globs": ["src/**/*.rs"], "ignore": ["target/**"], "root": "/abs/path"}`. `root` is the agent's cwd (resolved to absolute, canonicalized at registration time). `globs` are interpreted relative to `root`.

2. **`arm(ctx) -> Result<ArmedHandle>`**:
   - Verify `root` exists and is a directory; else return `Err("cwd_gone")`.
   - Build `globset::GlobSet` from `globs` and a separate `GlobSet` for `ignore`.
   - Compute the set of unique directory roots to watch: the shortest common prefix of each glob's literal segments under `root`. Cap at 1000 unique paths after expansion; over-cap returns `Err("watch_limit_exceeded")`.
   - Spawn a tokio task that owns: the `RecommendedWatcher`, a debounce timer (200ms), and a `mpsc::Receiver` from the notify callback.
   - The notify callback pushes events into a `crossbeam_channel::Sender` (notify's API is sync); the task pulls from a tokio channel bridged via `tokio::task::spawn_blocking`.
   - Each incoming event: check the changed path against the include `GlobSet` and NOT match the ignore set. If match, restart the 200ms debounce timer.
   - When the debounce timer fires, send one `FireMsg { wake_id, body: format!("[file-watch] {} changes; first: {}", count, first_path) }` and reset.

3. **Boot reconciliation:** On `replay_on_boot`, walk the watched roots once and compare any matched-path mtime to `last_fired_at`. If any matched path has `mtime > last_fired_at` (or `last_fired_at IS NULL`), fire once with body `[reconcile] paths newer than last_fired_at`.

4. **Cwd containment:** Each glob is resolved to absolute paths under `root`. Any resolved path that escapes `root` (after symlink resolution) is dropped with a warning log; if all paths are dropped, return `Err("path_traversal_blocked")` from `arm()`.

5. **Self-disable on cwd-gone:** If at runtime the watcher receives a `notify::Error` indicating the root no longer exists (or all watched paths disappear), the source flips to `state = 'failed'` with `fail_reason = "cwd_gone"`, emits `WakeSourceFailed { reason: "cwd_gone" }`, and the watcher task exits.

**Edge cases to handle:**
- Globs match no files at registration time — registration succeeds; watcher arms on the parent directories. Future creations under those directories that match the globs will fire.
- File modified rapidly (10 changes in 50ms) — debounce coalesces to one fire, body reflects count.
- Recursive watcher hits inotify limit on Linux — `notify::Error::IoError` with `os.raw_os_error() == Some(28)` (ENOSPC); flip source to `failed`, emit `WakeSourceFailed { reason: "watch_limit_exceeded" }`.
- Ignore globs override include globs (`["src/**/*.rs"]` + `["src/generated/**"]` → ignores generated files).
- Symlink under root pointing outside root — symlink itself is watched (it's under root); the target is not.

**Acceptance criteria:**
- [ ] Registering a file-watch source with `globs=["src/**/*.rs"]` and `root="/tmp/test_repo"` produces an armed handle and persists `state = 'armed'`.
- [ ] Registering with a non-existent `root` returns `Err` and persists `state = 'failed'` with `fail_reason = "cwd_gone"`.
- [ ] Touching a file matching the include glob produces exactly one wake fire after the 200ms debounce window elapses (regardless of how many writes happened in that window).
- [ ] Touching a file matching an ignore glob does NOT fire.
- [ ] Touching a file outside `root` does NOT fire.
- [ ] Deleting `root` after arming flips the source to `state = 'failed'`, emits `WakeSourceFailed { reason: "cwd_gone" }`, and the watcher task exits.
- [ ] Source registration with > 1000 expanded paths returns `Err("watch_limit_exceeded")` and persists `state = 'failed'`.
- [ ] On replay_on_boot: a file under the watched root with `mtime > last_fired_at` produces exactly one reconciliation fire.

**Contract tests (RED phase):**
- Test file: `tests/wake_file_watch.rs` (new) — uses `tempfile::TempDir` for an isolated root.
- Tests to write before implementing:
  - `register_with_missing_root_fails` — root path does not exist; assert Err and state=failed.
  - `single_file_change_fires_after_debounce` — register source on tempdir, write to a matching file, sleep 250ms, assert one fire.
  - `rapid_changes_coalesce_to_one_fire` — write to file 10 times in 50ms; sleep 250ms; assert exactly one fire.
  - `ignore_glob_excludes_match` — globs=`["**/*.rs"]`, ignore=`["target/**"]`; touch `target/x.rs`; assert no fire; touch `src/x.rs`; assert one fire.
  - `path_outside_root_does_not_fire` — touch a sibling tempdir's file; assert no fire.
  - `boot_replay_fires_if_path_newer_than_last_fired` — pre-create file with `mtime = now`; seed source with `last_fired_at = now - 1h`; replay; assert one fire.
  - `boot_replay_no_fire_if_no_paths_newer` — seed source with `last_fired_at = now`; replay; assert no fire.
  - `cwd_disappears_disables_source` — register on tempdir; remove tempdir; sleep; assert source state flipped to failed with reason cwd_gone.

**Non-testable items:**
- The 1000-path cap is verified by a test that constructs a deeply nested tempdir tree with > 1000 files; this test is gated behind `#[cfg(target_os = "linux")]` and `#[ignore]` by default to avoid CI flakiness.

**Notes/Warnings:**
- `notify` callback runs on a dedicated thread. Bridging to tokio: spawn a `std::thread` that owns the watcher and a `crossbeam_channel`, drain into `tokio::sync::mpsc::Sender` via `tokio::task::spawn_blocking` or a polling adapter.
- The 200ms debounce is fixed in v1 (per plan non-goal); make it a `const DEBOUNCE_MS: u64 = 200` for easy future extraction.
- macOS `FSEvents` and Linux `inotify` differ in semantics; `notify::RecommendedWatcher` abstracts this. Tests use real fs events on the host platform.

---

### Task 6: Per-agent token-bucket rate limiter

**Summary:** Add a per-agent token-bucket rate limiter (default 60/hr, configurable per source) intercepting wake fires; rate-limited fires emit `WakeSourceFailed { reason: "rate_limited" }` instead of mail.

**Dependencies:** Task 3

**Files to create/modify:**
- `src/daemon/wake_registry.rs` — Inject rate-limit gate in the common `fire()` path. Add `consume_token(agent_id) -> bool` method.
- `src/daemon/persistence.rs` — Add `Database::get_or_init_rate_limit(agent_id)` and `update_rate_limit_tokens(agent_id, tokens, last_refill_at)`.

**Detailed specification:**

1. **Token-bucket math** (called inside `fire()` before sending mail):
```
row = db.get_or_init_rate_limit(agent_id)
   // row defaults: tokens=capacity, last_refill_at=now, capacity=60, refill_per_sec=1/60
elapsed = clock.now().timestamp() - row.last_refill_at
new_tokens = min(row.tokens + elapsed * row.refill_per_sec, row.capacity)
if new_tokens >= 1.0:
    db.update_rate_limit_tokens(agent_id, new_tokens - 1.0, clock.now().timestamp())
    return Allow
else:
    db.update_rate_limit_tokens(agent_id, new_tokens, clock.now().timestamp())
    return Deny
```

2. **Fire path with rate limit:**
   - Acquire token; if Deny, emit `WakeSourceFailed { reason: "rate_limited" }`, increment fire_count anyway (so observability shows rejected fires), do NOT send mail, return.
   - If Allow, proceed with `mail_sender.send_wake_mail` and `WakeSourceFired`.

3. **Bypass:** `test_fire` (debug action) skips the rate-limit gate.

4. **Per-source override:** When a source is registered with `--capacity` and/or `--refill-per-hour`, those values overwrite the agent-level row. (V1 keeps it agent-scoped per the plan; per-source overrides are recorded but apply at the agent level, last-write-wins. Document this behavior in CLI help.)

**Edge cases to handle:**
- First fire ever for an agent — `get_or_init_rate_limit` creates the row at full capacity, fire allowed.
- Clock jumps backward (NTP correction) — `elapsed` could be negative; clamp at 0 to avoid bonus tokens.
- Multiple fires within the same second — refill is fractional; second fire may still be allowed if remaining tokens >= 1.
- 0 capacity — all fires denied; configurable for "fully manual" sources.

**Acceptance criteria:**
- [ ] First wake fire for a new agent is allowed (token bucket initializes at capacity).
- [ ] After 60 fires within an hour with default refill, the 61st is denied with `WakeSourceFailed { reason: "rate_limited" }`, no mail row written, `fire_count` still incremented.
- [ ] Advancing the `TestClock` by 1 hour after exhausting the bucket allows the next fire (refill restored capacity).
- [ ] `test_fire` succeeds even when the bucket is empty.
- [ ] Setting `capacity = 0` causes every regular fire to be denied (test_fire still allowed).
- [ ] Clock-skew protection: setting `TestClock` backward does NOT add tokens.

**Contract tests (RED phase):**
- Test file: `tests/wake_rate_limit.rs` (new)
- Tests to write before implementing:
  - `first_fire_for_new_agent_allowed` — fresh agent, fire once, assert mail written.
  - `bucket_exhaustion_denies_with_rate_limited` — set capacity=2, fire 3 times back-to-back; assert first two write mail, third emits `WakeSourceFailed{reason="rate_limited"}` and writes no mail.
  - `refill_restores_capacity_over_time` — capacity=2, fire twice (empty bucket), advance TestClock by 1h, fire third time, assert allowed.
  - `test_fire_bypasses_rate_limit` — exhaust bucket, call `test_fire`, assert mail written and event has `via: Some("test")`.
  - `capacity_zero_denies_all_regular_fires` — set capacity=0, fire once, assert denied.
  - `clock_skew_does_not_add_tokens` — exhaust bucket, set TestClock 1h earlier, fire, assert denied.
  - `fire_count_increments_on_rate_limit` — exhaust bucket, fire 3 more times, assert `fire_count` increased by 3 (regardless of denial).

**Non-testable items:** None.

**Notes/Warnings:**
- Rate-limit row updates happen on every fire attempt; index `wake_rate_limits` on `agent_id` (already PK).
- The "increment fire_count even on denial" choice gives operators visibility into wake-storm patterns; document this in `wake list` output.

---

### Task 7: `grim wake` CLI + RPC methods + banish cascade

**Summary:** Add the `grim wake` CLI subcommand group (add/list/remove/test), the corresponding `wake.*` JSON-RPC handlers, and the banish cascade that retires all of an agent's wake sources.

**Dependencies:** Task 3 (registry exists)

**Files to create/modify:**
- `src/cli/commands/wake.rs` (new) — `WakeCommand` enum (Add/List/Remove/Test), per-variant `run` functions calling JSON-RPC.
- `src/cli/commands/mod.rs` — `pub mod wake;`.
- `src/main.rs` — Add `Wake { #[command(subcommand)] cmd: cli::commands::wake::WakeCommand }` to the top-level `Commands` enum; dispatch to `wake::run`.
- `src/daemon/rpc.rs` — Add `handle_wake_add`, `handle_wake_list`, `handle_wake_remove`, `handle_wake_test`. Add `wake.add` / `wake.list` / `wake.remove` / `wake.test` to the method dispatch table.
- `src/shared/protocol.rs` — Add `WakeAddParams`, `WakeListParams`, `WakeRemoveParams`, `WakeTestParams`, and corresponding result types.
- `src/daemon/agent_manager.rs` — In `banish()`, after the state flip, call `wake_registry.retire_for_agent(agent_id)` (registry passed as new field on `AgentManager`).
- `src/daemon/server.rs` — Wire the registry into `AgentManager::new` so banish cascades work.

**Detailed specification:**

1. **CLI shape:**
```
grim wake add <agent-id> --cron "<expr>"                          # cron source
grim wake add <agent-id> --watch "<glob>" [--watch ...] [--ignore "<glob>" ...]
grim wake add <agent-id> --on-parent <parent-id> [--states complete,failed]
grim wake list [<agent-id>]                                        # filtered or all
grim wake remove <wake-id>
grim wake test <wake-id>
```

`add` accepts at most one of `--cron` / `--watch` / `--on-parent`. `--watch` may repeat.

2. **`wake.add` JSON-RPC params:**
```json
{ "agent_id": "abcd1234", "kind": "cron|file_watch|parent_completion", "config": { ... } }
```
Result: `{ "wake_id": "wake_<hex>" }`.

3. **`wake.list` params:** `{ "agent_id": Optional<String> }`. Result: `{ "sources": [WakeSource { id, agent_id, kind, config_json, state, fail_reason, last_fired_at, fire_count, created_at }] }`.

4. **`wake.remove` params:** `{ "wake_id": "wake_<hex>" }`. Result: `{ "success": true }`.

5. **`wake.test` params:** `{ "wake_id": "wake_<hex>" }`. Result: `{ "success": true, "mail_id": "<id>" }`.

6. **CLI human output:**
```
$ grim wake add abc12345 --cron "0 9 * * 1-5"
Wake source wake_a1b2c3d4 registered (cron) for agent abc12345.

$ grim wake list abc12345
ID            KIND               CONFIG                       STATE  LAST FIRED            FIRES
wake_a1b2c3d4 cron               0 9 * * 1-5                  armed  2026-04-25 09:00 UTC      4
wake_e5f6a7b8 file_watch         src/api/**/*.rs (+1 ignore)  armed  -                         0
```

7. **Banish cascade:** `AgentManager::banish` already cleans queues / kills processes / flips state / emits `StateChange`. Add a step: after the state flip, call `self.wake_registry.retire_for_agent(agent_id)`. Errors from retire are logged but do NOT fail the banish (banish must always succeed).

**Edge cases to handle:**
- `wake add` with non-existent agent — RPC returns `agent_not_found` error.
- `wake add --cron` with invalid expression — registration fails at `arm()`; CLI surfaces error (`invalid cron expression: ...`); no row left in `armed` state (row left in `failed` state for diagnosis, but CLI returns nonzero exit).
- `wake list` (no agent_id) on a freshly-installed daemon — empty table.
- `wake remove` with non-existent wake_id — RPC returns `wake_not_found`; CLI exits nonzero.
- `wake test` while agent is `Active` — same as any fire-while-busy: mail enqueued, picked up later. CLI prints `wake fired (mail queued); agent is currently Active`.
- Banish on agent with 0 wake sources — no-op (retire_for_agent returns successfully with empty list).

**Acceptance criteria:**
- [ ] `grim wake add <id> --cron "0 * * * *"` writes a wake_sources row with `kind = 'cron'`, `state = 'armed'`, and prints `wake_<hex>` to stdout.
- [ ] `grim wake add <id> --watch "src/**/*.rs"` writes a row with `kind = 'file_watch'`.
- [ ] `grim wake add <id> --on-parent <pid>` writes a row with `kind = 'parent_completion'` and default `states = ["complete"]`.
- [ ] `grim wake add <id> --on-parent <pid> --states complete,failed` writes a row with `states = ["complete","failed"]`.
- [ ] `grim wake list <id>` returns only that agent's sources.
- [ ] `grim wake list` (no agent_id) returns all sources.
- [ ] `grim wake remove <wake-id>` deletes the row and emits `WakeSourceRetired { reason: "user_removed" }`.
- [ ] `grim wake test <wake-id>` produces a mail row and emits `WakeSourceFired { via: Some("test") }`.
- [ ] `grim banish <agent-id>` removes all that agent's wake sources from the DB and emits one `WakeSourceRetired { reason: "agent_banished" }` per source.
- [ ] `grim wake add` to a non-existent agent fails with exit code != 0 and a `agent_not_found` error message.
- [ ] `grim wake add <id> --cron "not a cron"` fails with exit code != 0 and prints the cron parse error.

**Contract tests (RED phase):**
- Test file: `tests/cli_wake.rs` (new) — black-box CLI tests using a fake daemon in-process, following the pattern of `tests/cli_circle.rs` and `tests/cli_status.rs`.
- Tests to write before implementing:
  - `wake_add_cron_returns_wake_id` — start fake daemon, run `grim wake add <id> --cron "0 * * * *"`, assert exit 0 and stdout contains `wake_`.
  - `wake_add_unknown_agent_fails` — assert exit != 0 and stderr contains `agent_not_found`.
  - `wake_add_invalid_cron_fails` — assert exit != 0 and stderr contains `invalid cron`.
  - `wake_list_filters_by_agent` — register 2 sources for a, 1 for b; `wake list a` returns 2 lines, `wake list b` returns 1.
  - `wake_list_no_agent_returns_all` — assert table includes all 3.
  - `wake_remove_succeeds` — register, remove, list returns empty.
  - `wake_remove_unknown_fails` — assert exit != 0 and stderr `wake_not_found`.
  - `wake_test_fires_immediately` — register cron not due, `wake test <id>`, assert one mail written and `WakeSourceFired{via=test}` event.
- Test file: `tests/banish_cascade.rs` (new)
- Tests to write before implementing:
  - `banish_retires_all_agents_wake_sources` — register 3 sources for agent, banish, assert DB has 0 rows for agent and 3 `WakeSourceRetired{reason=agent_banished}` events.
  - `banish_with_no_wake_sources_succeeds` — banish agent without sources; banish completes normally.
  - `banish_does_not_affect_other_agents_sources` — register sources for two agents, banish one, other's sources remain.

**Non-testable items:**
- CLI human-readable output formatting (table widths, color) is verified by snapshot tests if added later.

**Notes/Warnings:**
- The fake-daemon test harness pattern is in `tests/support/grimw_fake_daemon.rs`. Extend that or add a sibling helper for daemon-side CLI tests.
- Avoid coupling CLI tests to exact wake-id strings; assert the prefix `wake_` and length.

---

### Task 8: `--keep-alive` flag on summon + `invoke` reconciliation

**Summary:** Add `--keep-alive` to `grim summon` so newly-summoned agents land in `Dormant` (not `Complete`) when they finish; collapse `grim invoke` into a thin wrapper over `mail.send --wake-eligible`; remove the dual session-restart code path from `agent_manager.rs`.

**Dependencies:** Tasks 1 and 7

**Files to create/modify:**
- `src/cli/commands/summon.rs` — Add `--keep-alive` (alias `-k`) flag; pass through to `agent.summon` RPC.
- `src/shared/protocol.rs` — Add `keep_alive: Option<bool>` to `SummonParams`.
- `src/daemon/rpc.rs` — `handle_summon` reads `keep_alive`, persists on the agent row.
- `src/daemon/persistence.rs` — Add `keep_alive` boolean column to `agents` table (additive `ALTER TABLE` migration); new `Database::get_keep_alive(agent_id)` and `set_keep_alive`.
- `src/daemon/agent_manager.rs` — On agent termination, branch: if `keep_alive == true` AND `session_id IS NOT NULL`, transition to `Dormant` instead of `Complete`. Remove the dual session-restart path from `invoke()` (now: `invoke()` simply rejects calls on non-Dormant agents — it's used only internally / via mail).
- `src/cli/commands/invoke.rs` — Replace JSON-RPC method from `agent.invoke` to `mail.send` with `to = format!("agent://{}", id)`, `body = message`, `wake_eligible = true`.
- `src/daemon/rpc.rs` — Optionally keep `agent.invoke` as a deprecated alias that internally redirects to `mail.send`; mark for removal in next major.

**Detailed specification:**

1. **`--keep-alive` semantics:**
   - `agents.keep_alive = 1` is set at `agent.summon` time.
   - On natural completion (executor exits 0), `agent_manager` checks `keep_alive`. If true AND `session_id` is set, transition `Active → Dormant`. Otherwise transition `Active → Complete` (existing behavior).
   - `--keep-alive` is the explicit opt-in; without it, agents finish in `Complete` as today.

2. **`invoke` collapse:**
   - `grim invoke <id> "<msg>"` becomes: `client.call("mail.send", { to: "agent://<id>", body: "<msg>", wake_eligible: true, sender_id: null })`.
   - The mail subsystem creates a `Pending` wake-eligible mail; the scheduler's `tick_mail_wake` (already updated in T1 to filter Dormant) picks it up and resumes the session.
   - The old `handle_invoke` RPC path is removed (or kept as an internal alias).
   - `agent_manager::invoke()` becomes: read agent state, require `Dormant` AND `session_id`; otherwise return error. Used only by `MailWaker` impl now.

3. **Behavioral compat:**
   - For users with old DBs: T1's boot migration already promoted Complete-with-session agents to Dormant.
   - `grim invoke <complete-no-session-id>` was always an error; remains an error (the mail bus rejects sending to non-Dormant addresses if we add that guard, OR the scheduler simply never picks the mail up — choose: scheduler-never-picks-up, so the mail sits Pending forever, which is fine because the agent isn't going to wake).

4. **`--keep-alive` output:**
```
$ grim summon "watch the API" --keep-alive
Agent abc12345 summoned (state: queued, keep-alive)
```

**Edge cases to handle:**
- `--keep-alive` without `session_id` — agent runs once, finishes; if executor never produced a `session_id`, agent transitions to `Complete` (no Dormant possible without session). Log a warn.
- Mail sent to a `Complete` (not Dormant) agent — sits Pending; never wakes. Document in `mail send` help.
- Double-invoke: send two messages back-to-back to a Dormant agent — both queue as Pending; scheduler folds them on next wake (existing fold logic).
- `--keep-alive` on a queued agent that fails to start — agent ends in `Failed`, never reaches Dormant.

**Acceptance criteria:**
- [ ] `grim summon "task" --keep-alive` persists `agents.keep_alive = 1`.
- [ ] An agent with `keep_alive = 1` that finishes normally and has a `session_id` transitions to `Dormant`, not `Complete`.
- [ ] An agent with `keep_alive = 0` still transitions to `Complete` on natural finish.
- [ ] An agent with `keep_alive = 1` that finishes without a `session_id` transitions to `Complete` (with a warn-level log).
- [ ] `grim invoke <dormant-id> "msg"` writes a wake-eligible mail row with `recipient_id = <id>`, `body = "msg"`, `wake_eligible = 1`, `state = 'Pending'`.
- [ ] After `grim invoke`, the scheduler's next `tick_mail_wake` picks up the mail and wakes the agent (via the existing path).
- [ ] `grim invoke <complete-id-no-session>` writes the mail but the agent never wakes (mail sits Pending).
- [ ] `agent_manager::invoke()` returns `Err` if called on a non-Dormant agent.

**Contract tests (RED phase):**
- Test file: `tests/keep_alive_summon.rs` (new)
- Tests to write before implementing:
  - `summon_keep_alive_persists_flag` — RPC `agent.summon` with `keep_alive=true`; assert DB row `keep_alive == 1`.
  - `keep_alive_agent_finishes_in_dormant` — full lifecycle: summon with keep-alive, simulate executor exit with session_id, assert state == Dormant.
  - `keep_alive_no_session_finishes_in_complete` — same but executor produces no session_id; assert state == Complete and a warn is logged.
  - `non_keep_alive_agent_finishes_in_complete` — without keep_alive flag, agent ends Complete.
- Test file: `tests/invoke_via_mail.rs` (new)
- Tests to write before implementing:
  - `invoke_writes_wake_eligible_mail` — call `grim invoke <dormant-id> msg`; assert one mail row with `wake_eligible=1`, `state='Pending'`.
  - `invoke_then_scheduler_tick_wakes_agent` — invoke a Dormant agent, run scheduler tick, assert `RecordingWaker::wake` called with the agent_id.
  - `agent_manager_invoke_rejects_non_dormant` — call `agent_manager.invoke()` on Complete agent; assert Err.
  - `invoke_complete_no_session_mail_stays_pending` — invoke a Complete-no-session agent; assert mail row exists; run scheduler tick; assert mail still Pending.

**Non-testable items:**
- Removing the deprecated `agent.invoke` RPC handler (if chosen) is verified by `cargo check` failing if anyone still calls it internally.

**Notes/Warnings:**
- The old code path in `agent_manager::invoke()` that handled Complete→Active resumes is removed. All resumption now flows through `MailWaker::wake` (which already calls `agent_manager::invoke()` internally). After T8, the only caller of `invoke()` is the scheduler's mail-wake path.
- Document `--keep-alive` in `README.md` with a "standing review agent" example pulling from the plan's user journey.

---

## Testing Strategy (TDD)

Implementation follows red-green TDD: for each task, the implementer writes failing contract tests from the acceptance criteria (RED), then implements the minimum code to make them pass (GREEN). Contract tests are immutable once committed.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `tests/dormant_state.rs`, `tests/dormant_migration.rs`, `tests/scheduler_mail_wake.rs` (extend) | 10 tests | Match-arm audits (cargo check); CLI render |
| 2 | `tests/wake_schema.rs`, `tests/clock_seam.rs`, `tests/wake_events.rs` | 12 tests | Module wiring (cargo check) |
| 3 | `tests/wake_registry_cron.rs`, `tests/wake_e2e_cron.rs` | 12 tests | Boot wiring (covered by T7 integration) |
| 4 | `tests/wake_parent_completion.rs` | 6 tests | None |
| 5 | `tests/wake_file_watch.rs` | 8 tests | 1000-path cap test (gated `#[ignore]`) |
| 6 | `tests/wake_rate_limit.rs` | 7 tests | None |
| 7 | `tests/cli_wake.rs`, `tests/banish_cascade.rs` | 11 tests | CLI output formatting |
| 8 | `tests/keep_alive_summon.rs`, `tests/invoke_via_mail.rs` | 8 tests | Old RPC handler removal (cargo check) |

### Integration Testing

- **End-to-end cron wake:** summon a `--keep-alive` agent, register cron source, advance `TestClock`, run scheduler tick, assert agent transitions Dormant → Active. Lives in `tests/wake_e2e_cron.rs` (T3) but extended after T8.
- **End-to-end file-watch wake:** summon a `--keep-alive` agent in a tempdir, register file-watch source, touch a file, sleep past debounce, assert agent transitions to Active. Lives in `tests/wake_e2e_file_watch.rs` (added in T8 cleanup).
- **End-to-end parent-completion wake:** summon parent A, summon child B with `--keep-alive`, register parent-completion source on B → A, complete A, assert B transitions Dormant → Active. `tests/wake_e2e_parent_completion.rs`.
- **Restart catch-up:** seed wake_sources rows with stale `last_fired_at`, restart daemon (in-process), assert one catch-up fire per source. `tests/wake_restart_catchup.rs`.
- **Banish during wake-in-flight:** register source, fire it (mail enqueued), banish agent before scheduler tick, assert mail moves to `Failed` and source row is gone.

### Manual Testing Checklist

- [ ] Run `grim daemon`; in another terminal, `grim summon "watch api" --keep-alive`; `grim wake add <id> --watch "src/api/**/*.rs"`; edit a file; verify the agent wakes (visible via `grim circle` and `grim bind <id>`).
- [ ] Run `grim wake add <id> --cron "* * * * *"`; wait one minute; verify agent wakes.
- [ ] Run `grim wake add <child> --on-parent <parent>`; complete parent; verify child wakes.
- [ ] Run `grim wake list` and verify table layout in a terminal.
- [ ] Run `grim wake test <id>` while agent is Dormant; verify wake.
- [ ] Run `grim wake test <id>` while agent is Active; verify mail queues but no immediate wake.
- [ ] Trigger a wake-storm (cron `* * * * *` + low-capacity rate limit); verify `WakeSourceFailed { reason: "rate_limited" }` events appear in the dashboard / event log.
- [ ] Banish an agent with multiple wake sources; verify all sources are gone via `grim wake list`.
- [ ] Restart `grim daemon` while a Dormant agent has registered sources; verify sources re-arm and fire as expected.
- [ ] Delete the cwd of an agent with a file-watch source; verify the source flips to `failed` with reason `cwd_gone`.

## Rollout Considerations

### Feature Flags

No feature flags. The work is additive at the data layer (new tables, new column with default), and the CLI/RPC surface is new (no existing user-visible behavior changes silently). The boot migration runs unconditionally on first daemon start with the new build.

### Migration Strategy

- **Schema migrations** are additive and idempotent (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE` gated on column existence).
- **Data migration** (`migrate_dormant_agents`) runs once per boot; idempotent due to the `WHERE state = 'complete' AND session_id IS NOT NULL` filter.
- **No downgrade path** — once an agent is Dormant, downgrading the daemon to a pre-Dormant build leaves the row in an unrecognized state. Document in release notes: rollback requires `UPDATE agents SET state = 'complete' WHERE state = 'dormant'`.

### Rollback Plan

If the boot migration causes harm (unlikely given the strict gate):
1. Stop the daemon.
2. Run `sqlite3 grimoire.db "UPDATE agents SET state = 'complete' WHERE state = 'dormant'"`.
3. Run `sqlite3 grimoire.db "DROP TABLE wake_sources; DROP TABLE wake_rate_limits"` (optional; old daemon ignores them).
4. Roll back the daemon binary.

If a wake source misbehaves at runtime:
1. `grim wake list` to find the source ID.
2. `grim wake remove <id>` to retire it.
3. If the daemon is unresponsive, `sqlite3 grimoire.db "UPDATE wake_sources SET state = 'disabled' WHERE id = '<id>'"` and restart.

## Open Items

- [ ] Confirm `cron` crate version `0.12` is the correct one (dual `cron` and `cron_clock` crates exist on crates.io; verify the API matches `Schedule::after(t).next()`).
- [ ] Confirm `notify` 6.x API for the cross-platform recommended-watcher path used in tests; specifically `notify::recommended_watcher` constructor signature.
- [ ] Decide whether to keep `agent.invoke` JSON-RPC method as a deprecated alias (for any external scripts) or remove it outright in T8. Default: keep as alias for one release.
- [ ] Confirm that `grim circle` and `grim status` output formatters need updates beyond just rendering the new state name (e.g., adding a "Dormant" column or summary count).

---

*This spec is implementation-ready. Each task is designed for red-green TDD. Tasks 1-3 form the critical path; tasks 4, 5, 6, and 7 can be parallelized once T3 lands; T8 is the last step.*
