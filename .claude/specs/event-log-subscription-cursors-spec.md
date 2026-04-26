# Implementation Spec: Event Log Subscription Cursors

> Generated from: inline prompt (ROADMAP.md Part 5, item 1 — durable event log; second slice)
> Generated on: 2026-04-24
> Builds on: `.claude/specs/durable-event-log-sqlite-spec.md`

## Overview

The previous slice added an append-only `events` table and a single writer
task. Every `StreamEvent` is now durable, but no consumer can read from the
log — the only way to receive events is still `EventBus::subscribe`, the
in-memory broadcast that drops on slow subscribers and forgets on restart.

This slice adds the **subscription primitive** that consumers will eventually
use to replace the broadcast: a cursor-based, scope-filtered read API plus an
async stream that combines durable catch-up with live tailing. Once it lands,
`grim logs <agent>`, the dashboard, webhooks, and downstream agents can all be
expressed as "subscribe with a cursor and a scope." This spec deliberately
ships *only* the primitive — no CLI, no consumer migrations, no broadcast
retirement. Those are follow-on slices that all depend on this foundation.

## Technical Context

### Relevant Codebase Areas
- `src/daemon/persistence.rs` — owns the `events` table; gains a `Scope` enum and `read_events_since` method.
- `src/daemon/event_bus.rs` — gains a second broadcast channel for `(id, StreamEvent)` and a new `subscribe_durable` API.
- `src/shared/protocol.rs` — gains the `Scope` filter type used by both the DB read and the bus subscribe API.
- `tests/database.rs` — read-API contract tests.
- `tests/event_bus.rs` — persisted-channel tests + small `subscribe_durable` smoke checks.
- `tests/event_log_subscription.rs` — new file, end-to-end behavior of `subscribe_durable` (catch-up + live + lag).

### Existing Patterns to Follow
- **`Mutex<Connection>` lock for reads.** `read_events_since` is a short blocking call, just like every other read on `Database`.
- **`async_stream` / `tokio_stream` already in `Cargo.toml`.** The `subscribe_durable` stream is built with `async_stream::stream!`; no new deps.
- **Non-blocking publish.** The persisted broadcast is fire-and-forget like the existing one; a slow durable subscriber must surface a `Lagged` error rather than back-pressure the writer.
- **Test helpers from the prior slice.** `Database::with_test_conn`, `TempDbPath`, the `all_six_variants()` fixture in `tests/database.rs`, and `fresh_bus_with_db` in `tests/event_bus.rs` are reused.

### Key Dependencies
- `tokio::sync::broadcast` (already present, second channel added).
- `async-stream = "0.3"` (already in `Cargo.toml`).
- `tokio-stream = "0.1"` (already in `Cargo.toml`) — `BroadcastStream` adapter.
- `serde_json` for payload deserialization on read.

### Ambiguity Resolutions

| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| Cursor type | Global `id` vs per-stream `(scope, seq)`? | Global `i64` from the `events.id` PK. | Monotonic by SQLite guarantee; works uniformly across `All`/`Agent`/`Scroll` scopes; `seq` stays as a derived column. |
| Live-tail id race | Can a live subscriber dedupe broadcast vs DB rows when broadcast has no id? | Add a **second** broadcast `persisted_sender: broadcast::Sender<(i64, StreamEvent)>` that the writer task fires *after* `append_event` returns the id. Original broadcast unchanged. | Original broadcast latency stays untouched; ids and payloads can never disagree because they ride together. |
| Scope filter | Where does `Scope` live? | `src/shared/protocol.rs`, alongside `StreamEvent`. | Mirrors `StreamEvent`'s placement; keeps the public surface in one module. |
| `All` vs filtered scope | What about events with both `agent_id = NULL` and `scroll_id = NULL`? | Visible in `Scope::All` only. Not reachable from `Scope::Agent` or `Scope::Scroll`. | Matches the spec-1 invariants for the columns. |
| Lag handling | Auto-recover from broadcast `Lagged` vs surface as error? | Surface `SubscribeError::Lagged { last_id }`. Consumer decides whether to re-call `subscribe_durable(scope, last_id)`. | Hidden auto-recovery would silently change ordering during a lag burst; explicit error keeps the contract honest. |
| Capacity of persisted channel | Same as `CHANNEL_CAPACITY` (1024) or larger? | Separate constant `PERSISTED_CAPACITY = 4096`. | Durable consumers tend to be slower than transient `bind` viewers; oversize the buffer to absorb writer bursts. |
| Page size | Caller-supplied or hard-coded? | `read_events_since(scope, after_id, limit: Option<usize>)`. `None` means unlimited; subscribe uses an internal `CATCHUP_PAGE = 256` and pages until exhausted. | Avoids loading 100k rows into memory on first subscribe to a long-lived agent. |
| Persist failure | What does the persisted channel see when `append_event` errors? | Nothing. Writer logs and skips; live durable subscribers do not see the event. | DB is the source of truth; emitting an id-less event would break the cursor contract. |
| Stream return type | Concrete struct vs `impl Stream` vs `Pin<Box<dyn Stream>>`? | `Pin<Box<dyn Stream<Item = Result<(i64, StreamEvent), SubscribeError>> + Send>>` (`BoxStream`). | Erases `async_stream`'s anonymous future type; lets the API live in trait objects (RPC server, etc.). |
| Scope match for live events | Use the DB columns or `StreamEvent`'s helpers? | The persisted message carries the full `StreamEvent`; filter live events with `event.agent_id()` / `event.scroll_id()` (added in spec 1). | Must agree with the indexed columns by construction; helper methods are the single source of truth. |
| Bus drop while a `subscribe_durable` stream is alive | What happens? | Stream ends gracefully when the persisted broadcast `Sender` drops. No error variant for that case. | Matches `tokio_stream::wrappers::BroadcastStream` semantics. |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | `Scope` + `Database::read_events_since` | None | Medium |
| 2 | Persisted broadcast channel in `EventBus` | 1 | Low |
| 3 | `EventBus::subscribe_durable` combined stream | 2 | High |
| 4 | End-to-end subscription integration test | 3 | Medium |

### Critical Path

Tasks are strictly linear: 1 → 2 → 3 → 4. Task 3 is the brain of this slice
and the only one that crosses both modules; tasks 1 and 2 are mechanical and
each fits in one short commit.

---

### Task 1: `Scope` enum + `Database::read_events_since`

**Summary:** Add a `Scope` filter type in `src/shared/protocol.rs` and a
blocking read method on `Database` that returns durable events past a cursor,
filtered by scope, ordered by `id`.

**Dependencies:** None (uses the `events` table from the prior slice).

**Files to create/modify:**
- `src/shared/protocol.rs` — new public `Scope` enum.
- `src/daemon/persistence.rs` — new `read_events_since` method.
- `tests/database.rs` — contract tests.

**Detailed specification:**

```rust
// src/shared/protocol.rs
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    All,
    Agent(String),
    Scroll(String),
}
```

```rust
// src/daemon/persistence.rs
impl Database {
    /// Read durable events with `id > after_id`, filtered by `scope`,
    /// ordered by `id` ASC, optionally limited to `limit` rows.
    /// Returns `(id, StreamEvent)` pairs; payload is decoded via serde.
    pub fn read_events_since(
        &self,
        scope: &Scope,
        after_id: i64,
        limit: Option<usize>,
    ) -> Result<Vec<(i64, StreamEvent)>>;
}
```

Query shape per scope (limit clause appended only when `Some`):

- `Scope::All`:
  `SELECT id, payload FROM events WHERE id > ?1 ORDER BY id ASC [LIMIT ?2]`
- `Scope::Agent(id)`:
  `SELECT id, payload FROM events WHERE agent_id = ?1 AND id > ?2 ORDER BY id ASC [LIMIT ?3]`
- `Scope::Scroll(id)`:
  `SELECT id, payload FROM events WHERE scroll_id = ?1 AND id > ?2 ORDER BY id ASC [LIMIT ?3]`

The agent and scroll forms ride the indexes added in spec 1 (`idx_events_agent_seq`, `idx_events_scroll_seq`). The `All` form scans by primary key, which is already an index.

Decode each row's `payload` with `serde_json::from_str::<StreamEvent>`. A
deserialization failure is propagated via `Result` — it indicates a corrupt
log row, not a transient error.

**Edge cases to handle:**
- Empty result set (no rows past cursor): return `Ok(vec![])`.
- `after_id = 0` (a fresh subscriber): yields all rows for the scope.
- `limit = Some(0)`: returns `Ok(vec![])` (don't error; degenerate-but-legal).
- `after_id` larger than any existing id: empty result, not an error.
- `Scope::Agent("does-not-exist")`: empty result, not an error.

**Acceptance criteria:**
- [ ] `read_events_since(&Scope::All, 0, None)` after appending 5 events returns 5 rows in ascending `id` order.
- [ ] `read_events_since(&Scope::All, last_id, None)` immediately after the previous call returns `[]`.
- [ ] `read_events_since(&Scope::Agent("A"), 0, None)` returns only rows whose payload's `agent_id() == Some("A")`.
- [ ] `read_events_since(&Scope::Scroll("S"), 0, None)` returns only `ScrollProgress` / `TaskStateChange` rows for scroll `"S"`.
- [ ] Returned `(id, event)` pairs deserialize to events whose `agent_id()` / `scroll_id()` match the indexed columns (round-trip with the writer is exact).
- [ ] `limit = Some(2)` against 5 rows returns the first 2 by `id`; a second call with `after_id` set to the second row's id returns the next ≤2.
- [ ] `limit = Some(0)` returns `Ok(vec![])`.
- [ ] Result ordering is strictly ascending `id` for every scope.
- [ ] Calling against an empty `events` table returns `Ok(vec![])`.

**Contract tests (RED phase):**
- Test file: `tests/database.rs`
- Tests to write before implementing:
  - `read_events_since_all_scope_returns_inserted_rows_in_id_order`
  - `read_events_since_advances_with_cursor` — paginate-by-cursor against the result of the prior call.
  - `read_events_since_filters_by_agent_scope`
  - `read_events_since_filters_by_scroll_scope`
  - `read_events_since_respects_limit`
  - `read_events_since_zero_limit_returns_empty`
  - `read_events_since_unknown_agent_returns_empty`
  - `read_events_since_round_trips_payload_for_each_variant` — reuse `all_six_variants()` fixture.

**Notes/Warnings:**
- Do not try to validate `Scope::Agent(id)` against the `agents` table — events legitimately outlive their agents (per spec 1's "no FK on `agent_id`").
- The method is `pub fn` (synchronous). Subscribe will pin a `tokio::task::spawn_blocking` around it inside the stream.

---

### Task 2: Persisted broadcast channel in `EventBus`

**Summary:** Add a second broadcast channel that the writer task fires *after*
a successful `append_event`, carrying the assigned `id` alongside the event.
Original `publish` / `subscribe` semantics are unchanged.

**Dependencies:** Task 1 only nominally; this task does not call
`read_events_since`, but the cursor type used here (`i64`) must match.

**Files to create/modify:**
- `src/daemon/event_bus.rs` — new field, ctor change, new `subscribe_persisted()` method.
- `tests/event_bus.rs` — new contract tests.

**Detailed specification:**

```rust
const CHANNEL_CAPACITY: usize = 1024;
const PERSISTED_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<StreamEvent>,
    writer: mpsc::UnboundedSender<StreamEvent>,
    persisted: broadcast::Sender<(i64, StreamEvent)>,
}

impl EventBus {
    pub fn new(db: Arc<Database>) -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (persisted, _) = broadcast::channel(PERSISTED_CAPACITY);
        let (writer, mut rx) = mpsc::unbounded_channel::<StreamEvent>();
        let persisted_tx = persisted.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match db.append_event(&event) {
                    Ok(id) => {
                        let _ = persisted_tx.send((id, event));
                    }
                    Err(err) => {
                        tracing::error!(?err, "failed to persist event");
                    }
                }
            }
        });
        Self { sender, writer, persisted }
    }

    pub fn subscribe_persisted(&self) -> broadcast::Receiver<(i64, StreamEvent)> {
        self.persisted.subscribe()
    }
}
```

`publish` is **unchanged**: still sends to the original broadcast and the
mpsc; persisted emission happens off the writer task on its own schedule.

**Edge cases to handle:**
- No persisted subscribers yet: `persisted_tx.send` returns `Err`, ignored.
- Persist failure: error logged (existing behavior); persisted channel sees nothing for that event.
- Writer task drops (e.g., daemon shutdown): persisted channel sender is also dropped, persisted receivers see `Closed`.

**Acceptance criteria:**
- [ ] `EventBus::subscribe_persisted()` returns a receiver that yields `(id, StreamEvent)` for each successfully persisted event in the order the writer commits them.
- [ ] The id from `subscribe_persisted` matches the row's `events.id` column for the same payload.
- [ ] Existing `subscribe()` (broadcast) tests continue to pass — original channel semantics are untouched.
- [ ] Publishing 100 events results in 100 persisted-channel deliveries (poll up to 2 s) when a subscriber is present from the start.
- [ ] When `append_event` is forced to fail (e.g., scope-controlled DB tampering), the persisted channel does **not** see that event.
- [ ] Publish remains non-blocking: 1000 events publish in under 100 ms even with a slow persisted subscriber.

**Contract tests (RED phase):**
- Test file: `tests/event_bus.rs`
- Tests to write before implementing:
  - `subscribe_persisted_yields_id_and_event` — publish one event, assert the persisted receiver yields `(id, event)` whose id matches `SELECT id FROM events`.
  - `persisted_channel_ordering_matches_writer_commit_order` — publish 50 events, assert the persisted receiver yields strictly increasing ids.
  - `existing_broadcast_unchanged_by_persisted_channel` — re-run a representative subset of the prior slice's broadcast tests to prove zero regression.
  - `persisted_channel_empty_on_persist_failure` — drop the database file mid-stream (or use a `Database` wrapping a poisoned mutex) and assert no persisted message is emitted for the failed insert.
  - `publish_remains_non_blocking_with_persisted_subscriber` — analogous to `publish_is_non_blocking` but with a `subscribe_persisted` receiver attached.

**Non-testable items:**
- Exact buffering of the persisted channel before the first subscriber attaches (broadcast drops events with no receivers; this is `tokio::sync::broadcast` semantics, not our contract to assert).

**Notes/Warnings:**
- Do **not** route the original `publish` through `persisted` — they intentionally serve different consumers (legacy in-memory vs. durable cursor-based). Conflating them re-introduces the race we're avoiding.
- Resist any urge to embed the id in `StreamEvent` itself; the id is a property of *durable storage*, not of the event payload.

---

### Task 3: `EventBus::subscribe_durable` — combined catch-up + live stream

**Summary:** Async stream that yields `(id, StreamEvent)` past a caller-supplied
cursor for a given `Scope`. Internally: subscribe to the persisted channel
first (live), then page-read the database from `after_id` up to the current
high-water, then drain the live receiver while filtering by scope and `id >
highwater`. Surfaces `SubscribeError::Lagged { last_id }` when the broadcast
overflows.

**Dependencies:** Tasks 1 and 2.

**Files to create/modify:**
- `src/daemon/event_bus.rs` — `SubscribeError` enum, `subscribe_durable` method.
- `tests/event_bus.rs` — small unit tests for happy-path catch-up and live tail.

**Detailed specification:**

```rust
use futures_core::Stream;  // or re-export via tokio_stream
use std::pin::Pin;

#[derive(Debug, thiserror::Error)] // OR a hand-rolled enum if `thiserror` is not in deps
pub enum SubscribeError {
    Lagged { last_id: i64 },
    Internal(String),
}

pub type DurableStream =
    Pin<Box<dyn Stream<Item = Result<(i64, StreamEvent), SubscribeError>> + Send>>;

impl EventBus {
    pub fn subscribe_durable(&self, scope: Scope, after_id: i64) -> DurableStream;
}
```

Implementation outline (using `async_stream::stream!`):

1. **Subscribe to live first.** Capture a `broadcast::Receiver<(i64, StreamEvent)>` from `self.persisted.subscribe()`. This pins a starting point in the live channel before we read the DB, so any event the writer commits during catch-up will land in the buffer.
2. **Catch-up loop.** Run `Database::read_events_since(&scope, cursor, Some(CATCHUP_PAGE))` (where `CATCHUP_PAGE = 256`) inside `tokio::task::spawn_blocking`. For each row, yield `Ok((id, event))` and advance `cursor = id`. Repeat until the page is short (< `CATCHUP_PAGE` rows). At loop exit, `cursor` is the high-water from disk.
3. **Live drain.** `loop { match recv.recv().await }`:
   - `Ok((id, event))` where `id <= cursor` → drop (already yielded in catch-up).
   - `Ok((id, event))` where `event_matches_scope(&event, &scope)` is false → drop.
   - Else → yield `Ok((id, event))` and set `cursor = id`.
   - `Err(RecvError::Lagged(_))` → yield `Err(SubscribeError::Lagged { last_id: cursor })` and **end the stream**.
   - `Err(RecvError::Closed)` → end the stream gracefully (writer task is gone).
4. **Scope match helper:**
   ```rust
   fn event_matches_scope(event: &StreamEvent, scope: &Scope) -> bool {
       match scope {
           Scope::All => true,
           Scope::Agent(id) => event.agent_id() == Some(id.as_str()),
           Scope::Scroll(id) => event.scroll_id() == Some(id.as_str()),
       }
   }
   ```

**Why the order matters:** subscribing to live *before* reading the DB
guarantees that any event committed during catch-up either (a) appears in the
DB read (already on disk by the time our query runs) or (b) sits in the
broadcast buffer waiting for our drain. Without both halves, an event
committed mid-transition could be missed.

**Edge cases to handle:**
- `after_id` past the current max: catch-up yields nothing; stream proceeds straight to live.
- Empty database, no live activity: stream yields nothing and remains open until the bus drops or the consumer drops.
- Writer fails to persist some event after the catch-up read: it never appears in the live channel either (Task 2's contract). No gap from the consumer's perspective because the missing event was never assigned an id.
- `Lagged` mid-catch-up: not possible — catch-up reads from DB, not the broadcast.
- A live event arrives with `id <= cursor` because the DB read raced ahead of the broadcast emit: drop it (de-dup).
- A live event matches another scope: drop it.

**Acceptance criteria:**
- [ ] `subscribe_durable(Scope::All, 0)` after no activity yields no items immediately and remains open.
- [ ] After publishing 5 events, `subscribe_durable(Scope::All, 0)` yields exactly those 5 in id order, then waits.
- [ ] `subscribe_durable(Scope::All, last_seen_id)` re-entered after a disconnect yields only events with `id > last_seen_id`.
- [ ] `subscribe_durable(Scope::Agent("A"), 0)` against a stream of mixed `A`/`B`/scroll events yields only events whose payload `agent_id() == Some("A")`.
- [ ] `subscribe_durable(Scope::Scroll("S"), 0)` against mixed events yields only `ScrollProgress` / `TaskStateChange` for `S`.
- [ ] Cursor is strictly monotonic in the yielded sequence (every yielded `(id, _)` has `id` strictly greater than the previous yielded id).
- [ ] When the persisted broadcast is overrun (slow consumer + bursts beyond `PERSISTED_CAPACITY`), the next item is `Err(SubscribeError::Lagged { last_id: <cursor at overrun> })` and the stream ends.
- [ ] Catch-up + live transition delivers each event exactly once: publishing N events while a consumer is mid-catch-up still produces a single, ordered, gap-free stream of N items at the consumer.
- [ ] Dropping the `EventBus` while a `subscribe_durable` stream is live causes the stream to end (return `None`) without yielding `Err`.

**Contract tests (RED phase):**
- Test file: `tests/event_bus.rs` (smoke / unit-level)
- Tests to write before implementing:
  - `subscribe_durable_yields_existing_events_then_live` — publish 3, subscribe with `after_id=0`, verify catch-up of 3, then publish 2 more and verify they arrive live.
  - `subscribe_durable_respects_after_id_cursor` — publish 5, subscribe with `after_id` = id of 3rd event, verify only the last 2 arrive.
  - `subscribe_durable_filters_by_agent_scope` — publish mixed events, assert filtering.
  - `subscribe_durable_yields_lagged_on_overrun` — bound `PERSISTED_CAPACITY` to a small size in a test-only setter (or assert via timing), publish far more than capacity faster than the consumer drains, assert next item is `Err(Lagged { last_id })` and the stream ends.
  - `subscribe_durable_dedupes_catchup_vs_live` — race-trigger the overlap by spawning a publisher inside the catch-up window and asserting no duplicate ids reach the consumer.

**Non-testable items:**
- Internal `CATCHUP_PAGE` paging behavior — a single-test consumer will see only the aggregate result.
- The exact moment of subscription vs. DB read inside the stream (the contract is "no event committed during transition is missed," which is testable; the implementation detail isn't).

**Notes/Warnings:**
- The DB read MUST happen inside `tokio::task::spawn_blocking` (or equivalent) — `Database` uses a blocking `Mutex`, and a long catch-up should not stall the runtime.
- `BroadcastStream` from `tokio_stream::wrappers` is *not* sufficient here, because catch-up + live require interleaved DB and channel sources. Roll the stream by hand with `async_stream::stream!`.
- `thiserror` is **not** currently in `Cargo.toml`. Either add it for `SubscribeError` (preferred — already idiomatic in the ecosystem and used by `anyhow`) or hand-roll the `Display`/`Error` impls. Implementer's call; document the choice in the commit.
- Do not wire `subscribe_durable` into any existing call site (RPC, dashboard, `bind`) in this slice. That's the next slice.

---

### Task 4: End-to-end subscription integration test

**Summary:** A standalone integration test that exercises the full
catch-up + live + scope + lag contract against a real `EventBus` +
`Database`. Mirrors the durability test from spec 1 in shape.

**Dependencies:** Task 3.

**Files to create/modify:**
- `tests/event_log_subscription.rs` — new integration test file.

**Detailed specification:**

Three scenarios, one test per scenario, all using
`Database::open_in_memory()` and the runtime helpers already defined in
`tests/event_bus.rs` (extracted into a small inline helper here as well —
no new shared crate).

**Scenario A — `catchup_then_live_round_trip`:**
1. Open in-memory `Database`, construct `EventBus`.
2. Publish 50 events spanning all six variants for agent `"A"` and scroll `"S"`.
3. Wait for the writer to drain (poll `events` count to 50, 2s timeout).
4. Call `subscribe_durable(Scope::All, 0)`.
5. While reading the stream, concurrently publish 50 more events.
6. Assert: collect the first 100 stream items. Ids are strictly monotonic.
7. Assert: collected `(id, event)` pairs match `SELECT id, payload FROM events ORDER BY id` exactly.

**Scenario B — `scope_filtering_during_catchup_and_live`:**
1. Pre-publish 30 events split across `agent="A"` (10), `agent="B"` (10), `scroll="S"` (10).
2. Drain to disk.
3. Subscribe with `Scope::Agent("A")`, `after_id = 0`. Read 10 items.
4. While still draining, publish 5 more `A` events, 5 more `B`, 5 more `S`.
5. Read 5 more items from the stream.
6. Assert: every yielded event has `agent_id() == Some("A")`. No `B`s, no `S`s.

**Scenario C — `lagged_error_surfaces_with_resumable_cursor`:**
1. Construct a bus with a small persisted-channel capacity (provide a test-only constructor `EventBus::with_capacities(db, broadcast_cap, persisted_cap)`; production stays on defaults).
2. Subscribe with `Scope::All`, `after_id = 0`.
3. Read the first event (any), record its id as `last_seen`.
4. Stop reading from the stream. From a separate task, publish enough events to exceed the persisted capacity by 2× while the consumer is paused.
5. Resume reading. Drain until either:
   - Stream returns `Err(SubscribeError::Lagged { last_id })`, or
   - 1 s timeout (fail).
6. Assert `last_id >= last_seen` (cursor advances at least to the last successfully yielded id).
7. Re-subscribe with `subscribe_durable(Scope::All, last_id)`.
8. Drain to completion. Assert: no event with `id > last_id` is missing — every row from the DB past `last_id` is in the resumed stream.

**Edge cases to handle:**
- Catch-up of 0 rows on a fresh DB: stream simply waits for live events.
- Live publishing during catch-up: the test does this explicitly in Scenario A.
- Re-subscribing twice with the same cursor must yield the same set of events (idempotent read).

**Acceptance criteria:**
- [ ] All three scenarios pass reliably (5 consecutive runs each).
- [ ] No flakes under default thread count (`cargo test` without `-j 1`).
- [ ] The end-to-end test completes in under 5 seconds total.
- [ ] No `Lagged` error appears in scenarios A or B.
- [ ] Scenario C produces a `Lagged` error; the resumed subscription fills exactly the gap (every `id > last_id` is delivered).

**Contract tests (RED phase):**
- Test file: `tests/event_log_subscription.rs`
- Tests to write before implementing:
  - `catchup_then_live_round_trip`
  - `scope_filtering_during_catchup_and_live`
  - `lagged_error_surfaces_with_resumable_cursor`

**Notes/Warnings:**
- Scenario C requires a test-only `EventBus::with_capacities` constructor. Annotate it `#[doc(hidden)]` and gate the doc with `#[allow(dead_code)]` so the lib build stays warning-free (mirror the `with_test_conn` precedent).
- This test does **not** involve the daemon, RPC, UDS, or HTTP. Keep it lightweight, like `event_log_durability.rs` from spec 1.
- Do not re-test things already covered by Task 3's smoke tests; the integration test's job is to prove they compose under realistic concurrency.

---

## Testing Strategy (TDD)

Same red-green discipline as the prior slice: each task's contract tests are
written first and frozen once committed. Implementation makes them pass.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `tests/database.rs` | 8 (read API: scope, cursor, limit, round-trip) | None |
| 2 | `tests/event_bus.rs` | 5 (persisted channel: id match, ordering, isolation, failure path, non-blocking) | Pre-subscriber buffering |
| 3 | `tests/event_bus.rs` | 5 (subscribe_durable: catch-up, cursor, scope, lag, dedup) | Internal paging cadence |
| 4 | `tests/event_log_subscription.rs` | 3 (end-to-end: round-trip, scope, lag-resume) | None |

### Integration Testing
- Task 4 is the cross-task integration test.
- Existing `tests/event_log_durability.rs` (spec 1) and `tests/scroll_lifecycle.rs` must still pass — `subscribe_durable` is purely additive.

### Manual Testing Checklist
- [ ] Start `grimd`, run `grim summon "echo hello"`.
- [ ] In a Rust REPL or temporary binary: open the daemon's DB read-only (`Database::open` + immediate read), call `read_events_since(&Scope::All, 0, None)`, confirm `output` and `state_change` rows are present.
- [ ] Restart `grimd`. Repeat. Old rows are still visible; new rows append after them in id order.
- [ ] (Optional) Add a temporary `tracing::info!` inside the writer task to log `(id, kind)` for each persisted event; remove before merge.

## Rollout Considerations

### Feature Flags
None. Pure addition: a new `Scope` type, a new method on `Database`, a new
field and method on `EventBus`. No existing call site changes behavior.

### Migration Strategy
- No schema change — uses the `events` table from spec 1 as-is.
- No data migration.
- No downgrade path needed; older `grimd` ignores the new fields and methods.

### Rollback Plan
- Revert the merge. The persisted broadcast disappears with no consumers
  affected, and the `events` table continues to grow under spec 1's writer.

## Open Items

- [ ] Decide whether to add `thiserror = "1"` to `Cargo.toml` for `SubscribeError`. Default is **yes** (smaller, idiomatic, used widely with `anyhow` which is already in deps); document the choice in the implementing commit.
- [ ] Confirm `async-stream` and `tokio-stream` versions in `Cargo.toml` are sufficient for `BoxStream` returns (they are at the time of writing).
- [ ] Future slice: `grim logs <scope>` CLI on top of `subscribe_durable`. Out of scope here.
- [ ] Future slice: migrate `bind`, dashboard, and webhook consumers off the legacy broadcast onto `subscribe_durable`. Out of scope here. After all consumers migrate, the legacy broadcast can be removed.
- [ ] Future slice: retention. Once the durable log is the source of truth for live consumers, an event-log retention policy (size cap, age cap, or per-scope retention) becomes load-bearing.

---

*This spec is implementation-ready. Each task is designed for red-green TDD.
Tasks 1–4 must be completed in order; each is a single commit.*
