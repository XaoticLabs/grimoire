# Implementation Spec: Durable Event Log (SQLite) — Foundation

> Generated from: inline prompt (ROADMAP.md Part 5, Deliverable 1)
> Generated on: 2026-04-24

## Overview

The daemon currently emits every state change, stdout line, and scroll update
through a single `tokio::broadcast` channel (`src/daemon/event_bus.rs`). The
channel is in-memory and lossy — a slow subscriber silently drops events, and
everything is forgotten when `grimd` restarts. This is the first atomic step
toward the "durable event log" entry in ROADMAP.md Part 5: a SQLite-backed
append-only log that captures every `StreamEvent` without changing any consumer
behavior.

Scope for this deliverable is deliberately narrow: add the table, add a single
writer task, persist every published event. No new read API, no CLI surface, no
retention, no migration of existing consumers. The broadcast channel stays
exactly as-is. Subsequent roadmap slices (subscription cursors, `grim logs`,
retiring the broadcast) all depend on this foundation.

## Technical Context

### Relevant Codebase Areas
- `src/daemon/event_bus.rs` — 27 lines today; gains an mpsc channel + spawned writer task.
- `src/daemon/persistence.rs` — hand-rolled SQLite schema + `Mutex<Connection>`; new table and `append_event` method land here.
- `src/shared/protocol.rs` — `StreamEvent` enum with 6 variants; gains `kind()` and `scroll_id()` helpers alongside the existing `agent_id()`.
- `src/daemon/mod.rs` — wires `Database` + `EventBus`; ctor call-site updates.
- `tests/event_bus.rs` — existing bus tests; ctor updates + new persistence tests.
- `tests/database.rs` — existing persistence tests; new schema + `append_event` tests.

### Existing Patterns to Follow
- **Hand-rolled migrations.** `Database::migrate()` uses `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`. No migration framework — just add to the batch.
- **Single `Mutex<Connection>`.** All writes go through the same lock; writers are short and blocking. The new writer task follows the same model.
- **Fire-and-forget publish.** `EventBus::publish` never returns an error; the mpsc write should behave the same way.
- **Serde tag conventions.** `StreamEvent` already has `#[serde(rename = "...")]` tags per variant — reuse those as the `kind` column value so the payload column and the `kind` column stay consistent by construction.
- **In-memory test database.** `Database::open_in_memory()` exists; all new unit tests should use it.

### Key Dependencies
- `rusqlite` (already in `Cargo.toml`).
- `serde_json` (already used for `AgentEvent.payload`).
- `tokio::sync::{broadcast, mpsc}` (broadcast already present).

### Ambiguity Resolutions
| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| Sequence scope | Should `seq` be global, per-agent, or per-scroll? | Per-`agent_id` when present, else per-`scroll_id`, else NULL. Global ordering is covered by `id`. | Future replay will tail from `(agent_id, seq)` offsets; roadmap Part 3 item 4. |
| Writer channel bounds | Bounded (back-pressure) vs unbounded? | Unbounded `mpsc`. | Keeps `publish` non-blocking — matches current broadcast semantics. Back-pressure is a later deliverable. |
| Writer failure handling | Crash daemon, retry, or drop? | Log via `tracing::error!` and drop. | Durable log is additive in this slice; strict delivery guarantees come with the consumer cutover. |
| Payload encoding | Columns per field vs single JSON blob? | One `payload TEXT` column holding `serde_json::to_string(&event)`. | Simplest round-trippable; schema stays stable as the enum grows. |
| Schema location | New module or extend `persistence.rs`? | Extend `Database::migrate()` in `persistence.rs`. | Matches every other table in the codebase. |
| `kind` column source | Derived from pattern match, or serde tag? | Add `StreamEvent::kind(&self) -> &'static str` returning the serde rename tag. | Single source of truth; kind and payload can't drift. |
| Shutdown | Drain mpsc on drop? | Best-effort: writer task runs until the mpsc sender is dropped; daemon shutdown waits briefly but does not block indefinitely. | Matches current "no graceful drain" posture; full drain lands with durable-queue work. |
| `Output` events volume | Persist every stdout line? | Yes, for this deliverable. Retention is Deliverable 5. | The whole point is durability; selective persistence is a product decision, not a foundation one. |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | Add `events` table + indexes to migration | None | Low |
| 2 | `Database::append_event` + `StreamEvent::{kind, scroll_id}` helpers | 1 | Medium |
| 3 | Single-writer task wired into `EventBus` | 2 | Medium |
| 4 | Crash-recovery integration test | 3 | Low |

### Critical Path

Tasks are strictly linear: 1 → 2 → 3 → 4. Nothing parallelizes usefully; each
task is a single commit. Task 3 is the only one that touches call-sites
(`src/daemon/mod.rs` and any tests constructing `EventBus`); keep those edits
mechanical.

---

### Task 1: Add `events` table + indexes to migration

**Summary:** Extend `Database::migrate()` with an append-only `events` table and
two indexes. No reader or writer yet.

**Dependencies:** None.

**Files to create/modify:**
- `src/daemon/persistence.rs` — append to the existing `execute_batch` in `migrate()`.

**Detailed specification:**

Add to the existing migration, after the scroll tables:

```sql
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY,
    agent_id    TEXT,
    scroll_id   TEXT,
    seq         INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    payload     TEXT NOT NULL,
    ts          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_agent_seq  ON events(agent_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_scroll_seq ON events(scroll_id, seq);
```

Idempotent by `IF NOT EXISTS`. No foreign key to `agents(id)` — events may
reference agents that no longer exist (e.g., after deletion) and future events
may have neither agent nor scroll.

**Edge cases to handle:**
- Opening an existing DB file (from the current schema) must succeed and create
  the new table without error.
- Repeated `Database::open` on the same file must be a no-op.

**Acceptance criteria:**
- [ ] After `Database::open_in_memory()`, `SELECT name FROM sqlite_master WHERE type='table' AND name='events'` returns exactly one row.
- [ ] Both indexes exist: `SELECT name FROM sqlite_master WHERE type='index' AND name IN ('idx_events_agent_seq','idx_events_scroll_seq')` returns two rows.
- [ ] Opening a fresh DB file, closing it, and re-opening the same file path succeeds without error (migration is idempotent).
- [ ] All pre-existing tests in `tests/database.rs` continue to pass unmodified.

**Contract tests (RED phase):**
- Test file: `tests/database.rs`
- Tests to write before implementing:
  - `events_table_exists_after_migration` — asserts the table is present after `open_in_memory()`.
  - `events_indexes_exist_after_migration` — asserts both indexes are present.
  - `migrate_is_idempotent` — opens the same tempfile path twice, asserts both calls succeed.

**Notes/Warnings:**
- No FK on `agent_id`/`scroll_id`: we want the log to outlive row deletions.
- Do not add a `NOT NULL` on `agent_id` — scroll events legitimately have none.

---

### Task 2: `Database::append_event` + `StreamEvent::{kind, scroll_id}` helpers

**Summary:** Blocking insert method that serializes a `StreamEvent`, computes
scope + seq, and writes one row. Adds two small accessors on `StreamEvent`.

**Dependencies:** Task 1.

**Files to create/modify:**
- `src/shared/protocol.rs` — add `StreamEvent::kind(&self) -> &'static str` and `StreamEvent::scroll_id(&self) -> Option<&str>`.
- `src/daemon/persistence.rs` — add `pub fn append_event(&self, event: &StreamEvent) -> Result<i64>`.

**Detailed specification:**

`StreamEvent::kind` returns the serde rename tag per variant:
- `Output` → `"output"`
- `StateChange` → `"state_change"`
- `AgentCreated` → `"agent_created"`
- `AgentEvent` → `"agent_event"`
- `ScrollProgress` → `"scroll_progress"`
- `TaskStateChange` → `"task_state_change"`

`StreamEvent::scroll_id` returns `Some(&scroll_id)` for `ScrollProgress` and
`TaskStateChange`, `None` for all other variants.

`Database::append_event`:
1. Lock the connection mutex.
2. Start a single `BEGIN IMMEDIATE` transaction so the seq lookup + insert are
   atomic against concurrent writers (even though we plan only one).
3. Determine scope: `agent_id = event.agent_id()`, `scroll_id = event.scroll_id()`.
4. Compute `seq`:
   - If `agent_id` is `Some`: `SELECT COALESCE(MAX(seq) + 1, 0) FROM events WHERE agent_id = ?1`.
   - Else if `scroll_id` is `Some`: same query against `scroll_id`.
   - Else: `seq = 0`.
5. Serialize payload: `serde_json::to_string(event)?`.
6. Timestamp: `chrono::Utc::now().to_rfc3339()`.
7. `INSERT INTO events (agent_id, scroll_id, seq, kind, payload, ts) VALUES (...)`.
8. Commit, return `last_insert_rowid()`.

**Edge cases to handle:**
- Event with both `agent_id` and `scroll_id` None (none exist today, but the
  signature allows it): row is inserted with both columns NULL and `seq = 0`.
- Serde failure: propagate via `Result` — caller logs and drops.
- Two events with the same `agent_id` inserted in rapid succession must receive
  adjacent, monotonic `seq` values — guaranteed by the transaction.

**Acceptance criteria:**
- [ ] For each of the 6 `StreamEvent` variants, `append_event` inserts exactly one row whose `kind` equals the serde rename tag.
- [ ] `SELECT payload FROM events WHERE id = ?` round-trips: `serde_json::from_str::<StreamEvent>(payload)` equals the original event (via `Debug`/manual field comparison).
- [ ] For two consecutive `Output` events with `agent_id = "A"`, the second row has `seq = first.seq + 1`.
- [ ] For a `ScrollProgress` event with `scroll_id = "S"` inserted between two `A` events, the scroll row has `seq = 0` and the two `A` rows have seqs `0, 1`.
- [ ] `scroll_id` column is non-NULL only for `ScrollProgress` and `TaskStateChange` rows; NULL otherwise.
- [ ] `agent_id` column is non-NULL for `Output`, `StateChange`, `AgentCreated`, `AgentEvent`; NULL for `ScrollProgress`, `TaskStateChange`.
- [ ] Global `id` column is strictly monotonic across successive inserts (SQLite `INTEGER PRIMARY KEY` guarantee).
- [ ] Return value equals the row's `id`.

**Contract tests (RED phase):**
- Test file: `tests/database.rs`
- Tests to write before implementing:
  - `append_event_round_trips_payload` — insert each variant, reload, deserialize, compare.
  - `append_event_sets_kind_per_variant` — parametrized: each variant produces the expected `kind` string.
  - `append_event_seq_monotonic_per_agent` — three events for same agent → seqs `0,1,2`.
  - `append_event_seq_monotonic_per_scroll` — same for scroll-scoped events.
  - `append_event_scopes_are_independent` — interleaved agent + scroll events → each stream restarts from 0.
  - `append_event_populates_scroll_id_only_for_scroll_variants` — exhaustive variant check.
  - `append_event_returns_monotonic_id` — three inserts → returned ids strictly increasing.

**Notes/Warnings:**
- Keep `append_event` signature blocking; the async writer task calls it directly. Do not sprinkle `tokio::spawn_blocking` here — the caller owns that decision.
- `BEGIN IMMEDIATE` avoids the "busy" path under WAL when we later add concurrent readers.

---

### Task 3: Single-writer task wired into `EventBus`

**Summary:** `EventBus` gains an unbounded mpsc sender. A spawned task consumes
it and calls `Database::append_event`. `publish` sends to both broadcast and
mpsc; neither path blocks.

**Dependencies:** Task 2.

**Files to create/modify:**
- `src/daemon/event_bus.rs` — add mpsc sender + `new(db: Arc<Database>)` constructor that spawns the writer.
- `src/daemon/mod.rs` — update the `EventBus::new()` call at line 47 to pass the `Arc<Database>`.
- `tests/event_bus.rs` — update existing tests to construct the bus with an in-memory DB; add new tests.

**Detailed specification:**

```rust
pub struct EventBus {
    sender: broadcast::Sender<StreamEvent>,
    writer: mpsc::UnboundedSender<StreamEvent>,
}

impl EventBus {
    pub fn new(db: Arc<Database>) -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (writer, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let Err(err) = db.append_event(&event) {
                    tracing::error!(?err, "failed to persist event");
                }
            }
        });
        Self { sender, writer }
    }

    pub fn publish(&self, event: StreamEvent) {
        let _ = self.sender.send(event.clone());
        let _ = self.writer.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StreamEvent> {
        self.sender.subscribe()
    }
}
```

When the last `EventBus` is dropped, the mpsc sender drops, `rx.recv().await`
returns `None`, and the writer task exits cleanly. There is no explicit join —
daemon shutdown does not block on it.

**Edge cases to handle:**
- Publisher publishing before any broadcast subscriber exists: already handled by the existing `let _ = send` pattern; unchanged here.
- DB write failure mid-stream (e.g., disk full): `tracing::error!`, skip, keep looping. The broadcast path still delivers.
- Rapid publish burst: `publish` returns immediately; writer catches up asynchronously.

**Acceptance criteria:**
- [ ] `EventBus::new` takes `Arc<Database>`; calling it with an in-memory DB succeeds.
- [ ] After `bus.publish(e)` and a bounded wait (poll up to 2s), `SELECT COUNT(*) FROM events` reflects each published event.
- [ ] Every existing `tests/event_bus.rs` test passes after the ctor update (broadcast semantics unchanged).
- [ ] Publishing 1000 events from the main task returns in under 100 ms (writer may still be draining in the background).
- [ ] Broadcast subscribers receive each event regardless of persistence success/failure.
- [ ] Dropping the `EventBus` causes the writer task to terminate without panic.
- [ ] `src/daemon/mod.rs:47` compiles and runs with the new signature.

**Contract tests (RED phase):**
- Test file: `tests/event_bus.rs`
- Tests to write before implementing:
  - `publish_persists_to_database` — construct with in-memory `Arc<Database>`, publish N events across variants, poll DB until rowcount == N (timeout 2s).
  - `broadcast_subscribers_still_receive_with_persistence_enabled` — analogous to today's `single_subscriber_receives_events` but with the new ctor.
  - `publish_is_non_blocking` — publish 1000 events; assert elapsed < 100ms on the publishing thread (writer runs in the background).
  - `mixed_variants_all_persisted` — publish one of each variant, assert distinct `kind` values in the DB.
  - `dropping_bus_shuts_down_writer_cleanly` — construct bus inside a scope, exit the scope, assert no panic and no leaked task (smoke-level; acceptable to just let `#[tokio::test]` reap).

**Non-testable items:**
- The `tracing::error!` log on DB failure — observable but not asserted in unit tests.
- Wiring in `src/daemon/mod.rs` — exercised by existing daemon smoke tests (if any) and by Task 4's integration test.

**Notes/Warnings:**
- `event.clone()` is required because broadcast and mpsc each need an owned copy. `StreamEvent` already derives `Clone`.
- Resist the urge to make this a bounded channel in the same PR — that changes publish semantics and is explicitly a later deliverable.
- Do not add a shutdown barrier; a clean mpsc drop is enough.

---

### Task 4: Crash-recovery integration test

**Summary:** End-to-end durability check — publish a mix of events through a
real `EventBus` + `Database` pair, drop both, reopen the same DB path, verify
every event is on disk and per-stream seq is contiguous.

**Dependencies:** Task 3.

**Files to create/modify:**
- `tests/event_log_durability.rs` — new integration test file.

**Detailed specification:**

Using `tempfile::NamedTempFile` for the DB path:

1. Open `Database` at `path`, wrap in `Arc`.
2. Construct `EventBus::new(db.clone())`.
3. Publish a known sequence: 3 `Output` events for agent `"A"`, 2 `ScrollProgress` events for scroll `"S"`, 1 `AgentCreated` for agent `"B"`, 2 more `Output` for `"A"`.
4. Poll `SELECT COUNT(*) FROM events` until it reaches 8 or 2-second timeout.
5. Drop `EventBus`. Drop `Database` (ensure the file handle is released; may need to drop the `Arc` and yield).
6. Reopen `Database` at the same path.
7. Assertions:
   - `SELECT COUNT(*) FROM events` = 8.
   - `SELECT payload FROM events ORDER BY id` yields events in publish order.
   - `SELECT seq FROM events WHERE agent_id = 'A' ORDER BY id` = `[0, 1, 2, 3, 4]`.
   - `SELECT seq FROM events WHERE scroll_id = 'S' ORDER BY id` = `[0, 1]`.
   - `SELECT seq FROM events WHERE agent_id = 'B'` = `[0]`.

**Edge cases to handle:**
- Writer task may still be draining when we drop — polling covers this.
- On some platforms SQLite holds a lock briefly after close; if the reopen flakes, add a small retry window (not a sleep loop — a bounded poll).

**Acceptance criteria:**
- [ ] `events_persist_across_database_reopen` passes reliably (5 consecutive runs).
- [ ] Per-agent seq for `"A"` is exactly `[0, 1, 2, 3, 4]` after reopen.
- [ ] Per-scroll seq for `"S"` is exactly `[0, 1]` after reopen.
- [ ] Publish order matches `id` order after reopen.
- [ ] Test completes in under 5 seconds.

**Contract tests (RED phase):**
- Test file: `tests/event_log_durability.rs`
- Tests to write before implementing:
  - `events_persist_across_database_reopen` — the full scenario above.
  - `per_agent_seq_is_contiguous_after_reopen` — isolates the seq assertion.
  - `publish_order_preserved_across_reopen` — isolates the `id`-order assertion.

**Notes/Warnings:**
- This test spawns a tokio runtime but not a full daemon — keep it lightweight. No UDS, no HTTP, no `AgentManager`.
- Do not SIGKILL a subprocess here; the goal is to prove that once the writer has persisted an event, closing + reopening preserves it. Actual daemon-crash recovery lives in a future deliverable once we have a real shutdown story.

---

## Testing Strategy (TDD)

Implementation follows red-green TDD: for each task, the implementer writes
failing contract tests from the acceptance criteria (RED), then implements the
minimum code to make them pass (GREEN). Contract tests are immutable once
committed.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `tests/database.rs` | 3 (schema/index/idempotency) | None |
| 2 | `tests/database.rs` | 7 (round-trip, kind, seq, scopes, scroll_id, id) | None |
| 3 | `tests/event_bus.rs` | 5 (persist, broadcast, non-block, variants, drop) | `tracing::error!` log; `daemon/mod.rs` wiring |
| 4 | `tests/event_log_durability.rs` | 3 (reopen, seq, order) | None |

### Integration Testing
- Task 4 is the cross-task integration test. It exercises Tasks 1–3 end-to-end with a real SQLite file.
- Existing `tests/scroll_lifecycle.rs` remains the behavioral backstop — verify it still passes after Task 3.

### Manual Testing Checklist
- [ ] Start `grimd`, run `grim summon "echo hello"`, confirm the daemon boots and the agent completes.
- [ ] `sqlite3 ~/.grimoire/grimoire.db 'SELECT kind, COUNT(*) FROM events GROUP BY kind;'` shows at least `output`, `state_change`, `agent_created` rows.
- [ ] Restart `grimd`. Re-run the query. Rows from the previous run are still present.
- [ ] `grim bind <id>` still streams in real time (broadcast path untouched).
- [ ] `grim scroll …` with a small scroll writes `scroll_progress` + `task_state_change` rows.

## Rollout Considerations

### Feature Flags
None. The change is additive: new table, new writer task, no consumer
behavior change. A flag would add risk without reducing any.

### Migration Strategy
- Existing DB files: `CREATE TABLE IF NOT EXISTS` handles them on first open.
- No data backfill — the log starts empty on first boot after the upgrade.
- No downgrade path needed; the `events` table is simply ignored by an older
  `grimd` binary.

### Rollback Plan
- Revert the merge; the `events` table persists but is unused. No data loss.
- If disk usage becomes a concern before Deliverable 5 ships, operators can
  manually `DELETE FROM events` — no foreign keys reference it.

## Open Items

- [ ] Confirm `serde_json` is already in the dependency graph (it is — used by `AgentEvent.payload`) — no new `Cargo.toml` edit expected.
- [ ] Decide whether to add a `tracing::trace!` per persisted event for debugging; default is **no** (too noisy under `Output` volume). Leave for the operator to enable via SQL.
- [ ] Retention policy, `grim logs`, and broadcast retirement are follow-on deliverables; do not bundle them into this PR.

---

*This spec is implementation-ready. Each task is designed for red-green TDD.
Tasks 1–4 must be completed in order; each is a single commit.*
