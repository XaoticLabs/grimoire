# Plan: Durable Work Queue with Admission Control

> Generated from planning session on 2026-04-25
> Source: ROADMAP.md Part 2 §2 / Part 5 build-order #2

## Problem Statement

Today, both `grim summon` and `scroll_keeper`'s task dispatcher call `executor.start()` synchronously. The daemon has no notion of *capacity* and no admission point between "user asks for work" and "a process gets spawned." Per-scroll concurrency caps exist (`max_concurrency`), but they are local to one scroll — there is no daemon-wide ceiling, no scheduler, and no durable record of work that hasn't started yet.

This becomes an active problem now that `grimw` workers exist. The control plane can place tasks on remote workers, but it still does so at the moment of `summon`: if no worker matches, the call fails immediately; if many calls arrive at once, the daemon spawns without bound. We have a worker pool and no scheduler to feed it.

### Who experiences this?

- **The daemon operator** running `grim inscribe big-spec.md -c 12` against a worker pool of 3, who watches placement either fail-fast or oversubscribe one worker.
- **The roadmap** itself — items #3 (agent-to-agent bus), #5 (dormant agents), and #10 (budgets) all assume an admission point that doesn't yet exist.
- **The "laptop grid" demo** — the headline use case for the worker pool. Without queueing, distributing 12 tasks across 4 laptops requires the user to manually batch.

### Why now?

The worker pool (Part 2 §1) and durable event log (Part 2 §3) are done. The queue is the missing glue: workers expose capacity, the event log gives us durability primitives, and the next roadmap items either need a scheduler or are easier with one. Building it now also keeps the daemon honest about its thesis — agents are processes the daemon owns, which means the daemon should own when they start.

### Current workarounds

- Users self-throttle by inspecting `grim circle` before summoning.
- `grim inscribe -c N` sets a per-scroll ceiling that's unaware of other scrolls or ad-hoc work running concurrently.
- For provider 429s and capability mismatches, the only recourse is to retry by hand.

## Goals

- A daemon-wide ceiling on concurrent agents, configurable via `daemon.max_concurrent_agents`.
- `summon` (RPC + CLI) accepts work even when no slot is free; the agent enters a new `Queued` state and is promoted automatically when capacity and an eligible worker exist.
- Queued work survives a daemon restart.
- A new `grim queue` command shows pending work, live capacity, and per-worker breakdown.
- Scroll-level `max_concurrency` continues to apply on top of the global cap; scrolls and ad-hoc summons share the same scheduler.
- Behavior is observable via the existing event bus (a new `Queued` lifecycle event, `Queued → Summoning → Active` transitions).

## Non-Goals (Explicit Scope Boundaries)

- **Per-cwd, per-provider, per-tenant concurrency caps.** v2.
- **Provider rate-limit / 429 backoff & retry policy.** v2 — overlaps with the retry/idempotency roadmap item.
- **User-visible priority flags** (`summon --priority high`). The internal schema will support priority so we can add it later without migration; no CLI surface in v1.
- **Per-tenant or per-cwd budgets / quotas.** Roadmap item #10 territory.
- **A schema-versioning / migration framework.** v1 keeps the existing `CREATE TABLE IF NOT EXISTS` + ad-hoc `ALTER` pattern.
- **TTL on unplaceable tasks.** Tasks queued without an eligible worker stay queued indefinitely. Workers can join later — that property is part of the daemon model.
- **Reattach to remote agents on daemon restart.** Active agents whose daemon died are marked `Failed`; reattach is a separate feature (it needs new worker RPC).
- **NATS / Redis upgrade path** for the queue. SQLite-only in v1, same as the event log.

## Proposed Solution

### Conceptual Overview

Introduce a single, daemon-owned **scheduler** that is the only path from "we want this agent to run" to `executor.start()`. Both ad-hoc `summon` and scroll-task dispatch enqueue tasks instead of calling the executor directly. The scheduler runs a tick loop (driven by event-bus signals plus a periodic wake) that pulls from the queue when the global cap and a placement check both allow it.

A new agent state, `Queued`, sits in front of `Summoning`. The wire contract for `summon` becomes honest: it acknowledges acceptance, not start. The CLI returns immediately with `<id> (queued)`; users follow with `grim bind`.

The queue is a SQLite table next to the existing `events`, `agents`, `tasks` tables. Two implicit lanes — ad-hoc and scroll-task — give us a knob to bias the dispatch order without any user-visible flag. FIFO within a lane.

### User Journey

**Happy path, capacity available:**
1. User runs `grim summon "investigate flaky test"`.
2. Daemon inserts an `agents` row in `Queued` state and a `task_queue` row, returns immediately.
3. CLI prints `42a8f1c3 (queued)`.
4. Within ~50ms the scheduler tick promotes it: `Queued → Summoning → Active`. `executor.start()` runs, `pid` is recorded.
5. User runs `grim bind 42` and sees output as if nothing changed.

**Saturated path:**
1. Global cap is 8; 8 agents are already Active. User runs `grim summon "..."`.
2. Same enqueue, same immediate return.
3. CLI prints `42a8f1c3 (queued)`. `grim queue` shows it pending with reason `capacity`.
4. When an Active agent completes, the scheduler picks the next queued task and promotes it.

**No eligible worker:**
1. User summons a task targeting a provider no registered worker advertises.
2. Task enters `Queued` with reason `no eligible worker`.
3. A new `grimw` registers with the matching capability — scheduler wakes on registration and promotes.

**Restart:**
1. Daemon dies with 4 Active and 6 Queued.
2. On startup, the 4 Active are marked `Failed` (their child processes are gone). The 6 Queued remain in the queue and the scheduler picks them up.

## Architecture

### Data Model

- **`task_queue` table** — one row per queued task. Fields: `id` (matches `agents.id`), `lane` (`adhoc` | `scroll`), `priority` (u8, default 0, hidden in v1), `enqueued_at`, `provider_name`, `cwd`, `model`, `task_text`, `block_reason` (nullable: `capacity` | `no_eligible_worker` | `scroll_conflict`).
- **`AgentState::Queued`** — new enum variant. Persisted in `agents.state`.
- **`StreamEvent::AgentQueued`** — new variant published when a task is enqueued. The existing `StateChange` covers `Queued → Summoning` and beyond.

The queue table is intentionally narrow. The full agent record stays on `agents`; the queue row is just "what does the scheduler need to pick this up?" Scheduler joins to `agents` and (for scroll tasks) to `tasks` for richer context.

### System Boundaries

The queue lives entirely in `grimd`. Workers (`grimw`) are unchanged — they still receive work via the existing executor → worker RPC path. The CLI is unchanged on the wire except for handling the new state in formatters.

### API Surface

- **RPC**: existing `agent.summon` remains; semantics shift to "enqueue and return." A new `agent.queue.list` RPC backs `grim queue`.
- **CLI**: new `grim queue` (subcommands TBD by spec — at minimum a default listing + `--json`).
- **Config**: new key `daemon.max_concurrent_agents` (default: a sensible number like 8).
- **Events**: new `AgentQueued` event; standard `StateChange` for promotions.

### Integration Points

- `agent_manager.summon()` — splits in two: a `summon` that enqueues, and an internal `dispatch` that the scheduler calls (which does what `summon` does today below `insert_agent`).
- `scroll_keeper::schedule_tasks()` — instead of calling `manager.summon()` (which dispatches), it calls `manager.enqueue()`. Scroll-level concurrency is enforced *before* enqueueing (so scrolls don't flood the queue with tasks that won't be released).
- `orchestrator` (pact firing) — calls the same `enqueue` path. A pact-fired agent may sit Queued.
- `process_manager` / `executor` — unchanged. The scheduler is the new caller of `executor.start()`.
- `rpc.handle_status` — must count `Queued` distinctly from `Active`.
- `dashboard` — same: queued vs. running needs visual distinction; out-of-scope for the spec to design, but the data must be present.

## Implementation Approach

### Recommended Pattern

Mirror the existing reactor style of `scroll_keeper`. The scheduler subscribes to event-bus completions (so a finished agent triggers an immediate dispatch attempt) and additionally wakes on a short interval (so worker registrations and timeouts also poke it). It is a single Tokio task owned by the daemon, parallel to `scroll_keeper`. `scroll_keeper` continues to own scroll-level scheduling decisions (dependency readiness, file conflicts, scroll concurrency); the new scheduler owns daemon-level admission. They compose by pipelining: `scroll_keeper` decides "this task is ready" and enqueues; the scheduler decides "we have a slot and a worker" and dispatches.

This avoids merging the two reactors into one giant loop — separation of concerns matches the codebase's existing module boundaries.

### Key Technical Decisions

| Decision | Choice | Rationale | Trade-offs |
|---|---|---|---|
| Where queue state lives | New `task_queue` SQLite table | Aligns with existing durable event log; survives restart; same backup story as `agents` and `events` | Adds one more table to the hand-rolled schema |
| `summon` semantics | Always enqueue, return `Queued` immediately | Honest wire contract once the daemon owns placement; one code path instead of "synchronous if free, async if not" | Every existing test that asserts `Active` right after `summon` needs updating; CLI UX changes |
| Scheduler shape | New module reactor + tick, parallel to `scroll_keeper` | Preserves separation of concerns; scrolls keep their dispatch logic; daemon-wide admission is its own thing | Two reactors instead of one — a future refactor could fuse them, but not now |
| Ordering | FIFO within two implicit lanes (ad-hoc, scroll) | Keeps the door open for priority lanes / quotas without re-architecting | Decision about which lane wins on tie has to be made (v1: ad-hoc wins, since users typing at the CLI expect responsiveness) |
| Capability check | Add a non-mutating `registry.has_eligible_worker(provider, version)` query | Lets the scheduler peek before committing; avoids reservation/release dance | New method on `WorkerRegistry`; must stay in sync with `pick_least_loaded` |
| Restart behavior for Active | Mark Failed on startup | Honest about reality — child processes are gone — and keeps recovery code small | No reattach story in v1 |

### Rough Task Breakdown

1. **`Queued` state plumbing** — add `AgentState::Queued`, `StreamEvent::AgentQueued`, update banish guards, scroll status counts, dashboard count, `invoke` gate, test fixtures. No queue yet; agents can be inserted Queued and stay there. *Foundation: nothing else compiles cleanly without this.*
2. **`task_queue` table + persistence helpers** — schema, insert, dequeue (claim a row), peek, list, delete, restart-recovery query (mark Active→Failed, leave Queued alone). Unit-level tests against in-memory SQLite.
3. **Capability peek on registry** — `has_eligible_worker(provider, version)` non-mutating query. Tests for "no workers", "wrong provider", "wrong version", "match".
4. **Scheduler module** — reactor wired to event bus + interval. Tick logic: while (global slot free && queue non-empty && head of queue placeable) → claim, dispatch via existing executor flow, transition state. Backpressure when blocked.
5. **Wire `summon` and scroll dispatch through the queue** — split `agent_manager.summon` into `enqueue` + `dispatch_internal`. Update `scroll_keeper::schedule_tasks` to enqueue. Update `orchestrator` pact firing.
6. **`grim queue` CLI + RPC** — pending list, live capacity, per-worker breakdown, `--json`. Reads from `task_queue` joined with `agents` and `worker_registry`.
7. **Test surface** — restart recovery test, capacity-saturation test, no-eligible-worker test, scroll + ad-hoc interleave test, banish-while-queued test. Update existing `summon`-assumes-Active tests.

Tasks are linear-ish; (3) can run in parallel with (1)–(2). (5) depends on (1)–(4). (6)–(7) follow.

### Riskiest Part

**Splitting `agent_manager.summon`.** The function's current contract is implicit and load-bearing: it returns an `Agent` whose state is `Active` and whose `pid` is set, and three callers depend on that (CLI RPC handler, `scroll_keeper::schedule_tasks`, `orchestrator::handle_completion` for pact firing). Splitting cleanly without leaving a half-async/half-sync mess is the highest-friction part. The bug surface is "things that look like they work in the happy path because the scheduler ran a tick within the test's timing tolerance" — flakiness magnet.

Mitigation: the test suite needs an explicit `wait_for_state(id, Active)` helper rather than asserting the post-summon agent is Active. And a mode where the scheduler tick is driven manually in tests (rather than time-based) to make state transitions deterministic.

## Edge Cases & Decisions

| Edge Case | Decision | Rationale |
|---|---|---|
| Daemon restart with Queued + Active agents | Queued stay queued and reschedule; Active marked Failed | Honest: child processes died with the daemon. Reattach is a future feature. |
| Task queued with no eligible worker | Stays Queued indefinitely with `block_reason = no_eligible_worker` | Workers can join later — that's a feature of the daemon model. TTL is a v2 concern. |
| Banish on a Queued agent | New guard accepts `Queued`; remove from `task_queue` and mark agent `Banished` | Without this, `grim banish` on a queued task is a silent no-op. |
| `invoke` on a Queued agent (never started) | Reject with a clear error: "agent has not started yet" | `invoke` resumes a session; there's no session to resume. |
| Pact fires when target queue is saturated | Pact-spawned task enters Queued like any other | Honest: pact is a producer; admission is the daemon's job. |
| Scroll-level cap reached but daemon cap free | Tasks for that scroll stay Ready (in `tasks` table); not enqueued | Preserves scroll's existing semantics. The queue holds tasks the daemon has *committed* to running soon. |
| Scroll-level cap free but daemon cap full | Tasks enter Queued with `block_reason = capacity` | Visible in `grim queue`; user sees the daemon is the bottleneck. |
| Lane tie-breaking on dispatch | Ad-hoc lane wins over scroll-task lane | Interactive `summon` should feel responsive; long-running scrolls can wait one slot. |
| Two summons enqueued in same tick | FIFO by `enqueued_at` with id as tiebreaker | Deterministic, debuggable. |
| `daemon.max_concurrent_agents` changed at runtime | Honored on next tick (no restart needed) | Operability; matches existing config-reload patterns. |
| Worker disappears between peek and dispatch | Dispatch fails → task returns to Queued, scheduler retries on next tick | Eventually consistent; no stuck-claimed state. |
| Dashboard "active" count semantics | Distinct counts: Queued, Active. `grim status` reports both. | Avoids the lie where a saturated daemon shows "1 active, 9 invisible." |

## Security Considerations

- **No new external surface.** The queue is internal; no new HTTP endpoint. `grim queue` goes over the existing UDS, which inherits whatever auth model the daemon already has (currently: local-process trust, called out as a hardening blocker in ROADMAP §1).
- **DoS via unbounded enqueue.** Without per-tenant caps (v2), a buggy script can flood the queue. v1 mitigation: log a warning when the queue depth exceeds a sane threshold; document the limitation. Real fix is the per-tenant policy item.
- **Task text persisted in SQLite.** Same sensitivity as the existing `agents.task` field — no new exposure, but a reminder that the SQLite file should be 0600 and not synced to backups by default. Pre-existing concern, called out for completeness.
- **No new secret material.** Scheduler does not touch provider credentials; that path stays in the executor.

## Failure Modes & Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Test flakiness from race between scheduler tick and assertions | High | Medium | Provide a manual-tick mode for tests + `wait_for_state` helper |
| `summon` API contract change breaks downstream tools / scripts | Medium | Medium | Version the protocol; document; CLI defaults remain UX-friendly |
| Scheduler livelock when every queued task is `no_eligible_worker` | Low | Low | Worker registration wakes the scheduler; periodic tick is the safety net |
| Capability peek and actual placement disagree (TOCTOU) | Medium | Low | Failed dispatch returns task to Queued; next tick retries; no permanent stuck state |
| Schema migration on existing databases | Medium | Medium | Use `CREATE TABLE IF NOT EXISTS` and `ALTER ... ADD COLUMN` (existing pattern); test upgrade from a pre-queue DB |
| `Queued` state breaks dashboard / scroll status counters silently | Medium | Medium | Audit all callers of `AgentState` enum exhaustively; rely on Rust's match-exhaustiveness to catch them |
| Long restart with 1000 queued tasks | Low | Low | Recovery query is a single `UPDATE ... WHERE state = 'Active'`; queue is read on demand by the scheduler, not all at once |
| Pact spawning a task that sits Queued forever (no eligible worker) | Low | Medium | Visible in `grim queue` with `block_reason`; document that pacts are subject to admission |

## Open Questions

- [ ] What should the default value of `daemon.max_concurrent_agents` be? Probably tied to detected worker total + a small buffer for local execution. Spec to decide.
- [ ] Lane tie-break direction (ad-hoc-wins) is a guess — worth one round of dogfooding before locking in.
- [ ] Should `grim summon --wait` exist as a sugar flag for "block until Active or N seconds"? Defer unless someone asks.
- [ ] Does the dashboard need design changes for queued agents in v1, or is "Queued" rendered as a state badge enough? Likely the latter.

## Alternatives Considered

### Global semaphore in `agent_manager`

**Description:** Add `daemon.max_concurrent_agents` and a `tokio::sync::Semaphore` around the existing synchronous `summon`. No queue table, no scheduler module, no `Queued` state — `summon` just blocks the caller until a permit is free.
**Rejected because:** Doesn't survive daemon restart, doesn't expose queued work to `grim queue`, blocks the RPC handler for arbitrary durations, and doesn't help the worker-pool placement story (the original driver). It's the v0.5 of this feature, not the v1.

### Single fused scheduler (merge `scroll_keeper` into the new scheduler)

**Description:** One reactor that owns scroll-task readiness, file-conflict detection, scroll-level concurrency, and global admission together.
**Rejected because:** Conflates two distinct concerns at different abstraction levels. Scroll-level scheduling is about DAGs and conflicts; daemon-level admission is about capacity and capability. Merging now is premature; pipelining (scroll_keeper enqueues, scheduler dispatches) is the smaller, reversible move.

### In-memory queue with WAL

**Description:** `VecDeque` in the orchestrator for the hot path; append-only writes to the events log so we can replay queue state on restart.
**Rejected because:** It's more code than a SQLite table for the same outcome. SQLite is already the durability layer; piggybacking on the events log just to avoid a table is over-engineered.

### Always-synchronous `summon`, queue only on saturation

**Description:** Keep current behavior when capacity exists; only enqueue when over the cap.
**Rejected because:** Forces every caller to handle two response shapes (synchronous Active vs. asynchronous Queued). The wire contract becomes "depends on daemon state at call time," which is harder to reason about than "always Queued, sometimes promoted fast."

---

*This plan captures the "what", "why", and high-level "how". It is input for `/write-spec`, which produces the detailed implementation specification.*
