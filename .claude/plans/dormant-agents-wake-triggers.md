# Plan: Dormant Agents with First-Class Wake Triggers

> Generated from planning session on 2026-04-26
> Source: ROADMAP.md Part 3 §5 / Part 5 #5 ("THE differentiator")

## Problem Statement

Grimoire's agents are processes, not function calls — but today they still die after one task. The mail-wake path proves the daemon *can* resurrect a finished agent, yet the lifecycle has no honest representation of "waiting." `AgentState::Complete` does double duty: "this work is done forever" and "this agent is parked, waiting for something to happen." Anything that wants to wake an agent has to overload `Complete` and special-case `session_id.is_some()`. There is one wake source (incoming mail) and no abstraction for adding more.

The roadmap's headline differentiator — agents that suspend and wake on cron, file changes, parent completion, webhooks — cannot be built cleanly on this foundation. Every new trigger would re-invent the same Complete-overloading hack the mail-wake path already uses.

### Who experiences this?

- **Operators of standing teams.** A user who wants the "review team" demo from Roadmap Part 4 — three agents subscribed to a topic, woken every time a PR opens, sleeping in between — has nowhere to express "wake me on this signal." Today they must restart the agent manually or invoke it themselves.
- **Scroll authors who want recurrence.** Anyone who wants "every weekday at 09:00, run my triage agent" must build it outside Grimoire (cron + a shell script that summons), losing all of the daemon's observability and rate-limiting.
- **Agents themselves.** A parent agent that decomposes work into children today has no way to say "wake me when child X reaches state Y" — it has to poll, exit, or hold the slot open.
- **Future Grimoire features.** Supervision trees (Roadmap §8) and federation (§11) both need to express "wake on event Z." Without a wake-source abstraction, every one of those rebuilds the trigger plumbing.

### Why now?

Roadmap Part 5 lists this as the next item on the build-order spine after worker pools, the durable queue, and mail. The four prerequisites are landed. The mail-wake plumbing is the proof-of-concept; this work generalizes it. Without it, the daemon-native story stays half-told: we can wake agents from one source, but the broader claim ("agents are long-lived, addressable processes that outlive their callers") needs the trigger surface to back it up.

### Current workarounds

- Manual `grim invoke <id>` for every wake — no automation.
- External cron + shell scripts that `grim summon` fresh agents (loses session continuity, accumulates dead agents in `circle`).
- The `pact` system for agent-to-agent chaining — but pacts spawn *new* agents on completion; they cannot wake an existing dormant agent.
- Polling: agents that loop with internal sleeps, holding a worker slot the whole time.

## Goals

- **Promote `Dormant` to a real `AgentState`.** Stop overloading `Complete`. An agent in `Dormant` is parked with a session and at least one wake source; an agent in `Complete` is finished forever.
- **Ship a `WakeRegistry` abstraction.** A single daemon-internal actor owns all wake-source lifecycle (register, persist, re-arm on boot, evaluate, fire, retire). Adding a new source type is a matter of implementing a small trait, not rewiring the scheduler.
- **Three working wake sources at v1:** `cron`, `file-watch`, and `parent-completion`. Each fires through the existing mail bus so the resume path is identical to today's mail wake.
- **Backward-compatible UX.** Existing `grim invoke <complete-id>` keeps working: completed agents with a `session_id` auto-migrate to `Dormant` on daemon boot; `invoke` becomes shorthand for "send wake mail to a Dormant recipient."
- **Per-agent loop guard.** A token-bucket rate limit on wakes per agent (default 60/hr, configurable per source) prevents runaway wake cycles.
- **Deterministic tests.** A `Clock` trait seam lets cron schedules, `last_fired_at`, and rate-limit buckets be exercised in tests without real-time sleeps.
- **Restart safety.** Wake sources persist in SQLite and re-arm on boot; missed fires during downtime coalesce to at most one make-up fire per source.
- **`grim wake` CLI group** for managing wake sources (add / list / remove / test), mirroring `grim mail` and `grim pact`.

## Non-Goals (Explicit Scope Boundaries)

- **Webhook wake source.** Adding HTTP listeners + auth + TLS to `grimd` is its own surface area and overlaps with the Roadmap Part 1 hardening item ("protocol versioning + auth tokens"). Deferred to a follow-up plan that can ride on top of that work.
- **Supervision trees / restart policies (Roadmap §8).** Wake triggers are how a supervisor would *implement* a restart policy, but the supervisor concept itself (max_restarts, escalation, supervision strategies) is a separate plan.
- **Generic event-bus subscriptions as wake sources.** Mail topics already cover that — subscribing a Dormant agent to a topic is equivalent to a wake source, and the existing mail path handles it. We are not introducing a parallel "wake on stream event" surface.
- **Cron syntax dialects.** We pick one cron implementation (5-field standard cron via the `cron` crate) and stick to it. No quartz extensions, no human-friendly ("every weekday") DSL.
- **File-watch debouncing controls.** A fixed 200ms debounce ships in v1; tunability lands later if needed.
- **Distributing wake evaluation across workers.** All wake evaluation runs in the control-plane daemon. Workers never own a wake source.
- **Dashboard UI.** CLI only at v1. Dashboard surfacing is a fast follow.

## Proposed Solution

### Conceptual Overview

Two new concepts arrive in lockstep:

1. **`Dormant` is a real agent state.** Non-final, but slot-free. An agent enters `Dormant` *only* if it finishes with a `session_id` and has at least one registered wake source (or has the implicit "mail-wake" source — see migration below). Otherwise it enters `Complete` and stays there. The state machine becomes: `Queued → Summoning → Active → {Complete, Failed, Banished, Dormant}`. From `Dormant`, the agent transitions back to `Active` on a wake fire, or to `Banished` on `grim banish`.

2. **`WakeRegistry` is a daemon-internal actor.** It owns a `wake_sources` SQLite table and a set of in-memory evaluators. Each source has a kind (`cron` | `file-watch` | `parent-completion`), an agent it belongs to, and kind-specific config. The registry's job: on register, persist + arm the evaluator; on fire, send wake mail to the agent (which the existing scheduler then picks up); on banish, retire all of an agent's sources; on daemon boot, replay the table and reconcile missed fires.

The flow becomes: **trigger fires → registry sends mail → scheduler sees `MailReceived` → scheduler wakes Dormant agent.** Triggers are coalescing producers; the mail bus is the single wake gateway. This reuses the proven path, gives every fire an audit row in `events`, and means a fire that arrives while the agent is already `Active` simply queues — it'll be picked up next time the agent returns to `Dormant`.

### User Journey

A team wants a standing review agent that wakes whenever `src/api/**/*.rs` changes.

1. `grim summon "watch the API for breakage" --keep-alive` — agent runs, completes its first pass, stays addressable. (`--keep-alive` is the explicit opt-in to land in `Dormant` rather than `Complete` even with no wake source attached yet.)
2. `grim wake add <agent-id> --watch "src/api/**/*.rs"` — daemon registers the source, `notify` watcher arms.
3. Developer edits `src/api/auth.rs`. Watcher fires, registry sends a wake mail (`"file changed: src/api/auth.rs"`), scheduler wakes the agent, agent reviews, completes, returns to `Dormant`.
4. `grim wake list <agent-id>` — shows the source, last fired, fire count.
5. `grim wake test <wake-id>` — manually fires the source for debugging without waiting on a real signal.
6. `grim banish <agent-id>` — agent retires; registry drops all its sources.

For cron: `grim wake add <id> --cron "0 9 * * 1-5"` — fires 09:00 weekdays. For parent-completion: `grim wake add <id> --on-parent <pid>` — fires when `<pid>` reaches `Complete` (default; states configurable).

## Architecture

### Data Model

A new SQLite table — `wake_sources`:

- `id` (text PK) — `wake_<8-hex>`
- `agent_id` (text, FK) — the dormant agent this source wakes
- `kind` (text) — `cron` | `file_watch` | `parent_completion`
- `config_json` (text) — kind-specific (cron expr; glob + cwd-relative root; parent agent_id + target states)
- `state` (text) — `armed` | `failed` | `disabled`
- `fail_reason` (text, nullable) — populated when `state = failed`
- `last_fired_at` (int, nullable, unix seconds) — for cron catch-up and observability
- `fire_count` (int) — observability
- `created_at` (int)

A new per-agent rate-limit row (token bucket): `wake_rate_limits` keyed by `agent_id`, columns `tokens` (float), `last_refill_at` (int), `capacity` (int default 60), `refill_per_sec` (float default 60/3600).

`AgentState` gains a `Dormant` variant. New methods on the enum:
- `is_terminal()` — Complete | Failed | Banished | **Dormant** (slot-free; scheduler frees the slot).
- `is_final()` — Complete | Failed | Banished only (truly done; UI/scrolls use this for "agent finished forever").

`StreamEvent` gains four variants: `WakeSourceRegistered`, `WakeSourceFired`, `WakeSourceFailed`, `WakeSourceRetired`.

### System Boundaries

Everything lives inside `grimd`. The `WakeRegistry` is a peer of the `Scheduler` and `EventBus` — it talks to the database directly for persistence, to the bus for events, and to the mail subsystem (`mail.send`) to deliver wakes.

### API Surface

CLI (the `grim wake` group):
- `grim wake add <agent-id> --cron "<expr>"` | `--watch "<glob>"` | `--on-parent <pid> [--states complete,failed]`
- `grim wake list <agent-id>` — sources for one agent
- `grim wake list` — all sources, all agents (operator view)
- `grim wake remove <wake-id>`
- `grim wake test <wake-id>` — manually fire (sends the wake mail, marks last_fired_at)

Daemon RPC: corresponding `wake.add`, `wake.list`, `wake.remove`, `wake.test` methods on the existing protocol.

`grim summon` gains `--keep-alive` to land in `Dormant` with no sources yet (so a user can `grim wake add` afterwards).

`grim banish <id>` cascades to retire all the agent's wake sources.

### Integration Points

- **Mail bus.** Wake fires send mail with a synthetic `sender_id = "wake://<wake-id>"` and `wake_eligible = true`. The existing scheduler mail-wake path picks it up unchanged (one extra branch: it must accept `Dormant` agents, not just `Complete`).
- **Scheduler.** The `tick_mail_wake` candidate filter changes from `state == Complete` to `state == Dormant`. `should_wake` keeps the existing `MailReceived` trigger. Slot accounting reads through the new `is_terminal()`.
- **Event log.** All four new `StreamEvent` variants flow through `EventBus` and persist in the durable `events` table — same path as `MailSent` etc.
- **`invoke`.** Becomes a thin wrapper: `invoke <id> <msg>` resolves to `mail.send agent://<id> <msg> --wake-eligible`. The dual code path for "session-restart on a Complete agent" goes away once auto-migration runs.
- **`pact`.** Unchanged. Pacts spawn *new* agents on completion; parent-completion wake sources wake *existing* dormant agents. Both can coexist on the same parent.

## Implementation Approach

### Recommended Pattern

The `Scheduler` already demonstrates the pattern this work extends: a daemon-owned actor with seams (`Dispatcher`, `MailWaker`, `AgentStateLookup`) that tests substitute. `WakeRegistry` follows that pattern verbatim — a struct with `Arc<Database>`, `EventBus`, an injected `Clock`, and a `MailSender` seam. Each wake-source kind implements a small trait:

```
trait WakeSource {
    fn arm(&self) -> Result<ArmedHandle>;
    fn evaluate(&self, ctx: &Ctx) -> Vec<FireDecision>;
}
```

The registry holds a `HashMap<wake_id, ArmedHandle>` in memory, persists rows in SQLite, and on `tick_now()`-equivalent loops runs `evaluate` for time-driven sources (cron) and processes events for event-driven sources (file-watch via `notify` callbacks; parent-completion via `EventBus` subscription).

This mirrors the **mail wake → scheduler** path: triggers are *producers* of mail; the scheduler is the single consumer. Two-actor design, one wake gateway.

### Key Technical Decisions

| Decision | Choice | Rationale | Trade-offs |
|----------|--------|-----------|------------|
| Dormant vs Complete | Add `Dormant` as new state, distinct from `Complete` | Honest lifecycle; `is_final` callers stay correct; future supervision trees need the distinction | One more state to maintain; migration step on boot |
| Wake fires fanout | Always go through mail bus | Reuses proven scheduler path; every fire audited in events log; busy-agent coalescing for free | Mail row written even for cron self-fires (small SQLite cost) |
| `is_terminal` vs `is_final` | Two-axis: `is_terminal` (slot-free) and `is_final` (lifecycle done) | Scheduler and lifecycle have genuinely different needs | Two methods to keep in sync; reviewers must pick the right one |
| Backward compat | Auto-migrate Complete-with-session → Dormant on boot | No user-visible UX break for `grim invoke` | One-shot migration must be idempotent and logged |
| Clock | Inject `Clock` trait, `SystemClock` + `TestClock` | Mirrors existing seam pattern; deterministic cron tests | New abstraction to thread through registry + rate limiter |
| Cron library | `cron` crate (5-field standard) | Mature, no scheduler embedded (we own the loop), no async runtime coupling | No quartz seconds field; no human DSL — fine, both are non-goals |
| File-watch library | `notify` crate, recursive, 200ms debounce | De facto standard, cross-platform; matches what most Rust tools use | Linux inotify watch limits (cap watcher count per agent) |
| Loop guard | Per-agent token bucket | Composes with `max_concurrent_agents`; default 60/hr is generous for humans, restrictive for runaway loops | One more knob; rate-limited fires need an event so users see them |
| Fire while busy | Coalesce via mail bus | Mail Pending naturally queues; agent picks up next return to Dormant | Pending mail can pile up if agent gets stuck Active |
| Restart catch-up | At most one make-up fire per source | Prevents downtime → mail flood | A long downtime hides intermediate fires (acceptable; cron is a heartbeat, not a log) |

### Rough Task Breakdown

1. **State machine + migration.** Add `Dormant` to `AgentState`, split `is_terminal` and `is_final`, write the boot-time auto-migration (Complete-with-session → Dormant), update every callsite that branched on `Complete`. Includes touching the scheduler's mail-wake candidate filter.
2. **Schema + Clock.** Add `wake_sources` and `wake_rate_limits` tables with `IF NOT EXISTS` migrations. Land `Clock` trait, `SystemClock`, `TestClock`. Wire `Clock` through the scheduler's existing rate-limit-adjacent code.
3. **`WakeRegistry` skeleton + cron source.** Registry actor, register/list/remove/test paths, cron evaluator with `last_fired_at` catch-up. `grim wake` CLI subcommands.
4. **Parent-completion source.** Subscribes to the existing `StateChange` events; configurable target states; integration with the registry.
5. **File-watch source.** `notify`-backed evaluator, debounced, glob-matched, cwd-anchored. Self-disable + `WakeSourceFailed` event when cwd disappears.
6. **Loop guard.** Token bucket on wake fires per agent; rate-limited fires emit `WakeSourceFailed` with `rate_limited` reason.
7. **`--keep-alive` + `invoke` reconciliation.** Add the `summon` flag, fold `invoke` into `mail.send --wake-eligible`, deprecate the dual code path.
8. **Test surface.** Per-source tests with `TestClock` + the dispatcher/mail seams; integration tests for register → fire → wake → resume; restart-recovery tests for catch-up.

Eight chunks is on the high side for a plan; the registry trait + cron (chunks 1-3) is the foundation, and 4-6 are independent sources that can each ship behind the same trait.

### Riskiest Part

**The state-machine migration** (chunk 1). Every `match` on `AgentState` and every `is_terminal` callsite needs to be audited. Miss one and you get either a "phantom slot held forever" bug (slot accounting wrong) or a UI that says an agent is "active" when it's parked. The migration must be idempotent (replay-safe across daemon restarts), must not flip agents that lack a `session_id`, and must emit a `StateChange` event so dashboards/clients see the transition.

Adjacent risk: **`notify` watch limits on Linux** (default ~8K watches per process). If many agents register recursive watches over deep trees, the daemon hits the limit silently. Mitigation: cap watched paths per source, surface the limit as a `WakeSourceFailed` reason, document the tunable.

## Edge Cases & Decisions

| Edge Case | Decision | Rationale |
|-----------|----------|-----------|
| Agent in `Active` when a wake fires | Trigger sends mail; mail sits `Pending` until agent returns to `Dormant`; existing scheduler picks it up | Reuses mail's coalescing semantics; no special "fire later" queue needed |
| Daemon restart with cron schedules due during downtime | Coalesce to ≤1 make-up fire per source (compare `last_fired_at` to expected next tick; fire once if we missed at least one) | Prevents downtime → wake flood |
| Daemon restart with file changes during downtime | One reconciliation fire per source if the watched paths' mtime is newer than `last_fired_at` | Same coalescing rule; precise change list isn't recoverable, single fire is enough |
| Agent's cwd was deleted (e.g. worktree torn down) | File-watch source self-disables, `WakeSourceFailed` event with `cwd_gone`, agent stays `Dormant` | Don't crash the daemon; let other sources still fire |
| Parent agent was banished (not Complete) | Parent-completion source by default does NOT fire (banishment is a kill, not a result) | Matches user expectation; configurable via `--states` if user wants it |
| Wake mail body too large | Existing 64 KiB cap on mail body and 16 KiB cap on folded resume prompt apply unchanged | Reuse the mail subsystem's existing limits |
| Cron expression invalid | `wake add` rejects at registration time with parse error | Fail fast at the CLI |
| Agent banished while wake mail is `Pending` | Mail rows for banished recipients move to `Failed` (already in mail subsystem) | Existing mail semantics handle this |
| Loop: A wakes B wakes A | Token bucket trips; further fires marked `Failed` with `rate_limited` reason | Default 60/hr per agent; configurable |
| Auto-migration encounters a Complete agent with stale session | Migrate to `Dormant`; first `invoke` will surface session staleness as a normal session-resume failure | Lossy migration is worse than letting the existing failure path handle it |
| Two wake sources fire near-simultaneously | Each sends its own mail row; scheduler folds both into one resume prompt (existing fold logic) | Mail fold is the natural batching point |
| `grim wake test` while agent is Active | Same as any fire while busy: mail enqueued, picked up later | Symmetric with normal fires; debug doesn't get special privileges |

## Security Considerations

- **No new external surface.** Cron, file-watch, and parent-completion all run inside the daemon. The webhook source — which would expose an HTTP endpoint and need auth, TLS, and CSRF — is explicitly deferred to a follow-up plan that builds on the Part 1 hardening (protocol versioning + auth tokens).
- **File-watch path traversal.** Wake-source globs resolve relative to the agent's `cwd` and are rejected if the resolved root escapes that cwd. Symlinks within the watched tree are followed but symlink targets that escape cwd are not watched. (The agent already runs with cwd as its workspace; file-watch shouldn't widen that.)
- **Cron expressions are not code.** Parsed by the `cron` crate; no eval.
- **Wake mail sender authentication.** Wake fires use `sender_id = "wake://<wake-id>"`. Receiving agents that gate behavior on sender identity must treat that as a system-originated message, never as a user impersonation.
- **Audit trail.** Every register / fire / failure is a `StreamEvent` row in the durable events log. Operators can answer "why did this agent wake at 03:14?" by querying the log.
- **Banish cascades.** `grim banish` retires all of an agent's wake sources atomically — no orphaned watchers consuming inotify slots after an agent is gone.
- **Rate limit prevents resource exhaustion.** A misconfigured wake source can't burn unbounded CPU or mail rows; the token bucket caps it.

## Failure Modes & Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| State migration misses a callsite | Med | High (slots leak or UI lies) | Audit every `match AgentState` and `is_terminal` callsite during chunk 1; integration test that exercises every transition |
| `notify` exceeds Linux inotify watch limit | Med | Med (some sources stop firing silently) | Cap watches per source; surface limit as `WakeSourceFailed`; document `fs.inotify.max_user_watches` tuning |
| Cron clock skew across daemon restarts | Low | Low (one duplicate or skipped fire) | Catch-up logic uses ≤1-fire rule; cron is a heartbeat, occasional skew acceptable |
| Wake mail fanout creates queue pressure | Low | Med (mail table grows fast) | Per-agent rate limit; existing mail retention policy applies; observability via `wake.list` shows fire counts |
| `notify` callback runs on a watcher thread, races with registry teardown | Med | Med (panic on dropped channel) | Bounded channel + drop-then-shutdown order documented; integration test that adds + removes sources rapidly |
| Auto-migration runs on a corrupted DB | Low | High (wrong agents flipped to Dormant) | Migration is gated on `state = Complete AND session_id IS NOT NULL` only; emits `StateChange` events; reversible via state update if a user complains |
| File-watch debounce eats a real change | Low | Low (next change picks it up; or `grim wake test` recovers) | 200ms is short; agents that need exact-once need their own dedup in-prompt |
| Parent-completion source on a banished parent | Low | Low (source never fires) | `wake list` shows `last_fired_at = null`; user can remove + re-target |
| Token-bucket starves a legitimate hot agent | Low | Med (real wakes get marked Failed) | Per-source override of capacity/refill; document the default; emit visible event when triggered |
| Race: agent transitions Dormant → Active just as a fire arrives | Low | Low | Mail row stays Pending; next return to Dormant picks it up (coalescing path) |

## Open Questions

- [ ] Should the wake-source ID be exposed in the wake-mail body (so the agent's resume prompt can see "you were woken by `wake_a1b2c3d4` (cron)")? Leaning yes — useful introspection for the agent — but adds surface.
- [ ] Is `--keep-alive` the right name on `summon`? Alternatives: `--dormant`, `--familiar`. Defer to spec phase; cosmetic.
- [ ] For file-watch globs, should we support exclusion patterns (`!target/**`)? Probably yes for v1 — `target/` would otherwise spam wakes during builds.
- [ ] Default rate-limit values (60/hr capacity, 60/hr refill) need a sanity check against the planned demos (review-team subscribed to PRs); may need to raise capacity for legitimately bursty patterns.

## Alternatives Considered

### Refactor first, sources later
**Description:** Promote `Dormant` and build the registry abstraction with no new sources — retro-fit only the existing mail wake into it. Ship the new sources in a follow-up plan.
**Rejected because:** The registry abstraction is hard to validate without multiple consumers. Building cron + parent-completion + file-watch together exercises the trait surface and surfaces design errors the mail-only retrofit would miss. The diff is bigger but the abstraction is more honest.

### Direct wake (bypass mail)
**Description:** Triggers call `Scheduler::wake()` directly — faster path, no mail row written.
**Rejected because:** Two code paths into wake. Triggers lose their audit trail in the events log. Coalescing-while-busy semantics would have to be reinvented per source. The mail bus is already proven for exactly this job.

### Out-of-process registry (on workers)
**Description:** Wake sources run on worker nodes (`grimw`), not the control plane.
**Rejected because:** Most wake state is daemon-local (cron timers, parent-completion subscriptions watching `EventBus`). Distributing it adds protocol surface for negligible benefit. Re-evaluate when federation lands.

### Webhook source in v1
**Description:** Add an HTTP wake source (`grim wake add <id> --webhook /path --token <t>`) alongside cron / file-watch / parent.
**Rejected because:** Opens external network surface that needs auth, TLS, and request validation — none of which `grimd` has today. The Part 1 hardening item ("protocol versioning + auth tokens on UDS/HTTP") is the prerequisite. Defer until that lands; webhook becomes a small follow-up plan that reuses this work's registry.

### Treat Dormant as a sub-flag on Complete
**Description:** Keep `AgentState::Complete`, add `is_dormant: bool` and `wake_sources: [...]` columns.
**Rejected because:** Less churn but less honest. Every callsite still has to combine `state == Complete` with `is_dormant` to know what's actually going on, and the lifecycle distinction supervision trees will need is hidden. The state-machine migration is the right place to absorb that pain once.

### Always fire on any terminal parent state
**Description:** Parent-completion source always fires on Complete OR Failed — no per-source state filter.
**Rejected because:** Forces every dormant child to handle "parent failed" in its resume prompt. Default-fire-on-Complete with a configurable override matches the common case while leaving the door open for self-healing patterns.

---

*This plan captures the "what", "why", and high-level "how". It is input for `/hatch:write-spec`, which will produce the detailed implementation specification — including the schema migration SQL, the `WakeSource` trait shape, file-by-file change list, and per-task acceptance criteria.*
