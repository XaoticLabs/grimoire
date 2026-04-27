# Plan: Supervision Trees — Restart Policy & Escalation

> Generated from planning session on 2026-04-27
> Source: ROADMAP.md Part 3 §8 / Part 5 #6 ("makes scrolls self-healing")

## Problem Statement

Grimoire treats agents as long-lived processes, but failure handling is still library-grade: an agent that crashes, gets a 5xx from its provider, or hits a transient permission error transitions to `Failed` and stays there. There is no policy layer the daemon can enforce — no automatic retry, no concept of a parent supervisor, no first-class "this child gave up." Anything resembling self-healing today has to be reimplemented per workflow: the user manually `grim invoke`s a failed agent, or wires a polling agent that watches for failures, or accepts that overnight scrolls go red on the first transient hiccup.

The roadmap's "wake up to a green scroll" demo (Part 4) is impossible against this state machine. The supervision-tree primitive is what closes the gap: declarative restart policy attached to the agent at summon time, budget-bounded so a flapping agent can't loop forever, and an escalation channel so failure can bubble up to a parent (or a topic) that decides what to do next.

### Who experiences this?

- **Operators of overnight scrolls.** Today a single transient provider error ends a 12-task scroll. They wake to a partial run and re-trigger by hand. They want "any task that fails gets up to N retries with a delay, only escalates the scroll if the budget is spent."
- **Authors of standing teams.** A dormant review agent that wakes on PRs and crashes mid-review currently stays crashed. They want it to recover automatically and only ping a human if it can't.
- **Future Grimoire features.** Scroll integration, federation, and OTP-style strategies (`one_for_all`, `rest_for_one`) all need a base supervision primitive. Without it, every one of those rebuilds the retry/escalation plumbing.
- **The "self-healing pipeline" demo.** This is the headline Part 4 demo for the next product story. It cannot be told without supervision.

### Why now?

Build-order item #6 on the Part 5 spine. Items 1–5 are landed. Mail-bus + dormant + wake-on-mail give us the *resume mechanism*; supervision adds the *policy* on top. Nothing else on the roadmap reuses as much of the existing plumbing — escalation rides the mail bus, restart fires through the same path as wake-on-mail, the `Clock` seam already exists for deterministic tests. The marginal cost is the lowest it will ever be; the demo payoff (green scrolls, self-healing pipelines) is the highest.

### Current workarounds

- Manual `grim invoke <id>` after the user notices a failure — slow, requires human attention.
- A polling agent that subscribes to a hypothetical "failure topic" — doesn't exist yet, would have to be built per workflow, and burns tokens on retry logic the daemon should enforce.
- Pact chains that fire on completion only — no failure path exists in `pact` today, and even if it did, pacts spawn *new* agents rather than retrying the same one.
- Living with red scrolls and re-running them by hand.

## Goals

- **Per-agent restart policy as data.** `restart_policy` (`never` | `on_failure`), `max_restarts` (N), `restart_window_secs` (T), `escalate_to` (agent address or topic), `restart_count`, `last_restart_at`. Persisted in SQLite, surfaced in `circle` / `status`, queryable.
- **Daemon enforces budget.** A flapping agent can burn at most `N` retries in `T` seconds. Beyond that, the daemon refuses to restart and emits `RestartBudgetExhausted`. Agents cannot lie about their retry count — it lives in the database, not the prompt.
- **Escalation via the mail bus.** When budget is exhausted, the daemon sends a mail to `escalate_to` containing the failed agent's id and last error message. Recipient may be `agent://<id>` (live, dormant, or complete) or `topic://<name>`. Reuses every existing primitive: mail send, wake-on-mail, dormant resume.
- **`Restarting` transient state.** A new non-terminal agent state visible in `circle` and `scroll <id>` — distinct from `Failed`. Scroll dependencies do not fire until the supervised task reaches a final outcome (success or budget exhausted). Enables the "task succeeded after 2 retries" UX.
- **Daemon-wide safety nets.** A global rate cap on restarts (e.g. 30/min across all agents) plus a tree-depth cap (escalation cannot recurse beyond N levels). Both belt-and-suspenders against runaway loops that per-agent budgets alone don't catch.
- **Crash-safe.** Pending restarts persist; on daemon reboot, agents in `Failed` with active policy and unspent budget are queued for restart with their counters intact.
- **Banish is final.** `grim banish <id>` cancels all pending restarts and retires the policy atomically, mirroring the existing wake-source cascade.
- **Deterministic tests.** Reuse the `Clock` seam from the dormant-agents work for retry-window timing.
- **Orthogonal to `--keep-alive`.** A supervised agent can also be dormant. Failure → restart; while alive, normal wake sources still fire. The two concerns compose.
- **Summon-time CLI surface only at v1.** `grim summon ... --restart on_failure --max-restarts 3/60s --escalate-to <addr>`. Post-hoc policy editing (`grim supervise <id>`) deferred — adds CLI surface that v1 doesn't need to validate the design.
- **Degrades cleanly to "just retry."** Without `--escalate-to`, supervision is a pure retry policy. The escalation feature is a free addition on the same code path; if nobody uses it after a month, deletion is straightforward.

## Non-Goals (Explicit Scope Boundaries)

- **`restart: always` policy.** Only `never` and `on_failure` ship in v1. `always` was Erlang's "restart on success too" semantic; for agents, that overlaps with dormant + cron wake source we already shipped, and adds confusion without paying its way.
- **Exponential backoff or jitter.** Fixed 2-second delay between restarts in v1. Backoff is a v2 concern once we have data on real flap patterns.
- **OTP supervision strategies.** No `one_for_all`, `rest_for_one`, or `simple_one_for_one`. v1 is per-agent only — each agent's policy applies to itself, not to siblings. Strategies layer on top of v1 plumbing.
- **`grim supervise <id>` post-hoc command.** Summon-time only. Live policy editing is a v2 question once we know whether anyone needs it.
- **Cost-aware restart limits.** "Refuse to restart if it would exceed the per-tenant cost cap" depends on Part 1 budget primitives, which are not landed. Out of scope.
- **Dashboard UI.** CLI only at v1. Dashboard surfacing is a fast follow.
- **Restart of `Banished` or `Complete` agents.** Banish is terminal-final by definition; `Complete` is a success and `restart: on_failure` is the only ship-with-v1 policy. Neither path triggers a restart.
- **Cross-daemon escalation.** Escalation targets must be addressable on the local daemon. Federation is its own roadmap item.

## Proposed Solution

### Conceptual Overview

Three things arrive together:

1. **A supervisor as a daemon-internal actor.** A `Supervisor` struct, peer of `Scheduler` and `WakeRegistry`, owns: a `restart_history` table, the daemon-wide rate counter, the in-memory pending-restart queue, and the policy-evaluation logic. It subscribes to terminal `StateChange` events and decides whether to schedule a restart, escalate, or do nothing.

2. **A new transient agent state, `Restarting`.** Non-terminal, slot-free (the failed process exited). Used to communicate "we're inside a retry window" to scroll-keeper and `circle`/`scroll` consumers so they don't prematurely conclude the task failed.

3. **Restart fires through the existing dispatch path.** Just like wake-on-mail, a restart is "scheduler picks an eligible agent up and re-invokes it." The supervisor doesn't spawn processes itself — it marks the agent as needing restart, and `tick_now()` picks it up under capacity. This reuses the proven dispatch path and makes restarts respect the global `max_concurrent_agents` cap.

The flow becomes: **agent fails → supervisor consults policy + budget + global caps → either marks `Restarting` and queues (delay 2s) or fires `RestartBudgetExhausted` and sends escalation mail → scheduler dispatches the restart on its next tick.** Escalation mail wakes the parent through the existing mail-wake path; if the parent is itself dormant or supervised, the entire failure-recovery story composes naturally on primitives that already ship.

### User Journey

A team runs an overnight 8-task scroll where each task talks to a flaky third-party API.

1. The user inscribes the scroll and summons the entry agents with `--restart on_failure --max-restarts 3/60s`.
2. Task 4 fails at 02:14 (transient 502).
3. Supervisor sees `Failed`, consults policy, decrements budget (1/3 used), schedules a restart in 2s. Agent transitions `Failed → Restarting`. Scroll-keeper sees `Restarting`, does *not* fail dependent tasks, leaves the scroll in flight.
4. At 02:14:02, scheduler picks up the pending restart, dispatches the agent, transitions `Restarting → Active`. The retry succeeds. Agent transitions to `Complete`. Scroll-keeper sees the success, fires dependents.
5. The user wakes up to a green scroll. `grim circle --filter restarted` shows task 4 had one restart; `grim scroll <id>` shows the same.
6. (Counterfactual) If task 4 had failed three times in 60s, the supervisor would have emitted `RestartBudgetExhausted`, transitioned the agent to `Failed` (final), and sent escalation mail to whatever the user passed in `--escalate-to`. If that was a topic, the standing supervisor agent subscribed to it would wake and decide what to do. If it was a parent agent id and the parent was dormant, wake-on-mail would resurrect the parent with the failure context.

For a standing review team: `grim summon "review on PRs" --keep-alive --restart on_failure --max-restarts 5/3600s --escalate-to topic://human-review`. The agent wakes on PRs, reviews, sometimes crashes; if it crashes 5 times in an hour, escalation fires and a human gets paged.

## Architecture

### Data Model

A new SQLite table — `restart_history`:

- `id` (int PK autoincrement)
- `agent_id` (text, FK)
- `attempted_at` (int, unix seconds)
- `outcome` (text) — `scheduled` | `succeeded` | `failed_again` | `budget_exhausted`
- `error_summary` (text, nullable) — last error captured at failure time

Indexed on `(agent_id, attempted_at)` for window queries and on `(attempted_at)` for the daemon-wide rate counter.

New columns on `agents` (added via existing `ALTER TABLE … ADD COLUMN` probe pattern):
- `restart_policy` (text) — `never` | `on_failure` (default `never`)
- `max_restarts` (int, nullable)
- `restart_window_secs` (int, nullable)
- `escalate_to` (text, nullable) — `agent://<id>` or `topic://<name>`
- `restart_count` (int, default 0) — lifetime; restart_history is the windowed truth
- `escalation_depth` (int, default 0) — for the tree-depth cap; incremented when this agent's escalation triggers a restart on the recipient

`AgentState` gains a `Restarting` variant. Updated methods:
- `is_terminal()` returns true for Complete | Failed | Banished | Dormant. **`Restarting` is not terminal** (slot-free, but the lifecycle is mid-flight).
- `is_final()` unchanged (Complete | Failed | Banished only).
- `is_supervisable()` (new) — returns true if state is `Failed` (entry condition for supervisor evaluation).

`StreamEvent` gains four variants: `RestartScheduled`, `Restarted`, `RestartBudgetExhausted`, `Escalated`. All flow through `EventBus` and persist in the durable events table.

### System Boundaries

Everything lives inside `grimd`. The `Supervisor` is a peer of `Scheduler`, `WakeRegistry`, and `EventBus`. It talks to the database directly for persistence, to the bus for events, to the existing mail subsystem for escalation, and to the scheduler (via shared queue or a dedicated "pending restarts" lane) for dispatch.

### API Surface

CLI:
- `grim summon "<task>" --restart on_failure --max-restarts <N>/<T>s --escalate-to <addr>` — register policy at summon time.
- `grim circle` and `grim status <id>` surface restart count and policy in their output.
- `grim banish <id>` cascades to retire any pending restart and clear the policy.
- No `grim supervise` subcommand at v1 (deferred).

Daemon RPC: an internal `supervisor.cancel_pending(agent_id)` method invoked by the banish path; otherwise no new public RPCs at v1 (policy is set at summon time, read via existing `agent.get`).

### Integration Points

- **Mail bus.** Escalation fires send mail with `sender_id = "supervisor://<failed-agent-id>"` and `wake_eligible = true`. Body is the failed agent's id and last error message. Existing scheduler mail-wake path handles delivery to dormant recipients unchanged. Topic recipients fan out via the existing topic subscription path.
- **Scheduler.** `tick_now()` gains a `tick_supervision()` step between `tick_mail_wake()` and queue dispatch. Supervised agents with eligible pending restarts (delay window elapsed) are dispatched alongside queued and mail-woken agents under the same capacity cap.
- **Scroll-keeper.** Today subscribes to `StateChange` and matches on `Complete | Failed | Banished`. Adds an explicit no-op arm for `Restarting` so dependent tasks don't fire prematurely. The terminal handling stays unchanged: success on retry looks identical to first-try success; budget-exhausted failure looks identical to today's failure.
- **`grim banish`.** The existing banish-cascade hook (which already retires wake sources) gains a `supervisor.cancel_pending(agent_id)` call to drop any queued restart and clear policy fields. Mirrors the wake-registry cascade, fire-and-forget for resilience.
- **Event log.** All four new `StreamEvent` variants persist in the durable `events` table.
- **`Clock` seam.** Reused unchanged. Supervisor's restart-window evaluator and the global rate counter both consume `Arc<dyn Clock>`.
- **Dormant-agent path.** Untouched. A dormant agent that fails after wake follows the same supervised flow; escalation can target the parent that registered the wake source.

## Implementation Approach

### Recommended Pattern

The `WakeRegistry` from the dormant-agents work is the template. `Supervisor` mirrors its shape:

```
struct Supervisor {
    db: Arc<Database>,
    bus: EventBus,
    clock: Arc<dyn Clock>,
    mail: MailSender,
    pending: Mutex<BinaryHeap<PendingRestart>>, // delay-ordered
    global_rate: Mutex<RateCounter>,
}

impl Supervisor {
    fn on_state_change(&self, agent_id, new_state) -> Result<()>;     // entry from event bus
    fn evaluate(&self, agent_id) -> RestartDecision;                  // policy + budget + caps
    fn schedule_restart(&self, agent_id, delay) -> Result<()>;
    fn fire_escalation(&self, agent_id, error) -> Result<()>;
    fn cancel_pending(&self, agent_id) -> usize;                       // for banish cascade
    fn drain_due(&self, now: DateTime<Utc>) -> Vec<AgentId>;          // called by scheduler tick
}
```

The supervisor subscribes to the event bus for terminal `StateChange`s, makes one decision per failure, persists the decision to `restart_history`, and either schedules a delayed dispatch or escalates. The scheduler's `tick_supervision()` calls `supervisor.drain_due(now)` to pull agents whose 2-second window has elapsed and dispatches them through the existing path.

### Key Technical Decisions

| Decision | Choice | Rationale | Trade-offs |
|----------|--------|-----------|------------|
| `Restarting` as new state vs. flag on `Failed` | New state | Honest lifecycle; consumers (scroll-keeper, dashboard, `circle`) can pattern-match cleanly; avoids the dormant-overload problem we just fixed | One more state to maintain |
| Restart delay | Fixed 2s | Simplest; testable with `TestClock`; covers the transient-provider case | No backoff for genuinely flapping providers — v2 concern |
| `always` policy | Dropped from v1 | Conflates with dormant + cron wake source; unclear semantics for agents | Users who want "restart on success" must use a wake source instead |
| Restart history storage | New `restart_history` table | Clean window queries; fast indexed lookups; auditable | ~50 lines of migration; one more table to back up |
| Daemon-wide rate cap | Global counter (e.g. 30/min) | Insurance against runaway pathologies a per-agent budget alone misses | One more knob; need a sensible default |
| Tree-depth cap | Track `escalation_depth` per agent, refuse restart beyond N | Prevents infinite up-the-tree recursion if parents are also supervised | Slight bookkeeping in mail-wake path to propagate depth |
| Escalation transport | Mail bus, agent or topic address | Reuses every primitive; topic case enables standing-supervisor agents | Escalation isn't visually distinct from regular mail unless the recipient gates on `sender_id` prefix |
| Escalation payload | Agent id + last error message | Compact; recipient can fetch transcript via existing APIs | Recipient needs another call to get full context |
| Crash recovery | Resume pending restarts on boot | Matches OTP; daemon flap shouldn't quietly drop recovery | Requires the pending queue to be reconstructable from `restart_history` + agent state |
| Banish vs. policy | Banish is final, cancels pending restarts and clears policy | Mirrors existing wake-source cascade; matches user intuition | A user who wants "kill but keep policy" must use a hypothetical future `suspend` command |
| `--restart on_failure` without `--escalate-to` | Permitted; pure retry, no escalation | Clean degradation to "just retry" — Alternative C is a free subset | Two valid configurations to test |

### Rough Task Breakdown

1. **State machine + schema.** Add `Restarting` to `AgentState`, update `is_terminal` / `is_final`, add `is_supervisable`. Add `restart_history` table and the new `agents` columns via the existing probe-and-add migration pattern. Audit every `match AgentState` callsite.
2. **`Supervisor` skeleton.** The actor, the rate counter, the pending-restart heap, the event-bus subscription. No CLI yet — just the policy machinery driven from tests.
3. **Scheduler integration.** Add `tick_supervision()` between `tick_mail_wake()` and queue dispatch. Wire `supervisor.drain_due()` and dispatch through the existing path. Capacity-respecting.
4. **Escalation path.** Mail send with `supervisor://` sender, agent-or-topic resolution, `Escalated` event, escalation_depth bookkeeping.
5. **Banish cascade + crash recovery.** `supervisor.cancel_pending()` on banish; on daemon boot, scan for `Failed` agents with active policy and unspent budget, queue restarts.
6. **CLI surface.** `--restart`, `--max-restarts`, `--escalate-to` flags on `grim summon`; restart count + policy in `circle` / `status` output.
7. **Scroll-keeper integration.** Add the `Restarting` no-op arm. Verify dependents do not fire on transient state. Update the scroll display to show "(restarting)" annotation.
8. **Test surface.** Restart on failure, budget exhaustion, escalation to agent and topic, `never` policy doesn't restart, banish cancels pending, crash recovery resumes pending, global rate cap, tree-depth cap, deterministic via `TestClock`.

Eight chunks, with chunks 1–3 forming the foundation and 4–7 being independent layers on it. Roughly the same shape as the dormant-agents plan that just shipped.

### Riskiest Part

**Crash-recovery semantics.** On daemon boot, the supervisor must reconstruct its pending-restart queue from `restart_history` + agent state without double-firing or dropping. The rules: an agent in `Failed` (final) with `restart_policy = on_failure`, unspent budget (windowed restart count < `max_restarts`), and no `Escalated` event since the last failure → queue a fresh restart with `delay = 0` (the original window has elapsed during downtime, so don't wait further). An agent in `Restarting` on disk at boot is a torn write; promote it to `Failed` and re-evaluate from scratch. The integration test must exercise: clean shutdown with pending restart → boot → fires; crash mid-restart → boot → re-fires once; crash after escalation → boot → does not re-escalate.

Adjacent risk: **scroll-keeper regressions.** Adding `Restarting` to the state machine without auditing every scroll-keeper callsite could mean a transient state slips through as either "task done" or "task failed." Mitigation: explicit no-op arm with a debug log; integration test that runs a scroll with a deliberately failing task and asserts dependents fire only after the final outcome.

## Edge Cases & Decisions

| Edge Case | Decision | Rationale |
|-----------|----------|-----------|
| Agent fails outside its restart window | Window resets on each failure; budget counts only failures inside `restart_window_secs` looking back from `now()` | Standard sliding-window semantics; matches user expectation |
| User passes `--max-restarts 3/60s` and the agent fails three times in 5s | All three restarts fire (each one starts a fresh window for the *next* failure); fourth failure within 60s of first triggers budget-exhausted | Window slides per-failure, not per-restart; simpler to reason about |
| Escalation target is `topic://` with zero subscribers | Mail rows still written to subscribers (none), `Escalated` event fired, agent stays `Failed` | Existing topic semantics handle the empty case; no special path |
| Escalation target is a banished agent id | Mail row marked `Failed` by existing mail subsystem; `Escalated` event still fired so audit is intact | Supervisor doesn't pre-validate the target; mail bus handles delivery failure |
| Tree-depth cap exceeded | Supervisor refuses the restart, fires `RestartBudgetExhausted` with `tree_depth_exceeded` reason, no escalation (would just push deeper) | Hard stop; user must manually intervene |
| Daemon-wide rate cap exceeded | Supervisor delays the individual restart by 60s and re-checks; emits `RestartScheduled` with `rate_limited = true` | Don't drop the restart; just throttle |
| Agent banished while in `Restarting` | Pending restart cancelled, agent transitions to `Banished`, policy cleared | Banish wins; mirrors wake-source cascade |
| `--escalate-to topic://...` and the failed agent itself is subscribed | Loop detected at fanout time (existing topic publish should already exclude self-sends; if not, supervisor adds the check) | Trivial loop; cheap to prevent |
| Two failures land for the same agent in <2s | Second is a no-op; the agent is already `Restarting` | Idempotent state transition |
| Crash mid-dispatch (agent transitioned `Restarting → Active` but daemon dies before exec) | Boot promotes to `Failed`, re-evaluates; counts as one budget consumption | Conservative; never dispatch a stale restart |
| `restart_policy = never` with `--escalate-to` set | Reject at summon; `--escalate-to` requires a non-`never` policy | Otherwise it does nothing; fail fast at the CLI |
| `max_restarts = 0` | Equivalent to `restart_policy = never`; reject at summon for clarity | Avoid silent no-op configurations |
| Failed agent with policy but `escalation_depth >= cap` and no remaining budget | Both gates fire; `Escalated` not sent; `RestartBudgetExhausted` event with reason includes both | Single terminal event; user sees one reason |
| Supervised agent inside a scroll, scroll is abandoned | Scroll abandonment cascades through existing scroll-keeper teardown; supervisor cancels pending restart on `Banished` | Reuses existing teardown; no special supervisor path |
| Wake-on-mail and supervised agent both target the same agent | Orthogonal; mail wake fires when agent is `Dormant`, supervisor fires when agent is `Failed` — disjoint state spaces | Two state-gated paths; no contention |

## Security Considerations

- **No new external surface.** The supervisor runs entirely inside `grimd`. Escalation fires through the existing mail bus, which already terminates at agents on the local daemon.
- **Escalation sender authentication.** Wake fires use `sender_id = "supervisor://<failed-agent-id>"`. Receiving agents that gate on sender identity must treat that prefix as system-originated. Critically, an agent must not be able to forge a `supervisor://` sender via `mail.send`; the mail subsystem should already reject reserved prefixes at the public RPC boundary, and we add `supervisor://` to that list as part of this work.
- **Escalation payload size.** The mail body is the failed agent's id and last error message, capped at the existing 16 KiB folded-resume limit. Errors longer than that get truncated with a marker, matching mail-wake behavior.
- **Audit trail.** Every restart, escalation, budget exhaustion, and rate-limit event is a `StreamEvent` row in the durable events log. Operators answer "why did this agent restart at 03:14?" from the log alone.
- **Rate-limit prevents resource exhaustion.** Per-agent budget plus daemon-wide rate cap plus tree-depth cap together bound the worst-case burn rate from a misconfigured tree. None of the three alone is sufficient; the combination is.
- **Banish cascade prevents orphan restarts.** A banished agent cannot have a pending restart fire after its lifecycle ends; the banish path explicitly drops queued restarts.
- **No code execution from policy.** Restart policy is data (enum + numbers + addresses). No expression language, no eval, no shell-out.

## Failure Modes & Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| State migration misses a callsite that should handle `Restarting` | Med | Med (transient state surfaces as "active" or "failed" wrongly in some UI) | Audit every `match AgentState` and every `is_terminal` callsite during chunk 1; integration test that asserts every state transition emits the right scroll-keeper behavior |
| Crash recovery double-fires a restart | Med | Med (extra provider call, extra mail row) | Boot reconciliation is idempotent; restart_history rows checked before queueing; integration test for the crash-mid-dispatch case |
| Daemon-wide rate cap starves legitimate concurrent retries | Low | Med (real restarts get delayed) | Sensible default (30/min); cap is configurable via `tome`; emit visible event when triggered so operators see throttling |
| Escalation creates a tight loop (parent restarts on child escalation, recursively) | Med | High (without depth cap, infinite mail) | Tree-depth cap is the explicit defense; `escalation_depth` propagates through mail-wake; integration test for a 3-level supervisor tree that hits the cap |
| Escalation mail piles up on a banished or unsubscribed topic | Low | Low (mail rows accumulate but are bounded by the existing retention policy) | Topic recipients with zero subs is a no-op; banished-recipient mail follows existing `Failed` semantics |
| Race: agent transitions `Failed → Restarting → Active` while a banish is in-flight | Med | Med (banish lands on Active agent, kills it mid-restart, supervisor sees a fresh failure) | Banish path explicitly cancels pending restarts and clears policy *before* killing the process; supervisor checks policy on every `on_state_change` and is a no-op if cleared |
| Boot-time restart queue grows large after a long outage | Low | Med (restart storm at boot) | Stagger boot-time restarts under the daemon-wide rate cap; document the behavior |
| Restart history grows unbounded | Low | Low (one row per failure; even a busy team is small) | Existing event-log retention covers; document the table; consider TTL in v2 if it becomes a real number |
| User passes `--escalate-to agent://<self>` (self-loop) | Low | Low (escalation lands in own mailbox, agent is `Failed` so won't wake from supervised mail unless it's also dormant) | Reject at summon-time validation; cheap |
| Scroll-keeper sees `Restarting` and treats it as completion | Low if test exists, High if missed | Med | Explicit no-op arm with test; the riskiest single regression in this work |
| `restart_window_secs` is set to a value larger than `restart_history` retention can survive | Low | Low | Document a recommended maximum (e.g. 7d) |
| Supervised agent's last error message contains shell metachars or prompt-injection content | Med | Low (it's data in mail body, not an exec'd string) | Mail bodies are inert text by construction; recipient agents are responsible for treating mail as untrusted input (same as today) |
| The `agent_state.is_terminal()` change breaks slot accounting for `Restarting` | Low | High (slot leak) | `Restarting` is non-terminal but slot-free; explicit slot-management test |

## Open Questions

- [ ] What is the right default for the daemon-wide restart rate cap? 30/min is a starting guess; needs a sanity check against expected scroll workloads (an 8-task scroll with 3 retries each = 24 max, well under 30/min).
- [ ] Should the supervisor expose a `grim supervisor stats` command for operators? Cheap to add; defers to "do we need this for v1 demos?" — leaning no, defer to dashboard work.
- [ ] Tree-depth cap default — is N=3 enough for realistic trees? Most production scrolls are 2 levels (scroll → tasks); only standing supervisor agents go to 3. 3 with override might be right.
- [ ] When `restart_policy = on_failure` is set on a non-supervisable agent (e.g. a daemon-internal task), should we silently no-op or reject? Probably reject at summon time.
- [ ] Should `grim circle --filter "restart-active"` be a v1 query (agents currently `Restarting`)? Cheap; useful for the demo. Defer to spec.

## Alternatives Considered

### Pact-on-failure
**Description:** Extend the existing `pact` chain to fire on `Failed` (currently only `Complete`). User writes "on failure of X → summon Y" as a pact rule.
**Rejected because:** It's per-agent wiring, not policy. No retry budget, no concept of "restart the same agent." The user would build a recovery agent for every workflow. It also doesn't compose: pact spawns *new* agents and can't restart the failed one. Useful as a future feature on top of the supervisor, not a substitute.

### Failure wake source
**Description:** Add a new wake-source kind (`agent_failed`) so a parent agent wakes when a child fails. Parent decides what to do.
**Rejected because:** Pushes policy into agent prompts — the daemon should enforce restart budgets, not hope an LLM does. Burns tokens on retry logic. Doesn't centralize the audit trail. It's also conceptually overlap with the parent-completion wake source, which already half-does this.

### Bare retry flag (Alternative C)
**Description:** Just `grim summon --max-restarts 3`, no escalation, no parent linkage, no `Restarting` state — daemon retries the same task on failure, gives up after N.
**Rejected because:** Captures ~70% of the value but rules out the differentiator. Specifically, no escalation chain means failure can't bubble up the parent tree, which is the unique value proposition versus LangGraph/CrewAI/Temporal. **However**: the scoping in this plan is deliberately "C is a free subset of full supervision" — `--restart on_failure --max-restarts 3` *without* `--escalate-to` *is* Alternative C, and works on the same code path. This rejection is about not stopping there.

### OTP supervision strategies in v1
**Description:** Ship `one_for_all`, `rest_for_one`, `simple_one_for_one` semantics from the start, not just per-agent restart.
**Rejected because:** Strategies require a notion of sibling agents and shared lifecycle, which isn't a primitive yet. Adding strategies on top of v1's per-agent supervision is straightforward; building strategies first means inventing the sibling primitive prematurely. v2 work, after v1 has data on real usage.

### Exponential backoff in v1
**Description:** Restarts at 1s, 2s, 4s, 8s … capped at e.g. 60s.
**Rejected because:** Adds state to track and test. Real provider flap patterns aren't well-characterized yet — a fixed 2s delay is good enough for the transient-provider case the demo cares about, and doesn't preclude swapping in backoff later. v2.

### Restart with a fresh agent ID
**Description:** Each restart spawns a new agent (new id, new history) rather than reusing the failed one's session.
**Rejected because:** Loses transcript continuity and the agent's context, which is exactly what makes Grimoire's daemon-native story work. The "self-healing pipeline" demo only sells if the retried agent has the same context and session as the failed one. Reusing the existing agent id is the whole point.

### Supervision metadata in the prompt
**Description:** Don't track restart count in the database — inject it into the agent's prompt on each restart so the LLM can see how many retries are left.
**Rejected because:** Agents would then be able to lie about, manipulate, or "negotiate" their retry budget by pattern-matching on the prompt. The whole point of daemon-enforced policy is that the LLM cannot subvert it. Surfacing the count in the resume prompt for *transparency* is fine and probably good; making it the source of truth is not.

---

*This plan captures the "what", "why", and high-level "how". It is input for `/hatch:write-spec`, which will produce the detailed implementation specification — including the schema migration SQL, the `Supervisor` actor's exact API, file-by-file change list, and per-task acceptance criteria.*
