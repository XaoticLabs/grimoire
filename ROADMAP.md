# Grimoire — Roadmap & Vision

A consolidated brain-dump of gap analysis and feature direction for evolving Grimoire from a personal daemon-based orchestrator into a staff-level, enterprise-grade, _daemon-native_ agent fabric.

---

## Thesis

> **Agents are processes, not function calls.**
> Grimoire is the `systemd` + `kubelet` + `nats` for AI workers.

Most orchestrators (LangGraph, CrewAI, AutoGen, even Temporal) treat agents as function invocations inside a parent program. Grimoire treats them as **long-lived, addressable processes with identity**. That unlocks a different product entirely.

Every feature below should ladder up to this thesis. If a feature only makes sense because agents are daemonized (stable IDs, outlive callers, addressable, observable, restartable), it belongs. If it could live in a Python library just as well, it's not the differentiator.

---

## Part 1 — Gap Analysis (Current State → Enterprise-Grade)

Current code: ~5.8K lines, single-node daemon, UDS + HTTP, SQLite, no auth, limited concurrency testing.

### Blockers for "enterprise-grade"

**Security & multi-tenancy**

- No authN/authZ on UDS or HTTP — anyone with local access controls every agent. No API tokens, no RBAC, no audit log of _who_ summoned what.
- HTTP dashboard binds `127.0.0.1` with no TLS, no CSRF, no session management. Fine for a dev tool, disqualifying for shared infra.
- No sandboxing of agent processes (cwd confinement, seccomp, cgroups, per-agent resource limits). A summoned agent inherits the daemon's full env and filesystem.
- Secrets in `config.toml` as plaintext. No integration with a secret manager (Vault, AWS SM, 1Password).

**Reliability & operational surface**

- No structured observability: `tracing` is wired but there's no OTel export, no Prometheus `/metrics`, no health endpoint beyond `status`.
- No crash recovery semantics documented — what happens to in-flight agents when `grimd` restarts? (`process_manager.rs` is 139 lines; worth verifying reattach vs. orphan.)
- SQLite with no migration framework (`persistence.rs` is 1K lines of hand-rolled schema). No backup/restore story, no WAL tuning guidance, no retention policy for the events table.
- No rate limiting / concurrency ceilings at the daemon level (scrolls have `-c N`, but a user can still `summon` infinitely).
- Graceful shutdown: need SIGTERM → drain → kill timeout → persist. Unclear if that exists.

**Scale boundary**

- ~~Single-node only. Staff-level orchestration usually means: multiple daemons, a scheduler, and agents running on worker pools (k8s jobs, Nomad, or at least remote SSH workers). Right now every agent is a child process of one `grimd`.~~ ✅ *partially addressed: `grimw` worker pool + remote executor + capability placement now land tasks on remote workers (see Part 2 §1).*
- No queue — if you `summon` 200 tasks, 200 processes spawn. Needs a proper work queue with priority, backoff, and admission control.
- ~~Event bus is `tokio::broadcast` (in-memory, lossy on slow subscribers).~~ ✅ *now write-through to SQLite `events` with per-stream sequence numbers (see Part 2 §3); upgrade path to NATS/Redis still open.*

**Correctness gaps worth tests for**

- Scroll DAG: cycle detection, file-conflict serialization under contention, cascade-failure semantics. Tests exist (`scroll_lifecycle.rs`) — audit coverage of partial-failure and restart mid-scroll.
- Pact chains: loop detection, `{output}` injection with adversarial content (shell metachars in task templates → command injection risk in `args_template`).
- Process reaping on daemon crash (zombie prevention).

### Table-stakes features missing

- **Agent artifacts**: no structured capture of files changed, diffs, or cost/token accounting per agent beyond what Claude streams. An orchestrator should own that record.
- **Approval gates / HITL**: no way to pause a scroll for human review between tasks. Enterprise workflows always need this.
- **Policy engine**: allow/deny rules per provider, per cwd, per user (e.g. "aider cannot touch `infra/`").
- **Retries & idempotency**: no task-level retry policy, no idempotency keys on `summon`.
- **Notifications**: Slack/webhook on scroll completion or failure.
- **Multi-user**: everything is implicitly single-user (`~/.grimoire/`). No concept of tenant, workspace, or project.

### Product-polish gaps

- No `grim logs <id>` separate from `bind` (historical log retrieval with filtering).
- No export/import of scrolls (share a spec + its execution history).
- Dashboard is a single `index.html`; no auth, no dark-mode persistence, no agent filtering/search at scale.
- No versioned protocol — `protocol.rs` has no `version` field, so older CLIs vs. newer daemons will break silently.

### Hardening milestone (first to tackle)

1. **Protocol versioning + auth token on UDS/HTTP** (one afternoon, unblocks everything else).
2. **Prometheus `/metrics` + OTel tracing export** (operability baseline).
3. **Resource limits per agent** (cgroups on Linux, `rlimit` fallback) + cwd jail.
4. ~~**Durable queue with admission control** — replace "spawn immediately" with "enqueue → scheduler pulls."~~ ✅ *single-node stage — see Part 2 §2.*
5. **Policy engine** (even a simple allowlist of cwd prefixes and providers per token).
6. ~~**Remote worker protocol** — let `grimd` dispatch to worker nodes, not just `tokio::process`.~~ ✅ *done — see Part 2 §1.*

---

## Part 2 — Scale & Distribution

### 1. Worker pool protocol — `grimd` ↔ `grimw` ✅ *implemented*

- `grimd` becomes the control plane. `grimw` is a thin worker binary that registers, heartbeats, advertises capabilities:
  ```json
  {
    "providers": ["claude", "codex"],
    "cwd_roots": ["…"],
    "gpu": false,
    "max_concurrent": 4
  }
  ```
- Registration over gRPC or QUIC (mTLS). Workers can be local LAN boxes, SSH-reached servers, k8s pods, or ephemeral cloud VMs spun up on demand.
- Scheduler in `grimd` does capability-aware placement: "this task needs `aider` + access to `~/repos/frontend` → worker-3."
- **Unique angle**: workers can be _user laptops_. A dev team pools idle compute; agents schedule across the team's machines. No other orchestrator offers this.
- *Status: `grimw` binary, worker registry, worker RPC server, remote executor, and capability-aware placement all landed (`src/grimw/`, `src/daemon/worker_registry.rs`, `src/daemon/worker_rpc_server.rs`, `src/daemon/executor.rs`, `tests/placement.rs`, `tests/executor_remote.rs`, `tests/worker_proto.rs`). Remaining: mTLS, ephemeral cloud workers, true cross-LAN registration UX.*

### 2. Durable work queue with admission control ✅ *implemented (single-node stage)*

- Replace "summon spawns immediately" with "summon enqueues."
- Queue backed by SQLite (same node) or Postgres/NATS JetStream (distributed).
- Priority lanes, per-tenant quotas, token-bucket rate limits, exponential backoff on provider errors.
- Admission policies: `max_concurrent_per_cwd`, `max_cost_per_hour`, `quiet_hours`.
- *Status: SQLite-backed queue with priority field, daemon-owned `Scheduler` (`src/daemon/scheduler.rs`) that promotes `Queued → Active` under a global `max_concurrent_agents` cap and worker-eligibility check, atomic `claim_for_dispatch` + requeue-on-failure, `block_reason` surfaced via `grim queue` (`src/cli/commands/queue.rs`). Reactor wakes on `AgentQueued` / `WorkerRegistered` / terminal `StateChange` plus a 100ms safety tick. Covered by `tests/scheduler.rs`, `tests/scheduler_integration.rs`, `tests/cli_queue.rs`. Remaining: per-tenant quotas, token-bucket rate limits, exponential backoff on provider errors, `max_concurrent_per_cwd` / `max_cost_per_hour` / `quiet_hours` policies, and a Postgres/NATS JetStream backend for distributed mode.*

### 3. Durable event log (replaces `tokio::broadcast`) ✅ *implemented (SQLite stage)*

- Append-only event log per agent + a global stream. Offsets, replay, retention.
- Start with SQLite-backed log + subscription cursors; upgrade path to NATS JetStream or Redis Streams without API change.
- Every client (`bind`, dashboard, webhooks, downstream agents) is just a consumer with an offset.
- *Status: `EventBus` writes through to the SQLite `events` table with per-stream contiguous sequence numbers; durability covered by `tests/event_log_durability.rs`. Remaining: replay/cursor API, retention policy, NATS/Redis upgrade path.*

---

## Part 3 — Daemon-Native Features (the real differentiators)

These are only possible because agents are daemons.

### 4. Agent-to-agent messaging bus ✅ *implemented (v1)*

- Every agent has an address (`agent://4a8c1b2f`) and a mailbox. Agents can `send(target, message)` and `recv()`.
- `grim pact` is the static form of this; the bus is the dynamic form. Agent A spawns agent B mid-task and messages it: "here's a subproblem, report back."
- Pub/sub topics too: `topic://reviews/frontend`, `topic://alerts/build-failed`. An agent subscribes and wakes up when matched.
- Turns Grimoire into a genuine multi-agent substrate.
- **Demo**: a "reviewer" agent subscribes to `topic://pr-opened`, wakes, reviews, publishes to `topic://pr-reviewed`, sleeps. Never exits. Lives for weeks.
- *Status (v1):*
  - Two new SQLite tables — `mail` (per-recipient `seq`, `wake_eligible`) and `subscriptions` (UNIQUE on `(subscriber_id, topic)`). Schema additions are `IF NOT EXISTS` so the migration is automatic.
  - Strict address parser (`agent://[0-9a-f]{8}` and `topic://[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}`) lives in `src/shared/mail.rs`.
  - Six new RPC methods: `mail.send`, `mail.list`, `mail.ack`, `mail.subscribe`, `mail.unsubscribe`, `mail.topics`. Topic publish is a single-transaction snapshot fanout (one row per subscriber); banished subscribers get `Failed` rows excluded from `delivered`.
  - Four new `StreamEvent` variants — `MailSent` / `MailReceived` / `MailDelivered` / `MailFailed` — flow through `EventBus` and the durable `events` log just like every other stream.
  - CLI surface: `grim mail send/list/ack/subscribe/unsubscribe/topics`.
  - **Wake-on-mail in the scheduler** (Part 5 §3 plus a foothold for §5): `Scheduler::tick_now()` runs `tick_mail_wake` before queue dispatch. Any `Complete` agent with a `session_id` and at least one `Pending`/`wake_eligible=1` mail row gets folded mail (joined with `\n\n---\n\n`, capped at 16 KiB, truncation-noted) and is invoked via `AgentManager::invoke()`. Capacity-respecting; failed `invoke()` leaves rows `Pending` for retry. `MailReceived` is in `should_wake()`.
  - Tests: `tests/database.rs` (14 mail/subscription helper tests, including idempotent subscribe and limit clamp) and `tests/scheduler_mail_wake.rs` (6 integration tests covering fold, no-session skip, banished skip, `wake_eligible=0`, and invoke-failure rollback).
  - Spec: `.claude/specs/agent-messaging-bus-spec.md`. Plan: `.claude/plans/agent-messaging-bus.md`.
- *Open (v2):* batch direct send (`to: [...]`), per-recipient rate limit / debounce on hot topics, auth on the local UDS so `sender` can be trusted, prefix resolution for `mail.ack` IDs, and a "system" stream for human-originated `MailSent` events.

### 5. Long-lived / dormant agents ("familiars") ✅ *implemented (v1)*

- Agents that don't exit after one task. They suspend (serialize context to disk) and wake on: incoming message, cron, file-watch, webhook, or another agent's completion.
- `invoke` for completed agents already hints at this — formalize into a first-class "dormant" state with wake triggers.
- This is the _real_ reason to be a daemon. No library-based orchestrator can do this cleanly because its process dies when the script ends.
- *Status (v1):*
  - First-class `AgentState::Dormant` with `is_terminal()` / `is_final()` split (`src/shared/types.rs`); auto-migration of `Complete`-with-session agents on boot (`tests/dormant_migration.rs`, `tests/dormant_state.rs`).
  - `WakeRegistry` actor (`src/daemon/wake_registry.rs`) owning a `wake_sources` table and three wake-source kinds: `cron`, `file_watch`, `parent_completion` (`src/daemon/wake_sources/`). All fire through the existing mail bus so the resume path is identical to mail-wake.
  - `Clock` seam (`src/daemon/clock.rs`, `tests/clock_seam.rs`) for deterministic cron and rate-limit tests.
  - Per-agent token-bucket rate limit on wake fires (`tests/wake_rate_limit.rs`).
  - `grim summon --keep-alive` to land in `Dormant` with no sources yet (`src/cli/commands/summon.rs`, `tests/keep_alive_summon.rs`).
  - `grim wake add/list/remove/test` CLI group (`src/cli/commands/wake.rs`).
  - `grim banish` cascades to retire all wake sources atomically (`tests/banish_cascade.rs`).
  - Four new `StreamEvent` variants — `WakeSourceRegistered` / `WakeSourceFired` / `WakeSourceFailed` / `WakeSourceRetired` — flow through `EventBus` and the durable events log (`tests/wake_events.rs`, `tests/wake_schema.rs`, `tests/wake_registry_cron.rs`, `tests/wake_file_watch.rs`, `tests/wake_parent_completion.rs`).
  - Spec: `.claude/specs/dormant-agents-wake-triggers-spec.md`. Plan: `.claude/plans/dormant-agents-wake-triggers.md`.
- *Open (v2):* webhook wake source (gated on Part 1 auth-token hardening), dashboard surfacing for wake sources, and tunable file-watch debounce / glob exclusion patterns.

### 6. Context-as-state, not as prompt

- Because agents are processes, their context window is a resource the daemon owns.
- Expose it: `grim context <id>` shows the current window, `grim context <id> --compact` triggers summarization, `grim context <id> --fork` branches the agent.
- Agents share a **working memory store** (daemon-managed KV or vector store): `memory.put("design-decisions/auth", …)`. Namespaced per scroll, per tenant.
- Replaces ad-hoc "copy previous agent's output into next prompt." It's how a team of agents builds shared understanding.

### 7. Filesystem as the shared blackboard

- Workspaces are first-class: `grim workspace create feature-auth` → daemon provisions a worktree, mounts it read-write for assigned agents, read-only for observers.
- Agents see each other's file changes in real time. File-watch events become bus events: "agent-2 wrote `src/auth.rs`, agent-5 is subscribed."
- Generalizes existing scroll conflict detection to the whole fabric.

### 8. Supervision trees (OTP-style) ✅ *implemented (v1)*

- Borrow from Erlang. Every agent has a supervisor policy: `restart: always | on_failure | never`, `max_restarts: 3 in 60s`, `escalate_to: <parent-id>`.
- Scrolls become supervision trees naturally. If a child fails too often, the parent agent wakes with the failure and decides: retry with different provider, decompose further, give up.
- Self-healing agent pipelines — totally unique in the space.
- *Status (v1):*
  - `Supervisor` actor (`src/daemon/supervisor.rs`, 631 LoC) owning restart policy, max-restarts window, and escalation routing; integrates with `AgentManager`, scheduler dispatch, and the durable events log.
  - Schema additions for parent/child links and restart history (`tests/supervision_schema.rs`); supervision-aware state transitions (`tests/supervision_state.rs`); crash-recovery reconciliation on daemon boot (`tests/supervisor_crash_recovery.rs`, `tests/supervisor_history_reconcile.rs`).
  - Restart / escalate / banish-cascade behavior covered by `tests/supervisor_actor.rs`, `tests/supervisor_dispatch.rs`, `tests/supervisor_escalation.rs`, `tests/supervisor_banish.rs`, `tests/supervisor_evaluate.rs`, `tests/banish_cascade.rs`, `tests/scroll_keeper_supervision.rs`, `tests/supervision_events.rs`.
  - CLI surface: `grim summon --supervisor …` and `grim circle` supervision view (`tests/cli_summon_supervision.rs`, `tests/cli_circle_supervision.rs`).
  - Spec: `.claude/specs/supervision-trees-spec.md`. Plan: `.claude/plans/supervision-trees.md`.
- *Open (v2):* dashboard surfacing of supervision trees, richer escalation policies (route to topic / external webhook), and per-policy budget/cost guards.

### 9. Time-travel & replay

- Because every event is durable, `grim replay <agent-id> --until <event>` reconstructs exactly what the agent saw.
- Fork from any point: "this agent went off the rails at step 12, fork at step 11 with a different system prompt."
- Powerful for debugging agent misbehavior and for eval/regression testing.

### 10. Policy & budget as daemon primitives

- `grim budget create team-frontend --daily $50 --providers claude,codex`. Daemon refuses to spawn when exhausted.
- Policy bundles: "this tenant's agents cannot run `rm`, cannot touch `infra/`, must approve before network calls." Enforced at the daemon, not hoped-for in the prompt.
- The enterprise sell. Only works because the daemon sits between the user and the agent.

### 11. Federation

- Two `grimd` instances peer with each other. Agents on daemon-A can message agents on daemon-B. Scrolls can span daemons.
- Use case: a laptop daemon delegates heavy work to the office-server daemon, which delegates sandboxed execution to an ephemeral cloud daemon. All one fabric.

### 12. Introspection & eval as first-class

- `grim eval <agent-id> --rubric <file>` runs an evaluator agent against a completed agent's transcript. Results stored alongside the agent.
- Aggregate: `grim circle --eval-score <0.7` to find underperformers. Staff-level teams need this.

---

## Part 4 — Killer Demos

Pick 2–3 of these and the product sells itself:

- **Standing review team** — 3 dormant agents subscribed to `topic://pr-opened`. They wake on every PR, review in parallel, post to `topic://pr-reviewed`, sleep. Running for 30 days. *Now feasible end-to-end — `grim mail subscribe` + scheduler wake-on-mail are in place; only needs a webhook → `mail.send` bridge to drive it from real GitHub events.*
- **Swarm decompose** — One agent gets a vague spec, spawns 5 children to explore approaches in parallel worktrees, messages them, picks the winner, reunifies. Visible live in `grim scry`. *Spawning + per-agent messaging are in place; the missing piece is the parent's "messages me back" loop, which `mail.list` covers but agents need a wrapper that drains it at turn boundaries.*
- **Laptop grid** — `grim worker register` on 4 dev laptops. `grim inscribe big-migration.md -c 12` distributes 12 parallel agents across the team's idle machines.
- **Self-healing pipeline** — A scroll runs overnight, one task fails, supervisor restarts with a different provider, succeeds, pipeline completes. You wake up to a green scroll and a report of what self-recovered.
- **Fork and race** — `grim fork <id> --variants "use redis,use postgres,use sqlite"` spawns 3 forks of the same agent with different directions. Winner by eval score gets merged.

---

## Part 5 — Suggested Build Order

Six-month spine:

1. ~~**Durable event log** — foundation for everything else. Replace `broadcast` first.~~ ✅
2. ~~**Work queue + admission control** — cheap once the log exists.~~ ✅ *single-node stage; see Part 2 §2.*
3. ~~**Agent-to-agent messaging bus** — tiny delta over the log, huge product unlock.~~ ✅ *v1 shipped — see `.claude/specs/agent-messaging-bus-spec.md`.*
4. ~~**Worker pool protocol** — unlocks scale story and "laptop grid" demo.~~ ✅
5. ~~**Dormant agents with wake triggers** — THE differentiator. This is what being a daemon is _for_.~~ ✅ *v1 shipped — see Part 3 §5 / `.claude/specs/dormant-agents-wake-triggers-spec.md`.*
6. ~~**Supervision trees** — layers on top of above; makes scrolls self-healing.~~ ✅ *v1 shipped — see Part 3 §8 / `.claude/specs/supervision-trees-spec.md`.*
7. **Shared memory store + workspaces** — enables real multi-agent collaboration.
8. **Federation** — last, after single-fabric is solid.

The through-line: **every feature above is impossible or awkward in a library-based orchestrator and natural in a daemon.** That's the story. Lead with it in the README; every feature should visibly reinforce it.
