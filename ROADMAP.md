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

### 6. Context-as-state, not as prompt ✅ *partially implemented (memory KV)*

- Because agents are processes, their context window is a resource the daemon owns.
- Expose it: `grim context <id>` shows the current window, `grim context <id> --compact` triggers summarization, `grim context <id> --fork` branches the agent.
- Agents share a **working memory store** (daemon-managed KV or vector store): `memory.put("design-decisions/auth", …)`. Namespaced per scroll, per tenant.
- Replaces ad-hoc "copy previous agent's output into next prompt." It's how a team of agents builds shared understanding.
- *Status (v1):*
  - **Memory KV done** — workspace-scoped, SQLite-backed, optimistic CAS via per-key version. `memory.put/get/list/delete` RPC + `grim memory` CLI. Writes emit `MemoryWritten` / `MemoryDeleted` stream events plus segment-prefix topic mail (`topic://workspace/<id>/memory/<prefix>`) so subscriber agents wake via the existing mail-wake plumbing. Per-value (256 KiB) and per-workspace (64 MiB) caps; segment-aligned `prefix` filter on `list`. Reserved sender prefix `workspace://` added to the `mail.send` guard so the daemon's writes can't be forged.
  - Spec: `.claude/specs/shared-memory-workspaces-spec.md` (Tasks 1–2). Plan: `.claude/plans/shared-memory-workspaces.md`. Tests: `tests/workspaces_e2e.rs` (CAS conflict, segment-prefix list, put/get roundtrip).
- *Open (v2):* `grim context <id>` (show / compact / fork) — context-window introspection still TODO; lives in the provider integration layer, not the daemon. Vector / embedding search is also deferred.

### 7. Filesystem as the shared blackboard ✅ *implemented (v1)*

- Workspaces are first-class: `grim workspace create feature-auth` → daemon provisions a worktree, mounts it read-write for assigned agents, read-only for observers.
- Agents see each other's file changes in real time. File-watch events become bus events: "agent-2 wrote `src/auth.rs`, agent-5 is subscribed."
- Generalizes existing scroll conflict detection to the whole fabric.
- *Status (v1):*
  - Three new SQLite tables — `workspaces`, `workspace_memory`, `workspace_assignments`; `agents.workspace_id` column added via guarded `ALTER TABLE`. All cascade-delete on workspace destroy.
  - `WorkspaceRegistry` actor (`src/daemon/workspace_registry.rs`) mirrors the `WakeRegistry` shape: shells out via a `GitRunner` trait seam (`SystemGitRunner` → `git worktree add/remove`; tests inject `FakeGit`). State machine: `Active → Destroying → gone`. Boot reconciliation handles orphan dirs (log + preserve, never auto-delete) and orphan rows (mark `Destroying`, cascade-delete).
  - `WorkspaceWatcher` (`src/daemon/workspace_watcher.rs`) wraps `notify::RecommendedWatcher` per active workspace: 200 ms debounce, 64-path batches with `truncated_count` overflow, default ignore globs (`.git/**`, `target/**`, `node_modules/**`, `.DS_Store`, `*.swp`). Lazy-starts on first `assign`, stops on `destroy`. Emits `WorkspaceFileChanged` stream events plus topic mail to `topic://workspace/<id>/files`.
  - Eight new RPC methods: `workspace.create / list / destroy / assign`, `memory.put / get / list / delete`. Six new `StreamEvent` variants flow through `EventBus` and the durable events log (`WorkspaceCreated`, `WorkspaceDestroyed`, `WorkspaceOrphanDirDetected`, `MemoryWritten`, `MemoryDeleted`, `WorkspaceFileChanged`).
  - CLI surface: `grim workspace create|list|destroy|show`, `grim memory put|get|list|delete` (with `@<file>` JSON read, `--expected-version` for CAS, distinct exit codes 0/1/2/3/4 for scripting).
  - `grim summon --workspace <name>` short-circuits cwd to the worktree path (mutually exclusive with `--cwd`); workspace-assignment row written post-insert. Scroll markdown gets top-level `- workspace:`, `- workspace_repo:`, `- workspace_branch:` directives parsed into `ScrollSpec`.
  - Tests: `tests/workspaces_e2e.rs` (10 scenarios — create/destroy roundtrip, invalid-name rejection, duplicate rejection, memory put/get, CAS conflict surface, segment-aligned prefix list, in-use destroy refusal, idempotent assign, orphan-dir reconcile, orphan-row reconcile).
  - Spec: `.claude/specs/shared-memory-workspaces-spec.md`. Plan: `.claude/plans/shared-memory-workspaces.md`.
- *Open (v2):* RO observer ACLs (needs OS-level sandboxing from Part 1 §3), per-workspace watch-ignore overrides, automatic GC / TTL, `--copy-from <other-workspace>` for swarm-decompose forks, cross-host workspaces under `grimw`, `scroll_keeper` auto-creating workspaces from the parsed `- workspace:` directive (parser captures the fields; `inscribe()` consumption is the remaining wire).

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
- **v1 status (landed on `main` via `059ac1a feat: federated workers`):** direct `mail.send` across peers + opt-in topic federation are wired end-to-end. New `DaemonId` minted on first boot (`~/.grimoire/daemon.id`) and surfaced via `grim status`; address parser accepts `agent://grimd-<id>/<id>`. Tonic-based `proto/peer.proto` channel (Hello/HelloAck/Heartbeat/MailDeliver/MailAck) reuses the worker-channel substrate. Outbox + at-least-once delivery with `(sender_daemon_id, sender_seq)` dedupe. CLI: `grim peer add/list/remove/ping`, `grim topic federate/unfederate`. Full implementation in `.claude/specs/federation-spec.md` (T1–T13). 18 new federation tests; full suite at 553 passing. Out-of-scope (still): scroll-spanning, federated workspaces, federated supervision trees, mTLS (gated on worker-channel mTLS), cross-peer wake sources, transitive routing.

### 12. Introspection & eval as first-class

- `grim eval <agent-id> --rubric <file>` runs an evaluator agent against a completed agent's transcript. Results stored alongside the agent.
- Aggregate: `grim circle --eval-score <0.7` to find underperformers. Staff-level teams need this.

---

## Part 4 — Killer Demos

Pick 2–3 of these and the product sells itself:

- **Standing review team** — 3 dormant agents subscribed to `topic://pr-opened` and `topic://workspace/<ws>/files`. They wake on every PR (or every file change in a watched workspace), review in parallel, store findings in `memory.put("findings/<area>", …)`, post to `topic://pr-reviewed`, sleep. Running for 30 days. *Buildable end-to-end now — `grim mail subscribe`, scheduler wake-on-mail, workspace filewatch, and memory KV are all in place; only needs a webhook → `mail.send` bridge to drive it from real GitHub events.*
- **Swarm decompose** — One agent gets a vague spec, creates a workspace, spawns 5 children into the same worktree (or sibling worktrees), they coordinate via `memory.put/get` with CAS, the parent subscribes to `topic://workspace/<ws>/memory/findings/*` to be woken on each child's deposit, picks the winner, reunifies. Visible live in `grim scry`. *Buildable end-to-end now — workspace + memory KV + topic-prefix subscriptions land the missing coordination substrate; the only remaining wrapper is "agents drain `mail.list` at turn boundaries."*
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
7. ~~**Shared memory store + workspaces** — enables real multi-agent collaboration.~~ ✅ *v1 shipped — see Part 3 §6 + §7 / `.claude/specs/shared-memory-workspaces-spec.md`.*
8. ~~**Federation** — last, after single-fabric is solid.~~ ✅ *v1 landed on `main` (`059ac1a`) — see Part 3 §11 / `.claude/specs/federation-spec.md`. Direct mail + opt-in topic federation across two `grimd` peers; scroll-spanning and mTLS still post-v1.*

The six-month spine is complete. The through-line: **every feature above is impossible or awkward in a library-based orchestrator and natural in a daemon.** That's the story. Lead with it in the README; every feature should visibly reinforce it.

---

## Part 6 — Now / Next (post-spine)

With items 1–8 of Part 5 shipped, the next axis of work is **trust & operability** — the things that turn the fabric from "demo-grade" into something a team can actually point at shared infra. Two parallel tracks, ordered by leverage:

### Track A — Enterprise hardening (the gate to multi-user)

The unfinished items from Part 1's "Hardening milestone." None of these are research; they are the table-stakes that everything below assumes.

1. ~~**Auth + protocol versioning on UDS/HTTP/peer RPC**~~ ✅ *Shipped.* The CLI/HTTP trust domain now uses `SO_PEERCRED` for owning-UID UDS connections and a bearer token (auto-generated at `~/.grimoire/auth.token`, mode `0600`; overridable via `GRIMOIRE_AUTH_TOKEN` or `[daemon.auth] token = …`) for everything else. HTTP `/api/*` requires `Authorization: Bearer` or a `grim_auth` cookie set by `/auth/login`; `grim scry` mints a one-shot login URL. Workers and peers carry their own per-link bearer tokens (unchanged). Protocol-version enforcement now lives in all four handshakes (UDS RPC, peer `Hello`, worker `Register`) — wire-incompatible bumps reject with `unsupported_protocol_version`. *Unblocks the v2 webhook wake source (Part 3 §5) and "system" stream for human-originated mail (Part 3 §4); follow-ons: token rotation, multi-token roles, TLS for HTTP and peer/worker channels (Part 6 v2).*
2. **Observability baseline** — Prometheus `/metrics` (queue depth, dispatch latency, restart counts, peer outbox lag, mail backlog) and OTel span export tied to scroll/agent IDs. Cheap given `tracing` is already wired.
3. **Sandboxing** — cwd jail + `rlimit`/cgroups per agent process, plus a per-agent env allowlist. Today a summoned agent inherits the daemon's full env and FS.

### Track B — Daemon-native features still on the table

The Part 3 items that don't yet have a v1:

4. **§10 — Policy & budget as daemon primitives.** `grim budget create … --daily $50`, allow/deny rules per provider/cwd/token, enforced at admission. Largest "enterprise sell" lever and a natural extension of the scheduler's existing admission hook.
5. **§9 — Time-travel & replay.** The durable event log (Part 2 §3) already records everything; what's missing is a replay cursor API and `grim replay <agent-id> --until <event>` / `grim fork <agent-id> @event`. Mostly a read-side feature on top of existing data.
6. **§12 — Introspection & eval.** `grim eval <agent-id> --rubric <file>` runs an evaluator agent against a transcript and stores the score alongside the agent. Pairs well with §9 (replay → eval → fork-and-retry).

### Recommended next pickup

With **Track A §1 done**, two candidates are roughly tied for next:

- **Track A §2 (observability baseline).** Cheap given `tracing` is already wired, and once metrics + OTel are flowing every other piece of work below gets easier to validate. Most "is this thing actually doing what I think" questions during the next features (budget, replay, sandboxing) want metrics first.
- **Track B §4 (policy & budget).** The scheduler already owns admission, so this is mostly schema + a check function. Largest "enterprise sell" lever and the natural next step now that the trust layer no longer leaks.

Pick observability first if the next thing you want is *confidence* in what's running; pick policy/budget first if the next thing you want is a *new visible feature*.

### v2 backlog (deferred, not forgotten)

Per-feature v2 work already enumerated in Part 3: webhook wake source (§5), dashboard wake-source surfacing (§5), `grim context <id>` show/compact/fork (§6), vector memory (§6), RO observer ACLs (§7), per-workspace watch-ignore overrides (§7), supervision-tree dashboard view + topic/webhook escalation (§8), federation scroll-spanning + mTLS + cross-peer wake sources (§11). Pick these up opportunistically when the parent feature gets touched.
