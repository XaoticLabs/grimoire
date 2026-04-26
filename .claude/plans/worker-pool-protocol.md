# Plan: Worker Pool Protocol (grimd ↔ grimw)

> Generated from planning session on 2026-04-24
> Source: conversation design discussion (no ticket)

## Problem Statement

Grimoire today spawns every agent process on the same machine that runs `grimd`. The daemon's `AgentManager::summon` calls `Provider::spawn(...)` and gets back a local `tokio::process::Child`, which `process_manager::monitor_agent` reads line-by-line for stdout/stderr. The "where" is hardcoded: wherever grimd runs.

This caps parallelism at one machine's cores, ties agent execution to whichever machine happens to have the relevant CLI installed, and makes the "distributed AI agent pool" pitch impossible to demo.

### Who experiences this?

The single user (you), running grimoire across multiple personal machines — a laptop, a desktop, and optionally a home server. All machines are trusted. All machines mount a consistent set of paths (synced `~/repos`, dotfiles, etc.). The user wants to route agent tasks across these machines without thinking about it.

### Why now?

- Scrolls (orchestrated multi-task specs) are landing, and parallelism on one box will become the visible bottleneck.
- "Distributed agent pool" is the feature no other agent orchestrator offers; the differentiator erodes the longer we wait.
- The spawn seam is small and simple today — abstracting it later, after more provider behaviors and scroll semantics have been piled on top, will be harder.

### Current workarounds

- Run grimd on the beefiest machine and SSH into it from other laptops. Works, but the other machines' CPUs sit idle and their repo checkouts are invisible to the daemon.
- Run one grimd per machine and manually pick which one to `grim summon` against. No shared state, no cross-machine scroll.

## Goals

- Introduce a worker-pool protocol and a `grimw` binary such that agent processes can run on any registered worker, with the daemon as the control plane.
- Keep the CLI and event-stream surface unchanged — `grim summon`, `grim bind`, `grim circle`, `grim scroll` behave identically whether the agent runs locally or on a remote worker.
- Ship a routing policy that picks a worker by provider availability and least in-flight load.
- Preserve existing single-machine behavior as the default (zero workers registered → falls back to local execution).
- Land the abstraction seam cleanly inside `AgentManager` so future placement strategies (cwd-root match, tags, GPU) are additive, not rewrites.

## Non-Goals (Explicit Scope Boundaries)

- **Multi-tenant / team pooling.** No per-user identity, no per-worker cwd allowlist, no task-ack UX, no mTLS. Future work.
- **cwd-root matching.** Workers do not advertise which repo roots they see; the MVP assumes all workers can resolve the same paths (shared/synced filesystem). Future work.
- **GPU / resource tags.** No `gpu`, `ram`, `cpu-class` capabilities or matching. Capabilities are: provider name + semver only.
- **Auto-retry / auto-reschedule.** A lost worker fails its in-flight agents. The user decides whether to re-summon.
- **Cross-worker scroll task dependencies with shared mutable state.** Task deps work across workers (scheduler places each task independently), but we do not attempt to coordinate shared files or locks beyond what the existing conflict-detection in `scroll_keeper` already does.
- **Autoscaling / ephemeral cloud VMs / k8s pods.** The protocol is designed to not preclude these, but the provisioner layer is not in this plan.
- **mDNS / auto-discovery.** Workers are registered by a config file pointing at the daemon URL.
- **QUIC.** gRPC over TLS is the transport.

## Proposed Solution

### Conceptual Overview

`grimd` becomes a control plane that knows about a set of registered workers. `grimw` is a thin binary that runs on each machine, registers itself with `grimd`, heartbeats, and executes agent tasks assigned to it by the daemon. When a user runs `grim summon ...`, the daemon picks a worker (via a pluggable `Placement` strategy), sends it an `AssignTask` message, and the worker spawns the provider process locally and streams stdout/stderr/state events back to the daemon. The daemon re-publishes those events on the existing `EventBus`, so CLI clients (`grim bind`, dashboard) see them with no changes.

A built-in `LocalPlacement` stays as the default when no workers are registered, preserving today's single-machine behavior.

### User Journey

1. User starts `grimd` on machine A (the control-plane host).
2. User starts `grimw --daemon grim://machine-a:PORT --secret $GRIM_WORKER_SECRET` on machines A, B, and C (A can run both; the daemon's built-in local executor and a co-resident grimw are allowed).
3. User runs `grim summon "refactor the auth module"` on any machine.
4. Daemon picks the least-loaded worker with the required provider installed.
5. Worker spawns the agent, streams events. `grim bind <id>` shows them live regardless of which machine the agent is on.
6. User runs `grim circle` and sees agents across all workers in one list, annotated with which worker each is on.

## Architecture

### Data Model

- **Worker** (in-memory in daemon; not persisted): `{ worker_id, address, providers: [(name, semver)], tags: [string], max_concurrent, in_flight, last_heartbeat, registered_at, assign_tx }`.
- **Agent** (existing, persisted): gains a nullable `worker_id` column. Null means "ran on the daemon's local executor."
- **AgentEvent** (existing): unchanged. Events originating on a worker are forwarded by the daemon to the EventBus with the same shape they have today.
- **Capabilities advertisement**: dumb strings + semver — `providers: ["claude@>=1.2", "codex@1"]`, `tags: ["beefy"]`. No typed capability enum.

### System Boundaries

- **grimd**: adds a worker-facing RPC service alongside the existing CLI JSON-RPC. Owns the `WorkerRegistry`, the `Placement` policy, and an `Executor` abstraction that wraps either local spawn or remote assignment.
- **grimw**: a new binary crate. Contains a trimmed subset of today's `provider_registry` + `process_manager` (spawn locally, monitor stdout/stderr/exit), plus an RPC client that dials the daemon and handles the bidi task-assignment stream.
- **CLI and dashboard**: unchanged.

### API Surface

Two new internal surfaces:

1. **Worker RPC** (daemon ↔ grimw, new): gRPC over TLS (tonic). Worker opens a long-lived bidi stream. Messages:
   - Worker → Daemon: `Register(worker_id, caps)`, `Heartbeat(in_flight)`, `TaskAccepted(agent_id)`, `TaskEvent(agent_id, event_type, payload)`, `TaskFinished(agent_id, state, exit_code, session_id)`.
   - Daemon → Worker: `AssignTask(agent_id, task, provider, cwd, model, env)`, `CancelTask(agent_id)`, `Ping`.
2. **Executor trait** (daemon-internal, new): abstracts "start an agent and give me a handle to its event stream + a way to cancel it." Two impls:
   - `LocalExecutor` — wraps the existing `provider.spawn(...)` + `monitor_agent(...)` path.
   - `RemoteExecutor` — issues `AssignTask` on the worker's bidi stream, funnels incoming `TaskEvent`s back into the same `EventBus` the local path uses.

No changes to the existing CLI JSON-RPC surface or to `StreamEvent`.

### Integration Points

- `AgentManager::summon` stops calling `provider.spawn` directly and instead goes through the active `Executor` (chosen by `Placement`).
- `process_manager::monitor_agent` is factored so its stdout/stderr/exit-producing core can accept either a `tokio::process::Child` (local) or an async stream of worker `TaskEvent` messages (remote). Same DB inserts, same `EventBus::publish` calls downstream.
- `scroll_keeper` is unaffected structurally — it already schedules agents one at a time through `AgentManager`. Cross-worker scheduling falls out for free because each task's placement decision is independent.
- Existing persistence gains one nullable column (`agent.worker_id`) used for UI annotation only; failure semantics don't depend on it.

## Implementation Approach

### Recommended Pattern

Mirror the existing `Provider` trait pattern. `Provider` abstracts *which CLI to spawn*; introduce a parallel `Executor` trait abstracting *where and how to spawn it*. The two axes are orthogonal and both already have registries (`ProviderRegistry` for providers; the new worker pool is effectively an "executor registry"). This keeps the mental model consistent with what's in `src/daemon/provider.rs` + `src/daemon/provider_registry.rs` today.

gRPC via tonic is the right transport choice because the daemon already has `prost`/serde patterns in the codebase, tonic is mature on Rust/tokio, and bidi streaming maps cleanly to "one long-lived control channel per worker." mTLS is deliberately skipped for MVP — a shared bearer token in `grimw.toml` is enough for single-tenant.

### Key Technical Decisions

| Decision | Choice | Rationale | Trade-offs |
|----------|--------|-----------|------------|
| Transport | gRPC over TLS (tonic), bidi stream | Bidi maps cleanly to push-style task assignment; tonic is mature; LAN/Tailscale-friendly | Breaks behind strict NAT without a VPN. Acceptable for personal multi-machine setup; revisit if team/cloud case becomes real. |
| Auth | Shared bearer token in `grimw.toml` | Zero PKI, zero rotation, correct granularity for single trust zone | Rework required if/when team pooling arrives. Captured as future work. |
| Placement seam | New `Executor` trait at `AgentManager` level | Orthogonal to `Provider`, matches existing pattern, keeps local path unchanged | One more abstraction layer to understand. |
| Scheduling policy | Provider-match + least in-flight | Solves the real MVP cases, trivial to implement and reason about | Doesn't handle repo-locality or GPU; opt-in tags can extend later. |
| Filesystem model | Assume all workers resolve the same paths | Zero scheduler complexity, matches personal-machine reality with synced repos | User is responsible for keeping paths in sync. Worker will reject if cwd doesn't exist. |
| Worker failure | Mark Failed, no auto-retry | Agents aren't idempotent (they commit, write files) — silent re-runs are worse than visible failure | User has to manually resummon; add later if it becomes a pain. |
| Capabilities shape | String + semver (e.g. `claude@>=1.2`) | Evolves freely as providers change without schema migrations | Matchers must be careful (semver parse failures); no compile-time safety. |
| Worker co-residency | Daemon can run an embedded grimw | Single-machine users get worker-pool semantics without a second process | Slight complication in the "no workers registered" fallback logic. |

### Rough Task Breakdown

1. **Define the `Executor` trait and refactor the local path to use it.** `LocalExecutor` wraps existing `provider.spawn` + `monitor_agent`. Zero behavior change visible to CLI. Commit boundary.
2. **Worker RPC protocol (proto definitions + tonic scaffolding).** `worker.proto` in `src/shared/`, generated types alongside existing `protocol.rs`. Wire types only, no behavior.
3. **`grimw` binary crate.** Trimmed provider registry + process manager; RPC client; bootstrap from `grimw.toml`. Runs tasks and streams events back; no scheduling logic lives here.
4. **`WorkerRegistry` + daemon-side worker RPC server.** Accepts registrations, tracks heartbeats, owns assign channels, emits `worker_lost` on eviction.
5. **`Placement` policy + `RemoteExecutor`.** `LeastLoadedPlacement` picks a worker by capability match + in-flight count. `RemoteExecutor` routes `AssignTask`s and translates incoming `TaskEvent`s into the existing `EventBus` shape.
6. **CLI + dashboard surfacing.** `grim circle` annotates each agent with its worker; `grim status` reports worker count and health. Small, high-value visibility layer.
7. **Config + docs.** `grimd` config gets `worker_listen_addr` + `worker_secret`; `grimw.toml` gets `daemon_url` + `secret` + `max_concurrent`. README section.

### Riskiest Part

Factoring `monitor_agent` to accept either a local `Child` or a remote event stream without duplicating the DB-write + event-publish logic. The current function is tightly coupled to `tokio::process::Child` (it takes stdout/stderr handles and a `wait()`-able child, and uses a single shared `session_id` extractor). Splitting the "produce a sequence of (stream, line) tuples" half from the "persist + publish" half is mechanical, but it's the change most likely to leak subtle behavior differences (EOF handling, partial lines, session_id capture order) if done carelessly. This is where I'd want the tightest test coverage before and after the refactor.

## Edge Cases & Decisions

| Edge Case | Decision | Rationale |
|-----------|----------|-----------|
| No workers registered | Fall back to `LocalExecutor` unconditionally | Zero-config default preserves today's behavior |
| Task's cwd doesn't exist on the picked worker | Worker rejects on receipt with `cwd_unreachable`; daemon marks agent Failed and emits an event | Fail visibly; don't thrash by reassigning to other workers |
| Worker heartbeat timeout mid-task | Evict worker, transition its in-flight agents to Failed with `worker_lost` reason | Same semantics as a local process crash |
| Worker reconnects after eviction | Register as a new worker; old in-flight agents stay Failed | No resurrection logic; keeps state machine simple |
| Provider version mismatch (task needs `claude@>=1.2`, worker has `1.1`) | Scheduler skips the worker; if no workers match, mark Failed with `no_capable_worker` | Don't run with an incompatible provider |
| Two workers tied on load | Pick deterministically by worker_id sort | Predictable during debugging |
| Agent sent `banish` while running on a remote worker | Daemon sends `CancelTask`; worker sends SIGTERM locally | Mirrors today's `kill_process` path |
| `grim invoke` (resume) on an agent that ran on a worker now offline | Mark the invoke as failed with `worker_lost` | Session resume requires the worker's local state; can't migrate |
| Scroll with 20 tasks and 3 workers | Each task placed independently at activation time; respects existing conflict rules | Fan-out falls out naturally from per-task placement |
| Worker clock skew | Use daemon-side monotonic timestamps for heartbeat expiry | Avoid relying on worker-reported times |

## Security Considerations

- **Trust model**: single-tenant, single trust zone. All workers and the daemon are owned by the same user. No adversarial insider considered.
- **Transport**: TLS required on the worker RPC. Self-signed cert acceptable; worker pins the daemon by hostname + cert fingerprint in `grimw.toml`.
- **AuthN**: shared bearer token. Worker presents it on `Register`; daemon rejects if missing/wrong. Rotation = edit config + restart.
- **AuthZ**: none beyond "registered = fully trusted." Explicitly not trying to bound what a worker can be asked to do.
- **Data sensitivity**: agent tasks contain natural-language prompts and can touch any file in the advertised cwd. Since we assume a shared filesystem, no data leaves the user's own machines.
- **Network exposure**: `grimd`'s worker-RPC port should default to binding localhost or Tailscale interface — not 0.0.0.0. Documented in config.
- **Deferred for team case**: per-worker cwd allowlists, per-task ack, mTLS with a CA, identity beyond a shared token, audit log of assignments. None in this MVP.

## Failure Modes & Risks

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Refactor of `monitor_agent` leaks behavior differences (EOF, partial lines, session_id) | Medium | Medium | Snapshot tests of event sequences before the refactor; run both paths through identical fixtures after |
| Filesystem-sync assumption breaks silently (user edits on laptop, worker on desktop sees stale files) | Medium | Medium | Not our problem to solve, but document clearly. Worker rejects if `cwd` doesn't exist as an early guard. |
| Worker behind NAT / restrictive wifi can't open bidi stream to daemon | Low (user's own LAN/Tailscale) | High if it happens | Document Tailscale as the recommended transport. Revisit pull-model transport if it becomes recurrent pain. |
| Heartbeat tuning wrong — false evictions or slow detection | Medium | Medium | Start with 5s heartbeat / 30s timeout; expose as config; revise after real use |
| Worker binary + daemon binary drift (proto schema mismatch) | Medium | Medium | Embed version in `Register`; daemon refuses workers below a min version with a clear error |
| gRPC/tonic dep adds substantial build time | Medium | Low | Accepted cost; alternative (handrolled framed JSON over TCP) would save compile time and cost correctness |
| "No workers registered, fall back to local" path silently kicks in when user *meant* to use a worker | Low | Medium | `grim status` shows worker count prominently; log at info on each summon which executor was picked |

## Open Questions

- [ ] Should the daemon always run an embedded grimw on its own host, or should local spawn remain a separate "zero workers" fallback? (Leaning: embedded grimw, removes the special-case path — but adds complexity to MVP.)
- [ ] Is `session_id` extraction (today driven by provider-specific stdout parsing) a worker-side concern or daemon-side? Workers already have the provider impl available; extracting locally and sending in `TaskFinished` is cleaner than streaming raw bytes for the daemon to re-parse.
- [ ] Do we need a `DrainWorker` RPC for graceful shutdown, or is "stop accepting new assignments when the worker process receives SIGTERM, let existing tasks finish" sufficient?
- [ ] Heartbeat interval and timeout — what's the default? (Proposed: 5s / 30s, but no measurement yet.)

## Alternatives Considered

### LocalPlacement-only refactor (protocol-only shell)

**Description:** Define the `Executor` trait and the `.proto` but don't build `grimw`, the `RemoteExecutor`, or the scheduler. Ship nothing runtime-visible.
**Rejected because:** The user picked "team-wide share idle compute" as the core pain, even while acknowledging it's differentiator-driven. A protocol without an implementation demos as nothing. Also — the risky refactor (`monitor_agent` split) is the same work either way, so shipping the trait without exercising it in a second implementation skips the only thing that proves the seam is correct.

### Pull-model transport (worker long-polls `LeaseTask`)

**Description:** Workers open outbound HTTPS to the daemon, long-poll for assignments. No bidi streaming. Event upload via a separate server-streaming RPC.
**Rejected because:** Single-tenant LAN/Tailscale machines don't suffer from NAT or corporate-proxy pain, so the main advantage of pull doesn't apply. Bidi is simpler to operate when it works. Revisit if/when team-pool or cloud-worker cases make NAT traversal a real problem.

### Multi-tenant from day one (mTLS + per-worker cwd allowlist + ack mode)

**Description:** Build the team-pool trust model up front: mTLS with a CA, per-worker cwd allowlists, optional human ack before task acceptance, per-user identity.
**Rejected because:** First real user is the author, across their own machines. Building multi-tenant features for zero multi-tenant users is the scope creep the planning session explicitly pushed back on. Protocol is designed to not preclude these additions.

### Do nothing — optimize single-machine instead

**Description:** Invest in better on-box concurrency, resource pinning, smarter scroll scheduling on one beefy machine.
**Rejected because:** Even with a beefy machine, two goals remain out of reach: running an agent on the machine that physically holds the repo/caches (if the user ever drops the shared-FS assumption), and the differentiator story that no competitor offers distributed execution. Single-machine optimization is additive, not a substitute.

### mDNS / LAN auto-discovery for workers

**Description:** Workers broadcast on the LAN; the daemon auto-discovers them.
**Rejected because:** Fragile across subnets, VPNs, and sleeping laptops. Every similar system has eventually ripped mDNS out. A config file with a URL is boring and correct.

---

*This plan captures the "what", "why", and high-level "how". It is input for `/hatch:write-spec`, which produces the detailed implementation specification.*
