# Grimoire

[![CI](https://github.com/XaoticLabs/grimoire/actions/workflows/ci.yml/badge.svg)](https://github.com/XaoticLabs/grimoire/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.95-blue.svg)](#msrv)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Grimoire runs AI coding agents as long-lived, supervised daemons. An agent you summon today still has an address next week, it sleeps when idle, wakes on a schedule or a file change or a message, restarts itself when it falls over, and pings you when something actually needs a human.

The experiment here is **agents are processes, not function calls.** Grimoire treats an agents like a service. It has an identity, a mailbox, and a supervisor.

What we have here is a `grimd` running on a server anyone in an organization can call. Standing agents on standby, messaging each other, woken by events, supervised and observable. It started as a way to stop hoping six git worktrees would come back in sync, and the bigger idea fell out of that first quickly vibe coded thought into unix sysadmin habits, applied to agents.

You bring your own CLI. Claude Code is the default, but `pi`, opencode, aider, or anything that takes a prompt works just as well.

## How it works: two primitives

Almost everything here is built from two things.

The first is a **durable event log.** Every state change, output line, and lifecycle event is written through to SQLite with per-stream sequence numbers. Nothing is lost if a subscriber lags or the daemon restarts. On reboot the log is the source of truth, in-flight agents are reconciled, and standing agents rehydrate.

The second is an **addressable mailbox.** Every agent has an address (`agent://<id>`) and a mailbox; topics (`topic://<name>`) fan out to whoever subscribed.

Everything else is those two composed. Wake triggers deliver to a mailbox. Supervision restarts and escalates through the log. Workspaces publish file changes to a topic. Notifications are log subscribers. Federation is mail between daemons.

## Install

```bash
cargo install --path .
```

## Quick start

```bash
grim daemon                                  # start the daemon
grim summon "analyze this codebase for bugs" # summon an agent
grim bind <id>                               # watch it work
grim circle                                  # list agents (ps)
grim scry                                    # open the web dashboard
```

Any command that takes an agent ID also takes a short prefix, so `grim bind 4a` works.

## The shape of it

A short tour of what the daemon does.

- **Standing agents.** `grim summon --keep-alive` lands an agent in `Dormant` after it finishes. It wakes on a cron schedule, a file change, another agent finishing, or incoming mail, then goes back to sleep.
- **Messaging.** Agents send each other mail and subscribe to topics. A dormant agent wakes on incoming mail, a live one reads it at its next turn boundary.
- **Supervision.** Restart policies borrowed from OTP (`always` / `on_failure` / `never`), with per-agent rate limits. When a child fails too often the parent agent wakes with the failure and decides what to do.
- **Scrolls.** A markdown spec of tasks with dependencies and file ownership. Independent tasks run in parallel, the DAG is respected, and tasks that touch the same files get serialized.
- **Workspaces & shared memory.** A managed git worktree several agents share, plus a per-workspace KV store with optimistic CAS and prefix subscriptions, so a write to `findings/auth` wakes whoever's watching.
- **Federated memory.** A namespace KV (`grim ns`) decoupled from worktrees that replicates across daemons. Writes converge last-write-wins on a Lamport tuple; deletes propagate as tombstones. This is the org-wide shared-context piece.
- **Federation.** Two `grimd` instances peer over mutually-authenticated gRPC. Direct mail, opt-in topics, and federated namespaces forward across daemons, deduped at the inbox by `(sender_daemon_id, seq)`.
- **Worker pool.** Dispatch agents to remote machines through the `grimw` worker binary over mutual TLS, with capability-aware placement. With no workers registered it runs everything locally.
- **Notifications.** The daemon reaches you, the missing half of "fire it and walk away." Any webhook (Slack, Discord, a relay) gets a JSON POST on completion, failure, wake, or when an agent decides it's worth a human.
- **Inbound webhooks.** The mirror image: configure `[webhooks.<name>]` and `POST /webhooks/<name>` becomes mail on a topic, so a standing agent subscribed to it wakes on real-world events (a GitHub PR, a Linear issue, a CI run).
- **Time-travel.** `grim chronicle <id>` (alias `grim replay`) reconstructs an agent's full life from the durable log — stdout interleaved with state changes, wake fires, restarts, mail, notifications — with state-at-point reconstruction at any seq. `grim fork <id> --at <seq>` branches a new agent seeded with the parent's transcript up to the cut. `grim eval <id> --rubric <file>` scores a transcript by spawning an evaluator agent against a rubric.
- **Metrics.** `GET /metrics` exposes a Prometheus snapshot (queue depth, agents by state, per-kind event counters, notifications by level) behind the same bearer auth as `/api/*`.

The command vocabulary is deliberately thematic. If you'd rather think in plain terms:

| `summon` | `circle` | `bind` | `banish` | `invoke` | `chronicle` | `fork` | `eval` | `scry` | `pact` | `scroll` | `wake` |
|----------|----------|--------|----------|----------|-------------|--------|--------|--------|--------|----------|--------|
| run | ps | tail | kill | follow-up | replay history | branch at a point | score with a rubric | dashboard | chain | run a DAG | resume a sleeper |

## Architecture

A single Rust binary (`grim`) runs as both CLI and daemon. A second binary (`grimw`) is the remote worker.

```
CLI (grim) ──UDS──▶ Daemon (grimd) ──┬─▶ Scheduler ──▶ LocalExecutor ──▶ child processes
Browser   ──HTTP──▶    │              └─▶ Scheduler ──▶ RemoteExecutor ──gRPC──▶ grimw (worker)
Peer grimd ──gRPC──▶   │
                       ├── SQLite (agents, events, mail, scrolls, wakes, workspaces, peers, ...)
                       ├── EventBus (durable, per-stream seq)        ← the log
                       ├── Orchestrator + Supervisor                ← pacts, restarts, escalation
                       ├── ScrollKeeper / WakeRegistry / Workspaces ← DAGs, triggers, worktrees
                       └── PeerClient/Server + Outbox/Inbox          ← federation
```

The daemon owns agent lifecycles and persists everything in SQLite, over three transports: a JSON-RPC Unix socket for the CLI, an HTTP/SSE dashboard, and gRPC for peers and workers. Each transport is its own trust domain with its own auth, on by default — the gRPC transports are mutual TLS with pinned self-signed certs alongside the per-link bearer token. The scheduler handles admission control and capability-aware placement, and a provider registry hides each CLI tool behind one trait.

## Status & roadmap

Here's where things actually stand.

**Works today:** summon, standing agents, wake triggers (cron / file / completion / mail / inbound webhook), pacts, scrolls, mail and topics, supervision trees, workspaces and shared memory, federated namespace memory across daemons, the `grimw` worker pool, outbound notifications, a live SSE dashboard, time-travel over the durable log (`grim chronicle` / `grim replay` / `grim fork`), and a Prometheus `/metrics` endpoint. The trust layer ships on by default across all three transports — mutual TLS on the gRPC peer and worker links — with negotiated protocol versioning.

**Partial:** Federated namespaces replicate writes made after `ns federate`, but there's no initial-state snapshot for a peer joining a populated namespace yet, and concurrent writes to one key resolve last-write-wins rather than surfacing the conflict. Federated *workspaces* are mid-build: the schema and operator surface (`grim workspace federate` / `federate-subscribe` / `unfederate`, plus the `Local | Shadow` workspace kind) shipped in F3a, but the event flow that actually wakes a remote agent on a file change in another daemon's workspace is F3b/F3c — the next concrete pickup. Metrics ships Prometheus only; OTel tracing export is not wired.

**Not built yet:**
- Sandboxing. A cwd jail, cgroups, per-agent resource limits.
- Policy and budget primitives. Per-provider and per-cwd allow rules, token and cost ceilings.

## MSRV

Minimum supported Rust version is **1.95**, pinned in `rust-toolchain.toml` and checked in CI. Bumping it is a minor-version release.

## License

Dual-licensed under either of [Apache-2.0](./LICENSE-APACHE) or [MIT](./LICENSE-MIT) at your option. Contributions are accepted under the same terms (per Apache-2.0 §5, unless you state otherwise).
