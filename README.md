# Grimoire

> **`cron` + `systemd` for AI coding agents.** Bring your own CLI.

[![CI](https://github.com/XaoticLabs/grimoire/actions/workflows/ci.yml/badge.svg)](https://github.com/XaoticLabs/grimoire/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.95-blue.svg)](#msrv)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Grimoire runs AI coding agents as **long-lived, supervised daemons**. An agent you summon today still has an address next week. It sleeps when idle, wakes on a schedule or a file change or a message, restarts itself when it falls over, and pings you when something actually needs a human.

The experiment: **agents are processes, not function calls.** Treat them like services. Identity, mailbox, supervisor.

You bring the CLI. Claude Code is the default; `pi`, opencode, aider, codex, or anything that takes a prompt works.

## Why this exists

- Has a first-class **`Dormant` state**. Agents survive daemon restarts and resume their CLI session when something wakes them.
- Is built on a **durable event log** (write-through SQLite, per-stream sequence numbers). `grim chronicle <id>` reconstructs any agent's full life; `grim fork --at <seq>` branches from any point.
- **Federates** across machines: cross-daemon mail, opt-in topics, and an LWW-replicated namespace KV, all over mTLS. Your laptop and the build box can be one fabric.
- Has a **worker pool** (`grimw`) so the same control plane can dispatch to many machines (capability-aware placement, mTLS).
- Treats **provider neutrality** as load-bearing infrastructure, not a plugin. Mix claude/pi/aider/opencode under one supervisor.

## Install

```bash
cargo install --path .
```

## 30-second tour

```bash
grim daemon                                      # start the daemon
grim summon "find bugs in this codebase"         # summon an agent
grim bind <id>                                   # watch it work
grim scry                                        # open the web dashboard

# The thing one-shot CLIs can't do:
grim demo standing-review --repo . --provider claude
# A reviewer is now Dormant in this repo. Edit a file, it wakes,
# diffs, decides, pings you, sleeps. Survives `grim daemon` restarts.
```

Any command that takes an agent ID also takes a short prefix, so `grim bind 4a` works.

## The two primitives

Almost everything here is built from two things.

1. **A durable event log.** Every state change, output line, and lifecycle event is written through to SQLite with per-stream sequence numbers. Nothing is lost if a subscriber lags or the daemon restarts. On reboot the log is the source of truth: in-flight agents are reconciled and standing agents rehydrate.
2. **An addressable mailbox.** Every agent has an address (`agent://<id>`) and a mailbox; topics (`topic://<name>`) fan out to subscribers.

Everything else composes those two. Wake triggers deliver to a mailbox. Supervision restarts and escalates through the log. Workspaces publish file changes to a topic. Notifications are log subscribers. Federation is mail between daemons.

## What's in the box

- **Standing agents.** `grim summon --keep-alive` lands an agent in `Dormant` after it finishes. Wakes on a cron schedule, a file change, another agent finishing, incoming mail, or an inbound webhook.
- **Messaging.** Agents send each other mail and subscribe to topics. Dormant agents wake on mail; live ones read at the next turn boundary.
- **Supervision.** OTP-style restart policies (`always` / `on_failure` / `never`) with per-agent rate limits. When a child fails too often, the parent agent wakes with the failure and decides what to do.
- **Scrolls.** A markdown spec of tasks with dependencies and file ownership. Independent tasks run in parallel; same-file tasks serialize.
- **Workspaces & shared memory.** A managed git worktree shared by several agents, plus a per-workspace KV store with optimistic CAS and prefix subscriptions. A write to `findings/auth` wakes whoever's watching.
- **Federated memory.** A namespace KV (`grim ns`) decoupled from worktrees that replicates across daemons. Writes converge LWW on a Lamport tuple; deletes propagate as tombstones.
- **Federation.** Two `grimd` instances peer over mutually-authenticated gRPC. Mail, opt-in topics, and federated namespaces forward across daemons, deduped at the inbox.
- **Worker pool.** Dispatch to remote machines via the `grimw` worker binary over mTLS, with capability-aware placement.
- **Notifications.** Outbound webhook (Slack/Discord/relay), local JSON-lines log, or `notify-send` desktop toast. Any combination, fan-out independent.
- **Inbound webhooks.** Configure `[webhooks.<name>]` and `POST /webhooks/<name>` becomes mail on a topic, so a standing agent subscribed to it wakes on real-world events.
- **Time-travel.** `grim chronicle <id>` (alias `grim replay`) reconstructs an agent's full life (stdout interleaved with state changes, wake fires, restarts, mail, and notifications) with state-at-point reconstruction at any seq. `grim fork <id> --at <seq>` branches a new agent seeded with the parent's transcript. `grim eval <id> --rubric <file>` scores a transcript.
- **Sandboxing.** Per-provider confinement: a `bwrap` filesystem jail (network off, explicit ro/rw paths), `systemd-run` cgroup limits (memory, CPU quota), and per-agent token budgets. Degrades gracefully — if the host tooling is absent it warns once and runs unconfined.
- **Policy & budgets.** Provider and cwd allow/deny rules enforced at summon; `[budgets.<name>]` daily USD ceilings per provider group, gated at dispatch with spend attributed from token counts.
- **Metrics & tracing.** `GET /metrics` exposes Prometheus text exposition behind the same bearer auth as `/api/*`. OTel span export ships behind `--features otel` (`OTEL_EXPORTER_OTLP_ENDPOINT` gates it at runtime).

## The mystic vocabulary, translated

The CLI is thematic. If you'd rather think in plain terms:

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

The daemon owns agent lifecycles and persists everything in SQLite over three transports: a JSON-RPC Unix socket for the CLI, an HTTP/SSE dashboard, and gRPC for peers and workers. Each transport is its own trust domain with its own auth, on by default. The gRPC transports use mutual TLS with pinned self-signed certs alongside the per-link bearer token. The scheduler does admission control and capability-aware placement; a provider registry hides each CLI tool behind one trait.

## Status

**Works today:** everything in [What's in the box](#whats-in-the-box). Trust layer ships on by default across all three transports with negotiated protocol versioning. Federation now spans the full surface: mail, topics, namespaces, workspace file events (a file change on daemon A wakes a subscribed agent on daemon B), cross-peer wake sources, and scroll tasks dispatched to peers (`grim peer set --accept-scroll-dispatch`).

**Partial:** Federated namespaces replicate writes made after `ns federate`, but there's no initial-state snapshot for a peer joining a populated namespace yet, and concurrent writes to one key resolve LWW rather than surfacing the conflict.

**Not built yet:**
- An MCP surface — exposing the daemon's queue to MCP clients (the Tasks primitive).
- Tree-level budget supervision: budgets gate dispatch per agent today; pausing or escalating a whole supervision tree at a spend ceiling is next.

## Recipes

- [Standing review agent](docs/recipes/standing-review-agent.md). The canonical "wake on file change, ping me if interesting, sleep" loop. Verified live against Claude and pi.

## MSRV

Minimum supported Rust version is **1.95**, pinned in `rust-toolchain.toml` and checked in CI. Bumping it is a minor-version release.

## License

Dual-licensed under either of [Apache-2.0](./LICENSE-APACHE) or [MIT](./LICENSE-MIT) at your option. Contributions are accepted under the same terms (per Apache-2.0 §5, unless you state otherwise).
