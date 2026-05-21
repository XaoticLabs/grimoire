# Grimoire

[![CI](https://github.com/XaoticLabs/grimoire/actions/workflows/ci.yml/badge.svg)](https://github.com/XaoticLabs/grimoire/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/MSRV-1.95-blue.svg)](#msrv)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

It's cron and systemd for AI agents, and you bring your own CLI.

Grimoire runs your coding agents as long-lived, supervised processes instead of one-shot commands you babysit in a tmux pane. They keep working after you close your laptop, wake on schedules and file changes, restart themselves when they fall over, and ping you when something actually needs a human.

You don't write your agents in Grimoire. You run the agents you already use under it. Claude Code is the default, but `pi`, opencode, aider, codex, or anything that takes a prompt works just as well (see [Providers](#providers)).

> The idea: agents are processes, not function calls. A library orchestrator's agents die when its script ends. A Grimoire agent still has an address next week. That's the whole reason to run a daemon.

## How it works: two primitives

Almost everything here is built from two things.

The first is a durable event log. Every state change, output line, and lifecycle event gets written through to SQLite with per-stream sequence numbers, so nothing is lost if a subscriber lags or the daemon restarts.

The second is an addressable mailbox. Every agent has an address (`agent://<id>`) and a mailbox, and topics (`topic://<name>`) fan out to whoever subscribed.

Everything else is those two composed. Wake triggers deliver to a mailbox. Supervision restarts and escalates through the log. Workspaces publish file changes to a topic. Notifications are just log subscribers. Federation is mail between daemons. Learn the two primitives and the rest falls out.

## Install

```bash
cargo install --path .
```

## Quick Start

```bash
# Start the daemon
grim daemon

# Summon an agent
grim summon "analyze this codebase for bugs"

# Watch it work
grim bind <id>

# See all agents
grim circle

# Open web dashboard
grim scry
```

## Commands

The vocabulary is deliberately thematic. If you'd rather think in plain terms, here's the decoder ring:

| Grimoire verb | Plain meaning |
|---------------|---------------|
| `summon` | start / run an agent |
| `circle` | list agents (`ps`) |
| `bind` | attach to / tail an agent's output |
| `banish` | kill an agent |
| `invoke` | send a follow-up message to a finished agent |
| `scry` | open the web dashboard |
| `tome` | view / edit config |
| `pact` | chain: when A finishes, start B |
| `inscribe` / `scroll` | load / run a multi-task spec (a DAG) |
| `wake` | trigger that resumes a dormant agent (cron / file / event) |
| `notify` | send a message out to you (webhook) |

### Core lifecycle

| Command | Description |
|---------|-------------|
| `grim daemon` | Start the daemon |
| `grim summon "<task>"` | Spawn a new agent |
| `grim summon --keep-alive "<task>"` | Land in `Dormant` after completion (no wake sources yet) |
| `grim summon --workspace <name> "<task>"` | Run inside a workspace worktree |
| `grim summon --supervisor <id> --restart <policy>` | Attach to a supervisor with `always`/`on_failure`/`never` |
| `grim circle` | List all agents (annotated with worker, supervision, workspace) |
| `grim bind <id>` | Stream an agent's formatted output |
| `grim banish <id>` | Kill a running agent (cascades wake sources, supervision children) |
| `grim invoke <id> "<msg>"` | Send a follow-up message to a completed/dormant agent |
| `grim status` | Daemon health, peer status, registered workers |
| `grim tome` | View/edit config |
| `grim queue` | Show queued/blocked agents and the reason they're held |

### Chaining & orchestration

| Command | Description |
|---------|-------------|
| `grim pact <id> --task "<tpl>"` | Chain agents: fire a new task when `<id>` completes |
| `grim pact --list` | List all pacts |
| `grim inscribe <spec> [-c N] [-a]` | Load a spec for orchestrated execution |
| `grim scroll [id]` | View scroll status or list all scrolls |
| `grim scroll <id> --activate` | Start executing a scroll |
| `grim scroll <id> --abandon` | Cancel a scroll |

### Messaging

| Command | Description |
|---------|-------------|
| `grim mail send <addr> <body>` | Send mail to `agent://<id>` or `topic://<name>` |
| `grim mail list <id> [--pending] [--all] [--after <seq>]` | List a recipient's mailbox |
| `grim mail ack <mail-id>` | Mark a Pending mail as Delivered |
| `grim mail subscribe <id> <topic>` | Subscribe an agent to a topic |
| `grim mail unsubscribe <subscription-id>` | Cancel a subscription |
| `grim mail topics` | List all topics with subscriber counts |

### Wake triggers (dormant agents)

| Command | Description |
|---------|-------------|
| `grim wake add <id> --cron "<expr>"` | Wake an agent on a cron schedule |
| `grim wake add <id> --file-watch <path>` | Wake on filesystem changes |
| `grim wake add <id> --on-complete <parent-id>` | Wake when another agent finishes |
| `grim wake list [<id>]` | List wake sources (globally or per agent) |
| `grim wake remove <wake-id>` | Retire a wake source |
| `grim wake test <wake-id>` | Force-fire a wake source for debugging |

### Workspaces & shared memory

| Command | Description |
|---------|-------------|
| `grim workspace create <name> [--repo <path>] [--branch <name>]` | Provision a git worktree as a shared workspace |
| `grim workspace list` | List active workspaces |
| `grim workspace show <id>` | Show workspace details + assigned agents |
| `grim workspace destroy <id>` | Tear down a workspace (refused if in use) |
| `grim memory put <ws> <key> <value>` (`@file.json` for stdin/file) | Write a memory key (CAS via `--expected-version`) |
| `grim memory get <ws> <key>` | Read a memory value |
| `grim memory list <ws> [--prefix <p>]` | List keys, segment-aligned prefix filter |
| `grim memory delete <ws> <key>` | Remove a key |

Memory writes emit `MemoryWritten` stream events and publish to `topic://workspace/<id>/memory/<prefix>` so subscriber agents wake automatically. File changes inside a workspace publish to `topic://workspace/<id>/files`.

### Federation (cross-daemon)

| Command | Description |
|---------|-------------|
| `grim peer add <name> <url> --secret <token>` | Register a peer `grimd` |
| `grim peer list` | List known peers and last-heartbeat |
| `grim peer remove <name>` | Drop a peer |
| `grim peer ping <name>` | Round-trip the peer link |
| `grim topic federate <topic> --peer <name>` | Forward published mail on `<topic>` to a peer |
| `grim topic unfederate <topic> --peer <name>` | Stop forwarding |

Once peered, `grim mail send agent://grimd-<peer>/<id> "<body>"` delivers across daemons.

### Web

| Command | Description |
|---------|-------------|
| `grim scry` | Open `http://127.0.0.1:6660` web dashboard |

Any command that takes an agent ID also takes a short prefix, so `grim bind 4a` works instead of `4a8c1b2f`.

## Worker Pool

Grimoire can run agents on remote machines through the `grimw` worker binary. With no workers registered it just uses a local executor and nothing changes.

Single machine (the default): no `[worker]` block in `config.toml`, and `grim summon` runs locally like always.

Multi-machine: add a `[worker]` block to the daemon `config.toml`:

```toml
[worker]
listen_addr = "127.0.0.1:7878"   # bind to your Tailscale interface for cross-host
secret = "shared-bearer-token"
heartbeat_timeout_secs = 30
heartbeat_interval_hint_secs = 5
```

On the worker host, install `grimw` and create `~/.grimoire/grimw.toml`:

```toml
daemon_url = "https://daemon.tailnet.ts.net:7878"
secret = "shared-bearer-token"
daemon_cert_sha256 = "abcd1234..."
max_concurrent = 4
tags = ["beefy"]

[providers.claude]
binary = "/usr/local/bin/claude"
```

Then run:

```bash
grimw --config ~/.grimoire/grimw.toml
```

`grim status` on the daemon host lists the registered worker, and `grim circle` shows which worker each agent ran on (or `local`). The scheduler picks the least-loaded worker that matches a summon's provider and capability tags, and falls back to the local executor if none match.

A note on security: `listen_addr` defaults to `127.0.0.1`, never `0.0.0.0`. For cross-machine setups, listen on a Tailscale interface or some equivalent overlay network, not the public internet.

## Pacts (Agent Chaining)

Pacts chain agents together. When a source agent completes, the pact fires and spawns a new agent with the source's output templated in.

```bash
grim summon "find all TODO comments in this project"
grim pact <id> --task "fix these TODOs: {output}"
```

`{output}` gets replaced with the source agent's result text. Chain pacts to build pipelines: A then B then C.

## Messaging

Agents can send each other mail and subscribe to topics. Direct addresses (`agent://<id>`) deliver one row per send. Topic addresses (`topic://<name>`) fan out to whoever's currently subscribed. A dormant agent with a `session_id` wakes on incoming mail, and a live agent reads pending mail at its next turn boundary.

```bash
# Direct
grim mail send agent://4a8c1b2f "review the latest commit"
grim mail list 4a --pending

# Topic pub/sub
grim mail subscribe 4a pr-opened
grim mail send topic://pr-opened "PR #42 opened: needs review"
grim mail topics

# Ack
grim mail ack <mail-id>
```

## Dormant Agents & Wake Triggers

Agents don't have to exit after one task. With `--keep-alive` (or once they finish a normal run with a `session_id`) they go `Dormant` and can be woken by:

- **cron**: `grim wake add 4a --cron "0 9 * * *"` for a 9am daily kickoff
- **file-watch**: `grim wake add 4a --file-watch ./src` (debounced, ignores `.git`/`target`/`node_modules`)
- **parent-completion**: `grim wake add 4a --on-complete 7b` to chain off another agent
- **incoming mail**: automatic for any dormant agent with subscribed topics or pending direct mail

Every wake fire is rate-limited per agent (token bucket) and logged to the durable event stream as `WakeSourceFired` / `WakeSourceFailed`. `grim banish <id>` cascades and retires every wake source atomically.

## Workspaces & Shared Memory

A workspace is a git worktree the daemon provisions and manages. You can assign several agents to the same workspace and let them coordinate two ways.

Through the filesystem: every change publishes to `topic://workspace/<id>/files`, so other agents in the workspace can subscribe and react.

Through a shared memory KV: `grim memory put/get/list/delete` with optimistic CAS (`--expected-version`), namespaced per workspace. It supports segment-prefix subscriptions, so writing to `findings/auth/jwt` wakes anything subscribed to `topic://workspace/<id>/memory/findings`.

```bash
grim workspace create feature-auth --repo ~/repos/app --branch feat/auth
grim summon --workspace feature-auth "implement JWT verification"
grim memory put feature-auth design/auth-decision @notes.md
```

Caps are 256 KiB per value and 64 MiB per workspace. Destroying a workspace cascades its memory and assignments.

## Supervision Trees

Every agent can have a supervisor policy borrowed from OTP:

```bash
grim summon --supervisor <parent-id> --restart on_failure --max-restarts 3 --window-secs 60 "<task>"
```

If a child fails too often, the parent agent wakes with the failure summary and decides what to do: retry with another provider, decompose further, or give up. `grim circle` shows the supervision view, and supervision events flow through the durable log.

## Federation

Two `grimd` instances can peer with each other. Each daemon mints a stable `DaemonId` on first boot (`~/.grimoire/daemon.id`, shown in `grim status`). Once peered:

```bash
grim peer add office https://office.tailnet.ts.net:7878 --secret <token>
grim mail send agent://grimd-office/4a8c "kick off the nightly run"
grim topic federate pr-opened --peer office
```

Direct mail and topic forwarding are at-least-once, with `(sender_daemon_id, sender_seq)` dedupe at the inbox. Scroll-spanning, federated workspaces, and mTLS are still to come.

## Providers

Grimoire drives multiple AI CLI tools through a provider system. Claude Code is the default.

```toml
[agent]
default_provider = "claude"

[providers.codex]
binary = "codex"
args_template = ["-q", "{task}"]

[providers.aider]
binary = "aider"
args_template = ["--message", "{task}", "--yes"]
```

Use `--provider` when summoning:

```bash
grim summon --provider codex "fix the login bug"
```

## Scrolls (Spec-based Orchestration)

A scroll is a spec document with multiple tasks, dependencies, and file ownership. Grimoire runs independent tasks in parallel, respects the dependency DAG, and detects file conflicts.

```markdown
# Scroll: Implement Auth

## Task: Database Schema
- files: src/db/schema.rs, migrations/
- depends: (none)

Create user and session database tables.

## Task: Auth Middleware
- files: src/middleware/auth.rs
- depends: Database Schema

Implement JWT verification middleware.

## Task: Frontend Login
- provider: aider
- files: frontend/src/pages/login.tsx
- depends: Login Endpoint

Create the login UI.
```

Optional top-level directives bind a scroll to a workspace:

```markdown
- workspace: feature-auth
- workspace_repo: ~/repos/app
- workspace_branch: feat/auth
```

Then:

```bash
grim inscribe spec.md --activate
grim scroll <id>
```

What you get:

- Parallel execution up to a configurable concurrency (`-c N`, default 4)
- Dependency tracking, so blocked tasks wait for what they depend on
- File conflict detection, so tasks with overlapping file patterns get serialized
- Failure propagation, so downstream tasks are skipped when a dependency fails
- Per-task provider/model overrides

## Scheduler & Queue

`summon` enqueues, and the daemon scheduler (`grim queue`) promotes `Queued` to `Active` under a global `max_concurrent_agents` cap plus a worker-eligibility check. Claim-for-dispatch is atomic and requeues on failure. The reactor wakes on `AgentQueued`, `WorkerRegistered`, a terminal `StateChange`, and a 100 ms safety tick. Per-tenant quotas, token-bucket rate limits, and `quiet_hours` are still on the list.

## Web Dashboard

`grim scry` opens `http://127.0.0.1:6660` with:

- A live agent list with per-agent activity previews
- Click into any agent for formatted output (tool calls, results, cost)
- Summon, banish, and invoke straight from the browser
- Real-time updates over SSE

## Demos

`grim demo` sets up a working standing-agent flow in one command. It prints each underlying `grim` action as it runs, so it's both a quickstart and a way to see exactly what's happening. Nothing magic: it's `summon --keep-alive` plus a file-watch wake source plus `grim notify`.

```bash
grim demo standing-review --repo ~/repos/myapp --provider claude
```

This summons a keep-alive reviewer rooted in the repo and registers a file-watch wake source. Edit a file and the agent wakes, looks at the change, and calls `grim notify` if it finds something worth your attention. Set `[notifications].webhook_url` (below) to get the pings, watch it live with `grim bind <id>` or `grim scry`, and tear it down with `grim banish <id>`.

It's worth running against your own workflow as an experiment: does a standing, event-woken agent actually earn its keep over running a one-shot agent in a shell loop?

## Notifications

The daemon can reach you, which is the missing half of "fire it and walk away." Point `[notifications]` at any webhook (a Slack or Discord incoming webhook, a relay, anything that accepts a JSON POST):

```toml
[notifications]
webhook_url = "https://hooks.example.com/grimoire"
on_completion = true    # agent reached Complete
on_failure = true       # Failed / Banished / restart budget exhausted
on_wake = true          # a standing agent's wake source fired
on_agent_decided = true # an agent called `grim notify`
timeout_secs = 10
```

Each enabled trigger POSTs `{ event, agent_id, message, level, timestamp }`. With no `webhook_url` set, the notifier never even starts, so there's no overhead.

Agents surface their own findings ("ping me only if it's interesting") by shelling out to `grim notify`. This works from any provider that can run a shell command (claude, `pi`, opencode, aider, and so on):

```bash
grim notify "build is red on main, needs a human" --level error
```

The daemon injects `GRIMOIRE_AGENT_ID` into every agent it spawns, so `grim notify` (and any other `grim` call the agent makes) is attributed to the right agent without the agent ever needing to know its own id. Notifications also land in the durable event log like everything else.

One caveat: identity injection currently covers locally-executed agents. Agents dispatched to a remote `grimw` worker don't get the env var yet. Worker-side injection is a follow-up.

## Configuration

Config lives at `~/.grimoire/config.toml`.

```bash
grim tome
grim tome agent.default_model sonnet
grim tome agent.claude_binary /usr/local/bin/claude
```

Available keys: `daemon.port`, `daemon.log_level`, `agent.default_model`, `agent.default_cwd`, `agent.claude_binary`, `agent.default_provider`, plus the `[worker]`, `[daemon.auth]`, `[notifications]`, and `[providers.*]` blocks.

## Auth

Grimoire has three independent trust domains, and each ships with auth on by default.

CLI to daemon (UDS): connections from the daemon's own UID are trusted via `SO_PEERCRED`, no token needed. Cross-UID connections have to present the daemon's bearer token on the first RPC. The token is auto-generated on first boot at `~/.grimoire/auth.token` (mode `0600`) and read by both `grim` and the dashboard. Resolution order is `GRIMOIRE_AUTH_TOKEN` env, then `[daemon.auth] token = "..."` in `config.toml`, then the file.

Dashboard (HTTP): `/api/*` needs `Authorization: Bearer <token>` or a `grim_auth` cookie set by `/auth/login`. `grim scry` mints a one-shot login URL (`/auth/login?t=<token>`) and opens the browser, the browser stores the cookie, and you're in. The listener is bound to loopback (`127.0.0.1`) and there's no loopback bypass of auth.

Federation peers (gRPC) and the worker pool (gRPC) each carry their own per-link tokens (`peer add --token ...` and `[worker] secret = ...`). These are separate from the CLI/HTTP token.

All three transports also negotiate a protocol version on the first message and reject mismatches with `unsupported_protocol_version`. That's what lets future wire-incompatible changes ship cleanly instead of silently misbehaving.

### Token recovery

The token at `~/.grimoire/auth.token` is the canonical source. If it's lost, or you want to rotate it:

```bash
# Stop the daemon, delete the token, restart, and a fresh token is minted on boot.
pkill -f "grim daemon"
rm ~/.grimoire/auth.token
grim daemon
```

If `[daemon.auth] token = "..."` is pinned in `config.toml`, that value wins on the next boot and the file is rewritten to match. The `GRIMOIRE_AUTH_TOKEN` env var (highest priority) is a one-shot override for the current process and doesn't persist back to the file.

There are no `grim auth rotate / show / reset` subcommands yet. The file is the UX.

### CLI vs worker vs peer tokens (side-by-side)

The three trust domains use independent credentials. Configuring one doesn't touch the others:

| Domain | Where it lives | Used by |
| --- | --- | --- |
| CLI to daemon (UDS) + Dashboard (HTTP) | `~/.grimoire/auth.token`, or `[daemon.auth] token` in `config.toml`, or `GRIMOIRE_AUTH_TOKEN` env | `grim *` cross-UID, browser dashboard |
| Daemon to worker (gRPC) | `[worker] secret = "..."` (matched on both daemon and worker `config.toml`) | `grimw` registering with the daemon |
| Peer to peer (gRPC) | Per-peer, set with `grim peer add ... --token <T>` (stored in the `peers` table) | Federation gossip / outbox |

Rotating any of these is independent. Change the daemon token without touching workers, swap a peer token without affecting the dashboard, and so on.

### Cross-UID deployment

The peercred bypass only fires for connections from the daemon's own UID. To run a shared `grimd` as a service account with multiple human users:

1. Run the daemon as a dedicated user (say `grimd`): `sudo -u grimd grim daemon`.
2. Read its token: `sudo -u grimd cat ~grimd/.grimoire/auth.token`.
3. Distribute the token to each user's `~/.grimoire/auth.token` (or set `GRIMOIRE_AUTH_TOKEN` in their shell, or pin `[daemon.auth] token` in a shared `config.toml`).
4. Each `grim` call now connects across UIDs and presents the token on the first RPC.

The daemon's UDS socket lives at `~grimd/.grimoire/grimd.sock` and is `0600`, so users also need group or ACL access to the socket path. Usually that means adding them to a `grimd` group and tightening the file mode in the service unit.

### HTTP login internals

`/auth/login` accepts the token two ways:

- `GET /auth/login?t=<token>` for the magic-link flow. `grim scry` opens `http://127.0.0.1:<port>/auth/login?t=<token>` in the browser, the browser exchanges it for the `grim_auth` cookie, and redirects to `/`.
- `POST /auth/login` (`token=<token>`, form-encoded) for the fallback login page when the URL doesn't carry the token.

The cookie is `HttpOnly; SameSite=Strict; Max-Age=86400`. It doesn't set `Secure` yet because the listener is plain HTTP on loopback. When peer/HTTP TLS lands, flip `Secure` on conditionally. There's a TODO comment in `src/daemon/server.rs::login_success_response`.

### Test-only env var

`GRIMOIRE_AUTH_TOKEN_PATH` overrides the token file location. It exists so the integration test suite can isolate auth state per-test under `tempfile`-managed directories. It's not meant for ops use. Pin `[daemon.auth] token` in `config.toml` instead if you want a stable token at a non-default path.

### Hard-cutover notes (v0.1)

These landed alongside the auth and protocol-version work and might surprise contributors upgrading mid-stream:

- Old workers are rejected. Workers built before `protocol_version` was added to `RegisterWorker` get rejected at registration with `FailedPrecondition: unsupported_protocol_version`. Rebuild `grimw` from this tree.
- Old `grim` CLIs still work from the owning UID. A CLI built before `auth_token` was added to the JSON-RPC envelope passes through the peercred bypass. Cross-UID calls from old CLIs are rejected, so rebuild.
- The UDS socket is `0600`. Earlier builds left it world-readable in some setups. If you script around the socket, expect EACCES from other UIDs. That's the point: fall back to the HTTP listener or TCP forwarding.

## Architecture

A single Rust binary (`grim`) runs as both CLI and daemon. A second binary (`grimw`) is the remote worker.

- **Daemon (`grimd`)** manages agent lifecycles, persists state in SQLite, and exposes a JSON-RPC UDS, an HTTP/SSE dashboard, and a peer gRPC channel.
- **Scheduler** owns admission control, capability-aware placement across workers, and dispatch.
- **Provider registry** abstracts CLI tools (Claude, Codex, aider, and so on) behind a common trait.
- **Orchestrator** fires pacts and supervision restarts on agent completions.
- **ScrollKeeper** runs spec DAGs with parallel scheduling, conflict detection, and dependency tracking.
- **WakeRegistry** owns cron, file-watch, and parent-completion wake sources, and fires through the mail bus.
- **WorkspaceRegistry + WorkspaceWatcher** provision worktrees and emit `WorkspaceFileChanged` topic mail.
- **Supervisor** enforces restart policies and escalation routing.
- **EventBus** writes through to a SQLite `events` table with per-stream sequence numbers (the durable log).
- **Peer client/server + outbox/inbox** federate direct mail and opt-in topics across daemons.

```
CLI (grim) ──UDS──▶ Daemon (grimd) ──┬─▶ Scheduler ──▶ LocalExecutor ──▶ child processes
Browser   ──HTTP──▶    │              └─▶ Scheduler ──▶ RemoteExecutor ──gRPC──▶ grimw (worker)
Peer grimd ──gRPC──▶   │
                       ├── SQLite (agents, events, pacts, scrolls, tasks, mail,
                       │           subscriptions, wake_sources, workspaces,
                       │           workspace_memory, peers, peer_outbox, ...)
                       ├── EventBus (durable, per-stream seq)
                       ├── Orchestrator + Supervisor
                       ├── ScrollKeeper / WakeRegistry / WorkspaceRegistry
                       └── PeerClient/Server (federation)
```

## Data

All state lives in `~/.grimoire/`:

- `grimoire.db` is the SQLite database (everything durable)
- `daemon.id` is the stable `DaemonId` minted on first boot
- `grimd.sock` is the Unix domain socket
- `grimd.pid` is the daemon PID file
- `config.toml` is the configuration
- `workspaces/<name>/` holds the git worktrees from `grim workspace create`

## MSRV

The minimum supported Rust version is **1.95**, pinned in `rust-toolchain.toml` and checked by a dedicated CI job. Bumping the MSRV is a minor-version release.

## Status

The user-facing feature surface is complete: summon, scrolls, pacts, mail, wake triggers, workspaces, memory, supervision, workers, federation, and outbound notifications. The trust layer has its first piece, with auth and protocol versioning on UDS, HTTP, peers, and workers (see [Auth](#auth)).

Right now the focus is making what's already here legible and useful rather than piling on more surface. Outbound notifications and the `grim demo` standing-agent flow have shipped. Next up is federated shared memory, which is the org-wide shared-context piece (today only mail and topics federate). Still open after that: observability (Prometheus and OTel), sandboxing (cwd jail and cgroups), policy and budget primitives, and replay/eval.

## License

Grimoire is dual-licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option. Contributions are accepted under the same dual MIT/Apache-2.0 terms (per Apache-2.0 §5, unless you state otherwise).
