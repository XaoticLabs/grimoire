# Grimoire

A daemon-based orchestrator for AI coding agents. Replaces fragile tmux workflows with proper process supervision — agents run whether or not you're looking at them, wake on schedules and file changes, message each other, and self-heal under supervision.

> **Thesis:** Agents are processes, not function calls. Grimoire is `systemd` + `kubelet` + `nats` for AI workers.

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
| `grim pact <id> --task "<tpl>"` | Chain agents — fire a new task when `<id>` completes |
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

All commands accepting an agent ID support short prefixes (e.g. `grim bind 4a` instead of `4a8c1b2f`).

## Worker Pool

Grimoire can run agents on remote machines via the `grimw` worker binary. With zero workers registered the daemon falls back to a local executor and behavior is unchanged.

**Single machine (default):** no `[worker]` block in `config.toml`. `grim summon` runs locally as before.

**Multi-machine:** add a `[worker]` block to the daemon `config.toml`:

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

`grim status` on the daemon host lists the registered worker. `grim circle` annotates each agent with the worker it ran on (or `local`). The scheduler picks the least-loaded worker matching each summon's provider and capability tags; if no worker matches it falls back to `LocalExecutor`.

**Security note:** `listen_addr` defaults to `127.0.0.1` — never `0.0.0.0`. Cross-machine deployments should listen on a Tailscale interface (or equivalent overlay network), not the public internet.

## Pacts (Agent Chaining)

Pacts let you chain agents together. When a source agent completes, the pact fires and spawns a new agent with the source's output templated in.

```bash
grim summon "find all TODO comments in this project"
grim pact <id> --task "fix these TODOs: {output}"
```

`{output}` is replaced with the source agent's result text. Chain pacts to build pipelines: A → B → C.

## Messaging

Agents can send each other mail and subscribe to topics. Direct addresses (`agent://<id>`) deliver one row per send; topic addresses (`topic://<name>`) fan out to current subscribers. Dormant agents with a `session_id` are woken on incoming mail; live agents read pending mail at their next turn boundary.

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

Agents don't have to exit after one task. With `--keep-alive` (or once they finish a normal run with a `session_id`) they enter `Dormant` and can be woken on:

- **cron** — `grim wake add 4a --cron "0 9 * * *"` for a 9am daily kickoff
- **file-watch** — `grim wake add 4a --file-watch ./src` (debounced, ignores `.git`/`target`/`node_modules`)
- **parent-completion** — `grim wake add 4a --on-complete 7b` to chain off another agent
- **incoming mail** — automatic for any dormant agent with subscribed topics or pending direct mail

All wake fires are rate-limited per agent (token bucket) and logged to the durable event stream as `WakeSourceFired` / `WakeSourceFailed`. `grim banish <id>` cascades and retires every wake source atomically.

## Workspaces & Shared Memory

A workspace is a git worktree the daemon provisions and lifecycles. Multiple agents can be assigned to the same workspace and coordinate through:

- The **filesystem itself** — every change publishes to `topic://workspace/<id>/files`, so other agents in the workspace can subscribe and react.
- **Shared memory KV** — `grim memory put/get/list/delete` with optimistic CAS (`--expected-version`), namespaced per workspace, with segment-prefix subscriptions: writing to `findings/auth/jwt` wakes anything subscribed to `topic://workspace/<id>/memory/findings`.

```bash
grim workspace create feature-auth --repo ~/repos/app --branch feat/auth
grim summon --workspace feature-auth "implement JWT verification"
grim memory put feature-auth design/auth-decision @notes.md
```

Per-value cap 256 KiB, per-workspace cap 64 MiB. Workspace destroy cascades memory and assignments.

## Supervision Trees

Every agent can have a supervisor policy borrowed from OTP:

```bash
grim summon --supervisor <parent-id> --restart on_failure --max-restarts 3 --window-secs 60 "<task>"
```

If the child fails too often the parent agent is woken with the failure summary and decides what to do (retry with another provider, decompose further, give up). `grim circle` shows the supervision view; supervision events flow through the durable log.

## Federation

Two `grimd` instances can peer with each other. Each daemon mints a stable `DaemonId` on first boot (`~/.grimoire/daemon.id`, surfaced via `grim status`). Once peered:

```bash
grim peer add office https://office.tailnet.ts.net:7878 --secret <token>
grim mail send agent://grimd-office/4a8c "kick off the nightly run"
grim topic federate pr-opened --peer office
```

Direct mail and topic forwarding are at-least-once with `(sender_daemon_id, sender_seq)` dedupe at the inbox. Scroll-spanning, federated workspaces, and mTLS are post-v1.

## Providers

Grimoire supports multiple AI CLI tools via a provider system. Claude Code is the default.

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

Scrolls let you define a spec document with multiple tasks, dependencies, and file ownership. Grimoire executes independent tasks in parallel, respects the dependency DAG, and detects file conflicts.

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

Features:
- Parallel execution up to configurable concurrency (`-c N`, default 4)
- Dependency tracking — blocked tasks wait for their dependencies
- File conflict detection — tasks with overlapping file patterns are serialized
- Failure propagation — downstream tasks are skipped when a dependency fails
- Per-task provider/model overrides

## Scheduler & Queue

`summon` enqueues; the daemon scheduler (`grim queue`) promotes `Queued → Active` under a global `max_concurrent_agents` cap and a worker-eligibility check. Atomic claim-for-dispatch with requeue-on-failure; reactor wakes on `AgentQueued`, `WorkerRegistered`, terminal `StateChange`, plus a 100 ms safety tick. Per-tenant quotas, token-bucket rate limits, and `quiet_hours` are still on the roadmap.

## Web Dashboard

`grim scry` opens `http://127.0.0.1:6660` with:

- Live agent list with per-agent activity previews
- Click into any agent for formatted output (tool calls, results, cost)
- Summon, banish, and invoke directly from the browser
- Real-time updates via SSE

## Configuration

Config lives at `~/.grimoire/config.toml`.

```bash
grim tome
grim tome agent.default_model sonnet
grim tome agent.claude_binary /usr/local/bin/claude
```

Available keys: `daemon.port`, `daemon.log_level`, `agent.default_model`, `agent.default_cwd`, `agent.claude_binary`, `agent.default_provider`, plus `[worker]` and `[providers.*]` blocks.

## Architecture

Single Rust binary (`grim`) that runs as both CLI and daemon. A second binary (`grimw`) is the remote worker.

- **Daemon (`grimd`)** manages agent lifecycles, persists state in SQLite, exposes a JSON-RPC UDS, an HTTP/SSE dashboard, and a peer gRPC channel.
- **Scheduler** owns admission control, capability-aware placement across workers, and dispatch.
- **Provider registry** abstracts CLI tools (Claude, Codex, aider, …) behind a common trait.
- **Orchestrator** fires pacts and supervision restarts on agent completions.
- **ScrollKeeper** executes spec DAGs with parallel scheduling, conflict detection, and dependency tracking.
- **WakeRegistry** owns cron / file-watch / parent-completion wake sources; fires through the mail bus.
- **WorkspaceRegistry + WorkspaceWatcher** provision worktrees and emit `WorkspaceFileChanged` topic mail.
- **Supervisor** enforces restart policies and escalation routing.
- **EventBus** is write-through to a SQLite `events` table with per-stream sequence numbers (durable log).
- **Peer client/server + outbox/inbox** federate direct mail and opt-in topics across daemons.

```
CLI (grim) ──UDS──▶ Daemon (grimd) ──┬─▶ Scheduler ──▶ LocalExecutor ──▶ child processes
Browser   ──HTTP──▶    │              └─▶ Scheduler ──▶ RemoteExecutor ──gRPC──▶ grimw (worker)
Peer grimd ──gRPC──▶   │
                       ├── SQLite (agents, events, pacts, scrolls, tasks, mail,
                       │           subscriptions, wake_sources, workspaces,
                       │           workspace_memory, peers, peer_outbox, …)
                       ├── EventBus (durable, per-stream seq)
                       ├── Orchestrator + Supervisor
                       ├── ScrollKeeper / WakeRegistry / WorkspaceRegistry
                       └── PeerClient/Server (federation)
```

## Data

All state lives in `~/.grimoire/`:

- `grimoire.db` — SQLite database (everything durable)
- `daemon.id` — stable `DaemonId` minted on first boot
- `grimd.sock` — Unix domain socket
- `grimd.pid` — daemon PID file
- `config.toml` — configuration
- `workspaces/<name>/` — git worktrees provisioned by `grim workspace create`

## Status

The user-facing feature surface (summon, scrolls, pacts, mail, wake triggers, workspaces, memory, supervision, workers, federation) is complete. What's still open is the **trust layer** under it — auth on the local sockets, observability (Prometheus + OTel), sandboxing (cwd jail + cgroups), policy/budget primitives, and replay/eval. See [`ROADMAP.md`](./ROADMAP.md) Part 6 for the next-pickup ordering.
