# Grimoire

A daemon-based orchestrator for AI coding agents. Replaces fragile tmux workflows with proper process supervision — agents run whether or not you're looking at them.

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

| Command | Description |
|---------|-------------|
| `grim daemon` | Start the daemon |
| `grim summon "<task>"` | Spawn a new agent |
| `grim circle` | List all agents |
| `grim bind <id>` | Stream an agent's formatted output |
| `grim banish <id>` | Kill a running agent |
| `grim invoke <id> "<msg>"` | Send a follow-up message to a completed agent |
| `grim pact <id> --task "<tpl>"` | Chain agents — fire a new task when `<id>` completes |
| `grim pact --list` | List all pacts |
| `grim status` | Daemon health check |
| `grim tome` | View/edit config |
| `grim inscribe <spec> [-c N] [-a]` | Load a spec for orchestrated execution |
| `grim scroll [id]` | View scroll status or list all scrolls |
| `grim scroll <id> --activate` | Start executing a scroll |
| `grim scroll <id> --abandon` | Cancel a scroll |
| `grim scry` | Open web dashboard |
| `grim mail send <addr> <body>` | Send mail to `agent://<id>` or `topic://<name>` |
| `grim mail list <id> [--pending] [--all] [--after <seq>]` | List a recipient's mailbox |
| `grim mail ack <mail-id>` | Mark a Pending mail as Delivered |
| `grim mail subscribe <id> <topic>` | Subscribe an agent to a topic |
| `grim mail unsubscribe <subscription-id>` | Cancel a subscription |
| `grim mail topics` | List all topics with subscriber counts |

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

`grim status` on the daemon host will list the registered worker. `grim circle` annotates each agent with the worker it ran on (or `local`). The daemon picks the least-loaded worker matching each summon's provider; if no worker matches it falls back to `LocalExecutor`.

**Security note:** `listen_addr` defaults to `127.0.0.1` — never `0.0.0.0`. Cross-machine deployments should listen on a Tailscale interface (or equivalent overlay network), not the public internet.

## Pacts (Agent Chaining)

Pacts let you chain agents together. When a source agent completes, the pact fires and spawns a new agent with the source's output templated in.

```bash
# Summon an agent
grim summon "find all TODO comments in this project"

# Chain a follow-up
grim pact <id> --task "fix these TODOs: {output}"

# The second agent spawns automatically when the first completes
```

`{output}` is replaced with the source agent's result text. You can chain pacts to build pipelines: A → B → C.

## Messaging

Agents can send each other mail and subscribe to topics. Direct addresses (`agent://<id>`) deliver one row per send; topic addresses (`topic://<name>`) fan out to current subscribers. Dormant agents with a `session_id` are woken on incoming mail; live agents read pending mail at their next turn boundary.

### Quickstart

```bash
# Send mail directly to one agent
grim mail send agent://4a8c1b2f "review the latest commit"

# Read a mailbox
grim mail list 4a --pending

# Topic pub/sub: any subscriber receives a row when someone publishes
grim mail subscribe 4a pr-opened
grim mail send topic://pr-opened "PR #42 opened: needs review"

# List all topics + subscriber counts
grim mail topics

# Mark a Pending mail as Delivered (so the recipient stops seeing it as new)
grim mail ack <mail-id>
```

### Limitations (v1)

- Mail surfaces at turn boundaries — no mid-turn interrupts of an `Active` agent.
- No auth on the local UDS — any caller may send as any sender.
- Single direct recipient per `mail.send` (use a topic for fanout).

## Providers

Grimoire supports multiple AI CLI tools via a provider system. Claude Code is the default, but you can configure others in `~/.grimoire/config.toml`:

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

Scrolls let you define a spec document with multiple tasks (tasks), dependencies between them, and file ownership. Grimoire executes independent tasks in parallel, respects the dependency DAG, and detects file conflicts.

Create a spec file (`spec.md`):

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

## Task: Login Endpoint
- files: src/routes/auth.rs
- depends: Database Schema, Auth Middleware

Build login and registration endpoints.

## Task: Frontend Login
- provider: aider
- files: frontend/src/pages/login.tsx
- depends: Login Endpoint

Create the login UI.
```

Then inscribe and activate:

```bash
# Load the spec
grim inscribe spec.md --activate

# Watch progress
grim scroll <id>

# Output:
# ◆ Scroll: Implement Auth  [active]
#   4 tasks: 1 complete, 2 active, 1 blocked
#
#   [✓] done     Database Schema         agent:a1b2c3d4
#   [◆] active   Auth Middleware          agent:e5f6g7h8
#   [◆] active   Login Endpoint           agent:i9j0k1l2
#   [◇] blocked  Frontend Login           waiting on: Login Endpoint
```

Features:
- Parallel execution up to configurable concurrency (`-c N`, default 4)
- Dependency tracking — blockedtasks wait for their dependencies
- File conflict detection —tasks with overlapping file patterns are serialized
- Failure propagation — downstreamtasks are skipped when a dependency fails
- Per-task provider/model overrides

## Web Dashboard

`grim scry` opens `http://127.0.0.1:6660` with:

- Live agent list with per-agent activity previews
- Click into any agent for formatted output (tool calls, results, cost)
- Summon, banish, and invoke directly from the browser
- Real-time updates via SSE

## Configuration

Config lives at `~/.grimoire/config.toml`.

```bash
# View current config
grim tome

# Set default model
grim tome agent.default_model sonnet

# Set claude binary path
grim tome agent.claude_binary /usr/local/bin/claude
```

Available keys: `daemon.port`, `daemon.log_level`, `agent.default_model`, `agent.default_cwd`, `agent.claude_binary`, `agent.default_provider`.

## Architecture

Single Rust binary (`grim`) that runs as both CLI and daemon.

- **Daemon** manages agent lifecycles via `tokio::process`, persists state in SQLite, broadcasts events via `tokio::sync::broadcast`
- **CLI** connects over a Unix domain socket using JSON-RPC
- **Web dashboard** served by the daemon via Axum with SSE for live updates
- **Provider registry** abstracts CLI tools (Claude, Codex, aider, etc.) behind a common trait
- **Orchestrator** listens for agent completions and fires matching pacts
- **ScrollKeeper** executes spec-based DAGs with parallel scheduling, conflict detection, and dependency tracking

```
CLI (grim) ──UDS──▶ Daemon (grimd) ──▶ Provider Registry ──▶ Agent processes
Browser   ──HTTP──▶    │                                     (claude, codex, aider, ...)
                       ├── SQLite (agents, events, pacts, scrolls, tasks)
                       ├── Event bus (tokio::broadcast)
                       ├── Orchestrator (pact engine)
                       └── ScrollKeeper (DAG executor)
```

## Data

All state lives in `~/.grimoire/`:

- `grimoire.db` — SQLite database (agents, events, pacts)
- `grimd.sock` — Unix domain socket
- `grimd.pid` — daemon PID file
- `config.toml` — configuration
