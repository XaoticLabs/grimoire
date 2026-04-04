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

All commands accepting an agent ID support short prefixes (e.g. `grim bind 4a` instead of `4a8c1b2f`).

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

Scrolls let you define a spec document with multiple tasks (runes), dependencies between them, and file ownership. Grimoire executes independent tasks in parallel, respects the dependency DAG, and detects file conflicts.

Create a spec file (`spec.md`):

```markdown
# Scroll: Implement Auth

## Rune: Database Schema
- files: src/db/schema.rs, migrations/
- depends: (none)

Create user and session database tables.

## Rune: Auth Middleware
- files: src/middleware/auth.rs
- depends: Database Schema

Implement JWT verification middleware.

## Rune: Login Endpoint
- files: src/routes/auth.rs
- depends: Database Schema, Auth Middleware

Build login and registration endpoints.

## Rune: Frontend Login
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
#   4 runes: 1 complete, 2 active, 1 blocked
#
#   [✓] done     Database Schema         agent:a1b2c3d4
#   [◆] active   Auth Middleware          agent:e5f6g7h8
#   [◆] active   Login Endpoint           agent:i9j0k1l2
#   [◇] blocked  Frontend Login           waiting on: Login Endpoint
```

Features:
- Parallel execution up to configurable concurrency (`-c N`, default 4)
- Dependency tracking — blocked runes wait for their dependencies
- File conflict detection — runes with overlapping file patterns are serialized
- Failure propagation — downstream runes are skipped when a dependency fails
- Per-rune provider/model overrides

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
                       ├── SQLite (agents, events, pacts, scrolls, runes)
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
