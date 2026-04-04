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

Available keys: `daemon.port`, `daemon.log_level`, `agent.default_model`, `agent.default_cwd`, `agent.claude_binary`.

## Architecture

Single Rust binary (`grim`) that runs as both CLI and daemon.

- **Daemon** manages agent lifecycles via `tokio::process`, persists state in SQLite, broadcasts events via `tokio::sync::broadcast`
- **CLI** connects over a Unix domain socket using JSON-RPC
- **Web dashboard** served by the daemon via Axum with SSE for live updates
- **Orchestrator** listens for agent completions and fires matching pacts

```
CLI (grim) ──UDS──▶ Daemon (grimd) ──▶ Agent processes
Browser   ──HTTP──▶    │               (claude --print --output-format stream-json)
                       ├── SQLite (state + event log)
                       ├── Event bus (tokio::broadcast)
                       └── Orchestrator (pact engine)
```

## Data

All state lives in `~/.grimoire/`:

- `grimoire.db` — SQLite database (agents, events, pacts)
- `grimd.sock` — Unix domain socket
- `grimd.pid` — daemon PID file
- `config.toml` — configuration
