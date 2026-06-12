# Providers: bring your own CLI

Grimoire is a supervisor, not an agent. The thing that actually talks to a
model is whatever CLI you point it at, and every CLI hides behind one
`Provider` trait: spawn a process with a task, watch its stdout, extract a
session id / result / token counts. This page explains the two adapter
tiers and gives copy-paste config for common CLIs.

## The two tiers

**Native-resume adapters** (`claude`, `pi`) know the CLI's session model.
When a dormant agent wakes, the daemon resumes the CLI's own session
(`claude --resume <id>`, `pi --session <id>`), so the agent keeps its full
context with no fidelity loss. These two are built in, verified live, and
their names are reserved — a `[providers.claude]` block can configure their
sandbox/pricing but can't replace the adapter.

**Generic adapters** (everything else) are declared in config with a
binary and an args template. They don't need to know anything about
sessions: when a dormant generic agent wakes, the daemon reconstructs a
context preamble from the durable event log (the agent's prior transcript,
tail-capped at 16 KiB) and prepends it to a fresh run. That transcript
replay is what makes *any* one-shot CLI usable as a standing agent.

| | session on wake | token accounting | model selection |
|---|---|---|---|
| native (`claude`, `pi`) | CLI's own session, full fidelity | yes (stream JSON) | yes |
| generic (`[providers.*]`) | transcript replay from the event log | only if you set `pricing` and the CLI prints counts | via `args_template` |

## Declaring a generic provider

```toml
[providers.<name>]
binary = "path-or-name"            # resolved via PATH
args_template = ["...", "{task}"]  # {task} is replaced with the prompt
env = { KEY = "value" }            # optional extra env
# Optional per-provider confinement (any field opts in; missing host
# tooling degrades to a one-time warning, not a failed spawn):
# [providers.<name>.sandbox]
# fs_jail = true                  # bwrap: ro_paths read-only, rest of / hidden
# allow_network = false           # only consulted when fs_jail = true
# ro_paths = ["/usr", "/etc"]
# rw_paths = []                   # the agent's cwd is added automatically
# memory_max = 2147483648         # bytes; systemd MemoryMax=
# cpu_quota_percent = 200         # 100 = one full core
# token_budget = 500000           # suspend the agent past this lifetime total
#
# Optional pricing so runs attribute USD spend to [budgets.*]:
# [providers.<name>.pricing]
# input_per_mtok = 3.0
# output_per_mtok = 15.0
```

The task is passed as a process argument (never through a shell), so
prompts with quotes/metacharacters are safe. Every spawned agent also gets
`GRIMOIRE_AGENT_ID` in its environment, so it can call back into `grim
mail` / `grim memory` / `grim notify` knowing who it is.

**AGENTS.md.** If the agent's cwd contains an `AGENTS.md`, generic
providers get it prepended to the prompt (capped at 8 KiB) under a
"Project instructions" heading — the same courtesy agent-native CLIs
(claude, codex, opencode) extend themselves by reading instruction files
directly. Workspaces are git worktrees, so a tracked `AGENTS.md` is
present in every workspace automatically.

## Presets

These use each CLI's documented non-interactive flags. Unlike the claude
and pi adapters they are not continuously verified against live runs —
check `<cli> --help` if a flag has drifted, and please file an issue if
one has.

### Codex (OpenAI)

```toml
[providers.codex]
binary = "codex"
args_template = ["exec", "{task}"]
```

`codex exec` is Codex's headless mode: runs the task, prints, exits.
Codex has its own native session store and resume; a native-resume
adapter (tier one) is a welcome contribution — until then, wakes use
transcript replay.

### opencode

```toml
[providers.opencode]
binary = "opencode"
args_template = ["run", "{task}"]
```

### Aider

```toml
[providers.aider]
binary = "aider"
args_template = ["--message", "{task}", "--yes-always"]
```

`--yes-always` keeps aider from blocking on confirmation prompts; pin the
files you want it allowed to touch with extra args if you don't want it
choosing.

### Anything that takes a prompt

A provider doesn't have to be an AI CLI. A shell script that reads `$1`
and prints to stdout is a valid provider, which is handy for testing wake
plumbing without spending tokens:

```toml
[providers.echo]
binary = "/usr/local/bin/my-script.sh"
args_template = ["{task}"]
```

## Using a provider

```bash
grim summon --provider codex "explain the failing test"
grim summon --provider aider --keep-alive "watch this repo's TODOs"
```

Set the default in config:

```toml
[agent]
default_provider = "claude"
```

## Writing a native adapter

If your CLI has a real session model (a stable session id printed at
start, a resume flag), a tier-one adapter is ~150 lines: implement
`Provider` (`src/daemon/provider.rs`) with `supports_resume: true`,
`spawn`/`spawn_resume`, and `extract_session_id`/`extract_result` over its
stdout format. The `pi` adapter (`src/daemon/providers/pi.rs`) is the
model to copy — including its top-of-file note recording exactly which CLI
version the stdout format was verified against. Verify against a live run
before claiming native support; that's the repo's bar.
