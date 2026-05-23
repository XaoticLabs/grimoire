# Recipe: a standing review agent

A reviewer that lives in your repo, wakes whenever a file changes, looks at the
diff, and pings you only when something's worth a human. It never exits. You set
it up once and walk away.

This is the canonical Grimoire workflow — the thing a daemon can do and a
one-shot CLI can't. The whole flow is three primitives composed: `summon
--keep-alive` (a standing agent), a file-watch wake source, and `grim notify`
(the agent reaching back out to you). Nothing here is special-cased; you can
build it by hand, and the bottom of this page shows how.

## The one-liner

```bash
grim demo standing-review --repo ~/repos/myapp --provider claude
```

That prints each underlying `grim` action as it runs, so it's also a way to see
exactly what it's doing. When it finishes you have a live reviewer. Edit a file
in the repo and it wakes.

`--repo` defaults to the current directory.

**Provider note:** the standing/wake/resume mechanic requires a provider whose
CLI supports session resume. When a standing agent wakes, the daemon resumes its
existing session so it carries what it saw last time. Support today:

- **Claude** (`claude --resume`) — supported, verified live.
- **pi** (`pi --session <id>`) — native resume, verified live (pi 0.75.4).
- **Generic CLIs wired via `[providers.*]`** (aider, etc.) — supported via
  **daemon-managed continuity** (`ContextReplay`): they have no native session,
  so the daemon mints a synthetic session id (they still go `Dormant` and wake)
  and, on each wake, replays the agent's prior output from the event log as a
  context preamble before the new request. Lower fidelity than a native session
  (it's a transcript replay, capped at 16 KiB), and not yet exercised in a live
  run — Claude and pi are the verified paths.

The reviewer *brief* itself is fully provider-neutral — it only shells out to
`git diff` and `grim notify`.

## What you get, and how to drive it

```bash
# See it think, live:
grim bind <id>          # stream its output
grim scry               # or watch in the dashboard

# Get the pings somewhere real — configure any sink in ~/.grimoire/config.toml:
[notifications]
# Pick one or more. Any sink configured spawns the notifier; multiple sinks
# fan out independently.
webhook_url = "https://hooks.slack.com/services/…"   # Slack/Discord/any JSON POST
log_file    = "/var/log/grimoire-notify.jsonl"       # append-only JSON Lines, offline
desktop     = true                                    # notify-send toast (Linux)
on_agent_decided = true                              # fire when the agent calls `grim notify`

# Tear it down (cascades the wake source):
grim banish <id>
```

With no sink configured, the agent's findings still land in `grim bind` and the
durable event log — you just won't get pushed a message. For a fully offline
setup, `log_file` + `desktop = true` is enough: the toast pops on each finding
and the JSONL trail is a tail-able local audit.

## What the agent is told

The reviewer's brief is deliberately narrow (provider-neutral, read-only):

> You are a standing code reviewer running under Grimoire. You wake whenever a
> file changes in this repository. On each wake: inspect the most recent changes
> (`git diff`, `git status`), and IF you find something a human should know
> about — a likely bug, a risky change, a failing test, a security issue —
> surface it with `grim notify "<short finding>" --level warn`. If nothing is
> noteworthy, stay quiet. Be terse. Do NOT modify any files.

It never needs to know its own agent ID: the daemon injects `GRIMOIRE_AGENT_ID`
into every agent it spawns, so `grim notify` is auto-attributed.

Note that the brief leans on the agent *not* re-flagging things it already
raised. Grimoire guarantees the mechanism for that — each wake resumes the
agent's existing CLI session (`agent_manager.rs` → the provider's native resume,
e.g. `claude --resume` / `pi --session`), so its context carries its prior turns
and it *can* see what it already said. Whether it reliably stays quiet on a
repeat is down to the model and the brief, not a daemon guarantee; tune the
prompt if it gets chatty.

## Building it by hand

The demo is exactly this, with nicer output:

```bash
# 1. Summon a keep-alive reviewer rooted in the repo. --keep-alive lands it in
#    Dormant after the first run instead of exiting.
grim summon --keep-alive --cwd ~/repos/myapp \
  "You are a standing code reviewer… (the brief above)"
# → reviewer summoned: 4a8c1b2f

# 2. Wake it on any file change under the repo. Debounced; ignores the noise.
grim wake add 4a --file-watch ~/repos/myapp
# → file-watch wake source registered

# 3. (optional) point notifications at a webhook — see config above.
```

Edit a file. The watcher debounces (200 ms), batches the changed paths, and
fires the wake source. The dormant agent resumes with the same session it had
last time — it remembers what it already looked at — runs `git diff`, decides,
and either calls `grim notify` or stays quiet and goes back to `Dormant`.

`.git/`, `target/`, and `node_modules/` are ignored by default so a build or a
commit doesn't wake it.

## Is this actually better than a shell loop? (the honest part)

You can approximate this with `while inotifywait …; do claude -p "review the
diff"; done`. So when is the daemon worth it? This is the experiment worth being
honest about, because the answer decides whether standing agents are the moat or
just ceremony.

**The daemon earns its keep when:**

- **Continuity matters.** The standing agent resumes its *session* on each wake
  (the provider's native resume — `claude --resume`, `pi --session`), so it
  carries the context of what it already flagged — it *can* avoid re-reporting
  the same thing every time you save, where a fresh `claude -p` starts from zero
  each loop. (Continuity is guaranteed by the daemon; whether the model acts on
  it is prompt-dependent. Requires a resume-capable provider — see the provider
  note above.)
- **You want it to outlive the terminal.** Close the laptop, the shell loop dies.
  The daemon keeps the agent dormant and ready; it survives daemon restarts
  (state is in SQLite, in-flight agents are reconciled on reboot).
- **You're running more than one.** Restart policy when the agent crashes, rate
  limiting so a rebase storm doesn't fire it 400 times, a durable event log of
  every wake and finding, one dashboard across all of them, the same agent
  reachable by mail or cron — none of that is in a `while` loop, and all of it is
  free here.
- **It's part of a fabric.** The reviewer can subscribe to `topic://pr-opened`,
  deposit findings in shared `memory`, or run on a pooled worker instead of your
  laptop. The shell loop is an island.

**The shell loop wins when:** you want one quick check, in one terminal, right
now, and you'll Ctrl-C it in five minutes. For that, the daemon is overhead.

The honest summary: for a *throwaway* check, reach for the loop. The moment you
want it to **persist, remember, survive, and be observable** — which is the
entire point of a standing agent — the loop starts reimplementing the daemon
badly, and Grimoire is the thing you'd otherwise build.

## Verified

This recipe's load-bearing claims were tested end-to-end on **2026-05-22** against
**Claude Code 2.1.148**, in an isolated `$HOME` with a webhook catcher recording
notifications:

- **Wakes on change.** A file edit fired the wake source ~1s later
  (`WakeSourceFired` in the event log, plus a `wake` webhook POST).
- **Reviews and notifies on real issues.** A planted off-by-one bug
  (`total / (len(nums) - 1)`) was diagnosed correctly — the agent also flagged
  the second-order `ZeroDivisionError` on single-element input — and surfaced via
  a `warn`-level `grim notify` POST.
- **Resumes the same session.** The agent's `session_id` was identical across the
  initial summon and both wakes — it resumed, it did not respawn.
- **Stayed quiet on a repeat.** On a second wake (a benign new file added, with
  the calc.py bug still present in `git diff`), it produced **no** new
  finding — session continuity was enough for it to recognize it had already
  raised that issue. This is observed behavior, not a daemon guarantee; a
  different prompt or model may re-report.

The same run was repeated with **pi 0.75.4** (`--provider pi`, gemini-3.5-flash)
to confirm provider-neutrality: the reviewer captured pi's session id, landed
`Dormant`, woke on the file edit, **resumed the same pi session** (id unchanged;
the persisted `~/.pi/agent/sessions/.../<id>.jsonl` was appended to), diagnosed
the planted bug, and fired `grim notify`. The standing-agent loop is not
Claude-specific.

## See also

- [`reference.md`](../reference.md) — full command, config, and subsystem reference
  (the [Demos](../reference.md#demos) and [Notifications](../reference.md#notifications)
  sections cover the pieces here).
- [`auth.md`](../auth.md) — the trust model, relevant once you expose the daemon
  beyond your own machine.
</content>
</invoke>
