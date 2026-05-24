# Recipe: a standing review agent

A reviewer that lives in your repo, wakes on a file change, reads the diff, and pings you only when a human should look. It doesn't exit. Set it up once, walk away.

This is the canonical Grimoire loop. Three primitives composed: `summon --keep-alive` (a standing agent), a file-watch wake source, and `grim notify` (the agent reaching back out). This is not a unique case, you can build it by hand as described below.

## The one-liner

```bash
grim demo standing-review --repo ~/repos/myapp --provider claude
```

Prints each underlying `grim` action as it runs. Edit a file and it wakes.

`--repo` defaults to the current directory.

**Provider note.** Standing agents need a CLI that can resume a session. On wake, the daemon resumes the existing session so the agent carries what it already saw. Supported today:

- **Claude** (`claude --resume`). Verified live.
- **pi** (`pi --session <id>`). Verified live.

## Driving it

```bash
# See it think:
grim bind <id>          # stream its output
grim scry               # or watch in the dashboard

# Send the pings somewhere, any combination in ~/.grimoire/config.toml:
[notifications]
webhook_url = "https://hooks.slack.com/services/…"   # Slack/Discord/any JSON POST
log_file    = "/var/log/grimoire-notify.jsonl"       # append-only JSON Lines
desktop     = true                                    # notify-send toast (Linux)
on_agent_decided = true                              # fire when the agent calls `grim notify`

# Tear it down (cascades the wake source):
grim banish <id>
```

With no sink configured, findings still land in `grim bind` and the event log. For a fully offline setup, `log_file` + `desktop = true` is enough: toast on each finding, JSONL trail for audit.

## The brief

Narrow, read-only, provider-neutral:

> You are a standing code reviewer running under Grimoire. You wake whenever a
> file changes in this repository. On each wake: inspect the most recent changes
> (`git diff`, `git status`). IF you find something a human should know
> about (a likely bug, a risky change, a failing test, a security issue),
> surface it with `grim notify "<short finding>" --level warn`. If nothing is
> noteworthy, stay quiet. Be terse. Do NOT modify any files.

The daemon injects `GRIMOIRE_AGENT_ID` into every spawned agent, so `grim notify` is auto-attributed.

## Building it by hand

The demo is this, with prettier output:

```bash
# 1. Summon a keep-alive reviewer rooted in the repo. --keep-alive lands it in
#    Dormant after the first run instead of exiting.
grim summon --keep-alive --cwd ~/repos/myapp \
  "You are a standing code reviewer… (the brief above)"
# → reviewer summoned: 4a8c1b2f

# 2. Wake it on any file change under the repo. Debounced; ignores noise.
grim wake add 4a --file-watch ~/repos/myapp
# → file-watch wake source registered

# 3. (optional) point notifications at a webhook, see config above.
```

Edit a file. The watcher debounces (200 ms), batches changed paths, fires the wake source. The dormant agent resumes with the same session (it remembers what it already looked at), runs `git diff`, decides, and either calls `grim notify` or goes back to `Dormant`.

`.git/`, `target/`, and `node_modules/` are ignored by default so a build or a commit doesn't wake it.

## Is this actually better than a shell loop?

You can approximate this with a bash loop on claude -p or other harness streaming at the basic level. This is really a proof of concept showing the agent as a process.

**The daemon earns its keep when:**

- **Continuity matters.** The standing agent resumes its session on each wake, so it carries what it already flagged; a fresh `claude -p` starts from zero every loop. (Daemon guarantees the resume; the model still has to act on it. Requires a resume-capable provider, see the note above.)
- **You want it to outlive the terminal.** Close the laptop, the shell loop dies. The daemon keeps the agent dormant and ready; it survives daemon restarts (state in SQLite, in-flight agents reconciled on reboot).
- **You're running more than one.** Restart policy. Rate limiting so a rebase storm doesn't fire it 400 times. A durable event log of every wake and finding. One dashboard across all of them. The same agent reachable by mail or cron. None of that is in a `while` loop; all of it is free here.
- **It's part of a fabric.** The reviewer can subscribe to `topic://pr-opened`, deposit findings in shared `memory`, or run on a pooled worker. The shell loop is an island.

**The shell loop wins when:** you want one quick check, in one terminal, right now, and you'll Ctrl-C it in five minutes. For that, the daemon is overhead.

The moment you want it to **persist, remember, survive, and be observable** (the point of a standing agent), the loop starts reimplementing the daemon badly, and Grimoire is the thing you'd otherwise build.

## Verified

Tested end-to-end on **2026-05-22** against **Claude Code 2.1.148**, in an isolated `$HOME` with a webhook catcher recording notifications:

- **Wakes on change.** A file edit fired the wake source ~1s later (`WakeSourceFired` in the event log, plus a `wake` webhook POST).
- **Reviews and notifies on real issues.** A planted off-by-one (`total / (len(nums) - 1)`) was diagnosed correctly. The agent also flagged the second-order `ZeroDivisionError` on single-element input, and surfaced both via a `warn`-level `grim notify` POST.
- **Resumes the same session.** The `session_id` was identical across the initial summon and both wakes. It resumed, it did not respawn.
- **Stayed quiet on a repeat.** On a second wake (benign new file, calc.py bug still in `git diff`), no new finding fired because session continuity was enough. Observed, not guaranteed; a different prompt or model may re-report.

Repeated against **pi 0.75.4** (`--provider pi`, gemini-3.5-flash) to confirm provider-neutrality: captured pi's session id, landed `Dormant`, woke on the edit, **resumed the same pi session** (id unchanged; `~/.pi/agent/sessions/.../<id>.jsonl` appended to), diagnosed the bug, fired `grim notify`. The loop isn't Claude-specific.
