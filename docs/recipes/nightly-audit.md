# Recipe: a nightly audit agent

An auditor that wakes at 3am, runs your checks (tests, lints, dependency
audit, whatever you give it), files what it finds in the event log, and
pings you only if something is red. You read the verdict over coffee; the
full transcript is always one `grim chronicle` away.

Where the [standing review agent](standing-review-agent.md) reacts to file
changes, this one runs on a clock. Same three primitives, different wake
source: `summon --keep-alive` + a cron wake + `grim notify`.

## Setup

```bash
cd ~/repos/myapp

grim summon --keep-alive --provider claude --name nightly-audit \
  "You are a standing audit agent running under Grimoire. Each time you wake:
   run the test suite and linter for this repository, check for new dependency
   advisories, and summarize what changed since your last wake (git log).
   If anything fails or looks risky, surface it with:
     grim notify \"<one-line finding>\" --level error
   If everything is green, end your turn with a one-line all-clear summary.
   Do not modify any files."

# It runs once, then parks in Dormant. Give it the clock:
grim wake add <id> --cron "0 3 * * *"
```

Any command that takes an agent id takes a prefix, so `grim wake add 4a` works.

## Getting the verdict

Configure any sink in `~/.grimoire/config.toml` — the webhook works with
Slack/Discord out of the box:

```toml
[notifications]
webhook_url = "https://hooks.slack.com/services/…"
on_agent_decided = true   # the agent called `grim notify`
on_failure = true         # the agent itself crashed / failed
```

`on_failure` is the supervisor watching the watcher: if the audit agent
dies, you hear about that too.

## Reviewing a run

```bash
grim chronicle nightly-audit          # full life: every wake, every run, every notify
grim chronicle nightly-audit --kinds wake_source_fired,notification
grim eval <id> --rubric audit-rubric.md   # score how well it's doing its job
```

Because the agent resumes its own session on each wake (native resume for
`claude`/`pi`, transcript replay for any other CLI), it remembers what it
reported yesterday — "the flaky test from Tuesday is still flaky" is a
sentence it can actually say.

## Variations

- **Quieter:** drop `on_agent_decided`, keep `log_file` — findings append
  to a JSONL file you grep when you care.
- **Weekly deep audit:** second agent, `--cron "0 6 * * 1"`, brief it to
  read the week of nightly findings first (`grim chronicle nightly-audit`).
- **Chain a fixer:** `grim wake add <fixer-id> --on-parent <audit-id>`
  wakes a second agent every time the audit finishes; brief the fixer to
  read the audit's findings and open a branch with fixes.
