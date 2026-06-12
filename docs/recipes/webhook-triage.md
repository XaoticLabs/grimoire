# Recipe: webhook-triggered triage

A triage agent that sleeps until the outside world POSTs something — a
Sentry alert, a CI failure, a GitHub event — then wakes with the payload
as its prompt, investigates, and reports back. No polling, no cron: the
event itself is the wake.

The flow is: `POST /webhooks/<name>` → mail on a topic → any dormant agent
subscribed to that topic wakes with the body as its prompt.

## Setup

Declare the webhook in `~/.grimoire/config.toml`:

```toml
[webhooks.alerts]
topic = "alerts"     # request body becomes mail on topic://alerts
secret = "long-random-string"
```

Summon the triage agent and subscribe it:

```bash
cd ~/repos/myapp

grim summon --keep-alive --provider claude --name triage \
  "You are a standing triage agent running under Grimoire. You wake when an
   alert payload arrives as mail. On each wake: parse the payload, find the
   relevant code, and decide severity. If it needs a human now, run:
     grim notify \"<what broke and where to look>\" --level error
   Otherwise reply with a short diagnosis and likely-cause file:line.
   Do not modify any files."

grim mail subscribe <id> alerts
```

Point the alert source at the daemon. From the source's side it's a plain
JSON POST:

```bash
curl -X POST \
  -H "X-Grimoire-Webhook-Token: long-random-string" \
  -d '{"error":"TypeError: cannot read properties of undefined","culprit":"src/auth/session.ts"}' \
  http://127.0.0.1:6660/webhooks/alerts
```

The daemon binds `127.0.0.1` by default. For real external services,
front it with a reverse proxy that terminates the provider's own auth
(GitHub HMAC, Sentry signature) and injects the daemon-side token — the
daemon deliberately doesn't reimplement per-provider signature schemes.

## What you get

- **Severity-gated pings.** The agent reads every alert; you read only
  the ones it escalates via `grim notify`.
- **A durable paper trail.** Every payload, wake, and diagnosis is in the
  event log: `grim chronicle triage` replays the whole on-call shift.
- **Context across alerts.** The agent resumes its session on each wake,
  so "this is the third time this endpoint has thrown today" comes free.

## Variations

- **Per-source agents:** declare `[webhooks.sentry]` and `[webhooks.ci]`
  with different topics, and give each its own specialist agent.
- **Direct delivery:** set `recipient = "<agent-id>"` instead of `topic`
  to hard-wire a webhook to one agent, skipping pub/sub.
- **Triage → fix chain:** `grim wake add <fixer-id> --on-complete <triage-id>`
  hands confirmed diagnoses to a second agent allowed to write code.
