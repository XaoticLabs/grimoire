# Plan: Agent-to-Agent Messaging Bus

> Drafted 2026-04-25
> Source: ROADMAP.md Part 3 §4 / Part 5 build-order #3

## Problem Statement

Today the only inter-agent bridge is a **pact**: a static, completion-triggered template that fires `{output}` from a finished agent into a freshly-summoned one. This is one-shot, one-way, and dead until the source agent terminates. There is no way for a live agent to ask another agent for data, hand off a subproblem, or wake an idle peer. There is no way for an agent to declare interest in a class of events ("any time a PR is opened") and be addressable later.

The daemon already has every primitive needed to fix this:
- A durable, append-only event log with per-stream sequence numbers (Part 2 §3).
- Stable agent identity (`AgentId`) tracked in `AgentManager`.
- Session-resume plumbing (`agent.invoke`) that can deliver a new prompt to a Complete agent with a `session_id`.
- A scheduler (Part 2 §2) that owns the transition from "we want this to run" to a running process.

What's missing is (a) the **address** abstraction — agents and topics as first-class destinations — and (b) the **delivery** path that turns a `send()` into either a wake (for a dormant/Complete agent) or an in-context message (for a live agent on its next turn). The roadmap calls this "a tiny delta over the [event] log" and it is — but only if we resist the urge to invent a new transport. Messages are events with a destination.

### Who experiences this?

- **The "standing review team" demo** (Part 4): three dormant reviewer agents subscribed to `topic://pr-opened`. Today there is no `subscribe` and no wake-on-publish.
- **The "swarm decompose" demo** (Part 4): a parent agent that spawns five children and then needs to ping them with refined sub-tasks mid-run. Today the parent's only lever is to wait for completion and read `{output}`.
- **The roadmap itself** — item #5 (dormant agents with wake triggers) is largely "messaging bus + subscriptions persisted across restarts." Build the bus right and #5 is mostly schema.

### Why now?

Items #1, #2, #4 are done. The event log gives us durable delivery for free; the scheduler gives us a single admission point for waking dormant agents without re-inventing dispatch. Building #3 next means #5 is downhill.

### Current workarounds

- Pacts (one-shot, completion-triggered, template-based).
- `grim invoke <id> <message>` from the CLI — works for human-driven re-prompting but is not callable from an agent's own runtime.
- Scrolls — a static DAG, not dynamic peer messaging.

## Goals

- A first-class **address** scheme: `agent://<id>` for direct messages, `topic://<name>` for pub/sub.
- A `mail` table in SQLite: durable, ordered, per-recipient, with delivery state (`Pending | Delivered | Failed`).
- A new RPC surface: `mail.send`, `mail.subscribe`, `mail.unsubscribe`, `mail.list`, `mail.ack`. Both human (`grim mail …`) and agent (in-process tool) callers use the same RPC.
- Delivery semantics:
  - **Live recipient** (Active/Summoning): message is queued in `mail` and surfaced as a `MailReceived` event on the agent's stream. It is the agent's responsibility (via wrapper / next turn) to read pending mail; v1 does not interrupt a running provider call.
  - **Dormant recipient** (Complete with `session_id`): the scheduler treats unread mail as a wake trigger. It calls the existing `agent.invoke` path with the message body as the resume prompt, transitioning the agent back to Active.
  - **Unknown / banished recipient**: `Failed`, surfaced via `MailDelivery` event; sender sees the error in their stream too.
- Topic delivery: `topic://X` fans out a single send into one `mail` row per current subscriber. Subscriptions are durable rows in a `subscriptions` table; they survive daemon restart.
- A new `grim mail` CLI: `send <addr> <body>`, `list <agent-id>`, `subscribe <agent-id> <topic>`, `topics`.
- Observability: `MailSent`, `MailDelivered`, `MailFailed` lifecycle events on both sender and recipient streams, written through the existing EventBus so `bind` / dashboard pick them up for free.
- Pacts are unchanged. Internally, a pact firing becomes "send `{output}` to a freshly-summoned agent's mailbox at spawn time" — but that refactor is optional and out of scope for v1.

## Non-Goals (Explicit Scope Boundaries)

- **Mid-turn delivery / SIGUSR1-style interruption of a running provider call.** v2. v1 surfaces mail at turn boundaries (live agents) or via wake (dormant agents).
- **A new in-agent "tool" / SDK for `recv()`.** v1 exposes mail via the existing event stream and via RPC; an agent reads its own mailbox by calling `mail.list` (or by subscribing to its own stream and watching for `MailReceived`). A first-class tool API for Claude/Codex providers is a follow-up.
- **Cross-daemon / federated topics.** Roadmap item #11.
- **Topic ACLs, tenant isolation, message-level encryption.** Hardening milestone, not v1.
- **Message TTL, dead-letter queue, retries with backoff.** v1 stores Pending forever; manual `mail.ack` or `mail.purge` removes them.
- **Wildcard / hierarchical topics** (`topic://reviews/*`). Exact-match strings only in v1.
- **Request/reply correlation IDs and ask-pattern helpers.** v2 — easy to add later via an optional `in_reply_to` column we put in the schema now.
- **Postgres / NATS backend.** SQLite-only, same posture as the queue and event log.

## Proposed Solution

### Conceptual Overview

A **mailbox is a durable queue of events with a recipient address.** The bus is two SQLite tables (`mail`, `subscriptions`), one new RPC namespace (`mail.*`), one CLI command (`grim mail`), and one new branch in the scheduler tick: "promote dormant agents that have unread wake-eligible mail."

`mail.send` is the only write path. It:
1. Resolves the destination. `agent://<id>` → one row. `topic://<name>` → one row per current subscriber (snapshot at send time; late subscribers do not retro-receive).
2. Inserts `mail` rows with `state = Pending`.
3. Publishes `MailSent` on the sender's stream and `MailReceived` on each recipient's stream (durable via EventBus → events table, same as today).
4. For dormant recipients with a session, signals the scheduler. The scheduler is already event-driven; we add `MailReceived` to its wake set and add a placement rule "agent in Complete state with pending wake-eligible mail is dispatch-eligible." Wake re-uses `agent_manager.invoke()` with the mail body as the resume prompt.

`mail.list` is a simple read with a cursor (per-agent `seq`, mirroring the events table pattern).

`mail.ack` flips `Pending → Delivered` and emits `MailDelivered`. v1: agents are expected to ack after consuming; failure to ack does not redeliver — it just means the mailbox grows. The schema supports redelivery semantics later without migration.

### User Journey

**Direct message between two live agents (parent → child mid-run):**
1. Parent agent's wrapper script calls `grim mail send agent://<child-id> "switch to redis backend"`.
2. Daemon writes one `mail` row, emits `MailSent` to parent's stream, `MailReceived` to child's stream.
3. Child agent's wrapper, on its next turn boundary, calls `grim mail list <self-id> --pending` and folds new mail into its prompt.
4. Child calls `grim mail ack <mail-id>`; daemon emits `MailDelivered`.

**Topic + dormant subscriber (review team demo):**
1. At setup: `grim mail subscribe agent://<reviewer-1-id> topic://pr-opened`. Subscription row persisted.
2. Reviewer-1 finishes its first task and reaches `Complete` with a `session_id`. It stays registered.
3. Hours later, a CI hook fires `grim mail send topic://pr-opened "PR #482 opened: …"`.
4. Daemon snapshots subscribers, inserts one `mail` row per reviewer. Emits `MailReceived` on each.
5. Scheduler tick sees Complete agents with Pending wake-eligible mail. For each, it invokes `agent_manager.invoke(id, mail_body)` — which already exists. Agents transition Complete → Summoning → Active; the existing pipeline takes over.

**Send to a banished or unknown agent:**
1. `mail.send agent://deadbeef "hi"`.
2. Daemon resolves: not found / banished. Inserts a `mail` row with `state = Failed`, emits `MailFailed` on sender's stream with reason. No retry.

### Schema (additions only — no migrations to existing tables)

```sql
CREATE TABLE IF NOT EXISTS mail (
  id              TEXT PRIMARY KEY,         -- short id, same generator as agents
  recipient_id    TEXT NOT NULL,            -- AgentId; resolved at send time even for topic fanout
  sender_id       TEXT,                     -- AgentId or NULL for human / external
  topic           TEXT,                     -- NULL for direct send; otherwise the topic the row was fanned from
  body            TEXT NOT NULL,
  in_reply_to     TEXT,                     -- reserved for v2 ask-pattern; NULL in v1
  state           TEXT NOT NULL,            -- 'Pending' | 'Delivered' | 'Failed'
  fail_reason     TEXT,
  created_at      INTEGER NOT NULL,
  delivered_at    INTEGER,
  seq             INTEGER NOT NULL,         -- per-recipient monotonic, like events.seq
  wake_eligible   INTEGER NOT NULL DEFAULT 1 -- 0 = "do not wake a dormant recipient"
);
CREATE INDEX IF NOT EXISTS mail_by_recipient ON mail (recipient_id, seq);
CREATE INDEX IF NOT EXISTS mail_pending_wake ON mail (recipient_id, state) WHERE state = 'Pending' AND wake_eligible = 1;

CREATE TABLE IF NOT EXISTS subscriptions (
  id              TEXT PRIMARY KEY,
  subscriber_id   TEXT NOT NULL,            -- AgentId
  topic           TEXT NOT NULL,
  created_at      INTEGER NOT NULL,
  UNIQUE (subscriber_id, topic)
);
CREATE INDEX IF NOT EXISTS subs_by_topic ON subscriptions (topic);
```

### RPC Surface

Add to `src/shared/protocol.rs` and dispatch in `src/daemon/rpc.rs`:

- `mail.send { to: String, body: String, sender?: AgentId, wake_eligible?: bool }` → `{ delivered: u32, mail_ids: [String] }`.
- `mail.list { agent_id: AgentId, after_seq?: u64, state?: "Pending"|"Delivered"|"Failed", limit?: u32 }` → `[Mail]`.
- `mail.ack { mail_id: String }` → `{}`.
- `mail.subscribe { agent_id: AgentId, topic: String }` → `{ subscription_id: String }`.
- `mail.unsubscribe { subscription_id: String }` → `{}`.
- `mail.topics {}` → `[{ topic: String, subscriber_count: u32 }]`.

### Scheduler Integration

`src/daemon/scheduler.rs` already wakes on `AgentQueued`, `WorkerRegistered`, terminal `StateChange`, plus a 100ms safety tick. Add:

- Wake on `MailReceived`.
- New eligibility branch in the tick: scan `mail` for `state = Pending AND wake_eligible = 1` grouped by `recipient_id`; for each recipient currently in `Complete` with a `session_id`, attempt `agent_manager.invoke(id, body)`. The existing global-cap and placement checks gate this exactly the same way they gate `Queued → Active`. Fold consecutive Pending mail into one wake (concatenate, capped) so a flood of topic publishes doesn't spawn N invocations of the same agent.

### CLI Surface

`src/cli/commands/mail.rs` (new), wired in `src/cli/commands/mod.rs`:

- `grim mail send <addr> <body...>`
- `grim mail list <agent-id> [--pending|--all] [--after <seq>]`
- `grim mail ack <mail-id>`
- `grim mail subscribe <agent-id> <topic>`
- `grim mail unsubscribe <subscription-id>`
- `grim mail topics`

Formatter follows the existing `circle` / `queue` table conventions.

## Risks & Open Questions

- **Wake storm on a hot topic.** A topic with N subscribers and a publish rate higher than agents can drain will pile up Pending mail and spawn invocations every tick. Mitigation in v1: the "fold consecutive mail into one wake" rule + the existing global cap. Real fix (per-recipient rate limit, debounce window) is v2.
- **Address parsing & validation.** `agent://` and `topic://` only. Reject any other scheme loudly so we don't paint ourselves into a corner when federation lands (`grimd://host/agent/<id>`).
- **What does an agent do with `MailReceived` mid-turn?** v1 answer: nothing automatic. The agent's wrapper is responsible for checking on turn boundaries. Documented limitation; the demo scripts in Part 4 will need that wrapper. Right answer long-term is provider-side tool integration — explicitly out of scope.
- **Pact / mail overlap.** Tempting to refactor pacts onto mail in v1; recommend not. Pacts are stable and tested; touching them adds risk for no user-visible win. Bridge in v2 once mail has soaked.
- **Sender authentication.** v1 accepts any `sender` claim from the RPC caller (no auth on UDS). Same posture as the rest of the daemon. Real auth is the hardening milestone.
- **`mail.list` from a *running* agent's own wrapper.** The wrapper uses the same UDS RPC the CLI does — no special transport. Confirm in the spec that `agent_id` in `mail.list` does not need to match the caller in v1 (no auth anyway).

## Build Order (proposed task breakdown)

1. Schema migration (additive `CREATE TABLE IF NOT EXISTS`) + persistence helpers (`insert_mail`, `list_mail_by_recipient`, `set_mail_state`, `insert_subscription`, `list_subscribers_for_topic`, `delete_subscription`). Tests: `tests/database.rs` extension.
2. Address parser + `mail.send` RPC (direct addresses only, no topics yet, no wake). EventBus emissions. Tests: `tests/protocol.rs` extension + new `tests/mail_send.rs`.
3. `mail.list` + `mail.ack` RPC + CLI `grim mail send|list|ack`. Tests: `tests/cli_mail.rs`.
4. Topic fanout in `mail.send` + `mail.subscribe` / `mail.unsubscribe` / `mail.topics` RPC + CLI. Tests: subscription persistence across restart, fanout snapshot semantics.
5. Scheduler wake-on-mail branch — fold + invoke path, gated by global cap & placement. Tests: `tests/scheduler_mail_wake.rs` (dormant agent receives mail → goes Active without a `summon`).
6. Documentation pass: README "messaging" section, ROADMAP.md check off Part 5 #3.

Each step is independently testable and shippable. Steps 1–3 are "human-driven mail" (useful even before subscriptions). Steps 4–5 are "the demo" (review team works end-to-end after #5 lands).
