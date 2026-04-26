# Implementation Spec: Agent Messaging Bus

> Generated from: `.claude/plans/agent-messaging-bus.md`
> Generated on: 2026-04-25

## Overview

A durable agent-to-agent messaging bus built on the existing event log and scheduler. Two new SQLite tables (`mail`, `subscriptions`), one new RPC namespace (`mail.*`), one new CLI command (`grim mail`), and one new branch in the scheduler tick that wakes dormant agents on incoming mail. Addresses are first-class: `agent://<id>` for direct send, `topic://<name>` for pub/sub fanout. Live recipients see a `MailReceived` event on their stream and consume via `mail.list` / `mail.ack`; dormant recipients with a `session_id` are woken through the existing `agent_manager.invoke()` path. Pacts are unchanged — this is additive.

The motivation is the roadmap's "standing review team" and "swarm decompose" demos. Today the only inter-agent bridge is a one-shot, completion-triggered pact. After this spec lands, agents can directly message each other mid-run, subscribe to topics that survive daemon restart, and be woken from `Complete` by a topic publish.

## Technical Context

### Relevant Codebase Areas

- `src/daemon/persistence.rs` — SQLite schema, migrations (`migrate()` at lines 79-205), and helper-fn pattern (e.g., `insert_pact` lines 410-425). Schema is additive and applied on every `Database::open()`.
- `src/daemon/event_bus.rs` — `EventBus::publish(StreamEvent)` (lines 30-35). Fire-and-forget broadcast + background mpsc writer that calls `db.append_event()`.
- `src/daemon/scheduler.rs` — `tick_now()` dispatch loop (lines 72-156); `should_wake()` wake set (lines 203-210); 100ms safety tick (line 30). Capacity check at lines 75-98; eligibility check at 103-116.
- `src/daemon/agent_manager.rs` — `invoke(&self, id, message, model) -> Result<()>` (lines 346-439) is the resume path that flips a `Complete` agent with a `session_id` back to `Active`. Reused verbatim by mail-wake.
- `src/daemon/rpc.rs` — flat dotted method names (`agent.summon`, `pact.create`, `agent.queue.list`, …) dispatched by `match req.method.as_str()` (lines 22-37). Each handler is a top-level `async fn`.
- `src/shared/protocol.rs` — `RpcRequest`/`RpcResponse` shape; `StreamEvent` enum (lines 187-232); `InvokeParams` (lines 77-80) is the pattern for new param structs.
- `src/cli/commands/queue.rs` — minimal CLI command pattern (calls `client.call("agent.queue.list", json!({}))` then formats). New `grim mail` follows this exactly.
- `src/cli/commands/mod.rs` + `src/main.rs` lines 14-140, 177-230 — clap subcommand wiring.
- `tests/scheduler_integration.rs` — `Harness` struct (lines 99-142) assembles full stack against an in-memory DB and a `ControlledExecutor`. Reused for mail-wake integration tests.
- `tests/database.rs` — `test_db()` (lines 15-17) and `make_agent()` (lines 48-64) helpers for persistence tests.

### Existing Patterns to Follow

- **Schema migration**: append a `CREATE TABLE IF NOT EXISTS … ;` block to the single `execute_batch()` call inside `migrate()`. Indexes use `CREATE INDEX IF NOT EXISTS`. No dedicated migration tooling.
- **Per-recipient `seq`**: `events.seq` is computed transactionally as `SELECT COALESCE(MAX(seq) + 1, 0)` scoped to the recipient (`persistence.rs` lines 212-246, `transaction_with_behavior(Immediate)`). Mail rows replicate this pattern per `recipient_id`.
- **Short ID generation**: `src/shared/constants.rs:25` — `Uuid::new_v4().to_string()[..8]`. Use the same helper for `mail.id` and `subscription.id`.
- **RPC namespacing**: dotted, flat. New methods: `mail.send`, `mail.list`, `mail.ack`, `mail.subscribe`, `mail.unsubscribe`, `mail.topics`.
- **State enums**: `impl_state_enum!` macro in `src/shared/types.rs:10-37` gives `as_str()` / `Display` / `FromStr`. `MailState` follows the same pattern.
- **EventBus emit-once**: every state-relevant change publishes a single `StreamEvent` variant. New variants: `MailSent`, `MailReceived`, `MailDelivered`, `MailFailed`.

### Key Dependencies

- `Database` (`persistence.rs`) — owns the connection pool; new helpers are methods on `Database`.
- `EventBus` (`event_bus.rs`) — only durable write path for `StreamEvent`; mail emissions go through it (no direct events-table writes from the mail layer).
- `AgentManager::invoke()` — reused unchanged by the scheduler's mail-wake branch.
- `Scheduler::tick_now()` — gains a mail-wake branch and a new entry in its wake set.
- `DaemonClient` (`src/cli/client.rs`) — RPC client used by every CLI command; `grim mail` calls `client.call("mail.*", …)` with no client-side changes.

### Ambiguity Resolutions

| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| Mail ID format | Plan says "short id, same generator as agents" | Reuse the 8-char UUID helper in `src/shared/constants.rs:25` for both `mail.id` and `subscription.id` | Codebase convention |
| Body size cap (single send) | Unspecified | Reject `mail.send` with bodies > 64 KiB; return `RpcError` with code `body_too_large` | Assumed default |
| Wake-fold body cap | Plan says "concatenate, capped" — cap unspecified | When folding N pending mail rows into one resume prompt, join with `\n\n---\n\n` and truncate the joined body to 16 KiB (drop overflow with a trailing `[... N more messages truncated]` note) | Assumed default |
| Subscribe idempotency | UNIQUE (subscriber_id, topic) constraint, behavior on duplicate undefined | `mail.subscribe` is idempotent: on duplicate it returns the existing `subscription_id` with no error | Assumed default |
| `mail.list` default `limit` | Unspecified | Default 100, max 1000; reject larger values with `RpcError` | Assumed default |
| `mail.list` `state` filter | Unspecified when omitted | Omitted = all states; explicit value filters | Assumed default |
| `mail.ack` on already-Delivered | Unspecified | No-op success (idempotent); does NOT re-emit `MailDelivered` | Assumed default |
| `mail.ack` on Failed mail | Unspecified | Return `RpcError` `cannot_ack_failed` (failed mail is removed via `mail.purge` in v2; v1 leaves it) | Assumed default |
| `mail.send` to Banished/unknown | Plan: insert `Failed` row + emit `MailFailed` | Same row inserted to `mail` table with `state='Failed'`, `fail_reason` set; sender stream gets `MailFailed`; recipient stream gets nothing | Plan |
| `mail.send` to `Queued` agent | Unspecified | Allowed: row inserted as `Pending`. Recipient sees mail when it transitions to `Active` and runs its wrapper. No wake (agent is already on the dispatch path). | Assumed default |
| Topic name validation | Plan says "exact-match strings" | Topic names must match `^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$`. Reject others with `invalid_topic_name`. | Assumed default |
| Empty topic publish | Unspecified | `mail.send` to a topic with 0 subscribers returns `{ delivered: 0, mail_ids: [] }` and emits a single `MailSent` on sender's stream. No `MailFailed`. | Assumed default |
| Wake-fold scope | Plan: "fold consecutive Pending wake-eligible mail" | Scope is per-recipient: every `Pending && wake_eligible=1` row not yet folded into a wake. Topic and direct mail are folded together in `seq` order. | Assumed default |
| `wake_eligible=0` semantics | Unspecified for live recipients | Has no effect on live recipients (still emits `MailReceived` and writes the row); only suppresses scheduler wake for dormant recipients. | Assumed default |
| Sender of unknown agent ID | RPC caller passes `sender: agent://deadbeef` | v1 does not validate the sender identity (no auth on UDS). Persisted as-is. Documented limitation. | Plan |
| `mail.list` from running agent's wrapper | Plan flags as open question | v1: any caller may call `mail.list` for any `agent_id` (no auth on UDS). Documented. | Plan |
| `mail.subscribe` for Banished agent | Unspecified | Allowed at write time. The wake branch later skips Banished recipients (they cannot transition to Active). | Assumed default |
| Address parser strictness | Plan: reject any non-`agent://`/`topic://` scheme | Parser returns `RpcError` `invalid_address` for any other scheme, including bare strings without `://` | Plan |
| Mail-wake re-entry on `invoke()` failure | Unspecified | If `agent_manager.invoke()` errors (e.g., agent already Active by race), leave mail rows as `Pending`; next tick retries. No retry-count cap in v1. | Assumed default |
| `MailReceived` for failed delivery | Unspecified | Failed mail does NOT emit `MailReceived` (recipient never sees it); only `MailFailed` on the sender stream. | Plan |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | Mail/subscription schema and persistence helpers | None | Medium |
| 2 | Address parser, StreamEvent variants, and `mail.send` (direct only) | 1 | Medium |
| 3 | `mail.list` + `mail.ack` RPC and `grim mail send/list/ack` CLI | 2 | Low |
| 4 | Topic fanout in `mail.send` plus `mail.subscribe` / `mail.unsubscribe` / `mail.topics` RPC and CLI | 3 | Medium |
| 5 | Scheduler wake-on-mail branch | 4 | Medium |
| 6 | Documentation pass (README + ROADMAP) | 5 | Low |

### Critical Path

Strict linear dependency: 1 → 2 → 3 → 4 → 5 → 6.

Tasks 1–3 are "human-driven mail" (useful in isolation: a developer can `grim mail send agent://X "hello"` and inspect via `grim mail list X` even before topics or wake exist). Tasks 4–5 unlock the review-team demo. Task 6 is the docs sweep.

No parallelization opportunities in v1 — every task touches state introduced by the prior one. Do not split task 4 into "subscribe RPC" and "topic fanout" — fanout is only meaningful once `subscriptions` writes are durable in the same task; otherwise integration tests can't verify the snapshot semantic.

---

### Task 1: Mail/subscription schema and persistence helpers

**Summary:** Add `mail` and `subscriptions` tables to the SQLite migration and expose CRUD helpers on `Database`.

**Dependencies:** None

**Files to create/modify:**
- `src/daemon/persistence.rs` — extend `migrate()` with two new `CREATE TABLE IF NOT EXISTS` blocks; add helper methods.
- `src/shared/types.rs` — add `Mail`, `MailState`, `Subscription` structs with `Serialize`/`Deserialize`. Use `impl_state_enum!` for `MailState { Pending, Delivered, Failed }`.
- `tests/database.rs` — extend with mail/subscription tests.

**Detailed specification:**

Schema (verbatim from plan, append to the `execute_batch` block in `migrate()`):

```sql
CREATE TABLE IF NOT EXISTS mail (
  id              TEXT PRIMARY KEY,
  recipient_id    TEXT NOT NULL,
  sender_id       TEXT,
  topic           TEXT,
  body            TEXT NOT NULL,
  in_reply_to     TEXT,
  state           TEXT NOT NULL,
  fail_reason     TEXT,
  created_at      INTEGER NOT NULL,
  delivered_at    INTEGER,
  seq             INTEGER NOT NULL,
  wake_eligible   INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS mail_by_recipient ON mail (recipient_id, seq);
CREATE INDEX IF NOT EXISTS mail_pending_wake ON mail (recipient_id, state) WHERE state = 'Pending' AND wake_eligible = 1;

CREATE TABLE IF NOT EXISTS subscriptions (
  id              TEXT PRIMARY KEY,
  subscriber_id   TEXT NOT NULL,
  topic           TEXT NOT NULL,
  created_at      INTEGER NOT NULL,
  UNIQUE (subscriber_id, topic)
);
CREATE INDEX IF NOT EXISTS subs_by_topic ON subscriptions (topic);
```

Helper methods on `Database`:

- `insert_mail(&self, mail: &Mail) -> Result<()>` — inserts inside `transaction_with_behavior(Immediate)`. Computes `seq` as `SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?` inside the transaction. Returns `Err` if `recipient_id` is empty.
- `list_mail_by_recipient(&self, recipient_id: &str, after_seq: Option<u64>, state_filter: Option<MailState>, limit: u32) -> Result<Vec<Mail>>` — clamps `limit` to `[1, 1000]`. Returns rows ordered by `seq ASC`.
- `get_mail(&self, id: &str) -> Result<Option<Mail>>`.
- `set_mail_state(&self, id: &str, new_state: MailState, fail_reason: Option<&str>) -> Result<()>` — sets `delivered_at = unix_now()` when transitioning to `Delivered` or `Failed`. Returns `Err` if mail not found.
- `list_pending_wake_eligible(&self, recipient_id: &str) -> Result<Vec<Mail>>` — selects only `state='Pending' AND wake_eligible=1`, ordered by `seq ASC`.
- `insert_subscription(&self, sub: &Subscription) -> Result<()>` — on UNIQUE conflict, returns the existing subscription's `id` via a follow-up SELECT (idempotent). Add unit test for the duplicate path.
- `delete_subscription(&self, id: &str) -> Result<bool>` — returns true if a row was removed.
- `list_subscribers_for_topic(&self, topic: &str) -> Result<Vec<Subscription>>` — used by topic fanout in task 4.
- `list_subscriptions_by_subscriber(&self, agent_id: &str) -> Result<Vec<Subscription>>`.
- `list_topics_with_counts(&self) -> Result<Vec<(String, u32)>>` — used by `mail.topics` in task 4. Group by topic, count subscribers.

Body length validation belongs at the RPC layer (task 2), not in `insert_mail` — persistence is a thin SQL helper.

**Edge cases to handle:**
- New install vs existing DB: migration is idempotent via `IF NOT EXISTS`.
- Concurrent inserts to the same `recipient_id`: serialized by `transaction_with_behavior(Immediate)` (already the codebase pattern).
- Empty `recipient_id`: rejected at helper level.
- `state` column round-trip: `MailState::from_str` must accept the exact strings `"Pending"`, `"Delivered"`, `"Failed"` written by the helpers.

**Acceptance criteria:**
- [ ] After `Database::open_in_memory()`, `pragma table_info('mail')` returns the 12 columns from the schema in declared order.
- [ ] After `Database::open_in_memory()`, `pragma table_info('subscriptions')` returns the 4 columns from the schema in declared order.
- [ ] Indexes `mail_by_recipient`, `mail_pending_wake`, `subs_by_topic` exist (queryable via `sqlite_master`).
- [ ] `insert_mail` for two mails with the same `recipient_id` produces `seq` values 0 then 1.
- [ ] `insert_mail` for two mails with different `recipient_id` produces `seq=0` for each.
- [ ] `list_mail_by_recipient` with `after_seq=0` excludes the row with `seq=0` and includes rows with `seq>=1`.
- [ ] `list_mail_by_recipient` with `state_filter=Pending` excludes rows in `Delivered` and `Failed`.
- [ ] `list_mail_by_recipient` with `limit=2000` is clamped: returns at most 1000 rows.
- [ ] `set_mail_state(id, Delivered, None)` sets `delivered_at` to a non-NULL value.
- [ ] `set_mail_state(id, Failed, Some("banished"))` sets both `state='Failed'` and `fail_reason='banished'`.
- [ ] `insert_subscription` for the same `(subscriber_id, topic)` twice returns the same `subscription_id` both times (no error).
- [ ] `delete_subscription` returns `true` for an existing id, `false` for a missing id.
- [ ] `list_subscribers_for_topic("pr-opened")` returns only rows with that exact topic.
- [ ] Migration is re-runnable: calling `migrate()` twice on the same DB produces no error and no schema change.

**Contract tests (RED phase):**
- Test file: `tests/database.rs`
- Tests to write before implementing:
  - `mail_table_schema_matches_spec` — asserts columns + indexes
  - `subscriptions_table_schema_matches_spec` — asserts columns + indexes
  - `insert_mail_assigns_per_recipient_seq` — asserts `seq` 0→1 for same recipient
  - `insert_mail_seq_is_independent_per_recipient` — asserts two recipients each start at 0
  - `list_mail_by_recipient_filters_by_state` — asserts state filter behavior
  - `list_mail_by_recipient_clamps_limit` — asserts 1000-row cap
  - `list_mail_by_recipient_after_seq_excludes_cursor` — asserts `seq > after_seq`
  - `set_mail_state_to_delivered_sets_delivered_at`
  - `set_mail_state_to_failed_records_reason`
  - `insert_subscription_is_idempotent` — asserts duplicate returns same id
  - `delete_subscription_returns_false_for_missing_id`
  - `list_subscribers_for_topic_returns_only_matching_topic`
  - `migrate_is_idempotent` — second `migrate()` call is a no-op
- These tests become immutable once committed.

**Non-testable items:**
- The `CREATE TABLE` SQL strings themselves (validated transitively via the schema-shape tests above).

**Notes/Warnings:**
- Use `unix_now()` (or whatever epoch helper the codebase already uses for `agents.created_at` / `events.ts`) for `created_at` and `delivered_at`. Confirm the convention by reading `insert_pact` (`persistence.rs:410-425`) before implementing.
- Do NOT add `mail` or `subscription` rows to the `events` table from inside helpers. EventBus emission happens in task 2, at the RPC layer, so persistence stays decoupled from the event stream.

---

### Task 2: Address parser, StreamEvent variants, and `mail.send` (direct only)

**Summary:** Implement `agent://` / `topic://` address parsing, add the four mail StreamEvent variants, and implement `mail.send` for direct addresses only (topic fanout is task 4).

**Dependencies:** 1

**Files to create/modify:**
- `src/shared/protocol.rs` — add `MailSendParams`, `MailSendResult`; add `MailSent`, `MailReceived`, `MailDelivered`, `MailFailed` variants to `StreamEvent`; bump `StreamEvent::stream_key()` if it discriminates per agent (verify in source).
- `src/shared/mail.rs` (new) — `Address` enum (`Agent(AgentId) | Topic(String)`), `parse_address(&str) -> Result<Address, AddressParseError>`. Topic name regex per Ambiguity Resolutions table.
- `src/shared/mod.rs` — `pub mod mail;`
- `src/daemon/rpc.rs` — register `"mail.send"` → `handle_mail_send` in the dispatch match; implement handler.
- `src/daemon/server.rs` (or wherever `RpcHandlerCtx`/equivalent is constructed) — pass `Database` and `EventBus` references through to `handle_mail_send` (follow how `handle_invoke` reaches `AgentManager`).
- `tests/protocol.rs` — extend with `mail.send` round-trip tests.
- `tests/mail_send.rs` (new) — integration tests for direct send.

**Detailed specification:**

`Address` API:

```rust
pub enum Address {
    Agent(AgentId),
    Topic(String),
}

pub fn parse_address(s: &str) -> Result<Address, AddressParseError>;
```

Rules:
- `agent://<id>` — `<id>` must match the existing 8-char UUID regex `^[0-9a-f]{8}$`. Reject anything else with `invalid_agent_id`.
- `topic://<name>` — `<name>` must match `^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$`. Reject with `invalid_topic_name`.
- Anything else (no `://`, unknown scheme like `grimd://`, empty) → `invalid_address`.

`StreamEvent` additions:

```rust
#[serde(rename = "mail_sent")]
MailSent { mail_id: String, sender_id: Option<AgentId>, recipient_id: AgentId, topic: Option<String> },
#[serde(rename = "mail_received")]
MailReceived { mail_id: String, recipient_id: AgentId, sender_id: Option<AgentId>, topic: Option<String>, body_preview: String, wake_eligible: bool },
#[serde(rename = "mail_delivered")]
MailDelivered { mail_id: String, recipient_id: AgentId },
#[serde(rename = "mail_failed")]
MailFailed { mail_id: String, recipient_id: AgentId, reason: String },
```

`body_preview` is the first 200 chars of `body` — the full body lives in the `mail` row.

`StreamEvent::stream_key()` (look up the existing impl in `src/shared/protocol.rs` near line 232; the agent dispatcher uses this to decide which agent's stream a given event belongs to): map each variant as follows:

- `MailSent` → `sender_id` if `Some`, else `None` (event has no per-agent stream — written to events table with NULL agent_id, surfaced on a future "system" stream if one exists).
- `MailReceived` → `recipient_id`.
- `MailDelivered` → `recipient_id`.
- `MailFailed` → `sender_id` if `Some`, else `recipient_id`.

`mail.send` RPC:

- Params: `MailSendParams { to: String, body: String, sender: Option<AgentId>, wake_eligible: Option<bool> }`. `wake_eligible` defaults to `true`.
- Result: `MailSendResult { delivered: u32, mail_ids: Vec<String> }`.
- Errors:
  - `invalid_address` (parser failure).
  - `body_too_large` (body > 64 KiB).
  - `unknown_recipient` — direct address points to no agent in the DB.
  - `recipient_banished` — recipient is in `Banished` state.

Handler flow for direct (`agent://`) only in this task:

1. Parse `to`. On error → return `RpcError`.
2. If `body.len() > 65_536` → `body_too_large`.
3. If `Address::Topic(_)` → for now, return `RpcError::not_implemented` (replaced in task 4). Task 2 contract tests must not exercise the topic path.
4. For `Address::Agent(id)`:
   - Look up agent. If missing: insert a `Failed` mail row with `fail_reason="unknown_recipient"`, emit `MailFailed`, return `Ok` with `delivered: 0` and the failed `mail_id`.
   - If state is `Banished`: same as above but `fail_reason="recipient_banished"`.
   - Otherwise: build `Mail { state: Pending, … }`, call `db.insert_mail()`. Emit `MailSent` (with `sender_id`) and `MailReceived` (with `recipient_id`). Return `delivered: 1` and the `mail_id`.

CLI (deferred to task 3) is NOT in this task. Task 2 ends with RPC + tests; no CLI surface yet.

**Edge cases to handle:**
- Sender is an unknown id: persisted verbatim, emits `MailSent` with that id. (No auth in v1 — see Ambiguity table.)
- Sender == recipient: allowed. Both `MailSent` and `MailReceived` are emitted on the same agent's stream.
- Body containing newlines, NULs, very long unicode: stored verbatim; preview is char-truncated, not byte-truncated, to avoid splitting a multi-byte codepoint.
- Address with extra path segments (`agent://abc/def`): rejected as `invalid_agent_id`.
- Body of length exactly 65_536: accepted (limit is `>`, not `>=`).

**Acceptance criteria:**
- [ ] `parse_address("agent://abcd1234")` returns `Address::Agent("abcd1234")`.
- [ ] `parse_address("topic://pr-opened")` returns `Address::Topic("pr-opened")`.
- [ ] `parse_address("agent://XYZ")` returns `Err(invalid_agent_id)` (uppercase / non-hex).
- [ ] `parse_address("topic://has space")` returns `Err(invalid_topic_name)`.
- [ ] `parse_address("grimd://host/x")` returns `Err(invalid_address)`.
- [ ] `parse_address("")` returns `Err(invalid_address)`.
- [ ] `mail.send` with `body.len() = 65_537` returns `RpcError` code `body_too_large`.
- [ ] `mail.send` to a valid `agent://` recipient with state `Active` inserts one `mail` row with `state='Pending'` and emits exactly two `StreamEvent`s: one `MailSent`, one `MailReceived`.
- [ ] `mail.send` to a `agent://` id that does not exist inserts one `mail` row with `state='Failed'`, `fail_reason='unknown_recipient'`, emits one `MailFailed`, returns `delivered=0` with one entry in `mail_ids`.
- [ ] `mail.send` to a `Banished` recipient inserts one `mail` row with `state='Failed'`, `fail_reason='recipient_banished'`, emits one `MailFailed`, returns `delivered=0`.
- [ ] `mail.send` with `wake_eligible: false` persists `wake_eligible=0` in the mail row and the `MailReceived` event payload reflects `wake_eligible=false`.
- [ ] `mail.send` with `to = "topic://x"` returns `RpcError` code `not_implemented` (placeholder; task 4 replaces).
- [ ] `MailReceived.body_preview` is the first 200 chars of body, not bytes (verified with a multi-byte unicode body of 250 chars).
- [ ] `MailSent` and `MailReceived` events round-trip through `serde_json` with the `mail_sent` / `mail_received` discriminators.

**Contract tests (RED phase):**
- Test file: `tests/protocol.rs` (parser + serde) and `tests/mail_send.rs` (handler integration)
- Tests in `tests/protocol.rs`:
  - `parse_address_accepts_agent_scheme`
  - `parse_address_accepts_topic_scheme`
  - `parse_address_rejects_uppercase_agent_id`
  - `parse_address_rejects_topic_with_space`
  - `parse_address_rejects_unknown_scheme`
  - `parse_address_rejects_empty_string`
  - `mail_sent_event_round_trips_through_serde`
  - `mail_received_event_round_trips_through_serde`
- Tests in `tests/mail_send.rs`:
  - `send_to_active_agent_inserts_pending_mail_and_emits_events`
  - `send_to_unknown_agent_inserts_failed_mail_and_emits_mail_failed`
  - `send_to_banished_agent_inserts_failed_mail_with_recipient_banished_reason`
  - `send_with_oversize_body_returns_body_too_large_error`
  - `send_with_wake_eligible_false_persists_zero_in_mail_row`
  - `send_to_topic_returns_not_implemented_in_task_2_scope`
  - `mail_received_body_preview_truncates_by_chars_not_bytes`
- These tests become immutable once committed.

**Non-testable items:**
- The string match arm registering `"mail.send"` in `rpc.rs` (covered transitively).

**Notes/Warnings:**
- Verify the exact `EventBus` injection pattern by reading how `handle_summon` reaches `EventBus` in `rpc.rs`. Don't introduce a new global.
- The `not_implemented` placeholder for topics in task 2 must use a distinct error code, NOT `invalid_address`, so task 4 can swap behavior without breaking the parser tests.

---

### Task 3: `mail.list` + `mail.ack` RPC and `grim mail send/list/ack` CLI

**Summary:** Read-side RPC and the first three subcommands of `grim mail`, all wired through the existing `DaemonClient` pattern.

**Dependencies:** 2

**Files to create/modify:**
- `src/shared/protocol.rs` — `MailListParams`, `MailListResult`, `MailAckParams`, `MailAckResult`.
- `src/daemon/rpc.rs` — `handle_mail_list`, `handle_mail_ack`; register methods.
- `src/cli/commands/mail.rs` (new) — clap subcommand enum `MailCommand { Send { addr, body }, List { agent_id, pending, after }, Ack { mail_id } }` plus `pub async fn run(cmd: MailCommand) -> Result<()>`.
- `src/cli/commands/mod.rs` — `pub mod mail;`.
- `src/main.rs` — add `Mail(MailCommand)` to the top-level `Commands` enum and a dispatch arm in the match.
- `src/cli/formatters.rs` — `format_mail_list(&[Mail])` table formatter consistent with `format_queue` / `format_circle`.
- `tests/cli_mail.rs` (new) — uses the `tests/support/grimw_fake_daemon.rs` pattern (or `tests/scheduler_integration.rs::Harness`) to drive the CLI end-to-end.

**Detailed specification:**

RPC:

- `mail.list`: `{ agent_id: AgentId, after_seq?: u64, state?: "Pending"|"Delivered"|"Failed", limit?: u32 }` → `{ mails: Vec<Mail> }`. Default `limit=100`, max 1000.
- `mail.ack`: `{ mail_id: String }` → `{ acked: bool }`. Behavior:
  - Mail not found → `RpcError` `mail_not_found`.
  - State `Pending` → flip to `Delivered`, emit `MailDelivered`, return `acked: true`.
  - State `Delivered` → no-op, return `acked: false`. Do NOT re-emit.
  - State `Failed` → `RpcError` `cannot_ack_failed`.

CLI:

- `grim mail send <addr> <body>...` — joins remaining args with spaces; calls `mail.send` with `sender = None`. Exit 0 on `delivered >= 1`, exit 1 if all entries failed (`delivered == 0` and `mail_ids` non-empty).
- `grim mail list <agent-id> [--pending] [--all] [--after <seq>]` — `--pending` → `state=Pending`; `--all` → no state filter (default behavior); they're mutually exclusive. Resolves short-prefix agent IDs the same way `grim bind <id>` does (see `main.rs:177-230` for the prefix resolution helper).
- `grim mail ack <mail-id>` — calls `mail.ack`; prints `acked: true|false` or the error.

Formatter columns for `format_mail_list`: `SEQ | ID | FROM | TOPIC | STATE | AGE | PREVIEW`. `PREVIEW` is the first 60 chars of body. `FROM` is `-` for null sender, otherwise `agent://<id-prefix>`. `TOPIC` is `-` for direct mail.

**Edge cases to handle:**
- Empty mailbox: `list` returns `{mails: []}`; CLI prints "no mail".
- `--pending` and `--all` both passed: clap error.
- `mail-id` prefix expansion: support short prefixes for `mail.ack` like other commands (resolve via `db.get_mail_by_prefix`). If ambiguous → CLI error before RPC.
- `agent-id` not found in CLI prefix resolver: clap-style error before RPC.
- `grim mail send agent://X "hello world"` with `body` containing shell-quoted spaces: clap collects trailing args; join with single spaces. Document the limitation (no preserved quoting).

**Acceptance criteria:**
- [ ] `mail.list` for a recipient with 3 Pending and 2 Delivered rows, no state filter, returns 5 entries ordered by `seq ASC`.
- [ ] `mail.list` with `state=Pending` returns only the 3 Pending entries.
- [ ] `mail.list` with `after_seq=2` returns rows with `seq >= 3`.
- [ ] `mail.list` with `limit=2` returns exactly 2 entries (lowest `seq` first).
- [ ] `mail.ack` on a Pending mail flips state to Delivered, sets `delivered_at`, and emits exactly one `MailDelivered` event.
- [ ] `mail.ack` on a Delivered mail returns `acked: false` and emits zero events.
- [ ] `mail.ack` on a Failed mail returns `RpcError` code `cannot_ack_failed`.
- [ ] `mail.ack` on a missing mail id returns `RpcError` code `mail_not_found`.
- [ ] `grim mail send agent://<id> "hello"` exits 0 and prints the new `mail_id` on stdout.
- [ ] `grim mail send agent://deadbeef "hi"` exits 1 (unknown recipient) and prints the failure reason.
- [ ] `grim mail list <id-prefix> --pending` calls `mail.list` with `state=Pending` and prints a table whose header is exactly `SEQ  ID  FROM  TOPIC  STATE  AGE  PREVIEW`.
- [ ] `grim mail list <id> --pending --all` exits non-zero with a clap usage error.
- [ ] `grim mail ack <mail-id>` for a Pending mail prints `acked: true` and exits 0.
- [ ] `Mail(MailCommand)` appears in the top-level `Commands` enum in `src/main.rs`.

**Contract tests (RED phase):**
- Test file: `tests/cli_mail.rs` (CLI-driven), with helper assertions cross-checked against RPC behavior.
- Tests:
  - `mail_list_returns_all_states_when_filter_omitted`
  - `mail_list_filters_to_pending_when_requested`
  - `mail_list_paginates_using_after_seq`
  - `mail_list_respects_limit`
  - `mail_ack_pending_flips_to_delivered_and_emits_event`
  - `mail_ack_delivered_is_idempotent_no_event`
  - `mail_ack_failed_returns_cannot_ack_failed_error`
  - `mail_ack_missing_returns_mail_not_found_error`
  - `cli_mail_send_to_unknown_agent_exits_nonzero`
  - `cli_mail_list_pending_and_all_flags_are_mutually_exclusive`
  - `cli_mail_list_renders_expected_header_columns`
- These tests become immutable once committed.

**Non-testable items:**
- Top-level `Commands::Mail` enum entry (covered by CLI tests transitively).

**Notes/Warnings:**
- Match the prefix-resolution UX for agent ids that `grim bind` / `grim invoke` already use. Reuse that helper rather than re-implementing.
- Do not let `mail.ack` change `seq` — `seq` is immutable once assigned.

---

### Task 4: Topic fanout in `mail.send` plus subscribe / unsubscribe / topics RPC and CLI

**Summary:** Replace the `not_implemented` topic placeholder with snapshot fanout, and add the subscription management surface.

**Dependencies:** 3

**Files to create/modify:**
- `src/shared/protocol.rs` — `MailSubscribeParams/Result`, `MailUnsubscribeParams/Result`, `MailTopicsResult` (`Vec<{topic, subscriber_count}>`).
- `src/daemon/rpc.rs` — replace topic branch in `handle_mail_send`; add `handle_mail_subscribe`, `handle_mail_unsubscribe`, `handle_mail_topics`.
- `src/cli/commands/mail.rs` — extend `MailCommand` with `Subscribe { agent_id, topic }`, `Unsubscribe { subscription_id }`, `Topics`.
- `tests/mail_topics.rs` (new) — fanout snapshot semantics and persistence-across-restart.

**Detailed specification:**

Topic fanout in `mail.send`:

1. Parse `to` → `Address::Topic(name)`.
2. `db.list_subscribers_for_topic(&name)` — snapshot at this exact instant.
3. For each subscriber, build a `Mail` row with `topic = Some(name)`, `recipient_id = subscriber_id`, `state = Pending`. Insert all rows in a single transaction so a partial fanout cannot be observed.
4. Emit one `MailSent` (with `topic = Some(name)`, `recipient_id` = first subscriber for routing — or, if the existing event-stream router can accept `recipient_id` per-subscriber, emit one `MailSent` per subscriber; pick whichever matches the existing event-stream contract — verify by reading `event_bus.rs` before implementing). Emit one `MailReceived` per subscriber.
5. Subscribers that are `Banished` at snapshot time still get a row inserted with `state='Failed'`, `fail_reason='recipient_banished'`. They count toward `mail_ids` but NOT toward `delivered`.
6. `delivered` in the result is the count of `Pending` rows inserted (i.e., excluding any `Failed`).
7. Empty subscriber list → `delivered: 0`, `mail_ids: []`, single `MailSent` with `topic` set, `recipient_id` = `""` (or omit if event router supports null — confirm before coding).

`mail.subscribe`:

- Params: `{ agent_id: AgentId, topic: String }`.
- Validate topic name via `parse_address` regex (reuse the topic validator function — refactor it out of `parse_address` if needed so it's reusable).
- Validate `agent_id` exists in `agents` table; if not, `unknown_agent` error.
- Insert via `db.insert_subscription`. Idempotent (returns existing id on duplicate).
- Result: `{ subscription_id: String }`.

`mail.unsubscribe`:

- Params: `{ subscription_id: String }`.
- `db.delete_subscription(id)`. If `false`, return `RpcError` `subscription_not_found`.
- Result: `{}`.

`mail.topics`:

- No params.
- `db.list_topics_with_counts()` — sorted by topic ASC.
- Result: `{ topics: Vec<{topic, subscriber_count}> }`.

CLI:

- `grim mail subscribe <agent-id> <topic>` — prints the `subscription_id`. Idempotent.
- `grim mail unsubscribe <subscription-id>` — prints `unsubscribed: true|false`.
- `grim mail topics` — prints a 2-column table (`TOPIC  SUBSCRIBERS`).

**Edge cases to handle:**
- Subscribing the same agent to the same topic twice: returns the same `subscription_id`. CLI prints it without warning.
- Publishing to a topic with subscribers that include duplicate agent IDs: cannot happen (UNIQUE constraint), but assert this in a test.
- Subscriber added between two publishes: the second publish sees the new subscriber, the first did not. (Snapshot semantics.)
- `mail.subscribe` with malformed topic: `invalid_topic_name` error.
- `mail.subscribe` for an unknown agent id: `unknown_agent` error.
- Daemon restart between `mail.subscribe` and `mail.send`: subscription persists; fanout still finds it.

**Acceptance criteria:**
- [ ] `mail.send` to `topic://X` with 3 active subscribers inserts 3 `Pending` mail rows (one per subscriber) and emits 3 `MailReceived` events.
- [ ] `mail.send` to a topic with 0 subscribers returns `{delivered: 0, mail_ids: []}` and emits at most one `MailSent`.
- [ ] `mail.send` to a topic where one of three subscribers is `Banished` returns `delivered: 2`, `mail_ids` length 3, and the banished subscriber's row has `state='Failed'` with reason `recipient_banished`.
- [ ] A subscriber added after the first publish does NOT receive a row from that publish; it does receive a row from a subsequent publish.
- [ ] `mail.subscribe` for a valid agent and topic returns a `subscription_id`. Calling again with the same args returns the same id.
- [ ] `mail.subscribe` with `topic="bad name"` returns `invalid_topic_name`.
- [ ] `mail.subscribe` with an unknown `agent_id` returns `unknown_agent`.
- [ ] `mail.unsubscribe` on an existing id removes the row and returns `{}`.
- [ ] `mail.unsubscribe` on a missing id returns `subscription_not_found`.
- [ ] `mail.topics` returns one entry per distinct topic with the correct subscriber count, sorted ASC.
- [ ] After `Database::open` is closed and re-opened (on-disk DB), `mail.topics` returns the same set of topics — proving subscription persistence.
- [ ] `grim mail subscribe <id> pr-opened` prints a non-empty `subscription_id` and exits 0.
- [ ] `grim mail topics` table header is exactly `TOPIC  SUBSCRIBERS`.

**Contract tests (RED phase):**
- Test file: `tests/mail_topics.rs`
- Tests:
  - `topic_send_inserts_one_row_per_subscriber`
  - `topic_send_to_zero_subscribers_emits_only_mail_sent`
  - `topic_send_to_banished_subscriber_inserts_failed_row_excluded_from_delivered_count`
  - `subscriber_added_after_publish_does_not_retroactively_receive`
  - `subscribe_is_idempotent_returns_same_subscription_id`
  - `subscribe_rejects_invalid_topic_name`
  - `subscribe_rejects_unknown_agent_id`
  - `unsubscribe_existing_returns_empty_object`
  - `unsubscribe_missing_returns_subscription_not_found_error`
  - `topics_lists_subscriber_counts_sorted_ascending`
  - `subscriptions_persist_across_database_reopen`
- These tests become immutable once committed.

**Non-testable items:**
- The clap argument shapes for the new CLI subcommands (covered transitively via cli_mail tests if extended; otherwise marked as wiring).

**Notes/Warnings:**
- The choice between "one MailSent per subscriber" vs "one MailSent for the publish" must match how `event_bus.rs::publish` already segments per-agent streams. Read it before deciding — do not invent a new convention.
- Do NOT eagerly resolve subscribers to a single event payload; topic fanout produces N rows so observability remains "one event per recipient."

---

### Task 5: Scheduler wake-on-mail branch

**Summary:** Add `MailReceived` to the scheduler's wake set and add a new tick branch that promotes `Complete` agents with pending wake-eligible mail back to `Active` via `agent_manager.invoke()`.

**Dependencies:** 4

**Files to create/modify:**
- `src/daemon/scheduler.rs` — extend `should_wake()` (lines 203-210); add `tick_mail_wake()` method called at the top of `tick_now()` before the queue dispatch loop.
- `src/daemon/agent_manager.rs` — small helper to expose `agents.get(&id)` state read-only, if not already available, so the scheduler can check for `Complete` + `session_id` without locking the world.
- `tests/scheduler_mail_wake.rs` (new) — integration tests using the existing `Harness`.

**Detailed specification:**

`should_wake()` additions:

```rust
matches!(event, StreamEvent::MailReceived { .. })
```

is added to the existing `||` chain.

`tick_mail_wake()` flow (called from `tick_now()` before line 75's capacity load — but accounting for capacity):

1. Compute candidate list: `db.list_recipients_with_pending_wake_eligible_mail()` — returns distinct `recipient_id`s with at least one `Pending && wake_eligible=1` row. Add this helper to `Database`.
2. Filter to recipients currently in `AgentState::Complete` with `session_id.is_some()`. Skip Banished/Failed/Active/Summoning/Queued.
3. Load global cap and current `in_flight` once, same as the existing dispatch loop.
4. For each candidate (ordered by oldest pending mail's `seq` ASC, stable):
   - If `in_flight >= cap`: stop. Mail stays Pending; next tick retries.
   - Fetch its pending wake-eligible mail rows in `seq ASC` order.
   - Fold into one prompt: join bodies with `\n\n---\n\n`, truncate to 16 KiB, append `\n\n[... N more messages truncated]` if overflow occurred.
   - Call `agent_manager.invoke(&id, &folded_prompt, None).await`.
   - On success: mark each folded mail row as `state='Delivered'`, set `delivered_at = now`, emit one `MailDelivered` per row. Increment `in_flight`.
   - On error: leave rows as `Pending`, log warning, continue to next candidate. Do NOT mark them `Failed` — the next tick may succeed.
5. Return.

`StreamEvent::MailReceived` should also nudge the scheduler — wire by adding it to `should_wake()` and ensuring the existing event subscription path forwards it.

**Edge cases to handle:**
- A candidate in `Complete` without a `session_id` (e.g., crashed mid-init): skip silently. Log at trace level.
- A candidate woken on tick N completes its run and returns to `Complete` before tick N+1; new mail received during its Active phase: handled by next tick with the new mail's `seq`.
- A topic publish that lands during a mail-wake tick: the new mail rows are picked up next tick (the snapshot for this tick was already taken).
- Wake-fold cap (16 KiB): when applied to a single 32 KiB message (allowed up to 64 KiB by `mail.send`), the prompt is truncated; the truncation note specifies that 1 message was partial. (Per Ambiguity table: this is acceptable in v1.)
- Capacity boundary: `in_flight == cap - 1` and 5 candidates → first wakes, others wait.
- `MailReceived` arriving while scheduler is mid-tick: handled by the existing tick re-arm path (verify by tracing `should_wake` callers).

**Acceptance criteria:**
- [ ] A `Complete` agent with a `session_id` and one `Pending` `wake_eligible=1` mail row is invoked by `agent_manager.invoke()` on the next `tick_now()` call.
- [ ] After wake, the mail row's `state` is `Delivered` with a non-NULL `delivered_at`.
- [ ] After wake, exactly one `MailDelivered` event is emitted per folded mail row.
- [ ] A `Complete` agent with `session_id = None` is skipped (no `invoke()` call, mail row remains `Pending`).
- [ ] A `Banished` agent with pending wake-eligible mail is skipped (no `invoke()` call, row remains `Pending`).
- [ ] An `Active` agent with pending wake-eligible mail is skipped (no `invoke()` call, row remains `Pending` — it's the wrapper's job to drain via `mail.list`).
- [ ] Three pending mail rows for one Complete recipient are folded into a single `invoke()` call whose prompt body contains all three bodies separated by `\n\n---\n\n`.
- [ ] When folded body would exceed 16 KiB, the prompt is truncated and ends with `[... N more messages truncated]` where N is the count of fully omitted messages.
- [ ] When `in_flight == cap`, mail-wake skips all candidates this tick; on next tick after `in_flight` drops, the candidates wake.
- [ ] `should_wake(&StreamEvent::MailReceived { … })` returns `true`.
- [ ] A mail row with `wake_eligible=0` does NOT trigger a wake even when all other conditions are met.
- [ ] If `agent_manager.invoke()` returns `Err`, all mail rows for that candidate remain `Pending` (no state change, no events emitted for them this tick).

**Contract tests (RED phase):**
- Test file: `tests/scheduler_mail_wake.rs`
- Tests:
  - `complete_agent_with_pending_wake_eligible_mail_is_woken`
  - `wake_marks_folded_mail_rows_as_delivered`
  - `wake_emits_one_mail_delivered_per_folded_row`
  - `complete_agent_without_session_id_is_skipped`
  - `banished_agent_is_skipped_even_with_pending_mail`
  - `active_agent_is_skipped_mail_remains_pending`
  - `multiple_pending_mails_are_folded_into_single_invoke`
  - `wake_fold_truncates_at_sixteen_kib_with_truncation_note`
  - `wake_respects_global_cap`
  - `should_wake_returns_true_for_mail_received_event`
  - `wake_eligible_zero_does_not_trigger_wake`
  - `invoke_failure_leaves_mail_rows_pending_and_emits_no_delivered`
- These tests become immutable once committed.

**Non-testable items:**
- The wiring inside `tick_now()` itself (the new `tick_mail_wake()` call) is covered by the integration tests above.

**Notes/Warnings:**
- Use the existing `tick_lock` mutex to serialize mail-wake with queue dispatch — do NOT introduce a second lock.
- The `ControlledExecutor` mock in `tests/scheduler_integration.rs:9-97` is the right place to start. You may need to extend it to record `invoke()` calls in addition to `start()` calls — pattern is identical.
- Be careful about ordering: a single tick should NOT both wake an agent AND dispatch a queued one to it — the agent should only be in the wake list while `Complete`, and `invoke()` flips it to `Active` before the queue branch runs. Confirm by reading `agent_manager::invoke()` lines 394-410 (state flip is synchronous-ish before the executor spawn).

---

### Task 6: Documentation pass

**Summary:** Update README and ROADMAP to reflect the shipped feature.

**Dependencies:** 5

**Files to create/modify:**
- `README.md` — add a "Messaging" section after the existing Commands table; add `grim mail …` rows to that table.
- `ROADMAP.md` — check off Part 5 build-order item #3 (agent-to-agent messaging bus).

**Detailed specification:**

README "Messaging" section content:

- Brief paragraph: "Agents can send each other mail and subscribe to topics. Direct addresses (`agent://<id>`) deliver one row per send; topic addresses (`topic://<name>`) fan out to current subscribers. Dormant agents with a `session_id` are woken on incoming mail; live agents read pending mail at their next turn boundary."
- Subsection "Quickstart" with commands: `grim mail send`, `grim mail list`, `grim mail subscribe`, `grim mail topics`, `grim mail ack`.
- Limitations callout: "v1 surfaces mail at turn boundaries (no mid-turn interrupts). v1 has no auth on the local UDS — any caller may send as any sender."

Commands-table additions: one row per `grim mail` subcommand.

ROADMAP edit: change Part 5 build-order #3 entry from open to checked, with a one-line note linking to `agent-messaging-bus-spec.md`.

**Edge cases to handle:**
- Existing README structure: don't reorder the Commands table; append rows only.
- ROADMAP may have re-ordered since the plan was written: search for "messaging" / "messaging bus" / "Part 5" rather than relying on a fixed line number.

**Acceptance criteria:**
- [ ] `README.md` contains a top-level `## Messaging` section.
- [ ] `README.md` Commands table includes one row each for `grim mail send`, `grim mail list`, `grim mail subscribe`, `grim mail unsubscribe`, `grim mail topics`, `grim mail ack`.
- [ ] `ROADMAP.md` Part 5 item #3 is marked complete.

**Contract tests (RED phase):**
- None — task 6 is documentation only.

**Non-testable items:**
- All of task 6 (markdown content). Verify manually before commit.

**Notes/Warnings:**
- Documentation tasks have no contract tests but are still gated by review.

---

## Testing Strategy (TDD)

Implementation follows red-green TDD: for each task, the implementer writes failing contract tests from the acceptance criteria (RED), then implements the minimum code to make them pass (GREEN). Contract tests are immutable once committed.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `tests/database.rs` (extended) | 13 | Schema SQL strings |
| 2 | `tests/protocol.rs` (extended), `tests/mail_send.rs` (new) | 14 | RPC dispatch wiring |
| 3 | `tests/cli_mail.rs` (new) | 11 | Top-level `Commands::Mail` enum entry |
| 4 | `tests/mail_topics.rs` (new) | 11 | CLI clap shapes |
| 5 | `tests/scheduler_mail_wake.rs` (new) | 12 | `tick_mail_wake()` call site in `tick_now()` |
| 6 | (none) | 0 | All of task 6 |

### Integration Testing

- **End-to-end demo** (after all tasks): the "review team" scenario. Three Complete agents subscribe to `topic://pr-opened`, daemon restart, publish mail, observe all three transition Complete → Summoning → Active and reach Complete again with the mail body in their resume prompt. This is a manual harness scripted in `tests/scheduler_mail_wake.rs::review_team_e2e` (can be `#[ignore]`d if too slow).
- **Cross-task integration** is implicit: tasks 4 and 5 depend on 1-3 working end-to-end, and their tests exercise the full stack.

### Manual Testing Checklist

- [ ] Start daemon. `grim mail send agent://<id> "hello"` to a Complete agent. Confirm `MailReceived` appears in `grim bind <id>` output.
- [ ] `grim mail subscribe <id> demo`. Restart the daemon. `grim mail topics` still shows `demo: 1`.
- [ ] `grim mail send topic://demo "broadcast"` with 3 subscribers. Watch `grim circle` show all three transition through Summoning → Active.
- [ ] `grim mail send agent://deadbeef "hi"` (unknown id). Confirm a `Failed` mail row exists via `grim mail list deadbeef --all`. (Note: this is informational only since `deadbeef` won't resolve via prefix lookup; the row exists in the table but is unreachable via CLI prefix — fine for v1.)
- [ ] `grim mail send` with a 100 KiB body returns `body_too_large`.
- [ ] Set `wake_eligible=false` via direct RPC call (no CLI flag in v1) — confirm a Complete recipient does NOT wake.

## Rollout Considerations

### Feature Flags

None. The feature is additive: new tables, new RPC methods, new CLI subcommand, and one new branch in the scheduler tick that is a no-op when no mail rows exist. There is no behavior change for existing callers.

### Migration Strategy

Schema additions are `CREATE TABLE IF NOT EXISTS` — applied automatically on `Database::open()`. No data migration; no user action required.

### Rollback Plan

To roll back v1:
1. Revert the commits (the schema changes are additive, so the `mail` and `subscriptions` tables remain in user DBs but are inert).
2. Optional cleanup: a follow-up release can ship a one-shot `DROP TABLE IF EXISTS mail; DROP TABLE IF EXISTS subscriptions;` if rollback is permanent. Not part of v1.
3. Pacts are untouched, so existing pact-based workflows are unaffected by a rollback.

## Open Items

- [ ] Confirm whether `event_bus.rs::publish` already supports a "system" stream (no `agent_id`) for `MailSent` from external/human senders, or whether we need to add one. Resolved during task 2 implementation, not before.
- [ ] Decide if `mail.send` should also accept `to: ["agent://a", "agent://b"]` for batch direct sends. v1 says no (single recipient per call); revisit after v1 ships if usage shows multi-recipient is common.
- [ ] Hardening: add a per-recipient rate limit / debounce on wake to handle hot topics. v2.

---

*This spec is implementation-ready. Each task is designed for red-green TDD. Tasks must be completed in order (1 → 2 → 3 → 4 → 5 → 6) — every task depends on the prior one's schema, RPC, or CLI surface.*
