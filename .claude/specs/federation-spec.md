# Implementation Spec: Federation (v1)

> Generated from: `.claude/plans/federation.md`
> Generated on: 2026-04-27

## Overview

Two `grimd` instances peer with each other so an agent on daemon-A can `mail.send` to an agent on daemon-B (and to opt-in topics shared across the pair). Peering is point-to-point, configured by hand, and built on the same Tonic gRPC + bearer-token substrate already used for the `grimd ↔ grimw` worker channel. No gossip, no transitive routing, no scroll-spanning, no federated workspaces.

The change touches three layers: the address space (introduces `DaemonId` and a `agent://<daemon-id>/<agent-id>` form, with bare `agent://[0-9a-f]{8}` still parsing as local), the persistence layer (new `peers`, `peer_outbox`, `peer_inbox`, `topic_federations` tables), and the transport layer (new `proto/peer.proto` with a single bidirectional `Channel` RPC). Mail forwarding is at-least-once with `(sender_daemon_id, sender_seq)` dedupe on the inbox — effectively-once delivery from the user's perspective. Existing `mail.send` callers, dashboard subscribers, and `grim bind` consumers are unaffected aside from now potentially seeing longer addresses.

## Technical Context

### Relevant Codebase Areas

- `src/shared/mail.rs` — `parse_address` (line 45) currently has two arms (`agent://` / `topic://`). Adding a third for the federated form. `is_valid_agent_id` (line 65) is the existing hex shape.
- `src/shared/constants.rs` — `generate_short_id` (line 41) is the 8-hex pattern reused for `DaemonId`. New constants: `DAEMON_ID_FILENAME`, `DAEMON_ID_PATH`, federation defaults.
- `src/daemon/persistence.rs` — `migrate()` (line 86) is the single migration block; new tables go here. `insert_mail` (line 1351) and `insert_mail_batch` (line 1411) wrap mail inserts in IMMEDIATE transactions; outbox writes co-commit with mail rows in the same transaction.
- `src/daemon/rpc.rs` — `handle_mail_send` (line 486), `handle_direct_send` (line 525), `handle_topic_send` (line 609). Reserved-prefix guard at line 499. Error helper `rpc_err(req.id, &str_code)` (line 16-ish).
- `src/daemon/scheduler.rs` — `tick_mail_wake` (line 329) consumes pending mail rows and wakes recipients. Inbound federated mail lands in the same `mail` table, so wake-on-mail fires for free.
- `src/daemon/event_bus.rs` — `EventBus::publish(StreamEvent)`. New variants: `PeerHandshakeOk`, `PeerHandshakeFailed`, `PeerStreamConnected`, `PeerStreamDisconnected`, `PeerMailForwarded`, `PeerMailReceived`, `TopicFederationAdded`, `TopicFederationRemoved`.
- `src/daemon/worker_rpc_server.rs` — Existing tonic server with bearer-token auth at handshake (line 51), bidirectional stream routing (line 87+). Peer service mirrors this shape exactly: `WorkerControlService` → `PeerService`.
- `src/grimw/rpc_client.rs` — Outbound tonic client + heartbeat loop (line 71) + reconnect-on-stream-drop pattern. Peer outbound client mirrors this shape.
- `proto/worker.proto` — Existing tonic proto. New `proto/peer.proto` lives next to it; `build.rs` already configures `tonic_build::compile_protos`.
- `src/daemon/server.rs` — `AppState` (line 18) and `run` (line 29) wire daemon services. New: `PeerRegistry`, `PeerOutboxDrainer`, `PeerInboxHandler` join here.
- `src/cli/commands/mod.rs` — `pub mod peer;` and `pub mod topic;` get added alongside `mail`, `wake`, `workspace`.
- `src/shared/protocol.rs` — `RpcRequest` (line 7) gets an optional `protocol_version: Option<u32>` field; existing callers send `None` and the daemon defaults to v1.
- `src/shared/types.rs` — `AgentId` is a `String` alias (line 6). New `DaemonId`, `PeerId`, `Peer`, `PeerState`, `PeerOutboxRow`, `PeerInboxRow`, `TopicFederation`. `Mail.recipient_id` and `Mail.sender_id` stay `String` — the address widening is purely in the parser and validators.
- `src/shared/config.rs` — `DaemonConfig` (line 60) gets federation defaults (`peer_outbox_max_depth`, `peer_handshake_timeout_secs`, `peer_heartbeat_interval_secs`).

### Existing Patterns to Follow

- **Tonic bidirectional stream + bearer auth** — `worker_rpc_server.rs` is the template. Server reads first message, validates `bearer_token`, opens a per-connection mpsc on the daemon side, returns a `ReceiverStream` for outbound. Peer reuses this verbatim with a different proto.
- **Reconnect loop with backoff** — `grimw/rpc_client.rs` connects with `WorkerControlClient::connect`, sends a `Register`/`Hello` first, runs heartbeat + inbound select. Peer outbound mirrors this; backoff uses the existing `Clock` seam (`src/daemon/clock.rs`) so tests drive time deterministically.
- **Migration guards for additive columns** — `let has_x: bool = conn.prepare("SELECT x FROM t LIMIT 0").is_ok(); if !has_x { ALTER TABLE … }`. Used throughout `persistence.rs`. New tables use `CREATE TABLE IF NOT EXISTS`.
- **IMMEDIATE-mode txn + per-recipient seq** — `insert_mail` and `insert_mail_batch`. Outbox writes follow the same shape: a single IMMEDIATE txn that inserts the `mail` row (state=`Pending` for federated direct sends, or fanned-out `Pending` rows for federated topics) plus the `peer_outbox` row.
- **Reserved sender prefix** — `wake://`, `supervisor://`, `workspace://` are already rejected at `mail.send`. `peer://<peer-name>` joins the list (used as `sender_id` on inbound federated mail rows when origin parsing fails or for system events).
- **Actor with `Arc<Self>` + `Mutex<HashMap<…>>` + EventBus** — `WakeRegistry` and `WorkspaceRegistry` are siblings. `PeerRegistry` follows the same shape: per-peer outbound stream handle, per-peer outbox drainer task handle, lifecycle events on the bus.
- **Idempotency UNIQUE index** — `subscriptions (subscriber_id, topic) UNIQUE` is the precedent for `peer_inbox (sender_daemon_id, sender_seq) UNIQUE`. ON CONFLICT DO NOTHING for replay safety.
- **CLI subcommand pattern** — `MailCommand` / `WorkspaceCommand` enums + `run(cmd)` dispatcher + per-handler `DaemonClient::connect().await?.call("method", json!({…}))`. Mirror in `cli/commands/peer.rs` and `cli/commands/topic.rs`.

### Key Dependencies

- `tonic = "0.12"`, `prost`, `prost-types` — already in `Cargo.toml` for the worker channel. Peer service compiles in the same `build.rs` invocation.
- `rusqlite` with WAL mode — already in use.
- `chrono`, `tokio`, `tracing` — standard across the daemon.
- `semver = "1"` — already used for worker version negotiation; reused for `peer_protocol_version`.
- `tokio::time::interval` + `Clock` seam — used by scheduler and wake registry; outbox backoff and handshake timeouts piggyback on it.

### Ambiguity Resolutions

| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| `DaemonId` shape | Hex vs `name@host` (plan Open Q #1) | **8-hex internally** (matches agent ID shape, no DNS coupling). Optional human-readable `name` field on `peers` for CLI display only — never used as an identifier. | Plan recommendation |
| `DaemonId` first-boot | When is it minted, where is it stored? | First call to `grimd daemon`: read `~/.grimoire/daemon.id`; if absent, generate via `generate_short_id()`, write atomically (`tempfile + rename`), and `chmod 0600`. Path overridable via `GRIMOIRE_DAEMON_ID_PATH` for tests. | Inferred — needed for determinism |
| Federated address path syntax | `agent://<daemon-id>/<agent-id>` — what separates, can `<daemon-id>` carry a prefix? | Strict shape: `agent://grimd-[0-9a-f]{8}/[0-9a-f]{8}`. Display form always carries the `grimd-` prefix; storage is the same string. Reject any other shape with `invalid_federated_agent_id`. | Inferred from plan |
| Topic dedup side | Publisher vs subscriber (plan Open Q #2) | **Publisher-side**: the fanout txn writes one `peer_outbox` row per remote subscriber, computed against the local mirror of the peer's subscription list. | Plan |
| `MailReceived` source-daemon attribution | New field or parsed from `sender_id`? (plan Open Q #3) | Add `origin_daemon_id: Option<DaemonId>` to `StreamEvent::MailReceived` and `StreamEvent::MailDelivered`. `None` for local mail; `Some(<id>)` for inbound federated mail. Cheap and avoids re-parsing in consumers. | Plan recommendation — chosen explicit field |
| `grim scry agent://grimd-xxx/yyyy` | What does cross-peer scry show? (plan Open Q #4) | v1 returns `RpcError { code: "scry_local_only", message: "agent introspection is local in v1" }`. No new RPC plumbing. | Plan |
| Peer name shape | `--name` on `peer add` | `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`. UNIQUE on `peers.name`. | Inferred from workspace name pattern |
| Bearer token shape | What format is the token the operator passes? | Opaque string, 32–256 chars, validated as printable ASCII (`b'!'..=b'~'`). Stored hashed (Blake3, 32-byte) on the local daemon, plaintext only in operator-typed CLI input and over the wire on initial `Hello`. | Inferred — security best practice |
| gRPC URL scheme | `--url` accepts what? | `http://host:port` or `https://host:port`. v1: `http://` only is supported on the wire (mTLS is Phase 2 and gates on the worker mTLS work). `https://` is parsed but produces `RpcError { code: "peer_tls_not_supported_yet" }`. | Inferred — phasing in plan |
| Outbox retry backoff | Schedule? | Exponential: 1s, 2s, 4s, 8s, 16s, 32s, capped at 60s. Reset to 1s on any successful `MailAck`. Per-peer, computed against `Clock`. | Inferred — common pattern |
| Outbox cap behavior | What does `mail.send` return at cap? | `RpcError { code: "peer_outbox_full", message: "outbox depth N exceeds cap M" }`. The cap default is `peer_outbox_max_depth = 10_000`. | Plan |
| Heartbeat cadence | How often, what timeout? | Send `Heartbeat` every `peer_heartbeat_interval_secs` (default 5). Mark stream `Down` if no `Heartbeat` or `MailDeliver` received for 3× interval (15s default). Reuses the worker-channel pattern. | Inferred — mirror worker channel |
| Handshake timeout | `peer add` deadline | `peer_handshake_timeout_secs` default 10. On timeout: do not write a `peers` row; surface `peer_handshake_timeout`. | Inferred — needed for `peer add` UX |
| `peer remove` outbox handling | Plan says cascade `peer_outbox` / retain `mail` | `peer_outbox` and `peer_inbox` are FK-cascade-deleted on `peers.id`. `mail` rows are retained as historical record (no FK). State transition: `Active → Removing → <row deleted>`. | Plan |
| `peer remove` while in-flight | Operator removes during outbox drain | Drainer checks `state` each iteration; if `Removing`, exits cleanly without writing further `MailAck` rows. The currently in-flight `MailDeliver` may still be processed by the remote — that's fine, it's idempotent. | Inferred — plan implies graceful drain |
| Daemon-id collision | Plan says reject at `Hello` | Handshake compares the peer's claimed `daemon_id` against existing `peers` rows. If collision under a *different* peer name, return `peer_daemon_id_collision`. If same name, treat as legitimate reconnect. | Plan |
| `RpcRequest.protocol_version` default | Existing callers don't send it | `Option<u32>`, server defaults to `1` when absent. Server rejects with `unsupported_protocol_version` if value is set and not in `[1]`. Adds the field today so adding `2` later is a value bump, not a schema change. | Plan prerequisite #2 |
| Mail body cap on inbound | Federated mail bypasses local `MAX_MAIL_BODY_BYTES`? | Inbound `MailDeliver` is rejected at the inbox handler if `body.len() > MAX_MAIL_BODY_BYTES`. Sends back `MailAck { mail_id, status: Failed, reason: "body_too_large" }`. The sending daemon marks the outbox row `Failed` and emits `PeerMailForwardFailed`. | Plan ("more of the same plumbing") |
| Federated topic topic-name shape | Same as local? | Identical: `is_valid_topic_name`. Reserved prefixes (`workspace/…`, `supervisor/…`) are blocked from federation with `topic_federation_reserved`. | Inferred |
| `topic federate` symmetry | Auto-mirror? | No. Each side runs its own `topic federate`. Plan calls this out. CLI surface acknowledges with a hint: "Run on both daemons to make traffic flow both ways." | Plan |
| Peer state machine | What states does a `peers` row take? | `Pending` (handshake not yet completed) → `Active` (stream up) ↔ `Down` (stream lost, retry pending) → `Removing` (cascade in progress) → row deleted. Stored in `peers.state`. | Inferred — needed for stream lifecycle |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | DaemonId minting and persistence | None | Low |
| 2 | Federated address parser (third arm) | None | Low |
| 3 | Schema, types, and StreamEvent variants | 1 | Medium |
| 4 | RPC validation + non-local rejection (slice 1 ship-gate) | 1, 2 | Low |
| 5 | `proto/peer.proto` and Peer service skeleton | 3 | Medium |
| 6 | Peer outbound client + reconnect loop | 5 | Medium |
| 7 | Handshake (Hello / HelloAck) with auth, version, ID checks | 5, 6 | Medium |
| 8 | Outbox drainer (mail forwarding loop) | 7 | High |
| 9 | Inbox handler (dedupe + local insert + wake) | 7 | Medium |
| 10 | Mail-send routing for federated recipients | 4, 8 | Medium |
| 11 | `grim peer add/list/remove/ping` (CLI + RPC) | 7 | Low |
| 12 | Federated topics: `grim topic federate` + fanout into outbox | 8, 10 | Medium |
| 13 | End-to-end two-daemon integration tests | 9, 10, 11, 12 | Medium |

### Critical Path

```
        ┌─► 4 (slice 1 ships here) ─────────────────────────┐
1 ──┬──►│                                                    │
    │   └─► 3 ──► 5 ──┬─► 6 ──► 7 ──┬─► 8 ──┬─► 10 ──┐       │
2 ──┘                 └─────────────┘        │        ├─► 13 ─┤
                                             ├─► 9 ───┤       │
                                             │        │       │
                                             ├─► 11 ──┘       │
                                             │                 │
                                             └─► 12 ──────────┘
```

Slice 1 ships after Task 4: identity + address widening, no peer code. Slice 2 ships after Task 11: direct mail forwarding works end-to-end. Slice 3 ships after Task 12: federated topics work. Task 13 is the final integration gate before any slice merges to main.

Parallelizable: {2, 3} after 1; {6} can start in parallel with handshake design (7) once 5 lands; {9, 11} run in parallel after 7; {12} runs in parallel with the back half of {8, 10}.

---

### Task 1: DaemonId minting and persistence

**Summary:** Mint a stable 8-hex `DaemonId` on first boot, persist to `~/.grimoire/daemon.id`, expose it through `daemon.status` and `grim status`.

**Dependencies:** None

**Files to create/modify:**
- `src/shared/constants.rs` — add `DAEMON_ID_FILENAME = "daemon.id"`, `daemon_id_path()` function (mirrors `socket_path()`, honours `GRIMOIRE_DAEMON_ID_PATH` env override).
- `src/shared/types.rs` — add `pub type DaemonId = String;` and a `validate_daemon_id(s: &str) -> bool` (8 hex chars, lowercase). Export.
- `src/daemon/mod.rs` (or a new `src/daemon/daemon_id.rs`) — `pub fn load_or_mint() -> Result<DaemonId>`. Reads file; if missing, generates via `generate_short_id()`, writes via `tempfile::NamedTempFile::persist` for atomic rename, sets mode `0o600` on the result. Returns the loaded ID either way.
- `src/main.rs` (daemon entry path) — call `daemon_id::load_or_mint()` once and stash on `AppState`.
- `src/daemon/server.rs` — add `pub daemon_id: DaemonId` to `AppState`.
- `src/shared/protocol.rs` — extend `DaemonStatusResult` with `daemon_id: DaemonId`.
- `src/cli/commands/status.rs` — render `Daemon ID: grimd-<id>` line.

**Detailed specification:**

`load_or_mint(path: &Path) -> Result<DaemonId>`:
1. If `path` exists: read, trim trailing `\n`, `validate_daemon_id`. If valid, return; if invalid, error `invalid_daemon_id_file` (do not auto-overwrite — surface to operator).
2. If absent: `mkdir -p` the parent (already done for the socket), call `generate_short_id()`, write to `tempfile::NamedTempFile::new_in(parent)?`, `set_permissions(0o600)`, `persist(path)?`.
3. Return ID (always lowercase hex).

Display form `grimd-<id>` is constructed at render time, never stored. The file holds the bare 8-hex string.

**Edge cases to handle:**
- File exists but has trailing whitespace, BOM, or extra lines: trim and re-validate; reject if validation fails.
- File exists but is unreadable (permission denied): bubble up the IO error verbatim — the operator must fix it.
- Two `grimd` instances racing first boot on the same `$HOME`: `tempfile::persist` is atomic on POSIX; the second instance reads the winner's value.

**Acceptance criteria:**
- [ ] `load_or_mint` against an empty tempdir creates the file and returns an 8-hex lowercase string.
- [ ] `load_or_mint` against a tempdir that already contains a valid `daemon.id` returns the existing value without writing.
- [ ] `load_or_mint` against a tempdir containing an invalid `daemon.id` (e.g. `"NOTHEX"`) returns `Err` whose root error message contains `invalid_daemon_id_file`.
- [ ] The persisted file is mode `0o600` on POSIX.
- [ ] `daemon.status` RPC result includes `daemon_id` matching the value `load_or_mint` returned.
- [ ] `grim status` output contains a line matching `^Daemon ID: grimd-[0-9a-f]{8}$`.
- [ ] `validate_daemon_id` accepts `"abcd1234"`, rejects `"abcd1234x"`, `"ABCD1234"`, `"abcd123"`, `""`.

**Contract tests (RED phase):**
- Test file: `tests/daemon_id.rs`
- Tests to write before implementing:
  - `load_or_mint_creates_file_when_absent` — asserts file exists, contents validate, returned value matches file.
  - `load_or_mint_returns_existing_value_when_present` — asserts no write happens (mtime stable across two calls).
  - `load_or_mint_rejects_invalid_file` — asserts error contains `invalid_daemon_id_file`.
  - `validate_daemon_id_accepts_8_lowercase_hex` (and rejects the contrast cases).
  - `daemon_status_includes_daemon_id` — boots a daemon under tempdir, calls `daemon.status`, asserts the field.
- These tests become immutable once committed.

**Notes/Warnings:**
- Do NOT seed a default ID in tests — let each test mint via `GRIMOIRE_DAEMON_ID_PATH` so two-daemon harness tests get distinct IDs.

---

### Task 2: Federated address parser (third arm)

**Summary:** Widen `parse_address` to accept `agent://<daemon-id>/<agent-id>` while keeping bare `agent://[0-9a-f]{8}` as "local agent."

**Dependencies:** None (functionally; co-merge with Task 1's `validate_daemon_id` for the shared validator).

**Files to create/modify:**
- `src/shared/mail.rs` — `Address` enum gains a third variant `FederatedAgent { daemon_id: DaemonId, agent_id: AgentId }`. `parse_address` matches `^agent://grimd-[0-9a-f]{8}/[0-9a-f]{8}$` first, falls back to the bare `agent://[0-9a-f]{8}` case, then `topic://`, then error. New error code: `invalid_federated_agent_id`.
- `src/shared/mail.rs` — keep `is_valid_agent_id` unchanged; add `parse_federated_agent(rest: &str) -> Option<(DaemonId, AgentId)>`.

**Detailed specification:**

Parse precedence inside the `agent://` arm:
1. If `rest` contains `/`: split on first `/`. Left must match `grimd-[0-9a-f]{8}`, right must match `[0-9a-f]{8}`. If both, return `FederatedAgent`. Otherwise `Err(InvalidFederatedAgentId)`.
2. If `rest` has no `/`: existing behavior. Validate as bare 8-hex; return `Address::Agent` or `Err(InvalidAgentId)`.

`AddressParseError` gains `InvalidFederatedAgentId` with code `"invalid_federated_agent_id"`.

The display impl on `Address::FederatedAgent` formats as `agent://grimd-<daemon>/<agent>`.

**Edge cases to handle:**
- `agent://grimd-xxxxxxxx/` (trailing slash, no agent): error.
- `agent:///abcd1234` (leading slash, no daemon): error.
- `agent://grimd-XXXXXXXX/abcd1234` (uppercase daemon): error — daemon IDs are lowercase.
- `agent://grimd-abcd1234/abcd1234/extra`: error (extra path segment).
- `agent://abcd1234/wxyz` — leading segment has no `grimd-` prefix, treat as junk → `InvalidAgentId` (mirrors today's "extra path segments" reject).

**Acceptance criteria:**
- [ ] `parse_address("agent://grimd-1a2b3c4d/deadbeef")` returns `Ok(Address::FederatedAgent { daemon_id: "1a2b3c4d", agent_id: "deadbeef" })`.
- [ ] `parse_address("agent://abcd1234")` still returns `Ok(Address::Agent("abcd1234"))` (no regression).
- [ ] `parse_address("topic://anything-valid")` still returns `Ok(Address::Topic(_))`.
- [ ] `parse_address("agent://grimd-XXXXXXXX/abcd1234")` returns `Err(InvalidFederatedAgentId)`.
- [ ] `parse_address("agent://grimd-1a2b3c4d/deadbeef/x")` returns `Err(InvalidFederatedAgentId)`.
- [ ] `Address::FederatedAgent { … }.to_string()` round-trips through `parse_address` to an equal value.
- [ ] All seven existing `parse_address_*` tests in `src/shared/mail.rs` still pass unchanged.

**Contract tests (RED phase):**
- Test file: `src/shared/mail.rs` (extend the existing `#[cfg(test)] mod tests`).
- Tests to write before implementing:
  - `parse_address_accepts_federated_form`
  - `parse_address_rejects_uppercase_federated_daemon_id`
  - `parse_address_rejects_federated_with_extra_segment`
  - `parse_address_bare_agent_form_still_works` (regression guard)
  - `federated_address_round_trips_via_display`

**Notes/Warnings:**
- This task ships behind no flag and is observable on its own — RPC handlers in Task 4 still reject non-local recipients until Task 10 wires forwarding.

---

### Task 3: Schema, types, and StreamEvent variants

**Summary:** Add the `peers`, `peer_outbox`, `peer_inbox`, `topic_federations` tables; the Rust types backing them; the new `StreamEvent` variants; and `RpcRequest.protocol_version`.

**Dependencies:** Task 1

**Files to create/modify:**
- `src/daemon/persistence.rs` — extend `migrate()` with the four new `CREATE TABLE IF NOT EXISTS` statements and their indexes. Add CRUD method stubs (signatures only; bodies for non-trivial ones land in the consuming task). Insert the bumped `peer_protocol_version` constant.
- `src/shared/types.rs` — add `DaemonId`, `PeerId` (newtype around UUID-derived 8-hex), `Peer { id, daemon_id, name, url, bearer_token_hash, public_key, state, last_seen, registered_at }`, `PeerState` (`Pending` | `Active` | `Down` | `Removing`) via `impl_state_enum!`, `PeerOutboxRow { id, peer_id, mail_id, sender_seq, recipient, body, topic, created_at, attempts, next_attempt_at, state }`, `PeerOutboxState` (`Pending` | `InFlight` | `Delivered` | `Failed`), `PeerInboxRow { sender_daemon_id, sender_seq, mail_id, received_at }`, `TopicFederation { id, peer_id, topic, direction, created_at }`, `FederationDirection` (`Inbound` | `Outbound` | `Both`).
- `src/shared/protocol.rs` — `RpcRequest` gains `#[serde(default)] pub protocol_version: Option<u32>`. Add `StreamEvent` variants:
  - `PeerHandshakeOk { peer_id: PeerId, peer_daemon_id: DaemonId, peer_name: String }`
  - `PeerHandshakeFailed { peer_name: Option<String>, reason: String }`
  - `PeerStreamConnected { peer_id: PeerId }`
  - `PeerStreamDisconnected { peer_id: PeerId, reason: String }`
  - `PeerMailForwarded { peer_id: PeerId, mail_id: String, sender_seq: u64 }`
  - `PeerMailForwardFailed { peer_id: PeerId, mail_id: String, reason: String }`
  - `PeerMailReceived { peer_id: PeerId, mail_id: String, sender_daemon_id: DaemonId }`
  - `TopicFederationAdded { peer_id: PeerId, topic: String, direction: String }`
  - `TopicFederationRemoved { peer_id: PeerId, topic: String }`
  - Extend `MailReceived` and `MailDelivered` with `#[serde(default)] origin_daemon_id: Option<DaemonId>` (default `None`, no breakage for existing serialized events).
- `src/shared/protocol.rs` — RPC param/result structs: `PeerAddParams { name, url, bearer_token }`, `PeerAddResult { peer_id, daemon_id }`, `PeerListParams {}`, `PeerListResult { peers: Vec<PeerSummary> }`, `PeerSummary { peer_id, name, daemon_id, url, state, last_seen, outbox_depth }`, `PeerRemoveParams { name }`, `PeerPingParams { name }`, `PeerPingResult { rtt_ms: u64, state: String }`, `TopicFederateParams { topic, peer: String, direction: String }` and `TopicUnfederateParams { topic, peer: String }`.

**Detailed specification:**

Schema:
```sql
CREATE TABLE IF NOT EXISTS peers (
  id                  TEXT PRIMARY KEY,           -- local 8-hex peer row id
  daemon_id           TEXT NOT NULL,              -- the *remote* daemon's DaemonId
  name                TEXT NOT NULL UNIQUE,
  url                 TEXT NOT NULL,
  bearer_token_hash   BLOB NOT NULL,              -- Blake3(token)
  public_key          BLOB,                       -- reserved for mTLS phase
  state               TEXT NOT NULL,              -- Pending | Active | Down | Removing
  last_seen           INTEGER,                    -- unix secs of last Heartbeat or MailDeliver
  registered_at       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS peers_by_daemon_id ON peers(daemon_id);

CREATE TABLE IF NOT EXISTS peer_outbox (
  id              TEXT PRIMARY KEY,
  peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
  mail_id         TEXT NOT NULL,
  sender_seq      INTEGER NOT NULL,               -- monotonic per (peer_id), set at insert
  recipient       TEXT NOT NULL,                  -- federated address: agent://grimd-X/Y
  sender          TEXT,                           -- nullable; sender as user supplied
  topic           TEXT,                           -- non-null for fanned topic rows
  body            TEXT NOT NULL,
  created_at      INTEGER NOT NULL,
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL,
  state           TEXT NOT NULL                   -- Pending | InFlight | Delivered | Failed
);
CREATE INDEX IF NOT EXISTS peer_outbox_drain ON peer_outbox(peer_id, state, next_attempt_at);
CREATE UNIQUE INDEX IF NOT EXISTS peer_outbox_seq ON peer_outbox(peer_id, sender_seq);

CREATE TABLE IF NOT EXISTS peer_inbox (
  sender_daemon_id TEXT NOT NULL,
  sender_seq       INTEGER NOT NULL,
  mail_id          TEXT NOT NULL,
  received_at      INTEGER NOT NULL,
  PRIMARY KEY (sender_daemon_id, sender_seq)
);

CREATE TABLE IF NOT EXISTS topic_federations (
  id          TEXT PRIMARY KEY,
  peer_id     TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
  topic       TEXT NOT NULL,
  direction   TEXT NOT NULL,                      -- Inbound | Outbound | Both
  created_at  INTEGER NOT NULL,
  UNIQUE (peer_id, topic)
);
CREATE INDEX IF NOT EXISTS topic_federations_by_topic ON topic_federations(topic);
```

`peer_protocol_version` is a constant `pub const PEER_PROTOCOL_VERSION: u32 = 1;` on `src/shared/constants.rs`.

**Edge cases to handle:**
- Existing `mail.recipient_id` column already accepts arbitrary strings, no schema change needed.
- Cascade delete on `peer_outbox` from `peers` is required so `peer remove` cleans up.
- `peer_inbox` does **not** cascade — after `peer remove`, dedupe history is no longer needed for that daemon (a re-added peer with the same `daemon_id` resets the dedupe window).

**Acceptance criteria:**
- [ ] Fresh DB after `migrate()` contains the four new tables (verified by `SELECT name FROM sqlite_master`).
- [ ] An existing DB (pre-federation) round-trips through `migrate()` without dropping or altering existing rows.
- [ ] `peer_outbox` UNIQUE on `(peer_id, sender_seq)` — second insert with same key returns `rusqlite::ErrorCode::ConstraintViolation`.
- [ ] `peer_inbox` PK on `(sender_daemon_id, sender_seq)` — `INSERT OR IGNORE` makes the second insert a no-op.
- [ ] `RpcRequest` deserializes from JSON without `protocol_version` (existing CLI keeps working) and from JSON with `protocol_version: 1`.
- [ ] All new `StreamEvent` variants serialize with the existing `#[serde(rename = "snake_case")]` discipline.

**Contract tests (RED phase):**
- Test file: `tests/peer_schema.rs` (new) and `tests/protocol.rs` (extend).
- Tests to write before implementing:
  - `migrate_creates_peer_tables`
  - `peer_outbox_unique_seq_rejects_duplicate`
  - `peer_inbox_pk_dedupes`
  - `peers_cascade_deletes_outbox`
  - `rpc_request_protocol_version_optional` — JSON without the field deserializes; JSON with `1` deserializes; JSON with `999` deserializes (the *handler*, not the parser, rejects unknown versions — Task 4).

**Notes/Warnings:**
- Stamp `bearer_token_hash` as `BLOB`, not `TEXT`. Plaintext token never lands in the DB.

---

### Task 4: RPC validation + non-local rejection (slice 1 ship-gate)

**Summary:** All RPC methods accept federated addresses (Task 2's parser); `mail.send` rejects federated recipients with `federation_not_configured`; `RpcRequest.protocol_version` is validated.

**Dependencies:** Task 1, Task 2

**Files to create/modify:**
- `src/daemon/rpc.rs` — `handle_rpc` dispatcher: before the method match, check `req.protocol_version.unwrap_or(1)` is in `[1]`; otherwise `rpc_err(req.id, "unsupported_protocol_version")`.
- `src/daemon/rpc.rs` — `handle_mail_send`: after `parse_address`, on `Address::FederatedAgent`, return `rpc_err(req.id, "federation_not_configured")` for the duration of slice 1. Task 10 replaces this branch with real forwarding.
- `src/daemon/rpc.rs` — emit a clear `rpc_err(req.id, "invalid_federated_agent_id")` when the parser returns that variant.

**Detailed specification:**

This is the slice 1 ship-gate: after this lands, identity and address widening are observable, federation traffic is explicitly refused with a typed error rather than silently succeeding or panicking. Operators can roll out the address widening across daemons before any peer code is online.

**Edge cases to handle:**
- `mail.send` to `agent://grimd-<self>/<agent>` (own daemon-id, federated form): treat as local. Rewrite to `Address::Agent(agent_id)` before reaching the federation branch. Otherwise operators flipping CLIs to use federated form against a single daemon would be blocked.

**Acceptance criteria:**
- [ ] `mail.send` with `to = "agent://grimd-<other>/abcd1234"` returns `RpcError { code: "federation_not_configured" }` and does NOT insert a `mail` row.
- [ ] `mail.send` with `to = "agent://grimd-<self>/<existing-local-agent>"` succeeds and behaves identically to `to = "agent://<existing-local-agent>"`.
- [ ] `mail.send` with `to = "agent://grimd-XXXXXXXX/abcd"` returns `invalid_federated_agent_id`.
- [ ] An RPC request with `protocol_version: 999` returns `unsupported_protocol_version`.
- [ ] An RPC request with no `protocol_version` field (existing CLI) succeeds.

**Contract tests (RED phase):**
- Test file: `tests/federation_slice1.rs`
- Tests to write before implementing:
  - `mail_send_to_remote_daemon_rejected_with_federation_not_configured`
  - `mail_send_to_self_daemon_via_federated_address_succeeds`
  - `mail_send_with_invalid_federated_address_returns_invalid_federated_agent_id`
  - `unknown_protocol_version_rejected`

**Notes/Warnings:**
- The "self via federated form" rewrite happens in one place (top of `handle_mail_send`) — do not sprinkle the check across handlers.

---

### Task 5: `proto/peer.proto` and Peer service skeleton

**Summary:** Define the peer wire format and a tonic service stub mirroring the `WorkerControl` shape.

**Dependencies:** Task 3

**Files to create/modify:**
- `proto/peer.proto` — new file. Service `Peer { rpc Channel(stream PeerOutbound) returns (stream PeerInbound) }`. Messages per plan: `Hello`, `HelloAck`, `Heartbeat`, `HeartbeatAck`, `MailDeliver`, `MailAck`, `TopicSubscribe`, `TopicUnsubscribe`, `Goodbye`. Two oneofs: `PeerOutbound { Hello | Heartbeat | MailDeliver | MailAck | TopicSubscribe | TopicUnsubscribe | Goodbye }` (sender → receiver) and `PeerInbound { HelloAck | HeartbeatAck | MailDeliver | MailAck | TopicSubscribe | TopicUnsubscribe | Goodbye }` (the receiving side relays its own outbound traffic over the same channel — symmetric oneofs).
- `build.rs` — extend the existing `tonic_build::configure().compile_protos(&[…], &[…])` call to include `proto/peer.proto`.
- `src/shared/peer_proto.rs` (new) — re-export the generated module, mirroring `worker_proto.rs`.
- `src/daemon/peer_rpc_server.rs` (new) — `PeerService` struct (fields: `Arc<PeerRegistry>`, `Arc<Database>`, `EventBus`, `DaemonId`). Implement `Peer::channel` as a Tonic async fn that:
  1. Reads the first message; if not `Hello`, returns `Status::invalid_argument`.
  2. Defers handshake validation to Task 7 (stub returns `Status::unimplemented` for now).

**Detailed specification:**

Proto layout (key fields only, full proto in source):

```proto
message Hello {
  string daemon_id = 1;          // sender's DaemonId
  uint32 protocol_version = 2;
  string bearer_token = 3;
  repeated string advertised_topics = 4;
}
message HelloAck {
  string daemon_id = 1;          // receiver's DaemonId
  uint32 protocol_version = 2;
  bool accepted = 3;
  string reject_reason = 4;
}
message MailDeliver {
  string mail_id = 1;
  string sender = 2;             // federated form; receiver parses
  string recipient = 3;          // federated form
  string body = 4;
  optional string topic = 5;
  uint64 sender_seq = 6;
}
message MailAck {
  string mail_id = 1;
  bool ok = 2;
  string reason = 3;             // populated when !ok
}
```

`PeerOutbound` and `PeerInbound` use identical oneof shapes — symmetric protocol, the names just disambiguate per-side at the type level.

**Edge cases to handle:**
- `tonic_build` panics on unknown proto syntax — keep `proto3` and avoid `optional` outside oneof for primitives we want to differentiate "absent" on (use a oneof for those, mirroring `assign_task::OptionalResumeSessionId` in `worker.proto`).

**Acceptance criteria:**
- [ ] `cargo build` compiles the new proto and re-exports it via `src/shared/peer_proto.rs`.
- [ ] `PeerService::channel` reads the first message; if `Hello`, returns `Status::unimplemented` (handshake completed in Task 7); if anything else, returns `Status::invalid_argument` with reason `first message must be Hello`.
- [ ] Server registration is wired into `server.rs` behind a `peer_listen_addr: Option<SocketAddr>` config field — `None` (default) skips the listener so existing tests don't have to bind a port.

**Contract tests (RED phase):**
- Test file: `tests/peer_proto.rs`
- Tests to write before implementing:
  - `peer_channel_first_message_must_be_hello` — sends `Heartbeat` first, asserts `Status::invalid_argument`.
  - `peer_channel_hello_returns_unimplemented_pre_task_7` — temporary; this test gets replaced by the handshake test in Task 7.

**Non-testable items:**
- `build.rs` proto wiring (covered transitively by Task 6 building successfully).

**Notes/Warnings:**
- Resist adding event/agent-introspection messages to v1 — they're explicitly post-v1.

---

### Task 6: Peer outbound client + reconnect loop

**Summary:** Per-peer outbound stream actor that connects, sends `Hello`, runs heartbeat + select loop, reconnects with backoff on disconnect.

**Dependencies:** Task 5

**Files to create/modify:**
- `src/daemon/peer_client.rs` (new) — `PeerClient { peer_id, url, bearer_token, daemon_id, clock, bus, send_tx, recv_rx, shutdown_rx }` with `pub async fn run(self) -> Result<()>` modelled on `grimw/rpc_client.rs`.
- `src/daemon/peer_registry.rs` (new) — `PeerRegistry { Arc<Mutex<HashMap<PeerId, PeerHandle>>>, db, bus, clock, daemon_id }`. `PeerHandle { send_tx: mpsc::Sender<PeerOutbound>, abort: oneshot::Sender<()>, state: Arc<Mutex<PeerState>> }`. `pub async fn ensure_connected(&self, peer: &Peer) -> Result<()>` spawns the client task and stashes the handle.

**Detailed specification:**

The client task's main loop:
1. Connect with `tonic::transport::Endpoint::from_shared(url).connect()`. Reject `https://` with `peer_tls_not_supported_yet`.
2. Open the bidirectional stream by sending `Hello { daemon_id, protocol_version: 1, bearer_token, advertised_topics }`.
3. Wait for `HelloAck`. On `accepted: false`, emit `PeerHandshakeFailed`, return; the registry will not retry until operator runs `peer ping` or modifies the row. (Handshake details in Task 7.)
4. Mark `peers.state = Active` via `db.set_peer_state(peer_id, Active)`. Emit `PeerStreamConnected`.
5. Spawn heartbeat task (interval `peer_heartbeat_interval_secs`).
6. Select loop on `(inbound_message, send_rx, shutdown_rx)`. On any inbound `MailDeliver` or `MailAck`, push onto `recv_tx` (consumed by the inbox handler in Task 9). On stream drop or heartbeat timeout, exit and reconnect after backoff.
7. Backoff is exponential per the ambiguity table (1s..60s), reset on every successful `HelloAck`.

**Edge cases to handle:**
- Daemon shutdown mid-stream: shutdown channel closes, client sends `Goodbye { reason: "shutdown" }`, drains in-flight `MailAck` rows, exits.
- Connect-time DNS failure: counts as a disconnect, contributes to backoff.
- Peer is in `Removing` state when reconnect timer fires: client task exits cleanly, registry removes the row.

**Acceptance criteria:**
- [ ] On a fresh `Peer { state: Pending }`, `PeerRegistry::ensure_connected` spawns a client task that immediately attempts a connect.
- [ ] On stream drop, the client task waits the backoff (driven by `Clock`) and reconnects.
- [ ] After 6 consecutive failures, the next attempt waits 60s (backoff cap).
- [ ] On daemon shutdown, the client task sends `Goodbye` and exits within 1s.
- [ ] `https://` URLs return `peer_tls_not_supported_yet` and the client never enters the loop.

**Contract tests (RED phase):**
- Test file: `tests/peer_reconnect.rs`
- Tests to write before implementing:
  - `peer_client_reconnects_after_drop` — uses an in-process tonic server that drops the stream after `HelloAck`; assert the client retries.
  - `peer_client_backoff_caps_at_60s` — `TestClock` advances; assert next-attempt time.
  - `peer_client_rejects_https_url`
  - `peer_client_sends_goodbye_on_shutdown`

**Notes/Warnings:**
- Keep the heartbeat loop on the same `tokio::select!` — losing a heartbeat is the only way the client *learns* about a silently dead peer.

---

### Task 7: Handshake (Hello / HelloAck) with auth, version, ID checks

**Summary:** Server-side handshake: validate bearer token against stored hash, check `protocol_version`, reject daemon-id collisions, persist `Active` state.

**Dependencies:** Task 5, Task 6

**Files to create/modify:**
- `src/daemon/peer_rpc_server.rs` — replace the Task 5 stub with the full handshake. Compute Blake3 hash of incoming `bearer_token`, look up `peers WHERE bearer_token_hash = ?` (constant-time-comparison-irrelevant since the lookup is hash-equality on a 32-byte BLOB indexed via a unique constraint we add now). On match: validate `protocol_version`, validate `daemon_id` matches the row's `daemon_id` (or row is `Pending` and we set it now). On mismatch: send `HelloAck { accepted: false, reject_reason: <code> }`, close stream.
- `src/daemon/persistence.rs` — `lookup_peer_by_token_hash(hash: &[u8]) -> Result<Option<Peer>>`, `set_peer_state(peer_id, state)`, `set_peer_last_seen(peer_id, ts)`, `update_peer_daemon_id(peer_id, daemon_id)`.

**Detailed specification:**

Handshake decision table:

| Incoming `Hello` field | Stored `peers` row | Action |
|---|---|---|
| token hash matches no row | — | `HelloAck { accepted: false, reject_reason: "invalid_token" }`, close. |
| version not in `[1]` | — | `HelloAck { accepted: false, reject_reason: "unsupported_protocol_version" }`, close. |
| token row found, row `daemon_id` is empty (Pending) | row.state = Pending | Persist `daemon_id` on row, set state Active, send `HelloAck { accepted: true }`. |
| token row found, `daemon_id` matches row | — | Set state Active, last_seen=now, send `HelloAck { accepted: true }` (legitimate reconnect). |
| token row found, `daemon_id` mismatch | — | `HelloAck { accepted: false, reject_reason: "peer_daemon_id_collision" }`, close. |
| `Hello.daemon_id` collides with **another** peer's row | — | `HelloAck { accepted: false, reject_reason: "peer_daemon_id_collision" }`, close. |

Emit `PeerHandshakeOk` or `PeerHandshakeFailed` on the bus.

**Edge cases to handle:**
- Token-hash UNIQUE constraint: enforce `UNIQUE` on `peers.bearer_token_hash` so the lookup is well-defined and operators can't accidentally reuse a token across peer entries. Migration adds the constraint; if existing rows would violate it, fail boot with `duplicate_peer_token` (no auto-resolve).
- Concurrent handshakes from the same peer: second arrival wins, first stream is dropped server-side via the registry.

**Acceptance criteria:**
- [ ] `Hello` with unknown token returns `HelloAck { accepted: false, reject_reason: "invalid_token" }` and the row is unchanged.
- [ ] `Hello` with known token but wrong `daemon_id` returns `HelloAck { accepted: false, reject_reason: "peer_daemon_id_collision" }`.
- [ ] `Hello` with `protocol_version = 999` returns `HelloAck { accepted: false, reject_reason: "unsupported_protocol_version" }`.
- [ ] Successful handshake: `peers.state` flips to `Active`, `last_seen` is set, `PeerHandshakeOk` is emitted on the bus.
- [ ] Outbound side (Task 6) observes `HelloAck { accepted: true }` and proceeds to the heartbeat loop.

**Contract tests (RED phase):**
- Test file: `tests/peer_handshake.rs`
- Tests to write before implementing:
  - `handshake_rejects_invalid_token`
  - `handshake_rejects_unsupported_version`
  - `handshake_rejects_daemon_id_collision`
  - `handshake_promotes_pending_peer_to_active`
  - `handshake_accepts_legitimate_reconnect`

**Notes/Warnings:**
- Plaintext bearer token is in memory only on the connection task — never log it, never persist it.

---

### Task 8: Outbox drainer (mail forwarding loop)

**Summary:** Per-peer drainer task picks `Pending` outbox rows, sends `MailDeliver`, marks `InFlight`, transitions to `Delivered` on `MailAck { ok: true }`, retries on failure with exponential backoff.

**Dependencies:** Task 7

**Files to create/modify:**
- `src/daemon/peer_outbox.rs` (new) — `OutboxDrainer { peer_id, db, send_tx, ack_rx, clock, bus }`. `pub async fn run(self) -> Result<()>` loop:
  1. `db.next_outbox_row(peer_id, now)?` — `SELECT … WHERE peer_id = ? AND state = 'Pending' AND next_attempt_at <= ? ORDER BY sender_seq LIMIT 1`.
  2. If row: mark `InFlight`, send `PeerOutbound::MailDeliver` over `send_tx`, await `MailAck` on `ack_rx` with timeout (per-row `peer_handshake_timeout_secs * 3`).
  3. On `MailAck { ok: true }`: `state = Delivered`, emit `PeerMailForwarded`.
  4. On `MailAck { ok: false }` or timeout / stream drop: `attempts += 1`, set `next_attempt_at = now + backoff(attempts)`, state back to `Pending`, emit `PeerMailForwardFailed` (with reason).
- `src/daemon/persistence.rs` — `next_outbox_row`, `mark_outbox_in_flight`, `mark_outbox_delivered`, `mark_outbox_failed_retry`, `outbox_depth(peer_id)`.

**Detailed specification:**

Drainer is a single task per peer (not a worker pool). Sequential delivery preserves `sender_seq` ordering, which the inbox dedupe relies on for monotonicity but does not require for correctness.

Backoff function: `backoff(attempts) = min(2.pow(attempts - 1), 60)` seconds, with `attempts >= 1`.

The drainer reads from a `Notify` (or short poll) so newly-inserted outbox rows are picked up promptly without busy-waiting.

**Edge cases to handle:**
- Stream up but daemon-side bug rejects every `MailDeliver`: depth grows toward `peer_outbox_max_depth`; `mail.send` fails fast at cap (Task 10).
- Daemon restart with `InFlight` rows: boot reconciliation flips them back to `Pending` (those `MailDeliver`s may have been received — but the receiver dedupes on `(sender_daemon_id, sender_seq)`, so re-send is a no-op).
- `peers.state == Removing`: drainer exits without writing further updates.

**Acceptance criteria:**
- [ ] A `Pending` row is sent as `MailDeliver` and, on `MailAck { ok: true }`, transitions to `Delivered` with `attempts = 1`.
- [ ] A `MailAck { ok: false, reason: "X" }` sets `attempts += 1`, `next_attempt_at` per backoff schedule, state back to `Pending`, and emits `PeerMailForwardFailed { reason: "X" }`.
- [ ] After 6 consecutive failures, `next_attempt_at` is exactly `now + 60s` (backoff cap).
- [ ] Boot reconciliation flips all `peers[*].state=Active` outbox rows from `InFlight` back to `Pending`.
- [ ] `peers.state == Removing` halts the drainer within one tick.

**Contract tests (RED phase):**
- Test file: `tests/peer_outbox_durability.rs`
- Tests to write before implementing:
  - `outbox_pending_to_delivered_on_ack_ok`
  - `outbox_failure_retries_with_backoff` (uses `TestClock`)
  - `outbox_in_flight_resets_to_pending_on_boot`
  - `outbox_halts_on_peer_removing`

**Notes/Warnings:**
- Do NOT delete `Delivered` rows automatically — keep them as audit trail until peer remove. (Operators can `VACUUM` separately.)

---

### Task 9: Inbox handler (dedupe + local insert + wake)

**Summary:** Inbound `MailDeliver` lands a row in `peer_inbox` (idempotency-keyed), then writes the local `mail` row, then emits `MailReceived` and triggers wake-on-mail through the existing scheduler path.

**Dependencies:** Task 7

**Files to create/modify:**
- `src/daemon/peer_inbox.rs` (new) — `InboxHandler { db, bus, daemon_id }`. `pub fn handle_mail_deliver(&self, peer: &Peer, msg: &MailDeliver) -> Result<MailAck>`:
  1. Body length check; if `> MAX_MAIL_BODY_BYTES`, return `MailAck { ok: false, reason: "body_too_large" }`.
  2. Parse `recipient`. Must be `Address::FederatedAgent { daemon_id: <self>, agent_id }` *or* `Address::Topic`. Reject anything else with `invalid_recipient`.
  3. Look up local agent (or topic subscribers). If recipient agent doesn't exist, return `MailAck { ok: false, reason: "unknown_recipient" }`.
  4. Open IMMEDIATE transaction:
     a. `INSERT OR IGNORE INTO peer_inbox(sender_daemon_id, sender_seq, mail_id, received_at)`. If `changes == 0`: this is a replay. Return `MailAck { ok: true }` *without* re-inserting `mail`.
     b. Insert `mail` row with `sender_id = msg.sender`, `recipient_id = <local agent id>`, etc. Same shape as local `handle_direct_send` / `handle_topic_send`.
     c. Commit.
  5. Emit `PeerMailReceived` and `MailReceived { …, origin_daemon_id: Some(peer.daemon_id) }`. Scheduler picks up wake-on-mail next tick (no new plumbing).
  6. Return `MailAck { ok: true }`.

**Detailed specification:**

For topic delivery, `recipient` is `topic://<name>`; the inbox handler looks up local subscribers and writes one `mail` row per subscriber via the existing `insert_mail_batch` path. The `peer_inbox` row is keyed on the *publisher's* `(sender_daemon_id, sender_seq)` so the entire fanout is replay-safe under one key.

`MailAck { ok: true }` is sent after the txn commits. If the daemon crashes between txn commit and ack send, the next reconnect will receive a replay; the inbox dedupe keeps it safe.

**Edge cases to handle:**
- Body precisely at `MAX_MAIL_BODY_BYTES`: accept (`<=` not `<`).
- Replay during normal operation (e.g. ack lost): `INSERT OR IGNORE` returns `changes == 0`; ack with `ok: true` so the sender progresses.
- Recipient agent banished between send and receive: `MailAck { ok: false, reason: "recipient_banished" }`. Consistent with local mail-send behavior.

**Acceptance criteria:**
- [ ] First `MailDeliver` for `(daemon, seq)` inserts `peer_inbox` and `mail`, emits both `PeerMailReceived` and `MailReceived`.
- [ ] Replayed `MailDeliver` (same `daemon, seq`) returns `MailAck { ok: true }` without inserting a duplicate `mail` row.
- [ ] `MailReceived` event carries `origin_daemon_id: Some(<sender>)`.
- [ ] Recipient is local-Dormant agent: scheduler's next `tick_mail_wake` wakes the agent (existing path, exercised end-to-end).
- [ ] Body of `MAX_MAIL_BODY_BYTES + 1` returns `MailAck { ok: false, reason: "body_too_large" }`.

**Contract tests (RED phase):**
- Test file: `tests/peer_idempotency.rs` and `tests/peer_inbox_wake.rs`
- Tests to write before implementing:
  - `inbox_inserts_local_mail_for_first_delivery`
  - `inbox_dedupes_replayed_delivery`
  - `inbox_wakes_dormant_recipient_via_existing_scheduler`
  - `inbox_rejects_oversize_body`
  - `inbox_rejects_unknown_recipient`

**Notes/Warnings:**
- Reuse `handle_direct_send` body-construction logic via a private helper if at all possible — the inbox path should not be a divergent copy.

---

### Task 10: Mail-send routing for federated recipients

**Summary:** `mail.send` to a federated recipient writes a `peer_outbox` row in the same IMMEDIATE transaction as the `mail` row. Outbox cap returns a typed error.

**Dependencies:** Task 4, Task 8

**Files to create/modify:**
- `src/daemon/rpc.rs` — replace the slice-1 `federation_not_configured` branch with the real federated send path. New helper `handle_federated_direct_send(db, bus, peer_registry, req, params, daemon_id, agent_id, wake_eligible) -> RpcResponse`.
- `src/daemon/persistence.rs` — `pub fn insert_mail_with_outbox(mail: &Mail, outbox_row: &PeerOutboxRow) -> Result<()>` — single IMMEDIATE txn that inserts both rows. Computes `sender_seq` per `peer_id` via `MAX(sender_seq) + 1` query inside the txn.
- `src/daemon/persistence.rs` — `pub fn outbox_depth(&self, peer_id: &str) -> Result<u64>`.

**Detailed specification:**

Path:
1. Resolve `daemon_id` to a `Peer`. If no peer registered: `peer_unknown_for_recipient`.
2. If `peer.state == Removing`: `peer_removing`.
3. Check `outbox_depth(peer_id) >= peer_outbox_max_depth`: return `peer_outbox_full`.
4. Build `Mail` row with `state = Pending`, `delivered_at = None`, `recipient_id = "agent://grimd-<daemon_id>/<agent_id>"` (federated form preserved).
5. Build `PeerOutboxRow { state: Pending, attempts: 0, next_attempt_at: now }`.
6. `insert_mail_with_outbox` (single txn).
7. Emit `MailSent { mail_id, sender_id, recipient_id: federated, topic: None }` on the bus. Drainer picks up the outbox row asynchronously.
8. Notify the drainer (`Notify::notify_one()`) so it picks up immediately.

**Edge cases to handle:**
- Caller passes federated form for own daemon (`grimd-<self>`): already rewritten to local in Task 4.
- Race between `peer remove` and `mail.send`: the txn observes `peers.state` consistently. If `Removing` mid-flight, the row insert proceeds but the drainer ignores it; eventually `peer_outbox` is cascade-deleted.

**Acceptance criteria:**
- [ ] `mail.send` to a federated recipient on a known peer returns `MailSendResult { delivered: 1 }` and creates exactly one `mail` row + one `peer_outbox` row, both in the same transaction (verified by stopping the daemon between the API call and the bus publish — the row count is 1+1 or 0+0, never 1+0).
- [ ] `mail.send` to a federated recipient on an unknown daemon returns `peer_unknown_for_recipient`.
- [ ] `mail.send` at outbox cap returns `peer_outbox_full`.
- [ ] The drainer picks up the new row within one tick.
- [ ] `MailSent` event is emitted; `MailReceived` is NOT emitted on the sending daemon (that's the receiver's job).

**Contract tests (RED phase):**
- Test file: `tests/peer_mail_routing.rs`
- Tests to write before implementing:
  - `mail_send_federated_writes_mail_and_outbox_atomically`
  - `mail_send_federated_unknown_peer_rejected`
  - `mail_send_federated_at_cap_returns_outbox_full`
  - `mail_send_federated_self_via_full_form_routes_locally` (regression of Task 4 behavior in the new code path)

**Notes/Warnings:**
- The full e2e (forward → ack → delivered on far daemon) is Task 13. This task only verifies the local routing.

---

### Task 11: `grim peer add/list/remove/ping` (CLI + RPC)

**Summary:** Operator surface for managing peers. RPC handlers + CLI subcommand.

**Dependencies:** Task 7

**Files to create/modify:**
- `src/cli/commands/peer.rs` (new) — `PeerCommand` enum (`Add { name, url, token }`, `List`, `Remove { name }`, `Ping { name }`), `pub async fn run(cmd: PeerCommand) -> Result<()>` dispatcher per existing CLI patterns.
- `src/cli/commands/mod.rs` — `pub mod peer;`
- `src/main.rs` — `Peer { #[command(subcommand)] cmd: peer::PeerCommand }`.
- `src/daemon/rpc.rs` — `peer.add`, `peer.list`, `peer.remove`, `peer.ping` handlers. Wired into `handle_rpc` dispatcher.
- `src/daemon/peer_registry.rs` — `register_peer`, `list_peers_with_outbox_depth`, `remove_peer`, `ping_peer` methods on `PeerRegistry`.

**Detailed specification:**

`peer add`:
1. Validate `name` shape, `url` scheme (Task 6 validation), `token` shape.
2. Insert `peers` row with `state = Pending`, `bearer_token_hash = blake3(token)`.
3. Call `PeerRegistry::ensure_connected`. Wait up to `peer_handshake_timeout_secs`.
4. On handshake success: return `PeerAddResult { peer_id, daemon_id }`.
5. On timeout / handshake failure: keep the row (state=Pending or Down) and return the failure code; operator can retry with `peer ping`.

`peer list`:
- Returns one row per peer with `outbox_depth` computed inline from `peer_outbox` (uses the index).

`peer remove`:
1. Set `peers.state = Removing`.
2. Drainer halts (Task 8).
3. Send `Goodbye` to the remote (best-effort).
4. Cascade-delete `peer_outbox` and `topic_federations` via FK ON DELETE CASCADE.
5. Delete the `peers` row.
6. `mail` rows are retained.

`peer ping`:
- If state=Active, send a `Heartbeat` and time the `HeartbeatAck`. Return RTT.
- If state=Down or Pending, force a reconnect attempt and return resulting state.

**Acceptance criteria:**
- [ ] `peer add` on a reachable peer with valid token returns `PeerAddResult` and `peers.state = Active`.
- [ ] `peer add` with invalid token completes handshake, gets `HelloAck { accepted: false }`, returns `peer_handshake_failed { reason: "invalid_token" }`, and leaves no `peers` row written. (Token-rejected handshakes do NOT leave Pending rows.)
- [ ] `peer add` with unreachable URL returns `peer_handshake_timeout`. Row is **also** rolled back.
- [ ] `peer list` shows live `outbox_depth` per peer.
- [ ] `peer remove` deletes the `peers` row and cascades `peer_outbox`/`topic_federations` but retains `mail` rows for that recipient.
- [ ] `peer ping` on an Active peer returns RTT > 0.

**Contract tests (RED phase):**
- Test file: `tests/peer_cli_rpc.rs` and `tests/peer_remove.rs`
- Tests to write before implementing:
  - `peer_add_happy_path`
  - `peer_add_invalid_token_returns_no_row`
  - `peer_add_unreachable_url_times_out`
  - `peer_list_includes_outbox_depth`
  - `peer_remove_cascades_outbox_retains_mail`
  - `peer_ping_returns_rtt`

**Non-testable items:**
- Help-text rendering for the CLI subcommand (covered by `clap` defaults).

---

### Task 12: Federated topics — `grim topic federate` + fanout into outbox

**Summary:** Add `topic_federations` rows via `topic.federate` RPC; `handle_topic_send` writes one `peer_outbox` row per remote subscriber as part of the same fanout transaction; receive side re-fans into local subscribers.

**Dependencies:** Task 8, Task 10

**Files to create/modify:**
- `src/cli/commands/topic.rs` (new) — `TopicCommand { Federate { topic, peer, direction }, Unfederate { topic, peer } }`.
- `src/cli/commands/mod.rs` — `pub mod topic;`
- `src/main.rs` — `Topic { #[command(subcommand)] cmd: topic::TopicCommand }`.
- `src/daemon/rpc.rs` — `topic.federate`, `topic.unfederate` handlers.
- `src/daemon/rpc.rs::handle_topic_send` — extend to also enumerate `topic_federations` rows for direction in (`Outbound`, `Both`); for each, append a `peer_outbox` row to the same IMMEDIATE transaction as `insert_mail_batch`. New helper: `insert_mail_batch_with_outbox`.
- `src/daemon/peer_inbox.rs::handle_mail_deliver` — when `recipient` is `topic://<name>`, validate `topic_federations` row exists with direction in (`Inbound`, `Both`). If not, `MailAck { ok: false, reason: "topic_federation_not_authorized" }`.
- Subscription propagation: when `topic.federate` adds an `Inbound`/`Both` row, send `TopicSubscribe { topic }` to the remote so it knows to start mirroring publishes. `Unfederate` sends `TopicUnsubscribe`. The remote logs and treats this advisory message as an *authorization signal* — it does NOT auto-create a federation row on receipt; the operator must federate symmetrically.

**Detailed specification:**

Direction semantics:
- `Outbound`: local publishes are mirrored to the remote.
- `Inbound`: accept mirrored publishes from the remote.
- `Both`: shorthand for both.

`TopicSubscribe` is advisory. The plan calls out: each side runs its own `topic federate`. The advisory message just lets a remote operator notice they should federate back.

Reserved-prefix block: `workspace/`, `supervisor/`, `wake/` topic names cannot be federated (matches the existing reserved sender prefixes).

**Edge cases to handle:**
- Federated topic with zero remote subscribers: outbound mirror still attempts a `MailDeliver` if the federation row exists; receiving side fans out to its local subscribers (potentially zero). The `peer_outbox` row carries `topic` set so the receiver routes correctly.
- Conflict: existing local row says `Outbound`, operator runs `topic federate --direction inbound`: row is updated to `Both` (idempotent merge — no error).
- Unfederate while outbox rows exist for that topic: rows continue to drain (they reference `peer_id`, not the federation row). The federation row deletion is purely "stop generating new ones."

**Acceptance criteria:**
- [ ] `topic federate --topic X --peer P --direction outbound` writes a `topic_federations` row.
- [ ] After federation: local `mail.send topic://X` writes one `peer_outbox` row to peer P in the same txn as the local subscriber fanout.
- [ ] Receiving side with `Inbound`/`Both` federation: a `MailDeliver { topic: Some(X) }` from peer P fans out to local subscribers of X. With no federation row: returns `topic_federation_not_authorized`.
- [ ] Topics under reserved prefixes (`workspace/`, `supervisor/`, `wake/`) are rejected at `topic.federate`.
- [ ] Direction merge is idempotent: federate Outbound then Inbound results in a single row with direction Both.

**Contract tests (RED phase):**
- Test file: `tests/peer_topic_federation.rs`
- Tests to write before implementing:
  - `topic_federate_writes_federation_row`
  - `topic_publish_with_outbound_federation_writes_outbox_row`
  - `topic_inbound_without_authorization_rejected`
  - `topic_federation_reserved_prefix_rejected`
  - `topic_federation_direction_merge_is_idempotent`

**Notes/Warnings:**
- The fanout transaction is the only place where the outbox-cap check might be circumvented (one publish to an N-subscriber topic adds N rows). Apply the cap *per peer* by checking `outbox_depth + new_rows_for_this_peer` before commit; abort the whole txn with `peer_outbox_full` if any peer would exceed.

---

### Task 13: End-to-end two-daemon integration tests

**Summary:** Pair of in-process `grimd` instances with distinct `~/.grimoire` roots and `daemon_id`s, exercising the full mail-forwarding and topic-federation paths.

**Dependencies:** 9, 10, 11, 12

**Files to create/modify:**
- `tests/peer_e2e.rs` (new) — two-daemon harness builder; tests for full mail send→deliver→wake; topic federation full path; peer remove during in-flight outbox.
- `tests/common/mod.rs` (new or extended) — `TwoDaemonHarness { a, b, peer_a_to_b, peer_b_to_a }` builder. Each daemon gets a tempdir `~/.grimoire`, distinct `GRIMOIRE_DAEMON_ID_PATH`, distinct UDS path, distinct peer listen port. Both share a `TestClock`.
- `tests/peer_address_parser.rs` — small file mirroring the plan; exercises `parse_address` matrix end-to-end via `mail.send` errors.

**Detailed specification:**

Tests:
1. `e2e_direct_mail_round_trip` — A summons agent `aa`; `peer add B` from A; `peer add A` from B (symmetric); B summons agent `bb` (Dormant via `--keep-alive`); from A: `mail.send agent://grimd-<B>/<bb> "hi"`. Assert: B's scheduler wakes `bb`, `MailReceived` fires on B with `origin_daemon_id = <A>`.
2. `e2e_outbox_durability_across_restart` — kill A's peer client mid-send (via `peer remove --force` then re-add — or by halting the client task directly), assert `peer_outbox` row stays `Pending`/`InFlight`, restart, assert delivery completes.
3. `e2e_idempotent_replay` — synthetically replay the last `MailDeliver` (sit on the client side and re-send), assert `peer_inbox` UNIQUE keeps `mail` row count stable.
4. `e2e_topic_federation` — federate `pr-opened` Outbound on A, Inbound on B; B subscribes a Dormant agent to `topic://pr-opened`; A publishes; assert delivery on B, wake fires.
5. `e2e_peer_remove_drains_outbox` — A has 5 pending outbox rows for B; `peer remove B`; assert rows cascaded, `mail` rows retained on A.

All tests use `TestClock` for backoff timing and `tempfile::tempdir()` for `~/.grimoire` isolation.

**Edge cases to handle:**
- The harness must avoid socket-path / port collisions when run with `--test-threads`; use `port: 0` (OS-assigned) and a per-test tempdir.

**Acceptance criteria:**
- [ ] All five tests pass under `cargo test --test peer_e2e`.
- [ ] Each test uses `TestClock` for backoff timing — no real-time `sleep` in test code.
- [ ] No flakes in 100 consecutive runs (verified locally before merge).

**Contract tests (RED phase):**
- Test file: `tests/peer_e2e.rs`
- This *is* the contract-test file for end-to-end behavior. Each scenario above is one test function.

**Notes/Warnings:**
- This is the gate before any slice-2 or slice-3 work merges to main. Slice 1 (Tasks 1–4) merges independently after Task 4's tests pass.

---

## Testing Strategy (TDD)

Implementation follows red-green TDD: for each task, the implementer writes failing contract tests from the acceptance criteria (RED), then implements the minimum code to make them pass (GREEN). Contract tests are immutable once committed.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `tests/daemon_id.rs` | 5 tests | none |
| 2 | `src/shared/mail.rs` (extend) | 5 tests | none |
| 3 | `tests/peer_schema.rs`, `tests/protocol.rs` | 5 tests | none |
| 4 | `tests/federation_slice1.rs` | 4 tests | none |
| 5 | `tests/peer_proto.rs` | 2 tests | `build.rs` proto wiring |
| 6 | `tests/peer_reconnect.rs` | 4 tests | none |
| 7 | `tests/peer_handshake.rs` | 5 tests | none |
| 8 | `tests/peer_outbox_durability.rs` | 4 tests | none |
| 9 | `tests/peer_idempotency.rs`, `tests/peer_inbox_wake.rs` | 5 tests | none |
| 10 | `tests/peer_mail_routing.rs` | 4 tests | none |
| 11 | `tests/peer_cli_rpc.rs`, `tests/peer_remove.rs` | 6 tests | clap help text |
| 12 | `tests/peer_topic_federation.rs` | 5 tests | none |
| 13 | `tests/peer_e2e.rs`, `tests/peer_address_parser.rs` | 5 e2e + address matrix | none |

### Integration Testing

Task 13 is the integration suite. Cross-task assertions:
- Outbox + Inbox + Scheduler integrate so that an inbound `MailDeliver` for a Dormant agent on the receiving daemon results in a wake within one scheduler tick.
- `peer remove` cascades through `peer_outbox` and `topic_federations` but never touches `mail` rows.
- Restart-time reconciliation of `InFlight` outbox rows produces correct delivery without duplicates.

### Manual Testing Checklist

- [ ] Two laptops, real network: `grim peer add` works with a non-loopback URL.
- [ ] `grim status` on each shows the correct `daemon_id`.
- [ ] Kill peer-B mid-`mail.send` flood; assert peer-A's outbox grows; restart peer-B; assert outbox drains.
- [ ] `grim peer list` shows `outbox_depth` increasing/decreasing under load.
- [ ] `grim topic federate pr-opened --peer B --direction outbound` on A; corresponding command on B; publish on A, observe receive on B.
- [ ] `grim peer remove B` mid-flight: subsequent `mail.send` to `grimd-B/...` returns `peer_unknown_for_recipient`.

## Rollout Considerations

### Feature Flags

No runtime flag. Slicing handles staged rollout:
- **Slice 1 (Tasks 1–4)** ships independently. After deploy, daemons mint `daemon_id`, accept federated address strings, and reject federated traffic with `federation_not_configured`. Operators see the new ID but no behavioral change otherwise.
- **Slice 2 (Tasks 5–11)** adds the peer link and direct mail forwarding. `grim peer add` is the entry point; without it, slice-1 behavior is unchanged.
- **Slice 3 (Task 12)** adds federated topics. Requires `grim topic federate`.

A new daemon talking to an old daemon (across `peer.proto` upgrades) is gated by the `protocol_version` field; mismatches are explicit `unsupported_protocol_version`.

### Migration Strategy

- `migrate()` is idempotent and adds the four new tables via `IF NOT EXISTS`. No data migration.
- First boot of a new daemon mints `~/.grimoire/daemon.id` once. Subsequent boots no-op.
- Existing `mail` rows are unaffected: their `recipient_id` strings remain bare-form for local mail, federated-form for any new federated mail.
- `peer_inbox` UNIQUE constraint on `(sender_daemon_id, sender_seq)`: fresh table, no migration risk.

### Rollback Plan

- Slice 1: rollback restores the binary. The minted `daemon.id` file persists on disk and is harmless if unused.
- Slice 2: `peer remove` for all peers before downgrading. Old binaries treat `peer_outbox` / `peers` tables as foreign and ignore them. Inbound peer connections fail (binary doesn't host the listener), which is correct for "rolled back" state.
- Slice 3: `topic unfederate` for all rows before downgrading.
- All rollbacks preserve historical `mail` rows.

## Open Items

- [ ] **mTLS for peer channel** (post-v1, Slice 3 polish). Gated on the worker channel mTLS work landing first; until then `https://` URLs are rejected with a typed error.
- [ ] **Cross-peer wake sources** (post-v1, plan non-goal). Re-examine after Task 13 when we have real two-laptop usage data.
- [ ] **Cross-peer `scry`** (post-v1, plan non-goal). Returns `scry_local_only` until then.
- [ ] **Outbox/inbox GC cadence** — `Delivered` outbox rows accumulate. v1 keeps them as audit; revisit when on-disk size becomes a complaint.
- [ ] **Token rotation UX** — current path is `peer remove` + `peer add`. A `peer rotate-token` would be cheaper but is post-v1.

---

*This spec is implementation-ready. Each task is designed for red-green TDD. Tasks can be picked up independently (respecting dependencies) and completed in a single iteration.*
