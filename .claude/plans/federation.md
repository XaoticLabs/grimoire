# Federation — Plan

**Status:** draft (v1 scope)
**Owner:** Cory
**Roadmap link:** Part 3 §11 / Part 5 spine #8 (final milestone)

---

## Goal

Two `grimd` instances peer with each other so that an agent on daemon-A can `mail.send` to an agent on daemon-B (and to topics shared across the pair). All one fabric, but with explicit, named peer links — not a gossip mesh.

The thesis test: this is only possible because agents are addressable, long-lived processes. A library orchestrator can't peer with another library orchestrator.

---

## Non-goals (v1)

Hold the line on these — they're each their own milestone.

- **Scrolls spanning daemons.** Tasks stay pinned to the daemon that owns the scroll. No `task: { daemon: peer-b }` directive yet.
- **Federated workspaces.** Workspaces remain local-fs only. `memory.*` does not sync across peers. (Cross-host workspaces is its own Part 3 §7 v2 item.)
- **Federated supervision trees.** A supervisor cannot supervise a child on a remote daemon.
- **Transitive routing.** A→B→C delivery is out. Peers are point-to-point; if daemon-A wants to reach daemon-C it peers with C directly.
- **Service discovery / gossip.** Peers are configured by hand (CLI / config). No mDNS, no DHT.
- **Cross-peer wake sources.** `wake add` cannot register a wake source against a remote agent in v1.

---

## Prerequisites (must land before federation v1 ships)

These are already on the roadmap but currently un-done. Federation should not be the thing that ships them, but federation depends on them and the work order matters.

1. **Auth tokens on UDS + HTTP** (Part 1 hardening item #1). The local UDS is currently unauthenticated; opening a daemon to network peers without first solving the local auth story is backwards.
2. **Protocol versioning** (Part 1 #1). `RpcRequest` has no version field; we need one before we add a second protocol surface that has to negotiate compat.
3. **mTLS for the worker channel** (Part 2 §1 open). Federation will reuse the same TLS substrate — solving it once for `grimd ↔ grimw` and `grimd ↔ grimd` together is cheaper than twice.

If those slip, federation should slip with them. Don't bolt federation onto an unauthed UDS.

---

## Identity model

The single biggest change. Today an agent address is `agent://[0-9a-f]{8}`, minted from a UUID v4 prefix in `src/shared/constants.rs:41`. That breaks under federation: two daemons mint into the same 8-hex space and collisions are inevitable over time.

**Proposal: introduce a `DaemonId` and a federated address scheme.**

- `DaemonId` = stable 8-hex, generated once on first `grimd` boot, persisted to `~/.grimoire/daemon.id`. Display form: `grimd-<8hex>`.
- Federated agent address: `agent://<daemon-id>/<agent-id>` (e.g. `agent://grimd-1a2b3c4d/deadbeef`).
- **Backwards compat:** a bare `agent://[0-9a-f]{8}` is parsed as "local daemon, this agent id." So existing CLI, scrolls, and pacts keep working unchanged. The address parser in `src/shared/mail.rs` gets a third arm.
- Topics stay un-prefixed (`topic://<name>`) but are scoped per-daemon by default. Cross-peer topic sharing is opt-in (see "Federated topics" below).

**Schema changes:**

- New `peers` table (peer_id, name, url, bearer_token_hash, public_key, state, last_seen, registered_at).
- New `peer_inbox` and `peer_outbox` tables for at-least-once mail forwarding (see "Mail forwarding").
- `mail.recipient_id` and `mail.sender_id` columns become "address strings" — already strings today, so no column change, but the parser/validator widens to accept the federated form.

---

## Transport

Reuse the Tonic gRPC + bearer token pattern from `grimw` (`src/daemon/worker_rpc_server.rs:34`, `src/grimw/rpc_client.rs:19`). It already exists, it already has bidirectional streams, and using one stack for both peering surfaces means one mTLS rollout, one auth audit, one connection reaper.

- New proto: `proto/peer.proto`. Service `Peer { Channel(stream PeerOutbound) returns (stream PeerInbound) }`.
- Auth: bearer token at handshake (Phase 1) → mTLS (Phase 2, gated on the worker mTLS work).
- Handshake exchanges `DaemonId`, `peer_protocol_version`, advertised topics. Either side may reject on version mismatch.
- One persistent stream per peer; daemon retries with backoff on disconnect (mirror `grimw`'s reconnect loop).

`PeerInbound` / `PeerOutbound` message kinds (v1):

- `Hello` / `HelloAck` (handshake)
- `Heartbeat` / `HeartbeatAck`
- `MailDeliver { mail_id, sender, recipient, body, topic?, seq }`
- `MailAck { mail_id, status }` (for at-least-once)
- `TopicSubscribe { topic, subscriber_address }` / `TopicUnsubscribe`
- `Goodbye { reason }`

Everything else (event federation, agent introspection across peers) is post-v1.

---

## Mail forwarding

When `mail.send` is called with a recipient whose daemon-id is non-local:

1. **Local insert into `peer_outbox`** (atomic with the existing `mail` insert, in the same transaction). Status: `Pending`. This makes forwarding crash-safe — if `grimd` dies mid-forward, the outbox replays on boot.
2. **Outbox drainer task** per peer link picks up `Pending` rows, sends `MailDeliver` over the gRPC stream, marks `InFlight`.
3. On `MailAck { ok }`: mark `Delivered`. On `MailAck { failed }` or stream drop: leave `Pending` for retry with exponential backoff.
4. Receiving side: `MailDeliver` lands in `peer_inbox` (idempotency-keyed on `(sender_daemon_id, sender_seq)`), then writes the actual `mail` row, then emits `MailReceived` and triggers wake-on-mail through the existing scheduler path (`src/daemon/scheduler.rs:112`). No new wake plumbing — it's just another way mail rows arrive.
5. The whole pipeline emits the existing `StreamEvent::MailSent / Delivered / Failed` variants on both daemons, with the `sender_id` / `recipient_id` strings now potentially federated. Existing event consumers (dashboard, `grim bind`) keep working; they just see longer addresses.

**Idempotency:** the `(sender_daemon_id, sender_seq)` tuple is a UNIQUE index on `peer_inbox`, so a re-delivered `MailDeliver` is a no-op. This is how "at-least-once + dedupe" gives us effectively-once.

**Backpressure:** outbox has a per-peer cap (`max_outbox_depth`, default 10k). Hitting the cap returns a `mail.send` error to the caller — better than silent unbounded growth.

---

## Federated topics

Subscriptions today live in a local `subscriptions` table (`src/shared/mail.rs`, schema in `persistence.rs`). For v1, topic federation is **opt-in and explicit**:

- New CLI: `grim topic federate <topic> --peer <name>`. This installs a row that says "for any local publish to `<topic>`, also forward as a `MailDeliver` to peer `<name>`'s subscribers, and accept `TopicSubscribe { topic, ... }` from that peer."
- Without that opt-in, topics are local-only.
- Why opt-in? Topics are cheap to create and a noisy topic on one daemon shouldn't auto-spam a peer. Also avoids the "should we federate the system stream?" question.
- The fanout transaction (`insert_mail_batch` in `rpc.rs:682`) already does an atomic per-recipient insert; the change is that for federated topics, remote subscribers also get one row inserted into `peer_outbox` in the same transaction.

---

## CLI surface

- `grim peer add <name> --url <grpc-url> --token <secret>` — register a peer, persist token hash, attempt handshake.
- `grim peer list` — show peers, last_seen, stream state, outbox depth.
- `grim peer remove <name>` — tear down stream, retain mail history (don't cascade-delete `mail` rows; do cascade `peer_outbox`/`peer_inbox`).
- `grim peer ping <name>` — manual handshake/health check.
- `grim topic federate <topic> --peer <name>` / `grim topic unfederate <topic> --peer <name>`.
- `grim mail send agent://grimd-xxx/yyyy "body"` — already works once the address parser widens.

No changes to `grim summon`, `grim scry`, `grim circle`, `grim wake`, `grim workspace`, `grim memory`. Federation is purely additive at the user surface.

---

## Failure modes & their answers

| Failure | Behavior |
|---|---|
| Peer unreachable at `mail.send` time | Outbox row stays `Pending`, drainer retries with backoff. `mail.send` still succeeds locally. |
| Peer permanently gone | Outbox grows until cap, then `mail.send` errors. Operator runs `peer remove` to drain. |
| Both peers send conflicting topic-federation requests | Last-write-wins on the local row. Symmetric setup is the operator's job — we don't auto-mirror federation config. |
| Daemon-id collision (two daemons happen to mint the same 8-hex) | Handshake rejects on `Hello { daemon_id }` if we already have a peer with that ID under a different name. 8 hex = 4B space; if we ever get bitten in practice, widen to 12. |
| Token leaked | `peer remove` invalidates locally; the leaked token is per-peer-link, scoped, and the peer can rotate via `peer add` again. No ambient credentials. |
| Mail body contains injection / oversized payload | Reuse the existing 16 KiB cap and validation from the local mail path. Federation is just more of the same plumbing — don't reinvent the limits. |

---

## Test plan

Mirror the structure of `tests/worker_proto.rs` + `tests/scheduler_mail_wake.rs`:

- `tests/peer_handshake.rs` — handshake happy path, version mismatch reject, bad token reject, daemon-id collision reject.
- `tests/peer_mail_forward.rs` — two in-process daemons (different `~/.grimoire` roots), `mail.send` from A→B, assert recipient row appears on B, `MailReceived` fires on B, scheduler wakes B's recipient if dormant.
- `tests/peer_outbox_durability.rs` — kill the link mid-send, assert `Pending` row persists, restart, assert delivery completes and `Delivered`.
- `tests/peer_idempotency.rs` — replay the same `MailDeliver`, assert `peer_inbox` UNIQUE keeps it from double-inserting into `mail`.
- `tests/peer_topic_federation.rs` — federate a topic, publish on A, assert subscribed agent on B receives.
- `tests/peer_address_parser.rs` — bare `agent://xxxxxxxx` still parses as local; `agent://grimd-xxx/yyyy` parses as federated; junk rejected.
- `tests/peer_remove.rs` — removal drains outbox, retains historical mail, tears down stream.

All tests use a `FakeClock` (already in `src/daemon/clock.rs`) for backoff timing and a pair of `tempdir`-rooted daemons for isolation.

---

## Phasing

Don't ship this as one PR. Three slices:

**Slice 1 — Identity + address parser widening.**
`DaemonId` minted on boot, persisted, exposed in `grim status`. Address parser accepts the federated form, all RPC methods accept federated addresses but reject any non-local one with a clear error ("federation not configured"). No transport yet. This is mergeable on its own and lets us roll out the address widening before any peer code exists.

**Slice 2 — Peer link + direct mail forwarding.**
`peer.proto`, `Peer` service, `peers` / `peer_outbox` / `peer_inbox` tables, `grim peer add/list/remove/ping`, the outbox drainer, the inbox dedupe. Direct (non-topic) mail crosses daemons. Topics still local.

**Slice 3 — Federated topics + polish.**
`grim topic federate`, fanout into outbox for federated topics, dashboard surfacing of peer state and outbox depth, mTLS once the worker side has it.

---

## Open questions (resolve before slice 2)

1. **DaemonId format** — 8 hex, or borrow Erlang's `name@host` style (`grimd@laptop.local`)? Hex is uniform with agent IDs and avoids DNS coupling; named is friendlier in CLI output. Probably hex internally + an optional human-readable `name` field on `peers`.
2. **Should federated topics deduplicate at the publisher or subscriber side?** Publisher-side (one outbox row per remote subscriber, computed in the fanout txn) matches the local model exactly. Subscriber-side (one outbox row to "the topic on peer X", peer X re-fans out) is cheaper on the wire but introduces a new asymmetry. **Lean publisher-side** for v1 — symmetry beats efficiency until we have a measurement saying otherwise.
3. **Do federated `MailReceived` events on the receiving daemon need to carry the originating `DaemonId` separately, or is the parsed `sender_id` address enough?** Probably enough — but if we ever want filtering by source daemon in `grim bind`, an explicit field is cheap.
4. **What does `grim scry agent://grimd-xxx/yyyy` show?** v1 answer: nothing — agent introspection is local-only. Cross-peer `scry` is post-v1 and opens the question of which peer's events are authoritative.

---

## Out-of-band: what this unlocks

Federation is the last roadmap-spine item, but it sets up two future stories cheaply:

- **Cloud burst.** A laptop daemon peers with an ephemeral cloud daemon spawned on demand; heavy work mails over, results mail back, cloud daemon despawns. The peer-add/peer-remove lifecycle already covers it.
- **Team fabric.** Multiple devs each running `grimd`, peered in a small mesh, sharing review topics. The "standing review team" demo (Part 4) becomes a team feature instead of a single-laptop one.

Neither needs scroll-spanning federation, which is why holding scrolls out of v1 still gets us the marquee demos.
