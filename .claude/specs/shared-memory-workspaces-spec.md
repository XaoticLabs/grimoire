# Implementation Spec: Shared Memory + Workspaces (v1)

> Generated from: `.claude/plans/shared-memory-workspaces.md`
> Generated on: 2026-04-27

## Overview

Ship three composing primitives that let multi-agent scrolls actually collaborate without prompt-engineering glue: a daemon-owned **workspace** (named git worktree), a workspace-scoped **memory KV** (SQLite-backed, optimistic CAS), and a workspace-scoped **filewatcher** (notify watcher publishing to existing topic-subscription mail plumbing). The unifying noun is *workspace*: it bundles a worktree (no-trample), a memory namespace (shared scratch), and a filewatch root (observability).

The unlock is concrete: two roadmap demos — *swarm decompose* and *standing review team* — become buildable end-to-end on top of the existing supervision/dormant/mail infrastructure. v1 is single-user, cooperative agents, no RO ACL, no GC, no vector store. Reuse `wake_registry` actor shape, existing `EventBus`, existing topic subscription / mail-wake plumbing, and existing `notify v6` debounce pattern.

## Technical Context

### Relevant Codebase Areas

- `src/daemon/persistence.rs` — `Database` (single `Mutex<Connection>`, WAL mode). New tables go in `migrate()` (lines 80–141 pattern); column adds use the `SELECT col FROM table LIMIT 0` guard before `ALTER TABLE ADD COLUMN`.
- `src/daemon/event_bus.rs` — `EventBus::publish(StreamEvent)` fans out to broadcast subscribers and persists via mpsc-driven `db.append_event` task.
- `src/shared/protocol.rs` — `StreamEvent` enum (lines 322+). All new event variants land here. RPC param/result structs colocated.
- `src/daemon/wake_registry.rs` — Actor template: `Arc<Self>` with handle map, fire channel (`mpsc::Sender<FireMsg>`, capacity 256), publishes lifecycle events. The `WorkspaceRegistry` mirrors this shape.
- `src/daemon/wake_sources/file_watch.rs` — `notify::RecommendedWatcher` wrapper with `DEBOUNCE_MS = 200`, glob+ignore filtering, `MatchedChange { path }` channel, `MAX_WATCH_PATHS = 1000`. Inherit defaults verbatim.
- `src/daemon/agent_manager.rs` — `resolve_cwd` (lines 155–158); `enqueue_with_options` (lines 258–304). Workspace short-circuits cwd; assignment row written in same scope as agent insert.
- `src/daemon/rpc.rs` — `handle_rpc` dispatcher (lines 22–56). `parse_params<T>` helper (lines 17–20). `rpc_err(req.id, &str_code)` for typed errors.
- `src/daemon/scheduler.rs` — `tick_mail_wake` (lines 329–389) wakes dormant agents subscribed to topics. Reuse — no new wake-source kind needed.
- `src/daemon/scroll_parser.rs` — Markdown-based scroll parser. Add top-level `- workspace: <name>` directive after the `# Scroll:` heading.
- `src/cli/commands/mail.rs` — Subcommand pattern. Mirror in new `src/cli/commands/workspace.rs` and `src/cli/commands/memory.rs`.
- `src/shared/config.rs` — `DaemonConfig` (lines 59–76). Add `workspace_value_cap_bytes` and `workspace_total_cap_bytes`.

### Existing Patterns to Follow

- **Migration guard**: `let has_x: bool = conn.prepare("SELECT x FROM t LIMIT 0").is_ok(); if !has_x { ALTER TABLE … }` — used for all `agents` column adds; use identical guard for `agents.workspace_id`.
- **Actor with SQLite + handles + EventBus**: `WakeRegistry` is the closest sibling. `WorkspaceRegistry` follows the same `Arc<Self>` + `Mutex<HashMap<...>>` shape.
- **Optimistic CAS via per-key version**: `mail.seq` is monotonic per-recipient. `workspace_memory.version` is monotonic per `(workspace_id, key)` — increment-on-write, compare on `expected_version`.
- **Topic-as-mail**: `topic://workspace/<name>/files` and `topic://workspace/<name>/memory/<key-prefix>` ride the existing `mail` table + `subscriptions` table + `tick_mail_wake` plumbing. No new wake-source kind.
- **Reserved sender prefix**: `mail.send` rejects `wake://` and `supervisor://` from user RPC (rpc.rs:454). Add `workspace://` to the rejection list; daemon-internal mail emitters use `workspace://<id>` as `sender_id` so subscribers see provenance.
- **CLI command pattern**: `MailCommand` enum + `run(cmd)` dispatcher + per-handler `DaemonClient::connect().await?.call("method", json!({…}))`.

### Key Dependencies

- `notify = "6"` (already in `Cargo.toml`) — wrap `RecommendedWatcher` per `file_watch.rs`.
- `globset = "0.4"` — used by file_watch for glob+ignore matching.
- `git` CLI — shell out via `tokio::process::Command::new("git").arg("worktree").arg("add")...`. Args passed individually (no shell interpolation). Same hardening as provider spawns.
- Existing `Database`, `EventBus`, `WakeRegistry`, `AgentManager`, `ScrollKeeper`, `ScrollParser`.

### Ambiguity Resolutions

| Area | Ambiguity | Resolution | Source |
|------|-----------|------------|--------|
| Filewatch event shape | Batched (one event per debounce window with N paths) vs fanned (one per file) | **Batched**: `WorkspaceFileChanged { workspace_id, paths: Vec<String>, kinds: Vec<String> }` per debounce window, max 64 paths per event (overflow event signals "+N more"). Mirrors notify's natural debounced output and limits bus pressure. | Plan flag — assumed default |
| Memory value type | bytes vs JSON | **JSON-only at API/RPC layer; bytes underneath in SQLite BLOB.** `memory.put` accepts `value: serde_json::Value`; `memory.get` returns `value: serde_json::Value`. CLI accepts `--json <inline>` or `@file` and validates parse. | Plan recommendation — assumed default |
| Cross-host workspaces | Worker pool: does a remote `grimw` need a checkout? | **Punt v1**: workspace-assigned agents must run on workers that share the daemon's filesystem. Worker registry filters: a worker is eligible only if it has the same `hostname` as the daemon (or a future-but-not-yet `--workspace-share` flag). RPC error `WorkspaceCrossHost` if no eligible worker. | Plan recommendation — assumed default |
| `--copy-from <other-workspace>` | Cheap fork for swarm-decompose | **Defer to v2.** Schema does not encode parent_id; v2 can add a nullable `parent_workspace_id` without migration churn. | Plan flag — assumed default |
| Memory key shape | What characters allowed? | `[a-zA-Z0-9._\-/]{1,256}`. Forward slash supported (key prefix subscriptions like `findings/auth/*`). Reject leading/trailing slash, double slash, empty segment. | Inferred from `topic://workspace/<name>/memory/<prefix>` plan example |
| Workspace name shape | Plan says `[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}` | Adopted verbatim. Lowercase recommended but not enforced. | Plan |
| Memory value cap | "256 KiB" | `workspace_value_cap_bytes` config, default `262144` (256 KiB). Per-workspace total cap default `67108864` (64 MiB), config `workspace_total_cap_bytes`. Both checked on `memory.put` after JSON canonicalization. | Plan + reasonable defaults |
| Memory key namespace topic mapping | "topic://workspace/<name>/memory/<key-prefix>" — what's the prefix granularity? | One topic per **key segment prefix**: writing `findings/auth` publishes to `topic://workspace/<name>/memory/findings`, `topic://workspace/<name>/memory/findings/auth`, and the wildcard `topic://workspace/<name>/memory/*`. Subscribers pick granularity. Three publishes per write, capped. | Inferred from plan |
| Boot reconciliation: orphan dir disposition | Plan says "log + leave" | Verbatim: emit `WorkspaceOrphanDirDetected { path }` event (new variant), log warning, surface in `grim workspace list --orphans`. Never auto-delete. | Plan |
| Boot reconciliation: orphan row disposition | Plan says "mark Destroying and clean up" | Set `state='Destroying'`, attempt `git worktree remove --force` (noop if path missing), emit `WorkspaceDestroyed`, delete row. Cascading FK removes `workspace_memory` and `workspace_assignments`. | Plan |
| `git worktree add` failure | Plan says surface verbatim | RPC returns `RpcError { code: "git_worktree_add_failed", message: <stderr verbatim, truncated to 4 KiB> }`. No DB row written. | Plan |
| Concurrency: two `workspace.create` for same name | Plan implicit | Name has UNIQUE constraint on `workspaces.id`. Second insert fails with `WorkspaceAlreadyExists`; the second `git worktree add` is never invoked because name validation + DB check happens first inside a transaction. | Inferred — needed for atomicity |
| `summon --workspace` for a Destroying workspace | Plan says refuse with `WorkspaceDestroying` | Verbatim. State check is part of the assign transaction. | Plan |

## Implementation Tasks

### Summary

| Task | Name | Dependencies | Estimated Complexity |
|------|------|--------------|---------------------|
| 1 | Schema, types, and StreamEvent variants | None | Medium |
| 2 | Memory KV with CAS and topic emission | 1 | Medium |
| 3 | WorkspaceRegistry: create + list + boot reconciliation | 1 | High |
| 4 | WorkspaceRegistry: destroy + assign + state machine | 3 | Medium |
| 5 | WorkspaceWatcher: per-workspace notify + topic emission | 3 | Medium |
| 6 | CLI: `grim workspace` and `grim memory` subcommands | 2, 4 | Low |
| 7 | `summon --workspace` and scroll `workspace:` field | 4 | Medium |
| 8 | End-to-end integration tests | 2, 4, 5, 7 | Medium |

### Critical Path

```
1 ──┬──► 2 ─────────────► 6 ─┐
    │                         │
    └──► 3 ──► 4 ──┬──► 5 ──┐ │
                   │         ├─┴─► 8
                   └──► 7 ──┘
```

Parallelizable: {2, 3} after 1; {5, 6, 7} after 4 (with 5 also depending on 3 directly).

---

### Task 1: Schema, types, and StreamEvent variants

**Summary:** Add the three new tables, the `agents.workspace_id` column, the Rust types backing them, and the new `StreamEvent` variants. Foundational change: every later task imports from this one.

**Dependencies:** None

**Files to create/modify:**
- `src/daemon/persistence.rs` — extend `migrate()` with `CREATE TABLE IF NOT EXISTS workspaces / workspace_memory / workspace_assignments`; add guarded `ALTER TABLE agents ADD COLUMN workspace_id`. Add insert/select methods stubs (signatures only — bodies live with consumers in tasks 2–4 if needed, but pure CRUD methods can land here).
- `src/shared/types.rs` — add `Workspace`, `WorkspaceState` (`Active` | `Destroying`), `WorkspaceId`, `MemoryEntry`, `MemoryKey` (newtype with validation). Add `workspace_id: Option<WorkspaceId>` to `Agent`.
- `src/shared/protocol.rs` — add `StreamEvent` variants: `WorkspaceCreated { workspace_id, path, branch }`, `WorkspaceDestroyed { workspace_id }`, `WorkspaceOrphanDirDetected { path }`, `MemoryWritten { workspace_id, key, version, agent_id }`, `MemoryDeleted { workspace_id, key, agent_id }`, `WorkspaceFileChanged { workspace_id, paths: Vec<String>, kinds: Vec<String>, truncated_count: u32 }`. Add RPC param/result structs for all 8 RPC methods named in the plan (`workspace.create/list/destroy/assign`, `memory.put/get/list/delete`).
- `src/shared/constants.rs` — add `MAX_WORKSPACE_NAME_LEN = 64`, `MAX_MEMORY_KEY_LEN = 256`, `WORKSPACE_NAME_REGEX`, `MEMORY_KEY_REGEX`.

**Detailed specification:**

Schema (all `CREATE TABLE IF NOT EXISTS`, all in `Database::migrate()`):

```sql
CREATE TABLE IF NOT EXISTS workspaces (
    id          TEXT PRIMARY KEY,
    path        TEXT NOT NULL UNIQUE,
    repo_path   TEXT NOT NULL,
    branch      TEXT NOT NULL,
    state       TEXT NOT NULL,        -- 'Active' | 'Destroying'
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_memory (
    workspace_id TEXT NOT NULL,
    key          TEXT NOT NULL,
    value        BLOB NOT NULL,
    version      INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    updated_by   TEXT NOT NULL,        -- agent_id or 'system'
    PRIMARY KEY (workspace_id, key),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS workspace_memory_by_prefix
    ON workspace_memory (workspace_id, key);

CREATE TABLE IF NOT EXISTS workspace_assignments (
    workspace_id TEXT NOT NULL,
    agent_id     TEXT NOT NULL,
    assigned_at  INTEGER NOT NULL,
    PRIMARY KEY (workspace_id, agent_id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
    FOREIGN KEY (agent_id)     REFERENCES agents(id)
);
CREATE INDEX IF NOT EXISTS workspace_assignments_by_agent
    ON workspace_assignments (agent_id);
```

`agents.workspace_id` add (after the existing column-add guards):

```rust
let has_workspace_id: bool = conn.prepare("SELECT workspace_id FROM agents LIMIT 0").is_ok();
if !has_workspace_id {
    conn.execute_batch("ALTER TABLE agents ADD COLUMN workspace_id TEXT;")?;
}
```

`Workspace` struct (chrono-or-i64 timestamps consistent with other types):

```rust
pub struct Workspace {
    pub id: WorkspaceId,
    pub path: PathBuf,
    pub repo_path: PathBuf,
    pub branch: String,
    pub state: WorkspaceState,
    pub created_at: DateTime<Utc>,
}
pub type WorkspaceId = String;
pub enum WorkspaceState { Active, Destroying }
```

`MemoryKey::parse(s) -> Result<Self, MemoryKeyError>` enforces regex `^[a-zA-Z0-9._\-]+(/[a-zA-Z0-9._\-]+)*$`, length ≤ 256, no leading/trailing/double slash, ≥ 1 segment. Returned segments via `.segments() -> &[String]`.

`WorkspaceId::parse(s) -> Result<Self, NameError>` enforces `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`.

**Edge cases to handle:**
- Existing databases without `workspace_id` column — guarded `ALTER TABLE` adds it (NULL for existing rows).
- `workspaces.path` UNIQUE prevents two rows pointing at the same dir.
- `workspaces.id` primary key prevents duplicate names.
- `WorkspaceId` rejects `..`, names starting with `.`, names with path separators.
- `MemoryKey` rejects empty string, single `/`, `a//b`, `/a`, `a/`.

**Acceptance criteria:**
- [ ] Opening a fresh `Database` creates `workspaces`, `workspace_memory`, `workspace_assignments` tables, and `agents` has a `workspace_id` column.
- [ ] Opening a `Database` whose file pre-exists from before this change adds the `workspace_id` column to `agents` without dropping data.
- [ ] `WorkspaceId::parse("ok-name_1")` returns `Ok`; `parse("../escape")`, `parse(".hidden")`, `parse("a b")`, and a 65-char name return `Err`.
- [ ] `MemoryKey::parse("findings/auth")` returns `Ok` with `segments() == &["findings", "auth"]`; `parse("/x")`, `parse("x/")`, `parse("a//b")`, `parse("")`, and a 257-char key return `Err`.
- [ ] `serde_json::to_string` on each new `StreamEvent` variant produces JSON with the expected discriminator and round-trips back to an equal value.
- [ ] `workspace_memory` cascade-deletes when its parent `workspaces` row is deleted (DB-level FK with `ON DELETE CASCADE`, `PRAGMA foreign_keys=ON` already set).

**Contract tests (RED phase):**
- Test file: `tests/workspaces_schema.rs`
- Tests:
  - `fresh_db_has_workspace_tables` — open in-memory DB, query `sqlite_master`, assert all three tables present.
  - `existing_db_gets_workspace_id_column_via_alter` — open file-backed DB pre-populated with old `agents` schema (no `workspace_id`), reopen, assert column added and existing rows preserved.
  - `workspace_id_validation_accepts_valid_names` — table-driven assertions on `WorkspaceId::parse`.
  - `workspace_id_validation_rejects_invalid_names` — table-driven, including the path-traversal cases.
  - `memory_key_validation_roundtrip` — accepts and rejects per spec.
  - `stream_event_variants_serde_roundtrip` — each new variant.
  - `workspace_memory_cascades_on_workspace_delete` — insert workspace + memory rows, delete workspace, assert memory rows gone.

**Non-testable items:**
- The new RPC param/result struct definitions in `protocol.rs` (no behavior, only shapes — they get exercised in tasks 2–4).
- `STREAM_EVENT` variants beyond serde roundtrip (downstream tasks exercise emission).

**Notes/Warnings:**
- `Database::open` already enables `PRAGMA foreign_keys=ON`. The cascade test depends on this.
- Don't store `Workspace.path` as relative — canonicalize at create time and persist absolute. Validate on read that it lives under `~/.grimoire/workspaces/`.

---

### Task 2: Memory KV with CAS and topic emission

**Summary:** Implement the four memory RPC methods (`memory.put/get/list/delete`) with optimistic CAS, value-size cap enforcement, `MemoryWritten`/`MemoryDeleted` event emission, and topic-mail publishing for memory subscriptions.

**Dependencies:** 1

**Files to create/modify:**
- `src/daemon/persistence.rs` — add `memory_put_cas`, `memory_get`, `memory_list_prefix`, `memory_delete_cas`, `memory_total_size_for_workspace` methods on `Database`.
- `src/daemon/rpc.rs` — register `memory.put`, `memory.get`, `memory.list`, `memory.delete` in `handle_rpc` dispatcher; implement handlers.
- `src/daemon/event_bus.rs` (or a new `src/daemon/memory_publisher.rs`) — small helper that, on a successful write, emits the `MemoryWritten` `StreamEvent` AND publishes topic-mail to `topic://workspace/<id>/memory/<segment-prefix>` for each ancestor key segment plus the wildcard `topic://workspace/<id>/memory/*`.
- `src/shared/config.rs` — add `workspace_value_cap_bytes` (default 262144) and `workspace_total_cap_bytes` (default 67108864) to `DaemonConfig`.

**Detailed specification:**

`memory.put` flow:
1. Parse `MemoryPutParams { workspace_id, key, value: serde_json::Value, expected_version: Option<u64>, sender: Option<AgentId> }`.
2. Validate workspace exists in `Active` state. If not: `RpcErr("workspace_not_found" | "workspace_destroying")`.
3. Validate key via `MemoryKey::parse`.
4. Canonicalize value to bytes via `serde_json::to_vec(&value)`. Reject if > `workspace_value_cap_bytes`: `RpcErr("memory_value_too_large", current_size, cap)`.
5. Compute new total = current total − existing row size (if any) + new size. Reject if > `workspace_total_cap_bytes`: `RpcErr("memory_total_cap_exceeded")`.
6. Inside a transaction: `SELECT version FROM workspace_memory WHERE workspace_id=? AND key=?`. If `expected_version` is `Some(v)`:
   - Existing row absent and `v != 0` → `CasConflict { current_version: 0 }` (version 0 = "did not exist").
   - Existing row present with version `cur != v` → `CasConflict { current_version: cur }`.
7. UPSERT with `version = COALESCE(old_version, 0) + 1`, `updated_at = now`, `updated_by = sender.unwrap_or("system")`.
8. Emit `StreamEvent::MemoryWritten { workspace_id, key, version, agent_id }`.
9. Publish topic mail (see below).
10. Return `MemoryPutResult { version }`.

Topic mail emission (helper used by both put and delete):
- For key `"findings/auth/token"`, publish to **three** topics:
  - `topic://workspace/<id>/memory/findings`
  - `topic://workspace/<id>/memory/findings/auth`
  - `topic://workspace/<id>/memory/findings/auth/token`
- Plus the catch-all `topic://workspace/<id>/memory/*`.
- Cap total topic publishes per write at the segment-count + 1 (no exponential blowup; max key segments × 1).
- Each publish reuses the existing `handle_topic_send` body code path (refactor into `publish_topic(db, bus, topic, body, sender, wake_eligible)` shared helper) — sender is `workspace://<id>`, body is small JSON `{"key":"...","version":N,"op":"put"|"delete"}`, `wake_eligible: true`.
- Add `workspace://` to the reserved-sender-prefix rejection in `handle_mail_send` (rpc.rs:454) so user RPC cannot spoof daemon writes.

`memory.get`: returns `{ value, version }` or `RpcErr("memory_not_found")`. Returns the latest version present.

`memory.list`: `MemoryListParams { workspace_id, prefix: Option<String> }` → `[{key, version, updated_at, value_size}]`. Prefix matches anchored to key start (segment-aligned: prefix `"a"` matches `"a"` and `"a/b"` but NOT `"abc"`). Implementation: SQL `WHERE key = ? OR key LIKE ? || '/%'`.

`memory.delete`: same CAS flow as put, but DELETE. Emits `MemoryDeleted` and topic mail with `op: "delete"`. Returns `{}` or `CasConflict`.

**Edge cases to handle:**
- `expected_version: Some(0)` on a non-existing key → success creates with version 1.
- `expected_version: None` on `put` → unconditional last-writer-wins (still bumps version).
- `expected_version: None` on `delete` → unconditional delete; idempotent (deleting a nonexistent key returns `Ok` and emits no event — document this).
- Workspace deleted mid-RPC → transaction sees `Active` state and proceeds; cascade is fine. The window is bounded by the destroy state-machine in task 4.
- Per-value cap enforced after JSON canonicalization (so the user can't sneak past with verbose unicode escapes; canonical form is what we store).
- Total-cap pre-check is best-effort under contention; the put transaction itself does not lock the whole table. Acceptable at v1 scale (single-user).

**Acceptance criteria:**
- [ ] `memory.put` with no `expected_version` on a fresh key returns `version: 1`; a subsequent `memory.get` returns the same value and `version: 1`.
- [ ] `memory.put` with `expected_version: Some(1)` after a successful version-1 write succeeds and returns `version: 2`.
- [ ] `memory.put` with `expected_version: Some(99)` on a key currently at version 1 returns `RpcErr` with code `cas_conflict` and includes `current_version: 1` in error data.
- [ ] `memory.put` with a value canonicalizing to `> workspace_value_cap_bytes` returns `RpcErr("memory_value_too_large")` and writes nothing.
- [ ] `memory.put` against an unknown workspace returns `RpcErr("workspace_not_found")`.
- [ ] `memory.put` against a `Destroying` workspace returns `RpcErr("workspace_destroying")`.
- [ ] `memory.list` with `prefix: Some("findings")` returns rows for `findings`, `findings/auth`, and `findings/auth/token` — but NOT `findingsX` or `unrelated`.
- [ ] After a successful `memory.put`, the `EventBus` receives one `MemoryWritten` event with the right `workspace_id`, `key`, `version`, and `agent_id`.
- [ ] After `memory.put` of key `findings/auth`, an agent subscribed to `topic://workspace/<id>/memory/findings` receives a mail row with `sender_id = "workspace://<id>"` and a JSON body containing the key, version, and `op: "put"`.
- [ ] An agent subscribed to `topic://workspace/<id>/memory/*` receives mail for any memory write in that workspace.
- [ ] Calling `mail.send` with `sender = "workspace://foo"` from user RPC returns `RpcErr("reserved_sender_prefix")`.
- [ ] `memory.delete` of an existing key publishes a `MemoryDeleted` event and topic mail with `op: "delete"`.

**Contract tests (RED phase):**
- Test file: `tests/memory_kv.rs`
- Tests:
  - `put_creates_row_at_version_1`
  - `put_with_correct_expected_version_bumps`
  - `put_with_stale_expected_version_returns_cas_conflict_with_current_version`
  - `put_with_no_expected_version_is_unconditional`
  - `put_oversized_value_rejected_and_table_unchanged`
  - `put_in_destroying_workspace_rejected`
  - `put_emits_memory_written_event`
  - `put_publishes_to_segment_prefix_topics`
  - `put_publishes_to_wildcard_topic`
  - `put_with_workspace_sender_prefix_via_user_rpc_rejected`
  - `list_with_prefix_segment_aligned`
  - `delete_with_cas_conflict`
  - `delete_emits_memory_deleted_event`
  - `delete_of_missing_key_is_noop_no_event`
  - `total_cap_exceeded_returns_typed_error`

**Non-testable items:**
- Adding `workspace://` to the reserved-prefix list (covered by the user-RPC test above, but the actual list edit is wiring).
- Refactor of `handle_topic_send` body into a shared helper (no behavior change beyond what tests already cover for mail).

**Notes/Warnings:**
- The total-cap query `SELECT SUM(LENGTH(value)) FROM workspace_memory WHERE workspace_id = ?` can be slow at large N. v1 scale is fine; flag for v2.
- Topic publish per-write is currently sync (3-ish DB inserts per write). If profiling later shows this is hot, batch the inserts inside one transaction. Don't optimize prematurely.
- Idempotent delete (no event on missing key) avoids spurious wakes; document in the RPC reference.

---

### Task 3: WorkspaceRegistry — create + list + boot reconciliation

**Summary:** Add `WorkspaceRegistry` actor (mirroring `WakeRegistry` shape) handling `workspace.create` (validates name, shells out to `git worktree add`, inserts row, emits event) and `workspace.list`. Implement boot-time reconciliation that scans `~/.grimoire/workspaces/`, logs orphan dirs, and cleans orphan rows.

**Dependencies:** 1

**Files to create/modify:**
- `src/daemon/workspace_registry.rs` (new) — `WorkspaceRegistry` struct with `Arc<Self>` constructor, `create`, `list`, `reconcile_on_boot` async methods. No fire channel (workspaces don't fire events back at agents the way wake sources do).
- `src/daemon/server.rs` — instantiate `Arc<WorkspaceRegistry>` at boot, pass to `handle_rpc`. Call `reconcile_on_boot` once before serving RPCs.
- `src/daemon/rpc.rs` — `handle_rpc` matches `workspace.create` and `workspace.list`; new handler functions `handle_workspace_create`, `handle_workspace_list`.
- `src/daemon/persistence.rs` — `insert_workspace`, `list_workspaces`, `list_workspace_paths_only` (for reconciliation).
- `src/shared/constants.rs` — `WORKSPACES_ROOT = "~/.grimoire/workspaces"` (resolved via `grimoire_dir().join("workspaces")`).

**Detailed specification:**

`WorkspaceRegistry::new(db, bus, config) -> Arc<Self>` — no actor task to spawn (synchronous; git shellout is the only async work, done inline in `create`).

`create(name, repo_path, branch) -> Result<Workspace>`:
1. Parse `WorkspaceId` from name.
2. Canonicalize `repo_path`; reject if not a directory or not readable. Reject if it's not a git repo (cheap check: `repo_path.join(".git").exists() || git_dir_check`).
3. Compute `path = constants::workspaces_root().join(&id)`.
4. DB pre-check: `SELECT 1 FROM workspaces WHERE id = ?`. If exists, return `WorkspaceAlreadyExists`.
5. Check filesystem: if `path` already exists (orphan dir), return `WorkspacePathOccupied { path }` with a message pointing the user at `grim workspace list --orphans`.
6. Shell out: `git -C <repo_path> worktree add <path> <branch>`. Args passed individually to `tokio::process::Command`. Capture stdout+stderr. On non-zero exit: return `RpcErr("git_worktree_add_failed", message: stderr_truncated_4kib)`. **No DB row written on failure** — filesystem may have partial state, but we did not enter an "exists in DB" condition.
7. After successful git: `INSERT INTO workspaces (id, path, repo_path, branch, state, created_at) VALUES (?, ?, ?, ?, 'Active', ?)`. UNIQUE conflict on race → `WorkspaceAlreadyExists` (and the worktree is now a leftover; reconciliation will detect it on next boot, OR we can `git worktree remove --force` here; choose the cleanup path for robustness).
8. Emit `StreamEvent::WorkspaceCreated`.

`list() -> Vec<WorkspaceListEntry>`:
- One DB query joining `workspaces` with a `LEFT JOIN` on `workspace_assignments` aggregated by count.
- `WorkspaceListEntry { id, path, branch, state, agent_count, created_at }`.

`reconcile_on_boot()`:
1. Read all rows from `workspaces` into `db_set` (id → path).
2. Read all entries under `workspaces_root()` (each subdir).
3. For each on-disk dir:
   - If matching DB row exists and `state == 'Active'`: OK.
   - If matching DB row in `state == 'Destroying'`: attempt cleanup (call into task 4's destroy logic) — gated by task 4 ordering, but emit `WorkspaceDestroyed` and remove row.
   - If no DB row: emit `StreamEvent::WorkspaceOrphanDirDetected { path }`, log warning. Do NOT delete.
4. For each DB row not on disk:
   - Set `state='Destroying'`, then delete row (cascade kills memory + assignments). Emit `WorkspaceDestroyed`.
5. Reconciliation runs once at boot, before RPC server accepts connections.

**Edge cases to handle:**
- `workspaces_root()` does not exist on first boot — create it (`mkdir -p`) before listing.
- A non-directory file under `workspaces_root()` — log warning, skip.
- Symlinks under `workspaces_root()` — treat as orphans, don't follow.
- `git worktree add` on a branch that already has a worktree elsewhere → git's own error surfaces verbatim.
- `repo_path` is itself a symlink → canonicalize before passing to git.
- Two concurrent `workspace.create` for the same name (very unlikely with single-user, but possible via two CLI invocations) → second one's UNIQUE insert fails after its `git worktree add` succeeded. Roll back by `git worktree remove --force <path>` on the second; return `WorkspaceAlreadyExists`.

**Acceptance criteria:**
- [ ] `workspace.create` with a valid name + valid repo_path + new branch creates a directory at `<workspaces_root>/<name>`, inserts a row in `workspaces` with `state='Active'`, and emits exactly one `WorkspaceCreated` event whose `workspace_id`, `path`, and `branch` match.
- [ ] `workspace.create` with an invalid name returns `RpcErr("invalid_workspace_name")` and does not invoke git.
- [ ] `workspace.create` for a name already in the DB returns `RpcErr("workspace_already_exists")` and does not invoke git or modify the filesystem.
- [ ] `workspace.create` whose `git worktree add` fails (e.g., branch already checked out) returns `RpcErr("git_worktree_add_failed")` with the git stderr in the message and writes no DB row.
- [ ] `workspace.list` returns one entry per workspace row, with `agent_count` reflecting the count of assignments in `workspace_assignments`.
- [ ] On boot, an orphan directory under `workspaces_root` (no matching DB row) emits one `WorkspaceOrphanDirDetected` event and the directory is preserved.
- [ ] On boot, a DB row whose path is missing from disk transitions to `Destroying`, then is deleted, emitting one `WorkspaceDestroyed` event. Memory and assignment rows for that workspace are gone.
- [ ] `workspace.create` after a failed prior attempt left a stale on-disk directory but no DB row → returns `RpcErr("workspace_path_occupied")`.

**Contract tests (RED phase):**
- Test file: `tests/workspace_create_destroy.rs`
- Tests:
  - `create_happy_path_writes_dir_row_event`
  - `create_invalid_name_rejected_no_git_invoked` — uses a fake `GitRunner` trait seam; assert `invoke_count == 0`.
  - `create_duplicate_name_returns_already_exists`
  - `create_with_git_failure_no_db_row` — fake `GitRunner` returns non-zero.
  - `create_with_orphan_dir_present_returns_path_occupied`
  - `list_returns_agent_count`
  - `boot_reconcile_orphan_dir_emits_event_and_preserves_dir`
  - `boot_reconcile_orphan_row_deletes_row_and_cascade`
  - `boot_reconcile_active_row_with_dir_is_noop`

**Non-testable items:**
- Creating `workspaces_root()` directory at boot (filesystem wiring; covered indirectly by happy-path test).
- Wiring `WorkspaceRegistry` into `server.rs` (covered by integration tests in task 8).

**Notes/Warnings:**
- Introduce a `GitRunner` trait (`async fn worktree_add(&self, repo: &Path, target: &Path, branch: &str) -> Result<(), GitError>`, `worktree_remove`, etc.) so tests don't actually shell out. Production impl wraps `tokio::process::Command`. Mirrors the `Clock` and `WakeMailSender` seams already in the codebase.
- Reconciliation must run before the RPC server starts accepting connections. A test that opens the daemon, kills it after `workspace.create`, deletes the dir externally, and reopens the daemon validates this end-to-end (task 8).
- `git worktree remove --force` is the rollback path on the duplicate-create race. Verify with the GitRunner seam that we call it.

---

### Task 4: WorkspaceRegistry — destroy + assign + state machine

**Summary:** Implement `workspace.destroy` (refuses if non-terminal agents are assigned, otherwise transitions `Active → Destroying → gone`), `workspace.assign` (used by `summon --workspace`), and the state-machine guards across both.

**Dependencies:** 3

**Files to create/modify:**
- `src/daemon/workspace_registry.rs` — add `destroy`, `assign`, `unassign_agent` methods.
- `src/daemon/persistence.rs` — `update_workspace_state`, `delete_workspace_row`, `insert_workspace_assignment`, `list_active_assigned_agents` (for the in-use refusal), `count_assignments_by_workspace`.
- `src/daemon/rpc.rs` — register `workspace.destroy` and `workspace.assign` (`workspace.assign` is daemon-internal but available via RPC for testability; document as internal).
- `src/shared/protocol.rs` — `WorkspaceInUseError { workspace_id, agent_ids }` shape in error data.

**Detailed specification:**

`destroy(workspace_id) -> Result<()>`:
1. Inside a transaction:
   - Read workspace row; if missing → `RpcErr("workspace_not_found")`.
   - Read assignments. For each assigned `agent_id`, look up `agents.state`. If any are in **non-terminal** states (per `AgentState::is_terminal()` from `tests/dormant_state.rs`'s contract — `Dormant` is terminal, `Running`/`Queued`/`Provisioning` are not), abort: `RpcErr("workspace_in_use", { agent_ids: [...] })`. Use `is_final()` if non-terminal-but-final agents (e.g., still cleaning up) shouldn't block.
   - Set `state='Destroying'`.
2. Outside the transaction (now that no new assignments can land — see assign):
   - Shell out: `git -C <repo_path> worktree remove --force <path>` via `GitRunner`. Tolerate non-zero exit: log warning, continue (filesystem may have been removed manually).
   - Delete the workspace row (cascade kills memory + assignments).
   - Emit `WorkspaceDestroyed`.
3. Return `Ok(())`.

`assign(workspace_id, agent_id) -> Result<()>`:
1. Inside a transaction:
   - Read workspace row; if missing → `RpcErr("workspace_not_found")`.
   - If `state == 'Destroying'` → `RpcErr("workspace_destroying")`.
   - Insert `workspace_assignments (workspace_id, agent_id, assigned_at)`. UNIQUE conflict (already assigned) is a no-op.
   - Update `agents.workspace_id = ?` for the agent.

State machine:
- `Active` allows: assign, destroy-initiation, memory ops.
- `Destroying` allows: nothing (no new assigns, no memory ops). Existing in-flight transactions complete.
- After delete: row is gone; lookups return `not_found`.

**Edge cases to handle:**
- Agent terminates between `destroy` precheck and state transition → check is best-effort. The transition still proceeds; if a wake fires after `Destroying` and a new mail tries to wake the agent, the dispatch path will see the workspace gone and surface a typed error. Acceptable for v1.
- `git worktree remove` fails because someone has files open inside the worktree → `--force` should handle it. If git still fails, log and leave the directory (next boot reconciliation will deal with it; the row goes away regardless).
- `assign` for an agent that already has `workspace_id` set to a *different* workspace → reject `RpcErr("agent_already_assigned")`. Each agent is in at most one workspace.
- Two concurrent `destroy` calls → first wins (Active → Destroying), second sees `Destroying` and returns `RpcErr("workspace_destroying")` (idempotent-ish; treat as in-progress).

**Acceptance criteria:**
- [ ] `workspace.destroy` of an empty workspace transitions to `Destroying`, calls `GitRunner::worktree_remove` once, deletes the row, and emits `WorkspaceDestroyed`. Memory rows are gone.
- [ ] `workspace.destroy` of a workspace with an assigned `Running` agent returns `RpcErr("workspace_in_use")` with `agent_ids` populated, and the workspace remains `Active`.
- [ ] `workspace.destroy` of a workspace with an assigned `Dormant` agent succeeds (Dormant is terminal per existing `is_terminal()` contract).
- [ ] `workspace.destroy` of an unknown id returns `RpcErr("workspace_not_found")`.
- [ ] `workspace.assign` against a `Destroying` workspace returns `RpcErr("workspace_destroying")`.
- [ ] `workspace.assign` for an agent already assigned to a different workspace returns `RpcErr("agent_already_assigned")`.
- [ ] `workspace.assign` for the same `(workspace_id, agent_id)` twice is idempotent (second call returns `Ok`).
- [ ] After a successful `destroy`, all `workspace_memory` rows for that workspace are gone (FK cascade verified).

**Contract tests (RED phase):**
- Test file: `tests/workspace_destroy_assign.rs`
- Tests:
  - `destroy_empty_workspace_succeeds_and_emits_event`
  - `destroy_workspace_with_running_agent_refuses_with_in_use`
  - `destroy_workspace_with_dormant_agent_succeeds`
  - `destroy_unknown_workspace_returns_not_found`
  - `destroy_cascades_memory_rows`
  - `assign_to_destroying_returns_destroying_error`
  - `assign_idempotent_for_same_pair`
  - `assign_to_second_workspace_when_already_assigned_rejected`
  - `concurrent_destroy_second_caller_sees_destroying`

**Non-testable items:**
- The decision rule "non-terminal" uses `AgentState::is_terminal` which already exists; no new code path.

**Notes/Warnings:**
- The state-machine race window between `destroy` precheck and a concurrent `assign` is closed by performing both inside transactions on the same connection. Since `Database` is `Mutex<Connection>`, lock acquisition serializes them. Document this — anyone who later splits to a connection pool must re-examine.

---

### Task 5: WorkspaceWatcher — per-workspace notify + topic emission

**Summary:** Add a `WorkspaceWatcher` that wraps `notify::RecommendedWatcher` per active workspace, applies the default ignore-glob set from `wake_sources/file_watch.rs`, debounces at 200 ms, batches paths up to 64 per emission, and publishes both a `WorkspaceFileChanged` `StreamEvent` and a topic mail to `topic://workspace/<id>/files`. Lazy-start when first agent is assigned; stop on workspace destroy.

**Dependencies:** 3

**Files to create/modify:**
- `src/daemon/workspace_watcher.rs` (new) — `WorkspaceWatcher` struct, `start(workspace_id, root) -> WatcherHandle`, `stop`. Drop on `WatcherHandle` stops the underlying watcher.
- `src/daemon/workspace_registry.rs` — track watcher handles in a `Mutex<HashMap<WorkspaceId, WatcherHandle>>`. Start on first assign (if not already running). Stop in `destroy` after the in-use check passes.
- `src/shared/constants.rs` — `WORKSPACE_WATCH_DEBOUNCE_MS = 200`, `WORKSPACE_WATCH_BATCH_MAX = 64`, `WORKSPACE_WATCH_DEFAULT_IGNORES = [".git/**", "target/**", "node_modules/**", ".DS_Store", "*.swp", ".*/**"]` (mirroring `file_watch.rs` defaults).

**Detailed specification:**

`WorkspaceWatcher::start`:
1. Build `GlobSet` from default ignores (config override available later via `workspace.set_watch_ignore` in v2 — out of scope for this task).
2. Construct `notify::recommended_watcher(callback)`. Callback filters on `EventKind::Create | Modify | Remove`, applies ignore-glob to relative path, sends `(path, kind)` over an unbounded `mpsc::UnboundedSender`.
3. Spawn a tokio task that:
   - Receives `(path, kind)` items.
   - Accumulates in a buffer.
   - On each item, schedules a debounce timer. If a new item arrives before the timer expires, restart it. (Use `tokio::time::sleep_until(Instant::now() + DEBOUNCE)` reset pattern, or a simpler 200 ms tick.)
   - On debounce expiry: if `buffer.len() <= 64`, emit one `WorkspaceFileChanged { paths, kinds, truncated_count: 0 }`. If `> 64`, emit with first 64 + `truncated_count = buffer.len() - 64`. Clear buffer.
   - For each emission, **also** publish to `topic://workspace/<id>/files` via the shared `publish_topic` helper from task 2. Body: small JSON `{"paths":[...],"kinds":[...],"truncated":N}`. Sender: `workspace://<id>`.
4. `WatcherHandle` owns the watcher and the task `JoinHandle`. Drop aborts the task and drops the watcher.

Default ignores (verbatim from `wake_sources/file_watch.rs` semantics, per-relative path under workspace root):
- `.git/**`, `target/**`, `node_modules/**`, `.DS_Store`, `*.swp`, dotfile dirs `.*/**` excluding the workspace root itself.

Watch root = `workspace.path` (the worktree dir). `RecursiveMode::Recursive`.

Lifecycle:
- `WorkspaceRegistry::assign` calls `ensure_watcher_started(workspace_id)`. Idempotent.
- `WorkspaceRegistry::destroy` calls `stop_watcher(workspace_id)` AFTER the in-use precheck passes and BEFORE the `git worktree remove` shellout (so file events from the removal itself don't leak).

**Edge cases to handle:**
- `notify` errors (e.g., inotify limit reached) → log warning at `WARN`, do not panic, mark watcher as failed in handle map but keep workspace usable. (No retry in v1; doc note for v2.)
- Watcher receives an event for a path outside `workspace.path` (shouldn't happen with recursive, but guard) → drop.
- High-churn sources (a `cargo build` running inside the workspace) → ignore-globs should exclude `target/`. Test on a real repo with a build running (manual).
- Buffer overflow within a single debounce window (e.g., `git checkout` rewriting 10000 files) → cap at 64 per emission with `truncated_count` set; subsequent debounce windows fire follow-up emissions.

**Acceptance criteria:**
- [ ] After `workspace.create` followed by `workspace.assign`, a file write to `<workspace.path>/foo.txt` produces, within 500 ms, exactly one `WorkspaceFileChanged` event whose `paths` includes `"foo.txt"` (or its absolute equivalent).
- [ ] A file write to `<workspace.path>/target/build/intermediate.o` produces NO `WorkspaceFileChanged` event (ignore-glob).
- [ ] A file write to `<workspace.path>/.git/HEAD` produces NO `WorkspaceFileChanged` event.
- [ ] An agent subscribed to `topic://workspace/<id>/files` receives a mail row after a watched file change.
- [ ] Writing 100 files in <50 ms produces exactly one `WorkspaceFileChanged` event (debounced) with `paths.len() <= 64` and `truncated_count == max(0, 100 - 64)`.
- [ ] After `workspace.destroy`, no further `WorkspaceFileChanged` events are emitted for that workspace (the watcher is stopped).
- [ ] Calling `workspace.assign` twice for the same workspace starts the watcher exactly once (idempotent).

**Contract tests (RED phase):**
- Test file: `tests/workspace_watcher.rs`
- Tests:
  - `single_file_change_emits_one_event`
  - `target_dir_changes_ignored`
  - `git_dir_changes_ignored`
  - `topic_mail_published_on_file_change`
  - `bursty_writes_debounced_to_one_event`
  - `over_64_paths_truncates_with_count`
  - `destroy_stops_watcher_no_more_events`
  - `assign_idempotent_starts_watcher_once`

**Non-testable items:**
- The exact `notify` callback shape (proxied through tests via real filesystem writes).

**Notes/Warnings:**
- macOS `notify` uses `FsEventsWatcher` by default, which is debounced at the OS level. Linux uses `inotify`, which is event-per-syscall. The 200 ms debounce smooths both — but assertions about exact timing are flaky; use polling-with-deadline (`poll_count` style from `tests/event_log_durability.rs`).
- Filesystem tests are slow and platform-flaky. Mark `#[cfg(target_os = "macos")]` / `#[cfg(target_os = "linux")]` if any prove unreliable, but try to keep them platform-neutral.

---

### Task 6: CLI — `grim workspace` and `grim memory` subcommands

**Summary:** Add `grim workspace create|list|destroy|show` and `grim memory put|get|list|delete` subcommands. Mirror the `MailCommand` pattern.

**Dependencies:** 2, 4

**Files to create/modify:**
- `src/cli/commands/workspace.rs` (new) — `WorkspaceCommand` enum, `run(cmd)` dispatcher, handlers.
- `src/cli/commands/memory.rs` (new) — `MemoryCommand` enum, `run(cmd)` dispatcher, handlers.
- `src/cli/commands/mod.rs` — `pub mod workspace; pub mod memory;`.
- `src/main.rs` — add `Workspace { #[command(subcommand)] cmd: ... }` and `Memory { … }` variants to the `Commands` enum, plus matchers.

**Detailed specification:**

`grim workspace create <name> --from <repo> --branch <branch>`:
- Calls `workspace.create` RPC.
- On success: prints `<id>\t<path>` to stdout (tab-separated, scriptable).
- On error: prints to stderr and exits non-zero. Specific errors get human-readable hints (e.g., `workspace_path_occupied` → "Run `grim workspace list --orphans` to inspect.").

`grim workspace list [--orphans]`:
- Without `--orphans`: calls `workspace.list`, prints a table: `ID  BRANCH  AGENTS  STATE  PATH`.
- With `--orphans`: also queries `workspace.list_orphans` (a new RPC method or a flag on `list` — choose one; recommend a flag on `list` returning an extra `orphans` field).
- Wait — to keep this task atomic, ship `--orphans` as a separate `workspace.list_orphans` RPC method or fold it into the `workspace.list` response shape. Decision: **fold into `workspace.list` response** as `orphans: Vec<String>`. Default CLI rendering hides them; `--orphans` shows them.

`grim workspace destroy <id>`:
- Calls `workspace.destroy`.
- On `WorkspaceInUse` error, prints "Workspace in use by: <agent_ids>. Banish them or wait." Exits non-zero with code 2 (distinguish from generic error).

`grim workspace show <id>`:
- Calls `workspace.list` (or a single-id variant) and prints all fields including assigned agents.

`grim memory put <workspace> <key> [<value> | --json <value> | @<file>]`:
- Default treats `<value>` as a JSON string literal (must parse). `--json` accepts inline JSON. `@<file>` reads file and parses as JSON.
- Optional `--expected-version <N>` for CAS.
- On `cas_conflict`, prints `Conflict: current version is <N>` and exits with code 3.

`grim memory get <workspace> <key>`:
- Pretty-prints JSON to stdout. Exits 1 if not found.

`grim memory list <workspace> [--prefix <p>]`:
- Prints table: `KEY  VERSION  SIZE  UPDATED_AT`.

`grim memory delete <workspace> <key> [--expected-version <N>]`:
- Same CAS exit-code contract.

**Edge cases to handle:**
- Daemon not running → existing `DaemonClient::connect()` already returns a friendly error; preserve it.
- `--json` with malformed JSON → parse client-side; print `Error: invalid JSON: <serde error>`; exit 1 without contacting daemon.
- `@file` for a nonexistent file → `Error: cannot read <path>`; exit 1.
- `grim memory put` value > value cap → daemon returns typed error; CLI exits 1 with a hint pointing at the cap config.

**Acceptance criteria:**
- [ ] `grim workspace create <name> --from <repo> --branch <br>` exits 0 on success and prints `<id>\t<path>`.
- [ ] `grim workspace list` prints a table that includes a workspace just created.
- [ ] `grim workspace destroy <id>` of an in-use workspace exits with code 2 and prints assigned agent IDs.
- [ ] `grim memory put ws key '"hello"'` then `grim memory get ws key` round-trips and prints `"hello"`.
- [ ] `grim memory put ws key '{"a":1}' --expected-version 99` returns CAS conflict with exit code 3.
- [ ] `grim memory list ws --prefix findings` only shows keys under `findings/...` and `findings`.
- [ ] `grim memory delete ws key` removes the row; `grim memory get ws key` then exits 1.
- [ ] `grim memory put ws key @/nonexistent/file` exits 1 without contacting the daemon (verify via socket presence).

**Contract tests (RED phase):**
- Test file: `tests/cli_workspace.rs`, `tests/cli_memory.rs`
- Tests (driven via `assert_cmd` or the existing test harness, mirroring `tests/cli_status.rs`):
  - `cli_workspace_create_prints_id_and_path`
  - `cli_workspace_list_shows_created_workspace`
  - `cli_workspace_destroy_in_use_exits_2`
  - `cli_memory_put_then_get_roundtrip`
  - `cli_memory_put_with_stale_version_exits_3`
  - `cli_memory_list_prefix_filter`
  - `cli_memory_delete_then_get_exits_1`
  - `cli_memory_put_at_file_missing_exits_1`

**Non-testable items:**
- Help-text wording (no behavioral test).
- Subcommand registration in `main.rs` (covered transitively).

**Notes/Warnings:**
- Use `colored` crate (already in use per mail.rs) for error highlighting; keep machine-readable stdout (tab-separated) clean of color codes.
- Distinct exit codes (0 success, 1 generic error, 2 in-use, 3 CAS conflict, 4 not found) make scripting reliable. Document in README workspace section.

---

### Task 7: `summon --workspace` and scroll `workspace:` field

**Summary:** Wire `--workspace <name>` into `grim summon` so the agent's cwd is the worktree path and `workspace.assign` runs in the same scope as agent insert. Add a top-level `- workspace: <name>` directive to scroll Markdown; `scroll_keeper` creates the workspace if missing and assigns all tasks to it.

**Dependencies:** 4

**Files to create/modify:**
- `src/main.rs` — add `--workspace <name>` flag to `Summon` variant.
- `src/cli/commands/summon.rs` (or wherever summon lives) — pass `workspace` through to RPC.
- `src/daemon/agent_manager.rs` — `enqueue_with_options` accepts `workspace_id: Option<WorkspaceId>`. Resolves cwd: if `Some(id)`, looks up `workspaces.path` and uses that, ignoring `--cwd` (or rejecting if both given). Inserts agent and `workspace_assignments` row in same transaction.
- `src/daemon/scroll_parser.rs` — add `workspace: <name>` to top-level directive parsing (sibling of `name`). Add `workspace: Option<String>` field to `ScrollSpec`.
- `src/daemon/scroll_keeper.rs` — if `ScrollSpec.workspace` is `Some(name)`:
  - Lookup workspace by name. If missing, create it (requires `--from` and `--branch` specified inline in the scroll, see below).
  - Pass `workspace_id` to every task's `enqueue_with_options`.
- `src/shared/protocol.rs` — `SummonParams { task, ..., workspace: Option<String> }` (add field).

**Detailed specification:**

`grim summon --workspace <name> "<task>"`:
1. CLI sends `summon` RPC with `workspace: Some(name)`.
2. Daemon `agent_manager.enqueue_with_options`:
   - If `workspace.is_some()` AND `cwd.is_some()` → reject `RpcErr("conflicting_options", "--workspace and --cwd are mutually exclusive")`.
   - If `workspace.is_some()`: look up workspace; if missing → `workspace_not_found`. If `Destroying` → `workspace_destroying`. Otherwise use `workspace.path` as cwd.
   - Insert agent row.
   - Call `WorkspaceRegistry::assign(workspace_id, agent_id)`. (Same transaction via `Database::Mutex<Connection>` serialization.)
3. The watcher (task 5) lazy-starts on first assign automatically.

Scroll YAML extension:

Existing scroll format:
```markdown
# Scroll: My Project

## Task: t1
...
```

Extend top-level directives:
```markdown
# Scroll: My Project
- workspace: my-ws
- workspace_repo: ~/repos/grimoire    # required if workspace doesn't exist
- workspace_branch: wip/scroll-1      # required if workspace doesn't exist

## Task: t1
...
```

Parser (`scroll_parser.rs`):
- After the `# Scroll: <name>` heading, before the first `## Task:`, scan lines matching `- key: value`.
- Recognize: `workspace`, `workspace_repo`, `workspace_branch`. Persist on `ScrollSpec`.

`scroll_keeper.rs::inscribe`:
- If `workspace.is_some()` and the workspace doesn't exist, call `WorkspaceRegistry::create(name, repo, branch)`. Both `workspace_repo` and `workspace_branch` must be present; else fail inscription with `RpcErr("missing_workspace_create_args")`.
- For every task in the scroll, set `workspace_id` on its `TaskSpec` (or pass through `enqueue_with_options`).

**Edge cases to handle:**
- Scroll declares `workspace: x` but `x` exists with a different `repo_path` than `workspace_repo` → log a warning but use existing workspace. Don't recreate.
- Scroll declares both `workspace: x` (existing) and `workspace_repo: ...` → ignore the create-args (workspace already there). Document.
- Scroll fanout: all tasks share one worktree (the explicit goal of the feature). The cwd-glob `TaskConflict::detect` keeps existing semantics, just inside one worktree now.
- `summon --workspace ws --cwd /path` → reject (mutually exclusive).
- `summon --workspace ws` against a workspace mid-destroy → `workspace_destroying`.
- Scroll inscribed against a `Destroying` workspace → fail entire inscription before any task is enqueued.

**Acceptance criteria:**
- [ ] `grim summon --workspace ws "task"` (with `ws` Active) creates an agent whose `cwd` matches `workspaces.path` for `ws` and inserts a `workspace_assignments` row.
- [ ] `grim summon --workspace ws --cwd /tmp "task"` exits with an error mentioning mutual exclusivity.
- [ ] `grim summon --workspace nonexistent "task"` exits with `workspace_not_found`.
- [ ] `grim summon --workspace ws "task"` against a `Destroying` workspace exits with `workspace_destroying`.
- [ ] A scroll file with `- workspace: ws` (where `ws` exists) inscribes successfully and every resulting agent has `workspace_id == ws`.
- [ ] A scroll file with `- workspace: new-ws`, `- workspace_repo: ~/r`, `- workspace_branch: br` and `new-ws` not yet existing creates the workspace exactly once (one `WorkspaceCreated` event) and assigns all tasks to it.
- [ ] A scroll file with `- workspace: missing` and no create args fails inscription with `missing_workspace_create_args` and creates no agents.
- [ ] After scroll completion (all tasks Dormant or terminal), `grim workspace destroy ws` succeeds.

**Contract tests (RED phase):**
- Test file: `tests/summon_workspace.rs`, `tests/scroll_workspace_field.rs`
- Tests:
  - `summon_with_workspace_uses_worktree_cwd`
  - `summon_with_workspace_writes_assignment`
  - `summon_with_workspace_and_cwd_rejected`
  - `summon_with_unknown_workspace_returns_not_found`
  - `summon_with_destroying_workspace_returns_destroying`
  - `scroll_with_existing_workspace_assigns_all_tasks`
  - `scroll_with_new_workspace_creates_once_and_assigns`
  - `scroll_with_missing_workspace_create_args_fails_inscribe`
  - `scroll_inscribed_no_agents_on_destroying_workspace`

**Non-testable items:**
- Clap flag wiring on `Summon` variant (covered transitively).

**Notes/Warnings:**
- The cwd-glob `TaskConflict::detect` is preserved as-is — workspace doesn't change conflict semantics, just constrains cwd. Confirm in an integration test that a scroll with two tasks touching the same file_pattern still conflicts.
- The `SummonParams.workspace` field is a string name; the daemon resolves it. Don't push the resolution to the CLI.

---

### Task 8: End-to-end integration tests

**Summary:** Multi-component scenarios that exercise the unlock the feature is meant to deliver. These tests are written after individual task contract tests are green; they catch composition bugs.

**Dependencies:** 2, 4, 5, 7

**Files to create/modify:**
- `tests/workspaces_e2e.rs` (new) — full-daemon integration tests booting `Database` + `EventBus` + `WorkspaceRegistry` + `AgentManager` + scheduler.

**Detailed specification:**

Scenarios (each is one `#[tokio::test]`):

1. **Two agents share via memory** — Create workspace `ws`. Summon agent A and agent B into it. A puts `findings/x` = `"alpha"`. B subscribes to `topic://workspace/ws/memory/*`. Assert B receives mail with the right body. B reads memory and gets value + version.

2. **CAS-conflict retry pattern** — Two agents both try to write `counter` with `expected_version: 1`. First wins (returns version 2). Second gets `cas_conflict` with `current_version: 2`, retries with `expected_version: 2`, wins (version 3).

3. **Filewatch wake** — Subscribe a Dormant agent to `topic://workspace/ws/files`. Write a file in the worktree. Assert the agent transitions out of Dormant within 1 second (fold-and-wake path).

4. **Crash-mid-create reconciliation** — Create workspace, kill daemon (drop), externally `git worktree remove` the dir, reopen daemon. Assert the row transitions to `Destroying`, gets deleted, and `WorkspaceDestroyed` event lands.

5. **Crash-mid-create orphan dir** — Create workspace, kill daemon. Externally delete only the DB row (sqlite3 CLI). Reopen daemon. Assert one `WorkspaceOrphanDirDetected` event and the dir is preserved.

6. **Destroy refusal then drain** — Workspace with running agent. `destroy` returns `WorkspaceInUse`. Banish the agent (it becomes terminal). Second `destroy` succeeds.

7. **Filewatch noise filter on a real-ish target dir** — Create a workspace, write 50 files into `<path>/target/build/`, write 1 file at `<path>/foo.txt`. Assert exactly one `WorkspaceFileChanged` event with `paths == ["foo.txt"]`.

8. **Scroll with workspace inheritance** — Inscribe a 2-task scroll declaring an existing workspace. Both agents land with the workspace's cwd; both assignments rows present; the cwd-glob TaskConflict warning still triggers if file_patterns overlap.

**Edge cases to handle:**
- Test isolation: each test uses a unique `TempDbPath` and a unique `workspaces_root` (override constants via env var or pass through registry constructor — choose the constructor-arg path for cleanliness).
- Tests that use real `git` binary need `git` in PATH — gate with `#[cfg_attr(not(feature = "real-git-tests"), ignore)]` if CI doesn't reliably have it. Otherwise rely on the `GitRunner` seam to substitute a fake; reserve real-git for one or two end-to-end tests.

**Acceptance criteria:**
- [ ] All 8 scenarios above pass on macOS and Linux.
- [ ] No test relies on absolute timing < 200 ms (use poll-with-deadline).
- [ ] Every test cleans up its workspace dir (`Drop` impl on a test guard).

**Contract tests (RED phase):**
- These tests are themselves the contract — they assert the *integrated* behavior, which is not covered by per-task tests. Following the test names above:
  - `two_agents_share_via_memory`
  - `cas_conflict_retry_succeeds_on_second_try`
  - `filewatch_wakes_dormant_subscriber`
  - `reconcile_orphan_row_after_external_dir_removal`
  - `reconcile_orphan_dir_after_external_row_deletion`
  - `destroy_refuses_then_succeeds_after_banish`
  - `target_dir_noise_filtered_real_workspace`
  - `scroll_inheritance_assigns_all_tasks`

**Non-testable items:**
- None — every scenario here is observable.

**Notes/Warnings:**
- Tests #4 and #5 require simulating "kill daemon" — in-process this is `drop(database); drop(registry); drop(bus);` in order. Make sure no background tasks hold `Arc<Database>` clones beyond expected lifetime, or use `Database::open_in_memory` + a separate file for the persisted reopen test.
- Test #7 will be flaky on slow filesystems. Use a 5-second poll deadline before asserting.

---

## Testing Strategy (TDD)

Implementation follows red-green TDD: for each task, the implementer writes failing contract tests from the acceptance criteria (RED), then implements the minimum code to make them pass (GREEN). Contract tests are immutable once committed.

### Contract Tests per Task

| Task | Test File | Contract Tests | Non-Testable Items |
|------|-----------|----------------|-------------------|
| 1 | `tests/workspaces_schema.rs` | 7 | RPC param/result struct shapes (no behavior) |
| 2 | `tests/memory_kv.rs` | 15 | Reserved-prefix list edit (covered by user-RPC test); shared `publish_topic` refactor |
| 3 | `tests/workspace_create_destroy.rs` | 9 | `workspaces_root()` `mkdir -p`; server.rs wiring |
| 4 | `tests/workspace_destroy_assign.rs` | 9 | None (state machine fully observable) |
| 5 | `tests/workspace_watcher.rs` | 8 | `notify` callback shape (proxied via filesystem) |
| 6 | `tests/cli_workspace.rs`, `tests/cli_memory.rs` | 8 | Help-text wording, subcommand registration |
| 7 | `tests/summon_workspace.rs`, `tests/scroll_workspace_field.rs` | 9 | Clap flag wiring |
| 8 | `tests/workspaces_e2e.rs` | 8 | None |

### Integration Testing

Task 8's e2e suite is the integration layer. Beyond that, manually verify on a real grimoire repo:

### Manual Testing Checklist

- [ ] Create a workspace from this grimoire repo, summon an agent into it; confirm `pwd` inside the agent equals the worktree path.
- [ ] Run `cargo build` inside a workspace; confirm zero `WorkspaceFileChanged` events from `target/`.
- [ ] Two-agent demo: `swarm decompose` style. Parent agent puts a task list into memory; two children subscribe and pick up keys; parent reads results back. Confirm no prompt-engineering glue is needed.
- [ ] `grim workspace destroy` with a Running agent prints the expected human message and exits 2.
- [ ] Reboot the daemon mid-create (Ctrl-C right after `git worktree add`); confirm boot reconciliation handles the orphan correctly.
- [ ] Memory value at exactly `workspace_value_cap_bytes` succeeds; one byte over is rejected with the typed error.

## Rollout Considerations

### Feature Flags

None. v1 is opt-in: if you don't pass `--workspace` on summon and don't put `workspace:` in a scroll, behavior is identical to today. The schema migrations are additive and run on first boot of the new binary.

### Migration Strategy

- Three new tables: created via `IF NOT EXISTS` on first boot.
- `agents.workspace_id`: added via guarded `ALTER TABLE ADD COLUMN` (NULL for existing rows).
- No data migration. Existing agents continue with their flat cwd.
- Existing scrolls without `- workspace:` are unaffected.
- The reserved-sender-prefix list expansion (`workspace://`) is backward-compatible: no production user is sending mail with that prefix (it's brand new).

### Rollback Plan

- The change is a code rollback away. Schema rollback is **not** required: leaving the new tables empty in an old binary is harmless — the old binary doesn't know they exist. However:
  - If a user has run `summon --workspace` on the new binary, agents have `workspace_id` set. Rolling back to a binary that doesn't read that column is fine (column ignored on read). Future re-roll-forward picks up where it left off.
  - If a user has destroyed workspaces, the rows are gone — no rollback needed.
- **Hard rollback** (drop new tables): only if the schema change itself is implicated in a failure. Provide a `grim diagnostic schema-drop-workspaces` admin command in v1.1 if needed; not in v1.

## Open Items

- [ ] `--copy-from <other-workspace>` deferred to v2 (would add `parent_workspace_id` to `workspaces`).
- [ ] Cross-host worker support deferred — current behavior: workspace-assigned agents run only on workers sharing the daemon's filesystem. Worker eligibility filter to enforce this is task-7-adjacent but lives in worker placement; document in v1, implement guard in v2 worker-pool work.
- [ ] Per-workspace watch-ignore overrides (`workspace.set_watch_ignore`) deferred to v2. v1 ships fixed defaults.
- [ ] Memory total-cap query optimization deferred — `SUM(LENGTH(value))` is O(N) per put; flag for v2 if we add a denormalized counter.
- [ ] `grim diagnostic schema-drop-workspaces` admin command — only if rollback path is invoked in practice.

---

*This spec is implementation-ready. Each task is designed for red-green TDD. Tasks can be picked up independently (respecting dependencies) and completed in a single iteration.*
