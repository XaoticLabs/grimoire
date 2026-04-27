# Plan: Shared Memory + Workspaces (v1)

> Generated from planning session on 2026-04-27
> Source: ROADMAP.md Part 3 §6 + §7 (the "shared memory store + workspaces" milestone in the six-month spine, item 7)

## Problem Statement

Grimoire today has long-lived agents (mail bus, dormant agents with wake triggers, supervision trees) but no shared substrate for them to *collaborate* on real work. Concretely:

1. **Agents can't share knowledge.** Two agents on the same problem either copy-paste outputs into each other's prompts, or stuff state into pact `{output}` templates. There is no daemon-owned scratch space, so multi-agent work degrades into prompt engineering.
2. **Parallel agents trample each other.** Every agent's `cwd` is a flat path stored in the `agents` table. When a scroll fans out, all tasks share one repo checkout. The only safety net is glob-based `TaskConflict::detect`, which *blocks dispatch* on overlap rather than letting work proceed in isolation.
3. **The daemon is blind to file changes.** Agents write files; the daemon only sees stdout/stderr. The single file-watch primitive (`wake_sources/file_watch.rs`) is per-agent opt-in via `wake.add` — there is no fabric-level "agent-2 wrote `src/auth.rs`" event for other agents to react to.

### Who experiences this?

The author, dogfooding multi-agent scrolls on macOS. v1 is explicitly single-user. Once shared memory + workspaces exist, two roadmap "killer demos" become buildable end-to-end: **swarm decompose** (parent agent fans children into parallel worktrees, polls a shared notes namespace, picks the winner) and **standing review team** (dormant reviewers wake on file events in a workspace, leave findings in shared memory).

### Why now?

Items 1–6 of the roadmap's six-month spine are shipped (durable event log, work queue, mail bus, worker pool, dormant agents, supervision trees). Without shared memory + workspaces, none of those primitives compose into a real multi-agent demo — they remain isolated long-lived processes. This is the rate-limiter on the product story for the rest of the year.

### Current workarounds

- Pact `{output}` interpolation, which only fires once per chain hop and breaks under fanout.
- Per-agent `wake.add` file-watch, which solves observation but not coordination or namespacing.
- Manual cwd juggling — author runs `git worktree add` by hand and passes `--cwd` per agent.

## Goals

- Ship a `workspace` primitive: a daemon-owned, named git worktree + a workspace-scoped memory KV + a workspace-scoped filesystem watcher feeding the existing event bus.
- Make the v1 surface usable from `grim` CLI alone: create, list, destroy, summon-into, and inspect memory.
- Reuse existing daemon infrastructure (SQLite migrations via `IF NOT EXISTS` + `ALTER TABLE` guards, `EventBus`, `notify` crate) — no new platform crates.
- Demonstrate the unlock by porting one example multi-agent scenario from "pact chain" to "workspace + memory.put/get + topic subscribe" without prompt-engineering glue.

## Non-Goals (Explicit Scope Boundaries)

- **No RO observer access.** All assigned agents are RW. Real ACL enforcement requires per-process sandboxing (Roadmap Part 1 hardening §3) which is its own milestone. Honor-system RO and watch-and-revert RO were both rejected as half-measures that mostly serve the demo story.
- **No vector / embedding store.** Plain string→bytes/json KV only. Semantic search is a separate, later product decision.
- **No `grim context <id>` (show / compact / fork).** Roadmap §6 also describes context-window introspection. v1 is workspaces + memory only; context inspection is a separate plan because it lives in the provider integration layer, not the daemon.
- **No multi-tenant ACL.** Single-user assumption holds. Tenant isolation waits for the auth-token hardening in Roadmap Part 1.
- **No automatic GC.** Workspaces are explicit destroy only. TTL/idle-sweep deferred to v2.
- **No backward-compatibility tax for scrolls.** No legacy scrolls exist. New scrolls opt in via a YAML `workspace:` field; no implicit migration.

## Proposed Solution

### Conceptual Overview

Three composing primitives that ship together because each is mostly useless without the others:

1. **Workspace** — a named, daemon-owned git worktree. `grim workspace create <name> --from <repo> --branch <branch>` does `git worktree add` under `~/.grimoire/workspaces/<name>` and records the workspace in a new `workspaces` table. Agents are summoned *into* a workspace; their cwd is the worktree path.
2. **Memory KV** — a SQLite-backed key/value store namespaced by workspace, with optimistic concurrency (compare-and-swap on a per-key version). Writes emit `MemoryWritten` StreamEvents on the existing bus, so reactive agents can subscribe via the existing `topic://` mail mechanism rather than inventing new subscription plumbing.
3. **Workspace filewatch** — a daemon-owned `notify` watcher per active workspace, with sane default ignores (`target/`, `node_modules/`, `.git/`, dotfiles), emitting `WorkspaceFileChanged` StreamEvents. Agents subscribe via `topic://workspace/<name>/files` (reusing the mail subscription path that already wakes dormant agents).

The unifying noun is **workspace**: it bundles worktree (no-trample), memory namespace (shared scratch), and filewatch root (observability) into one user-visible thing.

### User Journey

1. `grim workspace create review-q2 --from ~/repos/grimoire --branch wip/review-q2` — daemon provisions worktree, returns the cwd path.
2. `grim summon "audit src/daemon" --workspace review-q2` — agent's cwd is the worktree; agent sees workspace name in env or system prompt.
3. Agent calls (via provider tool or `grim memory put review-q2 findings/auth "needs token rotation"`) — daemon writes the row, emits `MemoryWritten`, version returned to caller.
4. Second agent: `grim summon "summarize findings" --workspace review-q2`, subscribes to `topic://workspace/review-q2/memory/findings/*`, reads with `memory.get` returning value + version.
5. Agent writes a file in the worktree → daemon emits `WorkspaceFileChanged` → any subscriber agent wakes via existing mail-wake path.
6. `grim workspace destroy review-q2` — daemon refuses if any agent assigned to the workspace is in a non-terminal state; otherwise removes worktree and the workspace row (memory rows cascade).

## Architecture

### Data Model

Three new SQLite tables, all created with `CREATE TABLE IF NOT EXISTS` in the existing `Database::migrate()` batch (matching the pattern from `mail` / `subscriptions` / `wake_sources`):

- `workspaces` — `id` (slug), `path` (absolute, daemon-owned), `repo_path`, `branch`, `created_at`, `state` (`Active` | `Destroying`).
- `workspace_memory` — `(workspace_id, key)` PK, `value` (BLOB), `version` (monotonic per key), `updated_at`, `updated_by` (agent id or "system"). FK cascade on workspace delete.
- `workspace_assignments` — `(workspace_id, agent_id)` PK. Records which agents have been summoned into a workspace; consulted on destroy.

Columns added to existing tables:

- `agents.workspace_id` — nullable FK, set on summon when `--workspace` is provided. Backwards compatible: NULL means "no workspace, free-floating cwd as today."

### System Boundaries

The workspace primitive lives entirely in `grimd`. No changes to `grimw` (workers); a remote worker is told a path and just runs in it as before. mTLS path-rewriting for cross-host workspaces is out of scope (open question).

The memory KV is daemon-resident (no external Redis/Postgres). Filewatch reuses the `notify` v6 dependency already pulled in for `wake_sources`.

### API Surface

New RPC methods (each follows the param-struct + handler pattern established by `mail.send` and `wake.add`):

- `workspace.create` { name, repo_path, branch } → { id, path }
- `workspace.list` → [{id, path, branch, agent_count}]
- `workspace.destroy` { id } → { } | error if in use
- `workspace.assign` { workspace_id, agent_id } (used internally by `summon --workspace`)
- `memory.put` { workspace_id, key, value, expected_version? } → { version } | CasConflict error
- `memory.get` { workspace_id, key } → { value, version } | NotFound
- `memory.list` { workspace_id, prefix? } → [{key, version, updated_at}]
- `memory.delete` { workspace_id, key, expected_version? } → { } | CasConflict

Four new `StreamEvent` variants flowing through `EventBus` and the durable `events` log:

- `WorkspaceCreated`, `WorkspaceDestroyed`
- `MemoryWritten` (key, version, agent_id), `MemoryDeleted`
- `WorkspaceFileChanged` (workspace_id, paths, kind)
- `WorkspaceAclViolation` reserved for v2 (no emitter in v1)

CLI surface: `grim workspace create|list|destroy|show`, `grim memory put|get|list|delete`, `grim summon --workspace <name>`. Scrolls add an optional top-level `workspace: <name>` field consumed by `scroll_keeper` (creates the workspace if missing, summons all tasks into it).

### Integration Points

- **Existing summon flow** (`agent_manager.rs:155 resolve_cwd`): if `--workspace` is given, cwd resolution short-circuits to the workspace path. Workspace assignment row written in the same transaction as agent insert.
- **Existing mail bus**: `WorkspaceFileChanged` and `MemoryWritten` are published under `topic://workspace/<name>/files` and `topic://workspace/<name>/memory/<key-prefix>` so the *existing* mail-wake scheduler (already wired for dormant agents) wakes subscribers — no new wake-source kind required.
- **Scroll keeper**: when a scroll declares `workspace:`, all of its tasks inherit the workspace and the cwd-glob conflict detection is preserved as-is (it now operates inside one worktree, which is what the user wanted to begin with).
- **Persistence migrations**: three `CREATE TABLE IF NOT EXISTS` blocks added to the boot migrate batch; `agents.workspace_id` added via the existing `ALTER TABLE ADD COLUMN`-with-existence-check guard pattern.

## Implementation Approach

### Recommended Pattern

Mirror the **wake_registry** shape (an actor + a SQLite table + RPC surface + StreamEvents + CLI), because it is the closest sibling: a daemon-owned registry of long-lived state with filesystem implications and event emission. A `WorkspaceRegistry` actor owns workspace lifecycle; a `WorkspaceWatcher` (one per active workspace, lazily started) wraps `notify::RecommendedWatcher` with the same debounce + ignore-glob plumbing already in `wake_sources/file_watch.rs`. The memory KV is plain `Database` methods plus an `EventBus` publish — no actor needed because there is no async lifecycle to manage.

### Key Technical Decisions

| Decision | Choice | Rationale | Trade-offs |
|---|---|---|---|
| Workspace = git worktree, always | Require `--from <repo>` and `--branch <name>` at create | Cleanest semantics; matches the trampling pain that motivated the feature | Punts non-git use cases (rare in this product); error if repo arg is missing |
| ACL | Drop RO for v1 | Real RO needs OS-level sandboxing (Part 1 §3, unbuilt) | Demo story uses "the dormant reviewer is just a topic subscriber" instead of "the dormant reviewer is RO-sandboxed" |
| Memory concurrency | Optimistic CAS via per-key version | Matches existing patterns (sequence numbers in mail, events); no deadlock risk; cheap | Lossy under heavy contention — agents may need to retry. Acceptable at v1 scale |
| Workspace lifecycle | Explicit destroy only | Simplest; no surprising auto-removal of in-use worktrees | Cruft accumulation; v2 adds optional `--ttl` and a periodic GC sweep |
| Filewatch event emission | Reuse existing topic-subscription / mail-wake plumbing | Zero new wake-source kind; subscribers compose with dormant agents for free | Coarser than per-agent file_watch; agents wanting glob-filtered wake still use `wake.add` |
| Memory backing store | SQLite with the existing `Database` | Same connection pool, migration story, durability guarantees | No horizontal scale story; deferred with the broader Postgres/NATS migration |

### Rough Task Breakdown

1. **Schema + migrations** — add three tables and the `agents.workspace_id` column; reconciliation on boot for crash-mid-create cleanup.
2. **`WorkspaceRegistry` actor** — create/destroy/assign, with `git worktree add` / `remove` shell-out. Handles the destroy-with-running-agents refusal.
3. **Memory KV CRUD** — RPC handlers, CAS semantics, `MemoryWritten` event emission.
4. **`WorkspaceWatcher`** — per-workspace `notify` watcher with default ignores, debounce, topic emission. Lazy-start when first agent assigned.
5. **CLI surface** — `grim workspace …`, `grim memory …`, `grim summon --workspace`. Scroll YAML `workspace:` field.
6. **Integration tests** — crash-mid-create reconciliation, destroy-with-running refusal, CAS conflict surfacing, filewatch noise filtering, end-to-end "two agents share via memory" test.

### Riskiest Part

**Crash-mid-create reconciliation.** The two side effects (filesystem `git worktree add` and DB row insert) cannot be made fully atomic. Either ordering produces a class of orphan: worktree-on-disk-without-row (orphan dir, easy to detect on boot scan) or row-without-worktree (orphan row, agents assigned to it would fail on summon). The plan: insert the DB row in `Active` state *after* the worktree exists; on boot, scan `~/.grimoire/workspaces/` and reconcile against the table — orphan dirs get logged + left alone (don't auto-delete user's possible work), orphan rows in non-`Active` states get cleaned. Spec must enumerate the exact reconciliation matrix.

The runner-up risk is filewatch noise on workspaces inside large monorepos — wrong defaults will drown the bus. The plan inherits the ignore-list shape already in `wake_sources/file_watch.rs`, but the defaults need explicit testing on a real repo.

## Edge Cases & Decisions

| Edge Case | Decision | Rationale |
|---|---|---|
| Daemon crash mid-create (worktree exists, row missing or vice versa) | Boot reconciliation. Worktree-without-row → log warning, leave directory in place (may contain user work). Row-without-worktree → mark `Destroying` and clean up; assigned agents fail on next dispatch with a typed error | Asymmetric handling because the filesystem may hold user work but a stale row never can |
| `workspace destroy` while agents are in non-terminal states | Refuse with `WorkspaceInUse { agent_ids }` error. User must banish or wait | Matches mental model of `git worktree remove` refusing on uncommitted changes; respects long-lived agents |
| Filewatch noise in large repos (`target/`, `node_modules/`, `.git/`) | Default ignore globs mirror `wake_sources/file_watch.rs`; per-workspace override via `workspace.set_watch_ignore` | Bus-flood is the realistic failure mode of this feature; the default must be conservative |
| `memory.put` CAS conflict | RPC returns typed `CasConflict { current_version }` error. CLI exits non-zero; agents using the daemon API see the structured error | Loud failure beats silent overwrite; current_version lets the agent retry trivially |
| `git worktree add` fails (branch exists, dirty parent, path collision) | Surface the underlying git error verbatim in the RPC response; no DB row written | Don't paper over git's existing errors; user already knows how to read them |
| Two agents in the same workspace write the same file | No daemon arbitration. Both writes land; `WorkspaceFileChanged` events fire normally | Explicit coordination via memory KV is the answer; mediating filesystem writes is out of scope |
| Agent summoned into a workspace that's mid-destroy | Refuse with `WorkspaceDestroying` | State machine is `Active → Destroying → gone`; new assignments only allowed in `Active` |
| Memory key/value size limits | Per-value cap (e.g. 256 KiB), per-workspace total cap (configurable in `tome`); enforced on `memory.put` | Don't let memory KV become a file store; that's what the worktree is for |

## Security Considerations

- **Workspace path traversal.** Workspace names are user-supplied and become directory names under `~/.grimoire/workspaces/`. Names must be validated (`[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}`) to prevent escape. The `repo_path` argument must be canonicalized + checked to be a directory the daemon can reach; if a future tenant model exists, it must be inside the tenant's allowed roots.
- **Branch / repo argument injection.** `git worktree add` is shelled out; arguments must be passed as separate `Command::arg` values (not interpolated into a shell string). Same hardening already applied to provider spawns.
- **Memory value confidentiality.** Values are stored in SQLite at rest with no encryption — same posture as `events` and `mail`. If a value contains secrets, it has the same exposure as any other daemon-stored data; compensating controls are filesystem perms on the SQLite file.
- **No RO enforcement.** Documented explicitly so users don't assume `--workspace` provides isolation between *adversarial* agents. v1 is for cooperative agents under one user.
- **Filewatch information leak.** Topic subscribers see filenames as they change. If a workspace contains files an agent shouldn't see, the agent shouldn't be in that workspace — this is consistent with the no-RO posture.
- **Defer to Part 1 hardening.** Auth tokens on UDS, policy engine (allowlist of cwd prefixes per token), and OS-level sandboxing are all explicitly out of scope; this plan does not preempt them.

## Failure Modes & Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Crash-mid-create produces orphan filesystem state that confuses next boot | Medium | Medium | Reconciliation routine on boot; don't auto-delete worktree dirs (may hold user work); log + report from `grim workspace list` so the user can act |
| Filewatch storms in large repos drown the bus | High if defaults are wrong | High | Conservative default ignores mirroring wake_sources; per-workspace overrides; integration test on a real repo with `target/` builds churning |
| CAS contention causes agents to thrash on retries | Low (single-user v1) | Low | Document the retry pattern; surface `current_version` in the error so retries are 1 RTT |
| `git worktree add` fails in surprising ways (submodules, sparse checkout, LFS) | Medium | Medium | Pass git errors through verbatim; doc page enumerates known-fragile cases; don't try to "fix" git in v1 |
| Memory table grows unboundedly | Medium | Medium | Per-value cap + per-workspace cap; cascade-delete on workspace destroy; v2 adds explicit `memory.compact` |
| Workspace destroy races with in-flight assignment | Low | Low | State machine enforces `Active → Destroying`; assignment in `Destroying` errors out |
| Agents exfiltrate via writing into someone else's workspace path | Out of scope | n/a | Single-user v1; future ACL milestone |

## Open Questions

- [ ] Should `workspace.create` accept `--copy-from <other-workspace>` so swarm-decompose can fork a parent's worktree+memory cheaply? (Probably v2; flag here in case it changes the schema.)
- [ ] Cross-host workspaces under `grimw` (worker pool §1): when a remote worker runs an agent assigned to a workspace, does the worker need a checkout of its own, or do we punt with "workspaces only on workers that share the daemon's filesystem"? Recommend punt for v1; needs a one-line constraint in the spec.
- [ ] Should `WorkspaceFileChanged` events be batched (one event per debounce window with N paths) or fanned (one per file)? Performance vs. observability — defer to spec phase, decide based on a real-repo benchmark.
- [ ] Memory value type — bytes vs. JSON. Storing JSON makes `memory.list` filtering nicer; storing bytes is simpler. Recommend JSON-only at the API layer with bytes underneath, but flag for the spec.

## Alternatives Considered

### Pure logical namespace (no filesystem provisioning)

**Description:** Workspace is a name + ACL + memory namespace + filewatch root, but the daemon never provisions a worktree. Users keep managing their own checkouts.
**Rejected because:** It loses the "parallel agents don't trample" win that drove the feature. The user explicitly cited that pain. Also leaves the filewatch root unanchored — what would the daemon watch?

### Plain dir with optional worktree

**Description:** Workspace is a daemon-owned directory under `~/.grimoire/workspaces/<name>`, with `git worktree add` only triggered if `--from-repo` is given.
**Rejected because:** The non-git path adds branching in every code path (provisioning, destroy, watch root) for a use case the user does not have. "Always a git worktree" is simpler now and trivially loosened later if a real non-git use case appears.

### Memory-only v1 (skip workspaces)

**Description:** Ship just the KV — namespaced under a string scope but no worktree primitive — then add workspaces in v2.
**Rejected because:** The KV's primary value is *coupled to a worktree* (agents collaborating on code in a shared checkout). A standalone KV without the spatial coordination would be used and then demanded a workspace concept anyway, just bolted on with a worse seam.

### OS-level RO ACL in v1

**Description:** Spawn observer agents under a separate UID or chmod the worktree readonly per-process. Real teeth, real enforcement.
**Rejected because:** 2–3 weeks of platform-specific plumbing (helper binary, sudo at install, macOS vs Linux divergence) for a feature that — in v1 — is for the author dogfooding cooperative agents. This belongs in the Part 1 §3 sandboxing milestone where the cost is shared across all sandbox use cases.

### Vector / embedding store in v1

**Description:** Store memory values with embeddings; expose `memory.search` as semantic search.
**Rejected because:** Brings in an embedding model dependency, vector index choice, and a query semantics design. The KV has independent value; semantic search is a separate product question.

---

*This plan captures the "what", "why", and high-level "how". It is input for `/hatch:write-spec`, which produces the detailed implementation specification (file paths, function signatures, atomic task breakdown, contract tests).*
