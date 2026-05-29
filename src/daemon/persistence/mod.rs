use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::path::Path;
use std::sync::Arc;

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{
    Agent, AgentState, Mail, MailState, Pact, PactState, RestartPolicy, Scroll, ScrollState,
    Subscription, Task, TaskState, WakeSource, WakeSourceKind, WakeSourceState,
};

pub mod agent_lifecycle;
mod agents;
mod mail;
mod pacts;
mod peers;
mod scrolls;
mod supervision;
mod wake;

/// One row in the `task_queue` table: work that has been requested but not
/// yet dispatched to an executor. Lives alongside the `agents` row whose `id`
/// it shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRow {
    pub id: crate::shared::types::AgentId,
    pub lane: String,
    pub priority: i64,
    pub enqueued_at: DateTime<Utc>,
    pub provider_name: Option<String>,
    pub cwd: String,
    pub model: Option<String>,
    pub task_text: String,
    pub block_reason: Option<String>,
}

/// One row in the `eval_results` table: a single rubric-scored verdict
/// from `evaluator_id` against `target_id`. Multiple verdicts per target
/// are allowed (different rubrics / evaluators); look-ups go through
/// `Database::list_eval_results`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EvalResultRow {
    pub id: String,
    pub target_id: String,
    pub evaluator_id: String,
    pub score: f64,
    pub verdict: Option<String>,
    pub rationale: Option<String>,
    pub created_at: i64,
}

/// One row of the durable `events` stream log, with its payload already
/// deserialized back into a `StreamEvent`. Returned by `read_stream_events`
/// and consumed by the replay/chronicle path.
#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub seq: i64,
    pub kind: String,
    /// RFC 3339 timestamp string as stored (kept as text so callers can parse
    /// it relative to the first event without a chrono dependency here).
    pub ts: String,
    pub event: StreamEvent,
}

/// Summary of daemon restart-recovery: which mid-flight agents were flipped
/// to `Failed` (with their prior state, so callers can publish accurate
/// `StateChange` events) and how many `Queued` agents survived for the
/// scheduler to discover on its first tick.
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub failed: Vec<(crate::shared::types::AgentId, AgentState)>,
    pub queued_remaining: usize,
}

/// One outbox fanout row: (peer_id, outbox_id, mail_id, recipient, body, sender, created_at).
pub type OutboxFanoutRow = (String, String, String, String, String, Option<String>, i64);

/// Column list for `SELECT … FROM mail`. Matches `row_to_mail`. Shared by
/// `mail` and `peers` submodules.
pub(super) const MAIL_COLS: &str = "id, recipient_id, sender_id, topic, body, in_reply_to, state, fail_reason, created_at, delivered_at, seq, wake_eligible";

pub struct Database {
    conn: Mutex<Connection>,
}

/// Best-effort `ALTER TABLE ADD COLUMN` for forward-only migrations. Returns
/// `Ok(())` whether or not the column already exists. `column_ddl` is the
/// full DDL fragment (e.g. `"keep_alive INTEGER NOT NULL DEFAULT 0"`).
/// All identifiers are crate literals, no injection surface.
fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    column_ddl: &str,
) -> Result<()> {
    let probe = format!("SELECT {column} FROM {table} LIMIT 0");
    if conn.prepare(&probe).is_err() {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column_ddl};"))?;
    }
    Ok(())
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.migrate()?;
        Ok(db)
    }

    /// Run a synchronous DB closure on the blocking thread pool so the
    /// caller's Tokio worker stays free. The Arc clone keeps lifetimes
    /// simple, the closure owns its handle for the duration.
    pub async fn run<F, R>(self: &Arc<Self>, f: F) -> R
    where
        F: FnOnce(&Self) -> R + Send + 'static,
        R: Send + 'static,
    {
        let me = self.clone();
        tokio::task::spawn_blocking(move || f(&me))
            .await
            .expect("DB blocking task panicked")
    }

    /// Test-only helper: run a closure with locked access to the underlying
    /// connection. Used by integration tests to inspect schema/contents
    /// without exposing every read query as a public API.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_test_conn<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&Connection) -> T,
    {
        let conn = self.conn.lock();
        f(&conn)
    }

    /// Lock and return a guard on the underlying connection. Used by sibling
    /// modules (e.g. `workspace_db`) that need transactional access.
    pub(crate) fn workspace_conn_lock(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.conn.lock()
    }

    /// Open an in-memory database (for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.migrate()?;
        Ok(db)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS agents (
                id          TEXT PRIMARY KEY,
                name        TEXT,
                state       TEXT NOT NULL,
                task        TEXT,
                model       TEXT,
                provider    TEXT,
                cwd         TEXT NOT NULL,
                pid         INTEGER,
                session_id  TEXT,
                exit_code   INTEGER,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                worker_id   TEXT
            );

            CREATE TABLE IF NOT EXISTS agent_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id    TEXT NOT NULL REFERENCES agents(id),
                event_type  TEXT NOT NULL,
                payload     TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_agent_events_agent_id
                ON agent_events(agent_id);

            CREATE TABLE IF NOT EXISTS pacts (
                id          TEXT PRIMARY KEY,
                source_id   TEXT NOT NULL REFERENCES agents(id),
                task_tpl    TEXT NOT NULL,
                name        TEXT,
                state       TEXT NOT NULL DEFAULT 'pending',
                target_id   TEXT,
                created_at  TEXT NOT NULL,
                fired_at    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_pacts_source_id
                ON pacts(source_id);
            ",
        )?;

        add_column_if_missing(&conn, "agents", "provider", "provider TEXT")?;
        add_column_if_missing(&conn, "agents", "worker_id", "worker_id TEXT")?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS scrolls (
                id              TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                state           TEXT NOT NULL DEFAULT 'inscribed',
                source_path     TEXT,
                max_concurrency INTEGER NOT NULL DEFAULT 4,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id              TEXT PRIMARY KEY,
                scroll_id       TEXT NOT NULL REFERENCES scrolls(id),
                name            TEXT NOT NULL,
                prompt          TEXT NOT NULL,
                state           TEXT NOT NULL DEFAULT 'blocked',
                agent_id        TEXT,
                provider        TEXT,
                model           TEXT,
                cwd             TEXT,
                file_patterns   TEXT NOT NULL DEFAULT '[]',
                order_index     INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tasks_scroll_id ON tasks(scroll_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_agent_id ON tasks(agent_id);

            CREATE TABLE IF NOT EXISTS task_dependencies (
                task_id         TEXT NOT NULL REFERENCES tasks(id),
                depends_on_id   TEXT NOT NULL REFERENCES tasks(id),
                PRIMARY KEY (task_id, depends_on_id)
            );

            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY,
                agent_id    TEXT,
                scroll_id   TEXT,
                seq         INTEGER NOT NULL,
                kind        TEXT NOT NULL,
                payload     TEXT NOT NULL,
                ts          TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_agent_seq  ON events(agent_id, seq);
            CREATE INDEX IF NOT EXISTS idx_events_scroll_seq ON events(scroll_id, seq);

            CREATE TABLE IF NOT EXISTS task_queue (
                id              TEXT PRIMARY KEY,
                lane            TEXT NOT NULL,
                priority        INTEGER NOT NULL DEFAULT 0,
                enqueued_at     TEXT NOT NULL,
                provider_name   TEXT,
                cwd             TEXT NOT NULL,
                model           TEXT,
                task_text       TEXT NOT NULL,
                block_reason    TEXT,
                FOREIGN KEY (id) REFERENCES agents(id)
            );
            CREATE INDEX IF NOT EXISTS idx_task_queue_dispatch
                ON task_queue (lane, priority DESC, enqueued_at, id);

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

            CREATE TABLE IF NOT EXISTS wake_sources (
                id              TEXT PRIMARY KEY,
                agent_id        TEXT NOT NULL,
                kind            TEXT NOT NULL,
                config_json     TEXT NOT NULL,
                state           TEXT NOT NULL,
                fail_reason     TEXT,
                last_fired_at   INTEGER,
                fire_count      INTEGER NOT NULL DEFAULT 0,
                created_at      INTEGER NOT NULL,
                FOREIGN KEY(agent_id) REFERENCES agents(id)
            );
            CREATE INDEX IF NOT EXISTS wake_sources_by_agent ON wake_sources(agent_id);
            CREATE INDEX IF NOT EXISTS wake_sources_armed
                ON wake_sources(state) WHERE state = 'armed';

            CREATE TABLE IF NOT EXISTS wake_rate_limits (
                agent_id        TEXT PRIMARY KEY,
                tokens          REAL NOT NULL,
                last_refill_at  INTEGER NOT NULL,
                capacity        INTEGER NOT NULL DEFAULT 60,
                refill_per_sec  REAL NOT NULL DEFAULT 0.01666666,
                FOREIGN KEY(agent_id) REFERENCES agents(id)
            );

            CREATE TABLE IF NOT EXISTS restart_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id        TEXT NOT NULL REFERENCES agents(id),
                attempted_at    INTEGER NOT NULL,
                outcome         TEXT NOT NULL,
                error_summary   TEXT
            );
            CREATE INDEX IF NOT EXISTS restart_history_by_agent_window
                ON restart_history(agent_id, attempted_at);
            CREATE INDEX IF NOT EXISTS restart_history_by_time
                ON restart_history(attempted_at);
            ",
        )?;

        add_column_if_missing(
            &conn,
            "agents",
            "keep_alive",
            "keep_alive INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &conn,
            "agents",
            "restart_policy",
            "restart_policy TEXT NOT NULL DEFAULT 'never'",
        )?;
        add_column_if_missing(&conn, "agents", "max_restarts", "max_restarts INTEGER")?;
        add_column_if_missing(
            &conn,
            "agents",
            "restart_window_secs",
            "restart_window_secs INTEGER",
        )?;
        add_column_if_missing(&conn, "agents", "escalate_to", "escalate_to TEXT")?;
        add_column_if_missing(
            &conn,
            "agents",
            "restart_count",
            "restart_count INTEGER NOT NULL DEFAULT 0",
        )?;
        add_column_if_missing(
            &conn,
            "agents",
            "escalation_depth",
            "escalation_depth INTEGER NOT NULL DEFAULT 0",
        )?;
        // Token-budget bookkeeping: cumulative input+output tokens charged to
        // the agent across all of its turns. Updated at process exit when the
        // provider reports a `usage` block. `SandboxConfig.token_budget`
        // compares against this value before the next dispatch.
        add_column_if_missing(
            &conn,
            "agents",
            "tokens_used",
            "tokens_used INTEGER NOT NULL DEFAULT 0",
        )?;
        // Supervision tree: when set, this agent dies with its parent.
        // Kept as a DB-only column (out of `Agent`) to minimise blast radius;
        // looked up via `db.list_children` only when a parent banishes.
        add_column_if_missing(&conn, "agents", "parent_agent_id", "parent_agent_id TEXT")?;
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_agents_parent ON agents(parent_agent_id);",
        )?;
        // USD spend attributed to this agent across its life, computed at
        // each run-completion from `tokens_used` × `[providers.<name>.pricing]`.
        // Stored as REAL (cents-precision is fine; vendor pricing is dollars
        // per million tokens).
        add_column_if_missing(
            &conn,
            "agents",
            "usd_spent",
            "usd_spent REAL NOT NULL DEFAULT 0",
        )?;
        // Per-budget, per-UTC-day spend ledger. Primary key is composite so
        // a budget can spend across many days and the `today` lookup is a
        // single indexed row read.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS budget_spend (
                budget_name TEXT NOT NULL,
                day         TEXT NOT NULL,
                usd         REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (budget_name, day)
            );",
        )?;
        // Rubric-scored evaluations of one agent's transcript by another.
        // Many-per-target (a target can be eval'd against different rubrics
        // or by different evaluators), keyed by a synthetic id so callers
        // can reference / ack a specific verdict.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS eval_results (
                id           TEXT PRIMARY KEY,
                target_id    TEXT NOT NULL,
                evaluator_id TEXT NOT NULL,
                score        REAL NOT NULL,
                verdict      TEXT,
                rationale    TEXT,
                created_at   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_eval_results_target
                ON eval_results(target_id, created_at);",
        )?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS workspaces (
                id          TEXT PRIMARY KEY,
                path        TEXT NOT NULL UNIQUE,
                repo_path   TEXT NOT NULL,
                branch      TEXT NOT NULL,
                state       TEXT NOT NULL,
                created_at  INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS workspace_memory (
                workspace_id TEXT NOT NULL,
                key          TEXT NOT NULL,
                value        BLOB NOT NULL,
                version      INTEGER NOT NULL,
                updated_at   INTEGER NOT NULL,
                updated_by   TEXT NOT NULL,
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

            -- Federation: peer link metadata, outbox, inbox, topic federations.
            CREATE TABLE IF NOT EXISTS peers (
                id                  TEXT PRIMARY KEY,
                daemon_id           TEXT NOT NULL,
                name                TEXT NOT NULL UNIQUE,
                url                 TEXT NOT NULL,
                bearer_token_hash   BLOB NOT NULL UNIQUE,
                bearer_token        TEXT NOT NULL,
                public_key          BLOB,
                state               TEXT NOT NULL,
                last_seen           INTEGER,
                registered_at       INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS peers_by_daemon_id ON peers(daemon_id);

            CREATE TABLE IF NOT EXISTS peer_outbox (
                id              TEXT PRIMARY KEY,
                peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                mail_id         TEXT NOT NULL,
                sender_seq      INTEGER NOT NULL,
                recipient       TEXT NOT NULL,
                sender          TEXT,
                topic           TEXT,
                body            TEXT NOT NULL,
                created_at      INTEGER NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                state           TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS peer_outbox_drain
                ON peer_outbox(peer_id, state, next_attempt_at);
            CREATE UNIQUE INDEX IF NOT EXISTS peer_outbox_seq
                ON peer_outbox(peer_id, sender_seq);

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
                direction   TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                UNIQUE (peer_id, topic)
            );
            CREATE INDEX IF NOT EXISTS topic_federations_by_topic
                ON topic_federations(topic);

            -- Federated namespace memory. A namespace is a string-named KV
            -- store decoupled from git workspaces; it can replicate to peers.
            -- Conflict resolution is last-write-wins on the (lamport,
            -- origin_daemon_id) tuple. Deletes are tombstones (deleted=1) so
            -- they propagate by the same LWW rule. See docs/specs for the v2
            -- vector-clock design.
            CREATE TABLE IF NOT EXISTS namespace_memory (
                namespace        TEXT NOT NULL,
                key              TEXT NOT NULL,
                value            BLOB NOT NULL,
                lamport          INTEGER NOT NULL,
                origin_daemon_id TEXT NOT NULL,
                deleted          INTEGER NOT NULL DEFAULT 0,
                updated_at       INTEGER NOT NULL,
                updated_by       TEXT NOT NULL,
                PRIMARY KEY (namespace, key)
            );
            CREATE INDEX IF NOT EXISTS namespace_memory_by_key
                ON namespace_memory(namespace, key);

            -- Per-daemon Lamport clock (single row). Advances on every local
            -- write and on observing a remote write's timestamp.
            CREATE TABLE IF NOT EXISTS namespace_lamport (
                node    INTEGER PRIMARY KEY CHECK (node = 0),
                counter INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO namespace_lamport (node, counter) VALUES (0, 0);

            -- Which peers a namespace replicates to, and in which direction.
            -- Mirrors topic_federations.
            CREATE TABLE IF NOT EXISTS namespace_federations (
                id          TEXT PRIMARY KEY,
                peer_id     TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                namespace   TEXT NOT NULL,
                direction   TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                UNIQUE (peer_id, namespace)
            );
            CREATE INDEX IF NOT EXISTS namespace_federations_by_ns
                ON namespace_federations(namespace);

            -- Durable per-peer queue of namespace writes awaiting replication.
            -- Same Pending/InFlight/Delivered backoff state machine as
            -- peer_outbox; redelivery is safe because LWW apply is idempotent.
            CREATE TABLE IF NOT EXISTS namespace_outbox (
                id               TEXT PRIMARY KEY,
                peer_id          TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                op_id            TEXT NOT NULL,
                namespace        TEXT NOT NULL,
                key              TEXT NOT NULL,
                value            BLOB NOT NULL,
                lamport          INTEGER NOT NULL,
                origin_daemon_id TEXT NOT NULL,
                deleted          INTEGER NOT NULL,
                updated_by       TEXT NOT NULL,
                created_at       INTEGER NOT NULL,
                attempts         INTEGER NOT NULL DEFAULT 0,
                next_attempt_at  INTEGER NOT NULL,
                state            TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS namespace_outbox_drain
                ON namespace_outbox(peer_id, state, next_attempt_at);
            ",
        )?;

        add_column_if_missing(&conn, "agents", "workspace_id", "workspace_id TEXT")?;

        // Shadow workspaces have no on-disk worktree, so the path column is
        // filled with a sentinel `shadow://<home-id>/<ws-id>` so the
        // existing UNIQUE constraint still holds.
        add_column_if_missing(
            &conn,
            "workspaces",
            "kind",
            "kind TEXT NOT NULL DEFAULT 'Local'",
        )?;
        add_column_if_missing(&conn, "workspaces", "home_daemon_id", "home_daemon_id TEXT")?;
        add_column_if_missing(
            &conn,
            "workspaces",
            "home_workspace_id",
            "home_workspace_id TEXT",
        )?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workspace_federations (
                id           TEXT PRIMARY KEY,
                peer_id      TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                workspace_id TEXT NOT NULL,
                direction    TEXT NOT NULL,
                created_at   INTEGER NOT NULL,
                UNIQUE (peer_id, workspace_id)
             );
             CREATE INDEX IF NOT EXISTS workspace_federations_by_ws
                ON workspace_federations(workspace_id);",
        )?;

        // F3b: per-peer outbox for workspace file events. Mirrors the
        // namespace_outbox shape — `sender_seq` is the monotonic
        // correlation key the receiver acks back; `payload` is the
        // JSON-serialized WorkspaceFileChanged batch.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workspace_event_outbox (
                id              TEXT PRIMARY KEY,
                peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                workspace_id    TEXT NOT NULL,
                sender_seq      INTEGER NOT NULL,
                payload         BLOB NOT NULL,
                created_at      INTEGER NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                state           TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS workspace_event_outbox_seq
                ON workspace_event_outbox(peer_id, sender_seq);
             CREATE INDEX IF NOT EXISTS workspace_event_outbox_due
                ON workspace_event_outbox(peer_id, state, next_attempt_at);",
        )?;

        // F3c: receiver-side dedupe. `(sender_daemon_id, sender_seq)` is
        // the at-least-once correlation key — the sender retries until
        // it gets a positive ack, so the receiver must ignore replays.
        // `received_at` is purely informational / for future pruning.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS workspace_event_inbox (
                sender_daemon_id TEXT NOT NULL,
                sender_seq       INTEGER NOT NULL,
                workspace_id     TEXT NOT NULL,
                received_at      INTEGER NOT NULL,
                PRIMARY KEY (sender_daemon_id, sender_seq)
             );
             CREATE INDEX IF NOT EXISTS workspace_event_inbox_by_ws
                ON workspace_event_inbox(workspace_id);",
        )?;

        // F4b: agent lifecycle federation. Subscription rows are
        // per-peer (no per-agent filter on the wire — receivers filter
        // via the `RemoteAgentCompletion` wake source's config). Outbox
        // mirrors `workspace_event_outbox`. Inbox dedupes on
        // `(sender_daemon_id, sender_seq)`.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agent_lifecycle_federations (
                id           TEXT PRIMARY KEY,
                peer_id      TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                direction    TEXT NOT NULL,
                created_at   INTEGER NOT NULL,
                UNIQUE (peer_id)
             );
             CREATE TABLE IF NOT EXISTS agent_lifecycle_outbox (
                id              TEXT PRIMARY KEY,
                peer_id         TEXT NOT NULL REFERENCES peers(id) ON DELETE CASCADE,
                sender_seq      INTEGER NOT NULL,
                payload         BLOB NOT NULL,
                created_at      INTEGER NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                state           TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS agent_lifecycle_outbox_seq
                ON agent_lifecycle_outbox(peer_id, sender_seq);
             CREATE INDEX IF NOT EXISTS agent_lifecycle_outbox_due
                ON agent_lifecycle_outbox(peer_id, state, next_attempt_at);
             CREATE TABLE IF NOT EXISTS agent_lifecycle_inbox (
                sender_daemon_id TEXT NOT NULL,
                sender_seq       INTEGER NOT NULL,
                received_at      INTEGER NOT NULL,
                PRIMARY KEY (sender_daemon_id, sender_seq)
             );",
        )?;

        Ok(())
    }

    pub(super) fn exec(&self, sql: &str, params: impl rusqlite::Params) -> Result<usize> {
        Ok(self.conn.lock().execute(sql, params)?)
    }

    pub(super) fn query_opt<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        map: impl FnOnce(&rusqlite::Row) -> Result<T>,
    ) -> Result<Option<T>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        match rows.next()? {
            Some(row) => Ok(Some(map(row)?)),
            None => Ok(None),
        }
    }

    pub(super) fn query_vec<T>(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
        mut map: impl FnMut(&rusqlite::Row) -> Result<T>,
    ) -> Result<Vec<T>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(sql)?;
        let mut rows = stmt.query(params)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map(row)?);
        }
        Ok(out)
    }

    /// Provide locked access to the underlying connection for submodules that
    /// need to run multi-statement transactions. Kept `pub(super)` so it does
    /// not leak outside the persistence module.
    pub(super) fn conn_lock(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.conn.lock()
    }
}

/// Current unix epoch seconds. Used for `mail.created_at` / `mail.delivered_at`
/// and for `subscriptions.created_at`.
pub fn unix_now() -> i64 {
    Utc::now().timestamp()
}

/// Parse an RFC3339 timestamp from a DB column, returning a proper error instead of panicking.
pub(super) fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| anyhow::anyhow!("invalid timestamp '{s}': {e}"))
}

pub(super) fn row_to_subscription(row: &rusqlite::Row) -> Result<Subscription> {
    Ok(Subscription {
        id: row.get(0)?,
        subscriber_id: row.get(1)?,
        topic: row.get(2)?,
        created_at: row.get(3)?,
    })
}

pub(super) fn row_to_peer(row: &rusqlite::Row) -> Result<crate::shared::types::Peer> {
    use crate::shared::types::{Peer, PeerState};
    let state_str: String = row.get(7)?;
    Ok(Peer {
        id: row.get(0)?,
        daemon_id: row.get(1)?,
        name: row.get(2)?,
        url: row.get(3)?,
        bearer_token_hash: row.get(4)?,
        bearer_token: row.get(5)?,
        public_key: row.get(6)?,
        state: state_str
            .parse::<PeerState>()
            .map_err(|e| anyhow::anyhow!("peer state: {e}"))?,
        last_seen: row.get(8)?,
        registered_at: row.get(9)?,
    })
}

pub(super) fn row_to_outbox(row: &rusqlite::Row) -> Result<crate::shared::types::PeerOutboxRow> {
    use crate::shared::types::{PeerOutboxRow, PeerOutboxState};
    let state_str: String = row.get(11)?;
    let attempts: i64 = row.get(9)?;
    let sender_seq: i64 = row.get(3)?;
    Ok(PeerOutboxRow {
        id: row.get(0)?,
        peer_id: row.get(1)?,
        mail_id: row.get(2)?,
        sender_seq: sender_seq as u64,
        recipient: row.get(4)?,
        sender: row.get(5)?,
        topic: row.get(6)?,
        body: row.get(7)?,
        created_at: row.get(8)?,
        attempts: attempts as u32,
        next_attempt_at: row.get(10)?,
        state: state_str
            .parse::<PeerOutboxState>()
            .map_err(|e| anyhow::anyhow!("outbox state: {e}"))?,
    })
}

pub(super) fn row_to_topic_federation(
    row: &rusqlite::Row,
) -> Result<crate::shared::types::TopicFederation> {
    use crate::shared::types::{FederationDirection, TopicFederation};
    let dir: String = row.get(3)?;
    Ok(TopicFederation {
        id: row.get(0)?,
        peer_id: row.get(1)?,
        topic: row.get(2)?,
        direction: dir
            .parse::<FederationDirection>()
            .map_err(|e| anyhow::anyhow!("direction: {e}"))?,
        created_at: row.get(4)?,
    })
}

pub(super) fn row_to_mail(row: &rusqlite::Row) -> Result<Mail> {
    let state_str: String = row.get(6)?;
    let wake_eligible: i64 = row.get(11)?;
    Ok(Mail {
        id: row.get(0)?,
        recipient_id: row.get(1)?,
        sender_id: row.get(2)?,
        topic: row.get(3)?,
        body: row.get(4)?,
        in_reply_to: row.get(5)?,
        state: state_str.parse().unwrap_or(MailState::Failed),
        fail_reason: row.get(7)?,
        created_at: row.get(8)?,
        delivered_at: row.get(9)?,
        seq: row.get(10)?,
        wake_eligible: wake_eligible != 0,
    })
}

pub(super) fn row_to_scroll(row: &rusqlite::Row) -> Result<Scroll> {
    let state_str: String = row.get(2)?;
    let created_str: String = row.get(5)?;
    let updated_str: String = row.get(6)?;

    Ok(Scroll {
        id: row.get(0)?,
        name: row.get(1)?,
        state: state_str.parse().unwrap_or(ScrollState::Failed),
        source_path: row.get(3)?,
        max_concurrency: row.get(4)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
    })
}

pub(super) fn row_to_task(row: &rusqlite::Row) -> Result<Task> {
    let state_str: String = row.get(4)?;
    let file_patterns_json: String = row.get(9)?;
    let created_str: String = row.get(11)?;
    let updated_str: String = row.get(12)?;

    Ok(Task {
        id: row.get(0)?,
        scroll_id: row.get(1)?,
        name: row.get(2)?,
        prompt: row.get(3)?,
        state: state_str.parse().unwrap_or(TaskState::Failed),
        agent_id: row.get(5)?,
        provider: row.get(6)?,
        model: row.get(7)?,
        cwd: row.get(8)?,
        file_patterns: serde_json::from_str(&file_patterns_json).unwrap_or_default(),
        order_index: row.get(10)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
    })
}

pub(super) fn row_to_pact(row: &rusqlite::Row) -> Result<Pact> {
    let state_str: String = row.get(4)?;
    let created_str: String = row.get(6)?;
    let fired_str: Option<String> = row.get(7)?;

    Ok(Pact {
        id: row.get(0)?,
        source_id: row.get(1)?,
        task_tpl: row.get(2)?,
        name: row.get(3)?,
        state: state_str.parse().unwrap_or(PactState::Failed),
        target_id: row.get(5)?,
        created_at: parse_timestamp(&created_str)?,
        fired_at: fired_str.as_deref().map(parse_timestamp).transpose()?,
    })
}

pub(super) fn row_to_queue_row(row: &rusqlite::Row) -> Result<QueueRow> {
    let enqueued_str: String = row.get(3)?;
    Ok(QueueRow {
        id: row.get(0)?,
        lane: row.get(1)?,
        priority: row.get(2)?,
        enqueued_at: parse_timestamp(&enqueued_str)?,
        provider_name: row.get(4)?,
        cwd: row.get(5)?,
        model: row.get(6)?,
        task_text: row.get(7)?,
        block_reason: row.get(8)?,
    })
}

pub(super) fn row_to_wake_source(row: &rusqlite::Row) -> Result<WakeSource> {
    let kind_str: String = row.get(2)?;
    let state_str: String = row.get(4)?;
    Ok(WakeSource {
        id: row.get(0)?,
        agent_id: row.get(1)?,
        kind: kind_str.parse().unwrap_or(WakeSourceKind::Cron),
        config_json: row.get(3)?,
        state: state_str.parse().unwrap_or(WakeSourceState::Failed),
        fail_reason: row.get(5)?,
        last_fired_at: row.get(6)?,
        fire_count: row.get(7)?,
        created_at: row.get(8)?,
    })
}

pub(super) fn row_to_agent(row: &rusqlite::Row) -> Result<Agent> {
    let state_str: String = row.get(2)?;
    let cwd_str: String = row.get(6)?;
    let created_str: String = row.get(10)?;
    let updated_str: String = row.get(11)?;
    let policy_str: Option<String> = row.get(13)?;
    let restart_policy: RestartPolicy = policy_str
        .as_deref()
        .unwrap_or("never")
        .parse()
        .unwrap_or(RestartPolicy::Never);
    let restart_count: i64 = row.get::<_, Option<i64>>(14)?.unwrap_or(0);

    Ok(Agent {
        id: row.get(0)?,
        name: row.get(1)?,
        state: state_str.parse().unwrap_or(AgentState::Failed),
        task: row.get(3)?,
        model: row.get(4)?,
        provider: row.get(5)?,
        cwd: std::path::PathBuf::from(cwd_str),
        pid: row.get::<_, Option<u32>>(7)?,
        session_id: row.get(8)?,
        exit_code: row.get(9)?,
        created_at: parse_timestamp(&created_str)?,
        updated_at: parse_timestamp(&updated_str)?,
        worker_id: row.get::<_, Option<String>>(12)?,
        restart_policy,
        restart_count: restart_count.max(0) as u32,
        workspace_id: row.get::<_, Option<String>>(15)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::types::*;
    use std::path::PathBuf;

    fn test_db() -> Database {
        Database::open_in_memory().unwrap()
    }

    fn make_agent(id: &str) -> Agent {
        Agent {
            id: id.to_string(),
            name: Some(format!("agent-{id}")),
            state: AgentState::Active,
            task: Some("test task".to_string()),
            model: Some("sonnet".to_string()),
            provider: Some("claude".to_string()),
            cwd: PathBuf::from("/tmp"),
            pid: Some(1234),
            session_id: None,
            exit_code: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            worker_id: None,
            restart_policy: RestartPolicy::Never,
            restart_count: 0,
            workspace_id: None,
        }
    }

    fn make_scroll(id: &str) -> Scroll {
        Scroll {
            id: id.to_string(),
            name: format!("Scroll {id}"),
            state: ScrollState::Active,
            source_path: None,
            max_concurrency: 4,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn make_task(id: &str, scroll_id: &str, state: TaskState) -> Task {
        Task {
            id: id.to_string(),
            scroll_id: scroll_id.to_string(),
            name: format!("Task {id}"),
            prompt: "test".to_string(),
            state,
            agent_id: None,
            provider: None,
            model: None,
            cwd: None,
            file_patterns: vec![],
            order_index: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn agent_insert_and_get() {
        let db = test_db();
        let agent = make_agent("abc12345");
        db.insert_agent(&agent).unwrap();

        let fetched = db.get_agent("abc12345").unwrap().unwrap();
        assert_eq!(fetched.id, "abc12345");
        assert_eq!(fetched.name.as_deref(), Some("agent-abc12345"));
        assert_eq!(fetched.state, AgentState::Active);
        assert_eq!(fetched.provider.as_deref(), Some("claude"));
    }

    #[test]
    fn agent_not_found() {
        let db = test_db();
        assert!(db.get_agent("nonexistent").unwrap().is_none());
    }

    #[test]
    fn agent_list_and_filter() {
        let db = test_db();

        let mut a1 = make_agent("aaaa1111");
        a1.state = AgentState::Active;
        db.insert_agent(&a1).unwrap();

        let mut a2 = make_agent("bbbb2222");
        a2.state = AgentState::Complete;
        db.insert_agent(&a2).unwrap();

        assert_eq!(db.list_agents(None).unwrap().len(), 2);
        assert_eq!(db.list_agents(Some("active")).unwrap().len(), 1);
        assert_eq!(db.list_agents(Some("complete")).unwrap().len(), 1);
        assert_eq!(db.list_agents(Some("banished")).unwrap().len(), 0);
    }

    #[test]
    fn agent_state_transition() {
        let db = test_db();
        db.insert_agent(&make_agent("state111")).unwrap();

        db.update_agent_state("state111", &AgentState::Complete, Some(0))
            .unwrap();

        let fetched = db.get_agent("state111").unwrap().unwrap();
        assert_eq!(fetched.state, AgentState::Complete);
        assert_eq!(fetched.exit_code, Some(0));
    }

    #[test]
    fn agent_session_id_update() {
        let db = test_db();
        db.insert_agent(&make_agent("sess1111")).unwrap();
        db.update_agent_session_id("sess1111", "session-abc")
            .unwrap();

        let fetched = db.get_agent("sess1111").unwrap().unwrap();
        assert_eq!(fetched.session_id.as_deref(), Some("session-abc"));
    }

    #[test]
    fn event_insert_and_tail() {
        let db = test_db();
        db.insert_agent(&make_agent("evt11111")).unwrap();

        for i in 0..5 {
            db.insert_event(&AgentEvent {
                id: None,
                agent_id: "evt11111".to_string(),
                event_type: "stdout".to_string(),
                payload: format!("line {i}"),
                created_at: Utc::now(),
            })
            .unwrap();
        }

        let all = db.get_events("evt11111", None).unwrap();
        assert_eq!(all.len(), 5);

        let tail = db.get_events("evt11111", Some(2)).unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].payload, "line 3");
        assert_eq!(tail[1].payload, "line 4");
    }

    #[test]
    fn read_stream_events_roundtrip_and_ordering() {
        let db = test_db();
        db.insert_agent(&make_agent("rse11111")).unwrap();

        let events = [
            StreamEvent::StateChange {
                agent_id: "rse11111".into(),
                old_state: AgentState::Summoning,
                new_state: AgentState::Active,
            },
            StreamEvent::Output {
                agent_id: "rse11111".into(),
                stream: "stdout".into(),
                line: "hello".into(),
            },
            StreamEvent::Output {
                agent_id: "rse11111".into(),
                stream: "stdout".into(),
                line: "world".into(),
            },
            StreamEvent::Notification {
                agent_id: Some("rse11111".into()),
                message: "ping".into(),
                level: "info".into(),
                source: "agent".into(),
            },
        ];
        for e in &events {
            db.append_event(e).unwrap();
        }

        let stored = db.read_stream_events("rse11111").unwrap();
        assert_eq!(stored.len(), 4);
        // seq is per-agent and dense from 0.
        for (i, s) in stored.iter().enumerate() {
            assert_eq!(s.seq, i as i64);
        }
        assert_eq!(stored[0].kind, "state_change");
        assert_eq!(stored[3].kind, "notification");
        // Unknown agent reads empty, not error.
        assert!(db.read_stream_events("nope0000").unwrap().is_empty());
    }

    #[test]
    fn agent_stdout_lines_in_order() {
        let db = test_db();
        db.insert_agent(&make_agent("out11111")).unwrap();
        for line in ["first", "second", "third"] {
            db.insert_event(&AgentEvent {
                id: None,
                agent_id: "out11111".to_string(),
                event_type: "stdout".to_string(),
                payload: line.to_string(),
                created_at: Utc::now(),
            })
            .unwrap();
        }
        // stderr must not leak into the transcript.
        db.insert_event(&AgentEvent {
            id: None,
            agent_id: "out11111".to_string(),
            event_type: "stderr".to_string(),
            payload: "noise".to_string(),
            created_at: Utc::now(),
        })
        .unwrap();

        assert_eq!(
            db.get_agent_stdout_lines("out11111").unwrap(),
            vec!["first", "second", "third"]
        );
    }

    #[test]
    fn agent_transcript_budget_truncates_oldest() {
        let db = test_db();
        db.insert_agent(&make_agent("trunc111")).unwrap();
        for i in 0..100 {
            db.insert_event(&AgentEvent {
                id: None,
                agent_id: "trunc111".to_string(),
                event_type: "stdout".to_string(),
                payload: format!("line-{i:03}"),
                created_at: Utc::now(),
            })
            .unwrap();
        }
        let t = db.get_agent_transcript("trunc111", 64).unwrap();
        assert!(t.len() <= 64 + "[…earlier output truncated…]\n".len());
        assert!(t.starts_with("[…earlier output truncated…]"));
        assert!(t.ends_with("line-099")); // newest retained
        assert!(!t.contains("line-000")); // oldest dropped
    }

    #[test]
    fn agent_stdout_lines_missing() {
        let db = test_db();
        db.insert_agent(&make_agent("noout111")).unwrap();
        assert!(db.get_agent_stdout_lines("noout111").unwrap().is_empty());
    }

    #[test]
    fn pact_lifecycle() {
        let db = test_db();
        db.insert_agent(&make_agent("pact1111")).unwrap();

        let pact = Pact {
            id: "pact0001".to_string(),
            source_id: "pact1111".to_string(),
            task_tpl: "do {output}".to_string(),
            name: Some("test pact".to_string()),
            state: PactState::Pending,
            target_id: None,
            created_at: Utc::now(),
            fired_at: None,
        };
        db.insert_pact(&pact).unwrap();

        assert_eq!(db.list_pacts(None).unwrap().len(), 1);
        assert_eq!(db.get_pending_pacts_for_agent("pact1111").unwrap().len(), 1);

        db.update_pact_fired("pact0001", "target01").unwrap();

        assert!(
            db.get_pending_pacts_for_agent("pact1111")
                .unwrap()
                .is_empty()
        );
        let fired = db.list_pacts(None).unwrap();
        assert_eq!(fired[0].state, PactState::Fired);
        assert_eq!(fired[0].target_id.as_deref(), Some("target01"));
    }

    #[test]
    fn scroll_crud() {
        let db = test_db();
        let mut scroll = make_scroll("scr11111");
        scroll.state = ScrollState::Inscribed;
        db.insert_scroll(&scroll).unwrap();

        let fetched = db.get_scroll("scr11111").unwrap().unwrap();
        assert_eq!(fetched.state, ScrollState::Inscribed);

        db.update_scroll_state("scr11111", &ScrollState::Active)
            .unwrap();
        assert_eq!(
            db.get_scroll("scr11111").unwrap().unwrap().state,
            ScrollState::Active
        );
        assert_eq!(db.list_scrolls().unwrap().len(), 1);
    }

    #[test]
    fn task_dependencies_and_ready() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr22222")).unwrap();

        let task_a = make_task("task_a01", "scr22222", TaskState::Complete);
        let task_b = make_task("task_b01", "scr22222", TaskState::Blocked);
        db.insert_task(&task_a).unwrap();
        db.insert_task(&task_b).unwrap();
        db.insert_task_dependency("task_b01", "task_a01").unwrap();

        // A is complete -> B is ready
        let ready = db.find_ready_tasks("scr22222").unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "task_b01");
    }

    #[test]
    fn task_blocked_by_incomplete_dep() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr33333")).unwrap();

        let task_a = make_task("blk_a001", "scr33333", TaskState::Active);
        let task_b = make_task("blk_b001", "scr33333", TaskState::Blocked);
        db.insert_task(&task_a).unwrap();
        db.insert_task(&task_b).unwrap();
        db.insert_task_dependency("blk_b001", "blk_a001").unwrap();

        assert!(db.find_ready_tasks("scr33333").unwrap().is_empty());
    }

    #[test]
    fn count_active_tasks() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr44444")).unwrap();

        db.insert_task(&make_task("cnt_a001", "scr44444", TaskState::Active))
            .unwrap();
        db.insert_task(&make_task("cnt_b001", "scr44444", TaskState::Active))
            .unwrap();
        db.insert_task(&make_task("cnt_c001", "scr44444", TaskState::Complete))
            .unwrap();

        assert_eq!(db.count_active_tasks("scr44444").unwrap(), 2);
    }

    #[test]
    fn task_agent_lookup() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr55555")).unwrap();
        db.insert_task(&make_task("lkp_a001", "scr55555", TaskState::Ready))
            .unwrap();

        db.update_task_agent("lkp_a001", "myagent1").unwrap();

        let found = db.get_task_by_agent_id("myagent1").unwrap().unwrap();
        assert_eq!(found.id, "lkp_a001");
        assert_eq!(found.state, TaskState::Active); // update_task_agent sets active

        assert!(db.get_task_by_agent_id("nonexist").unwrap().is_none());
    }

    #[test]
    fn delete_agent_removes_events() {
        let db = test_db();
        db.insert_agent(&make_agent("del11111")).unwrap();
        db.insert_event(&AgentEvent {
            id: None,
            agent_id: "del11111".to_string(),
            event_type: "stdout".to_string(),
            payload: "hello".to_string(),
            created_at: Utc::now(),
        })
        .unwrap();

        db.delete_agent("del11111").unwrap();
        assert!(db.get_agent("del11111").unwrap().is_none());
        assert!(db.get_events("del11111", None).unwrap().is_empty());
    }

    #[test]
    fn task_dependents() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr66666")).unwrap();
        db.insert_task(&make_task("dep_a001", "scr66666", TaskState::Complete))
            .unwrap();
        db.insert_task(&make_task("dep_b001", "scr66666", TaskState::Blocked))
            .unwrap();
        db.insert_task(&make_task("dep_c001", "scr66666", TaskState::Blocked))
            .unwrap();
        db.insert_task_dependency("dep_b001", "dep_a001").unwrap();
        db.insert_task_dependency("dep_c001", "dep_a001").unwrap();

        let dependents = db.get_task_dependents("dep_a001").unwrap();
        assert_eq!(dependents.len(), 2);
        assert!(dependents.contains(&"dep_b001".to_string()));
        assert!(dependents.contains(&"dep_c001".to_string()));
    }

    #[test]
    fn all_dependencies_for_scroll() {
        let db = test_db();
        db.insert_scroll(&make_scroll("scr77777")).unwrap();
        db.insert_task(&make_task("edg_a001", "scr77777", TaskState::Complete))
            .unwrap();
        db.insert_task(&make_task("edg_b001", "scr77777", TaskState::Blocked))
            .unwrap();
        db.insert_task_dependency("edg_b001", "edg_a001").unwrap();

        let edges = db.get_all_dependencies_for_scroll("scr77777").unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0], ("edg_b001".to_string(), "edg_a001".to_string()));
    }

    #[test]
    fn pact_failed_state() {
        let db = test_db();
        db.insert_agent(&make_agent("pfail111")).unwrap();
        let pact = Pact {
            id: "pf000001".to_string(),
            source_id: "pfail111".to_string(),
            task_tpl: "do {output}".to_string(),
            name: None,
            state: PactState::Pending,
            target_id: None,
            created_at: Utc::now(),
            fired_at: None,
        };
        db.insert_pact(&pact).unwrap();
        db.update_pact_failed("pf000001").unwrap();

        let pacts = db.list_pacts(None).unwrap();
        assert_eq!(pacts[0].state, PactState::Failed);
        assert!(pacts[0].fired_at.is_some());
    }
}
