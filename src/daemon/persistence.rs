use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::{Connection, params};
use std::path::Path;

use crate::shared::protocol::StreamEvent;
use crate::shared::types::{
    Agent, AgentEvent, AgentId, AgentState, Mail, MailState, Pact, PactState,
    RestartHistoryOutcome, RestartPolicy, Scroll, ScrollState, Subscription, SupervisionConfig,
    Task, TaskId, TaskState, WakeSource, WakeSourceKind, WakeSourceState,
};

/// One row in the `task_queue` table — work that has been requested but not
/// yet dispatched to an executor. Lives alongside the `agents` row whose `id`
/// it shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRow {
    pub id: AgentId,
    pub lane: String,
    pub priority: i64,
    pub enqueued_at: DateTime<Utc>,
    pub provider_name: Option<String>,
    pub cwd: String,
    pub model: Option<String>,
    pub task_text: String,
    pub block_reason: Option<String>,
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
    pub failed: Vec<(AgentId, AgentState)>,
    pub queued_remaining: usize,
}

/// One outbox fanout row: (peer_id, outbox_id, mail_id, recipient, body, sender, created_at).
pub type OutboxFanoutRow = (String, String, String, String, String, Option<String>, i64);

/// Column list for `SELECT … FROM agents` queries. Order must match
/// `row_to_agent`'s positional column reads.
const AGENT_COLS: &str = "id, name, state, task, model, provider, cwd, pid, session_id, exit_code, created_at, updated_at, worker_id, restart_policy, restart_count, workspace_id";

/// Column list for `SELECT … FROM wake_sources`. Matches `row_to_wake_source`.
const WAKE_SRC_COLS: &str =
    "id, agent_id, kind, config_json, state, fail_reason, last_fired_at, fire_count, created_at";

/// Column list for `SELECT … FROM peers`. Matches `row_to_peer`.
const PEER_COLS: &str = "id, daemon_id, name, url, bearer_token_hash, bearer_token, public_key, state, last_seen, registered_at";

/// Column list for `SELECT … FROM mail`. Matches `row_to_mail`.
const MAIL_COLS: &str = "id, recipient_id, sender_id, topic, body, in_reply_to, state, fail_reason, created_at, delivered_at, seq, wake_eligible";

pub struct Database {
    conn: Mutex<Connection>,
}

/// Best-effort `ALTER TABLE ADD COLUMN` for forward-only migrations. Returns
/// `Ok(())` whether or not the column already exists. `column_ddl` is the
/// full DDL fragment (e.g. `"keep_alive INTEGER NOT NULL DEFAULT 0"`).
/// All identifiers are crate literals — no injection surface.
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

        // Shadow workspaces have no on-disk worktree — the path column is
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

        Ok(())
    }

    fn exec(&self, sql: &str, params: impl rusqlite::Params) -> Result<usize> {
        Ok(self.conn.lock().execute(sql, params)?)
    }

    fn query_opt<T>(
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

    fn query_vec<T>(
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

    /// Append a stream event to the durable log. Returns the new row's id.
    /// Computes `seq` per (agent_id) when present, else per (scroll_id), else 0.
    pub fn append_event(&self, event: &StreamEvent) -> Result<i64> {
        let agent_id = event.agent_id();
        let scroll_id = event.scroll_id();
        let kind = event.kind();
        let payload = serde_json::to_string(event)?;
        let ts = Utc::now().to_rfc3339();

        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let seq: i64 = if let Some(aid) = agent_id {
            tx.query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0) FROM events WHERE agent_id = ?1",
                params![aid],
                |r| r.get(0),
            )?
        } else if let Some(sid) = scroll_id {
            tx.query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0) FROM events WHERE scroll_id = ?1",
                params![sid],
                |r| r.get(0),
            )?
        } else {
            0
        };

        tx.execute(
            "INSERT INTO events (agent_id, scroll_id, seq, kind, payload, ts) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![agent_id, scroll_id, seq, kind, payload, ts],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    pub fn insert_agent(&self, agent: &Agent) -> Result<()> {
        self.exec(
            "INSERT INTO agents (id, name, state, task, model, provider, cwd, pid, session_id, exit_code, created_at, updated_at, worker_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                agent.id,
                agent.name,
                agent.state.as_str(),
                agent.task,
                agent.model,
                agent.provider,
                agent.cwd.to_string_lossy().to_string(),
                agent.pid,
                agent.session_id,
                agent.exit_code,
                agent.created_at.to_rfc3339(),
                agent.updated_at.to_rfc3339(),
                agent.worker_id,
            ],
        )?;
        Ok(())
    }

    /// Update a single `agents` column plus `updated_at`. Caller supplies the
    /// column name; SQL uses a fixed template so no injection surface exists.
    fn update_agent_field(
        &self,
        id: &str,
        column: &str,
        value: &dyn rusqlite::ToSql,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            &format!("UPDATE agents SET {column} = ?1, updated_at = ?2 WHERE id = ?3"),
            params![value, now, id],
        )?;
        Ok(())
    }

    pub fn update_agent_state(
        &self,
        id: &str,
        state: &AgentState,
        exit_code: Option<i32>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            "UPDATE agents SET state = ?1, exit_code = ?2, updated_at = ?3 WHERE id = ?4",
            params![state.as_str(), exit_code, now, id],
        )?;
        Ok(())
    }

    pub fn update_agent_session_id(&self, id: &str, session_id: &str) -> Result<()> {
        self.update_agent_field(id, "session_id", &session_id)
    }

    pub fn update_agent_worker_id(&self, id: &str, worker_id: Option<&str>) -> Result<()> {
        self.update_agent_field(id, "worker_id", &worker_id)
    }

    pub fn update_agent_pid(&self, id: &str, pid: u32) -> Result<()> {
        self.update_agent_field(id, "pid", &pid)
    }

    /// Atomically add `tokens` to `agents.tokens_used` for `id`. Returns the
    /// new running total. A `0` increment is a no-op fast path.
    pub fn add_agent_tokens(&self, id: &str, tokens: u64) -> Result<u64> {
        if tokens == 0 {
            return self.get_agent_tokens(id);
        }
        let conn = self.conn.lock();
        conn.execute(
            "UPDATE agents SET tokens_used = tokens_used + ?1, updated_at = ?2 WHERE id = ?3",
            params![tokens as i64, chrono::Utc::now().to_rfc3339(), id],
        )?;
        let total: i64 = conn.query_row(
            "SELECT tokens_used FROM agents WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )?;
        Ok(total.max(0) as u64)
    }

    /// Set or clear the parent of an agent. Used by `agent.summon --parent`
    /// to wire the supervision tree at creation time.
    pub fn set_agent_parent(&self, id: &str, parent_id: Option<&str>) -> Result<()> {
        self.update_agent_field(id, "parent_agent_id", &parent_id)
    }

    /// Children of `parent_id` whose state is still in-flight (Queued,
    /// Summoning, Active, Dormant). Completed / Banished / Failed children
    /// are excluded — there's nothing to cascade onto.
    pub fn list_live_children(&self, parent_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT id FROM agents \
             WHERE parent_agent_id = ?1 \
               AND state IN ('Queued','Summoning','Active','Dormant')",
        )?;
        let ids = stmt
            .query_map(params![parent_id], |r| r.get::<_, String>(0))?
            .filter_map(Result::ok)
            .collect();
        Ok(ids)
    }

    pub fn get_agent_tokens(&self, id: &str) -> Result<u64> {
        let conn = self.conn.lock();
        let total: i64 = conn
            .query_row(
                "SELECT tokens_used FROM agents WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(total.max(0) as u64)
    }

    pub fn get_agent(&self, id: &str) -> Result<Option<Agent>> {
        self.query_opt(
            &format!("SELECT {AGENT_COLS} FROM agents WHERE id = ?1"),
            params![id],
            row_to_agent,
        )
    }

    pub fn list_agents(&self, state_filter: Option<&str>) -> Result<Vec<Agent>> {
        match state_filter {
            Some(state) => self.query_vec(
                &format!(
                    "SELECT {AGENT_COLS} FROM agents WHERE state = ?1 ORDER BY created_at DESC"
                ),
                params![state],
                row_to_agent,
            ),
            None => self.query_vec(
                &format!("SELECT {AGENT_COLS} FROM agents ORDER BY created_at DESC"),
                [],
                row_to_agent,
            ),
        }
    }

    pub fn insert_event(&self, event: &AgentEvent) -> Result<i64> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO agent_events (agent_id, event_type, payload, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                event.agent_id,
                event.event_type,
                event.payload,
                event.created_at.to_rfc3339(),
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get_events(&self, agent_id: &str, tail: Option<usize>) -> Result<Vec<AgentEvent>> {
        let conn = self.conn.lock();
        let mut events = Vec::new();
        let query = if let Some(limit) = tail {
            format!(
                "SELECT id, agent_id, event_type, payload, created_at
                 FROM agent_events WHERE agent_id = ?1
                 ORDER BY id DESC LIMIT {limit}"
            )
        } else {
            "SELECT id, agent_id, event_type, payload, created_at
             FROM agent_events WHERE agent_id = ?1
             ORDER BY id ASC"
                .to_string()
        };
        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query(params![agent_id])?;
        while let Some(row) = rows.next()? {
            events.push(AgentEvent {
                id: Some(row.get(0)?),
                agent_id: row.get(1)?,
                event_type: row.get(2)?,
                payload: row.get(3)?,
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)?
                    .with_timezone(&chrono::Utc),
            });
        }
        if tail.is_some() {
            events.reverse();
        }
        Ok(events)
    }

    /// Count agents grouped by state string (matches `AgentState::as_str`).
    /// Returned in no particular order; the metrics renderer fans missing
    /// states out to zero so the exposition is shape-stable across scrapes.
    pub fn count_agents_by_state(&self) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT state, COUNT(*) FROM agents GROUP BY state")?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push((row.get::<_, String>(0)?, row.get::<_, i64>(1)?));
        }
        Ok(out)
    }

    /// Total rows in the durable `events` stream log. Cheap (table has a
    /// rowid index) but not O(1); fine at metrics-scrape rates.
    pub fn count_events_total(&self) -> Result<i64> {
        let conn = self.conn.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))?;
        Ok(n)
    }

    /// Count durable events of a single `kind` (the per-variant tag from
    /// `StreamEvent::kind`). Backs the per-event-type counter metrics.
    pub fn count_events_by_kind(&self, kind: &str) -> Result<i64> {
        let conn = self.conn.lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM events WHERE kind = ?1",
            params![kind],
            |r| r.get(0),
        )?;
        Ok(n)
    }

    /// Count notification events grouped by their `level` payload field.
    /// Used to label the operator-facing notifications counter so warn/error
    /// rates show up distinctly in dashboards.
    pub fn count_notifications_by_level(&self) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT json_extract(payload, '$.level') AS lvl, COUNT(*) \
             FROM events WHERE kind = 'notification' GROUP BY lvl",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let level: String = row
                .get::<_, Option<String>>(0)?
                .unwrap_or_else(|| "unknown".to_string());
            out.push((level, row.get::<_, i64>(1)?));
        }
        Ok(out)
    }

    /// Read the full durable stream-event log for one agent, oldest first.
    /// This is the rich `events` table (every `StreamEvent` variant), not the
    /// legacy `agent_events` stdout/stderr stream that `get_events` serves.
    /// Rows whose payload fails to deserialize (a schema that predates a
    /// variant rename, say) are skipped rather than failing the whole read —
    /// a partial timeline beats no timeline.
    pub fn read_stream_events(&self, agent_id: &str) -> Result<Vec<StoredEvent>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT seq, kind, payload, ts FROM events \
             WHERE agent_id = ?1 ORDER BY seq ASC",
        )?;
        let mut rows = stmt.query(params![agent_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let seq: i64 = row.get(0)?;
            let kind: String = row.get(1)?;
            let payload: String = row.get(2)?;
            let ts: String = row.get(3)?;
            let Ok(event) = serde_json::from_str::<StreamEvent>(&payload) else {
                continue;
            };
            out.push(StoredEvent {
                seq,
                kind,
                ts,
                event,
            });
        }
        Ok(out)
    }

    #[allow(dead_code)]
    pub fn delete_agent(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM agent_events WHERE agent_id = ?1", params![id])?;
        conn.execute("DELETE FROM agents WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn insert_pact(&self, pact: &Pact) -> Result<()> {
        self.exec(
            "INSERT INTO pacts (id, source_id, task_tpl, name, state, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                pact.id,
                pact.source_id,
                pact.task_tpl,
                pact.name,
                pact.state.as_str(),
                pact.created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn list_pacts(&self, source_id: Option<&str>) -> Result<Vec<Pact>> {
        match source_id {
            Some(sid) => self.query_vec(
                "SELECT id, source_id, task_tpl, name, state, target_id, created_at, fired_at
                 FROM pacts WHERE source_id = ?1 ORDER BY created_at DESC",
                params![sid],
                row_to_pact,
            ),
            None => self.query_vec(
                "SELECT id, source_id, task_tpl, name, state, target_id, created_at, fired_at
                 FROM pacts ORDER BY created_at DESC",
                [],
                row_to_pact,
            ),
        }
    }

    pub fn get_pending_pacts_for_agent(&self, agent_id: &str) -> Result<Vec<Pact>> {
        self.query_vec(
            "SELECT id, source_id, task_tpl, name, state, target_id, created_at, fired_at
             FROM pacts WHERE source_id = ?1 AND state = 'pending'",
            params![agent_id],
            row_to_pact,
        )
    }

    pub fn update_pact_fired(&self, id: &str, target_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            "UPDATE pacts SET state = 'fired', target_id = ?1, fired_at = ?2 WHERE id = ?3",
            params![target_id, now, id],
        )?;
        Ok(())
    }

    pub fn update_pact_failed(&self, id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            "UPDATE pacts SET state = 'failed', fired_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        Ok(())
    }

    /// An agent's stdout lines in emission order. The raw material for a
    /// provider's `extract_result` (pact `{output}` injection) and the
    /// `ContextReplay` transcript. Provider-neutral — no format assumed here.
    pub fn get_agent_stdout_lines(&self, agent_id: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            "SELECT payload FROM agent_events
             WHERE agent_id = ?1 AND event_type = 'stdout'
             ORDER BY id ASC",
        )?;
        let mut rows = stmt.query(params![agent_id])?;
        let mut lines = Vec::new();
        while let Some(row) = rows.next()? {
            lines.push(row.get(0)?);
        }
        Ok(lines)
    }

    /// Reconstruct an agent's prior stdout as a single string, for the
    /// `ContextReplay` resume strategy (providers with no native session). Capped
    /// to the last `budget_bytes` — oldest output truncated with a note — mirroring
    /// the scheduler's mail-fold budgeting. Returns the empty string if the agent
    /// produced no output.
    pub fn get_agent_transcript(&self, agent_id: &str, budget_bytes: usize) -> Result<String> {
        let full = self.get_agent_stdout_lines(agent_id)?.join("\n");
        if full.len() <= budget_bytes {
            return Ok(full);
        }
        // Keep the tail; align the cut to a UTF-8 char boundary.
        let mut start = full.len() - budget_bytes;
        while start < full.len() && !full.is_char_boundary(start) {
            start += 1;
        }
        Ok(format!("[…earlier output truncated…]\n{}", &full[start..]))
    }

    pub fn insert_scroll(&self, scroll: &Scroll) -> Result<()> {
        self.exec(
            "INSERT INTO scrolls (id, name, state, source_path, max_concurrency, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                scroll.id,
                scroll.name,
                scroll.state.as_str(),
                scroll.source_path,
                scroll.max_concurrency,
                scroll.created_at.to_rfc3339(),
                scroll.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_scroll(&self, id: &str) -> Result<Option<Scroll>> {
        self.query_opt(
            "SELECT id, name, state, source_path, max_concurrency, created_at, updated_at
             FROM scrolls WHERE id = ?1",
            params![id],
            row_to_scroll,
        )
    }

    pub fn list_scrolls(&self) -> Result<Vec<Scroll>> {
        self.query_vec(
            "SELECT id, name, state, source_path, max_concurrency, created_at, updated_at
             FROM scrolls ORDER BY created_at DESC",
            [],
            row_to_scroll,
        )
    }

    pub fn update_scroll_state(&self, id: &str, state: &ScrollState) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            "UPDATE scrolls SET state = ?1, updated_at = ?2 WHERE id = ?3",
            params![state.as_str(), now, id],
        )?;
        Ok(())
    }

    pub fn insert_task(&self, task: &Task) -> Result<()> {
        let file_patterns_json = serde_json::to_string(&task.file_patterns)?;
        self.exec(
            "INSERT INTO tasks (id, scroll_id, name, prompt, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                task.id,
                task.scroll_id,
                task.name,
                task.prompt,
                task.state.as_str(),
                task.agent_id,
                task.provider,
                task.model,
                task.cwd,
                file_patterns_json,
                task.order_index,
                task.created_at.to_rfc3339(),
                task.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn insert_task_dependency(&self, task_id: &str, depends_on_id: &str) -> Result<()> {
        self.exec(
            "INSERT INTO task_dependencies (task_id, depends_on_id) VALUES (?1, ?2)",
            params![task_id, depends_on_id],
        )?;
        Ok(())
    }

    pub fn get_tasks_for_scroll(&self, scroll_id: &str) -> Result<Vec<Task>> {
        self.query_vec(
            "SELECT id, scroll_id, name, prompt, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at
             FROM tasks WHERE scroll_id = ?1 ORDER BY order_index ASC",
            params![scroll_id],
            row_to_task,
        )
    }

    pub fn get_task_by_agent_id(&self, agent_id: &str) -> Result<Option<Task>> {
        self.query_opt(
            "SELECT id, scroll_id, name, prompt, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at
             FROM tasks WHERE agent_id = ?1",
            params![agent_id],
            row_to_task,
        )
    }

    pub fn update_task_state(&self, id: &str, state: &TaskState) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            "UPDATE tasks SET state = ?1, updated_at = ?2 WHERE id = ?3",
            params![state.as_str(), now, id],
        )?;
        Ok(())
    }

    pub fn update_task_agent(&self, task_id: &str, agent_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            "UPDATE tasks SET agent_id = ?1, state = 'active', updated_at = ?2 WHERE id = ?3",
            params![agent_id, now, task_id],
        )?;
        Ok(())
    }

    pub fn get_task_dependencies(&self, task_id: &str) -> Result<Vec<TaskId>> {
        self.query_vec(
            "SELECT depends_on_id FROM task_dependencies WHERE task_id = ?1",
            params![task_id],
            |row| Ok(row.get(0)?),
        )
    }

    pub fn get_task_dependents(&self, task_id: &str) -> Result<Vec<TaskId>> {
        self.query_vec(
            "SELECT task_id FROM task_dependencies WHERE depends_on_id = ?1",
            params![task_id],
            |row| Ok(row.get(0)?),
        )
    }

    pub fn find_ready_tasks(&self, scroll_id: &str) -> Result<Vec<Task>> {
        self.query_vec(
            "SELECT r.id, r.scroll_id, r.name, r.prompt, r.state, r.agent_id, r.provider, r.model, r.cwd, r.file_patterns, r.order_index, r.created_at, r.updated_at
             FROM tasks r
             WHERE r.scroll_id = ?1 AND r.state = 'blocked'
             AND NOT EXISTS (
                 SELECT 1 FROM task_dependencies rd
                 JOIN tasks dep ON dep.id = rd.depends_on_id
                 WHERE rd.task_id = r.id AND dep.state != 'complete'
             )",
            params![scroll_id],
            row_to_task,
        )
    }

    /// Count active tasks in a scroll
    pub fn count_active_tasks(&self, scroll_id: &str) -> Result<usize> {
        let conn = self.conn.lock();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tasks WHERE scroll_id = ?1 AND state = 'active'",
            params![scroll_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    /// Insert a new row into `task_queue`. The corresponding `agents` row must
    /// already exist (foreign-key constraint).
    pub fn enqueue_task(&self, row: &QueueRow) -> Result<()> {
        self.exec(
            "INSERT INTO task_queue
                (id, lane, priority, enqueued_at, provider_name, cwd, model, task_text, block_reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                row.id,
                row.lane,
                row.priority,
                row.enqueued_at.to_rfc3339(),
                row.provider_name,
                row.cwd,
                row.model,
                row.task_text,
                row.block_reason,
            ],
        )?;
        Ok(())
    }

    /// List every queued row in dispatch order (ad-hoc lane first, then by
    /// priority DESC, then FIFO by `enqueued_at`, then by id).
    pub fn list_queue(&self) -> Result<Vec<QueueRow>> {
        self.query_vec(
            "SELECT id, lane, priority, enqueued_at, provider_name, cwd, model, task_text, block_reason
             FROM task_queue
             ORDER BY CASE lane WHEN 'adhoc' THEN 0 ELSE 1 END,
                      priority DESC, enqueued_at ASC, id ASC",
            [],
            row_to_queue_row,
        )
    }

    /// List queued rows restricted to a single lane, in dispatch order.
    pub fn list_queue_by_lane(&self, lane: &str) -> Result<Vec<QueueRow>> {
        self.query_vec(
            "SELECT id, lane, priority, enqueued_at, provider_name, cwd, model, task_text, block_reason
             FROM task_queue
             WHERE lane = ?1
             ORDER BY priority DESC, enqueued_at ASC, id ASC",
            params![lane],
            row_to_queue_row,
        )
    }

    /// Return the next row that should be dispatched, honoring lane order
    /// (ad-hoc first), then priority, then FIFO. Does not mutate state.
    pub fn peek_next_dispatch(&self) -> Result<Option<QueueRow>> {
        self.query_opt(
            "SELECT id, lane, priority, enqueued_at, provider_name, cwd, model, task_text, block_reason
             FROM task_queue
             ORDER BY CASE lane WHEN 'adhoc' THEN 0 ELSE 1 END,
                      priority DESC, enqueued_at ASC, id ASC
             LIMIT 1",
            [],
            row_to_queue_row,
        )
    }

    /// Atomically remove the queue row for `id` and flip the matching agent
    /// to `summoning`. Returns `true` if the queue row existed and was
    /// claimed; `false` if it was already gone (raced with another claim or
    /// a `banish`).
    pub fn claim_for_dispatch(&self, id: &AgentId) -> Result<bool> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let deleted = tx.execute("DELETE FROM task_queue WHERE id = ?1", params![id])?;
        if deleted == 0 {
            tx.rollback()?;
            return Ok(false);
        }
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE agents SET state = 'summoning', updated_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Re-insert a previously claimed row, preserving its original
    /// `enqueued_at` so fairness ordering is not lost. Sets the matching
    /// agent's state back to `queued`.
    pub fn requeue(&self, row: &QueueRow) -> Result<()> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO task_queue
                (id, lane, priority, enqueued_at, provider_name, cwd, model, task_text, block_reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                row.id,
                row.lane,
                row.priority,
                row.enqueued_at.to_rfc3339(),
                row.provider_name,
                row.cwd,
                row.model,
                row.task_text,
                row.block_reason,
            ],
        )?;
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE agents SET state = 'queued', updated_at = ?1 WHERE id = ?2",
            params![now, row.id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Remove the queue row for `id`, if it exists. Returns `true` when a
    /// row was actually deleted, `false` when it was already gone (idempotent).
    pub fn delete_from_queue(&self, id: &AgentId) -> Result<bool> {
        let conn = self.conn.lock();
        let n = conn.execute("DELETE FROM task_queue WHERE id = ?1", params![id])?;
        Ok(n > 0)
    }

    /// Update or clear the `block_reason` for a queued row.
    pub fn set_block_reason(&self, id: &AgentId, reason: Option<&str>) -> Result<()> {
        self.exec(
            "UPDATE task_queue SET block_reason = ?1 WHERE id = ?2",
            params![reason, id],
        )?;
        Ok(())
    }

    /// On daemon startup, mark every agent that was mid-flight (`Active` or
    /// `Summoning`) as `Failed` — their child processes are gone — and report
    /// what was changed plus how many `Queued` agents survived for the
    /// scheduler to pick up. `Complete`/`Failed`/`Banished` rows and `Queued`
    /// rows are left untouched.
    pub fn restart_recovery(&self) -> Result<RecoveryReport> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let failed: Vec<(AgentId, AgentState)> = {
            let mut stmt =
                tx.prepare("SELECT id, state FROM agents WHERE state IN ('active', 'summoning')")?;
            let rows = stmt.query_map([], |row| {
                let id: String = row.get(0)?;
                let state: String = row.get(1)?;
                Ok((id, state))
            })?;
            rows.filter_map(std::result::Result::ok)
                .map(|(id, s)| {
                    let parsed = s.parse().unwrap_or(AgentState::Failed);
                    (id, parsed)
                })
                .collect()
        };

        let now = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE agents SET state = 'failed', updated_at = ?1 \
             WHERE state IN ('active', 'summoning')",
            params![now],
        )?;

        let queued_remaining: i64 = tx.query_row(
            "SELECT COUNT(*) FROM agents WHERE state = 'queued'",
            [],
            |r| r.get(0),
        )?;

        tx.commit()?;
        Ok(RecoveryReport {
            failed,
            queued_remaining: queued_remaining as usize,
        })
    }

    pub fn insert_wake_source(&self, src: &WakeSource) -> Result<()> {
        self.exec(
            "INSERT INTO wake_sources \
                (id, agent_id, kind, config_json, state, fail_reason, last_fired_at, fire_count, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                src.id,
                src.agent_id,
                src.kind.as_str(),
                src.config_json,
                src.state.as_str(),
                src.fail_reason,
                src.last_fired_at,
                src.fire_count,
                src.created_at,
            ],
        )?;
        Ok(())
    }

    pub fn get_wake_source(&self, id: &str) -> Result<Option<WakeSource>> {
        self.query_opt(
            &format!("SELECT {WAKE_SRC_COLS} FROM wake_sources WHERE id = ?1"),
            params![id],
            row_to_wake_source,
        )
    }

    pub fn list_wake_sources_for_agent(&self, agent_id: &str) -> Result<Vec<WakeSource>> {
        self.query_vec(
            &format!("SELECT {WAKE_SRC_COLS} FROM wake_sources WHERE agent_id = ?1 ORDER BY created_at DESC, id ASC"),
            params![agent_id],
            row_to_wake_source,
        )
    }

    pub fn list_all_wake_sources(&self) -> Result<Vec<WakeSource>> {
        self.query_vec(
            &format!("SELECT {WAKE_SRC_COLS} FROM wake_sources ORDER BY created_at DESC, agent_id ASC, id ASC"),
            [],
            row_to_wake_source,
        )
    }

    pub fn list_armed_wake_sources(&self) -> Result<Vec<WakeSource>> {
        self.query_vec(
            &format!("SELECT {WAKE_SRC_COLS} FROM wake_sources WHERE state = 'armed' ORDER BY created_at ASC"),
            [],
            row_to_wake_source,
        )
    }

    pub fn delete_wake_source(&self, id: &str) -> Result<bool> {
        Ok(self.exec("DELETE FROM wake_sources WHERE id = ?1", params![id])? > 0)
    }

    pub fn delete_wake_sources_for_agent(&self, agent_id: &str) -> Result<usize> {
        self.exec(
            "DELETE FROM wake_sources WHERE agent_id = ?1",
            params![agent_id],
        )
    }

    pub fn update_wake_source_state(
        &self,
        id: &str,
        state: WakeSourceState,
        fail_reason: Option<&str>,
    ) -> Result<()> {
        self.exec(
            "UPDATE wake_sources SET state = ?1, fail_reason = ?2 WHERE id = ?3",
            params![state.as_str(), fail_reason, id],
        )?;
        Ok(())
    }

    pub fn bump_wake_source_fire(&self, id: &str, last_fired_at: i64) -> Result<()> {
        self.exec(
            "UPDATE wake_sources \
             SET fire_count = fire_count + 1, last_fired_at = ?1 \
             WHERE id = ?2",
            params![last_fired_at, id],
        )?;
        Ok(())
    }

    /// Per-agent token-bucket row used by the rate limiter. Returns
    /// `(tokens, last_refill_at, capacity, refill_per_sec)`. If the row
    /// doesn't exist yet, it is created at full capacity.
    pub fn get_or_init_rate_limit(&self, agent_id: &str, now: i64) -> Result<(f64, i64, i64, f64)> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row: Option<(f64, i64, i64, f64)> = tx
            .query_row(
                "SELECT tokens, last_refill_at, capacity, refill_per_sec \
                 FROM wake_rate_limits WHERE agent_id = ?1",
                params![agent_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .ok();
        let result = if let Some(r) = row {
            r
        } else {
            // Defaults: 60 tokens, 60-per-hour refill.
            let capacity: i64 = 60;
            let refill: f64 = 60.0 / 3600.0;
            tx.execute(
                "INSERT INTO wake_rate_limits (agent_id, tokens, last_refill_at, capacity, refill_per_sec) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![agent_id, capacity as f64, now, capacity, refill],
            )?;
            (capacity as f64, now, capacity, refill)
        };
        tx.commit()?;
        Ok(result)
    }

    pub fn update_rate_limit_tokens(
        &self,
        agent_id: &str,
        tokens: f64,
        last_refill_at: i64,
    ) -> Result<()> {
        self.exec(
            "UPDATE wake_rate_limits SET tokens = ?1, last_refill_at = ?2 WHERE agent_id = ?3",
            params![tokens, last_refill_at, agent_id],
        )?;
        Ok(())
    }

    pub fn set_rate_limit_capacity(
        &self,
        agent_id: &str,
        capacity: i64,
        refill_per_sec: f64,
        now: i64,
    ) -> Result<()> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let exists: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM wake_rate_limits WHERE agent_id = ?1",
                params![agent_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if exists == 0 {
            tx.execute(
                "INSERT INTO wake_rate_limits (agent_id, tokens, last_refill_at, capacity, refill_per_sec) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![agent_id, capacity as f64, now, capacity, refill_per_sec],
            )?;
        } else {
            tx.execute(
                "UPDATE wake_rate_limits SET capacity = ?1, refill_per_sec = ?2 WHERE agent_id = ?3",
                params![capacity, refill_per_sec, agent_id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_keep_alive(&self, agent_id: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let v: i64 = conn.query_row(
            "SELECT keep_alive FROM agents WHERE id = ?1",
            params![agent_id],
            |r| r.get(0),
        )?;
        Ok(v != 0)
    }

    pub fn set_keep_alive(&self, agent_id: &str, keep_alive: bool) -> Result<()> {
        self.exec(
            "UPDATE agents SET keep_alive = ?1 WHERE id = ?2",
            params![i64::from(keep_alive), agent_id],
        )?;
        Ok(())
    }

    /// Promote `Complete` agents that still have a `session_id` to `Dormant`.
    /// Idempotent: replays are no-ops because the WHERE clause filters
    /// already-Dormant rows. Returns the IDs that flipped, so the caller can
    /// emit `StateChange { Complete -> Dormant }` events for each.
    pub fn migrate_dormant_agents(&self) -> Result<Vec<AgentId>> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        let ids: Vec<AgentId> = {
            let mut stmt = tx.prepare(
                "SELECT id FROM agents \
                 WHERE state = 'complete' AND session_id IS NOT NULL",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.filter_map(std::result::Result::ok).collect()
        };

        if !ids.is_empty() {
            let now = Utc::now().to_rfc3339();
            tx.execute(
                "UPDATE agents SET state = 'dormant', updated_at = ?1 \
                 WHERE state = 'complete' AND session_id IS NOT NULL",
                params![now],
            )?;
        }

        tx.commit()?;
        Ok(ids)
    }

    /// Number of rows currently in `task_queue`.
    pub fn count_queued(&self) -> Result<usize> {
        let conn = self.conn.lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM task_queue", [], |r| r.get(0))?;
        Ok(n as usize)
    }

    /// Number of agents currently mid-flight (Active or Summoning) — the
    /// scheduler's `in_flight` count for capacity decisions.
    pub fn count_in_flight_agents(&self) -> Result<usize> {
        let conn = self.conn.lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM agents WHERE state IN ('active', 'summoning')",
            [],
            |r| r.get(0),
        )?;
        Ok(n as usize)
    }

    /// Insert a mail row, computing `seq` per `recipient_id` inside an
    /// IMMEDIATE transaction so concurrent inserts to the same recipient
    /// serialize. Returns `Err` if `recipient_id` is empty.
    pub fn insert_mail(&self, mail: &Mail) -> Result<()> {
        if mail.recipient_id.is_empty() {
            anyhow::bail!("recipient_id must not be empty");
        }
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?1",
            params![mail.recipient_id],
            |r| r.get(0),
        )?;
        tx.execute(
            &format!("INSERT INTO mail ({MAIL_COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"),
            params![
                mail.id,
                mail.recipient_id,
                mail.sender_id,
                mail.topic,
                mail.body,
                mail.in_reply_to,
                mail.state.as_str(),
                mail.fail_reason,
                mail.created_at,
                mail.delivered_at,
                seq,
                i64::from(mail.wake_eligible),
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Insert a mail row but use a caller-provided `seq` value. Used by topic
    /// fanout when inserting multiple rows in a single transaction.
    fn insert_mail_with_seq_in_tx(
        tx: &rusqlite::Transaction<'_>,
        mail: &Mail,
        seq: i64,
    ) -> Result<()> {
        tx.execute(
            &format!("INSERT INTO mail ({MAIL_COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"),
            params![
                mail.id,
                mail.recipient_id,
                mail.sender_id,
                mail.topic,
                mail.body,
                mail.in_reply_to,
                mail.state.as_str(),
                mail.fail_reason,
                mail.created_at,
                mail.delivered_at,
                seq,
                i64::from(mail.wake_eligible),
            ],
        )?;
        Ok(())
    }

    /// Insert multiple mail rows + per-peer `peer_outbox` fanout rows in a
    /// single IMMEDIATE transaction. Each `mail.seq` is computed per
    /// recipient; each outbox `sender_seq` is computed per `peer_id`.
    /// Returns the list of `(peer_id, outbox_id)` pairs inserted so callers
    /// can emit per-row events.
    pub fn insert_mail_batch_with_outbox(
        &self,
        mails: &[Mail],
        outbox_fanout: &[OutboxFanoutRow],
    ) -> Result<()> {
        if mails.is_empty() && outbox_fanout.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for m in mails {
            if m.recipient_id.is_empty() {
                anyhow::bail!("recipient_id must not be empty");
            }
            let seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?1",
                params![m.recipient_id],
                |r| r.get(0),
            )?;
            Self::insert_mail_with_seq_in_tx(&tx, m, seq)?;
        }
        for (peer_id, outbox_id, mail_id, recipient, body, sender, created_at) in outbox_fanout {
            let sender_seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(sender_seq) + 1, 1) FROM peer_outbox WHERE peer_id = ?1",
                params![peer_id],
                |r| r.get(0),
            )?;
            // For topic fanout, the recipient string here carries the
            // remote topic address (`topic://<name>`); receivers fan out
            // to local subscribers per `topic_federations` direction.
            tx.execute(
                "INSERT INTO peer_outbox (id, peer_id, mail_id, sender_seq, recipient, sender, topic, body, created_at, attempts, next_attempt_at, state)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?9, 'pending')",
                params![
                    outbox_id,
                    peer_id,
                    mail_id,
                    sender_seq,
                    recipient,
                    sender,
                    Some(recipient.strip_prefix("topic://").unwrap_or("")),
                    body,
                    created_at,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Insert multiple mail rows for distinct recipients in a single
    /// IMMEDIATE transaction. Each row's `seq` is computed per recipient.
    /// Used for topic fanout so a partial fanout cannot be observed.
    pub fn insert_mail_batch(&self, mails: &[Mail]) -> Result<()> {
        if mails.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        for m in mails {
            if m.recipient_id.is_empty() {
                anyhow::bail!("recipient_id must not be empty");
            }
            let seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?1",
                params![m.recipient_id],
                |r| r.get(0),
            )?;
            Self::insert_mail_with_seq_in_tx(&tx, m, seq)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_mail_by_recipient(
        &self,
        recipient_id: &str,
        after_seq: Option<i64>,
        state_filter: Option<MailState>,
        limit: u32,
    ) -> Result<Vec<Mail>> {
        use std::fmt::Write;
        let limit = i64::from(limit.clamp(1, 1000));
        let mut sql = format!("SELECT {MAIL_COLS} FROM mail WHERE recipient_id = ?1");
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(recipient_id.to_string())];
        if let Some(s) = after_seq {
            write!(sql, " AND seq > ?{}", args.len() + 1).unwrap();
            args.push(Box::new(s));
        }
        if let Some(st) = state_filter {
            write!(sql, " AND state = ?{}", args.len() + 1).unwrap();
            args.push(Box::new(st.as_str().to_string()));
        }
        write!(sql, " ORDER BY seq ASC LIMIT {limit}").unwrap();

        let conn = self.conn.lock();
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(args.iter().map(|b| &**b)))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row_to_mail(row)?);
        }
        Ok(out)
    }

    pub fn get_mail(&self, id: &str) -> Result<Option<Mail>> {
        self.query_opt(
            &format!("SELECT {MAIL_COLS} FROM mail WHERE id = ?1"),
            params![id],
            row_to_mail,
        )
    }

    /// Find a mail row by short id prefix. Returns `Err` if multiple match,
    /// `Ok(None)` if none match.
    pub fn get_mail_by_prefix(&self, prefix: &str) -> Result<Option<Mail>> {
        let conn = self.conn.lock();
        let sql = format!("SELECT {MAIL_COLS} FROM mail WHERE id LIKE ?1 || '%' LIMIT 2");
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(params![prefix])?;
        let first = match rows.next()? {
            Some(r) => row_to_mail(r)?,
            None => return Ok(None),
        };
        if rows.next()?.is_some() {
            anyhow::bail!("Ambiguous mail prefix '{prefix}'");
        }
        Ok(Some(first))
    }

    pub fn set_mail_state(
        &self,
        id: &str,
        new_state: MailState,
        fail_reason: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock();
        let now = unix_now();
        let delivered_at: Option<i64> = match new_state {
            MailState::Delivered | MailState::Failed => Some(now),
            MailState::Pending => None,
        };
        let n = conn.execute(
            "UPDATE mail SET state = ?1, fail_reason = COALESCE(?2, fail_reason), delivered_at = COALESCE(?3, delivered_at) WHERE id = ?4",
            params![new_state.as_str(), fail_reason, delivered_at, id],
        )?;
        if n == 0 {
            anyhow::bail!("mail not found: {id}");
        }
        Ok(())
    }

    pub fn list_pending_wake_eligible(&self, recipient_id: &str) -> Result<Vec<Mail>> {
        self.query_vec(
            &format!("SELECT {MAIL_COLS} FROM mail WHERE recipient_id = ?1 AND state = 'Pending' AND wake_eligible = 1 ORDER BY seq ASC"),
            params![recipient_id],
            row_to_mail,
        )
    }

    /// Returns distinct recipient ids that have at least one Pending,
    /// wake-eligible mail row. Used by the scheduler's mail-wake branch.
    pub fn list_recipients_with_pending_wake_eligible_mail(&self) -> Result<Vec<AgentId>> {
        self.query_vec(
            "SELECT recipient_id, MIN(seq) FROM mail \
             WHERE state = 'Pending' AND wake_eligible = 1 \
             GROUP BY recipient_id ORDER BY MIN(seq) ASC",
            [],
            |row| Ok(row.get(0)?),
        )
    }

    /// Insert a subscription. On UNIQUE conflict (subscriber_id, topic),
    /// returns the existing subscription's id. Sets `sub.id` to whatever id
    /// ends up in the DB. Returns the (possibly existing) id.
    pub fn insert_subscription(&self, sub: &Subscription) -> Result<String> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT id FROM subscriptions WHERE subscriber_id = ?1 AND topic = ?2",
                params![sub.subscriber_id, sub.topic],
                |r| r.get(0),
            )
            .ok();
        if let Some(id) = existing {
            tx.commit()?;
            return Ok(id);
        }
        tx.execute(
            "INSERT INTO subscriptions (id, subscriber_id, topic, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![sub.id, sub.subscriber_id, sub.topic, sub.created_at],
        )?;
        tx.commit()?;
        Ok(sub.id.clone())
    }

    pub fn delete_subscription(&self, id: &str) -> Result<bool> {
        Ok(self.exec("DELETE FROM subscriptions WHERE id = ?1", params![id])? > 0)
    }

    pub fn list_subscribers_for_topic(&self, topic: &str) -> Result<Vec<Subscription>> {
        self.query_vec(
            "SELECT id, subscriber_id, topic, created_at FROM subscriptions WHERE topic = ?1 ORDER BY created_at ASC, id ASC",
            params![topic],
            row_to_subscription,
        )
    }

    pub fn list_subscriptions_by_subscriber(&self, agent_id: &str) -> Result<Vec<Subscription>> {
        self.query_vec(
            "SELECT id, subscriber_id, topic, created_at FROM subscriptions WHERE subscriber_id = ?1 ORDER BY topic ASC",
            params![agent_id],
            row_to_subscription,
        )
    }

    pub fn list_topics_with_counts(&self) -> Result<Vec<(String, u32)>> {
        self.query_vec(
            "SELECT topic, COUNT(*) FROM subscriptions GROUP BY topic ORDER BY topic ASC",
            [],
            |row| Ok((row.get(0)?, row.get::<_, i64>(1)? as u32)),
        )
    }

    pub fn set_supervision(&self, agent_id: &str, cfg: &SupervisionConfig) -> Result<()> {
        self.exec(
            "UPDATE agents SET restart_policy = ?1, max_restarts = ?2, \
             restart_window_secs = ?3, escalate_to = ?4 WHERE id = ?5",
            params![
                cfg.policy.as_str(),
                cfg.max_restarts,
                cfg.window_secs,
                cfg.escalate_to,
                agent_id,
            ],
        )?;
        Ok(())
    }

    pub fn get_supervision(&self, agent_id: &str) -> Result<Option<SupervisionConfig>> {
        type SupervisionRow = (String, Option<u32>, Option<u32>, Option<String>);
        let conn = self.conn.lock();
        let row: Option<SupervisionRow> = conn
            .query_row(
                "SELECT restart_policy, max_restarts, restart_window_secs, escalate_to \
                 FROM agents WHERE id = ?1",
                params![agent_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .ok();
        Ok(row.map(|(policy, max, window, esc)| SupervisionConfig {
            policy: policy.parse().unwrap_or(RestartPolicy::Never),
            max_restarts: max,
            window_secs: window,
            escalate_to: esc,
        }))
    }

    pub fn clear_supervision(&self, agent_id: &str) -> Result<()> {
        self.exec(
            "UPDATE agents SET restart_policy = 'never', max_restarts = NULL, \
             restart_window_secs = NULL, escalate_to = NULL WHERE id = ?1",
            params![agent_id],
        )?;
        Ok(())
    }

    pub fn bump_restart_count(&self, agent_id: &str) -> Result<()> {
        self.exec(
            "UPDATE agents SET restart_count = restart_count + 1 WHERE id = ?1",
            params![agent_id],
        )?;
        Ok(())
    }

    pub fn get_escalation_depth(&self, agent_id: &str) -> Result<u32> {
        let conn = self.conn.lock();
        let v: i64 = conn
            .query_row(
                "SELECT escalation_depth FROM agents WHERE id = ?1",
                params![agent_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(v.max(0) as u32)
    }

    pub fn set_escalation_depth(&self, agent_id: &str, depth: u32) -> Result<()> {
        self.exec(
            "UPDATE agents SET escalation_depth = ?1 WHERE id = ?2",
            params![i64::from(depth), agent_id],
        )?;
        Ok(())
    }

    pub fn insert_restart_history_row(
        &self,
        agent_id: &str,
        attempted_at: i64,
        outcome: RestartHistoryOutcome,
        error_summary: Option<&str>,
    ) -> Result<i64> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO restart_history (agent_id, attempted_at, outcome, error_summary) \
             VALUES (?1, ?2, ?3, ?4)",
            params![agent_id, attempted_at, outcome.as_str(), error_summary],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn count_restarts_in_window(&self, agent_id: &str, window_start: i64) -> Result<u32> {
        let conn = self.conn.lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM restart_history \
             WHERE agent_id = ?1 AND attempted_at >= ?2 \
             AND outcome IN ('scheduled','failed_again')",
            params![agent_id, window_start],
            |r| r.get(0),
        )?;
        Ok(n.max(0) as u32)
    }

    /// Update the most recent `restart_history` row for `agent_id` whose
    /// `outcome = 'scheduled'`. Returns the number of rows updated.
    pub fn update_latest_scheduled_outcome(
        &self,
        agent_id: &str,
        new_outcome: RestartHistoryOutcome,
    ) -> Result<usize> {
        self.exec(
            "UPDATE restart_history SET outcome = ?1 \
             WHERE id = (SELECT id FROM restart_history \
                         WHERE agent_id = ?2 AND outcome = 'scheduled' \
                         ORDER BY attempted_at DESC, id DESC LIMIT 1)",
            params![new_outcome.as_str(), agent_id],
        )
    }

    /// Time of the most recent `restart_history` row for `agent_id`, or `None`.
    pub fn latest_restart_history_attempted_at(&self, agent_id: &str) -> Result<Option<i64>> {
        let conn = self.conn.lock();
        let v: Option<i64> = conn
            .query_row(
                "SELECT MAX(attempted_at) FROM restart_history WHERE agent_id = ?1",
                params![agent_id],
                |r| r.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten();
        Ok(v)
    }

    pub fn list_failed_with_active_policy(&self) -> Result<Vec<AgentId>> {
        self.query_vec(
            "SELECT id FROM agents \
             WHERE state = 'failed' AND restart_policy != 'never'",
            [],
            |row| Ok(row.get(0)?),
        )
    }

    pub fn mark_torn_restarting_as_failed(&self) -> Result<Vec<AgentId>> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let ids: Vec<AgentId> = {
            let mut stmt = tx.prepare("SELECT id FROM agents WHERE state = 'restarting'")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            rows.filter_map(std::result::Result::ok).collect()
        };
        if !ids.is_empty() {
            let now = Utc::now().to_rfc3339();
            tx.execute(
                "UPDATE agents SET state = 'failed', updated_at = ?1 WHERE state = 'restarting'",
                params![now],
            )?;
        }
        tx.commit()?;
        Ok(ids)
    }

    /// Return `true` if there is an `Escalated` event for `agent_id` whose
    /// row id is later than the latest `restart_history` row for the agent.
    /// Used by boot replay to skip re-escalation.
    pub fn has_escalated_event_after_latest_history(&self, agent_id: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let latest_history_ts: Option<String> = conn
            .query_row(
                "SELECT MAX(attempted_at) FROM restart_history WHERE agent_id = ?1",
                params![agent_id],
                |r| r.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten()
            .map(|t| {
                chrono::DateTime::<Utc>::from_timestamp(t, 0)
                    .unwrap_or_else(Utc::now)
                    .to_rfc3339()
            });
        let n: i64 = match latest_history_ts {
            Some(ts) => conn
                .query_row(
                    "SELECT COUNT(*) FROM events \
                     WHERE agent_id = ?1 AND kind = 'escalated' AND ts > ?2",
                    params![agent_id, ts],
                    |r| r.get(0),
                )
                .unwrap_or(0),
            None => conn
                .query_row(
                    "SELECT COUNT(*) FROM events \
                     WHERE agent_id = ?1 AND kind = 'escalated'",
                    params![agent_id],
                    |r| r.get(0),
                )
                .unwrap_or(0),
        };
        Ok(n > 0)
    }

    /// Get all dependency edges for a scroll (for cycle detection)
    pub fn get_all_dependencies_for_scroll(
        &self,
        scroll_id: &str,
    ) -> Result<Vec<(TaskId, TaskId)>> {
        self.query_vec(
            "SELECT rd.task_id, rd.depends_on_id
             FROM task_dependencies rd
             JOIN tasks r ON r.id = rd.task_id
             WHERE r.scroll_id = ?1",
            params![scroll_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
    }

    pub fn insert_peer(&self, peer: &crate::shared::types::Peer) -> Result<()> {
        self.exec(
            "INSERT INTO peers (id, daemon_id, name, url, bearer_token_hash, bearer_token, public_key, state, last_seen, registered_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                peer.id,
                peer.daemon_id,
                peer.name,
                peer.url,
                peer.bearer_token_hash,
                peer.bearer_token,
                peer.public_key,
                peer.state.as_str(),
                peer.last_seen,
                peer.registered_at,
            ],
        )?;
        Ok(())
    }

    pub fn delete_peer(&self, peer_id: &str) -> Result<bool> {
        Ok(self.exec("DELETE FROM peers WHERE id = ?1", params![peer_id])? > 0)
    }

    pub fn get_peer_by_name(&self, name: &str) -> Result<Option<crate::shared::types::Peer>> {
        self.query_opt(
            &format!("SELECT {PEER_COLS} FROM peers WHERE name = ?1"),
            params![name],
            row_to_peer,
        )
    }

    pub fn get_peer_by_daemon_id(
        &self,
        daemon_id: &str,
    ) -> Result<Option<crate::shared::types::Peer>> {
        self.query_opt(
            &format!("SELECT {PEER_COLS} FROM peers WHERE daemon_id = ?1"),
            params![daemon_id],
            row_to_peer,
        )
    }

    pub fn get_peer(&self, peer_id: &str) -> Result<Option<crate::shared::types::Peer>> {
        self.query_opt(
            &format!("SELECT {PEER_COLS} FROM peers WHERE id = ?1"),
            params![peer_id],
            row_to_peer,
        )
    }

    pub fn lookup_peer_by_token_hash(
        &self,
        hash: &[u8],
    ) -> Result<Option<crate::shared::types::Peer>> {
        self.query_opt(
            &format!("SELECT {PEER_COLS} FROM peers WHERE bearer_token_hash = ?1"),
            params![hash],
            row_to_peer,
        )
    }

    pub fn list_peers(&self) -> Result<Vec<crate::shared::types::Peer>> {
        self.query_vec(
            &format!("SELECT {PEER_COLS} FROM peers ORDER BY registered_at"),
            [],
            row_to_peer,
        )
    }

    pub fn set_peer_state(
        &self,
        peer_id: &str,
        state: crate::shared::types::PeerState,
    ) -> Result<()> {
        self.exec(
            "UPDATE peers SET state = ?1 WHERE id = ?2",
            params![state.as_str(), peer_id],
        )?;
        Ok(())
    }

    pub fn set_peer_last_seen(&self, peer_id: &str, ts: i64) -> Result<()> {
        self.exec(
            "UPDATE peers SET last_seen = ?1 WHERE id = ?2",
            params![ts, peer_id],
        )?;
        Ok(())
    }

    pub fn update_peer_daemon_id(&self, peer_id: &str, daemon_id: &str) -> Result<()> {
        self.exec(
            "UPDATE peers SET daemon_id = ?1 WHERE id = ?2",
            params![daemon_id, peer_id],
        )?;
        Ok(())
    }

    pub fn outbox_depth(&self, peer_id: &str) -> Result<u64> {
        let conn = self.conn.lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM peer_outbox WHERE peer_id = ?1 AND state IN ('pending','in_flight')",
            params![peer_id],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    /// Atomic: insert a `mail` row + `peer_outbox` row in a single
    /// IMMEDIATE transaction. `mail.seq` is computed per recipient as
    /// usual; `peer_outbox.sender_seq` is computed per `peer_id`.
    pub fn insert_mail_with_outbox(
        &self,
        mail: &Mail,
        peer_id: &str,
        outbox_id: &str,
        recipient: &str,
        topic: Option<&str>,
        next_attempt_at: i64,
    ) -> Result<u64> {
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mail_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(seq) + 1, 0) FROM mail WHERE recipient_id = ?1",
            params![mail.recipient_id],
            |r| r.get(0),
        )?;
        Self::insert_mail_with_seq_in_tx(&tx, mail, mail_seq)?;
        let sender_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(sender_seq) + 1, 1) FROM peer_outbox WHERE peer_id = ?1",
            params![peer_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO peer_outbox (id, peer_id, mail_id, sender_seq, recipient, sender, topic, body, created_at, attempts, next_attempt_at, state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, 'pending')",
            params![
                outbox_id,
                peer_id,
                mail.id,
                sender_seq,
                recipient,
                mail.sender_id,
                topic,
                mail.body,
                mail.created_at,
                next_attempt_at,
            ],
        )?;
        tx.commit()?;
        Ok(sender_seq as u64)
    }

    /// Pop the next `Pending` outbox row whose `next_attempt_at <= now`.
    pub fn next_outbox_row(
        &self,
        peer_id: &str,
        now: i64,
    ) -> Result<Option<crate::shared::types::PeerOutboxRow>> {
        self.query_opt(
            "SELECT id, peer_id, mail_id, sender_seq, recipient, sender, topic, body, created_at, attempts, next_attempt_at, state \
             FROM peer_outbox WHERE peer_id = ?1 AND state = 'pending' AND next_attempt_at <= ?2 \
             ORDER BY sender_seq ASC LIMIT 1",
            params![peer_id, now],
            row_to_outbox,
        )
    }

    pub fn mark_outbox_in_flight(&self, id: &str) -> Result<()> {
        self.exec(
            "UPDATE peer_outbox SET state = 'in_flight' WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn mark_outbox_delivered(&self, id: &str) -> Result<()> {
        self.exec(
            "UPDATE peer_outbox SET state = 'delivered', attempts = attempts + 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn mark_outbox_failed_retry(&self, id: &str, next_attempt_at: i64) -> Result<()> {
        self.exec(
            "UPDATE peer_outbox SET state = 'pending', attempts = attempts + 1, next_attempt_at = ?2 WHERE id = ?1",
            params![id, next_attempt_at],
        )?;
        Ok(())
    }

    /// On boot, flip any `in_flight` outbox rows back to `pending` so the
    /// drainer re-sends them. Idempotency on the receiver dedupes any
    /// already-delivered messages.
    pub fn reset_outbox_in_flight(&self) -> Result<u32> {
        let conn = self.conn.lock();
        let n = conn.execute(
            "UPDATE peer_outbox SET state = 'pending' WHERE state = 'in_flight'",
            [],
        )?;
        Ok(n as u32)
    }

    /// Idempotency-keyed inbox insert. Returns `true` if this is a new
    /// delivery (insertion happened); `false` if the (daemon, seq) pair
    /// already existed (replay).
    pub fn insert_peer_inbox_if_absent(
        &self,
        sender_daemon_id: &str,
        sender_seq: u64,
        mail_id: &str,
        received_at: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock();
        let n = conn.execute(
            "INSERT OR IGNORE INTO peer_inbox (sender_daemon_id, sender_seq, mail_id, received_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![sender_daemon_id, sender_seq as i64, mail_id, received_at],
        )?;
        Ok(n > 0)
    }

    pub fn insert_topic_federation(
        &self,
        id: &str,
        peer_id: &str,
        topic: &str,
        direction: crate::shared::types::FederationDirection,
        created_at: i64,
    ) -> Result<()> {
        self.exec(
            "INSERT INTO topic_federations (id, peer_id, topic, direction, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, peer_id, topic, direction.as_str(), created_at],
        )?;
        Ok(())
    }

    pub fn upsert_topic_federation(
        &self,
        id: &str,
        peer_id: &str,
        topic: &str,
        direction: crate::shared::types::FederationDirection,
        created_at: i64,
    ) -> Result<crate::shared::types::FederationDirection> {
        use crate::shared::types::FederationDirection;
        let mut conn = self.conn.lock();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT direction FROM topic_federations WHERE peer_id = ?1 AND topic = ?2",
                params![peer_id, topic],
                |r| r.get::<_, String>(0),
            )
            .ok();
        let final_dir = if let Some(s) = existing {
            let cur: FederationDirection = s.parse().unwrap_or(FederationDirection::Both);
            cur.merge(direction)
        } else {
            direction
        };
        tx.execute(
            "INSERT INTO topic_federations (id, peer_id, topic, direction, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(peer_id, topic) DO UPDATE SET direction = excluded.direction",
            params![id, peer_id, topic, final_dir.as_str(), created_at],
        )?;
        tx.commit()?;
        Ok(final_dir)
    }

    pub fn delete_topic_federation(&self, peer_id: &str, topic: &str) -> Result<bool> {
        Ok(self.exec(
            "DELETE FROM topic_federations WHERE peer_id = ?1 AND topic = ?2",
            params![peer_id, topic],
        )? > 0)
    }

    pub fn list_outbound_federations_for_topic(
        &self,
        topic: &str,
    ) -> Result<Vec<crate::shared::types::TopicFederation>> {
        self.query_vec(
            "SELECT id, peer_id, topic, direction, created_at FROM topic_federations WHERE topic = ?1 AND direction IN ('outbound','both')",
            params![topic],
            row_to_topic_federation,
        )
    }

    pub fn topic_federation_inbound_authorized(&self, peer_id: &str, topic: &str) -> Result<bool> {
        let conn = self.conn.lock();
        let dir: Option<String> = conn
            .query_row(
                "SELECT direction FROM topic_federations WHERE peer_id = ?1 AND topic = ?2",
                params![peer_id, topic],
                |r| r.get::<_, String>(0),
            )
            .ok();
        Ok(matches!(dir.as_deref(), Some("inbound" | "both")))
    }
}

fn row_to_subscription(row: &rusqlite::Row) -> Result<Subscription> {
    Ok(Subscription {
        id: row.get(0)?,
        subscriber_id: row.get(1)?,
        topic: row.get(2)?,
        created_at: row.get(3)?,
    })
}

fn row_to_peer(row: &rusqlite::Row) -> Result<crate::shared::types::Peer> {
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

fn row_to_outbox(row: &rusqlite::Row) -> Result<crate::shared::types::PeerOutboxRow> {
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

fn row_to_topic_federation(row: &rusqlite::Row) -> Result<crate::shared::types::TopicFederation> {
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

/// Current unix epoch seconds. Used for `mail.created_at` / `mail.delivered_at`
/// and for `subscriptions.created_at`.
pub fn unix_now() -> i64 {
    Utc::now().timestamp()
}

fn row_to_mail(row: &rusqlite::Row) -> Result<Mail> {
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

/// Parse an RFC3339 timestamp from a DB column, returning a proper error instead of panicking.
fn parse_timestamp(s: &str) -> Result<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| anyhow::anyhow!("invalid timestamp '{s}': {e}"))
}

fn row_to_scroll(row: &rusqlite::Row) -> Result<Scroll> {
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

fn row_to_task(row: &rusqlite::Row) -> Result<Task> {
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

fn row_to_pact(row: &rusqlite::Row) -> Result<Pact> {
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

fn row_to_queue_row(row: &rusqlite::Row) -> Result<QueueRow> {
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

fn row_to_wake_source(row: &rusqlite::Row) -> Result<WakeSource> {
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

fn row_to_agent(row: &rusqlite::Row) -> Result<Agent> {
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
