use anyhow::Result;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::path::Path;
use std::sync::Arc;

use crate::shared::protocol::StreamEvent;
use crate::shared::types::AgentState;

pub mod agent_lifecycle;
mod agents;
mod artifacts;
mod mail;
mod pacts;
mod peers;
mod rows;
mod schema;
pub mod scroll_dispatch;
mod scrolls;
mod supervision;
#[cfg(test)]
mod tests;
mod wake;

// Re-export the row-mapping helpers so sibling submodules can keep reaching
// them via `super::row_to_*` / `super::parse_timestamp`.
use rows::{
    row_to_agent, row_to_mail, row_to_outbox, row_to_pact, row_to_peer, row_to_queue_row,
    row_to_scroll, row_to_subscription, row_to_task, row_to_topic_federation, row_to_wake_source,
};

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
