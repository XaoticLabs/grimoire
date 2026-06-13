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

// Re-exported so sibling submodules reach them via `super::row_to_*`.
use rows::{
    row_to_agent, row_to_mail, row_to_outbox, row_to_pact, row_to_peer, row_to_queue_row,
    row_to_scroll, row_to_subscription, row_to_task, row_to_topic_federation, row_to_wake_source,
};

/// A `task_queue` row: requested-but-undispatched work, sharing its `id` with
/// the matching `agents` row.
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

/// An `eval_results` row: one rubric-scored verdict by `evaluator_id` against
/// `target_id`. Multiple verdicts per target are allowed.
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

/// A durable `events` row with its payload deserialized into a `StreamEvent`.
#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub seq: i64,
    pub kind: String,
    /// RFC 3339 timestamp, kept as stored text.
    pub ts: String,
    pub event: StreamEvent,
}

/// Restart-recovery summary: mid-flight agents flipped to `Failed` (with prior
/// state, for accurate `StateChange` events) and surviving `Queued` count.
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub failed: Vec<(crate::shared::types::AgentId, AgentState)>,
    pub queued_remaining: usize,
}

/// One outbox fanout row: (peer_id, outbox_id, mail_id, recipient, body, sender, created_at).
pub type OutboxFanoutRow = (String, String, String, String, String, Option<String>, i64);

/// `SELECT … FROM mail` column list; order must match `row_to_mail`.
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

    /// Run a synchronous DB closure on the blocking pool, off the Tokio worker.
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

    /// Test-only: run a closure with the connection locked, for inspecting
    /// schema/contents without a public read API.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_test_conn<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&Connection) -> T,
    {
        let conn = self.conn.lock();
        f(&conn)
    }

    /// Locked connection guard for sibling modules (e.g. `workspace_db`).
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

    /// Locked connection guard for submodules running multi-statement transactions.
    pub(super) fn conn_lock(&self) -> parking_lot::MutexGuard<'_, Connection> {
        self.conn.lock()
    }
}

/// Current unix epoch seconds.
pub fn unix_now() -> i64 {
    Utc::now().timestamp()
}
