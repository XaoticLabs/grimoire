//! Workspace + memory KV database methods, kept separate from the main
//! `persistence.rs` to keep this v1 feature self-contained.
//!
//! Uses the same `Database` connection pool via `with_test_conn` is not the
//! pattern — these are plain methods on `Database` added via an `impl` block
//! in this module.

use anyhow::{Result, anyhow};
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::params;
use std::path::PathBuf;

use crate::shared::types::{
    MemoryEntry, MemoryListItem, Workspace, WorkspaceListEntry, WorkspaceState,
};

use super::persistence::Database;

/// Outcome of a memory CAS write — surfaces the prior version on conflict so
/// the caller can retry trivially (1 RTT).
#[derive(Debug)]
pub enum MemoryWriteOutcome {
    /// Success; row now lives at `version`.
    Written { version: u64 },
    /// CAS conflict; the row's current version (0 = not present).
    Conflict { current_version: u64 },
}

impl Database {
    // --- Workspace CRUD ---

    pub fn insert_workspace(&self, ws: &Workspace) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute(
            "INSERT INTO workspaces (id, path, repo_path, branch, state, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                ws.id,
                ws.path.to_string_lossy(),
                ws.repo_path.to_string_lossy(),
                ws.branch,
                ws.state.as_str(),
                ws.created_at.timestamp(),
            ],
        )?;
        Ok(())
    }

    pub fn get_workspace(&self, id: &str) -> Result<Option<Workspace>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT id, path, repo_path, branch, state, created_at
             FROM workspaces WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_workspace(row)?)),
            None => Ok(None),
        }
    }

    pub fn list_workspaces_with_counts(&self) -> Result<Vec<WorkspaceListEntry>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT w.id, w.path, w.branch, w.state, w.created_at,
                    COALESCE(c.cnt, 0) AS agent_count
             FROM workspaces w
             LEFT JOIN (
                 SELECT workspace_id, COUNT(*) AS cnt
                 FROM workspace_assignments
                 GROUP BY workspace_id
             ) c ON c.workspace_id = w.id
             ORDER BY w.created_at",
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let path_str: String = row.get(1)?;
            let state_str: String = row.get(3)?;
            let created_at: i64 = row.get(4)?;
            let count: i64 = row.get(5)?;
            out.push(WorkspaceListEntry {
                id: row.get(0)?,
                path: PathBuf::from(path_str),
                branch: row.get(2)?,
                state: state_str.parse().unwrap_or(WorkspaceState::Active),
                agent_count: count.max(0) as u32,
                created_at: Utc.timestamp_opt(created_at, 0).single().unwrap_or_else(Utc::now),
            });
        }
        Ok(out)
    }

    pub fn update_workspace_state(&self, id: &str, state: WorkspaceState) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute(
            "UPDATE workspaces SET state = ?1 WHERE id = ?2",
            params![state.as_str(), id],
        )?;
        Ok(())
    }

    pub fn delete_workspace_row(&self, id: &str) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute("DELETE FROM workspaces WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn list_workspace_paths(&self) -> Result<Vec<(String, PathBuf, WorkspaceState)>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare("SELECT id, path, state FROM workspaces")?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let path_str: String = row.get(1)?;
            let state_str: String = row.get(2)?;
            out.push((
                row.get(0)?,
                PathBuf::from(path_str),
                state_str.parse().unwrap_or(WorkspaceState::Active),
            ));
        }
        Ok(out)
    }

    // --- Workspace assignments ---

    pub fn insert_workspace_assignment(
        &self,
        workspace_id: &str,
        agent_id: &str,
    ) -> Result<()> {
        let conn = self.workspace_conn_lock();
        // ON CONFLICT DO NOTHING to make assignment idempotent.
        conn.execute(
            "INSERT OR IGNORE INTO workspace_assignments (workspace_id, agent_id, assigned_at)
             VALUES (?1, ?2, ?3)",
            params![workspace_id, agent_id, Utc::now().timestamp()],
        )?;
        // Also stamp agents.workspace_id.
        conn.execute(
            "UPDATE agents SET workspace_id = ?1 WHERE id = ?2",
            params![workspace_id, agent_id],
        )?;
        Ok(())
    }

    pub fn list_active_assigned_agents(&self, workspace_id: &str) -> Result<Vec<(String, String)>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT a.id, a.state
             FROM workspace_assignments wa
             JOIN agents a ON a.id = wa.agent_id
             WHERE wa.workspace_id = ?1",
        )?;
        let mut rows = stmt.query(params![workspace_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push((row.get(0)?, row.get(1)?));
        }
        Ok(out)
    }

    pub fn agent_workspace_id(&self, agent_id: &str) -> Result<Option<String>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare("SELECT workspace_id FROM agents WHERE id = ?1")?;
        let mut rows = stmt.query(params![agent_id])?;
        match rows.next()? {
            Some(row) => Ok(row.get::<_, Option<String>>(0)?),
            None => Ok(None),
        }
    }

    // --- Memory KV ---

    pub fn memory_get(&self, workspace_id: &str, key: &str) -> Result<Option<MemoryEntry>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT workspace_id, key, value, version, updated_at, updated_by
             FROM workspace_memory WHERE workspace_id = ?1 AND key = ?2",
        )?;
        let mut rows = stmt.query(params![workspace_id, key])?;
        match rows.next()? {
            Some(row) => {
                let value_blob: Vec<u8> = row.get(2)?;
                let value: serde_json::Value =
                    serde_json::from_slice(&value_blob).map_err(|e| anyhow!("bad_json: {}", e))?;
                let version: i64 = row.get(3)?;
                let updated_at: i64 = row.get(4)?;
                Ok(Some(MemoryEntry {
                    workspace_id: row.get(0)?,
                    key: row.get(1)?,
                    value,
                    version: version.max(0) as u64,
                    updated_at,
                    updated_by: row.get(5)?,
                }))
            }
            None => Ok(None),
        }
    }

    /// Returns: previous version (0 if absent), and the size of the existing
    /// value. Used by put/delete CAS pre-check.
    pub fn memory_current_version_and_size(
        &self,
        workspace_id: &str,
        key: &str,
    ) -> Result<(u64, u64)> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT version, LENGTH(value) FROM workspace_memory
             WHERE workspace_id = ?1 AND key = ?2",
        )?;
        let mut rows = stmt.query(params![workspace_id, key])?;
        match rows.next()? {
            Some(row) => {
                let version: i64 = row.get(0)?;
                let len: i64 = row.get(1)?;
                Ok((version.max(0) as u64, len.max(0) as u64))
            }
            None => Ok((0, 0)),
        }
    }

    pub fn memory_total_size_for_workspace(&self, workspace_id: &str) -> Result<u64> {
        let conn = self.workspace_conn_lock();
        let total: Option<i64> = conn
            .query_row(
                "SELECT SUM(LENGTH(value)) FROM workspace_memory WHERE workspace_id = ?1",
                params![workspace_id],
                |row| row.get(0),
            )
            .ok();
        Ok(total.unwrap_or(0).max(0) as u64)
    }

    /// Optimistic-CAS put: writes only if `expected_version` matches the
    /// current version (or `None` for unconditional). Returns the new version
    /// or a `Conflict` outcome with the current version.
    pub fn memory_put_cas(
        &self,
        workspace_id: &str,
        key: &str,
        value: &[u8],
        expected_version: Option<u64>,
        updated_by: &str,
    ) -> Result<MemoryWriteOutcome> {
        let conn = self.workspace_conn_lock();
        let txn = conn.unchecked_transaction()?;
        let cur: Option<(i64, )> = txn
            .query_row(
                "SELECT version FROM workspace_memory WHERE workspace_id = ?1 AND key = ?2",
                params![workspace_id, key],
                |row| Ok((row.get::<_, i64>(0)?,)),
            )
            .ok();
        let cur_version = cur.map(|(v,)| v.max(0) as u64).unwrap_or(0);

        if let Some(expected) = expected_version {
            if cur_version != expected {
                return Ok(MemoryWriteOutcome::Conflict {
                    current_version: cur_version,
                });
            }
        }

        let new_version = cur_version + 1;
        let now = Utc::now().timestamp();
        txn.execute(
            "INSERT INTO workspace_memory
                (workspace_id, key, value, version, updated_at, updated_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(workspace_id, key) DO UPDATE SET
                value = excluded.value,
                version = excluded.version,
                updated_at = excluded.updated_at,
                updated_by = excluded.updated_by",
            params![workspace_id, key, value, new_version as i64, now, updated_by],
        )?;
        txn.commit()?;
        Ok(MemoryWriteOutcome::Written {
            version: new_version,
        })
    }

    /// Optimistic-CAS delete. If the key doesn't exist, this is a no-op
    /// (idempotent) and returns `Written { version: 0 }` to signal nothing
    /// happened. The caller knows by querying first whether to emit a
    /// `MemoryDeleted` event.
    pub fn memory_delete_cas(
        &self,
        workspace_id: &str,
        key: &str,
        expected_version: Option<u64>,
    ) -> Result<MemoryWriteOutcome> {
        let conn = self.workspace_conn_lock();
        let txn = conn.unchecked_transaction()?;
        let cur: Option<(i64, )> = txn
            .query_row(
                "SELECT version FROM workspace_memory WHERE workspace_id = ?1 AND key = ?2",
                params![workspace_id, key],
                |row| Ok((row.get::<_, i64>(0)?,)),
            )
            .ok();
        let cur_version = cur.map(|(v,)| v.max(0) as u64).unwrap_or(0);
        if cur_version == 0 {
            return Ok(MemoryWriteOutcome::Written { version: 0 });
        }
        if let Some(expected) = expected_version {
            if cur_version != expected {
                return Ok(MemoryWriteOutcome::Conflict {
                    current_version: cur_version,
                });
            }
        }
        txn.execute(
            "DELETE FROM workspace_memory WHERE workspace_id = ?1 AND key = ?2",
            params![workspace_id, key],
        )?;
        txn.commit()?;
        Ok(MemoryWriteOutcome::Written {
            version: cur_version,
        })
    }

    pub fn memory_list_prefix(
        &self,
        workspace_id: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<MemoryListItem>> {
        let conn = self.workspace_conn_lock();
        let mut out = Vec::new();
        match prefix {
            None => {
                let mut stmt = conn.prepare(
                    "SELECT key, version, updated_at, LENGTH(value)
                     FROM workspace_memory WHERE workspace_id = ?1
                     ORDER BY key",
                )?;
                let mut rows = stmt.query(params![workspace_id])?;
                while let Some(row) = rows.next()? {
                    let version: i64 = row.get(1)?;
                    let updated_at: i64 = row.get(2)?;
                    let value_size: i64 = row.get(3)?;
                    out.push(MemoryListItem {
                        key: row.get(0)?,
                        version: version.max(0) as u64,
                        updated_at,
                        value_size: value_size.max(0) as u64,
                    });
                }
            }
            Some(p) => {
                // Segment-aligned prefix: matches `p` exactly OR `p/...`.
                let like = format!("{}/%", p);
                let mut stmt = conn.prepare(
                    "SELECT key, version, updated_at, LENGTH(value)
                     FROM workspace_memory
                     WHERE workspace_id = ?1 AND (key = ?2 OR key LIKE ?3)
                     ORDER BY key",
                )?;
                let mut rows = stmt.query(params![workspace_id, p, like])?;
                while let Some(row) = rows.next()? {
                    let version: i64 = row.get(1)?;
                    let updated_at: i64 = row.get(2)?;
                    let value_size: i64 = row.get(3)?;
                    out.push(MemoryListItem {
                        key: row.get(0)?,
                        version: version.max(0) as u64,
                        updated_at,
                        value_size: value_size.max(0) as u64,
                    });
                }
            }
        }
        Ok(out)
    }
}

fn row_to_workspace(row: &rusqlite::Row) -> Result<Workspace> {
    let path_str: String = row.get(1)?;
    let repo_str: String = row.get(2)?;
    let state_str: String = row.get(4)?;
    let created_at: i64 = row.get(5)?;
    let dt: DateTime<Utc> = Utc
        .timestamp_opt(created_at, 0)
        .single()
        .unwrap_or_else(Utc::now);
    Ok(Workspace {
        id: row.get(0)?,
        path: PathBuf::from(path_str),
        repo_path: PathBuf::from(repo_str),
        branch: row.get(3)?,
        state: state_str.parse().unwrap_or(WorkspaceState::Active),
        created_at: dt,
    })
}

