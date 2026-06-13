//! Workspace + memory KV `Database` methods, kept separate from
//! `persistence.rs` to keep this feature self-contained.

use anyhow::{Result, anyhow};
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::params;
use std::path::PathBuf;

use crate::shared::types::{
    FederationDirection, MemoryEntry, MemoryListItem, Workspace, WorkspaceFederation,
    WorkspaceKind, WorkspaceListEntry, WorkspaceState,
};

use super::persistence::Database;

/// Outcome of a memory CAS write; surfaces the prior version on conflict so the
/// caller can retry in one round trip.
#[derive(Debug)]
pub enum MemoryWriteOutcome {
    Written { version: u64 },
    /// CAS conflict; current version (0 = not present).
    Conflict { current_version: u64 },
}

/// One pending workspace-event-outbox row, ready to ship.
#[derive(Debug, Clone)]
pub struct WsEventOutboxRow {
    pub id: String,
    pub workspace_id: String,
    pub sender_seq: u64,
    pub payload: Vec<u8>,
    pub attempts: u32,
}

impl Database {
    // --- Workspace CRUD ---

    pub fn insert_workspace(&self, ws: &Workspace) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute(
            "INSERT INTO workspaces
                (id, path, repo_path, branch, state, created_at,
                 kind, home_daemon_id, home_workspace_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                ws.id,
                ws.path.to_string_lossy(),
                ws.repo_path.to_string_lossy(),
                ws.branch,
                ws.state.as_str(),
                ws.created_at.timestamp(),
                ws.kind.as_str(),
                ws.home_daemon_id,
                ws.home_workspace_id,
            ],
        )?;
        Ok(())
    }

    pub fn get_workspace(&self, id: &str) -> Result<Option<Workspace>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT id, path, repo_path, branch, state, created_at,
                    kind, home_daemon_id, home_workspace_id
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
                    COALESCE(c.cnt, 0) AS agent_count,
                    w.kind, w.home_daemon_id, w.home_workspace_id
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
            let kind_str: String = row.get(6)?;
            out.push(WorkspaceListEntry {
                id: row.get(0)?,
                path: PathBuf::from(path_str),
                branch: row.get(2)?,
                state: state_str.parse().unwrap_or(WorkspaceState::Active),
                agent_count: count.max(0) as u32,
                created_at: Utc
                    .timestamp_opt(created_at, 0)
                    .single()
                    .unwrap_or_else(Utc::now),
                kind: kind_str.parse().unwrap_or(WorkspaceKind::Local),
                home_daemon_id: row.get(7)?,
                home_workspace_id: row.get(8)?,
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

    /// Workspaces with an on-disk worktree. Excludes shadows, whose `path` is
    /// the `shadow://…` sentinel — else the boot reconciler deletes them as
    /// orphans.
    pub fn list_workspace_paths(&self) -> Result<Vec<(String, PathBuf, WorkspaceState)>> {
        let conn = self.workspace_conn_lock();
        let mut stmt =
            conn.prepare("SELECT id, path, state FROM workspaces WHERE kind = 'Local'")?;
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

    pub fn insert_workspace_assignment(&self, workspace_id: &str, agent_id: &str) -> Result<()> {
        let conn = self.workspace_conn_lock();
        // INSERT OR IGNORE keeps assignment idempotent.
        conn.execute(
            "INSERT OR IGNORE INTO workspace_assignments (workspace_id, agent_id, assigned_at)
             VALUES (?1, ?2, ?3)",
            params![workspace_id, agent_id, Utc::now().timestamp()],
        )?;
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
                    serde_json::from_slice(&value_blob).map_err(|e| anyhow!("bad_json: {e}"))?;
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

    /// (version, value size); version 0 if absent. CAS pre-check for put/delete.
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

    /// Optimistic-CAS put: writes only if `expected_version` matches current
    /// (`None` = unconditional).
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
        let cur: Option<(i64,)> = txn
            .query_row(
                "SELECT version FROM workspace_memory WHERE workspace_id = ?1 AND key = ?2",
                params![workspace_id, key],
                |row| Ok((row.get::<_, i64>(0)?,)),
            )
            .ok();
        let cur_version = cur.map_or(0, |(v,)| v.max(0) as u64);

        if let Some(expected) = expected_version
            && cur_version != expected
        {
            return Ok(MemoryWriteOutcome::Conflict {
                current_version: cur_version,
            });
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
            params![
                workspace_id,
                key,
                value,
                new_version as i64,
                now,
                updated_by
            ],
        )?;
        txn.commit()?;
        Ok(MemoryWriteOutcome::Written {
            version: new_version,
        })
    }

    /// Optimistic-CAS delete. Missing key is an idempotent no-op returning
    /// `Written { version: 0 }`, signalling the caller to skip the event.
    pub fn memory_delete_cas(
        &self,
        workspace_id: &str,
        key: &str,
        expected_version: Option<u64>,
    ) -> Result<MemoryWriteOutcome> {
        let conn = self.workspace_conn_lock();
        let txn = conn.unchecked_transaction()?;
        let cur: Option<(i64,)> = txn
            .query_row(
                "SELECT version FROM workspace_memory WHERE workspace_id = ?1 AND key = ?2",
                params![workspace_id, key],
                |row| Ok((row.get::<_, i64>(0)?,)),
            )
            .ok();
        let cur_version = cur.map_or(0, |(v,)| v.max(0) as u64);
        if cur_version == 0 {
            return Ok(MemoryWriteOutcome::Written { version: 0 });
        }
        if let Some(expected) = expected_version
            && cur_version != expected
        {
            return Ok(MemoryWriteOutcome::Conflict {
                current_version: cur_version,
            });
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

    /// Bulk-copy `workspace_memory` rows into `to_workspace`, skipping keys
    /// that already exist (idempotent re-copy). Returns rows inserted.
    pub fn memory_copy_workspace(&self, from_workspace: &str, to_workspace: &str) -> Result<usize> {
        let conn = self.workspace_conn_lock();
        let now = Utc::now().timestamp();
        let mut stmt = conn.prepare(
            "INSERT INTO workspace_memory (workspace_id, key, value, version, updated_at, updated_by)
             SELECT ?1, key, value, 1, ?2, updated_by FROM workspace_memory
             WHERE workspace_id = ?3
             ON CONFLICT(workspace_id, key) DO NOTHING",
        )?;
        let inserted = stmt.execute(params![to_workspace, now, from_workspace])?;
        Ok(inserted)
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
                let like = format!("{p}/%");
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

    // --- Workspace federation ---

    /// Upsert a `workspace_federations` row, merging direction like
    /// `topic_federations` (Inbound + Outbound → Both). Returns the merged
    /// direction.
    pub fn upsert_workspace_federation(
        &self,
        id: &str,
        peer_id: &str,
        workspace_id: &str,
        direction: FederationDirection,
        created_at: i64,
    ) -> Result<FederationDirection> {
        let conn = self.workspace_conn_lock();
        let existing: Option<String> = conn
            .query_row(
                "SELECT direction FROM workspace_federations
                 WHERE peer_id = ?1 AND workspace_id = ?2",
                params![peer_id, workspace_id],
                |r| r.get::<_, String>(0),
            )
            .ok();
        let final_dir = if let Some(s) = existing {
            let cur: FederationDirection = s.parse().unwrap_or(FederationDirection::Both);
            cur.merge(direction)
        } else {
            direction
        };
        conn.execute(
            "INSERT INTO workspace_federations
                (id, peer_id, workspace_id, direction, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(peer_id, workspace_id) DO UPDATE SET direction = excluded.direction",
            params![id, peer_id, workspace_id, final_dir.as_str(), created_at],
        )?;
        Ok(final_dir)
    }

    /// Delete a `workspace_federations` row; returns the row count so the
    /// caller can distinguish removed from no-op.
    pub fn delete_workspace_federation(&self, peer_id: &str, workspace_id: &str) -> Result<usize> {
        let conn = self.workspace_conn_lock();
        Ok(conn.execute(
            "DELETE FROM workspace_federations WHERE peer_id = ?1 AND workspace_id = ?2",
            params![peer_id, workspace_id],
        )?)
    }

    /// Every `workspace_federations` row across all workspaces.
    pub fn list_workspace_federations(&self) -> Result<Vec<WorkspaceFederation>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT id, peer_id, workspace_id, direction, created_at
             FROM workspace_federations
             ORDER BY workspace_id, peer_id",
        )?;
        let mut rows = stmt.query(params![])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let dir_str: String = row.get(3)?;
            let created: i64 = row.get(4)?;
            out.push(WorkspaceFederation {
                id: row.get(0)?,
                peer_id: row.get(1)?,
                workspace_id: row.get(2)?,
                direction: dir_str.parse().unwrap_or(FederationDirection::Both),
                created_at: Utc
                    .timestamp_opt(created, 0)
                    .single()
                    .unwrap_or_else(Utc::now),
            });
        }
        Ok(out)
    }

    pub fn list_workspace_federations_for(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceFederation>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT id, peer_id, workspace_id, direction, created_at
             FROM workspace_federations
             WHERE workspace_id = ?1
             ORDER BY created_at ASC",
        )?;
        let mut rows = stmt.query(params![workspace_id])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let dir_str: String = row.get(3)?;
            let created: i64 = row.get(4)?;
            out.push(WorkspaceFederation {
                id: row.get(0)?,
                peer_id: row.get(1)?,
                workspace_id: row.get(2)?,
                direction: dir_str.parse().unwrap_or(FederationDirection::Both),
                created_at: Utc
                    .timestamp_opt(created, 0)
                    .single()
                    .unwrap_or_else(Utc::now),
            });
        }
        Ok(out)
    }

    /// Peers subscribed to outbound fanout for this workspace; the producer
    /// reads this to decide who gets an outbox row on each emit.
    pub fn workspace_outbound_peers(&self, workspace_id: &str) -> Result<Vec<String>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT peer_id FROM workspace_federations
             WHERE workspace_id = ?1 AND direction IN ('outbound', 'both')",
        )?;
        let rows = stmt.query_map(params![workspace_id], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// True iff `peer_id` may push events at our local shadow `workspace_id`.
    pub fn workspace_federation_inbound_authorized(
        &self,
        peer_id: &str,
        workspace_id: &str,
    ) -> Result<bool> {
        let conn = self.workspace_conn_lock();
        let dir: Option<String> = conn
            .query_row(
                "SELECT direction FROM workspace_federations
                 WHERE peer_id = ?1 AND workspace_id = ?2",
                params![peer_id, workspace_id],
                |r| r.get::<_, String>(0),
            )
            .ok();
        let Some(d) = dir else { return Ok(false) };
        let parsed: FederationDirection = d.parse().unwrap_or(FederationDirection::Both);
        Ok(matches!(
            parsed,
            FederationDirection::Inbound | FederationDirection::Both
        ))
    }

    /// Enqueue a `WorkspaceFileChanged` payload for one outbound peer.
    /// `sender_seq = MAX+1` per (peer, workspace), strictly monotonic. Seq
    /// allocation + INSERT run in one IMMEDIATE txn so concurrent emits can't
    /// collide on the same seq and trip `UNIQUE(peer_id, sender_seq)`.
    pub fn workspace_event_enqueue(
        &self,
        peer_id: &str,
        workspace_id: &str,
        payload: &[u8],
    ) -> Result<u64> {
        let mut conn = self.workspace_conn_lock();
        let now = Utc::now().timestamp();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let next_seq: i64 = tx.query_row(
            "SELECT COALESCE(MAX(sender_seq), 0) + 1 FROM workspace_event_outbox
             WHERE peer_id = ?1 AND workspace_id = ?2",
            params![peer_id, workspace_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO workspace_event_outbox
                (id, peer_id, workspace_id, sender_seq, payload,
                 created_at, attempts, next_attempt_at, state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?6, 'pending')",
            params![
                crate::shared::constants::generate_short_id(),
                peer_id,
                workspace_id,
                next_seq,
                payload,
                now,
            ],
        )?;
        tx.commit()?;
        Ok(u64::try_from(next_seq).unwrap_or(0))
    }

    pub fn workspace_event_next_outbox(
        &self,
        peer_id: &str,
        now: i64,
    ) -> Result<Option<WsEventOutboxRow>> {
        let conn = self.workspace_conn_lock();
        let row = conn
            .query_row(
                "SELECT id, workspace_id, sender_seq, payload, attempts
                 FROM workspace_event_outbox
                 WHERE peer_id = ?1 AND state = 'pending' AND next_attempt_at <= ?2
                 ORDER BY created_at ASC LIMIT 1",
                params![peer_id, now],
                |r| {
                    Ok(WsEventOutboxRow {
                        id: r.get(0)?,
                        workspace_id: r.get(1)?,
                        sender_seq: u64::try_from(r.get::<_, i64>(2)?).unwrap_or(0),
                        payload: r.get(3)?,
                        attempts: u32::try_from(r.get::<_, i64>(4)?).unwrap_or(0),
                    })
                },
            )
            .ok();
        Ok(row)
    }

    pub fn workspace_event_mark_in_flight(&self, id: &str) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute(
            "UPDATE workspace_event_outbox SET state = 'in_flight' WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Ack → drop the row. Workspace events are fire-and-ack; replay is handled
    /// by snapshotting, not by retaining outbox history.
    pub fn workspace_event_mark_delivered(&self, id: &str) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute(
            "DELETE FROM workspace_event_outbox WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn workspace_event_mark_failed_retry(&self, id: &str, next_attempt_at: i64) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute(
            "UPDATE workspace_event_outbox
             SET state = 'pending', attempts = attempts + 1, next_attempt_at = ?2
             WHERE id = ?1",
            params![id, next_attempt_at],
        )?;
        Ok(())
    }

    /// Boot recovery: revert `in_flight` → `pending` so the drainer reships.
    /// Receiver dedupe by `(sender_daemon_id, sender_seq)` makes resend safe.
    pub fn workspace_event_reset_in_flight(&self) -> Result<usize> {
        let conn = self.workspace_conn_lock();
        Ok(conn.execute(
            "UPDATE workspace_event_outbox SET state = 'pending' WHERE state = 'in_flight'",
            [],
        )?)
    }

    /// Record an inbound event at `(sender_daemon_id, sender_seq)`. Returns
    /// `true` on first sighting (caller republishes); `false` if already seen
    /// (caller still acks `ok: true` so the sender drops its row).
    pub fn workspace_event_inbox_record(
        &self,
        sender_daemon_id: &str,
        sender_seq: u64,
        workspace_id: &str,
    ) -> Result<bool> {
        let conn = self.workspace_conn_lock();
        let seq_i = i64::try_from(sender_seq).unwrap_or(i64::MAX);
        let n = conn.execute(
            "INSERT OR IGNORE INTO workspace_event_inbox
                (sender_daemon_id, sender_seq, workspace_id, received_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                sender_daemon_id,
                seq_i,
                workspace_id,
                Utc::now().timestamp(),
            ],
        )?;
        Ok(n > 0)
    }

    /// Resolve the local shadow mirroring a `(home_daemon_id,
    /// home_workspace_id)` pair (the republish target). `None` if no shadow
    /// exists; caller treats that as "drop with positive ack".
    pub fn find_shadow_workspace(
        &self,
        home_daemon_id: &str,
        home_workspace_id: &str,
    ) -> Result<Option<String>> {
        let conn = self.workspace_conn_lock();
        let id: Option<String> = conn
            .query_row(
                "SELECT id FROM workspaces
                 WHERE kind = 'Shadow' AND home_daemon_id = ?1 AND home_workspace_id = ?2
                 LIMIT 1",
                params![home_daemon_id, home_workspace_id],
                |r| r.get(0),
            )
            .ok();
        Ok(id)
    }

    /// Insert a shadow workspace row pointing at a remote home. No on-disk
    /// worktree; `path` is the sentinel `shadow://<home-daemon>/<home-ws>` to
    /// satisfy `UNIQUE(path)` without looking like a real directory.
    pub fn insert_shadow_workspace(
        &self,
        local_id: &str,
        home_daemon_id: &str,
        home_workspace_id: &str,
        branch: &str,
        now: DateTime<Utc>,
    ) -> Result<()> {
        let sentinel = format!("shadow://{home_daemon_id}/{home_workspace_id}");
        let conn = self.workspace_conn_lock();
        conn.execute(
            "INSERT INTO workspaces
                (id, path, repo_path, branch, state, created_at,
                 kind, home_daemon_id, home_workspace_id)
             VALUES (?1, ?2, '', ?3, ?4, ?5, 'Shadow', ?6, ?7)",
            params![
                local_id,
                sentinel,
                branch,
                WorkspaceState::Active.as_str(),
                now.timestamp(),
                home_daemon_id,
                home_workspace_id,
            ],
        )?;
        Ok(())
    }
}

fn row_to_workspace(row: &rusqlite::Row) -> Result<Workspace> {
    let path_str: String = row.get(1)?;
    let repo_str: String = row.get(2)?;
    let state_str: String = row.get(4)?;
    let created_at: i64 = row.get(5)?;
    let kind_str: String = row.get(6)?;
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
        kind: kind_str.parse().unwrap_or(WorkspaceKind::Local),
        home_daemon_id: row.get(7)?,
        home_workspace_id: row.get(8)?,
    })
}
