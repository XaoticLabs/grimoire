//! Federated namespace memory: a string-named KV store decoupled from git
//! workspaces, designed to replicate across daemons.
//!
//! ## Conflict resolution: last-write-wins on a Lamport tuple
//!
//! Every write carries a version tuple `(lamport, origin_daemon_id)`. A write
//! is accepted iff its tuple is strictly greater than the stored one, ordered
//! by `lamport` first and breaking ties on `origin_daemon_id` lexically. This
//! is deterministic across the cluster: given the same set of writes, every
//! daemon converges on the same value regardless of arrival order, and
//! re-delivering a write is a no-op (it can never be *strictly greater* than
//! the copy already applied) — so replication needs no separate dedupe.
//!
//! Deletes are **tombstones** (`deleted = 1`, empty value): a delete is just a
//! write with a fresh tuple, so it propagates and resolves by the same rule.
//!
//! LWW silently drops the loser of a truly concurrent write. That's the
//! accepted v1 tradeoff; the v2 design (vector clocks + conflict surfacing)
//! is specced in `docs/specs/federated-memory-v2.md`.

use anyhow::Result;
use chrono::Utc;
use rusqlite::params;

use super::persistence::Database;

/// A single namespace write, the unit of both local mutation and replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceWrite {
    pub namespace: String,
    pub key: String,
    pub value: Vec<u8>,
    pub lamport: u64,
    pub origin_daemon_id: String,
    pub deleted: bool,
    pub updated_by: String,
}

/// A live (non-tombstone) namespace entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceEntry {
    pub key: String,
    pub value: Vec<u8>,
    pub lamport: u64,
    pub origin_daemon_id: String,
    pub updated_at: i64,
    pub updated_by: String,
}

/// A claimed replication row, ready to send as a `MemoryDeliver`.
#[derive(Debug, Clone)]
pub struct NsOutboxRow {
    pub id: String,
    pub op_id: String,
    pub namespace: String,
    pub key: String,
    pub value: Vec<u8>,
    pub lamport: u64,
    pub origin_daemon_id: String,
    pub deleted: bool,
    pub updated_by: String,
    pub attempts: u32,
}

/// Outcome of applying a write via the LWW rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NsApply {
    /// The write was strictly newer and is now stored.
    Applied,
    /// An equal-or-newer tuple was already stored; nothing changed.
    Superseded,
}

impl Database {
    /// `true` iff `(a_lamport, a_origin)` strictly dominates `(b_lamport, b_origin)`.
    fn tuple_gt(a_lamport: u64, a_origin: &str, b_lamport: u64, b_origin: &str) -> bool {
        (a_lamport, a_origin) > (b_lamport, b_origin)
    }

    /// Advance the local Lamport counter for a fresh local event and return
    /// the new value (`counter + 1`).
    fn lamport_tick(txn: &rusqlite::Transaction<'_>) -> Result<u64> {
        txn.execute(
            "UPDATE namespace_lamport SET counter = counter + 1 WHERE node = 0",
            [],
        )?;
        let c: i64 = txn.query_row(
            "SELECT counter FROM namespace_lamport WHERE node = 0",
            [],
            |row| row.get(0),
        )?;
        Ok(c.max(0) as u64)
    }

    /// Observe a remote Lamport value: bump the local counter to at least
    /// `seen` so subsequent local events sort after everything we've applied.
    fn lamport_observe(txn: &rusqlite::Transaction<'_>, seen: u64) -> Result<()> {
        txn.execute(
            "UPDATE namespace_lamport SET counter = MAX(counter, ?1) WHERE node = 0",
            params![seen as i64],
        )?;
        Ok(())
    }

    /// Current Lamport counter (testing/inspection).
    #[must_use]
    pub fn namespace_lamport(&self) -> u64 {
        let conn = self.workspace_conn_lock();
        let c: i64 = conn
            .query_row(
                "SELECT counter FROM namespace_lamport WHERE node = 0",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        c.max(0) as u64
    }

    /// Apply a write under the LWW rule, also advancing the local clock to the
    /// write's lamport. Used for **inbound replication** — the tuple is
    /// supplied by the origin daemon, not minted here.
    pub fn namespace_apply_write(&self, w: &NamespaceWrite) -> Result<NsApply> {
        let conn = self.workspace_conn_lock();
        let txn = conn.unchecked_transaction()?;
        Self::lamport_observe(&txn, w.lamport)?;

        let cur: Option<(i64, String)> = txn
            .query_row(
                "SELECT lamport, origin_daemon_id FROM namespace_memory
                 WHERE namespace = ?1 AND key = ?2",
                params![w.namespace, w.key],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .ok();

        let wins = match &cur {
            None => true,
            Some((cl, co)) => {
                Self::tuple_gt(w.lamport, &w.origin_daemon_id, (*cl).max(0) as u64, co)
            }
        };
        if !wins {
            txn.commit()?;
            return Ok(NsApply::Superseded);
        }

        let now = Utc::now().timestamp();
        txn.execute(
            "INSERT INTO namespace_memory
                (namespace, key, value, lamport, origin_daemon_id, deleted, updated_at, updated_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(namespace, key) DO UPDATE SET
                value = excluded.value,
                lamport = excluded.lamport,
                origin_daemon_id = excluded.origin_daemon_id,
                deleted = excluded.deleted,
                updated_at = excluded.updated_at,
                updated_by = excluded.updated_by",
            params![
                w.namespace,
                w.key,
                w.value,
                w.lamport as i64,
                w.origin_daemon_id,
                i64::from(w.deleted),
                now,
                w.updated_by,
            ],
        )?;
        txn.commit()?;
        Ok(NsApply::Applied)
    }

    /// Local put: mint a fresh `(lamport, origin=this daemon)` tuple, store it,
    /// and return the `NamespaceWrite` so the caller can enqueue replication.
    pub fn namespace_put(
        &self,
        namespace: &str,
        key: &str,
        value: &[u8],
        daemon_id: &str,
        updated_by: &str,
    ) -> Result<NamespaceWrite> {
        self.namespace_local_write(namespace, key, value, false, daemon_id, updated_by)
    }

    /// Local delete: writes a tombstone with a fresh tuple. Idempotent — a
    /// delete of an absent key still produces a tombstone so the intent
    /// replicates.
    pub fn namespace_delete(
        &self,
        namespace: &str,
        key: &str,
        daemon_id: &str,
        updated_by: &str,
    ) -> Result<NamespaceWrite> {
        self.namespace_local_write(namespace, key, &[], true, daemon_id, updated_by)
    }

    fn namespace_local_write(
        &self,
        namespace: &str,
        key: &str,
        value: &[u8],
        deleted: bool,
        daemon_id: &str,
        updated_by: &str,
    ) -> Result<NamespaceWrite> {
        let conn = self.workspace_conn_lock();
        let txn = conn.unchecked_transaction()?;
        let lamport = Self::lamport_tick(&txn)?;
        let now = Utc::now().timestamp();
        txn.execute(
            "INSERT INTO namespace_memory
                (namespace, key, value, lamport, origin_daemon_id, deleted, updated_at, updated_by)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(namespace, key) DO UPDATE SET
                value = excluded.value,
                lamport = excluded.lamport,
                origin_daemon_id = excluded.origin_daemon_id,
                deleted = excluded.deleted,
                updated_at = excluded.updated_at,
                updated_by = excluded.updated_by",
            params![
                namespace,
                key,
                value,
                lamport as i64,
                daemon_id,
                i64::from(deleted),
                now,
                updated_by,
            ],
        )?;
        txn.commit()?;
        Ok(NamespaceWrite {
            namespace: namespace.to_string(),
            key: key.to_string(),
            value: value.to_vec(),
            lamport,
            origin_daemon_id: daemon_id.to_string(),
            deleted,
            updated_by: updated_by.to_string(),
        })
    }

    /// Read a live entry. Tombstones read as `None`.
    pub fn namespace_get(&self, namespace: &str, key: &str) -> Result<Option<NamespaceEntry>> {
        let conn = self.workspace_conn_lock();
        let row = conn
            .query_row(
                "SELECT key, value, lamport, origin_daemon_id, deleted, updated_at, updated_by
                 FROM namespace_memory WHERE namespace = ?1 AND key = ?2",
                params![namespace, key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                },
            )
            .ok();
        Ok(row.and_then(
            |(key, value, lamport, origin, deleted, updated_at, updated_by)| {
                if deleted != 0 {
                    None
                } else {
                    Some(NamespaceEntry {
                        key,
                        value,
                        lamport: lamport.max(0) as u64,
                        origin_daemon_id: origin,
                        updated_at,
                        updated_by,
                    })
                }
            },
        ))
    }

    /// Register (or merge) a namespace↔peer federation. Mirrors
    /// `upsert_topic_federation`: an existing row with a different direction
    /// merges to `Both`. Returns the effective direction.
    pub fn namespace_upsert_federation(
        &self,
        id: &str,
        peer_id: &str,
        namespace: &str,
        direction: crate::shared::types::FederationDirection,
        created_at: i64,
    ) -> Result<crate::shared::types::FederationDirection> {
        use crate::shared::types::FederationDirection;
        let conn = self.workspace_conn_lock();
        let tx = conn.unchecked_transaction()?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT direction FROM namespace_federations WHERE peer_id = ?1 AND namespace = ?2",
                params![peer_id, namespace],
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
            "INSERT INTO namespace_federations (id, peer_id, namespace, direction, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(peer_id, namespace) DO UPDATE SET direction = excluded.direction",
            params![id, peer_id, namespace, final_dir.as_str(), created_at],
        )?;
        tx.commit()?;
        Ok(final_dir)
    }

    /// Enqueue a write for replication to a single peer (Pending, due now).
    /// Redelivery is safe (LWW apply is idempotent), so there's no per-op
    /// dedupe — `op_id` is just a correlation handle for the ack.
    pub fn namespace_enqueue(&self, peer_id: &str, op_id: &str, w: &NamespaceWrite) -> Result<()> {
        let conn = self.workspace_conn_lock();
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO namespace_outbox
                (id, peer_id, op_id, namespace, key, value, lamport, origin_daemon_id,
                 deleted, updated_by, created_at, attempts, next_attempt_at, state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 0, ?11, 'pending')",
            params![
                crate::shared::constants::generate_short_id(),
                peer_id,
                op_id,
                w.namespace,
                w.key,
                w.value,
                w.lamport as i64,
                w.origin_daemon_id,
                i64::from(w.deleted),
                w.updated_by,
                now,
            ],
        )?;
        Ok(())
    }

    /// Next due replication row for a peer (`pending` and `next_attempt_at <=
    /// now`), oldest first. `None` when the queue is drained.
    pub fn namespace_next_outbox(&self, peer_id: &str, now: i64) -> Result<Option<NsOutboxRow>> {
        let conn = self.workspace_conn_lock();
        let row = conn
            .query_row(
                "SELECT id, op_id, namespace, key, value, lamport, origin_daemon_id,
                        deleted, updated_by, attempts
                 FROM namespace_outbox
                 WHERE peer_id = ?1 AND state = 'pending' AND next_attempt_at <= ?2
                 ORDER BY created_at ASC LIMIT 1",
                params![peer_id, now],
                |r| {
                    Ok(NsOutboxRow {
                        id: r.get(0)?,
                        op_id: r.get(1)?,
                        namespace: r.get(2)?,
                        key: r.get(3)?,
                        value: r.get(4)?,
                        lamport: r.get::<_, i64>(5)?.max(0) as u64,
                        origin_daemon_id: r.get(6)?,
                        deleted: r.get::<_, i64>(7)? != 0,
                        updated_by: r.get(8)?,
                        attempts: r.get::<_, i64>(9)?.max(0) as u32,
                    })
                },
            )
            .ok();
        Ok(row)
    }

    pub fn namespace_mark_outbox_in_flight(&self, id: &str) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute(
            "UPDATE namespace_outbox SET state = 'in_flight' WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Successful ack → drop the row (delivered).
    pub fn namespace_mark_outbox_delivered(&self, id: &str) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute("DELETE FROM namespace_outbox WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn namespace_mark_outbox_failed_retry(&self, id: &str, next_attempt_at: i64) -> Result<()> {
        let conn = self.workspace_conn_lock();
        conn.execute(
            "UPDATE namespace_outbox
             SET state = 'pending', attempts = attempts + 1, next_attempt_at = ?2
             WHERE id = ?1",
            params![id, next_attempt_at],
        )?;
        Ok(())
    }

    /// Boot recovery: any row left `in_flight` by a crash goes back to
    /// `pending` so the drainer re-sends it (idempotent under LWW).
    pub fn namespace_reset_outbox_in_flight(&self) -> Result<usize> {
        let conn = self.workspace_conn_lock();
        Ok(conn.execute(
            "UPDATE namespace_outbox SET state = 'pending' WHERE state = 'in_flight'",
            [],
        )?)
    }

    /// Peer ids that should receive outbound replication for `namespace`
    /// (direction `outbound` or `both`).
    pub fn namespace_outbound_peers(&self, namespace: &str) -> Result<Vec<String>> {
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT peer_id FROM namespace_federations
             WHERE namespace = ?1 AND direction IN ('outbound', 'both')",
        )?;
        let rows = stmt.query_map(params![namespace], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Drop a namespace federation row. Mirrors `delete_topic_federation`.
    pub fn delete_namespace_federation(&self, peer_id: &str, namespace: &str) -> Result<bool> {
        let conn = self.workspace_conn_lock();
        let n = conn.execute(
            "DELETE FROM namespace_federations WHERE peer_id = ?1 AND namespace = ?2",
            params![peer_id, namespace],
        )?;
        Ok(n > 0)
    }

    /// List every namespace_federations row across all namespaces. Powers
    /// the "active federations" view in CLI/web.
    pub fn list_namespace_federations(
        &self,
    ) -> Result<Vec<crate::shared::types::NamespaceFederation>> {
        use crate::shared::types::{FederationDirection, NamespaceFederation};
        let conn = self.workspace_conn_lock();
        let mut stmt = conn.prepare(
            "SELECT id, peer_id, namespace, direction, created_at
             FROM namespace_federations
             ORDER BY namespace, peer_id",
        )?;
        let rows = stmt.query_map(params![], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (id, peer_id, namespace, dir, created_at) = r?;
            let direction: FederationDirection = dir.parse().unwrap_or(FederationDirection::Both);
            out.push(NamespaceFederation {
                id,
                peer_id,
                namespace,
                direction,
                created_at,
            });
        }
        Ok(out)
    }

    /// Whether inbound replication into `namespace` from `peer_id` is
    /// authorized (direction `inbound` or `both`).
    pub fn namespace_inbound_authorized(&self, peer_id: &str, namespace: &str) -> Result<bool> {
        let conn = self.workspace_conn_lock();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM namespace_federations
                 WHERE peer_id = ?1 AND namespace = ?2 AND direction IN ('inbound', 'both')",
                params![peer_id, namespace],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(n > 0)
    }

    /// List live entries in a namespace, optionally filtered by key prefix.
    /// Tombstones are skipped.
    pub fn namespace_list(
        &self,
        namespace: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<NamespaceEntry>> {
        let conn = self.workspace_conn_lock();
        let like = prefix.map(|p| format!("{p}%"));
        let mut stmt = conn.prepare(
            "SELECT key, value, lamport, origin_daemon_id, updated_at, updated_by
             FROM namespace_memory
             WHERE namespace = ?1 AND deleted = 0 AND (?2 IS NULL OR key LIKE ?2)
             ORDER BY key",
        )?;
        let rows = stmt.query_map(params![namespace, like], |row| {
            Ok(NamespaceEntry {
                key: row.get(0)?,
                value: row.get(1)?,
                lamport: row.get::<_, i64>(2)?.max(0) as u64,
                origin_daemon_id: row.get(3)?,
                updated_at: row.get(4)?,
                updated_by: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Database {
        Database::open_in_memory().unwrap()
    }

    #[test]
    fn local_put_get_roundtrip() {
        let db = db();
        db.namespace_put("ns", "k", b"v1", "aaaa", "agent-1")
            .unwrap();
        let e = db.namespace_get("ns", "k").unwrap().unwrap();
        assert_eq!(e.value, b"v1");
        assert_eq!(e.origin_daemon_id, "aaaa");
    }

    #[test]
    fn local_writes_advance_lamport() {
        let db = db();
        let w1 = db.namespace_put("ns", "a", b"1", "aaaa", "u").unwrap();
        let w2 = db.namespace_put("ns", "b", b"2", "aaaa", "u").unwrap();
        assert!(w2.lamport > w1.lamport, "lamport must advance");
    }

    #[test]
    fn delete_tombstones_and_hides() {
        let db = db();
        db.namespace_put("ns", "k", b"v", "aaaa", "u").unwrap();
        db.namespace_delete("ns", "k", "aaaa", "u").unwrap();
        assert!(db.namespace_get("ns", "k").unwrap().is_none());
        // Tombstone is excluded from list output.
        assert!(db.namespace_list("ns", None).unwrap().is_empty());
    }

    #[test]
    fn lww_higher_lamport_wins_regardless_of_arrival() {
        let db = db();
        // Apply a high-lamport remote write first.
        db.namespace_apply_write(&NamespaceWrite {
            namespace: "ns".into(),
            key: "k".into(),
            value: b"newer".to_vec(),
            lamport: 10,
            origin_daemon_id: "bbbb".into(),
            deleted: false,
            updated_by: "remote".into(),
        })
        .unwrap();
        // A lower-lamport write must NOT overwrite it.
        let out = db
            .namespace_apply_write(&NamespaceWrite {
                namespace: "ns".into(),
                key: "k".into(),
                value: b"older".to_vec(),
                lamport: 5,
                origin_daemon_id: "zzzz".into(),
                deleted: false,
                updated_by: "remote".into(),
            })
            .unwrap();
        assert_eq!(out, NsApply::Superseded);
        assert_eq!(
            db.namespace_get("ns", "k").unwrap().unwrap().value,
            b"newer"
        );
    }

    #[test]
    fn lww_ties_break_on_origin_daemon_id() {
        let db = db();
        db.namespace_apply_write(&NamespaceWrite {
            namespace: "ns".into(),
            key: "k".into(),
            value: b"from-m".to_vec(),
            lamport: 7,
            origin_daemon_id: "mmmm".into(),
            deleted: false,
            updated_by: "r".into(),
        })
        .unwrap();
        // Same lamport, higher daemon_id wins.
        let out = db
            .namespace_apply_write(&NamespaceWrite {
                namespace: "ns".into(),
                key: "k".into(),
                value: b"from-z".to_vec(),
                lamport: 7,
                origin_daemon_id: "zzzz".into(),
                deleted: false,
                updated_by: "r".into(),
            })
            .unwrap();
        assert_eq!(out, NsApply::Applied);
        assert_eq!(
            db.namespace_get("ns", "k").unwrap().unwrap().value,
            b"from-z"
        );

        // Same lamport, lower daemon_id loses.
        let out = db
            .namespace_apply_write(&NamespaceWrite {
                namespace: "ns".into(),
                key: "k".into(),
                value: b"from-a".to_vec(),
                lamport: 7,
                origin_daemon_id: "aaaa".into(),
                deleted: false,
                updated_by: "r".into(),
            })
            .unwrap();
        assert_eq!(out, NsApply::Superseded);
    }

    #[test]
    fn reapplying_a_write_is_a_noop() {
        let db = db();
        let w = NamespaceWrite {
            namespace: "ns".into(),
            key: "k".into(),
            value: b"v".to_vec(),
            lamport: 4,
            origin_daemon_id: "bbbb".into(),
            deleted: false,
            updated_by: "r".into(),
        };
        assert_eq!(db.namespace_apply_write(&w).unwrap(), NsApply::Applied);
        // Redelivery is not strictly greater → superseded, value unchanged.
        assert_eq!(db.namespace_apply_write(&w).unwrap(), NsApply::Superseded);
    }

    #[test]
    fn observing_remote_lamport_advances_local_clock() {
        let db = db();
        db.namespace_apply_write(&NamespaceWrite {
            namespace: "ns".into(),
            key: "k".into(),
            value: b"v".to_vec(),
            lamport: 100,
            origin_daemon_id: "bbbb".into(),
            deleted: false,
            updated_by: "r".into(),
        })
        .unwrap();
        // The next local write must sort after the observed remote one.
        let w = db.namespace_put("ns", "k2", b"x", "aaaa", "u").unwrap();
        assert!(
            w.lamport > 100,
            "local clock must advance past observed remote"
        );
    }
}
