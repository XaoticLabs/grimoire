//! Persistence for per-agent artifacts and summon idempotency keys.

use anyhow::Result;
use rusqlite::{OptionalExtension, params};

use crate::shared::types::{AgentArtifact, FileChange};

impl super::Database {
    /// Record the baseline commit for an agent at dispatch time. Upserts so
    /// a restart re-baselines against the current HEAD. A `None` base (cwd
    /// is not a repo) still writes the row so the completion-time upsert has
    /// somewhere to land.
    pub fn set_artifact_base(&self, agent_id: &str, base_commit: Option<&str>) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "INSERT INTO agent_artifacts (agent_id, base_commit) VALUES (?1, ?2)
             ON CONFLICT(agent_id) DO UPDATE SET base_commit = excluded.base_commit",
            params![agent_id, base_commit],
        )?;
        Ok(())
    }

    /// Read the baseline commit recorded at dispatch for `agent_id`, if any.
    pub fn get_artifact_base(&self, agent_id: &str) -> Result<Option<String>> {
        let conn = self.conn_lock();
        let base = conn
            .query_row(
                "SELECT base_commit FROM agent_artifacts WHERE agent_id = ?1",
                params![agent_id],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        Ok(base)
    }

    /// Persist the full computed artifact for an agent, replacing any prior
    /// row (e.g. the dispatch-time base-only stub).
    pub fn upsert_artifact(&self, a: &AgentArtifact) -> Result<()> {
        let files_json = serde_json::to_string(&a.files_changed)?;
        let conn = self.conn_lock();
        conn.execute(
            "INSERT INTO agent_artifacts
                (agent_id, base_commit, files_changed, diff, insertions, deletions, tokens_used, usd_spent, captured_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(agent_id) DO UPDATE SET
                base_commit   = excluded.base_commit,
                files_changed = excluded.files_changed,
                diff          = excluded.diff,
                insertions    = excluded.insertions,
                deletions     = excluded.deletions,
                tokens_used   = excluded.tokens_used,
                usd_spent     = excluded.usd_spent,
                captured_at   = excluded.captured_at",
            params![
                a.agent_id,
                a.base_commit,
                files_json,
                a.diff,
                a.insertions as i64,
                a.deletions as i64,
                a.tokens_used as i64,
                a.usd_spent,
                a.captured_at,
            ],
        )?;
        Ok(())
    }

    /// Fetch an agent's artifact, if one has been captured. Returns `None`
    /// when the agent never ran to completion or produced no row.
    pub fn get_artifact(&self, agent_id: &str) -> Result<Option<AgentArtifact>> {
        let conn = self.conn_lock();
        let row = conn
            .query_row(
                "SELECT agent_id, base_commit, files_changed, diff, insertions, deletions, tokens_used, usd_spent, captured_at
                 FROM agent_artifacts WHERE agent_id = ?1",
                params![agent_id],
                |r| {
                    let files_json: String = r.get(2)?;
                    let files: Vec<FileChange> =
                        serde_json::from_str(&files_json).unwrap_or_default();
                    let captured: Option<i64> = r.get(8)?;
                    Ok(AgentArtifact {
                        agent_id: r.get(0)?,
                        base_commit: r.get(1)?,
                        files_changed: files,
                        diff: r.get(3)?,
                        insertions: r.get::<_, i64>(4)?.max(0) as u64,
                        deletions: r.get::<_, i64>(5)?.max(0) as u64,
                        tokens_used: r.get::<_, i64>(6)?.max(0) as u64,
                        usd_spent: r.get(7)?,
                        captured_at: captured.unwrap_or(0),
                    })
                },
            )
            .optional()?;
        // A base-only stub (captured_at NULL → 0) is not a real artifact yet.
        Ok(row.filter(|a| a.captured_at != 0))
    }

    // --- Idempotency keys -------------------------------------------------

    /// Resolve a summon idempotency key to the agent it first minted, if any.
    pub fn lookup_idempotency_key(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn_lock();
        let id = conn
            .query_row(
                "SELECT agent_id FROM idempotency_keys WHERE key = ?1",
                params![key],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        Ok(id)
    }

    /// Bind an idempotency key to an agent. Fails silently (no-op) if the
    /// key already exists — the caller should have looked it up first; the
    /// `OR IGNORE` defends against a race between two concurrent summons.
    pub fn insert_idempotency_key(&self, key: &str, agent_id: &str) -> Result<()> {
        let conn = self.conn_lock();
        conn.execute(
            "INSERT OR IGNORE INTO idempotency_keys (key, agent_id, created_at)
             VALUES (?1, ?2, ?3)",
            params![key, agent_id, chrono::Utc::now().timestamp()],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::daemon::persistence::Database;
    use crate::shared::types::{
        Agent, AgentArtifact, AgentState, FileChange, RestartPolicy,
    };
    use chrono::Utc;
    use std::path::PathBuf;

    fn db() -> Database {
        Database::open_in_memory().unwrap()
    }

    fn seed_agent(db: &Database, id: &str) {
        db.insert_agent(&Agent {
            id: id.to_string(),
            name: None,
            state: AgentState::Complete,
            task: Some("t".into()),
            model: None,
            provider: Some("claude".into()),
            cwd: PathBuf::from("/tmp"),
            pid: None,
            session_id: None,
            exit_code: Some(0),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            worker_id: None,
            restart_policy: RestartPolicy::Never,
            restart_count: 0,
            workspace_id: None,
        })
        .unwrap();
    }

    #[test]
    fn base_then_full_artifact_roundtrip() {
        let db = db();
        seed_agent(&db, "agent001");
        db.set_artifact_base("agent001", Some("abc123")).unwrap();
        assert_eq!(
            db.get_artifact_base("agent001").unwrap().as_deref(),
            Some("abc123")
        );
        // Base-only stub is not yet a real artifact.
        assert!(db.get_artifact("agent001").unwrap().is_none());

        let art = AgentArtifact {
            agent_id: "agent001".into(),
            base_commit: Some("abc123".into()),
            files_changed: vec![FileChange {
                path: "src/main.rs".into(),
                status: "M".into(),
                insertions: 10,
                deletions: 2,
            }],
            diff: Some("diff --git a/src/main.rs...".into()),
            insertions: 10,
            deletions: 2,
            tokens_used: 1500,
            usd_spent: 0.03,
            captured_at: 1_700_000_000,
        };
        db.upsert_artifact(&art).unwrap();
        let got = db.get_artifact("agent001").unwrap().unwrap();
        assert_eq!(got.files_changed.len(), 1);
        assert_eq!(got.insertions, 10);
        assert_eq!(got.tokens_used, 1500);
        assert!((got.usd_spent - 0.03).abs() < 1e-9);
    }

    #[test]
    fn idempotency_key_first_write_wins() {
        let db = db();
        assert!(db.lookup_idempotency_key("k1").unwrap().is_none());
        db.insert_idempotency_key("k1", "agentaaa").unwrap();
        db.insert_idempotency_key("k1", "agentbbb").unwrap(); // ignored
        assert_eq!(
            db.lookup_idempotency_key("k1").unwrap().as_deref(),
            Some("agentaaa")
        );
    }
}
