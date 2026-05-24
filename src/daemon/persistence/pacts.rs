use anyhow::Result;
use chrono::Utc;
use rusqlite::params;

use crate::shared::types::Pact;

use super::row_to_pact;

impl super::Database {
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
}
