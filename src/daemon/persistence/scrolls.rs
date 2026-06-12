use anyhow::Result;
use chrono::Utc;
use rusqlite::params;

use crate::shared::types::{AgentId, Scroll, ScrollState, Task, TaskId, TaskState};

use super::{EvalResultRow, QueueRow, row_to_queue_row, row_to_scroll, row_to_task};

impl super::Database {
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
            "INSERT INTO tasks (id, scroll_id, name, prompt, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at, peer_name, verify_rubric, verify_threshold, verifier_agent_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
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
                task.peer_name,
                task.verify_rubric,
                task.verify_threshold,
                task.verifier_agent_id,
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
            "SELECT id, scroll_id, name, prompt, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at, peer_name, verify_rubric, verify_threshold, verifier_agent_id
             FROM tasks WHERE scroll_id = ?1 ORDER BY order_index ASC",
            params![scroll_id],
            row_to_task,
        )
    }

    pub fn get_task(&self, task_id: &str) -> Result<Option<Task>> {
        self.query_opt(
            "SELECT id, scroll_id, name, prompt, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at, peer_name, verify_rubric, verify_threshold, verifier_agent_id
             FROM tasks WHERE id = ?1",
            params![task_id],
            row_to_task,
        )
    }

    pub fn get_task_by_agent_id(&self, agent_id: &str) -> Result<Option<Task>> {
        self.query_opt(
            "SELECT id, scroll_id, name, prompt, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at, peer_name, verify_rubric, verify_threshold, verifier_agent_id
             FROM tasks WHERE agent_id = ?1",
            params![agent_id],
            row_to_task,
        )
    }

    /// Find the task whose in-flight verification is being performed by
    /// `agent_id`. Mirrors `get_task_by_agent_id`, but resolves the
    /// *evaluator* side of a verification-gated task.
    pub fn get_task_by_verifier_agent_id(&self, agent_id: &str) -> Result<Option<Task>> {
        self.query_opt(
            "SELECT id, scroll_id, name, prompt, state, agent_id, provider, model, cwd, file_patterns, order_index, created_at, updated_at, peer_name, verify_rubric, verify_threshold, verifier_agent_id
             FROM tasks WHERE verifier_agent_id = ?1",
            params![agent_id],
            row_to_task,
        )
    }

    /// Record the evaluator agent summoned to verify `task_id`'s worker
    /// transcript. The task itself stays in its current state; the keeper
    /// settles it when the evaluator finishes.
    pub fn set_task_verifier(&self, task_id: &str, agent_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.exec(
            "UPDATE tasks SET verifier_agent_id = ?1, updated_at = ?2 WHERE id = ?3",
            params![agent_id, now, task_id],
        )?;
        Ok(())
    }

    /// Clear a task's verifier link so a re-run's completion re-triggers
    /// verification from scratch. Used by the retry path.
    pub fn clear_task_verifier(&self, task_id: &str) -> Result<()> {
        self.exec(
            "UPDATE tasks SET verifier_agent_id = NULL WHERE id = ?1",
            params![task_id],
        )?;
        Ok(())
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
        // Runnable tasks: those whose dependencies are all complete and that
        // are not yet in flight. A no-dependency task is created `ready`; a
        // dependency-bearing task sits `blocked` until its deps complete and
        // an approved gate flips it back to `ready`. Both states are
        // schedulable here; `awaiting_approval`, `active`, and terminal
        // states are deliberately excluded.
        self.query_vec(
            "SELECT r.id, r.scroll_id, r.name, r.prompt, r.state, r.agent_id, r.provider, r.model, r.cwd, r.file_patterns, r.order_index, r.created_at, r.updated_at
             FROM tasks r
             WHERE r.scroll_id = ?1 AND r.state IN ('blocked', 'ready')
             AND NOT EXISTS (
                 SELECT 1 FROM task_dependencies rd
                 JOIN tasks dep ON dep.id = rd.depends_on_id
                 WHERE rd.task_id = r.id AND dep.state != 'complete'
             )",
            params![scroll_id],
            row_to_task,
        )
    }

    // --- HITL approval + retry directives (DB-only task columns) ----------

    /// Stamp the approval/retry directives parsed from the spec onto a task
    /// row at inscribe time. Called right after `insert_task`.
    pub fn set_task_directives(
        &self,
        task_id: &str,
        requires_approval: bool,
        max_retries: u32,
    ) -> Result<()> {
        self.exec(
            "UPDATE tasks SET requires_approval = ?1, max_retries = ?2 WHERE id = ?3",
            params![i64::from(requires_approval), max_retries as i64, task_id],
        )?;
        Ok(())
    }

    /// `(requires_approval, approval_state)` for a task. Defaults to
    /// `(false, None)` for rows that predate the columns.
    pub fn get_task_approval(
        &self,
        task_id: &str,
    ) -> Result<(bool, crate::shared::types::ApprovalState)> {
        use crate::shared::types::ApprovalState;
        let row = self.query_opt(
            "SELECT requires_approval, approval_state FROM tasks WHERE id = ?1",
            params![task_id],
            |r| {
                let req: i64 = r.get(0)?;
                let state: String = r.get(1)?;
                Ok((req != 0, state))
            },
        )?;
        match row {
            Some((req, state)) => Ok((req, state.parse().unwrap_or(ApprovalState::None))),
            None => Ok((false, ApprovalState::None)),
        }
    }

    /// Set a task's approval state (the human decision or the pending hold).
    pub fn set_task_approval_state(
        &self,
        task_id: &str,
        state: crate::shared::types::ApprovalState,
    ) -> Result<()> {
        self.exec(
            "UPDATE tasks SET approval_state = ?1 WHERE id = ?2",
            params![state.as_str(), task_id],
        )?;
        Ok(())
    }

    /// `(max_retries, retry_count)` for a task. Defaults to `(0, 0)`.
    pub fn get_task_retry(&self, task_id: &str) -> Result<(u32, u32)> {
        let row = self.query_opt(
            "SELECT max_retries, retry_count FROM tasks WHERE id = ?1",
            params![task_id],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )?;
        Ok(row.map_or((0, 0), |(m, c)| (m.max(0) as u32, c.max(0) as u32)))
    }

    /// Increment a task's retry counter, returning the new count.
    pub fn bump_task_retry(&self, task_id: &str) -> Result<u32> {
        let conn = self.conn_lock();
        conn.execute(
            "UPDATE tasks SET retry_count = retry_count + 1 WHERE id = ?1",
            params![task_id],
        )?;
        let n: i64 = conn.query_row(
            "SELECT retry_count FROM tasks WHERE id = ?1",
            params![task_id],
            |r| r.get(0),
        )?;
        Ok(n.max(0) as u32)
    }

    /// Count active tasks in a scroll
    pub fn count_active_tasks(&self, scroll_id: &str) -> Result<usize> {
        let conn = self.conn_lock();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tasks WHERE scroll_id = ?1 AND state = 'active'",
            params![scroll_id],
            |row| row.get(0),
        )?;
        Ok(count as usize)
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
        let mut conn = self.conn_lock();
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
        let mut conn = self.conn_lock();
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
        let conn = self.conn_lock();
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

    /// Number of rows currently in `task_queue`.
    pub fn count_queued(&self) -> Result<usize> {
        let conn = self.conn_lock();
        let n: i64 = conn.query_row("SELECT COUNT(*) FROM task_queue", [], |r| r.get(0))?;
        Ok(n as usize)
    }

    /// Number of agents currently mid-flight (Active or Summoning), the
    /// scheduler's `in_flight` count for capacity decisions.
    pub fn count_in_flight_agents(&self) -> Result<usize> {
        let conn = self.conn_lock();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM agents WHERE state IN ('active', 'summoning')",
            [],
            |r| r.get(0),
        )?;
        Ok(n as usize)
    }

    /// Insert one rubric-scored evaluation row, returning its id.
    pub fn insert_eval_result(
        &self,
        target_id: &str,
        evaluator_id: &str,
        score: f64,
        verdict: Option<&str>,
        rationale: Option<&str>,
    ) -> Result<String> {
        let id = crate::shared::constants::generate_short_id();
        let now = chrono::Utc::now().timestamp();
        let conn = self.conn_lock();
        conn.execute(
            "INSERT INTO eval_results(id, target_id, evaluator_id, score, verdict, rationale, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![id, target_id, evaluator_id, score, verdict, rationale, now],
        )?;
        Ok(id)
    }

    /// All evals for `target_id`, newest first.
    pub fn list_eval_results(&self, target_id: &str) -> Result<Vec<EvalResultRow>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare(
            "SELECT id, target_id, evaluator_id, score, verdict, rationale, created_at \
             FROM eval_results WHERE target_id = ?1 ORDER BY created_at DESC",
        )?;
        let rows = stmt
            .query_map(params![target_id], |r| {
                Ok(EvalResultRow {
                    id: r.get(0)?,
                    target_id: r.get(1)?,
                    evaluator_id: r.get(2)?,
                    score: r.get(3)?,
                    verdict: r.get(4)?,
                    rationale: r.get(5)?,
                    created_at: r.get(6)?,
                })
            })?
            .filter_map(Result::ok)
            .collect();
        Ok(rows)
    }

    /// Latest score per target across all evaluated agents. Lets `grim circle
    /// --eval-score-lt` filter in one trip without an N+1 fanout.
    pub fn latest_eval_scores_all(&self) -> Result<Vec<(String, f64)>> {
        let conn = self.conn_lock();
        let mut stmt = conn.prepare(
            "SELECT target_id, score FROM eval_results er WHERE created_at = (\
                SELECT MAX(created_at) FROM eval_results WHERE target_id = er.target_id)",
        )?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    /// Highest score recorded for `target_id`, used by `grim circle --eval`
    /// to surface a single representative number per agent.
    pub fn latest_eval_score(&self, target_id: &str) -> Result<Option<f64>> {
        let conn = self.conn_lock();
        let row: Option<f64> = conn
            .query_row(
                "SELECT score FROM eval_results WHERE target_id = ?1 \
                 ORDER BY created_at DESC LIMIT 1",
                params![target_id],
                |r| r.get(0),
            )
            .ok();
        Ok(row)
    }

    /// Increment `budget_spend.usd` for `(budget_name, day)` by `usd`,
    /// inserting the row on first write. Returns the new running total
    /// for that day.
    pub fn add_budget_spend(&self, budget_name: &str, day: &str, usd: f64) -> Result<f64> {
        if usd <= 0.0 {
            return self.get_budget_spend(budget_name, day);
        }
        let conn = self.conn_lock();
        conn.execute(
            "INSERT INTO budget_spend(budget_name, day, usd) VALUES (?1, ?2, ?3) \
             ON CONFLICT(budget_name, day) DO UPDATE SET usd = usd + excluded.usd",
            params![budget_name, day, usd],
        )?;
        let total: f64 = conn.query_row(
            "SELECT usd FROM budget_spend WHERE budget_name = ?1 AND day = ?2",
            params![budget_name, day],
            |r| r.get(0),
        )?;
        Ok(total.max(0.0))
    }

    pub fn get_budget_spend(&self, budget_name: &str, day: &str) -> Result<f64> {
        let conn = self.conn_lock();
        let total: f64 = conn
            .query_row(
                "SELECT usd FROM budget_spend WHERE budget_name = ?1 AND day = ?2",
                params![budget_name, day],
                |r| r.get(0),
            )
            .unwrap_or(0.0);
        Ok(total.max(0.0))
    }
}
