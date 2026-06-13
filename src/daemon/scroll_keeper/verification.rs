//! Verification gating: summon an evaluator to score a rubric-bearing
//! task's worker transcript, then settle the task on the verdict.

use std::path::PathBuf;
use tracing::{error, info, warn};

use crate::shared::types::Task;

use super::{DEFAULT_VERIFY_THRESHOLD, ScrollKeeper};

impl ScrollKeeper {
    /// The worker for a rubric-bearing task just completed: summon an
    /// evaluator agent to score the worker's transcript. The task stays
    /// in its current (non-terminal) state until the verdict arrives.
    pub(super) async fn start_verification(&self, task: &Task, worker_agent_id: &str) {
        let rubric = task.verify_rubric.clone().unwrap_or_default();

        let events = self
            .db
            .read_stream_events(worker_agent_id)
            .unwrap_or_else(|e| {
                warn!(agent_id = %worker_agent_id, error = %e,
                    "Failed to read worker events; verifying against an empty transcript");
                Vec::new()
            });
        let max_seq = events.last().map_or(0, |e| e.seq);
        let transcript = crate::shared::eval::fold_stdout_output(events.iter().map(|e| &e.event));
        let prompt =
            crate::shared::eval::build_eval_prompt(worker_agent_id, max_seq, &rubric, &transcript);

        let cwd = self
            .manager
            .resolve_cwd(task.cwd.as_ref().map(PathBuf::from));
        match self
            .manager
            .enqueue(
                &prompt,
                Some(format!("verify:{}", task.name)),
                task.model.clone(),
                task.provider.clone(),
                &cwd,
                crate::daemon::agent_manager::Lane::Adhoc,
            )
            .await
        {
            Ok(evaluator) => {
                if let Err(e) = self.db.set_task_verifier(&task.id, &evaluator.id) {
                    error!(task_id = %task.id, error = %e,
                        "Failed to record verifier agent; failing task instead of leaving it hung");
                    self.fail_task_and_advance(task).await;
                    return;
                }
                info!(
                    scroll_id = %task.scroll_id,
                    task = %task.name,
                    worker = %worker_agent_id,
                    verifier = %evaluator.id,
                    "Worker finished; evaluator summoned to verify against rubric"
                );
            }
            Err(e) => {
                warn!(
                    scroll_id = %task.scroll_id,
                    task = %task.name,
                    error = %e,
                    "Failed to enqueue verifier agent; treating verification as failed"
                );
                self.fail_task_and_advance(task).await;
            }
        }
    }

    /// An evaluator agent finished: parse its verdict and settle the
    /// task it was verifying. A missing or unparsable verdict counts
    /// as a failed verification — the gate must never silently pass.
    pub(super) async fn finish_verification(&self, task: &Task, evaluator_id: &str) {
        let threshold = task.verify_threshold.unwrap_or(DEFAULT_VERIFY_THRESHOLD);

        let verdict = self
            .manager
            .agent_result(evaluator_id)
            .ok_or_else(|| anyhow::anyhow!("verifier produced no result text"))
            .and_then(|text| crate::shared::eval::parse_verdict(&text));

        let verdict = match verdict {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    task = %task.name,
                    verifier = %evaluator_id,
                    error = %e,
                    "No usable verdict from verifier; verification fails"
                );
                self.fail_task_and_advance(task).await;
                return;
            }
        };

        // Record the verdict so `grim eval <worker> --list` shows scroll
        // verifications alongside manual evals.
        let target_id = task.agent_id.clone().unwrap_or_else(|| task.id.clone());
        if let Err(e) = self.db.insert_eval_result(
            &target_id,
            evaluator_id,
            verdict.score,
            verdict.verdict.as_deref(),
            verdict.rationale.as_deref(),
        ) {
            warn!(task = %task.name, error = %e, "Failed to persist verification verdict");
        }

        if verdict.score >= threshold {
            info!(
                scroll_id = %task.scroll_id,
                task = %task.name,
                verifier = %evaluator_id,
                score = verdict.score,
                threshold,
                "Verification passed"
            );
            self.complete_task_and_advance(task).await;
        } else {
            warn!(
                scroll_id = %task.scroll_id,
                task = %task.name,
                verifier = %evaluator_id,
                score = verdict.score,
                threshold,
                "Verification failed: score below threshold"
            );
            self.retry_or_fail(task).await;
        }
    }
}
