//! `Supervisor` — daemon-internal actor that owns restart-policy evaluation,
//! a windowed budget per agent, a global rate counter, and a tree-depth cap.
//!
//! Responsibilities:
//! - Subscribe to `StateChange { new_state: Failed }` events on the bus.
//! - For agents with an active restart policy, decide whether to schedule a
//!   restart, escalate (when budget is exhausted and `escalate_to` is set),
//!   or no-op.
//! - Maintain a min-heap of `PendingRestart { agent_id, attempt, fire_at }`
//!   so the scheduler's `tick_supervision()` can drain due entries.
//! - Persist every decision to `restart_history` for windowed budget
//!   evaluation and audit.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::daemon::clock::Clock;
use crate::daemon::event_bus::EventBus;
use crate::daemon::persistence::Database;
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{
    AgentId, AgentState, RestartHistoryOutcome, RestartPolicy,
};

/// 16 KiB body cap for escalation mail (matches `WAKE_FOLD_MAX_BYTES`).
const ESCALATION_BODY_MAX_BYTES: usize = 16_384;

/// Fixed delay between `Failed` decision and restart fire.
const RESTART_DELAY_SECS: i64 = 2;

/// Delay applied when the global rate cap denies a restart.
const RATE_LIMITED_DELAY_SECS: i64 = 60;

/// Pending entry in the supervisor's min-heap.
#[derive(Debug, Clone)]
pub struct PendingRestart {
    pub agent_id: AgentId,
    pub attempt: u32,
    pub fire_at: DateTime<Utc>,
}

impl PartialEq for PendingRestart {
    fn eq(&self, other: &Self) -> bool {
        self.fire_at == other.fire_at && self.agent_id == other.agent_id
    }
}
impl Eq for PendingRestart {}
impl PartialOrd for PendingRestart {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PendingRestart {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.fire_at
            .cmp(&other.fire_at)
            .then_with(|| self.agent_id.cmp(&other.agent_id))
    }
}

/// Output of policy evaluation.
#[derive(Debug, Clone)]
pub enum RestartDecision {
    Restart {
        attempt: u32,
        fire_at: DateTime<Utc>,
        rate_limited: bool,
    },
    BudgetExhausted {
        reason: &'static str, // "budget_spent" | "tree_depth_exceeded"
    },
    NotSupervised,
}

/// Token-bucket rate counter (in-memory only).
pub struct RateCounter {
    tokens: f64,
    last: DateTime<Utc>,
    capacity: f64,
    refill_per_sec: f64,
}

impl RateCounter {
    pub fn new(per_min: u32, now: DateTime<Utc>) -> Self {
        let cap = per_min.max(1) as f64;
        Self {
            tokens: cap,
            last: now,
            capacity: cap,
            refill_per_sec: cap / 60.0,
        }
    }

    /// Try to consume one token. Returns `true` on accept.
    pub fn try_consume(&mut self, now: DateTime<Utc>) -> bool {
        let elapsed = (now - self.last).num_milliseconds().max(0) as f64 / 1000.0;
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Sender seam for escalation mail. Default impl writes mail rows directly.
#[async_trait]
pub trait EscalationMailSender: Send + Sync {
    /// Returns the number of mail rows written (`fanout_count`) and a vector
    /// of recipient agent IDs whose `escalation_depth` should be propagated.
    async fn send_escalation(
        &self,
        sender_id: &str,
        target: &str,
        body: &str,
    ) -> Result<EscalationOutcome>;
}

/// Result of `EscalationMailSender::send_escalation`.
#[derive(Debug, Default, Clone)]
pub struct EscalationOutcome {
    pub fanout_count: u32,
    pub recipient_ids: Vec<AgentId>,
}

/// Default `EscalationMailSender` — writes one mail row per recipient with
/// `sender_id = "supervisor://<failed-agent-id>"`.
pub struct DbEscalationMailSender {
    pub db: Arc<Database>,
    pub bus: EventBus,
}

#[async_trait]
impl EscalationMailSender for DbEscalationMailSender {
    async fn send_escalation(
        &self,
        sender_id: &str,
        target: &str,
        body: &str,
    ) -> Result<EscalationOutcome> {
        use crate::shared::mail::{Address, parse_address};
        use crate::shared::types::{Mail, MailState};

        let now = crate::daemon::persistence::unix_now();
        let address = parse_address(target)
            .map_err(|e| anyhow::anyhow!("invalid escalate_to: {}", e))?;

        match address {
            Address::Agent(recipient_id) => {
                let mail_id = format!(
                    "em{}",
                    &crate::shared::constants::generate_short_id()[..6]
                );
                let mail = Mail {
                    id: mail_id.clone(),
                    recipient_id: recipient_id.clone(),
                    sender_id: Some(sender_id.to_string()),
                    topic: None,
                    body: body.to_string(),
                    in_reply_to: None,
                    state: MailState::Pending,
                    fail_reason: None,
                    created_at: now,
                    delivered_at: None,
                    seq: 0,
                    wake_eligible: true,
                };
                self.db.insert_mail(&mail)?;
                self.bus.publish(StreamEvent::MailReceived {
                    mail_id: mail_id.clone(),
                    recipient_id: recipient_id.clone(),
                    sender_id: Some(sender_id.to_string()),
                    topic: None,
                    body_preview: body.chars().take(200).collect(),
                    wake_eligible: true,
                    origin_daemon_id: None,
                });
                Ok(EscalationOutcome {
                    fanout_count: 1,
                    recipient_ids: vec![recipient_id],
                })
            }
            Address::Topic(topic) => {
                let subs = self.db.list_subscribers_for_topic(&topic)?;
                let mut mails: Vec<Mail> = Vec::with_capacity(subs.len());
                for sub in &subs {
                    let mail_id = format!(
                        "em{}",
                        &crate::shared::constants::generate_short_id()[..6]
                    );
                    mails.push(Mail {
                        id: mail_id,
                        recipient_id: sub.subscriber_id.clone(),
                        sender_id: Some(sender_id.to_string()),
                        topic: Some(topic.clone()),
                        body: body.to_string(),
                        in_reply_to: None,
                        state: MailState::Pending,
                        fail_reason: None,
                        created_at: now,
                        delivered_at: None,
                        seq: 0,
                        wake_eligible: true,
                    });
                }
                self.db.insert_mail_batch(&mails)?;
                let recipients: Vec<AgentId> =
                    subs.iter().map(|s| s.subscriber_id.clone()).collect();
                for m in &mails {
                    self.bus.publish(StreamEvent::MailReceived {
                        mail_id: m.id.clone(),
                        recipient_id: m.recipient_id.clone(),
                        sender_id: Some(sender_id.to_string()),
                        topic: Some(topic.clone()),
                        body_preview: body.chars().take(200).collect(),
                        wake_eligible: true,
                        origin_daemon_id: None,
                    });
                }
                Ok(EscalationOutcome {
                    fanout_count: mails.len() as u32,
                    recipient_ids: recipients,
                })
            }
            Address::FederatedAgent { .. } => {
                // Supervisor escalation does not yet cross daemons. Surface
                // a clear error so operators can re-target locally.
                anyhow::bail!("escalate_to_federated_unsupported")
            }
        }
    }
}

/// Restart-dispatch seam for the scheduler's `tick_supervision()` step.
#[async_trait]
pub trait RestartDispatcher: Send + Sync {
    async fn restart_dispatch(&self, agent_id: &str, attempt: u32) -> Result<()>;
}

pub struct Supervisor {
    db: Arc<Database>,
    bus: EventBus,
    clock: Arc<dyn Clock>,
    pending: Mutex<BinaryHeap<Reverse<PendingRestart>>>,
    global_rate: Mutex<RateCounter>,
    tree_depth_cap: u32,
    mail_sender: Arc<dyn EscalationMailSender>,
}

impl Supervisor {
    pub fn new(
        db: Arc<Database>,
        bus: EventBus,
        clock: Arc<dyn Clock>,
        restart_rate_per_min: u32,
        tree_depth_cap: u32,
        mail_sender: Arc<dyn EscalationMailSender>,
    ) -> Arc<Self> {
        let now = clock.now();
        Arc::new(Self {
            db,
            bus,
            clock,
            pending: Mutex::new(BinaryHeap::new()),
            global_rate: Mutex::new(RateCounter::new(restart_rate_per_min, now)),
            tree_depth_cap,
            mail_sender,
        })
    }

    /// Convenience constructor wiring the default mail sender.
    pub fn with_default_sender(
        db: Arc<Database>,
        bus: EventBus,
        clock: Arc<dyn Clock>,
        restart_rate_per_min: u32,
        tree_depth_cap: u32,
    ) -> Arc<Self> {
        let sender: Arc<dyn EscalationMailSender> = Arc::new(DbEscalationMailSender {
            db: db.clone(),
            bus: bus.clone(),
        });
        Self::new(
            db,
            bus,
            clock,
            restart_rate_per_min,
            tree_depth_cap,
            sender,
        )
    }

    /// Subscribe to the bus and route `Failed` state changes to
    /// `on_state_change`. Returns the spawned task handle.
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        let mut rx = self.bus.subscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(StreamEvent::StateChange {
                        agent_id,
                        new_state,
                        ..
                    }) => {
                        if new_state.is_supervisable()
                            && let Err(e) = me.on_state_change(&agent_id, new_state).await
                        {
                            warn!(agent_id = %agent_id, error = %e, "supervisor on_state_change failed");
                        }
                    }
                    Ok(_) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(missed = n, "supervisor missed broadcast events");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    /// Handle a `Failed` event for `agent_id`.
    pub async fn on_state_change(
        self: &Arc<Self>,
        agent_id: &str,
        new_state: AgentState,
    ) -> Result<()> {
        if !new_state.is_supervisable() {
            return Ok(());
        }
        // Idempotency: if the agent is already pending, drop.
        {
            let pending = self.pending.lock().await;
            if pending.iter().any(|p| p.0.agent_id == agent_id) {
                debug!(agent_id = %agent_id, "supervisor: duplicate Failed for already-pending agent");
                return Ok(());
            }
        }

        let decision = self.evaluate(agent_id).await?;
        match decision {
            RestartDecision::NotSupervised => {
                debug!(agent_id = %agent_id, "supervisor: agent not supervised");
                Ok(())
            }
            RestartDecision::Restart {
                attempt,
                fire_at,
                rate_limited,
            } => {
                self.schedule_restart(agent_id, attempt, fire_at, rate_limited)
                    .await
            }
            RestartDecision::BudgetExhausted { reason } => {
                self.handle_budget_exhausted(agent_id, reason).await
            }
        }
    }

    /// Evaluate restart policy for `agent_id` and return the decision.
    pub async fn evaluate(self: &Arc<Self>, agent_id: &str) -> Result<RestartDecision> {
        let cfg = match self.db.get_supervision(agent_id)? {
            Some(c) => c,
            None => return Ok(RestartDecision::NotSupervised),
        };
        if cfg.policy == RestartPolicy::Never {
            return Ok(RestartDecision::NotSupervised);
        }

        // Tree-depth check FIRST — it overrides budget.
        let depth = self.db.get_escalation_depth(agent_id).unwrap_or(0);
        if depth + 1 > self.tree_depth_cap {
            return Ok(RestartDecision::BudgetExhausted {
                reason: "tree_depth_exceeded",
            });
        }

        let max_restarts = cfg.max_restarts.unwrap_or(0);
        let window_secs = cfg.window_secs.unwrap_or(0) as i64;
        let now = self.clock.now();
        let window_start = now.timestamp() - window_secs;
        let count = self
            .db
            .count_restarts_in_window(agent_id, window_start)
            .unwrap_or(0);
        if count >= max_restarts {
            return Ok(RestartDecision::BudgetExhausted {
                reason: "budget_spent",
            });
        }

        let attempt = count + 1;
        let allowed = {
            let mut rc = self.global_rate.lock().await;
            rc.try_consume(now)
        };
        let (fire_at, rate_limited) = if allowed {
            (now + chrono::Duration::seconds(RESTART_DELAY_SECS), false)
        } else {
            (
                now + chrono::Duration::seconds(RATE_LIMITED_DELAY_SECS),
                true,
            )
        };
        Ok(RestartDecision::Restart {
            attempt,
            fire_at,
            rate_limited,
        })
    }

    /// Persist scheduling, flip state to Restarting, push onto the heap,
    /// publish RestartScheduled.
    pub async fn schedule_restart(
        self: &Arc<Self>,
        agent_id: &str,
        attempt: u32,
        fire_at: DateTime<Utc>,
        rate_limited: bool,
    ) -> Result<()> {
        let cfg = self.db.get_supervision(agent_id)?;
        let max = cfg
            .as_ref()
            .and_then(|c| c.max_restarts)
            .unwrap_or(0);

        let error_summary = self.last_error_summary(agent_id);
        let now = self.clock.now().timestamp();
        self.db.insert_restart_history_row(
            agent_id,
            now,
            RestartHistoryOutcome::Scheduled,
            error_summary.as_deref(),
        )?;

        // Flip state to Restarting (prior state is Failed by precondition).
        self.db
            .update_agent_state(agent_id, &AgentState::Restarting, None)?;
        self.bus.publish(StreamEvent::StateChange {
            agent_id: agent_id.to_string(),
            old_state: AgentState::Failed,
            new_state: AgentState::Restarting,
        });

        {
            let mut pending = self.pending.lock().await;
            pending.push(Reverse(PendingRestart {
                agent_id: agent_id.to_string(),
                attempt,
                fire_at,
            }));
        }

        self.bus.publish(StreamEvent::RestartScheduled {
            agent_id: agent_id.to_string(),
            attempt,
            max,
            fire_at_unix: fire_at.timestamp(),
            rate_limited,
        });
        Ok(())
    }

    /// Cancel all pending entries for `agent_id`. Returns the count cancelled.
    pub async fn cancel_pending(self: &Arc<Self>, agent_id: &str) -> Result<usize> {
        let mut pending = self.pending.lock().await;
        let total = pending.len();
        let kept: BinaryHeap<Reverse<PendingRestart>> = pending
            .drain()
            .filter(|p| p.0.agent_id != agent_id)
            .collect();
        let removed = total - kept.len();
        *pending = kept;
        Ok(removed)
    }

    /// Pop and return all entries with `fire_at <= now`.
    pub async fn drain_due(self: &Arc<Self>, now: DateTime<Utc>) -> Vec<PendingRestart> {
        let mut out = Vec::new();
        let mut pending = self.pending.lock().await;
        while let Some(top) = pending.peek() {
            if top.0.fire_at <= now {
                let Reverse(p) = pending.pop().unwrap();
                out.push(p);
            } else {
                break;
            }
        }
        out
    }

    /// Push an entry back onto the heap (used by the scheduler when capacity
    /// is full).
    pub async fn requeue(self: &Arc<Self>, entry: PendingRestart) {
        let mut pending = self.pending.lock().await;
        pending.push(Reverse(entry));
    }

    /// Snapshot of pending entries (for tests / observability).
    pub async fn pending_snapshot(self: &Arc<Self>) -> Vec<PendingRestart> {
        let pending = self.pending.lock().await;
        pending.iter().map(|p| p.0.clone()).collect()
    }

    /// Boot replay: promote any torn `Restarting` rows to `Failed`, then
    /// re-evaluate every `Failed` agent with active policy.
    pub async fn replay_pending_on_boot(self: &Arc<Self>) -> Result<()> {
        let torn = self.db.mark_torn_restarting_as_failed().unwrap_or_default();
        for id in &torn {
            self.bus.publish(StreamEvent::StateChange {
                agent_id: id.clone(),
                old_state: AgentState::Restarting,
                new_state: AgentState::Failed,
            });
        }

        let candidates = self
            .db
            .list_failed_with_active_policy()
            .unwrap_or_default();
        for id in candidates {
            // Skip if there's an Escalated event newer than the latest
            // restart_history row — we already escalated this agent.
            if self
                .db
                .has_escalated_event_after_latest_history(&id)
                .unwrap_or(false)
            {
                debug!(agent_id = %id, "supervisor: boot-skip already-escalated agent");
                continue;
            }
            let decision = self.evaluate(&id).await?;
            match decision {
                RestartDecision::Restart { attempt, .. } => {
                    // Use now (immediate) — the original window has elapsed.
                    let now = self.clock.now();
                    self.schedule_restart(&id, attempt, now, false).await?;
                }
                RestartDecision::BudgetExhausted { reason } => {
                    self.handle_budget_exhausted(&id, reason).await?;
                }
                RestartDecision::NotSupervised => {}
            }
        }
        Ok(())
    }

    async fn handle_budget_exhausted(
        self: &Arc<Self>,
        agent_id: &str,
        reason: &'static str,
    ) -> Result<()> {
        let now = self.clock.now().timestamp();
        let error_summary = self.last_error_summary(agent_id);
        self.db.insert_restart_history_row(
            agent_id,
            now,
            RestartHistoryOutcome::BudgetExhausted,
            error_summary.as_deref(),
        )?;
        self.bus.publish(StreamEvent::RestartBudgetExhausted {
            agent_id: agent_id.to_string(),
            reason: reason.to_string(),
        });

        // Tree-depth-exceeded does NOT escalate (point is to stop).
        if reason == "tree_depth_exceeded" {
            return Ok(());
        }

        // Try to escalate if configured.
        #[allow(clippy::collapsible_if)]
        if let Some(cfg) = self.db.get_supervision(agent_id)? {
            if let Some(target) = cfg.escalate_to {
                let summary = error_summary
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                let mut body = format!(
                    "[supervisor] agent {} failed (budget exhausted): {}",
                    agent_id, summary
                );
                if body.len() > ESCALATION_BODY_MAX_BYTES {
                    body.truncate(ESCALATION_BODY_MAX_BYTES);
                }
                let sender_id = format!("supervisor://{}", agent_id);
                match self
                    .mail_sender
                    .send_escalation(&sender_id, &target, &body)
                    .await
                {
                    Ok(outcome) => {
                        let depth =
                            self.db.get_escalation_depth(agent_id).unwrap_or(0);
                        for rid in &outcome.recipient_ids {
                            let cur = self.db.get_escalation_depth(rid).unwrap_or(0);
                            let new_depth = cur.max(depth + 1);
                            let _ = self.db.set_escalation_depth(rid, new_depth);
                        }
                        self.bus.publish(StreamEvent::Escalated {
                            agent_id: agent_id.to_string(),
                            target,
                            fanout_count: outcome.fanout_count,
                        });
                    }
                    Err(e) => {
                        warn!(agent_id = %agent_id, error = %e, "supervisor escalation send failed");
                    }
                }
            }
        }
        Ok(())
    }

    /// Best-effort summary of the agent's last error: read recent
    /// agent_events looking for a stderr-style entry, fall back to None.
    fn last_error_summary(&self, agent_id: &str) -> Option<String> {
        // Cheap heuristic — get the agent's last few events and grep for any
        // stderr line. If none, return None and callers fall back to
        // "unknown".
        let evts = self.db.get_events(agent_id, Some(20)).ok()?;
        for e in evts.iter().rev() {
            if e.event_type == "stderr" && !e.payload.is_empty() {
                return Some(e.payload.chars().take(200).collect());
            }
        }
        None
    }
}
