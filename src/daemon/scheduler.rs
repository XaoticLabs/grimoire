//! Daemon-owned scheduler that promotes `Queued` agents to `Active` while
//! global capacity allows and an eligible worker exists. The scheduler is the
//! single caller of the dispatch path; `agent_manager::enqueue` only inserts
//! work, the scheduler decides when it actually starts.
//!
//! The reactor wakes on:
//!   * `StateChange` events whose `new_state` is terminal (slot freed),
//!   * `AgentQueued` events (new work arrived),
//!   * `WorkerRegistered` events (a previously-blocked task may now fit),
//!   * a 100ms periodic tick (safety net for any signal we missed).
//!
//! Tests drive the scheduler via [`Scheduler::tick_now`] without spawning the
//! background task — see `tests/scheduler.rs`.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use semver::VersionReq;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

use crate::daemon::event_bus::EventBus;
use crate::daemon::persistence::{Database, QueueRow};
use crate::daemon::supervisor::{RestartDispatcher, Supervisor};
use crate::daemon::worker_registry::WorkerRegistry;
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{AgentState, MailState};

/// Maximum bytes for the folded wake prompt. Single mail bodies up to 64 KiB
/// are accepted by `mail.send`; the wake-fold cap is intentionally tighter so
/// resume prompts stay manageable.
const WAKE_FOLD_MAX_BYTES: usize = 16_384;

const TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Seam for the scheduler to call back into `AgentManager::dispatch_internal`.
/// Tests substitute a fake.
#[async_trait]
pub trait Dispatcher: Send + Sync {
    async fn dispatch(&self, row: QueueRow) -> Result<()>;
}

/// Seam for the scheduler to wake a `Complete` agent on incoming mail.
/// The implementation calls `AgentManager::invoke()`; tests substitute a
/// recording fake.
#[async_trait]
pub trait MailWaker: Send + Sync {
    async fn wake(&self, agent_id: &str, prompt: &str) -> Result<()>;
}

/// Get-only access to an agent's runtime state for the mail-wake check.
/// Tests can substitute a fake; production wires this to the database.
pub trait AgentStateLookup: Send + Sync {
    fn get_state_and_session(&self, id: &str) -> Result<Option<(AgentState, Option<String>)>>;
}

/// Default implementation backed by the database.
pub struct DbStateLookup {
    pub db: Arc<Database>,
}

impl AgentStateLookup for DbStateLookup {
    fn get_state_and_session(&self, id: &str) -> Result<Option<(AgentState, Option<String>)>> {
        Ok(self.db.get_agent(id)?.map(|a| (a.state, a.session_id)))
    }
}

/// The scheduler. Construct via [`Scheduler::new`], then either drive ticks
/// manually with [`Scheduler::tick_now`] (tests) or spawn the background
/// reactor via [`Scheduler::spawn`] (daemon boot).
pub struct Scheduler {
    db: Arc<Database>,
    workers: Arc<WorkerRegistry>,
    bus: EventBus,
    cap: Arc<AtomicU32>,
    dispatcher: Arc<dyn Dispatcher>,
    tick_lock: Mutex<()>,
    mail_waker: Option<Arc<dyn MailWaker>>,
    state_lookup: Option<Arc<dyn AgentStateLookup>>,
    supervisor: Option<Arc<Supervisor>>,
    restart_dispatcher: Option<Arc<dyn RestartDispatcher>>,
    /// Providers the daemon's own `LocalExecutor` can run. The eligibility
    /// check waives the "must have a remote worker" requirement for these,
    /// so a local-only daemon (no federated workers) can dispatch.
    local_providers: HashSet<String>,
}

impl Scheduler {
    pub fn new(
        db: Arc<Database>,
        workers: Arc<WorkerRegistry>,
        bus: EventBus,
        cap: Arc<AtomicU32>,
        dispatcher: Arc<dyn Dispatcher>,
    ) -> Self {
        Self {
            db,
            workers,
            bus,
            cap,
            dispatcher,
            tick_lock: Mutex::new(()),
            mail_waker: None,
            state_lookup: None,
            supervisor: None,
            restart_dispatcher: None,
            local_providers: HashSet::new(),
        }
    }

    /// Declare providers handled by the daemon's in-process `LocalExecutor`.
    /// Dispatch for these will not be gated on a registered remote worker.
    /// When the local executor and a matching remote worker both exist, the
    /// dispatcher (AgentManager) currently routes locally — federated
    /// placement is a separate concern owned by the executor layer.
    #[must_use]
    pub fn with_local_providers<I, S>(mut self, providers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.local_providers = providers.into_iter().map(Into::into).collect();
        self
    }

    /// Wire mail-wake. When both a waker and a state lookup are set, every
    /// `tick_now()` call inspects pending wake-eligible mail and invokes the
    /// recipient (if Complete with a session_id) up to the global cap.
    #[must_use]
    pub fn with_mail_wake(
        mut self,
        waker: Arc<dyn MailWaker>,
        lookup: Arc<dyn AgentStateLookup>,
    ) -> Self {
        self.mail_waker = Some(waker);
        self.state_lookup = Some(lookup);
        self
    }

    /// Wire the supervisor + restart dispatcher. When set, every `tick_now()`
    /// pulls due restarts from the supervisor's pending heap.
    #[must_use]
    pub fn with_supervision(
        mut self,
        supervisor: Arc<Supervisor>,
        dispatcher: Arc<dyn RestartDispatcher>,
    ) -> Self {
        self.supervisor = Some(supervisor);
        self.restart_dispatcher = Some(dispatcher);
        self
    }

    /// Run one full tick: dispatch as many queued rows as capacity and
    /// eligibility allow, then return. Re-entrant calls serialize via an
    /// internal mutex so two wake-up signals can't interleave their claims.
    pub async fn tick_now(&self) -> Result<()> {
        let _guard = self.tick_lock.lock().await;

        let mut in_flight = self.db.count_in_flight_agents()?;
        let cap = self.cap.load(Ordering::Relaxed) as usize;

        if cap == 0 {
            return Ok(());
        }

        // Mail-wake branch runs before the queue dispatch loop so a wake
        // and a queued dispatch can't race for the same slot.
        in_flight = self.tick_mail_wake(in_flight, cap).await?;
        if in_flight >= cap {
            return Ok(());
        }

        // Supervision branch: drain due pending restarts.
        in_flight = self.tick_supervision(in_flight, cap).await?;
        if in_flight >= cap {
            return Ok(());
        }

        // We iterate `list_queue()` (already in dispatch order) rather than
        // calling `peek_next_dispatch()` repeatedly so we can skip rows that
        // have no eligible worker without losing place in the line.
        let rows = self.db.list_queue()?;

        for row in rows {
            if in_flight >= cap {
                // Mark the rest as capacity-blocked for visibility in
                // `grim queue`. Cosmetic — scheduler will revisit them on
                // the next signal.
                if row.block_reason.as_deref() != Some("capacity")
                    && let Err(e) = self.db.set_block_reason(&row.id, Some("capacity"))
                {
                    warn!(agent_id = %row.id, error = %e, "failed to set capacity block_reason");
                }
                continue;
            }

            // Eligibility peek: provider_name=None means "any worker" — skip
            // the check entirely and let dispatch_internal pick. When set,
            // require at least one registered worker that advertises it,
            // OR that the provider is listed in `local_providers` (the
            // daemon's in-process LocalExecutor handles it).
            if let Some(provider) = row.provider_name.as_deref()
                && !self.local_providers.contains(provider)
                && !self
                    .workers
                    .has_eligible_worker(provider, &VersionReq::STAR)
            {
                if row.block_reason.as_deref() != Some("no_eligible_worker")
                    && let Err(e) = self
                        .db
                        .set_block_reason(&row.id, Some("no_eligible_worker"))
                {
                    warn!(agent_id = %row.id, error = %e, "failed to set no_eligible_worker block_reason");
                }
                continue;
            }

            // Atomically claim: deletes the queue row and flips agents.state
            // to `summoning` in one transaction. `false` means someone else
            // already claimed (e.g., banish raced us).
            match self.db.claim_for_dispatch(&row.id) {
                Ok(true) => {}
                Ok(false) => {
                    debug!(agent_id = %row.id, "queue row vanished mid-tick (raced)");
                    continue;
                }
                Err(e) => {
                    error!(agent_id = %row.id, error = %e, "claim_for_dispatch failed");
                    continue;
                }
            }

            // Hand off to dispatcher. We pass the row by value (the caller no
            // longer owns it after the claim succeeded — the queue row is
            // gone). On success the dispatcher is responsible for moving the
            // agent through `Summoning -> Active`. On failure we requeue with
            // the original `enqueued_at` so fairness is preserved, then break
            // to avoid a tight failure loop on this tick.
            match self.dispatcher.dispatch(row.clone()).await {
                Ok(()) => {
                    in_flight += 1;
                }
                Err(e) => {
                    warn!(agent_id = %row.id, error = %e, "dispatch failed; requeuing");
                    let mut rollback = row.clone();
                    rollback.block_reason = None;
                    if let Err(req_err) = self.db.requeue(&rollback) {
                        error!(agent_id = %rollback.id, error = %req_err, "requeue failed");
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    /// Spawn the background reactor: subscribe to the event bus, also wake
    /// every 100ms as a safety net, and call `tick_now()` on each signal.
    /// The returned handle owns the task; dropping the handle aborts it.
    pub fn spawn(self: Arc<Self>) -> SchedulerHandle {
        let mut rx = self.bus.subscribe();
        let sched = self;
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK_INTERVAL);
            // Skip the immediate fire so a quiet boot doesn't spin a tick
            // before the daemon has anything queued.
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = sched.tick_now().await {
                            error!(error = %e, "scheduler tick failed");
                        }
                    }
                    msg = rx.recv() => {
                        match msg {
                            Ok(event) => {
                                if Self::should_wake(&event)
                                    && let Err(e) = sched.tick_now().await
                                {
                                    error!(error = %e, "scheduler tick failed");
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(missed = n, "scheduler missed broadcast events; ticking anyway");
                                if let Err(e) = sched.tick_now().await {
                                    error!(error = %e, "scheduler tick failed");
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        });

        SchedulerHandle { handle }
    }

    pub const fn should_wake(event: &StreamEvent) -> bool {
        match event {
            StreamEvent::AgentQueued { .. }
            | StreamEvent::WorkerRegistered { .. }
            | StreamEvent::MailReceived { .. }
            | StreamEvent::RestartScheduled { .. } => true,
            StreamEvent::StateChange { new_state, .. } => new_state.is_terminal(),
            _ => false,
        }
    }

    /// Drain due pending restarts and dispatch them under the cap. Returns
    /// the updated in-flight count.
    async fn tick_supervision(&self, mut in_flight: usize, cap: usize) -> Result<usize> {
        let (Some(sup), Some(dispatcher)) = (&self.supervisor, &self.restart_dispatcher) else {
            return Ok(in_flight);
        };
        if in_flight >= cap {
            return Ok(in_flight);
        }
        let now = chrono::Utc::now();
        let due = sup.drain_due(now).await;
        for entry in due {
            if in_flight >= cap {
                sup.requeue(entry).await;
                continue;
            }
            match dispatcher
                .restart_dispatch(&entry.agent_id, entry.attempt)
                .await
            {
                Ok(()) => {
                    in_flight += 1;
                }
                Err(e) => {
                    warn!(agent_id = %entry.agent_id, error = %e, "restart_dispatch failed");
                }
            }
        }
        Ok(in_flight)
    }

    /// Process mail-wake candidates. Returns the updated in-flight count.
    /// Skips silently if mail-wake is not configured.
    async fn tick_mail_wake(&self, mut in_flight: usize, cap: usize) -> Result<usize> {
        let (Some(waker), Some(lookup)) = (&self.mail_waker, &self.state_lookup) else {
            return Ok(in_flight);
        };

        let candidates = self.db.list_recipients_with_pending_wake_eligible_mail()?;
        for agent_id in candidates {
            if in_flight >= cap {
                break;
            }

            let Some(state_session) = lookup.get_state_and_session(&agent_id)? else {
                continue;
            };
            // Only Dormant agents with a session_id can be woken. Complete
            // is the truly-finished state and is no longer a wake candidate;
            // boot-time migration promotes Complete-with-session agents to
            // Dormant so existing DBs continue to behave the same way.
            if state_session.0 != AgentState::Dormant {
                continue;
            }
            let Some(_session_id) = state_session.1 else {
                continue;
            };

            let pending = self.db.list_pending_wake_eligible(&agent_id)?;
            if pending.is_empty() {
                continue;
            }
            let (prompt, folded_ids) = build_wake_prompt(&pending);

            match waker.wake(&agent_id, &prompt).await {
                Ok(()) => {
                    let now = chrono::Utc::now().timestamp();
                    for id in &folded_ids {
                        if let Err(e) = self.db.set_mail_state(id, MailState::Delivered, None) {
                            warn!(mail_id = %id, error = %e, "failed to mark mail Delivered after wake");
                            continue;
                        }
                        // Look up recipient for the event payload.
                        if let Some(m) = pending.iter().find(|m| &m.id == id) {
                            self.bus.publish(StreamEvent::MailDelivered {
                                mail_id: m.id.clone(),
                                recipient_id: m.recipient_id.clone(),
                                origin_daemon_id: None,
                            });
                        }
                    }
                    let _ = now;
                    in_flight += 1;
                }
                Err(e) => {
                    warn!(agent_id = %agent_id, error = %e, "mail-wake invoke failed; mail stays Pending");
                }
            }
        }

        Ok(in_flight)
    }
}

/// Fold a list of pending mail rows into a single resume prompt.
/// Bodies are joined with `\n\n---\n\n` and the result is truncated at
/// `WAKE_FOLD_MAX_BYTES` bytes; if any messages are dropped, a
/// `[... N more messages truncated]` note is appended.
///
/// Returns (prompt, folded_mail_ids) where `folded_mail_ids` is the list of
/// mail rows that contributed (including any partially truncated message — it
/// still counts as folded so it doesn't block forever).
pub fn build_wake_prompt(mails: &[crate::shared::types::Mail]) -> (String, Vec<String>) {
    const SEP: &str = "\n\n---\n\n";
    let mut buf = String::new();
    let mut folded_ids: Vec<String> = Vec::new();
    let mut dropped: usize = 0;

    for (i, m) in mails.iter().enumerate() {
        let candidate = if i == 0 {
            m.body.clone()
        } else {
            format!("{}{}", SEP, m.body)
        };
        if buf.len() + candidate.len() > WAKE_FOLD_MAX_BYTES {
            // The remaining messages (this one and the rest) won't fit in
            // full. If `buf` is still empty, accept a single oversized
            // message in truncated form so we don't deadlock.
            if buf.is_empty() {
                let take_chars = m.body.chars().count().min(WAKE_FOLD_MAX_BYTES / 4);
                buf.push_str(&m.body.chars().take(take_chars).collect::<String>());
                folded_ids.push(m.id.clone());
                dropped = mails.len() - 1;
            } else {
                dropped = mails.len() - i;
            }
            break;
        }
        buf.push_str(&candidate);
        folded_ids.push(m.id.clone());
    }

    if dropped > 0 {
        let _ = write!(buf, "\n\n[... {dropped} more messages truncated]");
    }
    (buf, folded_ids)
}

/// Handle to a spawned scheduler. Drop to abort.
pub struct SchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl SchedulerHandle {
    pub fn abort(&self) {
        self.handle.abort();
    }
}

impl Drop for SchedulerHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
