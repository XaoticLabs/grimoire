//! Daemon-owned scheduler that promotes `Queued` agents to `Active` while
//! global capacity allows and an eligible worker exists. It is the single
//! caller of the dispatch path; `agent_manager::enqueue` only inserts work.
//!
//! The reactor wakes on terminal `StateChange` (slot freed), `AgentQueued`,
//! `WorkerRegistered`, and a 100ms periodic tick as a missed-signal safety net.

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

/// Folded wake-prompt cap. Tighter than `mail.send`'s 64 KiB body limit so
/// resume prompts stay manageable.
const WAKE_FOLD_MAX_BYTES: usize = 16_384;

const TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Seam onto `AgentManager::dispatch_internal`.
#[async_trait]
pub trait Dispatcher: Send + Sync {
    async fn dispatch(&self, row: QueueRow) -> Result<()>;
}

/// Seam to wake an agent on incoming mail (wired to `AgentManager::invoke()`).
#[async_trait]
pub trait MailWaker: Send + Sync {
    async fn wake(&self, agent_id: &str, prompt: &str) -> Result<()>;
}

/// Get-only access to an agent's runtime state for the mail-wake check.
pub trait AgentStateLookup: Send + Sync {
    fn get_state_and_session(&self, id: &str) -> Result<Option<(AgentState, Option<String>)>>;
}

pub struct DbStateLookup {
    pub db: Arc<Database>,
}

impl AgentStateLookup for DbStateLookup {
    fn get_state_and_session(&self, id: &str) -> Result<Option<(AgentState, Option<String>)>> {
        Ok(self.db.get_agent(id)?.map(|a| (a.state, a.session_id)))
    }
}

/// Drive ticks manually with [`Scheduler::tick_now`] (tests) or spawn the
/// background reactor via [`Scheduler::spawn`] (daemon boot).
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
    /// Providers the in-process `LocalExecutor` runs; eligibility waives the
    /// "must have a remote worker" requirement for these.
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

    /// Declare providers handled by the in-process `LocalExecutor`; dispatch
    /// for these is not gated on a registered remote worker.
    #[must_use]
    pub fn with_local_providers<I, S>(mut self, providers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.local_providers = providers.into_iter().map(Into::into).collect();
        self
    }

    /// Wire mail-wake: each tick invokes recipients of pending wake-eligible
    /// mail (Dormant + session_id) up to the global cap.
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

    /// Wire supervision: each tick pulls due restarts from the supervisor's
    /// pending heap.
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

    /// Dispatch as many queued rows as capacity and eligibility allow.
    /// Re-entrant calls serialize via a mutex so concurrent wake-up signals
    /// can't interleave their claims.
    #[tracing::instrument(name = "scheduler.tick", skip(self))]
    pub async fn tick_now(&self) -> Result<()> {
        let _guard = self.tick_lock.lock().await;

        let mut in_flight = self.db.count_in_flight_agents()?;
        let cap = self.cap.load(Ordering::Relaxed) as usize;

        if cap == 0 {
            return Ok(());
        }

        // Mail-wake runs before queue dispatch so a wake and a queued dispatch
        // can't race for the same slot.
        in_flight = self.tick_mail_wake(in_flight, cap).await?;
        if in_flight >= cap {
            return Ok(());
        }

        in_flight = self.tick_supervision(in_flight, cap).await?;
        if in_flight >= cap {
            return Ok(());
        }

        // `list_queue()` is already in dispatch order; iterating it lets us
        // skip ineligible rows without losing place in line.
        let rows = self.db.list_queue()?;

        for row in rows {
            if in_flight >= cap {
                // Mark the rest capacity-blocked for `grim queue` visibility;
                // cosmetic, revisited on next signal.
                if row.block_reason.as_deref() != Some("capacity")
                    && let Err(e) = self.db.set_block_reason(&row.id, Some("capacity"))
                {
                    warn!(agent_id = %row.id, error = %e, "failed to set capacity block_reason");
                }
                continue;
            }

            // Eligibility peek: provider_name=None means "any worker" (let
            // dispatch_internal pick). When set, require a worker advertising
            // it or a matching local_providers entry.
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

            // Atomic claim: deletes the queue row and flips state to
            // `summoning` in one txn. `false` means someone else claimed first.
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

            // On failure, requeue with the original `enqueued_at` to preserve
            // fairness, then break to avoid a tight failure loop this tick.
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

    /// Spawn the background reactor: tick on each bus signal plus a 100ms
    /// safety-net interval. Dropping the returned handle aborts the task.
    pub fn spawn(self: Arc<Self>) -> SchedulerHandle {
        let mut rx = self.bus.subscribe();
        let sched = self;
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await; // consume the immediate fire

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

    /// Drain due restarts and dispatch them under the cap. Returns the
    /// updated in-flight count.
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
            // Only Dormant agents with a session_id are wakeable; Complete is
            // truly-finished. (Boot migration promotes Complete-with-session
            // agents to Dormant.)
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

/// Fold pending mail into one resume prompt: bodies joined with `\n\n---\n\n`,
/// truncated at `WAKE_FOLD_MAX_BYTES` with a `[... N more truncated]` note.
/// Returns (prompt, folded_mail_ids). A partially-truncated message still
/// counts as folded so it can't block forever.
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
            // If `buf` is empty, accept a single oversized message truncated
            // so we don't deadlock.
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
