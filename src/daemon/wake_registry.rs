//! `WakeRegistry` — a daemon-internal actor that owns wake-source lifecycle.
//!
//! Responsibilities:
//! - Persist wake_sources rows in SQLite.
//! - Arm in-memory evaluators (cron timers, file watchers, parent-completion
//!   subscriptions) and tear them down on remove/banish.
//! - Periodically evaluate cron sources; respond to event-driven sources via
//!   their fire channel.
//! - Send wake mail through a `WakeMailSender` seam (default impl writes a
//!   wake-eligible mail row directly so the existing mail-wake path picks it
//!   up).
//! - Apply per-agent rate limiting (Task 6) on the common fire path.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::Mutex;

use crate::daemon::clock::Clock;
use crate::daemon::event_bus::EventBus;
use crate::daemon::persistence::{Database, unix_now};
use crate::daemon::wake_sources::cron::{CronConfig, CronSource};
use crate::daemon::wake_sources::file_watch::{FileWatchConfig, FileWatchSource};
use crate::daemon::wake_sources::parent_completion::{
    ParentCompletionConfig, ParentCompletionSource,
};
use crate::shared::constants;
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{
    Mail, MailState, WakeSource, WakeSourceKind, WakeSourceState,
};

/// Sender seam for wake-fire mail. Default impl writes the row directly.
#[async_trait]
pub trait WakeMailSender: Send + Sync {
    async fn send_wake_mail(
        &self,
        wake_id: &str,
        agent_id: &str,
        body: &str,
    ) -> Result<String>;
}

/// Default `WakeMailSender` — writes a wake-eligible mail row with
/// `sender_id = wake://<wake_id>` so the scheduler's mail-wake path can pick
/// it up unchanged.
pub struct DbWakeMailSender {
    pub db: Arc<Database>,
    pub bus: EventBus,
}

#[async_trait]
impl WakeMailSender for DbWakeMailSender {
    async fn send_wake_mail(
        &self,
        wake_id: &str,
        agent_id: &str,
        body: &str,
    ) -> Result<String> {
        let mail_id = format!("wm{}", &constants::generate_short_id()[..6]);
        let now = unix_now();
        let mail = Mail {
            id: mail_id.clone(),
            recipient_id: agent_id.to_string(),
            sender_id: Some(format!("wake://{}", wake_id)),
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
        // Publish MailReceived so the scheduler's reactor wakes immediately.
        self.bus.publish(StreamEvent::MailReceived {
            mail_id: mail_id.clone(),
            recipient_id: agent_id.to_string(),
            sender_id: Some(format!("wake://{}", wake_id)),
            topic: None,
            body_preview: body.chars().take(200).collect(),
            wake_eligible: true,
        });
        Ok(mail_id)
    }
}

/// Handle to an armed source. Dropping the handle tears down its watchers /
/// subscriptions.
pub enum ArmedHandle {
    Cron(CronSource),
    FileWatch {
        _watcher: notify::RecommendedWatcher,
        _drain_task: tokio::task::JoinHandle<()>,
    },
    ParentCompletion {
        _task: tokio::task::JoinHandle<()>,
    },
}

pub struct WakeRegistry {
    db: Arc<Database>,
    bus: EventBus,
    clock: Arc<dyn Clock>,
    mail_sender: Arc<dyn WakeMailSender>,
    handles: Mutex<HashMap<String, ArmedHandle>>,
    /// Channel event-driven sources push fire requests onto.
    fire_tx: tokio::sync::mpsc::Sender<FireMsg>,
    /// Held until `start()` is called so the drain loop can take it.
    fire_rx: Mutex<Option<tokio::sync::mpsc::Receiver<FireMsg>>>,
}

pub struct FireMsg {
    pub wake_id: String,
    pub body: String,
    pub via: Option<String>,
}

impl WakeRegistry {
    pub fn new(
        db: Arc<Database>,
        bus: EventBus,
        clock: Arc<dyn Clock>,
        mail_sender: Arc<dyn WakeMailSender>,
    ) -> Arc<Self> {
        let (fire_tx, fire_rx) = tokio::sync::mpsc::channel::<FireMsg>(256);
        Arc::new(Self {
            db,
            bus,
            clock,
            mail_sender,
            handles: Mutex::new(HashMap::new()),
            fire_tx,
            fire_rx: Mutex::new(Some(fire_rx)),
        })
    }

    /// Convenience constructor wiring the default `DbWakeMailSender`.
    pub fn with_default_sender(
        db: Arc<Database>,
        bus: EventBus,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        let sender: Arc<dyn WakeMailSender> = Arc::new(DbWakeMailSender {
            db: db.clone(),
            bus: bus.clone(),
        });
        Self::new(db, bus, clock, sender)
    }

    pub fn fire_tx(&self) -> tokio::sync::mpsc::Sender<FireMsg> {
        self.fire_tx.clone()
    }

    /// Spawn the registry's background tasks: a cron tick loop and a fire
    /// drain loop. Idempotent: repeated calls after the first no-op (the
    /// second call sees `fire_rx == None`).
    pub fn spawn(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let mut guard = self.fire_rx.try_lock().ok()?;
        let mut rx = guard.take()?;

        let me = self.clone();
        let drain = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Err(e) = me.fire(&msg.wake_id, &msg.body, msg.via.as_deref()).await {
                    tracing::warn!(wake_id = %msg.wake_id, error = %e, "wake fire failed");
                }
            }
        });

        // Cron tick loop: every 30s evaluate all cron sources.
        let me2 = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(e) = me2.tick_cron().await {
                    tracing::warn!(error = %e, "cron tick failed");
                }
            }
        });

        Some(drain)
    }

    /// Manually drive a single cron evaluation pass — used by tests that
    /// inject a `TestClock` and don't want to wait 30s.
    pub async fn tick_cron(self: &Arc<Self>) -> Result<()> {
        let now = self.clock.now();
        let armed = self.db.list_armed_wake_sources()?;
        for src in armed {
            if !matches!(src.kind, WakeSourceKind::Cron) {
                continue;
            }
            let cfg: CronConfig = match serde_json::from_str(&src.config_json) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(wake_id = %src.id, error = %e, "invalid cron config json");
                    continue;
                }
            };
            let cron = match CronSource::new(&cfg.expr) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let last = src.last_fired_at.and_then(|t| Utc.timestamp_opt(t, 0).single());
            let registered = Utc
                .timestamp_opt(src.created_at, 0)
                .single()
                .unwrap_or(now);
            if cron.evaluate(now, last, registered).is_some() {
                let body = format!("[cron] {} fired at {}", cfg.expr, now.to_rfc3339());
                let _ = self.fire(&src.id, &body, None).await;
            }
        }
        Ok(())
    }

    /// Register a new wake source. Persists, arms, emits
    /// `WakeSourceRegistered`, returns the new wake_id.
    pub async fn register(
        self: &Arc<Self>,
        agent_id: &str,
        kind: WakeSourceKind,
        config_json: &str,
    ) -> Result<String> {
        // Validate / parse config eagerly so bad input fails before persisting
        // an `armed` row that will never fire.
        validate_config(kind, config_json)?;

        let wake_id = format!("wake_{}", &constants::generate_short_id()[..8]);
        let now = self.clock.now().timestamp();
        let row = WakeSource {
            id: wake_id.clone(),
            agent_id: agent_id.to_string(),
            kind,
            config_json: config_json.to_string(),
            state: WakeSourceState::Armed,
            fail_reason: None,
            last_fired_at: None,
            fire_count: 0,
            created_at: now,
        };
        self.db.insert_wake_source(&row)?;

        // Arm in-memory plumbing.
        match self.arm_one(&row).await {
            Ok(handle) => {
                self.handles.lock().await.insert(wake_id.clone(), handle);
                self.bus.publish(StreamEvent::WakeSourceRegistered {
                    wake_id: wake_id.clone(),
                    agent_id: agent_id.to_string(),
                    kind: kind.as_str().to_string(),
                });
                Ok(wake_id)
            }
            Err(e) => {
                let reason = e.to_string();
                let _ = self.db.update_wake_source_state(
                    &wake_id,
                    WakeSourceState::Failed,
                    Some(&reason),
                );
                self.bus.publish(StreamEvent::WakeSourceFailed {
                    wake_id: wake_id.clone(),
                    agent_id: agent_id.to_string(),
                    reason: reason.clone(),
                });
                Err(anyhow!(reason))
            }
        }
    }

    /// Remove (retire) a single source. User-initiated.
    pub async fn remove(self: &Arc<Self>, wake_id: &str) -> Result<bool> {
        let src = match self.db.get_wake_source(wake_id)? {
            Some(s) => s,
            None => return Ok(false),
        };
        self.db.delete_wake_source(wake_id)?;
        self.handles.lock().await.remove(wake_id);
        self.bus.publish(StreamEvent::WakeSourceRetired {
            wake_id: wake_id.to_string(),
            agent_id: src.agent_id,
            reason: "user_removed".to_string(),
        });
        Ok(true)
    }

    /// Bulk retire — used by `grim banish`.
    pub async fn retire_for_agent(self: &Arc<Self>, agent_id: &str) -> Result<usize> {
        let sources = self.db.list_wake_sources_for_agent(agent_id)?;
        let n = sources.len();
        for s in &sources {
            let _ = self.db.delete_wake_source(&s.id);
            self.handles.lock().await.remove(&s.id);
            self.bus.publish(StreamEvent::WakeSourceRetired {
                wake_id: s.id.clone(),
                agent_id: agent_id.to_string(),
                reason: "agent_banished".to_string(),
            });
        }
        Ok(n)
    }

    pub async fn list_for_agent(&self, agent_id: &str) -> Result<Vec<WakeSource>> {
        self.db.list_wake_sources_for_agent(agent_id)
    }

    pub async fn list_all(&self) -> Result<Vec<WakeSource>> {
        self.db.list_all_wake_sources()
    }

    /// Manually fire a source bypassing the rate limit. Used by `grim wake test`.
    pub async fn test_fire(self: &Arc<Self>, wake_id: &str) -> Result<String> {
        let src = self
            .db
            .get_wake_source(wake_id)?
            .ok_or_else(|| anyhow!("wake_not_found"))?;
        let body = format!("[wake_test] manual fire of {}", src.id);
        self.fire_unrated(&src.id, &src.agent_id, &body, Some("test"))
            .await
    }

    /// Common fire path with rate-limit gate (Task 6 behavior).
    pub async fn fire(
        self: &Arc<Self>,
        wake_id: &str,
        body: &str,
        via: Option<&str>,
    ) -> Result<String> {
        let src = self
            .db
            .get_wake_source(wake_id)?
            .ok_or_else(|| anyhow!("wake_not_found"))?;

        // Rate limit (Task 6).
        let allow = self.consume_token(&src.agent_id).await?;
        if !allow {
            // Bump fire_count for observability of denied fires.
            let _ = self
                .db
                .bump_wake_source_fire(&src.id, self.clock.now().timestamp());
            self.bus.publish(StreamEvent::WakeSourceFailed {
                wake_id: src.id.clone(),
                agent_id: src.agent_id.clone(),
                reason: "rate_limited".to_string(),
            });
            return Err(anyhow!("rate_limited"));
        }

        self.fire_unrated(&src.id, &src.agent_id, body, via).await
    }

    /// Fire path skipping the rate-limit gate. Used by `test_fire` and
    /// internally after rate-limit acceptance.
    async fn fire_unrated(
        self: &Arc<Self>,
        wake_id: &str,
        agent_id: &str,
        body: &str,
        via: Option<&str>,
    ) -> Result<String> {
        let mail_id = self
            .mail_sender
            .send_wake_mail(wake_id, agent_id, body)
            .await?;
        let now = self.clock.now().timestamp();
        self.db.bump_wake_source_fire(wake_id, now)?;
        self.bus.publish(StreamEvent::WakeSourceFired {
            wake_id: wake_id.to_string(),
            agent_id: agent_id.to_string(),
            mail_id: mail_id.clone(),
            via: via.map(|s| s.to_string()),
        });
        Ok(mail_id)
    }

    /// Token-bucket gate. Returns Ok(true) on accept, Ok(false) on deny.
    async fn consume_token(&self, agent_id: &str) -> Result<bool> {
        let now = self.clock.now().timestamp();
        let (tokens, last, capacity, refill) =
            self.db.get_or_init_rate_limit(agent_id, now)?;
        // Clamp negative elapsed (clock skew) at 0 so backwards jumps don't
        // mint tokens.
        let elapsed = (now - last).max(0) as f64;
        let new_tokens = (tokens + elapsed * refill).min(capacity as f64);
        if new_tokens >= 1.0 {
            self.db
                .update_rate_limit_tokens(agent_id, new_tokens - 1.0, now)?;
            Ok(true)
        } else {
            self.db.update_rate_limit_tokens(agent_id, new_tokens, now)?;
            Ok(false)
        }
    }

    /// Re-arm every persisted `armed` source from the DB. Cron sources get a
    /// catch-up fire if they missed at least one tick during downtime.
    pub async fn replay_on_boot(self: &Arc<Self>) -> Result<()> {
        let armed = self.db.list_armed_wake_sources()?;
        for src in armed {
            match self.arm_one(&src).await {
                Ok(handle) => {
                    self.handles.lock().await.insert(src.id.clone(), handle);
                }
                Err(e) => {
                    let reason = e.to_string();
                    let _ = self.db.update_wake_source_state(
                        &src.id,
                        WakeSourceState::Failed,
                        Some(&reason),
                    );
                    self.bus.publish(StreamEvent::WakeSourceFailed {
                        wake_id: src.id.clone(),
                        agent_id: src.agent_id.clone(),
                        reason,
                    });
                    continue;
                }
            }

            // Cron catch-up.
            if let WakeSourceKind::Cron = src.kind {
                if let Ok(cfg) = serde_json::from_str::<CronConfig>(&src.config_json) {
                    if let Ok(cron) = CronSource::new(&cfg.expr) {
                        let now = self.clock.now();
                        let last = src
                            .last_fired_at
                            .and_then(|t| Utc.timestamp_opt(t, 0).single());
                        let registered = Utc
                            .timestamp_opt(src.created_at, 0)
                            .single()
                            .unwrap_or(now);
                        if cron.evaluate(now, last, registered).is_some() {
                            let body = format!(
                                "[catch-up] cron {} (last fired: {:?})",
                                cfg.expr, src.last_fired_at
                            );
                            let _ = self.fire(&src.id, &body, None).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn arm_one(self: &Arc<Self>, src: &WakeSource) -> Result<ArmedHandle> {
        match src.kind {
            WakeSourceKind::Cron => {
                let cfg: CronConfig = serde_json::from_str(&src.config_json)
                    .map_err(|e| anyhow!("invalid_cron_config_json: {}", e))?;
                let cron = CronSource::new(&cfg.expr)?;
                Ok(ArmedHandle::Cron(cron))
            }
            WakeSourceKind::FileWatch => {
                let cfg: FileWatchConfig = serde_json::from_str(&src.config_json)
                    .map_err(|e| anyhow!("invalid_file_watch_config_json: {}", e))?;
                let source = Arc::new(FileWatchSource::new(cfg)?);
                let (notify_tx, mut notify_rx) =
                    tokio::sync::mpsc::channel::<crate::daemon::wake_sources::file_watch::MatchedChange>(256);
                let watcher = source.clone().arm(notify_tx)?;

                // Debounce drain task: collect changes during a 200ms window
                // after the first event, then send one FireMsg.
                let fire_tx = self.fire_tx.clone();
                let wake_id = src.id.clone();
                let drain_task = tokio::spawn(async move {
                    while let Some(first) = notify_rx.recv().await {
                        let mut count = 1usize;
                        let first_path = first.path.clone();
                        let deadline = tokio::time::Instant::now()
                            + std::time::Duration::from_millis(
                                crate::daemon::wake_sources::file_watch::DEBOUNCE_MS,
                            );
                        loop {
                            let now = tokio::time::Instant::now();
                            if now >= deadline {
                                break;
                            }
                            match tokio::time::timeout(deadline - now, notify_rx.recv()).await {
                                Ok(Some(_)) => count += 1,
                                _ => break,
                            }
                        }
                        let body = format!(
                            "[file-watch] {} changes; first: {}",
                            count,
                            first_path.display()
                        );
                        let _ = fire_tx
                            .send(FireMsg {
                                wake_id: wake_id.clone(),
                                body,
                                via: None,
                            })
                            .await;
                    }
                });

                Ok(ArmedHandle::FileWatch {
                    _watcher: watcher,
                    _drain_task: drain_task,
                })
            }
            WakeSourceKind::ParentCompletion => {
                let cfg: ParentCompletionConfig = serde_json::from_str(&src.config_json)
                    .map_err(|e| anyhow!("invalid_parent_completion_config_json: {}", e))?;
                let source = ParentCompletionSource::new(cfg)?;
                let mut rx = self.bus.subscribe();
                let fire_tx = self.fire_tx.clone();
                let wake_id = src.id.clone();
                let task = tokio::spawn(async move {
                    while let Ok(ev) = rx.recv().await {
                        if let StreamEvent::StateChange {
                            agent_id,
                            new_state,
                            ..
                        } = ev
                        {
                            if source.should_fire(&agent_id, &new_state) {
                                let body = format!(
                                    "[parent {} -> {}]",
                                    agent_id,
                                    new_state.as_str()
                                );
                                let _ = fire_tx
                                    .send(FireMsg {
                                        wake_id: wake_id.clone(),
                                        body,
                                        via: None,
                                    })
                                    .await;
                            }
                        }
                    }
                });
                Ok(ArmedHandle::ParentCompletion { _task: task })
            }
        }
    }
}

fn validate_config(kind: WakeSourceKind, config_json: &str) -> Result<()> {
    match kind {
        WakeSourceKind::Cron => {
            let cfg: CronConfig = serde_json::from_str(config_json)
                .map_err(|e| anyhow!("invalid_cron_config_json: {}", e))?;
            CronSource::new(&cfg.expr)?;
        }
        WakeSourceKind::FileWatch => {
            let cfg: FileWatchConfig = serde_json::from_str(config_json)
                .map_err(|e| anyhow!("invalid_file_watch_config_json: {}", e))?;
            // Validate root + globs eagerly.
            FileWatchSource::new(cfg)?;
        }
        WakeSourceKind::ParentCompletion => {
            let _cfg: ParentCompletionConfig = serde_json::from_str(config_json)
                .map_err(|e| anyhow!("invalid_parent_completion_config_json: {}", e))?;
        }
    }
    Ok(())
}

// Helper: convenience for callers who want to register without crafting the
// raw config_json themselves.
impl WakeRegistry {
    pub async fn register_cron(
        self: &Arc<Self>,
        agent_id: &str,
        expr: &str,
    ) -> Result<String> {
        let cfg = CronConfig {
            expr: expr.to_string(),
        };
        let json = serde_json::to_string(&cfg)?;
        self.register(agent_id, WakeSourceKind::Cron, &json).await
    }

    pub async fn register_file_watch(
        self: &Arc<Self>,
        agent_id: &str,
        cfg: FileWatchConfig,
    ) -> Result<String> {
        let json = serde_json::to_string(&cfg)?;
        self.register(agent_id, WakeSourceKind::FileWatch, &json)
            .await
    }

    pub async fn register_parent_completion(
        self: &Arc<Self>,
        agent_id: &str,
        cfg: ParentCompletionConfig,
    ) -> Result<String> {
        let json = serde_json::to_string(&cfg)?;
        self.register(agent_id, WakeSourceKind::ParentCompletion, &json)
            .await
    }
}
