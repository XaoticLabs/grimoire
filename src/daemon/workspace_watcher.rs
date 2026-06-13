//! Per-workspace `notify::RecommendedWatcher` wrapper. Owns the watcher and a
//! debounce/batch task that emits `WorkspaceFileChanged` stream events plus
//! topic mail to `topic://workspace/<id>/files`.

use anyhow::{Result, anyhow};
use globset::{Glob, GlobSet, GlobSetBuilder};
use notify::{Event as NotifyEvent, EventKind, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;

use super::peer_registry::PeerRegistry;

/// Late-bound handle to the daemon's `PeerRegistry`. Set once at boot
/// (see `daemon::start`) so the per-workspace watcher tasks can wake
/// the peer outbox drainer immediately after enqueueing federated
/// events. Optional — when unset (tests, daemon variants without
/// federation), workspace fanout still enqueues; drains just wait for
/// the next inbound message or heartbeat tick.
static PEER_REGISTRY: OnceLock<Arc<PeerRegistry>> = OnceLock::new();

/// Install the peer registry handle used to wake outbox drainers
/// after `workspace_event_enqueue`. Safe to call once; later calls are
/// no-ops.
pub fn set_peer_registry(reg: Arc<PeerRegistry>) {
    let _ = PEER_REGISTRY.set(reg);
}

use crate::shared::constants::{
    WORKSPACE_WATCH_BATCH_MAX, WORKSPACE_WATCH_DEBOUNCE_MS, WORKSPACE_WATCH_DEFAULT_IGNORES,
};
use crate::shared::protocol::StreamEvent;
use crate::shared::types::{Mail, MailState};

use super::event_bus::EventBus;
use super::persistence::Database;

pub struct WorkspaceWatcher;

pub struct WorkspaceWatcherHandle {
    _watcher: notify::RecommendedWatcher,
    shutdown_tx: mpsc::UnboundedSender<()>,
    _task: tokio::task::JoinHandle<()>,
}

impl WorkspaceWatcherHandle {
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        // Dropping `_watcher` stops the OS-level watch.
    }
}

#[derive(Debug, Clone)]
struct ChangeRecord {
    rel_path: String,
    kind: String,
}

impl WorkspaceWatcher {
    pub fn start(
        workspace_id: String,
        root: PathBuf,
        db: Arc<Database>,
        bus: EventBus,
    ) -> Result<WorkspaceWatcherHandle> {
        let canonical_root =
            std::fs::canonicalize(&root).map_err(|e| anyhow!("workspace_root_missing: {e}"))?;

        let ignore_set = build_default_ignores()?;

        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ChangeRecord>();
        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();

        let watcher_root = canonical_root.clone();
        let watcher_ignore = ignore_set;
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<NotifyEvent>| {
            let event = match res {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(error = %e, "workspace notify error");
                    return;
                }
            };
            let kind_str = match event.kind {
                EventKind::Create(_) => "create",
                EventKind::Modify(_) => "modify",
                EventKind::Remove(_) => "remove",
                _ => return,
            };
            for path in &event.paths {
                let rel = match path.strip_prefix(&watcher_root) {
                    Ok(r) => r.to_path_buf(),
                    Err(_) => continue,
                };
                if watcher_ignore.is_match(&rel) {
                    continue;
                }
                let s = rel.to_string_lossy().to_string();
                if s.is_empty() {
                    continue;
                }
                let _ = event_tx.send(ChangeRecord {
                    rel_path: s,
                    kind: kind_str.to_string(),
                });
            }
        })?;
        watcher.watch(&canonical_root, RecursiveMode::Recursive)?;

        let task_workspace = workspace_id;
        let task_bus = bus;
        let task_db = db;
        let task = tokio::spawn(async move {
            let debounce = Duration::from_millis(WORKSPACE_WATCH_DEBOUNCE_MS);
            let mut buffer: Vec<ChangeRecord> = Vec::new();
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        return;
                    }
                    msg = event_rx.recv() => {
                        let Some(item) = msg else { return };
                        buffer.push(item);
                        // Drain anything else that's already queued.
                        while let Ok(extra) = event_rx.try_recv() {
                            buffer.push(extra);
                        }
                        // Wait debounce window for any follow-up.
                        loop {
                            tokio::select! {
                                _ = shutdown_rx.recv() => return,
                                () = tokio::time::sleep(debounce) => break,
                                msg = event_rx.recv() => {
                                    match msg {
                                        Some(m) => buffer.push(m),
                                        None => return,
                                    }
                                }
                            }
                        }
                        emit_batch(&task_workspace, &task_db, &task_bus, &mut buffer);
                    }
                }
            }
        });

        Ok(WorkspaceWatcherHandle {
            _watcher: watcher,
            shutdown_tx,
            _task: task,
        })
    }
}

fn emit_batch(workspace_id: &str, db: &Database, bus: &EventBus, buffer: &mut Vec<ChangeRecord>) {
    if buffer.is_empty() {
        return;
    }
    // Dedup adjacent same paths (notify can fire multiple events per change).
    buffer.dedup_by(|a, b| a.rel_path == b.rel_path && a.kind == b.kind);

    let total = buffer.len();
    let truncated_count = if total > WORKSPACE_WATCH_BATCH_MAX {
        (total - WORKSPACE_WATCH_BATCH_MAX) as u32
    } else {
        0
    };
    let take = total.min(WORKSPACE_WATCH_BATCH_MAX);
    let paths: Vec<String> = buffer
        .iter()
        .take(take)
        .map(|r| r.rel_path.clone())
        .collect();
    let kinds: Vec<String> = buffer.iter().take(take).map(|r| r.kind.clone()).collect();

    publish_workspace_file_change(workspace_id, db, bus, &paths, &kinds, truncated_count);

    // Fan out to federated peers. One outbox row per peer with direction
    // `outbound` or `both`. Failures are logged but never abort the local
    // publish — federation is best-effort on top of the always-durable
    // local event.
    fanout_to_federated_peers(db, workspace_id, &paths, &kinds, truncated_count);

    buffer.clear();
}

/// Publish a `WorkspaceFileChanged` stream event and topic mail. The
/// shared path between the watcher (local FS changes) and the federation
/// receiver (events arriving from a home daemon onto a shadow workspace),
/// so shadows and locals emit byte-identical events.
pub fn publish_workspace_file_change(
    workspace_id: &str,
    db: &Database,
    bus: &EventBus,
    paths: &[String],
    kinds: &[String],
    truncated_count: u32,
) {
    bus.publish(StreamEvent::WorkspaceFileChanged {
        workspace_id: workspace_id.to_string(),
        paths: paths.to_vec(),
        kinds: kinds.to_vec(),
        truncated_count,
    });
    publish_files_topic(db, bus, workspace_id, paths, kinds, truncated_count);
}

fn fanout_to_federated_peers(
    db: &Database,
    workspace_id: &str,
    paths: &[String],
    kinds: &[String],
    truncated: u32,
) {
    let peers = match db.workspace_outbound_peers(workspace_id) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, workspace_id, "workspace_outbound_peers failed");
            return;
        }
    };
    if peers.is_empty() {
        return;
    }
    let payload = serde_json::json!({
        "paths": paths,
        "kinds": kinds,
        "truncated": truncated,
    })
    .to_string();
    let payload_bytes = payload.as_bytes();
    let mut woke: Vec<String> = Vec::with_capacity(peers.len());
    for peer_id in peers {
        match db.workspace_event_enqueue(&peer_id, workspace_id, payload_bytes) {
            Ok(_) => woke.push(peer_id),
            Err(e) => {
                tracing::warn!(error = %e, peer = %peer_id, workspace_id,
                    "workspace_event_enqueue failed");
            }
        }
    }
    if woke.is_empty() {
        return;
    }
    if let Some(registry) = PEER_REGISTRY.get().cloned() {
        // notify_outbox takes an async mutex — hop onto the runtime so
        // we don't block the watcher task's emit path.
        tokio::spawn(async move {
            for peer_id in woke {
                registry.notify_outbox(&peer_id).await;
            }
        });
    }
}

fn publish_files_topic(
    db: &Database,
    bus: &EventBus,
    workspace_id: &str,
    paths: &[String],
    kinds: &[String],
    truncated: u32,
) {
    let topic = format!("workspace/{workspace_id}/files");
    let body = serde_json::json!({
        "paths": paths,
        "kinds": kinds,
        "truncated": truncated,
    })
    .to_string();
    let Ok(subscribers) = db.list_subscribers_for_topic(&topic) else {
        return;
    };
    if subscribers.is_empty() {
        return;
    }
    let now = chrono::Utc::now().timestamp();
    let sender = format!("workspace://{workspace_id}");
    let mut mails: Vec<Mail> = Vec::with_capacity(subscribers.len());
    for sub in subscribers {
        mails.push(Mail {
            id: crate::shared::constants::generate_short_id(),
            recipient_id: sub.subscriber_id.clone(),
            sender_id: Some(sender.clone()),
            topic: Some(topic.clone()),
            body: body.clone(),
            in_reply_to: None,
            state: MailState::Pending,
            fail_reason: None,
            created_at: now,
            delivered_at: None,
            seq: 0,
            wake_eligible: true,
        });
    }
    if let Err(e) = db.insert_mail_batch(&mails) {
        tracing::warn!(error = %e, "workspace files topic mail insert failed");
        return;
    }
    for mail in &mails {
        bus.publish(StreamEvent::MailReceived {
            mail_id: mail.id.clone(),
            recipient_id: mail.recipient_id.clone(),
            sender_id: Some(sender.clone()),
            topic: Some(topic.clone()),
            body_preview: body.chars().take(200).collect(),
            wake_eligible: true,
            origin_daemon_id: None,
        });
    }
}

fn build_default_ignores() -> Result<GlobSet> {
    let mut b = GlobSetBuilder::new();
    for pat in WORKSPACE_WATCH_DEFAULT_IGNORES {
        let g = Glob::new(pat).map_err(|e| anyhow!("invalid_glob: {e}"))?;
        b.add(g);
    }
    Ok(b.build()?)
}
