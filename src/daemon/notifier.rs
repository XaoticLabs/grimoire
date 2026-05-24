//! Outbound notifications. A pure [`EventBus`] subscriber that forwards
//! selected events to any configured sink: an HTTPS POST (`webhook_url`), an
//! append-only local JSON log (`log_file`), and/or a desktop toast via
//! `notify-send` (`desktop`). Never spawned when no sink is configured.

use std::path::PathBuf;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use tracing::{debug, warn};

use crate::shared::config::NotificationsConfig;
use crate::shared::protocol::StreamEvent;
use crate::shared::types::AgentState;

use super::event_bus::EventBus;

type HttpsClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

pub struct Notifier {
    config: NotificationsConfig,
    client: HttpsClient,
}

/// What a matched event becomes on the wire.
struct Payload {
    event: &'static str,
    agent_id: Option<String>,
    message: String,
    level: String,
}

impl Notifier {
    /// Build a notifier with an HTTPS-capable hyper client. Errors only if the
    /// TLS root store can't be loaded.
    pub fn new(config: NotificationsConfig) -> anyhow::Result<Self> {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()?
            .https_or_http()
            .enable_http1()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(https);
        Ok(Self { config, client })
    }

    /// Spawn the subscriber loop. No-op if no sink is configured, so callers
    /// can construct unconditionally.
    pub fn start(self, event_bus: &EventBus) {
        if !self.config.has_sink() {
            return;
        }
        let mut rx = event_bus.subscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if let Some(payload) = match_trigger(&self.config, &event) {
                            self.dispatch(&payload).await;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "Notifier lagged, some events missed");
                    }
                    Err(_) => break,
                }
            }
        });
    }

    /// Fan a matched payload out to every configured sink. Each sink is
    /// fire-and-forget; one sink failing must not prevent the others or
    /// affect agent execution.
    async fn dispatch(&self, payload: &Payload) {
        let body = payload_body(payload);
        if self.config.webhook_url.is_some() {
            self.post(payload, &body).await;
        }
        if let Some(path) = self.config.log_file.as_ref() {
            append_log(path, &body).await;
        }
        if self.config.desktop {
            notify_desktop(payload).await;
        }
    }

    /// Fire-and-forget POST. Failures are logged but never propagated; a flaky
    /// webhook must not affect agent execution.
    async fn post(&self, payload: &Payload, body: &serde_json::Value) {
        let Some(url) = self.config.webhook_url.as_deref() else {
            return;
        };
        let bytes = match serde_json::to_vec(body) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "notification payload serialize failed");
                return;
            }
        };
        let _ = payload; // used by other sinks
        let request = match Request::builder()
            .method(Method::POST)
            .uri(url)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(bytes)))
        {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, url = %url, "notification request build failed (bad webhook_url?)");
                return;
            }
        };
        let timeout = Duration::from_secs(self.config.timeout_secs);
        match tokio::time::timeout(timeout, self.client.request(request)).await {
            Ok(Ok(resp)) => {
                let status = resp.status();
                let _ = resp.into_body().collect().await;
                if status.is_success() {
                    debug!(%status, event = payload.event, "notification posted");
                } else {
                    warn!(%status, "notification webhook returned non-2xx");
                }
            }
            Ok(Err(e)) => warn!(error = %e, "notification webhook request failed"),
            Err(_) => warn!(
                timeout_secs = self.config.timeout_secs,
                "notification webhook timed out"
            ),
        }
    }
}

/// Shared JSON shape used by the webhook and log sinks.
fn payload_body(payload: &Payload) -> serde_json::Value {
    serde_json::json!({
        "event": payload.event,
        "agent_id": payload.agent_id,
        "message": payload.message,
        "level": payload.level,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    })
}

/// Append one JSON line to `path`. Best-effort: open failures are logged,
/// not propagated.
async fn append_log(path: &PathBuf, body: &serde_json::Value) {
    use tokio::io::AsyncWriteExt;
    let mut line = match serde_json::to_vec(body) {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "notification log serialize failed");
            return;
        }
    };
    line.push(b'\n');
    let open = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await;
    let mut file = match open {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, path = %path.display(), "notification log open failed");
            return;
        }
    };
    if let Err(e) = file.write_all(&line).await {
        warn!(error = %e, path = %path.display(), "notification log write failed");
    }
}

/// Fire a desktop toast via `notify-send`. Missing binary is logged once at
/// debug level; desktop sinks are best-effort by design.
async fn notify_desktop(payload: &Payload) {
    let urgency = match payload.level.as_str() {
        "error" => "critical",
        "warn" => "normal",
        _ => "low",
    };
    let title = match payload.agent_id.as_deref() {
        Some(id) => format!("Grimoire · {} · {}", payload.event, &id[..id.len().min(8)]),
        None => format!("Grimoire · {}", payload.event),
    };
    let result = tokio::process::Command::new("notify-send")
        .arg("--urgency")
        .arg(urgency)
        .arg("--app-name=grimoire")
        .arg(&title)
        .arg(&payload.message)
        .status()
        .await;
    match result {
        Ok(s) if s.success() => debug!(event = payload.event, "desktop notification sent"),
        Ok(s) => warn!(status = %s, "notify-send returned non-zero"),
        Err(e) => debug!(error = %e, "notify-send unavailable"),
    }
}

/// Map an event to a payload if it matches an enabled trigger. Pure (no I/O)
/// so the trigger policy can be unit-tested without a network client.
fn match_trigger(config: &NotificationsConfig, event: &StreamEvent) -> Option<Payload> {
    match event {
        StreamEvent::StateChange {
            agent_id,
            new_state,
            ..
        } => match new_state {
            AgentState::Complete if config.on_completion => Some(Payload {
                event: "completion",
                agent_id: Some(agent_id.clone()),
                message: format!("Agent {agent_id} completed"),
                level: "info".to_string(),
            }),
            AgentState::Failed if config.on_failure => Some(Payload {
                event: "failure",
                agent_id: Some(agent_id.clone()),
                message: format!("Agent {agent_id} failed"),
                level: "error".to_string(),
            }),
            AgentState::Banished if config.on_failure => Some(Payload {
                event: "failure",
                agent_id: Some(agent_id.clone()),
                message: format!("Agent {agent_id} was banished"),
                level: "warn".to_string(),
            }),
            _ => None,
        },
        StreamEvent::RestartBudgetExhausted { agent_id, reason } if config.on_failure => {
            Some(Payload {
                event: "failure",
                agent_id: Some(agent_id.clone()),
                message: format!("Agent {agent_id} restart budget exhausted ({reason})"),
                level: "error".to_string(),
            })
        }
        StreamEvent::WakeSourceFired {
            agent_id, wake_id, ..
        } if config.on_wake => Some(Payload {
            event: "wake",
            agent_id: Some(agent_id.clone()),
            message: format!("Agent {agent_id} woken by wake source {wake_id}"),
            level: "info".to_string(),
        }),
        StreamEvent::Notification {
            agent_id,
            message,
            level,
            ..
        } if config.on_agent_decided => Some(Payload {
            event: "agent",
            agent_id: agent_id.clone(),
            message: message.clone(),
            level: level.clone(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled() -> NotificationsConfig {
        NotificationsConfig {
            webhook_url: Some("http://example.invalid/hook".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn has_sink_reflects_each_sink_independently() {
        let none = NotificationsConfig::default();
        assert!(!none.has_sink());

        let webhook = NotificationsConfig {
            webhook_url: Some("http://x".into()),
            ..Default::default()
        };
        assert!(webhook.has_sink());

        let log = NotificationsConfig {
            log_file: Some(PathBuf::from("/tmp/x.log")),
            ..Default::default()
        };
        assert!(log.has_sink());

        let desk = NotificationsConfig {
            desktop: true,
            ..Default::default()
        };
        assert!(desk.has_sink());
    }

    #[tokio::test]
    async fn append_log_writes_one_json_line_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notify.log");
        let payload = Payload {
            event: "agent",
            agent_id: Some("abc12345".into()),
            message: "first finding".into(),
            level: "warn".into(),
        };
        append_log(&path, &payload_body(&payload)).await;
        append_log(&path, &payload_body(&payload)).await;

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<_> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "one line per call");
        for line in lines {
            let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON line");
            assert_eq!(v["event"], "agent");
            assert_eq!(v["message"], "first finding");
            assert_eq!(v["agent_id"], "abc12345");
            assert_eq!(v["level"], "warn");
            assert!(v.get("timestamp").is_some());
        }
    }

    fn state_change(new_state: AgentState) -> StreamEvent {
        StreamEvent::StateChange {
            agent_id: "abc12345".to_string(),
            old_state: AgentState::Active,
            new_state,
        }
    }

    #[test]
    fn notifier_constructs_with_tls() {
        // Exercises the rustls/native-roots connector build. Catches a
        // misconfigured CryptoProvider, which would otherwise only surface
        // at the first live webhook POST.
        assert!(Notifier::new(enabled()).is_ok());
    }

    #[test]
    fn completion_triggers_when_enabled() {
        let p = match_trigger(&enabled(), &state_change(AgentState::Complete)).unwrap();
        assert_eq!(p.event, "completion");
        assert_eq!(p.agent_id.as_deref(), Some("abc12345"));
        assert_eq!(p.level, "info");
    }

    #[test]
    fn completion_suppressed_when_disabled() {
        let cfg = NotificationsConfig {
            on_completion: false,
            ..enabled()
        };
        assert!(match_trigger(&cfg, &state_change(AgentState::Complete)).is_none());
    }

    #[test]
    fn failure_and_banish_map_to_failure_event() {
        let cfg = enabled();
        assert_eq!(
            match_trigger(&cfg, &state_change(AgentState::Failed))
                .unwrap()
                .event,
            "failure"
        );
        assert_eq!(
            match_trigger(&cfg, &state_change(AgentState::Banished))
                .unwrap()
                .level,
            "warn"
        );
    }

    #[test]
    fn intermediate_states_never_trigger() {
        let cfg = enabled();
        assert!(match_trigger(&cfg, &state_change(AgentState::Active)).is_none());
        assert!(match_trigger(&cfg, &state_change(AgentState::Dormant)).is_none());
    }

    #[test]
    fn wake_fired_triggers() {
        let ev = StreamEvent::WakeSourceFired {
            wake_id: "w1".to_string(),
            agent_id: "abc12345".to_string(),
            mail_id: "m1".to_string(),
            via: None,
        };
        assert_eq!(match_trigger(&enabled(), &ev).unwrap().event, "wake");
    }

    #[test]
    fn agent_notification_passthrough_preserves_level() {
        let ev = StreamEvent::Notification {
            agent_id: Some("abc12345".to_string()),
            message: "build is red".to_string(),
            level: "error".to_string(),
            source: "agent".to_string(),
        };
        let p = match_trigger(&enabled(), &ev).unwrap();
        assert_eq!(p.event, "agent");
        assert_eq!(p.message, "build is red");
        assert_eq!(p.level, "error");
    }

    #[test]
    fn agent_notification_suppressed_when_disabled() {
        let cfg = NotificationsConfig {
            on_agent_decided: false,
            ..enabled()
        };
        let ev = StreamEvent::Notification {
            agent_id: None,
            message: "x".to_string(),
            level: "info".to_string(),
            source: "system".to_string(),
        };
        assert!(match_trigger(&cfg, &ev).is_none());
    }
}
