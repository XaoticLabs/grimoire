//! Prometheus-format metrics renderer.
//!
//! Hand-rolls the text exposition format rather than pulling in a `prometheus`
//! crate dep. The surface is small (a dozen series) and the rendering is one
//! function; a dependency would cost more than it saves and a slow drift away
//! from the project's hand-rolled style.
//!
//! Scrape semantics: `render` runs a handful of `COUNT(*)` queries against
//! the live SQLite db (cheap; agents/events tables are indexed). It is safe to
//! call at any cadence Prometheus is configured for. There is no caching,
//! every scrape sees fresh data.

use std::fmt::Write as _;
use std::time::Instant;

use crate::daemon::persistence::Database;
use crate::shared::types::AgentState;

/// Agent states we always emit, in a stable order, so dashboards don't see
/// a series appear and disappear when the count rolls to zero. New variants
/// added later still show up via the `count_agents_by_state` union below.
const STANDARD_STATES: &[AgentState] = &[
    AgentState::Summoning,
    AgentState::Queued,
    AgentState::Active,
    AgentState::Dormant,
    AgentState::Complete,
    AgentState::Failed,
    AgentState::Banished,
];

/// Event kinds surfaced as their own counter series. Chosen for operator
/// signal: things that page you or that you'd want a graph of over time.
/// Other kinds are still aggregated into `grimoire_events_total`.
const COUNTER_KINDS: &[(&str, &str, &str)] = &[
    // (event-kind, metric-name, help)
    (
        "restarted",
        "grimoire_restarts_total",
        "Agent restarts that have actually fired.",
    ),
    (
        "wake_source_fired",
        "grimoire_wake_fires_total",
        "Wake-source firings (cron, file-watch, mail, parent-completion).",
    ),
    (
        "escalated",
        "grimoire_escalations_total",
        "Supervisor escalations (restart budget exhausted, route to parent/topic).",
    ),
    (
        "mail_failed",
        "grimoire_mail_failed_total",
        "Mail messages that failed delivery.",
    ),
    (
        "peer_handshake_failed",
        "grimoire_peer_handshake_failures_total",
        "Peer handshakes that failed (TLS, token, version mismatch).",
    ),
];

/// Render the full Prometheus exposition snapshot. Errors from individual
/// queries are surfaced as a `# ERROR` comment line for that metric rather
/// than failing the whole scrape, since a partial snapshot beats none.
pub fn render(db: &Database, started_at: Instant, version: &str) -> String {
    let mut out = String::with_capacity(2048);

    let _ = writeln!(out, "# HELP grimoire_build_info Grimoire build info.");
    let _ = writeln!(out, "# TYPE grimoire_build_info gauge");
    let _ = writeln!(out, "grimoire_build_info{{version=\"{version}\"}} 1");

    // Uptime: a gauge so a daemon restart is visible as a drop to zero,
    // not as a counter reset that confuses rate() queries.
    let uptime_secs = started_at.elapsed().as_secs();
    let _ = writeln!(
        out,
        "# HELP grimoire_uptime_seconds Seconds since the daemon started."
    );
    let _ = writeln!(out, "# TYPE grimoire_uptime_seconds gauge");
    let _ = writeln!(out, "grimoire_uptime_seconds {uptime_secs}");

    // Agents by state: always emit every standard state so series stay stable.
    let _ = writeln!(out, "# HELP grimoire_agents Agents grouped by state.");
    let _ = writeln!(out, "# TYPE grimoire_agents gauge");
    match db.count_agents_by_state() {
        Ok(rows) => {
            for state in STANDARD_STATES {
                let label = state.as_str();
                let value = rows.iter().find(|(k, _)| k == label).map_or(0, |(_, v)| *v);
                let _ = writeln!(out, "grimoire_agents{{state=\"{label}\"}} {value}");
            }
            // Surface unexpected states (future variants) so they don't vanish.
            for (label, value) in &rows {
                if !STANDARD_STATES.iter().any(|s| s.as_str() == label) {
                    let _ = writeln!(out, "grimoire_agents{{state=\"{label}\"}} {value}");
                }
            }
        }
        Err(e) => {
            let _ = writeln!(out, "# ERROR grimoire_agents: {e}");
        }
    }

    // Queue depth: Queued-state agents waiting for the scheduler to promote.
    let _ = writeln!(
        out,
        "# HELP grimoire_queue_depth Agents in Queued state awaiting dispatch."
    );
    let _ = writeln!(out, "# TYPE grimoire_queue_depth gauge");
    match db.list_queue() {
        Ok(rows) => {
            let _ = writeln!(out, "grimoire_queue_depth {}", rows.len());
        }
        Err(e) => {
            let _ = writeln!(out, "# ERROR grimoire_queue_depth: {e}");
        }
    }

    // Events total: one number for the whole durable log.
    let _ = writeln!(
        out,
        "# HELP grimoire_events_total Total durable stream-log events recorded."
    );
    let _ = writeln!(out, "# TYPE grimoire_events_total counter");
    match db.count_events_total() {
        Ok(n) => {
            let _ = writeln!(out, "grimoire_events_total {n}");
        }
        Err(e) => {
            let _ = writeln!(out, "# ERROR grimoire_events_total: {e}");
        }
    }

    // Per-kind event counters: the operator-signal subset.
    for (kind, metric, help) in COUNTER_KINDS {
        let _ = writeln!(out, "# HELP {metric} {help}");
        let _ = writeln!(out, "# TYPE {metric} counter");
        match db.count_events_by_kind(kind) {
            Ok(n) => {
                let _ = writeln!(out, "{metric} {n}");
            }
            Err(e) => {
                let _ = writeln!(out, "# ERROR {metric}: {e}");
            }
        }
    }

    // Notifications by level: distinct series so warn/error rates are graphable.
    let _ = writeln!(
        out,
        "# HELP grimoire_notifications_total Operator-facing notifications by level."
    );
    let _ = writeln!(out, "# TYPE grimoire_notifications_total counter");
    match db.count_notifications_by_level() {
        Ok(rows) => {
            for (level, count) in rows {
                let _ = writeln!(
                    out,
                    "grimoire_notifications_total{{level=\"{level}\"}} {count}"
                );
            }
        }
        Err(e) => {
            let _ = writeln!(out, "# ERROR grimoire_notifications_total: {e}");
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::persistence::Database;
    use crate::shared::protocol::StreamEvent;
    use crate::shared::types::{Agent, AgentId, AgentState, RestartPolicy};
    use chrono::Utc;
    use std::path::PathBuf;

    fn db() -> Database {
        Database::open_in_memory().unwrap()
    }

    fn agent(id: &str, state: AgentState) -> Agent {
        Agent {
            id: id.into(),
            name: None,
            state,
            task: Some("t".into()),
            model: None,
            provider: None,
            cwd: PathBuf::from("/tmp"),
            pid: None,
            session_id: None,
            exit_code: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            worker_id: None,
            restart_policy: RestartPolicy::default(),
            restart_count: 0,
            workspace_id: None,
        }
    }

    #[test]
    fn render_shape_is_stable_with_empty_db() {
        let snap = render(&db(), Instant::now(), "0.0.0-test");
        // Every standard state must appear, zeroed.
        for s in STANDARD_STATES {
            assert!(
                snap.contains(&format!("grimoire_agents{{state=\"{}\"}} 0", s.as_str())),
                "missing zero series for {}",
                s.as_str()
            );
        }
        assert!(snap.contains("grimoire_build_info{version=\"0.0.0-test\"} 1"));
        assert!(snap.contains("grimoire_queue_depth 0"));
        assert!(snap.contains("grimoire_events_total 0"));
    }

    #[test]
    fn render_reflects_live_counts() {
        let db = db();
        db.insert_agent(&agent("a0000001", AgentState::Active))
            .unwrap();
        db.insert_agent(&agent("a0000002", AgentState::Dormant))
            .unwrap();
        db.insert_agent(&agent("a0000003", AgentState::Dormant))
            .unwrap();
        // A notification and a wake fire so the per-kind counters tick.
        db.append_event(&StreamEvent::Notification {
            agent_id: Some(AgentId::from("a0000001")),
            message: "hi".into(),
            level: "warn".into(),
            source: "agent".into(),
        })
        .unwrap();
        db.append_event(&StreamEvent::WakeSourceFired {
            wake_id: "w1".into(),
            agent_id: "a0000001".into(),
            mail_id: "m1".into(),
            via: None,
        })
        .unwrap();

        let snap = render(&db, Instant::now(), "0.0.0-test");
        assert!(snap.contains("grimoire_agents{state=\"active\"} 1"));
        assert!(snap.contains("grimoire_agents{state=\"dormant\"} 2"));
        assert!(snap.contains("grimoire_wake_fires_total 1"));
        assert!(snap.contains("grimoire_notifications_total{level=\"warn\"} 1"));
        assert!(snap.contains("grimoire_events_total 2"));
    }
}
