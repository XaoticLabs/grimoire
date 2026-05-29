use anyhow::{Context, Result};
use chrono::DateTime;
use colored::Colorize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::cli::client::DaemonClient;
use crate::cli::stream_formatter;
use crate::shared::protocol::{ReplayEntry, ReplayResponse, StreamEvent};

/// Reconstructed agent state at the `--until` cut point. Folded from the full
/// considered window (seq 0..=cut) regardless of display filters, so the
/// footer always reflects the true state even when `--kinds`/`--no-output`
/// hide rows from the printed timeline.
#[derive(Default)]
struct ReconState {
    state: Option<String>,
    session: Option<String>,
    restarts: u32,
    wakes: u32,
    escalations: u32,
    mail_in: u32,
    mail_out: u32,
    notifications: u32,
    last_output: Option<String>,
}

impl ReconState {
    fn fold(&mut self, entry: &ReplayEntry) {
        match &entry.event {
            StreamEvent::AgentCreated { agent } => {
                self.state = Some(agent.state.to_string());
                if let Some(s) = &agent.session_id {
                    self.session = Some(s.clone());
                }
            }
            StreamEvent::StateChange { new_state, .. } => {
                self.state = Some(new_state.to_string());
            }
            StreamEvent::Restarted { .. } => self.restarts += 1,
            StreamEvent::WakeSourceFired { .. } => self.wakes += 1,
            StreamEvent::Escalated { .. } => self.escalations += 1,
            StreamEvent::MailReceived { .. } => self.mail_in += 1,
            StreamEvent::MailSent { .. } => self.mail_out += 1,
            StreamEvent::Notification { .. } => self.notifications += 1,
            StreamEvent::Output { stream, line, .. } if stream == "stdout" => {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(line)
                    && v.get("type").and_then(|t| t.as_str()) == Some("system")
                    && let Some(sid) = v.get("session_id").and_then(|s| s.as_str())
                {
                    self.session = Some(sid.to_string());
                }
                if let Some(f) = stream_formatter::format_stream_json(line) {
                    self.last_output = Some(f);
                }
            }
            _ => {}
        }
    }
}

#[allow(clippy::fn_params_excessive_bools, clippy::too_many_arguments)]
pub async fn run(
    id: &str,
    until: Option<String>,
    from: Option<i64>,
    kinds: Option<String>,
    no_output: bool,
    json: bool,
    follow: bool,
    scroll: bool,
) -> Result<()> {
    if scroll {
        return run_scroll(id, kinds, no_output, json).await;
    }

    let mut client = DaemonClient::connect().await?;
    let resp: ReplayResponse = client
        .call_typed("agent.replay", serde_json::json!({ "id": id }))
        .await?;

    let cut = match until.as_deref() {
        None => resp.entries.len(),
        Some(u) => match u.parse::<i64>() {
            Ok(seq) => resp
                .entries
                .iter()
                .position(|e| e.seq > seq)
                .unwrap_or(resp.entries.len()),
            Err(_) => resp
                .entries
                .iter()
                .position(|e| e.kind == u)
                .map_or(resp.entries.len(), |i| i + 1),
        },
    };
    let considered = &resp.entries[..cut];

    let mut recon = ReconState::default();
    for entry in considered {
        recon.fold(entry);
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&considered)?);
        return Ok(());
    }

    let kind_filter: Option<Vec<String>> = kinds.as_ref().map(|k| {
        k.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });

    let base_ts = considered
        .first()
        .and_then(|e| DateTime::parse_from_rfc3339(&e.ts).ok());

    println!(
        "{} chronicle of agent {} {}",
        "⊛".cyan(),
        resp.agent_id.bold(),
        format!("({} events)", considered.len()).dimmed()
    );
    println!();

    for entry in considered {
        if entry.seq < from.unwrap_or(i64::MIN) {
            continue;
        }
        if let Some(filter) = &kind_filter
            && !filter.iter().any(|k| k == &entry.kind)
        {
            continue;
        }
        render_entry(entry, no_output, base_ts);
    }

    print_footer(&recon, considered.last(), base_ts);

    if follow {
        // If the agent already terminated, there's nothing to follow.
        let terminal = recon
            .state
            .as_deref()
            .is_some_and(|s| matches!(s, "Complete" | "Failed" | "Banished"));
        if terminal {
            return Ok(());
        }
        println!();
        println!("{}", "── live (Ctrl-C to stop) ──".dimmed());
        follow_live(&resp.agent_id, no_output, kind_filter, base_ts).await?;
    }

    Ok(())
}

fn render_entry(
    entry: &ReplayEntry,
    no_output: bool,
    base_ts: Option<DateTime<chrono::FixedOffset>>,
) {
    let rel = rel_ts(base_ts, &entry.ts);
    match &entry.event {
        StreamEvent::Output { stream, line, .. } => {
            if no_output {
                return;
            }
            if stream == "stderr" {
                eprintln!("{}", line.dimmed());
            } else if let Some(formatted) = stream_formatter::format_stream_json(line) {
                println!("{formatted}");
            }
        }
        StreamEvent::AgentEvent { event } => {
            if no_output {
                return;
            }
            if event.event_type == "stdout" {
                if let Some(f) = stream_formatter::format_stream_json(&event.payload) {
                    println!("{f}");
                }
            } else if event.event_type == "stderr" {
                eprintln!("{}", event.payload.dimmed());
            }
        }
        other => {
            if let Some(detail) = stream_formatter::format_lifecycle(other) {
                println!(
                    "{:>4}  {:>8}  {}",
                    entry.seq.to_string().dimmed(),
                    rel.dimmed(),
                    detail
                );
            }
        }
    }
}

/// Subscribe to live events for `agent_id` via the streaming `agent.bind`
/// path. Exits when the agent reaches a terminal state or the socket closes.
/// There is a small replay-vs-subscribe race: events emitted between the
/// `agent.replay` snapshot and this subscribe may be lost. Acceptable for
/// `--follow`; documented in the help text.
async fn follow_live(
    agent_id: &str,
    no_output: bool,
    kind_filter: Option<Vec<String>>,
    base_ts: Option<DateTime<chrono::FixedOffset>>,
) -> Result<()> {
    let auth_token = crate::shared::auth::load_for_client()
        .ok()
        .map(|t| t.as_str().to_string());
    let req = crate::shared::protocol::RpcRequest {
        method: "agent.bind".to_string(),
        params: serde_json::json!({ "id": agent_id, "tail": 0_usize }),
        id: 1,
        protocol_version: None,
        auth_token,
    };
    let path = crate::shared::constants::socket_path();
    let stream = tokio::net::UnixStream::connect(&path).await?;
    let (reader, mut writer) = stream.into_split();
    let json = serde_json::to_string(&req)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    let reader = tokio::io::BufReader::new(reader);
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(event) = serde_json::from_str::<StreamEvent>(&line) else {
            continue;
        };
        // Build a synthetic ReplayEntry-shape for the formatter.
        let kind = event_kind(&event);
        if let Some(filter) = &kind_filter
            && !filter.iter().any(|k| k == &kind)
        {
            if let StreamEvent::StateChange { new_state, .. } = &event
                && new_state.is_terminal()
            {
                break;
            }
            continue;
        }
        let ts = chrono::Utc::now().to_rfc3339();
        let synthetic = ReplayEntry {
            seq: 0,
            kind: kind.clone(),
            ts,
            event: event.clone(),
        };
        render_entry(&synthetic, no_output, base_ts);
        if let StreamEvent::StateChange { new_state, .. } = &event
            && new_state.is_terminal()
        {
            break;
        }
    }
    Ok(())
}

fn event_kind(event: &StreamEvent) -> String {
    match event {
        StreamEvent::Output { .. } | StreamEvent::AgentEvent { .. } => "output".to_string(),
        StreamEvent::StateChange { .. } => "state_change".to_string(),
        StreamEvent::AgentCreated { .. } => "agent_created".to_string(),
        StreamEvent::Restarted { .. } => "restarted".to_string(),
        StreamEvent::Escalated { .. } => "escalated".to_string(),
        StreamEvent::WakeSourceFired { .. } => "wake_source_fired".to_string(),
        StreamEvent::MailSent { .. } => "mail_sent".to_string(),
        StreamEvent::MailReceived { .. } => "mail_received".to_string(),
        StreamEvent::MailDelivered { .. } => "mail_delivered".to_string(),
        StreamEvent::Notification { .. } => "notification".to_string(),
        _ => "other".to_string(),
    }
}

/// `chronicle --scroll <id>`: merge every agent's timeline within a scroll
/// into one ts-sorted view, with each line prefixed by the short agent id.
async fn run_scroll(
    scroll_id: &str,
    kinds: Option<String>,
    no_output: bool,
    json: bool,
) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let response = client
        .call("scroll.status", serde_json::json!({ "id": scroll_id }))
        .await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let value = response
        .result
        .context("daemon returned `ok` with empty result payload")?;
    let tasks = value
        .get("tasks")
        .and_then(|t| t.as_array())
        .context("scroll.status: missing tasks array")?;
    let agent_ids: Vec<String> = tasks
        .iter()
        .filter_map(|t| t.get("agent_id").and_then(|a| a.as_str()).map(String::from))
        .collect();
    if agent_ids.is_empty() {
        println!(
            "{} scroll {} has no dispatched agents yet",
            "⊛".cyan(),
            scroll_id
        );
        return Ok(());
    }

    let mut all: Vec<(String, ReplayEntry)> = Vec::new();
    for aid in &agent_ids {
        let resp: ReplayResponse = client
            .call_typed("agent.replay", serde_json::json!({ "id": aid }))
            .await?;
        for entry in resp.entries {
            all.push((resp.agent_id.clone(), entry));
        }
    }
    // Sort by ts (lexical RFC3339 sorts chronologically).
    all.sort_by(|a, b| a.1.ts.cmp(&b.1.ts));

    if json {
        let view: Vec<_> = all
            .iter()
            .map(|(aid, e)| {
                serde_json::json!({
                    "agent_id": aid,
                    "seq": e.seq,
                    "kind": e.kind,
                    "ts": e.ts,
                    "event": e.event,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&view)?);
        return Ok(());
    }

    let kind_filter: Option<Vec<String>> = kinds.map(|k| {
        k.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    });
    let base_ts = all
        .first()
        .and_then(|(_, e)| DateTime::parse_from_rfc3339(&e.ts).ok());

    println!(
        "{} chronicle of scroll {} {}",
        "⊛".cyan(),
        scroll_id.bold(),
        format!("({} agents, {} events)", agent_ids.len(), all.len()).dimmed()
    );
    println!();

    for (aid, entry) in &all {
        if let Some(filter) = &kind_filter
            && !filter.iter().any(|k| k == &entry.kind)
        {
            continue;
        }
        let short: String = aid.chars().take(8).collect();
        print!("{}  ", short.dimmed());
        render_entry(entry, no_output, base_ts);
    }
    Ok(())
}

fn rel_ts(base: Option<DateTime<chrono::FixedOffset>>, ts: &str) -> String {
    let (Some(base), Ok(this)) = (base, DateTime::parse_from_rfc3339(ts)) else {
        return String::new();
    };
    let secs = (this - base).num_milliseconds().max(0) as f64 / 1000.0;
    if secs < 60.0 {
        format!("+{secs:.1}s")
    } else {
        format!("+{}m{:02}s", (secs / 60.0) as u64, (secs % 60.0) as u64)
    }
}

fn print_footer(
    recon: &ReconState,
    last: Option<&ReplayEntry>,
    base: Option<DateTime<chrono::FixedOffset>>,
) {
    let at = last.map_or_else(
        || "start".to_string(),
        |e| format!("seq {} ({})", e.seq, rel_ts(base, &e.ts)),
    );
    println!();
    println!("{} {}", "── state at".dimmed(), format!("{at} ──").dimmed());
    println!(
        "  {:<14}{}",
        "state:",
        recon.state.as_deref().unwrap_or("?").bold()
    );
    if let Some(session) = &recon.session {
        let short = &session[..12.min(session.len())];
        println!("  {:<14}{}", "session:", short.dimmed());
    }
    println!("  {:<14}{}", "restarts:", recon.restarts);
    println!("  {:<14}{}", "wakes:", recon.wakes);
    println!("  {:<14}{}", "escalations:", recon.escalations);
    println!(
        "  {:<14}in {} / out {}",
        "mail:", recon.mail_in, recon.mail_out
    );
    println!("  {:<14}{}", "notifications:", recon.notifications);
}
