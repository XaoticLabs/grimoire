use anyhow::Result;
use chrono::DateTime;
use colored::Colorize;

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
                // Best-effort: capture the latest session id announced by a
                // provider `system` line, and remember the last rendered line.
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

#[allow(clippy::fn_params_excessive_bools)]
pub async fn run(
    id: &str,
    until: Option<String>,
    from: Option<i64>,
    kinds: Option<String>,
    no_output: bool,
    json: bool,
) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp: ReplayResponse = client
        .call_typed("agent.replay", serde_json::json!({ "id": id }))
        .await?;

    // Determine the cut index from `--until`: an integer is an inclusive seq
    // bound; anything else is a kind name and we stop after its first match.
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

    // State footer folds the whole considered window, ignoring display filters.
    let mut recon = ReconState::default();
    for entry in considered {
        recon.fold(entry);
    }

    if json {
        // Emit the considered window verbatim (the durable rows) for piping.
        println!("{}", serde_json::to_string_pretty(&considered)?);
        return Ok(());
    }

    let kind_filter: Option<Vec<String>> = kinds.map(|k| {
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

        let rel = rel_ts(base_ts, &entry.ts);

        match &entry.event {
            StreamEvent::Output { stream, line, .. } => {
                if no_output {
                    continue;
                }
                if stream == "stderr" {
                    eprintln!("{}", line.dimmed());
                } else if let Some(formatted) = stream_formatter::format_stream_json(line) {
                    println!("{formatted}");
                }
            }
            StreamEvent::AgentEvent { event } => {
                if no_output {
                    continue;
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

    print_footer(&recon, considered.last(), base_ts);
    Ok(())
}

/// Render `ts` as an offset from the first event, e.g. `+0.4s` / `+3m12s`.
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
