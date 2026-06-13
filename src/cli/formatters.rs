use std::fmt::Write as _;

use chrono::Utc;

use crate::shared::protocol::QueueEntry;
use crate::shared::types::{Agent, AgentSummary, Mail, RestartPolicy};

/// Render `<used>/<max>` for an agent given its supervision config.
pub fn format_restart_with_cap(used: u32, max: Option<u32>) -> String {
    match max {
        Some(m) => format!("{used}/{m}"),
        None => format!("{used}"),
    }
}

/// Render the `grim circle` table as plain text with a `WORKER` column.
pub fn circle_text(agents: &[AgentSummary]) -> String {
    let mut out = String::new();
    out.push_str("ID        STATE     WORKER   TASK\n");
    for a in agents {
        let worker = match &a.worker_id {
            Some(w) => {
                let truncated: String = w.chars().take(6).collect();
                format!("{truncated:<8}")
            }
            None => "local   ".to_string(),
        };
        let task = a.task.as_deref().unwrap_or("");
        let _ = writeln!(
            out,
            "{:<10}{:<10}{} {}",
            a.id,
            a.state.as_str(),
            worker,
            task,
        );
    }
    out
}

pub fn format_age(secs: i64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86400)
    }
}

pub fn format_circle(agents: &[AgentSummary]) {
    println!("{}", format_circle_text(agents));
}

/// Render the circle with an extra `SCORE` column (latest eval verdict).
pub fn format_circle_with_scores<S: std::hash::BuildHasher>(
    agents: &[AgentSummary],
    scores: &std::collections::HashMap<String, f64, S>,
) {
    if agents.is_empty() {
        println!("No agents match the filter.");
        return;
    }
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<10} {:<12} {:<10} {:<8} {:<6} {:<6} TASK",
        "ID", "NAME", "STATE", "RESTART", "AGE", "SCORE",
    );
    out.push_str(&"-".repeat(76));
    out.push('\n');
    for agent in agents {
        let name = agent
            .name
            .as_deref()
            .unwrap_or("-")
            .chars()
            .take(10)
            .collect::<String>();
        let task = agent
            .task
            .as_deref()
            .unwrap_or("-")
            .chars()
            .take(35)
            .collect::<String>();
        let restart_col = match agent.restart_policy {
            RestartPolicy::Never => "-".to_string(),
            RestartPolicy::OnFailure => {
                format_restart_with_cap(agent.restart_count, agent.max_restarts)
            }
        };
        let score_col = scores
            .get(&agent.id)
            .map_or_else(|| "-".to_string(), |s| format!("{s:.2}"));
        let _ = writeln!(
            out,
            "{:<10} {:<12} {:<10} {:<8} {:<6} {:<6} {}",
            agent.id,
            name,
            agent.state.as_str(),
            restart_col,
            format_age(agent.age_secs),
            score_col,
            task,
        );
    }
    print!("{out}");
}

/// Pure text rendering for tests / non-tty consumers.
pub fn format_circle_text(agents: &[AgentSummary]) -> String {
    let mut out = String::new();
    if agents.is_empty() {
        out.push_str("No agents in the circle.\n");
        return out;
    }

    let _ = writeln!(
        out,
        "{:<10} {:<12} {:<10} {:<8} {:<6} TASK",
        "ID", "NAME", "STATE", "RESTART", "AGE",
    );
    out.push_str(&"-".repeat(70));
    out.push('\n');

    for agent in agents {
        let name = agent
            .name
            .as_deref()
            .unwrap_or("-")
            .chars()
            .take(10)
            .collect::<String>();
        let task = agent
            .task
            .as_deref()
            .unwrap_or("-")
            .chars()
            .take(35)
            .collect::<String>();
        let restart_col = match agent.restart_policy {
            RestartPolicy::Never => "-".to_string(),
            RestartPolicy::OnFailure => {
                format_restart_with_cap(agent.restart_count, agent.max_restarts)
            }
        };
        let _ = writeln!(
            out,
            "{:<10} {:<12} {:<10} {:<8} {:<6} {}",
            agent.id,
            name,
            agent.state.as_str(),
            restart_col,
            format_age(agent.age_secs),
            task,
        );
    }
    out
}

/// Render a block_reason value into the column shown by `grim queue`.
fn block_reason_text(reason: Option<&str>) -> &'static str {
    match reason {
        Some("capacity") => "capacity",
        Some("no_eligible_worker") => "no worker",
        Some(_) => "other",
        None => "-",
    }
}

pub fn format_queue(entries: &[QueueEntry]) -> String {
    if entries.is_empty() {
        return "No queued work.\n".to_string();
    }

    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<10} {:<8} {:<6} {:<14} BLOCK",
        "ID", "LANE", "AGE", "PROVIDER",
    );
    for e in entries {
        let id_short: String = e.id.chars().take(8).collect();
        let provider = e.provider.as_deref().unwrap_or("-");
        let block = block_reason_text(e.block_reason.as_deref());
        let _ = writeln!(
            out,
            "{:<10} {:<8} {:<6} {:<14} {}",
            id_short,
            e.lane,
            format_age(e.age_seconds as i64),
            provider,
            block,
        );
    }
    out
}

/// Render the supervision block shown by `grim status <id>` for an agent
/// with an active restart policy. Returns an empty string for `Never`.
pub fn format_status_supervision_block(
    agent: &Agent,
    cfg: Option<&crate::shared::types::SupervisionConfig>,
    escalation_depth: u32,
) -> String {
    if agent.restart_policy == RestartPolicy::Never {
        return String::new();
    }
    let mut out = String::new();
    let max_window = cfg
        .and_then(|c| c.max_restarts.zip(c.window_secs))
        .map_or_else(|| "?".into(), |(m, w)| format!("{m}/{w}s"));
    let _ = writeln!(
        out,
        "restart-policy: {} ({})",
        agent.restart_policy.as_str(),
        max_window
    );
    let _ = writeln!(out, "restart-count: {}", agent.restart_count);
    if let Some(addr) = cfg.and_then(|c| c.escalate_to.as_deref()) {
        let _ = writeln!(out, "escalate-to: {addr}");
    }
    let _ = writeln!(out, "escalation-depth: {escalation_depth}");
    out
}

/// Render a mail listing. Columns: SEQ  ID  FROM  TOPIC  STATE  AGE  PREVIEW.
pub fn format_mail_list(mails: &[Mail]) -> String {
    let mut out = String::new();
    out.push_str("SEQ  ID  FROM  TOPIC  STATE  AGE  PREVIEW\n");
    let now = Utc::now().timestamp();
    for m in mails {
        let from = match &m.sender_id {
            Some(id) => {
                let prefix: String = id.chars().take(8).collect();
                format!("agent://{prefix}")
            }
            None => "-".to_string(),
        };
        let topic = m.topic.as_deref().unwrap_or("-");
        let age = format_age((now - m.created_at).max(0));
        let preview: String = m.body.chars().take(60).collect();
        let preview = preview.replace('\n', " ");
        let _ = writeln!(
            out,
            "{}  {}  {}  {}  {}  {}  {}",
            m.seq,
            m.id,
            from,
            topic,
            m.state.as_str(),
            age,
            preview,
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_age_seconds() {
        assert_eq!(format_age(0), "0s");
        assert_eq!(format_age(30), "30s");
        assert_eq!(format_age(59), "59s");
    }

    #[test]
    fn format_age_minutes() {
        assert_eq!(format_age(60), "1m");
        assert_eq!(format_age(3599), "59m");
    }

    #[test]
    fn format_age_hours() {
        assert_eq!(format_age(3600), "1h");
        assert_eq!(format_age(86399), "23h");
    }

    #[test]
    fn format_age_days() {
        assert_eq!(format_age(86_400), "1d");
        assert_eq!(format_age(172_800), "2d");
    }
}
