use chrono::Utc;
use colored::Colorize;

use crate::shared::protocol::QueueEntry;
use crate::shared::types::{Agent, AgentState, AgentSummary, Mail, RestartPolicy};

/// Render an agent's `RESTART` column for `grim circle`.
/// Shows `<used>/<max>` for `OnFailure`, `-` for `Never`.
pub fn format_restart_column(agent: &Agent) -> String {
    match agent.restart_policy {
        RestartPolicy::Never => "-".to_string(),
        RestartPolicy::OnFailure => {
            // We don't have max_restarts on AgentSummary, so this column
            // shows just the lifetime count. Callers wanting the cap can
            // call `format_restart_with_cap` with a SupervisionConfig.
            format!("{}", agent.restart_count)
        }
    }
}

/// Render `<used>/<max>` for an agent given its supervision config.
pub fn format_restart_with_cap(used: u32, max: Option<u32>) -> String {
    match max {
        Some(m) => format!("{}/{}", used, m),
        None => format!("{}", used),
    }
}

pub fn format_state(state: &AgentState) -> String {
    match state {
        AgentState::Queued => "queued".cyan().to_string(),
        AgentState::Summoning => "summoning".yellow().to_string(),
        AgentState::Active => "active".green().bold().to_string(),
        AgentState::Complete => "complete".blue().to_string(),
        AgentState::Failed => "failed".red().to_string(),
        AgentState::Banished => "banished".magenta().to_string(),
        AgentState::Dormant => "dormant".bright_blue().to_string(),
        AgentState::Restarting => "restarting".yellow().to_string(),
    }
}

/// Render the `grim circle` table as plain text. Includes a `WORKER` column.
/// Worker ids are truncated to the first 6 chars; `local` for None.
pub fn circle_text(agents: &[AgentSummary]) -> String {
    let mut out = String::new();
    out.push_str("ID        STATE     WORKER   TASK\n");
    for a in agents {
        let worker = match &a.worker_id {
            Some(w) => {
                let truncated: String = w.chars().take(6).collect();
                format!("{:<8}", truncated)
            }
            None => "local   ".to_string(),
        };
        let task = a.task.as_deref().unwrap_or("");
        out.push_str(&format!(
            "{:<10}{:<10}{} {}\n",
            a.id,
            a.state.as_str(),
            worker,
            task,
        ));
    }
    out
}

pub fn format_age(secs: i64) -> String {
    if secs < 60 {
        format!("{}s", secs)
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

/// Pure text rendering for tests / non-tty consumers.
pub fn format_circle_text(agents: &[AgentSummary]) -> String {
    let mut out = String::new();
    if agents.is_empty() {
        out.push_str("No agents in the circle.\n");
        return out;
    }

    out.push_str(&format!(
        "{:<10} {:<12} {:<10} {:<8} {:<6} {}\n",
        "ID", "NAME", "STATE", "RESTART", "AGE", "TASK",
    ));
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
        out.push_str(&format!(
            "{:<10} {:<12} {:<10} {:<8} {:<6} {}\n",
            agent.id,
            name,
            agent.state.as_str(),
            restart_col,
            format_age(agent.age_secs),
            task,
        ));
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
    out.push_str(&format!(
        "{:<10} {:<8} {:<6} {:<14} {}\n",
        "ID", "LANE", "AGE", "PROVIDER", "BLOCK",
    ));
    for e in entries {
        let id_short: String = e.id.chars().take(8).collect();
        let provider = e.provider.as_deref().unwrap_or("-");
        let block = block_reason_text(e.block_reason.as_deref());
        out.push_str(&format!(
            "{:<10} {:<8} {:<6} {:<14} {}\n",
            id_short,
            e.lane,
            format_age(e.age_seconds as i64),
            provider,
            block,
        ));
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
        .map(|(m, w)| format!("{}/{}s", m, w))
        .unwrap_or_else(|| "?".into());
    out.push_str(&format!(
        "restart-policy: {} ({})\n",
        agent.restart_policy.as_str(),
        max_window
    ));
    out.push_str(&format!("restart-count: {}\n", agent.restart_count));
    if let Some(addr) = cfg.and_then(|c| c.escalate_to.as_deref()) {
        out.push_str(&format!("escalate-to: {}\n", addr));
    }
    out.push_str(&format!("escalation-depth: {}\n", escalation_depth));
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
                format!("agent://{}", prefix)
            }
            None => "-".to_string(),
        };
        let topic = m.topic.as_deref().unwrap_or("-");
        let age = format_age((now - m.created_at).max(0));
        let preview: String = m.body.chars().take(60).collect();
        let preview = preview.replace('\n', " ");
        out.push_str(&format!(
            "{}  {}  {}  {}  {}  {}  {}\n",
            m.seq,
            m.id,
            from,
            topic,
            m.state.as_str(),
            age,
            preview,
        ));
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
        assert_eq!(format_age(86400), "1d");
        assert_eq!(format_age(172800), "2d");
    }

    #[test]
    fn format_state_all_variants() {
        // Just ensure they produce non-empty strings (actual colors are terminal-dependent)
        for state in [
            AgentState::Queued,
            AgentState::Summoning,
            AgentState::Active,
            AgentState::Complete,
            AgentState::Failed,
            AgentState::Banished,
            AgentState::Dormant,
        ] {
            assert!(!format_state(&state).is_empty());
        }
    }

    #[test]
    fn format_state_handles_queued() {
        assert!(format_state(&AgentState::Queued).contains("queued"));
    }
}
