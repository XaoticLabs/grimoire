use chrono::Utc;
use colored::Colorize;

use crate::shared::protocol::QueueEntry;
use crate::shared::types::{AgentState, AgentSummary, Mail};

pub fn format_state(state: &AgentState) -> String {
    match state {
        AgentState::Queued => "queued".cyan().to_string(),
        AgentState::Summoning => "summoning".yellow().to_string(),
        AgentState::Active => "active".green().bold().to_string(),
        AgentState::Complete => "complete".blue().to_string(),
        AgentState::Failed => "failed".red().to_string(),
        AgentState::Banished => "banished".magenta().to_string(),
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
    if agents.is_empty() {
        println!("{}", "No agents in the circle.".dimmed());
        return;
    }

    // Header
    println!(
        "{:<10} {:<12} {:<8} {:<6} {}",
        "ID".bold(),
        "NAME".bold(),
        "STATE".bold(),
        "AGE".bold(),
        "TASK".bold(),
    );
    println!("{}", "─".repeat(70).dimmed());

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

        println!(
            "{:<10} {:<12} {:<8} {:<6} {}",
            agent.id.dimmed(),
            name,
            format_state(&agent.state),
            format_age(agent.age_secs).dimmed(),
            task,
        );
    }
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
            m.seq, m.id, from, topic, m.state.as_str(), age, preview,
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
        ] {
            assert!(!format_state(&state).is_empty());
        }
    }

    #[test]
    fn format_state_handles_queued() {
        assert!(format_state(&AgentState::Queued).contains("queued"));
    }
}
