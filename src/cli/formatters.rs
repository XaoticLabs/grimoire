use colored::Colorize;

use crate::shared::types::{AgentState, AgentSummary};

pub fn format_state(state: &AgentState) -> String {
    match state {
        AgentState::Summoning => "summoning".yellow().to_string(),
        AgentState::Active => "active".green().bold().to_string(),
        AgentState::Complete => "complete".blue().to_string(),
        AgentState::Failed => "failed".red().to_string(),
        AgentState::Banished => "banished".magenta().to_string(),
    }
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
