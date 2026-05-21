use anyhow::Result;
use colored::Colorize;

use crate::shared::config::Config;

pub async fn run(key: Option<String>, value: Option<String>) -> Result<()> {
    let config_path = Config::config_path();

    match (key, value) {
        // No args: show current config
        (None, _) => {
            let config = Config::load()?;
            println!(
                "{} {}",
                "◆".cyan(),
                config_path.display().to_string().dimmed()
            );
            println!();
            let toml = toml::to_string_pretty(&config)?;
            println!("{toml}");
        }
        (Some(key), Some(value)) => {
            let mut config = Config::load()?;

            match key.as_str() {
                "daemon.port" => {
                    config.daemon.port = value.parse()?;
                }
                "daemon.log_level" => {
                    config.daemon.log_level = value;
                }
                "daemon.socket_path" => {
                    config.daemon.socket_path = Some(value.into());
                }
                "agent.default_model" => {
                    config.agent.default_model = if value == "none" { None } else { Some(value) };
                }
                "agent.default_cwd" => {
                    config.agent.default_cwd = if value == "none" {
                        None
                    } else {
                        Some(value.into())
                    };
                }
                "agent.claude_binary" => {
                    config.agent.claude_binary = if value == "none" { None } else { Some(value) };
                }
                _ => {
                    eprintln!("{} Unknown key: {}", "Error:".red(), key);
                    eprintln!();
                    print_keys();
                    std::process::exit(1);
                }
            }

            config.save()?;
            println!(
                "{} Set {} = {}",
                "✓".green(),
                key.bold(),
                config_path.display().to_string().dimmed()
            );
        }
        // Key only: show that value
        (Some(key), None) => {
            let config = Config::load()?;
            let value = match key.as_str() {
                "daemon.port" => config.daemon.port.to_string(),
                "daemon.log_level" => config.daemon.log_level,
                "daemon.socket_path" => config
                    .daemon
                    .socket_path
                    .map_or_else(|| "(default)".to_string(), |p| p.display().to_string()),
                "agent.default_model" => config
                    .agent
                    .default_model
                    .unwrap_or_else(|| "(none)".to_string()),
                "agent.default_cwd" => config
                    .agent
                    .default_cwd
                    .map_or_else(|| "(none)".to_string(), |p| p.display().to_string()),
                "agent.claude_binary" => config
                    .agent
                    .claude_binary
                    .unwrap_or_else(|| "(default: claude)".to_string()),
                _ => {
                    eprintln!("{} Unknown key: {}", "Error:".red(), key);
                    eprintln!();
                    print_keys();
                    std::process::exit(1);
                }
            };
            println!("{value}");
        }
    }

    Ok(())
}

fn print_keys() {
    eprintln!("Available keys:");
    eprintln!("  {}  HTTP port (default: 6660)", "daemon.port".bold());
    eprintln!("  {}  Log level (default: info)", "daemon.log_level".bold());
    eprintln!("  {}  UDS socket path", "daemon.socket_path".bold());
    eprintln!(
        "  {}  Default model for new agents",
        "agent.default_model".bold()
    );
    eprintln!(
        "  {}  Default working directory",
        "agent.default_cwd".bold()
    );
    eprintln!("  {}  Path to claude binary", "agent.claude_binary".bold());
}
