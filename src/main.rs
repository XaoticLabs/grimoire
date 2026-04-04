use clap::{Parser, Subcommand};

mod cli;
mod daemon;
mod shared;

#[derive(Parser)]
#[command(name = "grim", about = "Grimoire — AI Agent Orchestrator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the grimoire daemon
    Daemon,

    /// Summon a new agent
    Summon {
        /// The task/prompt for the agent
        task: String,

        /// Optional name for the agent
        #[arg(short, long)]
        name: Option<String>,

        /// Model to use
        #[arg(short, long)]
        model: Option<String>,
    },

    /// List all agents in the circle
    Circle {
        /// Filter by state (summoning, active, complete, failed, banished)
        #[arg(short, long)]
        state: Option<String>,
    },

    /// Bind to an agent's output stream
    Bind {
        /// Agent ID
        id: String,

        /// Number of historical events to show
        #[arg(short, long)]
        tail: Option<usize>,
    },

    /// Banish (kill) an agent
    Banish {
        /// Agent ID
        id: String,
    },

    /// Open the web dashboard
    Scry,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon => {
            // Initialize logging for daemon
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "grimoire=info".parse().unwrap()),
                )
                .init();

            if let Err(e) = daemon::start().await {
                eprintln!("Daemon error: {}", e);
                std::process::exit(1);
            }
        }

        // All CLI commands: ensure daemon is running, then dispatch
        cmd => {
            if !cli::client::is_daemon_running().await {
                eprintln!("Daemon not running. Starting...");
                if let Err(e) = cli::client::auto_start_daemon() {
                    eprintln!("Failed to auto-start daemon: {}", e);
                    eprintln!("Start it manually with: grim daemon");
                    std::process::exit(1);
                }
                // Verify it started
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                if !cli::client::is_daemon_running().await {
                    eprintln!("Daemon failed to start. Start it manually with: grim daemon");
                    std::process::exit(1);
                }
                eprintln!("Daemon started.");
            }

            let result = match cmd {
                Commands::Summon { task, name, model } => {
                    cli::commands::summon::run(task, name, model).await
                }
                Commands::Circle { state } => cli::commands::circle::run(state).await,
                Commands::Bind { id, tail } => cli::commands::bind::run(id, tail).await,
                Commands::Banish { id } => cli::commands::banish::run(id).await,
                Commands::Scry => cli::commands::scry::run().await,
                Commands::Daemon => unreachable!(),
            };

            if let Err(e) = result {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
