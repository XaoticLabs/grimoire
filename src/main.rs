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
        /// Agent ID (or prefix)
        id: String,

        /// Number of historical events to show
        #[arg(short, long)]
        tail: Option<usize>,
    },

    /// Banish (kill) an agent
    Banish {
        /// Agent ID (or prefix)
        id: String,
    },

    /// Send a follow-up message to a completed agent
    Invoke {
        /// Agent ID (or prefix)
        id: String,

        /// The follow-up message
        message: String,
    },

    /// Create or list pacts (agent chains)
    Pact {
        /// Source agent ID (omit to list all pacts)
        source_id: Option<String>,

        /// Task template for the triggered agent ({output} = source result)
        #[arg(short, long)]
        task: Option<String>,

        /// Name for the triggered agent
        #[arg(short, long)]
        name: Option<String>,

        /// List pacts instead of creating
        #[arg(short, long)]
        list: bool,
    },

    /// Show daemon status
    Status,

    /// View or edit configuration
    Tome {
        /// Config key (e.g. agent.default_model)
        key: Option<String>,

        /// Value to set (omit to read)
        value: Option<String>,
    },

    /// Open the web dashboard
    Scry,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon => {
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

        cmd => {
            if !cli::client::is_daemon_running().await {
                eprintln!("Daemon not running. Starting...");
                if let Err(e) = cli::client::auto_start_daemon() {
                    eprintln!("Failed to auto-start daemon: {}", e);
                    eprintln!("Start it manually with: grim daemon");
                    std::process::exit(1);
                }
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
                Commands::Bind { id, tail } => {
                    let id = resolve_id(&id).await;
                    cli::commands::bind::run(id, tail).await
                }
                Commands::Banish { id } => {
                    let id = resolve_id(&id).await;
                    cli::commands::banish::run(id).await
                }
                Commands::Invoke { id, message } => {
                    let id = resolve_id(&id).await;
                    cli::commands::invoke::run(id, message).await
                }
                Commands::Pact {
                    source_id,
                    task,
                    name,
                    list,
                } => {
                    let source_id = match source_id {
                        Some(id) => Some(resolve_id(&id).await),
                        None => None,
                    };
                    cli::commands::pact::run(source_id, task, name, list).await
                }
                Commands::Status => cli::commands::status::run().await,
                Commands::Tome { key, value } => cli::commands::tome::run(key, value).await,
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

/// Resolve a short ID prefix to a full agent ID
async fn resolve_id(id: &str) -> String {
    match cli::client::resolve_agent_id(id).await {
        Ok(full_id) => full_id,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
