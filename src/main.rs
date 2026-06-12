#![cfg_attr(not(test), forbid(unsafe_code))]
#![cfg_attr(not(test), warn(clippy::unwrap_used))]
// `grim` is the CLI entry point, printing diagnostics/usage hints to the
// user's terminal is its job. Library/daemon code is still gated by the
// global `print_stdout`/`print_stderr` lints.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::time::Duration;

use clap::{Parser, Subcommand};

/// How long the CLI waits after auto-spawning the daemon before retrying
/// the socket connection. Tuned so a healthy daemon is usually up by the
/// first retry without making CLI startup feel laggy when the daemon is
/// already running.
const DAEMON_AUTOSTART_POLL: Duration = Duration::from_millis(500);

pub mod cli;
pub mod daemon;
pub mod shared;

#[derive(Parser)]
#[command(name = "grim", about = "Grimoire AI Agent Orchestrator")]
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

        /// Provider to use (e.g. claude, pi, aider)
        #[arg(short, long)]
        provider: Option<String>,

        /// Working directory for the agent (defaults to the daemon's
        /// `agent.default_cwd`, then the daemon's own cwd). Mutually
        /// exclusive with --workspace.
        #[arg(long, conflicts_with = "workspace")]
        cwd: Option<std::path::PathBuf>,

        /// Park the agent in Dormant after it finishes its first run, so
        /// future wake sources or `grim invoke` can resume it.
        #[arg(short = 'k', long)]
        keep_alive: bool,

        /// Restart policy: "never" (default) or "on_failure".
        #[arg(long, default_value = "never")]
        restart: String,

        /// Restart budget in `<N>/<T>s` format, e.g. `3/60s`.
        #[arg(long)]
        max_restarts: Option<String>,

        /// Address (`agent://<id>` or `topic://<name>`) to escalate to when
        /// the restart budget is exhausted.
        #[arg(long)]
        escalate_to: Option<String>,

        /// Workspace name to summon into. Mutually exclusive with `--cwd`.
        #[arg(long)]
        workspace: Option<String>,

        /// Supervision-tree parent (agent id or prefix). When set, banishing
        /// the parent cascades to this agent and any of its descendants.
        #[arg(long)]
        parent: Option<String>,

        /// USD ceiling for the supervision tree rooted at this agent.
        /// When the tree's summed spend reaches the cap, dispatches and
        /// wakes anywhere in the tree are blocked and you're notified once.
        #[arg(long, value_name = "USD")]
        tree_budget_usd: Option<f64>,

        /// Idempotency key. A repeat summon with the same key returns the
        /// agent the first call minted instead of spawning a duplicate.
        #[arg(long)]
        idempotency_key: Option<String>,
    },

    /// List all agents in the circle
    Circle {
        /// Filter by state (summoning, active, complete, failed, banished)
        #[arg(short, long)]
        state: Option<String>,
        /// Show only agents whose latest eval score is < this value (0.0–1.0).
        /// Unevaluated agents are excluded.
        #[arg(long, value_name = "SCORE")]
        eval_score_lt: Option<f64>,
        /// Show only agents whose latest eval score is >= this value.
        #[arg(long, value_name = "SCORE")]
        eval_score_gte: Option<f64>,
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

    /// Score an agent's transcript against a rubric using an evaluator agent.
    Eval {
        /// Target agent ID (or prefix) to evaluate
        id: String,

        /// Path to the rubric markdown file. Required unless `--list` is set.
        #[arg(short, long)]
        rubric: Option<String>,

        /// Override the evaluator's provider. Defaults to the daemon default.
        #[arg(short, long)]
        provider: Option<String>,

        /// Override the evaluator's model.
        #[arg(short, long)]
        model: Option<String>,

        /// Optional name for the evaluator agent. Defaults to `eval:<short-target-id>`.
        #[arg(short, long)]
        name: Option<String>,

        /// Return immediately after summoning the evaluator. Default is to
        /// block until the evaluator finishes and print its parsed score.
        #[arg(long)]
        no_wait: bool,

        /// How long to wait for the evaluator to finish, in seconds.
        #[arg(long, default_value_t = 300)]
        timeout: u64,

        /// List recorded evaluations for the target instead of running a
        /// new one. Mutually exclusive with `--rubric` in practice.
        #[arg(long)]
        list: bool,
    },

    /// Fork an agent at a point in its history: spawn a new agent seeded
    /// with the parent's output up to the cut.
    Fork {
        /// Parent agent ID (or prefix)
        id: String,

        /// Cut point. An event seq (inclusive) or kind name (stop after
        /// the first occurrence). Defaults to the parent's full life.
        #[arg(long)]
        at: Option<String>,

        /// Override the fork's task. Defaults to reusing the parent's task.
        #[arg(long)]
        task: Option<String>,

        /// Override the fork's provider. Defaults to the parent's.
        #[arg(short, long)]
        provider: Option<String>,

        /// Override the fork's model. Defaults to the parent's.
        #[arg(short, long)]
        model: Option<String>,

        /// Optional name for the fork.
        #[arg(short, long)]
        name: Option<String>,
    },

    /// Show an agent's artifact: files changed, unified diff, and cost.
    Artifact {
        /// Agent ID (or prefix)
        id: String,

        /// Emit the raw artifact record as JSON.
        #[arg(long)]
        json: bool,

        /// Print the full unified diff after the summary.
        #[arg(long)]
        diff: bool,
    },

    /// Replay an agent's full life from the durable event log
    #[command(visible_alias = "replay")]
    Chronicle {
        /// Agent ID (or prefix)
        id: String,

        /// Stop at a point: an event seq (inclusive) or a kind name
        /// (stops after its first occurrence), e.g. `--until escalated`.
        #[arg(long)]
        until: Option<String>,

        /// Skip events below this seq in the printed timeline.
        #[arg(long)]
        from: Option<i64>,

        /// Comma-separated event kinds to show, e.g.
        /// `--kinds wake_source_fired,restarted,escalated`.
        #[arg(long)]
        kinds: Option<String>,

        /// Hide stdout/stderr output lines; show only lifecycle events.
        #[arg(long)]
        no_output: bool,

        /// Emit the raw durable event rows as JSON.
        #[arg(long)]
        json: bool,

        /// After the history, keep streaming live events until the agent
        /// reaches a terminal state. Mutually exclusive with `--scroll`.
        #[arg(long, conflicts_with = "scroll")]
        follow: bool,

        /// Replay the merged timelines of every agent in the named scroll.
        /// When set, `id` is interpreted as a scroll id.
        #[arg(long)]
        scroll: bool,
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

    /// Inscribe a scroll (load a spec for orchestrated execution)
    Inscribe {
        /// Path to the spec markdown file
        spec: String,

        /// Max concurrent agents
        #[arg(short = 'c', long, default_value = "4")]
        concurrency: u32,

        /// Immediately activate after inscribing
        #[arg(short, long)]
        activate: bool,
    },

    /// View scroll status or control scrolls
    Scroll {
        /// Scroll ID (omit to list all)
        id: Option<String>,

        /// Activate a scroll
        #[arg(long)]
        activate: bool,

        /// Abandon a scroll
        #[arg(long)]
        abandon: bool,

        /// F5a: dispatch one task of this scroll to a peer instead of
        /// running it locally. Pair with `--to <peer>`. The receiver
        /// must have `peer set-accept-dispatch` enabled for the
        /// coordinator.
        #[arg(long = "dispatch-task", requires_all = ["id", "to"])]
        dispatch_task: Option<String>,

        /// Peer name to dispatch the task to. Used with
        /// `--dispatch-task`.
        #[arg(long)]
        to: Option<String>,

        /// HITL: approve a held task (by name or id) so the DAG schedules
        /// it. Requires the scroll `id`.
        #[arg(long, value_name = "TASK", requires = "id")]
        approve: Option<String>,

        /// HITL: reject a held task (by name or id), failing it and
        /// skipping everything downstream. Requires the scroll `id`.
        #[arg(long, value_name = "TASK", requires = "id")]
        reject: Option<String>,
    },

    /// Show daemon status
    Status {
        /// Emit raw JSON instead of human text
        #[arg(long)]
        json: bool,
    },

    /// Show pending queued work, oldest first
    Queue {
        /// Emit raw JSON instead of human text
        #[arg(long)]
        json: bool,
    },

    /// List OS processes backing live agents. Flags "stuck" rows where the
    /// agent is terminal but its process is still alive.
    Ps {
        /// Emit raw JSON instead of human text
        #[arg(long)]
        json: bool,
    },

    /// View or edit configuration
    Tome {
        /// Config key (e.g. agent.default_model)
        key: Option<String>,

        /// Value to set (omit to read)
        value: Option<String>,
    },

    /// Open the web dashboard
    Scry,

    /// Inspect daily USD budgets configured under `[budgets.*]`.
    Budget {
        #[command(subcommand)]
        cmd: cli::commands::budget::BudgetCommand,
    },

    /// Agent-to-agent messaging (send/list/ack/subscribe/unsubscribe/topics).
    Mail {
        #[command(subcommand)]
        cmd: cli::commands::mail::MailCommand,
    },

    /// Send a mail and block until a reply arrives (or the timeout fires).
    /// The reply is any subsequent mail whose `in_reply_to` matches the id
    /// of the mail sent here. Pairs with `grim mail send --in-reply-to <id>`.
    Ask {
        /// Recipient address (`agent://<id>` or `topic://<name>`).
        addr: String,
        /// Body. Trailing args are joined with single spaces.
        #[arg(trailing_var_arg = true, num_args = 1..)]
        body: Vec<String>,
        /// How long to wait for a reply, in seconds. Defaults to 30.
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },

    /// Post a tender (auction) to a topic or agent and collect every bid
    /// received within the deadline. Subscribers reply with
    /// `grim mail send --in-reply-to <id>`; this command lists the bids in
    /// arrival order so the caller can pick a winner.
    Tender {
        /// Recipient address (`agent://<id>` or `topic://<name>`).
        addr: String,
        /// Body. Trailing args are joined with single spaces.
        #[arg(trailing_var_arg = true, num_args = 1..)]
        body: Vec<String>,
        /// How long to collect bids, in seconds. Defaults to 30.
        #[arg(long, default_value_t = 30)]
        deadline: u64,
    },

    /// Manage agent wake sources (cron / file-watch / parent-completion).
    Wake {
        #[command(subcommand)]
        cmd: cli::commands::wake::WakeCommand,
    },

    /// Manage workspaces (named git worktrees with shared memory + filewatch).
    Workspace {
        #[command(subcommand)]
        cmd: cli::commands::workspace::WorkspaceCommand,
    },

    /// Read/write workspace memory KV.
    Memory {
        #[command(subcommand)]
        cmd: cli::commands::memory::MemoryCommand,
    },

    /// Federated namespace memory (cross-daemon shared KV).
    Ns {
        #[command(subcommand)]
        cmd: cli::commands::ns::NsCommand,
    },

    /// Manage federation peers (`add` / `list` / `remove` / `ping`).
    Peer {
        #[command(subcommand)]
        cmd: cli::commands::peer::PeerCommand,
    },

    /// Federate / unfederate topics across peers.
    Topic {
        #[command(subcommand)]
        cmd: cli::commands::topic::TopicCommand,
    },

    /// Emit an operator-facing notification (forwarded to the configured
    /// webhook). Agents call this to surface something worth a human's
    /// attention; the agent id is read from `GRIMOIRE_AGENT_ID`.
    Notify {
        /// The notification message
        message: String,

        /// Severity hint (e.g. info, warn, error). Defaults to info.
        #[arg(short, long)]
        level: Option<String>,
    },

    /// Scaffold a working standing-agent flow in one command. Prints each
    /// underlying `grim` action it runs. Demos: `standing-review`.
    Demo {
        /// Which demo to set up (e.g. standing-review).
        name: String,

        /// Repository to watch (defaults to the current directory).
        #[arg(long)]
        repo: Option<std::path::PathBuf>,

        /// Provider for the agent (e.g. claude, pi, opencode, aider).
        #[arg(short, long)]
        provider: Option<String>,
    },

    /// Serve the daemon to MCP clients over stdio (tools + the Tasks
    /// primitive). Add to an MCP host as: command `grim`, args `["mcp"]`.
    Mcp,

    /// Print shell completions to stdout (bash, zsh, fish, elvish, powershell)
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },

    /// Render man pages for grim and every subcommand into a directory.
    /// Used by packaging; hidden from --help.
    #[command(hide = true)]
    Mangen {
        /// Output directory for the roff pages
        #[arg(long, default_value = "man")]
        dir: std::path::PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Daemon => {
            daemon::observability::init();

            let result = daemon::start().await;
            daemon::observability::shutdown();
            if let Err(e) = result {
                eprintln!("Daemon error: {e}");
                std::process::exit(1);
            }
        }

        // Pure-local commands: no daemon needed, must not auto-start one.
        Commands::Completions { shell } => {
            use clap::CommandFactory;
            clap_complete::generate(shell, &mut Cli::command(), "grim", &mut std::io::stdout());
        }
        Commands::Mangen { dir } => {
            use clap::CommandFactory;
            let render = std::fs::create_dir_all(&dir)
                .map_err(anyhow::Error::from)
                .and_then(|()| clap_mangen::generate_to(Cli::command(), &dir).map_err(Into::into));
            match render {
                Ok(()) => println!("man pages written to {}", dir.display()),
                Err(e) => {
                    eprintln!("Error: {e}");
                    std::process::exit(1);
                }
            }
        }

        cmd => {
            if !cli::client::is_daemon_running().await {
                eprintln!("Daemon not running. Starting...");
                if let Err(e) = cli::client::auto_start_daemon() {
                    eprintln!("Failed to auto-start daemon: {e}");
                    eprintln!("Start it manually with: grim daemon");
                    std::process::exit(1);
                }
                tokio::time::sleep(DAEMON_AUTOSTART_POLL).await;
                if !cli::client::is_daemon_running().await {
                    eprintln!("Daemon failed to start. Start it manually with: grim daemon");
                    std::process::exit(1);
                }
                eprintln!("Daemon started.");
            }

            let result = match cmd {
                Commands::Summon {
                    task,
                    name,
                    model,
                    provider,
                    cwd,
                    keep_alive,
                    restart,
                    max_restarts,
                    escalate_to,
                    workspace,
                    parent,
                    tree_budget_usd,
                    idempotency_key,
                } => {
                    cli::commands::summon::run(
                        &task,
                        name,
                        model,
                        provider,
                        cwd,
                        keep_alive,
                        &restart,
                        max_restarts,
                        escalate_to,
                        workspace,
                        parent,
                        tree_budget_usd,
                        idempotency_key,
                    )
                    .await
                }
                Commands::Circle {
                    state,
                    eval_score_lt,
                    eval_score_gte,
                } => cli::commands::circle::run(state, eval_score_lt, eval_score_gte).await,
                Commands::Bind { id, tail } => {
                    let id = resolve_id(&id).await;
                    cli::commands::bind::run(&id, tail).await
                }
                Commands::Banish { id } => {
                    let id = resolve_id(&id).await;
                    cli::commands::banish::run(&id).await
                }
                Commands::Chronicle {
                    id,
                    until,
                    from,
                    kinds,
                    no_output,
                    json,
                    follow,
                    scroll,
                } => {
                    // Don't resolve agent-id prefixes when targeting a scroll.
                    let resolved = if scroll { id } else { resolve_id(&id).await };
                    cli::commands::chronicle::run(
                        &resolved, until, from, kinds, no_output, json, follow, scroll,
                    )
                    .await
                }
                Commands::Eval {
                    id,
                    rubric,
                    provider,
                    model,
                    name,
                    no_wait,
                    timeout,
                    list,
                } => {
                    let id = resolve_id(&id).await;
                    if list {
                        cli::commands::eval::run_list(&id).await
                    } else {
                        match rubric {
                            Some(rubric) => {
                                cli::commands::eval::run(
                                    &id, &rubric, provider, model, name, !no_wait, timeout,
                                )
                                .await
                            }
                            None => {
                                Err(anyhow::anyhow!("--rubric is required unless --list is set"))
                            }
                        }
                    }
                }
                Commands::Fork {
                    id,
                    at,
                    task,
                    provider,
                    model,
                    name,
                } => {
                    let id = resolve_id(&id).await;
                    cli::commands::fork::run(&id, at, task, provider, model, name).await
                }
                Commands::Artifact { id, json, diff } => {
                    let id = resolve_id(&id).await;
                    cli::commands::artifact::run(&id, json, diff).await
                }
                Commands::Invoke { id, message } => {
                    let id = resolve_id(&id).await;
                    cli::commands::invoke::run(&id, &message).await
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
                Commands::Inscribe {
                    spec,
                    concurrency,
                    activate,
                } => cli::commands::inscribe::run(&spec, concurrency, activate).await,
                Commands::Scroll {
                    id,
                    activate,
                    abandon,
                    dispatch_task,
                    to,
                    approve,
                    reject,
                } => {
                    cli::commands::scroll::run(
                        id,
                        activate,
                        abandon,
                        dispatch_task,
                        to,
                        approve,
                        reject,
                    )
                    .await
                }
                Commands::Status { json } => cli::commands::status::run(json).await,
                Commands::Queue { json } => cli::commands::queue::run(json).await,
                Commands::Ps { json } => cli::commands::ps::run(json).await,
                Commands::Tome { key, value } => cli::commands::tome::run(key, value).await,
                Commands::Scry => cli::commands::scry::run().await,
                Commands::Budget { cmd } => cli::commands::budget::run(cmd).await,
                Commands::Mail { cmd } => cli::commands::mail::run(cmd).await,
                Commands::Ask {
                    addr,
                    body,
                    timeout,
                } => cli::commands::mail::run_ask(&addr, &body.join(" "), timeout).await,
                Commands::Tender {
                    addr,
                    body,
                    deadline,
                } => cli::commands::mail::run_tender(&addr, &body.join(" "), deadline).await,
                Commands::Wake { cmd } => cli::commands::wake::run(cmd).await,
                Commands::Workspace { cmd } => cli::commands::workspace::run(cmd).await,
                Commands::Memory { cmd } => cli::commands::memory::run(cmd).await,
                Commands::Ns { cmd } => cli::commands::ns::run(cmd).await,
                Commands::Peer { cmd } => cli::commands::peer::run(cmd).await,
                Commands::Topic { cmd } => cli::commands::topic::run(cmd).await,
                Commands::Mcp => cli::commands::mcp::run().await,
                Commands::Notify { message, level } => {
                    cli::commands::notify::run(&message, level).await
                }
                Commands::Demo {
                    name,
                    repo,
                    provider,
                } => cli::commands::demo::run(&name, repo, provider).await,
                // `Daemon`, `Completions`, and `Mangen` are dispatched by the
                // outer match arms above and never reach this client dispatch.
                Commands::Daemon | Commands::Completions { .. } | Commands::Mangen { .. } => {
                    unreachable!("handled by outer match arm")
                }
            };

            if let Err(e) = result {
                eprintln!("Error: {e}");
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
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}
