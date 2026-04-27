use anyhow::Result;
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{
    WorkspaceCreateResult, WorkspaceDestroyResult, WorkspaceListResult,
};

#[derive(Debug, Subcommand)]
pub enum WorkspaceCommand {
    /// Provision a new workspace (git worktree under ~/.grimoire/workspaces/<name>).
    Create {
        name: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        branch: String,
    },
    /// List workspaces and their assigned agent counts.
    List {
        #[arg(long)]
        orphans: bool,
    },
    /// Destroy a workspace (refused if any non-terminal agent is assigned).
    Destroy { id: String },
    /// Show details for a single workspace.
    Show { id: String },
}

pub async fn run(cmd: WorkspaceCommand) -> Result<()> {
    match cmd {
        WorkspaceCommand::Create { name, from, branch } => run_create(&name, &from, &branch).await,
        WorkspaceCommand::List { orphans } => run_list(orphans).await,
        WorkspaceCommand::Destroy { id } => run_destroy(&id).await,
        WorkspaceCommand::Show { id } => run_show(&id).await,
    }
}

async fn run_create(name: &str, from: &str, branch: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "workspace.create",
            serde_json::json!({
                "name": name,
                "repo_path": from,
                "branch": branch,
            }),
        )
        .await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: WorkspaceCreateResult = serde_json::from_value(resp.result.unwrap())?;
    println!("{}\t{}", r.id, r.path.display());
    Ok(())
}

async fn run_list(orphans: bool) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client.call("workspace.list", serde_json::json!({})).await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: WorkspaceListResult = serde_json::from_value(resp.result.unwrap())?;
    if r.workspaces.is_empty() {
        println!("no workspaces");
    } else {
        println!("ID\tBRANCH\tAGENTS\tSTATE\tPATH");
        for w in &r.workspaces {
            println!(
                "{}\t{}\t{}\t{}\t{}",
                w.id,
                w.branch,
                w.agent_count,
                w.state.as_str(),
                w.path.display()
            );
        }
    }
    if orphans && !r.orphans.is_empty() {
        println!();
        println!("ORPHAN DIRS:");
        for o in &r.orphans {
            println!("  {}", o);
        }
    }
    Ok(())
}

async fn run_destroy(id: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call("workspace.destroy", serde_json::json!({ "id": id }))
        .await?;
    if let Some(err) = resp.error {
        if err.message.starts_with("workspace_in_use") {
            eprintln!("{} {}", "✗".red(), err.message);
            std::process::exit(2);
        }
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let _: WorkspaceDestroyResult = serde_json::from_value(resp.result.unwrap())?;
    println!("destroyed: {}", id);
    Ok(())
}

async fn run_show(id: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client.call("workspace.list", serde_json::json!({})).await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: WorkspaceListResult = serde_json::from_value(resp.result.unwrap())?;
    let ws = r.workspaces.iter().find(|w| w.id == id);
    match ws {
        Some(w) => {
            println!("id:         {}", w.id);
            println!("branch:     {}", w.branch);
            println!("state:      {}", w.state.as_str());
            println!("agents:     {}", w.agent_count);
            println!("path:       {}", w.path.display());
            println!("created_at: {}", w.created_at);
        }
        None => {
            eprintln!("workspace not found: {}", id);
            std::process::exit(4);
        }
    }
    Ok(())
}
