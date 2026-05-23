use anyhow::{Context, Result};
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::DaemonClient;
use crate::shared::protocol::{
    WorkspaceCreateResult, WorkspaceDestroyResult, WorkspaceFederateResult,
    WorkspaceFederateSubscribeResult, WorkspaceListResult, WorkspaceUnfederateResult,
};

#[derive(Debug, Subcommand)]
pub enum WorkspaceCommand {
    /// Provision a new workspace (git worktree under `~/.grimoire/workspaces/<name>`).
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
    /// Federate a local workspace's file events with a peer (home-side).
    /// Run on the daemon that owns the worktree. Direction: inbound | outbound | both.
    Federate {
        /// Local workspace id.
        workspace: String,
        #[arg(long)]
        peer: String,
        #[arg(long, default_value = "outbound")]
        direction: String,
    },
    /// Subscribe to a remote workspace's events (consumer-side). Creates a
    /// local "shadow" workspace pointing at `<home-daemon-id>/<home-ws-id>`.
    /// Shadows have no on-disk worktree — they exist to receive federated
    /// events and wake local agents.
    FederateSubscribe {
        /// `<home-daemon-id>/<home-workspace-id>` address of the remote workspace.
        home: String,
        #[arg(long)]
        peer: String,
        /// Optional local alias. Defaults to `<home-workspace-id>-shadow`.
        #[arg(long)]
        alias: Option<String>,
        /// Display-only branch label. Defaults to `main`.
        #[arg(long, default_value = "main")]
        branch: String,
    },
    /// Drop a workspace federation row for a peer. Run on each side
    /// independently. Does not destroy the local (shadow) workspace.
    Unfederate {
        workspace: String,
        #[arg(long)]
        peer: String,
    },
}

pub async fn run(cmd: WorkspaceCommand) -> Result<()> {
    match cmd {
        WorkspaceCommand::Create { name, from, branch } => run_create(&name, &from, &branch).await,
        WorkspaceCommand::List { orphans } => run_list(orphans).await,
        WorkspaceCommand::Destroy { id } => run_destroy(&id).await,
        WorkspaceCommand::Show { id } => run_show(&id).await,
        WorkspaceCommand::Federate {
            workspace,
            peer,
            direction,
        } => run_federate(&workspace, &peer, &direction).await,
        WorkspaceCommand::FederateSubscribe {
            home,
            peer,
            alias,
            branch,
        } => run_federate_subscribe(&home, &peer, alias.as_deref(), &branch).await,
        WorkspaceCommand::Unfederate { workspace, peer } => run_unfederate(&workspace, &peer).await,
    }
}

async fn run_federate(workspace: &str, peer: &str, direction: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "workspace.federate",
            serde_json::json!({
                "workspace": workspace,
                "peer": peer,
                "direction": direction,
            }),
        )
        .await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: WorkspaceFederateResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!(
        "Federated workspace {} with peer {} (direction={}). Run `grim workspace federate-subscribe` on {peer} to create the shadow.",
        r.workspace, peer, r.direction
    );
    Ok(())
}

async fn run_federate_subscribe(
    home: &str,
    peer: &str,
    alias: Option<&str>,
    branch: &str,
) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let mut params = serde_json::json!({
        "home": home,
        "peer": peer,
        "branch": branch,
    });
    if let Some(a) = alias {
        params["alias"] = serde_json::json!(a);
    }
    let resp = client.call("workspace.federate-subscribe", params).await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: WorkspaceFederateSubscribeResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!(
        "Created shadow workspace {} → {}/{}",
        r.local_workspace_id, r.home_daemon_id, r.home_workspace_id
    );
    Ok(())
}

async fn run_unfederate(workspace: &str, peer: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client
        .call(
            "workspace.unfederate",
            serde_json::json!({ "workspace": workspace, "peer": peer }),
        )
        .await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: WorkspaceUnfederateResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    if r.removed {
        println!("Unfederated workspace {workspace} from peer {peer}");
    } else {
        println!("No federation row found for {workspace}/{peer}");
    }
    Ok(())
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
    let r: WorkspaceCreateResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
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
    let r: WorkspaceListResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
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
            println!("  {o}");
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
    let _: WorkspaceDestroyResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("destroyed: {id}");
    Ok(())
}

async fn run_show(id: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let resp = client.call("workspace.list", serde_json::json!({})).await?;
    if let Some(err) = resp.error {
        eprintln!("{} {}", "Error:".red(), err.message);
        std::process::exit(1);
    }
    let r: WorkspaceListResult = serde_json::from_value(
        resp.result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    let ws = r.workspaces.iter().find(|w| w.id == id);
    if let Some(w) = ws {
        println!("id:         {}", w.id);
        println!("branch:     {}", w.branch);
        println!("state:      {}", w.state.as_str());
        println!("agents:     {}", w.agent_count);
        println!("path:       {}", w.path.display());
        println!("created_at: {}", w.created_at);
    } else {
        eprintln!("workspace not found: {id}");
        std::process::exit(4);
    }
    Ok(())
}
