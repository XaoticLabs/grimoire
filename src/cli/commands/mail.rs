use anyhow::{Context, Result};
use clap::Subcommand;
use colored::Colorize;

use crate::cli::client::{DaemonClient, resolve_agent_id};
use crate::cli::formatters;
use crate::shared::protocol::{
    MailAckResult, MailListResult, MailSendResult, MailSubscribeResult, MailTopicsResult,
    MailUnsubscribeResult,
};
use crate::shared::types::MailState;

#[derive(Debug, Subcommand)]
pub enum MailCommand {
    /// Send mail to an `agent://<id>` or `topic://<name>` address.
    Send {
        addr: String,
        /// Body — trailing args are joined with single spaces.
        #[arg(trailing_var_arg = true, num_args = 1..)]
        body: Vec<String>,
        /// Correlation id of the mail this one replies to. Pairs with
        /// `grim ask` for synchronous request/reply.
        #[arg(long)]
        in_reply_to: Option<String>,
    },
    /// List a recipient's mailbox.
    List {
        /// Agent id (or short prefix).
        agent_id: String,
        /// Show only Pending mail.
        #[arg(long, conflicts_with = "all")]
        pending: bool,
        /// Show all states (default).
        #[arg(long, conflicts_with = "pending")]
        all: bool,
        /// Skip rows with `seq <= AFTER`.
        #[arg(long)]
        after: Option<i64>,
    },
    /// Mark a Pending mail as Delivered.
    Ack { mail_id: String },
    /// Subscribe an agent to a topic.
    Subscribe { agent_id: String, topic: String },
    /// Remove a subscription by id.
    Unsubscribe { subscription_id: String },
    /// List all topics with their subscriber counts.
    Topics,
}

pub async fn run(cmd: MailCommand) -> Result<()> {
    match cmd {
        MailCommand::Send {
            addr,
            body,
            in_reply_to,
        } => {
            let body = body.join(" ");
            run_send(&addr, &body, in_reply_to).await
        }
        MailCommand::List {
            agent_id,
            pending,
            all: _all,
            after,
        } => run_list(&agent_id, pending, after).await,
        MailCommand::Ack { mail_id } => run_ack(&mail_id).await,
        MailCommand::Subscribe { agent_id, topic } => run_subscribe(&agent_id, &topic).await,
        MailCommand::Unsubscribe { subscription_id } => run_unsubscribe(&subscription_id).await,
        MailCommand::Topics => run_topics().await,
    }
}

async fn run_send(addr: &str, body: &str, in_reply_to: Option<String>) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let mut params = serde_json::json!({ "to": addr, "body": body });
    if let Some(rt) = in_reply_to {
        params["in_reply_to"] = serde_json::Value::String(rt);
    }
    let response = client.call("mail.send", params).await?;

    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }

    let result: MailSendResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    if result.delivered == 0 && !result.mail_ids.is_empty() {
        eprintln!(
            "{} send failed (mail_id {})",
            "✗".red(),
            result.mail_ids[0].dimmed()
        );
        std::process::exit(1);
    }
    if let Some(id) = result.mail_ids.first() {
        println!("{id}");
    } else {
        println!("delivered: {}", result.delivered);
    }
    Ok(())
}

async fn run_list(agent_prefix: &str, pending_only: bool, after: Option<i64>) -> Result<()> {
    let agent_id = resolve_agent_id(agent_prefix).await?;
    let mut client = DaemonClient::connect().await?;
    let mut params = serde_json::json!({ "agent_id": agent_id });
    if pending_only {
        params["state"] = serde_json::json!(MailState::Pending);
    }
    if let Some(a) = after {
        params["after_seq"] = serde_json::json!(a);
    }
    let response = client.call("mail.list", params).await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: MailListResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    if result.mails.is_empty() {
        println!("no mail");
        return Ok(());
    }
    print!("{}", formatters::format_mail_list(&result.mails));
    Ok(())
}

async fn run_ack(prefix: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    // Resolve via mail.list-style prefix? For simplicity here, pass through;
    // RPC accepts full mail_id only. Caller must paste the full id printed by
    // `grim mail list`.
    let response = client
        .call("mail.ack", serde_json::json!({ "mail_id": prefix }))
        .await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: MailAckResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("acked: {}", result.acked);
    Ok(())
}

async fn run_subscribe(agent_prefix: &str, topic: &str) -> Result<()> {
    let agent_id = resolve_agent_id(agent_prefix).await?;
    let mut client = DaemonClient::connect().await?;
    let response = client
        .call(
            "mail.subscribe",
            serde_json::json!({ "agent_id": agent_id, "topic": topic }),
        )
        .await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: MailSubscribeResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("{}", result.subscription_id);
    Ok(())
}

async fn run_unsubscribe(subscription_id: &str) -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let response = client
        .call(
            "mail.unsubscribe",
            serde_json::json!({ "subscription_id": subscription_id }),
        )
        .await?;
    if let Some(error) = response.error {
        if error.message == "subscription_not_found" {
            println!("unsubscribed: false");
            return Ok(());
        }
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let _: MailUnsubscribeResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("unsubscribed: true");
    Ok(())
}

async fn run_topics() -> Result<()> {
    let mut client = DaemonClient::connect().await?;
    let response = client.call("mail.topics", serde_json::json!({})).await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: MailTopicsResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    if result.topics.is_empty() {
        println!("no topics");
        return Ok(());
    }
    println!("TOPIC  SUBSCRIBERS");
    for t in &result.topics {
        println!("{}  {}", t.topic, t.subscriber_count);
    }
    Ok(())
}

/// Block until a mail with `in_reply_to` matching this send arrives, or the
/// timeout expires. Pairs with `grim mail send --in-reply-to <id>` on the
/// replier side.
pub async fn run_ask(addr: &str, body: &str, timeout_secs: u64) -> Result<()> {
    use crate::shared::protocol::MailAskResult;
    let mut client = DaemonClient::connect().await?;
    let params = serde_json::json!({
        "to": addr,
        "body": body,
        "timeout_ms": timeout_secs * 1000,
    });
    let response = client.call("mail.ask", params).await?;
    if let Some(error) = response.error {
        eprintln!("{} {}", "Error:".red(), error.message);
        std::process::exit(1);
    }
    let result: MailAskResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` with empty result payload")?,
    )?;
    println!("{}", result.reply.body);
    Ok(())
}
