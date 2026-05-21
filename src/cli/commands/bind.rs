use anyhow::Result;
use colored::Colorize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::cli::stream_formatter;
use crate::shared::protocol::StreamEvent;

pub async fn run(id: &str, tail: Option<usize>) -> Result<()> {
    println!("{} Binding to agent {}...", "⊛".cyan(), id.bold());

    let params = serde_json::json!({ "id": id, "tail": tail });

    // Same load-or-fall-through pattern as `client.rs`: peercred-trusted
    // UDS connections work without a token, so a missing file isn't fatal.
    let auth_token = crate::shared::auth::load_for_client()
        .ok()
        .map(|t| t.as_str().to_string());

    let req = crate::shared::protocol::RpcRequest {
        method: "agent.bind".to_string(),
        params,
        id: 1,
        protocol_version: None,
        auth_token,
    };

    let path = crate::shared::constants::socket_path();
    let stream = tokio::net::UnixStream::connect(&path).await?;
    let (reader, mut writer) = stream.into_split();

    let json = serde_json::to_string(&req)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;

    let reader = tokio::io::BufReader::new(reader);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if let Ok(event) = serde_json::from_str::<StreamEvent>(&line) {
            match event {
                StreamEvent::Output { stream, line, .. } => {
                    if stream == "stderr" {
                        eprintln!("{}", line.dimmed());
                    } else if let Some(formatted) = stream_formatter::format_stream_json(&line) {
                        // Suppress any line that the formatter declines
                        // (rate_limit notices, internal events, etc.)
                        println!("{formatted}");
                    }
                }
                StreamEvent::StateChange { ref new_state, .. } => {
                    println!(
                        "\n{} Agent state: {}",
                        "→".yellow(),
                        new_state.to_string().bold()
                    );
                    if new_state.is_terminal() {
                        break;
                    }
                }
                StreamEvent::AgentEvent { event } => {
                    if event.event_type == "stdout" {
                        if let Some(formatted) =
                            stream_formatter::format_stream_json(&event.payload)
                        {
                            println!("{formatted}");
                        }
                    } else if event.event_type == "stderr" {
                        eprintln!("{}", event.payload.dimmed());
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}
