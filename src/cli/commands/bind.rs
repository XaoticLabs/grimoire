use anyhow::Result;
use colored::Colorize;

use crate::cli::stream_formatter;
use crate::shared::protocol::StreamEvent;

pub async fn run(id: String, tail: Option<usize>) -> Result<()> {
    println!("{} Binding to agent {}...", "⊛".cyan(), id.bold());

    let params = serde_json::json!({ "id": id, "tail": tail });

    let req = crate::shared::protocol::RpcRequest {
        method: "agent.bind".to_string(),
        params,
        id: 1,
    };

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
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
                    } else {
                        // Try to parse as stream-json, fall back to raw
                        match stream_formatter::format_stream_json(&line) {
                            Some(formatted) => println!("{}", formatted),
                            None => {} // suppressed events (rate_limit, etc.)
                        }
                    }
                }
                StreamEvent::StateChange { new_state, .. } => {
                    println!("\n{} Agent state: {}", "→".yellow(), new_state.bold());
                    if new_state == "complete" || new_state == "failed" || new_state == "banished" {
                        break;
                    }
                }
                StreamEvent::AgentEvent { event } => {
                    if event.event_type == "stdout" {
                        match stream_formatter::format_stream_json(&event.payload) {
                            Some(formatted) => println!("{}", formatted),
                            None => {}
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
