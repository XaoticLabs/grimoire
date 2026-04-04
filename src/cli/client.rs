use anyhow::{Context, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::shared::constants;
use crate::shared::protocol::{RpcRequest, RpcResponse};

static REQ_ID: AtomicU64 = AtomicU64::new(1);

pub struct DaemonClient {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: tokio::net::unix::OwnedWriteHalf,
}

impl DaemonClient {
    pub async fn connect() -> Result<Self> {
        let path = constants::socket_path();
        let stream = UnixStream::connect(&path)
            .await
            .with_context(|| format!("Cannot connect to daemon at {}. Is grimd running? Start it with: grim daemon", path.display()))?;

        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
        })
    }

    pub async fn call(&mut self, method: &str, params: serde_json::Value) -> Result<RpcResponse> {
        let id = REQ_ID.fetch_add(1, Ordering::Relaxed);
        let req = RpcRequest {
            method: method.to_string(),
            params,
            id,
        };

        let json = serde_json::to_string(&req)?;
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;

        let mut line = String::new();
        self.reader.read_line(&mut line).await?;

        let response: RpcResponse = serde_json::from_str(&line)
            .with_context(|| format!("Invalid response from daemon: {}", line.trim()))?;

        Ok(response)
    }

    /// Send a bind request and return lines as they come
    pub async fn bind(&mut self, method: &str, params: serde_json::Value) -> Result<()> {
        let id = REQ_ID.fetch_add(1, Ordering::Relaxed);
        let req = RpcRequest {
            method: method.to_string(),
            params,
            id,
        };

        let json = serde_json::to_string(&req)?;
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;

        let mut line = String::new();
        loop {
            line.clear();
            let n = self.reader.read_line(&mut line).await?;
            if n == 0 {
                break;
            }
            // Print the raw event line (commands will parse this)
            print!("{}", line);
        }

        Ok(())
    }
}

/// Check if daemon is running by testing the socket
pub async fn is_daemon_running() -> bool {
    DaemonClient::connect().await.is_ok()
}

/// Auto-start daemon as a background process
pub fn auto_start_daemon() -> Result<()> {
    use std::process::Command;

    let exe = std::env::current_exe()?;
    Command::new(exe)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("Failed to start daemon")?;

    // Give it a moment to start
    std::thread::sleep(std::time::Duration::from_millis(500));
    Ok(())
}
