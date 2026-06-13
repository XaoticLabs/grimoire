use anyhow::{Context, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::shared::auth;
use crate::shared::constants;
use crate::shared::protocol::{RpcRequest, RpcResponse};

static REQ_ID: AtomicU64 = AtomicU64::new(1);

pub struct DaemonClient {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: tokio::net::unix::OwnedWriteHalf,
    /// `None` if no token loaded; peercred trust may still authorize us.
    auth_token: Option<String>,
}

impl DaemonClient {
    pub async fn connect() -> Result<Self> {
        let path = constants::socket_path();
        let stream = UnixStream::connect(&path).await.with_context(|| {
            format!(
                "Cannot connect to daemon at {}. Is grimd running? Start it with: grim daemon",
                path.display()
            )
        })?;

        // Best-effort: same-UID UDS connections get a peercred bypass server-side,
        // so a missing token is fine; cross-UID calls without one get "unauthenticated".
        let auth_token = auth::load_for_client().ok().map(|t| t.as_str().to_string());

        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: BufReader::new(reader),
            writer,
            auth_token,
        })
    }

    /// Send `method` with `params` and decode the OK payload as `T`.
    /// Every error path carries the method name for grep-ability.
    pub async fn call_typed<T: serde::de::DeserializeOwned>(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T> {
        let response = self.call(method, params).await?;
        if let Some(err) = response.error {
            anyhow::bail!("daemon returned error for `{method}`: {}", err.message);
        }
        let raw = response.result.with_context(|| {
            format!("daemon returned `ok` for `{method}` but result payload was empty")
        })?;
        serde_json::from_value(raw)
            .with_context(|| format!("decoding `{method}` result payload from daemon"))
    }

    pub async fn call(&mut self, method: &str, params: serde_json::Value) -> Result<RpcResponse> {
        let id = REQ_ID.fetch_add(1, Ordering::Relaxed);
        let req = RpcRequest {
            method: method.to_string(),
            params,
            id,
            protocol_version: None,
            auth_token: self.auth_token.clone(),
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
}

/// Resolve a short ID prefix to a unique full agent ID.
pub async fn resolve_agent_id(prefix: &str) -> Result<String> {
    let mut client = DaemonClient::connect().await?;
    let response = client.call("agent.circle", serde_json::json!({})).await?;

    if let Some(error) = response.error {
        anyhow::bail!("Failed to list agents: {}", error.message);
    }

    let result: crate::shared::protocol::CircleResult = serde_json::from_value(
        response
            .result
            .context("daemon returned `ok` for `agent.circle` with empty payload")?,
    )?;

    let matches: Vec<_> = result
        .agents
        .iter()
        .filter(|a| a.id.starts_with(prefix))
        .collect();

    match matches.len() {
        0 => anyhow::bail!("No agent matching '{prefix}'"),
        1 => Ok(matches[0].id.clone()),
        n => {
            let ids: Vec<_> = matches.iter().map(|a| a.id.as_str()).collect();
            anyhow::bail!(
                "Ambiguous prefix '{}' matches {} agents: {}",
                prefix,
                n,
                ids.join(", ")
            )
        }
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

    Ok(())
}
