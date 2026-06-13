//! Unix-domain-socket JSON-RPC server: connection loop, peercred/token auth
//! gate, the `agent.bind` event stream, and the framed response writer.

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tracing::{info, warn};

use crate::shared::auth::AuthToken;
use crate::shared::constants;
use crate::shared::protocol::*;

use super::super::rpc;
use super::AppState;

/// UID the daemon runs as, used by the peer-credentials check: the owning user
/// is trusted (no token); a different UID must present a valid token.
#[cfg(unix)]
fn daemon_uid() -> u32 {
    nix::unistd::Uid::current().as_raw()
}

#[cfg(not(unix))]
fn daemon_uid() -> u32 {
    0
}

pub(super) async fn run_uds_server(state: AppState) -> Result<()> {
    let socket_path = constants::socket_path();

    if socket_path.exists() {
        std::fs::remove_file(&socket_path)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    // Lock the socket to the owning user (0o600). Belt-and-braces with the
    // per-connection peercred check: other UIDs can't open it, and couldn't
    // pass the in-band check if they did.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) =
            std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))
        {
            warn!(error = %e, path = %socket_path.display(), "failed to set socket permissions");
        }
    }
    let owner_uid = daemon_uid();
    info!(path = %socket_path.display(), uid = owner_uid, "UDS server listening");

    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();

        // Owning-user connections are trusted via kernel peer credentials;
        // other UIDs must carry a valid `auth_token` on the first RPC.
        let peercred_trusted = match stream.peer_cred() {
            Ok(cred) => cred.uid() == owner_uid,
            Err(e) => {
                warn!(error = %e, "could not read SO_PEERCRED; falling back to token auth");
                false
            }
        };

        tokio::spawn(async move {
            let (reader, mut writer) = stream.into_split();
            let reader = BufReader::new(reader);
            let mut lines = reader.lines();
            // Sticky per-connection auth: the token is checked once, not per RPC.
            let mut authed = peercred_trusted;

            while let Ok(Some(line)) = lines.next_line().await {
                let req: RpcRequest = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = RpcResponse::error(0, -32700, format!("Parse error: {e}"));
                        let _ = write_response(&mut writer, &err).await;
                        continue;
                    }
                };

                // Auth gate: peercred-trusted connections skip it; others must
                // present a matching token on the first RPC, then stay authed.
                match check_uds_auth(authed, req.auth_token.as_deref(), &state.auth_token) {
                    UdsAuthDecision::Pass => {
                        authed = true;
                    }
                    UdsAuthDecision::Reject => {
                        let err = RpcResponse::error(req.id, -32000, "unauthenticated".to_string());
                        let _ = write_response(&mut writer, &err).await;
                        // Close on failed auth; retries on the same socket would
                        // be a brute-force vector.
                        return;
                    }
                }

                if req.method == "agent.bind" {
                    let params: BindParams = match serde_json::from_value(req.params.clone()) {
                        Ok(p) => p,
                        Err(e) => {
                            let err =
                                RpcResponse::error(req.id, -32602, format!("Invalid params: {e}"));
                            let _ = write_response(&mut writer, &err).await;
                            continue;
                        }
                    };

                    if let Ok(events) = state.manager.get_events(&params.id, params.tail) {
                        for event in events {
                            let stream_event = StreamEvent::AgentEvent { event };
                            let Ok(json) = serde_json::to_string(&stream_event) else {
                                continue;
                            };
                            if writer.write_all(json.as_bytes()).await.is_err() {
                                return;
                            }
                            if writer.write_all(b"\n").await.is_err() {
                                return;
                            }
                        }
                    }

                    let mut rx = state.manager.subscribe();
                    loop {
                        match rx.recv().await {
                            Ok(event) => {
                                if event.agent_id() == Some(params.id.as_str()) {
                                    let Ok(json) = serde_json::to_string(&event) else {
                                        continue;
                                    };
                                    if writer.write_all(json.as_bytes()).await.is_err() {
                                        return;
                                    }
                                    if writer.write_all(b"\n").await.is_err() {
                                        return;
                                    }
                                    let _ = writer.flush().await;
                                }

                                if let StreamEvent::StateChange { new_state, .. } = &event
                                    && new_state.is_terminal()
                                {
                                    return;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(_) => return,
                        }
                    }
                }

                let bus = state.manager.event_bus();
                let response = rpc::handle_rpc(
                    &state.manager,
                    &state.db,
                    &state.scroll_keeper,
                    &state.wake_registry,
                    &state.workspace_registry,
                    &state.peer_registry,
                    &bus,
                    &state.daemon_id,
                    req,
                )
                .await;
                if write_response(&mut writer, &response).await.is_err() {
                    return;
                }
            }
        });
    }
}

/// Outcome of evaluating a single RPC's auth state. The caller caches `Pass`
/// across subsequent RPCs on the same connection.
#[derive(Debug, PartialEq, Eq)]
pub enum UdsAuthDecision {
    Pass,
    Reject,
}

pub fn check_uds_auth(
    already_authed: bool,
    presented: Option<&str>,
    daemon_token: &AuthToken,
) -> UdsAuthDecision {
    if already_authed {
        return UdsAuthDecision::Pass;
    }
    match presented {
        Some(tok) if daemon_token.verify(tok) => UdsAuthDecision::Pass,
        _ => UdsAuthDecision::Reject,
    }
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &RpcResponse,
) -> Result<()> {
    let json = serde_json::to_string(response)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(s: &str) -> AuthToken {
        AuthToken::new(s)
    }

    // --- UDS auth decision matrix ---

    #[test]
    fn peercred_trusted_bypasses_token() {
        // already_authed=true models a peercred-trusted connection on its
        // first RPC. The decision must pass regardless of what the client
        // sent (including nothing).
        assert_eq!(
            check_uds_auth(true, None, &tok("secret")),
            UdsAuthDecision::Pass
        );
        assert_eq!(
            check_uds_auth(true, Some("wrong"), &tok("secret")),
            UdsAuthDecision::Pass
        );
    }

    #[test]
    fn untrusted_with_matching_token_passes() {
        assert_eq!(
            check_uds_auth(false, Some("secret"), &tok("secret")),
            UdsAuthDecision::Pass
        );
    }

    #[test]
    fn untrusted_with_wrong_token_rejects() {
        assert_eq!(
            check_uds_auth(false, Some("nope"), &tok("secret")),
            UdsAuthDecision::Reject
        );
    }

    #[test]
    fn untrusted_with_missing_token_rejects() {
        assert_eq!(
            check_uds_auth(false, None, &tok("secret")),
            UdsAuthDecision::Reject
        );
    }

    #[test]
    fn untrusted_with_empty_token_rejects() {
        assert_eq!(
            check_uds_auth(false, Some(""), &tok("secret")),
            UdsAuthDecision::Reject
        );
    }
}
