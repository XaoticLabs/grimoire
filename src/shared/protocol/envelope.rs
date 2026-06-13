//! JSON-RPC envelope types shared by every method: the request/response
//! frames, the error shape, and the two single-field param/result aliases.
use serde::{Deserialize, Serialize};

/// Empty `{}` result body for RPC methods that just report success.
/// Serializes to `{}` so the wire format matches per-method empty result types.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyResult {}

/// Single-`id` params shape for RPC methods whose only argument is an id
/// (agent, scroll, workspace, …). Aliases share the wire shape `{"id": "..."}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdParams {
    pub id: String,
}

/// JSON-RPC request from CLI to daemon
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RpcRequest {
    pub method: String,
    pub params: serde_json::Value,
    pub id: u64,
    /// RPC protocol version. Existing callers omit this; the dispatcher
    /// defaults to `1`. Unknown versions are rejected with
    /// `unsupported_protocol_version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<u32>,
    /// Bearer token. Required when the daemon cannot identify the caller
    /// via `SO_PEERCRED` (i.e. UDS connections from a different UID, or
    /// any future non-UDS transport that reuses `RpcRequest`). UDS
    /// connections from the daemon's own UID may omit the token; the
    /// kernel's peer-credential check substitutes for authentication.
    /// Sent on every request; the server caches `authed=true` per
    /// connection after the first successful check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

/// JSON-RPC response from daemon to CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

impl RpcResponse {
    pub const fn success(id: u64, result: serde_json::Value) -> Self {
        Self {
            id,
            result: Some(result),
            error: None,
        }
    }

    /// Build a success response from any serializable payload. Panics only if
    /// serialization fails, which for the plain `derive(Serialize)` result
    /// structs used here is a programmer error, not a runtime condition.
    pub fn success_json<T: Serialize>(id: u64, value: &T) -> Self {
        let result = serde_json::to_value(value)
            .expect("RPC result payloads are plain derive(Serialize) structs");
        Self::success(id, result)
    }

    pub const fn error(id: u64, code: i32, message: String) -> Self {
        Self {
            id,
            result: None,
            error: Some(RpcError { code, message }),
        }
    }
}
