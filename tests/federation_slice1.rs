//! RPC validation for federated addresses and the `protocol_version` field.
//! The daemon mints a `daemon_id`, parses federated addresses, and rejects
//! federation traffic with `federation_not_configured` until real forwarding
//! is wired.

use grimoire::shared::protocol::RpcRequest;

fn req(method: &str, params: serde_json::Value, pv: Option<u32>) -> RpcRequest {
    RpcRequest {
        method: method.to_string(),
        params,
        id: 1,
        protocol_version: pv,
        auth_token: None,
    }
}

#[test]
fn rpc_request_protocol_version_is_optional() {
    // Existing CLI omits the field entirely.
    let json = r#"{"method":"daemon.status","params":{},"id":1}"#;
    let parsed: RpcRequest = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.protocol_version, None);

    // Explicit 1 deserializes too.
    let json2 = r#"{"method":"daemon.status","params":{},"id":1,"protocol_version":1}"#;
    let parsed2: RpcRequest = serde_json::from_str(json2).unwrap();
    assert_eq!(parsed2.protocol_version, Some(1));

    // 999 still parses; the *handler* rejects it (covered below).
    let json3 = r#"{"method":"daemon.status","params":{},"id":1,"protocol_version":999}"#;
    let parsed3: RpcRequest = serde_json::from_str(json3).unwrap();
    assert_eq!(parsed3.protocol_version, Some(999));
}

#[test]
fn req_helper_constructs_rpc_request() {
    let r = req("noop", serde_json::json!({}), Some(1));
    assert_eq!(r.method, "noop");
    assert_eq!(r.protocol_version, Some(1));
}

// Full handler-level tests for `unsupported_protocol_version` and
// `federation_not_configured` require booting the daemon harness; they
// live in `tests/peer_e2e.rs`.
