//! Node calls zecd forwards to its upstream node unchanged.
//!
//! Mining-pool software (Miningcore in particular) talks to one daemon for both mining and
//! payouts, so the block-template and block-submission calls have to be answered by the same
//! endpoint as the wallet. zecd has no block data of its own for these, so it sends them to the
//! node on the daemon's `[backend]` connection and returns the node's answer as is.
//!
//! What a caller sees:
//! - the node's `result`, unchanged;
//! - the node's own error code and message when the node refuses the call (Miningcore probes
//!   `submitblock` with no params and only checks the code is not `-32601`);
//! - a generic `-1` when the node could not be reached, with the details in the server log
//!   only, since they name the node's address (the same rule as `From<anyhow::Error>` for
//!   [`RpcError`]);
//! - `-32601` on a lightwalletd backend, which has no node JSON-RPC to forward to.

use std::sync::Arc;

use serde_json::Value;

use crate::chain::hub::ChainHub;
use crate::chain::PassthroughError;
use crate::error::{codes, RpcError};
use crate::server::jsonrpc::RpcRequest;
use crate::state::AppState;

/// Forward `req` to the node behind the daemon's global `[backend]`.
pub(crate) async fn forward(state: &AppState, req: &RpcRequest) -> Result<Value, RpcError> {
    forward_via(state.node_hub.as_ref(), req).await
}

async fn forward_via(hub: Option<&Arc<ChainHub>>, req: &RpcRequest) -> Result<Value, RpcError> {
    let Some(hub) = hub else {
        return Err(RpcError::method_not_found(&req.method));
    };
    let params = Value::Array(req.params.clone());
    hub.passthrough(&req.method, params)
        .await
        .map_err(|e| to_rpc_error(&req.method, e))
}

/// How a forwarding failure reads to the RPC caller.
fn to_rpc_error(method: &str, e: PassthroughError) -> RpcError {
    match e {
        PassthroughError::Rpc { code, message } => {
            let code = i32::try_from(code).unwrap_or(codes::RPC_MISC_ERROR);
            RpcError::new(code, message)
        }
        PassthroughError::Unsupported => RpcError::method_not_found(method),
        PassthroughError::Transport(e) => {
            tracing::warn!(method, "forwarding to the node failed: {e:#}");
            RpcError::misc(format!("{method}: the node did not answer"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use axum::{routing::post, Json, Router};
    use serde_json::{json, Value};

    use super::*;

    /// A node answering the mining calls the tests forward, and refusing everything else.
    async fn fake_node() -> SocketAddr {
        async fn handler(Json(req): Json<Value>) -> Json<Value> {
            let ok = |v: Value| Json(json!({ "result": v, "error": null, "id": req["id"] }));
            let err = |code: i64, msg: &str| {
                Json(
                    json!({ "result": null, "error": { "code": code, "message": msg }, "id": req["id"] }),
                )
            };
            match req["method"].as_str().unwrap_or_default() {
                // The dial's liveness probe.
                "getblockchaininfo" => {
                    ok(json!({ "chain": "regtest", "blocks": 10, "bestblockhash": "00" }))
                }
                "getdifficulty" => ok(json!(1.5)),
                "getblock" => ok(json!({ "echo": req["params"].clone() })),
                "submitblock" if req["params"].as_array().is_none_or(|p| p.is_empty()) => {
                    err(-32602, "Invalid params: expected a block")
                }
                "submitblock" => ok(json!("duplicate")),
                other => err(-32601, &format!("Method not found: {other}")),
            }
        }
        let app = Router::new().route("/", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    fn hub_at(addr: SocketAddr) -> Arc<ChainHub> {
        let server = crate::backend::resolve(
            &format!("zebra://{}:{}", addr.ip(), addr.port()),
            crate::network::ZNetwork::Main,
        )
        .expect("loopback zebra endpoint resolves");
        ChainHub::new(server, Duration::from_secs(5))
    }

    fn call(method: &str, params: Vec<Value>) -> RpcRequest {
        RpcRequest::positional(method, params)
    }

    #[tokio::test]
    async fn the_node_result_is_returned_unchanged() {
        let hub = hub_at(fake_node().await);
        let v = forward_via(Some(&hub), &call("getdifficulty", vec![]))
            .await
            .unwrap();
        assert_eq!(v, json!(1.5));
        // Params go out exactly as the caller sent them.
        let v = forward_via(Some(&hub), &call("getblock", vec![json!("ab"), json!(1)]))
            .await
            .unwrap();
        assert_eq!(v, json!({ "echo": ["ab", 1] }));
        let v = forward_via(Some(&hub), &call("submitblock", vec![json!("00")]))
            .await
            .unwrap();
        assert_eq!(v, json!("duplicate"));
    }

    #[tokio::test]
    async fn a_node_refusal_keeps_the_node_code_and_message() {
        let hub = hub_at(fake_node().await);
        let e = forward_via(Some(&hub), &call("submitblock", vec![]))
            .await
            .unwrap_err();
        assert_eq!(e.code, -32602);
        assert_eq!(e.message, "Invalid params: expected a block");
    }

    #[tokio::test]
    async fn an_unreachable_node_is_a_generic_error_that_does_not_name_the_node() {
        // Bind and drop a listener, so the port is known to refuse connections.
        let addr = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap()
        };
        let hub = hub_at(addr);
        let e = forward_via(Some(&hub), &call("getmininginfo", vec![]))
            .await
            .unwrap_err();
        assert_eq!(e.code, codes::RPC_MISC_ERROR);
        assert!(!e.message.contains("127.0.0.1"), "{}", e.message);
        assert!(
            !e.message.contains(&addr.port().to_string()),
            "{}",
            e.message
        );
    }

    #[tokio::test]
    async fn no_node_means_method_not_found() {
        let e = forward_via(None, &call("getblocktemplate", vec![]))
            .await
            .unwrap_err();
        assert_eq!(e.code, codes::RPC_METHOD_NOT_FOUND);
    }

    #[test]
    fn a_lightwalletd_backend_answers_method_not_found() {
        let e = to_rpc_error("getblocktemplate", PassthroughError::Unsupported);
        assert_eq!(e.code, codes::RPC_METHOD_NOT_FOUND);
    }

    #[test]
    fn a_node_code_outside_the_i32_range_becomes_a_misc_error() {
        let e = to_rpc_error(
            "getblock",
            PassthroughError::Rpc {
                code: i64::MAX,
                message: "odd".into(),
            },
        );
        assert_eq!(e.code, codes::RPC_MISC_ERROR);
        assert_eq!(e.message, "odd");
    }
}
