//! Turn one cluster operation result into the reply frame.

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::mpsc;

use super::dispatch::dispatch_op;
use super::{Inner, Msg};

/// Handle one inbound `msg` for `inner`, using `peer_tx` to answer on the socket. Returns the response message. Handler failures come back as `ok: false`, not as a dropped socket.
pub(super) async fn dispatch(inner: &Arc<Inner>, msg: &Msg, peer_tx: mpsc::Sender<String>) -> Msg {
    let result = dispatch_op(inner, msg, peer_tx).await;
    match result {
        Ok(payload) => Msg {
            id: msg.id,
            op: "reply".into(),
            ok: true,
            error: String::new(),
            payload,
            v: 1,
            node_id: inner.node_id.clone(),
            from: inner.node_id.clone(),
            kind: String::new(),
        },
        Err(err) => Msg {
            id: msg.id,
            op: "reply".into(),
            ok: false,
            error: err.to_string(),
            v: 1,
            node_id: inner.node_id.clone(),
            from: inner.node_id.clone(),
            kind: String::new(),
            payload: Value::Null,
        },
    }
}
