//! Management mutation handlers: CRUD for topology, users, permissions; publish/get.
//! Route groups live in sibling modules. This module runs metadata calls,
//! optional cluster replication, and the vhost-exists check those handlers share.

use std::sync::Arc;

use serde::Deserialize;

use crate::error::MgmtError;
use crate::state::{MgmtState, ReplicateReq};

/// Optional `if-unused` and `if-empty` flags on DELETE.
///
/// A missing flag is false. Queue delete passes both through; exchange delete
/// reads `if_unused`.
#[derive(Debug, Default, Deserialize)]
pub struct IfUnusedQuery {
    if_unused: Option<bool>,
    if_empty: Option<bool>,
}

/// Run one metadata operation off the HTTP worker.
///
/// `state` owns the store. `f` is the store call. Returns that call's value.
/// A store error becomes [`MgmtError`].
pub(super) async fn db<T, F>(state: &MgmtState, f: F) -> Result<T, MgmtError>
where
    T: Send + 'static,
    F: FnOnce(&queueforge_store::MetadataStore) -> queueforge_store::Result<T> + Send + 'static,
{
    queueforge_store::MetadataStore::blocking(Arc::clone(&state.store), f)
        .await
        .map_err(MgmtError::from)
}

/// Forward one mutation to cluster peers when replication is configured.
///
/// `state` holds the optional replicate channel. `kind` names the mutation and
/// `payload` is its JSON body. Returns when the peer ack arrives, or immediately
/// when replication is off. A closed channel is ignored.
pub(super) async fn replicate(state: &MgmtState, kind: &str, payload: serde_json::Value) {
    let Some(tx) = &state.replicate_tx else {
        return;
    };
    let (done, rx) = tokio::sync::oneshot::channel();
    if tx
        .send(ReplicateReq {
            kind: kind.to_string(),
            payload,
            done,
        })
        .is_err()
    {
        return;
    }
    let _ = rx.await;
}

/// Require `vhost` to exist in `state`'s metadata store.
///
/// Returns `Ok` when the row is present. A missing vhost is [`MgmtError::NotFound`].
pub(super) async fn ensure_vhost(state: &MgmtState, vhost: &str) -> Result<(), MgmtError> {
    let name = vhost.to_string();
    if db(state, move |s| s.get_vhost(&name)).await?.is_none() {
        return Err(MgmtError::NotFound(format!("vhost '{vhost}'")));
    }
    Ok(())
}

mod binding;
mod exchange;
mod getmsg;
mod operator;
mod permission;
mod policy;
mod publish;
mod queue;
mod shovel;
mod user;
mod vhost;

pub use binding::{
    create_binding, delete_binding, list_bindings, list_exchange_bindings, list_queue_bindings,
};
pub use exchange::{delete_exchange, put_exchange};
pub use getmsg::get_messages;
pub use operator::{
    delete_operator_policy, list_operator_policies, list_operator_policies_vhost,
    put_operator_policy,
};
pub use permission::{delete_permission, list_permissions, put_permission};
pub use policy::{delete_policy, list_policies, list_policies_vhost, put_policy};
pub use publish::publish;
pub use queue::{delete_queue, purge_queue, put_queue};
pub use shovel::{put_federation_upstream, put_shovel};
pub use user::{delete_user, list_users, put_user};
pub use vhost::{delete_vhost, put_vhost};

pub(crate) use binding::args_from_json;
pub(crate) use policy::{policy_from_body, PutPolicyBody};
pub(crate) use queue::parse_mgmt_queue_args;
