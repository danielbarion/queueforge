//! Runtime membership. The config list is the bootstrap. `members.json` in the
//! data directory is the list after a join or forget, and every peer keeps a copy.

use std::net::SocketAddr;
use std::sync::Arc;

use queueforge_core::{ClusterMember, Error};
use queueforge_store::MetadataStore;
use serde_json::Value;

use super::Inner;

const FILE: &str = "members.json";

/// Load `members.json` when it exists. Otherwise keep `configured` and write it.
pub(super) fn load_members(
    store: &MetadataStore,
    configured: &[ClusterMember],
) -> Vec<ClusterMember> {
    let path = store.data_dir().join(FILE);
    if let Ok(text) = std::fs::read_to_string(&path) {
        if let Some(parsed) = parse_members(&text) {
            if !parsed.is_empty() {
                return parsed;
            }
        }
    }
    save_members(store, configured);
    configured.to_vec()
}

/// Replace `inner`'s member list and write `members.json`.
pub(super) fn install_members(inner: &Inner, members: Vec<ClusterMember>) {
    if members.is_empty() {
        return;
    }
    save_members(&inner.store, &members);
    *inner.members.lock().unwrap_or_else(|err| err.into_inner()) = members;
    super::consensus::members_changed(inner);
}

/// `members` with `id` added at `addr`, or its address refreshed.
pub(super) fn joined(inner: &Inner, id: &str, addr: SocketAddr) -> Vec<ClusterMember> {
    let mut members = inner.member_list();
    if let Some(existing) = members.iter_mut().find(|member| member.id == id) {
        existing.addr = addr;
    } else {
        members.push(ClusterMember {
            id: id.to_string(),
            addr,
        });
    }
    members.sort_by(|a, b| a.id.cmp(&b.id));
    members
}

/// Change the member list. With Raft on, the list commits through the meta
/// log first (docs/raft.md, section 7), so a change without a majority is
/// refused. Then this node installs it and pushes it to the old and new
/// members, which apply it before the next heartbeat carries the commit.
pub(super) async fn change_members(inner: &Arc<Inner>, members: Vec<ClusterMember>) -> Result<Vec<ClusterMember>, Error> {
    if members.is_empty() {
        return Err(Error::PreconditionFailed("the member list cannot become empty".into()));
    }
    let before = inner.member_list();
    if let Some(node) = super::consensus::node(inner) {
        node.propose(super::raft_node::META, "members", members_json(&members))
            .await
            .map_err(|why| Error::Unavailable(format!("membership change did not commit: {why}")))?;
    }
    install_members(inner, members.clone());
    let mut targets: Vec<String> = before.iter().chain(members.iter()).map(|m| m.id.clone()).collect();
    targets.sort();
    targets.dedup();
    let cluster = super::Cluster { inner: Arc::clone(inner) };
    let body = serde_json::json!({"kind": "members", "body": members_json(&members)});
    for id in targets {
        if id != inner.node_id {
            let _ = cluster.call(&id, "apply", body.clone()).await;
        }
    }
    Ok(members)
}

/// Remove `id` when no stored queue names it as home. Returns the new list, or the error.
pub(super) fn forget_member(inner: &Inner, id: &str) -> Result<Vec<ClusterMember>, Error> {
    if id == inner.node_id {
        return Err(Error::PreconditionFailed(
            "a node cannot forget itself".into(),
        ));
    }
    if member_is_home(inner, id) {
        return Err(Error::PreconditionFailed(format!(
            "member {id} still homes a classic queue"
        )));
    }
    let members: Vec<_> = inner
        .member_list()
        .into_iter()
        .filter(|member| member.id != id)
        .collect();
    if members.is_empty() {
        return Err(Error::PreconditionFailed(
            "the member list cannot become empty".into(),
        ));
    }
    Ok(members)
}

/// JSON array of `{id, addr}` for a membership broadcast.
pub(super) fn members_json(members: &[ClusterMember]) -> Value {
    Value::Array(
        members
            .iter()
            .map(|member| serde_json::json!({"id": member.id, "addr": member.addr.to_string()}))
            .collect(),
    )
}

fn member_is_home(inner: &Inner, id: &str) -> bool {
    let Ok(vhosts) = inner.store.list_vhosts() else {
        return true;
    };
    for vhost in vhosts {
        let Ok(queues) = inner.store.list_queues(vhost.name.as_str()) else {
            return true;
        };
        if queues.iter().any(|queue| queue.home.as_deref() == Some(id)) {
            return true;
        }
    }
    false
}

fn save_members(store: &MetadataStore, members: &[ClusterMember]) {
    let path = store.data_dir().join(FILE);
    let _ = std::fs::write(path, members_json(members).to_string());
}

fn parse_members(text: &str) -> Option<Vec<ClusterMember>> {
    let value: Value = serde_json::from_str(text).ok()?;
    let rows = value.as_array()?;
    let mut members = Vec::new();
    for row in rows {
        let id = row.get("id")?.as_str()?.to_string();
        let addr: SocketAddr = row.get("addr")?.as_str()?.parse().ok()?;
        if !id.is_empty() {
            members.push(ClusterMember { id, addr });
        }
    }
    Some(members)
}

/// Read `[{id, addr}]`. Rows without a readable address are skipped.
pub(super) fn members_from(value: &Value) -> Vec<ClusterMember> {
    parse_members(&value.to_string()).unwrap_or_default()
}

/// Read a join or forget body. `inner` is unused except to keep the helper next to the ops.
pub(super) fn addr_of(payload: &Value) -> Option<SocketAddr> {
    payload
        .get("addr")
        .and_then(|value| value.as_str())
        .and_then(|text| text.parse().ok())
}

/// Apply a broadcast member list. An empty or unreadable payload leaves the current list.
pub(super) fn apply_members(inner: &Arc<Inner>, body: &Value) {
    let Some(text) = body.as_array().map(|_| body.to_string()) else {
        return;
    };
    let Some(members) = parse_members(&text) else {
        return;
    };
    install_members(inner, members);
}

/// Replace memory from `members.json` when the file parses. Called on the dial loop.
pub(super) fn reload_from_disk(inner: &Inner) {
    let path = inner.store.data_dir().join(FILE);
    let Ok(text) = std::fs::read_to_string(path) else {
        return;
    };
    let Some(members) = parse_members(&text) else {
        return;
    };
    if members.is_empty() {
        return;
    }
    *inner.members.lock().unwrap_or_else(|err| err.into_inner()) = members;
}

