//! Topology snapshots and the consumed-message set replicated to peers.

use std::sync::Arc;
use std::time::Duration;

use queueforge_core::QueueKey;
use queueforge_store::MetadataStore;
use serde_json::Value;

use super::quorum::local_forget;
use super::{Cluster, Inner, Snapshot};

/// Build the topology snapshot this node sends to a peer. `inner` supplies the metadata store. Returns JSON the peer can apply.
pub(super) async fn snapshot(inner: &Inner) -> Value {
    let store = Arc::clone(&inner.store);
    let snap = MetadataStore::blocking(store, |store| {
        let mut users = Vec::new();
        let mut permissions = Vec::new();
        let vhosts = store.list_vhosts()?;
        for user in store.list_users()? {
            permissions.extend(store.list_permissions_for_user(user.name.as_str())?);
            users.push(user);
        }
        let mut exchanges = Vec::new();
        let mut queues = Vec::new();
        let mut bindings = Vec::new();
        for vhost in &vhosts {
            exchanges.extend(store.list_exchanges(vhost.name.as_str())?);
            queues.extend(store.list_queues(vhost.name.as_str())?);
            bindings.extend(store.list_bindings(vhost.name.as_str())?);
        }
        Ok(Snapshot {
            users,
            vhosts,
            permissions,
            exchanges,
            queues,
            bindings,
        })
    })
    .await;
    serde_json::to_value(snap.unwrap_or(Snapshot {
        users: Vec::new(),
        vhosts: Vec::new(),
        permissions: Vec::new(),
        exchanges: Vec::new(),
        queues: Vec::new(),
        bindings: Vec::new(),
    }))
    .unwrap_or(Value::Null)
}

/// Record `message_id` as consumed on `key` in `inner`. A later snapshot must not resurrect that id on this node.
pub(super) async fn remember_consumed(inner: &Inner, key: &QueueKey, message_id: &str) {
    let entry = serde_json::json!({"vhost": key.vhost.as_str(), "queue": key.name.as_str(), "id": message_id});
    let mut consumed = inner.consumed.lock().await;
    if !consumed.iter().any(|item| item == &entry) {
        consumed.push(entry);
    }
}

/// Apply a peer consumed-set update in `value` to `inner`. Unknown queue keys are ignored.
pub(super) async fn apply_consumed(inner: &Arc<Inner>, value: &Value) {
    let Some(items) = value.as_array() else {
        return;
    };
    for item in items {
        let vhost = item.get("vhost").and_then(|v| v.as_str()).unwrap_or("");
        let queue = item.get("queue").and_then(|v| v.as_str()).unwrap_or("");
        let id = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if vhost.is_empty() || queue.is_empty() || id.is_empty() {
            continue;
        }
        let key = QueueKey::new(vhost, queue);
        remember_consumed(inner, &key, id).await;
        let inner = Arc::clone(inner);
        let id = id.to_string();
        tokio::spawn(async move {
            for _ in 0..15 {
                let _ = local_forget(&inner, &key, &id).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }
}

/// Replace local replicated topology with the peer snapshot in `value`. `inner` is the node being updated. A malformed snapshot leaves the previous rows.
pub(super) async fn apply_snapshot(inner: &Arc<Inner>, value: &Value) {
    let Ok(snap) = serde_json::from_value::<Snapshot>(value.clone()) else {
        return;
    };
    let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
        let snap = snap_clone(&snap);
        move |store| apply_snapshot_store(store, &snap)
    })
    .await;
    for exchange in snap.exchanges {
        inner.router.put_exchange(exchange);
    }
    for binding in &snap.bindings {
        let _ = inner.router.bind(binding.clone());
    }
    for queue in snap.queues {
        if queue
            .home
            .as_deref()
            .is_some_and(|home| home != inner.node_id)
        {
            let cluster = Cluster {
                inner: Arc::clone(inner),
            };
            let _ = cluster.proxy_for(&queue).await;
        }
    }
}

/// Clone `snap` so a blocking metadata write does not borrow the actor's snapshot. Returns an owned copy.
pub(super) fn snap_clone(snap: &Snapshot) -> Snapshot {
    Snapshot {
        users: snap.users.clone(),
        vhosts: snap.vhosts.clone(),
        permissions: snap.permissions.clone(),
        exchanges: snap.exchanges.clone(),
        queues: snap.queues.clone(),
        bindings: snap.bindings.clone(),
    }
}

/// Write `snap` into `store`. Returns the store error. The caller does not confirm quorum publishes on this path.
pub(super) fn apply_snapshot_store(
    store: &MetadataStore,
    snap: &Snapshot,
) -> queueforge_store::Result<()> {
    for vhost in &snap.vhosts {
        if store.get_vhost(vhost.name.as_str())?.is_none() {
            let _ = store.create_vhost(vhost.name.as_str());
        }
    }
    for user in &snap.users {
        if store.get_user(user.name.as_str())?.is_none() {
            let _ = store.create_user(user);
        }
    }
    for perm in &snap.permissions {
        let _ = store.put_permission(perm);
    }
    for exchange in &snap.exchanges {
        if store
            .get_exchange(exchange.vhost.as_str(), exchange.name.as_str())?
            .is_none()
        {
            let _ = store.create_exchange(exchange);
        }
    }
    for queue in &snap.queues {
        if store
            .get_queue(queue.vhost.as_str(), queue.name.as_str())?
            .is_none()
        {
            let _ = store.create_queue(queue);
        }
    }
    for binding in &snap.bindings {
        let _ = store.put_binding(binding);
    }
    Ok(())
}
