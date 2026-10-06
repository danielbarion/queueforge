//! Apply replicated topology and forward a queue command without waiting for confirms.

use std::sync::Arc;

use compact_str::CompactString;
use queueforge_core::{
    Binding, Error, Exchange, Permission, Policy, Queue, QueueCmd, QueueDeclareOpts, QueueKey,
    QueueRegistry, User,
};
use queueforge_store::MetadataStore;
use serde_json::Value;

use super::wire::json_str;
use super::{Cluster, Inner};

/// Send `cmd` to `key` in `queues` and do not wait for a publisher confirm. Returns an empty JSON value, or unavailable when the queue actor is gone.
pub(super) async fn forward_nowait(
    queues: &QueueRegistry,
    key: &QueueKey,
    cmd: QueueCmd,
) -> Result<Value, Error> {
    let handle = queues
        .get(key)
        .ok_or_else(|| Error::Unavailable(format!("queue {key} is not local")))?;
    handle
        .tx
        .send(cmd)
        .await
        .map_err(|_| Error::Unavailable(format!("queue {key} is down")))?;
    Ok(Value::Null)
}

/// Apply one replicated mutation of `kind` with JSON `body` on `inner`. Unknown kinds are ignored so a newer peer cannot crash an older node.
pub(super) async fn apply_one(inner: &Arc<Inner>, kind: &str, body: &Value) {
    match kind {
        "members" => {
            super::membership::apply_members(inner, body);
        }
        "queue" => {
            let normalized = normalize_queue_json(body);
            if let Ok(queue) = serde_json::from_value::<Queue>(normalized) {
                if queue.durable {
                    let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                        let queue = queue.clone();
                        move |store| {
                            if store
                                .get_queue(queue.vhost.as_str(), queue.name.as_str())?
                                .is_some()
                            {
                                store.put_queue(&queue)
                            } else {
                                store.create_queue(&queue)
                            }
                        }
                    })
                    .await;
                }
                if queue.args.queue_type == Some(queueforge_core::QueueType::Quorum) {
                    if inner
                        .queues
                        .get(&QueueKey::new(queue.vhost.as_str(), queue.name.as_str()))
                        .is_none()
                    {
                        let _ = inner
                            .queues
                            .declare(
                                queue.vhost.as_str(),
                                queue.name.as_str(),
                                QueueDeclareOpts {
                                    durable: queue.durable,
                                    exclusive: queue.exclusive,
                                    auto_delete: queue.auto_delete,
                                    passive: false,
                                    exclusive_owner: None,
                                    args: queue.args.clone(),
                                    declared_args: None,
                                    home: Some(CompactString::from(inner.node_id.as_str())),
                                },
                            )
                            .await;
                    }
                } else if queue
                    .home
                    .as_deref()
                    .is_some_and(|home| home != inner.node_id)
                {
                    let cluster = Cluster {
                        inner: Arc::clone(inner),
                    };
                    let _ = cluster.proxy_for(&queue).await;
                } else if inner
                    .queues
                    .get(&QueueKey::new(queue.vhost.as_str(), queue.name.as_str()))
                    .is_none()
                {
                    let _ = inner
                        .queues
                        .declare(
                            queue.vhost.as_str(),
                            queue.name.as_str(),
                            QueueDeclareOpts {
                                durable: queue.durable,
                                exclusive: queue.exclusive,
                                auto_delete: queue.auto_delete,
                                passive: false,
                                exclusive_owner: None,
                                args: queue.args.clone(),
                                declared_args: None,
                                home: queue.home.clone(),
                            },
                        )
                        .await;
                }
            }
        }
        "delete_queue" => {
            let vhost = json_str(body, "vhost");
            let name = json_str(body, "queue");
            let key = QueueKey::new(vhost.as_str(), name.as_str());
            if inner.queues.get(&key).is_some() {
                let _ = inner.queues.delete(&key, false, false).await;
            }
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| {
                store.delete_queue(&vhost, &name)
            })
            .await;
        }
        "exchange" => {
            if let Ok(exchange) = serde_json::from_value::<Exchange>(body.clone()) {
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let exchange = exchange.clone();
                    move |store| {
                        if store
                            .get_exchange(exchange.vhost.as_str(), exchange.name.as_str())?
                            .is_some()
                        {
                            Ok(())
                        } else {
                            store.create_exchange(&exchange)
                        }
                    }
                })
                .await;
                inner.router.put_exchange(exchange);
            }
        }
        "binding" => {
            if let Ok(binding) = serde_json::from_value::<Binding>(body.clone()) {
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let binding = binding.clone();
                    move |store| store.put_binding(&binding)
                })
                .await;
                let _ = inner.router.bind(binding);
            }
        }
        "policy" => {
            if let Ok(policy) = serde_json::from_value::<Policy>(body.clone()) {
                let _ = inner.router.upsert_policy(policy.clone());
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let policy = policy.clone();
                    move |store| store.put_policy(&policy)
                })
                .await;
            }
        }
        "delete_policy" => {
            let vhost = json_str(body, "vhost");
            let name = json_str(body, "name");
            let _ = inner.router.delete_policy(&vhost, &name);
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| {
                store.delete_policy(&vhost, &name)
            })
            .await;
        }
        "user" => {
            if let Ok(user) = serde_json::from_value::<User>(body.clone()) {
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let user = user.clone();
                    move |store| {
                        if store.get_user(user.name.as_str())?.is_some() {
                            store.put_user(&user)
                        } else {
                            store.create_user(&user)
                        }
                    }
                })
                .await;
            }
        }
        "delete_user" => {
            let name = json_str(body, "name");
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| {
                store.delete_user(&name)
            })
            .await;
        }
        "permission" => {
            if let Ok(permission) = serde_json::from_value::<Permission>(body.clone()) {
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let permission = permission.clone();
                    move |store| store.put_permission(&permission)
                })
                .await;
            }
        }
        "delete_permission" => {
            let user = json_str(body, "user");
            let vhost = json_str(body, "vhost");
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), move |store| {
                store.delete_permission(&user, &vhost)
            })
            .await;
        }
        "vhost" => {
            let name = json_str(body, "name");
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                let name = name.clone();
                move |store| {
                    if store.get_vhost(&name)?.is_none() {
                        store.create_vhost(&name)?;
                    }
                    Ok(())
                }
            })
            .await;
            for exchange in queueforge_core::Exchange::builtins_for(&name) {
                inner.router.put_exchange(exchange);
            }
        }
        "delete_vhost" => {
            let name = json_str(body, "name");
            let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                let name = name.clone();
                move |store| {
                    let _ = store.delete_vhost(&name);
                    Ok(())
                }
            })
            .await;
        }
        "unbind" => {
            if let Ok(binding) = serde_json::from_value::<Binding>(body.clone()) {
                let _ = inner.router.unbind(&binding);
                let args = queueforge_core::binding_args_key(&binding.args);
                let _ = MetadataStore::blocking(Arc::clone(&inner.store), {
                    let binding = binding.clone();
                    let args = args.clone();
                    move |store| {
                        store.delete_binding(
                            binding.vhost.as_str(),
                            binding.exchange.as_str(),
                            binding.queue.as_str(),
                            binding.routing_key.as_str(),
                            &args,
                        )
                    }
                })
                .await;
            }
        }
        _ => {}
    }
}

/// Turn a Bun queue record into the Rust `Queue` shape.
///
/// Bun sends `autoDelete` and `args["x-queue-type"]`. Serde rejects those
/// names, and a rejected record never becomes a local quorum actor.
pub(super) fn normalize_queue_json(body: &Value) -> Value {
    let Some(obj) = body.as_object() else {
        return body.clone();
    };
    let mut out = serde_json::Map::new();
    for key in [
        "vhost",
        "name",
        "durable",
        "exclusive",
        "auto_delete",
        "home",
    ] {
        if let Some(value) = obj.get(key) {
            out.insert(key.to_string(), value.clone());
        }
    }
    if !out.contains_key("name") {
        if let Some(value) = obj.get("queue").or_else(|| obj.get("name")) {
            out.insert("name".to_string(), value.clone());
        }
    }
    if !out.contains_key("auto_delete") {
        if let Some(value) = obj.get("autoDelete") {
            out.insert("auto_delete".to_string(), value.clone());
        }
    }
    let mut args = serde_json::Map::new();
    if let Some(raw) = obj.get("args").and_then(|value| value.as_object()) {
        const PAIRS: &[(&str, &str)] = &[
            ("x-message-ttl", "message_ttl_ms"),
            ("message_ttl_ms", "message_ttl_ms"),
            ("x-expires", "expires_ms"),
            ("expires_ms", "expires_ms"),
            ("x-max-length", "max_length"),
            ("max_length", "max_length"),
            ("x-max-length-bytes", "max_length_bytes"),
            ("max_length_bytes", "max_length_bytes"),
            ("x-overflow", "overflow"),
            ("overflow", "overflow"),
            ("x-dead-letter-exchange", "dead_letter_exchange"),
            ("dead_letter_exchange", "dead_letter_exchange"),
            ("x-dead-letter-routing-key", "dead_letter_routing_key"),
            ("dead_letter_routing_key", "dead_letter_routing_key"),
            ("x-max-death-hops", "max_death_hops"),
            ("max_death_hops", "max_death_hops"),
            ("x-max-priority", "max_priority"),
            ("max_priority", "max_priority"),
            ("x-single-active-consumer", "single_active"),
            ("single_active", "single_active"),
            ("x-delivery-limit", "delivery_limit"),
            ("delivery_limit", "delivery_limit"),
            ("x-queue-type", "queue_type"),
            ("queue_type", "queue_type"),
            ("x-dead-letter-strategy", "dead_letter_strategy"),
            ("dead_letter_strategy", "dead_letter_strategy"),
        ];
        for (from, to) in PAIRS {
            if let Some(value) = raw.get(*from) {
                args.insert((*to).to_string(), value.clone());
            }
        }
    }
    if !args.is_empty() {
        out.insert("args".to_string(), Value::Object(args));
    }
    Value::Object(out)
}
