//! Settings kept beside topology: user and vhost limits, topic permissions,
//! runtime parameters and global parameters.
//!
//! Each is stored in the metadata store's parameter table, as Bun stores
//! them, loaded at startup, and replicated with Bun's mutation kinds
//! (`user_limits`, `vhost_limits`, `topic_permission`, `parameter`,
//! `global_parameter` and their `delete_` forms), so a cluster of Rust and
//! Bun members keeps one copy.

use queueforge_store::MetadataStore;
use serde_json::{json, Value};

use crate::connections::{ConnectionTracker, TopicPermission};

const USER_LIMITS: &str = "qf-user-limits";
const VHOST_LIMITS: &str = "qf-vhost-limits";
const TOPIC_PERMS: &str = "topic-permissions";
const GLOBAL: &str = "global";
/// Runtime parameters of any other component, as `rt:<component>`.
const GENERIC: &str = "rt:";

/// The replicated kinds this module applies.
pub const KINDS: &[&str] = &[
    "user_limits",
    "vhost_limits",
    "topic_permission",
    "delete_topic_permission",
    "parameter",
    "delete_parameter",
    "global_parameter",
    "delete_global_parameter",
];

fn limit(v: Option<&Value>) -> Option<u32> {
    v.and_then(|v| v.as_i64()).filter(|n| *n >= 0).map(|n| n.min(i64::from(u32::MAX)) as u32)
}

fn text(v: &Value, field: &str) -> String {
    v.get(field).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

/// A user's limits as Bun replicates them.
pub fn user_limits_row(conns: &ConnectionTracker, user: &str) -> Value {
    let (c, ch) = conns
        .list_user_limits()
        .into_iter()
        .find(|row| row.0 == user)
        .map(|row| (row.1, row.2))
        .unwrap_or((None, None));
    json!({"user": user, "max-connections": c, "max-channels": ch})
}

/// A vhost's limits as Bun replicates them.
pub fn vhost_limits_row(conns: &ConnectionTracker, vhost: &str) -> Value {
    let (c, q) = conns
        .list_vhost_limits()
        .into_iter()
        .find(|row| row.0 == vhost)
        .map(|row| (row.1, row.2))
        .unwrap_or((None, None));
    json!({"vhost": vhost, "max-connections": c, "max-queues": q})
}

/// A topic permission as Bun replicates it.
pub fn topic_row(p: &TopicPermission) -> Value {
    json!({"user": p.user, "vhost": p.vhost, "exchange": p.exchange, "write": p.write, "read": p.read})
}

/// Store the current limits of `user`.
pub fn store_user_limits(conns: &ConnectionTracker, store: &MetadataStore, user: &str) {
    let row = user_limits_row(conns, user);
    let _ = if row["max-connections"].is_null() && row["max-channels"].is_null() {
        store.delete_parameter(USER_LIMITS, "", user)
    } else {
        store.put_parameter(USER_LIMITS, "", user, row.to_string().as_bytes())
    };
}

/// Store the current limits of `vhost`.
pub fn store_vhost_limits(conns: &ConnectionTracker, store: &MetadataStore, vhost: &str) {
    let row = vhost_limits_row(conns, vhost);
    let _ = if row["max-connections"].is_null() && row["max-queues"].is_null() {
        store.delete_parameter(VHOST_LIMITS, vhost, "")
    } else {
        store.put_parameter(VHOST_LIMITS, vhost, "", row.to_string().as_bytes())
    };
}

/// Store one topic permission, or remove it when `perm` is `None`.
pub fn store_topic(store: &MetadataStore, user: &str, vhost: &str, exchange: &str, perm: Option<&TopicPermission>) {
    let name = format!("{user}\0{exchange}");
    let _ = match perm {
        Some(p) => store.put_parameter(TOPIC_PERMS, vhost, &name, topic_row(p).to_string().as_bytes()),
        None => store.delete_parameter(TOPIC_PERMS, vhost, &name),
    };
}

fn apply_limits(conns: &ConnectionTracker, store: &MetadataStore, kind: &str, row: &Value) {
    if kind == "user_limits" {
        let user = text(row, "user");
        if user.is_empty() {
            return;
        }
        conns.set_user_limit(&user, limit(row.get("max-connections")), limit(row.get("max-channels")));
        store_user_limits(conns, store, &user);
    } else {
        let vhost = text(row, "vhost");
        if vhost.is_empty() {
            return;
        }
        conns.set_vhost_limit(&vhost, limit(row.get("max-connections")), limit(row.get("max-queues")));
        store_vhost_limits(conns, store, &vhost);
    }
}

fn topic_from(row: &Value) -> Option<TopicPermission> {
    let p = TopicPermission {
        user: text(row, "user"),
        vhost: row.get("vhost").and_then(|v| v.as_str()).unwrap_or("/").to_string(),
        exchange: text(row, "exchange"),
        write: text(row, "write"),
        read: text(row, "read"),
    };
    let valid = !p.user.is_empty()
        && !p.exchange.is_empty()
        && regex::Regex::new(&p.write).is_ok()
        && regex::Regex::new(&p.read).is_ok();
    valid.then_some(p)
}

/// Set one runtime parameter. `vhost-limits` sets that vhost's limits, as
/// RabbitMQ's component does.
pub fn put_parameter(conns: &ConnectionTracker, store: &MetadataStore, component: &str, vhost: &str, name: &str, value: &Value) {
    if component == "vhost-limits" {
        let mut row = value.clone();
        if let Some(obj) = row.as_object_mut() {
            obj.insert("vhost".into(), json!(vhost));
        }
        apply_limits(conns, store, "vhost_limits", &row);
        return;
    }
    let _ = store.put_parameter(&format!("{GENERIC}{component}"), vhost, name, value.to_string().as_bytes());
}

/// Remove one runtime parameter. Returns whether it existed.
pub fn delete_parameter(conns: &ConnectionTracker, store: &MetadataStore, component: &str, vhost: &str, name: &str) -> bool {
    if component == "vhost-limits" {
        let had = conns.list_vhost_limits().iter().any(|row| row.0 == vhost);
        conns.set_vhost_limit(vhost, None, None);
        store_vhost_limits(conns, store, vhost);
        return had;
    }
    let component = format!("{GENERIC}{component}");
    let had = store.get_parameter(&component, vhost, name).ok().flatten().is_some();
    let _ = store.delete_parameter(&component, vhost, name);
    had
}

/// Runtime parameters as RabbitMQ lists them: `{component, vhost, name, value}`.
pub fn list_parameters(conns: &ConnectionTracker, store: &MetadataStore, component: Option<&str>, vhost: Option<&str>) -> Vec<Value> {
    let mut rows = Vec::new();
    for (vh, c, q) in conns.list_vhost_limits() {
        let mut value = serde_json::Map::new();
        if let Some(c) = c {
            value.insert("max-connections".into(), json!(c));
        }
        if let Some(q) = q {
            value.insert("max-queues".into(), json!(q));
        }
        rows.push(json!({"component": "vhost-limits", "vhost": vh, "name": "limits", "value": value}));
    }
    for c in store.list_parameter_components().unwrap_or_default() {
        let Some(name) = c.strip_prefix(GENERIC) else { continue };
        for (vh, n, bytes) in store.list_parameters(&c).unwrap_or_default() {
            let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            rows.push(json!({"component": name, "vhost": vh, "name": n, "value": value}));
        }
    }
    rows.retain(|r| {
        component.is_none_or(|c| r["component"] == c) && vhost.is_none_or(|v| r["vhost"] == v)
    });
    rows.sort_by(|a, b| {
        let key = |r: &Value| (r["component"].to_string(), r["vhost"].to_string(), r["name"].to_string());
        key(a).cmp(&key(b))
    });
    rows
}

/// Global parameters: `{name, value}`. `cluster_name` is always present.
pub fn list_globals(store: &MetadataStore, node_name: &str) -> Vec<Value> {
    let mut rows: Vec<Value> = store
        .list_parameters(GLOBAL)
        .unwrap_or_default()
        .into_iter()
        .map(|(_, name, bytes)| json!({"name": name, "value": serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null)}))
        .collect();
    if !rows.iter().any(|r| r["name"] == "cluster_name") {
        rows.push(json!({"name": "cluster_name", "value": node_name}));
    }
    rows.sort_by(|a, b| a["name"].to_string().cmp(&b["name"].to_string()));
    rows
}

/// Set one global parameter.
pub fn put_global(store: &MetadataStore, name: &str, value: &Value) {
    let _ = store.put_parameter(GLOBAL, "", name, value.to_string().as_bytes());
}

/// Remove one global parameter. Returns whether it existed.
pub fn delete_global(store: &MetadataStore, name: &str) -> bool {
    let had = store.get_parameter(GLOBAL, "", name).ok().flatten().is_some();
    let _ = store.delete_parameter(GLOBAL, "", name);
    had
}

/// Apply one replicated setting. Returns false for a kind this module
/// does not own.
pub fn apply(conns: &ConnectionTracker, store: &MetadataStore, kind: &str, body: &Value) -> bool {
    match kind {
        "user_limits" | "vhost_limits" => apply_limits(conns, store, kind, body),
        "topic_permission" => {
            if let Some(p) = topic_from(body) {
                store_topic(store, &p.user, &p.vhost, &p.exchange, Some(&p));
                conns.put_topic_permission(p);
            }
        }
        "delete_topic_permission" => {
            let (user, vhost, exchange) = (text(body, "user"), text(body, "vhost"), text(body, "exchange"));
            conns.delete_topic_permission(&user, &vhost, &exchange);
            store_topic(store, &user, &vhost, &exchange, None);
        }
        "parameter" => {
            let component = text(body, "component");
            if component.is_empty() || component == "shovel" {
                return true;
            }
            let value = body.get("value").cloned().unwrap_or(Value::Null);
            put_parameter(conns, store, &component, &text(body, "vhost"), &text(body, "name"), &value);
        }
        "delete_parameter" => {
            let component = text(body, "component");
            if component != "shovel" {
                delete_parameter(conns, store, &component, &text(body, "vhost"), &text(body, "name"));
            }
        }
        "global_parameter" => put_global(store, &text(body, "name"), body.get("value").unwrap_or(&Value::Null)),
        "delete_global_parameter" => {
            delete_global(store, &text(body, "name"));
        }
        _ => return false,
    }
    true
}

/// Load the stored settings into `conns` at startup.
pub fn load(conns: &ConnectionTracker, store: &MetadataStore) {
    for (_, user, bytes) in store.list_parameters(USER_LIMITS).unwrap_or_default() {
        if let Ok(row) = serde_json::from_slice::<Value>(&bytes) {
            conns.set_user_limit(&user, limit(row.get("max-connections")), limit(row.get("max-channels")));
        }
    }
    for (vhost, _, bytes) in store.list_parameters(VHOST_LIMITS).unwrap_or_default() {
        if let Ok(row) = serde_json::from_slice::<Value>(&bytes) {
            conns.set_vhost_limit(&vhost, limit(row.get("max-connections")), limit(row.get("max-queues")));
        }
    }
    for (_, _, bytes) in store.list_parameters(TOPIC_PERMS).unwrap_or_default() {
        if let Some(p) = serde_json::from_slice::<Value>(&bytes).ok().as_ref().and_then(topic_from) {
            conns.put_topic_permission(p);
        }
    }
}

/// The settings part of a meta snapshot, in Bun's field names.
pub fn snapshot(conns: &ConnectionTracker, store: &MetadataStore) -> Value {
    let users: Vec<Value> = conns.list_user_limits().iter().map(|row| user_limits_row(conns, &row.0)).collect();
    let vhosts: Vec<Value> = conns.list_vhost_limits().iter().map(|row| vhost_limits_row(conns, &row.0)).collect();
    let topics: Vec<Value> = conns.list_topic_permissions(None).iter().map(topic_row).collect();
    let params: Vec<Value> = list_parameters(conns, store, None, None)
        .into_iter()
        .filter(|r| r["component"] != "vhost-limits")
        .collect();
    let globals: Vec<Value> = store
        .list_parameters(GLOBAL)
        .unwrap_or_default()
        .into_iter()
        .map(|(_, name, bytes)| json!({"name": name, "value": serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null)}))
        .collect();
    json!({
        "userLimits": users,
        "vhostLimits": vhosts,
        "topicPermissions": topics,
        "parameters": params,
        "globalParameters": globals,
    })
}

/// Take the settings of a snapshot that this member does not have.
pub fn install_missing(conns: &ConnectionTracker, store: &MetadataStore, snap: &Value) {
    let list = |field: &str| snap.get(field).and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let users = conns.list_user_limits();
    for row in list("userLimits") {
        if !users.iter().any(|u| u.0 == text(&row, "user")) {
            apply(conns, store, "user_limits", &row);
        }
    }
    let vhosts = conns.list_vhost_limits();
    for row in list("vhostLimits") {
        if !vhosts.iter().any(|v| v.0 == text(&row, "vhost")) {
            apply(conns, store, "vhost_limits", &row);
        }
    }
    let topics = conns.list_topic_permissions(None);
    for row in list("topicPermissions") {
        let have = topics
            .iter()
            .any(|p| p.user == text(&row, "user") && p.vhost == text(&row, "vhost") && p.exchange == text(&row, "exchange"));
        if !have {
            apply(conns, store, "topic_permission", &row);
        }
    }
    for row in list("parameters") {
        let component = format!("{GENERIC}{}", text(&row, "component"));
        if store.get_parameter(&component, &text(&row, "vhost"), &text(&row, "name")).ok().flatten().is_none() {
            apply(conns, store, "parameter", &row);
        }
    }
    for row in list("globalParameters") {
        if store.get_parameter(GLOBAL, "", &text(&row, "name")).ok().flatten().is_none() {
            apply(conns, store, "global_parameter", &row);
        }
    }
}
