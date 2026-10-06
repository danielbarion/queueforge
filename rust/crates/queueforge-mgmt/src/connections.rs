//! Live AMQP connection tracker for management list/force-close.
//!
//! Each registered connection holds a [`tokio::sync::watch`] sender so the
//! management API can request a server-initiated `connection.close`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::watch;

/// Snapshot of a live AMQP connection for management listing.
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Opaque connection id (stable for the connection lifetime).
    pub id: String,
    /// Peer socket address.
    pub peer: SocketAddr,
    /// Authenticated username (empty before open-ok).
    pub user: String,
    /// Selected virtual host (empty before open-ok).
    pub vhost: String,
    /// Unix timestamp (seconds) when the connection opened.
    pub connected_at: u64,
    /// Number of open channels.
    pub channels: u32,
    /// Client-provided connection name / properties summary (optional).
    pub client_properties: String,
}

/// Internally tracked connection plus force-close signal.
#[derive(Debug)]
struct TrackedConnection {
    info: ConnectionInfo,
    /// `true` means administrator requested force-close.
    force_close: watch::Sender<bool>,
}

/// One open AMQP channel on a tracked connection.
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    /// `{connection}:{number}`.
    pub name: String,
    /// Parent connection id.
    pub connection: String,
    /// Channel number.
    pub number: u16,
    /// Authenticated user.
    pub user: String,
    /// Virtual host.
    pub vhost: String,
    /// Peer host.
    pub peer_host: String,
    /// Peer port.
    pub peer_port: u16,
}

/// One live consumer.
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    /// Consumer tag.
    pub consumer_tag: String,
    /// Parent connection id.
    pub connection: String,
    /// Channel number.
    pub channel: u16,
    /// Queue vhost.
    pub vhost: String,
    /// Queue name.
    pub queue: String,
}

/// Topic permission row. Patterns are regular expressions.
#[derive(Debug, Clone)]
pub struct TopicPermission {
    /// User name.
    pub user: String,
    /// Virtual host.
    pub vhost: String,
    /// Exchange the patterns apply to.
    pub exchange: String,
    /// Publish routing-key pattern.
    pub write: String,
    /// Consume routing-key pattern.
    pub read: String,
}

#[derive(Debug, Default)]
struct Limits {
    user_connections: HashMap<String, u32>,
    user_channels: HashMap<String, u32>,
    vhost_connections: HashMap<String, u32>,
    vhost_queues: HashMap<String, u32>,
}

/// Process-wide registry of live AMQP connections.
#[derive(Debug, Default)]
pub struct ConnectionTracker {
    next_id: AtomicU64,
    connections: Mutex<HashMap<String, TrackedConnection>>,
    channels: Mutex<HashMap<String, ChannelInfo>>,
    consumers: Mutex<Vec<ConsumerInfo>>,
    limits: Mutex<Limits>,
    topic_perms: Mutex<Vec<TopicPermission>>,
    transient_nonexcl: AtomicBool,
}

impl ConnectionTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Shared handle for cloning into connection tasks and the management API.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Register a new connection; returns the assigned id and a force-close receiver.
    ///
    /// The receiver starts at `false`. When [`Self::force_close`] is called for this
    /// id, the sender publishes `true` and the AMQP task should close the connection.
    ///
    /// Call after successful `connection.open` (when user + vhost are known).
    pub fn register(
        &self,
        peer: SocketAddr,
        user: impl Into<String>,
        vhost: impl Into<String>,
    ) -> (String, watch::Receiver<bool>) {
        let id = format!("conn-{}", self.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = watch::channel(false);
        let info = ConnectionInfo {
            id: id.clone(),
            peer,
            user: user.into(),
            vhost: vhost.into(),
            connected_at: unix_now(),
            channels: 0,
            client_properties: String::new(),
        };
        self.connections
            .lock()
            .expect("connection tracker poisoned")
            .insert(
                id.clone(),
                TrackedConnection {
                    info,
                    force_close: tx,
                },
            );
        (id, rx)
    }

    /// Request force-close of a connection by id.
    ///
    /// Returns `true` if the connection existed and the signal was sent.
    /// The AMQP connection task observes the watch receiver and performs the close.
    pub fn force_close(&self, id: &str) -> bool {
        let guard = self
            .connections
            .lock()
            .expect("connection tracker poisoned");
        if let Some(tracked) = guard.get(id) {
            let _ = tracked.force_close.send(true);
            true
        } else {
            false
        }
    }

    /// Update channel count for a connection.
    pub fn set_channels(&self, id: &str, channels: u32) {
        if let Some(c) = self
            .connections
            .lock()
            .expect("connection tracker poisoned")
            .get_mut(id)
        {
            c.info.channels = channels;
        }
    }

    /// Remove a connection on close.
    pub fn unregister(&self, id: &str) {
        self.connections
            .lock()
            .expect("connection tracker poisoned")
            .remove(id);
        self.clear_connection_children(id);
    }

    /// Snapshot all connections (sorted by id).
    pub fn list(&self) -> Vec<ConnectionInfo> {
        let mut items: Vec<_> = self
            .connections
            .lock()
            .expect("connection tracker poisoned")
            .values()
            .map(|t| t.info.clone())
            .collect();
        items.sort_by(|a, b| a.id.cmp(&b.id));
        items
    }

    /// Number of live connections.
    pub fn len(&self) -> usize {
        self.connections
            .lock()
            .expect("connection tracker poisoned")
            .len()
    }

    /// Whether there are no live connections.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Replace the channel set for one connection.
    pub fn sync_channels(
        &self,
        conn_id: &str,
        user: &str,
        vhost: &str,
        peer: SocketAddr,
        numbers: &[u16],
    ) {
        let mut guard = self.channels.lock().expect("channel tracker poisoned");
        guard.retain(|_, ch| ch.connection != conn_id);
        for number in numbers {
            let name = format!("{conn_id}:{number}");
            guard.insert(
                name.clone(),
                ChannelInfo {
                    name,
                    connection: conn_id.to_string(),
                    number: *number,
                    user: user.to_string(),
                    vhost: vhost.to_string(),
                    peer_host: peer.ip().to_string(),
                    peer_port: peer.port(),
                },
            );
        }
        if let Some(conn) = self
            .connections
            .lock()
            .expect("connection tracker poisoned")
            .get_mut(conn_id)
        {
            conn.info.channels = numbers.len() as u32;
        }
    }

    /// Drop every channel and consumer for a connection.
    pub fn clear_connection_children(&self, conn_id: &str) {
        self.channels
            .lock()
            .expect("channel tracker poisoned")
            .retain(|_, ch| ch.connection != conn_id);
        self.consumers
            .lock()
            .expect("consumer tracker poisoned")
            .retain(|c| c.connection != conn_id);
    }

    /// Replace consumers observed on one connection.
    pub fn sync_consumers(&self, conn_id: &str, items: Vec<ConsumerInfo>) {
        let mut guard = self.consumers.lock().expect("consumer tracker poisoned");
        guard.retain(|c| c.connection != conn_id);
        guard.extend(items);
    }

    /// Channels sorted by name.
    pub fn list_channels(&self) -> Vec<ChannelInfo> {
        let mut items: Vec<_> = self
            .channels
            .lock()
            .expect("channel tracker poisoned")
            .values()
            .cloned()
            .collect();
        items.sort_by(|a, b| a.name.cmp(&b.name));
        items
    }

    /// One channel by name.
    pub fn get_channel(&self, name: &str) -> Option<ChannelInfo> {
        self.channels
            .lock()
            .expect("channel tracker poisoned")
            .get(name)
            .cloned()
    }

    /// Consumers, optionally filtered by vhost and queue.
    pub fn list_consumers(&self, vhost: Option<&str>, queue: Option<&str>) -> Vec<ConsumerInfo> {
        self.consumers
            .lock()
            .expect("consumer tracker poisoned")
            .iter()
            .filter(|c| {
                vhost.map(|v| c.vhost == v).unwrap_or(true)
                    && queue.map(|q| c.queue == q).unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    /// `true` when another connection for this user or vhost is still allowed.
    pub fn connection_allowed(&self, user: &str, vhost: &str) -> bool {
        let limits = self.limits.lock().expect("limits poisoned");
        let conns = self
            .connections
            .lock()
            .expect("connection tracker poisoned");
        if let Some(max) = limits.user_connections.get(user) {
            let n = conns.values().filter(|c| c.info.user == user).count();
            if n >= *max as usize {
                return false;
            }
        }
        if let Some(max) = limits.vhost_connections.get(vhost) {
            let n = conns.values().filter(|c| c.info.vhost == vhost).count();
            if n >= *max as usize {
                return false;
            }
        }
        true
    }

    /// `true` when this user may open another channel.
    pub fn channel_allowed(&self, user: &str) -> bool {
        let limits = self.limits.lock().expect("limits poisoned");
        let Some(max) = limits.user_channels.get(user).copied() else {
            return true;
        };
        let n = self
            .channels
            .lock()
            .expect("channel tracker poisoned")
            .values()
            .filter(|c| c.user == user)
            .count();
        n < max as usize
    }

    /// `true` when this vhost may hold another queue.
    pub fn queue_allowed(&self, vhost: &str, current: usize) -> bool {
        let limits = self.limits.lock().expect("limits poisoned");
        match limits.vhost_queues.get(vhost) {
            Some(max) => current < *max as usize,
            None => true,
        }
    }

    /// Set or clear a user connection/channel limit. `None` removes that limit.
    pub fn set_user_limit(
        &self,
        user: &str,
        max_connections: Option<u32>,
        max_channels: Option<u32>,
    ) {
        let mut limits = self.limits.lock().expect("limits poisoned");
        match max_connections {
            Some(n) => {
                limits.user_connections.insert(user.to_string(), n);
            }
            None => {
                limits.user_connections.remove(user);
            }
        }
        match max_channels {
            Some(n) => {
                limits.user_channels.insert(user.to_string(), n);
            }
            None => {
                limits.user_channels.remove(user);
            }
        }
    }

    /// Set or clear a vhost connection/queue limit.
    pub fn set_vhost_limit(
        &self,
        vhost: &str,
        max_connections: Option<u32>,
        max_queues: Option<u32>,
    ) {
        let mut limits = self.limits.lock().expect("limits poisoned");
        match max_connections {
            Some(n) => {
                limits.vhost_connections.insert(vhost.to_string(), n);
            }
            None => {
                limits.vhost_connections.remove(vhost);
            }
        }
        match max_queues {
            Some(n) => {
                limits.vhost_queues.insert(vhost.to_string(), n);
            }
            None => {
                limits.vhost_queues.remove(vhost);
            }
        }
    }

    /// Current limit rows for the management Limits page.
    pub fn list_user_limits(&self) -> Vec<(String, Option<u32>, Option<u32>)> {
        let limits = self.limits.lock().expect("limits poisoned");
        let mut users: Vec<String> = limits
            .user_connections
            .keys()
            .chain(limits.user_channels.keys())
            .cloned()
            .collect();
        users.sort();
        users.dedup();
        users
            .into_iter()
            .map(|user| {
                let connections = limits.user_connections.get(&user).copied();
                let channels = limits.user_channels.get(&user).copied();
                (user, connections, channels)
            })
            .collect()
    }

    /// Current vhost limit rows.
    pub fn list_vhost_limits(&self) -> Vec<(String, Option<u32>, Option<u32>)> {
        let limits = self.limits.lock().expect("limits poisoned");
        let mut vhosts: Vec<String> = limits
            .vhost_connections
            .keys()
            .chain(limits.vhost_queues.keys())
            .cloned()
            .collect();
        vhosts.sort();
        vhosts.dedup();
        vhosts
            .into_iter()
            .map(|vhost| {
                let connections = limits.vhost_connections.get(&vhost).copied();
                let queues = limits.vhost_queues.get(&vhost).copied();
                (vhost, connections, queues)
            })
            .collect()
    }

    /// Replace one topic permission.
    pub fn put_topic_permission(&self, perm: TopicPermission) {
        let mut rows = self.topic_perms.lock().expect("topic perms poisoned");
        if let Some(slot) = rows.iter().position(|p| {
            p.user == perm.user && p.vhost == perm.vhost && p.exchange == perm.exchange
        }) {
            rows[slot] = perm;
        } else {
            rows.push(perm);
        }
    }

    /// Remove one topic permission.
    pub fn delete_topic_permission(&self, user: &str, vhost: &str, exchange: &str) -> bool {
        let mut rows = self.topic_perms.lock().expect("topic perms poisoned");
        let before = rows.len();
        rows.retain(|p| !(p.user == user && p.vhost == vhost && p.exchange == exchange));
        rows.len() != before
    }

    /// Topic permissions, optionally for one user.
    pub fn list_topic_permissions(&self, user: Option<&str>) -> Vec<TopicPermission> {
        let mut rows: Vec<_> = self
            .topic_perms
            .lock()
            .expect("topic perms poisoned")
            .iter()
            .filter(|p| user.map(|u| p.user == u).unwrap_or(true))
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            (&a.user, &a.vhost, &a.exchange).cmp(&(&b.user, &b.vhost, &b.exchange))
        });
        rows
    }

    /// When a topic permission exists for this publish, the routing key must match `write`.
    pub fn topic_write_allowed(
        &self,
        user: &str,
        vhost: &str,
        exchange: &str,
        routing_key: &str,
    ) -> bool {
        let rows = self.topic_perms.lock().expect("topic perms poisoned");
        let Some(perm) = rows
            .iter()
            .find(|p| p.user == user && p.vhost == vhost && p.exchange == exchange)
        else {
            return true;
        };
        regex::Regex::new(&perm.write).is_ok_and(|re| re.is_match(routing_key))
    }

    /// Whether deprecated transient non-exclusive queues are permitted.
    pub fn transient_nonexcl_permitted(&self) -> bool {
        self.transient_nonexcl.load(Ordering::Relaxed)
    }

    /// Permit or deny deprecated transient non-exclusive queues.
    pub fn set_transient_nonexcl(&self, permitted: bool) {
        self.transient_nonexcl.store(permitted, Ordering::Relaxed);
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_list_unregister() {
        let t = ConnectionTracker::new();
        let (id, mut rx) = t.register("127.0.0.1:1234".parse().unwrap(), "admin", "/");
        assert_eq!(t.len(), 1);
        assert!(!*rx.borrow_and_update());
        let list = t.list();
        assert_eq!(list[0].id, id);
        assert_eq!(list[0].user, "admin");
        t.unregister(&id);
        assert!(t.is_empty());
    }

    #[test]
    fn force_close_signals_receiver() {
        let t = ConnectionTracker::new();
        let (id, mut rx) = t.register("127.0.0.1:5672".parse().unwrap(), "admin", "/");
        assert!(!t.force_close("missing"));
        assert!(t.force_close(&id));
        assert!(rx.has_changed().unwrap_or(false) || *rx.borrow());
        // Polling has_changed/borrow is enough in this unit test; do not leave a future.
        assert!(*rx.borrow_and_update());
        t.unregister(&id);
        assert!(!t.force_close(&id));
    }
}
