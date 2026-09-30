//! Live AMQP connection tracker for management list/force-close.
//!
//! Each registered connection holds a [`tokio::sync::watch`] sender so the
//! management API can request a server-initiated `connection.close`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Process-wide registry of live AMQP connections.
#[derive(Debug, Default)]
pub struct ConnectionTracker {
    next_id: AtomicU64,
    connections: Mutex<HashMap<String, TrackedConnection>>,
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
