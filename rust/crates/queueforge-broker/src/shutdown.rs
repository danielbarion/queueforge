//! Graceful process drain: track AMQP connections and broadcast shutdown.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tracing::{info, warn};

/// Time to wait for AMQP clients after broadcasting `connection.close` (design: 10s).
pub const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Tracks live AMQP connection tasks and broadcasts a process-wide drain signal.
///
/// Accept loops stop independently; once [`Self::begin_drain`] is called, each
/// connection that holds a [`watch::Receiver`] from [`Self::subscribe`] should
/// send `connection.close` and exit. [`Self::wait_drained`] waits until all
/// [`ConnectionGuard`]s drop (or the timeout elapses).
///
/// **Registration must happen before the connection task is invisible to the
/// tracker** (track in the accept loop before `tokio::spawn`).
#[derive(Clone)]
pub struct ConnectionTracker {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ConnectionTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionTracker")
            .field("draining", &self.is_draining())
            .field("active", &self.active())
            .finish()
    }
}

struct Inner {
    /// `false` = serving, `true` = draining.
    shutdown_tx: watch::Sender<bool>,
    /// Kept alive so [`watch::Sender::send`] always has a receiver and updates value.
    _shutdown_rx: watch::Receiver<bool>,
    active: AtomicUsize,
    zero: Notify,
}

impl Default for ConnectionTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionTracker {
    /// Create a tracker in the serving (not draining) state.
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                shutdown_tx,
                _shutdown_rx: shutdown_rx,
                active: AtomicUsize::new(0),
                zero: Notify::new(),
            }),
        }
    }

    /// Subscribe to the drain signal (`true` when draining).
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.inner.shutdown_tx.subscribe()
    }

    /// Whether [`Self::begin_drain`] has been called.
    pub fn is_draining(&self) -> bool {
        *self.inner.shutdown_tx.borrow()
    }

    /// Number of currently tracked connection tasks.
    pub fn active(&self) -> usize {
        self.inner.active.load(Ordering::Acquire)
    }

    /// Register a live connection. Drop the returned guard when the task ends.
    ///
    /// Call this **before** `tokio::spawn` so [`Self::wait_drained`] cannot
    /// observe `active == 0` while a just-accepted handler has not yet started.
    pub fn track(&self) -> ConnectionGuard {
        self.inner.active.fetch_add(1, Ordering::AcqRel);
        ConnectionGuard {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Broadcast drain to all subscribers (idempotent).
    pub fn begin_drain(&self) {
        if self.is_draining() {
            return;
        }
        info!(
            active = self.active(),
            "beginning AMQP connection drain (connection.close)"
        );
        let _ = self.inner.shutdown_tx.send(true);
    }

    /// Wait until all tracked connections finish or `timeout` elapses.
    ///
    /// Returns `true` if fully drained, `false` on timeout with connections left.
    ///
    /// Uses the tokio [`Notify`] pattern: register interest **before** re-checking
    /// `active` so a `notify_waiters` between check and wait is not lost.
    pub async fn wait_drained(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Issue 2: register notified future *before* checking the counter.
            let notified = self.inner.zero.notified();
            if self.active() == 0 {
                return true;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                let left = self.active();
                if left == 0 {
                    return true;
                }
                warn!(
                    remaining = left,
                    "AMQP connection drain timed out; continuing shutdown"
                );
                return false;
            }
            let remaining = deadline - now;
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }
    }
}

/// RAII ticket for one live AMQP connection task.
pub struct ConnectionGuard {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ConnectionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionGuard")
            .field("active", &self.inner.active.load(Ordering::Acquire))
            .finish()
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let prev = self.inner.active.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.inner.zero.notify_waiters();
        }
        debug_assert!(prev > 0, "ConnectionGuard underflow");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tracker_drain_waits_for_guards() {
        let tracker = ConnectionTracker::new();
        assert!(!tracker.is_draining());
        assert_eq!(tracker.active(), 0);

        let g1 = tracker.track();
        let g2 = tracker.track();
        assert_eq!(tracker.active(), 2);

        tracker.begin_drain();
        assert!(tracker.is_draining());

        let t = tracker.clone();
        let wait = tokio::spawn(async move { t.wait_drained(Duration::from_secs(2)).await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!wait.is_finished());

        drop(g1);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!wait.is_finished());

        drop(g2);
        let drained = wait.await.expect("join");
        assert!(drained);
        assert_eq!(tracker.active(), 0);
    }

    #[tokio::test]
    async fn tracker_wait_timeout_returns_false() {
        let tracker = ConnectionTracker::new();
        let _g = tracker.track();
        tracker.begin_drain();
        let drained = tracker.wait_drained(Duration::from_millis(50)).await;
        assert!(!drained);
        assert_eq!(tracker.active(), 1);
    }

    /// Regression for Issue 2: last guard drops in the window between active
    /// check and notify registration — drain must still finish quickly.
    #[tokio::test(flavor = "current_thread")]
    async fn wait_drained_no_lost_notify_wakeup() {
        // Stress the race: waiter loops check→notified while last guard drops.
        for _ in 0..100 {
            let tracker = ConnectionTracker::new();
            let guard = tracker.track();
            tracker.begin_drain();

            let t = tracker.clone();
            let wait = tokio::spawn(async move { t.wait_drained(Duration::from_secs(2)).await });

            // Yield so wait_drained can run past active!=0 and toward notified().
            tokio::task::yield_now().await;
            drop(guard);

            let start = std::time::Instant::now();
            let drained = wait.await.expect("join");
            let elapsed = start.elapsed();
            assert!(drained, "must report fully drained");
            assert!(
                elapsed < Duration::from_millis(200),
                "lost Notify wakeup slept too long: {elapsed:?}"
            );
            assert_eq!(tracker.active(), 0);
        }
    }

    /// Registration before spawn: wait_drained must not complete while a
    /// pre-spawn guard is still held (Issue 1).
    #[tokio::test]
    async fn pre_spawn_track_is_visible_to_wait_drained() {
        let tracker = ConnectionTracker::new();
        // Simulate accept loop: track before the task body runs.
        let guard = tracker.track();
        tracker.begin_drain();

        let t = tracker.clone();
        let wait = tokio::spawn(async move { t.wait_drained(Duration::from_millis(100)).await });

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !wait.is_finished(),
            "wait_drained must not finish while pre-spawn guard lives"
        );
        assert_eq!(tracker.active(), 1);

        drop(guard);
        let drained = wait.await.expect("join");
        assert!(drained);
    }
}
