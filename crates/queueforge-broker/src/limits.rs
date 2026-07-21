//! Connection and process resource limit helpers for the AMQP frontend.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Atomic gate for the maximum number of concurrent AMQP connections.
///
/// `max == 0` means unlimited. Acquire on accept; release when the connection
/// task ends (whether or not the AMQP handshake completed).
#[derive(Debug)]
pub struct ConnectionLimiter {
    active: AtomicU32,
    max: u32,
}

impl ConnectionLimiter {
    /// Create a limiter. `max == 0` disables the cap.
    pub fn new(max: u32) -> Self {
        Self {
            active: AtomicU32::new(0),
            max,
        }
    }

    /// Shared handle for the accept loop and connection tasks.
    pub fn shared(max: u32) -> Arc<Self> {
        Arc::new(Self::new(max))
    }

    /// Configured maximum (`0` = unlimited).
    pub fn max(&self) -> u32 {
        self.max
    }

    /// Current in-flight connection count (accepted, not yet released).
    pub fn active(&self) -> u32 {
        self.active.load(Ordering::Relaxed)
    }

    /// Try to reserve a connection slot. Returns `false` when at capacity.
    pub fn try_acquire(&self) -> bool {
        if self.max == 0 {
            self.active.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        loop {
            let cur = self.active.load(Ordering::Relaxed);
            if cur >= self.max {
                return false;
            }
            if self
                .active
                .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Release a previously acquired slot.
    pub fn release(&self) {
        let _ = self
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(1))
            });
    }
}

/// RAII permit that releases the connection slot on drop.
pub struct ConnectionPermit {
    limiter: Arc<ConnectionLimiter>,
}

impl ConnectionPermit {
    /// Acquire a permit, or `None` if at capacity.
    pub fn try_acquire(limiter: &Arc<ConnectionLimiter>) -> Option<Self> {
        if limiter.try_acquire() {
            Some(Self {
                limiter: Arc::clone(limiter),
            })
        } else {
            None
        }
    }
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforces_max() {
        let lim = ConnectionLimiter::shared(2);
        let a = ConnectionPermit::try_acquire(&lim).expect("1");
        let b = ConnectionPermit::try_acquire(&lim).expect("2");
        assert!(ConnectionPermit::try_acquire(&lim).is_none());
        assert_eq!(lim.active(), 2);
        drop(a);
        assert_eq!(lim.active(), 1);
        let _c = ConnectionPermit::try_acquire(&lim).expect("after release");
        drop(b);
    }

    #[test]
    fn zero_is_unlimited() {
        let lim = ConnectionLimiter::shared(0);
        for _ in 0..100 {
            assert!(lim.try_acquire());
        }
        assert_eq!(lim.active(), 100);
        for _ in 0..100 {
            lim.release();
        }
        assert_eq!(lim.active(), 0);
    }
}
