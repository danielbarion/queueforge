//! Global memory watermark accounting for ready/unacked queue messages.
//!
//! Tracked total = payload + fixed per-message overhead (see [`crate::queue::Message::tracked_bytes`]).
//! Soft alarm logs + metric; hard alarm blocks new publishes via [`MemoryTracker::try_add`].

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tracing::warn;

use crate::config::MemoryConfig;
use crate::sysinfo::system_total_memory_bytes;

/// Process-wide tracked message/body bytes with soft/hard watermarks.
#[derive(Debug)]
pub struct MemoryTracker {
    tracked_bytes: AtomicU64,
    /// Soft limit in bytes (`u64::MAX` = disabled).
    soft_limit_bytes: AtomicU64,
    /// Hard limit in bytes (`u64::MAX` = disabled / unlimited).
    hard_limit_bytes: AtomicU64,
    soft_alarm: AtomicBool,
    hard_alarm: AtomicBool,
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self {
            tracked_bytes: AtomicU64::new(0),
            soft_limit_bytes: AtomicU64::new(u64::MAX),
            hard_limit_bytes: AtomicU64::new(u64::MAX),
            soft_alarm: AtomicBool::new(false),
            hard_alarm: AtomicBool::new(false),
        }
    }
}

impl MemoryTracker {
    /// Create a tracker with no effective limits (tests / recovery-only stubs).
    pub fn new() -> Self {
        Self::default()
    }

    /// Wrap in an [`Arc`] for sharing across the registry and actors.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Absolute soft/hard byte limits (`u64::MAX` disables that level).
    pub fn with_limits(soft_limit_bytes: u64, hard_limit_bytes: u64) -> Self {
        let t = Self::new();
        t.set_limits(soft_limit_bytes, hard_limit_bytes);
        t
    }

    /// Shared tracker from absolute limits.
    pub fn shared_with_limits(soft_limit_bytes: u64, hard_limit_bytes: u64) -> Arc<Self> {
        Arc::new(Self::with_limits(soft_limit_bytes, hard_limit_bytes))
    }

    /// Build limits from relative fractions of detected system RAM.
    pub fn from_config(cfg: &MemoryConfig) -> Self {
        let total = system_total_memory_bytes();
        let soft = relative_to_bytes(cfg.soft_watermark_relative, total);
        let hard = relative_to_bytes(cfg.high_watermark_relative, total);
        Self::with_limits(soft, hard)
    }

    /// Shared tracker from config relative watermarks.
    pub fn shared_from_config(cfg: &MemoryConfig) -> Arc<Self> {
        Arc::new(Self::from_config(cfg))
    }

    /// Replace soft/hard limits (bytes). `u64::MAX` disables.
    pub fn set_limits(&self, soft_limit_bytes: u64, hard_limit_bytes: u64) {
        self.soft_limit_bytes
            .store(soft_limit_bytes.max(1), Ordering::Relaxed);
        self.hard_limit_bytes
            .store(hard_limit_bytes.max(1), Ordering::Relaxed);
        self.refresh_alarms();
    }

    /// Soft limit in bytes.
    pub fn soft_limit_bytes(&self) -> u64 {
        self.soft_limit_bytes.load(Ordering::Relaxed)
    }

    /// Hard limit in bytes.
    pub fn hard_limit_bytes(&self) -> u64 {
        self.hard_limit_bytes.load(Ordering::Relaxed)
    }

    /// Current tracked byte count.
    pub fn tracked_bytes(&self) -> u64 {
        self.tracked_bytes.load(Ordering::Relaxed)
    }

    /// Soft alarm currently raised.
    pub fn soft_alarm(&self) -> bool {
        self.soft_alarm.load(Ordering::Relaxed)
    }

    /// Hard alarm currently raised (new publishes should be blocked).
    pub fn hard_alarm(&self) -> bool {
        self.hard_alarm.load(Ordering::Relaxed)
    }

    /// Whether adding `n` more tracked bytes would exceed the hard watermark.
    pub fn would_exceed_hard(&self, n: u64) -> bool {
        let hard = self.hard_limit_bytes();
        if hard == u64::MAX {
            return false;
        }
        self.tracked_bytes()
            .checked_add(n)
            .map(|sum| sum > hard)
            .unwrap_or(true)
    }

    /// Unconditionally add `n` bytes (recovery path; may raise alarms).
    pub fn add(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.tracked_bytes.fetch_add(n, Ordering::Relaxed);
        self.refresh_alarms();
    }

    /// Try to reserve `n` tracked bytes under the hard watermark.
    ///
    /// Returns `false` without mutating the counter when the hard limit would be
    /// exceeded. Hard alarm is derived solely from `tracked >= hard_limit` (not
    /// forced on a refused attempt that leaves the total still under hard).
    pub fn try_add(&self, n: u64) -> bool {
        if n == 0 {
            return true;
        }
        let hard = self.hard_limit_bytes();
        loop {
            let cur = self.tracked_bytes.load(Ordering::Relaxed);
            if hard != u64::MAX {
                match cur.checked_add(n) {
                    Some(sum) if sum <= hard => {}
                    _ => {
                        // Do not force hard_alarm when tracked is still under the
                        // limit; blocked attempts are counted via publish metrics.
                        self.refresh_alarms();
                        warn!(
                            tracked = cur,
                            hard_limit = hard,
                            requested = n,
                            "memory hard watermark would be exceeded; blocking publish"
                        );
                        return false;
                    }
                }
            }
            let next = cur.saturating_add(n);
            if self
                .tracked_bytes
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.refresh_alarms();
                return true;
            }
        }
    }

    /// Subtract `n` bytes from the tracked total (floors at zero).
    pub fn sub(&self, n: u64) {
        if n == 0 {
            return;
        }
        let _ = self
            .tracked_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(n))
            });
        self.refresh_alarms();
    }

    /// Reset to zero (tests / shutdown).
    pub fn reset(&self) {
        self.tracked_bytes.store(0, Ordering::Relaxed);
        self.soft_alarm.store(false, Ordering::Relaxed);
        self.hard_alarm.store(false, Ordering::Relaxed);
        self.publish_alarm_metrics(false, false);
        metrics::gauge!("queueforge_memory_tracked_bytes").set(0.0);
    }

    /// Recompute soft/hard alarm flags from the current total and emit metrics.
    pub fn refresh_alarms(&self) {
        let tracked = self.tracked_bytes();
        let soft_limit = self.soft_limit_bytes();
        let hard_limit = self.hard_limit_bytes();

        let soft_now = soft_limit != u64::MAX && tracked >= soft_limit;
        let hard_now = hard_limit != u64::MAX && tracked >= hard_limit;

        let soft_was = self.soft_alarm.swap(soft_now, Ordering::Relaxed);
        let hard_was = self.hard_alarm.swap(hard_now, Ordering::Relaxed);

        if soft_now && !soft_was {
            warn!(tracked, soft_limit, "memory soft watermark exceeded");
        }
        if hard_now && !hard_was {
            warn!(
                tracked,
                hard_limit, "memory hard watermark exceeded; blocking publishes"
            );
        }
        if soft_was && !soft_now {
            warn!(tracked, soft_limit, "memory soft watermark cleared");
        }
        if hard_was && !hard_now {
            warn!(tracked, hard_limit, "memory hard watermark cleared");
        }

        self.publish_alarm_metrics(soft_now, hard_now);
        metrics::gauge!("queueforge_memory_tracked_bytes").set(tracked as f64);
    }

    fn publish_alarm_metrics(&self, soft: bool, hard: bool) {
        metrics::gauge!("queueforge_memory_alarm", "level" => "soft").set(if soft {
            1.0
        } else {
            0.0
        });
        metrics::gauge!("queueforge_memory_alarm", "level" => "hard").set(if hard {
            1.0
        } else {
            0.0
        });
    }
}

fn relative_to_bytes(relative: f64, total: u64) -> u64 {
    if relative <= 0.0 {
        return 1;
    }
    if relative >= 1.0 {
        return total.max(1);
    }
    let bytes = (total as f64 * relative).floor() as u64;
    bytes.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_sub_and_floor() {
        let t = MemoryTracker::new();
        t.add(100);
        assert_eq!(t.tracked_bytes(), 100);
        t.sub(40);
        assert_eq!(t.tracked_bytes(), 60);
        t.sub(1000);
        assert_eq!(t.tracked_bytes(), 0);
        t.reset();
        assert_eq!(t.tracked_bytes(), 0);
    }

    #[test]
    fn soft_and_hard_alarms() {
        let t = MemoryTracker::with_limits(50, 100);
        assert!(!t.soft_alarm());
        assert!(!t.hard_alarm());

        assert!(t.try_add(50));
        assert!(t.soft_alarm());
        assert!(!t.hard_alarm());

        assert!(t.try_add(50));
        assert!(t.soft_alarm());
        assert!(t.hard_alarm());
        assert_eq!(t.tracked_bytes(), 100);

        // Over hard limit is refused.
        assert!(!t.try_add(1));
        assert_eq!(t.tracked_bytes(), 100);
        assert!(t.hard_alarm());

        t.sub(60);
        assert_eq!(t.tracked_bytes(), 40);
        assert!(!t.soft_alarm());
        assert!(!t.hard_alarm());
    }

    #[test]
    fn unlimited_by_default() {
        let t = MemoryTracker::new();
        assert!(t.try_add(u64::MAX / 4));
        assert!(!t.hard_alarm());
    }

    #[test]
    fn from_config_scales_with_relative() {
        let cfg = MemoryConfig {
            high_watermark_relative: 0.6,
            soft_watermark_relative: 0.5,
        };
        let t = MemoryTracker::from_config(&cfg);
        assert!(t.hard_limit_bytes() >= t.soft_limit_bytes());
        assert!(t.hard_limit_bytes() < u64::MAX);
    }

    #[test]
    fn would_exceed_hard() {
        let t = MemoryTracker::with_limits(10, 20);
        t.add(15);
        assert!(t.would_exceed_hard(6));
        assert!(!t.would_exceed_hard(5));
    }

    #[test]
    fn refused_try_add_does_not_force_hard_alarm_when_under_limit() {
        let t = MemoryTracker::with_limits(50, 100);
        // Empty tracker; request larger than hard — refuse but tracked still 0.
        assert!(!t.try_add(101));
        assert_eq!(t.tracked_bytes(), 0);
        assert!(!t.hard_alarm());
        assert!(!t.soft_alarm());
    }
}
