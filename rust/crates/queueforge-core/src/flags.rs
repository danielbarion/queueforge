//! Cluster feature flags shared between the broker and the management API.
//!
//! The broker crate owns the cluster; the management crate only reports it.
//! This module is the one place both can read without depending on each
//! other.

use std::sync::atomic::{AtomicU8, Ordering};

/// The `raft` flag of docs/raft.md, section 8.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftFlag {
    /// A single node, or a build that cannot run Raft.
    Unsupported,
    /// This node supports Raft; not every voter has advertised it yet.
    Disabled,
    /// Every voter advertised Raft. Never turned off again.
    Enabled,
}

static RAFT: AtomicU8 = AtomicU8::new(0);

/// Record the `raft` flag state.
pub fn set_raft(flag: RaftFlag) {
    let v = match flag {
        RaftFlag::Unsupported => 0,
        RaftFlag::Disabled => 1,
        RaftFlag::Enabled => 2,
    };
    RAFT.store(v, Ordering::Release);
}

/// The current `raft` flag state.
pub fn raft() -> RaftFlag {
    match RAFT.load(Ordering::Acquire) {
        2 => RaftFlag::Enabled,
        1 => RaftFlag::Disabled,
        _ => RaftFlag::Unsupported,
    }
}
