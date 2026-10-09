//! Cluster feature flags shared between the broker and the management API.
//!
//! The broker crate owns the cluster; the management crate only reports it.
//! This module is the one place both can read without depending on each
//! other.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::RwLock;

/// The `raft` flag of docs/raft.md, section 8.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftFlag {
    /// A single node, or a build that cannot run Raft.
    Unsupported,
    /// This node supports Raft; not every voter has advertised it yet.
    Disabled,
    /// Turned on, by itself in a new cluster or by an operator. Never turned off again.
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

type Enabler = Box<dyn Fn() -> bool + Send + Sync>;

static RAFT_ENABLER: RwLock<Option<Enabler>> = RwLock::new(None);

/// Install the function that turns Raft on for the whole cluster. The broker
/// sets it when its cluster starts.
pub fn set_raft_enabler(enabler: impl Fn() -> bool + Send + Sync + 'static) {
    *RAFT_ENABLER.write().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(enabler));
}

/// A quorum queue's or replicated stream's Raft view, for the management API.
#[derive(Debug, Clone, Default)]
pub struct QueueRaft {
    /// Current leader, empty during an election.
    pub leader: String,
    /// Voters of the queue's group.
    pub members: Vec<String>,
    /// `q:<vhost>/<name>`, or `quorum` for the shared group.
    pub group: String,
}

type RaftInfo = Box<dyn Fn(&str, &str) -> Option<QueueRaft> + Send + Sync>;

static RAFT_INFO: RwLock<Option<RaftInfo>> = RwLock::new(None);

/// Install the function that reports a queue's Raft view. The broker sets
/// it when its cluster starts.
pub fn set_queue_raft(info: impl Fn(&str, &str) -> Option<QueueRaft> + Send + Sync + 'static) {
    *RAFT_INFO.write().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(info));
}

/// The Raft view of queue `name` in `vhost`, when it has one.
pub fn queue_raft(vhost: &str, name: &str) -> Option<QueueRaft> {
    RAFT_INFO.read().unwrap_or_else(|e| e.into_inner()).as_ref().and_then(|f| f(vhost, name))
}

/// Enable the `raft` flag, as `PUT /api/feature-flags/raft/enable` does.
/// Returns false when there is no cluster or a voter cannot run Raft.
pub fn enable_raft() -> bool {
    RAFT_ENABLER
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .is_some_and(|enable| enable())
}
