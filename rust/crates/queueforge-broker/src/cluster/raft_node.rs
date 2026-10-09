//! The Raft driver: runs the `meta` and `quorum` groups of docs/raft.md on
//! [`super::raft::Core`], and one `q:v2:<hex-vhost>:<hex-name>` group per quorum queue
//! or replicated stream when every voter runs them (`raft/q-<hash>/`, with
//! `group.json` naming the group, as Bun lays them out).
//!
//! One OS thread owns every core. It writes what a core reports dirty, with
//! an fsync, before any message that depends on it leaves, then hands
//! committed entries in log order to an applier task on the broker runtime.
//! A proposal resolves once the applier has applied its entry here (or, for
//! a forwarded one, on the leader).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use super::raft::{Core, Entry, Role, Snapshot};

/// Metadata group (section 5).
pub(super) const META: &str = "meta";
/// Quorum queue group (section 6).
pub(super) const QUORUM: &str = "quorum";
const GROUPS: [&str; 2] = [META, QUORUM];

/// The group of one quorum queue or replicated stream (docs/raft.md, section 2).
pub(super) fn queue_group(vhost: &str, name: &str) -> String {
    // Versioned, separate UTF-8 components avoid delimiter collisions. Existing
    // groups retain their persisted names; this is only used for new queues.
    let hex = |value: &str| {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        value
            .bytes()
            .flat_map(|b| {
                [
                    DIGITS[(b >> 4) as usize] as char,
                    DIGITS[(b & 15) as usize] as char,
                ]
            })
            .collect::<String>()
    };
    format!("q:v2:{}:{}", hex(vhost), hex(name))
}

/// Directory of a group under `raft/`. A queue group's name is hashed
/// (FNV-1a, 64 bits), as names may hold any byte; Bun uses the same names.
fn group_dir(group: &str) -> String {
    if group == META || group == QUORUM {
        return group.to_string();
    }
    let mut h: u64 = 0xcbf29ce484222325;
    for b in group.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("q-{h:016x}")
}
const TICK: Duration = Duration::from_millis(20);
/// A proposer gives up after 5 s (section 4, propose_r).
const PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Applied entries kept in the log before a snapshot replaces them (section 7).
pub(super) fn compact_after(group: &str) -> u64 {
    if group == META {
        50_000
    } else {
        10_000
    }
}

/// Who is told when an entry has been applied.
pub(super) enum Waiter {
    /// A proposal made on this node.
    Local(oneshot::Sender<Result<(), String>>),
    /// A proposal a follower forwarded; answered with `propose_r`.
    Remote { from: String, rid: String },
}

/// What the applier receives, in order.
pub(super) enum Applied {
    /// Committed entries of one group, each with the proposal waiting on it.
    Entries {
        group: String,
        items: Vec<(Entry, Option<Waiter>)>,
    },
    /// A snapshot that replaces the group's state, from disk or the leader.
    Install { group: String, snapshot: Snapshot },
    /// The group's leader changed. `None` while an election runs.
    Leader {
        group: String,
        leader: Option<String>,
    },
}

enum Cmd {
    Step {
        from: String,
        msg: Value,
    },
    Propose {
        group: String,
        kind: String,
        data: Value,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Compact {
        group: String,
        index: u64,
        state: Value,
    },
    Voters(Vec<String>),
    /// Start a queue group; `lead` campaigns at once.
    AddGroup {
        group: String,
        lead: bool,
    },
    /// Stop a queue group and remove its files.
    DropGroup {
        group: String,
    },
    /// The disk thread wrote `group` through `index`.
    Durable {
        group: String,
        index: u64,
    },
}

/// Leader and voters of one group, as last seen by the driver thread.
#[derive(Clone, Debug, Default)]
pub(super) struct GroupView {
    pub leader: Option<String>,
    pub voters: Vec<String>,
}

/// Handle to the driver thread.
pub(super) struct RaftNode {
    tx: StdMutex<std_mpsc::Sender<Cmd>>,
    view: Arc<StdMutex<HashMap<String, GroupView>>>,
    out: mpsc::UnboundedSender<(String, Value)>,
}

impl RaftNode {
    /// Load both groups from `dir` and start the driver thread.
    ///
    /// `out` carries `(peer, payload)` pairs for the caller to wrap in an
    /// `op:"raft"` envelope. `applied` receives commits in order.
    pub(super) fn start(
        node_id: &str,
        dir: &Path,
        voters: Vec<String>,
        out: mpsc::UnboundedSender<(String, Value)>,
        applied: mpsc::UnboundedSender<Applied>,
    ) -> std::io::Result<Arc<RaftNode>> {
        let (tx, rx) = std_mpsc::channel();
        let view = Arc::new(StdMutex::new(HashMap::new()));
        let mut names: Vec<String> = GROUPS.iter().map(|g| g.to_string()).collect();
        // Queue groups this node ran before a restart.
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let file = entry.file_name();
                let Some(name) = file.to_str() else { continue };
                if !name.starts_with("q-") {
                    continue;
                }
                let named = fs::read(entry.path().join("group.json"))
                    .ok()
                    .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                    .and_then(|v| v.get("group").and_then(|g| g.as_str()).map(str::to_string));
                // A directory left half made by a crash names nothing.
                if let Some(group) = named {
                    names.push(group);
                }
            }
        }
        let mut groups = Vec::new();
        for (i, name) in names.into_iter().enumerate() {
            groups.push(open_group(
                node_id, dir, &name, &voters, i as u64, &applied, 0,
            )?);
        }
        let node = Arc::new(RaftNode {
            tx: StdMutex::new(tx.clone()),
            view: Arc::clone(&view),
            out: out.clone(),
        });
        let (disk_tx, disk_rx) = std_mpsc::channel();
        let disk_out = out.clone();
        let disk_back = tx.clone();
        let disk_dir = dir.to_path_buf();
        std::thread::Builder::new()
            .name("queueforge-raft-disk".into())
            .spawn(move || run_disk(disk_dir, disk_rx, disk_out, disk_back))?;
        let mut driver = Driver {
            node_id: node_id.to_string(),
            dir: dir.to_path_buf(),
            groups,
            pending: Vec::new(),
            next_rid: 0,
            wanted: crate_sorted(voters),
            out,
            applied,
            view,
            disk: disk_tx,
        };
        std::thread::Builder::new()
            .name("queueforge-raft".into())
            .spawn(move || driver.run(rx))?;
        info!(node = node_id, "raft groups started");
        Ok(node)
    }

    fn send(&self, cmd: Cmd) {
        let _ = self.tx.lock().unwrap_or_else(|e| e.into_inner()).send(cmd);
    }

    /// Hand one received `raft` payload to its group.
    pub(super) fn step(&self, from: &str, msg: Value) {
        self.send(Cmd::Step {
            from: from.to_string(),
            msg,
        });
    }

    /// Replicate a command and wait until it is applied (here, or on the
    /// leader for a forwarded proposal). Errors after 5 s or when the
    /// entry was overwritten by a new leader.
    pub(super) async fn propose(&self, group: &str, kind: &str, data: Value) -> Result<(), String> {
        Self::settled(self.submit(group, kind, data)).await
    }

    /// Hand a proposal to the driver now, so proposals made in turn are
    /// appended in that order; [`RaftNode::settled`] waits for it.
    pub(super) fn submit(
        &self,
        group: &str,
        kind: &str,
        data: Value,
    ) -> oneshot::Receiver<Result<(), String>> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Propose {
            group: group.to_string(),
            kind: kind.to_string(),
            data,
            reply,
        });
        rx
    }

    /// The outcome of a [`RaftNode::submit`]ted proposal.
    pub(super) async fn settled(rx: oneshot::Receiver<Result<(), String>>) -> Result<(), String> {
        match tokio::time::timeout(PROPOSE_TIMEOUT + Duration::from_millis(500), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("raft driver stopped".into()),
            Err(_) => Err("raft proposal timed out".into()),
        }
    }

    /// Replace the applied prefix of `group` with `state` as of `index`.
    pub(super) fn compact(&self, group: &str, index: u64, state: Value) {
        self.send(Cmd::Compact {
            group: group.to_string(),
            index,
            state,
        });
    }

    /// Start a queue group. Starting one that runs is a no-op. `lead` makes
    /// this member campaign at once: the leader locator chose it.
    pub(super) fn add_group(&self, group: &str, lead: bool) {
        self.send(Cmd::AddGroup {
            group: group.to_string(),
            lead,
        });
    }

    /// Stop a queue group and remove its files: its queue was deleted.
    pub(super) fn drop_group(&self, group: &str) {
        if group == META || group == QUORUM {
            return;
        }
        self.view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(group);
        self.send(Cmd::DropGroup {
            group: group.to_string(),
        });
    }

    /// The voters of a group, for the management API's `members`.
    pub(super) fn members(&self, group: &str) -> Vec<String> {
        self.view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(group)
            .map(|v| v.voters.clone())
            .unwrap_or_default()
    }

    /// How many queue groups each member leads, for the `balanced` locator.
    pub(super) fn queue_leaders(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for (name, view) in self.view.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            if name == META || name == QUORUM {
                continue;
            }
            if let Some(leader) = &view.leader {
                *counts.entry(leader.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Move the voter set toward `voters`, one member per config entry.
    pub(super) fn set_voters(&self, voters: Vec<String>) {
        self.send(Cmd::Voters(voters));
    }

    /// Answer a forwarded proposal once the leader has applied it.
    pub(super) fn reply_remote(
        &self,
        to: &str,
        group: &str,
        rid: &str,
        ok: bool,
        error: Option<&str>,
    ) {
        let mut msg = json!({"t": "propose_r", "g": group, "rid": rid, "ok": ok});
        if let Some(e) = error {
            msg["error"] = json!(e);
        }
        let _ = self.out.send((to.to_string(), msg));
    }
}

/// A proposal forwarded, or waiting for a leader to forward to.
struct Pending {
    group: String,
    rid: String,
    kind: String,
    data: Value,
    reply: oneshot::Sender<Result<(), String>>,
    deadline: Instant,
    sent_to: Option<String>,
}

struct Group {
    name: String,
    core: Core,
    /// State of the latest snapshot, sent to a follower that needs it.
    source: Arc<StdMutex<Value>>,
    /// Index → (term, waiter, deadline) for proposals this node appended as leader.
    waiters: HashMap<u64, (u64, Waiter, Instant)>,
    last_leader: Option<String>,
    /// While leader: the index of the term's first entry (its noop). This
    /// node is announced as leader only once that index has committed, so
    /// every earlier entry has gone to the applier first.
    lead_from: Option<u64>,
}

struct Driver {
    node_id: String,
    dir: PathBuf,
    groups: Vec<Group>,
    pending: Vec<Pending>,
    next_rid: u64,
    wanted: Vec<String>,
    out: mpsc::UnboundedSender<(String, Value)>,
    applied: mpsc::UnboundedSender<Applied>,
    view: Arc<StdMutex<HashMap<String, GroupView>>>,
    disk: std_mpsc::Sender<DiskJob>,
}

impl Driver {
    fn run(&mut self, rx: std_mpsc::Receiver<Cmd>) {
        let start = Instant::now();
        loop {
            let first = match rx.recv_timeout(TICK) {
                Ok(cmd) => Some(cmd),
                Err(std_mpsc::RecvTimeoutError::Timeout) => None,
                Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
            };
            let now = start.elapsed().as_millis() as u64;
            if let Some(cmd) = first {
                self.handle(cmd, now);
            }
            // Everything already queued joins this round, so one fsync covers it.
            for _ in 0..4096 {
                match rx.try_recv() {
                    Ok(cmd) => self.handle(cmd, now),
                    Err(_) => break,
                }
            }
            for g in &mut self.groups {
                g.core.tick(now);
                if g.core.role == Role::Leader {
                    g.core.reconfigure(&self.wanted, now);
                }
            }
            self.forward_pending(now);
            self.flush();
        }
    }

    fn group(&mut self, name: &str) -> Option<&mut Group> {
        self.groups.iter_mut().find(|g| g.name == name)
    }

    fn handle(&mut self, cmd: Cmd, now: u64) {
        match cmd {
            Cmd::Step { from, msg } => self.on_step(&from, msg, now),
            Cmd::Propose {
                group,
                kind,
                data,
                reply,
            } => {
                let Some(g) = self.group(&group) else {
                    let _ = reply.send(Err(format!("no raft group {group}")));
                    return;
                };
                if g.core.role == Role::Leader {
                    if let Some(index) = g.core.propose(&kind, data, now) {
                        let term = g.core.term;
                        g.waiters.insert(
                            index,
                            (term, Waiter::Local(reply), Instant::now() + PROPOSE_TIMEOUT),
                        );
                    }
                    return;
                }
                self.next_rid += 1;
                let rid = format!("{}-{}", self.node_id, self.next_rid);
                self.pending.push(Pending {
                    group,
                    rid,
                    kind,
                    data,
                    reply,
                    deadline: Instant::now() + PROPOSE_TIMEOUT,
                    sent_to: None,
                });
            }
            Cmd::Compact {
                group,
                index,
                state,
            } => {
                let Some(g) = self.group(&group) else { return };
                if let Some(snapshot) = g.core.compact_at(index, state) {
                    *g.source.lock().unwrap_or_else(|e| e.into_inner()) = snapshot.state.clone();
                    let job = DiskJob {
                        group,
                        remove: false,
                        hard: None,
                        append: Vec::new(),
                        rewrite: Some(g.core.log().to_vec()),
                        snapshot: Some(snapshot),
                        last_index: g.core.last_index(),
                        then_send: Vec::new(),
                    };
                    let _ = self.disk.send(job);
                }
            }
            Cmd::Durable { group, index } => {
                if let Some(g) = self.group(&group) {
                    g.core.set_durable(index);
                }
            }
            Cmd::AddGroup { group, lead } => {
                if self.group(&group).is_none() {
                    let salt = self.groups.len() as u64;
                    match open_group(
                        &self.node_id,
                        &self.dir,
                        &group,
                        &self.wanted,
                        salt,
                        &self.applied,
                        now,
                    ) {
                        Ok(g) => self.groups.push(g),
                        Err(err) => {
                            warn!(group = %group, error = %err, "raft group did not start");
                            return;
                        }
                    }
                }
                if lead {
                    if let Some(g) = self.group(&group) {
                        g.core.expedite();
                    }
                }
            }
            Cmd::DropGroup { group } => {
                let Some(pos) = self.groups.iter().position(|g| g.name == group) else {
                    return;
                };
                let g = self.groups.swap_remove(pos);
                for (_, (_, w, _)) in g.waiters {
                    fail(&self.out, &g.name, w, "queue deleted");
                }
                let pending = std::mem::take(&mut self.pending);
                for p in pending {
                    if p.group == group {
                        let _ = p.reply.send(Err("queue deleted".into()));
                    } else {
                        self.pending.push(p);
                    }
                }
                let _ = self.disk.send(DiskJob {
                    group,
                    remove: true,
                    hard: None,
                    append: Vec::new(),
                    rewrite: None,
                    snapshot: None,
                    last_index: 0,
                    then_send: Vec::new(),
                });
            }
            Cmd::Voters(voters) => {
                self.wanted = crate_sorted(voters);
                for g in &mut self.groups {
                    if g.core.role == Role::Leader {
                        g.core.reconfigure(&self.wanted, now);
                    }
                }
            }
        }
    }

    fn on_step(&mut self, from: &str, msg: Value, now: u64) {
        let t = msg
            .get("t")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let group_name = msg
            .get("g")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        match t.as_str() {
            "propose" => {
                let rid = msg
                    .get("rid")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let kind = msg
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let data = msg.get("data").cloned().unwrap_or(Value::Null);
                let out = self.out.clone();
                let Some(g) = self.group(&group_name) else {
                    return;
                };
                if g.core.role == Role::Leader {
                    if let Some(index) = g.core.propose(&kind, data, now) {
                        let term = g.core.term;
                        g.waiters.insert(
                            index,
                            (
                                term,
                                Waiter::Remote {
                                    from: from.to_string(),
                                    rid,
                                },
                                Instant::now() + PROPOSE_TIMEOUT,
                            ),
                        );
                    }
                } else {
                    let mut reply = json!({"t": "propose_r", "g": group_name, "rid": rid, "ok": false, "error": "not leader"});
                    if let Some(leader) = &g.core.leader {
                        reply["leader"] = json!(leader);
                    }
                    let _ = out.send((from.to_string(), reply));
                }
            }
            "propose_r" => {
                let rid = msg.get("rid").and_then(|v| v.as_str()).unwrap_or("");
                let Some(pos) = self.pending.iter().position(|p| p.rid == rid) else {
                    return;
                };
                if msg.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                    // `remove`, not `swap_remove`: proposals not yet sent are
                    // forwarded in the order they were made.
                    let p = self.pending.remove(pos);
                    let _ = p.reply.send(Ok(()));
                } else {
                    // Not the leader any more: try again once one is known.
                    self.pending[pos].sent_to = None;
                }
            }
            _ => {
                if let Some(g) = self.group(&group_name) {
                    g.core.step(from, &msg, now);
                }
            }
        }
    }

    /// Send queued proposals to the leader, or append them if this node now leads.
    fn forward_pending(&mut self, now: u64) {
        let pending = std::mem::take(&mut self.pending);
        for mut p in pending {
            if Instant::now() >= p.deadline {
                let _ = p.reply.send(Err("raft proposal timed out".into()));
                continue;
            }
            let Some(g) = self.groups.iter_mut().find(|g| g.name == p.group) else {
                let _ = p.reply.send(Err("no raft group".into()));
                continue;
            };
            if g.core.role == Role::Leader {
                if let Some(index) = g.core.propose(&p.kind, p.data, now) {
                    let term = g.core.term;
                    g.waiters
                        .insert(index, (term, Waiter::Local(p.reply), p.deadline));
                }
                continue;
            }
            let leader = g.core.leader.clone();
            match leader {
                Some(leader)
                    if leader != self.node_id && p.sent_to.as_deref() != Some(leader.as_str()) =>
                {
                    let msg = json!({"t": "propose", "g": p.group, "rid": p.rid, "kind": p.kind, "data": p.data});
                    let _ = self.out.send((leader.clone(), msg));
                    p.sent_to = Some(leader);
                    self.pending.push(p);
                }
                _ => self.pending.push(p),
            }
        }
    }

    /// Persist, then hand off commits, then send. Raft requires that order
    /// for votes and append replies. A leader's own `append` and `snap` go
    /// out before its fsync (the Raft thesis, section 10.2.1): the leader
    /// counts itself only toward a commit decided after this flush, because
    /// a reply is read in a later round.
    fn flush(&mut self) {
        for g in &mut self.groups {
            let outbox = g.core.take_outbox();
            let (early, late): (Vec<_>, Vec<_>) = outbox.into_iter().partition(|(_, msg)| {
                matches!(
                    msg.get("t").and_then(|t| t.as_str()),
                    Some("append") | Some("snap")
                )
            });
            for (to, msg) in early {
                let _ = self.out.send((to, msg));
            }
            let dirty = g.core.take_dirty();
            let mut job = DiskJob {
                group: g.name.clone(),
                remove: false,
                hard: dirty
                    .hard_state
                    .then(|| (g.core.term, g.core.voted_for.clone())),
                append: Vec::new(),
                rewrite: None,
                snapshot: None,
                last_index: g.core.last_index(),
                then_send: late,
            };
            if let Some(snapshot) = dirty.snapshot {
                *g.source.lock().unwrap_or_else(|e| e.into_inner()) = snapshot.state.clone();
                job.rewrite = Some(g.core.log().to_vec());
                job.snapshot = Some(snapshot);
            } else if let Some(from) = dirty.truncated_from {
                g.core.cap_durable(from.saturating_sub(1));
                job.rewrite = Some(g.core.log().to_vec());
            } else {
                job.append = dirty.appended;
            }
            let writes = job.hard.is_some() || job.rewrite.is_some() || !job.append.is_empty();
            if writes || !job.then_send.is_empty() {
                let _ = self.disk.send(job);
            }
            if let Some(snapshot) = g.core.take_installed() {
                let _ = self.applied.send(Applied::Install {
                    group: g.name.clone(),
                    snapshot,
                });
            }
            let committed = g.core.take_committed();
            if !committed.is_empty() {
                let mut items = Vec::with_capacity(committed.len());
                for e in committed {
                    let waiter = match g.waiters.remove(&e.index) {
                        Some((term, w, _)) if term == e.term => Some(w),
                        Some((_, w, _)) => {
                            fail(&self.out, &g.name, w, "entry replaced by a new leader");
                            None
                        }
                        None => None,
                    };
                    items.push((e, waiter));
                }
                let _ = self.applied.send(Applied::Entries {
                    group: g.name.clone(),
                    items,
                });
            }
            // A proposal whose index was cut off, or that never committed.
            let now = Instant::now();
            let expired: Vec<u64> = g
                .waiters
                .iter()
                .filter(|(_, (_, _, d))| now >= *d)
                .map(|(i, _)| *i)
                .collect();
            for index in expired {
                if let Some((_, w, _)) = g.waiters.remove(&index) {
                    fail(&self.out, &g.name, w, "raft proposal timed out");
                }
            }
            // Anything a commit or apply step produced is sent with the next round.
            // A new leader serves only after it has applied everything committed
            // before its term (Raft §8): until its noop commits, it is announced
            // as no leader at all.
            if g.core.role == Role::Leader {
                if g.lead_from.is_none() {
                    g.lead_from = Some(g.core.last_index());
                }
            } else {
                g.lead_from = None;
            }
            let usable = match g.lead_from {
                Some(from) if g.core.commit < from => None,
                _ => g.core.leader.clone(),
            };
            if usable != g.last_leader {
                g.last_leader = usable;
                let _ = self.applied.send(Applied::Leader {
                    group: g.name.clone(),
                    leader: g.last_leader.clone(),
                });
            }
            self.view.lock().unwrap_or_else(|e| e.into_inner()).insert(
                g.name.clone(),
                GroupView {
                    leader: g.last_leader.clone(),
                    voters: g.core.voters(),
                },
            );
        }
    }
}

/// Load one group from `dir/<group_dir>` and make its core. A queue group
/// opened for the first time writes `group.json` naming it.
fn open_group(
    node_id: &str,
    dir: &Path,
    name: &str,
    voters: &[String],
    salt: u64,
    applied: &mpsc::UnboundedSender<Applied>,
    now: u64,
) -> std::io::Result<Group> {
    let path = dir.join(group_dir(name));
    let fresh = !path.exists();
    let disk = Disk::open(&path)?;
    if fresh && name != META && name != QUORUM {
        write_atomic(
            &path,
            "group.json",
            &serde_json::to_vec(&json!({"group": name}))?,
        )?;
    }
    let mut core = Core::new(
        node_id,
        name,
        voters.to_vec(),
        seed_for(node_id, salt ^ seed_for(name, 0)),
    );
    let (term, vote, snapshot, log) = disk.load();
    let source = Arc::new(StdMutex::new(Value::Null));
    if let Some(s) = &snapshot {
        *source.lock().unwrap_or_else(|e| e.into_inner()) = s.state.clone();
        let _ = applied.send(Applied::Install {
            group: name.to_string(),
            snapshot: s.clone(),
        });
    }
    let applied_index = snapshot.as_ref().map(|s| s.index).unwrap_or(0);
    core.restore(term, vote, snapshot, log, applied_index);
    let src = Arc::clone(&source);
    core.set_snapshot_source(Box::new(move || {
        src.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }));
    core.set_durable(core.last_index());
    // A group opened after the node started times out from now, not from 0.
    core.start_timer(now);
    Ok(Group {
        name: name.to_string(),
        core,
        source,
        waiters: HashMap::new(),
        last_leader: None,
        lead_from: None,
    })
}

fn fail(out: &mpsc::UnboundedSender<(String, Value)>, group: &str, w: Waiter, why: &str) {
    match w {
        Waiter::Local(reply) => {
            let _ = reply.send(Err(why.to_string()));
        }
        Waiter::Remote { from, rid } => {
            let _ = out.send((
                from,
                json!({"t": "propose_r", "g": group, "rid": rid, "ok": false, "error": why}),
            ));
        }
    }
}

fn crate_sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v.dedup();
    v
}

/// A per-node seed so members do not time out together.
fn seed_for(node_id: &str, salt: u64) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325 ^ salt;
    for byte in node_id.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    hash ^ nanos
}

/// Writes for one group, and the replies that must follow them.
struct DiskJob {
    group: String,
    /// Close the group's files and remove its directory.
    remove: bool,
    hard: Option<(u64, Option<String>)>,
    append: Vec<Entry>,
    rewrite: Option<Vec<Entry>>,
    snapshot: Option<Snapshot>,
    last_index: u64,
    then_send: Vec<(String, Value)>,
}

/// Write jobs in order. Every job queued at once shares one fsync per log;
/// then their replies go out and the driver learns what is durable.
fn run_disk(
    dir: PathBuf,
    rx: std_mpsc::Receiver<DiskJob>,
    out: mpsc::UnboundedSender<(String, Value)>,
    back: std_mpsc::Sender<Cmd>,
) {
    let mut disks: HashMap<String, Disk> = HashMap::new();
    while let Ok(first) = rx.recv() {
        let mut jobs = vec![first];
        while let Ok(job) = rx.try_recv() {
            jobs.push(job);
        }
        let mut durable: HashMap<String, u64> = HashMap::new();
        let mut written = 0;
        loop {
            let result = (|| -> std::io::Result<()> {
                while written < jobs.len() {
                    let job = &mut jobs[written];
                    write_disk_job(&dir, &mut disks, job)?;
                    if job.remove {
                        durable.remove(&job.group);
                    } else {
                        durable.insert(job.group.clone(), job.last_index);
                    }
                    written += 1;
                }
                for disk in disks.values_mut() {
                    disk.sync()?;
                }
                Ok(())
            })();
            match result {
                Ok(()) => break,
                Err(err) => {
                    warn!(error = %err, "raft disk batch failed; acknowledgments held until retry");
                    // Keep the batch and its write cursor: never skip a failed
                    // vote/log write or let a later rewrite compact past it.
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
        for job in jobs {
            for (to, msg) in job.then_send {
                let _ = out.send((to, msg));
            }
        }
        for (group, index) in durable {
            let _ = back.send(Cmd::Durable { group, index });
        }
    }
}

/// Complete each operation once. A retry keeps failed operations and never
/// replays an already-successful append while retrying hard state or fsync.
fn write_disk_job(
    dir: &Path,
    disks: &mut HashMap<String, Disk>,
    job: &mut DiskJob,
) -> std::io::Result<()> {
    if job.remove {
        if let Some(disk) = disks.get_mut(&job.group) {
            disk.sync()?;
        }
        match fs::remove_dir_all(dir.join(group_dir(&job.group))) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        disks.remove(&job.group);
        return Ok(());
    }
    if !disks.contains_key(&job.group) {
        disks.insert(
            job.group.clone(),
            Disk::open(&dir.join(group_dir(&job.group)))?,
        );
    }
    let disk = disks.get_mut(&job.group).expect("opened disk");
    if let Some(snapshot) = &job.snapshot {
        disk.save_snapshot(snapshot, job.rewrite.as_deref().unwrap_or(&[]))?;
        job.snapshot = None;
        job.rewrite = None;
    } else if let Some(log) = &job.rewrite {
        disk.rewrite_log(log)?;
        job.rewrite = None;
    }
    if !job.append.is_empty() {
        disk.write_entries(&job.append)?;
        job.append.clear();
    }
    if let Some((term, vote)) = &job.hard {
        disk.save_hard(*term, vote.as_deref())?;
        job.hard = None;
    }
    Ok(())
}

/// One group's files: `state.json` (term and vote), `log.jsonl` (entries
/// after the snapshot) and `snapshot.json`. Every write is fsynced.
struct Disk {
    dir: PathBuf,
    log: Option<File>,
    unsynced: bool,
    /// Roll back a partial failed append before retrying its complete batch.
    append_start: Option<u64>,
}

impl Disk {
    fn open(dir: &Path) -> std::io::Result<Disk> {
        fs::create_dir_all(dir)?;
        Ok(Disk {
            dir: dir.to_path_buf(),
            log: None,
            unsynced: false,
            append_start: None,
        })
    }

    fn load(&self) -> (u64, Option<String>, Option<Snapshot>, Vec<Entry>) {
        let hard: Value = fs::read(self.dir.join("state.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(Value::Null);
        let term = hard.get("term").and_then(|v| v.as_u64()).unwrap_or(0);
        let vote = hard
            .get("vote")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let snapshot = fs::read(self.dir.join("snapshot.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .and_then(|v| {
                Some(Snapshot {
                    index: v.get("index")?.as_u64()?,
                    term: v.get("term")?.as_u64()?,
                    voters: v
                        .get("voters")?
                        .as_array()?
                        .iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect(),
                    state: v.get("state").cloned().unwrap_or(Value::Null),
                })
            });
        let mut log = Vec::new();
        if let Ok(file) = File::open(self.dir.join("log.jsonl")) {
            for line in BufReader::new(file).lines() {
                let Ok(line) = line else { break };
                // A torn last line from a crash mid-write is dropped.
                let Some(e) = serde_json::from_str::<Value>(&line)
                    .ok()
                    .and_then(|v| Entry::from_json(&v))
                else {
                    break;
                };
                // A later line for the same index replaces the earlier ones.
                while log.last().is_some_and(|last: &Entry| last.index >= e.index) {
                    log.pop();
                }
                log.push(e);
            }
        }
        (term, vote, snapshot, log)
    }

    fn save_hard(&self, term: u64, vote: Option<&str>) -> std::io::Result<()> {
        write_atomic(
            &self.dir,
            "state.json",
            &serde_json::to_vec(&json!({"term": term, "vote": vote}))?,
        )
    }

    #[cfg(test)]
    fn append(&mut self, entries: &[Entry]) -> std::io::Result<()> {
        self.write_entries(entries)?;
        self.sync()
    }

    /// Append entries without an fsync; [`Disk::sync`] makes them durable.
    fn write_entries(&mut self, entries: &[Entry]) -> std::io::Result<()> {
        if self.log.is_none() {
            self.log = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.dir.join("log.jsonl"))?,
            );
        }
        let file = self.log.as_mut().expect("log file");
        let mut buf = Vec::new();
        for e in entries {
            serde_json::to_writer(&mut buf, &e.to_json())?;
            buf.push(b'\n');
        }
        if let Some(start) = self.append_start {
            file.set_len(start)?;
        } else {
            self.append_start = Some(file.metadata()?.len());
        }
        self.unsynced = true;
        file.write_all(&buf)?;
        self.append_start = None;
        Ok(())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        if !self.unsynced {
            return Ok(());
        }
        match self.log.as_mut() {
            Some(file) => file.sync_data()?,
            None => return Err(std::io::Error::other("unsynced Raft log has no file")),
        }
        self.unsynced = false;
        Ok(())
    }

    fn rewrite_log(&mut self, entries: &[Entry]) -> std::io::Result<()> {
        // Appends not yet synced are part of `entries` or were cut off.
        let mut buf = Vec::new();
        for e in entries {
            serde_json::to_writer(&mut buf, &e.to_json())?;
            buf.push(b'\n');
        }
        write_atomic(&self.dir, "log.jsonl", &buf)?;
        self.log = None;
        self.unsynced = false;
        self.append_start = None;
        Ok(())
    }

    fn save_snapshot(&mut self, s: &Snapshot, log: &[Entry]) -> std::io::Result<()> {
        let body = json!({"index": s.index, "term": s.term, "voters": s.voters, "state": s.state});
        write_atomic(&self.dir, "snapshot.json", &serde_json::to_vec(&body)?)?;
        self.rewrite_log(log)
    }
}

/// Write `name` in `dir` through a temporary file, fsync it, rename it over
/// the old one and fsync the directory.
fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dir.join(name))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_groups_have_unambiguous_components() {
        assert_ne!(queue_group("/a", "b/c"), queue_group("/a/b", "c"));
        assert_ne!(queue_group("", "/"), queue_group("/", ""));
        assert_eq!(queue_group("/", "orders"), "q:v2:2f:6f7264657273");
        assert_eq!(queue_group("", ""), "q:v2::");
        assert!(!queue_group("/a", "b/c").contains('/'));
    }

    #[test]
    fn queue_groups_encode_utf8_identically_to_bun() {
        assert_eq!(
            queue_group("/á", "队列/📦"),
            "q:v2:2fc3a1:e9989fe588972ff09f93a6"
        );
        assert_ne!(queue_group("/é", "q"), queue_group("/e\u{301}", "q"));
    }

    #[test]
    fn failed_hard_state_holds_replies_and_later_jobs_until_retry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(META);
        fs::create_dir_all(path.join("state.json.tmp")).expect("block hard-state write");
        let (tx, rx) = std_mpsc::channel();
        let (out, mut replies) = mpsc::unbounded_channel();
        let (back, durable) = std_mpsc::channel();
        let worker_dir = dir.path().to_path_buf();
        let worker = std::thread::spawn(move || run_disk(worker_dir, rx, out, back));
        for i in 1..=2 {
            tx.send(DiskJob {
                group: META.into(),
                remove: false,
                hard: (i == 1).then(|| (3, Some("b".into()))),
                append: vec![Entry {
                    index: i,
                    term: 3,
                    kind: "x".into(),
                    data: json!(i),
                }],
                rewrite: None,
                snapshot: None,
                last_index: i,
                then_send: vec![("b".into(), json!({"ack": i}))],
            })
            .expect("job");
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while fs::read_to_string(path.join("log.jsonl"))
            .map(|s| s.lines().count())
            .unwrap_or(0)
            != 1
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(replies.try_recv().is_err(), "failed write emitted reply");
        assert!(durable.try_recv().is_err(), "failed write counted durable");
        let log = fs::read_to_string(path.join("log.jsonl")).expect("first append");
        assert_eq!(
            log.lines().count(),
            1,
            "later job bypassed failed hard state"
        );
        fs::remove_dir(path.join("state.json.tmp")).expect("repair hard-state path");
        drop(tx);
        worker.join().expect("disk worker");
        assert_eq!(replies.try_recv().expect("first reply").1["ack"], 1);
        assert_eq!(replies.try_recv().expect("second reply").1["ack"], 2);
        assert!(matches!(durable.try_recv(), Ok(Cmd::Durable { .. })));
        let disk = Disk::open(&path).expect("reopen");
        let (term, vote, _, entries) = disk.load();
        assert_eq!((term, vote.as_deref()), (3, Some("b")));
        assert_eq!(
            entries.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn failed_sync_remains_unsynced_until_successful_retry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut disk = Disk::open(dir.path()).expect("open");
        disk.log = Some(
            OpenOptions::new()
                .write(true)
                .open("/dev/null")
                .expect("non-syncable file"),
        );
        disk.unsynced = true;
        assert!(disk.sync().is_err());
        assert!(disk.unsynced);
        assert!(disk.sync().is_err(), "failed fsync must be retried");
        disk.log = Some(File::create(dir.path().join("log.jsonl")).expect("repair"));
        disk.sync().expect("successful retry");
        assert!(!disk.unsynced);
    }

    #[test]
    fn retry_rolls_back_partial_append_before_rewriting_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut disk = Disk::open(dir.path()).expect("open");
        let entry = |index| Entry {
            index,
            term: 1,
            kind: "x".into(),
            data: json!(index),
        };
        disk.append(&[entry(1)]).expect("first entry");
        let original_len = disk
            .log
            .as_ref()
            .expect("log")
            .metadata()
            .expect("metadata")
            .len();
        disk.log
            .as_mut()
            .expect("log")
            .write_all(b"{torn")
            .expect("partial append");
        disk.append_start = Some(original_len);
        disk.append(&[entry(2)]).expect("retry");
        let (_, _, _, entries) = disk.load();
        assert_eq!(
            entries.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn disk_round_trips_state_log_and_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut disk = Disk::open(dir.path()).expect("open");
        disk.save_hard(3, Some("b")).expect("hard");
        let e = |i, t| Entry {
            index: i,
            term: t,
            kind: "x".into(),
            data: json!(i),
        };
        disk.append(&[e(1, 1), e(2, 1)]).expect("append");
        // A rewrite after a conflict keeps index 1 and replaces index 2.
        disk.rewrite_log(&[e(1, 1), e(2, 3)]).expect("rewrite");
        disk.append(&[e(3, 3)]).expect("append");
        let (term, vote, snap, log) = disk.load();
        assert_eq!((term, vote.as_deref()), (3, Some("b")));
        assert!(snap.is_none());
        assert_eq!(
            log.iter().map(|e| (e.index, e.term)).collect::<Vec<_>>(),
            vec![(1, 1), (2, 3), (3, 3)]
        );
        let s = Snapshot {
            index: 2,
            term: 3,
            voters: vec!["a".into()],
            state: json!({"k": 1}),
        };
        disk.save_snapshot(&s, &[e(3, 3)]).expect("snapshot");
        let (_, _, snap, log) = disk.load();
        assert_eq!(snap, Some(s));
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn a_torn_last_line_is_dropped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut disk = Disk::open(dir.path()).expect("open");
        disk.append(&[Entry {
            index: 1,
            term: 1,
            kind: "x".into(),
            data: Value::Null,
        }])
        .expect("append");
        let mut f = OpenOptions::new()
            .append(true)
            .open(dir.path().join("log.jsonl"))
            .expect("open");
        f.write_all(b"{\"i\":2,\"te").expect("torn");
        let (_, _, _, log) = disk.load();
        assert_eq!(log.len(), 1);
    }
}
