//! The Raft driver: runs the `meta` and `quorum` groups of docs/raft.md on
//! [`super::raft::Core`].
//!
//! One OS thread owns both cores. It writes what a core reports dirty, with
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
const TICK: Duration = Duration::from_millis(20);
/// A proposer gives up after 5 s (section 4, propose_r).
const PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Applied entries kept in the log before a snapshot replaces them (section 7).
pub(super) fn compact_after(group: &str) -> u64 {
    if group == QUORUM {
        10_000
    } else {
        50_000
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
        group: &'static str,
        items: Vec<(Entry, Option<Waiter>)>,
    },
    /// A snapshot that replaces the group's state, from disk or the leader.
    Install {
        group: &'static str,
        snapshot: Snapshot,
    },
    /// The group's leader changed. `None` while an election runs.
    Leader {
        group: &'static str,
        leader: Option<String>,
    },
}

enum Cmd {
    Step { from: String, msg: Value },
    Propose {
        group: &'static str,
        kind: String,
        data: Value,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Compact { group: &'static str, index: u64, state: Value },
    Voters(Vec<String>),
    /// The disk thread wrote `group` through `index`.
    Durable { group: &'static str, index: u64 },
}

/// Leader of one group, as last seen by the driver thread.
#[derive(Clone, Debug, Default)]
pub(super) struct GroupView {
    pub leader: Option<String>,
}

/// Handle to the driver thread.
pub(super) struct RaftNode {
    tx: StdMutex<std_mpsc::Sender<Cmd>>,
    view: Arc<StdMutex<HashMap<&'static str, GroupView>>>,
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
        let mut groups = Vec::new();
        let mut disks = Vec::new();
        for (i, group) in GROUPS.iter().enumerate() {
            let disk = Disk::open(&dir.join(group))?;
            let seed = seed_for(node_id, i as u64);
            let mut core = Core::new(node_id, group, voters.clone(), seed);
            let (term, vote, snapshot, log) = disk.load();
            let source = Arc::new(StdMutex::new(Value::Null));
            if let Some(s) = &snapshot {
                *source.lock().unwrap_or_else(|e| e.into_inner()) = s.state.clone();
                let _ = applied.send(Applied::Install { group, snapshot: s.clone() });
            }
            let applied_index = snapshot.as_ref().map(|s| s.index).unwrap_or(0);
            core.restore(term, vote, snapshot, log, applied_index);
            let src = Arc::clone(&source);
            core.set_snapshot_source(Box::new(move || src.lock().unwrap_or_else(|e| e.into_inner()).clone()));
            core.set_durable(core.last_index());
            disks.push((*group, disk));
            groups.push(Group {
                name: group,
                core,
                source,
                waiters: HashMap::new(),
                last_leader: None,
                lead_from: None,
            });
        }
        let node = Arc::new(RaftNode {
            tx: StdMutex::new(tx.clone()),
            view: Arc::clone(&view),
            out: out.clone(),
        });
        let (disk_tx, disk_rx) = std_mpsc::channel();
        let disk_out = out.clone();
        let disk_back = tx.clone();
        std::thread::Builder::new()
            .name("queueforge-raft-disk".into())
            .spawn(move || run_disk(disks, disk_rx, disk_out, disk_back))?;
        let mut driver = Driver {
            node_id: node_id.to_string(),
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
        self.send(Cmd::Step { from: from.to_string(), msg });
    }

    /// Replicate a command and wait until it is applied (here, or on the
    /// leader for a forwarded proposal). Errors after 5 s or when the
    /// entry was overwritten by a new leader.
    pub(super) async fn propose(&self, group: &'static str, kind: &str, data: Value) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        self.send(Cmd::Propose { group, kind: kind.to_string(), data, reply });
        match tokio::time::timeout(PROPOSE_TIMEOUT + Duration::from_millis(500), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("raft driver stopped".into()),
            Err(_) => Err("raft proposal timed out".into()),
        }
    }

    /// Replace the applied prefix of `group` with `state` as of `index`.
    pub(super) fn compact(&self, group: &'static str, index: u64, state: Value) {
        self.send(Cmd::Compact { group, index, state });
    }

    /// Move the voter set toward `voters`, one member per config entry.
    pub(super) fn set_voters(&self, voters: Vec<String>) {
        self.send(Cmd::Voters(voters));
    }

    /// The group's current leader, if one is known.
    pub(super) fn leader(&self, group: &str) -> Option<String> {
        self.view
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(group)
            .and_then(|v| v.leader.clone())
    }

    /// Answer a forwarded proposal once the leader has applied it.
    pub(super) fn reply_remote(&self, to: &str, group: &str, rid: &str, ok: bool, error: Option<&str>) {
        let mut msg = json!({"t": "propose_r", "g": group, "rid": rid, "ok": ok});
        if let Some(e) = error {
            msg["error"] = json!(e);
        }
        let _ = self.out.send((to.to_string(), msg));
    }
}

/// A proposal forwarded, or waiting for a leader to forward to.
struct Pending {
    group: &'static str,
    rid: String,
    kind: String,
    data: Value,
    reply: oneshot::Sender<Result<(), String>>,
    deadline: Instant,
    sent_to: Option<String>,
}

struct Group {
    name: &'static str,
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
    groups: Vec<Group>,
    pending: Vec<Pending>,
    next_rid: u64,
    wanted: Vec<String>,
    out: mpsc::UnboundedSender<(String, Value)>,
    applied: mpsc::UnboundedSender<Applied>,
    view: Arc<StdMutex<HashMap<&'static str, GroupView>>>,
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
            Cmd::Propose { group, kind, data, reply } => {
                let Some(g) = self.group(group) else {
                    let _ = reply.send(Err(format!("no raft group {group}")));
                    return;
                };
                if g.core.role == Role::Leader {
                    if let Some(index) = g.core.propose(&kind, data, now) {
                        let term = g.core.term;
                        g.waiters.insert(index, (term, Waiter::Local(reply), Instant::now() + PROPOSE_TIMEOUT));
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
            Cmd::Compact { group, index, state } => {
                let Some(g) = self.group(group) else { return };
                if let Some(snapshot) = g.core.compact_at(index, state) {
                    *g.source.lock().unwrap_or_else(|e| e.into_inner()) = snapshot.state.clone();
                    let job = DiskJob {
                        group,
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
                if let Some(g) = self.group(group) {
                    g.core.set_durable(index);
                }
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
        let t = msg.get("t").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let group_name = msg.get("g").and_then(|v| v.as_str()).unwrap_or("").to_string();
        match t.as_str() {
            "propose" => {
                let rid = msg.get("rid").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let kind = msg.get("kind").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let data = msg.get("data").cloned().unwrap_or(Value::Null);
                let out = self.out.clone();
                let Some(g) = self.group(&group_name) else { return };
                if g.core.role == Role::Leader {
                    if let Some(index) = g.core.propose(&kind, data, now) {
                        let term = g.core.term;
                        g.waiters.insert(
                            index,
                            (term, Waiter::Remote { from: from.to_string(), rid }, Instant::now() + PROPOSE_TIMEOUT),
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
                let Some(pos) = self.pending.iter().position(|p| p.rid == rid) else { return };
                if msg.get("ok").and_then(|v| v.as_bool()).unwrap_or(false) {
                    let p = self.pending.swap_remove(pos);
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
                    g.waiters.insert(index, (term, Waiter::Local(p.reply), p.deadline));
                }
                continue;
            }
            let leader = g.core.leader.clone();
            match leader {
                Some(leader) if leader != self.node_id && p.sent_to.as_deref() != Some(leader.as_str()) => {
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
                matches!(msg.get("t").and_then(|t| t.as_str()), Some("append") | Some("snap"))
            });
            for (to, msg) in early {
                let _ = self.out.send((to, msg));
            }
            let dirty = g.core.take_dirty();
            let mut job = DiskJob {
                group: g.name,
                hard: dirty.hard_state.then(|| (g.core.term, g.core.voted_for.clone())),
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
                let _ = self.applied.send(Applied::Install { group: g.name, snapshot });
            }
            let committed = g.core.take_committed();
            if !committed.is_empty() {
                let mut items = Vec::with_capacity(committed.len());
                for e in committed {
                    let waiter = match g.waiters.remove(&e.index) {
                        Some((term, w, _)) if term == e.term => Some(w),
                        Some((_, w, _)) => {
                            fail(&self.out, g.name, w, "entry replaced by a new leader");
                            None
                        }
                        None => None,
                    };
                    items.push((e, waiter));
                }
                let _ = self.applied.send(Applied::Entries { group: g.name, items });
            }
            // A proposal whose index was cut off, or that never committed.
            let now = Instant::now();
            let expired: Vec<u64> = g.waiters.iter().filter(|(_, (_, _, d))| now >= *d).map(|(i, _)| *i).collect();
            for index in expired {
                if let Some((_, w, _)) = g.waiters.remove(&index) {
                    fail(&self.out, g.name, w, "raft proposal timed out");
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
                let _ = self.applied.send(Applied::Leader { group: g.name, leader: g.last_leader.clone() });
            }
            self.view.lock().unwrap_or_else(|e| e.into_inner()).insert(
                g.name,
                GroupView { leader: g.last_leader.clone() },
            );
        }
    }
}

fn fail(out: &mpsc::UnboundedSender<(String, Value)>, group: &str, w: Waiter, why: &str) {
    match w {
        Waiter::Local(reply) => {
            let _ = reply.send(Err(why.to_string()));
        }
        Waiter::Remote { from, rid } => {
            let _ = out.send((from, json!({"t": "propose_r", "g": group, "rid": rid, "ok": false, "error": why})));
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
    group: &'static str,
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
    mut disks: Vec<(&'static str, Disk)>,
    rx: std_mpsc::Receiver<DiskJob>,
    out: mpsc::UnboundedSender<(String, Value)>,
    back: std_mpsc::Sender<Cmd>,
) {
    while let Ok(first) = rx.recv() {
        let mut jobs = vec![first];
        while let Ok(job) = rx.try_recv() {
            jobs.push(job);
        }
        let mut durable: HashMap<&'static str, u64> = HashMap::new();
        for job in &jobs {
            let Some((_, disk)) = disks.iter_mut().find(|(g, _)| *g == job.group) else { continue };
            let result = (|| -> std::io::Result<()> {
                if let Some(snapshot) = &job.snapshot {
                    disk.save_snapshot(snapshot, job.rewrite.as_deref().unwrap_or(&[]))?;
                } else if let Some(log) = &job.rewrite {
                    disk.rewrite_log(log)?;
                }
                if !job.append.is_empty() {
                    disk.write_entries(&job.append)?;
                }
                if let Some((term, vote)) = &job.hard {
                    disk.save_hard(*term, vote.as_deref())?;
                }
                Ok(())
            })();
            if let Err(err) = result {
                warn!(group = job.group, error = %err, "raft write failed");
            }
            durable.insert(job.group, job.last_index);
        }
        for (_, disk) in &mut disks {
            if let Err(err) = disk.sync() {
                warn!(error = %err, "raft log fsync failed");
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

/// One group's files: `state.json` (term and vote), `log.jsonl` (entries
/// after the snapshot) and `snapshot.json`. Every write is fsynced.
struct Disk {
    dir: PathBuf,
    log: Option<File>,
    unsynced: bool,
}

impl Disk {
    fn open(dir: &Path) -> std::io::Result<Disk> {
        fs::create_dir_all(dir)?;
        Ok(Disk { dir: dir.to_path_buf(), log: None, unsynced: false })
    }

    fn load(&self) -> (u64, Option<String>, Option<Snapshot>, Vec<Entry>) {
        let hard: Value = fs::read(self.dir.join("state.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or(Value::Null);
        let term = hard.get("term").and_then(|v| v.as_u64()).unwrap_or(0);
        let vote = hard.get("vote").and_then(|v| v.as_str()).map(str::to_string);
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
                let Some(e) = serde_json::from_str::<Value>(&line).ok().and_then(|v| Entry::from_json(&v)) else {
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
        write_atomic(&self.dir, "state.json", &serde_json::to_vec(&json!({"term": term, "vote": vote}))?)
    }

    #[cfg(test)]
    fn append(&mut self, entries: &[Entry]) -> std::io::Result<()> {
        self.write_entries(entries)?;
        self.sync()
    }

    /// Append entries without an fsync; [`Disk::sync`] makes them durable.
    fn write_entries(&mut self, entries: &[Entry]) -> std::io::Result<()> {
        if self.log.is_none() {
            self.log = Some(OpenOptions::new().create(true).append(true).open(self.dir.join("log.jsonl"))?);
        }
        let file = self.log.as_mut().expect("log file");
        let mut buf = Vec::new();
        for e in entries {
            serde_json::to_writer(&mut buf, &e.to_json())?;
            buf.push(b'\n');
        }
        file.write_all(&buf)?;
        self.unsynced = true;
        Ok(())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        if !self.unsynced {
            return Ok(());
        }
        self.unsynced = false;
        match self.log.as_mut() {
            Some(file) => file.sync_data(),
            None => Ok(()),
        }
    }

    fn rewrite_log(&mut self, entries: &[Entry]) -> std::io::Result<()> {
        // Appends not yet synced are part of `entries` or were cut off.
        self.log = None;
        self.unsynced = false;
        let mut buf = Vec::new();
        for e in entries {
            serde_json::to_writer(&mut buf, &e.to_json())?;
            buf.push(b'\n');
        }
        write_atomic(&self.dir, "log.jsonl", &buf)
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
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_round_trips_state_log_and_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut disk = Disk::open(dir.path()).expect("open");
        disk.save_hard(3, Some("b")).expect("hard");
        let e = |i, t| Entry { index: i, term: t, kind: "x".into(), data: json!(i) };
        disk.append(&[e(1, 1), e(2, 1)]).expect("append");
        // A rewrite after a conflict keeps index 1 and replaces index 2.
        disk.rewrite_log(&[e(1, 1), e(2, 3)]).expect("rewrite");
        disk.append(&[e(3, 3)]).expect("append");
        let (term, vote, snap, log) = disk.load();
        assert_eq!((term, vote.as_deref()), (3, Some("b")));
        assert!(snap.is_none());
        assert_eq!(log.iter().map(|e| (e.index, e.term)).collect::<Vec<_>>(), vec![(1, 1), (2, 3), (3, 3)]);
        let s = Snapshot { index: 2, term: 3, voters: vec!["a".into()], state: json!({"k": 1}) };
        disk.save_snapshot(&s, &[e(3, 3)]).expect("snapshot");
        let (_, _, snap, log) = disk.load();
        assert_eq!(snap, Some(s));
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn a_torn_last_line_is_dropped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut disk = Disk::open(dir.path()).expect("open");
        disk.append(&[Entry { index: 1, term: 1, kind: "x".into(), data: Value::Null }]).expect("append");
        let mut f = OpenOptions::new().append(true).open(dir.path().join("log.jsonl")).expect("open");
        f.write_all(b"{\"i\":2,\"te").expect("torn");
        let (_, _, _, log) = disk.load();
        assert_eq!(log.len(), 1);
    }
}
