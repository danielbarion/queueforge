//! A Raft group as a deterministic state machine, as docs/raft.md specifies.
//!
//! [`Core`] does no I/O. The driver feeds it [`Core::tick`], [`Core::step`]
//! and [`Core::propose`], persists what [`Core::take_dirty`] reports before
//! sending [`Core::take_outbox`], and applies [`Core::take_committed`] in
//! order. Messages are the JSON payloads of section 4, so the Bun and PHP
//! brokers can be members of the same group.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};

/// Leader heartbeat interval.
pub const HEARTBEAT_MS: u64 = 150;
/// Election timeout bounds; each member picks a random value in between.
pub const ELECTION_MIN_MS: u64 = 1000;
pub const ELECTION_MAX_MS: u64 = 2000;
/// Most entries in one `append`.
const MAX_BATCH: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub kind: String,
    pub data: Value,
}

impl Entry {
    pub fn to_json(&self) -> Value {
        json!({"i": self.index, "term": self.term, "kind": self.kind, "data": self.data})
    }

    pub fn from_json(v: &Value) -> Option<Entry> {
        Some(Entry {
            index: v.get("i")?.as_u64()?,
            term: v.get("term")?.as_u64()?,
            kind: v.get("kind")?.as_str()?.to_string(),
            data: v.get("data").cloned().unwrap_or(Value::Null),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

/// What must reach disk before the outbox may be sent.
#[derive(Debug, Default, PartialEq)]
pub struct Dirty {
    /// `currentTerm` or `votedFor` changed.
    pub hard_state: bool,
    /// The log was truncated from this index (inclusive) before new entries.
    pub truncated_from: Option<u64>,
    /// Entries appended since the last [`Core::take_dirty`].
    pub appended: Vec<Entry>,
    /// A snapshot was installed; the driver stores it and drops the log.
    pub snapshot: Option<Snapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub voters: Vec<String>,
    pub state: Value,
}

/// A Raft member of one group.
pub struct Core {
    pub id: String,
    pub group: String,
    pub term: u64,
    pub voted_for: Option<String>,
    pub role: Role,
    pub leader: Option<String>,
    pub commit: u64,
    pub applied: u64,
    /// Entries after the snapshot, in index order.
    log: Vec<Entry>,
    snap_index: u64,
    snap_term: u64,
    snap_voters: Vec<String>,
    initial_voters: Vec<String>,
    votes: BTreeSet<String>,
    next_index: BTreeMap<String, u64>,
    match_index: BTreeMap<String, u64>,
    /// Last time each peer answered this leader, for pre-vote checks.
    heard_leader_at: u64,
    election_deadline: u64,
    heartbeat_due: u64,
    rng: u64,
    outbox: Vec<(String, Value)>,
    dirty: Dirty,
    /// The snapshot state a lagging follower needs; set by the driver.
    snapshot_source: Option<Box<dyn Fn() -> Value + Send>>,
    /// A snapshot received but not yet applied by the driver.
    installed: Option<Snapshot>,
    /// Highest index known to be on this member's disk. A leader counts
    /// itself toward a commit only up to here. `None` treats every entry as
    /// durable, as the in-memory tests do.
    durable: Option<u64>,
}

impl Core {
    /// A member with persisted state. `seed` varies election timeouts.
    pub fn new(id: &str, group: &str, voters: Vec<String>, seed: u64) -> Self {
        let mut c = Core {
            id: id.to_string(),
            group: group.to_string(),
            term: 0,
            voted_for: None,
            role: Role::Follower,
            leader: None,
            commit: 0,
            applied: 0,
            log: Vec::new(),
            snap_index: 0,
            snap_term: 0,
            snap_voters: Vec::new(),
            initial_voters: sorted(voters),
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            heard_leader_at: 0,
            election_deadline: 0,
            heartbeat_due: 0,
            rng: seed | 1,
            outbox: Vec::new(),
            dirty: Dirty::default(),
            snapshot_source: None,
            installed: None,
            durable: None,
        };
        c.reset_election(0);
        c
    }

    /// Restore persisted state: term, vote, snapshot and log.
    pub fn restore(&mut self, term: u64, voted_for: Option<String>, snapshot: Option<Snapshot>, log: Vec<Entry>, applied: u64) {
        self.term = term;
        self.voted_for = voted_for;
        if let Some(s) = snapshot {
            self.snap_index = s.index;
            self.snap_term = s.term;
            self.snap_voters = s.voters;
        }
        self.log = log.into_iter().filter(|e| e.index > self.snap_index).collect();
        self.applied = applied.max(self.snap_index).min(self.last_index());
        self.commit = self.applied;
    }

    pub fn set_snapshot_source(&mut self, f: Box<dyn Fn() -> Value + Send>) {
        self.snapshot_source = Some(f);
    }

    fn rand(&mut self) -> u64 {
        // xorshift64*
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Start the election timer from `now`: a group opened after the node started.
    pub fn start_timer(&mut self, now: u64) {
        self.reset_election(now);
    }

    /// Campaign at the next tick, when there is no leader: the member a new
    /// queue should be led by.
    pub fn expedite(&mut self) {
        if self.role != Role::Leader && self.leader.is_none() {
            self.election_deadline = 0;
        }
    }

    fn reset_election(&mut self, now: u64) {
        let span = ELECTION_MAX_MS - ELECTION_MIN_MS;
        self.election_deadline = now + ELECTION_MIN_MS + self.rand() % span;
    }

    pub fn last_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(self.snap_index)
    }

    pub fn last_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(self.snap_term)
    }

    /// Term of the entry at `index`, if it is in the log or is the snapshot point.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snap_index {
            return Some(self.snap_term);
        }
        if index < self.snap_index {
            return None;
        }
        self.log.get((index - self.snap_index - 1) as usize).map(|e| e.term)
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        if index <= self.snap_index {
            return None;
        }
        self.log.get((index - self.snap_index - 1) as usize)
    }

    /// The voter set: the latest config entry in the log, else the snapshot's, else the initial set.
    pub fn voters(&self) -> Vec<String> {
        for e in self.log.iter().rev() {
            if e.kind == "config" {
                if let Some(v) = e.data.get("voters").and_then(|v| v.as_array()) {
                    return sorted(v.iter().filter_map(|x| x.as_str().map(str::to_string)).collect());
                }
            }
        }
        if !self.snap_voters.is_empty() {
            return self.snap_voters.clone();
        }
        self.initial_voters.clone()
    }

    fn quorum(&self) -> usize {
        self.voters().len() / 2 + 1
    }

    fn peers(&self) -> Vec<String> {
        self.voters().into_iter().filter(|v| *v != self.id).collect()
    }

    fn send(&mut self, to: &str, msg: Value) {
        self.outbox.push((to.to_string(), msg));
    }

    pub fn take_outbox(&mut self) -> Vec<(String, Value)> {
        std::mem::take(&mut self.outbox)
    }

    pub fn take_dirty(&mut self) -> Dirty {
        std::mem::take(&mut self.dirty)
    }

    /// A snapshot installed from the leader, for the driver to load into its state machine.
    pub fn take_installed(&mut self) -> Option<Snapshot> {
        self.installed.take()
    }

    /// Entries committed but not yet applied, in order. Marks them applied.
    pub fn take_committed(&mut self) -> Vec<Entry> {
        let mut out = Vec::new();
        while self.applied < self.commit {
            let next = self.applied + 1;
            match self.entry(next) {
                Some(e) => out.push(e.clone()),
                None => break,
            }
            self.applied = next;
        }
        out
    }

    fn become_follower(&mut self, term: u64, leader: Option<String>, now: u64) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.dirty.hard_state = true;
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.reset_election(now);
    }

    fn append_local(&mut self, kind: &str, data: Value) -> u64 {
        let e = Entry { index: self.last_index() + 1, term: self.term, kind: kind.to_string(), data };
        self.dirty.appended.push(e.clone());
        self.log.push(e);
        self.last_index()
    }

    /// Append a command as leader. Returns its index, or None when not leader.
    ///
    /// The entry goes out with the next [`Core::tick`], so a burst of
    /// proposals shares one `append` per peer instead of one each.
    pub fn propose(&mut self, kind: &str, data: Value, now: u64) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        let index = self.append_local(kind, data);
        self.maybe_commit();
        self.heartbeat_due = self.heartbeat_due.min(now);
        Some(index)
    }

    /// Change the voters one member at a time toward `wanted`. Leader only;
    /// does nothing while an earlier config entry is uncommitted.
    pub fn reconfigure(&mut self, wanted: &[String], now: u64) {
        if self.role != Role::Leader {
            return;
        }
        let pending = self.log.iter().any(|e| e.kind == "config" && e.index > self.commit);
        if pending {
            return;
        }
        let current = self.voters();
        let wanted = sorted(wanted.to_vec());
        if current == wanted || wanted.is_empty() {
            return;
        }
        let mut next = current.clone();
        if let Some(add) = wanted.iter().find(|w| !current.contains(w)) {
            next.push(add.clone());
        } else if let Some(remove) = current.iter().find(|c| !wanted.contains(c)) {
            next.retain(|c| c != remove);
        }
        let next = sorted(next);
        self.append_local("config", json!({"voters": next}));
        let last = self.last_index();
        for p in self.peers() {
            self.next_index.entry(p.clone()).or_insert(last);
            self.match_index.entry(p).or_insert(0);
        }
        self.maybe_commit();
        self.broadcast_append(now);
    }

    /// Advance time: start an election, or send heartbeats.
    pub fn tick(&mut self, now: u64) {
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_due {
                    self.broadcast_append(now);
                }
            }
            _ => {
                if now >= self.election_deadline && self.voters().contains(&self.id) {
                    self.start_pre_vote(now);
                }
            }
        }
    }

    fn start_pre_vote(&mut self, now: u64) {
        self.role = Role::PreCandidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id.clone());
        self.reset_election(now);
        if self.votes.len() >= self.quorum() {
            return self.start_election(now);
        }
        let msg = json!({"t": "vote", "g": self.group, "term": self.term + 1, "cand": self.id,
            "lli": self.last_index(), "llt": self.last_term(), "pre": true});
        for p in self.peers() {
            self.send(&p, msg.clone());
        }
    }

    fn start_election(&mut self, now: u64) {
        self.role = Role::Candidate;
        self.term += 1;
        self.voted_for = Some(self.id.clone());
        self.dirty.hard_state = true;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.id.clone());
        self.reset_election(now);
        if self.votes.len() >= self.quorum() {
            return self.become_leader(now);
        }
        let msg = json!({"t": "vote", "g": self.group, "term": self.term, "cand": self.id,
            "lli": self.last_index(), "llt": self.last_term(), "pre": false});
        for p in self.peers() {
            self.send(&p, msg.clone());
        }
    }

    fn become_leader(&mut self, now: u64) {
        self.role = Role::Leader;
        self.leader = Some(self.id.clone());
        self.next_index.clear();
        self.match_index.clear();
        let next = self.last_index() + 1;
        for p in self.peers() {
            self.next_index.insert(p.clone(), next);
            self.match_index.insert(p, 0);
        }
        // A noop in the new term lets earlier entries commit.
        self.append_local("noop", Value::Null);
        self.maybe_commit();
        self.broadcast_append(now);
    }

    fn broadcast_append(&mut self, now: u64) {
        self.heartbeat_due = now + HEARTBEAT_MS;
        for p in self.peers() {
            self.send_append(&p);
        }
    }

    fn send_append(&mut self, peer: &str) {
        let next = *self.next_index.get(peer).unwrap_or(&(self.last_index() + 1));
        if next <= self.snap_index {
            let state = self.snapshot_source.as_ref().map(|f| f()).unwrap_or(Value::Null);
            let msg = json!({"t": "snap", "g": self.group, "term": self.term, "leader": self.id,
                "lii": self.snap_index, "lit": self.snap_term, "voters": self.snap_voters_or_current(), "state": state});
            return self.send(peer, msg);
        }
        let pli = next - 1;
        let plt = self.term_at(pli).unwrap_or(0);
        let entries: Vec<Value> = (next..=self.last_index()).take(MAX_BATCH).filter_map(|i| self.entry(i).map(Entry::to_json)).collect();
        // Pipelined: the next append starts after what is now in flight. The
        // connection is ordered, so the follower has it by then; a rejection
        // rewinds `next_index` to the follower's hint.
        let sent = entries.len() as u64;
        let msg = json!({"t": "append", "g": self.group, "term": self.term, "leader": self.id,
            "pli": pli, "plt": plt, "entries": entries, "commit": self.commit});
        self.send(peer, msg);
        if sent > 0 {
            self.next_index.insert(peer.to_string(), next + sent);
        }
    }

    fn snap_voters_or_current(&self) -> Vec<String> {
        if self.snap_voters.is_empty() {
            self.voters()
        } else {
            self.snap_voters.clone()
        }
    }

    fn maybe_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let voters = self.voters();
        let mut matched: Vec<u64> = voters
            .iter()
            .map(|v| if *v == self.id { self.durable_last() } else { *self.match_index.get(v).unwrap_or(&0) })
            .collect();
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&n) = matched.get(self.quorum() - 1) else { return };
        // Only entries of the current term commit by counting (Raft §5.4.2).
        if n > self.commit && self.term_at(n) == Some(self.term) {
            self.commit = n;
        }
    }

    /// The driver's disk writer finished writing through `index`.
    pub fn set_durable(&mut self, index: u64) {
        self.durable = Some(self.durable.map_or(index, |d| d.max(index)));
        self.maybe_commit();
    }

    /// Entries from `index + 1` on were replaced and are not yet on disk.
    pub fn cap_durable(&mut self, index: u64) {
        if let Some(d) = self.durable {
            self.durable = Some(d.min(index));
        }
    }

    fn durable_last(&self) -> u64 {
        self.durable.map_or(self.last_index(), |d| d.min(self.last_index()))
    }

    /// Handle one message from `from`.
    pub fn step(&mut self, from: &str, msg: &Value, now: u64) {
        let t = msg.get("t").and_then(Value::as_str).unwrap_or_default();
        let term = msg.get("term").and_then(Value::as_u64).unwrap_or(0);
        let pre = msg.get("pre").and_then(Value::as_bool).unwrap_or(false);
        let pre_vote = (t == "vote" || t == "vote_r") && pre;
        if !pre_vote && term > self.term && matches!(t, "vote" | "vote_r" | "append" | "append_r" | "snap" | "snap_r") {
            let leader = matches!(t, "append" | "snap").then(|| from.to_string());
            self.become_follower(term, leader, now);
        }
        match t {
            "vote" => self.on_vote(from, msg, term, pre, now),
            "vote_r" => self.on_vote_reply(from, msg, term, pre, now),
            "append" => self.on_append(from, msg, term, now),
            "append_r" => self.on_append_reply(from, msg, term),
            "snap" => self.on_snap(from, msg, term, now),
            "snap_r" => self.on_snap_reply(from, msg, term),
            _ => {}
        }
    }

    fn log_ok(&self, lli: u64, llt: u64) -> bool {
        llt > self.last_term() || (llt == self.last_term() && lli >= self.last_index())
    }

    fn on_vote(&mut self, from: &str, msg: &Value, term: u64, pre: bool, now: u64) {
        let cand = msg.get("cand").and_then(Value::as_str).unwrap_or(from).to_string();
        let lli = msg.get("lli").and_then(Value::as_u64).unwrap_or(0);
        let llt = msg.get("llt").and_then(Value::as_u64).unwrap_or(0);
        let granted = if pre {
            // A leader, or a follower that heard one recently, refuses: the cluster is not leaderless.
            let leader_live = self.role == Role::Leader || (self.leader.is_some() && now < self.heard_leader_at + ELECTION_MIN_MS);
            term > self.term && self.log_ok(lli, llt) && !leader_live
        } else {
            term == self.term && self.voted_for.as_ref().is_none_or(|v| *v == cand) && self.log_ok(lli, llt)
        };
        if granted && !pre {
            self.voted_for = Some(cand.clone());
            self.dirty.hard_state = true;
            self.reset_election(now);
        }
        let reply_term = if pre { term.max(self.term) } else { self.term };
        let reply = json!({"t": "vote_r", "g": self.group, "term": if granted && pre { term } else { reply_term }, "granted": granted, "pre": pre});
        self.send(from, reply);
    }

    fn on_vote_reply(&mut self, from: &str, msg: &Value, term: u64, pre: bool, now: u64) {
        let granted = msg.get("granted").and_then(Value::as_bool).unwrap_or(false);
        if !granted {
            return;
        }
        if pre {
            if self.role != Role::PreCandidate || term != self.term + 1 {
                return;
            }
            self.votes.insert(from.to_string());
            if self.votes.len() >= self.quorum() {
                self.start_election(now);
            }
        } else {
            if self.role != Role::Candidate || term != self.term {
                return;
            }
            self.votes.insert(from.to_string());
            if self.votes.len() >= self.quorum() {
                self.become_leader(now);
            }
        }
    }

    fn on_append(&mut self, from: &str, msg: &Value, term: u64, now: u64) {
        if term < self.term {
            let reply = json!({"t": "append_r", "g": self.group, "term": self.term, "ok": false, "hint": self.last_index() + 1});
            return self.send(from, reply);
        }
        // A leader of this term exists; a candidate of the same term steps down.
        if self.role != Role::Follower || self.leader.as_deref() != Some(from) {
            self.role = Role::Follower;
            self.leader = Some(from.to_string());
            self.votes.clear();
        }
        self.heard_leader_at = now;
        self.reset_election(now);
        let pli = msg.get("pli").and_then(Value::as_u64).unwrap_or(0);
        let plt = msg.get("plt").and_then(Value::as_u64).unwrap_or(0);
        let leader_commit = msg.get("commit").and_then(Value::as_u64).unwrap_or(0);
        if pli > self.last_index() {
            let reply = json!({"t": "append_r", "g": self.group, "term": self.term, "ok": false, "hint": self.last_index() + 1});
            return self.send(from, reply);
        }
        if pli >= self.snap_index {
            if self.term_at(pli) != Some(plt) {
                // Skip back over the whole conflicting term.
                let bad = self.term_at(pli).unwrap_or(0);
                let mut hint = pli;
                while hint > self.snap_index + 1 && self.term_at(hint - 1) == Some(bad) {
                    hint -= 1;
                }
                let reply = json!({"t": "append_r", "g": self.group, "term": self.term, "ok": false, "hint": hint.max(self.snap_index + 1)});
                return self.send(from, reply);
            }
        }
        let entries: Vec<Entry> = msg.get("entries").and_then(Value::as_array).map(|a| a.iter().filter_map(Entry::from_json).collect()).unwrap_or_default();
        let mut last_new = pli;
        for e in entries {
            last_new = e.index;
            if e.index <= self.snap_index {
                continue;
            }
            match self.term_at(e.index) {
                Some(t) if t == e.term => continue,
                Some(_) => {
                    // Conflict: drop it and everything after.
                    let keep = (e.index - self.snap_index - 1) as usize;
                    self.log.truncate(keep);
                    self.dirty.appended.retain(|a| a.index < e.index);
                    self.dirty.truncated_from = Some(self.dirty.truncated_from.map_or(e.index, |f| f.min(e.index)));
                    self.dirty.appended.push(e.clone());
                    self.log.push(e);
                }
                None => {
                    self.dirty.appended.push(e.clone());
                    self.log.push(e);
                }
            }
        }
        if leader_commit > self.commit {
            self.commit = leader_commit.min(last_new).max(self.commit);
        }
        let reply = json!({"t": "append_r", "g": self.group, "term": self.term, "ok": true, "match": last_new});
        self.send(from, reply);
    }

    fn on_append_reply(&mut self, from: &str, msg: &Value, term: u64) {
        if self.role != Role::Leader || term != self.term {
            return;
        }
        if msg.get("ok").and_then(Value::as_bool).unwrap_or(false) {
            let m = msg.get("match").and_then(Value::as_u64).unwrap_or(0);
            let prev = *self.match_index.get(from).unwrap_or(&0);
            if m > prev {
                self.match_index.insert(from.to_string(), m);
            }
            // Keep a `next_index` already past this reply: those entries are in flight.
            let next = (*self.next_index.get(from).unwrap_or(&0)).max(m.max(prev) + 1);
            self.next_index.insert(from.to_string(), next);
            self.maybe_commit();
            if self.next_index[from] <= self.last_index() {
                self.send_append(from);
            }
        } else {
            let hint = msg.get("hint").and_then(Value::as_u64).unwrap_or(1).max(1);
            let next = hint.min(self.last_index() + 1);
            self.next_index.insert(from.to_string(), next);
            self.send_append(from);
        }
    }

    fn on_snap(&mut self, from: &str, msg: &Value, term: u64, now: u64) {
        if term < self.term {
            let reply = json!({"t": "snap_r", "g": self.group, "term": self.term, "lii": 0});
            return self.send(from, reply);
        }
        self.role = Role::Follower;
        self.leader = Some(from.to_string());
        self.heard_leader_at = now;
        self.reset_election(now);
        let lii = msg.get("lii").and_then(Value::as_u64).unwrap_or(0);
        let lit = msg.get("lit").and_then(Value::as_u64).unwrap_or(0);
        if lii > self.commit {
            let voters = msg.get("voters").and_then(Value::as_array).map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default();
            let snap = Snapshot { index: lii, term: lit, voters, state: msg.get("state").cloned().unwrap_or(Value::Null) };
            // Keep the log after the snapshot only if it agrees with it.
            if self.term_at(lii) == Some(lit) {
                self.log.retain(|e| e.index > lii);
            } else {
                self.log.clear();
            }
            self.snap_index = lii;
            self.snap_term = lit;
            self.snap_voters = sorted(snap.voters.clone());
            self.commit = lii;
            self.applied = lii;
            self.dirty.snapshot = Some(snap.clone());
            self.installed = Some(snap);
        }
        let reply = json!({"t": "snap_r", "g": self.group, "term": self.term, "lii": lii});
        self.send(from, reply);
    }

    fn on_snap_reply(&mut self, from: &str, msg: &Value, term: u64) {
        if self.role != Role::Leader || term != self.term {
            return;
        }
        let lii = msg.get("lii").and_then(Value::as_u64).unwrap_or(0);
        if lii > *self.match_index.get(from).unwrap_or(&0) {
            self.match_index.insert(from.to_string(), lii);
        }
        self.next_index.insert(from.to_string(), lii + 1);
        self.maybe_commit();
    }

    /// Replace the applied prefix of the log with a snapshot of `state`.
    /// Returns the snapshot for the driver to store.
    pub fn compact(&mut self, state: Value) -> Option<Snapshot> {
        self.compact_at(self.applied, state)
    }

    /// Like [`Core::compact`], at `index`: the point the driver's state
    /// machine had reached when it produced `state`. `index` past `applied`
    /// or at or before the current snapshot does nothing.
    pub fn compact_at(&mut self, index: u64, state: Value) -> Option<Snapshot> {
        if index <= self.snap_index || index > self.applied {
            return None;
        }
        let term = self.term_at(index)?;
        let voters = self.voters_at(index);
        self.log.retain(|e| e.index > index);
        self.snap_index = index;
        self.snap_term = term;
        self.snap_voters = voters.clone();
        Some(Snapshot { index, term, voters, state })
    }

    fn voters_at(&self, index: u64) -> Vec<String> {
        for e in self.log.iter().rev().filter(|e| e.index <= index) {
            if e.kind == "config" {
                if let Some(v) = e.data.get("voters").and_then(|v| v.as_array()) {
                    return sorted(v.iter().filter_map(|x| x.as_str().map(str::to_string)).collect());
                }
            }
        }
        if !self.snap_voters.is_empty() {
            self.snap_voters.clone()
        } else {
            self.initial_voters.clone()
        }
    }

    /// Entries in the log (after the snapshot), for persistence rewrites.
    pub fn log(&self) -> &[Entry] {
        &self.log
    }

    pub fn snapshot_point(&self) -> (u64, u64) {
        (self.snap_index, self.snap_term)
    }
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v.dedup();
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// An in-memory cluster with a message queue and partition control.
    struct Sim {
        nodes: HashMap<String, Core>,
        down: BTreeSet<String>,
        now: u64,
        applied: HashMap<String, Vec<Entry>>,
    }

    impl Sim {
        fn new(ids: &[&str]) -> Sim {
            let voters: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
            let nodes = ids.iter().enumerate().map(|(i, id)| (id.to_string(), Core::new(id, "meta", voters.clone(), 7 + i as u64 * 1000))).collect();
            Sim { nodes, down: BTreeSet::new(), now: 0, applied: HashMap::new() }
        }

        /// Deliver messages until quiet, then advance time by `ms`.
        fn run(&mut self, ms: u64) {
            let end = self.now + ms;
            while self.now < end {
                self.now += 10;
                let ids: Vec<String> = self.nodes.keys().cloned().collect();
                for id in &ids {
                    if self.down.contains(id) {
                        continue;
                    }
                    let now = self.now;
                    self.nodes.get_mut(id).unwrap().tick(now);
                }
                self.flush();
            }
        }

        fn flush(&mut self) {
            loop {
                let mut moved = false;
                let ids: Vec<String> = self.nodes.keys().cloned().collect();
                for id in &ids {
                    let n = self.nodes.get_mut(id).unwrap();
                    let _ = n.take_dirty();
                    let out = n.take_outbox();
                    let done = n.take_committed();
                    self.applied.entry(id.clone()).or_default().extend(done.into_iter().filter(|e| e.kind != "noop" && e.kind != "config"));
                    for (to, msg) in out {
                        if self.down.contains(id) || self.down.contains(&to) {
                            continue;
                        }
                        let now = self.now;
                        if let Some(dst) = self.nodes.get_mut(&to) {
                            dst.step(id, &msg, now);
                            moved = true;
                        }
                    }
                }
                if !moved {
                    break;
                }
            }
        }

        fn leaders(&self) -> Vec<String> {
            let mut by_term: Vec<(u64, String)> = self.nodes.values().filter(|n| n.role == Role::Leader && !self.down.contains(&n.id)).map(|n| (n.term, n.id.clone())).collect();
            by_term.sort();
            by_term.into_iter().map(|(_, id)| id).collect()
        }

        fn leader(&self) -> String {
            let l = self.leaders();
            assert_eq!(l.len(), 1, "expected one live leader, got {l:?}");
            l[0].clone()
        }

        fn propose(&mut self, kind: &str, data: Value) -> u64 {
            let l = self.leader();
            let now = self.now;
            let idx = self.nodes.get_mut(&l).unwrap().propose(kind, data, now).unwrap();
            self.flush();
            idx
        }

        fn values(&self, id: &str) -> Vec<Value> {
            self.applied.get(id).map(|v| v.iter().map(|e| e.data.clone()).collect()).unwrap_or_default()
        }
    }

    #[test]
    fn elects_one_leader_and_replicates() {
        let mut s = Sim::new(&["a", "b", "c"]);
        s.run(3000);
        let leader = s.leader();
        s.propose("x", json!(1));
        s.propose("x", json!(2));
        s.run(500);
        for id in ["a", "b", "c"] {
            assert_eq!(s.values(id), vec![json!(1), json!(2)], "node {id}, leader {leader}");
        }
    }

    #[test]
    fn a_new_leader_is_elected_after_the_leader_fails_and_keeps_committed_entries() {
        let mut s = Sim::new(&["a", "b", "c"]);
        s.run(3000);
        s.propose("x", json!("before"));
        s.run(300);
        let old = s.leader();
        s.down.insert(old.clone());
        s.run(4000);
        let new = s.leader();
        assert_ne!(new, old);
        s.propose("x", json!("after"));
        s.run(300);
        s.down.clear();
        s.run(1000);
        for id in ["a", "b", "c"] {
            assert_eq!(s.values(id), vec![json!("before"), json!("after")], "node {id}");
        }
    }

    #[test]
    fn a_minority_cannot_commit() {
        let mut s = Sim::new(&["a", "b", "c"]);
        s.run(3000);
        let leader = s.leader();
        let others: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).filter(|x| *x != leader).collect();
        for o in &others {
            s.down.insert(o.clone());
        }
        let now = s.now;
        s.nodes.get_mut(&leader).unwrap().propose("x", json!("lost"), now);
        s.run(500);
        assert!(s.values(&leader).is_empty());
        // The majority side elects a leader whose log wins; the stray entry is replaced.
        s.down.clear();
        s.down.insert(leader.clone());
        s.run(4000);
        s.propose("x", json!("kept"));
        s.down.clear();
        s.run(2000);
        for id in ["a", "b", "c"] {
            assert_eq!(s.values(id), vec![json!("kept")], "node {id}");
        }
    }

    #[test]
    fn a_partitioned_node_rejoins_without_disrupting_the_leader() {
        let mut s = Sim::new(&["a", "b", "c"]);
        s.run(3000);
        let leader = s.leader();
        let term = s.nodes[&leader].term;
        let other = ["a", "b", "c"].iter().map(|s| s.to_string()).find(|x| *x != leader).unwrap();
        s.down.insert(other.clone());
        s.run(6000);
        s.down.clear();
        s.run(1000);
        // Pre-vote keeps the isolated node from forcing a new term.
        assert_eq!(s.leader(), leader);
        assert_eq!(s.nodes[&leader].term, term);
    }

    #[test]
    fn a_lagging_follower_is_caught_up_by_snapshot() {
        let mut s = Sim::new(&["a", "b", "c"]);
        s.run(3000);
        let leader = s.leader();
        let lag = ["a", "b", "c"].iter().map(|s| s.to_string()).find(|x| *x != leader).unwrap();
        s.down.insert(lag.clone());
        for i in 0..20 {
            s.propose("x", json!(i));
        }
        s.run(300);
        let l = s.nodes.get_mut(&leader).unwrap();
        let snap = l.compact(json!({"count": 20})).unwrap();
        l.set_snapshot_source(Box::new(|| json!({"count": 20})));
        assert!(snap.index >= 20);
        s.down.clear();
        s.run(1000);
        assert!(s.nodes.get_mut(&lag).unwrap().take_installed().is_some());
        assert!(s.nodes[&lag].commit >= snap.index);
        s.propose("x", json!("next"));
        s.run(300);
        assert_eq!(s.values(&lag).last(), Some(&json!("next")));
    }

    #[test]
    fn membership_changes_one_voter_at_a_time() {
        let mut s = Sim::new(&["a", "b", "c"]);
        s.nodes.insert("d".into(), Core::new("d", "meta", vec!["a".into(), "b".into(), "c".into()], 99));
        s.run(3000);
        let leader = s.leader();
        let wanted: Vec<String> = ["a", "b", "c", "d"].iter().map(|x| x.to_string()).collect();
        for _ in 0..4 {
            let now = s.now;
            s.nodes.get_mut(&leader).unwrap().reconfigure(&wanted, now);
            s.run(300);
        }
        for id in ["a", "b", "c", "d"] {
            assert_eq!(s.nodes[id].voters(), wanted, "node {id}");
        }
        s.propose("x", json!("four"));
        s.run(300);
        assert_eq!(s.values("d"), vec![json!("four")]);
    }

    #[test]
    fn a_single_voter_commits_alone() {
        let mut s = Sim::new(&["solo"]);
        s.run(2500);
        s.propose("x", json!(1));
        assert_eq!(s.values("solo"), vec![json!(1)]);
    }
}
