# QueueForge cluster protocol: Raft

This is the wire format the Rust, Bun and PHP brokers speak to each other
for consensus. It extends the cluster protocol they already share (newline
JSON over TCP, one envelope per line, `v: 1`). Every implementation must
produce and accept exactly what is written here; anything not written here
is implementation detail.

## 1. Transport

- Raft messages ride the existing cluster connection between two members,
  in either direction, as the envelope
  `{"v":1,"op":"raft","id":0,"from":"<sender node id>","payload":{...}}`.
- They are one-way. A reply is another `raft` envelope, not an `op:"reply"`.
  `id` is always `0`, and a receiver never answers `op:"raft"` with a reply.
- A message to a peer that is not connected is dropped. Raft retries.
- Unknown `payload.t` values are ignored, so new message types can be added
  behind a feature flag (section 8).

## 2. Groups

A broker runs several independent Raft groups over one connection. Each
message names its group in `payload.g`.

| group    | holds                                                        |
|----------|--------------------------------------------------------------|
| `meta`   | vhosts, users, permissions, exchanges, queues, bindings, policies |
| `quorum` | every quorum queue's messages: appends and drops             |

A group's voters are the cluster members (section 7). Group ids are
strings; a later version may add one group per quorum queue (`q:<vhost>/<name>`).

## 3. Terms, indexes and the log

- `term` and `index` are JSON integers, starting at 1. Index 0 and term 0
  mean "before the log".
- A log entry is `{"i": index, "term": term, "kind": "<kind>", "data": <JSON>}`.
- `kind: "noop"` (`data: null`) is appended by a new leader so that it can
  commit entries from earlier terms.
- `kind: "config"` (`data: {"voters": ["<id>", ...]}`) changes the voter set.
  It takes effect when it is appended, not when it commits. A leader
  appends at most one uncommitted `config` at a time and changes one voter
  per entry, as in the Raft thesis' single-server changes.
- Every other kind is a state-machine command (sections 5 and 6).

## 4. Messages

All fields are required unless marked optional. `g` is the group id.

### vote: RequestVote

```json
{"t":"vote","g":"meta","term":7,"cand":"n2","lli":40,"llt":6,"pre":false}
```

`lli` and `llt` are the candidate's last log index and term. With
`pre: true` it is a pre-vote. The receiver answers as for a real vote but
changes neither its term nor its vote, so a node that was partitioned
cannot raise everyone's term on return.

### vote_r: vote reply

```json
{"t":"vote_r","g":"meta","term":7,"granted":true,"pre":false}
```

`pre` echoes the request. A voter grants when all of these hold:

1. `term >= currentTerm` (for a pre-vote, `term > currentTerm`).
2. It has not voted for another candidate in `term` (real votes only).
3. The candidate's log is at least as up to date: `llt > myLastTerm`, or
   `llt == myLastTerm && lli >= myLastIndex`.
4. For a pre-vote, it has not heard from a live leader within the minimum
   election timeout.

### append: AppendEntries

```json
{"t":"append","g":"meta","term":7,"leader":"n1","pli":40,"plt":6,
 "entries":[{"i":41,"term":7,"kind":"noop","data":null}],"commit":40}
```

`pli` and `plt` are the index and term just before `entries[0]`, or the
follower's next index minus one for a heartbeat (empty `entries`).
`commit` is the leader's commit index.

### append_r: AppendEntries reply

```json
{"t":"append_r","g":"meta","term":7,"ok":true,"match":41}
{"t":"append_r","g":"meta","term":7,"ok":false,"hint":35}
```

- With `ok: true`, `match` is the follower's last index known to equal the
  leader's.
- With `ok: false`, `hint` is the index the leader should retry from: the
  follower's last index + 1 if its log is shorter, else the first index of
  the conflicting term.
- A follower answers only once the entries it accepted are durable
  (fsynced).

### snap: InstallSnapshot

```json
{"t":"snap","g":"meta","term":7,"leader":"n1","lii":500,"lit":6,
 "voters":["n1","n2","n3"],"state":{...}}
```

Sent when a follower needs an index the leader has compacted away. `state`
is the group's whole state machine (sections 5 and 6) as of `lii`.

### snap_r: InstallSnapshot reply

```json
{"t":"snap_r","g":"meta","term":7,"lii":500}
```

### propose: forward a command to the leader

```json
{"t":"propose","g":"meta","rid":"n3-118","kind":"exchange","data":{...}}
```

A non-leader forwards a command it was given. `rid` is unique to the sender.

### propose_r: proposal outcome

```json
{"t":"propose_r","g":"meta","rid":"n3-118","ok":true,"index":512}
{"t":"propose_r","g":"meta","rid":"n3-118","ok":false,"leader":"n2","error":"not leader"}
```

The leader answers once the entry commits (`ok: true`), or at once when it
is not the leader (`leader` names the one it knows, if any). The proposer
gives up after 5 s.

### Term rule

Any message whose `term` is higher than the receiver's `currentTerm`
(except a pre-vote and its reply) makes the receiver adopt that term, clear
its vote and become a follower before handling the message. A message with
a lower `term` gets a reply carrying the receiver's term and is otherwise
ignored. `propose` and `propose_r` carry no term.

## 5. The `meta` state machine

Each command is applied, on commit, in log order on every member. Its
`kind` and `data` are exactly the `kind` and `body` of the existing
`op:"apply"` replication message, which every implementation already
parses:

`vhost`, `delete_vhost`, `user`, `delete_user`, `permission`,
`delete_permission`, `exchange`, `delete_exchange`, `queue`,
`delete_queue`, `binding`, `unbind`, `policy`, `delete_policy`.

Applying is idempotent: declaring something that exists with the same
definition, or deleting something missing, is not an error. The node that
proposed a command has already applied it, so applying it again on commit
is a no-op.

The proposer also pushes the command as a version 1 `op:"apply"` to the
members it is connected to, after the commit. A follower learns a commit
index only with the next `append`, up to one heartbeat later, and a client
that declares a queue on one node and uses it on another expects it to be
there at once, as on RabbitMQ. The push is the same idempotent apply.

Snapshot `state`, for `snap`:

```json
{"vhosts":[...],"users":[...],"permissions":[...],"exchanges":[...],
 "queues":[...],"bindings":[...],"policies":[...]}
```

Each list holds `data` values of the matching create command. Installing a
snapshot replaces the durable metadata it covers; exclusive queues are
local and are kept.

## 6. The `quorum` state machine

| kind   | data                                   | effect on every member |
|--------|----------------------------------------|------------------------|
| `enq`  | quorum append v1 body (below)          | store the message on queue `vhost/queue` |
| `drop` | `{"vhost":..,"queue":..,"ids":[..]}`   | forget those message ids |
| `purge`| `{"vhost":..,"queue":..}`              | forget every message of the queue |

The quorum append v1 body is the one the brokers already exchange:
`{"v":1,"vhost","queue","message_id","body_b64","persistent","routing_key","exchange"}`.
`message_id` is unique cluster-wide; applying an `enq` whose id is known is
a no-op.

- The leader of the `quorum` group is the leader of every quorum queue. It
  alone delivers to consumers; consumers on other members are served by it,
  as today.
- A publish is confirmed when its `enq` commits, so a majority has it on
  disk.
- A delivery is preceded by a committed `drop` of that id, so after a
  failover the new leader cannot deliver it again. A `nack` with requeue
  needs no entry; the message never left the leader's queue.
- Followers keep their copies outside the ready queue. A member that
  becomes leader moves them into it.
- Because the drop commits at delivery, a message delivered by a leader
  that then dies before the consumer acks is not redelivered by the next
  leader. RabbitMQ's quorum queues do redeliver it; this is the one place
  the semantics differ.
- A consume or `basic.get` that arrives while no connected leader is known
  waits up to two election timeouts for one, instead of failing.

Snapshot `state`, for `snap`: `{"queues":[{"vhost","queue","messages":[<enq data>...]}]}`.

## 7. Voters, timing and persistence

- **Voters** are the cluster members, from the config list, the
  `members.json` runtime list, `QUEUEFORGE_MEMBERS`, or DNS discovery.
  Every group starts with the sorted member ids as its voters. When the
  member list changes, the leader appends `config` entries one voter at a
  time.
- **Timing:** a leader sends `append` (a heartbeat if it has nothing new)
  every 150 ms. A follower that hears nothing from a leader for a random
  1000 to 2000 ms starts a pre-vote, then an election.
- **Persistence:** before sending `vote_r` with `granted: true`, or
  `append_r` with `ok: true`, a member must durably store `currentTerm`,
  `votedFor`, and the log entries it accepted. The file layout is up to each
  implementation. Rust and Bun both use `raft/<group>/state.json`,
  `log.jsonl` (one entry per line; a torn last line is dropped on load) and
  `snapshot.json` in the data directory.
- **Leader writes:** a leader may send `append` and `snap` before its own
  fsync of the same entries (the Raft thesis, section 10.2.1). It counts
  itself toward a commit only up to the index its disk writer reported
  durable.
- **Pipelining:** a leader advances a follower's next index past the
  entries it just sent, so the next `append` carries only new entries. A
  rejection's `hint` moves it back.
- **Compaction:** a member may replace a committed prefix of its log with a
  snapshot. Rust and Bun compact `quorum` past 10 000 entries and `meta`
  past 50 000.

## 8. Feature flags and versions

- The `hello` payload gains `"features": ["raft"]`, the flags this build
  supports. `QUEUEFORGE_RAFT=0` leaves it out, so a node stays on
  version 1 as a build without Raft would.
- `raft` is **enabled** once every voter has advertised it in a hello. It is
  then written to the data directory and never turned off. Until then, the
  members keep the version 1 behaviour (majority-ack quorum queues and
  pushed metadata), so a cluster can be upgraded one node at a time, and a
  member without Raft (PHP today) keeps the whole cluster on it.
- The management API lists the flags, with `state: "enabled"` or
  `"disabled"`, at `GET /api/feature-flags`.
- A future incompatible change bumps the envelope `v` and adds a new flag.
  A member accepts any `v` it knows and drops lines with a higher `v`.

## 9. Classic queue homes

A classic queue's home member is chosen the same way everywhere: sort the
member ids, hash the UTF-8 bytes of `vhost`, then one `0xff` byte, then
the queue name, with 64-bit FNV-1a (offset basis `0xcbf29ce484222325`,
prime `0x100000001b3`), and take the hash modulo the member count. The
chosen home is stored with the queue, so a later membership change does
not move it.

## 10. Peer discovery

Members can come from:

1. `[cluster] members = [{id, addr}, ...]` in the config file;
2. `QUEUEFORGE_MEMBERS`, a JSON array of `{id, addr}`, which replaces (1);
3. `[cluster] discovery = "dns"` with `dns_name` and `dns_port`: every A or
   AAAA record of `dns_name`, polled every 5 s, becomes a member whose id
   and address are both `<ip>:<dns_port>`. The local node's id is its
   `listen` address.

`members.json` in the data directory, written by the membership API,
overrides all three.
