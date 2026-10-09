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
strings.

**Queue groups.** A member that advertises `raft_qgroups` (Rust and Bun) runs one
more group per quorum queue and per replicated stream, `q:v2:<hex-vhost>:<hex-name>`,
as RabbitMQ runs one Ra cluster per queue:

The v2 identity hex-encodes each UTF-8 component separately. New groups cannot collide with each other or with legacy `q:<vhost>/<name>` identities. Existing queue rows and group directories keep their stored names; they are read as opaque identities during upgrades and rollback. Previously colliding legacy queues require recovery from their retained data; the upgrade does not guess how to split a shared log.

- A new quorum queue or stream gets its own group when Raft is on and every
  voter advertises `raft_qgroups`. The queue row carries `raftGroup`, so the
  queue and its group are created by the same `meta` entry on every member.
  Otherwise, as in a cluster with an older member, a quorum queue stays in
  the shared `quorum` group and a stream on its home node.
  `QUEUEFORGE_RAFT_QGROUPS=0` keeps a member from advertising it.
- The row may carry `raftLeader`, the member that campaigns at once: the
  declaring member (`client-local`, the default) or the one leading the
  fewest queue groups (`balanced`). A declare returns once the group has a
  leader. Rust's declaring member campaigns once the row is committed and
  pushed, so the other members run the group when its vote arrives.
- Each queue group elects its own leader, so losing a member fails over
  only the queues it led. Deleting the queue stops the group on every
  member and removes its files.
- Files are `raft/q-<fnv64 of the group id>/`, with `group.json` naming the
  group. A message for a queue group a member does not run starts it when
  the queue exists there (Bun); Rust starts it when the queue row applies,
  and Raft resends what it missed before that.
- A queue group of a quorum queue holds the `quorum` entries of section 6
  for that queue only, and its snapshot only that queue's messages.
- A queue group of a stream holds `sappend` entries:
  `{"vhost","queue","ts","body_b64","exchange","routing_key","headers","propRaw"}`.
  Every member appends each one to its copy of the stream in log order, so
  offsets agree everywhere and a consumer reads on the member it is
  connected to. A publish is confirmed once its `sappend` commits. Each
  stored entry keeps its Raft index, so a replay after a restart is
  skipped. The snapshot is the retained entries with `first`, `next` and
  the Raft index.

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
`delete_queue`, `binding`, `unbind`, `policy`, `delete_policy`,
`members` (section 7), and the settings kinds:

- `user_limits` `{"user","max-connections","max-channels"}` and
  `vhost_limits` `{"vhost","max-connections","max-queues"}`, the whole row
  (`null` clears a limit);
- `topic_permission` `{"user","vhost","exchange","write","read"}` and
  `delete_topic_permission` `{"user","vhost","exchange"}`;
- `parameter` `{"component","vhost","name","value"}` and `delete_parameter`
  (a `shovel` runs where it was declared and is not replicated);
- `global_parameter` `{"name","value"}` and `delete_global_parameter`.

`delete_queue` names the queue as `queue` (Rust) or `name` (Bun); both are
read.

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
 "queues":[...],"bindings":[...],"policies":[...],
 "userLimits":[...],"vhostLimits":[...],"topicPermissions":[...],
 "parameters":[...],"globalParameters":[...]}
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

- The leader of a queue's group (its own, or `quorum`) is the queue's
  leader. It alone delivers to consumers; consumers on other members are
  served by it.
- A publish is confirmed when its `enq` commits, so a majority has it on
  disk. The body carries `propRaw`, `headers`, `priority` and `expiration`
  when it has them, so a follower that becomes leader delivers the message
  as published. A reader that does not know these fields ignores them.
- **Bun** commits the `drop` when the message is settled: on ack, on a
  reject without requeue, at delivery for an auto-ack consumer or
  `basic.get`. A message delivered by a leader that dies before the
  consumer acks stays on the followers and the next leader delivers it
  again, with `redelivered` set, as RabbitMQ's quorum queues do. A member
  that forwards a settle to a leader of the other implementation commits
  the `drop` itself too; a drop applies once however often it commits.
- **Rust** does the same: the leader that reports a settle (a local ack or
  reject, a member's forwarded `ack`/`nack`, or a remote noAck `get`)
  commits the `drop`, and an auto-ack delivery commits it before the body
  is written.
- Proposals made in turn (publishes on one channel) are handed to the
  driver in that order, and every member enqueues a committed `enq` when it
  applies, so a queue holds messages in log order. Raft messages to a
  member keep to one socket while it is open, so a burst forwarded to a
  leader is not split across a dialed and an accepted connection.
- A `nack` with requeue needs no entry; the message never left the queue.
- Followers keep their copies outside the ready queue. A member that
  becomes leader moves them into it.
- A consume or `basic.get` that arrives while no connected leader is known
  waits up to two election timeouts for one, instead of failing.

Snapshot `state`, for `snap`: `{"queues":[{"vhost","queue","messages":[<enq data>...]}]}`.

## 7. Voters, timing and persistence

- **Voters** are the cluster members, from the config list, the
  `members.json` runtime list, `QUEUEFORGE_MEMBERS`, or DNS discovery.
  Every group starts with the sorted member ids as its voters. When the
  member list changes, the leader appends `config` entries one voter at a
  time.
- **Membership changes** (`POST /api/nodes`, `DELETE /api/nodes/{name}`, a
  joining node's `join`) commit a `members` entry, the whole new list,
  through the `meta` log when Raft is on, and every member installs it from
  there. Without a majority the change is refused with `503`, as RabbitMQ
  refuses to forget a node without a Khepri majority. The list is also
  pushed, for members without Raft. Rust and Bun both commit it.
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
- `raft` behaves as a RabbitMQ feature flag, as `khepri_db` does:
  - A node whose data directory was new when it started writes `raft/auto`
    and adds `raft_auto` to its features. A cluster whose voters all
    advertise `raft` and `raft_auto` turns the flag on by itself.
  - A node upgraded from a build without Raft has data and no `raft/auto`.
    The cluster keeps the version 1 behaviour (majority-ack quorum queues
    and pushed metadata) until an operator sends
    `PUT /api/feature-flags/raft/enable`. That fails with `400 unsupported`
    while a voter has not advertised `raft`.
  - Enabling sends `{"op":"feature","payload":{"name":"raft"}}` to every
    member, and a node with Raft on adds `raft_on` to its hellos, so a
    member that was down enables it when it reconnects.
  - Once on, `raft/enabled` is written and the flag is never turned off.
    `POST .../disable` answers `400`.
- A member without Raft (PHP today) keeps the whole cluster on version 1.
- The management API lists the flags, with `state: "enabled"` or
  `"disabled"`, at `GET /api/feature-flags`, next to the RabbitMQ 4.3 flags
  whose behaviour the broker has (`quorum_queue`, `stream_queue`,
  `implicit_default_bindings`, `user_limits`).
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
