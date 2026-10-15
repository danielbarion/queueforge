# Bun queue ownership and scaling

Design notes for `PENDING.md` B3 and B4, 2026-10-09, and B10, 2026-10-15. These options preserve the
distinction between one physical broker and several independent replica nodes.
This document is not a benchmark result; measured numbers are in `PENDING.md`
and `BENCHMARK.md`.

## B3: quorum groups on sibling workers

The supervisor creates one process and data directory per granted core.
Ordinary forwarded publishes use cluster TCP. Today every queue group inherits
the cluster voter list, and `RaftNode.round()` reconfigures every leader toward
that same list. Changing only the group's initial voters would therefore be
temporary and could create conflicting membership after a restart.

Use an explicit, persisted physical-broker identity and worker membership
record created by the supervisor. Independent cluster configuration must never
gain this identity by guessing from an address, process ID or node name.
Advertise support through a new capability; create local-worker groups only
when every member belongs to that one recorded physical broker and supports
the membership format. Mixed and independent-node clusters retain the current
majority behavior.

For each new local-worker queue group, persist its scope, owner, initial voters
and physical identity with queue metadata and the group's identity file.
Keep its desired voters separate from the cluster-wide desired voters. Metadata
continues to use the cluster's existing consensus group. Existing queue groups
retain their existing membership; switching a stored multi-voter group to a
singleton requires an explicit committed membership transition.

The sole queue owner must fsync before confirming. A worker crash makes that
queue unavailable until the same owner restarts and replays its existing data;
another worker must not elect itself or silently create an empty queue. A
physical broker loss remains a physical broker loss. Changing the number of
workers must preserve existing owners and directories, or fail before serving
traffic with an actionable configuration error. A queue's single owner is not
an extra replica or an independent failure domain.

Required proof before rollout: supervisor identity survives restart; old groups
keep voters; genuine three-node partitions retain majority requirements;
worker kill/restart retains confirmed bodies and ownership; membership updates
cannot broaden or collapse pinned groups accidentally; snapshots replace the
correct group's state; streams and mixed Rust/Bun clusters retain their
current contracts. Compare one and four cores only after B1 and the multicore
reliability gates pass. This design does not authorize bypassing those gates.

## B4: a single queue on several cores

| Option | Ordering and delivery contract | Recovery and compatibility | Recommendation |
| --- | --- | --- | --- |
| Keep one queue actor; distribute independent queues | The actor owns enqueue order, priority, TTL, single-active-consumer selection, prefetch and settlement. A single queue remains limited by that actor. | Existing homes, logs and protocols remain usable. Worker restart restores the same owner. | Keep as the default and describe multi-queue scaling accurately. |
| Move bounded storage or encoding work around one actor | Assign sequence numbers before dispatch. Commit and confirm only after covering durability; merge results in sequence. Keep priority selection, consumer credit and settlement at the actor. Slow tasks must not reorder delivery or allow unbounded work. | Requires a bounded worker protocol, replayable sequence/commit state and tests for worker failure between write and completion. Can retain the AMQP API if observable ordering and durability remain unchanged. | Profile payload encoding, log/fsync and scheduling first. Move only a measured dominant cost; do not add a pool speculatively. |
| Introduce partitioned queues explicitly | Ordering is per partition. Global FIFO and global priority require a coordinator and can recover the original bottleneck. Single-active-consumer must have a clearly defined partition scope. Prefetch and requeue must carry partition identity. | Requires a new opt-in API, persisted partition map, ownership epochs, restart/failover rules and client-visible semantics. Existing queues cannot be silently converted. | A separate product feature requiring an agreed contract and measured benefit. |

The current frame and storage paths contain several copies and synchronous
durability work, but source inspection alone does not establish which limits a
single queue. B5/B7 measurements should decide whether work can move safely
off the actor. Preserve AMQP ordering and confirm semantics in all comparisons;
multi-queue throughput is not evidence of single-queue scalability.

## B10: store-only messages in a one-voter quorum group

On sibling workers (B3) a quorum queue's group has one voter, its owner. Each
persistent publish still appended a Raft `enq` entry, a JSON head and the
body, CRC'd and fsynced to the group's `log.bin`, and also wrote the body to
the queue's store. Each ack proposed a `drop` entry. With one voter, a
majority is the owner's own disk, and the store row, fsynced before the
confirm, already is that copy.

A persistent publish to a queue whose group's only voter is this process
(`soloLocal`) therefore skips the log. The body is stored with a `ql-` id
(`STORE_ONLY_PREFIX`), held from delivery until the store's fsync
(`store.whenDurable`), and confirmed then, as a classic durable queue. Its
settle deletes the store row and proposes nothing (`quorumDrop`). A
transient publish has no store row, so it still commits through the log.

Rules that keep this safe:

- Scope is the group's voter list, which the solo group's name fixes. Groups
  with more than one voter, the shared `quorum` group and mixed Rust or PHP
  clusters never take this path, and a solo group's membership cannot change.
- Bodies committed through the log before the upgrade keep `q-` ids. Their
  drops still go through the log, so a replay cannot bring back an acked one.
- Recovery reads `ql-` bodies from the store, as it reads every quorum body,
  and `promoteIfLeader` makes them ready. The log replay skips them.
- A snapshot covers only log-committed bodies. Its install, which runs on
  every start of a group that has one, keeps `ql-` bodies and their rows and
  puts them back after the snapshot's messages. Before this rule a restart
  after any snapshot dropped every store-only body
  (`test/quorum-solo-store.test.ts` fails without it).
- Purge and queue deletion already delete store rows.
- Order is publish order for one kind of message. During an upgrade, bodies
  replayed from an old snapshot come before newer store-only ones.

Measured 2026-10-15, broker on 2 CPUs and client on 2 others, 4-CPU VM, three
interleaved pairs (medians): quorum 88,134 to 159,557 msg/s, p99 12.75 to
10.12 ms; work-queue 194,225 to 201,493, unchanged within noise.

Proof: `test/quorum-solo-store.test.ts` (kill -9 of every worker after a group
snapshot keeps exactly the confirmed, unacked bodies, in order) and
`test/quorum-cores.test.ts`.
