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

## Measuring scaling: growing load and the host ceiling

`bench/scaling.py` runs a scenario at 1 to 6 broker CPUs in a 15-CPU VM, with
the client on the same eight CPUs at every size. It records the client's CPU
and flags points where the client, not the broker, set the rate. It fits the
Universal Scalability Law, X(N) = X(1) N / (1 + s(N-1) + k N(N-1)), where s is
work that serializes and k is work that grows with every pair of cores.

The PerfTest scenarios are the wrong shape for this. Their load is fixed: in
work-queue, four publishers keep 100 messages each unconfirmed. Past 2 CPUs
the broker idles on that window (Bun at 3 CPUs used 62%), so the curve
measures fsync latency and zig-zags for both brokers. A server with more
cores serves more tenants, so the curve that predicts it grows the load with
N. `weak` adds, per broker CPU, 4 classic durable queues with 4 publishers
(512 unconfirmed each) and 4 consumers, as two `bench/loadgen` processes. A
single-threaded loadgen tops out near 300,000 msg/s. `weak-t` is the same
with transient messages, so no fsync sits in the confirm loop and the curve
shows CPU scaling alone. Every point in these runs had its broker at 95% or
more of its CPUs and its client under 20% of its eight.

`bench/ceiling.py` gives the reference curve. It runs K independent 1-CPU
brokers that share nothing but the host. Their efficiency at 6 is 0.71
(2.00 to 2.82 us per message), and that loss belongs to the VM and the Mac
(network path, memory bandwidth, efficiency cores), not to any broker. A
broker's multi-process overhead is the gap between its curve and this one.

## B14: one GC marker per worker

At 6 CPUs, Bun's `weak-t` reached 1,785,342 msg/s against the host
ceiling's 2,109,003. Per-thread CPU showed each worker's main thread at
~74% of a core. Each worker also ran five JavaScriptCore `HeapHelper`
threads (parallel GC marking) at 5-7% of a core apiece, about 30% of a core
per worker. JavaScriptCore sizes that pool from the machine's cores, not the
cgroup's, so N workers each brought up to N - 1 helpers onto the same N
CPUs. Their marking was parallel within one heap and contended across
heaps.

`BUN_JSC_numberOfGCMarkers=1` (no helpers) is now the default for workers:
the supervisor sets it for every worker unless the environment sets its own
(`workerBaseEnv`). A one-process broker keeps JavaScriptCore's default, since
it has no sibling heaps to contend with. A 1 CPU A/B on large messages first
showed -4% on `large-1m` with one marker, but the same image moved as much by
its position in the run order (1,099.8 first, 1,058.2 third), so that was
noise, and keeping the default there is the cautious choice. A/B, the same
image with and without it (`weak-t`):

| CPUs | default | one marker |
| ---: | ---: | ---: |
| 1 | 479,083 | 488,653–496,387 |
| 2 | 771,326 | 773,219–832,728 |
| 4 | 1,291,016 | 1,434,030–1,437,280 |
| 6 | 1,785,342 | 2,056,610–2,062,166 |

Two markers (one helper) gave 1,854,846–1,872,226 at 6. Peak anonymous
memory rose 2–10% (449 to 478 MiB at 6 workers). The gain grows with the
worker count, so it is larger on bigger machines.

## B15: classic queues balanced over workers

A new classic queue's home was the name hash over the members (`queueHome`),
the rule every implementation in a mixed cluster shares. With 4 queues per
worker the busiest worker held 5 of 20 at 5 workers, 6 of 64 at 16, and 9 of
128 at 32. Throughput then follows the busiest worker, an efficiency cap of
0.80, 0.67 and 0.44 (`bun/scripts/home-spread.ts`).

When every member is a worker this broker's supervisor started
(`workerPool`: a child with a broker id and members `n0`..`nN-1`, fixed at
start and the same on every worker), the placement worker `n0` assigns
homes (`assignHome`, cluster op `place_queue`):

- a known queue keeps its home;
- a name already handed out gets the same answer for 60 s, so concurrent
  declares on two workers agree;
- otherwise the worker with the fewest classic queues, counting pending ones,
  ties to the lowest id.

Explicit `x-queue-leader-locator`, exclusive queues, quorum queues (B3, B10)
and streams keep their rules. The wait on `n0` sits between the declare's
existence check and the insert, so the declare checks again after it: a queue
that another declare made meanwhile is answered as existing, never replaced. If `n0` cannot be reached the hash decides, as
before. Homes are stored with the queue, so a restart keeps them. The one
remaining hash use, `tryMigrate` for a name this worker does not know yet, only
picks where a connection moves first; the publish is forwarded to the real
home. Test: `test/cores-placement.test.ts` (16 queues declared racing from 4
workers land 4/4/4/4; the hash gives 3/5/5/3).

## After B14 and B15 (2026-10-15)

`bench/results/scaling-2026-10-15/`, three repeats per point, medians:

| CPUs | weak before | weak after | weak-t before | weak-t after |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 302,917 | 305,282 | 479,083 | 494,416 |
| 2 | 520,621 | 534,949 | 771,326 | 823,374 |
| 3 | 704,589 | 753,541 | 1,051,266 | 1,120,288 |
| 4 | 938,867 | 1,017,875 | 1,291,016 | 1,436,203 |
| 5 | 1,081,373 | 1,228,382 | 1,590,865 | 1,761,050 |
| 6 | 1,306,698 | 1,412,531 | 1,785,342 | 2,031,339 |

The fitted contention s fell from 0.094 to 0.066 (`weak`) and from 0.136 to
0.107 (`weak-t`); k is 0 in all four fits. At 6 CPUs `weak-t` is 96% of the
host ceiling. Rust in the same runs: `weak` 498,464 and `weak-t` 560,500 at
6 (s = 0.29 and 0.23). The extrapolations to 32 or 128 cores in `scaling.md`
are the model's, not measurements: a larger machine adds NUMA, network-card
queues and per-worker coordination (each worker is a cluster member with a
link to every other, and a voter in the metadata group). Eight cores cannot
show those, so confirm on a large host before relying on the projection.

## Per-message work (2026-10-15, after B14-B16)

With the multi-worker overhead near the host ceiling, the remaining cost is
per message, and it multiplies over every core. Profiles of one worker under
`weak-t` led to five changes, each A/B'd against the one before
(1 CPU `weak-t`, three interleaved repeats):

| Change | Before | After |
| --- | ---: | ---: |
| Names read with one Latin-1 Buffer per socket read; a whole body frame copied once from the read buffer; back-pressure destinations computed only when asked | 464,803–502,902 | 551,061–573,683 |
| A channel's property block reused when the next publish repeats it | 566,944–576,085 | 605,026–607,384 |
| One `Deferred` object per publish instead of eight closures | 604,475–609,979 | 629,211–646,632 |
| Short delivery fields and frames copied byte by byte (`set` costs ~10 ns at any length) | 642,096–651,517 | 668,702–675,376 |

Tried and not kept: reading `basic.publish` and `basic.ack` in place in the
read buffer, without a view (669,000–694,000 against 673,000–684,000, inside
the noise). A loop of `String.fromCharCode` for names was faster for 5
characters but slower than `TextDecoder` for PerfTest's 36-character keys
(work-queue -6%, telemetry -9% at 1 CPU), so names use a Latin-1 Buffer read.

1 CPU PerfTest against the B14 image: telemetry +21%, routing-64 +9%,
work-queue +7%, priority +7%, quorum +5%, fanout-20 +3%; large-64k, large-1m
and backlog unchanged.

Scaling curve with all of it (`bench/results/scaling-2026-10-15/weak-final/`):

| CPUs | weak | weak-t |
| ---: | ---: | ---: |
| 1 | 364,658 | 655,838 |
| 2 | 639,914 | 1,090,534 |
| 3 | 896,435 | 1,469,832 |
| 4 | 1,228,758 | 1,892,536 |
| 5 | 1,441,333 | 2,302,406 |
| 6 | 1,685,293 | 2,653,190 |

Efficiency at 6 is 0.77 and 0.67, as before the per-message work, so the
cheaper message did not cost scaling. s = 0.067 and 0.112.
