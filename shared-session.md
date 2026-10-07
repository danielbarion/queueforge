# Shared session: PHP multi-core queue homes

Written 2026-10-07 for a separate session. Scope: `php/` only. Do not edit
`bun/`, `rust/` or `site/` for this.

## The bug

With more than one CPU, the PHP broker runs one child process per core
(`php/src/Supervise.php`). The parent accepts each AMQP connection and hands it
to the next child round-robin (`Supervise.php:92`, `$ids[$next % count($ids)]`).
Every child is started with `QUEUEFORGE_LOCAL=1` (`Supervise.php:44`), which
makes `Broker::remoteHomeOf()` return null (`php/src/Broker.php:993`). So no
child forwards anything, and every child keeps its own private copy of every
classic queue.

Effects seen in `BENCHMARK.md`, section "PHP cores (2026-10-07T03:47:05Z)":

- **One publisher, one consumer, more than 1 CPU: nothing delivered.** The
  publisher lands on child n0, the consumer on n1. n0 confirms and stores; n1's
  copy of the queue stays empty. Cells are `ok=0 err=no_consume`, consume/s 0.0.
- **16 connections on "one" queue is really N queues.** 378k/s at 4 CPU is four
  independent copies of `q0`, one per child. FIFO across the queue, a single
  `message_count`, and single-active-consumer do not hold.
- **16 queues only works by luck.** The benchmark opens all 16 publishers
  before all 16 consumers, so publisher i and consumer i both land on child
  `i % cores`. Any other connection order breaks it.

The class comment in `Supervise.php:4-10` already describes the intended
design ("A child that does not own the queue hands the socket on, once, to the
child that does"). That part was never wired.

## What already exists

- **Parent relay**: `Supervise::pump()` (`Supervise.php:110-155`) receives
  `{type: "migrate", home, state, bytes}` plus the socket fd from a child and
  re-sends it to `$kids[$home]` as `{type: "conn", state, bytes}`.
- **Receiving child**: `Server::pullHandoff()` (`php/src/Server.php:223-243`)
  base64-decodes `bytes` and calls `Server::take($fp, $state, $raw)`
  (`Server.php:193-221`), which rebuilds user, vhost and channels
  (`confirm`, `prefetch`), puts the bytes back in `$sock->in`, and replays them
  through `onData`.
- **Queue name parser**: `Handoff::namedQueue($buf, $at)`
  (`php/src/Handoff.php:106-133`) returns the queue named by a `basic.publish`
  on the default exchange (60.40) or a `basic.consume` (60.20), else null. It is
  never called.
- **Home hash**: `Broker::home($queue)` (`Broker.php:1787`) via
  `Features::home()` (`php/src/Features.php:181-192`), fnv1a over the sorted
  member ids, the same hash Bun uses.
- **Fd passing**: `Handoff::send($msg, $tcp)` (`Handoff.php:43-69`) sends JSON
  plus the fd over `SCM_RIGHTS`.

## What is missing: the sender in the child

Nothing in `php/` sends `'type' => 'migrate'` (`grep -rn migrate php/` finds
only the parent). Add it, following Bun's `tryMigrate`
(`bun/src/amqp/frames.ts:353-396`):

1. In `Server::onData()` (`Server.php:339`), in the frame loop starting at
   `Server.php:376`, before a method frame is dispatched: if this child runs
   under the supervisor (`$this->handoff !== null`), the connection has not
   migrated yet, and `Handoff::namedQueue($sock->in, $sock->inAt)` returns a
   queue name:
   - compute `$home = $this->broker->home($queue)`;
   - if `$home` is this child's node id, mark the connection as settled (no
     second move) and continue normally;
   - otherwise send
     `['type' => 'migrate', 'home' => $home, 'state' => [user, vhost, channels[{id, confirm, prefetch}]], 'bytes' => base64_encode(substr($sock->in, $sock->inAt))]`
     with the socket via `$this->handoff->send(...)`, then forget the
     connection locally **without** writing to or closing the TCP socket
     (remove it from `$this->conns` / `$this->byFp`, decrement
     `prom['connections']`, and `fclose` only the local stream resource after
     the send succeeded). The naming frame must stay unprocessed so the home
     replays it.
2. One move per connection, like Bun (`migrated` flag). A connection that
   later names a different queue stays where it is. For that case keep
   forwarding by turning `QUEUEFORGE_LOCAL` off for multi-core children, or
   limit `QUEUEFORGE_LOCAL` to "do not forward a queue this child already
   moved to". Bun keeps cluster forwarding as the fallback; match it.
3. The handoff must happen before anything on that connection is acked or
   stored. Channel opens and `confirm.select` before the first publish are fine:
   they travel in `state`.
4. `bytes` must be base64 because the receiver decodes it
   (`Server.php:237`) and JSON cannot carry raw binary.
5. Also check `Supervise.php:96`: the parent `fclose`s its copy of the client
   after the send. The child must do the same with its copy after a
   successful migrate, or the fd leaks.

Edge cases to cover:

- `basic.publish` to a non-default exchange: `namedQueue` returns null, the
  connection stays. Routing to a queue homed elsewhere then needs the cluster
  forward path, which is why `QUEUEFORGE_LOCAL=1` cannot simply stay on.
- `queue.declare` arrives before the first publish/consume. It runs on the
  first child. The declare must be visible to the home (the children already
  form a cluster through `QUEUEFORGE_MEMBERS`), or be replayed there.
- Quorum queues are served everywhere (`Broker.php:990`); never migrate for
  them.
- AMQP 1.0 connections (`stage === 'amqp10'`) do not migrate.

## How to verify

1. Unit: extend `php/test/cores.test.php` or add a test where one publisher
   and one consumer of the same queue connect to a 2-child supervisor in both
   orders (consumer first, publisher first) and the consumer receives every
   message.
2. Existing checks in `php/README.md` ("Checks") still pass.
3. Re-run the load cells with the same client, shapes, sizes and pins as
   `BENCHMARK.md` "PHP cores": the three `1 conn` cells at 2 and 4 CPU must be
   `ok=1` with consume/s equal to confirm/s.
4. Expect the `16 conn` (one queue) cells to drop to about one child's rate
   (the 1 CPU rows are about 89k–99k), because one queue now lives on one
   process. That is the correct number; the 210k/378k rows were N separate
   queues.

## After the fix

- Update `BENCHMARK.md` "PHP cores" with the new image digest and cells.
- In `site/app/bench.ts`, replace the PHP `one: null` values and remove the
  `caveats` entries on the PHP rows at 2 CPU / 2 GiB, 4 CPU / 4 GiB and
  4 CPU / 8 GiB. `site/app/benchmark/page.tsx` footnotes a and b can then go.
- `site/app/features.ts`: re-check the PHP cells on clustering / queue homes.
