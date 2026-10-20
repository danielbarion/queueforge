# QueueForge console

Run `bun install` and `bun run dev` in this directory. The console opens at
http://127.0.0.1:3200. Run `bun test` and `bun run build` to validate it.
Keep the repository checkout intact: the comparison view imports the website's
capability and benchmark data from `../site/app`, using the repository as the
Turbopack root.

Set `DATABASE_URL`, `CONSOLE_USER`, and `CONSOLE_PASSWORD` before starting.
The password must be at least 12 characters. The first start creates that
operator in Postgres when the operators table is empty. Sessions, saved
brokers, and broker logins live in that database. `CONSOLE_DATA_KEY` encrypts
the broker credentials. Without `DATABASE_URL` every page redirects to the
operator login and the APIs answer 503. Logging in to a broker is a second
step, after this one.

`CONSOLE_REGISTRATION` opens self-service registration. `1`, `true`, `yes`, or
`on` shows **Create an account** on the login page and accepts
`POST /api/operator/register`. Any other value, including leaving it unset,
keeps registration closed: the link stays hidden and the route answers 403.
A new operator needs a password of at least 8 characters and is logged in
immediately. The account starts with no saved brokers.

## Explore

Choose **Try interactive demo** on Overview or Brokers. Demo mode uses local,
read-only fixtures, blocks calls to configured brokers, and retains saved
profiles. Exit demo to restore the selected real broker. Throughput in demo mode
is simulated; normal mode records up to five minutes of polling-derived rates.
Missing counters, counter resets and stale intervals leave gaps.

Use **Ctrl+K / Command+K** or the toolbar search to navigate to a page, queue or
broker. Queues supports name, type and message-state filters and sorting.
Fleet probes saved profiles independently without changing the selected broker;
queue counts are scoped to `/`, resources are labeled by their reported scope,
and unavailable values remain unknown.

Compare uses the website's saved feature audit and benchmark methodology.
`lib/reference-conformance.json` is a bundled snapshot of
`conformance/results/summary.json`; the original summary contains no run date or
revision. These results describe historical tests, not a live capability probe.
To update that snapshot, copy a reviewed summary into it after a conformance run.

## Inspect and replay

Inspection explicitly fetches messages and requests requeue. It can affect
ordering, redelivery and delivery limits. It never runs automatically on opening
the drawer. Streams do not support this basic.get workflow. Rust can return the
same head message more than once in a batch; PHP may omit properties or lose
binary encoding in its management response.

The dead-letter workspace discovers configured destination candidates from
source queue arguments, applied policies and bindings. A matching destination
is not proof that every message there was dead-lettered. Inspect the returned
x-death headers to see recorded reasons and history, or select a queue manually
when metadata is unavailable. Export saves local snapshots or original payload
bytes without an additional fetch.

Replay publishes a **copy** of a saved snapshot, retaining the queued original.
It requires explicit confirmation and is enabled only for complete RabbitMQ
snapshots with returned properties. Rust omits management headers, Bun's publish
API strips properties, and PHP omits inspection properties, so faithful replay
is disabled there. Saved headers and expiration are preserved. Routing success
does not confirm consumer processing; check an ambiguous outcome before retrying.
