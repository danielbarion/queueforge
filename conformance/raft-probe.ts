import amqp from "amqplib";
// Publish N quorum messages with confirms through `port`, then consume them all.
const [port, n, inflight] = [Number(process.argv[2] ?? 37100), Number(process.argv[3] ?? 200), Number(process.argv[4] ?? 1)];
const c = await amqp.connect(`amqp://admin:devpassword12@127.0.0.1:${port}`);
const ch = await c.createConfirmChannel();
const q = process.argv[5] ?? "rq";
await ch.assertQueue(q, { durable: true, arguments: { "x-queue-type": "quorum" } });
const lat: number[] = [];
let sent = 0;
const t0 = performance.now();
async function one(i: number) {
  const s = performance.now();
  await new Promise<void>((res, rej) => ch.publish("", q, Buffer.from(`m${i}`), { persistent: true }, (err) => (err ? rej(err) : res())));
  lat.push(performance.now() - s);
}
const workers = Array.from({ length: inflight }, async () => {
  while (sent < n) await one(sent++);
});
await Promise.all(workers);
const dt = performance.now() - t0;
lat.sort((a, b) => a - b);
console.log(`confirmed ${lat.length} in ${dt.toFixed(0)} ms, ${(lat.length / dt * 1000).toFixed(0)}/s, p50 ${lat[lat.length >> 1].toFixed(1)} ms p99 ${lat[Math.floor(lat.length * 0.99)].toFixed(1)} ms`);
const got = new Set<string>();
await ch.prefetch(256);
await new Promise<void>((res) => {
  ch.consume(q, (m) => { got.add(m!.content.toString()); ch.ack(m!); if (got.size >= n) res(); });
  setTimeout(res, 5000);
});
console.log(`consumed ${got.size} distinct`);
await c.close();
