import { afterAll, expect, test } from "bun:test";
import { connect as tcp, type Socket } from "node:net";
import { channel, eventually, sleep, uniq } from "../lib.ts";

const PORT = Number(process.env.QF_STOMP_PORT ?? 61613);
const WS = process.env.QF_STOMP_WS ?? "";

type Frame = { command: string; headers: Record<string, string>; body: string };

/** A minimal STOMP 1.2 client over TCP or WebSocket. */
class Stomp {
  frames: Frame[] = [];
  private buf = "";
  private constructor(private send_: (s: string) => void, private close_: () => void) {}

  static async tcp(): Promise<Stomp> {
    const sock: Socket = tcp(PORT, "127.0.0.1");
    await new Promise<void>((r, j) => {
      sock.once("connect", () => r());
      sock.once("error", j);
    });
    sock.on("error", () => {});
    const s = new Stomp((x) => sock.write(x), () => sock.destroy());
    sock.on("data", (d) => s.feed(d.toString("utf8")));
    return s;
  }

  static async ws(url: string): Promise<Stomp> {
    const ws = new WebSocket(url, ["v12.stomp"]);
    await new Promise<void>((r, j) => {
      ws.onopen = () => r();
      ws.onerror = () => j(new Error("ws error"));
    });
    const s = new Stomp((x) => ws.send(x), () => ws.close());
    ws.onmessage = (ev) => s.feed(typeof ev.data === "string" ? ev.data : new TextDecoder().decode(ev.data as ArrayBuffer));
    return s;
  }

  private feed(chunk: string) {
    this.buf += chunk;
    for (;;) {
      this.buf = this.buf.replace(/^[\r\n]+/, "");
      const nul = this.buf.indexOf("\0");
      if (nul < 0) return;
      const raw = this.buf.slice(0, nul);
      this.buf = this.buf.slice(nul + 1);
      const split = raw.indexOf("\n\n");
      const head = (split < 0 ? raw : raw.slice(0, split)).split("\n").map((l) => l.replace(/\r$/, ""));
      const headers: Record<string, string> = {};
      for (const line of head.slice(1)) {
        const at = line.indexOf(":");
        if (at > 0 && !(line.slice(0, at) in headers)) headers[line.slice(0, at)] = line.slice(at + 1);
      }
      this.frames.push({ command: head[0] ?? "", headers, body: split < 0 ? "" : raw.slice(split + 2) });
    }
  }

  send(command: string, headers: Record<string, string> = {}, body = "") {
    const lines = Object.entries(headers).map(([k, v]) => `${k}:${v}`);
    this.send_(`${command}\n${lines.join("\n")}${lines.length ? "\n" : ""}\n${body}\0`);
  }

  async next(command: string, ms = 3000, where: (f: Frame) => boolean = () => true): Promise<Frame | null> {
    const deadline = Date.now() + ms;
    for (;;) {
      const at = this.frames.findIndex((f) => f.command === command && where(f));
      if (at >= 0) return this.frames.splice(at, 1)[0]!;
      if (Date.now() > deadline) return null;
      await sleep(15);
    }
  }

  async login(passcode = "devpassword12"): Promise<Frame | null> {
    this.send("CONNECT", { "accept-version": "1.2", host: "/", login: "admin", passcode, "heart-beat": "0,0" });
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
      const f = this.frames.find((x) => x.command === "CONNECTED" || x.command === "ERROR");
      if (f) {
        this.frames.splice(this.frames.indexOf(f), 1);
        return f;
      }
      await sleep(15);
    }
    return null;
  }

  close() {
    this.close_();
  }
}

const clients: Stomp[] = [];
async function stomp(): Promise<Stomp> {
  const s = await Stomp.tcp();
  clients.push(s);
  const f = await s.login();
  if (f?.command !== "CONNECTED") throw new Error(`login: ${f?.command} ${f?.headers.message ?? ""}`);
  return s;
}

afterAll(() => {
  for (const c of clients) c.close();
});

test("STOMP :: a wrong passcode gets an ERROR frame", async () => {
  const s = await Stomp.tcp();
  clients.push(s);
  const f = await s.login("wrong-password");
  expect(f?.command).toBe("ERROR");
});

test("STOMP :: SEND to /queue reaches a subscriber with its headers", async () => {
  const q = uniq("stq");
  const s = await stomp();
  s.send("SUBSCRIBE", { id: "1", destination: `/queue/${q}`, ack: "auto" });
  await sleep(200);
  s.send("SEND", { destination: `/queue/${q}`, "content-type": "text/plain", "x-custom": "yes" }, "hello");
  const m = await s.next("MESSAGE");
  expect(m?.body).toBe("hello");
  expect(m?.headers.subscription).toBe("1");
  expect(m?.headers["x-custom"]).toBe("yes");
});

test("STOMP :: a RECEIPT is sent when a frame asks for one", async () => {
  const s = await stomp();
  s.send("SEND", { destination: `/queue/${uniq("rcpt")}`, receipt: "r-42" }, "x");
  const r = await s.next("RECEIPT");
  expect(r?.headers["receipt-id"]).toBe("r-42");
});

test("STOMP :: ACK settles and NACK redelivers in client-individual mode", async () => {
  const q = uniq("stack");
  const s = await stomp();
  s.send("SUBSCRIBE", { id: "a", destination: `/queue/${q}`, ack: "client-individual", "prefetch-count": "1" });
  await sleep(200);
  s.send("SEND", { destination: `/queue/${q}` }, "retry-me");
  const first = await s.next("MESSAGE");
  expect(first?.body).toBe("retry-me");
  s.send("NACK", { id: first!.headers.ack ?? first!.headers["message-id"]! });
  const again = await s.next("MESSAGE");
  expect(again?.body).toBe("retry-me");
  expect(again?.headers.redelivered).toBe("true");
  s.send("ACK", { id: again!.headers.ack ?? again!.headers["message-id"]!, receipt: "done" });
  expect((await s.next("RECEIPT"))?.headers["receipt-id"]).toBe("done");
  expect(await s.next("MESSAGE", 400)).toBeNull();
});

test("STOMP :: a transaction publishes on COMMIT and discards on ABORT", async () => {
  const q = uniq("sttx");
  const s = await stomp();
  s.send("SUBSCRIBE", { id: "t", destination: `/queue/${q}` });
  await sleep(200);
  s.send("BEGIN", { transaction: "tx1" });
  s.send("SEND", { destination: `/queue/${q}`, transaction: "tx1" }, "kept");
  await sleep(200);
  expect(s.frames.some((f) => f.command === "MESSAGE")).toBe(false);
  s.send("COMMIT", { transaction: "tx1" });
  expect((await s.next("MESSAGE"))?.body).toBe("kept");
  s.send("BEGIN", { transaction: "tx2" });
  s.send("SEND", { destination: `/queue/${q}`, transaction: "tx2" }, "dropped");
  s.send("ABORT", { transaction: "tx2", receipt: "aborted" });
  await s.next("RECEIPT");
  expect(await s.next("MESSAGE", 400)).toBeNull();
});

test("STOMP :: /topic delivers each message to every subscriber", async () => {
  const topic = uniq("sttopic");
  const a = await stomp();
  const b = await stomp();
  a.send("SUBSCRIBE", { id: "1", destination: `/topic/${topic}` });
  b.send("SUBSCRIBE", { id: "1", destination: `/topic/${topic}` });
  await sleep(300);
  const p = await stomp();
  p.send("SEND", { destination: `/topic/${topic}` }, "all");
  expect((await a.next("MESSAGE"))?.body).toBe("all");
  expect((await b.next("MESSAGE"))?.body).toBe("all");
});

test("STOMP :: /exchange/amq.topic/key reaches an AMQP queue", async () => {
  const ch = await channel();
  const q = uniq("st-amqp");
  const key = uniq("k");
  await ch.assertQueue(q, { durable: true });
  await ch.bindQueue(q, "amq.topic", key);
  const s = await stomp();
  s.send("SEND", { destination: `/exchange/amq.topic/${key}` }, "from-stomp");
  let got: string | null = null;
  await eventually(async () => {
    const m = await ch.get(q, { noAck: true });
    if (m) got = m.content.toString();
    return got !== null;
  }, 3000);
  expect(got).toBe("from-stomp");
  await ch.deleteQueue(q);
});

test("WebSockets :: STOMP over WebSocket delivers a message", async () => {
  expect(WS).not.toBe("");
  const s = await Stomp.ws(WS);
  clients.push(s);
  expect((await s.login())?.command).toBe("CONNECTED");
  const q = uniq("stws");
  s.send("SUBSCRIBE", { id: "w", destination: `/queue/${q}` });
  await sleep(200);
  s.send("SEND", { destination: `/queue/${q}` }, "over-ws");
  expect((await s.next("MESSAGE"))?.body).toBe("over-ws");
});
