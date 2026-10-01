/**
 * STOMP 1.2 listener.
 *
 * SEND stores the body on a queue derived from the destination and fans it out
 * to subscribers of that destination.
 */
import type { Broker } from "../broker/index.ts";
import { pull, push } from "./queue.ts";

type StompSub = { destination: string; id: string; write: (frame: string) => void };
const stompHub: StompSub[] = [];

function stompQueue(dest: string): string {
  return dest.replace(/^\/queue\//, "").replace(/^\/topic\//, "");
}

function stompFanout(dest: string, body: string) {
  const queue = stompQueue(dest);
  for (const sub of stompHub) {
    if (sub.destination === dest || stompQueue(sub.destination) === queue) {
      sub.write(`MESSAGE\nsubscription:${sub.id}\ndestination:${dest}\ncontent-length:${body.length}\n\n${body}\0`);
    }
  }
}
/**
 * Listen for STOMP on `host`:`port`.
 *
 * @param host Bind address passed to Bun.listen.
 * @param port TCP port from config.
 * @param broker Broker that stores SEND bodies. A publish error is logged
 * and does not close the socket.
 * @returns Nothing. The listener stays open until the process exits.
 */
export function startStomp(host: string, port: number, broker: Broker) {
  Bun.listen({
    hostname: host,
    port,
    socket: {
      open(socket) {
        socket.data = { buf: new Uint8Array() };
      },
      async data(socket, data) {
        const state = socket.data as { buf: Uint8Array };
        const merged = new Uint8Array(state.buf.length + data.length);
        merged.set(state.buf);
        merged.set(data, state.buf.length);
        state.buf = merged;
        const text = new TextDecoder().decode(state.buf);
        let nul = text.indexOf("\0");
        let consumed = 0;
        while (nul >= 0) {
          const frame = text.slice(consumed, nul);
          consumed = nul + 1;
          const lines = frame.split("\n").map((l) => l.replace(/\r$/, ""));
          const cmd = lines[0] ?? "";
          const headers = new Map<string, string>();
          let bodyAt = lines.length;
          for (let i = 1; i < lines.length; i++) {
            if (lines[i] === "") {
              bodyAt = i + 1;
              break;
            }
            const sep = lines[i]!.indexOf(":");
            if (sep > 0) headers.set(lines[i]!.slice(0, sep), lines[i]!.slice(sep + 1));
          }
          const body = lines.slice(bodyAt).join("\n");
          if (cmd === "CONNECT" || cmd === "STOMP") socket.write("CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0");
          else if (cmd === "SEND") {
            const dest = headers.get("destination") ?? "";
            const queue = stompQueue(dest);
            const n = Number(headers.get("content-length"));
            const raw = Number.isFinite(n) ? body.slice(0, n) : body;
            stompFanout(dest, raw);
            try {
              await push(broker, queue, new TextEncoder().encode(raw));
            } catch (err) {
              console.error("stomp publish", err);
            }
          } else if (cmd === "SUBSCRIBE") {
            const id = headers.get("id") ?? "0";
            const dest = headers.get("destination") ?? "";
            stompHub.push({
              destination: dest,
              id,
              write: (frame) => {
                try {
                  socket.write(frame);
                } catch {
                  /* subscriber already closed */
                }
              },
            });
            const queued = await pull(broker, stompQueue(dest));
            if (queued) {
              const textBody = new TextDecoder().decode(queued);
              socket.write(`MESSAGE\nsubscription:${id}\ndestination:${dest}\ncontent-length:${textBody.length}\n\n${textBody}\0`);
            }
          } else if (cmd === "DISCONNECT") socket.end();
          nul = text.indexOf("\0", consumed);
        }
        state.buf = new TextEncoder().encode(text.slice(consumed));
      },
    },
  });
}
