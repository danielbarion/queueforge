/**
 * connection class methods: start-ok, tune-ok, open, and close.
 *
 * Owns the authenticated user, the open vhost, the heartbeat timer, and the
 * management connection id created at open.
 */
import { method, methodFrame, R, readTable } from "../codec.ts";
import { heartbeatFrame } from "../codec.ts";
import { Conn } from "./listen.ts";

/**
 * Check PLAIN credentials from connection.start-ok and answer with tune.
 *
 * @param payload Method payload. The client table is skipped; mechanism and
 * response follow it. Only PLAIN is accepted. A bad login closes the connection.
 */
export async function handleStartOk(this: Conn, payload: Uint8Array) {
  const rr = new R(payload.subarray(4));
  readTable(rr);
  const mechanism = rr.shortstr();
  const response = rr.longstr();
  if (mechanism !== "PLAIN") return this.connClose(403, "ACCESS_REFUSED - mechanism");
  const parts = new TextDecoder().decode(response).split("\0");
  const user = parts.length >= 3 ? parts[1]! : parts[0] ?? "";
  const pass = parts.length >= 3 ? parts[2]! : parts[1] ?? "";
  if (!(await this.broker.verify(user, pass))) return this.connClose(403, "ACCESS_REFUSED - login");
  this.user = user;
  await this.send(methodFrame(0, method(10, 30, (w) => {
    w.u16(2047);
    w.u32(131072);
    w.u16(60);
  })));
}

/**
 * Record the heartbeat from connection.tune-ok and start the server timer.
 *
 * @param payload Method payload. Channel-max and frame-max are read and ignored.
 * A heartbeat of 0 disables the timer. Otherwise a heartbeat frame is sent
 * at half the negotiated interval, and never faster than once a second.
 */
export function handleTuneOk(this: Conn, payload: Uint8Array) {
  const rr = new R(payload.subarray(4));
  rr.u16();
  rr.u32();
  this.heartbeat = rr.u16();
  if (this.heartbeat > 0) {
    this.timer = setInterval(() => void this.send(heartbeatFrame()), Math.max(1000, (this.heartbeat * 1000) / 2));
  }
}

/**
 * Open the vhost from connection.open and register the management connection.
 *
 * @param payload Method payload. An empty vhost means `/`.
 * Access or connection-limit failures close the connection. The management
 * close callback forces this connection shut. Metrics are incremented once.
 */
export async function handleConnectionOpen(this: Conn, payload: Uint8Array) {
  const rr = new R(payload.subarray(4));
  const vhost = rr.shortstr() || "/";
  if (!this.broker.hasVhostAccess(this.user, vhost)) return this.connClose(403, `ACCESS_REFUSED - vhost ${vhost}`);
  if (!this.broker.connectionAllowed(this.user, vhost)) return this.connClose(403, "ACCESS_REFUSED - connection limit");
  this.vhost = vhost;
  if (!this.metricsOpened) {
    this.metricsOpened = true;
    this.broker.prom.connections++;
    this.broker.prom.connectionsOpened++;
    this.mgmtName = this.broker.openMgmtConnection({
      user: this.user,
      vhost,
      peerHost: this.socket.remoteAddress ?? "",
      peerPort: this.socket.remotePort ?? 0,
      close: () => {
        void this.connClose(320, "CONNECTION_FORCED - closed by management");
      },
    });
  }
  await this.send(methodFrame(0, method(10, 41, (w) => w.shortstr(""))));
}

/**
 * Answer connection.close and drop the socket.
 *
 * The peer asked to close. No further methods are read after the reply.
 */
export async function handleConnectionClose(this: Conn) {
  await this.send(methodFrame(0, method(10, 51, () => {})));
  this.connClosed = true;
  this.socket.end();
}

/**
 * Send connection.close and shut the socket down.
 *
 * @param code AMQP reply code, such as 403 or 541.
 * @param text Reply text. It is truncated to 200 bytes on the wire.
 * Calling this twice still sends one close; the connection is marked closed
 * before the write. The frame is written and the socket is ended before this
 * returns, so the peer observes the TCP close.
 */
export async function connClose(this: Conn, code: number, text: string) {
  if (this.connClosed) return;
  this.connClosed = true;
  this.closed = true;
  this.noteMetricsClosed();
  const bytes = new TextEncoder().encode(text);
  const reply = bytes.length > 200 ? bytes.subarray(0, 200) : bytes;
  const frame = methodFrame(
    0,
    method(10, 50, (w) => {
      w.u16(code);
      w.u8(reply.length);
      w.bytes(reply);
      w.u16(0);
      w.u16(0);
    }),
  );
  // The close frame and the FIN go out before this stack yields. A socket.end()
  // deferred to a timer never ran, so the peer kept the TCP connection open.
  this.staged.push(frame);
  this.stagedBytes += frame.length;
  try {
    this.flush();
    this.socket.flush?.();
    this.socket.end();
  } catch {
    try {
      this.socket.end();
    } catch {
      /* this connection is already gone */
    }
  }
}

/**
 * Count this connection closed in metrics, once.
 *
 * A connection that never opened is a no-op. The management row is removed
 * and the live channel gauge drops by the channels still on this connection.
 */
export function noteMetricsClosed(this: Conn) {
  if (this.metricsClosed || !this.metricsOpened) return;
  this.metricsClosed = true;
  if (this.mgmtName) {
    this.broker.forgetMgmtConnection(this.mgmtName);
    this.mgmtName = "";
  }
  this.broker.prom.connections = Math.max(0, this.broker.prom.connections - 1);
  this.broker.prom.connectionsClosed++;
  this.broker.prom.channels = Math.max(0, this.broker.prom.channels - this.channels.size);
  this.broker.prom.channelsClosed += this.channels.size;
}

Conn.prototype.handleStartOk = handleStartOk;
Conn.prototype.handleTuneOk = handleTuneOk;
Conn.prototype.handleConnectionOpen = handleConnectionOpen;
Conn.prototype.handleConnectionClose = handleConnectionClose;
Conn.prototype.connClose = connClose;
Conn.prototype.noteMetricsClosed = noteMetricsClosed;
