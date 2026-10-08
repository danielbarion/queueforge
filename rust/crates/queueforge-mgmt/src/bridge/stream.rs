//! The RabbitMQ stream protocol (port 5552), over a loopback AMQP 0-9-1
//! connection to this broker's stream queues.
//!
//! A stream is the same `x-queue-type: stream` queue AMQP clients use, so
//! every protocol reads every other's messages. Messages on the wire are
//! AMQP 1.0 encoded, as RabbitMQ's are, and mapped with the AMQP 1.0
//! bridge's [`map`](super::amqp10::map).
//!
//! Supported: SASL PLAIN, tune, open, heartbeats, create and delete stream,
//! metadata, declare and delete publisher, publish (v1 and v2) with confirms
//! and deduplication by publisher reference, query publisher sequence,
//! subscribe from first, last, next, an offset or a timestamp, credit,
//! single active consumer (also across super stream partitions),
//! unsubscribe, store and query offset, stream stats, command versions, and
//! super streams: create, delete, partitions and route. Each message is
//! delivered as a chunk of one, so credit counts messages. Consumer offsets
//! and publisher sequences are kept in the metadata store's parameters.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions,
    ExchangeDeclareOptions, ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::{Channel, Connection, ConnectionProperties, ExchangeKind};
use queueforge_core::{HeaderArg, QueueType};
use queueforge_store::MetadataStore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use super::amqp10::{login_ok, map};
use super::amqp_uri;

const OK: u16 = 1;
const STREAM_DOES_NOT_EXIST: u16 = 2;
const SUBSCRIPTION_ID_ALREADY_EXISTS: u16 = 3;
const SUBSCRIPTION_ID_DOES_NOT_EXIST: u16 = 4;
const STREAM_ALREADY_EXISTS: u16 = 5;
const SASL_MECHANISM_NOT_SUPPORTED: u16 = 7;
const AUTHENTICATION_FAILURE: u16 = 8;
const VIRTUAL_HOST_ACCESS_FAILURE: u16 = 12;
const INTERNAL_ERROR: u16 = 15;
const ACCESS_REFUSED: u16 = 16;
const PRECONDITION_FAILED: u16 = 17;
const PUBLISHER_DOES_NOT_EXIST: u16 = 18;
const NO_OFFSET: u16 = 19;

const DECLARE_PUBLISHER: u16 = 1;
const PUBLISH: u16 = 2;
const PUBLISH_CONFIRM: u16 = 3;
const PUBLISH_ERROR: u16 = 4;
const QUERY_PUBLISHER_SEQUENCE: u16 = 5;
const DELETE_PUBLISHER: u16 = 6;
const SUBSCRIBE: u16 = 7;
const DELIVER: u16 = 8;
const CREDIT: u16 = 9;
const STORE_OFFSET: u16 = 10;
const QUERY_OFFSET: u16 = 11;
const UNSUBSCRIBE: u16 = 12;
const CREATE: u16 = 13;
const DELETE: u16 = 14;
const METADATA: u16 = 15;
const METADATA_UPDATE: u16 = 16;
const PEER_PROPERTIES: u16 = 17;
const SASL_HANDSHAKE: u16 = 18;
const SASL_AUTHENTICATE: u16 = 19;
const TUNE: u16 = 20;
const OPEN: u16 = 21;
const CLOSE: u16 = 22;
const HEARTBEAT: u16 = 23;
const ROUTE: u16 = 24;
const PARTITIONS: u16 = 25;
const CONSUMER_UPDATE: u16 = 26;
const COMMAND_VERSIONS: u16 = 27;
const STREAM_STATS: u16 = 28;
const CREATE_SUPER_STREAM: u16 = 29;
const DELETE_SUPER_STREAM: u16 = 30;

/// Commands answered, with the versions spoken.
const VERSIONS: &[(u16, u16, u16)] = &[
    (DECLARE_PUBLISHER, 1, 1),
    (PUBLISH, 1, 2),
    (PUBLISH_CONFIRM, 1, 1),
    (PUBLISH_ERROR, 1, 1),
    (QUERY_PUBLISHER_SEQUENCE, 1, 1),
    (DELETE_PUBLISHER, 1, 1),
    (SUBSCRIBE, 1, 1),
    (DELIVER, 1, 1),
    (CREDIT, 1, 1),
    (STORE_OFFSET, 1, 1),
    (QUERY_OFFSET, 1, 1),
    (UNSUBSCRIBE, 1, 1),
    (CREATE, 1, 1),
    (DELETE, 1, 1),
    (METADATA, 1, 1),
    (METADATA_UPDATE, 1, 1),
    (PEER_PROPERTIES, 1, 1),
    (SASL_HANDSHAKE, 1, 1),
    (SASL_AUTHENTICATE, 1, 1),
    (TUNE, 1, 1),
    (OPEN, 1, 1),
    (CLOSE, 1, 1),
    (HEARTBEAT, 1, 1),
    (ROUTE, 1, 1),
    (PARTITIONS, 1, 1),
    (CONSUMER_UPDATE, 1, 1),
    (COMMAND_VERSIONS, 1, 1),
    (STREAM_STATS, 1, 1),
    (CREATE_SUPER_STREAM, 1, 1),
    (DELETE_SUPER_STREAM, 1, 1),
];

const FRAME_MAX: usize = 1024 * 1024;
const HEARTBEAT_S: u32 = 60;
/// Deliveries a subscription's consumer may hold before the bridge acks.
const PREFETCH: u16 = 512;
const OFFSETS: &str = "stream-offsets";
const SEQUENCES: &str = "stream-publishers";
const PARTITION_ORDER: &str = "x-stream-partition-order";
/// Headers the stream store adds that a stream client does not see.
const STREAM_ONLY: &[&str] = &["x-stream-offset"];

/// What the bridge needs from the broker besides the AMQP port.
pub struct StreamContext {
    /// Metadata store: queue types, bindings, and stored offsets.
    pub store: Arc<MetadataStore>,
    /// Plain AMQP port sessions log in through.
    pub amqp_port: u16,
    /// Host clients are told to connect to.
    pub advertised_host: String,
    /// Port clients are told to connect to.
    pub advertised_port: u16,
    /// Live queues, for stream stats. `None` reports them empty.
    pub queues: Option<Arc<queueforge_core::QueueRegistry>>,
}

struct Rd<'a> {
    b: &'a [u8],
    at: usize,
}

type R<T> = Result<T, ()>;

impl<'a> Rd<'a> {
    fn left(&self) -> usize {
        self.b.len() - self.at
    }
    fn take(&mut self, n: usize) -> R<&'a [u8]> {
        if self.at + n > self.b.len() {
            return Err(());
        }
        let out = &self.b[self.at..self.at + n];
        self.at += n;
        Ok(out)
    }
    fn u8(&mut self) -> R<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> R<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> R<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn i32(&mut self) -> R<i32> {
        Ok(self.u32()? as i32)
    }
    fn u64(&mut self) -> R<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_be_bytes(a))
    }
    fn i64(&mut self) -> R<i64> {
        Ok(self.u64()? as i64)
    }
    fn str(&mut self) -> R<String> {
        let n = self.u16()? as i16;
        if n < 0 {
            return Ok(String::new());
        }
        Ok(String::from_utf8_lossy(self.take(n as usize)?).into_owned())
    }
    fn bytes(&mut self) -> R<&'a [u8]> {
        let n = self.i32()?;
        if n < 0 {
            return Ok(&[]);
        }
        self.take(n as usize)
    }
    fn strings(&mut self) -> R<Vec<String>> {
        let n = self.i32()?.max(0);
        (0..n).map(|_| self.str()).collect()
    }
    fn map(&mut self) -> R<Vec<(String, String)>> {
        let n = self.i32()?.max(0);
        (0..n).map(|_| Ok((self.str()?, self.str()?))).collect()
    }
}

#[derive(Default)]
struct Wr(Vec<u8>);

impl Wr {
    fn u8(&mut self, n: u8) -> &mut Self {
        self.0.push(n);
        self
    }
    fn u16(&mut self, n: u16) -> &mut Self {
        self.0.extend(n.to_be_bytes());
        self
    }
    fn u32(&mut self, n: u32) -> &mut Self {
        self.0.extend(n.to_be_bytes());
        self
    }
    fn i32(&mut self, n: i32) -> &mut Self {
        self.0.extend(n.to_be_bytes());
        self
    }
    fn u64(&mut self, n: u64) -> &mut Self {
        self.0.extend(n.to_be_bytes());
        self
    }
    fn i64(&mut self, n: i64) -> &mut Self {
        self.0.extend(n.to_be_bytes());
        self
    }
    fn str(&mut self, s: &str) -> &mut Self {
        self.u16(s.len() as u16);
        self.0.extend(s.as_bytes());
        self
    }
    fn strings(&mut self, items: &[String]) -> &mut Self {
        self.i32(items.len() as i32);
        for s in items {
            self.str(s);
        }
        self
    }
    fn map(&mut self, items: &[(&str, String)]) -> &mut Self {
        self.i32(items.len() as i32);
        for (k, v) in items {
            self.str(k).str(v);
        }
        self
    }
}

/// One frame: size, key, version, body.
fn frame(key: u16, body: impl FnOnce(&mut Wr)) -> Vec<u8> {
    let mut w = Wr::default();
    w.u32(0).u16(key).u16(1);
    body(&mut w);
    let n = (w.0.len() - 4) as u32;
    w.0[..4].copy_from_slice(&n.to_be_bytes());
    w.0
}

fn response(key: u16, corr: u32, code: u16, rest: impl FnOnce(&mut Wr)) -> Vec<u8> {
    frame(key | 0x8000, |w| {
        w.u32(corr).u16(code);
        rest(w);
    })
}

/// An offset specification as an `x-stream-offset` argument.
fn read_offset(r: &mut Rd) -> R<Option<AMQPValue>> {
    let text = |s: &str| Some(AMQPValue::LongString(LongString::from(s.as_bytes().to_vec())));
    Ok(match r.u16()? {
        1 => text("first"),
        2 => text("last"),
        3 => text("next"),
        4 => Some(AMQPValue::LongLongInt(r.u64()? as i64)),
        5 => Some(AMQPValue::Timestamp((r.i64()? / 1000).max(0) as u64)),
        _ => None,
    })
}

/// Off-socket events for one connection.
enum Ev {
    Delivery { sub: u8, gen: u64, delivery: Delivery },
    Activate { sub: u8, gen: u64 },
    Deactivate { sub: u8, gen: u64 },
}

struct Sub {
    stream: String,
    gen: u64,
    credit: u32,
    spec: AMQPValue,
    ch: Option<Channel>,
    tag: String,
    buffer: VecDeque<Delivery>,
    group: Option<String>,
}

struct Publisher {
    stream: String,
    reference: Option<String>,
    /// Highest publishing id stored for a named publisher.
    last: u64,
}

/// One member of a single-active-consumer group.
struct Member {
    conn: u64,
    sub: u8,
    gen: u64,
    tx: mpsc::UnboundedSender<Ev>,
}

struct Group {
    /// Partition index for a super stream member's stream; 0 otherwise.
    index: usize,
    members: Vec<Member>,
    active: Option<(u64, u8, u64)>,
}

/// Groups shared by every connection: one per vhost, stream and consumer name.
fn groups() -> &'static Mutex<HashMap<String, Group>> {
    static GROUPS: OnceLock<Mutex<HashMap<String, Group>>> = OnceLock::new();
    GROUPS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Make member `index % n` active, telling the old and new active members.
/// On a plain stream `index` is 0, so the oldest member is active.
fn rebalance(key: &str) {
    let Ok(mut all) = groups().lock() else { return };
    let Some(g) = all.get_mut(key) else { return };
    if g.members.is_empty() {
        all.remove(key);
        return;
    }
    let want = &g.members[g.index % g.members.len()];
    let want_id = (want.conn, want.sub, want.gen);
    if g.active == Some(want_id) {
        return;
    }
    if let Some((conn, sub, gen)) = g.active {
        if let Some(old) = g.members.iter().find(|m| m.conn == conn && m.sub == sub && m.gen == gen) {
            let _ = old.tx.send(Ev::Deactivate { sub, gen });
        }
    }
    let _ = want.tx.send(Ev::Activate { sub: want.sub, gen: want.gen });
    g.active = Some(want_id);
}

fn leave_group(key: &str, conn: u64, sub: u8, gen: u64) {
    if let Ok(mut all) = groups().lock() {
        if let Some(g) = all.get_mut(key) {
            g.members.retain(|m| !(m.conn == conn && m.sub == sub && m.gen == gen));
            if g.active == Some((conn, sub, gen)) {
                g.active = None;
            }
        }
    }
    rebalance(key);
}

struct Session {
    ctx: Arc<StreamContext>,
    user: String,
    pass: String,
    vhost: String,
    amqp: Option<Connection>,
    publish: Option<Channel>,
    publishers: HashMap<u8, Publisher>,
    subs: HashMap<u8, Sub>,
    /// Consumer updates sent, waiting for the client's answer.
    pending: HashMap<u32, (u8, u64)>,
    next_corr: u32,
    gen: u64,
    id: u64,
    tx: mpsc::UnboundedSender<Ev>,
    out: Vec<u8>,
    heartbeat: u32,
    done: bool,
}

/// Serve one stream protocol connection.
pub async fn serve<S: AsyncRead + AsyncWrite + Unpin + Send>(mut io: S, ctx: Arc<StreamContext>) {
    static CONN_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut s = Session {
        ctx,
        user: String::new(),
        pass: String::new(),
        vhost: "/".into(),
        amqp: None,
        publish: None,
        publishers: HashMap::new(),
        subs: HashMap::new(),
        pending: HashMap::new(),
        next_corr: 1,
        gen: 0,
        id: CONN_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        tx,
        out: Vec::new(),
        heartbeat: 0,
        done: false,
    };
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut chunk = vec![0u8; 64 * 1024];
    let mut beat = tokio::time::interval(Duration::from_secs(3600));
    beat.tick().await;
    let mut beating = 0u32;
    loop {
        while buf.len() >= 4 {
            let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if size > FRAME_MAX || size < 4 {
                s.done = true;
                break;
            }
            if buf.len() < 4 + size {
                break;
            }
            let body: Vec<u8> = buf[4..4 + size].to_vec();
            buf.drain(..4 + size);
            if s.on_frame(&body).await.is_err() {
                s.done = true;
            }
            if s.done {
                break;
            }
        }
        if s.heartbeat != beating && s.heartbeat > 0 {
            beating = s.heartbeat;
            beat = tokio::time::interval(Duration::from_secs(u64::from(beating)));
            beat.tick().await;
        }
        if !s.out.is_empty() {
            let out = std::mem::take(&mut s.out);
            if io.write_all(&out).await.is_err() {
                break;
            }
        }
        if s.done {
            break;
        }
        tokio::select! {
            n = io.read(&mut chunk) => match n {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            },
            Some(ev) = rx.recv() => s.on_event(ev).await,
            _ = beat.tick(), if beating > 0 => s.out.extend(frame(HEARTBEAT, |_| {})),
        }
        if !s.out.is_empty() {
            let out = std::mem::take(&mut s.out);
            if io.write_all(&out).await.is_err() {
                break;
            }
        }
    }
    s.teardown().await;
}

impl Session {
    fn send(&mut self, bytes: Vec<u8>) {
        self.out.extend(bytes);
    }

    async fn on_frame(&mut self, body: &[u8]) -> R<()> {
        let mut r = Rd { b: body, at: 0 };
        let key = r.u16()?;
        let version = r.u16()?;
        let handshake = matches!(key, PEER_PROPERTIES | SASL_HANDSHAKE | SASL_AUTHENTICATE | TUNE | OPEN | HEARTBEAT | CLOSE);
        if self.amqp.is_none() && !handshake {
            self.done = true;
            return Ok(());
        }
        match key {
            PEER_PROPERTIES => {
                let corr = r.u32()?;
                // `version` is the RabbitMQ stream protocol level answered; clients gate features on it.
                let props = [
                    ("product", "QueueForge".to_string()),
                    ("platform", "Rust".to_string()),
                    ("version", "4.3.0".to_string()),
                    ("information", "RabbitMQ stream protocol".to_string()),
                ];
                self.send(response(key, corr, OK, |w| {
                    w.map(&props);
                }));
            }
            SASL_HANDSHAKE => {
                let corr = r.u32()?;
                self.send(response(key, corr, OK, |w| {
                    w.strings(&["PLAIN".to_string()]);
                }));
            }
            SASL_AUTHENTICATE => {
                let corr = r.u32()?;
                let mechanism = r.str()?;
                let data = r.bytes()?;
                if mechanism != "PLAIN" {
                    self.send(response(key, corr, SASL_MECHANISM_NOT_SUPPORTED, |_| {}));
                    return Ok(());
                }
                let text = String::from_utf8_lossy(data).into_owned();
                let mut parts = text.split('\0');
                let _ = parts.next();
                let user = parts.next().unwrap_or_default().to_string();
                let pass = parts.next().unwrap_or_default().to_string();
                if user.is_empty() || !login_ok(self.ctx.amqp_port, &user, &pass).await {
                    self.send(response(key, corr, AUTHENTICATION_FAILURE, |_| {}));
                    self.done = true;
                    return Ok(());
                }
                self.user = user;
                self.pass = pass;
                self.send(response(key, corr, OK, |_| {}));
                self.send(frame(TUNE, |w| {
                    w.u32(FRAME_MAX as u32).u32(HEARTBEAT_S);
                }));
            }
            TUNE => {
                r.u32()?;
                self.heartbeat = r.u32()?;
            }
            OPEN => {
                let corr = r.u32()?;
                let vhost = r.str()?;
                let uri = amqp_uri(self.ctx.amqp_port, &self.user, &self.pass, &vhost);
                let conn = if self.user.is_empty() { None } else { Connection::connect(&uri, ConnectionProperties::default()).await.ok() };
                let Some(conn) = conn else {
                    self.send(response(key, corr, VIRTUAL_HOST_ACCESS_FAILURE, |_| {}));
                    self.done = true;
                    return Ok(());
                };
                self.publish = confirm_channel(&conn).await;
                self.amqp = Some(conn);
                self.vhost = vhost;
                let props = [("advertised_host", self.ctx.advertised_host.clone()), ("advertised_port", self.ctx.advertised_port.to_string())];
                self.send(response(key, corr, OK, |w| {
                    w.map(&props);
                }));
            }
            CLOSE => {
                let corr = r.u32()?;
                self.send(response(key, corr, OK, |_| {}));
                self.done = true;
            }
            HEARTBEAT => {}
            COMMAND_VERSIONS => {
                let corr = r.u32()?;
                self.send(response(key, corr, OK, |w| {
                    w.i32(VERSIONS.len() as i32);
                    for (k, min, max) in VERSIONS {
                        w.u16(*k).u16(*min).u16(*max);
                    }
                }));
            }
            CREATE => {
                let corr = r.u32()?;
                let stream = r.str()?;
                let args = r.map()?;
                let code = self.create_stream(&stream, &args).await;
                self.send(response(key, corr, code, |_| {}));
            }
            DELETE => {
                let corr = r.u32()?;
                let stream = r.str()?;
                let code = self.delete_stream(&stream).await;
                self.send(response(key, corr, code, |_| {}));
            }
            METADATA => {
                let corr = r.u32()?;
                let streams = r.strings()?;
                let exists: Vec<bool> = streams.iter().map(|s| self.is_stream(s)).collect();
                let (host, port) = (self.ctx.advertised_host.clone(), u32::from(self.ctx.advertised_port));
                self.send(frame(METADATA | 0x8000, |w| {
                    w.u32(corr);
                    // One broker: this node, as reference 0.
                    w.i32(1).u16(0).str(&host).u32(port);
                    w.i32(streams.len() as i32);
                    for (s, ok) in streams.iter().zip(exists) {
                        w.str(s).u16(if ok { OK } else { STREAM_DOES_NOT_EXIST }).u16(if ok { 0 } else { 0xffff }).i32(0);
                    }
                }));
            }
            DECLARE_PUBLISHER => {
                let corr = r.u32()?;
                let id = r.u8()?;
                let reference = r.str()?;
                let stream = r.str()?;
                let code = if self.publishers.contains_key(&id) {
                    PRECONDITION_FAILED
                } else if !self.is_stream(&stream) {
                    STREAM_DOES_NOT_EXIST
                } else {
                    let reference = (!reference.is_empty()).then_some(reference);
                    let last = reference.as_deref().map(|r| self.sequence(&stream, r)).unwrap_or(0);
                    self.publishers.insert(id, Publisher { stream, reference, last });
                    OK
                };
                self.send(response(key, corr, code, |_| {}));
            }
            DELETE_PUBLISHER => {
                let corr = r.u32()?;
                let id = r.u8()?;
                let code = if self.publishers.remove(&id).is_some() { OK } else { PUBLISHER_DOES_NOT_EXIST };
                self.send(response(key, corr, code, |_| {}));
            }
            PUBLISH => self.on_publish(&mut r, version).await?,
            QUERY_PUBLISHER_SEQUENCE => {
                let corr = r.u32()?;
                let reference = r.str()?;
                let stream = r.str()?;
                let seq = self.sequence(&stream, &reference);
                self.send(response(key, corr, OK, |w| {
                    w.u64(seq);
                }));
            }
            SUBSCRIBE => self.on_subscribe(&mut r).await?,
            CREDIT => {
                let id = r.u8()?;
                let credit = r.u16()?;
                match self.subs.get_mut(&id) {
                    Some(sub) => {
                        sub.credit += u32::from(credit);
                        self.pump(id).await;
                    }
                    None => self.send(frame(CREDIT | 0x8000, |w| {
                        w.u16(SUBSCRIPTION_ID_DOES_NOT_EXIST).u8(id);
                    })),
                }
            }
            UNSUBSCRIBE => {
                let corr = r.u32()?;
                let id = r.u8()?;
                let code = if self.subs.contains_key(&id) {
                    self.drop_sub(id).await;
                    OK
                } else {
                    SUBSCRIPTION_ID_DOES_NOT_EXIST
                };
                self.send(response(key, corr, code, |_| {}));
            }
            STORE_OFFSET => {
                let reference = r.str()?;
                let stream = r.str()?;
                let offset = r.u64()?;
                if self.is_stream(&stream) {
                    self.put_param(OFFSETS, format!("{stream}\0{reference}"), offset.to_string()).await;
                }
            }
            QUERY_OFFSET => {
                let corr = r.u32()?;
                let reference = r.str()?;
                let stream = r.str()?;
                let (code, offset) = if !self.is_stream(&stream) {
                    (STREAM_DOES_NOT_EXIST, 0)
                } else {
                    match self.get_param(OFFSETS, &format!("{stream}\0{reference}")).and_then(|v| v.parse().ok()) {
                        Some(n) => (OK, n),
                        None => (NO_OFFSET, 0),
                    }
                };
                self.send(response(key, corr, code, |w| {
                    w.u64(offset);
                }));
            }
            STREAM_STATS => {
                let corr = r.u32()?;
                let stream = r.str()?;
                let code = if self.is_stream(&stream) { OK } else { STREAM_DOES_NOT_EXIST };
                // A chunk holds one message here, so chunk ids are message offsets.
                let offsets = if code == OK { self.stream_offsets(&stream).await } else { None };
                self.send(response(key, corr, code, |w| match offsets {
                    Some((first, last)) => {
                        w.i32(3);
                        w.str("first_chunk_id");
                        w.i64(first as i64);
                        w.str("last_chunk_id");
                        w.i64(last as i64);
                        w.str("committed_chunk_id");
                        w.i64(last as i64);
                    }
                    None => {
                        w.i32(0);
                    }
                }));
            }
            ROUTE => {
                let corr = r.u32()?;
                let routing_key = r.str()?;
                let super_stream = r.str()?;
                let (code, streams) = match self.bindings(&super_stream) {
                    Some(b) => (OK, b.into_iter().filter(|(_, k, _)| *k == routing_key).map(|(q, _, _)| q).collect()),
                    None => (STREAM_DOES_NOT_EXIST, Vec::new()),
                };
                self.send(response(key, corr, code, |w| {
                    w.strings(&streams);
                }));
            }
            PARTITIONS => {
                let corr = r.u32()?;
                let super_stream = r.str()?;
                let (code, streams) = match self.partitions(&super_stream) {
                    Some(p) => (OK, p),
                    None => (STREAM_DOES_NOT_EXIST, Vec::new()),
                };
                self.send(response(key, corr, code, |w| {
                    w.strings(&streams);
                }));
            }
            k if k == CONSUMER_UPDATE | 0x8000 => {
                let corr = r.u32()?;
                r.u16()?;
                if let Some((sub, gen)) = self.pending.remove(&corr) {
                    let spec = read_offset(&mut r).ok().flatten();
                    self.start_consuming(sub, gen, spec).await;
                }
            }
            CREATE_SUPER_STREAM => {
                let corr = r.u32()?;
                let name = r.str()?;
                let partitions = r.strings()?;
                let keys = r.strings()?;
                let args = r.map()?;
                let code = self.create_super_stream(&name, &partitions, &keys, &args).await;
                self.send(response(key, corr, code, |_| {}));
            }
            DELETE_SUPER_STREAM => {
                let corr = r.u32()?;
                let name = r.str()?;
                let code = match self.partitions(&name) {
                    None => STREAM_DOES_NOT_EXIST,
                    Some(parts) => {
                        let ok = match self.control().await {
                            Some(ch) => ch.exchange_delete(&name, ExchangeDeleteOptions::default()).await.is_ok(),
                            None => false,
                        };
                        for p in parts {
                            self.delete_stream(&p).await;
                        }
                        if ok {
                            OK
                        } else {
                            ACCESS_REFUSED
                        }
                    }
                };
                self.send(response(key, corr, code, |_| {}));
            }
            _ => {
                if r.left() >= 4 {
                    let corr = r.u32()?;
                    self.send(response(key, corr, PRECONDITION_FAILED, |_| {}));
                }
            }
        }
        Ok(())
    }

    /// First and last offsets of a live stream on this node, if any.
    async fn stream_offsets(&self, name: &str) -> Option<(u64, u64)> {
        let queues = self.ctx.queues.as_ref()?;
        let handle = queues.get(&queueforge_core::QueueKey::new(self.vhost.as_str(), name))?;
        let (reply, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(queueforge_core::QueueCmd::StreamOffsets { reply })
            .await
            .ok()?;
        rx.await.ok().flatten()
    }

    fn is_stream(&self, name: &str) -> bool {
        matches!(self.ctx.store.get_queue(&self.vhost, name), Ok(Some(q)) if q.args.queue_type == Some(QueueType::Stream))
    }

    fn get_param(&self, component: &str, name: &str) -> Option<String> {
        self.ctx
            .store
            .get_parameter(component, &self.vhost, name)
            .ok()
            .flatten()
            .map(|v| String::from_utf8_lossy(&v).into_owned())
    }

    async fn put_param(&self, component: &'static str, name: String, value: String) {
        let store = Arc::clone(&self.ctx.store);
        let vhost = self.vhost.clone();
        let _ = tokio::task::spawn_blocking(move || store.put_parameter(component, &vhost, &name, value.as_bytes())).await;
    }

    fn sequence(&self, stream: &str, reference: &str) -> u64 {
        self.get_param(SEQUENCES, &format!("{stream}\0{reference}")).and_then(|v| v.parse().ok()).unwrap_or(0)
    }

    /// A channel for one management operation. A refused operation closes it, not the session.
    async fn control(&self) -> Option<Channel> {
        self.amqp.as_ref()?.create_channel().await.ok()
    }

    async fn create_stream(&self, stream: &str, args: &[(String, String)]) -> u16 {
        if stream.is_empty() || stream.starts_with("amq.") {
            return PRECONDITION_FAILED;
        }
        if matches!(self.ctx.store.get_queue(&self.vhost, stream), Ok(Some(_))) {
            return STREAM_ALREADY_EXISTS;
        }
        let mut table = FieldTable::default();
        table.insert("x-queue-type".into(), AMQPValue::LongString(LongString::from(b"stream".to_vec())));
        for (k, v) in args {
            let key = if k.starts_with("x-") { k.clone() } else { format!("x-{k}") };
            let value = match v.parse::<i64>() {
                Ok(n) => AMQPValue::LongLongInt(n),
                Err(_) => AMQPValue::LongString(LongString::from(v.as_bytes().to_vec())),
            };
            table.insert(ShortString::from(key), value);
        }
        let Some(ch) = self.control().await else { return INTERNAL_ERROR };
        let opts = QueueDeclareOptions { durable: true, ..Default::default() };
        let code = match ch.queue_declare(stream, opts, table).await {
            Ok(_) => OK,
            Err(e) => refusal(&e.to_string()),
        };
        let _ = ch.close(200, "done").await;
        code
    }

    async fn delete_stream(&mut self, stream: &str) -> u16 {
        if !self.is_stream(stream) {
            return STREAM_DOES_NOT_EXIST;
        }
        let Some(ch) = self.control().await else { return INTERNAL_ERROR };
        let code = match ch.queue_delete(stream, QueueDeleteOptions::default()).await {
            Ok(_) => OK,
            Err(e) => refusal(&e.to_string()),
        };
        let _ = ch.close(200, "done").await;
        let gone: Vec<u8> = self.subs.iter().filter(|(_, s)| s.stream == stream).map(|(id, _)| *id).collect();
        for id in gone {
            self.drop_sub(id).await;
            self.send(frame(METADATA_UPDATE, |w| {
                w.u16(STREAM_DOES_NOT_EXIST).str(stream);
            }));
        }
        code
    }

    /// `(queue, routing key, partition order)` for every binding of an exchange, or None when it is missing.
    fn bindings(&self, exchange: &str) -> Option<Vec<(String, String, i64)>> {
        matches!(self.ctx.store.get_exchange(&self.vhost, exchange), Ok(Some(_))).then_some(())?;
        let rows = self.ctx.store.list_bindings_for_exchange(&self.vhost, exchange).ok()?;
        Some(
            rows.into_iter()
                .map(|b| {
                    let order = b
                        .args
                        .iter()
                        .find(|(k, _)| k.as_str() == PARTITION_ORDER)
                        .map(|(_, v)| match v {
                            HeaderArg::Int(n) => *n,
                            HeaderArg::Str(s) => s.parse().unwrap_or(0),
                        })
                        .unwrap_or(0);
                    (b.queue.to_string(), b.routing_key.to_string(), order)
                })
                .collect(),
        )
    }

    /// Partition streams of a super stream, in partition order.
    fn partitions(&self, super_stream: &str) -> Option<Vec<String>> {
        let mut rows = self.bindings(super_stream)?;
        rows.sort_by_key(|(_, _, order)| *order);
        Some(rows.into_iter().map(|(q, _, _)| q).collect())
    }

    async fn create_super_stream(&self, name: &str, partitions: &[String], keys: &[String], args: &[(String, String)]) -> u16 {
        if name.is_empty() || partitions.is_empty() || partitions.len() != keys.len() {
            return PRECONDITION_FAILED;
        }
        if matches!(self.ctx.store.get_exchange(&self.vhost, name), Ok(Some(_))) {
            return STREAM_ALREADY_EXISTS;
        }
        for p in partitions {
            if matches!(self.ctx.store.get_queue(&self.vhost, p), Ok(Some(_))) {
                return STREAM_ALREADY_EXISTS;
            }
        }
        let Some(ch) = self.control().await else { return INTERNAL_ERROR };
        // A super stream is a direct exchange with one stream bound per partition, as in RabbitMQ.
        let opts = ExchangeDeclareOptions { durable: true, ..Default::default() };
        if let Err(e) = ch.exchange_declare(name, ExchangeKind::Direct, opts, FieldTable::default()).await {
            return refusal(&e.to_string());
        }
        for (i, (p, k)) in partitions.iter().zip(keys).enumerate() {
            let code = self.create_stream(p, args).await;
            if code != OK {
                return code;
            }
            let mut bind_args = FieldTable::default();
            bind_args.insert(PARTITION_ORDER.into(), AMQPValue::LongLongInt(i as i64));
            if let Err(e) = ch.queue_bind(p, name, k, QueueBindOptions::default(), bind_args).await {
                return refusal(&e.to_string());
            }
        }
        let _ = ch.close(200, "done").await;
        OK
    }

    async fn on_publish(&mut self, r: &mut Rd<'_>, version: u16) -> R<()> {
        let id = r.u8()?;
        let count = r.u32()?;
        let mut confirmed = Vec::new();
        let mut failed: Vec<(u64, u16)> = Vec::new();
        let mut waiting = Vec::new();
        let stream_ok = self.publishers.get(&id).map(|p| self.is_stream(&p.stream));
        for _ in 0..count {
            let publishing_id = r.u64()?;
            if version >= 2 {
                r.str()?;
            }
            let payload = r.bytes()?;
            let (Some(publisher), Some(ch)) = (self.publishers.get_mut(&id), self.publish.as_ref()) else {
                failed.push((publishing_id, PUBLISHER_DOES_NOT_EXIST));
                continue;
            };
            if stream_ok != Some(true) {
                failed.push((publishing_id, STREAM_DOES_NOT_EXIST));
                continue;
            }
            // A named publisher's ids rise; one at or below the last stored is a duplicate.
            if publisher.reference.is_some() && publishing_id <= publisher.last {
                confirmed.push(publishing_id);
                continue;
            }
            let Ok(msg) = map::inbound(payload) else {
                failed.push((publishing_id, INTERNAL_ERROR));
                continue;
            };
            match ch.basic_publish("", &publisher.stream, BasicPublishOptions::default(), &msg.body, msg.props).await {
                Ok(confirm) => waiting.push((publishing_id, confirm)),
                Err(_) => failed.push((publishing_id, INTERNAL_ERROR)),
            }
            if publisher.reference.is_some() {
                publisher.last = publishing_id;
            }
        }
        for (publishing_id, confirm) in waiting {
            match confirm.await {
                Ok(c) if !c.is_nack() => confirmed.push(publishing_id),
                _ => failed.push((publishing_id, INTERNAL_ERROR)),
            }
        }
        if let Some(p) = self.publishers.get(&id) {
            if let Some(reference) = &p.reference {
                if p.last > 0 {
                    self.put_param(SEQUENCES, format!("{}\0{}", p.stream, reference), p.last.to_string()).await;
                }
            }
        }
        if !confirmed.is_empty() {
            self.send(frame(PUBLISH_CONFIRM, |w| {
                w.u8(id).u32(confirmed.len() as u32);
                for c in &confirmed {
                    w.u64(*c);
                }
            }));
        }
        if !failed.is_empty() {
            self.send(frame(PUBLISH_ERROR, |w| {
                w.u8(id).u32(failed.len() as u32);
                for (c, code) in &failed {
                    w.u64(*c).u16(*code);
                }
            }));
        }
        Ok(())
    }

    async fn on_subscribe(&mut self, r: &mut Rd<'_>) -> R<()> {
        let corr = r.u32()?;
        let id = r.u8()?;
        let stream = r.str()?;
        let spec = read_offset(r)?;
        let credit = r.u16()?;
        let props: HashMap<String, String> = if r.left() >= 4 { r.map()?.into_iter().collect() } else { HashMap::new() };
        let Some(spec) = spec else {
            self.send(response(SUBSCRIBE, corr, PRECONDITION_FAILED, |_| {}));
            return Ok(());
        };
        let code = if self.subs.contains_key(&id) {
            SUBSCRIPTION_ID_ALREADY_EXISTS
        } else if !self.is_stream(&stream) {
            STREAM_DOES_NOT_EXIST
        } else {
            OK
        };
        let single = props.get("single-active-consumer").map(String::as_str) == Some("true");
        let name = props.get("name").cloned().unwrap_or_default();
        let code = if code == OK && single && name.is_empty() { PRECONDITION_FAILED } else { code };
        self.send(response(SUBSCRIBE, corr, code, |_| {}));
        if code != OK {
            return Ok(());
        }
        self.gen += 1;
        let gen = self.gen;
        let group = single.then(|| format!("{}\0{stream}\0{name}", self.vhost));
        let sub = Sub {
            stream: stream.clone(),
            gen,
            credit: u32::from(credit),
            spec,
            ch: None,
            tag: format!("stream-{}-{id}-{gen}", self.id),
            buffer: VecDeque::new(),
            group: group.clone(),
        };
        self.subs.insert(id, sub);
        let Some(key) = group else {
            self.start_consuming(id, gen, None).await;
            return Ok(());
        };
        // On a super stream partition, member `index % n` is active, so partitions spread over the group.
        let index = props
            .get("super-stream")
            .and_then(|s| self.partitions(s))
            .and_then(|parts| parts.iter().position(|p| *p == stream))
            .unwrap_or(0);
        if let Ok(mut all) = groups().lock() {
            let g = all.entry(key.clone()).or_insert(Group { index, members: Vec::new(), active: None });
            g.members.push(Member { conn: self.id, sub: id, gen, tx: self.tx.clone() });
        }
        rebalance(&key);
        Ok(())
    }

    /// Start a consumer for a subscription, at `spec` or where it subscribed.
    async fn start_consuming(&mut self, id: u8, gen: u64, spec: Option<AMQPValue>) {
        let Some(amqp) = self.amqp.as_ref() else { return };
        let Some(sub) = self.subs.get(&id).filter(|s| s.gen == gen && s.ch.is_none()) else { return };
        let Ok(ch) = amqp.create_channel().await else { return };
        let _ = ch.basic_qos(PREFETCH, BasicQosOptions::default()).await;
        let mut args = FieldTable::default();
        args.insert("x-stream-offset".into(), spec.unwrap_or_else(|| sub.spec.clone()));
        let Ok(consumer) = ch.basic_consume(&sub.stream, &sub.tag, BasicConsumeOptions::default(), args).await else { return };
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let mut consumer = consumer;
            while let Some(Ok(delivery)) = consumer.next().await {
                if tx.send(Ev::Delivery { sub: id, gen, delivery }).is_err() {
                    break;
                }
            }
        });
        if let Some(sub) = self.subs.get_mut(&id) {
            sub.ch = Some(ch);
        }
    }

    async fn stop_consuming(&mut self, id: u8) {
        let Some(sub) = self.subs.get_mut(&id) else { return };
        sub.buffer.clear();
        if let Some(ch) = sub.ch.take() {
            let _ = ch.basic_cancel(&sub.tag, BasicCancelOptions::default()).await;
            let _ = ch.close(200, "inactive").await;
        }
    }

    async fn on_event(&mut self, ev: Ev) {
        match ev {
            Ev::Delivery { sub, gen, delivery } => {
                let Some(s) = self.subs.get_mut(&sub) else { return };
                if s.gen != gen || s.ch.is_none() {
                    return;
                }
                s.buffer.push_back(delivery);
                self.pump(sub).await;
            }
            Ev::Activate { sub, gen } => {
                if !self.subs.get(&sub).is_some_and(|s| s.gen == gen) {
                    return;
                }
                let corr = self.next_corr;
                self.next_corr += 1;
                self.pending.insert(corr, (sub, gen));
                self.send(frame(CONSUMER_UPDATE, |w| {
                    w.u32(corr).u8(sub).u8(1);
                }));
            }
            Ev::Deactivate { sub, gen } => {
                if !self.subs.get(&sub).is_some_and(|s| s.gen == gen) {
                    return;
                }
                self.stop_consuming(sub).await;
                let corr = self.next_corr;
                self.next_corr += 1;
                self.send(frame(CONSUMER_UPDATE, |w| {
                    w.u32(corr).u8(sub).u8(0);
                }));
            }
        }
    }

    /// Send buffered deliveries while the subscription has credit, each as a chunk of one.
    async fn pump(&mut self, id: u8) {
        let Some(sub) = self.subs.get_mut(&id) else { return };
        while sub.credit > 0 {
            let Some(d) = sub.buffer.pop_front() else { break };
            sub.credit -= 1;
            let offset = d
                .properties
                .headers()
                .as_ref()
                .and_then(|h| h.inner().get("x-stream-offset"))
                .and_then(|v| match v {
                    AMQPValue::LongLongInt(n) => Some(*n as u64),
                    AMQPValue::LongInt(n) => Some(*n as u64),
                    AMQPValue::LongUInt(n) => Some(u64::from(*n)),
                    _ => None,
                })
                .unwrap_or(0);
            let entry = map::outbound(d.exchange.as_str(), d.routing_key.as_str(), false, &d.properties, &d.data, STREAM_ONLY);
            let mut data = Vec::with_capacity(4 + entry.len());
            data.extend((entry.len() as u32).to_be_bytes());
            data.extend(&entry);
            let crc = crc32fast::hash(&data);
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0);
            self.out.extend(frame(DELIVER, |w| {
                w.u8(id);
                // Osiris chunk header: magic and version, user chunk, 1 entry, 1 record.
                w.u8(0x50).u8(0).u16(1).u32(1);
                w.i64(now).u64(0).u64(offset);
                w.u32(crc).u32(data.len() as u32).u32(0).u32(0);
                w.0.extend(&data);
            }));
            // A stream ack only returns the consumer's prefetch credit.
            if let Some(ch) = &sub.ch {
                let _ = ch.basic_ack(d.delivery_tag, BasicAckOptions::default()).await;
            }
        }
    }

    async fn drop_sub(&mut self, id: u8) {
        self.stop_consuming(id).await;
        if let Some(sub) = self.subs.remove(&id) {
            if let Some(key) = sub.group {
                leave_group(&key, self.id, id, sub.gen);
            }
        }
    }

    async fn teardown(&mut self) {
        let ids: Vec<u8> = self.subs.keys().copied().collect();
        for id in ids {
            self.drop_sub(id).await;
        }
        if let Some(c) = self.amqp.take() {
            let _ = c.close(200, "stream client closed").await;
        }
    }
}

async fn confirm_channel(amqp: &Connection) -> Option<Channel> {
    let ch = amqp.create_channel().await.ok()?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.ok()?;
    Some(ch)
}

/// A refused 0-9-1 operation as a stream response code.
fn refusal(error: &str) -> u16 {
    if error.contains("ACCESS_REFUSED") || error.contains("403") {
        ACCESS_REFUSED
    } else if error.contains("NOT_FOUND") || error.contains("404") {
        STREAM_DOES_NOT_EXIST
    } else {
        PRECONDITION_FAILED
    }
}
