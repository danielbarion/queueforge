//! AMQP 1.0 over a loopback AMQP 0-9-1 connection, the way RabbitMQ's
//! plugins sit on the broker.
//!
//! SASL PLAIN logs in. The open hostname `vhost:<name>` picks the vhost.
//! Links use RabbitMQ 4's v2 addresses: a client sender targets
//! `/exchanges/:x/:key`, `/exchanges/:x` (the subject is the key),
//! `/queues/:q`, or a null target with the address in each message's `to`.
//! A client receiver reads `/queues/:q` and gets at most its link credit,
//! with drain honoured. Each receiving link has its own channel and
//! consumer; deliveries beyond the credit wait in the bridge. Dispositions
//! map to ack (accepted), requeue (released, modified) and dead-letter
//! (rejected, modified undeliverable-here).

pub(crate) mod map;
pub(crate) mod types;

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, ExchangeDeclareOptions, QueueDeclareOptions,
};
use lapin::publisher_confirm::Confirmation;
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, ExchangeKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use super::amqp_uri;
use types::{encode, Decoder, V};

const HEADER_AMQP: [u8; 8] = *b"AMQP\x00\x01\x00\x00";
const HEADER_SASL: [u8; 8] = *b"AMQP\x03\x01\x00\x00";
/// The largest frame accepted, and sent unless the peer wants smaller.
const MAX_FRAME: usize = 128 * 1024;
/// Credit granted to a client sender, topped up when half is used.
const LINK_CREDIT: u32 = 256;
/// Deliveries a receiving link's consumer may hold before the bridge settles.
const PREFETCH: u16 = 256;
const WINDOW: u32 = 0x7fff_ffff;

const OPEN: u64 = 0x10;
const BEGIN: u64 = 0x11;
const ATTACH: u64 = 0x12;
const FLOW: u64 = 0x13;
const TRANSFER: u64 = 0x14;
const DISPOSITION: u64 = 0x15;
const DETACH: u64 = 0x16;
const END: u64 = 0x17;
const CLOSE: u64 = 0x18;
const ACCEPTED: u64 = 0x24;
const REJECTED: u64 = 0x25;
const RELEASED: u64 = 0x26;
const MODIFIED: u64 = 0x27;

/// Where a client sender's messages go. `None` key: use the subject.
#[derive(Clone)]
struct Target {
    exchange: String,
    key: Option<String>,
}

enum Address {
    Exchange(Target),
    Queue(String),
}

/// Percent-decode one address segment. A bad escape keeps the raw text.
fn segment(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = b.get(i + 1..i + 3).and_then(|h| std::str::from_utf8(h).ok()).and_then(|h| u8::from_str_radix(h, 16).ok());
        match (b[i], hex) {
            (b'%', Some(n)) => {
                out.push(n);
                i += 3;
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

/// Parse a v2 address, plus the v1 forms older clients send.
fn parse_address(address: &str) -> Option<Address> {
    let parts: Vec<&str> = address.split('/').collect();
    if parts.first() != Some(&"") || parts.len() < 3 {
        return None;
    }
    let exchange = |name: &str, key: Option<String>| {
        let name = segment(name);
        Address::Exchange(Target { exchange: if name == "amq.default" { String::new() } else { name }, key })
    };
    match (parts[1], parts.len()) {
        ("queues", 3) if !parts[2].is_empty() => Some(Address::Queue(segment(parts[2]))),
        ("exchanges", 3) => Some(exchange(parts[2], None)),
        ("exchanges", 4) => Some(exchange(parts[2], Some(segment(parts[3])))),
        ("queue", 3) => Some(Address::Queue(segment(parts[2]))),
        ("amq", 4) if parts[2] == "queue" => Some(Address::Queue(segment(parts[3]))),
        ("exchange", _) => Some(exchange(parts[2], Some(segment(&parts[3..].join("/"))))),
        ("topic", _) => Some(Address::Exchange(Target { exchange: "amq.topic".into(), key: Some(segment(&parts[2..].join("/"))) })),
        _ => None,
    }
}

fn error(condition: &str, description: &str) -> V {
    V::described(0x1d, V::List(vec![V::sym(condition), V::Str(description.into())]))
}

fn performative(code: u64, mut fields: Vec<V>) -> Vec<u8> {
    while fields.last().is_some_and(V::is_null) {
        fields.pop();
    }
    encode(&V::described(code, V::List(fields)))
}

fn frame(kind: u8, channel: u16, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend(((8 + body.len()) as u32).to_be_bytes());
    out.extend([2, kind]);
    out.extend(channel.to_be_bytes());
    out.extend(body);
    out
}

/// Something that happened off the socket: a delivery on a link's consumer,
/// or a drain whose wait for in-flight deliveries is over.
enum Ev {
    Delivery { channel: u16, handle: u32, gen: u64, delivery: Delivery },
    DrainDue { channel: u16, handle: u32, gen: u64, tries: u8 },
}

struct InLink {
    target: Option<Target>,
    credit: u32,
    delivery_count: u32,
    /// The frames of a transfer whose last frame has not come yet.
    partial: Option<(Vec<u8>, u32, bool)>,
}

struct OutLink {
    queue: String,
    ch: Channel,
    tag: String,
    gen: u64,
    presettled: bool,
    credit: u32,
    delivery_count: u32,
    drain: bool,
    buffer: VecDeque<Delivery>,
}

enum Link {
    In(InLink),
    Out(OutLink),
}

struct Session {
    links: HashMap<u32, Link>,
    next_outgoing_id: u32,
    next_incoming_id: u32,
    /// We may send transfers while next_outgoing_id is below this.
    remote_limit: u64,
    next_delivery_id: u32,
    /// delivery-id to (handle, link generation, 0-9-1 delivery tag).
    unsettled: HashMap<u32, (u32, u64, u64)>,
    held: VecDeque<Vec<u8>>,
}

struct Conn {
    amqp: Connection,
    publish: Channel,
    sessions: HashMap<u16, Session>,
    out: Vec<u8>,
    remote_max_frame: usize,
    tx: mpsc::UnboundedSender<Ev>,
    gen: u64,
    id: String,
    closing: bool,
}

struct Refuse(&'static str, String);

/// Serve one AMQP 1.0 connection whose protocol header is still unread.
pub async fn serve<S: AsyncRead + AsyncWrite + Unpin + Send>(mut io: S, amqp_port: u16) {
    let mut buf = Vec::with_capacity(64 * 1024);
    let Some((user, pass)) = sasl(&mut io, &mut buf, amqp_port).await else { return };
    // The client sends a plain AMQP header once SASL succeeds.
    if read_exact_buf(&mut io, &mut buf, 8).await.is_none() || buf[..8] != HEADER_AMQP {
        return;
    }
    buf.drain(..8);
    if io.write_all(&HEADER_AMQP).await.is_err() {
        return;
    }
    // open: pick the vhost, then log in to it over loopback.
    let Some((open, _)) = next_frame(&mut io, &mut buf).await else { return };
    let open = match Decoder::new(&open.2).value() {
        Ok(v) if v.code() == Some(OPEN) => v,
        _ => return,
    };
    let vhost = open.field(1).as_str().and_then(|h| h.strip_prefix("vhost:")).unwrap_or("/").to_string();
    let remote_max_frame = open.field(2).as_u64().map(|n| (n as usize).clamp(512, MAX_FRAME)).unwrap_or(MAX_FRAME);
    let idle = open.field(4).as_u64().unwrap_or(0);
    let id = format!("queueforge-{}", super::token());
    let reply = performative(
        OPEN,
        vec![
            V::Str(id.clone()),
            V::Null,
            V::Uint(MAX_FRAME as u32),
            V::Ushort(65535),
            V::Null,
            V::Null,
            V::Null,
            V::Null,
            V::Null,
            V::Map(vec![(V::sym("product"), V::Str("QueueForge".into())), (V::sym("platform"), V::Str("Rust".into()))]),
        ],
    );
    let _ = io.write_all(&frame(0, 0, &reply)).await;
    let amqp = match Connection::connect(&amqp_uri(amqp_port, &user, &pass, &vhost), ConnectionProperties::default()).await {
        Ok(c) => c,
        Err(e) => {
            let close = performative(CLOSE, vec![error("amqp:not-allowed", &format!("vhost '{vhost}': {e}"))]);
            let _ = io.write_all(&frame(0, 0, &close)).await;
            return;
        }
    };
    let Some(publish) = confirm_channel(&amqp).await else { return };
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut c = Conn { amqp, publish, sessions: HashMap::new(), out: Vec::new(), remote_max_frame, tx, gen: 0, id, closing: false };
    let mut beat = tokio::time::interval(Duration::from_millis(if idle > 0 { (idle / 2).max(500) } else { 3_600_000 }));
    beat.tick().await;
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        // Frames already buffered are handled before more is read.
        while let Some(f) = take_frame(&mut buf) {
            match f {
                Ok((kind, channel, body)) => c.on_frame(kind, channel, &body).await,
                Err(e) => c.fail("amqp:connection:framing-error", &e),
            }
            if c.flush(&mut io).await.is_err() || c.closing {
                return c.teardown().await;
            }
        }
        tokio::select! {
            n = io.read(&mut chunk) => match n {
                Ok(0) | Err(_) => return c.teardown().await,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            },
            Some(ev) = rx.recv() => c.on_event(ev).await,
            _ = beat.tick(), if idle > 0 => c.out.extend(frame(0, 0, &[])),
        }
        if c.flush(&mut io).await.is_err() || c.closing {
            return c.teardown().await;
        }
    }
}

async fn confirm_channel(amqp: &Connection) -> Option<Channel> {
    let ch = amqp.create_channel().await.ok()?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.ok()?;
    Some(ch)
}

async fn read_exact_buf<S: AsyncRead + Unpin>(io: &mut S, buf: &mut Vec<u8>, n: usize) -> Option<()> {
    let mut chunk = [0u8; 8192];
    while buf.len() < n {
        let got = io.read(&mut chunk).await.ok()?;
        if got == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..got]);
    }
    Some(())
}

type RawFrame = (u8, u16, Vec<u8>);

/// One complete frame from the front of `buf`, if there is one. Empty frames are skipped.
fn take_frame(buf: &mut Vec<u8>) -> Option<Result<RawFrame, String>> {
    loop {
        if buf.len() < 8 {
            return None;
        }
        let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if !(8..=MAX_FRAME).contains(&size) {
            return Some(Err(format!("frame size {size}")));
        }
        if buf.len() < size {
            return None;
        }
        let doff = (buf[4] as usize * 4).clamp(8, size);
        let kind = buf[5];
        let channel = u16::from_be_bytes([buf[6], buf[7]]);
        let body = buf[doff..size].to_vec();
        buf.drain(..size);
        if !body.is_empty() {
            return Some(Ok((kind, channel, body)));
        }
    }
}

async fn next_frame<S: AsyncRead + Unpin>(io: &mut S, buf: &mut Vec<u8>) -> Option<(RawFrame, ())> {
    let mut chunk = [0u8; 8192];
    loop {
        match take_frame(buf) {
            Some(Ok(f)) => return Some((f, ())),
            Some(Err(_)) => return None,
            None => {
                let n = io.read(&mut chunk).await.ok()?;
                if n == 0 {
                    return None;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
        }
    }
}

/// Run the SASL exchange. Returns the PLAIN credentials once they log in.
async fn sasl<S: AsyncRead + AsyncWrite + Unpin>(io: &mut S, buf: &mut Vec<u8>, amqp_port: u16) -> Option<(String, String)> {
    read_exact_buf(io, buf, 8).await?;
    let header: [u8; 8] = buf[..8].try_into().ok()?;
    buf.drain(..8);
    // Plain AMQP without SASL has no identity: offer SASL and stop, as RabbitMQ does.
    io.write_all(&HEADER_SASL).await.ok()?;
    if header != HEADER_SASL {
        return None;
    }
    let mechanisms = performative(0x40, vec![V::Array(vec![V::sym("PLAIN")])]);
    io.write_all(&frame(1, 0, &mechanisms)).await.ok()?;
    let ((kind, _, body), _) = next_frame(io, buf).await?;
    let init = Decoder::new(&body).value().ok()?;
    if kind != 1 || init.code() != Some(0x41) {
        return None;
    }
    let creds = match (init.field(0).as_str(), init.field(1)) {
        (Some("PLAIN"), V::Binary(response)) => {
            let text = String::from_utf8_lossy(response).into_owned();
            let mut parts = text.split('\0');
            let _authzid = parts.next();
            Some((parts.next().unwrap_or_default().to_string(), parts.next().unwrap_or_default().to_string()))
        }
        _ => None,
    };
    let ok = match &creds {
        Some((user, pass)) if !user.is_empty() => login_ok(amqp_port, user, pass).await,
        _ => false,
    };
    let outcome = performative(0x44, vec![V::Ubyte(if ok { 0 } else { 1 })]);
    io.write_all(&frame(1, 0, &outcome)).await.ok()?;
    if ok {
        creds
    } else {
        None
    }
}

/// Whether the broker accepts these credentials. The vhost is not known
/// yet, so a refusal that names the vhost still counts as a good login.
pub(crate) async fn login_ok(amqp_port: u16, user: &str, pass: &str) -> bool {
    match Connection::connect(&amqp_uri(amqp_port, user, pass, "/"), ConnectionProperties::default()).await {
        Ok(c) => {
            let _ = c.close(200, "login checked").await;
            true
        }
        // Only a refusal about the vhost itself means the password was right.
        Err(e) => {
            let text = format!("{e:?}");
            text.contains("access to vhost") || text.contains("vhost / not found")
        }
    }
}

impl Conn {
    async fn flush<S: AsyncWrite + Unpin>(&mut self, io: &mut S) -> std::io::Result<()> {
        if self.out.is_empty() {
            return Ok(());
        }
        let out = std::mem::take(&mut self.out);
        io.write_all(&out).await?;
        io.flush().await
    }

    fn send(&mut self, channel: u16, body: Vec<u8>) {
        self.out.extend(frame(0, channel, &body));
    }

    fn fail(&mut self, condition: &str, description: &str) {
        let close = performative(CLOSE, vec![error(condition, description)]);
        self.send(0, close);
        self.closing = true;
    }

    async fn teardown(mut self) {
        let sessions: Vec<u16> = self.sessions.keys().copied().collect();
        for channel in sessions {
            self.end_session(channel).await;
        }
        let _ = self.amqp.close(200, "amqp 1.0 client closed").await;
    }

    async fn on_frame(&mut self, kind: u8, channel: u16, body: &[u8]) {
        let mut d = Decoder::new(body);
        let perf = match d.value() {
            Ok(v) => v,
            Err(e) => return self.fail("amqp:decode-error", &e),
        };
        if kind != 0 {
            return self.fail("amqp:not-allowed", "unexpected SASL frame");
        }
        let payload = &body[d.at..];
        match perf.code() {
            Some(BEGIN) => self.on_begin(channel, &perf),
            Some(ATTACH) => self.on_attach(channel, &perf).await,
            Some(FLOW) => self.on_flow(channel, &perf).await,
            Some(TRANSFER) => self.on_transfer(channel, &perf, payload).await,
            Some(DISPOSITION) => self.on_disposition(channel, &perf).await,
            Some(DETACH) => {
                let handle = perf.field(0).as_u32().unwrap_or(0);
                self.drop_link(channel, handle).await;
                self.send(channel, performative(DETACH, vec![V::Uint(handle), V::Bool(true)]));
            }
            Some(END) => {
                self.end_session(channel).await;
                self.send(channel, performative(END, vec![]));
            }
            Some(CLOSE) => {
                self.send(0, performative(CLOSE, vec![]));
                self.closing = true;
            }
            Some(OPEN) => self.fail("amqp:illegal-state", "connection is already open"),
            other => self.fail("amqp:not-implemented", &format!("performative {other:?}")),
        }
    }

    fn on_begin(&mut self, channel: u16, f: &V) {
        let session = Session {
            links: HashMap::new(),
            next_outgoing_id: 0,
            next_incoming_id: f.field(1).as_u32().unwrap_or(0),
            remote_limit: f.field(2).as_u64().unwrap_or(u64::from(WINDOW)),
            next_delivery_id: 0,
            unsettled: HashMap::new(),
            held: VecDeque::new(),
        };
        self.sessions.insert(channel, session);
        let reply = performative(BEGIN, vec![V::Ushort(channel), V::Uint(0), V::Uint(WINDOW), V::Uint(WINDOW), V::Uint(255)]);
        self.send(channel, reply);
    }

    async fn on_attach(&mut self, channel: u16, f: &V) {
        if !self.sessions.contains_key(&channel) {
            return self.fail("amqp:not-found", "attach on a channel with no session");
        }
        let name = f.field(0).clone();
        let handle = f.field(1).as_u32().unwrap_or(0);
        let client_receives = f.field(2).as_bool();
        let snd_settle = f.field(3).as_u64().unwrap_or(2) as u8;
        let rcv_settle = f.field(4).as_u64().unwrap_or(0) as u8;
        let source = f.field(5).clone();
        let target = f.field(6).clone();
        let reply = |src: V, tgt: V| {
            let mut fields =
                vec![name.clone(), V::Uint(handle), V::Bool(!client_receives), V::Ubyte(snd_settle), V::Ubyte(rcv_settle), src, tgt, V::Null, V::Null];
            if client_receives {
                fields.push(V::Uint(0));
            }
            performative(ATTACH, fields)
        };
        let result = if client_receives {
            self.attach_out(channel, handle, &source, snd_settle == 1).await
        } else {
            self.attach_in(channel, handle, &target, f.field(9).as_u32().unwrap_or(0)).await
        };
        match result {
            Ok(()) => {
                self.send(channel, reply(source, target));
                if !client_receives {
                    self.link_flow(channel, handle);
                }
            }
            Err(Refuse(condition, message)) => {
                // A refused attach is an attach with no terminus, then a detach with the error.
                let refused = if client_receives { reply(V::Null, target) } else { reply(source, V::Null) };
                self.send(channel, refused);
                self.send(channel, performative(DETACH, vec![V::Uint(handle), V::Bool(true), error(condition, &message)]));
            }
        }
    }

    async fn attach_out(&mut self, channel: u16, handle: u32, source: &V, presettled: bool) -> Result<(), Refuse> {
        let address = source.field(0).as_str().unwrap_or_default().to_string();
        let Some(Address::Queue(queue)) = parse_address(&address) else {
            return Err(Refuse("amqp:invalid-field", format!("source address '{address}' is not a queue")));
        };
        let ch = self.amqp.create_channel().await.map_err(|e| Refuse("amqp:internal-error", e.to_string()))?;
        let passive = QueueDeclareOptions { passive: true, ..Default::default() };
        if let Err(e) = ch.queue_declare(&queue, passive, FieldTable::default()).await {
            return Err(refusal(&e.to_string(), &queue));
        }
        let _ = ch.basic_qos(PREFETCH, BasicQosOptions::default()).await;
        self.gen += 1;
        let gen = self.gen;
        let tag = format!("amq.ctag-1.0-{}-{channel}-{handle}-{gen}", self.id);
        let consumer = ch
            .basic_consume(&queue, &tag, BasicConsumeOptions::default(), FieldTable::default())
            .await
            .map_err(|e| refusal(&e.to_string(), &queue))?;
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let mut consumer = consumer;
            while let Some(Ok(delivery)) = consumer.next().await {
                if tx.send(Ev::Delivery { channel, handle, gen, delivery }).is_err() {
                    break;
                }
            }
        });
        let link = OutLink { queue, ch, tag, gen, presettled, credit: 0, delivery_count: 0, drain: false, buffer: VecDeque::new() };
        if let Some(s) = self.sessions.get_mut(&channel) {
            s.links.insert(handle, Link::Out(link));
        }
        Ok(())
    }

    async fn attach_in(&mut self, channel: u16, handle: u32, target: &V, initial: u32) -> Result<(), Refuse> {
        let resolved = match target.field(0) {
            V::Null => None,
            addr => {
                let text = addr.as_str().unwrap_or_default().to_string();
                match parse_address(&text) {
                    Some(Address::Queue(q)) => {
                        self.check_queue(&q).await?;
                        Some(Target { exchange: String::new(), key: Some(q) })
                    }
                    Some(Address::Exchange(t)) => {
                        self.check_exchange(&t.exchange).await?;
                        Some(t)
                    }
                    None => return Err(Refuse("amqp:invalid-field", format!("target address '{text}' is not valid"))),
                }
            }
        };
        let link = InLink { target: resolved, credit: LINK_CREDIT, delivery_count: initial, partial: None };
        if let Some(s) = self.sessions.get_mut(&channel) {
            s.links.insert(handle, Link::In(link));
        }
        Ok(())
    }

    async fn check_queue(&self, queue: &str) -> Result<(), Refuse> {
        let ch = self.amqp.create_channel().await.map_err(|e| Refuse("amqp:internal-error", e.to_string()))?;
        let passive = QueueDeclareOptions { passive: true, ..Default::default() };
        let out = ch.queue_declare(queue, passive, FieldTable::default()).await.map(|_| ()).map_err(|e| refusal(&e.to_string(), queue));
        let _ = ch.close(200, "checked").await;
        out
    }

    async fn check_exchange(&self, exchange: &str) -> Result<(), Refuse> {
        if exchange.is_empty() {
            return Ok(());
        }
        let ch = self.amqp.create_channel().await.map_err(|e| Refuse("amqp:internal-error", e.to_string()))?;
        let passive = ExchangeDeclareOptions { passive: true, ..Default::default() };
        let out = ch
            .exchange_declare(exchange, ExchangeKind::Direct, passive, FieldTable::default())
            .await
            .map(|_| ())
            .map_err(|e| refusal(&e.to_string(), exchange));
        let _ = ch.close(200, "checked").await;
        out
    }

    /// Tell the peer a link's credit and delivery count, with the session window.
    fn link_flow(&mut self, channel: u16, handle: u32) {
        let Some(s) = self.sessions.get(&channel) else { return };
        let (count, credit, drain) = match s.links.get(&handle) {
            Some(Link::In(l)) => (l.delivery_count, l.credit, V::Null),
            Some(Link::Out(l)) => (l.delivery_count, l.credit, V::Bool(l.drain)),
            None => return,
        };
        let body = performative(
            FLOW,
            vec![
                V::Uint(s.next_incoming_id),
                V::Uint(WINDOW),
                V::Uint(s.next_outgoing_id),
                V::Uint(WINDOW),
                V::Uint(handle),
                V::Uint(count),
                V::Uint(credit),
                V::Null,
                drain,
            ],
        );
        self.send(channel, body);
    }

    async fn on_flow(&mut self, channel: u16, f: &V) {
        let Some(s) = self.sessions.get_mut(&channel) else { return };
        let next_incoming = f.field(0).as_u64().unwrap_or(0);
        let window = f.field(1).as_u64().unwrap_or(u64::from(WINDOW));
        s.remote_limit = next_incoming + window;
        while !s.held.is_empty() && u64::from(s.next_outgoing_id) < s.remote_limit {
            s.next_outgoing_id = s.next_outgoing_id.wrapping_add(1);
            if let Some(held) = s.held.pop_front() {
                self.out.extend(held);
            }
        }
        let Some(handle) = f.field(4).as_u32() else { return };
        let echo = f.field(9).as_bool();
        let out_link = match s.links.get_mut(&handle) {
            Some(Link::In(_)) => None,
            Some(Link::Out(l)) => {
                // The receiver's credit is relative to the delivery count it has seen.
                let seen = i64::from(f.field(5).as_u32().unwrap_or(0));
                let credit = i64::from(f.field(6).as_u32().unwrap_or(0));
                l.credit = (seen + credit - i64::from(l.delivery_count)).clamp(0, i64::from(u32::MAX)) as u32;
                l.drain = f.field(8).as_bool();
                Some((l.gen, l.drain))
            }
            None => return,
        };
        match out_link {
            None if echo => self.link_flow(channel, handle),
            None => {}
            Some((gen, drain)) => {
                self.pump(channel, handle).await;
                if drain {
                    self.finish_drain(channel, handle, gen, 0).await;
                } else if echo {
                    self.link_flow(channel, handle);
                }
            }
        }
    }

    /// End a drain: once nothing more is on the way, the rest of the credit is spent.
    async fn finish_drain(&mut self, channel: u16, handle: u32, gen: u64, tries: u8) {
        let Some(Link::Out(l)) = self.sessions.get_mut(&channel).and_then(|s| s.links.get_mut(&handle)) else { return };
        if l.gen != gen || !l.drain {
            return;
        }
        if l.credit > 0 && l.buffer.is_empty() && tries < 10 {
            // Deliveries may still be in flight from the queue to the consumer.
            let passive = QueueDeclareOptions { passive: true, ..Default::default() };
            let ready = l.ch.queue_declare(&l.queue, passive, FieldTable::default()).await.map(|q| q.message_count()).unwrap_or(0);
            if ready > 0 {
                let tx = self.tx.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let _ = tx.send(Ev::DrainDue { channel, handle, gen, tries: tries + 1 });
                });
                return;
            }
        }
        let Some(Link::Out(l)) = self.sessions.get_mut(&channel).and_then(|s| s.links.get_mut(&handle)) else { return };
        l.delivery_count = l.delivery_count.wrapping_add(l.credit);
        l.credit = 0;
        self.link_flow(channel, handle);
        if let Some(Link::Out(l)) = self.sessions.get_mut(&channel).and_then(|s| s.links.get_mut(&handle)) {
            l.drain = false;
        }
    }

    async fn on_event(&mut self, ev: Ev) {
        match ev {
            Ev::Delivery { channel, handle, gen, delivery } => {
                let Some(Link::Out(l)) = self.sessions.get_mut(&channel).and_then(|s| s.links.get_mut(&handle)) else { return };
                if l.gen != gen {
                    return;
                }
                l.buffer.push_back(delivery);
                self.pump(channel, handle).await;
                let drain = matches!(self.sessions.get(&channel).and_then(|s| s.links.get(&handle)), Some(Link::Out(l)) if l.drain && l.credit == 0);
                if drain {
                    self.finish_drain(channel, handle, gen, 10).await;
                }
            }
            Ev::DrainDue { channel, handle, gen, tries } => {
                self.pump(channel, handle).await;
                self.finish_drain(channel, handle, gen, tries).await;
            }
        }
    }

    /// Send buffered deliveries while the link has credit.
    async fn pump(&mut self, channel: u16, handle: u32) {
        let max_frame = self.remote_max_frame;
        let Some(s) = self.sessions.get_mut(&channel) else { return };
        let Some(Link::Out(l)) = s.links.get_mut(&handle) else { return };
        while l.credit > 0 {
            let Some(d) = l.buffer.pop_front() else { break };
            l.credit -= 1;
            l.delivery_count = l.delivery_count.wrapping_add(1);
            let id = s.next_delivery_id;
            s.next_delivery_id = s.next_delivery_id.wrapping_add(1);
            let payload = map::outbound(d.exchange.as_str(), d.routing_key.as_str(), d.redelivered, &d.properties, &d.data, &[]);
            let tag = V::Binary(id.to_be_bytes().to_vec());
            let head = |more: bool| {
                performative(TRANSFER, vec![V::Uint(handle), V::Uint(id), tag.clone(), V::Uint(0), V::Bool(l.presettled), V::Bool(more)])
            };
            // Room for payload once the performative is in the frame.
            let room = max_frame - 8 - head(true).len() - 8;
            let mut at = 0;
            let mut first = true;
            loop {
                let end = (at + room).min(payload.len());
                let more = end < payload.len();
                let mut body = if first {
                    head(more)
                } else {
                    performative(TRANSFER, vec![V::Uint(handle), V::Null, V::Null, V::Null, V::Bool(l.presettled), V::Bool(more)])
                };
                body.extend_from_slice(&payload[at..end]);
                let f = frame(0, channel, &body);
                if s.held.is_empty() && u64::from(s.next_outgoing_id) < s.remote_limit {
                    s.next_outgoing_id = s.next_outgoing_id.wrapping_add(1);
                    self.out.extend(f);
                } else {
                    s.held.push_back(f);
                }
                first = false;
                at = end;
                if !more {
                    break;
                }
            }
            if l.presettled {
                let _ = l.ch.basic_ack(d.delivery_tag, BasicAckOptions::default()).await;
            } else {
                s.unsettled.insert(id, (handle, l.gen, d.delivery_tag));
            }
        }
    }

    async fn on_transfer(&mut self, channel: u16, f: &V, payload: &[u8]) {
        let Some(s) = self.sessions.get_mut(&channel) else { return };
        s.next_incoming_id = s.next_incoming_id.wrapping_add(1);
        let handle = f.field(0).as_u32().unwrap_or(0);
        let Some(Link::In(l)) = s.links.get_mut(&handle) else {
            return self.fail("amqp:session:unattached-handle", "transfer on an unknown link");
        };
        let more = f.field(5).as_bool();
        if f.field(9).as_bool() {
            l.partial = None;
            return;
        }
        let partial = l.partial.get_or_insert_with(|| (Vec::new(), f.field(1).as_u32().unwrap_or(0), f.field(4).as_bool()));
        partial.0.extend_from_slice(payload);
        if more {
            return;
        }
        let Some((message, id, settled)) = l.partial.take() else { return };
        l.credit = l.credit.saturating_sub(1);
        l.delivery_count = l.delivery_count.wrapping_add(1);
        let refill = l.credit < LINK_CREDIT / 2;
        if refill {
            l.credit = LINK_CREDIT;
        }
        let target = l.target.clone();
        if refill {
            self.link_flow(channel, handle);
        }
        let outcome = self.publish(target, &message).await;
        if !settled {
            self.send(channel, performative(DISPOSITION, vec![V::Bool(true), V::Uint(id), V::Null, V::Bool(true), outcome]));
        }
    }

    /// Route one message. Returns the outcome to settle it with.
    async fn publish(&mut self, target: Option<Target>, payload: &[u8]) -> V {
        let rejected = |condition: &str, text: &str| V::described(REJECTED, V::List(vec![error(condition, text)]));
        let msg = match map::inbound(payload) {
            Ok(m) => m,
            Err(e) => return rejected("amqp:decode-error", &e),
        };
        let target = match target {
            Some(t) => t,
            None => match msg.to.as_deref().and_then(parse_address) {
                Some(Address::Queue(q)) => Target { exchange: String::new(), key: Some(q) },
                Some(Address::Exchange(t)) => {
                    if let Err(Refuse(c, m)) = self.check_exchange(&t.exchange).await {
                        return rejected(c, &m);
                    }
                    t
                }
                None => return rejected("amqp:invalid-field", &format!("'to' address '{}' is not valid", msg.to.unwrap_or_default())),
            },
        };
        let key = target.key.clone().or(msg.subject.clone()).unwrap_or_default();
        let options = BasicPublishOptions { mandatory: true, ..Default::default() };
        let sent = self.publish.basic_publish(&target.exchange, &key, options, &msg.body, msg.props).await;
        let outcome = match sent {
            Ok(confirm) => confirm.await.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        match outcome {
            Ok(Confirmation::Ack(Some(_))) => V::described(RELEASED, V::List(vec![])),
            Ok(Confirmation::Ack(None)) | Ok(Confirmation::NotRequested) => V::described(ACCEPTED, V::List(vec![])),
            Ok(Confirmation::Nack(_)) => rejected("amqp:internal-error", "the queue refused the message"),
            Err(e) => {
                // A refused publish closes the 0-9-1 channel; open a fresh one.
                if let Some(ch) = confirm_channel(&self.amqp).await {
                    self.publish = ch;
                }
                let condition = if e.contains("ACCESS_REFUSED") || e.contains("403") { "amqp:unauthorized-access" } else { "amqp:not-found" };
                rejected(condition, &e)
            }
        }
    }

    async fn on_disposition(&mut self, channel: u16, f: &V) {
        if !f.field(0).as_bool() {
            return;
        }
        let first = f.field(1).as_u32().unwrap_or(0);
        let last = f.field(2).as_u32().unwrap_or(first);
        let settled = f.field(3).as_bool();
        let state = f.field(4).clone();
        let code = state.code();
        let undeliverable = code == Some(MODIFIED) && state.field(1).as_bool();
        let mut touched = Vec::new();
        let Some(s) = self.sessions.get_mut(&channel) else { return };
        let mut id = first;
        loop {
            if let Some((handle, gen, tag)) = s.unsettled.remove(&id) {
                if let Some(Link::Out(l)) = s.links.get(&handle) {
                    if l.gen == gen {
                        let result = match code {
                            Some(ACCEPTED) => l.ch.basic_ack(tag, BasicAckOptions::default()).await,
                            Some(REJECTED) => l.ch.basic_nack(tag, BasicNackOptions { multiple: false, requeue: false }).await,
                            _ if undeliverable => l.ch.basic_nack(tag, BasicNackOptions { multiple: false, requeue: false }).await,
                            _ => l.ch.basic_nack(tag, BasicNackOptions { multiple: false, requeue: true }).await,
                        };
                        let _ = result;
                        touched.push(handle);
                    }
                }
            }
            if id == last {
                break;
            }
            id = id.wrapping_add(1);
        }
        if !settled {
            self.send(channel, performative(DISPOSITION, vec![V::Bool(false), V::Uint(first), V::Uint(last), V::Bool(true), state]));
        }
        for handle in touched {
            self.pump(channel, handle).await;
        }
    }

    /// Cancel a link's consumer. Closing its channel requeues what it held.
    async fn drop_link(&mut self, channel: u16, handle: u32) {
        let Some(s) = self.sessions.get_mut(&channel) else { return };
        let Some(link) = s.links.remove(&handle) else { return };
        if let Link::Out(l) = link {
            s.unsettled.retain(|_, (h, g, _)| !(*h == handle && *g == l.gen));
            let _ = l.ch.basic_cancel(&l.tag, BasicCancelOptions::default()).await;
            let _ = l.ch.close(200, "link detached").await;
        }
    }

    async fn end_session(&mut self, channel: u16) {
        let handles: Vec<u32> = self.sessions.get(&channel).map(|s| s.links.keys().copied().collect()).unwrap_or_default();
        for handle in handles {
            self.drop_link(channel, handle).await;
        }
        self.sessions.remove(&channel);
    }
}

/// A 0-9-1 refusal as an AMQP 1.0 error condition.
fn refusal(error: &str, name: &str) -> Refuse {
    if error.contains("ACCESS_REFUSED") || error.contains("403") {
        Refuse("amqp:unauthorized-access", format!("access to '{name}' refused: {error}"))
    } else if error.contains("RESOURCE_LOCKED") || error.contains("405") {
        Refuse("amqp:resource-locked", format!("'{name}' is locked: {error}"))
    } else {
        Refuse("amqp:not-found", format!("'{name}' was not found"))
    }
}
