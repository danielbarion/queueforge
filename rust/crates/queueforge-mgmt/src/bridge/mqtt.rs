//! MQTT 3.1, 3.1.1 and 5.0 over a loopback AMQP connection.
//!
//! RabbitMQ's MQTT plugin is an AMQP client of its own broker; this is the
//! same design. Each MQTT session logs in to the local AMQP listener with
//! the client's credentials, so the user store, permissions, routing,
//! confirms and acks all come from the AMQP path.
//!
//! A topic is a routing key on `amq.topic` (`/` becomes `.`, `+` becomes
//! `*`). Each client has one queue, `mqtt-subscription-<client id>qos1`: an
//! exclusive one for a clean session, a durable one that keeps QoS 1
//! messages for a persistent session (MQTT 5: a session expiry interval,
//! enforced with `x-expires` as RabbitMQ does). QoS 2 is served as QoS 1, as
//! in RabbitMQ 4. Retained messages are kept per vhost and topic in memory
//! and in the metadata store's parameters (component `mqtt_retained`), so
//! they survive a restart, as RabbitMQ's DETS store does.
//!
//! MQTT 5 message properties travel in the `x-mqtt-props` header. The
//! bridge rewrites them on delivery: the remaining message expiry interval,
//! the matching subscription identifiers, and no topic alias.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, ByteArray, FieldTable, LongString, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use queueforge_store::MetadataStore;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;

use super::amqp_uri;

const TOPIC_EXCHANGE: &str = "amq.topic";
/// Unacked QoS 1 deliveries per client before the queue waits.
const INFLIGHT: u16 = 128;
/// Topic aliases a client may set (CONNACK topic-alias-maximum).
const TOPIC_ALIAS_MAX: u16 = 16;
/// Parameter component the retained messages are stored under.
const RETAINED_COMPONENT: &str = "mqtt_retained";

// MQTT 5 property ids used here.
const P_EXPIRY: u8 = 0x02;
const P_SUB_ID: u8 = 0x0B;
const P_SESSION_EXPIRY: u8 = 0x11;
const P_ASSIGNED_ID: u8 = 0x12;
const P_WILL_DELAY: u8 = 0x18;
const P_ALIAS_MAX: u8 = 0x22;
const P_ALIAS: u8 = 0x23;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Retained {
    payload: Vec<u8>,
    qos: u8,
    props: Vec<u8>,
    /// Unix ms after which the message is gone (message expiry interval).
    expires_at_ms: Option<u64>,
}

impl Retained {
    /// `[qos][expires u64, 0 = never][props len u32][props][payload]`.
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(13 + self.props.len() + self.payload.len());
        out.push(self.qos);
        out.extend_from_slice(&self.expires_at_ms.unwrap_or(0).to_be_bytes());
        out.extend_from_slice(&(self.props.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.props);
        out.extend_from_slice(&self.payload);
        out
    }

    fn decode(b: &[u8]) -> Option<Self> {
        let qos = *b.first()?;
        let at = u64::from_be_bytes(b.get(1..9)?.try_into().ok()?);
        let n = u32::from_be_bytes(b.get(9..13)?.try_into().ok()?) as usize;
        let props = b.get(13..13 + n)?.to_vec();
        let payload = b.get(13 + n..)?.to_vec();
        Some(Self { payload, qos, props, expires_at_ms: (at != 0).then_some(at) })
    }

    fn live(&self, now: u64) -> bool {
        self.expires_at_ms.is_none_or(|at| at > now)
    }
}

/// Live sessions by vhost and client id. Sending on the channel ends a session.
static LIVE: LazyLock<Mutex<HashMap<(String, String), oneshot::Sender<()>>>> = LazyLock::new(Default::default);
/// Wills waiting out their delay interval. Sending cancels one (the client came back).
static PENDING_WILLS: LazyLock<Mutex<HashMap<(String, String), oneshot::Sender<()>>>> = LazyLock::new(Default::default);
/// Where retained messages are stored. Set once at startup.
static STORE: OnceLock<Arc<MetadataStore>> = OnceLock::new();
/// Retained messages by vhost, then topic, loaded from the store on first use.
static RETAINED: LazyLock<Mutex<HashMap<String, HashMap<String, Retained>>>> = LazyLock::new(|| {
    let mut all: HashMap<String, HashMap<String, Retained>> = HashMap::new();
    if let Some(store) = STORE.get() {
        for (vhost, topic, value) in store.list_parameters(RETAINED_COMPONENT).unwrap_or_default() {
            if let Some(r) = Retained::decode(&value) {
                all.entry(vhost).or_default().insert(topic, r);
            }
        }
    }
    Mutex::new(all)
});
/// One writer keeps retained-store updates in publish order.
static RETAINED_WRITER: LazyLock<std::sync::mpsc::Sender<(String, String, Option<Vec<u8>>)>> = LazyLock::new(|| {
    let (tx, rx) = std::sync::mpsc::channel::<(String, String, Option<Vec<u8>>)>();
    std::thread::Builder::new()
        .name("mqtt-retained".into())
        .spawn(move || {
            while let Ok((vhost, topic, value)) = rx.recv() {
                let Some(store) = STORE.get() else { continue };
                let result = match value {
                    Some(v) => store.put_parameter(RETAINED_COMPONENT, &vhost, &topic, &v),
                    None => store.delete_parameter(RETAINED_COMPONENT, &vhost, &topic),
                };
                if let Err(err) = result {
                    tracing::warn!(error = %err, "mqtt retained store write failed");
                }
            }
        })
        .expect("spawn mqtt retained writer");
    tx
});

/// Keep retained messages in `store` (and load the ones already there).
/// The first call wins.
pub fn set_store(store: Arc<MetadataStore>) {
    let _ = STORE.set(store);
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Set or clear (`None`) a retained message, in memory and on disk.
fn put_retained(vhost: &str, topic: &str, value: Option<Retained>) {
    {
        let mut all = RETAINED.lock().expect("retained");
        let map = all.entry(vhost.to_string()).or_default();
        match &value {
            Some(r) => {
                map.insert(topic.to_string(), r.clone());
            }
            None => {
                map.remove(topic);
            }
        }
    }
    if STORE.get().is_some() {
        let _ = RETAINED_WRITER.send((vhost.to_string(), topic.to_string(), value.map(|r| r.encode())));
    }
}

fn topic_to_key(topic: &str) -> String {
    topic.replace('/', ".")
}

fn key_to_topic(key: &str) -> String {
    key.replace('.', "/")
}

fn filter_to_key(filter: &str) -> String {
    filter.split('/').map(|p| if p == "+" { "*" } else { p }).collect::<Vec<_>>().join(".")
}

/// Whether an MQTT filter matches a topic.
pub fn mqtt_match(filter: &str, topic: &str) -> bool {
    let f: Vec<&str> = filter.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    for (i, part) in f.iter().enumerate() {
        if *part == "#" {
            return true;
        }
        if i >= t.len() {
            return false;
        }
        if *part != "+" && *part != t[i] {
            return false;
        }
    }
    f.len() == t.len()
}

fn varint(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

fn packet(first: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![first];
    varint(body.len(), &mut out);
    out.extend_from_slice(body);
    out
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// One MQTT 5 property value.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Prop {
    Byte(u8),
    U16(u16),
    U32(u32),
    Var(u32),
    /// A UTF-8 string or binary data: both are length-prefixed bytes.
    Bytes(Vec<u8>),
    Pair(Vec<u8>, Vec<u8>),
}

fn prop_kind(id: u8) -> Option<u8> {
    Some(match id {
        0x01 | 0x17 | 0x19 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => 0,
        0x13 | 0x21 | 0x22 | 0x23 => 1,
        0x02 | 0x11 | 0x18 | 0x27 => 2,
        0x0B => 3,
        0x03 | 0x08 | 0x09 | 0x12 | 0x15 | 0x16 | 0x1A | 0x1C | 0x1F => 4,
        0x26 => 5,
        _ => return None,
    })
}

/// Parse a property block (without its length prefix).
fn parse_props(b: &[u8]) -> Option<Vec<(u8, Prop)>> {
    let mut r = Reader::new(b);
    let mut out = Vec::new();
    while r.left() > 0 {
        let id = r.u8()?;
        let value = match prop_kind(id)? {
            0 => Prop::Byte(r.u8()?),
            1 => Prop::U16(r.u16()?),
            2 => Prop::U32(r.u32()?),
            3 => Prop::Var(r.varint()? as u32),
            4 => Prop::Bytes(r.binary()?.to_vec()),
            _ => Prop::Pair(r.binary()?.to_vec(), r.binary()?.to_vec()),
        };
        out.push((id, value));
    }
    Some(out)
}

fn encode_props(props: &[(u8, Prop)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (id, value) in props {
        out.push(*id);
        match value {
            Prop::Byte(v) => out.push(*v),
            Prop::U16(v) => out.extend_from_slice(&v.to_be_bytes()),
            Prop::U32(v) => out.extend_from_slice(&v.to_be_bytes()),
            Prop::Var(v) => varint(*v as usize, &mut out),
            Prop::Bytes(v) => {
                out.extend_from_slice(&(v.len() as u16).to_be_bytes());
                out.extend_from_slice(v);
            }
            Prop::Pair(k, v) => {
                out.extend_from_slice(&(k.len() as u16).to_be_bytes());
                out.extend_from_slice(k);
                out.extend_from_slice(&(v.len() as u16).to_be_bytes());
                out.extend_from_slice(v);
            }
        }
    }
    out
}

fn prop_u32(props: &[(u8, Prop)], id: u8) -> Option<u32> {
    props.iter().find(|(i, _)| *i == id).and_then(|(_, v)| match v {
        Prop::U32(n) | Prop::Var(n) => Some(*n),
        Prop::U16(n) => Some(u32::from(*n)),
        _ => None,
    })
}

/// A cursor over one packet body.
struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, at: 0 }
    }
    fn left(&self) -> usize {
        self.b.len().saturating_sub(self.at)
    }
    fn u8(&mut self) -> Option<u8> {
        let v = *self.b.get(self.at)?;
        self.at += 1;
        Some(v)
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from(self.u8()?) << 8 | u16::from(self.u8()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from(self.u16()?) << 16 | u32::from(self.u16()?))
    }
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(n)?;
        let out = self.b.get(self.at..end)?;
        self.at = end;
        Some(out)
    }
    fn binary(&mut self) -> Option<&'a [u8]> {
        let n = usize::from(self.u16()?);
        self.bytes(n)
    }
    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.binary()?.to_vec()).ok()
    }
    fn varint(&mut self) -> Option<usize> {
        let mut value = 0usize;
        let mut mult = 1usize;
        for _ in 0..4 {
            let byte = self.u8()?;
            value += usize::from(byte & 0x7f) * mult;
            if byte & 0x80 == 0 {
                return Some(value);
            }
            mult *= 128;
        }
        None
    }
    fn props(&mut self) -> Option<&'a [u8]> {
        let n = self.varint()?;
        self.bytes(n)
    }
}

#[derive(Clone)]
struct Sub {
    filter: String,
    qos: u8,
    key: String,
    /// MQTT 5 no-local: the client's own publishes do not come back.
    no_local: bool,
    /// MQTT 5 retain-as-published.
    retain_as_published: bool,
    /// MQTT 5 subscription identifier.
    id: Option<u32>,
}

struct Will {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    props: Vec<u8>,
    /// Will delay interval in seconds.
    delay: u32,
}

struct Session {
    version: u8,
    vhost: String,
    client_id: String,
    /// The queue goes when the connection does (no session to keep).
    clean: bool,
    /// MQTT 5 session expiry interval in seconds (0 for 3.x).
    expiry: u32,
    /// The queue already existed at CONNECT (session present).
    present: bool,
    queue: String,
    subs: Vec<Sub>,
    will: Option<Will>,
    conn: Connection,
    ch: Channel,
    consuming: Option<lapin::Consumer>,
    inflight: HashMap<u16, u64>,
    next_id: u16,
    /// Client-to-server topic aliases.
    aliases: HashMap<u16, String>,
}

impl Session {
    /// Identifies this client's publishes for no-local.
    fn origin(&self) -> String {
        format!("{}/{}", self.vhost, self.client_id)
    }

    /// `x-expires` for a persistent MQTT 5 session's queue.
    fn queue_args(&self) -> FieldTable {
        let mut args = FieldTable::default();
        if self.version == 5 && !self.clean && self.expiry != u32::MAX {
            args.insert("x-expires".into(), AMQPValue::LongLongInt(i64::from(self.expiry.max(1)) * 1000));
        }
        args
    }
}

/// Serve one MQTT client on `io` until it disconnects.
pub async fn serve<S>(mut io: S, amqp_port: u16)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let Some((first, body)) = read_packet(&mut io, &mut buf).await else { return };
    if first >> 4 != 1 {
        return;
    }
    let (mut session, kicked, keepalive) = match connect(&mut io, &body, amqp_port).await {
        Some(s) => s,
        None => return,
    };
    let mut kicked = kicked;
    let mut last_seen = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut tmp = vec![0u8; 16 * 1024];
    let mut abnormal = true;
    loop {
        // Packets already buffered first.
        while let Some((first, body, used)) = take_packet(&buf) {
            buf.drain(..used);
            last_seen = Instant::now();
            match handle(&mut session, &mut io, first, &body).await {
                Ok(true) => {}
                Ok(false) => {
                    abnormal = false;
                    return finish(session, abnormal).await;
                }
                Err(()) => return finish(session, abnormal).await,
            }
        }
        tokio::select! {
            _ = &mut kicked => {
                session.will = None;
                return finish(session, false).await;
            }
            read = io.read(&mut tmp) => {
                match read {
                    Ok(0) | Err(_) => return finish(session, abnormal).await,
                    Ok(n) => buf.extend_from_slice(&tmp[..n]),
                }
            }
            delivery = next_delivery(&mut session.consuming) => {
                match delivery {
                    Some(Ok(d)) => {
                        if deliver(&mut session, &mut io, d).await.is_err() {
                            return finish(session, abnormal).await;
                        }
                    }
                    _ => session.consuming = None,
                }
            }
            _ = tick.tick() => {
                if keepalive > 0 && last_seen.elapsed() > Duration::from_millis(u64::from(keepalive) * 1500) {
                    return finish(session, true).await;
                }
            }
        }
    }
}

async fn next_delivery(consumer: &mut Option<lapin::Consumer>) -> Option<Result<lapin::message::Delivery, lapin::Error>> {
    match consumer.as_mut() {
        Some(c) => c.next().await,
        None => std::future::pending().await,
    }
}

fn take_packet(buf: &[u8]) -> Option<(u8, Vec<u8>, usize)> {
    if buf.len() < 2 {
        return None;
    }
    let mut value = 0usize;
    let mut mult = 1usize;
    let mut i = 1;
    loop {
        let byte = *buf.get(i)?;
        value += usize::from(byte & 0x7f) * mult;
        i += 1;
        if byte & 0x80 == 0 {
            break;
        }
        if i > 4 {
            return None;
        }
        mult *= 128;
    }
    if buf.len() < i + value {
        return None;
    }
    Some((buf[0], buf[i..i + value].to_vec(), i + value))
}

async fn read_packet<S: AsyncRead + Unpin>(io: &mut S, buf: &mut Vec<u8>) -> Option<(u8, Vec<u8>)> {
    let mut tmp = [0u8; 4096];
    loop {
        if let Some((first, body, used)) = take_packet(buf) {
            buf.drain(..used);
            return Some((first, body));
        }
        let n = tokio::time::timeout(Duration::from_secs(10), io.read(&mut tmp)).await.ok()?.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn send<S: AsyncWrite + Unpin>(io: &mut S, bytes: &[u8]) -> Result<(), ()> {
    io.write_all(bytes).await.map_err(|_| ())?;
    io.flush().await.map_err(|_| ())
}

async fn connack<S: AsyncWrite + Unpin>(io: &mut S, version: u8, present: bool, code: u8) {
    connack_props(io, version, present, code, &[]).await;
}

async fn connack_props<S: AsyncWrite + Unpin>(io: &mut S, version: u8, present: bool, code: u8, props: &[(u8, Prop)]) {
    let mut body = vec![u8::from(present), code];
    if version == 5 {
        let block = encode_props(props);
        varint(block.len(), &mut body);
        body.extend_from_slice(&block);
    }
    let _ = send(io, &packet(0x20, &body)).await;
}

async fn new_channel(conn: &Connection) -> Option<Channel> {
    let ch = conn.create_channel().await.ok()?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.ok()?;
    ch.basic_qos(INFLIGHT, BasicQosOptions::default()).await.ok()?;
    Some(ch)
}

/// Parse CONNECT, log in over AMQP, and set the session up.
async fn connect<S>(io: &mut S, body: &[u8], amqp_port: u16) -> Option<(Session, oneshot::Receiver<()>, u16)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut r = Reader::new(body);
    let name = r.string()?;
    let version = r.u8()?;
    if !(name == "MQTT" || name == "MQIsdp") || ![3, 4, 5].contains(&version) {
        connack(io, 4, false, 1).await;
        return None;
    }
    let flags = r.u8()?;
    let keepalive = r.u16()?;
    let connect_props = if version == 5 { parse_props(r.props()?)? } else { Vec::new() };
    let mut client_id = r.string()?;
    let clean_start = flags & 0x02 != 0;
    let will = if flags & 0x04 != 0 {
        let raw = if version == 5 { r.props()?.to_vec() } else { Vec::new() };
        let mut props = parse_props(&raw).unwrap_or_default();
        let delay = prop_u32(&props, P_WILL_DELAY).unwrap_or(0);
        props.retain(|(id, _)| *id != P_WILL_DELAY);
        let topic = r.string()?;
        let payload = r.binary()?.to_vec();
        Some(Will {
            topic,
            payload,
            qos: ((flags >> 3) & 3).min(1),
            retain: flags & 0x20 != 0,
            props: encode_props(&props),
            delay,
        })
    } else {
        None
    };
    let username = if flags & 0x80 != 0 { r.string()? } else { String::new() };
    let password = if flags & 0x40 != 0 { String::from_utf8_lossy(r.binary()?).into_owned() } else { String::new() };
    let (vhost, user) = match username.split_once(':') {
        Some((v, u)) => (if v.is_empty() { "/".to_string() } else { v.to_string() }, u.to_string()),
        None => ("/".to_string(), username),
    };
    // mqtt.ssl_cert_login: without a username, the certificate names the user.
    let (user, password) = match (user.is_empty(), super::cert_login()) {
        (true, Some((cn, ticket))) => (cn, ticket),
        _ => (user, password),
    };
    let bad_login = if version == 5 { 0x86 } else { 4 };
    if user.is_empty() {
        connack(io, version, false, bad_login).await;
        return None;
    }
    let mut assigned = None;
    if client_id.is_empty() {
        if !clean_start && version != 5 {
            connack(io, version, false, 2).await;
            return None;
        }
        client_id = format!("mqtt-{}", super::token());
        assigned = Some(client_id.clone());
    }
    // MQTT 5 keeps the session for its expiry interval; 3.x for clean=false.
    let expiry = if version == 5 { prop_u32(&connect_props, P_SESSION_EXPIRY).unwrap_or(0) } else { 0 };
    let persistent = if version == 5 { expiry > 0 } else { !clean_start };
    let conn = match Connection::connect(&amqp_uri(amqp_port, &user, &password, &vhost), ConnectionProperties::default()).await {
        Ok(c) => c,
        Err(_) => {
            connack(io, version, false, bad_login).await;
            return None;
        }
    };
    let ch = new_channel(&conn).await?;
    // A second connection with the same client id takes the session over,
    // and a will still waiting out its delay is dropped.
    if let Some(cancel) = PENDING_WILLS.lock().expect("mqtt wills").remove(&(vhost.clone(), client_id.clone())) {
        let _ = cancel.send(());
    }
    let (kick_tx, kick_rx) = oneshot::channel();
    let old = LIVE.lock().expect("mqtt live").insert((vhost.clone(), client_id.clone()), kick_tx);
    if let Some(old) = old {
        let _ = old.send(());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let queue = format!("mqtt-subscription-{client_id}qos1");
    let present = !clean_start
        && ch
            .queue_declare(&queue, QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default())
            .await
            .is_ok();
    // A failed passive declare closes the channel; start a fresh one.
    let ch = if !clean_start && !present { new_channel(&conn).await? } else { ch };
    if clean_start {
        let _ = ch.queue_delete(&queue, QueueDeleteOptions::default()).await;
        STORED_SUBS.lock().expect("mqtt subs").remove(&(vhost.clone(), client_id.clone()));
    }
    let mut session = Session {
        version,
        vhost,
        client_id,
        clean: !persistent,
        expiry,
        present,
        queue,
        subs: Vec::new(),
        will,
        conn,
        ch,
        consuming: None,
        inflight: HashMap::new(),
        next_id: 1,
        aliases: HashMap::new(),
    };
    let mut props = vec![(P_ALIAS_MAX, Prop::U16(TOPIC_ALIAS_MAX))];
    if let Some(id) = assigned {
        props.push((P_ASSIGNED_ID, Prop::Bytes(id.into_bytes())));
    }
    connack_props(io, version, present, 0, &props).await;
    if present {
        // The persistent session's filters come back from its bindings: the
        // queue name is all the session state RabbitMQ keeps on its side too.
        session.subs = restore_subs(&session);
        if declare_and_consume(&mut session).await.is_err() {
            return None;
        }
    }
    Some((session, kick_rx, keepalive))
}

fn restore_subs(session: &Session) -> Vec<Sub> {
    STORED_SUBS
        .lock()
        .expect("mqtt subs")
        .get(&(session.vhost.clone(), session.client_id.clone()))
        .cloned()
        .unwrap_or_default()
}

/// Subscriptions of persistent sessions, so a reconnect knows its filters.
static STORED_SUBS: LazyLock<Mutex<HashMap<(String, String), Vec<Sub>>>> = LazyLock::new(Default::default);

async fn declare_and_consume(s: &mut Session) -> Result<(), ()> {
    if s.consuming.is_some() {
        return Ok(());
    }
    let opts = QueueDeclareOptions { durable: !s.clean, exclusive: s.clean, ..Default::default() };
    if s.ch.queue_declare(&s.queue, opts, s.queue_args()).await.is_err() {
        // A queue left by an earlier session with other settings: use it as it is.
        s.ch = new_channel(&s.conn).await.ok_or(())?;
        let passive = QueueDeclareOptions { passive: true, ..Default::default() };
        s.ch.queue_declare(&s.queue, passive, FieldTable::default()).await.map_err(|_| ())?;
    }
    let consumer = s
        .ch
        .basic_consume(&s.queue, &format!("mqtt-{}", s.client_id), BasicConsumeOptions::default(), FieldTable::default())
        .await
        .map_err(|_| ())?;
    s.consuming = Some(consumer);
    Ok(())
}

/// Handle one packet. `Ok(false)` is a clean DISCONNECT.
async fn handle<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, first: u8, body: &[u8]) -> Result<bool, ()> {
    match first >> 4 {
        3 => publish_from_client(s, io, first, body).await.map(|_| true),
        4 => {
            let id = u16::from(*body.first().ok_or(())?) << 8 | u16::from(*body.get(1).ok_or(())?);
            if let Some(tag) = s.inflight.remove(&id) {
                let _ = s.ch.basic_ack(tag, BasicAckOptions::default()).await;
            }
            Ok(true)
        }
        5 => send(io, &packet(0x62, body.get(..2).ok_or(())?)).await.map(|_| true),
        6 => send(io, &packet(0x70, body.get(..2).ok_or(())?)).await.map(|_| true),
        8 => subscribe(s, io, body).await.map(|_| true),
        10 => unsubscribe(s, io, body).await.map(|_| true),
        12 => send(io, &[0xd0, 0x00]).await.map(|_| true),
        14 => {
            // MQTT 5 reason 0x04 (disconnect with will message) keeps the will.
            if body.first() != Some(&0x04) {
                s.will = None;
            }
            Ok(false)
        }
        _ => Err(()),
    }
}

async fn publish_from_client<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, first: u8, body: &[u8]) -> Result<(), ()> {
    let real_qos = (first >> 1) & 3;
    let retain = first & 1 != 0;
    let mut r = Reader::new(body);
    let mut topic = r.string().ok_or(())?;
    let id = if real_qos > 0 { r.u16().ok_or(())? } else { 0 };
    let mut props = if s.version == 5 { parse_props(r.props().ok_or(())?).ok_or(())? } else { Vec::new() };
    let payload = r.bytes(r.left()).ok_or(())?.to_vec();
    // Topic alias: a topic sets it, an empty topic uses it.
    if let Some(alias) = prop_u32(&props, P_ALIAS) {
        let alias = alias as u16;
        if alias == 0 || alias > TOPIC_ALIAS_MAX {
            return Err(());
        }
        if topic.is_empty() {
            topic = s.aliases.get(&alias).cloned().ok_or(())?;
        } else {
            s.aliases.insert(alias, topic.clone());
        }
        props.retain(|(id, _)| *id != P_ALIAS);
    }
    if topic.is_empty() || topic.contains('+') || topic.contains('#') {
        return Err(());
    }
    let origin = s.origin();
    let ok = publish(&s.ch, &s.vhost, &origin, &topic, payload, real_qos.min(1), retain, props).await;
    if !ok {
        if s.version == 5 && real_qos > 0 {
            return send(io, &packet(0x40, &[(id >> 8) as u8, id as u8, 0x87, 0])).await;
        }
        return Err(());
    }
    match real_qos {
        1 => send(io, &packet(0x40, &[(id >> 8) as u8, id as u8])).await,
        2 => send(io, &packet(0x50, &[(id >> 8) as u8, id as u8])).await,
        _ => Ok(()),
    }
}

/// Publish to amq.topic and keep a retained copy. False when the broker refused.
#[allow(clippy::too_many_arguments)]
async fn publish(
    ch: &Channel,
    vhost: &str,
    origin: &str,
    topic: &str,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    mut props: Vec<(u8, Prop)>,
) -> bool {
    // Message expiry interval: an absolute deadline travels with the message,
    // and the AMQP per-message TTL drops it from queues.
    let expiry = prop_u32(&props, P_EXPIRY);
    props.retain(|(id, _)| *id != P_EXPIRY);
    let expires_at_ms = expiry.map(|secs| now_ms() + u64::from(secs) * 1000);
    let encoded = encode_props(&props);
    if retain {
        if payload.is_empty() {
            put_retained(vhost, topic, None);
            return true;
        }
        put_retained(vhost, topic, Some(Retained { payload: payload.clone(), qos, props: encoded.clone(), expires_at_ms }));
    }
    let mut headers = FieldTable::default();
    headers.insert("x-mqtt-publish-qos".into(), AMQPValue::LongInt(i32::from(qos)));
    headers.insert("x-mqtt-origin".into(), AMQPValue::LongString(LongString::from(origin.as_bytes().to_vec())));
    if retain {
        headers.insert("x-mqtt-retain".into(), AMQPValue::Boolean(true));
    }
    if let Some(at) = expires_at_ms {
        headers.insert("x-mqtt-expires-at".into(), AMQPValue::LongLongInt(at as i64));
    }
    if !encoded.is_empty() {
        headers.insert("x-mqtt-props".into(), AMQPValue::ByteArray(ByteArray::from(encoded)));
    }
    let mut properties = BasicProperties::default().with_headers(headers).with_delivery_mode(if qos == 1 { 2 } else { 1 });
    if let Some(secs) = expiry {
        properties = properties.with_expiration(ShortString::from((u64::from(secs) * 1000).to_string()));
    }
    let published = ch
        .basic_publish(TOPIC_EXCHANGE, &topic_to_key(topic), BasicPublishOptions::default(), &payload, properties)
        .await;
    match published {
        Ok(confirm) => match confirm.await {
            Ok(c) => !c.is_nack(),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_packet(version: u8, topic: &str, payload: &[u8], qos: u8, retain: bool, dup: bool, id: u16, props: &[u8]) -> Vec<u8> {
    let first = 0x30 | if dup { 0x08 } else { 0 } | (qos << 1) | u8::from(retain);
    let mut body = Vec::with_capacity(topic.len() + payload.len() + 8);
    put_str(&mut body, topic);
    if qos > 0 {
        body.extend_from_slice(&id.to_be_bytes());
    }
    if version == 5 {
        varint(props.len(), &mut body);
        body.extend_from_slice(props);
    }
    body.extend_from_slice(payload);
    packet(first, &body)
}

/// Properties for one delivery: the stored ones, the remaining expiry
/// interval, and the identifiers of the matching subscriptions.
fn delivery_props(stored: &[u8], expires_at_ms: Option<u64>, now: u64, subs: &[&Sub]) -> Vec<u8> {
    let mut props = parse_props(stored).unwrap_or_default();
    props.retain(|(id, _)| *id != P_EXPIRY && *id != P_SUB_ID && *id != P_ALIAS);
    if let Some(at) = expires_at_ms {
        props.push((P_EXPIRY, Prop::U32(at.saturating_sub(now).div_ceil(1000) as u32)));
    }
    for sub in subs {
        if let Some(id) = sub.id {
            props.push((P_SUB_ID, Prop::Var(id)));
        }
    }
    encode_props(&props)
}

fn header_i64(headers: &FieldTable, name: &str) -> Option<i64> {
    match headers.inner().get(name)? {
        AMQPValue::LongInt(n) => Some(i64::from(*n)),
        AMQPValue::ShortShortInt(n) => Some(i64::from(*n)),
        AMQPValue::LongLongInt(n) => Some(*n),
        AMQPValue::Timestamp(n) => Some(*n as i64),
        _ => None,
    }
}

async fn deliver<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, d: lapin::message::Delivery) -> Result<(), ()> {
    let topic = key_to_topic(d.routing_key.as_str());
    let headers = d.properties.headers().clone().unwrap_or_default();
    let own = match headers.inner().get("x-mqtt-origin") {
        Some(AMQPValue::LongString(o)) => o.as_bytes() == s.origin().as_bytes(),
        _ => false,
    };
    let now = now_ms();
    let expires_at_ms = header_i64(&headers, "x-mqtt-expires-at").map(|n| n.max(0) as u64);
    let all: Vec<&Sub> = s.subs.iter().filter(|x| mqtt_match(&x.filter, &topic)).collect();
    let matching: Vec<&Sub> = all.iter().copied().filter(|x| !(own && x.no_local)).collect();
    // Only no-local subscriptions match the client's own publish, or the
    // message expired on the way: settle it without sending.
    if (!all.is_empty() && matching.is_empty()) || expires_at_ms.is_some_and(|at| at <= now) {
        let _ = s.ch.basic_ack(d.delivery_tag, BasicAckOptions::default()).await;
        return Ok(());
    }
    let sub_qos = matching.iter().map(|x| x.qos).max().unwrap_or(0);
    let msg_qos = match header_i64(&headers, "x-mqtt-publish-qos") {
        Some(n) => n.clamp(0, 1) as u8,
        None => u8::from(*d.properties.delivery_mode() == Some(2)),
    };
    let stored = match headers.inner().get("x-mqtt-props") {
        Some(AMQPValue::ByteArray(b)) => b.as_slice().to_vec(),
        _ => Vec::new(),
    };
    let retained = matches!(headers.inner().get("x-mqtt-retain"), Some(AMQPValue::Boolean(true)));
    let retain = retained && matching.iter().any(|x| x.retain_as_published);
    let props = if s.version == 5 { delivery_props(&stored, expires_at_ms, now, &matching) } else { Vec::new() };
    let qos = sub_qos.min(msg_qos);
    let mut id = 0;
    if qos == 1 {
        for _ in 0..u16::MAX {
            id = s.next_id;
            s.next_id = if s.next_id == u16::MAX { 1 } else { s.next_id + 1 };
            if !s.inflight.contains_key(&id) {
                break;
            }
        }
        s.inflight.insert(id, d.delivery_tag);
    }
    send(io, &publish_packet(s.version, &topic, &d.data, qos, retain, d.redelivered && qos == 1, id, &props)).await?;
    if qos == 0 {
        let _ = s.ch.basic_ack(d.delivery_tag, BasicAckOptions::default()).await;
    }
    Ok(())
}

async fn subscribe<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, body: &[u8]) -> Result<(), ()> {
    let mut r = Reader::new(body);
    let id = r.u16().ok_or(())?;
    let sub_id = if s.version == 5 { prop_u32(&parse_props(r.props().ok_or(())?).ok_or(())?, P_SUB_ID) } else { None };
    let mut codes = Vec::new();
    // (filter, retain handling, was already subscribed)
    let mut added: Vec<(String, u8, bool)> = Vec::new();
    declare_and_consume(s).await?;
    while r.left() > 0 {
        let filter = r.string().ok_or(())?;
        let options = r.u8().ok_or(())?;
        let qos = (options & 3).min(1);
        let (no_local, rap, rh) = if s.version == 5 {
            (options & 0x04 != 0, options & 0x08 != 0, (options >> 4) & 3)
        } else {
            (false, false, 0)
        };
        if filter.starts_with("$share/") {
            // RabbitMQ 4.3 does not support shared subscriptions either.
            codes.push(if s.version == 5 { 0x9e } else { 0x80 });
            continue;
        }
        let key = filter_to_key(&filter);
        match s.ch.queue_bind(&s.queue, TOPIC_EXCHANGE, &key, QueueBindOptions::default(), FieldTable::default()).await {
            Ok(()) => {
                let existed = s.subs.iter().any(|x| x.filter == filter);
                s.subs.retain(|x| x.filter != filter);
                s.subs.push(Sub { filter: filter.clone(), qos, key: key.clone(), no_local, retain_as_published: rap, id: sub_id });
                added.push((filter, rh, existed));
                codes.push(qos);
            }
            Err(_) => {
                codes.push(if s.version == 5 { 0x87 } else { 0x80 });
                // A refused bind closes the channel; open another.
                if let Some(ch) = new_channel(&s.conn).await {
                    s.ch = ch;
                    s.consuming = None;
                    s.inflight.clear();
                    declare_and_consume(s).await?;
                }
            }
        }
    }
    if !s.clean {
        STORED_SUBS.lock().expect("mqtt subs").insert((s.vhost.clone(), s.client_id.clone()), s.subs.clone());
    }
    let mut ack = id.to_be_bytes().to_vec();
    if s.version == 5 {
        ack.push(0);
    }
    ack.extend_from_slice(&codes);
    send(io, &packet(0x90, &ack)).await?;
    // Retained messages for each new filter, flagged as retained. Retain
    // handling 1 sends them only for a new subscription, 2 never.
    let now = now_ms();
    let retained: Vec<(String, Retained)> = RETAINED
        .lock()
        .expect("retained")
        .get(&s.vhost)
        .map(|m| m.iter().filter(|(_, r)| r.live(now)).map(|(t, r)| (t.clone(), r.clone())).collect())
        .unwrap_or_default();
    for (filter, rh, existed) in added {
        if rh == 2 || (rh == 1 && existed) {
            continue;
        }
        let Some(sub) = s.subs.iter().find(|x| x.filter == filter) else { continue };
        for (topic, msg) in &retained {
            if mqtt_match(&filter, topic) {
                let props = if s.version == 5 { delivery_props(&msg.props, msg.expires_at_ms, now, &[sub]) } else { Vec::new() };
                send(io, &publish_packet(s.version, topic, &msg.payload, 0, true, false, 0, &props)).await?;
            }
        }
    }
    Ok(())
}

async fn unsubscribe<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, body: &[u8]) -> Result<(), ()> {
    let mut r = Reader::new(body);
    let id = r.u16().ok_or(())?;
    if s.version == 5 {
        r.props().ok_or(())?;
    }
    let mut codes = Vec::new();
    while r.left() > 0 {
        let filter = r.string().ok_or(())?;
        match s.subs.iter().position(|x| x.filter == filter) {
            Some(at) => {
                let sub = s.subs.remove(at);
                let _ = s.ch.queue_unbind(&s.queue, TOPIC_EXCHANGE, &sub.key, FieldTable::default()).await;
                codes.push(0);
            }
            None => codes.push(0x11),
        }
    }
    if !s.clean {
        STORED_SUBS.lock().expect("mqtt subs").insert((s.vhost.clone(), s.client_id.clone()), s.subs.clone());
    }
    let mut ack = id.to_be_bytes().to_vec();
    if s.version == 5 {
        ack.push(0);
        ack.extend_from_slice(&codes);
    }
    send(io, &packet(0xb0, &ack)).await
}

/// End the session. A missing DISCONNECT publishes the will, after its
/// delay interval when it has one (capped by the session expiry interval),
/// unless the client reconnects first.
async fn finish(mut s: Session, abnormal: bool) {
    {
        let mut live = LIVE.lock().expect("mqtt live");
        let key = (s.vhost.clone(), s.client_id.clone());
        if live.get(&key).is_some_and(|tx| tx.is_closed()) {
            live.remove(&key);
        }
    }
    // Unacked QoS 1 deliveries go back to the queue for the next session.
    for tag in s.inflight.values() {
        let _ = s.ch.basic_nack(*tag, BasicNackOptions { requeue: true, ..Default::default() }).await;
    }
    if s.clean {
        let _ = s.ch.queue_delete(&s.queue, QueueDeleteOptions::default()).await;
    }
    let will = if abnormal { s.will.take() } else { None };
    let origin = s.origin();
    let Some(w) = will else {
        let _ = s.conn.close(200, "mqtt session closed").await;
        return;
    };
    let props = parse_props(&w.props).unwrap_or_default();
    if w.delay == 0 {
        let _ = publish(&s.ch, &s.vhost, &origin, &w.topic, w.payload, w.qos, w.retain, props).await;
        let _ = s.conn.close(200, "mqtt session closed").await;
        return;
    }
    let wait = if s.version == 5 { w.delay.min(s.expiry) } else { w.delay };
    let (cancel_tx, cancel_rx) = oneshot::channel();
    PENDING_WILLS.lock().expect("mqtt wills").insert((s.vhost.clone(), s.client_id.clone()), cancel_tx);
    let key = (s.vhost.clone(), s.client_id.clone());
    tokio::spawn(async move {
        let cancelled = tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(u64::from(wait))) => false,
            _ = cancel_rx => true,
        };
        if !cancelled {
            PENDING_WILLS.lock().expect("mqtt wills").remove(&key);
            let _ = publish(&s.ch, &s.vhost, &origin, &w.topic, w.payload, w.qos, w.retain, props).await;
        }
        let _ = s.conn.close(200, "mqtt session closed").await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_round_trips() {
        let r = Retained { payload: b"on".to_vec(), qos: 1, props: vec![3, 0, 1, b'x'], expires_at_ms: Some(42) };
        assert_eq!(Retained::decode(&r.encode()), Some(r.clone()));
        let never = Retained { expires_at_ms: None, ..r };
        assert_eq!(Retained::decode(&never.encode()), Some(never));
    }

    #[test]
    fn props_round_trip_and_rewrite() {
        let props = vec![
            (0x01, Prop::Byte(1)),
            (P_EXPIRY, Prop::U32(60)),
            (0x03, Prop::Bytes(b"text/plain".to_vec())),
            (P_SUB_ID, Prop::Var(300)),
            (0x26, Prop::Pair(b"k".to_vec(), b"v".to_vec())),
        ];
        let encoded = encode_props(&props);
        assert_eq!(parse_props(&encoded), Some(props));
        let sub = Sub { filter: "a".into(), qos: 1, key: "a".into(), no_local: false, retain_as_published: false, id: Some(42) };
        let out = parse_props(&delivery_props(&encoded, Some(10_500), 1_000, &[&sub])).unwrap();
        assert!(out.contains(&(P_EXPIRY, Prop::U32(10))));
        assert!(out.contains(&(P_SUB_ID, Prop::Var(42))));
        assert!(!out.contains(&(P_SUB_ID, Prop::Var(300))));
    }

    #[test]
    fn retained_messages_survive_a_restart_through_the_store() {
        let dir = std::env::temp_dir().join(format!("qf-mqtt-retained-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.clone();
        {
            let store = MetadataStore::open(&path).unwrap();
            let r = Retained { payload: b"kept".to_vec(), qos: 1, props: Vec::new(), expires_at_ms: None };
            store.put_parameter(RETAINED_COMPONENT, "/", "home/door", &r.encode()).unwrap();
        }
        // A new process opens the same store: the retained map loads from it.
        let store = Arc::new(MetadataStore::open(&path).unwrap());
        set_store(store);
        let got = RETAINED.lock().unwrap().get("/").and_then(|m| m.get("home/door").cloned());
        assert_eq!(got.map(|r| r.payload), Some(b"kept".to_vec()));
        // Updates go back to disk.
        put_retained("/", "home/light", Some(Retained { payload: b"on".to_vec(), qos: 0, props: Vec::new(), expires_at_ms: None }));
        put_retained("/", "home/door", None);
        let store = STORE.get().unwrap();
        let mut stored = Vec::new();
        for _ in 0..100 {
            stored = store.list_parameters(RETAINED_COMPONENT).unwrap();
            if stored.len() == 1 && stored[0].1 == "home/light" {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].1, "home/light");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
