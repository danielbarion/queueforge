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
//! messages for a persistent session. QoS 2 is served as QoS 1, as in
//! RabbitMQ 4. Retained messages are kept in memory per vhost and topic.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions, BasicQosOptions,
    ConfirmSelectOptions, QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
};
use lapin::types::{AMQPValue, ByteArray, FieldTable};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::oneshot;

use super::amqp_uri;

const TOPIC_EXCHANGE: &str = "amq.topic";
/// Unacked QoS 1 deliveries per client before the queue waits.
const INFLIGHT: u16 = 128;

#[derive(Clone)]
struct Retained {
    payload: Vec<u8>,
    qos: u8,
    props: Vec<u8>,
}

/// Live sessions by vhost and client id. Sending on the channel ends a session.
static LIVE: LazyLock<Mutex<HashMap<(String, String), oneshot::Sender<()>>>> = LazyLock::new(Default::default);
/// Retained messages by vhost, then topic.
static RETAINED: LazyLock<Mutex<HashMap<String, HashMap<String, Retained>>>> = LazyLock::new(Default::default);

fn topic_to_key(topic: &str) -> String {
    topic.replace('/', ".")
}

fn key_to_topic(key: &str) -> String {
    key.replace('.', "/")
}

fn filter_to_key(filter: &str) -> String {
    filter.split('/').map(|p| if p == "+" { "*" } else { p }).collect::<Vec<_>>().join(".")
}

fn key_to_filter(key: &str) -> String {
    key.split('.').map(|p| if p == "*" { "+" } else { p }).collect::<Vec<_>>().join("/")
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

struct Sub {
    filter: String,
    qos: u8,
    key: String,
}

struct Will {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    props: Vec<u8>,
}

struct Session {
    version: u8,
    vhost: String,
    client_id: String,
    clean: bool,
    queue: String,
    subs: Vec<Sub>,
    will: Option<Will>,
    conn: Connection,
    ch: Channel,
    consuming: Option<lapin::Consumer>,
    inflight: HashMap<u16, u64>,
    next_id: u16,
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
    let body: Vec<u8> = if version == 5 { vec![u8::from(present), code, 0] } else { vec![u8::from(present), code] };
    let _ = send(io, &packet(0x20, &body)).await;
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
    if version == 5 {
        r.props()?;
    }
    let mut client_id = r.string()?;
    let clean = flags & 0x02 != 0;
    let will = if flags & 0x04 != 0 {
        let props = if version == 5 { r.props()?.to_vec() } else { Vec::new() };
        let topic = r.string()?;
        let payload = r.binary()?.to_vec();
        Some(Will { topic, payload, qos: ((flags >> 3) & 3).min(1), retain: flags & 0x20 != 0, props })
    } else {
        None
    };
    let username = if flags & 0x80 != 0 { r.string()? } else { String::new() };
    let password = if flags & 0x40 != 0 { String::from_utf8_lossy(r.binary()?).into_owned() } else { String::new() };
    let (vhost, user) = match username.split_once(':') {
        Some((v, u)) => (if v.is_empty() { "/".to_string() } else { v.to_string() }, u.to_string()),
        None => ("/".to_string(), username),
    };
    let bad_login = if version == 5 { 0x86 } else { 4 };
    if user.is_empty() {
        connack(io, version, false, bad_login).await;
        return None;
    }
    if client_id.is_empty() {
        if !clean && version != 5 {
            connack(io, version, false, 2).await;
            return None;
        }
        client_id = format!("mqtt-{}", super::token());
    }
    let conn = match Connection::connect(&amqp_uri(amqp_port, &user, &password, &vhost), ConnectionProperties::default()).await {
        Ok(c) => c,
        Err(_) => {
            connack(io, version, false, bad_login).await;
            return None;
        }
    };
    let ch = conn.create_channel().await.ok()?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.ok()?;
    ch.basic_qos(INFLIGHT, BasicQosOptions::default()).await.ok()?;
    // A second connection with the same client id takes the session over.
    let (kick_tx, kick_rx) = oneshot::channel();
    let old = LIVE.lock().expect("mqtt live").insert((vhost.clone(), client_id.clone()), kick_tx);
    if let Some(old) = old {
        let _ = old.send(());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let queue = format!("mqtt-subscription-{client_id}qos1");
    let present = !clean
        && ch
            .queue_declare(&queue, QueueDeclareOptions { passive: true, ..Default::default() }, FieldTable::default())
            .await
            .is_ok();
    // A failed passive declare closes the channel; start a fresh one.
    let ch = if !clean && !present { conn.create_channel().await.ok()? } else { ch };
    if !clean && !present {
        ch.confirm_select(ConfirmSelectOptions::default()).await.ok()?;
        ch.basic_qos(INFLIGHT, BasicQosOptions::default()).await.ok()?;
    }
    if clean {
        let _ = ch.queue_delete(&queue, QueueDeleteOptions::default()).await;
    }
    let mut session = Session {
        version,
        vhost,
        client_id,
        clean,
        queue,
        subs: Vec::new(),
        will,
        conn,
        ch,
        consuming: None,
        inflight: HashMap::new(),
        next_id: 1,
    };
    connack(io, version, present, 0).await;
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
        .map(|keys| keys.iter().map(|k| Sub { filter: key_to_filter(k), qos: 1, key: k.clone() }).collect())
        .unwrap_or_default()
}

/// Binding keys of persistent sessions, so a reconnect knows its filters.
static STORED_SUBS: LazyLock<Mutex<HashMap<(String, String), Vec<String>>>> = LazyLock::new(Default::default);

async fn declare_and_consume(s: &mut Session) -> Result<(), ()> {
    if s.consuming.is_some() {
        return Ok(());
    }
    let opts = QueueDeclareOptions { durable: !s.clean, exclusive: s.clean, ..Default::default() };
    s.ch.queue_declare(&s.queue, opts, FieldTable::default()).await.map_err(|_| ())?;
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
            s.will = None;
            Ok(false)
        }
        _ => Err(()),
    }
}

async fn publish_from_client<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, first: u8, body: &[u8]) -> Result<(), ()> {
    let real_qos = (first >> 1) & 3;
    let retain = first & 1 != 0;
    let mut r = Reader::new(body);
    let topic = r.string().ok_or(())?;
    let id = if real_qos > 0 { r.u16().ok_or(())? } else { 0 };
    let props = if s.version == 5 { r.props().ok_or(())?.to_vec() } else { Vec::new() };
    let payload = r.bytes(r.left()).ok_or(())?.to_vec();
    if topic.is_empty() || topic.contains('+') || topic.contains('#') {
        return Err(());
    }
    let ok = publish(s, &topic, payload, real_qos.min(1), retain, props).await;
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
async fn publish(s: &mut Session, topic: &str, payload: Vec<u8>, qos: u8, retain: bool, props: Vec<u8>) -> bool {
    if retain {
        let mut all = RETAINED.lock().expect("retained");
        let map = all.entry(s.vhost.clone()).or_default();
        if payload.is_empty() {
            map.remove(topic);
            return true;
        }
        map.insert(topic.to_string(), Retained { payload: payload.clone(), qos, props: props.clone() });
    }
    let mut headers = FieldTable::default();
    headers.insert("x-mqtt-publish-qos".into(), AMQPValue::LongInt(i32::from(qos)));
    if !props.is_empty() {
        headers.insert("x-mqtt-props".into(), AMQPValue::ByteArray(ByteArray::from(props)));
    }
    let properties = BasicProperties::default().with_headers(headers).with_delivery_mode(if qos == 1 { 2 } else { 1 });
    let published = s
        .ch
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

async fn deliver<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, d: lapin::message::Delivery) -> Result<(), ()> {
    let topic = key_to_topic(d.routing_key.as_str());
    let sub_qos = s.subs.iter().filter(|x| mqtt_match(&x.filter, &topic)).map(|x| x.qos).max().unwrap_or(0);
    let headers = d.properties.headers().clone().unwrap_or_default();
    let msg_qos = match headers.inner().get("x-mqtt-publish-qos") {
        Some(AMQPValue::LongInt(n)) => (*n).clamp(0, 1) as u8,
        Some(AMQPValue::ShortShortInt(n)) => (*n).clamp(0, 1) as u8,
        Some(AMQPValue::LongLongInt(n)) => (*n).clamp(0, 1) as u8,
        _ => u8::from(*d.properties.delivery_mode() == Some(2)),
    };
    let props = match headers.inner().get("x-mqtt-props") {
        Some(AMQPValue::ByteArray(b)) => b.as_slice().to_vec(),
        _ => Vec::new(),
    };
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
    send(io, &publish_packet(s.version, &topic, &d.data, qos, false, d.redelivered && qos == 1, id, &props)).await?;
    if qos == 0 {
        let _ = s.ch.basic_ack(d.delivery_tag, BasicAckOptions::default()).await;
    }
    Ok(())
}

async fn subscribe<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, body: &[u8]) -> Result<(), ()> {
    let mut r = Reader::new(body);
    let id = r.u16().ok_or(())?;
    if s.version == 5 {
        r.props().ok_or(())?;
    }
    let mut codes = Vec::new();
    let mut added = Vec::new();
    declare_and_consume(s).await?;
    while r.left() > 0 {
        let filter = r.string().ok_or(())?;
        let options = r.u8().ok_or(())?;
        let qos = (options & 3).min(1);
        if filter.starts_with("$share/") {
            // RabbitMQ 4.3 does not support shared subscriptions either.
            codes.push(if s.version == 5 { 0x9e } else { 0x80 });
            continue;
        }
        let key = filter_to_key(&filter);
        match s.ch.queue_bind(&s.queue, TOPIC_EXCHANGE, &key, QueueBindOptions::default(), FieldTable::default()).await {
            Ok(()) => {
                s.subs.retain(|x| x.filter != filter);
                s.subs.push(Sub { filter: filter.clone(), qos, key: key.clone() });
                added.push((filter, qos));
                codes.push(qos);
            }
            Err(_) => {
                codes.push(if s.version == 5 { 0x87 } else { 0x80 });
                // A refused bind closes the channel; open another.
                if let Ok(ch) = s.conn.create_channel().await {
                    let _ = ch.confirm_select(ConfirmSelectOptions::default()).await;
                    let _ = ch.basic_qos(INFLIGHT, BasicQosOptions::default()).await;
                    s.ch = ch;
                    s.consuming = None;
                    s.inflight.clear();
                    declare_and_consume(s).await?;
                }
            }
        }
    }
    if !s.clean {
        STORED_SUBS
            .lock()
            .expect("mqtt subs")
            .insert((s.vhost.clone(), s.client_id.clone()), s.subs.iter().map(|x| x.key.clone()).collect());
    }
    let mut ack = id.to_be_bytes().to_vec();
    if s.version == 5 {
        ack.push(0);
    }
    ack.extend_from_slice(&codes);
    send(io, &packet(0x90, &ack)).await?;
    // Retained messages for each new filter, flagged as retained.
    let retained: Vec<(String, Retained)> = RETAINED
        .lock()
        .expect("retained")
        .get(&s.vhost)
        .map(|m| m.iter().map(|(t, r)| (t.clone(), r.clone())).collect())
        .unwrap_or_default();
    for (filter, _qos) in added {
        for (topic, msg) in &retained {
            if mqtt_match(&filter, topic) {
                send(io, &publish_packet(s.version, topic, &msg.payload, 0, true, false, 0, &msg.props)).await?;
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
    let mut ack = id.to_be_bytes().to_vec();
    if s.version == 5 {
        ack.push(0);
        ack.extend_from_slice(&codes);
    }
    send(io, &packet(0xb0, &ack)).await
}

/// End the session. A missing DISCONNECT publishes the will.
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
    if abnormal {
        if let Some(w) = s.will.take() {
            let _ = publish(&mut s, &w.topic, w.payload, w.qos, w.retain, w.props).await;
        }
    }
    if s.clean {
        let _ = s.ch.queue_delete(&s.queue, QueueDeleteOptions::default()).await;
    }
    let _ = s.conn.close(200, "mqtt session closed").await;
}
