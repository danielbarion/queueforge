//! STOMP 1.0, 1.1 and 1.2 over a loopback AMQP connection, as RabbitMQ's
//! STOMP plugin does it.
//!
//! Destinations: `/queue/<name>` (durable, declared on first use),
//! `/topic/<key>` (`amq.topic`; each subscription gets an exclusive queue),
//! `/exchange/<name>/<key>`, and `/amq/queue/<name>` (an existing queue).
//! Each subscription has its own channel so prefetch and cancel are per
//! subscription. ACK and NACK settle on that channel; `client` mode acks
//! cumulatively. BEGIN, COMMIT and ABORT hold SEND, ACK and NACK until
//! commit. Any frame with a `receipt` header gets a RECEIPT.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
    BasicQosOptions, ConfirmSelectOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use super::amqp_uri;

struct Frame {
    command: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Frame {
    fn get(&self, k: &str) -> Option<&str> {
        self.headers.iter().find(|(h, _)| h == k).map(|(_, v)| v.as_str())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum AckMode {
    Auto,
    Client,
    Individual,
}

struct Sub {
    ch: Channel,
    tag: String,
    mode: AckMode,
    /// Unacked deliveries in order: message id to delivery tag.
    pending: BTreeMap<u64, (String, u64)>,
}

/// A delivery forwarded from a subscription task to the writer.
struct Out {
    sub: String,
    seq: u64,
    message_id: String,
    tag: u64,
    frame: Vec<u8>,
}

/// Headers that describe the STOMP frame, not the message.
const FRAME_HEADERS: &[&str] = &[
    "destination", "receipt", "transaction", "content-length", "content-type", "persistent", "priority",
    "expiration", "reply-to", "correlation-id", "message-id", "ack", "id", "subscription", "prefetch-count",
];

fn unescape(v: &str, version: &str) -> String {
    if version == "1.0" {
        return v.to_string();
    }
    let mut out = String::with_capacity(v.len());
    let mut chars = v.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('c') => out.push(':'),
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

fn escape(v: &str, version: &str) -> String {
    if version == "1.0" {
        return v.to_string();
    }
    v.replace('\\', "\\\\").replace('\n', "\\n").replace('\r', "\\r").replace(':', "\\c")
}

fn render(command: &str, headers: &[(String, String)], body: &[u8], version: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + body.len());
    out.extend_from_slice(command.as_bytes());
    out.push(b'\n');
    for (k, v) in headers {
        out.extend_from_slice(escape(k, version).as_bytes());
        out.push(b':');
        out.extend_from_slice(escape(v, version).as_bytes());
        out.push(b'\n');
    }
    out.push(b'\n');
    out.extend_from_slice(body);
    out.push(0);
    out
}

/// One frame from the front of `buf`, or `None` until it is complete.
fn parse(buf: &mut Vec<u8>, version: &str) -> Option<Frame> {
    let skip = buf.iter().take_while(|b| **b == b'\n' || **b == b'\r').count();
    if skip > 0 {
        buf.drain(..skip);
    }
    let head_end = buf.windows(2).position(|w| w == b"\n\n").map(|p| (p, p + 2)).or_else(|| buf.windows(3).position(|w| w == b"\n\r\n").map(|p| (p, p + 3)))?;
    let head = String::from_utf8_lossy(&buf[..head_end.0]).into_owned();
    let mut lines = head.split('\n').map(|l| l.trim_end_matches('\r'));
    let command = lines.next().unwrap_or_default().to_string();
    let raw = command == "CONNECT" || command == "STOMP";
    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        let Some((k, v)) = line.split_once(':') else { continue };
        let (k, v) = if raw { (k.to_string(), v.to_string()) } else { (unescape(k, version), unescape(v, version)) };
        // The first occurrence of a repeated header wins.
        if !headers.iter().any(|(h, _)| *h == k) {
            headers.push((k, v));
        }
    }
    let start = head_end.1;
    let length = headers.iter().find(|(k, _)| k == "content-length").and_then(|(_, v)| v.parse::<usize>().ok());
    let end = match length {
        Some(n) => {
            if buf.len() < start + n + 1 {
                return None;
            }
            start + n
        }
        None => start + buf[start..].iter().position(|b| *b == 0)?,
    };
    let body = buf[start..end].to_vec();
    buf.drain(..=end);
    Some(Frame { command, headers, body })
}

struct Session {
    version: String,
    conn: Connection,
    ch: Channel,
    subs: HashMap<String, Sub>,
    txs: HashMap<String, Vec<Frame>>,
    out_tx: mpsc::UnboundedSender<Out>,
    next_seq: u64,
}

/// Serve one STOMP client on `io` until it disconnects.
pub async fn serve<S>(mut io: S, amqp_port: u16)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut tmp = vec![0u8; 16 * 1024];
    let Some(first) = read_frame(&mut io, &mut buf, &mut tmp).await else { return };
    let Some(mut s) = login(&mut io, first, amqp_port).await else { return };
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Out>();
    s.out_tx = out_tx;
    loop {
        while let Some(frame) = parse(&mut buf, &s.version) {
            match handle(&mut s, &mut io, frame).await {
                Ok(true) => {}
                Ok(false) | Err(()) => return finish(s).await,
            }
        }
        tokio::select! {
            read = io.read(&mut tmp) => match read {
                Ok(0) | Err(_) => return finish(s).await,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
            },
            Some(out) = out_rx.recv() => {
                if let Some(sub) = s.subs.get_mut(&out.sub) {
                    if sub.mode != AckMode::Auto {
                        sub.pending.insert(out.seq, (out.message_id, out.tag));
                    }
                    if send(&mut io, &out.frame).await.is_err() {
                        return finish(s).await;
                    }
                }
            }
        }
    }
}

async fn read_frame<S: AsyncRead + Unpin>(io: &mut S, buf: &mut Vec<u8>, tmp: &mut [u8]) -> Option<Frame> {
    loop {
        if let Some(f) = parse(buf, "1.0") {
            return Some(f);
        }
        let n = tokio::time::timeout(Duration::from_secs(10), io.read(tmp)).await.ok()?.ok()?;
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

async fn error<S: AsyncWrite + Unpin>(io: &mut S, version: &str, message: &str, receipt: Option<&str>) {
    let mut headers = vec![("message".to_string(), message.to_string()), ("content-type".to_string(), "text/plain".to_string())];
    if let Some(r) = receipt {
        headers.push(("receipt-id".to_string(), r.to_string()));
    }
    let _ = send(io, &render("ERROR", &headers, message.as_bytes(), version)).await;
}

async fn login<S: AsyncWrite + Unpin>(io: &mut S, f: Frame, amqp_port: u16) -> Option<Session> {
    if f.command != "CONNECT" && f.command != "STOMP" {
        error(io, "1.2", "not connected", None).await;
        return None;
    }
    let accept = f.get("accept-version").unwrap_or("1.0");
    let version = if accept.split(',').any(|v| v == "1.2") {
        "1.2"
    } else if accept.split(',').any(|v| v == "1.1") {
        "1.1"
    } else {
        "1.0"
    }
    .to_string();
    let vhost = f.get("host").filter(|h| !h.is_empty() && *h != "localhost").unwrap_or("/").to_string();
    let user = f.get("login").unwrap_or_default().to_string();
    let pass = f.get("passcode").unwrap_or_default().to_string();
    let conn = match Connection::connect(&amqp_uri(amqp_port, &user, &pass, &vhost), ConnectionProperties::default()).await {
        Ok(c) if !user.is_empty() => c,
        _ => {
            error(io, &version, "Access refused", None).await;
            return None;
        }
    };
    let ch = conn.create_channel().await.ok()?;
    ch.confirm_select(ConfirmSelectOptions::default()).await.ok()?;
    let headers = vec![
        ("version".to_string(), version.clone()),
        ("server".to_string(), "QueueForge".to_string()),
        ("heart-beat".to_string(), "0,0".to_string()),
        ("session".to_string(), format!("session-{}", super::token())),
    ];
    send(io, &render("CONNECTED", &headers, &[], &version)).await.ok()?;
    let (out_tx, _) = mpsc::unbounded_channel();
    Some(Session { version, conn, ch, subs: HashMap::new(), txs: HashMap::new(), out_tx, next_seq: 1 })
}

/// Handle one frame. `Ok(false)` ends the session.
async fn handle<S: AsyncWrite + Unpin>(s: &mut Session, io: &mut S, f: Frame) -> Result<bool, ()> {
    let receipt = f.get("receipt").map(str::to_string);
    let result = match f.command.as_str() {
        "SEND" | "ACK" | "NACK" if f.get("transaction").is_some() => {
            let tx = f.get("transaction").unwrap_or_default().to_string();
            match s.txs.get_mut(&tx) {
                Some(ops) => {
                    ops.push(f);
                    Ok(())
                }
                None => Err(format!("transaction {tx} is not active")),
            }
        }
        "SEND" => on_send(s, &f).await,
        "ACK" => on_ack(s, &f, false).await,
        "NACK" => on_ack(s, &f, true).await,
        "SUBSCRIBE" => on_subscribe(s, &f).await,
        "UNSUBSCRIBE" => on_unsubscribe(s, &f).await,
        "BEGIN" => match f.get("transaction") {
            Some(tx) => {
                s.txs.insert(tx.to_string(), Vec::new());
                Ok(())
            }
            None => Err("missing transaction header".into()),
        },
        "COMMIT" => {
            let tx = f.get("transaction").unwrap_or_default().to_string();
            match s.txs.remove(&tx) {
                Some(ops) => {
                    let mut out = Ok(());
                    for op in ops {
                        out = match op.command.as_str() {
                            "SEND" => on_send(s, &op).await,
                            "ACK" => on_ack(s, &op, false).await,
                            _ => on_ack(s, &op, true).await,
                        };
                        if out.is_err() {
                            break;
                        }
                    }
                    out
                }
                None => Err(format!("transaction {tx} is not active")),
            }
        }
        "ABORT" => {
            let tx = f.get("transaction").unwrap_or_default().to_string();
            if s.txs.remove(&tx).is_some() { Ok(()) } else { Err(format!("transaction {tx} is not active")) }
        }
        "DISCONNECT" => {
            if let Some(r) = &receipt {
                let _ = send(io, &render("RECEIPT", &[("receipt-id".into(), r.clone())], &[], &s.version)).await;
            }
            return Ok(false);
        }
        other => Err(format!("unknown command {other}")),
    };
    match result {
        Ok(()) => {
            if let Some(r) = receipt {
                send(io, &render("RECEIPT", &[("receipt-id".into(), r)], &[], &s.version)).await?;
            }
            Ok(true)
        }
        Err(message) => {
            error(io, &s.version, &message, receipt.as_deref()).await;
            Ok(false)
        }
    }
}

/// Exchange, routing key, and the queue to declare first, for a SEND destination.
fn target(dest: &str) -> Result<(String, String, Option<String>), String> {
    if let Some(q) = dest.strip_prefix("/queue/") {
        return Ok((String::new(), q.to_string(), Some(q.to_string())));
    }
    if let Some(q) = dest.strip_prefix("/amq/queue/") {
        return Ok((String::new(), q.to_string(), None));
    }
    if let Some(k) = dest.strip_prefix("/topic/") {
        return Ok(("amq.topic".into(), k.to_string(), None));
    }
    if let Some(rest) = dest.strip_prefix("/exchange/") {
        return Ok(match rest.split_once('/') {
            Some((x, k)) => (x.to_string(), k.to_string(), None),
            None => (rest.to_string(), String::new(), None),
        });
    }
    Err(format!("unknown destination '{dest}'"))
}

async fn reopen(s: &mut Session) {
    if s.ch.status().connected() {
        return;
    }
    if let Ok(ch) = s.conn.create_channel().await {
        let _ = ch.confirm_select(ConfirmSelectOptions::default()).await;
        s.ch = ch;
    }
}

async fn on_send(s: &mut Session, f: &Frame) -> Result<(), String> {
    let dest = f.get("destination").ok_or("missing destination header")?;
    let (exchange, key, declare) = target(dest)?;
    if let Some(q) = declare {
        let opts = QueueDeclareOptions { durable: true, ..Default::default() };
        if let Err(e) = s.ch.queue_declare(&q, opts, FieldTable::default()).await {
            reopen(s).await;
            return Err(e.to_string());
        }
    }
    let mut headers = FieldTable::default();
    for (k, v) in &f.headers {
        if !FRAME_HEADERS.contains(&k.as_str()) {
            headers.insert(k.as_str().into(), AMQPValue::LongString(LongString::from(v.as_bytes().to_vec())));
        }
    }
    let persistent = f.get("persistent") == Some("true");
    let mut props = BasicProperties::default().with_headers(headers).with_delivery_mode(if persistent { 2 } else { 1 });
    if let Some(v) = f.get("content-type") {
        props = props.with_content_type(ShortString::from(v));
    }
    if let Some(v) = f.get("correlation-id") {
        props = props.with_correlation_id(ShortString::from(v));
    }
    if let Some(v) = f.get("reply-to") {
        props = props.with_reply_to(ShortString::from(v));
    }
    if let Some(v) = f.get("expiration") {
        props = props.with_expiration(ShortString::from(v));
    }
    if let Some(p) = f.get("priority").and_then(|p| p.parse::<u8>().ok()) {
        props = props.with_priority(p);
    }
    let confirm = s.ch.basic_publish(&exchange, &key, BasicPublishOptions::default(), &f.body, props).await;
    let outcome = match confirm {
        Ok(c) => c.await.map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    match outcome {
        Ok(c) if c.is_nack() => Err("message was refused".into()),
        Ok(_) => Ok(()),
        Err(e) => {
            reopen(s).await;
            Err(e)
        }
    }
}

async fn on_subscribe(s: &mut Session, f: &Frame) -> Result<(), String> {
    let id = f.get("id").or(if s.version == "1.0" { f.get("destination") } else { None }).ok_or("missing id header")?.to_string();
    if s.subs.contains_key(&id) {
        return Err(format!("duplicate subscription id {id}"));
    }
    let dest = f.get("destination").ok_or("missing destination header")?.to_string();
    let mode = match f.get("ack") {
        Some("client") => AckMode::Client,
        Some("client-individual") => AckMode::Individual,
        _ => AckMode::Auto,
    };
    let ch = s.conn.create_channel().await.map_err(|e| e.to_string())?;
    let prefetch = f.get("prefetch-count").and_then(|p| p.parse::<u16>().ok()).unwrap_or(0);
    if prefetch > 0 {
        ch.basic_qos(prefetch, BasicQosOptions::default()).await.map_err(|e| e.to_string())?;
    }
    let queue = if let Some(q) = dest.strip_prefix("/queue/") {
        let opts = QueueDeclareOptions { durable: true, ..Default::default() };
        ch.queue_declare(q, opts, FieldTable::default()).await.map_err(|e| e.to_string())?;
        q.to_string()
    } else if let Some(q) = dest.strip_prefix("/amq/queue/") {
        let opts = QueueDeclareOptions { passive: true, ..Default::default() };
        ch.queue_declare(q, opts, FieldTable::default()).await.map_err(|e| e.to_string())?;
        q.to_string()
    } else {
        let (exchange, key, _) = target(&dest)?;
        let name = format!("stomp-subscription-{}", super::token());
        let opts = QueueDeclareOptions { exclusive: true, auto_delete: true, ..Default::default() };
        ch.queue_declare(&name, opts, FieldTable::default()).await.map_err(|e| e.to_string())?;
        ch.queue_bind(&name, &exchange, &key, QueueBindOptions::default(), FieldTable::default())
            .await
            .map_err(|e| e.to_string())?;
        name
    };
    let tag = format!("stomp-{id}-{}", super::token());
    let mut consumer = ch
        .basic_consume(&queue, &tag, BasicConsumeOptions { no_ack: mode == AckMode::Auto, ..Default::default() }, FieldTable::default())
        .await
        .map_err(|e| e.to_string())?;
    let out = s.out_tx.clone();
    let version = s.version.clone();
    let sub_id = id.clone();
    let first_seq = s.next_seq;
    s.next_seq += 1_000_000_000;
    tokio::spawn(async move {
        let mut seq = first_seq;
        while let Some(Ok(d)) = consumer.next().await {
            seq += 1;
            let message_id = format!("T_{sub_id}@@session@@{seq}");
            let mut headers = vec![
                ("subscription".to_string(), sub_id.clone()),
                ("destination".to_string(), dest.clone()),
                ("message-id".to_string(), message_id.clone()),
                ("redelivered".to_string(), d.redelivered.to_string()),
            ];
            if mode != AckMode::Auto {
                headers.push(("ack".to_string(), message_id.clone()));
            }
            if let Some(table) = d.properties.headers() {
                for (k, v) in table.inner() {
                    if FRAME_HEADERS.contains(&k.as_str()) || k.as_str().starts_with("x-mqtt") {
                        continue;
                    }
                    let text = match v {
                        AMQPValue::LongString(s) => String::from_utf8_lossy(s.as_bytes()).into_owned(),
                        AMQPValue::ShortString(s) => s.to_string(),
                        AMQPValue::LongInt(n) => n.to_string(),
                        AMQPValue::LongLongInt(n) => n.to_string(),
                        AMQPValue::Boolean(b) => b.to_string(),
                        _ => continue,
                    };
                    headers.push((k.to_string(), text));
                }
            }
            if let Some(ct) = d.properties.content_type() {
                headers.push(("content-type".to_string(), ct.to_string()));
            }
            headers.push(("content-length".to_string(), d.data.len().to_string()));
            let frame = render("MESSAGE", &headers, &d.data, &version);
            if out.send(Out { sub: sub_id.clone(), seq, message_id, tag: d.delivery_tag, frame }).is_err() {
                break;
            }
        }
    });
    s.subs.insert(id, Sub { ch, tag, mode, pending: BTreeMap::new() });
    Ok(())
}

async fn on_ack(s: &mut Session, f: &Frame, negative: bool) -> Result<(), String> {
    let id = f.get("id").or(f.get("message-id")).unwrap_or_default();
    let requeue = f.get("requeue") != Some("false");
    for sub in s.subs.values_mut() {
        let Some((&seq, &(_, tag))) = sub.pending.iter().find(|(_, (mid, _))| mid == id) else { continue };
        let multiple = sub.mode == AckMode::Client;
        if multiple {
            sub.pending.retain(|k, _| *k > seq);
        } else {
            sub.pending.remove(&seq);
        }
        let result = if negative {
            sub.ch.basic_nack(tag, BasicNackOptions { multiple, requeue }).await
        } else {
            sub.ch.basic_ack(tag, BasicAckOptions { multiple }).await
        };
        return result.map_err(|e| e.to_string());
    }
    Ok(())
}

async fn on_unsubscribe(s: &mut Session, f: &Frame) -> Result<(), String> {
    let id = f.get("id").or(f.get("destination")).unwrap_or_default().to_string();
    if let Some(sub) = s.subs.remove(&id) {
        let _ = sub.ch.basic_cancel(&sub.tag, BasicCancelOptions::default()).await;
        // Closing the channel returns unacked deliveries to the queue.
        let _ = sub.ch.close(200, "unsubscribed").await;
    }
    Ok(())
}

async fn finish(s: Session) {
    let _ = s.conn.close(200, "stomp session closed").await;
}
