//! MQTT 3.1.1, STOMP 1.2, AMQP 1.0, and RabbitMQ stream listeners.
//!
//! Each listener accepts many connections. A publish is stored on a classic queue
//! and fanned out to every live subscriber on this process.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use compact_str::CompactString;
use queueforge_core::{Message, QueueCmd, QueueDeclareOpts, QueueKey, QueueRegistry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::warn;

struct MqttSub {
    filter: String,
    tx: mpsc::UnboundedSender<(String, Vec<u8>)>,
}

struct StompSub {
    destination: String,
    id: String,
    tx: mpsc::UnboundedSender<String>,
}

/// Declare `name` on `/` when it is missing and enqueue `body`.
pub async fn push_queue(queues: &QueueRegistry, name: &str, body: &[u8]) -> Result<(), String> {
    let key = QueueKey::new("/", name);
    if queues.get(&key).is_none() {
        queues
            .declare("/", name, QueueDeclareOpts::default())
            .await
            .map_err(|err| err.to_string())?;
    }
    let handle = queues
        .get(&key)
        .ok_or_else(|| format!("queue {name} missing"))?;
    let mut msg = Message::blank();
    msg.routing_key = CompactString::from(name);
    msg.body = Bytes::copy_from_slice(body);
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(QueueCmd::Enqueue {
            msg: Arc::new(msg),
            reply: tx,
        })
        .await
        .map_err(|_| format!("queue {name} is down"))?;
    rx.await
        .map_err(|_| format!("queue {name} dropped the publish"))?
        .map_err(|err| err.to_string())?;
    Ok(())
}

/// `basic.get` one body from `name`, acknowledging it.
pub async fn pull_queue(queues: &QueueRegistry, name: &str) -> Result<Option<Vec<u8>>, String> {
    let key = QueueKey::new("/", name);
    let Some(handle) = queues.get(&key) else {
        return Ok(None);
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .tx
        .send(QueueCmd::Get {
            no_ack: true,
            reply: tx,
        })
        .await
        .map_err(|_| format!("queue {name} is down"))?;
    let got = rx
        .await
        .map_err(|_| format!("queue {name} dropped the get"))?;
    Ok(got.map(|(_, message, _)| message.message.body.to_vec()))
}

/// Bind `addr` and serve MQTT 3.1.1 until the task is dropped.
pub fn spawn_mqtt(
    addr: std::net::SocketAddr,
    queues: Arc<QueueRegistry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Ok(listener) = TcpListener::bind(addr).await else {
            warn!(%addr, "mqtt bind failed");
            return;
        };
        let subs = Arc::new(Mutex::new(Vec::<MqttSub>::new()));
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let queues = Arc::clone(&queues);
            let subs = Arc::clone(&subs);
            tokio::spawn(async move {
                if let Err(err) = mqtt_conn(socket, queues, subs).await {
                    warn!(error = %err, "mqtt connection ended");
                }
            });
        }
    })
}

/// Bind `addr` and serve STOMP 1.2 until the task is dropped.
pub fn spawn_stomp(
    addr: std::net::SocketAddr,
    queues: Arc<QueueRegistry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Ok(listener) = TcpListener::bind(addr).await else {
            warn!(%addr, "stomp bind failed");
            return;
        };
        let subs = Arc::new(Mutex::new(Vec::<StompSub>::new()));
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let queues = Arc::clone(&queues);
            let subs = Arc::clone(&subs);
            tokio::spawn(async move {
                if let Err(err) = stomp_conn(socket, queues, subs).await {
                    warn!(error = %err, "stomp connection ended");
                }
            });
        }
    })
}

/// Bind `addr` and serve the RabbitMQ stream commands this broker implements.
pub fn spawn_stream(
    addr: std::net::SocketAddr,
    queues: Arc<QueueRegistry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Ok(listener) = TcpListener::bind(addr).await else {
            warn!(%addr, "stream bind failed");
            return;
        };
        let streams = Arc::new(Mutex::new(
            std::collections::HashMap::<String, Vec<Vec<u8>>>::new(),
        ));
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let queues = Arc::clone(&queues);
            let streams = Arc::clone(&streams);
            tokio::spawn(async move {
                if let Err(err) = stream_conn(socket, queues, streams).await {
                    warn!(error = %err, "stream connection ended");
                }
            });
        }
    })
}

async fn read_some(socket: &mut TcpStream, buf: &mut Vec<u8>) -> Result<(), String> {
    let mut tmp = [0u8; 4096];
    let n = socket.read(&mut tmp).await.map_err(|err| err.to_string())?;
    if n == 0 {
        return Err("eof".into());
    }
    buf.extend_from_slice(&tmp[..n]);
    Ok(())
}

fn mqtt_remaining(buf: &[u8], at: usize) -> Option<(usize, usize)> {
    let mut value = 0usize;
    let mut shift = 0;
    let mut i = at;
    while i < buf.len() && shift < 28 {
        let byte = buf[i];
        value += ((byte & 0x7f) as usize) << shift;
        i += 1;
        if byte & 0x80 == 0 {
            return Some((value, i));
        }
        shift += 7;
    }
    None
}

fn mqtt_str(buf: &[u8], at: usize) -> Option<(String, usize)> {
    if at + 2 > buf.len() {
        return None;
    }
    let n = u16::from_be_bytes([buf[at], buf[at + 1]]) as usize;
    let end = at + 2 + n;
    if end > buf.len() {
        return None;
    }
    let text = String::from_utf8_lossy(&buf[at + 2..end]).into_owned();
    Some((text, end))
}

fn mqtt_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    rest.extend_from_slice(topic.as_bytes());
    rest.extend_from_slice(payload);
    let mut out = vec![0x30];
    mqtt_encode_len(&mut out, rest.len());
    out.extend_from_slice(&rest);
    out
}

fn mqtt_encode_len(out: &mut Vec<u8>, mut len: usize) {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

fn mqtt_fanout(subs: &Mutex<Vec<MqttSub>>, topic: &str, payload: &[u8]) {
    let guard = subs.lock().unwrap_or_else(|e| e.into_inner());
    for sub in guard.iter() {
        if mqtt_match(&sub.filter, topic) {
            let _ = sub.tx.send((topic.to_string(), payload.to_vec()));
        }
    }
}

async fn mqtt_conn(
    mut socket: TcpStream,
    queues: Arc<QueueRegistry>,
    subs: Arc<Mutex<Vec<MqttSub>>>,
) -> Result<(), String> {
    let mut buf = Vec::new();
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, Vec<u8>)>();
    loop {
        tokio::select! {
            incoming = rx.recv() => {
                let Some((topic, payload)) = incoming else { return Ok(()) };
                socket.write_all(&mqtt_publish(&topic, &payload)).await.map_err(|e| e.to_string())?;
            }
            read = read_some(&mut socket, &mut buf) => {
                read?;
                while let Some((kind, body, header, len)) = mqtt_packet(&buf) {
                    buf.drain(..header + len);
                    mqtt_handle(kind, &body, &mut socket, &queues, &subs, &tx).await?;
                }
            }
        }
    }
}

fn mqtt_packet(buf: &[u8]) -> Option<(u8, Vec<u8>, usize, usize)> {
    if buf.is_empty() {
        return None;
    }
    let (len, header) = mqtt_remaining(buf, 1)?;
    if buf.len() < header + len {
        return None;
    }
    Some((buf[0] >> 4, buf[header..header + len].to_vec(), header, len))
}

async fn mqtt_handle(
    kind: u8,
    body: &[u8],
    socket: &mut TcpStream,
    queues: &QueueRegistry,
    subs: &Arc<Mutex<Vec<MqttSub>>>,
    tx: &mpsc::UnboundedSender<(String, Vec<u8>)>,
) -> Result<(), String> {
    match kind {
        1 => socket
            .write_all(&[0x20, 0x02, 0x00, 0x00])
            .await
            .map_err(|e| e.to_string())?,
        3 => {
            let (topic, payload_at) = mqtt_str(body, 0).ok_or_else(|| "mqtt topic".to_string())?;
            let payload = body[payload_at..].to_vec();
            push_queue(queues, &topic, &payload).await?;
            mqtt_fanout(subs, &topic, &payload);
        }
        8 => {
            if body.len() < 2 {
                return Ok(());
            }
            let id = [body[0], body[1]];
            let mut at = 2;
            let mut codes = Vec::new();
            let mut last = None;
            while at < body.len() {
                let (filter, next) = mqtt_str(body, at).ok_or_else(|| "mqtt filter".to_string())?;
                at = next + 1;
                {
                    let mut guard = subs.lock().unwrap_or_else(|e| e.into_inner());
                    guard.push(MqttSub {
                        filter: filter.clone(),
                        tx: tx.clone(),
                    });
                }
                last = Some(filter);
                codes.push(0);
            }
            let mut out = vec![0x90];
            mqtt_encode_len(&mut out, 2 + codes.len());
            out.extend_from_slice(&id);
            out.extend_from_slice(&codes);
            socket.write_all(&out).await.map_err(|e| e.to_string())?;
            if let Some(filter) = last {
                if let Some(queued) = pull_queue(queues, &filter).await? {
                    socket
                        .write_all(&mqtt_publish(&filter, &queued))
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        10 => {
            if body.len() >= 2 {
                let id0 = body[0];
                let id1 = body[1];
                let mut at = 2;
                let mut guard = subs.lock().unwrap_or_else(|e| e.into_inner());
                while at < body.len() {
                    let Some((filter, next)) = mqtt_str(body, at) else {
                        break;
                    };
                    guard.retain(|sub| !(sub.filter == filter && sub.tx.same_channel(tx)));
                    at = next;
                }
                let _ = (id0, id1);
            }
            socket
                .write_all(&[
                    0xb0,
                    0x02,
                    body.first().copied().unwrap_or(0),
                    body.get(1).copied().unwrap_or(0),
                ])
                .await
                .map_err(|e| e.to_string())?
        }
        12 => socket
            .write_all(&[0xd0, 0x00])
            .await
            .map_err(|e| e.to_string())?,
        14 => return Err("disconnect".into()),
        _ => {}
    }
    Ok(())
}

fn mqtt_match(filter: &str, topic: &str) -> bool {
    if filter == topic || filter == "#" {
        return true;
    }
    let f: Vec<_> = filter.split('/').collect();
    let t: Vec<_> = topic.split('/').collect();
    let mut i = 0;
    while i < f.len() {
        if f[i] == "#" {
            return true;
        }
        if i >= t.len() {
            return false;
        }
        if f[i] != "+" && f[i] != t[i] {
            return false;
        }
        i += 1;
    }
    i == t.len()
}

fn stomp_queue(dest: &str) -> String {
    dest.trim_start_matches("/queue/")
        .trim_start_matches("/topic/")
        .to_string()
}

fn stomp_fanout(subs: &Mutex<Vec<StompSub>>, dest: &str, body: &str) {
    let queue = stomp_queue(dest);
    let guard = subs.lock().unwrap_or_else(|e| e.into_inner());
    for sub in guard.iter() {
        if sub.destination == dest || stomp_queue(&sub.destination) == queue {
            let msg = format!(
                "MESSAGE\nsubscription:{}\ndestination:{dest}\ncontent-length:{}\n\n{body}\0",
                sub.id,
                body.len()
            );
            let _ = sub.tx.send(msg);
        }
    }
}

async fn stomp_conn(
    mut socket: TcpStream,
    queues: Arc<QueueRegistry>,
    subs: Arc<Mutex<Vec<StompSub>>>,
) -> Result<(), String> {
    let mut buf = Vec::new();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    loop {
        tokio::select! {
            incoming = rx.recv() => {
                let Some(msg) = incoming else { return Ok(()) };
                socket.write_all(msg.as_bytes()).await.map_err(|e| e.to_string())?;
            }
            read = read_some(&mut socket, &mut buf) => {
                read?;
                while let Some(end) = buf.iter().position(|b| *b == 0) {
                    let frame = String::from_utf8_lossy(&buf[..end]).into_owned();
                    buf.drain(..=end);
                    stomp_handle(&frame, &mut socket, &queues, &subs, &tx).await?;
                }
            }
        }
    }
}

async fn stomp_handle(
    frame: &str,
    socket: &mut TcpStream,
    queues: &QueueRegistry,
    subs: &Arc<Mutex<Vec<StompSub>>>,
    tx: &mpsc::UnboundedSender<String>,
) -> Result<(), String> {
    let mut lines = frame.split('\n').map(|l| l.trim_end_matches('\r'));
    let cmd = lines.next().unwrap_or("");
    let mut headers = Vec::new();
    let mut body = String::new();
    let mut in_body = false;
    for line in lines {
        if in_body {
            if !body.is_empty() {
                body.push('\n');
            }
            body.push_str(line);
            continue;
        }
        if line.is_empty() {
            in_body = true;
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.to_string(), v.to_string()));
        }
    }
    let header = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    match cmd {
        "CONNECT" | "STOMP" => {
            socket
                .write_all(b"CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0")
                .await
                .map_err(|e| e.to_string())?;
        }
        "SEND" => {
            let dest = header("destination");
            let queue = stomp_queue(&dest);
            let raw = if let Some(n) = header("content-length").parse::<usize>().ok() {
                body.as_bytes().get(..n).unwrap_or(body.as_bytes()).to_vec()
            } else {
                body.as_bytes().to_vec()
            };
            push_queue(queues, &queue, &raw).await?;
            let text = String::from_utf8_lossy(&raw).into_owned();
            stomp_fanout(subs, &dest, &text);
        }
        "SUBSCRIBE" => {
            let id = header("id");
            let dest = header("destination");
            {
                let mut guard = subs.lock().unwrap_or_else(|e| e.into_inner());
                guard.push(StompSub {
                    destination: dest.clone(),
                    id: id.clone(),
                    tx: tx.clone(),
                });
            }
            let queue = stomp_queue(&dest);
            if let Some(queued) = pull_queue(queues, &queue).await? {
                let text = String::from_utf8_lossy(&queued);
                let msg = format!(
                    "MESSAGE\nsubscription:{id}\ndestination:{dest}\ncontent-length:{}\n\n{text}\0",
                    text.len()
                );
                socket
                    .write_all(msg.as_bytes())
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        "UNSUBSCRIBE" => {
            let id = header("id");
            let mut guard = subs.lock().unwrap_or_else(|e| e.into_inner());
            guard.retain(|sub| !(sub.id == id && sub.tx.same_channel(tx)));
        }
        "DISCONNECT" => return Err("disconnect".into()),
        _ => {}
    }
    Ok(())
}

async fn stream_conn(
    mut socket: TcpStream,
    queues: Arc<QueueRegistry>,
    streams: Arc<Mutex<std::collections::HashMap<String, Vec<Vec<u8>>>>>,
) -> Result<(), String> {
    let mut buf = Vec::new();
    let mut publishers: std::collections::HashMap<u8, String> = std::collections::HashMap::new();
    loop {
        if buf.len() < 4 {
            read_some(&mut socket, &mut buf).await?;
            continue;
        }
        let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + size {
            read_some(&mut socket, &mut buf).await?;
            continue;
        }
        let frame = buf[4..4 + size].to_vec();
        buf.drain(..4 + size);
        if frame.len() < 4 {
            continue;
        }
        let key = u16::from_be_bytes([frame[0], frame[1]]);
        let rest = &frame[4..];
        let corr = if rest.len() >= 4 {
            u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]])
        } else {
            0
        };
        match key {
            0x0011 => {
                let body = stream_string_table(&[("product", "RabbitMQ"), ("version", "4.3.6")]);
                socket
                    .write_all(&stream_response(0x8011, corr, &body))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            0x0012 => {
                let mut body = Vec::new();
                body.extend_from_slice(&1u32.to_be_bytes());
                body.extend_from_slice(&stream_string("PLAIN"));
                socket
                    .write_all(&stream_response(0x8012, corr, &body))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            0x0013 => {
                socket
                    .write_all(&stream_response(0x8013, corr, &[]))
                    .await
                    .map_err(|e| e.to_string())?;
                socket
                    .write_all(&stream_tune())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            0x0014 => {}
            0x0015 => {
                socket
                    .write_all(&stream_response(0x8015, corr, &stream_map_empty()))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            0x000d => {
                let name = stream_read_string(&rest[4..]).unwrap_or_else(|| "stream".into());
                streams
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entry(name)
                    .or_default();
                socket
                    .write_all(&stream_response(0x800d, corr, &[]))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            0x0001 => {
                let publisher = rest.get(4).copied().unwrap_or(1);
                let after_id = stream_skip_string(&rest[5..]).unwrap_or(0);
                let stream =
                    stream_read_string(&rest[5 + after_id..]).unwrap_or_else(|| "stream".into());
                publishers.insert(publisher, stream);
                socket
                    .write_all(&stream_response(0x8001, corr, &[]))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            0x0002 => {
                let publisher = rest.first().copied().unwrap_or(1);
                let stream = publishers
                    .get(&publisher)
                    .cloned()
                    .unwrap_or_else(|| "stream".into());
                let mut ids = Vec::new();
                if rest.len() >= 5 {
                    let count = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
                    let mut at = 5;
                    for _ in 0..count {
                        if at + 8 > rest.len() {
                            break;
                        }
                        let id = u64::from_be_bytes(rest[at..at + 8].try_into().unwrap_or([0; 8]));
                        at += 8;
                        if at + 4 > rest.len() {
                            break;
                        }
                        let n = i32::from_be_bytes([
                            rest[at],
                            rest[at + 1],
                            rest[at + 2],
                            rest[at + 3],
                        ]);
                        at += 4;
                        if n < 0 || at + n as usize > rest.len() {
                            break;
                        }
                        let raw = rest[at - 4..at + n as usize].to_vec();
                        let payload = rest[at..at + n as usize].to_vec();
                        at += n as usize;
                        ids.push(id);
                        streams
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .entry(stream.clone())
                            .or_default()
                            .push(raw);
                        push_queue(&queues, &stream, &payload).await.ok();
                    }
                }
                socket
                    .write_all(&stream_confirm(publisher, &ids))
                    .await
                    .map_err(|e| e.to_string())?;
            }
            0x0007 => {
                let sub_id = rest.get(4).copied().unwrap_or(1);
                let stream = stream_read_string(&rest[5..]).unwrap_or_else(|| "stream".into());
                let queued = streams
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&stream)
                    .cloned()
                    .unwrap_or_default();
                socket
                    .write_all(&stream_response(0x8007, corr, &[]))
                    .await
                    .map_err(|e| e.to_string())?;
                if !queued.is_empty() {
                    socket
                        .write_all(&stream_deliver(sub_id, &queued))
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
            0x0016 => {
                socket
                    .write_all(&stream_response(0x8016, corr, &[]))
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
            0x0017 => {}
            _ => {
                if key & 0x8000 == 0 {
                    socket
                        .write_all(&stream_response(key | 0x8000, corr, &[]))
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
        }
    }
}

fn stream_string(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(text.len() as i16).to_be_bytes());
    out.extend_from_slice(text.as_bytes());
    out
}

fn stream_string_table(rows: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(rows.len() as i32).to_be_bytes());
    for (k, v) in rows {
        out.extend_from_slice(&stream_string(k));
        out.extend_from_slice(&stream_string(v));
    }
    out
}

fn stream_map_empty() -> Vec<u8> {
    0i32.to_be_bytes().to_vec()
}

fn stream_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn stream_response(key: u16, corr: u32, extra: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&key.to_be_bytes());
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.extend_from_slice(&corr.to_be_bytes());
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.extend_from_slice(extra);
    stream_frame(&payload)
}

fn stream_tune() -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0x0014u16.to_be_bytes());
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.extend_from_slice(&1_048_576u32.to_be_bytes());
    payload.extend_from_slice(&60u32.to_be_bytes());
    stream_frame(&payload)
}

fn stream_confirm(publisher: u8, ids: &[u64]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0x0003u16.to_be_bytes());
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.push(publisher);
    payload.extend_from_slice(&(ids.len() as u32).to_be_bytes());
    for id in ids {
        payload.extend_from_slice(&id.to_be_bytes());
    }
    stream_frame(&payload)
}

fn stream_deliver(sub_id: u8, messages: &[Vec<u8>]) -> Vec<u8> {
    let mut data = Vec::new();
    for message in messages {
        data.extend_from_slice(message);
    }
    let mut chunk = vec![0x50, 0x00];
    chunk.extend_from_slice(&(messages.len() as u16).to_be_bytes());
    chunk.extend_from_slice(&(messages.len() as u32).to_be_bytes());
    chunk.extend_from_slice(&0u64.to_be_bytes());
    chunk.extend_from_slice(&1u64.to_be_bytes());
    chunk.extend_from_slice(&0u64.to_be_bytes());
    chunk.extend_from_slice(&0u32.to_be_bytes());
    chunk.extend_from_slice(&(data.len() as u32).to_be_bytes());
    chunk.extend_from_slice(&0u32.to_be_bytes());
    chunk.push(0);
    chunk.extend_from_slice(&[0, 0, 0]);
    chunk.extend_from_slice(&data);
    let mut payload = Vec::new();
    payload.extend_from_slice(&0x0008u16.to_be_bytes());
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.push(sub_id);
    payload.extend_from_slice(&chunk);
    stream_frame(&payload)
}

fn stream_skip_string(buf: &[u8]) -> Option<usize> {
    stream_read_string(buf).map(|text| 2 + text.len())
}

fn stream_read_string(buf: &[u8]) -> Option<String> {
    if buf.len() < 2 {
        return None;
    }
    let n = i16::from_be_bytes([buf[0], buf[1]]) as usize;
    if buf.len() < 2 + n {
        return None;
    }
    Some(String::from_utf8_lossy(&buf[2..2 + n]).into_owned())
}

/// AMQP 1.0 on a socket that still contains the protocol header.
pub async fn amqp10_conn(mut socket: TcpStream, queues: Arc<QueueRegistry>) -> Result<(), String> {
    let mut buf = Vec::new();
    while buf.len() < 8 {
        read_some(&mut socket, &mut buf).await?;
    }
    if buf.starts_with(b"AMQP\x03\x01\x00\x00") {
        buf.drain(..8);
        socket
            .write_all(b"AMQP\x03\x01\x00\x00")
            .await
            .map_err(|e| e.to_string())?;
        socket
            .write_all(&amqp_sasl_mechanisms())
            .await
            .map_err(|e| e.to_string())?;
        loop {
            if let Some(frame) = amqp_take_frame(&mut buf) {
                if frame.windows(3).any(|w| w == [0x00, 0x53, 0x41]) {
                    socket
                        .write_all(&amqp_sasl_outcome())
                        .await
                        .map_err(|e| e.to_string())?;
                    break;
                }
            } else {
                read_some(&mut socket, &mut buf).await?;
            }
        }
        while !buf.starts_with(b"AMQP\x00\x01\x00\x00") {
            read_some(&mut socket, &mut buf).await?;
        }
        buf.drain(..8);
    } else if buf.starts_with(b"AMQP\x00\x01\x00\x00") {
        buf.drain(..8);
    }
    socket
        .write_all(b"AMQP\x00\x01\x00\x00")
        .await
        .map_err(|e| e.to_string())?;
    let mut sender: Option<String> = None;
    let mut receiver: Option<String> = None;
    loop {
        while let Some(frame) = amqp_take_frame(&mut buf) {
            let body = &frame[8..];
            if body.windows(3).any(|w| w == [0x00, 0x53, 0x10]) {
                socket
                    .write_all(&amqp_performative(0x10))
                    .await
                    .map_err(|e| e.to_string())?;
            } else if body.windows(3).any(|w| w == [0x00, 0x53, 0x11]) {
                socket
                    .write_all(&amqp_performative(0x11))
                    .await
                    .map_err(|e| e.to_string())?;
            } else if body.windows(3).any(|w| w == [0x00, 0x53, 0x12]) {
                if let Some(queue) = amqp_queue_name(body) {
                    if body.windows(1).any(|w| w == [0x41])
                        && body.windows(3).any(|w| w == [0x00, 0x53, 0x28])
                    {
                        receiver = Some(queue);
                    } else {
                        sender = Some(queue);
                    }
                }
                socket
                    .write_all(&amqp_performative(0x12))
                    .await
                    .map_err(|e| e.to_string())?;
                if sender.is_some() && receiver.is_none() {
                    socket
                        .write_all(&amqp_performative(0x13))
                        .await
                        .map_err(|e| e.to_string())?;
                }
            } else if let Some(payload) = amqp_data_section(body) {
                if let Some(queue) = sender.clone() {
                    push_queue(&queues, &queue, &payload).await?;
                }
            } else if body.windows(3).any(|w| w == [0x00, 0x53, 0x13]) {
                if let Some(queue) = receiver.clone() {
                    while let Some(payload) = pull_queue(&queues, &queue).await? {
                        socket
                            .write_all(&amqp_transfer(&payload))
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                }
            } else if body.windows(3).any(|w| w == [0x00, 0x53, 0x18]) {
                socket
                    .write_all(&amqp_performative(0x18))
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
        }
        read_some(&mut socket, &mut buf).await?;
    }
}

fn amqp_frame(ftype: u8, body: &[u8]) -> Vec<u8> {
    let size = 8 + body.len();
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(size as u32).to_be_bytes());
    out.push(2);
    out.push(ftype);
    out.extend_from_slice(&0u16.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn amqp_take_frame(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buf.len() < 8 || buf.starts_with(b"AMQP") {
        return None;
    }
    let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if size < 8 || buf.len() < size {
        return None;
    }
    let frame = buf[..size].to_vec();
    buf.drain(..size);
    Some(frame)
}

fn amqp_performative(code: u8) -> Vec<u8> {
    amqp_frame(0, &[0x00, 0x53, code, 0x45])
}

fn amqp_sasl_outcome() -> Vec<u8> {
    amqp_frame(1, &[0x00, 0x53, 0x44, 0xc0, 0x04, 0x02, 0x50, 0x00, 0x40])
}

fn amqp_queue_name(body: &[u8]) -> Option<String> {
    let marker = b"/queues/";
    let pos = body.windows(marker.len()).position(|w| w == marker)?;
    if pos < 2 || body[pos - 2] != 0xa1 {
        return None;
    }
    let n = body[pos - 1] as usize;
    let start = pos;
    let end = pos - 2 + 2 + n;
    if end > body.len() || start >= end {
        return None;
    }
    Some(String::from_utf8_lossy(&body[start + marker.len()..end]).into_owned())
}

fn amqp_data_section(body: &[u8]) -> Option<Vec<u8>> {
    let pos = body.windows(3).position(|w| w == [0x00, 0x53, 0x75])?;
    let at = pos + 3;
    if at >= body.len() || body[at] != 0xa0 {
        return None;
    }
    let n = body[at + 1] as usize;
    let from = at + 2;
    if from + n > body.len() {
        return None;
    }
    Some(body[from..from + n].to_vec())
}

fn amqp_transfer(payload: &[u8]) -> Vec<u8> {
    let mut fields = vec![0x52, 0x00, 0x43, 0xa0, 1, 1, 0x43, 0x41];
    let mut list = vec![0xc0, (1 + fields.len()) as u8, 5];
    list.append(&mut fields);
    let mut body = vec![0x00, 0x53, 0x14];
    body.extend_from_slice(&list);
    body.extend_from_slice(&[0x00, 0x53, 0x75, 0xa0, payload.len() as u8]);
    body.extend_from_slice(payload);
    amqp_frame(0, &body)
}

fn amqp_sasl_mechanisms() -> Vec<u8> {
    let mut body = vec![0x00, 0x53, 0x40, 0xc0, 0x0a, 0x01];
    body.extend_from_slice(b"\xa3\x05PLAIN");
    amqp_frame(1, &body)
}
