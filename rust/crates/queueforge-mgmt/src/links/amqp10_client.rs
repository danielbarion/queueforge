//! A small AMQP 1.0 client for shovel endpoints (`src-protocol` /
//! `dest-protocol` = `amqp10`): SASL PLAIN, one session, one link.
//!
//! Message bodies are mapped to and from AMQP 0-9-1 with the same code the
//! broker's AMQP 1.0 listener uses, so a shovel moves the same properties
//! whichever protocol each side speaks.

use std::time::Duration;

use lapin::BasicProperties;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::bridge::amqp10::map;
use crate::bridge::amqp10::types::{encode, Decoder, V};

const HEADER_AMQP: [u8; 8] = *b"AMQP\x00\x01\x00\x00";
const HEADER_SASL: [u8; 8] = *b"AMQP\x03\x01\x00\x00";
const MAX_FRAME: u32 = 128 * 1024;
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
const IO_TIMEOUT: Duration = Duration::from_secs(10);

fn performative(code: u64, mut fields: Vec<V>) -> Vec<u8> {
    while fields.last().is_some_and(V::is_null) {
        fields.pop();
    }
    encode(&V::described(code, V::List(fields)))
}

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend(((8 + body.len()) as u32).to_be_bytes());
    out.extend([2, kind, 0, 0]);
    out.extend(body);
    out
}

/// One message read from a receiving link.
pub struct Received {
    /// Delivery id to settle with [`Client::accept`].
    pub delivery_id: u32,
    /// Body as AMQP 0-9-1 sees it.
    pub body: Vec<u8>,
    /// Properties as AMQP 0-9-1 sees them.
    pub props: BasicProperties,
    /// `properties.subject`, used as the routing key.
    pub subject: Option<String>,
}

/// A plain or TLS byte stream.
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// One AMQP 1.0 connection with one session and one link.
pub struct Client {
    io: Box<dyn Io>,
    buf: Vec<u8>,
    /// Credit still granted to the peer (receiver) or by the peer (sender).
    credit: u32,
    /// Credit a receiver grants each time it tops up.
    window: u32,
    delivery_count: u32,
    next_outgoing_id: u32,
    next_incoming_id: u32,
    /// Bytes of a transfer split over several frames.
    partial: Option<(u32, Vec<u8>)>,
}

/// Turn a shovel address into a v2 address: `/queues/x` stays, a plain name is a queue.
pub fn address(raw: &str) -> String {
    if raw.starts_with('/') {
        raw.to_string()
    } else {
        format!("/queues/{raw}")
    }
}

impl Client {
    /// Connect (over TLS when `tls` is set), authenticate with PLAIN, open
    /// `vhost` and begin a session.
    pub(crate) async fn connect(host: &str, port: u16, user: &str, pass: &str, vhost: &str, tls: Option<&super::tls::Tls>) -> Result<Self, String> {
        let io: Box<dyn Io> = match tls {
            Some(t) => Box::new(super::tls::connect_stream(host, port, t).await.map_err(|e| format!("amqp10 {e}"))?),
            None => {
                let tcp = tokio::time::timeout(IO_TIMEOUT, TcpStream::connect((host, port)))
                    .await
                    .map_err(|_| "amqp10 connect timed out".to_string())?
                    .map_err(|e| format!("amqp10 connect: {e}"))?;
                let _ = tcp.set_nodelay(true);
                Box::new(tcp)
            }
        };
        let mut c = Client {
            io,
            buf: Vec::new(),
            credit: 0,
            window: 0,
            delivery_count: 0,
            next_outgoing_id: 0,
            next_incoming_id: 0,
            partial: None,
        };
        c.write(&HEADER_SASL).await?;
        c.expect_header(HEADER_SASL).await?;
        // sasl-mechanisms, then sasl-init PLAIN, then the outcome.
        let (_, mechs) = c.next_frame().await?;
        if mechs.code() != Some(0x40) {
            return Err("amqp10: expected sasl-mechanisms".into());
        }
        let mut response = vec![0u8];
        response.extend_from_slice(user.as_bytes());
        response.push(0);
        response.extend_from_slice(pass.as_bytes());
        let init = performative(0x41, vec![V::sym("PLAIN"), V::Binary(response)]);
        c.write(&frame(1, &init)).await?;
        let (_, outcome) = c.next_frame().await?;
        if outcome.code() != Some(0x44) || outcome.field(0).as_u64() != Some(0) {
            return Err("amqp10: login refused".into());
        }
        c.write(&HEADER_AMQP).await?;
        c.expect_header(HEADER_AMQP).await?;
        let open = performative(
            OPEN,
            vec![V::Str("queueforge-shovel".into()), V::Str(format!("vhost:{vhost}")), V::Uint(MAX_FRAME)],
        );
        c.write(&frame(0, &open)).await?;
        let begin = performative(BEGIN, vec![V::Null, V::Uint(0), V::Uint(0x7fff_ffff), V::Uint(0x7fff_ffff)]);
        c.write(&frame(0, &begin)).await?;
        let mut opened = false;
        let mut begun = false;
        while !(opened && begun) {
            let (_, perf) = c.next_frame().await?;
            match perf.code() {
                Some(OPEN) => opened = true,
                Some(BEGIN) => begun = true,
                Some(CLOSE) | Some(END) => return Err(format!("amqp10 refused: {:?}", perf.field(0))),
                _ => {}
            }
        }
        Ok(c)
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.io.write_all(bytes).await.map_err(|e| format!("amqp10 write: {e}"))?;
        // A TLS stream holds written bytes until flushed.
        self.io.flush().await.map_err(|e| format!("amqp10 write: {e}"))
    }

    async fn fill(&mut self) -> Result<(), String> {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.io.read(&mut chunk).await.map_err(|e| format!("amqp10 read: {e}"))?;
        if n == 0 {
            return Err("amqp10 connection closed".into());
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }

    async fn expect_header(&mut self, want: [u8; 8]) -> Result<(), String> {
        while self.buf.len() < 8 {
            tokio::time::timeout(IO_TIMEOUT, self.fill()).await.map_err(|_| "amqp10 header timed out".to_string())??;
        }
        let got: Vec<u8> = self.buf.drain(..8).collect();
        if got != want {
            return Err("amqp10: unexpected protocol header".into());
        }
        Ok(())
    }

    /// Next non-empty frame: the performative and the bytes after it.
    async fn next_frame_raw(&mut self) -> Result<(V, Vec<u8>), String> {
        loop {
            if self.buf.len() >= 8 {
                let size = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
                if size < 8 {
                    return Err("amqp10: bad frame".into());
                }
                if self.buf.len() >= size {
                    let doff = (usize::from(self.buf[4]) * 4).clamp(8, size);
                    let body: Vec<u8> = self.buf[doff..size].to_vec();
                    self.buf.drain(..size);
                    if body.is_empty() {
                        continue;
                    }
                    let mut d = Decoder::new(&body);
                    let perf = d.value().map_err(|e| format!("amqp10 decode: {e:?}"))?;
                    return Ok((perf, body[d.at..].to_vec()));
                }
            }
            self.fill().await?;
        }
    }

    async fn next_frame(&mut self) -> Result<(u8, V), String> {
        let (perf, _) = tokio::time::timeout(IO_TIMEOUT, self.next_frame_raw())
            .await
            .map_err(|_| "amqp10 timed out".to_string())??;
        Ok((0, perf))
    }

    async fn attach(&mut self, receiver: bool, address: &str) -> Result<(), String> {
        let terminus = V::described(if receiver { 0x28 } else { 0x29 }, V::List(vec![V::Str(address.to_string())]));
        let (source, target) = if receiver { (terminus, V::Null) } else { (V::Null, terminus) };
        let attach = performative(
            ATTACH,
            vec![
                V::Str(format!("queueforge-shovel-{}", if receiver { "in" } else { "out" })),
                V::Uint(0),
                V::Bool(receiver),
                V::Ubyte(0),
                V::Ubyte(0),
                source,
                target,
                V::Null,
                V::Null,
                if receiver { V::Null } else { V::Uint(0) },
            ],
        );
        self.write(&frame(0, &attach)).await?;
        loop {
            let (_, perf) = self.next_frame().await?;
            match perf.code() {
                Some(ATTACH) => {
                    // A refused link has no terminus on the reply and is detached next.
                    let ours = if receiver { perf.field(5) } else { perf.field(6) };
                    if ours.is_null() {
                        return Err(format!("amqp10: {address} refused"));
                    }
                    return Ok(());
                }
                Some(DETACH) | Some(END) | Some(CLOSE) => return Err(format!("amqp10: {address} refused: {:?}", perf.field(2))),
                Some(FLOW) => self.on_flow(&perf),
                _ => {}
            }
        }
    }

    /// Attach a receiving link on `address` and grant `credit`.
    pub async fn receiver(&mut self, address: &str, credit: u32) -> Result<(), String> {
        self.attach(true, address).await?;
        self.window = credit.max(1);
        self.grant().await
    }

    /// Attach a sending link on `address`.
    pub async fn sender(&mut self, address: &str) -> Result<(), String> {
        self.attach(false, address).await
    }

    async fn grant(&mut self) -> Result<(), String> {
        self.credit = self.window;
        let flow = performative(
            FLOW,
            vec![
                V::Uint(self.next_incoming_id),
                V::Uint(0x7fff_ffff),
                V::Uint(self.next_outgoing_id),
                V::Uint(0x7fff_ffff),
                V::Uint(0),
                V::Uint(self.delivery_count),
                V::Uint(self.credit),
            ],
        );
        self.write(&frame(0, &flow)).await
    }

    fn on_flow(&mut self, perf: &V) {
        // On a sending link the peer's delivery-count + link-credit bounds what we may send.
        if perf.field(4).is_null() {
            return;
        }
        let count = perf.field(5).as_u32().unwrap_or(self.delivery_count);
        let credit = perf.field(6).as_u32().unwrap_or(0);
        self.credit = count.wrapping_add(credit).wrapping_sub(self.delivery_count);
    }

    /// Wait for the next message on the receiving link.
    pub async fn recv(&mut self) -> Result<Received, String> {
        loop {
            let (perf, payload) = self.next_frame_raw().await?;
            match perf.code() {
                Some(TRANSFER) => {
                    let more = perf.field(5).as_bool();
                    let id = perf.field(1).as_u32();
                    let (first, mut bytes) = self.partial.take().unwrap_or((id.unwrap_or(0), Vec::new()));
                    bytes.extend_from_slice(&payload);
                    if more {
                        self.partial = Some((first, bytes));
                        continue;
                    }
                    self.next_incoming_id = self.next_incoming_id.wrapping_add(1);
                    self.delivery_count = self.delivery_count.wrapping_add(1);
                    self.credit = self.credit.saturating_sub(1);
                    let msg = map::inbound(&bytes).map_err(|e| format!("amqp10 message: {e}"))?;
                    return Ok(Received { delivery_id: first, body: msg.body, props: msg.props, subject: msg.subject });
                }
                Some(DETACH) | Some(END) | Some(CLOSE) => return Err(format!("amqp10 link closed: {:?}", perf.field(2))),
                _ => {}
            }
        }
    }

    /// Accept (settle) one received delivery, and top the credit up when it runs low.
    pub async fn accept(&mut self, delivery_id: u32) -> Result<(), String> {
        let disposition = performative(
            DISPOSITION,
            vec![V::Bool(true), V::Uint(delivery_id), V::Null, V::Bool(true), V::described(ACCEPTED, V::List(vec![]))],
        );
        self.write(&frame(0, &disposition)).await?;
        if self.credit <= self.window / 2 {
            self.grant().await?;
        }
        Ok(())
    }

    /// Send one message and wait until the peer accepts it.
    pub async fn send(&mut self, exchange: &str, routing_key: &str, props: &BasicProperties, body: &[u8]) -> Result<(), String> {
        while self.credit == 0 {
            let (_, perf) = self.next_frame().await?;
            match perf.code() {
                Some(FLOW) => self.on_flow(&perf),
                Some(DETACH) | Some(END) | Some(CLOSE) => return Err(format!("amqp10 link closed: {:?}", perf.field(2))),
                _ => {}
            }
        }
        let id = self.next_outgoing_id;
        let payload = map::outbound(exchange, routing_key, false, props, body, &[]);
        let transfer = performative(
            TRANSFER,
            vec![V::Uint(0), V::Uint(id), V::Binary(id.to_be_bytes().to_vec()), V::Uint(0), V::Bool(false)],
        );
        let mut body_bytes = transfer;
        body_bytes.extend_from_slice(&payload);
        self.write(&frame(0, &body_bytes)).await?;
        self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);
        self.delivery_count = self.delivery_count.wrapping_add(1);
        self.credit -= 1;
        loop {
            let (_, perf) = self.next_frame().await?;
            match perf.code() {
                Some(DISPOSITION) => {
                    let first = perf.field(1).as_u32().unwrap_or(u32::MAX);
                    let last = perf.field(2).as_u32().unwrap_or(first);
                    if (first..=last).contains(&id) {
                        return if perf.field(4).code() == Some(ACCEPTED) {
                            Ok(())
                        } else {
                            Err("amqp10: message not accepted".into())
                        };
                    }
                }
                Some(FLOW) => self.on_flow(&perf),
                Some(DETACH) | Some(END) | Some(CLOSE) => return Err(format!("amqp10 link closed: {:?}", perf.field(2))),
                _ => {}
            }
        }
    }
}
