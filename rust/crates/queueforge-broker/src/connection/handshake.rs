//! connection.start, tune, and open.
//!
//! These methods belong to [`Connection`]. The frame loop in the parent module calls them.

use super::*;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use queueforge_amqp::channel as chan_method;
use queueforge_amqp::tx as tx_method;
use queueforge_amqp::confirm as confirm_method;
use queueforge_amqp::connection as conn_method;
use queueforge_amqp::exchange as exchange_method;
use queueforge_amqp::queue as queue_method;
use queueforge_amqp::{
    basic as basic_method, BasicProperties, ContentHeader, FieldTable, FieldValue, Frame, FrameType,
    Method,
};
use queueforge_auth::{PermissionKind, ResourceKind};
use queueforge_core::{
    generate_server_queue_name, Binding, ConsumerDeliveryId, ConsumerSessionId, Error as CoreError,
    Exchange, ExchangeType, Message, QueueCmd, QueueDeclareOpts, QueueDelivery, QueueHandle,
    QueueKey, QueueType, DEFAULT_EXCHANGE_NAME,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, info, trace, warn};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `on_start_sent` on the open connection.
    pub(in crate::connection) async fn on_start_sent(&mut self, channel: u16, method: Method) -> Result<Step, ConnError> {
        if channel != 0 {
            let _ = self
                .send_connection_close(
                    REPLY_COMMAND_INVALID,
                    "handshake methods must use channel 0",
                    0,
                    0,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        match method {
            Method::ConnectionStartOk(start_ok) => self.handle_start_ok(start_ok).await,
            Method::ConnectionClose(close) => {
                debug!(
                    peer = %self.peer,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "client closed during handshake"
                );
                let _ = self
                    .send_method(0, &Method::ConnectionCloseOk(conn_method::CloseOk))
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
            other => {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        &format!(
                            "expected connection.start-ok, got class={} method={}",
                            other.class_id(),
                            other.method_id()
                        ),
                        other.class_id(),
                        other.method_id(),
                    )
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
        }
    }

    /// `handle_start_ok` on the open connection.
    pub(in crate::connection) async fn handle_start_ok(&mut self, start_ok: conn_method::StartOk) -> Result<Step, ConnError> {
        if !start_ok.mechanism.eq_ignore_ascii_case("PLAIN") {
            let _ = self
                .send_connection_close(
                    REPLY_ACCESS_REFUSED,
                    &format!("unsupported SASL mechanism {}", start_ok.mechanism),
                    conn_method::CLASS_ID,
                    conn_method::StartOk::METHOD_ID,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        let (username, password) = match parse_plain_response(&start_ok.response) {
            Ok(pair) => pair,
            Err(msg) => {
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        msg,
                        conn_method::CLASS_ID,
                        conn_method::StartOk::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        };

        // Argon2 is intentionally slow; run off the async worker.
        let store = Arc::clone(&self.store);
        let user_owned = username.clone();
        let pass_owned = password.clone();
        let auth_result = MetadataStore::blocking(store, move |s| {
            Ok(AuthService::new(s).authenticate(&user_owned, &pass_owned))
        })
        .await
        .map_err(|e| ConnError::Protocol(format!("auth task: {e}")))?;

        match auth_result {
            Ok(Some(user)) => {
                info!(peer = %self.peer, user = %user.name, "authenticated");
                self.user = Some(user.name.to_string());
            }
            Ok(None) => {
                warn!(peer = %self.peer, user = %username, "authentication failed");
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - Login was refused using authentication mechanism PLAIN",
                        conn_method::CLASS_ID,
                        conn_method::StartOk::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            Err(e) => {
                warn!(peer = %self.peer, error = %e, "auth error");
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - authentication error",
                        conn_method::CLASS_ID,
                        conn_method::StartOk::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        }

        self.send_connection_tune().await?;
        self.state = State::TuneSent;
        Ok(Step::Continue)
    }

    /// `on_tune_sent` on the open connection.
    pub(in crate::connection) async fn on_tune_sent(&mut self, channel: u16, method: Method) -> Result<Step, ConnError> {
        if channel != 0 {
            let _ = self
                .send_connection_close(
                    REPLY_COMMAND_INVALID,
                    "handshake methods must use channel 0",
                    0,
                    0,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        match method {
            Method::ConnectionTuneOk(tune_ok) => {
                if let Err(e) = self.apply_tune_ok(&tune_ok) {
                    warn!(peer = %self.peer, error = %e, "tune-ok rejected");
                    let _ = self
                        .send_connection_close(
                            REPLY_COMMAND_INVALID,
                            &e.to_string(),
                            conn_method::CLASS_ID,
                            conn_method::TuneOk::METHOD_ID,
                        )
                        .await;
                    self.mark_closed();
                    return Ok(Step::Done);
                }
                debug!(
                    peer = %self.peer,
                    channel_max = self.channel_max,
                    frame_max = self.frame_max,
                    heartbeat = self.heartbeat,
                    "tuned"
                );
                self.state = State::OpenWait;
                Ok(Step::Continue)
            }
            Method::ConnectionClose(close) => {
                debug!(
                    peer = %self.peer,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "client closed during tune"
                );
                let _ = self
                    .send_method(0, &Method::ConnectionCloseOk(conn_method::CloseOk))
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
            other => {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        &format!(
                            "expected connection.tune-ok, got class={} method={}",
                            other.class_id(),
                            other.method_id()
                        ),
                        other.class_id(),
                        other.method_id(),
                    )
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
        }
    }

    /// `apply_tune_ok` on the open connection.
    pub(in crate::connection) fn apply_tune_ok(&mut self, tune_ok: &conn_method::TuneOk) -> Result<(), ConnError> {
        // Reject client proposals that exceed what we offered (except 0 = default)
        // *before* writing negotiated values (Issue 7).
        if tune_ok.channel_max != 0
            && self.params.channel_max != 0
            && tune_ok.channel_max > self.params.channel_max
        {
            return Err(ConnError::Protocol(format!(
                "client channel_max {} exceeds server offer {}",
                tune_ok.channel_max, self.params.channel_max
            )));
        }
        if tune_ok.frame_max != 0
            && tune_ok.frame_max > self.params.frame_max
            && self.params.frame_max != 0
        {
            return Err(ConnError::Protocol(format!(
                "client frame_max {} exceeds server offer {}",
                tune_ok.frame_max, self.params.frame_max
            )));
        }

        // channel_max: min(client, server); 0 → server default
        self.channel_max = negotiate_channel_max(self.params.channel_max, tune_ok.channel_max);
        // frame_max: min; floor 4096; 0 → server default
        self.frame_max = negotiate_frame_max(self.params.frame_max, tune_ok.frame_max);
        // heartbeat: min; either side 0 disables (Issue 1)
        self.heartbeat = negotiate_heartbeat(self.params.heartbeat, tune_ok.heartbeat);
        Ok(())
    }

    /// `on_open_wait` on the open connection.
    pub(in crate::connection) async fn on_open_wait(&mut self, channel: u16, method: Method) -> Result<Step, ConnError> {
        if channel != 0 {
            let _ = self
                .send_connection_close(
                    REPLY_COMMAND_INVALID,
                    "handshake methods must use channel 0",
                    0,
                    0,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }

        match method {
            Method::ConnectionOpen(open) => self.handle_connection_open(open).await,
            Method::ConnectionClose(close) => {
                debug!(
                    peer = %self.peer,
                    code = close.reply_code,
                    text = %close.reply_text,
                    "client closed before open"
                );
                let _ = self
                    .send_method(0, &Method::ConnectionCloseOk(conn_method::CloseOk))
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
            other => {
                let _ = self
                    .send_connection_close(
                        REPLY_COMMAND_INVALID,
                        &format!(
                            "expected connection.open, got class={} method={}",
                            other.class_id(),
                            other.method_id()
                        ),
                        other.class_id(),
                        other.method_id(),
                    )
                    .await;
                self.mark_closed();
                Ok(Step::Done)
            }
        }
    }

    /// `handle_connection_open` on the open connection.
    pub(in crate::connection) async fn handle_connection_open(&mut self, open: conn_method::Open) -> Result<Step, ConnError> {
        let vhost = open.virtual_host;
        let user = self
            .user
            .clone()
            .ok_or_else(|| ConnError::Protocol("open without auth".into()))?;

        // Vhost must exist. redb reads run off the worker.
        let store = Arc::clone(&self.store);
        let vhost_lookup = vhost.clone();
        match MetadataStore::blocking(store, move |s| s.get_vhost(&vhost_lookup)).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = self
                    .send_connection_close(
                        REPLY_NOT_ALLOWED,
                        &format!("NOT_ALLOWED - vhost {vhost} not found"),
                        conn_method::CLASS_ID,
                        conn_method::Open::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            Err(e) => {
                warn!(error = %e, "get_vhost failed");
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - internal error",
                        conn_method::CLASS_ID,
                        conn_method::Open::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        }

        // User must have a permission row on this vhost.
        let store = Arc::clone(&self.store);
        let user_lookup = user.clone();
        let vhost_perm = vhost.clone();
        match MetadataStore::blocking(store, move |s| s.get_permission(&user_lookup, &vhost_perm))
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        &format!("ACCESS_REFUSED - user '{user}' lacks access to vhost '{vhost}'"),
                        conn_method::CLASS_ID,
                        conn_method::Open::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            Err(e) => {
                warn!(error = %e, "get_permission failed");
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - internal error",
                        conn_method::CLASS_ID,
                        conn_method::Open::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
        }

        self.vhost = Some(vhost.clone());
        self.send_method(0, &Method::ConnectionOpenOk(conn_method::OpenOk::new()))
            .await?;
        info!(
            peer = %self.peer,
            user = %user,
            vhost = %vhost,
            "connection open"
        );
        metrics::gauge!("queueforge_connections").increment(1.0);
        queueforge_core::prom::connection_opened();
        self.gauges_held = true;
        if !self.connections.connection_allowed(user.as_str(), vhost.as_str()) {
            let _ = self
                .send_connection_close(
                    REPLY_ACCESS_REFUSED,
                    "ACCESS_REFUSED - connection limit reached",
                    conn_method::CLASS_ID,
                    conn_method::Open::METHOD_ID,
                )
                .await;
            self.mark_closed();
            return Ok(Step::Done);
        }
        let (track_id, force_rx) =
            self.connections
                .register(self.peer, user.as_str(), vhost.as_str());
        self.conn_track_id = Some(track_id);
        self.force_close_rx = Some(force_rx);
        self.state = State::Open;
        Ok(Step::Continue)
    }

    /// `send_connection_start` on the open connection.
    pub(in crate::connection) async fn send_connection_start(&mut self) -> Result<(), ConnError> {
        let mut props = FieldTable::new();
        props.insert("product", FieldValue::long_str("QueueForge"));
        props.insert("version", FieldValue::long_str(env!("CARGO_PKG_VERSION")));
        props.insert(
            "copyright",
            FieldValue::long_str("Copyright (c) QueueForge"),
        );
        props.insert(
            "capabilities",
            FieldValue::Table(FieldTable::from_pairs([
                // Only advertise capabilities we actually honor.
                ("publisher_confirms", FieldValue::Bool(true)),
                ("consumer_cancel_notify", FieldValue::Bool(true)),
                ("basic.nack", FieldValue::Bool(true)),
                ("connection.blocked", FieldValue::Bool(false)),
                ("authentication_failure_close", FieldValue::Bool(true)),
            ])),
        );
        props.insert(
            "information",
            FieldValue::long_str("https://github.com/queueforge/queueforge"),
        );

        let start = Method::ConnectionStart(conn_method::Start {
            version_major: 0,
            version_minor: 9,
            server_properties: props,
            mechanisms: b"PLAIN".to_vec(),
            locales: b"en_US".to_vec(),
        });
        self.send_method(0, &start).await
    }

    /// `send_connection_tune` on the open connection.
    pub(in crate::connection) async fn send_connection_tune(&mut self) -> Result<(), ConnError> {
        let tune = Method::ConnectionTune(conn_method::Tune {
            channel_max: self.params.channel_max,
            frame_max: self.params.frame_max,
            heartbeat: self.params.heartbeat,
        });
        self.send_method(0, &tune).await
    }

    /// `send_connection_close` on the open connection.
    pub(in crate::connection) async fn send_connection_close(
        &mut self,
        reply_code: u16,
        reply_text: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), ConnError> {
        let close = Method::ConnectionClose(conn_method::Close {
            reply_code,
            reply_text: reply_text.to_string(),
            class_id,
            method_id,
        });
        // Best-effort; peer may already be gone.
        let _ = self.send_method(0, &close).await;
        Ok(())
    }
}
