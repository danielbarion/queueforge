//! connection.open and connection.close.

use super::*;

use std::sync::Arc;

use queueforge_amqp::connection as conn_method;
use queueforge_amqp::{FieldTable, FieldValue, Method};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, warn};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `handle_connection_open` on the open connection.
    pub(in crate::connection) async fn handle_connection_open(
        &mut self,
        open: conn_method::Open,
    ) -> Result<Step, ConnError> {
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
        if !self
            .connections
            .connection_allowed(user.as_str(), vhost.as_str())
        {
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
