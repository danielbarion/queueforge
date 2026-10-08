//! connection.start and start-ok.

use super::*;

use std::sync::Arc;

use queueforge_amqp::connection as conn_method;
use queueforge_amqp::Method;
use queueforge_auth::{dummy_password_hash, verify_password};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info, warn};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `on_start_sent` on the open connection.
    pub(in crate::connection) async fn on_start_sent(
        &mut self,
        channel: u16,
        method: Method,
    ) -> Result<Step, ConnError> {
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
    pub(in crate::connection) async fn handle_start_ok(
        &mut self,
        start_ok: conn_method::StartOk,
    ) -> Result<Step, ConnError> {
        // RabbitMQ sends connection.blocked only to clients that ask for it.
        self.wants_blocked = matches!(
            start_ok.client_properties.get("capabilities"),
            Some(queueforge_amqp::FieldValue::Table(caps))
                if matches!(caps.get("connection.blocked"), Some(queueforge_amqp::FieldValue::Bool(true)))
        );
        // EXTERNAL: the user is the verified client certificate's common name,
        // as RabbitMQ's ssl_cert_login_from = common_name. It must exist.
        if start_ok.mechanism.eq_ignore_ascii_case("EXTERNAL") {
            let cn = super::peer_cn();
            let store = Arc::clone(&self.store);
            let lookup = cn.clone().unwrap_or_default();
            let known = match cn {
                Some(_) => MetadataStore::blocking(store, move |s| s.get_user(&lookup))
                    .await
                    .map_err(|e| ConnError::Protocol(format!("auth task: {e}")))?
                    .is_some(),
                None => false,
            };
            if !known {
                let _ = self
                    .send_connection_close(
                        REPLY_ACCESS_REFUSED,
                        "ACCESS_REFUSED - EXTERNAL login refused",
                        conn_method::CLASS_ID,
                        conn_method::StartOk::METHOD_ID,
                    )
                    .await;
                self.mark_closed();
                return Ok(Step::Done);
            }
            self.user = super::peer_cn();
            self.send_connection_tune().await?;
            self.state = State::TuneSent;
            return Ok(Step::Continue);
        }
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

        // redb lookup stays on the blocking pool. The SHA-256 password-hash
        // is checked here.
        let store = Arc::clone(&self.store);
        let user_owned = username.clone();
        let looked_up = MetadataStore::blocking(store, move |s| s.get_user(&user_owned))
            .await
            .map_err(|e| ConnError::Protocol(format!("auth task: {e}")))?;

        let authed = match looked_up {
            Some(user) => match verify_password(&password, &user.password_hash) {
                Ok(true) => Some(user),
                Ok(false) => None,
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
            },
            None => {
                let _ = verify_password(&password, dummy_password_hash());
                None
            }
        };

        // The next backends: OAuth 2.0, then LDAP.
        let external = match authed {
            Some(_) => None,
            None => queueforge_auth::external::login(&username, &password).await,
        };
        match (authed, external) {
            (Some(user), _) => {
                info!(peer = %self.peer, user = %user.name, "authenticated");
                self.user = Some(user.name.to_string());
            }
            (None, Some(name)) => {
                info!(peer = %self.peer, user = %name, "authenticated by an external backend");
                self.user = Some(name);
            }
            (None, None) => {
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
        }

        self.send_connection_tune().await?;
        self.state = State::TuneSent;
        Ok(Step::Continue)
    }
}
