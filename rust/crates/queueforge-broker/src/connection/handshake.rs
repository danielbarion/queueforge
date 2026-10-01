//! connection.start and start-ok.

use super::*;

use std::sync::Arc;

use queueforge_amqp::connection as conn_method;
use queueforge_amqp::Method;
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
}
