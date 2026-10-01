//! connection.tune and tune-ok.

use super::*;

use queueforge_amqp::connection as conn_method;
use queueforge_amqp::Method;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, warn};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// `on_tune_sent` on the open connection.
    pub(in crate::connection) async fn on_tune_sent(
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
    pub(in crate::connection) fn apply_tune_ok(
        &mut self,
        tune_ok: &conn_method::TuneOk,
    ) -> Result<(), ConnError> {
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
    pub(in crate::connection) async fn on_open_wait(
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
}
