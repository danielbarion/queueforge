//! Connection and channel teardown on close.

use super::*;

use queueforge_amqp::channel as chan_method;
use queueforge_amqp::Method;
use tokio::io::{AsyncRead, AsyncWrite};

impl<'a, S> Connection<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Drop every channel on this connection and delete exclusive queues it owns.
    ///
    /// Returns when the cascade finishes. A second call is a no-op.
    pub(in crate::connection) async fn cleanup_on_close(&mut self) {
        if self.cleaned_up {
            return;
        }
        self.cleaned_up = true;

        let channels: Vec<u16> = self.channels.keys().copied().collect();
        for ch in channels {
            self.teardown_channel(ch, /*requeue=*/ true).await;
        }

        // Delete exclusive queues owned by this connection (cascade bindings).
        let owner = self.peer.to_string();
        let exclusive = self.queues.list_exclusive_owned_by(&owner);
        for handle in exclusive {
            let key = handle.info.key.clone();
            let _ = self.delete_queue_and_bindings(&key, false, false).await;
            self.declared_queues.retain(|k| k != &key);
        }

        // Auto-delete queues declared here with no consumers (exclusive already gone).
        let vhost = self.vhost.clone().unwrap_or_else(|| "/".into());
        let auto = self.queues.list_auto_delete_in_vhost(&vhost);
        for handle in auto {
            // Only delete if this connection declared them and they have no consumers.
            if self.declared_queues.iter().any(|k| k == &handle.info.key) {
                let _ = self
                    .delete_queue_and_bindings(&handle.info.key, /*if_unused=*/ true, false)
                    .await;
            }
        }
        self.declared_queues.clear();
        self.sessions.clear();
    }
    /// Server-initiated channel.close: drop the channel from the open set
    /// (and the channels gauge) before sending, so clients may reopen the id.
    pub(in crate::connection) async fn server_channel_close(
        &mut self,
        channel: u16,
        reply_code: u16,
        reply_text: &str,
        class_id: u16,
        method_id: u16,
    ) -> Result<(), ConnError> {
        // Requeue unacked and drop consumers for this channel.
        self.teardown_channel(channel, /*requeue=*/ true).await;
        let close = Method::ChannelClose(chan_method::Close {
            reply_code,
            reply_text: reply_text.to_string(),
            class_id,
            method_id,
        });
        self.send_method(channel, &close).await
    }
}
