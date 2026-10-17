//! Messages held by `x-delayed-message` exchanges until their `x-delay`.
//!
//! Each pending message is a row in the metadata store's parameters
//! (component `delayed`, keyed by due time), written before the publish is
//! confirmed and removed once the message has been routed. A restart
//! schedules every row again, so a delay outlives the process; a message
//! that fell due while the broker was down is routed on startup.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use compact_str::CompactString;
use queueforge_core::{AppHeaderValue, ExchangeRouter, HeaderArg, Message, QueueCmd, QueueRegistry};
use queueforge_store::MetadataStore;
use tokio::sync::oneshot;
use tracing::warn;

/// Parameter component of the pending rows.
pub const COMPONENT: &str = "delayed";

struct Ctx {
    store: Arc<MetadataStore>,
    router: Arc<ExchangeRouter>,
    queues: Arc<QueueRegistry>,
}

static CTX: OnceLock<Ctx> = OnceLock::new();
static SEQ: AtomicU64 = AtomicU64::new(0);

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// Install the store and routing, then schedule the rows a previous run
/// left. Call once, after durable queues are recovered.
pub fn init(store: Arc<MetadataStore>, router: Arc<ExchangeRouter>, queues: Arc<QueueRegistry>) -> usize {
    let rows = store.list_parameters(COMPONENT).unwrap_or_default();
    if CTX.set(Ctx { store, router, queues }).is_err() {
        return 0;
    }
    let mut restored = 0;
    for (vhost, name, value) in rows {
        let Some(msg) = decode(&value) else {
            warn!(%vhost, %name, "unreadable delayed message dropped");
            continue;
        };
        let due = name.split('-').next().and_then(|d| d.parse::<u64>().ok()).unwrap_or(0);
        schedule(vhost, name, Arc::new(msg), due);
        restored += 1;
    }
    restored
}

fn encode(msg: &Message) -> Option<Vec<u8>> {
    queueforge_store::wal::encode_enqueue_record(0, msg).ok()
}

fn decode(bytes: &[u8]) -> Option<Message> {
    queueforge_store::wal::decode_records(bytes).ok()?.into_iter().next()?.message
}

/// Keep `msg` (published to delayed exchange `msg.exchange` on `vhost`)
/// until `delay` has passed, then route it as the exchange's
/// `x-delayed-type`. Returns once the message is stored.
pub async fn hold(vhost: &str, msg: Arc<Message>, delay: Duration) {
    let due = now_ms().saturating_add(delay.as_millis() as u64);
    let name = format!("{due:020}-{:010}", SEQ.fetch_add(1, Ordering::Relaxed));
    if let (Some(ctx), Some(bytes)) = (CTX.get(), encode(&msg)) {
        let store = Arc::clone(&ctx.store);
        let (v, n) = (vhost.to_string(), name.clone());
        let stored = tokio::task::spawn_blocking(move || store.put_parameter(COMPONENT, &v, &n, &bytes)).await;
        if !matches!(stored, Ok(Ok(()))) {
            warn!(%vhost, "delayed message not stored; it is held in memory only");
        }
    }
    schedule(vhost.to_string(), name, msg, due);
}

/// The header arguments a headers-type delayed exchange routes on.
fn header_args(msg: &Message) -> Vec<(CompactString, HeaderArg)> {
    msg.headers
        .app
        .iter()
        .filter_map(|(k, v)| {
            let arg = match v {
                AppHeaderValue::Str(s) => HeaderArg::Str(s.clone()),
                AppHeaderValue::I64(n) => HeaderArg::Int(*n),
                AppHeaderValue::Bool(b) => HeaderArg::Str(b.to_string()),
                _ => return None,
            };
            Some((CompactString::from(k.as_str()), arg))
        })
        .collect()
}

fn schedule(vhost: String, name: String, msg: Arc<Message>, due_ms: u64) {
    tokio::spawn(async move {
        let wait = due_ms.saturating_sub(now_ms());
        tokio::time::sleep(Duration::from_millis(wait)).await;
        let Some(ctx) = CTX.get() else { return };
        if let Ok(route) = ctx.router.route_publish(&vhost, &msg.exchange, &msg.routing_key, &header_args(&msg)) {
            for key in route.destinations {
                let Some(handle) = ctx.queues.get(&key) else { continue };
                let (reply, done) = oneshot::channel();
                if handle.tx.send(QueueCmd::Enqueue { msg: Arc::clone(&msg), reply }).await.is_ok() {
                    if let Ok(Ok(completion)) = done.await {
                        let _ = completion.durable_done.await;
                    }
                }
            }
        }
        let store = Arc::clone(&ctx.store);
        let _ = tokio::task::spawn_blocking(move || store.delete_parameter(COMPONENT, &vhost, &name)).await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_message_round_trips_through_its_row() {
        let mut msg = Message::blank();
        msg.exchange = "dx".into();
        msg.routing_key = "k".into();
        msg.body = bytes::Bytes::from_static(b"later");
        msg.persistent = true;
        msg.headers.app.push(("x-delay".into(), AppHeaderValue::I64(600)));
        let back = decode(&encode(&msg).unwrap()).unwrap();
        assert_eq!(back.exchange.as_str(), "dx");
        assert_eq!(back.routing_key.as_str(), "k");
        assert_eq!(&back.body[..], b"later");
        assert_eq!(header_args(&back).len(), 1);
    }
}
