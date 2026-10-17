//! Dynamic shovels and federation links, RabbitMQ's shovel and federation
//! plugins. Their definitions are runtime parameters (and policies) in the
//! metadata store, so every link comes back after a broker restart.
//!
//! A link is an AMQP client: 0-9-1 through lapin, 1.0 through
//! [`amqp10_client`]. A URI with no host (`amqp://`, `amqp:///vhost`) means
//! this broker; the link then logs in over loopback as an administrator
//! with a one-off ticket, as RabbitMQ's local links use its internal user.

pub mod amqp10_client;
pub mod federation;
pub mod shovel;

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use lapin::types::{AMQPValue, FieldArray, FieldTable, LongString, ShortString};
use queueforge_core::UserTag;

use crate::state::MgmtState;

static STATE: OnceLock<MgmtState> = OnceLock::new();

/// Start the stored shovels and the federation links. The first call wins.
pub fn start(state: &MgmtState) {
    if STATE.set(state.clone()).is_err() {
        return;
    }
    tokio::spawn(async {
        shovel::restore().await;
        federation::run().await;
    });
}

pub(crate) fn state() -> Option<&'static MgmtState> {
    STATE.get()
}

/// Where a link connects, from its URI.
#[derive(Clone, Debug)]
pub(crate) struct Endpoint {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub vhost: String,
    /// Whether the URI named this broker (no host).
    pub local: bool,
    /// The URI as reported, without the password.
    pub shown: String,
}

impl Endpoint {
    /// An AMQP 0-9-1 URI lapin can dial.
    pub fn amqp_uri(&self) -> String {
        format!(
            "amqp://{}:{}@{}:{}/{}",
            pct(&self.user),
            pct(&self.pass),
            self.host,
            self.port,
            pct(&self.vhost)
        )
    }
}

fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn unpct(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = b.get(i + 1..i + 3).and_then(|h| std::str::from_utf8(h).ok()).and_then(|h| u8::from_str_radix(h, 16).ok());
        match (b[i], hex) {
            (b'%', Some(n)) => {
                out.push(n);
                i += 3;
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

/// Split `amqp://user:pass@host:port/vhost`. Missing parts are empty; the
/// vhost defaults to `/`.
pub(crate) fn parse_uri(uri: &str) -> Result<(String, String, String, Option<u16>, String), String> {
    let rest = uri
        .strip_prefix("amqp://")
        .or_else(|| uri.strip_prefix("amqps://"))
        .ok_or_else(|| format!("unsupported URI '{uri}'"))?;
    let rest = rest.split('?').next().unwrap_or_default();
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (u, h),
        None => ("", authority),
    };
    let (user, pass) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() => (h, Some(p.parse::<u16>().map_err(|_| format!("bad port in '{uri}'"))?)),
        _ => (hostport, None),
    };
    let vhost = if path.is_empty() { "/".to_string() } else { unpct(path) };
    Ok((unpct(user), unpct(pass), host.to_string(), port, vhost))
}

/// The URI without its password, as RabbitMQ reports link URIs.
pub(crate) fn shown_uri(uri: &str) -> String {
    let Some((scheme, rest)) = uri.split_once("://") else { return uri.to_string() };
    let (authority, path) = match rest.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (rest, None),
    };
    let host = match authority.rsplit_once('@') {
        Some((userinfo, host)) => match userinfo.split(':').next().unwrap_or_default() {
            "" => host.to_string(),
            user => format!("{user}@{host}"),
        },
        None => authority.to_string(),
    };
    match path {
        Some(p) => format!("{scheme}://{host}/{p}"),
        None => format!("{scheme}://{host}"),
    }
}

/// Resolve a link URI. A local one logs in as an administrator with a ticket.
pub(crate) async fn resolve(uri: &str) -> Result<Endpoint, String> {
    let (user, pass, host, port, vhost) = parse_uri(uri)?;
    if !host.is_empty() {
        return Ok(Endpoint {
            host,
            port: port.unwrap_or(5672),
            user: if user.is_empty() { "guest".into() } else { user },
            pass: if pass.is_empty() { "guest".into() } else { pass },
            vhost,
            local: false,
            shown: shown_uri(uri),
        });
    }
    let state = state().ok_or("links not started")?;
    let port = crate::bridge::loopback_port()
        .or_else(|| state.config.amqp_listeners.first().map(|(_, p)| *p))
        .unwrap_or(5672);
    let store = Arc::clone(&state.store);
    let users = tokio::task::spawn_blocking(move || store.list_users())
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    let admin = users
        .iter()
        .find(|u| u.tags.contains(&UserTag::Administrator))
        .or(users.first())
        .ok_or("no user to run a local link as")?
        .name
        .to_string();
    let pass = queueforge_auth::external::issue_ticket(&admin);
    Ok(Endpoint { host: "127.0.0.1".into(), port, user: admin, pass, vhost, local: true, shown: shown_uri(uri) })
}

/// The first URI of a `uri` field that may be a string or a list.
pub(crate) fn first_uri(value: Option<&serde_json::Value>) -> Option<String> {
    match value? {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Array(items) => items.iter().find_map(|v| v.as_str().map(str::to_string)),
        _ => None,
    }
}

/// A JSON object as an AMQP field table (consumer and queue arguments).
pub(crate) fn json_table(value: Option<&serde_json::Value>) -> FieldTable {
    let mut table = FieldTable::default();
    if let Some(serde_json::Value::Object(map)) = value {
        for (k, v) in map {
            if let Some(v) = json_amqp(v) {
                table.insert(ShortString::from(k.clone()), v);
            }
        }
    }
    table
}

fn json_amqp(v: &serde_json::Value) -> Option<AMQPValue> {
    Some(match v {
        serde_json::Value::Bool(b) => AMQPValue::Boolean(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => AMQPValue::LongLongInt(i),
            None => AMQPValue::Double(n.as_f64()?),
        },
        serde_json::Value::String(s) => AMQPValue::LongString(LongString::from(s.as_bytes().to_vec())),
        serde_json::Value::Array(items) => AMQPValue::FieldArray(FieldArray::from(items.iter().filter_map(json_amqp).collect::<Vec<_>>())),
        serde_json::Value::Object(_) => AMQPValue::FieldTable(json_table(Some(v))),
        serde_json::Value::Null => return None,
    })
}

/// Wait before reconnect `attempt`: the first few come sooner than
/// `reconnect-delay`, so links started with the broker do not wait out a
/// full delay for its AMQP listener.
pub(crate) fn retry_delay(reconnect_delay: std::time::Duration, attempt: u32) -> std::time::Duration {
    if attempt <= 5 {
        reconnect_delay.min(std::time::Duration::from_millis(500))
    } else {
        reconnect_delay
    }
}

/// A link's state for the management API.
#[derive(Clone, Debug)]
pub(crate) struct Status {
    pub state: &'static str,
    pub error: Option<String>,
    pub timestamp: String,
}

pub(crate) type SharedStatus = Arc<Mutex<Status>>;

pub(crate) fn new_status() -> SharedStatus {
    Arc::new(Mutex::new(Status { state: "starting", error: None, timestamp: timestamp() }))
}

pub(crate) fn set_status(status: &SharedStatus, state: &'static str, error: Option<String>) {
    if let Ok(mut s) = status.lock() {
        if s.state != state || s.error != error {
            s.state = state;
            s.error = error;
            s.timestamp = timestamp();
        }
    }
}

/// `YYYY-MM-DD HH:MM:SS` in UTC, the form RabbitMQ reports.
pub(crate) fn timestamp() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil-from-days (Howard Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}", rem / 3600, rem % 3600 / 60, rem % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uris_split_and_hide_passwords() {
        let (u, p, h, port, v) = parse_uri("amqp://bob:s%40cret@db.example:5673/prod").unwrap();
        assert_eq!((u.as_str(), p.as_str(), h.as_str(), port, v.as_str()), ("bob", "s@cret", "db.example", Some(5673), "prod"));
        let (_, _, h, port, v) = parse_uri("amqp://").unwrap();
        assert_eq!((h.as_str(), port, v.as_str()), ("", None, "/"));
        assert_eq!(parse_uri("amqp:///%2f").unwrap().4, "/");
        assert!(!shown_uri("amqp://bob:secret@host:5672/v").contains("secret"));
        assert!(parse_uri("http://x").is_err());
    }

    #[test]
    fn timestamps_are_rabbitmq_shaped() {
        let t = timestamp();
        assert_eq!(t.len(), 19);
        assert_eq!(&t[4..5], "-");
        assert_eq!(&t[10..11], " ");
    }
}
