//! In-process exchange federation links.
//!
//! A link copies a publish on the upstream vhost to queues bound to the same
//! exchange on the downstream vhost. RabbitMQ does this over AMQP; one
//! QueueForge process does it in memory so a single node can still federate
//! between vhosts.

use std::sync::Mutex;

use regex::Regex;

struct Link {
    upstream: String,
    downstream: String,
    pattern: String,
}

static LINKS: Mutex<Vec<Link>> = Mutex::new(Vec::new());
static UPSTREAMS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

/// Record the upstream vhost a downstream vhost federates from.
pub fn add_federation_upstream(downstream: String, upstream: String) {
    UPSTREAMS.lock().unwrap_or_else(|err| err.into_inner()).push((downstream, upstream));
}

/// Remember that publishes to `upstream` exchanges matching `pattern` also
/// route on `downstream`.
pub fn add_federation_link(upstream: String, downstream: String, pattern: String) {
    let mut guard = LINKS.lock().unwrap_or_else(|err| err.into_inner());
    guard.push(Link { upstream, downstream, pattern });
}

/// Upstream vhosts configured for `downstream`.
pub fn upstreams_of(downstream: &str) -> Vec<String> {
    UPSTREAMS
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .iter()
        .filter(|(down, _)| down == downstream)
        .map(|(_, up)| up.clone())
        .collect()
}

/// Downstream vhosts that should also receive this publish.
pub fn federation_targets(upstream: &str, exchange: &str) -> Vec<String> {
    let guard = LINKS.lock().unwrap_or_else(|err| err.into_inner());
    let mut out = Vec::new();
    for link in guard.iter() {
        if link.upstream != upstream {
            continue;
        }
        let Ok(re) = Regex::new(&link.pattern) else {
            continue;
        };
        if re.is_match(exchange) && !out.iter().any(|v| v == &link.downstream) {
            out.push(link.downstream.clone());
        }
    }
    out
}
