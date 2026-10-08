//! Management policies applied when a queue or exchange is declared without
//! the matching argument.
//!
//! The highest `priority` wins. Equal priorities use the lexicographically
//! smaller policy name. Declare arguments override policy keys that are set.

use compact_str::CompactString;
use regex::Regex;

use crate::queue::{OverflowPolicy, QueueArgs};

/// Which entities a policy can attach to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PolicyTarget {
    /// Queues only.
    Queues,
    /// Exchanges only.
    Exchanges,
    /// Queues and exchanges.
    All,
}

impl PolicyTarget {
    /// Parse `queues`, `exchanges`, or `all`.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queues" => Some(Self::Queues),
            "exchanges" => Some(Self::Exchanges),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    /// Wire value used by the management API.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queues => "queues",
            Self::Exchanges => "exchanges",
            Self::All => "all",
        }
    }

    fn covers(self, entity: Self) -> bool {
        matches!(self, Self::All) || self == entity
    }
}

/// One policy row in a vhost.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Policy {
    /// Owning virtual host.
    pub vhost: CompactString,
    /// Policy name. Unique within the vhost.
    pub name: CompactString,
    /// Regular expression matched against the queue or exchange name.
    pub pattern: String,
    /// Entity kinds this policy applies to.
    pub apply_to: PolicyTarget,
    /// Higher numbers win.
    pub priority: i32,
    /// `message-ttl` in milliseconds.
    pub message_ttl_ms: Option<u64>,
    /// `dead-letter-exchange`.
    pub dead_letter_exchange: Option<CompactString>,
    /// `dead-letter-routing-key`.
    pub dead_letter_routing_key: Option<CompactString>,
    /// `max-length`.
    pub max_length: Option<u64>,
    /// `max-length-bytes`.
    pub max_length_bytes: Option<u64>,
    /// `expires` in milliseconds.
    pub expires_ms: Option<u64>,
    /// `overflow` when set. Unset leaves the declare value.
    pub overflow: Option<OverflowPolicy>,
    /// Quorum `delivery-limit`.
    pub delivery_limit: Option<u32>,
    /// `alternate-exchange`.
    pub alternate_exchange: Option<CompactString>,
    /// `dead-letter-strategy` when set.
    #[serde(default)]
    pub dead_letter_strategy: Option<crate::queue::DeadLetterStrategy>,
}

impl Policy {
    fn matches(&self, vhost: &str, name: &str, entity: PolicyTarget) -> bool {
        if self.vhost.as_str() != vhost || !self.apply_to.covers(entity) {
            return false;
        }
        Regex::new(&self.pattern).is_ok_and(|re| re.is_match(name))
    }
}

/// Pick the winning policy for an entity, if any pattern matches.
pub fn select_policy<'a>(
    policies: &'a [Policy],
    vhost: &str,
    name: &str,
    entity: PolicyTarget,
) -> Option<&'a Policy> {
    let mut best: Option<&Policy> = None;
    for policy in policies {
        if !policy.matches(vhost, name, entity) {
            continue;
        }
        best = Some(match best {
            None => policy,
            Some(current) => {
                if policy.priority > current.priority
                    || (policy.priority == current.priority && policy.name < current.name)
                {
                    policy
                } else {
                    current
                }
            }
        });
    }
    best
}

/// User policy fills unset keys. An operator policy then caps each numeric
/// limit it sets (message TTL, length, length bytes, expiry, delivery limit):
/// the lower of the queue's value and the operator's wins, as in RabbitMQ.
/// Other operator keys fill only what is still unset.
pub fn apply_user_and_operator(
    declared: &QueueArgs,
    user: Option<&Policy>,
    operator: Option<&Policy>,
) -> QueueArgs {
    let mut out = apply_queue_policy(declared, user);
    let Some(op) = operator else {
        return out;
    };
    fn cap<T: Ord + Copy>(have: Option<T>, limit: Option<T>) -> Option<T> {
        match (have, limit) {
            (Some(h), Some(l)) => Some(h.min(l)),
            (None, l) => l,
            (h, None) => h,
        }
    }
    out.message_ttl_ms = cap(out.message_ttl_ms, op.message_ttl_ms);
    if declared.dead_letter_exchange.is_none() && op.dead_letter_exchange.is_some() {
        out.dead_letter_exchange = op.dead_letter_exchange.clone();
    }
    if declared.dead_letter_routing_key.is_none() && op.dead_letter_routing_key.is_some() {
        out.dead_letter_routing_key = op.dead_letter_routing_key.clone();
    }
    out.max_length = cap(out.max_length, op.max_length);
    out.max_length_bytes = cap(out.max_length_bytes, op.max_length_bytes);
    out.expires_ms = cap(out.expires_ms, op.expires_ms);
    if declared.overflow == OverflowPolicy::DropHead {
        if let Some(overflow) = op.overflow {
            out.overflow = overflow;
        }
    }
    out.delivery_limit = cap(out.delivery_limit, op.delivery_limit);
    if declared.dead_letter_strategy == crate::queue::DeadLetterStrategy::AtMostOnce {
        if let Some(strategy) = op.dead_letter_strategy {
            out.dead_letter_strategy = strategy;
        }
    }
    out
}

/// Fill unset queue arguments from the matching policy. Set declare arguments stay.
pub fn apply_queue_policy(declared: &QueueArgs, policy: Option<&Policy>) -> QueueArgs {
    let Some(policy) = policy else {
        return declared.clone();
    };
    let mut out = declared.clone();
    if out.message_ttl_ms.is_none() {
        out.message_ttl_ms = policy.message_ttl_ms;
    }
    if out.dead_letter_exchange.is_none() {
        out.dead_letter_exchange = policy.dead_letter_exchange.clone();
    }
    if out.dead_letter_routing_key.is_none() {
        out.dead_letter_routing_key = policy.dead_letter_routing_key.clone();
    }
    if out.max_length.is_none() {
        out.max_length = policy.max_length;
    }
    if out.max_length_bytes.is_none() {
        out.max_length_bytes = policy.max_length_bytes;
    }
    if out.expires_ms.is_none() {
        out.expires_ms = policy.expires_ms;
    }
    if policy.overflow.is_some() && out.overflow == OverflowPolicy::DropHead {
        out.overflow = policy.overflow.unwrap_or(out.overflow);
    }
    if out.delivery_limit.is_none() {
        out.delivery_limit = policy.delivery_limit;
    }
    if out.dead_letter_strategy == crate::queue::DeadLetterStrategy::AtMostOnce {
        if let Some(strategy) = policy.dead_letter_strategy {
            out.dead_letter_strategy = strategy;
        }
    }
    out
}

/// Declare-time `alternate-exchange` wins. Otherwise use the exchange policy.
pub fn effective_alternate(
    declared: Option<&str>,
    policy: Option<&Policy>,
) -> Option<CompactString> {
    if let Some(name) = declared.filter(|s| !s.is_empty()) {
        return Some(CompactString::from(name));
    }
    policy.and_then(|p| p.alternate_exchange.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(name: &str, priority: i32, pattern: &str) -> Policy {
        Policy {
            vhost: "/".into(),
            name: name.into(),
            pattern: pattern.into(),
            apply_to: PolicyTarget::All,
            priority,
            message_ttl_ms: Some(10),
            dead_letter_exchange: None,
            dead_letter_routing_key: None,
            max_length: None,
            max_length_bytes: None,
            expires_ms: None,
            overflow: None,
            delivery_limit: None,
            alternate_exchange: Some("ae".into()),
            dead_letter_strategy: None,
        }
    }

    #[test]
    fn higher_priority_wins_and_name_breaks_ties() {
        let rows = vec![
            policy("b", 1, "^q"),
            policy("a", 1, "^q"),
            policy("z", 0, "^q"),
        ];
        let won = select_policy(&rows, "/", "q1", PolicyTarget::Queues).unwrap();
        assert_eq!(won.name.as_str(), "a");
    }

    #[test]
    fn declare_argument_overrides_policy() {
        let mut declared = QueueArgs::default();
        declared.message_ttl_ms = Some(5);
        let merged = apply_queue_policy(&declared, Some(&policy("p", 0, ".*")));
        assert_eq!(merged.message_ttl_ms, Some(5));
        assert_eq!(merged.dead_letter_exchange, None);
    }

    #[test]
    fn queue_policy_does_not_apply_to_exchanges_when_scoped() {
        let mut row = policy("p", 0, "^ex");
        row.apply_to = PolicyTarget::Queues;
        assert!(select_policy(&[row], "/", "ex", PolicyTarget::Exchanges).is_none());
    }
}
