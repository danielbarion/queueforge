//! Closed queue declare-arguments schema (`x-message-ttl`, DLX, max-length, …).
//!
//! Unknown keys are rejected with [`crate::error::Error::PreconditionFailed`]
//! (stricter than RabbitMQ’s ignore-unknown-x-args).

use std::time::Duration;

use compact_str::CompactString;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Default hop limit for dead-letter chains ([`QueueArgs::max_death_hops`]).
pub const DEFAULT_MAX_DEATH_HOPS: u32 = 16;

/// Overflow policy when `x-max-length` / `x-max-length-bytes` is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OverflowPolicy {
    /// Drop the oldest ready message (optionally dead-letter it).
    #[default]
    DropHead,
    /// Reject the publish that would exceed the limit.
    RejectPublish,
}

impl OverflowPolicy {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "drop-head" => Ok(Self::DropHead),
            "reject-publish" => Ok(Self::RejectPublish),
            other => Err(Error::PreconditionFailed(format!(
                "invalid x-overflow value '{other}' (expected drop-head|reject-publish)"
            ))),
        }
    }
}

/// Parsed, validated queue declare arguments (v1 closed set).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueArgs {
    /// `x-message-ttl` — max ready residence time in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_ttl_ms: Option<u64>,
    /// `x-expires` — auto-delete after unused this many milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<u64>,
    /// `x-max-length` — max ready message count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<u64>,
    /// `x-max-length-bytes` — max ready payload bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length_bytes: Option<u64>,
    /// `x-overflow` policy (default `drop-head`).
    #[serde(default)]
    pub overflow: OverflowPolicy,
    /// `x-dead-letter-exchange`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_exchange: Option<CompactString>,
    /// `x-dead-letter-routing-key` (else original RK).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_routing_key: Option<CompactString>,
    /// `x-max-death-hops` (default 16).
    #[serde(default = "default_max_death_hops")]
    pub max_death_hops: u32,
    /// `x-max-priority` — max priority band (1–255). `None` / 0 = plain FIFO.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_priority: Option<u8>,
}

fn default_max_death_hops() -> u32 {
    DEFAULT_MAX_DEATH_HOPS
}

impl Default for QueueArgs {
    fn default() -> Self {
        Self {
            message_ttl_ms: None,
            expires_ms: None,
            max_length: None,
            max_length_bytes: None,
            overflow: OverflowPolicy::DropHead,
            dead_letter_exchange: None,
            dead_letter_routing_key: None,
            max_death_hops: DEFAULT_MAX_DEATH_HOPS,
            max_priority: None,
        }
    }
}

impl QueueArgs {
    /// Whether this is the all-defaults args set (no declare x-args).
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    /// `x-message-ttl` as a [`Duration`].
    pub fn message_ttl(&self) -> Option<Duration> {
        self.message_ttl_ms.map(Duration::from_millis)
    }

    /// `x-expires` as a [`Duration`].
    pub fn expires(&self) -> Option<Duration> {
        self.expires_ms.map(Duration::from_millis)
    }

    /// Parse from name/value pairs (AMQP field-table converted at the edge).
    ///
    /// Rejects unknown keys and invalid types/values.
    pub fn parse<'a, I>(pairs: I) -> Result<Self>
    where
        I: IntoIterator<Item = (&'a str, ArgValue<'a>)>,
    {
        let mut args = Self::default();

        for (key, value) in pairs {
            match key {
                "x-message-ttl" => {
                    args.message_ttl_ms = Some(positive_long(value, "x-message-ttl")?);
                }
                "x-expires" => {
                    args.expires_ms = Some(positive_long(value, "x-expires")?);
                }
                "x-max-length" => {
                    args.max_length = Some(positive_long(value, "x-max-length")?);
                }
                "x-max-length-bytes" => {
                    args.max_length_bytes = Some(positive_long(value, "x-max-length-bytes")?);
                }
                "x-overflow" => {
                    let s = shortstr(value, "x-overflow")?;
                    args.overflow = OverflowPolicy::parse(s)?;
                }
                "x-dead-letter-exchange" => {
                    args.dead_letter_exchange = Some(CompactString::from(shortstr(
                        value,
                        "x-dead-letter-exchange",
                    )?));
                }
                "x-dead-letter-routing-key" => {
                    args.dead_letter_routing_key = Some(CompactString::from(shortstr(
                        value,
                        "x-dead-letter-routing-key",
                    )?));
                }
                "x-max-death-hops" => {
                    let n = positive_long(value, "x-max-death-hops")?;
                    if n > u64::from(u32::MAX) {
                        return Err(Error::PreconditionFailed(
                            "x-max-death-hops out of range".into(),
                        ));
                    }
                    args.max_death_hops = n as u32;
                }
                "x-max-priority" => {
                    args.max_priority = parse_max_priority(value)?;
                }
                other => {
                    return Err(Error::PreconditionFailed(format!(
                        "unknown queue argument '{other}'"
                    )));
                }
            }
        }
        Ok(args)
    }
}

/// Lightweight view of an AMQP field-table value for declare-arg parsing.
#[derive(Debug, Clone, Copy)]
pub enum ArgValue<'a> {
    /// Signed/unsigned integer (long / longlong).
    Long(i64),
    /// Short or long string.
    Str(&'a str),
    /// Other / unsupported type.
    Other,
}

fn positive_long(value: ArgValue<'_>, name: &str) -> Result<u64> {
    match value {
        ArgValue::Long(n) if n > 0 => Ok(n as u64),
        ArgValue::Long(n) => Err(Error::PreconditionFailed(format!(
            "{name} must be a positive long (got {n})"
        ))),
        ArgValue::Str(s) => s.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| {
            Error::PreconditionFailed(format!("{name} must be a positive long (got '{s}')"))
        }),
        ArgValue::Other => Err(Error::PreconditionFailed(format!(
            "{name} must be a long integer"
        ))),
    }
}

fn shortstr<'a>(value: ArgValue<'a>, name: &str) -> Result<&'a str> {
    match value {
        ArgValue::Str(s) => Ok(s),
        _ => Err(Error::PreconditionFailed(format!(
            "{name} must be a shortstr"
        ))),
    }
}

/// Parse `x-max-priority`: integer 0–255; 0 means unset (FIFO).
fn parse_max_priority(value: ArgValue<'_>) -> Result<Option<u8>> {
    let n = match value {
        ArgValue::Long(n) if (0..=255).contains(&n) => n as u8,
        ArgValue::Long(n) => {
            return Err(Error::PreconditionFailed(format!(
                "x-max-priority must be 0–255 (got {n})"
            )));
        }
        ArgValue::Str(s) => s.parse::<u8>().map_err(|_| {
            Error::PreconditionFailed(format!("x-max-priority must be 0–255 (got '{s}')"))
        })?,
        ArgValue::Other => {
            return Err(Error::PreconditionFailed(
                "x-max-priority must be a long integer".into(),
            ));
        }
    };
    Ok(if n == 0 { None } else { Some(n) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_closed_set() {
        let args = QueueArgs::parse([
            ("x-message-ttl", ArgValue::Long(5000)),
            ("x-max-length", ArgValue::Long(10)),
            ("x-overflow", ArgValue::Str("reject-publish")),
            ("x-dead-letter-exchange", ArgValue::Str("dlx")),
            ("x-dead-letter-routing-key", ArgValue::Str("dead")),
            ("x-max-death-hops", ArgValue::Long(8)),
            ("x-expires", ArgValue::Long(60_000)),
            ("x-max-length-bytes", ArgValue::Long(1024)),
            ("x-max-priority", ArgValue::Long(9)),
        ])
        .unwrap();
        assert_eq!(args.message_ttl(), Some(Duration::from_millis(5000)));
        assert_eq!(args.max_length, Some(10));
        assert_eq!(args.overflow, OverflowPolicy::RejectPublish);
        assert_eq!(args.dead_letter_exchange.as_deref(), Some("dlx"));
        assert_eq!(args.dead_letter_routing_key.as_deref(), Some("dead"));
        assert_eq!(args.max_death_hops, 8);
        assert_eq!(args.expires(), Some(Duration::from_millis(60_000)));
        assert_eq!(args.max_length_bytes, Some(1024));
        assert_eq!(args.max_priority, Some(9));
    }

    #[test]
    fn max_priority_zero_is_unset() {
        let args = QueueArgs::parse([("x-max-priority", ArgValue::Long(0))]).unwrap();
        assert_eq!(args.max_priority, None);
    }

    #[test]
    fn max_priority_out_of_range() {
        assert!(QueueArgs::parse([("x-max-priority", ArgValue::Long(256))]).is_err());
        assert!(QueueArgs::parse([("x-max-priority", ArgValue::Long(-1))]).is_err());
    }

    #[test]
    fn reject_unknown_key() {
        let err = QueueArgs::parse([("x-foo", ArgValue::Long(1))]).unwrap_err();
        assert!(matches!(err, Error::PreconditionFailed(_)));
        assert!(err.to_string().contains("x-foo"));
    }

    #[test]
    fn reject_non_positive_ttl() {
        assert!(QueueArgs::parse([("x-message-ttl", ArgValue::Long(0))]).is_err());
        assert!(QueueArgs::parse([("x-message-ttl", ArgValue::Long(-1))]).is_err());
    }

    #[test]
    fn reject_bad_overflow() {
        assert!(QueueArgs::parse([("x-overflow", ArgValue::Str("drop-tail"))]).is_err());
    }

    #[test]
    fn default_hops_is_16() {
        assert_eq!(QueueArgs::default().max_death_hops, 16);
    }
}
