//! Permission kinds and regex matching (RabbitMQ-compatible).

use queueforge_core::Permission;
use regex::Regex;

use crate::error::{AuthError, Result};

/// Resource permission kind (configure / write / read).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PermissionKind {
    /// Declare / delete resource definitions.
    Configure,
    /// Publish to exchanges; write side of bind (queue on `queue.bind`).
    Write,
    /// Consume / get / purge; read side of bind (exchange on `queue.bind`).
    Read,
}

impl PermissionKind {
    /// Stable name for logs and errors.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Configure => "configure",
            Self::Write => "write",
            Self::Read => "read",
        }
    }
}

/// Whether the resource name is a queue or an exchange for permission matching.
///
/// Only exchanges normalize the empty name to [`DEFAULT_EXCHANGE_PERM_NAME`].
/// Empty queue names are not remapped (AMQP rejects empty queue names at the
/// protocol layer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    /// Queue name (no empty-name remapping).
    Queue,
    /// Exchange name (`""` → `amq.default`).
    Exchange,
}

/// Name used for the default exchange (`""`) in permission checks.
///
/// RabbitMQ maps the blank default exchange name to `amq.default` when matching
/// permission regexes.
pub const DEFAULT_EXCHANGE_PERM_NAME: &str = "amq.default";

/// Normalize a resource name for permission matching.
///
/// For [`ResourceKind::Exchange`], maps the empty default exchange name to
/// [`DEFAULT_EXCHANGE_PERM_NAME`]. Queue names are returned unchanged.
pub fn normalize_resource_name(resource: &str, kind: ResourceKind) -> &str {
    match kind {
        ResourceKind::Exchange if resource.is_empty() => DEFAULT_EXCHANGE_PERM_NAME,
        _ => resource,
    }
}

/// Compile a permission regex. Empty string means match nothing (`^$`).
fn compile_regex(pattern: &str, kind: &'static str) -> Result<Regex> {
    let effective = if pattern.is_empty() { "^$" } else { pattern };
    Regex::new(effective).map_err(|source| AuthError::InvalidRegex { kind, source })
}

/// Check whether `permission` grants `kind` on `resource` within its vhost.
///
/// `resource` is matched as a whole against the corresponding regex
/// (`configure` / `write` / `read`). Empty exchange names are normalized to
/// `amq.default` when `resource_kind` is [`ResourceKind::Exchange`].
///
/// Returns `false` if the regex does not match; returns `Err` only if a stored
/// regex is invalid.
pub fn check_permission(
    permission: &Permission,
    resource: &str,
    resource_kind: ResourceKind,
    kind: PermissionKind,
) -> Result<bool> {
    let pattern = match kind {
        PermissionKind::Configure => permission.configure.as_str(),
        PermissionKind::Write => permission.write.as_str(),
        PermissionKind::Read => permission.read.as_str(),
    };
    let re = compile_regex(pattern, kind.as_str())?;
    let name = normalize_resource_name(resource, resource_kind);
    Ok(re.is_match(name))
}

/// Authorize `queue.bind` / `queue.unbind`.
///
/// Requires **write** on the queue and **read** on the exchange (RabbitMQ
/// semantics). The default exchange name is normalized only on the exchange
/// (read) side.
pub fn check_queue_bind(permission: &Permission, queue: &str, exchange: &str) -> Result<bool> {
    let write_queue = check_permission(
        permission,
        queue,
        ResourceKind::Queue,
        PermissionKind::Write,
    )?;
    let read_exchange = check_permission(
        permission,
        exchange,
        ResourceKind::Exchange,
        PermissionKind::Read,
    )?;
    Ok(write_queue && read_exchange)
}

/// Authorize `queue.unbind` (same predicates as bind).
pub fn check_queue_unbind(permission: &Permission, queue: &str, exchange: &str) -> Result<bool> {
    check_queue_bind(permission, queue, exchange)
}

/// High-level check used by callers that have already loaded the user's
/// permission row for `vhost` (or `None` if missing).
///
/// Missing permission row → denied. Does not consult user tags (tags are
/// management-plane only).
pub fn check_user_permission(
    permission: Option<&Permission>,
    resource: &str,
    resource_kind: ResourceKind,
    kind: PermissionKind,
) -> Result<bool> {
    match permission {
        Some(p) => check_permission(p, resource, resource_kind, kind),
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queueforge_core::Permission;

    fn perm(configure: &str, write: &str, read: &str) -> Permission {
        Permission::new("u", "/", configure, write, read)
    }

    #[test]
    fn full_access_matches_all() {
        let p = Permission::full_access("admin", "/");
        assert!(
            check_permission(&p, "orders", ResourceKind::Queue, PermissionKind::Configure).unwrap()
        );
        assert!(check_permission(
            &p,
            "amq.direct",
            ResourceKind::Exchange,
            PermissionKind::Write
        )
        .unwrap());
        assert!(check_permission(&p, "q.jobs", ResourceKind::Queue, PermissionKind::Read).unwrap());
    }

    #[test]
    fn empty_regex_matches_nothing() {
        let p = perm("", "", "");
        assert!(!check_permission(
            &p,
            "anything",
            ResourceKind::Queue,
            PermissionKind::Configure
        )
        .unwrap());
        assert!(!check_permission(&p, "", ResourceKind::Exchange, PermissionKind::Write).unwrap());
        // Even amq.default after normalize should not match ^$
        assert!(!check_permission(&p, "", ResourceKind::Exchange, PermissionKind::Read).unwrap());
    }

    #[test]
    fn prefix_regex() {
        let p = perm("^app\\.", "^app\\.", ".*");
        assert!(check_permission(
            &p,
            "app.orders",
            ResourceKind::Queue,
            PermissionKind::Configure
        )
        .unwrap());
        assert!(
            !check_permission(&p, "other", ResourceKind::Queue, PermissionKind::Configure).unwrap()
        );
        assert!(
            check_permission(&p, "anything", ResourceKind::Queue, PermissionKind::Read).unwrap()
        );
    }

    #[test]
    fn default_exchange_maps_to_amq_default() {
        // Only match amq.default
        let p = perm("^$", "^amq\\.default$", "^$");
        assert!(check_permission(&p, "", ResourceKind::Exchange, PermissionKind::Write).unwrap());
        assert!(!check_permission(
            &p,
            "amq.direct",
            ResourceKind::Exchange,
            PermissionKind::Write
        )
        .unwrap());
    }

    #[test]
    fn empty_queue_name_is_not_remapped_to_amq_default() {
        // write regex only matches amq.default — empty *queue* must not gain that match.
        let p = perm("^$", "^amq\\.default$", "^$");
        assert!(!check_permission(&p, "", ResourceKind::Queue, PermissionKind::Write).unwrap());
        assert_eq!(normalize_resource_name("", ResourceKind::Queue), "");
        assert_eq!(
            normalize_resource_name("", ResourceKind::Exchange),
            DEFAULT_EXCHANGE_PERM_NAME
        );
    }

    #[test]
    fn queue_bind_requires_write_queue_and_read_exchange() {
        let p = perm(".*", "^jobs$", "^events$");
        assert!(check_queue_bind(&p, "jobs", "events").unwrap());
        assert!(!check_queue_bind(&p, "other", "events").unwrap());
        assert!(!check_queue_bind(&p, "jobs", "other").unwrap());
        assert!(check_queue_unbind(&p, "jobs", "events").unwrap());
    }

    #[test]
    fn queue_bind_default_exchange() {
        // write on queue, read on default exchange via amq.default
        let p = perm(".*", ".*", "^amq\\.default$");
        assert!(check_queue_bind(&p, "q1", "").unwrap());
        assert!(!check_queue_bind(&p, "q1", "amq.topic").unwrap());
    }

    #[test]
    fn missing_permission_row_denies() {
        assert!(
            !check_user_permission(None, "q", ResourceKind::Queue, PermissionKind::Read).unwrap()
        );
    }

    #[test]
    fn invalid_regex_errors() {
        let p = perm("[", ".*", ".*");
        let err =
            check_permission(&p, "x", ResourceKind::Queue, PermissionKind::Configure).unwrap_err();
        assert!(matches!(
            err,
            AuthError::InvalidRegex {
                kind: "configure",
                ..
            }
        ));
    }
}
