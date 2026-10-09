//! Synchronous durable metadata writes and server-named queue names.

use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::meta::QueueMetaStore;
use super::SERVER_QUEUE_SEQ;
use super::{QueueDeclareOpts, QueueKey};
use crate::domain::Queue;
use crate::error::{Error, Result};

/// Write durable metadata for `key` from `opts` on the calling thread. `meta` is the store. Returns the store error. Callers on the async runtime must use the blocking wrapper instead.
pub(super) fn ensure_durable_meta_sync(
    meta: &dyn QueueMetaStore,
    key: &QueueKey,
    opts: &QueueDeclareOpts,
) -> Result<()> {
    let domain = Queue {
        vhost: key.vhost.clone(),
        name: key.name.clone(),
        durable: opts.durable,
        exclusive: opts.exclusive,
        auto_delete: opts.auto_delete,
        args: opts.args.clone(),
        home: opts.home.clone(),
        raft_group: None,
    };

    match meta.create_queue(&domain) {
        Ok(()) => Ok(()),
        Err(Error::AlreadyExists(_)) => {
            match meta.get_queue(key.vhost.as_str(), key.name.as_str())? {
                Some(existing) => {
                    if props_match(&existing, opts) {
                        Ok(())
                    } else {
                        Err(Error::PreconditionFailed(format!(
                            "queue {key} exists with different properties"
                        )))
                    }
                }
                // Meta row vanished between create race and get (e.g. concurrent
                // delete finished); re-create so live durable actor has a row.
                None => meta.create_queue(&domain),
            }
        }
        Err(e) => Err(e),
    }
}

/// Report whether `existing` has the same durable, exclusive, and auto-delete flags as `opts`. Returns false when a redeclare would change one of those. Arguments other than those flags are not compared here.
pub(super) fn props_match(existing: &Queue, opts: &QueueDeclareOpts) -> bool {
    existing.durable == opts.durable
        && existing.exclusive == opts.exclusive
        && existing.auto_delete == opts.auto_delete
        && existing.args == opts.args
}

/// Build declare options from stored queue `q`, keeping request-only fields from `request`. Returns the options a redeclare checks. The stored row wins for durable and exclusive.
pub(super) fn queue_to_opts(q: &Queue, request: &QueueDeclareOpts) -> QueueDeclareOpts {
    QueueDeclareOpts {
        durable: q.durable,
        exclusive: q.exclusive,
        auto_delete: q.auto_delete,
        passive: false,
        exclusive_owner: request.exclusive_owner.clone(),
        args: q.args.clone(),
        declared_args: None,
        home: q.home.clone(),
    }
}

/// Generate a unique server-named queue (`amq.gen-…`).
///
/// Uses a process-wide atomic sequence so concurrent empty-name declares never
/// share a name. Callers that AuthZ before declare should generate the name
/// first and pass it into [`QueueRegistry::declare`].
pub fn generate_server_queue_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SERVER_QUEUE_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    format!("amq.gen-{nanos:x}-{pid:x}-{seq:x}")
}
