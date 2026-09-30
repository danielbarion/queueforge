//! [`QueueRegistry`]: `(vhost, name) → QueueHandle` map with actor spawn/supervision.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use compact_str::CompactString;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tracing::{debug, error, info, warn};

/// Outcome of [`QueueRegistry::shutdown_all`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Queues that were present when drain started.
    pub queues: usize,
    /// Actors whose final WAL fsync failed.
    pub fsync_errors: usize,
    /// Actors that did not reply within the global budget.
    pub timeouts: usize,
    /// Mailboxes already closed (actor gone).
    pub mailbox_closed: usize,
}

impl ShutdownReport {
    /// True when every actor answered and no durable fsync failed.
    pub fn is_clean(&self) -> bool {
        self.fsync_errors == 0 && self.timeouts == 0
    }
}

use super::actor::{self, DEFAULT_MAILBOX_CAPACITY};
use super::args::QueueArgs;
use super::cmd::{QueueCmd, QueueStats};
use super::dlx::DlxRouter;
use super::durable::{DurabilityPolicy, QueueActorBootstrap, SharedDurableLogFactory};
use super::meta::QueueMetaStore;
use crate::disk::DiskBudget;
use crate::domain::Queue;
use crate::error::{Error, Result};
use crate::memory::MemoryTracker;

/// Process-wide counter for unique server-generated queue names.
static SERVER_QUEUE_SEQ: AtomicU64 = AtomicU64::new(1);

/// Runtime state of a queue actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QueueActorState {
    /// Actor is running and accepting commands.
    Running = 0,
    /// Actor panicked; registry keeps the entry but does **not** auto-restart.
    Unavailable = 1,
}

impl QueueActorState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Running,
            _ => Self::Unavailable,
        }
    }
}

/// Composite key for a queue in a vhost.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueKey {
    /// Virtual host name.
    pub vhost: CompactString,
    /// Queue name.
    pub name: CompactString,
}

impl QueueKey {
    /// Build a key from vhost + name.
    pub fn new(vhost: impl Into<CompactString>, name: impl Into<CompactString>) -> Self {
        Self {
            vhost: vhost.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for QueueKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.vhost, self.name)
    }
}

/// Shared metadata for a live (or unavailable) queue.
#[derive(Debug)]
pub struct QueueInfo {
    /// Queue key.
    pub key: QueueKey,
    /// Survives broker restart when true.
    pub durable: bool,
    /// Restricted to a single connection when true.
    pub exclusive: bool,
    /// Deleted when last consumer cancels.
    pub auto_delete: bool,
    /// Connection id that owns an exclusive queue (if any).
    ///
    /// Until the connection-manager lands, the broker passes the peer
    /// `SocketAddr` string as a per-TCP-connection stand-in.
    pub exclusive_owner: Option<CompactString>,
    /// Arguments from the declare, before policy fill.
    pub declared_args: QueueArgs,
    /// Effective arguments (declare plus policy).
    pub args: std::sync::Mutex<QueueArgs>,
    state: AtomicU8,
    /// Bytes this queue currently holds in the process-wide [`crate::MemoryTracker`].
    ///
    /// Released on normal message lifecycle and as a safety net when the actor
    /// panics or exits without an ordered shutdown (see actor `MemoryGuard`).
    pub(crate) reserved_bytes: AtomicU64,
}

impl QueueInfo {
    /// Create queue info from declare options (also used by actor tests).
    pub(crate) fn new(key: QueueKey, opts: &QueueDeclareOpts) -> Self {
        Self {
            key,
            durable: opts.durable,
            exclusive: opts.exclusive,
            auto_delete: opts.auto_delete,
            exclusive_owner: opts.exclusive_owner.clone(),
            declared_args: opts.declared_args.clone().unwrap_or_else(|| opts.args.clone()),
            args: std::sync::Mutex::new(opts.args.clone()),
            state: AtomicU8::new(QueueActorState::Running as u8),
            reserved_bytes: AtomicU64::new(0),
        }
    }

    /// Current actor state.
    pub fn state(&self) -> QueueActorState {
        QueueActorState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Whether the actor is accepting work.
    pub fn is_available(&self) -> bool {
        self.state() == QueueActorState::Running
    }

    /// Mark the actor unavailable after panic isolation.
    pub fn mark_unavailable(&self) {
        self.state
            .store(QueueActorState::Unavailable as u8, Ordering::Release);
    }

    /// Bytes currently reserved by this queue in the global memory tracker.
    pub fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes.load(Ordering::Relaxed)
    }

    /// Domain [`Queue`] snapshot (for persistence / management).
    pub fn to_domain(&self) -> Queue {
        Queue {
            vhost: self.key.vhost.clone(),
            name: self.key.name.clone(),
            durable: self.durable,
            exclusive: self.exclusive,
            auto_delete: self.auto_delete,
            args: self.args.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            home: None,
        }
    }
}

/// Handle returned to connection tasks: mailbox sender + shared info.
#[derive(Debug, Clone)]
pub struct QueueHandle {
    /// Bounded command mailbox.
    pub tx: mpsc::Sender<QueueCmd>,
    /// Shared queue metadata / runtime state.
    pub info: Arc<QueueInfo>,
}

impl QueueHandle {
    /// Whether the actor is still running.
    pub fn is_available(&self) -> bool {
        self.info.is_available()
    }
}

impl QueueRegistry {
    /// Register a handle whose actor is not spawned by this registry.
    ///
    /// Used for a cluster proxy mailbox. Returns `false` when a handle already
    /// exists for the same queue.
    pub fn insert_external(&self, handle: QueueHandle) -> bool {
        let key = handle.info.key.clone();
        let mut guard = self.entries.write().expect("queue registry lock poisoned");
        if guard.contains_key(&key) {
            return false;
        }
        guard.insert(key, QueueEntry { handle });
        true
    }

    /// Build a proxy mailbox for a queue homed on a peer.
    ///
    /// Returns the command receiver the proxy task must drain. `None` when a
    /// handle is already registered.
    pub fn begin_proxy(
        &self,
        key: QueueKey,
        opts: QueueDeclareOpts,
        capacity: usize,
    ) -> Option<(QueueHandle, mpsc::Receiver<QueueCmd>)> {
        if self.get(&key).is_some() {
            return None;
        }
        let info = Arc::new(QueueInfo::new(key, &opts));
        let (tx, rx) = mpsc::channel(capacity.max(1));
        let handle = QueueHandle {
            tx,
            info: Arc::clone(&info),
        };
        if !self.insert_external(handle.clone()) {
            return None;
        }
        Some((handle, rx))
    }
}

/// Options for [`QueueRegistry::declare`].
#[derive(Debug, Clone, Default)]
pub struct QueueDeclareOpts {
    /// Survive broker restart.
    pub durable: bool,
    /// Exclusive to one connection.
    pub exclusive: bool,
    /// Auto-delete when unused.
    pub auto_delete: bool,
    /// Passive declare (must already exist).
    pub passive: bool,
    /// Declaring connection id (required when `exclusive`).
    pub exclusive_owner: Option<CompactString>,
    /// Closed declare-arguments set (already validated at the protocol edge).
    pub args: QueueArgs,
    /// Declare arguments before policy merge. `None` means "same as `args`".
    pub declared_args: Option<QueueArgs>,
    /// Cluster node that should own the actor. `None` keeps single-node behavior.
    pub home: Option<CompactString>,
}

/// Result of a successful declare (name may be server-generated).
#[derive(Debug, Clone)]
pub struct DeclareResult {
    /// Handle for the queue actor.
    pub handle: QueueHandle,
    /// Message count (0 until publish lands).
    pub message_count: u32,
    /// Consumer count.
    pub consumer_count: u32,
}

struct QueueEntry {
    handle: QueueHandle,
}

/// Maps `(vhost, queue_name) → QueueHandle` and owns actor lifecycle.
///
/// Create/delete/passive-restore mutations are serialized with [`Self::lifecycle`]
/// so durable metadata and registry membership stay one logical transaction
/// (no live durable actor without a meta row; no orphan meta from race loss).
pub struct QueueRegistry {
    entries: RwLock<HashMap<QueueKey, QueueEntry>>,
    /// Serializes create / delete / passive-restore so meta + map stay consistent.
    lifecycle: AsyncMutex<()>,
    meta: Arc<dyn QueueMetaStore>,
    /// Shared memory watermark counters (updated on enqueue/dequeue).
    pub memory: Arc<MemoryTracker>,
    /// Optional disk free budget for durable WAL publishes.
    pub disk: Option<Arc<DiskBudget>>,
    mailbox_capacity: usize,
    /// Opens per-queue WALs for durable queues (`None` = memory-only / tests).
    durable_factory: Option<SharedDurableLogFactory>,
    /// Group-commit policy applied to newly opened durable logs.
    durability_policy: DurabilityPolicy,
    /// When set, this registry must not spawn queues homed on a peer.
    local_node: Option<String>,
    /// Dead-letter router (set after construction to break Arc cycles).
    dlx: RwLock<Option<Arc<DlxRouter>>>,
    /// Fan-in of per-actor `x-expires` notifications.
    expired_tx: mpsc::UnboundedSender<QueueKey>,
    /// Receiver side held until [`Self::take_expired_rx`] is called.
    expired_rx: AsyncMutex<Option<mpsc::UnboundedReceiver<QueueKey>>>,
}

impl QueueRegistry {
    /// Create a registry backed by `meta` (use [`super::meta::NoopMetaStore`] in unit tests).
    pub fn new(meta: Arc<dyn QueueMetaStore>, memory: Arc<MemoryTracker>) -> Self {
        let (expired_tx, expired_rx) = mpsc::unbounded_channel();
        Self {
            entries: RwLock::new(HashMap::new()),
            lifecycle: AsyncMutex::new(()),
            meta,
            memory,
            disk: None,
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            durable_factory: None,
            durability_policy: DurabilityPolicy::default(),
            local_node: None,
            dlx: RwLock::new(None),
            expired_tx,
            expired_rx: AsyncMutex::new(Some(expired_rx)),
        }
    }

    /// Attach a durable log factory and group-commit policy (production path).
    pub fn with_durability(
        mut self,
        factory: SharedDurableLogFactory,
        policy: DurabilityPolicy,
    ) -> Self {
        self.durable_factory = Some(factory);
        self.durability_policy = policy;
        self
    }

    /// Attach a disk free-space budget (durable publish gate).
    pub fn with_disk_budget(mut self, disk: Arc<DiskBudget>) -> Self {
        self.disk = Some(disk);
        self
    }

    /// Record this process's cluster node id so peer-homed queues are not spawned.
    pub fn with_local_node(mut self, node_id: impl Into<String>) -> Self {
        self.local_node = Some(node_id.into());
        self
    }

    /// Override mailbox capacity (tests / config).
    pub fn with_mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = capacity.max(1);
        self
    }

    /// Install the dead-letter router (typically after the exchange router is ready).
    pub fn set_dlx(&self, dlx: Arc<DlxRouter>) {
        *self.dlx.write().expect("dlx lock poisoned") = Some(dlx);
    }

    /// Take the `x-expires` notification receiver (at most once).
    ///
    /// The broker should spawn a task that deletes queues as they expire.
    pub async fn take_expired_rx(&self) -> Option<mpsc::UnboundedReceiver<QueueKey>> {
        self.expired_rx.lock().await.take()
    }

    /// Shared handle for cloning into connection tasks.
    pub fn shared(meta: Arc<dyn QueueMetaStore>, memory: Arc<MemoryTracker>) -> Arc<Self> {
        Arc::new(Self::new(meta, memory))
    }

    /// Shared registry with durable WAL factory.
    pub fn shared_with_durability(
        meta: Arc<dyn QueueMetaStore>,
        memory: Arc<MemoryTracker>,
        factory: SharedDurableLogFactory,
        policy: DurabilityPolicy,
    ) -> Arc<Self> {
        Arc::new(Self::new(meta, memory).with_durability(factory, policy))
    }

    /// Shared registry with durability + disk budget.
    pub fn shared_with_durability_and_disk(
        meta: Arc<dyn QueueMetaStore>,
        memory: Arc<MemoryTracker>,
        factory: SharedDurableLogFactory,
        policy: DurabilityPolicy,
        disk: Arc<DiskBudget>,
    ) -> Arc<Self> {
        Arc::new(
            Self::new(meta, memory)
                .with_durability(factory, policy)
                .with_disk_budget(disk),
        )
    }

    /// Lookup a live queue handle.
    pub fn get(&self, key: &QueueKey) -> Option<QueueHandle> {
        let guard = self.entries.read().expect("queue registry lock poisoned");
        guard.get(key).map(|e| e.handle.clone())
    }

    /// Number of queues currently registered (including unavailable).
    pub fn len(&self) -> usize {
        self.entries
            .read()
            .expect("queue registry lock poisoned")
            .len()
    }

    /// Whether the registry has no queues.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// List keys currently registered.
    pub fn list_keys(&self) -> Vec<QueueKey> {
        self.entries
            .read()
            .expect("queue registry lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Graceful process drain: fan-out `Shutdown` to every live queue actor
    /// (WAL fsync + watermark flush for durable queues), wait under a single
    /// global budget, then clear the registry map.
    ///
    /// Returns a [`ShutdownReport`]. Callers should treat
    /// [`ShutdownReport::is_clean`] == false as an unclean process exit
    /// (fsync errors and/or timeouts).
    pub async fn shutdown_all(&self) -> ShutdownReport {
        self.shutdown_all_with_timeout(Duration::from_secs(10))
            .await
    }

    /// Like [`Self::shutdown_all`] with an explicit global wait budget.
    pub async fn shutdown_all_with_timeout(&self, budget: Duration) -> ShutdownReport {
        let keys = self.list_keys();
        info!(
            queues = keys.len(),
            budget_ms = budget.as_millis() as u64,
            "draining queue actors (Shutdown + fsync, parallel)"
        );

        let mailboxes: Vec<(QueueKey, mpsc::Sender<QueueCmd>)> = {
            let guard = self.entries.read().expect("queue registry lock poisoned");
            keys.iter()
                .filter_map(|k| guard.get(k).map(|e| (k.clone(), e.handle.tx.clone())))
                .collect()
        };

        let mut report = ShutdownReport {
            queues: mailboxes.len(),
            ..ShutdownReport::default()
        };

        // Fan-out Shutdown to all mailboxes first (Issue 6: not N×serial waits).
        let mut pending: Vec<(QueueKey, oneshot::Receiver<crate::error::Result<()>>)> = Vec::new();
        for (key, tx) in mailboxes {
            let (reply_tx, reply_rx) = oneshot::channel();
            if tx
                .send(QueueCmd::Shutdown { reply: reply_tx })
                .await
                .is_err()
            {
                debug!(
                    vhost = %key.vhost,
                    queue = %key.name,
                    "queue actor mailbox closed during shutdown"
                );
                report.mailbox_closed += 1;
                continue;
            }
            pending.push((key, reply_rx));
        }

        // Join all replies under one global timeout barrier.
        let mut join_set = tokio::task::JoinSet::new();
        for (key, reply_rx) in pending {
            join_set.spawn(async move {
                let res = reply_rx.await;
                (key, res)
            });
        }

        let deadline = tokio::time::Instant::now() + budget;
        while !join_set.is_empty() {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                let left = join_set.len();
                report.timeouts += left;
                warn!(
                    remaining = left,
                    "queue actor drain budget exhausted; aborting remaining waits"
                );
                join_set.abort_all();
                // Drain aborted join results so JoinSet drops cleanly.
                while join_set.join_next().await.is_some() {}
                break;
            }
            let remaining = deadline - now;
            match tokio::time::timeout(remaining, join_set.join_next()).await {
                Ok(Some(Ok((key, Ok(Ok(())))))) => {
                    debug!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        "queue actor shut down"
                    );
                }
                Ok(Some(Ok((key, Ok(Err(e)))))) => {
                    report.fsync_errors += 1;
                    error!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        error = %e,
                        "queue actor final fsync failed during shutdown"
                    );
                }
                Ok(Some(Ok((key, Err(_))))) => {
                    warn!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        "queue actor dropped shutdown reply"
                    );
                }
                Ok(Some(Err(join_err))) => {
                    if join_err.is_cancelled() {
                        report.timeouts += 1;
                    } else {
                        warn!(error = %join_err, "queue shutdown join error");
                    }
                }
                Ok(None) => break,
                Err(_elapsed) => {
                    let left = join_set.len();
                    report.timeouts += left;
                    warn!(
                        remaining = left,
                        "queue actor drain timed out; aborting remaining waits"
                    );
                    join_set.abort_all();
                    while join_set.join_next().await.is_some() {}
                    break;
                }
            }
        }

        self.entries
            .write()
            .expect("queue registry lock poisoned")
            .clear();

        if report.is_clean() {
            info!(queues = report.queues, "all queue actors drained");
        } else {
            // Issue 4: do not claim "all drained" when timeouts / fsync failed.
            warn!(
                queues = report.queues,
                fsync_errors = report.fsync_errors,
                timeouts = report.timeouts,
                mailbox_closed = report.mailbox_closed,
                "queue actor drain incomplete"
            );
        }
        report
    }

    /// Queues owned exclusively by `owner` (connection id / peer string).
    pub fn list_exclusive_owned_by(&self, owner: &str) -> Vec<QueueHandle> {
        self.entries
            .read()
            .expect("queue registry lock poisoned")
            .values()
            .filter(|e| {
                e.handle.info.exclusive
                    && e.handle
                        .info
                        .exclusive_owner
                        .as_ref()
                        .map(|o| o.as_str() == owner)
                        .unwrap_or(false)
            })
            .map(|e| e.handle.clone())
            .collect()
    }

    /// Queues in `vhost` with `auto_delete` set.
    pub fn list_auto_delete_in_vhost(&self, vhost: &str) -> Vec<QueueHandle> {
        self.entries
            .read()
            .expect("queue registry lock poisoned")
            .values()
            .filter(|e| e.handle.info.auto_delete && e.handle.info.key.vhost.as_str() == vhost)
            .map(|e| e.handle.clone())
            .collect()
    }

    /// Declare a queue: spawn actor, insert registry, persist if durable.
    ///
    /// - `passive`: must already exist; does not create.
    /// - Empty `name`: server generates `amq.gen-<unique>` (prefer generating
    ///   the name before AuthZ at the AMQP layer via [`generate_server_queue_name`]).
    /// - Existing queue: properties must match; exclusive owner must match.
    /// - Unavailable actor (post-panic): error for both passive and active declare.
    pub async fn declare(
        &self,
        vhost: impl Into<CompactString>,
        name: impl Into<CompactString>,
        opts: QueueDeclareOpts,
    ) -> Result<DeclareResult> {
        let vhost = vhost.into();
        let mut name = name.into();

        if name.is_empty() {
            if opts.passive {
                return Err(Error::NotFound(
                    "passive declare requires a queue name".into(),
                ));
            }
            name = CompactString::from(generate_server_queue_name());
        }

        let key = QueueKey::new(vhost.clone(), name.clone());

        // Fast path: already registered (no lifecycle lock needed for redeclare).
        if let Some(handle) = self.get(&key) {
            return self.declare_existing(handle, &opts).await;
        }

        // Create / passive-restore: serialize with delete so meta + map stay consistent.
        let _lifecycle = self.lifecycle.lock().await;

        // Re-check under lifecycle lock (another task may have won).
        if let Some(handle) = self.get(&key) {
            return self.declare_existing(handle, &opts).await;
        }

        if opts.passive {
            if let Some(q) = self
                .meta_blocking({
                    let vhost = vhost.to_string();
                    let name = name.to_string();
                    move |meta| meta.get_queue(&vhost, &name)
                })
                .await?
            {
                return self
                    .spawn_insert_and_persist(
                        key,
                        queue_to_opts(&q, &opts),
                        /*from_meta=*/ true,
                    )
                    .await;
            }
            return Err(Error::NotFound(format!("queue {key}")));
        }

        self.spawn_insert_and_persist(key, opts, /*from_meta=*/ false)
            .await
    }

    async fn declare_existing(
        &self,
        handle: QueueHandle,
        opts: &QueueDeclareOpts,
    ) -> Result<DeclareResult> {
        let info = &handle.info;

        // Unavailable check applies to passive and non-passive (Issue 3).
        if !info.is_available() {
            return Err(Error::Unavailable(format!(
                "queue {} actor unavailable (panic isolation; delete/recreate)",
                info.key
            )));
        }

        if opts.passive {
            return self.stats_result(handle).await;
        }

        if info.durable != opts.durable
            || info.exclusive != opts.exclusive
            || info.auto_delete != opts.auto_delete
        {
            return Err(Error::PreconditionFailed(format!(
                "queue {} exists with different properties",
                info.key
            )));
        }
        // Redeclare with different x-args is a precondition failure (RabbitMQ-like).
        if *info.args.lock().unwrap_or_else(|e| e.into_inner()) != opts.args {
            return Err(Error::PreconditionFailed(format!(
                "queue {} exists with different arguments",
                info.key
            )));
        }
        if info.exclusive {
            if let (Some(owner), Some(req)) =
                (info.exclusive_owner.as_ref(), opts.exclusive_owner.as_ref())
            {
                if owner != req {
                    return Err(Error::ResourceLocked(format!(
                        "queue {} is exclusive to another connection",
                        info.key
                    )));
                }
            }
        }
        // Redeclare counts as use for x-expires.
        let (touch_tx, touch_rx) = oneshot::channel();
        let _ = handle.tx.send(QueueCmd::Touch { reply: touch_tx }).await;
        let _ = touch_rx.await;
        self.stats_result(handle).await
    }

    /// Caller must hold [`Self::lifecycle`].
    ///
    /// Order: spawn → map insert (win) → durable meta (if needed) → gauge.
    /// On meta failure after insert: roll back map + shutdown actor.
    /// Race loss: never writes meta (avoids orphan durable rows).
    async fn spawn_insert_and_persist(
        &self,
        key: QueueKey,
        opts: QueueDeclareOpts,
        from_meta: bool,
    ) -> Result<DeclareResult> {
        // Double-check (lifecycle held, but re-entrant get is cheap).
        if let Some(existing) = self.get(&key) {
            return self.declare_existing(existing, &opts).await;
        }
        if let Some(local) = &self.local_node {
            if opts.home.as_deref().is_some_and(|home| home != local) {
                return Err(Error::Unavailable(format!(
                    "queue {key} is homed on {}",
                    opts.home.as_deref().unwrap_or(local)
                )));
            }
        }

        let info = Arc::new(QueueInfo::new(key.clone(), &opts));
        let bootstrap = self.bootstrap_for_declare(&key, &opts)?;
        let tx = spawn_supervised_actor(
            key.clone(),
            Arc::clone(&info),
            self.mailbox_capacity,
            Arc::clone(&self.memory),
            self.disk.clone(),
            bootstrap,
        );
        let handle = QueueHandle {
            tx,
            info: Arc::clone(&info),
        };

        let race_winner = {
            let mut guard = self.entries.write().expect("queue registry lock poisoned");
            if let Some(entry) = guard.get(&key) {
                Some(entry.handle.clone())
            } else {
                guard.insert(
                    key.clone(),
                    QueueEntry {
                        handle: handle.clone(),
                    },
                );
                None
            }
        };

        if let Some(existing) = race_winner {
            // We did not insert; do not touch meta. Shut down our unused actor.
            let _ = shutdown_actor(&handle.tx).await;
            return self.declare_existing(existing, &opts).await;
        }

        // We own the map entry. Persist durable definition only after the win.
        // Passive restore (`from_meta`) already has a row — do not re-create.
        if opts.durable && !from_meta {
            if let Err(e) = self.ensure_durable_meta(&key, &opts).await {
                self.rollback_insert(&key, &handle).await;
                return Err(e);
            }
        }

        // Always count registry membership (including passive restore).
        metrics::gauge!("queueforge_queues").increment(1.0);
        if from_meta {
            debug!(vhost = %key.vhost, queue = %key.name, "queue actor restored from metadata");
        } else {
            info!(vhost = %key.vhost, queue = %key.name, durable = opts.durable, "queue declared");
        }

        let message_count = {
            // Prefer live stats; fall back to 0 if actor has not settled.
            query_stats(&handle.tx)
                .await
                .map(|s| s.messages_ready)
                .unwrap_or(0)
        };

        Ok(DeclareResult {
            handle,
            message_count,
            consumer_count: 0,
        })
    }

    fn bootstrap_for_declare(
        &self,
        key: &QueueKey,
        opts: &QueueDeclareOpts,
    ) -> Result<QueueActorBootstrap> {
        let mut boot = QueueActorBootstrap::new_empty(opts.durable)
            .with_args(opts.args.clone())
            .with_expired_tx(self.expired_tx.clone());
        boot.durability_policy = self.durability_policy;
        if let Some(dlx) = self.dlx.read().expect("dlx lock poisoned").clone() {
            boot = boot.with_dlx(dlx);
        }
        if opts.durable {
            if let Some(factory) = &self.durable_factory {
                // Issue 3: always recover_messages on open — never spawn an empty
                // actor over existing segments / corrupt WAL (which must Err).
                let opened = factory.open(key.vhost.as_str(), key.name.as_str())?;
                boot = boot
                    .with_recovered(opened.ready, opened.next_offset)
                    .with_log(opened.log, self.durability_policy);
            }
        }
        Ok(boot)
    }

    /// Apply shared DLX / expires hooks onto a recovery bootstrap before spawn.
    pub fn decorate_bootstrap(&self, mut boot: QueueActorBootstrap) -> QueueActorBootstrap {
        boot = boot.with_expired_tx(self.expired_tx.clone());
        if let Some(dlx) = self.dlx.read().expect("dlx lock poisoned").clone() {
            boot = boot.with_dlx(dlx);
        }
        boot
    }

    /// Insert a recovered durable queue with rebuilt ready set (broker recovery).
    ///
    /// Caller must hold no conflicting live entry for `queue`. Does not re-create
    /// the metadata row (already present). Opens no new WAL — uses `bootstrap`.
    pub async fn restore_recovered(
        &self,
        queue: &Queue,
        bootstrap: QueueActorBootstrap,
    ) -> Result<QueueHandle> {
        let _lifecycle = self.lifecycle.lock().await;
        let key = QueueKey::new(queue.vhost.clone(), queue.name.clone());
        if self.get(&key).is_some() {
            return Err(Error::AlreadyExists(format!("queue {key}")));
        }

        let opts = QueueDeclareOpts {
            durable: queue.durable,
            exclusive: queue.exclusive,
            auto_delete: queue.auto_delete,
            passive: false,
            exclusive_owner: None,
            args: queue.args.clone(),
            declared_args: None,
            home: queue.home.clone(),
        };
        let info = Arc::new(QueueInfo::new(key.clone(), &opts));
        let bootstrap = self
            .decorate_bootstrap(bootstrap)
            .with_args(queue.args.clone());
        let tx = spawn_supervised_actor(
            key.clone(),
            Arc::clone(&info),
            self.mailbox_capacity,
            Arc::clone(&self.memory),
            self.disk.clone(),
            bootstrap,
        );
        let handle = QueueHandle {
            tx,
            info: Arc::clone(&info),
        };
        {
            let mut guard = self.entries.write().expect("queue registry lock poisoned");
            guard.insert(
                key.clone(),
                QueueEntry {
                    handle: handle.clone(),
                },
            );
        }
        metrics::gauge!("queueforge_queues").increment(1.0);
        debug!(
            vhost = %key.vhost,
            queue = %key.name,
            "queue restored from WAL recovery"
        );
        Ok(handle)
    }

    /// Run a metadata call on the blocking pool. The registry is used from async
    /// tasks; redb must not occupy a Tokio worker.
    async fn meta_blocking<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&dyn QueueMetaStore) -> Result<T> + Send + 'static,
    {
        let meta = Arc::clone(&self.meta);
        tokio::task::spawn_blocking(move || f(meta.as_ref()))
            .await
            .map_err(|e| Error::Store(format!("metadata task panicked: {e}")))?
    }

    /// Ensure durable meta row exists with matching props; fail closed on mismatch.
    async fn ensure_durable_meta(&self, key: &QueueKey, opts: &QueueDeclareOpts) -> Result<()> {
        let key = key.clone();
        let opts = opts.clone();
        self.meta_blocking(move |meta| ensure_durable_meta_sync(meta, &key, &opts))
            .await
    }

    async fn rollback_insert(&self, key: &QueueKey, handle: &QueueHandle) {
        {
            let mut guard = self.entries.write().expect("queue registry lock poisoned");
            // Only remove if still our handle (should be — we hold lifecycle).
            if let Some(entry) = guard.get(key) {
                if Arc::ptr_eq(&entry.handle.info, &handle.info) {
                    guard.remove(key);
                }
            }
        }
        let _ = shutdown_actor(&handle.tx).await;
    }

    async fn stats_result(&self, handle: QueueHandle) -> Result<DeclareResult> {
        match query_stats(&handle.tx).await {
            Some(stats) => Ok(DeclareResult {
                handle,
                message_count: stats.messages_ready,
                consumer_count: stats.consumer_count,
            }),
            // Mailbox closed / actor dead between availability check and stats.
            None => Err(Error::Unavailable(format!(
                "queue {} actor unavailable",
                handle.info.key
            ))),
        }
    }

    /// Delete a queue: optional unused/empty checks, shutdown actor, drop metadata.
    ///
    /// Returns the number of messages that were in the queue (0 until publish).
    pub async fn delete(&self, key: &QueueKey, if_unused: bool, if_empty: bool) -> Result<u32> {
        // Serialize with create so we never delete meta for a recreated queue.
        let _lifecycle = self.lifecycle.lock().await;

        let handle = {
            let guard = self.entries.read().expect("queue registry lock poisoned");
            guard.get(key).map(|e| e.handle.clone())
        };
        let Some(handle) = handle else {
            // Best-effort metadata cleanup for durable leftovers.
            // The map lock is released before this await (std RwLockGuard is !Send).
            let vhost = key.vhost.to_string();
            let name = key.name.to_string();
            let _ = self
                .meta_blocking(move |meta| meta.delete_queue(&vhost, &name))
                .await?;
            return Err(Error::NotFound(format!("queue {key}")));
        };

        let stats = if handle.is_available() {
            query_stats(&handle.tx).await.unwrap_or_default()
        } else {
            QueueStats::default()
        };

        if if_unused && stats.consumer_count > 0 {
            return Err(Error::PreconditionFailed(format!(
                "queue {key} is in use (consumers={})",
                stats.consumer_count
            )));
        }
        if if_empty && (stats.messages_ready > 0 || stats.messages_unacked > 0) {
            return Err(Error::PreconditionFailed(format!(
                "queue {key} is not empty"
            )));
        }

        // Remove only our handle; concurrent recreate under lifecycle is impossible
        // while we hold the lock, but ptr_eq still guards against logic bugs.
        let removed = {
            let mut guard = self.entries.write().expect("queue registry lock poisoned");
            match guard.get(key) {
                Some(entry) if Arc::ptr_eq(&entry.handle.info, &handle.info) => {
                    guard.remove(key);
                    true
                }
                Some(_) => false,
                None => false,
            }
        };

        if !removed {
            return Err(Error::NotFound(format!("queue {key}")));
        }

        metrics::gauge!("queueforge_queues").decrement(1.0);

        if handle.is_available() {
            let _ = shutdown_actor(&handle.tx).await;
        }

        // Safe: lifecycle held, map entry gone, no concurrent declare can recreate yet.
        let vhost = key.vhost.to_string();
        let name = key.name.to_string();
        let _ = self
            .meta_blocking(move |meta| meta.delete_queue(&vhost, &name))
            .await?;

        if handle.info.durable {
            if let Some(factory) = &self.durable_factory {
                if let Err(e) = factory.delete(key.vhost.as_str(), key.name.as_str()) {
                    debug!(
                        vhost = %key.vhost,
                        queue = %key.name,
                        error = %e,
                        "WAL delete after queue delete failed (best-effort)"
                    );
                }
            }
        }

        info!(vhost = %key.vhost, queue = %key.name, "queue deleted");
        Ok(stats.messages_ready)
    }
}

fn ensure_durable_meta_sync(
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

fn props_match(existing: &Queue, opts: &QueueDeclareOpts) -> bool {
    existing.durable == opts.durable
        && existing.exclusive == opts.exclusive
        && existing.auto_delete == opts.auto_delete
        && existing.args == opts.args
}

fn queue_to_opts(q: &Queue, request: &QueueDeclareOpts) -> QueueDeclareOpts {
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

fn spawn_supervised_actor(
    key: QueueKey,
    info: Arc<QueueInfo>,
    mailbox_capacity: usize,
    memory: Arc<MemoryTracker>,
    disk: Option<Arc<DiskBudget>>,
    bootstrap: QueueActorBootstrap,
) -> mpsc::Sender<QueueCmd> {
    let (tx, rx) = mpsc::channel(mailbox_capacity);
    let actor_key = key.clone();
    let info_for_actor = Arc::clone(&info);
    let memory_for_actor = Arc::clone(&memory);
    let join = tokio::spawn(async move {
        actor::run(
            actor_key,
            rx,
            memory_for_actor,
            disk,
            info_for_actor,
            bootstrap,
        )
        .await;
    });

    // Supervisor: on panic mark Unavailable + metric; do NOT restart empty.
    // Memory release is handled by the actor's MemoryGuard Drop during unwind.
    let sup_key = key;
    let info_for_sup = Arc::clone(&info);
    let memory_for_sup = Arc::clone(&memory);
    tokio::spawn(async move {
        match join.await {
            Ok(()) => {
                debug!(vhost = %sup_key.vhost, queue = %sup_key.name, "queue actor exited cleanly");
            }
            Err(join_err) if join_err.is_panic() => {
                error!(
                    vhost = %sup_key.vhost,
                    queue = %sup_key.name,
                    "queue actor panicked; marking unavailable (no auto-restart)"
                );
                info_for_sup.mark_unavailable();
                // Safety net if Drop did not run (e.g. abort) — release residual.
                let residual = info_for_sup.reserved_bytes.swap(0, Ordering::AcqRel);
                if residual > 0 {
                    memory_for_sup.sub(residual);
                    warn_reserved_release(&sup_key, residual);
                }
                metrics::counter!("queueforge_queue_actor_panics_total").increment(1);
            }
            Err(join_err) => {
                debug!(
                    vhost = %sup_key.vhost,
                    queue = %sup_key.name,
                    error = %join_err,
                    "queue actor join error (cancelled?)"
                );
                let residual = info_for_sup.reserved_bytes.swap(0, Ordering::AcqRel);
                if residual > 0 {
                    memory_for_sup.sub(residual);
                }
            }
        }
    });

    tx
}

fn warn_reserved_release(key: &QueueKey, residual: u64) {
    tracing::warn!(
        vhost = %key.vhost,
        queue = %key.name,
        residual,
        "released residual memory reservation after queue actor panic"
    );
}

async fn shutdown_actor(tx: &mpsc::Sender<QueueCmd>) -> bool {
    let (reply_tx, reply_rx) = oneshot::channel();
    if tx
        .send(QueueCmd::Shutdown { reply: reply_tx })
        .await
        .is_err()
    {
        return false;
    }
    // Treat Ok(Ok) and Ok(Err(fsync)) as "actor stopped"; only missing reply fails.
    reply_rx.await.is_ok()
}

async fn query_stats(tx: &mpsc::Sender<QueueCmd>) -> Option<QueueStats> {
    let (reply_tx, reply_rx) = oneshot::channel();
    if tx.send(QueueCmd::Stats { reply: reply_tx }).await.is_err() {
        return None;
    }
    reply_rx.await.ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::meta::NoopMetaStore;
    use std::collections::HashMap as StdHashMap;
    use std::sync::Mutex;

    /// In-memory meta store for durable persist tests.
    #[derive(Default)]
    struct MemMeta {
        queues: Mutex<StdHashMap<(String, String), Queue>>,
    }

    impl QueueMetaStore for MemMeta {
        fn create_queue(&self, queue: &Queue) -> Result<()> {
            let mut g = self.queues.lock().unwrap();
            let key = (queue.vhost.to_string(), queue.name.to_string());
            if g.contains_key(&key) {
                return Err(Error::AlreadyExists(format!(
                    "queue {}/{}",
                    queue.vhost, queue.name
                )));
            }
            g.insert(key, queue.clone());
            Ok(())
        }

        fn put_queue(&self, queue: &Queue) -> Result<()> {
            let mut g = self.queues.lock().unwrap();
            g.insert(
                (queue.vhost.to_string(), queue.name.to_string()),
                queue.clone(),
            );
            Ok(())
        }

        fn get_queue(&self, vhost: &str, name: &str) -> Result<Option<Queue>> {
            let g = self.queues.lock().unwrap();
            Ok(g.get(&(vhost.to_string(), name.to_string())).cloned())
        }

        fn delete_queue(&self, vhost: &str, name: &str) -> Result<bool> {
            let mut g = self.queues.lock().unwrap();
            Ok(g.remove(&(vhost.to_string(), name.to_string())).is_some())
        }
    }

    fn test_registry() -> QueueRegistry {
        QueueRegistry::new(Arc::new(NoopMetaStore), MemoryTracker::shared())
            .with_mailbox_capacity(16)
    }

    fn mem_registry() -> (Arc<MemMeta>, QueueRegistry) {
        let meta = Arc::new(MemMeta::default());
        let reg = QueueRegistry::new(
            Arc::clone(&meta) as Arc<dyn QueueMetaStore>,
            MemoryTracker::shared(),
        )
        .with_mailbox_capacity(16);
        (meta, reg)
    }

    #[tokio::test]
    async fn declare_lookup_delete() {
        let reg = test_registry();
        let opts = QueueDeclareOpts::default();
        let declared = reg.declare("/", "orders", opts).await.expect("declare");
        assert_eq!(declared.handle.info.key.name.as_str(), "orders");
        assert!(declared.handle.is_available());
        assert_eq!(reg.len(), 1);

        let key = QueueKey::new("/", "orders");
        let got = reg.get(&key).expect("lookup");
        assert_eq!(got.info.key.name.as_str(), "orders");

        let msgs = reg.delete(&key, false, false).await.expect("delete");
        assert_eq!(msgs, 0);
        assert!(reg.get(&key).is_none());
        assert!(reg.is_empty());
    }

    #[tokio::test]
    async fn declare_idempotent_same_props() {
        let reg = test_registry();
        let opts = QueueDeclareOpts {
            durable: true,
            ..QueueDeclareOpts::default()
        };
        let _ = reg.declare("/", "q1", opts.clone()).await.unwrap();
        let again = reg.declare("/", "q1", opts).await.unwrap();
        assert_eq!(again.handle.info.key.name.as_str(), "q1");
        assert_eq!(reg.len(), 1);
    }

    #[tokio::test]
    async fn declare_rejects_property_mismatch() {
        let reg = test_registry();
        let _ = reg
            .declare("/", "q1", QueueDeclareOpts::default())
            .await
            .unwrap();
        let err = reg
            .declare(
                "/",
                "q1",
                QueueDeclareOpts {
                    durable: true,
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::PreconditionFailed(_)));
    }

    #[tokio::test]
    async fn passive_missing_is_not_found() {
        let reg = test_registry();
        let err = reg
            .declare(
                "/",
                "nope",
                QueueDeclareOpts {
                    passive: true,
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn server_named_queue() {
        let reg = test_registry();
        let a = reg
            .declare("/", "", QueueDeclareOpts::default())
            .await
            .unwrap();
        let b = reg
            .declare("/", "", QueueDeclareOpts::default())
            .await
            .unwrap();
        assert!(a.handle.info.key.name.starts_with("amq.gen-"));
        assert!(b.handle.info.key.name.starts_with("amq.gen-"));
        assert_ne!(
            a.handle.info.key.name.as_str(),
            b.handle.info.key.name.as_str(),
            "concurrent empty-name declares must not collide"
        );
        assert_eq!(reg.len(), 2);
    }

    #[tokio::test]
    async fn durable_persists_to_meta() {
        let (meta, reg) = mem_registry();
        let _ = reg
            .declare(
                "/",
                "dur",
                QueueDeclareOpts {
                    durable: true,
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap();
        let stored = meta.get_queue("/", "dur").unwrap().expect("stored");
        assert!(stored.durable);

        let key = QueueKey::new("/", "dur");
        let _ = reg.delete(&key, false, false).await.unwrap();
        assert!(meta.get_queue("/", "dur").unwrap().is_none());
    }

    #[tokio::test]
    async fn transient_not_persisted() {
        let (meta, reg) = mem_registry();
        let _ = reg
            .declare("/", "tmp", QueueDeclareOpts::default())
            .await
            .unwrap();
        assert!(meta.get_queue("/", "tmp").unwrap().is_none());
    }

    #[tokio::test]
    async fn actor_panic_marks_unavailable_no_restart() {
        let reg = test_registry();
        let declared = reg
            .declare("/", "boom", QueueDeclareOpts::default())
            .await
            .unwrap();
        let handle = declared.handle;

        // Enqueue so the queue holds reserved memory before panic.
        let msg = Arc::new(crate::queue::Message {
            exchange: CompactString::from(""),
            routing_key: CompactString::from("boom"),
            body: bytes::Bytes::from_static(b"leak-check"),
            persistent: false,
            redelivered: false,
            content_type: None,
            content_encoding: None,
            correlation_id: None,
            message_id: None,
            reply_to: None,
            expiration: None,
            app_id: None,
            user_id: None,
            type_: None,
            priority: None,
            timestamp: None,
            expires_unix_ms: None,
            headers: Default::default(),
        });
        let tracked = msg.tracked_bytes();
        let before = reg.memory.tracked_bytes();
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .tx
            .send(QueueCmd::Enqueue {
                msg,
                reply: reply_tx,
            })
            .await
            .expect("enqueue");
        reply_rx.await.unwrap().expect("enqueue ok");
        assert!(reg.memory.tracked_bytes() >= before + tracked);
        assert!(handle.info.reserved_bytes() >= tracked);

        handle
            .tx
            .send(QueueCmd::TestPanic)
            .await
            .expect("send panic");

        // Wait until supervisor marks unavailable.
        let mut ok = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            if !handle.is_available() {
                ok = true;
                break;
            }
        }
        assert!(ok, "expected unavailable after panic");

        // Issue 1: panic must release residual watermark reservation.
        for _ in 0..50 {
            if reg.memory.tracked_bytes() == before && handle.info.reserved_bytes() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            handle.info.reserved_bytes(),
            0,
            "per-queue reservation must be released after panic"
        );
        assert_eq!(
            reg.memory.tracked_bytes(),
            before,
            "global tracked_bytes must not stay inflated after queue panic"
        );

        // Still registered — no silent remove/restart.
        assert!(reg.get(&QueueKey::new("/", "boom")).is_some());
        assert_eq!(reg.len(), 1);

        // Active redeclare must surface unavailable.
        let err = reg
            .declare("/", "boom", QueueDeclareOpts::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unavailable(_)));

        // Passive declare must also surface unavailable (Issue 3).
        let err = reg
            .declare(
                "/",
                "boom",
                QueueDeclareOpts {
                    passive: true,
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unavailable(_)));
    }

    #[tokio::test]
    async fn delete_missing_is_not_found() {
        let reg = test_registry();
        let err = reg
            .delete(&QueueKey::new("/", "missing"), false, false)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn exclusive_owner_lock() {
        let reg = test_registry();
        let _ = reg
            .declare(
                "/",
                "ex",
                QueueDeclareOpts {
                    exclusive: true,
                    exclusive_owner: Some(CompactString::from("conn-a")),
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap();
        let err = reg
            .declare(
                "/",
                "ex",
                QueueDeclareOpts {
                    exclusive: true,
                    exclusive_owner: Some(CompactString::from("conn-b")),
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ResourceLocked(_)));
    }

    #[tokio::test]
    async fn concurrent_durable_declare_and_delete_meta_consistent() {
        let (meta, reg) = mem_registry();
        let reg = Arc::new(reg);
        let key = QueueKey::new("/", "race-dd");

        // Seed a durable queue, then thrash declare ‖ delete.
        let _ = reg
            .declare(
                "/",
                "race-dd",
                QueueDeclareOpts {
                    durable: true,
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap();

        let mut joins = Vec::new();
        for i in 0..20 {
            let reg_d = Arc::clone(&reg);
            let reg_c = Arc::clone(&reg);
            let key_d = key.clone();
            joins.push(tokio::spawn(async move {
                let _ = reg_d.delete(&key_d, false, false).await;
            }));
            joins.push(tokio::spawn(async move {
                let _ = reg_c
                    .declare(
                        "/",
                        "race-dd",
                        QueueDeclareOpts {
                            durable: true,
                            ..QueueDeclareOpts::default()
                        },
                    )
                    .await;
                let _ = i;
            }));
        }
        for j in joins {
            let _ = j.await;
        }

        // Invariant: if registry has a durable live entry, meta row must exist.
        // If registry is empty, meta must be absent (no orphan durable definition).
        match reg.get(&key) {
            Some(h) => {
                assert!(h.info.durable);
                assert!(
                    meta.get_queue("/", "race-dd").unwrap().is_some(),
                    "live durable actor must have meta row"
                );
            }
            None => {
                assert!(
                    meta.get_queue("/", "race-dd").unwrap().is_none(),
                    "no orphan durable meta when queue absent"
                );
            }
        }
    }

    #[tokio::test]
    async fn concurrent_durable_vs_transient_no_orphan_meta() {
        let (meta, reg) = mem_registry();
        let reg = Arc::new(reg);

        let mut joins = Vec::new();
        for _ in 0..16 {
            let r1 = Arc::clone(&reg);
            let r2 = Arc::clone(&reg);
            joins.push(tokio::spawn(async move {
                let _ = r1
                    .declare(
                        "/",
                        "race-props",
                        QueueDeclareOpts {
                            durable: true,
                            ..QueueDeclareOpts::default()
                        },
                    )
                    .await;
            }));
            joins.push(tokio::spawn(async move {
                let _ = r2
                    .declare(
                        "/",
                        "race-props",
                        QueueDeclareOpts {
                            durable: false,
                            ..QueueDeclareOpts::default()
                        },
                    )
                    .await;
            }));
        }
        for j in joins {
            let _ = j.await;
        }

        let key = QueueKey::new("/", "race-props");
        match reg.get(&key) {
            Some(h) if h.info.durable => {
                assert!(meta.get_queue("/", "race-props").unwrap().is_some());
            }
            Some(h) => {
                // Transient winner: must not leave durable meta from a loser.
                assert!(!h.info.durable);
                assert!(
                    meta.get_queue("/", "race-props").unwrap().is_none(),
                    "transient live queue must not have orphan durable meta"
                );
            }
            None => {
                assert!(meta.get_queue("/", "race-props").unwrap().is_none());
            }
        }
    }

    #[tokio::test]
    async fn concurrent_double_delete_second_is_not_found() {
        let reg = Arc::new(test_registry());
        let _ = reg
            .declare("/", "once", QueueDeclareOpts::default())
            .await
            .unwrap();
        let key = QueueKey::new("/", "once");

        let r1 = Arc::clone(&reg);
        let r2 = Arc::clone(&reg);
        let k1 = key.clone();
        let k2 = key.clone();
        let (a, b) = tokio::join!(
            async move { r1.delete(&k1, false, false).await },
            async move { r2.delete(&k2, false, false).await },
        );

        let oks = [a.is_ok(), b.is_ok()].into_iter().filter(|x| *x).count();
        let not_founds = [a, b]
            .into_iter()
            .filter(|r| matches!(r, Err(Error::NotFound(_))))
            .count();
        assert_eq!(oks, 1, "exactly one delete succeeds");
        assert_eq!(not_founds, 1, "loser is NotFound");
        assert!(reg.is_empty());
    }

    #[tokio::test]
    async fn durable_vs_transient_sequential_mismatch() {
        let (meta, reg) = mem_registry();
        let _ = reg
            .declare(
                "/",
                "mix",
                QueueDeclareOpts {
                    durable: true,
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap();
        let err = reg
            .declare("/", "mix", QueueDeclareOpts::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::PreconditionFailed(_)));
        // Durable meta still present for the live durable queue.
        assert!(meta.get_queue("/", "mix").unwrap().is_some());
    }

    #[tokio::test]
    async fn dlx_routes_on_nack_no_requeue() {
        use crate::domain::{Binding, Exchange, ExchangeType};
        use crate::queue::args::QueueArgs;
        use crate::queue::cmd::{ConsumerSessionId, Message, QueueCmd, QueueDelivery};
        use crate::queue::dlx::DlxRouter;
        use crate::router::ExchangeRouter;
        use bytes::Bytes;
        use compact_str::CompactString;
        use std::sync::Arc;
        use tokio::sync::{mpsc, oneshot};

        let reg = Arc::new(test_registry());
        let router = Arc::new(ExchangeRouter::new());
        router.put_exchange(Exchange::new("/", "dlx", ExchangeType::Fanout));
        reg.set_dlx(Arc::new(DlxRouter::new(
            Arc::clone(&router),
            Arc::downgrade(&reg),
        )));

        // Dead-letter destination.
        let _ = reg
            .declare("/", "dead", QueueDeclareOpts::default())
            .await
            .unwrap();
        router.bind(Binding::new("/", "dlx", "dead", "")).unwrap();

        // Source with DLX.
        let src = reg
            .declare(
                "/",
                "src",
                QueueDeclareOpts {
                    args: QueueArgs {
                        dead_letter_exchange: Some(CompactString::from("dlx")),
                        ..QueueArgs::default()
                    },
                    ..QueueDeclareOpts::default()
                },
            )
            .await
            .unwrap();

        let msg = Arc::new(Message {
            exchange: CompactString::from(""),
            routing_key: CompactString::from("src"),
            body: Bytes::from_static(b"poison"),
            persistent: false,
            redelivered: false,
            content_type: None,
            content_encoding: None,
            correlation_id: None,
            message_id: None,
            reply_to: None,
            expiration: None,
            app_id: None,
            user_id: None,
            type_: None,
            priority: None,
            timestamp: None,
            expires_unix_ms: None,
            headers: Default::default(),
        });
        let (reply_tx, reply_rx) = oneshot::channel();
        src.handle
            .tx
            .send(QueueCmd::Enqueue {
                msg,
                reply: reply_tx,
            })
            .await
            .unwrap();
        let _ = reply_rx.await.unwrap().unwrap();

        let (dtx, mut drx) = mpsc::channel::<QueueDelivery>(64);
        let (reg_tx, reg_rx) = oneshot::channel();
        src.handle
            .tx
            .send(QueueCmd::RegisterConsumer {
                session: ConsumerSessionId(1),
                no_ack: false,
                exclusive: false,
                priority: 0,
                initial_credit: Some(1),
                deliver_tx: dtx,
                reply: reg_tx,
            })
            .await
            .unwrap();
        reg_rx.await.unwrap().unwrap();
        let d = drx.recv().await.unwrap();

        src.handle
            .tx
            .send(QueueCmd::Nack {
                id: d.delivery_id,
                requeue: false,
            })
            .await
            .unwrap();

        // Message should appear on dead queue with x-death.
        let dead = reg.get(&QueueKey::new("/", "dead")).unwrap();
        let mut got = None;
        for _ in 0..50 {
            let (gtx, grx) = oneshot::channel();
            dead.tx
                .send(QueueCmd::Get {
                    no_ack: true,
                    reply: gtx,
                })
                .await
                .unwrap();
            if let Some((_, qm, _)) = grx.await.unwrap() {
                got = Some(qm);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let qm = got.expect("dead-lettered message on dead queue");
        assert_eq!(qm.message.body.as_ref(), b"poison");
        assert_eq!(qm.message.headers.deaths.len(), 1);
        assert_eq!(qm.message.headers.deaths[0].queue.as_str(), "src");
        assert_eq!(
            qm.message.headers.deaths[0].reason,
            crate::queue::dlx::DeathReason::Rejected
        );
    }

    #[tokio::test]
    async fn shutdown_all_stops_actors_and_clears_registry() {
        let reg = test_registry();
        let _ = reg
            .declare("/", "a", QueueDeclareOpts::default())
            .await
            .unwrap();
        let _ = reg
            .declare("/", "b", QueueDeclareOpts::default())
            .await
            .unwrap();
        assert_eq!(reg.len(), 2);

        let report = reg.shutdown_all().await;
        assert!(report.is_clean(), "report={report:?}");
        assert_eq!(report.queues, 2);
        assert!(reg.is_empty());
        assert!(reg.get(&QueueKey::new("/", "a")).is_none());
    }
}
