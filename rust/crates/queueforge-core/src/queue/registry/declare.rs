//! Queue declare, idempotent redeclare, durable metadata, and recovery restore.

use std::sync::Arc;

use compact_str::CompactString;
use tokio::sync::oneshot;
use tracing::{debug, info};

use super::super::cmd::QueueCmd;
use super::super::durable::QueueActorBootstrap;
use super::super::meta::QueueMetaStore;
use super::shutdown::{query_stats, shutdown_actor, spawn_supervised_actor};
use super::{
    DeclareResult, QueueDeclareOpts, QueueEntry, QueueHandle, QueueInfo, QueueKey, QueueRegistry,
};
use crate::domain::Queue;
use crate::error::{Error, Result};

impl QueueRegistry {
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

    /// Redeclare `key` when `entry` is already in the map. `opts` is the new request. Returns the existing handle when properties match, or a precondition error when they do not. Passive declare of a missing queue is not this path.
    pub(super) async fn declare_existing(
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
    pub(super) async fn spawn_insert_and_persist(
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

        if opts.args.queue_type == Some(crate::queue::QueueType::Stream)
            && (!opts.durable || opts.exclusive || opts.auto_delete)
        {
            return Err(Error::PreconditionFailed(
                "a stream queue must be durable, non-exclusive and not auto-delete".into(),
            ));
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

    /// Build the actor bootstrap for a new `key` from `opts`. Returns the bootstrap the supervised task starts with. Recovered messages are not included; restore has its own path.
    pub(super) fn bootstrap_for_declare(
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
        boot.replicator = self.stream_replicator();
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
        boot.replicator = self.stream_replicator();
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
    pub(super) async fn meta_blocking<T, F>(&self, f: F) -> Result<T>
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
    pub(super) async fn ensure_durable_meta(
        &self,
        key: &QueueKey,
        opts: &QueueDeclareOpts,
    ) -> Result<()> {
        let key = key.clone();
        let opts = opts.clone();
        self.meta_blocking(move |meta| ensure_durable_meta_sync(meta, &key, &opts))
            .await
    }

    /// Remove `key` and stop `handle` after a declare fails. The map must not keep an actor whose metadata write failed.
    pub(super) async fn rollback_insert(&self, key: &QueueKey, handle: &QueueHandle) {
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

    /// Read stats from `handle` and wrap them in a declare result. Returns the result, or unavailable when the actor does not answer.
    pub(super) async fn stats_result(&self, handle: QueueHandle) -> Result<DeclareResult> {
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
}

pub use super::declare_sync::generate_server_queue_name;
pub(super) use super::declare_sync::{ensure_durable_meta_sync, queue_to_opts};
