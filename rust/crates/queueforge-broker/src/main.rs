//! QueueForge broker binary shell.
//!
//! Loads configuration, opens the metadata store, bootstraps admin auth when
//! needed, initializes logging, metrics/health and management HTTP listeners,
//! starts the AMQP TCP listener, and on SIGINT/SIGTERM runs the ordered graceful
//! drain: readyz 503 → stop accept → connection.close → queue Shutdown/fsync → redb close.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use queueforge_auth::{AuthService, BootstrapMode};
use queueforge_broker::{
    load_server_config, start_amqp_listener_with_limits, with_https_alpn, ConnectionLimiter,
    ConnectionParams, CONNECTION_DRAIN_TIMEOUT, DEFAULT_CHANNEL_MAX, DEFAULT_FRAME_MAX,
    DEFAULT_MAX_MESSAGE_BYTES, FRAME_MAX_FLOOR,
};
use queueforge_core::{
    system_total_memory_bytes, Config, DiskBudget, DlxRouter, DurabilityPolicy, ExchangeRouter,
    MemoryTracker, QueueMetaStore, QueueRegistry,
};
use queueforge_metrics::{install_recorder, start_server, ReadyFlag};
use queueforge_mgmt::{
    start_server as start_mgmt_server, ConnectionTracker, MgmtConfig, MgmtState,
};
use queueforge_store::{recover_durable_queues, MetadataStore, RecoveryConfig, WalFactory};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// QueueForge — high-performance message queue broker.
#[derive(Debug, Parser)]
#[command(
    name = "queueforge",
    version,
    about = "QueueForge message broker",
    long_about = None
)]
struct Cli {
    /// Path to TOML configuration file.
    #[arg(short, long, env = "QUEUEFORGE_CONFIG", value_name = "PATH")]
    config: Option<PathBuf>,

    /// When the user table is empty and admin env vars are unset, create a
    /// local development administrator (`admin` / `devpassword12`).
    ///
    /// Production deployments must set `QUEUEFORGE_ADMIN_USER` and
    /// `QUEUEFORGE_ADMIN_PASSWORD` instead. No remote `guest`/`guest`.
    #[arg(long, default_value_t = false)]
    dev_bootstrap: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    let mut config = match &cli.config {
        Some(path) => Config::load_from_file(path)
            .with_context(|| format!("loading config {}", path.display()))?,
        None => Config::default(),
    };
    config
        .apply_env_overrides()
        .context("applying environment overrides")?;
    config.validate().context("validating configuration")?;
    config.tls.validate().context("invalid TLS configuration")?;

    init_tracing(&config.logging.level)?;

    let tls_config = if config.tls.enabled {
        let cert = config.tls.cert_path_required().context("TLS cert_path")?;
        let key = config.tls.key_path_required().context("TLS key_path")?;
        let cfg = load_server_config(cert, key).with_context(|| {
            format!(
                "loading TLS cert {} / key {}",
                cert.display(),
                key.display()
            )
        })?;
        info!(cert = %cert.display(), "TLS enabled (AMQPS + HTTPS)");
        Some(cfg)
    } else {
        None
    };

    let total_ram = system_total_memory_bytes();
    info!(
        version = env!("CARGO_PKG_VERSION"),
        amqp = %config.listeners.amqp,
        management = %config.listeners.management,
        metrics = %config.listeners.metrics,
        data_dir = %config.data.dir.display(),
        total_ram_bytes = total_ram,
        soft_watermark = config.memory.soft_watermark_relative,
        high_watermark = config.memory.high_watermark_relative,
        disk_free_limit = config.data.disk_free_limit_bytes,
        max_connections = config.limits.max_connections,
        max_message_bytes = config.limits.max_message_bytes,
        tls = config.tls.enabled,
        "QueueForge starting"
    );

    // Create data_dir if needed and open (or bootstrap) the metadata store.
    let store = MetadataStore::open(&config.data.dir).context("opening metadata store")?;
    info!(
        data_dir = %store.data_dir().display(),
        schema_version = store.schema_version().context("reading schema version")?,
        vhosts = store.list_vhosts().context("listing vhosts")?.len(),
        "metadata store ready"
    );

    // Bootstrap administrator when the user table is empty.
    let auth = AuthService::new(&store);
    let bootstrap_mode = if cli.dev_bootstrap {
        BootstrapMode::DevFallback
    } else {
        BootstrapMode::EnvRequired
    };
    let bootstrapped = auth
        .bootstrap_admin_if_empty(bootstrap_mode)
        .context("bootstrapping administrator")?;
    info!(
        users = store.user_count().context("counting users")?,
        bootstrapped, "auth ready"
    );

    let store = Arc::new(store);

    // Durability: WAL factory + group-commit policy from config.
    let durability_policy = DurabilityPolicy::from_parts(
        config.data.fsync_policy,
        config.data.fsync_interval_ms,
        config.data.fsync_every_n_messages,
    );
    let wal_factory = Arc::new(WalFactory::new(
        store.data_dir(),
        config.data.wal_segment_max_bytes,
    ));

    // Memory watermarks + disk free budget.
    let memory = MemoryTracker::shared_from_config(&config.memory);
    info!(
        soft_limit_bytes = memory.soft_limit_bytes(),
        hard_limit_bytes = memory.hard_limit_bytes(),
        "memory tracker ready"
    );
    let disk = DiskBudget::shared(
        store.data_dir().to_path_buf(),
        config.data.disk_free_limit_bytes,
    );
    let free = disk.refresh();
    info!(
        disk_free_bytes = free,
        disk_free_limit = disk.limit_bytes(),
        "disk budget ready"
    );

    // Queue registry + per-queue actors (with durable WAL for durable declares).
    let mut registry = QueueRegistry::new(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        Arc::clone(&memory),
    )
    .with_durability(wal_factory, durability_policy)
    .with_disk_budget(Arc::clone(&disk))
    .with_mailbox_capacity(config.limits.queue_enqueue_bound);
    if config.cluster.is_enabled() {
        registry = registry.with_local_node(config.cluster.node_id.clone());
    }
    let queues: Arc<QueueRegistry> = Arc::new(registry);
    let connections = ConnectionTracker::shared();

    // Exchange router before recovery so durable actors get a live DLX handle.
    // Implicit default-exchange routing is queue-name lookup (no binding rows).
    let router: Arc<ExchangeRouter> = Arc::new(
        store
            .bootstrap_router()
            .context("bootstrapping exchange router")?,
    );
    queues.set_dlx(Arc::new(DlxRouter::new(
        Arc::clone(&router),
        Arc::downgrade(&queues),
    )));
    info!(
        exchanges = router.list_exchanges("/").len(),
        bindings = router.index().len(),
        "exchange router ready (user bindings loaded)"
    );

    // x-expires auto-delete worker.
    if let Some(mut expired_rx) = queues.take_expired_rx().await {
        let queues_exp = Arc::clone(&queues);
        let router_exp = Arc::clone(&router);
        tokio::spawn(async move {
            while let Some(key) = expired_rx.recv().await {
                match queues_exp.delete(&key, false, false).await {
                    Ok(_) => {
                        router_exp.remove_queue_bindings(key.vhost.as_str(), key.name.as_str());
                        queueforge_core::prom::queue_deleted();
                        info!(vhost = %key.vhost, queue = %key.name, "queue deleted by x-expires");
                    }
                    Err(e) => {
                        tracing::debug!(
                            vhost = %key.vhost,
                            queue = %key.name,
                            error = %e,
                            "x-expires delete skipped"
                        );
                    }
                }
            }
        });
    }

    // Metrics recorder + health endpoints.
    // `/readyz` stays 503 until durable WAL recovery finishes.
    let ready = ReadyFlag::new();
    let metrics_handle = install_recorder().context("installing metrics recorder")?;
    queueforge_core::prom::prime();
    queueforge_core::prom::identity(
        &config.cluster.local_node_id().unwrap_or_else(|| "queueforge".into()),
        "queueforge",
    );
    let metrics_server = start_server(config.listeners.metrics, metrics_handle, ready.clone())
        .await
        .context("starting metrics/health listener")?;
    info!(
        local_addr = %metrics_server.local_addr,
        "metrics/health listening (readyz=503 during recovery)"
    );

    // Periodic disk free gauge refresh.
    {
        let disk = Arc::clone(&disk);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                disk.refresh();
            }
        });
    }

    // Recovery algorithm: exclusive purge, WAL replay, spawn durable actors.
    let recovery_cfg = RecoveryConfig {
        wal_segment_max_bytes: config.data.wal_segment_max_bytes,
        durability_policy,
        local_node: config.cluster.local_node_id(),
    };
    let recovery_report = recover_durable_queues(store.as_ref(), queues.as_ref(), &recovery_cfg)
        .await
        .context("durable queue recovery")?;
    info!(
        exclusive_purged = recovery_report.exclusive_purged,
        queues_restored = recovery_report.queues_restored,
        messages_recovered = recovery_report.messages_recovered,
        queues_corrupt = recovery_report.queues_corrupt,
        "recovery finished"
    );

    // Issue 7: mark ready before binding AMQP so /readyz is not 503 while
    // the listener already accepts connections (recovery is already finished).
    ready.set_ready(true);
    info!("broker ready (recovery complete)");

    // Management HTTP API (session auth + resource CRUD).
    // Dev cookie defaults: HttpOnly + SameSite=Lax, Secure=false (plain HTTP).
    let (replicate_tx, replicate_rx) = if config.cluster.is_enabled() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let mut mgmt_state = MgmtState::new(
        Arc::clone(&store),
        Arc::clone(&queues),
        Arc::clone(&router),
        Arc::clone(&connections),
        ready.clone(),
        MgmtConfig {
            cookie_secure: config.tls.enabled,
            product_version: env!("CARGO_PKG_VERSION").to_string(),
            trusted_proxy_cidrs: config.management.trusted_proxy_cidrs.clone(),
        },
    );
    if let Some(tx) = replicate_tx {
        mgmt_state = mgmt_state.with_replicator(tx);
    }
    let mgmt_tls = tls_config
        .as_ref()
        .map(|c| with_https_alpn(std::sync::Arc::clone(c)));
    let mgmt_server = start_mgmt_server(config.listeners.management, mgmt_state, mgmt_tls)
        .await
        .with_context(|| {
            format!(
                "binding management listener on {}",
                config.listeners.management
            )
        })?;
    info!(local_addr = %mgmt_server.local_addr, tls = mgmt_server.tls, "management HTTP listening");

    let conn_params = ConnectionParams {
        channel_max: if config.limits.channel_max == 0 {
            DEFAULT_CHANNEL_MAX
        } else {
            config.limits.channel_max
        },
        frame_max: {
            let f = if config.limits.frame_max == 0 {
                DEFAULT_FRAME_MAX
            } else {
                config.limits.frame_max
            };
            f.max(FRAME_MAX_FLOOR)
        },
        heartbeat: if config.limits.heartbeat_default == 0 {
            // 0 disables heartbeats — pass through as offered.
            0
        } else {
            config.limits.heartbeat_default
        },
        max_message_bytes: if config.limits.max_message_bytes == 0 {
            DEFAULT_MAX_MESSAGE_BYTES
        } else {
            config.limits.max_message_bytes
        },
        default_prefetch: if config.limits.default_prefetch == 0 {
            256
        } else {
            config.limits.default_prefetch
        },
        default_queue_type: if config.limits.default_queue_type == "quorum" {
            queueforge_core::QueueType::Quorum
        } else {
            queueforge_core::QueueType::Classic
        },
    };

    let conn_limiter = ConnectionLimiter::shared(config.limits.max_connections);

    let cluster = if config.cluster.is_enabled() {
        let listen = config.cluster.listen.context("cluster.listen")?;
        Some(
            queueforge_broker::Cluster::start(
                config.cluster.node_id.clone(),
                config.cluster.members.clone(),
                listen,
                Arc::clone(&store),
                Arc::clone(&queues),
                Arc::clone(&router),
            )
            .await
            .context("starting cluster listener")?,
        )
    } else {
        None
    };
    if let (Some(cluster), Some(mut replicate_rx)) = (cluster.clone(), replicate_rx) {
        tokio::spawn(async move {
            while let Some(req) = replicate_rx.recv().await {
                cluster.replicate_json(&req.kind, req.payload).await;
                let _ = req.done.send(());
            }
        });
    }

    // AMQP TCP listener + connection state machine.
    let amqp_listener = start_amqp_listener_with_limits(
        config.listeners.amqp,
        Arc::clone(&store),
        Arc::clone(&queues),
        Arc::clone(&router),
        Arc::clone(&connections),
        conn_params,
        Some(conn_limiter),
        tls_config,
        cluster.clone(),
    )
    .await
    .with_context(|| format!("binding AMQP listener on {}", config.listeners.amqp))?;
    info!(local_addr = %amqp_listener.local_addr, tls = amqp_listener.tls, "AMQP listener ready");
    if let Some(addr) = config.listeners.mqtt {
        queueforge_broker::protocols::spawn_mqtt(addr, Arc::clone(&queues));
        info!(%addr, "MQTT listening");
    }
    if let Some(addr) = config.listeners.stomp {
        queueforge_broker::protocols::spawn_stomp(addr, Arc::clone(&queues));
        info!(%addr, "STOMP listening");
    }
    if let Some(addr) = config.listeners.stream {
        queueforge_broker::protocols::spawn_stream(addr, Arc::clone(&queues));
        info!(%addr, "stream listening");
    }

    wait_for_shutdown().await?;

    // ── Ordered graceful shutdown (design Operability section) ─────────
    // 1. /readyz → 503; stop accepting new TCP connections.
    // 2. Close AMQP listeners; send connection.close (timeout 10s).
    // 3. Stop management write routes (mgmt lands in a later PR).
    // 4. Drain queue actors: fsync WAL + watermark; answer Shutdown.
    // 5. Flush/close redb; exit 0.

    ready.set_ready(false);
    info!("readyz=503; beginning graceful shutdown");

    // Stop accept + broadcast connection.close; wait for connection tasks.
    let conns_drained = amqp_listener.graceful_stop(CONNECTION_DRAIN_TIMEOUT).await;

    // Drain queue actors (durable actors fsync in Shutdown); surface fsync errors.
    let queue_report = queues.shutdown_all().await;
    drop(queues);
    drop(router);

    // Stop management HTTP after AMQP drain.
    mgmt_server.abort();

    // Metrics/health can go away once drain is done (/healthz no longer needed).
    metrics_server.abort();

    // Explicit redb close after no connection/queue tasks hold store refs.
    match Arc::try_unwrap(store) {
        Ok(store) => store.close(),
        Err(store) => {
            warn!(
                refs = Arc::strong_count(&store),
                "metadata store still shared after drain; dropping remaining Arcs"
            );
            drop(store);
        }
    }

    // Issue 3/4: do not claim clean exit when fsync failed or drain timed out.
    if !queue_report.is_clean() || !conns_drained {
        warn!(
            conns_drained,
            fsync_errors = queue_report.fsync_errors,
            queue_timeouts = queue_report.timeouts,
            "QueueForge shut down with errors"
        );
        anyhow::bail!(
            "graceful shutdown incomplete (conns_drained={conns_drained}, \
             fsync_errors={}, queue_timeouts={})",
            queue_report.fsync_errors,
            queue_report.timeouts
        );
    }

    info!("QueueForge shut down cleanly");
    Ok(())
}

fn init_tracing(level: &str) -> Result<()> {
    let filter =
        EnvFilter::try_new(level).with_context(|| format!("invalid log filter {level:?}"))?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .compact()
        .init();

    Ok(())
}

async fn wait_for_shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
        let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

        tokio::select! {
            _ = sigint.recv() => {
                info!("received SIGINT");
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .context("install ctrl-c handler")?;
        info!("received Ctrl-C");
    }

    Ok(())
}
