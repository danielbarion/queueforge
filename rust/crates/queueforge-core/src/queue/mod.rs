//! Per-queue actors, command mailbox, and the [`QueueRegistry`].

mod actor;
mod args;
mod cmd;
mod dlx;
mod durable;
mod meta;
mod ready;
mod registry;

pub use actor::{run as run_queue_actor, DEFAULT_MAILBOX_CAPACITY};
pub use args::{
    ArgValue, DeadLetterStrategy, OverflowPolicy, QueueArgs, QueueType, DEFAULT_MAX_DEATH_HOPS,
};
pub use cmd::{
    AppHeaderValue, ConsumerDeliveryId, ConsumerSessionId, DlxFailAction, EnqueueCompletion,
    Message, MessageHeaders, QueueCmd, QueueDelivery, QueueMessage, QueueOffset, QueueStats,
};
pub use dlx::{
    death_hop_count, is_cycle_destination, prepare_dead_letter, should_drop_for_cycle, DeathEntry,
    DeathReason, DlxOutcome, DlxPublishResult, DlxRouter, PrepareDeath,
};
pub use durable::{
    DurabilityPolicy, DurableLogFactory, DurableQueueLog, OpenedDurableLog, QueueActorBootstrap,
    SharedDurableLogFactory,
};
pub use meta::{NoopMetaStore, QueueMetaStore};
pub use ready::{effective_priority, Ready};
pub use registry::{
    generate_server_queue_name, DeclareResult, QueueActorState, QueueDeclareOpts, QueueHandle,
    QueueInfo, QueueKey, QueueRegistry, ShutdownReport,
};
