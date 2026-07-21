//! AMQP reply codes used by the connection state machine.

/// AMQP reply: CONNECTION_FORCED (server-initiated, e.g. heartbeat timeout).
pub(super) const REPLY_CONNECTION_FORCED: u16 = 320;
/// AMQP reply: NO_ROUTE (basic.return for unroutable mandatory publish).
pub(super) const REPLY_NO_ROUTE: u16 = 312;
/// AMQP reply: NOT_FOUND.
pub(super) const REPLY_NOT_FOUND: u16 = 404;
/// AMQP reply: RESOURCE_LOCKED.
pub(super) const REPLY_RESOURCE_LOCKED: u16 = 405;
/// AMQP reply: PRECONDITION_FAILED.
pub(super) const REPLY_PRECONDITION_FAILED: u16 = 406;
/// AMQP reply: RESOURCE_ERROR (memory/disk watermarks).
pub(super) const REPLY_RESOURCE_ERROR: u16 = 506;
/// AMQP reply: ACCESS_REFUSED.
pub(super) const REPLY_ACCESS_REFUSED: u16 = 403;
/// AMQP reply: COMMAND_INVALID.
pub(super) const REPLY_COMMAND_INVALID: u16 = 503;
/// AMQP reply: NOT_ALLOWED (unknown vhost).
pub(super) const REPLY_NOT_ALLOWED: u16 = 530;
/// AMQP reply: NOT_IMPLEMENTED.
pub(super) const REPLY_NOT_IMPLEMENTED: u16 = 540;
/// AMQP reply: INTERNAL_ERROR.
pub(super) const REPLY_INTERNAL_ERROR: u16 = 541;
