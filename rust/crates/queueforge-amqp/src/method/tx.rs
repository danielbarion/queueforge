//! tx class (90). RabbitMQ transactions batch publish and ack/reject.

use crate::error::Result;
use crate::types::{Decoder, Encoder};

/// Class id for tx.
pub const CLASS_ID: u16 = 90;

/// tx.select
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select;

impl Select {
    /// Method id.
    pub const METHOD_ID: u16 = 10;
    /// Encode (no arguments).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }
    /// Decode (no arguments).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}

/// tx.select-ok
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectOk;

impl SelectOk {
    /// Method id.
    pub const METHOD_ID: u16 = 11;
    /// Encode (no arguments).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }
    /// Decode (no arguments).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}

/// tx.commit
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit;

impl Commit {
    /// Method id.
    pub const METHOD_ID: u16 = 20;
    /// Encode (no arguments).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }
    /// Decode (no arguments).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}

/// tx.commit-ok
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOk;

impl CommitOk {
    /// Method id.
    pub const METHOD_ID: u16 = 21;
    /// Encode (no arguments).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }
    /// Decode (no arguments).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}

/// tx.rollback
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rollback;

impl Rollback {
    /// Method id.
    pub const METHOD_ID: u16 = 30;
    /// Encode (no arguments).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }
    /// Decode (no arguments).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}

/// tx.rollback-ok
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackOk;

impl RollbackOk {
    /// Method id.
    pub const METHOD_ID: u16 = 31;
    /// Encode (no arguments).
    pub fn encode_args(&self, _enc: &mut Encoder) -> Result<()> {
        Ok(())
    }
    /// Decode (no arguments).
    pub fn decode_args(_dec: &mut Decoder<'_>) -> Result<Self> {
        Ok(Self)
    }
}
