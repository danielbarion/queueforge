//! `queue-meta.json` sidecar for a durable queue.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{Result, StoreError};

/// Filename of the per-queue metadata sidecar.
pub const META_FILE_NAME: &str = "queue-meta.json";

/// Schema version for [`QueueMetaFile`].
pub const QUEUE_META_SCHEMA_VERSION: u32 = 1;

/// Persisted queue WAL metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMetaFile {
    /// Schema version.
    pub schema_version: u32,
    /// Next segment id to allocate (1-based in filenames).
    pub next_segment_id: u64,
    /// Next message offset to assign.
    pub next_offset: u64,
    /// Highest contiguous acked offset (0 = none).
    pub ack_watermark: u64,
    /// Unix seconds when the queue WAL was created.
    pub created_at: u64,
}

impl QueueMetaFile {
    /// Fresh meta for a new durable queue.
    pub fn new() -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Self {
            schema_version: QUEUE_META_SCHEMA_VERSION,
            next_segment_id: 1,
            next_offset: 1,
            ack_watermark: 0,
            created_at,
        }
    }

    /// Load from `path`.
    pub fn load(path: &Path) -> Result<Self> {
        let text = fs::read_to_string(path)?;
        let meta: Self = serde_json::from_str(&text)?;
        if meta.schema_version != QUEUE_META_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchema(meta.schema_version));
        }
        Ok(meta)
    }

    /// Write atomically (temp + rename) without fsync of the file itself.
    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("json.tmp");
        {
            let mut f = File::create(&tmp)?;
            let text = serde_json::to_string_pretty(self)?;
            f.write_all(text.as_bytes())?;
            f.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        // Fsync parent directory for rename durability (best-effort on all platforms).
        if let Some(parent) = path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }

    /// Save and fsync (same as save — already syncs file).
    pub fn save_sync(&self, path: &Path) -> Result<()> {
        self.save(path)
    }
}

impl Default for QueueMetaFile {
    fn default() -> Self {
        Self::new()
    }
}

// silence unused import on some toolchains
#[allow(dead_code)]
fn _open_opts() -> OpenOptions {
    OpenOptions::new()
}
