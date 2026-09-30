//! Segmented write-ahead log for durable queue messages.
//!
//! Layout (task PR 9):
//! ```text
//! data_dir/queues/{encoded_vhost}/{encoded_queue}/
//!   segment-00000001.log
//!   queue-meta.json
//! ```
//!
//! Record format (little-endian):
//! magic u32 | version u8 | rtype u8 | offset u64 | flags u8 |
//! props_len u32 | props | body_len u32 | body | crc32 u32

mod codec;
mod meta;
mod path;

pub use codec::{decode_records, encode_enqueue_record, RecordType, WAL_MAGIC, WAL_VERSION};
pub use meta::{QueueMetaFile, META_FILE_NAME};
pub use path::{encode_name, queue_dir, segment_path};

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use std::sync::Arc;

use queueforge_core::{
    DurableLogFactory, DurableQueueLog, Error as CoreError, Message, OpenedDurableLog,
    QueueMessage, QueueOffset, Result as CoreResult,
};
use tracing::{debug, error, info, warn};

use crate::error::{Result, StoreError};

/// Default max segment size when not configured (128 MiB).
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 134_217_728;

/// Per-queue segmented WAL.
pub struct QueueWal {
    dir: PathBuf,
    meta: QueueMetaFile,
    /// Open segment id → (file, current length, max offset written).
    active: Option<ActiveSegment>,
    /// Closed segments: id → (path, max_offset).
    closed: BTreeMap<u64, ClosedSegment>,
    segment_max_bytes: u64,
    /// Highest offset covered by a completed fsync.
    durable_offset: u64,
    /// Pending acks not yet contiguous with watermark.
    pending_acks: std::collections::BTreeSet<u64>,
    /// `queue-meta.json` has unpersisted watermark / next_offset changes.
    meta_dirty: bool,
}

struct ActiveSegment {
    id: u64,
    file: File,
    len: u64,
    max_offset: u64,
}

struct ClosedSegment {
    path: PathBuf,
    max_offset: u64,
    len: u64,
}

impl QueueWal {
    /// Open or create a WAL directory for `(vhost, queue)`.
    pub fn open(
        data_dir: impl AsRef<Path>,
        vhost: &str,
        queue: &str,
        segment_max_bytes: u64,
    ) -> Result<Self> {
        let dir = queue_dir(data_dir.as_ref(), vhost, queue);
        fs::create_dir_all(&dir)?;

        let meta_path = dir.join(META_FILE_NAME);
        let meta = if meta_path.exists() {
            QueueMetaFile::load(&meta_path)?
        } else {
            let m = QueueMetaFile::new();
            m.save(&meta_path)?;
            m
        };

        let mut wal = Self {
            dir,
            meta,
            active: None,
            closed: BTreeMap::new(),
            segment_max_bytes: segment_max_bytes.max(4096),
            durable_offset: 0,
            pending_acks: std::collections::BTreeSet::new(),
            meta_dirty: false,
        };
        wal.scan_segments()?;
        Ok(wal)
    }

    /// Directory containing this queue's segments.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Current meta snapshot.
    pub fn meta(&self) -> &QueueMetaFile {
        &self.meta
    }

    /// Recursively delete a queue WAL directory (exclusive purge / queue delete).
    pub fn delete_dir(data_dir: impl AsRef<Path>, vhost: &str, queue: &str) -> Result<()> {
        let dir = queue_dir(data_dir.as_ref(), vhost, queue);
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
            debug!(path = %dir.display(), "deleted queue WAL directory");
        }
        Ok(())
    }

    fn scan_segments(&mut self) -> Result<()> {
        let mut entries: Vec<(u64, PathBuf)> = Vec::new();
        if self.dir.exists() {
            for ent in fs::read_dir(&self.dir)? {
                let ent = ent?;
                let name = ent.file_name();
                let name = name.to_string_lossy();
                if let Some(id) = parse_segment_name(&name) {
                    entries.push((id, ent.path()));
                }
            }
        }
        entries.sort_by_key(|(id, _)| *id);

        if entries.is_empty() {
            return Ok(());
        }

        let max_id = entries.last().map(|(id, _)| *id).unwrap_or(0);
        // Ensure next_segment_id is past existing segments.
        if self.meta.next_segment_id <= max_id {
            self.meta.next_segment_id = max_id.saturating_add(1);
        }

        for (id, path) in entries {
            let len = fs::metadata(&path)?.len();
            let max_offset = scan_max_offset(&path)?;
            if id == max_id {
                // Re-open the latest segment for append.
                let file = OpenOptions::new().read(true).write(true).open(&path)?;
                let mut file = file;
                file.seek(SeekFrom::End(0))?;
                self.active = Some(ActiveSegment {
                    id,
                    file,
                    len,
                    max_offset,
                });
            } else {
                self.closed.insert(
                    id,
                    ClosedSegment {
                        path,
                        max_offset,
                        len,
                    },
                );
            }
            if max_offset > self.durable_offset {
                // On-disk data from a previous run is already durable.
                self.durable_offset = max_offset;
            }
        }
        Ok(())
    }

    fn ensure_active(&mut self) -> Result<()> {
        if self.active.is_some() {
            return Ok(());
        }
        let id = self.meta.next_segment_id;
        let path = segment_path(&self.dir, id);
        let created_new = !path.exists();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let mut file = file;
        let len = file.seek(SeekFrom::End(0))?;
        // Issue 8: parent-dir fsync after creating a new segment file.
        if created_new {
            sync_dir(&self.dir)?;
        }
        self.active = Some(ActiveSegment {
            id,
            file,
            len,
            max_offset: 0,
        });
        self.meta.next_segment_id = id.saturating_add(1);
        self.meta_dirty = true;
        Ok(())
    }

    fn rotate_if_needed(&mut self, upcoming_len: u64) -> Result<()> {
        let need_rotate = match &self.active {
            Some(a) => a.len.saturating_add(upcoming_len) > self.segment_max_bytes && a.len > 0,
            None => false,
        };
        if !need_rotate {
            return Ok(());
        }
        // Close current into closed map.
        if let Some(a) = self.active.take() {
            let path = segment_path(&self.dir, a.id);
            // Ensure data is on disk before rotating metadata.
            let file = a.file;
            file.sync_all()?;
            self.closed.insert(
                a.id,
                ClosedSegment {
                    path,
                    max_offset: a.max_offset,
                    len: a.len,
                },
            );
        }
        self.ensure_active()?;
        Ok(())
    }

    /// Replay all valid records with offset > ack_watermark into memory.
    ///
    /// On CRC mismatch mid-file: returns Corrupt error (halt that queue).
    /// Torn tail at EOF: truncate and continue.
    pub fn recover_messages(&mut self) -> Result<RecoveredState> {
        let ack_wm = self.meta.ack_watermark;
        let mut ready: Vec<(u64, Message)> = Vec::new();
        let mut last_offset = ack_wm;

        // Collect segments in order.
        let mut seg_ids: Vec<u64> = self.closed.keys().copied().collect();
        if let Some(a) = &self.active {
            seg_ids.push(a.id);
        }
        seg_ids.sort_unstable();

        for id in seg_ids {
            let path = segment_path(&self.dir, id);
            if !path.exists() {
                continue;
            }
            match replay_segment(&path, ack_wm) {
                Ok(ReplayOutcome {
                    messages,
                    last_valid_offset,
                    truncated_to,
                }) => {
                    if let Some(trunc) = truncated_to {
                        // Truncate torn tail.
                        let f = OpenOptions::new().write(true).open(&path)?;
                        f.set_len(trunc)?;
                        f.sync_all()?;
                        // Update in-memory length.
                        if let Some(a) = self.active.as_mut() {
                            if a.id == id {
                                a.len = trunc;
                            }
                        }
                        if let Some(c) = self.closed.get_mut(&id) {
                            c.len = trunc;
                        }
                        info!(
                            path = %path.display(),
                            truncated_to = trunc,
                            "truncated torn WAL segment tail"
                        );
                    }
                    for (off, msg) in messages {
                        if off > last_offset {
                            last_offset = off;
                        }
                        ready.push((off, msg));
                    }
                    if let Some(lo) = last_valid_offset {
                        if lo > last_offset {
                            last_offset = lo;
                        }
                        if let Some(a) = self.active.as_mut() {
                            if a.id == id {
                                a.max_offset = a.max_offset.max(lo);
                            }
                        }
                        if let Some(c) = self.closed.get_mut(&id) {
                            c.max_offset = c.max_offset.max(lo);
                        }
                    }
                }
                Err(StoreError::WalCorrupt {
                    ref path,
                    ref reason,
                }) => {
                    error!(%path, %reason, "WAL corrupt — halting queue recovery");
                    return Err(StoreError::WalCorrupt {
                        path: path.clone(),
                        reason: reason.clone(),
                    });
                }
                Err(e) => return Err(e),
            }
        }

        ready.sort_by_key(|(o, _)| *o);
        // Dedup by offset (keep first).
        ready.dedup_by_key(|(o, _)| *o);

        let next_offset = last_offset.saturating_add(1).max(1);
        self.meta.next_offset = next_offset;
        self.durable_offset = last_offset;

        Ok(RecoveredState {
            messages: ready,
            next_offset,
            ack_watermark: ack_wm,
        })
    }
}

/// Outcome of recovering a single queue WAL.
#[derive(Debug)]
pub struct RecoveredState {
    /// Enqueued messages with offset > ack_watermark (sorted).
    pub messages: Vec<(u64, Message)>,
    /// Next offset to assign.
    pub next_offset: u64,
    /// Ack watermark from meta.
    pub ack_watermark: u64,
}

struct ReplayOutcome {
    messages: Vec<(u64, Message)>,
    last_valid_offset: Option<u64>,
    truncated_to: Option<u64>,
}

fn replay_segment(path: &Path, ack_wm: u64) -> Result<ReplayOutcome> {
    let file = File::open(path)?;
    let meta_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut pos: u64 = 0;
    let mut messages = Vec::new();
    let mut last_valid_offset = None;
    let mut truncated_to = None;

    loop {
        if pos >= meta_len {
            break;
        }
        let start = pos;
        match codec::read_record(&mut reader) {
            Ok(Some(rec)) => {
                pos = start + rec.encoded_len as u64;
                last_valid_offset = Some(rec.offset.max(last_valid_offset.unwrap_or(0)));
                if rec.rtype == RecordType::Enqueue && rec.offset > ack_wm {
                    if let Some(msg) = rec.message {
                        messages.push((rec.offset, msg));
                    }
                }
            }
            Ok(None) => {
                // Clean EOF.
                break;
            }
            Err(StoreError::WalTornTail { .. }) => {
                // Issue 1: truncate to the absolute start of the incomplete
                // record (`start`), never the relative in-record `at`.
                truncated_to = Some(start);
                break;
            }
            Err(e @ StoreError::WalCorrupt { .. }) => {
                // Issue 5: complete-record CRC / magic / version mismatch must
                // halt the queue — never demote to torn truncate.
                let path_s = path.display().to_string();
                return Err(match e {
                    StoreError::WalCorrupt { reason, .. } => StoreError::WalCorrupt {
                        path: path_s,
                        reason,
                    },
                    other => other,
                });
            }
            Err(e) => return Err(e),
        }
    }

    Ok(ReplayOutcome {
        messages,
        last_valid_offset,
        truncated_to,
    })
}

fn scan_max_offset(path: &Path) -> Result<u64> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut max = 0u64;
    loop {
        match codec::read_record(&mut reader) {
            Ok(Some(rec)) => {
                max = max.max(rec.offset);
            }
            Ok(None) => break,
            Err(StoreError::WalTornTail { .. }) => break,
            Err(StoreError::WalCorrupt { .. }) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(max)
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let name = name.strip_prefix("segment-")?.strip_suffix(".log")?;
    name.parse().ok()
}

/// Best-effort fsync of a directory (durability of creates/unlinks).
fn sync_dir(dir: &Path) -> Result<()> {
    match File::open(dir) {
        Ok(d) => d.sync_all().map_err(StoreError::Io),
        Err(e) => Err(StoreError::Io(e)),
    }
}

impl DurableQueueLog for QueueWal {
    fn append_enqueue(&mut self, offset: QueueOffset, msg: &Message) -> CoreResult<()> {
        let bytes =
            encode_enqueue_record(offset.0, msg).map_err(|e| CoreError::Store(e.to_string()))?;
        self.rotate_if_needed(bytes.len() as u64)
            .map_err(|e| CoreError::Store(e.to_string()))?;
        self.ensure_active()
            .map_err(|e| CoreError::Store(e.to_string()))?;

        let active = self
            .active
            .as_mut()
            .ok_or_else(|| CoreError::Store("no active WAL segment".into()))?;
        active
            .file
            .write_all(&bytes)
            .map_err(|e| CoreError::Store(e.to_string()))?;
        active.len = active.len.saturating_add(bytes.len() as u64);
        active.max_offset = active.max_offset.max(offset.0);
        if offset.0.saturating_add(1) > self.meta.next_offset {
            self.meta.next_offset = offset.0.saturating_add(1);
            self.meta_dirty = true;
        }
        Ok(())
    }

    fn acknowledge(&mut self, offset: QueueOffset) -> CoreResult<()> {
        self.pending_acks.insert(offset.0);
        let before = self.meta.ack_watermark;
        // Advance watermark while contiguous from the previous watermark.
        loop {
            let next = self.meta.ack_watermark.saturating_add(1);
            if self.pending_acks.remove(&next) {
                self.meta.ack_watermark = next;
            } else {
                break;
            }
        }
        if self.meta.ack_watermark != before {
            self.meta_dirty = true;
        }
        Ok(())
    }

    fn fsync(&mut self) -> CoreResult<QueueOffset> {
        // Sync active + recently written closed segments.
        if let Some(active) = self.active.as_mut() {
            active
                .file
                .sync_all()
                .map_err(|e| CoreError::Store(format!("segment fsync: {e}")))?;
            self.durable_offset = self.durable_offset.max(active.max_offset);
        }
        // Persist meta after data fsync (invariant 2).
        let meta_path = self.dir.join(META_FILE_NAME);
        self.meta
            .save_sync(&meta_path)
            .map_err(|e| CoreError::Store(format!("meta fsync: {e}")))?;
        self.meta_dirty = false;
        Ok(QueueOffset(self.durable_offset))
    }

    fn durable_offset(&self) -> QueueOffset {
        QueueOffset(self.durable_offset)
    }

    fn ack_watermark(&self) -> QueueOffset {
        QueueOffset(self.meta.ack_watermark)
    }

    fn meta_dirty(&self) -> bool {
        self.meta_dirty
    }

    fn compact(&mut self) -> CoreResult<()> {
        let wm = self.meta.ack_watermark;
        let doomed: Vec<u64> = self
            .closed
            .iter()
            .filter(|(_, s)| s.max_offset > 0 && s.max_offset <= wm)
            .map(|(id, _)| *id)
            .collect();
        let mut removed_any = false;
        for id in doomed {
            if let Some(seg) = self.closed.remove(&id) {
                match fs::remove_file(&seg.path) {
                    Ok(()) => {
                        removed_any = true;
                        debug!(
                            path = %seg.path.display(),
                            max_offset = seg.max_offset,
                            ack_watermark = wm,
                            "compacted WAL segment"
                        );
                        metrics::counter!("queueforge_wal_segments_compacted_total").increment(1);
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => {
                        warn!(path = %seg.path.display(), error = %e, "failed to delete compacted segment");
                    }
                }
            }
        }
        // Issue 8: parent-dir fsync after unlinks.
        if removed_any {
            let _ = sync_dir(&self.dir);
        }
        Ok(())
    }
}

/// Factory that opens [`QueueWal`] instances under a data directory.
#[derive(Debug, Clone)]
pub struct WalFactory {
    data_dir: PathBuf,
    segment_max_bytes: u64,
}

impl WalFactory {
    /// Create a factory rooted at `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>, segment_max_bytes: u64) -> Self {
        Self {
            data_dir: data_dir.into(),
            segment_max_bytes,
        }
    }

    /// Root data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

impl DurableLogFactory for WalFactory {
    fn open(&self, vhost: &str, queue: &str) -> CoreResult<OpenedDurableLog> {
        let mut wal = QueueWal::open(&self.data_dir, vhost, queue, self.segment_max_bytes)
            .map_err(|e| CoreError::Store(e.to_string()))?;
        // Issue 3: always recover so declare never serves empty over live segments.
        let recovered = wal.recover_messages().map_err(|e| {
            // Preserve corrupt classification for callers.
            match e {
                StoreError::WalCorrupt { path, reason } => {
                    CoreError::Unavailable(format!("WAL corrupt at {path}: {reason}"))
                }
                other => CoreError::Store(other.to_string()),
            }
        })?;
        let mut ready = std::collections::VecDeque::with_capacity(recovered.messages.len());
        for (offset, mut msg) in recovered.messages {
            msg.redelivered = true;
            ready.push_back(QueueMessage {
                offset: QueueOffset(offset),
                message: Arc::new(msg),
                expires_at: None,
            });
        }
        Ok(OpenedDurableLog {
            log: Box::new(wal),
            ready,
            next_offset: recovered.next_offset,
        })
    }

    fn delete(&self, vhost: &str, queue: &str) -> CoreResult<()> {
        QueueWal::delete_dir(&self.data_dir, vhost, queue)
            .map_err(|e| CoreError::Store(e.to_string()))
    }
}

/// Open a concrete [`QueueWal`] (recovery path needs recover_messages).
pub fn open_queue_wal(
    data_dir: impl AsRef<Path>,
    vhost: &str,
    queue: &str,
    segment_max_bytes: u64,
) -> Result<QueueWal> {
    QueueWal::open(data_dir, vhost, queue, segment_max_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use compact_str::CompactString;
    use queueforge_core::Message;
    use tempfile::TempDir;

    fn sample(body: &[u8], persistent: bool) -> Message {
        Message {
            exchange: CompactString::from(""),
            routing_key: CompactString::from("q"),
            body: Bytes::copy_from_slice(body),
            persistent,
            redelivered: false,
            content_type: Some(CompactString::from("text/plain")),
            content_encoding: None,
            correlation_id: None,
            message_id: None,
            reply_to: None,
            expiration: None,
            app_id: None,
            user_id: None,
            type_: None,
            priority: Some(5),
            timestamp: None,
            expires_unix_ms: None,
            headers: Default::default(),
        }
    }

    #[test]
    fn append_fsync_recover_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "orders", 1024 * 1024).unwrap();
        let msg = sample(b"hello-durable", true);
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &msg).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"second", true))
            .unwrap();
        let synced = DurableQueueLog::fsync(&mut wal).unwrap();
        assert!(synced.0 >= 2);

        drop(wal);

        let mut wal2 = QueueWal::open(dir.path(), "/", "orders", 1024 * 1024).unwrap();
        let recovered = wal2.recover_messages().unwrap();
        assert_eq!(recovered.messages.len(), 2);
        assert_eq!(recovered.messages[0].1.body.as_ref(), b"hello-durable");
        assert_eq!(recovered.messages[1].1.body.as_ref(), b"second");
        assert_eq!(recovered.next_offset, 3);
    }

    #[test]
    fn ack_watermark_and_compact() {
        let dir = TempDir::new().unwrap();
        // Tiny segment size forces multiple segments.
        let mut wal = QueueWal::open(dir.path(), "/", "c", 200).unwrap();
        for i in 1..=5u64 {
            let body = format!("msg-{i}");
            DurableQueueLog::append_enqueue(
                &mut wal,
                QueueOffset(i),
                &sample(body.as_bytes(), true),
            )
            .unwrap();
            let _ = DurableQueueLog::fsync(&mut wal).unwrap();
        }
        // Ack 1..5
        for i in 1..=5u64 {
            DurableQueueLog::acknowledge(&mut wal, QueueOffset(i)).unwrap();
        }
        assert_eq!(DurableQueueLog::ack_watermark(&wal).0, 5);
        DurableQueueLog::fsync(&mut wal).unwrap();
        DurableQueueLog::compact(&mut wal).unwrap();
        // Closed fully-acked segments should be gone; active may remain.
        let closed_left = wal.closed.len();
        assert_eq!(
            closed_left, 0,
            "fully acked closed segments must be deleted"
        );
    }

    #[test]
    fn path_encodes_default_vhost() {
        let p = queue_dir(Path::new("/data"), "/", "q");
        assert!(p.ends_with("queues/%2F/q") || p.to_string_lossy().contains("%2F"));
    }

    /// Issue 1: torn tail must truncate at the incomplete record start, not a
    /// relative in-record offset (which would wipe prior valid records).
    #[test]
    fn torn_tail_preserves_prior_records() {
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "torn", 1024 * 1024).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"one", true)).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"two", true)).unwrap();
        DurableQueueLog::fsync(&mut wal).unwrap();

        // Locate the active segment and append a partial third record.
        let seg_path = {
            let a = wal.active.as_ref().expect("active segment");
            segment_path(&wal.dir, a.id)
        };
        let full_len = std::fs::metadata(&seg_path).unwrap().len();
        drop(wal);

        // Append garbage partial header (not a full record).
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&seg_path).unwrap();
            f.write_all(b"VLRA\x01\x01").unwrap(); // incomplete
            f.sync_all().unwrap();
        }
        let torn_len = std::fs::metadata(&seg_path).unwrap().len();
        assert!(torn_len > full_len);

        let mut wal2 = QueueWal::open(dir.path(), "/", "torn", 1024 * 1024).unwrap();
        let recovered = wal2.recover_messages().unwrap();
        assert_eq!(
            recovered.messages.len(),
            2,
            "both complete records must survive"
        );
        assert_eq!(recovered.messages[0].1.body.as_ref(), b"one");
        assert_eq!(recovered.messages[1].1.body.as_ref(), b"two");
        assert_eq!(recovered.next_offset, 3);

        let after = std::fs::metadata(&seg_path).unwrap().len();
        assert_eq!(
            after, full_len,
            "file must be truncated to end of last complete record ({full_len}), got {after}"
        );
    }

    /// Issue 5: complete-record CRC mismatch must halt, not truncate.
    #[test]
    fn crc_mismatch_halts_not_truncate() {
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "crc", 1024 * 1024).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"ok", true)).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"bad", true)).unwrap();
        DurableQueueLog::fsync(&mut wal).unwrap();
        let seg_path = {
            let a = wal.active.as_ref().expect("active");
            segment_path(&wal.dir, a.id)
        };
        let before_len = std::fs::metadata(&seg_path).unwrap().len();
        drop(wal);

        // Flip the last 4 CRC bytes of the file (complete record, bad CRC).
        {
            use std::io::{Read, Seek, SeekFrom, Write};
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&seg_path)
                .unwrap();
            let len = f.metadata().unwrap().len();
            f.seek(SeekFrom::Start(len - 4)).unwrap();
            let mut crc = [0u8; 4];
            f.read_exact(&mut crc).unwrap();
            crc[0] ^= 0xFF;
            f.seek(SeekFrom::Start(len - 4)).unwrap();
            f.write_all(&crc).unwrap();
            f.sync_all().unwrap();
        }

        let mut wal2 = QueueWal::open(dir.path(), "/", "crc", 1024 * 1024).unwrap();
        let err = wal2.recover_messages().unwrap_err();
        assert!(
            matches!(err, StoreError::WalCorrupt { .. }),
            "expected WalCorrupt, got {err}"
        );
        let after_len = std::fs::metadata(&seg_path).unwrap().len();
        assert_eq!(
            after_len, before_len,
            "corrupt segment must not be truncated"
        );
    }

    /// Issue 2: ack watermark advances and persists across reopen without further appends.
    #[test]
    fn ack_watermark_persists_without_new_appends() {
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "wm", 1024 * 1024).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"m1", true)).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"m2", true)).unwrap();
        DurableQueueLog::fsync(&mut wal).unwrap();

        DurableQueueLog::acknowledge(&mut wal, QueueOffset(1)).unwrap();
        DurableQueueLog::acknowledge(&mut wal, QueueOffset(2)).unwrap();
        assert!(DurableQueueLog::meta_dirty(&wal));
        assert_eq!(DurableQueueLog::ack_watermark(&wal).0, 2);
        DurableQueueLog::fsync(&mut wal).unwrap();
        assert!(!DurableQueueLog::meta_dirty(&wal));
        drop(wal);

        let mut wal2 = QueueWal::open(dir.path(), "/", "wm", 1024 * 1024).unwrap();
        let recovered = wal2.recover_messages().unwrap();
        assert!(
            recovered.messages.is_empty(),
            "acked messages must not reappear after meta fsync"
        );
        assert_eq!(recovered.ack_watermark, 2);
    }

    /// Issue 3: factory open recovers existing segments (not empty next_offset=1).
    #[test]
    fn factory_open_recovers_messages() {
        let dir = TempDir::new().unwrap();
        {
            let mut wal = QueueWal::open(dir.path(), "/", "fo", 1024 * 1024).unwrap();
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"live", true))
                .unwrap();
            DurableQueueLog::fsync(&mut wal).unwrap();
        }
        let factory = WalFactory::new(dir.path(), 1024 * 1024);
        let opened = DurableLogFactory::open(&factory, "/", "fo").unwrap();
        assert_eq!(opened.ready.len(), 1);
        assert_eq!(opened.ready[0].message.body.as_ref(), b"live");
        assert!(opened.ready[0].message.redelivered);
        assert_eq!(opened.next_offset, 2);
    }
}
