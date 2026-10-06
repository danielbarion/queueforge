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

pub use codec::{
    decode_records, encode_ack_record, encode_enqueue_record, RecordType, WAL_MAGIC, WAL_VERSION,
};
pub use meta::{QueueMetaFile, META_FILE_NAME};
pub use path::{encode_name, queue_dir, segment_path};

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use queueforge_core::{
    DurableLogFactory, DurableQueueLog, Error as CoreError, Message, OpenedDurableLog,
    QueueMessage, QueueOffset, Result as CoreResult,
};
use tracing::{debug, error, info, warn};

use crate::error::{Result, StoreError};

/// Default max segment size when not configured (128 MiB).
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 134_217_728;

/// Bytes to gather before one `write`. `fsync` flushes a short tail first.
const WAL_WRITE_BATCH: usize = 32 * 1024;

/// File header for a segment whose tail is preallocated. Records start after it.
const PREAMBLE_LEN: u64 = 16;
const PREAMBLE_MAGIC: &[u8; 4] = b"VLHP";

/// How far ahead of the logical end the file is materialized. Overwriting
/// those blocks keeps `fdatasync` off the allocating path.
const ALLOC_CHUNK: u64 = 4 * 1024 * 1024;

/// Start the next chunk once less than this much allocated space remains.
const EXTEND_AHEAD: u64 = 1024 * 1024;

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
    /// Watermark or next offset changed since the last group commit.
    ///
    /// [`Self::fsync`] clears this after the segment sync. Recovery reads the
    /// watermark from that segment, so the flag does not mean the json sidecar
    /// was rewritten.
    meta_dirty: bool,
}

struct ActiveSegment {
    id: u64,
    file: File,
    /// End of valid bytes, including bytes still sitting in `buf`.
    /// Preamble segments start at [`PREAMBLE_LEN`].
    len: u64,
    /// Bytes that are already materialized. Appends overwrite this region.
    allocated: Arc<AtomicU64>,
    extending: Arc<AtomicBool>,
    max_offset: u64,
    buf: Vec<u8>,
    segment_max: u64,
    preamble: bool,
}

fn write_all_at(file: &File, mut buf: &[u8], mut at: u64) -> io::Result<()> {
    while !buf.is_empty() {
        match file.write_at(buf, at) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "wal segment write",
                ))
            }
            Ok(n) => {
                buf = &buf[n..];
                at += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Write zeros over `[from, to)` and `fdatasync` so later overwrites do not allocate.
fn materialize_range(file: &File, from: u64, to: u64) -> io::Result<()> {
    if to <= from {
        return Ok(());
    }
    let zeros = vec![0u8; 64 * 1024];
    let mut at = from;
    while at < to {
        let n = ((to - at) as usize).min(zeros.len());
        write_all_at(file, &zeros[..n], at)?;
        at += n as u64;
    }
    file.sync_data()?;
    Ok(())
}

fn write_preamble(file: &File, logical_len: u64) -> io::Result<()> {
    let mut hdr = [0u8; PREAMBLE_LEN as usize];
    hdr[0..4].copy_from_slice(PREAMBLE_MAGIC);
    hdr[4..8].copy_from_slice(&1u32.to_le_bytes());
    hdr[8..16].copy_from_slice(&logical_len.to_le_bytes());
    write_all_at(file, &hdr, 0)
}

/// Logical end recorded in a preallocated segment, when the file has that header.
fn read_preamble_len(file: &File) -> io::Result<Option<u64>> {
    let mut hdr = [0u8; PREAMBLE_LEN as usize];
    let mut filled = 0;
    while filled < hdr.len() {
        match file.read_at(&mut hdr[filled..], filled as u64) {
            Ok(0) => return Ok(None),
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    if &hdr[0..4] != PREAMBLE_MAGIC {
        return Ok(None);
    }
    let logical = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    Ok(Some(logical.max(PREAMBLE_LEN)))
}

#[cfg(target_os = "linux")]
extern "C" {
    fn sync_file_range(fd: i32, offset: i64, nbytes: i64, flags: u32) -> i32;
}

/// Sync the logical prefix the confirm is waiting on.
///
/// `fdatasync` writes every dirty page of this file. While a background extend
/// is filling the tail, sync only the logical prefix so that extend does not
/// join the confirm. With no extend in flight, one `fdatasync` covers the
/// records and the header.
fn sync_logical(file: &File, len: u64, extending: bool) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    if extending {
        const WRITE_AND_WAIT: u32 = 2 | 4;
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(file);
        let nbytes = i64::try_from(len).unwrap_or(i64::MAX);
        let rc = unsafe { sync_file_range(fd, 0, nbytes, WRITE_AND_WAIT) };
        if rc == 0 {
            return Ok(());
        }
    }
    let _ = (len, extending);
    file.sync_data()
}

impl ActiveSegment {
    fn push_record(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.buf.extend_from_slice(bytes);
        self.len = self.len.saturating_add(bytes.len() as u64);
        if self.buf.len() >= WAL_WRITE_BATCH {
            self.flush_buf()?;
        }
        Ok(())
    }

    /// Push gathered records into the kernel at the logical end. A later `sync_data` covers them.
    fn flush_buf(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let start = self.len - self.buf.len() as u64;
        self.reserve(self.len)?;
        if let Err(e) = write_all_at(&self.file, &self.buf, start) {
            return Err(e);
        }
        self.buf.clear();
        Ok(())
    }

    /// Make `[allocated, needed)` safe to overwrite. Waits if a background extend owns the range.
    fn reserve(&mut self, needed: u64) -> io::Result<()> {
        for _ in 0..10_000 {
            let alloc = self.allocated.load(Ordering::Acquire);
            if needed <= alloc {
                return Ok(());
            }
            if self.try_extend(needed)? {
                return Ok(());
            }
            std::thread::sleep(Duration::from_micros(200));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "wal segment preallocation",
        ))
    }

    /// Claim the extend. `Ok(true)` means `allocated` now covers `needed`.
    fn try_extend(&mut self, needed: u64) -> io::Result<bool> {
        if self
            .extending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(false);
        }
        let start = self.allocated.load(Ordering::Acquire);
        if needed <= start {
            self.extending.store(false, Ordering::Release);
            return Ok(true);
        }
        let target = extend_target(start, needed, self.segment_max);
        if target < needed {
            self.extending.store(false, Ordering::Release);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "wal segment is full",
            ));
        }
        let result = materialize_range(&self.file, start, target);
        if result.is_ok() {
            self.allocated.store(target, Ordering::Release);
        }
        self.extending.store(false, Ordering::Release);
        result.map(|()| true)
    }

    /// Materialize the next chunk on another thread once the runway is short.
    /// The confirm path does not wait for it.
    fn kick_extend(&self) {
        let alloc = self.allocated.load(Ordering::Acquire);
        if alloc >= self.segment_max || alloc.saturating_sub(self.len) > EXTEND_AHEAD {
            return;
        }
        if self
            .extending
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let start = self.allocated.load(Ordering::Acquire);
        let target = extend_target(start, start.saturating_add(1), self.segment_max);
        if target <= start {
            self.extending.store(false, Ordering::Release);
            return;
        }
        let file = match self.file.try_clone() {
            Ok(file) => file,
            Err(_) => {
                self.extending.store(false, Ordering::Release);
                return;
            }
        };
        let allocated = Arc::clone(&self.allocated);
        let extending = Arc::clone(&self.extending);
        std::thread::spawn(move || {
            if materialize_range(&file, start, target).is_ok() {
                allocated.store(target, Ordering::Release);
            }
            extending.store(false, Ordering::Release);
        });
    }

    fn wait_extend(&self) {
        for _ in 0..10_000 {
            if !self.extending.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_micros(200));
        }
    }
}

impl Drop for ActiveSegment {
    fn drop(&mut self) {
        self.wait_extend();
    }
}

fn extend_target(start: u64, needed: u64, segment_max: u64) -> u64 {
    let mut target = start.saturating_add(ALLOC_CHUNK);
    if target < needed {
        target = needed;
    }
    if segment_max > start {
        target = target.min(segment_max);
    }
    target
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
            let phys = fs::metadata(&path)?.len();
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            let preamble_len = read_preamble_len(&file)?;
            let preamble = preamble_len.is_some();
            let logical = preamble_len.unwrap_or(phys).min(phys);
            let max_offset = scan_max_offset(&path)?;
            if id == max_id {
                // Re-open the latest segment for append. Logical end is the
                // preamble length, not the preallocated file size.
                self.active = Some(ActiveSegment {
                    id,
                    file,
                    len: logical,
                    allocated: Arc::new(AtomicU64::new(phys)),
                    extending: Arc::new(AtomicBool::new(false)),
                    max_offset,
                    buf: Vec::with_capacity(WAL_WRITE_BATCH),
                    segment_max: self.segment_max_bytes,
                    preamble,
                });
            } else {
                self.closed.insert(
                    id,
                    ClosedSegment {
                        path,
                        max_offset,
                        len: logical,
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
        let file = file;
        // Issue 8: parent-dir fsync after creating a new segment file.
        let (len, allocated, preamble) = if created_new {
            write_preamble(&file, PREAMBLE_LEN)?;
            // Materialize the first chunk before any confirm so the fsync
            // overwrites blocks that already have extents.
            let target = extend_target(PREAMBLE_LEN, PREAMBLE_LEN + 1, self.segment_max_bytes);
            materialize_range(&file, PREAMBLE_LEN, target)?;
            sync_dir(&self.dir)?;
            (PREAMBLE_LEN, target, true)
        } else {
            let phys = file.metadata()?.len();
            let preamble = read_preamble_len(&file)?.is_some();
            let logical = read_preamble_len(&file)?.unwrap_or(phys).min(phys);
            (logical, phys, preamble)
        };
        self.active = Some(ActiveSegment {
            id,
            file,
            len,
            allocated: Arc::new(AtomicU64::new(allocated)),
            extending: Arc::new(AtomicBool::new(false)),
            max_offset: 0,
            buf: Vec::with_capacity(WAL_WRITE_BATCH),
            segment_max: self.segment_max_bytes,
            preamble,
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
        if let Some(active) = self.active.as_ref() {
            active.wait_extend();
        }
        // Close current into closed map.
        if let Some(mut a) = self.active.take() {
            let path = segment_path(&self.dir, a.id);
            // Ensure data is on disk before rotating metadata.
            a.flush_buf()?;
            // A later confirm can cover these offsets. The header has to
            // include them or recovery would drop bytes the client was acked.
            if a.preamble {
                write_preamble(&a.file, a.len)?;
            }
            a.file.sync_all()?;
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
        let mut ack_wm = self.meta.ack_watermark;
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
                    ack_watermark,
                }) => {
                    ack_wm = ack_wm.max(ack_watermark);
                    if let Some(trunc) = truncated_to {
                        // Drop the torn tail. A preallocated segment keeps its
                        // blocks and rewinds the header instead of shrinking.
                        let f = OpenOptions::new().write(true).read(true).open(&path)?;
                        if read_preamble_len(&f)?.is_some() {
                            write_preamble(&f, trunc)?;
                            f.sync_data()?;
                        } else {
                            f.set_len(trunc)?;
                            f.sync_all()?;
                        }
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
        ready.retain(|(off, _)| *off > ack_wm);

        last_offset = last_offset.max(ack_wm);
        let next_offset = last_offset.saturating_add(1).max(1);
        self.meta.next_offset = next_offset;
        self.meta.ack_watermark = ack_wm;
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
    ack_watermark: u64,
}

/// Byte range that holds records. Preallocated padding after the preamble's
/// logical end is not part of the log.
fn segment_bounds(path: &Path) -> Result<(u64, u64)> {
    let file = File::open(path)?;
    let phys = file.metadata()?.len();
    match read_preamble_len(&file)? {
        Some(logical) => {
            let end = logical.min(phys);
            Ok((PREAMBLE_LEN.min(end), end))
        }
        None => Ok((0, phys)),
    }
}

fn replay_segment(path: &Path, ack_wm: u64) -> Result<ReplayOutcome> {
    let (start, end) = segment_bounds(path)?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file).take(end.saturating_sub(start));
    let mut pos = start;
    let mut messages = Vec::new();
    let mut last_valid_offset = None;
    let mut truncated_to = None;
    let mut wal_ack = ack_wm;
    let preamble = start == PREAMBLE_LEN;

    loop {
        if pos >= end {
            break;
        }
        let rec_start = pos;
        match codec::read_record(&mut reader) {
            Ok(Some(rec)) => {
                let next = rec_start.saturating_add(rec.encoded_len as u64);
                if next > end {
                    truncated_to = Some(rec_start);
                    break;
                }
                pos = next;
                if rec.rtype == RecordType::AckWatermark {
                    wal_ack = wal_ack.max(rec.offset);
                } else if rec.rtype == RecordType::Enqueue {
                    last_valid_offset = Some(rec.offset.max(last_valid_offset.unwrap_or(0)));
                    if let Some(msg) = rec.message {
                        messages.push((rec.offset, msg));
                    }
                }
            }
            Ok(None) => break,
            Err(StoreError::WalTornTail { .. }) => {
                // Truncate to the absolute start of the incomplete record,
                // never the relative in-record offset.
                truncated_to = Some(rec_start);
                break;
            }
            // A zero page inside a preallocated segment is the logical end,
            // not a corrupt record. A non-zero bad magic still halts.
            Err(StoreError::WalCorrupt { reason, .. })
                if preamble && reason == "bad magic 0x0" =>
            {
                truncated_to = Some(rec_start);
                break;
            }
            Err(e @ StoreError::WalCorrupt { .. }) => {
                // A complete record with a bad CRC, magic, or version halts
                // the queue. Do not demote that to a torn truncate.
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

    messages.retain(|(off, _)| *off > wal_ack);
    Ok(ReplayOutcome {
        messages,
        last_valid_offset,
        truncated_to,
        ack_watermark: wal_ack,
    })
}

fn scan_max_offset(path: &Path) -> Result<u64> {
    let (start, end) = segment_bounds(path)?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file).take(end.saturating_sub(start));
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
            .push_record(&bytes)
            .map_err(|e| CoreError::Store(e.to_string()))?;
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
        // The ack watermark rides in the segment so one sync covers bodies and acks.
        if self.meta_dirty && self.meta.ack_watermark > 0 {
            if let Some(active) = self.active.as_mut() {
                let bytes = encode_ack_record(self.meta.ack_watermark);
                active
                    .push_record(&bytes)
                    .map_err(|e| CoreError::Store(e.to_string()))?;
            }
        }
        if let Some(active) = self.active.as_mut() {
            active
                .flush_buf()
                .map_err(|e| CoreError::Store(format!("segment flush: {e}")))?;
            if active.preamble {
                write_preamble(&active.file, active.len)
                    .map_err(|e| CoreError::Store(format!("segment header: {e}")))?;
            }
            // The segment is preallocated. Sync the logical prefix while a
            // background extend is dirtying the tail; otherwise fdatasync.
            // The json rename is not on this path.
            let extending = active.extending.load(Ordering::Acquire);
            let logical = active.len;
            sync_logical(&active.file, logical, extending)
                .map_err(|e| CoreError::Store(format!("segment fsync: {e}")))?;
            active.kick_extend();
            self.durable_offset = self.durable_offset.max(active.max_offset);
        }
        if self.meta_dirty {
            if self.active.is_some() {
                // The ack record written above is covered by the segment sync.
                // Rewriting queue-meta.json here was a rename on every group commit.
                // scan_segments repairs next_segment_id from the filenames.
                self.meta_dirty = false;
            } else {
                let meta_path = self.dir.join(META_FILE_NAME);
                self.meta
                    .save_relaxed(&meta_path)
                    .map_err(|e| CoreError::Store(format!("meta save: {e}")))?;
                self.meta_dirty = false;
            }
        }
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

fn logical_len(path: &std::path::Path) -> u64 {
    let f = std::fs::File::open(path).unwrap();
    super::read_preamble_len(&f)
        .unwrap()
        .expect("preallocated segment")
}

    /// A torn tail rewinds the logical end and keeps the preallocated file.
    #[test]
    fn torn_tail_preserves_prior_records() {
        use std::os::unix::fs::FileExt;
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "torn", 1024 * 1024).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"one", true)).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"two", true)).unwrap();
        DurableQueueLog::fsync(&mut wal).unwrap();

        let seg_path = {
            let a = wal.active.as_ref().expect("active segment");
            segment_path(&wal.dir, a.id)
        };
        drop(wal);

        let logical = logical_len(&seg_path);
        let phys = std::fs::metadata(&seg_path).unwrap().len();
        assert!(phys > logical, "segment should be preallocated");

        // Partial record at the logical end, with the header advanced over it.
        {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&seg_path)
                .unwrap();
            f.write_at(b"VLRA\x01\x01", logical).unwrap();
            super::write_preamble(&f, logical + 6).unwrap();
            f.sync_all().unwrap();
        }
        assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), phys);

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
        assert_eq!(logical_len(&seg_path), logical, "torn tail rewinds the header");
        assert_eq!(
            std::fs::metadata(&seg_path).unwrap().len(),
            phys,
            "preallocated file must not shrink"
        );
    }

    /// A complete record with a bad CRC halts and does not shrink the file.
    #[test]
    fn crc_mismatch_halts_not_truncate() {
        use std::os::unix::fs::FileExt;
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "crc", 1024 * 1024).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"ok", true)).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"bad", true)).unwrap();
        DurableQueueLog::fsync(&mut wal).unwrap();
        let seg_path = {
            let a = wal.active.as_ref().expect("active");
            segment_path(&wal.dir, a.id)
        };
        drop(wal);

        let logical = logical_len(&seg_path);
        let before_len = std::fs::metadata(&seg_path).unwrap().len();
        assert!(logical > 4 && before_len > logical);

        // The CRC is the 4 bytes that end at the logical end, not the file end.
        {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&seg_path)
                .unwrap();
            let at = logical - 4;
            let mut crc = [0u8; 4];
            f.read_at(&mut crc, at).unwrap();
            crc[0] ^= 0xFF;
            f.write_at(&crc, at).unwrap();
            f.sync_all().unwrap();
        }

        let mut wal2 = QueueWal::open(dir.path(), "/", "crc", 1024 * 1024).unwrap();
        let err = wal2.recover_messages().unwrap_err();
        assert!(
            matches!(err, StoreError::WalCorrupt { .. }),
            "expected WalCorrupt, got {err}"
        );
        assert_eq!(logical_len(&seg_path), logical);
        assert_eq!(
            std::fs::metadata(&seg_path).unwrap().len(),
            before_len,
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

    #[test]
    fn ack_watermark_in_the_segment_survives_a_missing_meta_file() {
        let dir = TempDir::new().unwrap();
        {
            let mut wal = QueueWal::open(dir.path(), "/", "ackseg", 1024 * 1024).unwrap();
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"m1", true))
                .unwrap();
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"m2", true))
                .unwrap();
            DurableQueueLog::acknowledge(&mut wal, QueueOffset(1)).unwrap();
            DurableQueueLog::acknowledge(&mut wal, QueueOffset(2)).unwrap();
            DurableQueueLog::fsync(&mut wal).unwrap();
        }
        let meta = queue_dir(dir.path(), "/", "ackseg").join(META_FILE_NAME);
        std::fs::remove_file(&meta).unwrap();
        let mut wal2 = QueueWal::open(dir.path(), "/", "ackseg", 1024 * 1024).unwrap();
        let recovered = wal2.recover_messages().unwrap();
        assert!(
            recovered.messages.is_empty(),
            "acked bodies must stay acked when meta json is gone"
        );
        assert_eq!(recovered.ack_watermark, 2);
    }

    /// The group commit syncs the segment ack record and leaves queue-meta.json alone.
    #[test]
    fn group_commit_does_not_rewrite_meta_json() {
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "meta", 1024 * 1024).unwrap();
        let meta_path = queue_dir(dir.path(), "/", "meta").join(META_FILE_NAME);
        let before = std::fs::read(&meta_path).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"m1", true)).unwrap();
        DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"m2", true)).unwrap();
        DurableQueueLog::acknowledge(&mut wal, QueueOffset(1)).unwrap();
        DurableQueueLog::acknowledge(&mut wal, QueueOffset(2)).unwrap();
        assert!(DurableQueueLog::meta_dirty(&wal));
        DurableQueueLog::fsync(&mut wal).unwrap();
        assert!(!DurableQueueLog::meta_dirty(&wal));
        let after = std::fs::read(&meta_path).unwrap();
        assert_eq!(
            before, after,
            "group commit must not rewrite queue-meta.json"
        );
        drop(wal);

        let mut wal2 = QueueWal::open(dir.path(), "/", "meta", 1024 * 1024).unwrap();
        let recovered = wal2.recover_messages().unwrap();
        assert!(
            recovered.messages.is_empty(),
            "acked bodies stay acked via the segment record"
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

    /// The physical file is longer than the logical log, and a later fsync
    /// overwrites that tail instead of growing it.
    #[test]
    fn preallocated_tail_recovers_and_the_next_fsync_does_not_grow() {
        let dir = TempDir::new().unwrap();
        let seg_path = {
            let mut wal = QueueWal::open(dir.path(), "/", "pre", 1024 * 1024).unwrap();
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(1), &sample(b"body", true))
                .unwrap();
            DurableQueueLog::fsync(&mut wal).unwrap();
            let path = segment_path(&wal.dir, wal.active.as_ref().unwrap().id);
            drop(wal);
            path
        };
        let phys = std::fs::metadata(&seg_path).unwrap().len();
        let logical = logical_len(&seg_path);
        assert!(
            phys > logical,
            "phys {phys} should pass the logical end {logical}"
        );

        {
            let mut wal = QueueWal::open(dir.path(), "/", "pre", 1024 * 1024).unwrap();
            let recovered = wal.recover_messages().unwrap();
            assert_eq!(recovered.messages.len(), 1);
            assert_eq!(recovered.messages[0].1.body.as_ref(), b"body");
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(2), &sample(b"more", true))
                .unwrap();
            DurableQueueLog::fsync(&mut wal).unwrap();
        }
        assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), phys);
        assert!(logical_len(&seg_path) > logical);

        let mut wal = QueueWal::open(dir.path(), "/", "pre", 1024 * 1024).unwrap();
        let recovered = wal.recover_messages().unwrap();
        assert_eq!(recovered.messages.len(), 2);
        assert_eq!(recovered.messages[1].1.body.as_ref(), b"more");
    }

    /// A segment written before the preamble still replays.
    #[test]
    fn legacy_segment_without_preamble_still_recovers() {
        use std::io::Write;
        let dir = TempDir::new().unwrap();
        let qdir = queue_dir(dir.path(), "/", "old");
        std::fs::create_dir_all(&qdir).unwrap();
        let bytes = encode_enqueue_record(1, &sample(b"legacy", true)).unwrap();
        let seg = segment_path(&qdir, 1);
        {
            let mut f = std::fs::File::create(&seg).unwrap();
            f.write_all(&bytes).unwrap();
            f.sync_all().unwrap();
        }
        let mut wal = QueueWal::open(dir.path(), "/", "old", 1024 * 1024).unwrap();
        let recovered = wal.recover_messages().unwrap();
        assert_eq!(recovered.messages.len(), 1);
        assert_eq!(recovered.messages[0].1.body.as_ref(), b"legacy");
        assert_eq!(recovered.next_offset, 2);
    }
}
