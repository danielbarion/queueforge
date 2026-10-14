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
    DurableLogFactory, DurableQueueLog, Error as CoreError, LogPosition, Message, OpenedDurableLog,
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

/// Header version of a segment renamed from a compacted one. Its record CRCs
/// are XORed with [`salt_of`] its id: the previous file's records are still
/// past the write point, and under the new id they never read back.
const REUSED_VERSION: u32 = 2;

/// A compacted segment kept for the next one, so a busy queue stops
/// zero-filling and syncing every 4 MiB it writes twice (B5 in Bun).
const SPARE_NAME: &str = "spare-segment.log";

/// The CRC salt of segment `id`. A multiply by an odd constant is one to one
/// on 32 bits, so two ids that share a file never share a salt; zero only for
/// id 0, which is never a reuse target.
fn salt_of(id: u64) -> u32 {
    ((id as u32) ^ ((id >> 32) as u32)).wrapping_mul(0x9e37_79b1)
}

/// The salt records of segment `id` carry under header `version`.
fn salt_for(id: u64, version: u32) -> u32 {
    if version >= REUSED_VERSION {
        salt_of(id)
    } else {
        0
    }
}

/// XOR an encoded record's trailing CRC with `salt`.
fn salt_record(bytes: &mut [u8], salt: u32) {
    if salt == 0 || bytes.len() < 4 {
        return;
    }
    let at = bytes.len() - 4;
    let crc = u32::from_le_bytes(bytes[at..].try_into().unwrap()) ^ salt;
    bytes[at..].copy_from_slice(&crc.to_le_bytes());
}

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
    /// Where the last enqueue record went.
    last_pos: Option<LogPosition>,
    /// Read handle for one closed segment, reused by [`DurableQueueLog::read_at`].
    reader: Option<(u64, File)>,
    /// Where recovery found each live enqueue, until the first append or fsync.
    recovered_at: std::collections::HashMap<u64, LogPosition>,
    /// Body lengths of the enqueues recovery left on disk, as long as
    /// `recovered_at` lasts.
    recovered_len: std::collections::HashMap<u64, u64>,
    /// A compacted segment waiting to become the next one ([`SPARE_NAME`]).
    spare: Option<PathBuf>,
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
    /// Header version, kept on every header rewrite.
    version: u32,
    /// CRC salt of this segment's records; 0 unless it was reused.
    salt: u32,
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

fn write_preamble(file: &File, logical_len: u64, version: u32) -> io::Result<()> {
    let mut hdr = [0u8; PREAMBLE_LEN as usize];
    hdr[0..4].copy_from_slice(PREAMBLE_MAGIC);
    hdr[4..8].copy_from_slice(&version.to_le_bytes());
    hdr[8..16].copy_from_slice(&logical_len.to_le_bytes());
    write_all_at(file, &hdr, 0)
}

/// Logical end recorded in a preallocated segment, when the file has that header.
fn read_preamble_len(file: &File) -> io::Result<Option<u64>> {
    Ok(read_preamble(file)?.map(|(len, _)| len))
}

/// Logical end and header version of a preallocated segment.
fn read_preamble(file: &File) -> io::Result<Option<(u64, u32)>> {
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
    let version = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
    let logical = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    Ok(Some((logical.max(PREAMBLE_LEN), version)))
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
            return Err(io::Error::new(io::ErrorKind::Other, "wal segment is full"));
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
    salt: u32,
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
            last_pos: None,
            reader: None,
            recovered_at: std::collections::HashMap::new(),
            recovered_len: std::collections::HashMap::new(),
            spare: None,
        };
        let spare = wal.dir.join(SPARE_NAME);
        if spare.exists() {
            wal.spare = Some(spare);
        }
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
            let header = read_preamble(&file)?;
            let preamble = header.is_some();
            let logical = header.map(|(len, _)| len).unwrap_or(phys).min(phys);
            let version = header.map(|(_, v)| v).unwrap_or(1);
            let salt = salt_for(id, version);
            let max_offset = scan_max_offset(&path, salt)?;
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
                    version,
                    salt,
                });
            } else {
                self.closed.insert(
                    id,
                    ClosedSegment {
                        path,
                        max_offset,
                        len: logical,
                        salt,
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
        let reused = if created_new && salt_of(id) != 0 {
            self.take_spare(&path)?
        } else {
            None
        };
        let (file, len, allocated, preamble, version) = if let Some((file, phys)) = reused {
            // Already written and synced end to end: appends overwrite it.
            (file, PREAMBLE_LEN, phys, true, REUSED_VERSION)
        } else {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)?;
            if created_new {
                write_preamble(&file, PREAMBLE_LEN, 1)?;
                // Materialize the first chunk before any confirm so the fsync
                // overwrites blocks that already have extents.
                let target = extend_target(PREAMBLE_LEN, PREAMBLE_LEN + 1, self.segment_max_bytes);
                materialize_range(&file, PREAMBLE_LEN, target)?;
                // Issue 8: parent-dir fsync after creating a new segment file.
                sync_dir(&self.dir)?;
                (file, PREAMBLE_LEN, target, true, 1)
            } else {
                let phys = file.metadata()?.len();
                let header = read_preamble(&file)?;
                let logical = header.map(|(len, _)| len).unwrap_or(phys).min(phys);
                let version = header.map(|(_, v)| v).unwrap_or(1);
                (file, logical, phys, header.is_some(), version)
            }
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
            version,
            salt: salt_for(id, version),
        });
        self.meta.next_segment_id = id.saturating_add(1);
        self.meta_dirty = true;
        Ok(())
    }

    /// Turn the spare into segment `path`: an empty, salted header is synced
    /// before the rename, so a crash leaves either the spare or an empty
    /// segment. Returns the file and its size.
    fn take_spare(&mut self, path: &Path) -> Result<Option<(File, u64)>> {
        let Some(spare) = self.spare.take() else {
            return Ok(None);
        };
        let file = match OpenOptions::new().read(true).write(true).open(&spare) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        write_preamble(&file, PREAMBLE_LEN, REUSED_VERSION)?;
        file.sync_data()?;
        fs::rename(&spare, path)?;
        sync_dir(&self.dir)?;
        let phys = file.metadata()?.len();
        metrics::counter!("queueforge_wal_segments_reused_total").increment(1);
        Ok(Some((file, phys)))
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
                write_preamble(&a.file, a.len, a.version)?;
            }
            a.file.sync_all()?;
            self.closed.insert(
                a.id,
                ClosedSegment {
                    path,
                    max_offset: a.max_offset,
                    len: a.len,
                    salt: a.salt,
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
        self.recover_messages_within(None)
    }

    /// [`Self::recover_messages`], holding at most `keep_bytes` of bodies.
    /// The oldest past it come back with an empty body; their length is in
    /// [`DurableQueueLog::recovered_body_len`] and the record stays on disk
    /// for [`DurableQueueLog::read_at`].
    pub fn recover_messages_within(&mut self, keep_bytes: Option<u64>) -> Result<RecoveredState> {
        let mut ack_wm = self.meta.ack_watermark;
        let mut ready: Vec<(u64, Message)> = Vec::new();
        let mut budget = keep_bytes.map(BodyBudget::new);
        let mut last_offset = ack_wm;

        // Collect segments in order.
        let mut seg_ids: Vec<u64> = self.closed.keys().copied().collect();
        if let Some(a) = &self.active {
            seg_ids.push(a.id);
        }
        seg_ids.sort_unstable();

        let last_id = seg_ids.last().copied();
        for id in seg_ids {
            let path = segment_path(&self.dir, id);
            if !path.exists() {
                continue;
            }
            let salt = self
                .active
                .as_ref()
                .filter(|a| a.id == id)
                .map(|a| a.salt)
                .or_else(|| self.closed.get(&id).map(|c| c.salt))
                .unwrap_or(0);
            match replay_segment(&path, ack_wm, &mut ready, budget.as_mut(), salt, Some(id) == last_id) {
                Ok(ReplayOutcome {
                    positions,
                    last_valid_offset,
                    truncated_to,
                    ack_watermark,
                }) => {
                    for (offset, pos, len) in positions {
                        self.recovered_at.insert(
                            offset,
                            LogPosition {
                                segment: id,
                                pos,
                                len,
                            },
                        );
                    }
                    ack_wm = ack_wm.max(ack_watermark);
                    if let Some(trunc) = truncated_to {
                        // Drop the torn tail. A preallocated segment keeps its
                        // blocks and rewinds the header instead of shrinking.
                        let f = OpenOptions::new().write(true).read(true).open(&path)?;
                        if let Some((_, version)) = read_preamble(&f)? {
                            write_preamble(&f, trunc, version)?;
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
        self.recovered_at.retain(|off, _| *off > ack_wm);
        self.recovered_len = budget.map(|b| b.dropped).unwrap_or_default();
        self.recovered_len.retain(|off, _| *off > ack_wm);

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
    /// `(offset, position, encoded length)` of each enqueue record.
    positions: Vec<(u64, u64, u32)>,
    last_valid_offset: Option<u64>,
    truncated_to: Option<u64>,
    ack_watermark: u64,
}

/// Recovery's limit on bodies held in memory. Past it the oldest replayed
/// bodies are dropped as each record is read, so a long stream never sits
/// in memory whole.
struct BodyBudget {
    keep: u64,
    resident: u64,
    /// Index in `ready` of the oldest body not yet dropped.
    next: usize,
    /// Offset to body length of every dropped body.
    dropped: std::collections::HashMap<u64, u64>,
}

impl BodyBudget {
    fn new(keep: u64) -> Self {
        BodyBudget {
            keep,
            resident: 0,
            next: 0,
            dropped: std::collections::HashMap::new(),
        }
    }

    /// Count the body just pushed onto `ready`, then drop the oldest past `keep`.
    fn admit(&mut self, ready: &mut [(u64, Message)]) {
        if let Some((_, newest)) = ready.last() {
            self.resident = self.resident.saturating_add(newest.body.len() as u64);
        }
        while self.resident > self.keep && self.next < ready.len() {
            let (offset, msg) = &mut ready[self.next];
            let len = msg.body.len() as u64;
            if len > 0 {
                msg.body = Default::default();
                self.dropped.insert(*offset, len);
                self.resident -= len;
            }
            self.next += 1;
        }
    }
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

/// Replay one segment, pushing its enqueues onto `ready`. Callers drop those
/// at or below the final ack watermark.
///
/// `salt` is the segment's CRC salt. In the newest segment of a reused file,
/// a record that does not check is the logical end: a crash can leave the
/// header ahead of the data, and the bytes there are the previous file's.
fn replay_segment(
    path: &Path,
    ack_wm: u64,
    ready: &mut Vec<(u64, Message)>,
    mut budget: Option<&mut BodyBudget>,
    salt: u32,
    newest: bool,
) -> Result<ReplayOutcome> {
    let (start, end) = segment_bounds(path)?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file).take(end.saturating_sub(start));
    let mut pos = start;
    let mut positions = Vec::new();
    let mut last_valid_offset = None;
    let mut truncated_to = None;
    let mut wal_ack = ack_wm;
    let preamble = start == PREAMBLE_LEN;
    let reused_tail = newest && salt != 0;

    loop {
        if pos >= end {
            break;
        }
        let rec_start = pos;
        match codec::read_record_with(&mut reader, salt, end - rec_start) {
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
                        positions.push((rec.offset, rec_start, rec.encoded_len as u32));
                        ready.push((rec.offset, msg));
                        if let Some(budget) = budget.as_deref_mut() {
                            budget.admit(ready);
                        }
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
            Err(StoreError::WalCorrupt { reason, .. }) if preamble && reason == "bad magic 0x0" => {
                truncated_to = Some(rec_start);
                break;
            }
            Err(StoreError::WalCorrupt { .. }) if reused_tail => {
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

    positions.retain(|(off, _, _)| *off > wal_ack);
    Ok(ReplayOutcome {
        positions,
        last_valid_offset,
        truncated_to,
        ack_watermark: wal_ack,
    })
}

fn scan_max_offset(path: &Path, salt: u32) -> Result<u64> {
    let (start, end) = segment_bounds(path)?;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(start))?;
    let mut reader = BufReader::new(file).take(end.saturating_sub(start));
    let mut max = 0u64;
    let mut pos = start;
    loop {
        match codec::read_record_with(&mut reader, salt, end.saturating_sub(pos)) {
            Ok(Some(rec)) => {
                pos = pos.saturating_add(rec.encoded_len as u64);
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

/// The file starts with the preallocated-segment header, so it can be reused.
fn is_preallocated(path: &Path) -> bool {
    File::open(path)
        .and_then(|f| read_preamble_len(&f))
        .map(|len| len.is_some())
        .unwrap_or(false)
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
        let mut bytes =
            encode_enqueue_record(offset.0, msg).map_err(|e| CoreError::Store(e.to_string()))?;
        self.rotate_if_needed(bytes.len() as u64)
            .map_err(|e| CoreError::Store(e.to_string()))?;
        self.ensure_active()
            .map_err(|e| CoreError::Store(e.to_string()))?;

        let active = self
            .active
            .as_mut()
            .ok_or_else(|| CoreError::Store("no active WAL segment".into()))?;
        salt_record(&mut bytes, active.salt);
        let at = active.len;
        self.recovered_at = std::collections::HashMap::new();
        self.recovered_len = std::collections::HashMap::new();
        active
            .push_record(&bytes)
            .map_err(|e| CoreError::Store(e.to_string()))?;
        active.max_offset = active.max_offset.max(offset.0);
        self.last_pos = Some(LogPosition {
            segment: active.id,
            pos: at,
            len: bytes.len() as u32,
        });
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
        if !self.recovered_at.is_empty() {
            self.recovered_at = std::collections::HashMap::new();
            self.recovered_len = std::collections::HashMap::new();
        }
        // The ack watermark rides in the segment so one sync covers bodies and acks.
        if self.meta_dirty && self.meta.ack_watermark > 0 {
            if let Some(active) = self.active.as_mut() {
                let mut bytes = encode_ack_record(self.meta.ack_watermark);
                salt_record(&mut bytes, active.salt);
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
                write_preamble(&active.file, active.len, active.version)
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
                if self.reader.as_ref().map(|(rid, _)| *rid) == Some(id) {
                    self.reader = None;
                }
                if self.spare.is_none() && is_preallocated(&seg.path) {
                    let spare = self.dir.join(SPARE_NAME);
                    match fs::rename(&seg.path, &spare) {
                        Ok(()) => {
                            self.spare = Some(spare);
                            removed_any = true;
                            metrics::counter!("queueforge_wal_segments_compacted_total").increment(1);
                            continue;
                        }
                        Err(e) => {
                            warn!(path = %seg.path.display(), error = %e, "failed to keep compacted segment as spare");
                        }
                    }
                }
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

    fn last_position(&self) -> Option<LogPosition> {
        self.last_pos
    }

    fn recovered_position(&self, offset: QueueOffset) -> Option<LogPosition> {
        self.recovered_at.get(&offset.0).copied()
    }

    fn recovered_body_len(&self, offset: QueueOffset) -> Option<u64> {
        self.recovered_len.get(&offset.0).copied()
    }

    fn read_at(&mut self, at: LogPosition) -> CoreResult<Message> {
        let mut buf = vec![0u8; at.len as usize];
        let mut salt = 0;
        let read = match self.active.as_mut() {
            Some(active) if active.id == at.segment => {
                salt = active.salt;
                // A record still in the write buffer is not on disk yet.
                if at.pos + at.len as u64 > active.len - active.buf.len() as u64 {
                    active
                        .flush_buf()
                        .map_err(|e| CoreError::Store(format!("segment flush: {e}")))?;
                }
                active.file.read_exact_at(&mut buf, at.pos)
            }
            _ => {
                if self.reader.as_ref().map(|(id, _)| *id) != Some(at.segment) {
                    let seg = self.closed.get(&at.segment).ok_or_else(|| {
                        CoreError::Store(format!("WAL segment {} is gone", at.segment))
                    })?;
                    salt = seg.salt;
                    let file = File::open(&seg.path)
                        .map_err(|e| CoreError::Store(format!("WAL open: {e}")))?;
                    self.reader = Some((at.segment, file));
                }
                if let Some(seg) = self.closed.get(&at.segment) {
                    salt = seg.salt;
                }
                let (_, file) = self.reader.as_ref().expect("opened above");
                file.read_exact_at(&mut buf, at.pos)
            }
        };
        read.map_err(|e| CoreError::Store(format!("WAL read: {e}")))?;
        let max = buf.len() as u64;
        match codec::read_record_with(&mut buf.as_slice(), salt, max) {
            Ok(Some(rec)) => rec
                .message
                .ok_or_else(|| CoreError::Store("WAL record is not an enqueue".into())),
            Ok(None) => Err(CoreError::Store("WAL record is empty".into())),
            Err(e) => Err(CoreError::Store(e.to_string())),
        }
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

    fn open_within(
        &self,
        vhost: &str,
        queue: &str,
        keep_bytes: Option<u64>,
    ) -> CoreResult<OpenedDurableLog> {
        let mut wal = QueueWal::open(&self.data_dir, vhost, queue, self.segment_max_bytes)
            .map_err(|e| CoreError::Store(e.to_string()))?;
        // Issue 3: always recover so declare never serves empty over live segments.
        let recovered = wal.recover_messages_within(keep_bytes).map_err(|e| {
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
}

impl DurableLogFactory for WalFactory {
    fn open(&self, vhost: &str, queue: &str) -> CoreResult<OpenedDurableLog> {
        self.open_within(vhost, queue, None)
    }

    fn open_paged(&self, vhost: &str, queue: &str, keep_bytes: u64) -> CoreResult<OpenedDurableLog> {
        self.open_within(vhost, queue, Some(keep_bytes))
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
    fn read_at_returns_records_from_closed_active_and_buffered_segments() {
        let dir = TempDir::new().unwrap();
        // 4 KiB segments rotate every few records.
        let mut wal = QueueWal::open(dir.path(), "/", "s", 4096).unwrap();
        let mut positions = Vec::new();
        for i in 1..=40u64 {
            let body = vec![b'a' + (i % 26) as u8; 500 + i as usize];
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(i), &sample(&body, true))
                .unwrap();
            positions.push((body, wal.last_position().unwrap()));
        }
        let segments: std::collections::BTreeSet<u64> =
            positions.iter().map(|(_, at)| at.segment).collect();
        assert!(segments.len() > 2, "rotated: {segments:?}");
        // The newest records are still in the write buffer; read_at flushes them.
        for (body, at) in positions.iter().rev() {
            let message = DurableQueueLog::read_at(&mut wal, *at).unwrap();
            assert_eq!(message.body.as_ref(), body.as_slice());
            assert_eq!(message.priority, Some(5));
        }
        DurableQueueLog::fsync(&mut wal).unwrap();
        for (body, at) in &positions {
            assert_eq!(
                DurableQueueLog::read_at(&mut wal, *at)
                    .unwrap()
                    .body
                    .as_ref(),
                body.as_slice()
            );
        }
    }

    #[test]
    fn recovery_reports_where_each_live_record_is_until_the_next_write() {
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "s", 4096).unwrap();
        for i in 1..=20u64 {
            let body = vec![b'a' + (i % 26) as u8; 700];
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(i), &sample(&body, true))
                .unwrap();
        }
        DurableQueueLog::acknowledge(&mut wal, QueueOffset(1)).unwrap();
        DurableQueueLog::fsync(&mut wal).unwrap();
        drop(wal);
        let mut wal = QueueWal::open(dir.path(), "/", "s", 4096).unwrap();
        let recovered = wal.recover_messages().unwrap();
        assert_eq!(recovered.messages.len(), 19);
        assert!(DurableQueueLog::recovered_position(&wal, QueueOffset(1)).is_none());
        for (offset, message) in &recovered.messages {
            let at = DurableQueueLog::recovered_position(&wal, QueueOffset(*offset)).unwrap();
            let read = DurableQueueLog::read_at(&mut wal, at).unwrap();
            assert_eq!(read.body, message.body, "offset {offset}");
        }
        DurableQueueLog::fsync(&mut wal).unwrap();
        assert!(DurableQueueLog::recovered_position(&wal, QueueOffset(2)).is_none());
    }

    #[test]
    fn recovery_within_a_budget_leaves_the_oldest_bodies_on_disk() {
        let dir = TempDir::new().unwrap();
        let mut wal = QueueWal::open(dir.path(), "/", "s", 4096).unwrap();
        let mut bodies = std::collections::HashMap::new();
        for i in 1..=30u64 {
            let body = vec![b'a' + (i % 26) as u8; 700];
            DurableQueueLog::append_enqueue(&mut wal, QueueOffset(i), &sample(&body, true))
                .unwrap();
            bodies.insert(i, body);
        }
        DurableQueueLog::acknowledge(&mut wal, QueueOffset(1)).unwrap();
        DurableQueueLog::fsync(&mut wal).unwrap();
        drop(wal);
        let mut wal = QueueWal::open(dir.path(), "/", "s", 4096).unwrap();
        // Room for four 700-byte bodies.
        let recovered = wal.recover_messages_within(Some(3000)).unwrap();
        assert_eq!(recovered.messages.len(), 29);
        let offsets: Vec<u64> = recovered.messages.iter().map(|(o, _)| *o).collect();
        assert_eq!(offsets, (2..=30).collect::<Vec<_>>());
        for (offset, message) in &recovered.messages {
            let on_disk = DurableQueueLog::recovered_body_len(&wal, QueueOffset(*offset));
            if *offset > 26 {
                assert_eq!(message.body.as_ref(), bodies[offset].as_slice());
                assert_eq!(on_disk, None, "offset {offset}");
            } else {
                assert!(message.body.is_empty(), "offset {offset}");
                assert_eq!(on_disk, Some(700), "offset {offset}");
            }
            let at = DurableQueueLog::recovered_position(&wal, QueueOffset(*offset)).unwrap();
            let read = DurableQueueLog::read_at(&mut wal, at).unwrap();
            assert_eq!(read.body.as_ref(), bodies[offset].as_slice(), "offset {offset}");
        }
        assert_eq!(DurableQueueLog::recovered_body_len(&wal, QueueOffset(1)), None);
        DurableQueueLog::fsync(&mut wal).unwrap();
        assert_eq!(DurableQueueLog::recovered_body_len(&wal, QueueOffset(2)), None);
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

    /// Write `bodies` from offset `first` and sync each one.
    fn write_synced(wal: &mut QueueWal, first: u64, bodies: &[Vec<u8>]) {
        for (i, body) in bodies.iter().enumerate() {
            DurableQueueLog::append_enqueue(wal, QueueOffset(first + i as u64), &sample(body, true)).unwrap();
            DurableQueueLog::fsync(wal).unwrap();
        }
    }

    /// Segments of a queue that has written and acked a few segments' worth:
    /// the last compaction kept one as the spare.
    fn churned(dir: &Path) -> (QueueWal, u64) {
        let mut wal = QueueWal::open(dir, "/", "reuse", 16 * 1024).unwrap();
        let bodies: Vec<Vec<u8>> = (0..40u8).map(|i| vec![i; 1500]).collect();
        write_synced(&mut wal, 1, &bodies);
        for i in 1..=40u64 {
            DurableQueueLog::acknowledge(&mut wal, QueueOffset(i)).unwrap();
        }
        DurableQueueLog::fsync(&mut wal).unwrap();
        DurableQueueLog::compact(&mut wal).unwrap();
        (wal, 41)
    }

    #[test]
    fn a_compacted_segment_is_reused_and_a_restart_replays_only_live_rows() {
        let dir = TempDir::new().unwrap();
        let (mut wal, next) = churned(dir.path());
        let queue = wal.dir().to_path_buf();
        assert!(queue.join(SPARE_NAME).exists(), "compaction kept a spare");
        let live: Vec<Vec<u8>> = (0..20u8).map(|i| vec![100 + i; 1500]).collect();
        write_synced(&mut wal, next, &live);
        assert!(!queue.join(SPARE_NAME).exists(), "the next segment took the spare");
        let reused = wal.closed.values().map(|c| c.salt).chain(wal.active.as_ref().map(|a| a.salt));
        assert!(reused.into_iter().any(|salt| salt != 0), "one segment is a reused, salted file");
        drop(wal);

        let mut again = QueueWal::open(dir.path(), "/", "reuse", 16 * 1024).unwrap();
        let state = again.recover_messages().unwrap();
        let bodies: Vec<Vec<u8>> = state.messages.iter().map(|(_, m)| m.body.to_vec()).collect();
        assert_eq!(bodies, live);
        assert_eq!(state.messages.first().map(|(o, _)| *o), Some(next));
    }

    #[test]
    fn a_reused_segment_whose_header_ran_ahead_ends_at_its_last_good_record() {
        let dir = TempDir::new().unwrap();
        let (mut wal, next) = churned(dir.path());
        // Write until the reused file is the active segment, then two more.
        let mut live = Vec::new();
        let mut extra = 2;
        while extra > 0 {
            let body = vec![(live.len() % 250) as u8; 1500];
            write_synced(&mut wal, next + live.len() as u64, std::slice::from_ref(&body));
            live.push(body);
            if wal.active.as_ref().is_some_and(|a| a.salt != 0) {
                extra -= 1;
            }
            assert!(live.len() < 200, "never rotated into the spare");
        }
        let active = wal.active.as_ref().unwrap();
        assert_ne!(active.salt, 0, "the active segment is the reused file");
        let path = segment_path(wal.dir(), active.id);
        drop(wal);

        // A crash after the header reached the disk but not the data: the
        // header claims the whole file, and past the live records are the
        // previous segment's, mid-record and with its own salt.
        let f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let phys = f.metadata().unwrap().len();
        write_preamble(&f, phys, REUSED_VERSION).unwrap();
        drop(f);

        let mut again = QueueWal::open(dir.path(), "/", "reuse", 16 * 1024).unwrap();
        let state = again.recover_messages().unwrap();
        let bodies: Vec<Vec<u8>> = state.messages.iter().map(|(_, m)| m.body.to_vec()).collect();
        assert_eq!(bodies, live);
        // The header was pulled back to the last good record.
        let f = File::open(&path).unwrap();
        assert!(read_preamble_len(&f).unwrap().unwrap() < phys);
    }

    #[test]
    fn salts_differ_per_segment_and_salted_records_need_their_own_salt() {
        assert_ne!(salt_of(1), salt_of(2));
        assert_ne!(salt_of(1), 0);
        let mut bytes = encode_enqueue_record(5, &sample(b"x", true)).unwrap();
        salt_record(&mut bytes, salt_of(3));
        assert!(codec::read_record_with(&mut bytes.as_slice(), salt_of(3), u64::MAX).unwrap().is_some());
        assert!(codec::read_record_with(&mut bytes.as_slice(), salt_of(4), u64::MAX).is_err());
        assert!(codec::read_record(&mut bytes.as_slice()).is_err());
        // A length past the bytes left is torn, not an allocation of that size.
        assert!(matches!(
            codec::read_record_with(&mut bytes.as_slice(), salt_of(3), 10),
            Err(StoreError::WalTornTail { .. })
        ));
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
            super::write_preamble(&f, logical + 6, 1).unwrap();
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
        assert_eq!(
            logical_len(&seg_path),
            logical,
            "torn tail rewinds the header"
        );
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
