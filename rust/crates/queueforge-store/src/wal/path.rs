//! On-disk path helpers for per-queue WAL directories.

use std::path::{Path, PathBuf};

/// Percent-encode a vhost or queue name for use as a single path component.
///
/// Encodes bytes outside `[A-Za-z0-9._-]` so `/` (default vhost) and other
/// special characters never split the path.
pub fn encode_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for &b in name.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                out.push(b as char);
            }
            _ => {
                out.push('%');
                out.push(nibble(b >> 4));
                out.push(nibble(b & 0xf));
            }
        }
    }
    out
}

fn nibble(n: u8) -> char {
    char::from(if n < 10 { b'0' + n } else { b'A' + (n - 10) })
}

/// `data_dir/queues/{encoded_vhost}/{encoded_queue}`.
pub fn queue_dir(data_dir: &Path, vhost: &str, queue: &str) -> PathBuf {
    data_dir
        .join("queues")
        .join(encode_name(vhost))
        .join(encode_name(queue))
}

/// Segment file path for `segment-{id:08}.log`.
pub fn segment_path(queue_dir: &Path, segment_id: u64) -> PathBuf {
    queue_dir.join(format!("segment-{segment_id:08}.log"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_slash_vhost() {
        assert_eq!(encode_name("/"), "%2F");
        assert_eq!(encode_name("orders"), "orders");
    }
}
