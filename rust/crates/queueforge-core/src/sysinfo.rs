//! Lightweight system probes (total RAM, disk free) without heavy crates.
//!
//! # Memory basis for watermarks
//!
//! Prefer **cgroup memory max** when finite (containers / k8s), otherwise host
//! `/proc/meminfo` `MemTotal`. This keeps relative watermarks effective inside
//! pods whose limit is far below host RAM.

use std::path::Path;

/// Best-effort total memory in bytes used as the watermark base.
///
/// Order:
/// 1. cgroup v2 `memory.max` / cgroup v1 `memory.limit_in_bytes` when finite
/// 2. Linux `/proc/meminfo` `MemTotal`, or macOS `sysctl hw.memsize`
/// 3. **8 GiB** fallback when the host total cannot be read
///
/// When both cgroup and host are available, returns `min(cgroup, host)`.
pub fn system_total_memory_bytes() -> u64 {
    let host = host_total_memory_bytes();
    if let Some(cg) = cgroup_memory_limit_bytes() {
        return cg.min(host).max(1);
    }
    host
}

fn host_total_memory_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(text) = std::fs::read_to_string("/proc/meminfo") {
            for line in text.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    if let Some(kb_str) = rest.split_whitespace().next() {
                        if let Ok(kb) = kb_str.parse::<u64>() {
                            return kb.saturating_mul(1024).max(1);
                        }
                    }
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(bytes) = macos_hw_memsize() {
            return bytes;
        }
    }

    // Fallback when the host total cannot be read.
    8 * 1024 * 1024 * 1024
}

/// `hw.memsize` from sysctl. `None` when the command is missing or unreadable.
#[cfg(target_os = "macos")]
fn macos_hw_memsize() -> Option<u64> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?;
    let n = text.trim().parse::<u64>().ok()?;
    Some(n.max(1))
}

/// Finite cgroup memory limit, if any.
///
/// - cgroup v2: `/sys/fs/cgroup/memory.max` (`max` = unlimited → `None`)
/// - cgroup v1: `/sys/fs/cgroup/memory/memory.limit_in_bytes` (huge values ignored)
fn cgroup_memory_limit_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        const CANDIDATES: &[&str] = &[
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ];
        for path in CANDIDATES {
            if let Ok(raw) = std::fs::read_to_string(path) {
                let s = raw.trim();
                if s.is_empty() || s == "max" {
                    continue;
                }
                if let Ok(n) = s.parse::<u64>() {
                    // v1 often exposes ~2^63-1 for "unlimited".
                    if n > 0 && n < (1u64 << 62) {
                        return Some(n);
                    }
                }
            }
        }
    }
    None
}

/// Free bytes available to non-root on the filesystem containing `path`.
///
/// Uses `statvfs(3)`. On error returns `Err` so callers can fail open/closed
/// according to policy.
pub fn disk_free_bytes(path: &Path) -> std::io::Result<u64> {
    disk_free_bytes_inner(path)
}

#[cfg(unix)]
fn disk_free_bytes_inner(path: &Path) -> std::io::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "path contains interior NUL",
        )
    })?;

    // Prefer an existing path; fall back to parent, then ".".
    let probe = if path.exists() {
        c_path
    } else if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        CString::new(parent.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "parent path contains interior NUL",
            )
        })?
    } else {
        CString::new(".").expect("static")
    };

    unsafe {
        let mut s: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(probe.as_ptr(), &mut s) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let free = (s.f_bavail as u64).saturating_mul(s.f_frsize as u64);
        Ok(free)
    }
}

#[cfg(not(unix))]
fn disk_free_bytes_inner(_path: &Path) -> std::io::Result<u64> {
    // Conservative: report a large free value so disk limit does not brick Windows builds.
    Ok(u64::MAX / 4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_memory_is_nonzero() {
        assert!(system_total_memory_bytes() > 0);
    }

    #[test]
    fn disk_free_for_cwd() {
        let free = disk_free_bytes(Path::new(".")).expect("disk free");
        assert!(free > 0);
    }

    #[test]
    fn host_memory_nonzero() {
        assert!(host_total_memory_bytes() > 0);
    }

    #[test]
    fn macos_host_ram_is_not_the_8gib_fallback() {
        #[cfg(target_os = "macos")]
        {
            let host = host_total_memory_bytes();
            let fallback = 8 * 1024 * 1024 * 1024;
            assert_ne!(
                host, fallback,
                "macOS host RAM must come from sysctl hw.memsize"
            );
            assert_eq!(system_total_memory_bytes(), host);
        }
    }
}
