//! Percent-decode path vhost names.

use queueforge_core::DEFAULT_VHOST;

use crate::error::MgmtError;

/// Decode a path-captured vhost. Axum percent-decodes path params, so `%2F`
/// arrives as `/`. Also accept the literal `%2F` if a proxy left it encoded.
pub fn decode_vhost(raw: &str) -> Result<String, MgmtError> {
    if raw.is_empty() {
        // `/api/queues/` with empty segment → treat as default vhost.
        return Ok(DEFAULT_VHOST.to_string());
    }
    // If still percent-encoded (e.g. double-encoding avoided by client tools).
    if raw.contains('%') {
        let decoded = percent_decode(raw).map_err(MgmtError::BadRequest)?;
        return Ok(decoded);
    }
    Ok(raw.to_string())
}

/// Decode a percent-encoded path segment `input`. Returns the decoded string, or an error when a `%` escape is truncated or not hex.
pub(super) fn percent_decode(input: &str) -> Result<String, String> {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let h = std::str::from_utf8(&bytes[i + 1..i + 3]).map_err(|e| e.to_string())?;
                let v = u8::from_str_radix(h, 16).map_err(|e| e.to_string())?;
                out.push(v);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|e| e.to_string())
}
