//! AMQP 0-9-1 framing and method codec for QueueForge.
//!
//! This crate provides:
//! - Protocol-header handling ([`PROTOCOL_HEADER`])
//! - Low-level frame encode/decode (method, content-header, content-body, heartbeat)
//! - Method catalog for connection / channel / exchange / queue / basic / confirm
//! - Field-table and field-value encode/decode
//!
//! # Protocol header
//!
//! Clients open a connection by sending the 8-byte header
//! [`PROTOCOL_HEADER`] (`AMQP\\0\\0\\x09\\x01`).
//!
//! # Frames
//!
//! Every frame is:
//!
//! ```text
//! type (1) | channel (2) | size (4) | payload (size) | 0xCE (1)
//! ```
//!
//! Use [`Frame::encode`] / [`Frame::decode`] for round-trips. Prefer
//! [`Frame::decode_with_limit`] with negotiated `frame_max` on untrusted
//! connections. Heartbeats are available via [`Frame::heartbeat`].
//!
//! # Methods
//!
//! Method frame payloads are `class-id (short) | method-id (short) | arguments`.
//! Use [`Method::encode`] / [`Method::decode`], or [`Method::to_frame`] /
//! [`Method::from_frame`] to wrap them in frames.

#![deny(missing_docs)]

mod content;
mod error;
mod frame;
mod method;
mod protocol;
mod types;

pub use content::{BasicProperties, ContentHeader};
pub use error::{Error, Result};
pub use frame::{Frame, FrameType, FRAME_END, FRAME_END_LEN, FRAME_HEADER_LEN, FRAME_MIN_LEN};
pub use method::{basic, channel, confirm, connection, exchange, queue, tx, Method};
pub use protocol::{
    decode_protocol_header, encode_protocol_header, PROTOCOL_HEADER, PROTOCOL_HEADER_LEN,
};
pub use types::{Decoder, Encoder, FieldTable, FieldValue, SHORTSTR_MAX};

/// Fuzz / hardening entry: feed arbitrary bytes into AMQP decoders.
///
/// Must never panic. Intended for unit tests, `scripts/fuzz-frame-decode.sh`,
/// and cargo-fuzz targets (`crates/queueforge-amqp/fuzz`).
// fuzz target entry
pub fn fuzz_decode_input(data: &[u8]) {
    let _ = decode_protocol_header(data);
    let _ = Frame::decode(data);
    let _ = Frame::decode_with_limit(data, 4096);
    let _ = Frame::decode_with_limit(data, 128);
    let _ = Frame::decode_with_limit(data, 8);
    let _ = Method::decode(data);

    // Content header decode path when enough bytes exist.
    if data.len() >= 14 {
        let _ = ContentHeader::decode(data);
    }

    // Try after a plausible method-frame header prefix.
    if !data.is_empty() {
        let mut buf = vec![1u8, 0, 0, 0, 0, 0, 0]; // method, ch=0, size=0
        buf.extend_from_slice(data);
        let _ = Frame::decode(&buf);
        let _ = Frame::decode_with_limit(&buf, 128);
        let _ = Frame::decode_with_limit(&buf, 8192);
    }

    // Prefix as content-header / body / heartbeat frame types.
    for frame_type in [2u8, 3u8, 8u8] {
        let mut buf = vec![frame_type, 0, 1];
        let size = (data.len().min(u32::MAX as usize)) as u32;
        buf.extend_from_slice(&size.to_be_bytes());
        buf.extend_from_slice(data);
        buf.push(FRAME_END);
        let _ = Frame::decode(&buf);
        let _ = Frame::decode_with_limit(&buf, 4096);
    }

    // Protocol header variants (truncated / mutated).
    if data.len() >= 8 {
        let mut hdr = PROTOCOL_HEADER.to_vec();
        for (i, b) in data.iter().take(8).enumerate() {
            hdr[i] ^= b;
        }
        let _ = decode_protocol_header(&hdr);
    }
}

/// Lightweight fuzz-oriented checks exercised from unit tests.
///
/// Full cargo-fuzz lives under `crates/queueforge-amqp/fuzz` (nightly + libFuzzer).
/// `scripts/fuzz-frame-decode.sh` runs a libFuzzer-free loop via `fuzz_random_loop`.
#[cfg(test)]
mod fuzz_skeleton {
    use crate::{fuzz_decode_input, Frame, FRAME_END, PROTOCOL_HEADER};

    #[test]
    fn fuzz_empty() {
        fuzz_decode_input(&[]);
    }

    #[test]
    fn fuzz_short_patterns() {
        for b in 0u8..=16 {
            fuzz_decode_input(&[b]);
            fuzz_decode_input(&[b, b, b, b, b, b, b, b]);
        }
        // Valid-looking heartbeat with bad end marker variants.
        fuzz_decode_input(&[8, 0, 0, 0, 0, 0, 0, 0x00]);
        fuzz_decode_input(&[8, 0, 0, 0, 0, 0, 0, 0xCE]);
        // Unknown type.
        fuzz_decode_input(&[4, 0, 0, 0, 0, 0, 0, 0xCE]);
        // Large claimed size without payload.
        fuzz_decode_input(&[1, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF]);
        // Oversized size field with tiny buffer.
        fuzz_decode_input(&[1, 0, 0, 0x00, 0x00, 0x10, 0x00, FRAME_END]);
        // Protocol header.
        fuzz_decode_input(&PROTOCOL_HEADER);
        // Almost protocol header.
        let mut almost = PROTOCOL_HEADER.to_vec();
        almost[7] = 0x00;
        fuzz_decode_input(&almost);
    }

    #[test]
    fn fuzz_structured_frames() {
        // Empty method frame, ch=0.
        fuzz_decode_input(&[1, 0, 0, 0, 0, 0, 0, FRAME_END]);
        // Method with class/method ids only.
        fuzz_decode_input(&[1, 0, 1, 0, 0, 0, 4, 0, 10, 0, 10, FRAME_END]);
        // Content header shell (class=60, weight=0, body_size=0, flags=0).
        fuzz_decode_input(&[
            2, 0, 1, 0, 0, 0, 14, // header, ch=1, size=14
            0, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, FRAME_END,
        ]);
        // Body frame with payload.
        fuzz_decode_input(&[3, 0, 1, 0, 0, 0, 4, b'p', b'i', b'n', b'g', FRAME_END]);
        // Heartbeat.
        fuzz_decode_input(&[8, 0, 0, 0, 0, 0, 0, FRAME_END]);
        // Nested / garbage after valid frame.
        let mut multi = vec![8, 0, 0, 0, 0, 0, 0, FRAME_END];
        multi.extend_from_slice(&[1, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF]);
        fuzz_decode_input(&multi);
    }

    #[test]
    fn fuzz_roundtrip_payloads() {
        for size in [0usize, 1, 15, 256, 1024] {
            let payload: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let frame = Frame::method(7, payload);
            let encoded = frame.encode().expect("encode");
            let (decoded, n) = Frame::decode(&encoded).expect("decode");
            assert_eq!(n, encoded.len());
            assert_eq!(decoded, frame);
            // Encoded bytes must also be safe for the fuzz entry.
            fuzz_decode_input(&encoded);
        }
    }

    #[test]
    fn fuzz_bit_flips_on_valid_frame() {
        let frame = Frame::method(1, vec![0, 60, 0, 40]); // basic.publish-ish ids
        let encoded = frame.encode().expect("encode");
        for i in 0..encoded.len() {
            let mut mut_buf = encoded.clone();
            mut_buf[i] ^= 0xFF;
            fuzz_decode_input(&mut_buf);
            mut_buf[i] ^= 0x01;
            fuzz_decode_input(&mut_buf);
        }
    }

    /// Pseudo-random corpus for scripts/fuzz-frame-decode.sh (libFuzzer-free).
    ///
    /// Runs a bounded number of iterations using a simple LCG so CI stays
    /// deterministic and fast. The shell script can re-run this test in a loop
    /// for longer soak.
    #[test]
    fn fuzz_random_loop() {
        // xorshift64* seed fixed for reproducibility in `cargo test`.
        let mut state: u64 = 0xDECA_FBAD_F00D_1234;
        let iterations = std::env::var("QUEUEFORGE_FUZZ_ITERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2_000usize);
        let mut buf = vec![0u8; 512];
        for _ in 0..iterations {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = state as usize % 257;
            for b in buf.iter_mut().take(len) {
                state = state
                    .wrapping_mul(0x2545_F491_4F6C_DD1D)
                    .wrapping_add(0x9E37_79B9_7F4A_7C15);
                *b = (state >> 33) as u8;
            }
            fuzz_decode_input(&buf[..len]);
        }
    }
}
