#![no_main]
//! cargo-fuzz target wrapping [`queueforge_amqp::fuzz_decode_input`].
//!
//! Build/run (nightly + cargo-fuzz):
//!   cargo install cargo-fuzz
//!   cd crates/queueforge-amqp && cargo +nightly fuzz run frame_decode

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    queueforge_amqp::fuzz_decode_input(data);
});
