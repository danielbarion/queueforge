//! Unit tests for connection helpers.

use super::helpers::*;
use super::FRAME_MAX_FLOOR;

#[test]
fn plain_response_forms() {
    let (u, p) = parse_plain_response(b"\0admin\0secret").unwrap();
    assert_eq!(u, "admin");
    assert_eq!(p, "secret");

    let (u, p) = parse_plain_response(b"authz\0bob\0pw").unwrap();
    assert_eq!(u, "bob");
    assert_eq!(p, "pw");

    let (u, p) = parse_plain_response(b"bob\0pw").unwrap();
    assert_eq!(u, "bob");
    assert_eq!(p, "pw");

    assert!(parse_plain_response(b"").is_err());
    assert!(parse_plain_response(b"\0\0").is_err());
}

#[test]
fn tune_negotiation() {
    assert_eq!(negotiate_channel_max(2047, 0), 2047);
    assert_eq!(negotiate_channel_max(2047, 100), 100);
    assert_eq!(negotiate_channel_max(100, 2047), 100);
    assert_eq!(negotiate_frame_max(131_072, 4096), 4096);
    assert_eq!(negotiate_frame_max(131_072, 0), 131_072);
    assert_eq!(negotiate_frame_max(100, 100), FRAME_MAX_FLOOR); // floor
    assert_eq!(negotiate_heartbeat(60, 30), 30);
    // Issue 1: either side 0 disables heartbeats.
    assert_eq!(negotiate_heartbeat(60, 0), 0);
    assert_eq!(negotiate_heartbeat(0, 30), 0);
    assert_eq!(negotiate_heartbeat(0, 0), 0);
    assert_eq!(negotiate_heartbeat(10, 20), 10);
}
