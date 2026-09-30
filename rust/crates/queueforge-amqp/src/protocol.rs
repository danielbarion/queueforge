//! AMQP 0-9-1 protocol header.

use crate::error::{Error, Result};

/// AMQP 0-9-1 protocol header: `"AMQP" + 0 + 0 + 9 + 1`.
///
/// Clients send this as the first 8 octets on a new TCP connection. The broker
/// responds with a `connection.start` method frame (not handled here).
pub const PROTOCOL_HEADER: [u8; 8] = *b"AMQP\0\0\x09\x01";

/// Length of [`PROTOCOL_HEADER`] in bytes.
pub const PROTOCOL_HEADER_LEN: usize = 8;

/// Encode the AMQP 0-9-1 protocol header into `buf`, appending 8 bytes.
pub fn encode_protocol_header(buf: &mut Vec<u8>) {
    buf.extend_from_slice(&PROTOCOL_HEADER);
}

/// Decode and validate an AMQP 0-9-1 protocol header from `buf`.
///
/// Returns the number of bytes consumed (`8`) on success.
pub fn decode_protocol_header(buf: &[u8]) -> Result<usize> {
    if buf.len() < PROTOCOL_HEADER_LEN {
        return Err(Error::Incomplete {
            need: PROTOCOL_HEADER_LEN - buf.len(),
            have: buf.len(),
        });
    }
    if buf[..PROTOCOL_HEADER_LEN] != PROTOCOL_HEADER {
        return Err(Error::InvalidProtocolHeader);
    }
    Ok(PROTOCOL_HEADER_LEN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_header_bytes() {
        assert_eq!(&PROTOCOL_HEADER, b"AMQP\0\0\x09\x01");
        assert_eq!(PROTOCOL_HEADER.len(), 8);
        assert_eq!(PROTOCOL_HEADER[0], b'A');
        assert_eq!(PROTOCOL_HEADER[1], b'M');
        assert_eq!(PROTOCOL_HEADER[2], b'Q');
        assert_eq!(PROTOCOL_HEADER[3], b'P');
        assert_eq!(PROTOCOL_HEADER[4], 0);
        assert_eq!(PROTOCOL_HEADER[5], 0);
        assert_eq!(PROTOCOL_HEADER[6], 9);
        assert_eq!(PROTOCOL_HEADER[7], 1);
    }

    #[test]
    fn protocol_header_roundtrip() {
        let mut buf = Vec::new();
        encode_protocol_header(&mut buf);
        assert_eq!(buf, PROTOCOL_HEADER);
        assert_eq!(decode_protocol_header(&buf).unwrap(), 8);
    }

    #[test]
    fn protocol_header_incomplete() {
        let err = decode_protocol_header(b"AMQP").unwrap_err();
        assert!(matches!(err, Error::Incomplete { need: 4, have: 4 }));
    }

    #[test]
    fn protocol_header_invalid() {
        let bad = b"AMQP\0\0\x09\x02";
        assert!(matches!(
            decode_protocol_header(bad),
            Err(Error::InvalidProtocolHeader)
        ));
        let bad2 = b"XXXX\0\0\x09\x01";
        assert!(matches!(
            decode_protocol_header(bad2),
            Err(Error::InvalidProtocolHeader)
        ));
    }
}
