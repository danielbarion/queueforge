//! Password hashing.
//!
//! Passwords are the RabbitMQ SHA-256 form: base64(4-byte salt || SHA-256).
//! A SHA-512 password-hash still verifies. An argon2 PHC string does not.

use std::sync::OnceLock;

use base64::Engine;
use rand_core::{OsRng, RngCore};
use sha2::{Digest, Sha256, Sha512};

use crate::error::{AuthError, Result};

/// Minimum password length in Unicode scalar values.
pub const MIN_PASSWORD_LEN: usize = 8;

/// Maximum password length in **bytes** (DoS guard before hashing).
pub const MAX_PASSWORD_BYTES: usize = 1024;

/// Validate password policy: non-empty, min length, max bytes.
pub fn validate_password_policy(password: &str) -> Result<()> {
    if password.is_empty() {
        return Err(AuthError::PasswordPolicy(
            "password must not be empty".into(),
        ));
    }
    if password.len() > MAX_PASSWORD_BYTES {
        return Err(AuthError::PasswordPolicy(format!(
            "password must be at most {MAX_PASSWORD_BYTES} bytes"
        )));
    }
    if password.chars().count() < MIN_PASSWORD_LEN {
        return Err(AuthError::PasswordPolicy(format!(
            "password must be at least {MIN_PASSWORD_LEN} characters"
        )));
    }
    Ok(())
}

/// Hash a password as base64(4-byte salt || SHA-256(salt || password)).
///
/// Enforces the password policy before hashing. This is the RabbitMQ SHA-256
/// password-hash, which both brokers verify.
pub fn hash_password(password: &str) -> Result<String> {
    validate_password_policy(password)?;
    let mut salt = [0u8; 4];
    OsRng.fill_bytes(&mut salt);
    Ok(rabbit_sha256_with_salt(password, salt))
}

/// RabbitMQ SHA-256 password-hash for a caller-supplied 4-byte salt.
pub fn rabbit_sha256_with_salt(password: &str, salt: [u8; 4]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(password.as_bytes());
    let digest = hasher.finalize();
    let mut raw = [0u8; 36];
    raw[..4].copy_from_slice(&salt);
    raw[4..].copy_from_slice(&digest);
    base64::engine::general_purpose::STANDARD.encode(raw)
}

/// Verify a plaintext password against a stored RabbitMQ password-hash.
///
/// Oversized passwords return `Ok(false)`.
pub fn verify_password(password: &str, password_hash: &str) -> Result<bool> {
    if password.len() > MAX_PASSWORD_BYTES {
        return Ok(false);
    }
    Ok(rabbit_password_hash_matches(password, password_hash))
}

/// RabbitMQ definitions store `password_hash` as base64(salt[4] || digest).
/// SHA-256 digests are 32 bytes and SHA-512 digests are 64 bytes.
fn rabbit_password_hash_matches(password: &str, encoded: &str) -> bool {
    let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return false;
    };
    if raw.len() < 5 {
        return false;
    }
    let (salt, digest) = raw.split_at(4);
    let computed: Vec<u8> = match digest.len() {
        32 => Sha256::digest([salt, password.as_bytes()].concat()).to_vec(),
        64 => Sha512::digest([salt, password.as_bytes()].concat()).to_vec(),
        _ => return false,
    };
    if computed.len() != digest.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in computed.iter().zip(digest.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Dummy RabbitMQ SHA-256 hash for an unknown username.
///
/// The unknown-user path does the same cheap compare as a real login.
pub fn dummy_password_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        hash_password("queueforge-dummy-timing-pad")
            .expect("dummy password hash with design params")
    })
    .as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed salt shared with the Bun verifier tests.
    const CROSS_SALT: [u8; 4] = [0x10, 0x22, 0x33, 0x44];
    const CROSS_HASH: &str = "ECIzRClc/+1u2ev0HwDpqq+CC4ixd405UlwH0cCTvqGs8avG";
    /// Old argon2id PHC. QueueForge does not verify this form.
    const ARGON2_FIXTURE: &str = "$argon2id$v=19$m=19456,t=2,p=1$WbkrTzkiA7IX96DqFx4oyqY4ycwq51Qc2ernvzYq2H8$E44EIl5HVbAgkCuygMc0dOWT79GeatS2lEh4vIWdbvQ";

    #[test]
    fn hash_and_verify_roundtrip() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(!hash.starts_with('$'), "{hash}");
        let raw = base64::engine::general_purpose::STANDARD
            .decode(&hash)
            .unwrap();
        assert_eq!(raw.len(), 36, "4-byte salt || SHA-256");
        assert!(verify_password("correct horse battery staple", &hash).unwrap());
        assert!(!verify_password("wrong password!!", &hash).unwrap());
    }

    #[test]
    fn fixed_salt_matches_bun_vector() {
        assert_eq!(
            rabbit_sha256_with_salt("devpassword12", CROSS_SALT),
            CROSS_HASH
        );
        assert!(verify_password("devpassword12", CROSS_HASH).unwrap());
        assert!(!verify_password("wrong-password", CROSS_HASH).unwrap());
    }

    #[test]
    fn argon2id_phc_does_not_verify() {
        assert!(!verify_password("argon-fixture-pw", ARGON2_FIXTURE).unwrap());
    }

    #[test]
    fn verifies_rabbitmq_sha256_password_hash() {
        let hash = "XDVnyR80sAgu/NQdMe51rAF4zxAm+3LIbszQsGNTcTWuz2cK";
        assert!(verify_password("quorum-password", hash).unwrap());
        assert!(!verify_password("wrong-password", hash).unwrap());
    }

    #[test]
    fn rejects_short_and_empty_passwords() {
        assert!(matches!(
            hash_password(""),
            Err(AuthError::PasswordPolicy(_))
        ));
        assert!(matches!(
            hash_password("short"),
            Err(AuthError::PasswordPolicy(_))
        ));
        assert!(matches!(
            hash_password("1234567"),
            Err(AuthError::PasswordPolicy(_))
        ));
        assert!(hash_password("12345678").is_ok());
    }

    #[test]
    fn rejects_oversized_password() {
        let big = "a".repeat(MAX_PASSWORD_BYTES + 1);
        assert!(matches!(
            hash_password(&big),
            Err(AuthError::PasswordPolicy(_))
        ));
        assert!(!verify_password(&big, dummy_password_hash()).unwrap());
    }

    #[test]
    fn dummy_hash_is_sha256() {
        let h = dummy_password_hash();
        assert!(!h.starts_with('$'), "{h}");
        let raw = base64::engine::general_purpose::STANDARD.decode(h).unwrap();
        assert_eq!(raw.len(), 36);
        assert!(!verify_password("unrelated-password-xx", h).unwrap());
    }
}
