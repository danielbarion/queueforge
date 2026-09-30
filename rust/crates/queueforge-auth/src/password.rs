//! Argon2id password hashing (design Security section params).

use std::sync::OnceLock;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::Engine;
use rand_core::OsRng;
use sha2::{Digest, Sha256, Sha512};

use crate::error::{AuthError, Result};

/// Minimum password length in Unicode scalar values (design: min 12; reject empty).
pub const MIN_PASSWORD_LEN: usize = 12;

/// Maximum password length in **bytes** (DoS guard before Argon2 work).
pub const MAX_PASSWORD_BYTES: usize = 1024;

/// Argon2 memory cost in KiB (~19 MiB). Design: `memory_cost = 19456`.
pub const ARGON2_MEMORY_KIB: u32 = 19_456;

/// Argon2 time cost (iterations). Design: `time_cost = 2`.
pub const ARGON2_TIME_COST: u32 = 2;

/// Argon2 parallelism. Design: `parallelism = 1`.
pub const ARGON2_PARALLELISM: u32 = 1;

/// Build the Argon2id hasher with design-doc parameters.
pub fn argon2_hasher() -> Result<Argon2<'static>> {
    let params = Params::new(
        ARGON2_MEMORY_KIB,
        ARGON2_TIME_COST,
        ARGON2_PARALLELISM,
        None,
    )
    .map_err(|e| AuthError::PasswordHash(e.to_string()))?;
    Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
}

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

/// Hash a password with Argon2id; returns a PHC-encoded string.
///
/// Enforces the password policy before hashing.
pub fn hash_password(password: &str) -> Result<String> {
    validate_password_policy(password)?;
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = argon2_hasher()?;
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| AuthError::PasswordHash(e.to_string()))?;
    Ok(hash.to_string())
}

/// Verify a plaintext password against a PHC-encoded Argon2id hash.
///
/// Returns `true` if the password matches. Uses the parameters embedded in the
/// hash (not only the current defaults) so older hashes remain verifiable after
/// param rotation.
///
/// Oversized passwords return `Ok(false)` without running Argon2 (DoS guard).
pub fn verify_password(password: &str, password_hash: &str) -> Result<bool> {
    if password.len() > MAX_PASSWORD_BYTES {
        return Ok(false);
    }
    if !password_hash.starts_with('$') {
        return Ok(rabbit_password_hash_matches(password, password_hash));
    }
    let parsed = PasswordHash::new(password_hash)
        .map_err(|e| AuthError::PasswordHash(format!("invalid stored hash: {e}")))?;
    // Use a default Argon2 instance for verification — params come from the PHC string.
    let argon2 = Argon2::default();
    match argon2.verify_password(password.as_bytes(), &parsed) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::Password) => Ok(false),
        Err(e) => Err(AuthError::PasswordHash(e.to_string())),
    }
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

/// Dummy PHC hash used when authenticating an unknown username so the Argon2
/// cost is comparable to a real verify (mitigates online username enumeration).
///
/// Generated once per process with design params; salt is random per process.
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

    #[test]
    fn hash_and_verify_roundtrip() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(verify_password("correct horse battery staple", &hash).unwrap());
        assert!(!verify_password("wrong password!!", &hash).unwrap());
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
        assert!(hash_password("exactly12chr").is_ok());
    }

    #[test]
    fn rejects_oversized_password() {
        let big = "a".repeat(MAX_PASSWORD_BYTES + 1);
        assert!(matches!(
            hash_password(&big),
            Err(AuthError::PasswordPolicy(_))
        ));
        // verify short-circuits without error
        assert!(!verify_password(&big, dummy_password_hash()).unwrap());
    }

    #[test]
    fn uses_design_params() {
        let hash = hash_password("twelve chars!").unwrap();
        // PHC: $argon2id$v=19$m=19456,t=2,p=1$...
        assert!(
            hash.contains("m=19456") && hash.contains("t=2") && hash.contains("p=1"),
            "unexpected PHC params in {hash}"
        );
    }

    #[test]
    fn dummy_hash_is_valid_argon2id() {
        let h = dummy_password_hash();
        assert!(h.starts_with("$argon2id$"));
        assert!(h.contains("m=19456"));
        // Not the real dummy password from the outside.
        assert!(!verify_password("unrelated-password-xx", h).unwrap());
    }
}
