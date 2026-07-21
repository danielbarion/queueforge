//! rustls server configuration for AMQPS and HTTPS management.
//!
//! Loads a PEM certificate (chain) + private key and builds a
//! [`rustls::ServerConfig`]. AMQP keeps empty ALPN; management gets an
//! HTTPS-specific clone via [`with_https_alpn`].

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;

/// Errors while loading TLS material or building a server config.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// I/O failure reading cert/key files.
    #[error("tls io: {0}")]
    Io(#[from] std::io::Error),
    /// PEM parse or rustls configuration failure.
    #[error("tls config: {0}")]
    Config(String),
}

/// Ensure a process-level rustls [`CryptoProvider`] is installed.
///
/// Safe to call multiple times; only the first successful install wins.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        // ring is the workspace-selected provider (see root Cargo.toml).
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Load a rustls [`ServerConfig`] from PEM certificate and private-key paths.
///
/// - Certificate file may contain a single cert or a full chain (leaf first).
/// - Private key may be PKCS#8 (`BEGIN PRIVATE KEY`) or RSA (`BEGIN RSA PRIVATE KEY`).
/// - Client authentication is not requested (server-only TLS).
/// - **ALPN is empty** — suitable for raw AMQPS. For management HTTPS, pass a
///   clone through [`with_https_alpn`] (or let `queueforge-mgmt` fill ALPN).
pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<Arc<ServerConfig>, TlsError> {
    ensure_crypto_provider();

    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TlsError::Config(format!("invalid cert/key pair: {e}")))?;

    // Session tickets / resumption defaults from rustls are fine for v1.
    // Empty ALPN: AMQP over TLS is not HTTP. Do not share this Arc with HTTPS
    // without cloning via `with_https_alpn` first.
    config.alpn_protocols = Vec::new();

    Ok(Arc::new(config))
}

/// HTTP/2 ALPN identifier (`h2`).
pub const ALPN_H2: &[u8] = b"h2";
/// HTTP/1.1 ALPN identifier.
pub const ALPN_HTTP11: &[u8] = b"http/1.1";

/// Clone a server config and advertise `h2` + `http/1.1` ALPN for HTTPS.
///
/// Leaves the original (typically AMQPS) config's empty ALPN unchanged.
/// `RustlsConfig::from_config` does **not** inject ALPN itself — callers must
/// set it on the management clone.
pub fn with_https_alpn(config: Arc<ServerConfig>) -> Arc<ServerConfig> {
    let mut cfg = (*config).clone();
    cfg.alpn_protocols = vec![ALPN_H2.to_vec(), ALPN_HTTP11.to_vec()];
    Arc::new(cfg)
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = File::open(path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("failed to open cert {}: {e}", path.display()),
        )
    })?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse certs in {}: {e}", path.display()),
            )
        })?;
    if certs.is_empty() {
        return Err(TlsError::Config(format!(
            "no certificates found in {}",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = File::open(path).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("failed to open key {}: {e}", path.display()),
        )
    })?;
    let mut reader = BufReader::new(file);
    // Handles PKCS#8, PKCS#1 (RSA), and SEC1 (EC) PEM private keys.
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse private key in {}: {e}", path.display()),
            )
            .into()
        })
        .and_then(|key| {
            key.ok_or_else(|| {
                TlsError::Config(format!("no private key found in {}", path.display()))
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // Self-signed localhost cert (RSA-2048, CN=localhost) for unit tests only.
    const TEST_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUY7M5vDmE/0vVxiZwYMxg4N8mbFEwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDcyMDE5MTg1NFoXDTM2MDcx
NzE5MTg1NFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAouFaI2RhOLRdiNUHz3BW4CczNVYA0CzDsVwkZfhpLtXF
3Hq+6n7JmaUF1kX2og0r699RuDh5OM2cEY1Xignq2+ZUXZKrsMqVmWaLQqy20bW3
zsUPrBrEgdUY1eu8FNHQWvKgtuPyMoT0nAduZFEIePQu/VCTLj+pnv95lpITh7wo
6o8Jgj23GPc0aWOCdxkSqoS/uils62UGqTQIZFsLVc++0znxA5pHd3c0kQGwPMzr
zAeRyREIzDE4HrMvGLUBd3bm4nwmvGvhn+6tLM98cRrRUF/iz2d36pGUxzkiiBsW
Ei62uizwlx1tgnEejdBMQNSeC/dxBcCKWO1bkqu3BwIDAQABo1MwUTAdBgNVHQ4E
FgQUG7r3j8xu5o5RQ3IwCat3dHRtIPcwHwYDVR0jBBgwFoAUG7r3j8xu5o5RQ3Iw
Cat3dHRtIPcwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAW7mk
cr3Mcrn6Z4i1bQMRd/+0mXPSJbxSjdHW66rsSvyRVlJral0bT+EuXpaCem8jIfP2
NUjC3BjIXsVlYMmFAYZIYW442mLc5TzpfOuEiOm8zdeZpLH4Pn1zPa517hJGTYFC
hNblpqpEsouGpwXN/WQ5tzzxN2FwU1gOfXx7e9/YXT89BVuveDiw1Dr+TeCfCVY0
O9TnVSmojV6Zn9MMNgyV+sMsoiFRPwt2ew+wmgiS3m6ltk0WKTldZ7NsNPyYC3PY
2ESZYFdfT6V8n6ldnWA3deHjwypsecVPhJ+3mgTioWG7ma7gKQRP1kCYdWHpwunY
5FpAiVzyishHqH3IfA==
-----END CERTIFICATE-----
"#;

    const TEST_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCi4VojZGE4tF2I
1QfPcFbgJzM1VgDQLMOxXCRl+Gku1cXcer7qfsmZpQXWRfaiDSvr31G4OHk4zZwR
jVeKCerb5lRdkquwypWZZotCrLbRtbfOxQ+sGsSB1RjV67wU0dBa8qC24/IyhPSc
B25kUQh49C79UJMuP6me/3mWkhOHvCjqjwmCPbcY9zRpY4J3GRKqhL+6KWzrZQap
NAhkWwtVz77TOfEDmkd3dzSRAbA8zOvMB5HJEQjMMTgesy8YtQF3dubifCa8a+Gf
7q0sz3xxGtFQX+LPZ3fqkZTHOSKIGxYSLra6LPCXHW2CcR6N0ExA1J4L93EFwIpY
7VuSq7cHAgMBAAECggEAAr7VrB8MXHj90p6fS78oV7g8GWa3th/rCdmfFhuXz52e
1ivjfURh1YsojAdl3tll/Mso2iK+4wGO4zovWDj5PSJRbbpZK9kI/dWdUfm4oymd
ot3uI7KCeXBuw2b+e+4FcGMCk9KdHyjf4/lkF6DItHE5PDUte0FrdINN41yBo8S1
6XsKdKqMpAPz99F3Pt7/GjRfNvJAjudEaXomXOm72hXTJfpj3/0J3vuKAtw91Fxw
PD/jXwRIzfnDd9rPDOZAKNoCATowSvheA2cAvuOrLAh1pq5VirD6MtkDVxpBkjkW
kqeijuegsMo91+EXv1/7ouD4ltCJmJwMLCkYn9at8QKBgQDgt7Ktnj/sL75GN4pd
OtDbJTE+YJw0q5pZSRzckrLIDzzzL8x+ZHOzL2lpybAyjm1f3X0VADlPeyxwPZBB
spd9vGY4H7uvGEqE4F9Mbi4q91lR382QXHLiIUaYHebpWXA0xqDW5CrpfhnHg5HA
BpskxhALV8K9UAphr2gwBQ3IuwKBgQC5jfJ3dXLAlVkRyCZNLAEAFl2HEdWjgrqd
IqAXfPiT9u2piloD6i239xSDninvQCfKMFPiO7aMc82ZnnwELWw3dpWo6aPXn6bG
A3UUS0eyQmCMJzB5dBIyVNTA/8cs3baXgWzXDyDdwB4TMkHepsmLjD1bmTeKz7/d
h2Q8LTfcJQKBgEkVNTUl3GAx/s3TTlqXwEklRWil/udaT+5tyscppp9N5WKpzvXk
MYS7DKts/rLSg1vEKuPjmL/yrTcrrnjPXll0JkJmf6GoYsPoPNYcl0M+AnyQLsie
aHaGn/Dk+K43ejiPyMtalWIusq+iaIptG5PQHnOx7RGosFeotle3rQ31AoGAU4jT
33PAdXLG5np0w8lLqf5nnKcqxrHT0WoFKI3aWsKPvAPNAnYqnuddFOPffRYk06Fu
Iis/w3te1AnFSxwn29BHEAQe/rOhIQPtcXVykY3QaUg7SnI2vvHx1fFQeaJW0V4y
4Z6t7SbQY1P803/CvFAmT1Zq6tMcTV7mgTDaNQECgYAMQP6rwWcRojNdI5idyV4h
OEWtpKpGHAbaSlJ+zCcXL+vPCawZkdjUUtB2IemY/ZwAogM4BYoAMGOMyLbwrKwy
RFtnw3/3TqxtflqiJoKVJmjSAP+S6Ne6/H3fcQ9R2sCCdDhFhK3WZzz/jY/woPic
5EHEIGqwzazNCRxcXndi6Q==
-----END PRIVATE KEY-----
"#;

    fn write_temp_pair() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cert = dir.path().join("server.crt");
        let key = dir.path().join("server.key");
        let mut c = File::create(&cert).unwrap();
        c.write_all(TEST_CERT_PEM.as_bytes()).unwrap();
        let mut k = File::create(&key).unwrap();
        k.write_all(TEST_KEY_PEM.as_bytes()).unwrap();
        (dir, cert, key)
    }

    #[test]
    fn load_server_config_from_pem_pair() {
        let (_dir, cert, key) = write_temp_pair();
        let cfg = load_server_config(&cert, &key).expect("load rustls ServerConfig");
        // Base config is for AMQPS: empty ALPN, no client auth.
        assert!(
            cfg.alpn_protocols.is_empty(),
            "AMQP ServerConfig must not advertise HTTP ALPN"
        );
    }

    #[test]
    fn https_alpn_clone_is_independent_of_amqp_config() {
        let (_dir, cert, key) = write_temp_pair();
        let amqp = load_server_config(&cert, &key).expect("load");
        let https = with_https_alpn(Arc::clone(&amqp));

        assert!(
            amqp.alpn_protocols.is_empty(),
            "AMQP config must stay empty after clone"
        );
        assert_eq!(
            https.alpn_protocols,
            vec![ALPN_H2.to_vec(), ALPN_HTTP11.to_vec()],
            "HTTPS clone must advertise h2 then http/1.1"
        );
    }

    #[test]
    fn load_server_config_missing_cert_errors() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("missing.crt");
        let key = dir.path().join("server.key");
        std::fs::write(&key, TEST_KEY_PEM).unwrap();
        let err = load_server_config(&cert, &key).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("failed to open cert") || msg.contains("No such file"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn load_server_config_empty_cert_errors() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("empty.crt");
        let key = dir.path().join("server.key");
        std::fs::write(&cert, "").unwrap();
        std::fs::write(&key, TEST_KEY_PEM).unwrap();
        let err = load_server_config(&cert, &key).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no certificates") || msg.contains("tls"),
            "unexpected: {msg}"
        );
    }
}
