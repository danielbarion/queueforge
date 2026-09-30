//! Automated TLS smoke: AMQPS handshake + HTTPS health/login with Secure cookie.
//!
//! Uses a self-signed PEM pair (same fixture as unit tests) and rustls clients
//! that accept the lab certificate.

use std::sync::Arc;
use std::time::Duration;

use queueforge_auth::{AuthService, BootstrapMode, DEV_BOOTSTRAP_PASSWORD, DEV_BOOTSTRAP_USER};
use queueforge_broker::{
    load_server_config, start_amqp_listener_with_limits, with_https_alpn, ConnectionParams,
};
use queueforge_core::{MemoryTracker, QueueMetaStore, QueueRegistry};
use queueforge_metrics::ReadyFlag;
use queueforge_mgmt::{
    start_server as start_mgmt_server, ConnectionTracker, MgmtConfig, MgmtState,
    SESSION_COOKIE_NAME,
};
use queueforge_store::MetadataStore;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::ClientConfig;
use rustls::DigitallySignedStruct;
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Self-signed localhost cert (RSA-2048, CN=localhost) — test fixture only.
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

/// Accept any server cert (lab self-signed only).
#[derive(Debug)]
struct AcceptAnyCert;

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn write_pem_pair(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert = dir.join("server.crt");
    let key = dir.join("server.key");
    std::fs::write(&cert, TEST_CERT_PEM).unwrap();
    std::fs::write(&key, TEST_KEY_PEM).unwrap();
    (cert, key)
}

fn install_ring() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[tokio::test]
async fn amqps_tls_handshake_succeeds() {
    install_ring();
    let dir = TempDir::new().unwrap();
    let (cert, key) = write_pem_pair(dir.path());
    let tls = load_server_config(&cert, &key).expect("load server tls");
    assert!(
        tls.alpn_protocols.is_empty(),
        "AMQPS config must not advertise HTTP ALPN"
    );

    let store = MetadataStore::open(dir.path().join("data")).unwrap();
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .unwrap());
    let store = Arc::new(store);
    let queues = QueueRegistry::shared(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    );
    let connections = ConnectionTracker::shared();

    let router = Arc::new(store.bootstrap_router().expect("router"));
    let listener = start_amqp_listener_with_limits(
        "127.0.0.1:0".parse().unwrap(),
        store,
        queues,
        router,
        connections,
        ConnectionParams::default(),
        None,
        Some(tls),
        None,
    )
    .await
    .expect("bind AMQPS");
    assert!(listener.tls);
    let port = listener.local_addr.port();

    // Client TLS handshake only (full AMQP optional; proves acceptor works).
    let mut client_cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert))
        .with_no_client_auth();
    // AMQP has no ALPN; leave client ALPN empty.
    client_cfg.alpn_protocols.clear();
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let tcp = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    let _tls_stream = tokio::time::timeout(Duration::from_secs(5), connector.connect(name, tcp))
        .await
        .expect("handshake timed out")
        .expect("TLS handshake");

    listener.abort();
}

#[tokio::test]
async fn https_health_and_login_sets_secure_cookie() {
    install_ring();
    let dir = TempDir::new().unwrap();
    let (cert, key) = write_pem_pair(dir.path());
    let amqp_tls = load_server_config(&cert, &key).expect("load server tls");
    let https_tls = with_https_alpn(Arc::clone(&amqp_tls));
    assert!(
        !https_tls.alpn_protocols.is_empty(),
        "HTTPS config must advertise ALPN"
    );

    let store = MetadataStore::open(dir.path().join("data")).unwrap();
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .unwrap());
    let store = Arc::new(store);
    let queues = QueueRegistry::shared(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    );
    let connections = ConnectionTracker::shared();
    let ready = ReadyFlag::new();
    ready.set_ready(true);

    let router = Arc::new(store.bootstrap_router().expect("router"));
    let state = MgmtState::new(
        store,
        queues,
        router,
        connections,
        ready,
        MgmtConfig {
            cookie_secure: true,
            product_version: "0.1.0-test".into(),
            trusted_proxy_cidrs: Vec::new(),
        },
    );

    let server = start_mgmt_server("127.0.0.1:0".parse().unwrap(), state, Some(https_tls))
        .await
        .expect("bind HTTPS");
    assert!(server.tls);
    let port = server.local_addr.port();
    let base = format!("https://127.0.0.1:{port}");

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Health over HTTPS.
    let res = client
        .get(format!("{base}/healthz"))
        .send()
        .await
        .expect("healthz");
    assert_eq!(res.status(), 200);
    assert_eq!(res.text().await.unwrap().trim(), "ok");

    // Login → Secure cookie.
    let res = client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({
            "username": DEV_BOOTSTRAP_USER,
            "password": DEV_BOOTSTRAP_PASSWORD,
        }))
        .send()
        .await
        .expect("login");
    assert_eq!(res.status(), 200);

    let set_cookie = res
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find(|s| s.contains(SESSION_COOKIE_NAME))
        .expect("session Set-Cookie")
        .to_string();
    assert!(
        set_cookie
            .split(';')
            .any(|p| p.trim().eq_ignore_ascii_case("Secure")),
        "TLS login cookie must be Secure: {set_cookie}"
    );
    assert!(set_cookie.contains("HttpOnly"));

    server.abort();
}
