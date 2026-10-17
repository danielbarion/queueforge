//! TLS for `amqps://` link URIs, with RabbitMQ's URI query parameters:
//! `cacertfile`, `certfile`, `keyfile`, `verify` (`verify_peer`, the
//! default, or `verify_none`) and `server_name_indication` (a host name, or
//! `disable`). With `verify_peer` and no `cacertfile` the system roots are
//! trusted.

use std::sync::Arc;
use std::time::Duration;

use lapin::tcp::{RustlsConnector, TcpStream};
use lapin::{Connection, ConnectionProperties};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A client TLS setup for one endpoint.
#[derive(Clone, Debug)]
pub(crate) struct Tls {
    pub config: Arc<ClientConfig>,
    /// The name the server certificate is checked against (and sent as SNI unless disabled).
    pub server_name: String,
}

/// The query parameters of a link URI.
fn query(uri: &str) -> Vec<(String, String)> {
    let Some((_, q)) = uri.split_once('?') else { return Vec::new() };
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (super::unpct(k), super::unpct(&v.replace('+', " ")))
        })
        .collect()
}

fn read_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let pem = std::fs::read(path).map_err(|e| format!("{path}: {e}"))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut pem.as_slice()).collect::<Result<_, _>>().map_err(|e| format!("{path}: {e}"))?;
    if certs.is_empty() {
        return Err(format!("{path}: no certificate"));
    }
    Ok(certs)
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The TLS setup an `amqps://` URI asks for. `host` is the URI's host.
pub(crate) fn from_uri(uri: &str, host: &str) -> Result<Tls, String> {
    let mut cacertfile = None;
    let mut certfile = None;
    let mut keyfile = None;
    let mut verify = true;
    let mut sni: Option<String> = None;
    for (k, v) in query(uri) {
        match k.as_str() {
            "cacertfile" => cacertfile = Some(v),
            "certfile" => certfile = Some(v),
            "keyfile" => keyfile = Some(v),
            "verify" => match v.as_str() {
                "verify_peer" => verify = true,
                "verify_none" => verify = false,
                other => return Err(format!("unknown verify mode '{other}'")),
            },
            "server_name_indication" => sni = Some(v),
            _ => {}
        }
    }
    let builder = ClientConfig::builder_with_provider(provider()).with_safe_default_protocol_versions().map_err(|e| e.to_string())?;
    let builder = if verify {
        let mut roots = RootCertStore::empty();
        match &cacertfile {
            Some(path) => {
                for cert in read_certs(path)? {
                    roots.add(cert).map_err(|e| format!("{path}: {e}"))?;
                }
            }
            None => {
                let native = rustls_native_certs::load_native_certs().map_err(|e| format!("system roots: {e}"))?;
                roots.add_parsable_certificates(native);
                if roots.is_empty() {
                    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                }
            }
        }
        builder.with_root_certificates(roots)
    } else {
        builder.dangerous().with_custom_certificate_verifier(Arc::new(NoVerify(provider())))
    };
    let mut config = match (&certfile, &keyfile) {
        (Some(cert), Some(key)) => {
            let chain = read_certs(cert)?;
            let pem = std::fs::read(key).map_err(|e| format!("{key}: {e}"))?;
            let key_der = rustls_pemfile::private_key(&mut pem.as_slice())
                .map_err(|e| format!("{key}: {e}"))?
                .ok_or_else(|| format!("{key}: no private key"))?;
            builder.with_client_auth_cert(chain, key_der).map_err(|e| format!("client certificate: {e}"))?
        }
        (Some(_), None) | (None, Some(_)) => return Err("certfile and keyfile go together".into()),
        (None, None) => builder.with_no_client_auth(),
    };
    let server_name = match sni.as_deref() {
        Some("disable") => {
            config.enable_sni = false;
            host.to_string()
        }
        Some(name) if !name.is_empty() => name.to_string(),
        _ => host.to_string(),
    };
    ServerName::try_from(server_name.clone()).map_err(|e| format!("server name '{server_name}': {e}"))?;
    Ok(Tls { config: Arc::new(config), server_name })
}

/// `verify_none`: any certificate is accepted; signatures are still checked.
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for NoVerify {
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
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Open an AMQP 0-9-1 connection over TLS to `host:port`.
pub(crate) async fn connect_091(uri: &str, host: &str, port: u16, tls: &Tls) -> Result<Connection, lapin::Error> {
    let parsed: lapin::uri::AMQPUri = uri.parse().map_err(|e: String| lapin::Error::IOError(Arc::new(std::io::Error::other(e))))?;
    let connector = RustlsConnector::from(Arc::clone(&tls.config));
    let addr = format!("{host}:{port}");
    let name = tls.server_name.clone();
    let dial = Box::new(move |_: &lapin::uri::AMQPUri| {
        let stream = TcpStream::connect_timeout(addr.as_str(), CONNECT_TIMEOUT)?;
        let stream = stream.into_rustls(&connector, &name)?;
        stream.set_nonblocking(true)?;
        Ok(stream)
    });
    Connection::connector(parsed, dial, ConnectionProperties::default()).await
}

/// Open a TLS stream for the AMQP 1.0 client.
pub(crate) async fn connect_stream(
    host: &str,
    port: u16,
    tls: &Tls,
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, String> {
    let tcp = tokio::time::timeout(CONNECT_TIMEOUT, tokio::net::TcpStream::connect((host, port)))
        .await
        .map_err(|_| "connect timed out".to_string())?
        .map_err(|e| format!("connect: {e}"))?;
    let _ = tcp.set_nodelay(true);
    let name = ServerName::try_from(tls.server_name.clone()).map_err(|e| e.to_string())?;
    let connector = tokio_rustls::TlsConnector::from(Arc::clone(&tls.config));
    tokio::time::timeout(CONNECT_TIMEOUT, connector.connect(name, tcp))
        .await
        .map_err(|_| "TLS handshake timed out".to_string())?
        .map_err(|e| format!("TLS handshake: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_rabbitmq_query_parameters() {
        let q = query("amqps://u:p@h:5671/v?cacertfile=%2Ftmp%2Fca.pem&verify=verify_none&server_name_indication=disable");
        assert_eq!(q[0], ("cacertfile".into(), "/tmp/ca.pem".into()));
        assert_eq!(q[1].1, "verify_none");
        let tls = from_uri("amqps://h?verify=verify_none&server_name_indication=disable", "h").unwrap();
        assert!(!tls.config.enable_sni);
        let tls = from_uri("amqps://h?verify=verify_none&server_name_indication=broker.example", "h").unwrap();
        assert_eq!(tls.server_name, "broker.example");
        assert!(from_uri("amqps://h?verify=sometimes", "h").is_err());
        assert!(from_uri("amqps://h?cacertfile=/no/such/file.pem", "h").is_err());
        assert!(from_uri("amqps://h?certfile=/x.pem", "h").is_err());
    }
}
