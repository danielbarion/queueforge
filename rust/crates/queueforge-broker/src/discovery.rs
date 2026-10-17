//! Cloud peer discovery: `[cluster] discovery = "k8s" | "consul" | "etcd" | "aws"`
//! (`dns` is resolved in `queueforge_core::config`).
//!
//! Each backend answers one question, the hosts of the cluster's nodes. They
//! become members `<ip>:<port>` with the cluster port of `dns_port` or this
//! node's listen port, as DNS discovery's do. The members are resolved at
//! startup and re-checked every few seconds into `members.json`, which the
//! cluster's dial loop reloads. Consul and etcd also register this node and
//! keep the registration alive (a TTL check, a lease); it is removed again
//! on shutdown.
//!
//! The HTTP client is a small HTTP/1.1 one (TLS with a configured CA for
//! the Kubernetes API), so the backends run against plain test servers.

use std::collections::BTreeSet;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use queueforge_core::config::ClusterConfig;
use queueforge_core::ClusterMember;
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

const TIMEOUT: Duration = Duration::from_secs(5);
const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// The backends this module serves.
pub fn is_cloud(discovery: Option<&str>) -> bool {
    matches!(discovery, Some("k8s" | "consul" | "etcd" | "aws"))
}

// ── HTTP ────────────────────────────────────────────────────────────────

/// A response: status code and body.
#[derive(Debug)]
pub struct Response {
    /// HTTP status code.
    pub status: u16,
    /// Body, de-chunked.
    pub body: Vec<u8>,
}

impl Response {
    fn json(&self) -> Result<Value, String> {
        serde_json::from_slice(&self.body).map_err(|e| format!("bad JSON ({e}): {}", String::from_utf8_lossy(&self.body)))
    }

    fn ok(self, what: &str) -> Result<Self, String> {
        if (200..300).contains(&self.status) {
            Ok(self)
        } else {
            Err(format!("{what}: HTTP {} {}", self.status, String::from_utf8_lossy(&self.body)))
        }
    }
}

/// One HTTP/1.1 request. `ca` (a PEM file) is the trust root for https.
pub async fn request(
    method: &str,
    url: &str,
    headers: &[(&str, String)],
    body: &[u8],
    ca: Option<&str>,
) -> Result<Response, String> {
    let (tls, rest) = match url.split_once("://") {
        Some(("https", r)) => (true, r),
        Some(("http", r)) => (false, r),
        _ => return Err(format!("unsupported URL {url}")),
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !h.ends_with(']') || h.starts_with('[') => (h.trim_matches(['[', ']']), p.parse::<u16>().map_err(|_| format!("bad port in {url}"))?),
        _ => (authority, if tls { 443 } else { 80 }),
    };
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: {}\r\n", body.len());
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    let mut raw = req.into_bytes();
    raw.extend_from_slice(body);
    let tcp = tokio::time::timeout(TIMEOUT, TcpStream::connect((host, port)))
        .await
        .map_err(|_| format!("{url}: connect timed out"))?
        .map_err(|e| format!("{url}: {e}"))?;
    let reply = if tls {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(ca) = ca {
            let pem = std::fs::read(ca).map_err(|e| format!("{ca}: {e}"))?;
            for cert in rustls_pemfile::certs(&mut pem.as_slice()).flatten() {
                let _ = roots.add(cert);
            }
        }
        let config = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|e| e.to_string())?;
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await
            .map_err(|e| format!("{url}: TLS {e}"))?;
        exchange(stream, &raw).await?
    } else {
        exchange(tcp, &raw).await?
    };
    parse_response(&reply)
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, req: &[u8]) -> Result<Vec<u8>, String> {
    s.write_all(req).await.map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    tokio::time::timeout(TIMEOUT, s.read_to_end(&mut out)).await.map_err(|_| "HTTP read timed out".to_string())?.map_err(|e| e.to_string())?;
    Ok(out)
}

fn parse_response(raw: &[u8]) -> Result<Response, String> {
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").ok_or("malformed HTTP response")?;
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let status = head.split_whitespace().nth(1).and_then(|s| s.parse::<u16>().ok()).ok_or("malformed status line")?;
    let lower = head.to_ascii_lowercase();
    let mut body = raw[split + 4..].to_vec();
    if lower.contains("transfer-encoding: chunked") {
        let mut out = Vec::new();
        let mut b = body.as_slice();
        while let Some(end) = b.windows(2).position(|w| w == b"\r\n") {
            let size = usize::from_str_radix(String::from_utf8_lossy(&b[..end]).split(';').next().unwrap_or("0").trim(), 16).unwrap_or(0);
            if size == 0 || b.len() < end + 2 + size {
                break;
            }
            out.extend_from_slice(&b[end + 2..end + 2 + size]);
            b = &b[(end + 4 + size).min(b.len())..];
        }
        body = out;
    }
    Ok(Response { status, body })
}

// ── Members ─────────────────────────────────────────────────────────────

/// Members from discovered hosts (IPs or names), each `<ip>:<port>`.
pub fn members_from_hosts<'a>(hosts: impl IntoIterator<Item = &'a str>, port: u16) -> Vec<ClusterMember> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for host in hosts {
        let addr: Option<SocketAddr> = match host.parse::<std::net::IpAddr>() {
            Ok(ip) => Some(SocketAddr::new(ip, port)),
            Err(_) => (host, port).to_socket_addrs().ok().and_then(|mut a| a.next()),
        };
        if let Some(addr) = addr {
            if seen.insert(addr.to_string()) {
                out.push(ClusterMember { id: addr.to_string(), addr });
            }
        }
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| fallback.to_string())
}

/// The address this node registers under (consul, etcd).
fn advertised_host(c: &ClusterConfig) -> String {
    if let Some(addr) = c.consul_svc_addr.clone().filter(|a| !a.is_empty()) {
        return addr;
    }
    match c.listen {
        Some(l) if !l.ip().is_unspecified() => l.ip().to_string(),
        _ => env_or("HOSTNAME", "127.0.0.1"),
    }
}

fn member_port(c: &ClusterConfig) -> u16 {
    c.dns_port.or(c.listen.map(|l| l.port())).unwrap_or(25672)
}

// ── Kubernetes ──────────────────────────────────────────────────────────

/// Hosts of the service's endpoints (ready and not ready).
pub async fn k8s_hosts(c: &ClusterConfig) -> Result<Vec<String>, String> {
    let scheme = c.k8s_scheme.clone().unwrap_or_else(|| "https".into());
    let host = c.k8s_host.clone().unwrap_or_else(|| env_or("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc"));
    let port = c.k8s_port.unwrap_or_else(|| env_or("KUBERNETES_SERVICE_PORT", "443").parse().unwrap_or(443));
    let token_path = c.k8s_token_path.clone().unwrap_or_else(|| format!("{SA_DIR}/token"));
    let ca_path = c.k8s_ca_path.clone().unwrap_or_else(|| format!("{SA_DIR}/ca.crt"));
    let ns_path = c.k8s_namespace_path.clone().unwrap_or_else(|| format!("{SA_DIR}/namespace"));
    let token = std::fs::read_to_string(&token_path).map_err(|e| format!("{token_path}: {e}"))?;
    let namespace = std::fs::read_to_string(&ns_path).map_err(|e| format!("{ns_path}: {e}"))?;
    let service = c.k8s_service_name.clone().unwrap_or_else(|| "queueforge".into());
    let url = format!("{scheme}://{host}:{port}/api/v1/namespaces/{}/endpoints/{service}", namespace.trim());
    let ca = (scheme == "https").then_some(ca_path);
    let res = request("GET", &url, &[("Authorization", format!("Bearer {}", token.trim())), ("Accept", "application/json".into())], b"", ca.as_deref())
        .await?
        .ok("kubernetes endpoints")?;
    let doc = res.json()?;
    let by_name = c.k8s_address_type.as_deref() == Some("hostname");
    let suffix = c.k8s_hostname_suffix.clone().unwrap_or_default();
    let mut out = Vec::new();
    for subset in doc.get("subsets").and_then(Value::as_array).into_iter().flatten() {
        for key in ["addresses", "notReadyAddresses"] {
            for a in subset.get(key).and_then(Value::as_array).into_iter().flatten() {
                let host = if by_name {
                    a.get("hostname").and_then(Value::as_str).map(|h| format!("{h}{suffix}"))
                } else {
                    a.get("ip").and_then(Value::as_str).map(str::to_string)
                };
                out.extend(host);
            }
        }
    }
    Ok(out)
}

// ── Consul ──────────────────────────────────────────────────────────────

struct Consul {
    base: String,
    token: Option<String>,
    svc: String,
    id: String,
    addr: String,
    port: u16,
    ttl: u64,
}

impl Consul {
    fn new(c: &ClusterConfig) -> Self {
        let scheme = c.consul_scheme.clone().unwrap_or_else(|| "http".into());
        let host = c.consul_host.clone().unwrap_or_else(|| "localhost".into());
        let svc = c.consul_svc.clone().unwrap_or_else(|| "queueforge".into());
        let addr = advertised_host(c);
        let port = member_port(c);
        Consul {
            base: format!("{scheme}://{host}:{}", c.consul_port.unwrap_or(8500)),
            token: c.consul_acl_token.clone().filter(|t| !t.is_empty()),
            id: format!("{svc}:{addr}:{port}"),
            svc,
            addr,
            port,
            ttl: c.consul_svc_ttl.unwrap_or(30).max(1),
        }
    }

    fn headers(&self) -> Vec<(&'static str, String)> {
        let mut h = vec![("Content-Type", "application/json".to_string())];
        if let Some(t) = &self.token {
            h.push(("X-Consul-Token", t.clone()));
        }
        h
    }

    async fn register(&self) -> Result<(), String> {
        let body = json!({
            "ID": self.id,
            "Name": self.svc,
            "Address": self.addr,
            "Port": self.port,
            "Check": { "TTL": format!("{}s", self.ttl), "DeregisterCriticalServiceAfter": format!("{}s", self.ttl * 10) },
        });
        request("PUT", &format!("{}/v1/agent/service/register", self.base), &self.headers(), body.to_string().as_bytes(), None)
            .await?
            .ok("consul register")?;
        self.pass().await
    }

    async fn pass(&self) -> Result<(), String> {
        request("PUT", &format!("{}/v1/agent/check/pass/service:{}", self.base, self.id), &self.headers(), b"", None)
            .await?
            .ok("consul check pass")
            .map(|_| ())
    }

    async fn deregister(&self) -> Result<(), String> {
        request("PUT", &format!("{}/v1/agent/service/deregister/{}", self.base, self.id), &self.headers(), b"", None)
            .await?
            .ok("consul deregister")
            .map(|_| ())
    }

    /// Passing instances as `(host, port)`.
    async fn hosts(&self) -> Result<Vec<(String, Option<u16>)>, String> {
        let res = request("GET", &format!("{}/v1/health/service/{}?passing=true", self.base, self.svc), &self.headers(), b"", None)
            .await?
            .ok("consul health")?;
        let doc = res.json()?;
        let mut out = Vec::new();
        for entry in doc.as_array().into_iter().flatten() {
            let service = entry.get("Service");
            let addr = service
                .and_then(|s| s.get("Address"))
                .and_then(Value::as_str)
                .filter(|a| !a.is_empty())
                .or_else(|| entry.get("Node").and_then(|n| n.get("Address")).and_then(Value::as_str));
            let port = service.and_then(|s| s.get("Port")).and_then(Value::as_u64).map(|p| p as u16);
            if let Some(a) = addr {
                out.push((a.to_string(), port));
            }
        }
        Ok(out)
    }
}

// ── etcd ────────────────────────────────────────────────────────────────

struct Etcd {
    endpoints: Vec<String>,
    dir: String,
    me: String,
    ttl: u64,
    user: Option<(String, String)>,
}

fn b64(s: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(s)
}

fn unb64(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(s).unwrap_or_default()
}

/// The key after the last byte of `prefix`'s range, as etcd's `range_end`.
pub fn range_end(prefix: &str) -> Vec<u8> {
    let mut end = prefix.as_bytes().to_vec();
    while let Some(last) = end.pop() {
        if last < 0xff {
            end.push(last + 1);
            return end;
        }
    }
    vec![0]
}

impl Etcd {
    fn new(c: &ClusterConfig) -> Self {
        let prefix = c.etcd_key_prefix.clone().unwrap_or_else(|| "queueforge".into());
        let cluster = c.etcd_cluster_name.clone().unwrap_or_else(|| "default".into());
        let endpoints = if c.etcd_endpoints.is_empty() { vec!["http://127.0.0.1:2379".to_string()] } else { c.etcd_endpoints.clone() };
        Etcd {
            endpoints,
            dir: format!("/{}/discovery/{cluster}/nodes/", prefix.trim_matches('/')),
            me: format!("{}:{}", advertised_host(c), member_port(c)),
            ttl: c.etcd_node_ttl.unwrap_or(30).max(1),
            user: match (&c.etcd_username, &c.etcd_password) {
                (Some(u), Some(p)) => Some((u.clone(), p.clone())),
                _ => None,
            },
        }
    }

    /// POST a v3 gateway call to the first endpoint that answers.
    async fn call(&self, path: &str, body: Value) -> Result<Value, String> {
        let mut last = String::from("no etcd endpoint");
        for ep in &self.endpoints {
            let base = ep.trim_end_matches('/');
            let mut headers = vec![("Content-Type", "application/json".to_string())];
            if let Some((user, pass)) = &self.user {
                match request("POST", &format!("{base}/v3/auth/authenticate"), &headers, json!({"name": user, "password": pass}).to_string().as_bytes(), None).await {
                    Ok(r) => match r.ok("etcd authenticate").and_then(|r| r.json()) {
                        Ok(doc) => headers.push(("Authorization", doc.get("token").and_then(Value::as_str).unwrap_or_default().to_string())),
                        Err(e) => {
                            last = e;
                            continue;
                        }
                    },
                    Err(e) => {
                        last = e;
                        continue;
                    }
                }
            }
            match request("POST", &format!("{base}{path}"), &headers, body.to_string().as_bytes(), None).await {
                Ok(r) => return r.ok(path)?.json(),
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    /// Grant a lease and put this node's key under it. Returns the lease id.
    async fn register(&self) -> Result<String, String> {
        let lease = self.call("/v3/lease/grant", json!({ "TTL": self.ttl })).await?;
        let id = match lease.get("ID") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            _ => return Err("etcd lease grant: no ID".into()),
        };
        let key = format!("{}{}", self.dir, self.me);
        self.call("/v3/kv/put", json!({ "key": b64(key.as_bytes()), "value": b64(self.me.as_bytes()), "lease": id })).await?;
        Ok(id)
    }

    async fn keepalive(&self, lease: &str) -> Result<(), String> {
        self.call("/v3/lease/keepalive", json!({ "ID": lease })).await.map(|_| ())
    }

    async fn revoke(&self, lease: &str) -> Result<(), String> {
        self.call("/v3/lease/revoke", json!({ "ID": lease })).await.map(|_| ())
    }

    /// `host:port` of every registered node.
    async fn nodes(&self) -> Result<Vec<String>, String> {
        let doc = self
            .call("/v3/kv/range", json!({ "key": b64(self.dir.as_bytes()), "range_end": b64(&range_end(&self.dir)) }))
            .await?;
        let mut out = Vec::new();
        for kv in doc.get("kvs").and_then(Value::as_array).into_iter().flatten() {
            let key = String::from_utf8_lossy(&unb64(kv.get("key").and_then(Value::as_str).unwrap_or_default())).to_string();
            if let Some(node) = key.strip_prefix(&self.dir) {
                out.push(node.to_string());
            }
        }
        Ok(out)
    }
}

// ── AWS ─────────────────────────────────────────────────────────────────

/// Credentials for SigV4.
#[derive(Clone, Debug)]
pub struct AwsCredentials {
    /// Access key id.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
    /// Session token of temporary credentials.
    pub session_token: Option<String>,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn sha256(b: &[u8]) -> Vec<u8> {
    ring::digest::digest(&ring::digest::SHA256, b).as_ref().to_vec()
}

fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    ring::hmac::sign(&ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key), msg).as_ref().to_vec()
}

/// RFC 3986 encoding as SigV4 wants it (unreserved characters stay).
pub fn uri_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The SigV4 signing key for a date (`YYYYMMDD`).
pub fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, service.as_bytes());
    hmac(&k_service, b"aws4_request")
}

/// The `Authorization` header for one request (AWS Signature Version 4).
/// `headers` must include `host` and `x-amz-date`; `query` is already
/// encoded `k=v` pairs.
#[allow(clippy::too_many_arguments)]
pub fn sigv4_authorization(
    method: &str,
    path: &str,
    query: &[(String, String)],
    headers: &[(String, String)],
    payload: &[u8],
    creds: &AwsCredentials,
    region: &str,
    service: &str,
    amz_date: &str,
) -> String {
    let mut q: Vec<(String, String)> = query.iter().map(|(k, v)| (uri_encode(k), uri_encode(v))).collect();
    q.sort();
    let canonical_query = q.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let mut h: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.split_whitespace().collect::<Vec<_>>().join(" ")))
        .collect();
    h.sort();
    let canonical_headers: String = h.iter().map(|(k, v)| format!("{k}:{v}\n")).collect();
    let signed_headers = h.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(";");
    let canonical_request = format!("{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{}", hex(&sha256(payload)));
    let date = &amz_date[..8];
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}", hex(&sha256(canonical_request.as_bytes())));
    let signature = hex(&hmac(&signing_key(&creds.secret_key, date, region, service), to_sign.as_bytes()));
    format!("AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}", creds.access_key)
}

fn amz_date_now() -> String {
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0) as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// Instance metadata (IMDSv2) base, `AWS_EC2_METADATA_SERVICE_ENDPOINT` as the SDKs read it.
fn imds_base() -> String {
    env_or("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://169.254.169.254").trim_end_matches('/').to_string()
}

async fn imds_get(path: &str) -> Result<String, String> {
    let base = imds_base();
    let token = request("PUT", &format!("{base}/latest/api/token"), &[("X-aws-ec2-metadata-token-ttl-seconds", "21600".into())], b"", None)
        .await?
        .ok("IMDS token")?;
    let token = String::from_utf8_lossy(&token.body).trim().to_string();
    let res = request("GET", &format!("{base}{path}"), &[("X-aws-ec2-metadata-token", token)], b"", None).await?.ok("IMDS")?;
    Ok(String::from_utf8_lossy(&res.body).trim().to_string())
}

async fn aws_credentials(c: &ClusterConfig) -> Result<AwsCredentials, String> {
    let key = c.aws_access_key_id.clone().or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok()).filter(|k| !k.is_empty());
    let secret = c.aws_secret_access_key.clone().or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok()).filter(|k| !k.is_empty());
    if let (Some(access_key), Some(secret_key)) = (key, secret) {
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok().filter(|t| !t.is_empty());
        return Ok(AwsCredentials { access_key, secret_key, session_token });
    }
    // The instance role, from IMDSv2.
    let role = imds_get("/latest/meta-data/iam/security-credentials/").await?;
    let role = role.lines().next().unwrap_or_default().to_string();
    let doc: Value = serde_json::from_str(&imds_get(&format!("/latest/meta-data/iam/security-credentials/{role}")).await?).map_err(|e| e.to_string())?;
    let field = |k: &str| doc.get(k).and_then(Value::as_str).map(str::to_string);
    Ok(AwsCredentials {
        access_key: field("AccessKeyId").ok_or("instance role: no AccessKeyId")?,
        secret_key: field("SecretAccessKey").ok_or("instance role: no SecretAccessKey")?,
        session_token: field("Token"),
    })
}

/// Text of every `<tag>...</tag>` element in `xml`.
fn xml_values(xml: &str, tag: &str) -> Vec<String> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(after[..j].trim().to_string());
        rest = &after[j + close.len()..];
    }
    out
}

/// Addresses of the running instances that carry every configured tag.
pub async fn aws_hosts(c: &ClusterConfig) -> Result<Vec<String>, String> {
    let region = match c.aws_region.clone().or_else(|| std::env::var("AWS_REGION").ok()).filter(|r| !r.is_empty()) {
        Some(r) => r,
        None => imds_get("/latest/meta-data/placement/region").await?,
    };
    let creds = aws_credentials(c).await?;
    let endpoint = c.aws_endpoint.clone().unwrap_or_else(|| format!("https://ec2.{region}.amazonaws.com"));
    let endpoint = endpoint.trim_end_matches('/').to_string();
    let authority = endpoint.split_once("://").map(|(_, r)| r).unwrap_or(&endpoint).split('/').next().unwrap_or_default().to_string();
    let mut query: Vec<(String, String)> = vec![("Action".into(), "DescribeInstances".into()), ("Version".into(), "2016-11-15".into())];
    let mut n = 0;
    for (k, v) in &c.aws_ec2_tags {
        n += 1;
        query.push((format!("Filter.{n}.Name"), format!("tag:{k}")));
        query.push((format!("Filter.{n}.Value.1"), v.clone()));
    }
    n += 1;
    query.push((format!("Filter.{n}.Name"), "instance-state-name".into()));
    query.push((format!("Filter.{n}.Value.1"), "running".into()));
    let amz_date = amz_date_now();
    let mut headers = vec![("host".to_string(), authority), ("x-amz-date".to_string(), amz_date.clone())];
    if let Some(t) = &creds.session_token {
        headers.push(("x-amz-security-token".into(), t.clone()));
    }
    let auth = sigv4_authorization("GET", "/", &query, &headers, b"", &creds, &region, "ec2", &amz_date);
    let qs = query.iter().map(|(k, v)| format!("{}={}", uri_encode(k), uri_encode(v))).collect::<Vec<_>>().join("&");
    let mut send: Vec<(&str, String)> = vec![("X-Amz-Date", amz_date), ("Authorization", auth)];
    if let Some(t) = &creds.session_token {
        send.push(("X-Amz-Security-Token", t.clone()));
    }
    let res = request("GET", &format!("{endpoint}/?{qs}"), &send, b"", None).await?.ok("DescribeInstances")?;
    let xml = String::from_utf8_lossy(&res.body).to_string();
    // Each instance item: its first address field is the instance's own.
    let mut out = Vec::new();
    for item in xml.split("<instancesSet>").skip(1).flat_map(|set| set.split("</instancesSet>").next()).flat_map(|set| set.split("<item>").skip(1)) {
        let pick = if c.aws_use_private_ip.unwrap_or(true) {
            xml_values(item, "privateIpAddress").into_iter().next().or_else(|| xml_values(item, "privateDnsName").into_iter().next())
        } else {
            xml_values(item, "ipAddress").into_iter().next().or_else(|| xml_values(item, "dnsName").into_iter().next())
        };
        if let Some(addr) = pick.filter(|a| !a.is_empty()) {
            if !out.contains(&addr) {
                out.push(addr);
            }
        }
    }
    Ok(out)
}

// ── Resolution and upkeep ───────────────────────────────────────────────

/// A registered node, kept alive until [`Registration::remove`].
pub enum Registration {
    /// Nothing registered (k8s, aws, dns, static).
    None,
    /// A Consul service with a TTL check.
    Consul(Arc<ConsulHandle>),
    /// An etcd key under a lease.
    Etcd(Arc<EtcdHandle>),
}

/// A Consul registration.
pub struct ConsulHandle(Consul);
/// An etcd registration and its lease id.
pub struct EtcdHandle(Etcd, String);

impl Registration {
    /// Deregister (consul) or revoke the lease (etcd). Called on shutdown.
    pub async fn remove(&self) {
        let result = match self {
            Registration::None => Ok(()),
            Registration::Consul(c) => c.0.deregister().await,
            Registration::Etcd(e) => e.0.revoke(&e.1).await,
        };
        if let Err(err) = result {
            tracing::warn!(error = %err, "peer discovery deregistration failed");
        }
    }
}

/// The member list one backend reports now.
pub async fn discover(c: &ClusterConfig) -> Result<Vec<ClusterMember>, String> {
    let port = member_port(c);
    match c.discovery.as_deref() {
        Some("k8s") => Ok(members_from_hosts(k8s_hosts(c).await?.iter().map(String::as_str), port)),
        Some("aws") => Ok(members_from_hosts(aws_hosts(c).await?.iter().map(String::as_str), port)),
        Some("consul") => {
            let hosts = Consul::new(c).hosts().await?;
            let mut out = Vec::new();
            for (h, _service_port) in hosts {
                out.extend(members_from_hosts([h.as_str()], port));
            }
            out.sort_by(|a, b| a.id.cmp(&b.id));
            out.dedup_by(|a, b| a.id == b.id);
            Ok(out)
        }
        Some("etcd") => {
            let nodes = Etcd::new(c).nodes().await?;
            let hosts: Vec<String> = nodes.iter().map(|n| n.rsplit_once(':').map_or(n.clone(), |(h, _)| h.to_string())).collect();
            Ok(members_from_hosts(hosts.iter().map(String::as_str), port))
        }
        other => Err(format!("unknown discovery backend {other:?}")),
    }
}

/// With a cloud backend: register this node (consul, etcd), then fill
/// `c.members` the way DNS discovery does. This node is always a member.
pub async fn resolve(c: &mut ClusterConfig) -> Result<Registration, String> {
    if !is_cloud(c.discovery.as_deref()) {
        return Ok(Registration::None);
    }
    let backend = c.discovery.clone().unwrap_or_default();
    let listen = c.listen.ok_or_else(|| format!("cluster.listen is required with discovery = \"{backend}\""))?;
    let registration = match backend.as_str() {
        "consul" => {
            let consul = Consul::new(c);
            consul.register().await?;
            Registration::Consul(Arc::new(ConsulHandle(consul)))
        }
        "etcd" => {
            let etcd = Etcd::new(c);
            let lease = etcd.register().await?;
            Registration::Etcd(Arc::new(EtcdHandle(etcd, lease)))
        }
        _ => Registration::None,
    };
    // A failed lookup at startup leaves a single-node member list; the
    // periodic check picks the peers up later.
    let found = match discover(c).await {
        Ok(found) => found,
        Err(err) => {
            tracing::warn!(backend = %backend, error = %err, "peer discovery failed");
            Vec::new()
        }
    };
    if c.node_id.is_empty() {
        c.node_id = listen.to_string();
    }
    c.members = found;
    if !c.members.iter().any(|m| m.id == c.node_id) {
        c.members.push(ClusterMember { id: c.node_id.clone(), addr: listen });
    }
    c.members.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(registration)
}

/// Keep a registration alive and re-check the members every `every`,
/// writing them to `members_path` when they change.
pub fn spawn_upkeep(c: ClusterConfig, registration: &Registration, members_path: std::path::PathBuf, every: Duration) {
    let me = ClusterMember { id: c.node_id.clone(), addr: c.listen.unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0))) };
    let consul = match registration {
        Registration::Consul(h) => Some(Arc::clone(h)),
        _ => None,
    };
    let etcd = match registration {
        Registration::Etcd(h) => Some(Arc::clone(h)),
        _ => None,
    };
    let heartbeat = Duration::from_secs((c.consul_svc_ttl.or(c.etcd_node_ttl).unwrap_or(30) / 2).max(1));
    tokio::spawn(async move {
        let mut last = String::new();
        let mut next_beat = tokio::time::Instant::now() + heartbeat;
        loop {
            tokio::time::sleep(every.min(heartbeat)).await;
            if tokio::time::Instant::now() >= next_beat {
                next_beat = tokio::time::Instant::now() + heartbeat;
                if let Some(h) = &consul {
                    if let Err(err) = h.0.pass().await {
                        tracing::warn!(error = %err, "consul TTL check failed");
                    }
                }
                if let Some(h) = &etcd {
                    if let Err(err) = h.0.keepalive(&h.1).await {
                        tracing::warn!(error = %err, "etcd lease keepalive failed");
                    }
                }
            }
            let Ok(mut found) = discover(&c).await else { continue };
            if found.is_empty() {
                continue;
            }
            if !found.iter().any(|m| m.id == me.id) {
                found.push(me.clone());
            }
            found.sort_by(|a, b| a.id.cmp(&b.id));
            let text = serde_json::to_string(&found).unwrap_or_default();
            if text != last {
                let _ = std::fs::write(&members_path, &text);
                last = text;
            }
        }
    });
}

#[cfg(test)]
mod tests;
