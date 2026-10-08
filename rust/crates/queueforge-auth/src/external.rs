//! Authentication backends after the internal user store, in RabbitMQ's
//! `auth_backends` order: OAuth 2.0 tokens, then LDAP.
//!
//! **OAuth 2.0:** the password is an RS256 JWT. Its key comes from the JWKS
//! URL by `kid`; `exp` must be in the future and `aud` must name the
//! resource server id. Scopes use RabbitMQ's form,
//! `<id>.<perm>:<vhost>/<resource>[/<routing key>]` (perm is configure,
//! write or read; `*` is a wildcard), and `<id>.tag:<tag>`.
//!
//! **LDAP:** a simple bind as the DN from `user_dn_pattern`. A member of
//! `admin_group` gets the administrator tag, everyone else management;
//! vhost and resource access are granted, as RabbitMQ's default queries do.
//!
//! A successful login records a [`Principal`] under the login name.
//! Permission checks consult it for names that are not in the internal
//! store. Two logins with one name but different tokens share it: the
//! latest wins.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use queueforge_core::config::{AuthConfig, LdapConfig, OAuth2Config};
use regex::Regex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::permission::PermissionKind;

/// One granted scope: a permission on resources matching a vhost pattern.
#[derive(Debug, Clone)]
pub struct Scope {
    kind: PermissionKind,
    vhost: Regex,
    resource: Regex,
    routing_key: Option<Regex>,
}

/// A login from OAuth 2.0 or LDAP.
#[derive(Debug, Clone)]
pub struct Principal {
    /// User tags (administrator, management, ...).
    pub tags: Vec<String>,
    /// Token scopes; `None` grants every vhost and resource (LDAP).
    pub scopes: Option<Vec<Scope>>,
    /// Epoch milliseconds after which the login no longer grants anything.
    pub expires_at_ms: Option<u64>,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

impl Principal {
    fn live(&self) -> bool {
        self.expires_at_ms.is_none_or(|t| now_ms() < t)
    }

    /// Whether a scope grants `kind` on `resource` in `vhost`. A routing key
    /// is checked only against scopes that name one.
    pub fn allows(&self, vhost: &str, kind: PermissionKind, resource: &str, routing_key: Option<&str>) -> bool {
        if !self.live() {
            return false;
        }
        let Some(scopes) = &self.scopes else { return true };
        scopes.iter().any(|s| {
            s.kind == kind
                && s.vhost.is_match(vhost)
                && s.resource.is_match(resource)
                && match (routing_key, &s.routing_key) {
                    (Some(k), Some(p)) => p.is_match(k),
                    _ => true,
                }
        })
    }

    /// Whether any scope names `vhost`.
    pub fn has_vhost(&self, vhost: &str) -> bool {
        self.live() && self.scopes.as_ref().is_none_or(|s| s.iter().any(|x| x.vhost.is_match(vhost)))
    }
}

static CONFIG: OnceLock<AuthConfig> = OnceLock::new();

fn principals() -> &'static RwLock<HashMap<String, Arc<Principal>>> {
    static P: OnceLock<RwLock<HashMap<String, Arc<Principal>>>> = OnceLock::new();
    P.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Install the backend configuration. The first call wins.
pub fn configure(cfg: AuthConfig) {
    let _ = CONFIG.set(cfg);
}

/// The OAuth or LDAP login recorded for `user`, if it is still valid.
pub fn principal(user: &str) -> Option<Arc<Principal>> {
    let p = principals().read().ok()?.get(user).cloned()?;
    p.live().then_some(p)
}

/// Try OAuth 2.0 (when the password looks like a JWT), then LDAP. A success
/// is recorded under `user`, or the token subject when `user` is empty.
/// Returns the name it was recorded under.
pub async fn login(user: &str, password: &str) -> Option<String> {
    let cfg = CONFIG.get()?;
    if let Some(oauth) = &cfg.oauth2 {
        if password.split('.').count() == 3 {
            if let Some((p, sub)) = oauth_login(oauth, password).await {
                let name = if user.is_empty() { sub } else { user.to_string() };
                principals().write().ok()?.insert(name.clone(), Arc::new(p));
                return Some(name);
            }
        }
    }
    if let Some(ldap) = &cfg.ldap {
        if let Some(p) = ldap_login(ldap, user, password).await {
            principals().write().ok()?.insert(user.to_string(), Arc::new(p));
            return Some(user.to_string());
        }
    }
    None
}

/// A RabbitMQ scope wildcard as an anchored regex.
fn wildcard(pattern: &str) -> Option<Regex> {
    let text = percent_decode(pattern);
    let parts: Vec<String> = text.split('*').map(regex::escape).collect();
    Regex::new(&format!("^{}$", parts.join(".*"))).ok()
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = b.get(i + 1..i + 3).and_then(|h| std::str::from_utf8(h).ok()).and_then(|h| u8::from_str_radix(h, 16).ok());
        match (b[i], hex) {
            (b'%', Some(n)) => {
                out.push(n);
                i += 3;
            }
            (c, _) => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

/// Scopes and tags for one resource server id; other scopes are ignored.
pub fn parse_scopes(scopes: &[String], resource_server_id: &str) -> (Vec<Scope>, Vec<String>) {
    let prefix = format!("{resource_server_id}.");
    let mut out = Vec::new();
    let mut tags = Vec::new();
    for raw in scopes {
        let Some(s) = raw.strip_prefix(&prefix) else { continue };
        if let Some(tag) = s.strip_prefix("tag:") {
            tags.push(tag.to_string());
            continue;
        }
        let Some((perm, rest)) = s.split_once(':') else { continue };
        let kind = match perm {
            "configure" => PermissionKind::Configure,
            "write" => PermissionKind::Write,
            "read" => PermissionKind::Read,
            _ => continue,
        };
        let mut parts = rest.splitn(3, '/');
        let (Some(vhost), Some(resource)) = (parts.next(), parts.next()) else { continue };
        let routing_key = parts.next();
        let (Some(vhost), Some(resource)) = (wildcard(vhost), wildcard(resource)) else { continue };
        let routing_key = match routing_key {
            Some(k) => match wildcard(k) {
                Some(r) => Some(r),
                None => continue,
            },
            None => None,
        };
        out.push(Scope { kind, vhost, resource, routing_key });
    }
    (out, tags)
}

/// JWKS keys by `kid`, and when they were fetched.
struct Jwks {
    keys: HashMap<String, (Vec<u8>, Vec<u8>)>,
    fetched_ms: u64,
}

fn jwks() -> &'static Mutex<Jwks> {
    static J: OnceLock<Mutex<Jwks>> = OnceLock::new();
    J.get_or_init(|| Mutex::new(Jwks { keys: HashMap::new(), fetched_ms: 0 }))
}

fn b64url(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s.trim_end_matches('=')).ok()
}

/// RSA modulus and exponent for `kid`, fetching the JWKS when it is unknown or older than 10 s.
async fn rsa_key(cfg: &OAuth2Config, kid: &str) -> Option<(Vec<u8>, Vec<u8>)> {
    let stale = {
        let j = jwks().lock().ok()?;
        !j.keys.contains_key(kid) || now_ms().saturating_sub(j.fetched_ms) > 10_000
    };
    if stale {
        if let Some(body) = http_get(&cfg.jwks_url, cfg.jwks_ca_path.as_deref()).await {
            if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&body) {
                let mut keys = HashMap::new();
                for k in doc.get("keys").and_then(|v| v.as_array()).into_iter().flatten() {
                    let field = |name: &str| k.get(name).and_then(|v| v.as_str());
                    if field("kty") != Some("RSA") {
                        continue;
                    }
                    if let (Some(id), Some(n), Some(e)) = (field("kid"), field("n").and_then(b64url), field("e").and_then(b64url)) {
                        keys.insert(id.to_string(), (n, e));
                    }
                }
                if let Ok(mut j) = jwks().lock() {
                    j.keys = keys;
                    j.fetched_ms = now_ms();
                }
            }
        }
    }
    jwks().lock().ok()?.keys.get(kid).cloned()
}

async fn oauth_login(cfg: &OAuth2Config, token: &str) -> Option<(Principal, String)> {
    let mut parts = token.split('.');
    let (h, c, sig) = (parts.next()?, parts.next()?, parts.next()?);
    let header: serde_json::Value = serde_json::from_slice(&b64url(h)?).ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&b64url(c)?).ok()?;
    if header.get("alg")?.as_str()? != "RS256" {
        return None;
    }
    let (n, e) = rsa_key(cfg, header.get("kid")?.as_str()?).await?;
    let key = ring::signature::RsaPublicKeyComponents { n: &n, e: &e };
    let signed = format!("{h}.{c}");
    key.verify(&ring::signature::RSA_PKCS1_2048_8192_SHA256, signed.as_bytes(), &b64url(sig)?).ok()?;
    let now = now_ms() / 1000;
    let exp = claims.get("exp")?.as_u64()?;
    if exp <= now {
        return None;
    }
    if claims.get("nbf").and_then(|v| v.as_u64()).is_some_and(|nbf| nbf > now + 60) {
        return None;
    }
    let aud_ok = match claims.get("aud") {
        Some(serde_json::Value::String(a)) => *a == cfg.resource_server_id,
        Some(serde_json::Value::Array(a)) => a.iter().any(|v| v.as_str() == Some(cfg.resource_server_id.as_str())),
        _ => false,
    };
    if !aud_ok {
        return None;
    }
    let scope: Vec<String> = match claims.get("scope") {
        Some(serde_json::Value::String(s)) => s.split(' ').map(str::to_string).collect(),
        Some(serde_json::Value::Array(a)) => a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect(),
        _ => Vec::new(),
    };
    let (scopes, tags) = parse_scopes(&scope, &cfg.resource_server_id);
    let sub = claims.get("sub").or(claims.get("client_id")).and_then(|v| v.as_str()).unwrap_or_default().to_string();
    Some((Principal { tags, scopes: Some(scopes), expires_at_ms: Some(exp * 1000) }, sub))
}

/// GET a small document over http or https. HTTPS trusts only `ca_path`.
async fn http_get(url: &str, ca_path: Option<&std::path::Path>) -> Option<Vec<u8>> {
    let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else {
        (false, url.strip_prefix("http://")?)
    };
    let (authority, path) = rest.split_once('/').map(|(a, p)| (a, format!("/{p}"))).unwrap_or((rest, "/".to_string()));
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().ok()?),
        None => (authority.to_string(), if tls { 443 } else { 80 }),
    };
    let tcp = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect((host.as_str(), port))).await.ok()?.ok()?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {authority}\r\nAccept: application/json\r\nConnection: close\r\n\r\n");
    let raw = if tls {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(ca) = ca_path {
            let pem = std::fs::read(ca).ok()?;
            for cert in rustls_pemfile::certs(&mut pem.as_slice()).flatten() {
                let _ = roots.add(cert);
            }
        }
        let config = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from(host.clone()).ok()?;
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config)).connect(name, tcp).await.ok()?;
        exchange(stream, &request).await?
    } else {
        exchange(tcp, &request).await?
    };
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    if !head.starts_with("http/1.1 200") && !head.starts_with("http/1.0 200") {
        return None;
    }
    let body = &raw[split + 4..];
    if head.contains("transfer-encoding: chunked") {
        return Some(dechunk(body));
    }
    Some(body.to_vec())
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, request: &str) -> Option<Vec<u8>> {
    s.write_all(request.as_bytes()).await.ok()?;
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut out)).await;
    (!out.is_empty()).then_some(out)
}

fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(end) = b.windows(2).position(|w| w == b"\r\n") {
        let size = usize::from_str_radix(String::from_utf8_lossy(&b[..end]).split(';').next().unwrap_or("0").trim(), 16).unwrap_or(0);
        if size == 0 || b.len() < end + 2 + size {
            break;
        }
        out.extend_from_slice(&b[end + 2..end + 2 + size]);
        b = &b[(end + 4 + size).min(b.len())..];
    }
    out
}

/// Escape a value for a DN attribute (RFC 4514).
fn dn_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (i, c) in s.chars().enumerate() {
        if matches!(c, ',' | '+' | '"' | '\\' | '<' | '>' | ';' | '=') || (i == 0 && matches!(c, ' ' | '#')) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

async fn ldap_login(cfg: &LdapConfig, user: &str, password: &str) -> Option<Principal> {
    // An empty password would be an unauthenticated bind, which succeeds.
    if user.is_empty() || password.is_empty() {
        return None;
    }
    let dn = cfg.user_dn_pattern.replace("${username}", &dn_value(user));
    let mut c = Ldap::connect(&cfg.server, cfg.port).await?;
    if c.bind(&dn, password).await? != 0 {
        return None;
    }
    let mut tags = vec!["management".to_string()];
    if let Some(group) = &cfg.admin_group {
        let bound = match &cfg.bind_dn {
            Some(admin) => c.bind(admin, cfg.bind_password.as_deref().unwrap_or_default()).await == Some(0),
            None => true,
        };
        if bound && c.base_has(group, "member", &dn).await == Some(true) {
            tags.insert(0, "administrator".to_string());
        }
    }
    c.unbind().await;
    Some(Principal { tags, scopes: None, expires_at_ms: None })
}

/// The few LDAPv3 operations the backend needs, BER-encoded by hand.
struct Ldap {
    io: TcpStream,
    next_id: i64,
}

fn ber(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    let n = content.len();
    if n < 0x80 {
        out.push(n as u8);
    } else if n <= 0xff {
        out.extend([0x81, n as u8]);
    } else if n <= 0xffff {
        out.extend([0x82, (n >> 8) as u8, n as u8]);
    } else {
        out.push(0x84);
        out.extend((n as u32).to_be_bytes());
    }
    out.extend_from_slice(content);
    out
}

fn ber_int(tag: u8, n: i64) -> Vec<u8> {
    let bytes = n.to_be_bytes();
    let mut start = 0;
    while start < 7 && ((bytes[start] == 0 && bytes[start + 1] & 0x80 == 0) || (bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0)) {
        start += 1;
    }
    ber(tag, &bytes[start..])
}

/// Split one TLV off the front: (tag, content, rest).
fn tlv(b: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let tag = *b.first()?;
    let first = *b.get(1)?;
    let (len, at) = if first < 0x80 {
        (first as usize, 2)
    } else {
        let k = (first & 0x7f) as usize;
        if k == 0 || k > 4 {
            return None;
        }
        let mut n = 0usize;
        for i in 0..k {
            n = (n << 8) | *b.get(2 + i)? as usize;
        }
        (n, 2 + k)
    };
    let content = b.get(at..at + len)?;
    Some((tag, content, &b[at + len..]))
}

impl Ldap {
    async fn connect(host: &str, port: u16) -> Option<Self> {
        let io = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect((host, port))).await.ok()?.ok()?;
        Some(Ldap { io, next_id: 1 })
    }

    async fn send(&mut self, op: Vec<u8>) -> Option<i64> {
        let id = self.next_id;
        self.next_id += 1;
        let mut msg = ber_int(0x02, id);
        msg.extend(op);
        self.io.write_all(&ber(0x30, &msg)).await.ok()?;
        Some(id)
    }

    /// Read one LDAPMessage; returns the protocol op's tag and content.
    async fn recv(&mut self) -> Option<(u8, Vec<u8>)> {
        let mut head = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), self.io.read_exact(&mut head)).await.ok()?.ok()?;
        let mut len_bytes = Vec::new();
        let len = if head[1] < 0x80 {
            head[1] as usize
        } else {
            let k = (head[1] & 0x7f) as usize;
            if k == 0 || k > 4 {
                return None;
            }
            len_bytes.resize(k, 0);
            self.io.read_exact(&mut len_bytes).await.ok()?;
            len_bytes.iter().fold(0usize, |n, b| (n << 8) | *b as usize)
        };
        let mut body = vec![0u8; len];
        self.io.read_exact(&mut body).await.ok()?;
        let (_, _, rest) = tlv(&body)?; // messageID
        let (tag, content, _) = tlv(rest)?;
        Some((tag, content.to_vec()))
    }

    /// The resultCode of an LDAPResult.
    fn result_code(content: &[u8]) -> Option<u8> {
        let (tag, code, _) = tlv(content)?;
        (tag == 0x0a).then(|| code.last().copied().unwrap_or(0))
    }

    /// Simple bind. Returns the LDAP result code (0 is success).
    async fn bind(&mut self, dn: &str, password: &str) -> Option<u8> {
        let mut body = ber_int(0x02, 3);
        body.extend(ber(0x04, dn.as_bytes()));
        body.extend(ber(0x80, password.as_bytes()));
        self.send(ber(0x60, &body)).await?;
        let (tag, content) = self.recv().await?;
        (tag == 0x61).then_some(())?;
        Self::result_code(&content)
    }

    /// Whether the entry at `base` has `attr` equal to `value`.
    async fn base_has(&mut self, base: &str, attr: &str, value: &str) -> Option<bool> {
        let mut filter = ber(0x04, attr.as_bytes());
        filter.extend(ber(0x04, value.as_bytes()));
        let mut body = ber(0x04, base.as_bytes());
        body.extend(ber_int(0x0a, 0)); // scope: base object
        body.extend(ber_int(0x0a, 0)); // derefAliases: never
        body.extend(ber_int(0x02, 1)); // sizeLimit
        body.extend(ber_int(0x02, 5)); // timeLimit
        body.extend(ber(0x01, &[0])); // typesOnly
        body.extend(ber(0xa3, &filter)); // equalityMatch
        body.extend(ber(0x30, &ber(0x04, b"1.1"))); // no attributes
        self.send(ber(0x63, &body)).await?;
        let mut found = false;
        loop {
            let (tag, content) = self.recv().await?;
            match tag {
                0x64 => found = true,
                0x65 => return Some(found && Self::result_code(&content) == Some(0)),
                _ => {}
            }
        }
    }

    async fn unbind(&mut self) {
        let _ = self.send(vec![0x42, 0x00]).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_follow_rabbitmq_format() {
        let raw = ["rabbitmq.read:*/*".to_string(), "rabbitmq.configure:prod/app-*".to_string(), "other.write:*/*".to_string(), "rabbitmq.tag:management".to_string()];
        let (scopes, tags) = parse_scopes(&raw, "rabbitmq");
        let p = Principal { tags, scopes: Some(scopes), expires_at_ms: None };
        assert!(p.allows("/", PermissionKind::Read, "anything", None));
        assert!(p.allows("prod", PermissionKind::Configure, "app-1", None));
        assert!(!p.allows("dev", PermissionKind::Configure, "app-1", None));
        assert!(!p.allows("/", PermissionKind::Write, "q", None));
        assert_eq!(p.tags, vec!["management".to_string()]);
    }

    #[test]
    fn ber_lengths_round_trip() {
        let long = vec![7u8; 300];
        let enc = ber(0x04, &long);
        let (tag, content, rest) = tlv(&enc).unwrap();
        assert_eq!((tag, content.len(), rest.len()), (0x04, 300, 0));
        assert_eq!(ber_int(0x02, 3), vec![0x02, 0x01, 0x03]);
        assert_eq!(ber_int(0x02, 128), vec![0x02, 0x02, 0x00, 0x80]);
    }
}
