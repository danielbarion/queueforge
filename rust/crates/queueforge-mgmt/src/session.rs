//! Server-side session store and cookie helpers.
//!
//! Sessions are **process-local in-memory** state: a broker restart invalidates
//! every management session (operators must log in again). There is a single
//! active session per username; idle TTL is [`IDLE_TTL`] and absolute lifetime
//! is [`ABSOLUTE_TTL`]. See `docs/OPERATIONS.md`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use cookie::{Cookie, SameSite};
use queueforge_core::UserTag;
use rand_core::{OsRng, RngCore};

/// Cookie name used for management sessions.
pub const SESSION_COOKIE_NAME: &str = "queueforge_session";

/// Idle timeout (design: 8 hours).
pub const IDLE_TTL: Duration = Duration::from_secs(8 * 60 * 60);
/// Absolute lifetime (design: 24 hours).
pub const ABSOLUTE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Rate limit: max login failures per IP within the window.
pub const LOGIN_FAIL_MAX: u32 = 5;
/// Rate limit window (design: 60 seconds).
pub const LOGIN_FAIL_WINDOW: Duration = Duration::from_secs(60);
/// Hard cap on distinct IPs tracked for login failures (memory bound).
pub const LOGIN_FAIL_MAP_MAX: usize = 10_000;
/// Soft cap on concurrent sessions (expired entries GC'd first).
pub const SESSION_MAP_SOFT_MAX: usize = 10_000;

/// Data stored for an authenticated management session.
#[derive(Debug, Clone)]
pub struct Session {
    /// Authenticated username.
    pub username: String,
    /// Capability tags at login time.
    pub tags: Vec<UserTag>,
    /// When the session was created.
    pub created_at: Instant,
    /// Last activity timestamp (idle TTL).
    pub last_seen: Instant,
}

impl Session {
    /// Whether the session has expired (idle or absolute).
    pub fn is_expired(&self, now: Instant) -> bool {
        now.duration_since(self.last_seen) > IDLE_TTL
            || now.duration_since(self.created_at) > ABSOLUTE_TTL
    }
}

/// In-memory session store keyed by opaque token.
///
/// **Multi-session policy:** a successful login replaces any prior sessions for
/// the same username (single active session per user). Expired sessions are
/// removed opportunistically on create/get and via [`SessionStore::gc`].
#[derive(Debug, Default)]
pub struct SessionStore {
    sessions: Mutex<HashMap<String, Session>>,
    login_failures: Mutex<HashMap<String, FailureWindow>>,
}

#[derive(Debug, Clone)]
struct FailureWindow {
    count: u32,
    window_start: Instant,
}

impl SessionStore {
    /// Create an empty session store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Shared handle.
    pub fn shared() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::new())
    }

    /// Mint a new session for `username` with `tags`. Returns the opaque token.
    ///
    /// Drops any existing sessions for the same username, then runs opportunistic
    /// GC of expired entries.
    pub fn create(&self, username: impl Into<String>, tags: Vec<UserTag>) -> String {
        let username = username.into();
        let token = random_token();
        let now = Instant::now();
        let session = Session {
            username: username.clone(),
            tags,
            created_at: now,
            last_seen: now,
        };
        let mut guard = self.sessions.lock().expect("session store poisoned");
        // Single active session per user: invalidate prior logins.
        guard.retain(|_, s| s.username != username);
        // Opportunistic GC of expired tokens.
        gc_sessions(&mut guard, now);
        // Bound map size if still over soft max after GC (drop oldest last_seen).
        if guard.len() >= SESSION_MAP_SOFT_MAX {
            if let Some(oldest_key) = guard
                .iter()
                .min_by_key(|(_, s)| s.last_seen)
                .map(|(k, _)| k.clone())
            {
                guard.remove(&oldest_key);
            }
        }
        guard.insert(token.clone(), session);
        token
    }

    /// Look up a session by token, refreshing idle TTL on hit.
    ///
    /// Returns `None` if missing or expired (expired entries are removed).
    /// Also opportunistically GCs other expired sessions under the same lock.
    pub fn get(&self, token: &str) -> Option<Session> {
        let mut guard = self.sessions.lock().expect("session store poisoned");
        let now = Instant::now();
        // Opportunistic GC (cheap when map is small).
        if guard.len() > 64 {
            gc_sessions(&mut guard, now);
        }
        match guard.get_mut(token) {
            Some(session) if !session.is_expired(now) => {
                session.last_seen = now;
                Some(session.clone())
            }
            Some(_) => {
                guard.remove(token);
                None
            }
            None => None,
        }
    }

    /// Delete a session (logout). Returns `true` if it existed.
    pub fn remove(&self, token: &str) -> bool {
        self.sessions
            .lock()
            .expect("session store poisoned")
            .remove(token)
            .is_some()
    }

    /// Drop all expired sessions. Returns how many were removed.
    pub fn gc(&self) -> usize {
        let mut guard = self.sessions.lock().expect("session store poisoned");
        let before = guard.len();
        gc_sessions(&mut guard, Instant::now());
        before.saturating_sub(guard.len())
    }

    /// Number of live (non-GC'd) sessions currently stored.
    pub fn session_count(&self) -> usize {
        self.sessions.lock().expect("session store poisoned").len()
    }

    /// Check whether `ip` is currently rate-limited for login.
    ///
    /// `ip` must be the real peer address (not a client-supplied proxy header).
    pub fn is_login_rate_limited(&self, ip: &str) -> bool {
        let mut guard = self.login_failures.lock().expect("login fail map poisoned");
        let now = Instant::now();
        gc_login_failures(&mut guard, now);
        if let Some(w) = guard.get(ip) {
            return now.duration_since(w.window_start) <= LOGIN_FAIL_WINDOW
                && w.count >= LOGIN_FAIL_MAX;
        }
        false
    }

    /// Record a failed login for peer `ip`.
    pub fn record_login_failure(&self, ip: &str) {
        let mut guard = self.login_failures.lock().expect("login fail map poisoned");
        let now = Instant::now();
        gc_login_failures(&mut guard, now);

        match guard.get_mut(ip) {
            Some(w) if now.duration_since(w.window_start) <= LOGIN_FAIL_WINDOW => {
                w.count = w.count.saturating_add(1);
            }
            Some(w) => {
                // Window expired; reset counter for this IP.
                w.count = 1;
                w.window_start = now;
            }
            None => {
                // Cap map size: refuse to track more distinct IPs until GC.
                if guard.len() >= LOGIN_FAIL_MAP_MAX {
                    // Drop oldest windows to make room.
                    evict_oldest_login_failures(&mut guard, LOGIN_FAIL_MAP_MAX / 10);
                }
                guard.insert(
                    ip.to_string(),
                    FailureWindow {
                        count: 1,
                        window_start: now,
                    },
                );
            }
        }
    }

    /// Clear failure counter on successful login.
    pub fn clear_login_failures(&self, ip: &str) {
        self.login_failures
            .lock()
            .expect("login fail map poisoned")
            .remove(ip);
    }

    /// Number of IPs currently tracked in the failure map (for tests).
    pub fn login_failure_tracked_ips(&self) -> usize {
        self.login_failures
            .lock()
            .expect("login fail map poisoned")
            .len()
    }
}

fn gc_sessions(map: &mut HashMap<String, Session>, now: Instant) {
    map.retain(|_, s| !s.is_expired(now));
}

fn gc_login_failures(map: &mut HashMap<String, FailureWindow>, now: Instant) {
    map.retain(|_, w| now.duration_since(w.window_start) <= LOGIN_FAIL_WINDOW);
}

fn evict_oldest_login_failures(map: &mut HashMap<String, FailureWindow>, count: usize) {
    let mut keys: Vec<(Instant, String)> = map
        .iter()
        .map(|(k, w)| (w.window_start, k.clone()))
        .collect();
    keys.sort_by_key(|(t, _)| *t);
    for (_, k) in keys.into_iter().take(count) {
        map.remove(&k);
    }
}

fn random_token() -> String {
    let mut bytes = [0u8; 32]; // 256-bit
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Build a `Set-Cookie` value for a new session.
///
/// Dev defaults: `HttpOnly; SameSite=Lax; Path=/`. `Secure` only when
/// `secure` is true (TLS enabled).
pub fn build_session_cookie(token: &str, secure: bool) -> String {
    let mut builder = Cookie::build((SESSION_COOKIE_NAME, token))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .max_age(cookie::time::Duration::seconds(
            ABSOLUTE_TTL.as_secs() as i64
        ));
    if secure {
        builder = builder.secure(true);
    }
    builder.build().to_string()
}

/// Build a `Set-Cookie` that clears the session cookie.
pub fn clear_session_cookie(secure: bool) -> String {
    let mut builder = Cookie::build((SESSION_COOKIE_NAME, ""))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .max_age(cookie::time::Duration::seconds(0));
    if secure {
        builder = builder.secure(true);
    }
    builder.build().to_string()
}

/// Extract the session token from a raw `Cookie` header value.
pub fn extract_token_from_cookie_header(header: &str) -> Option<String> {
    for part in header.split(';') {
        let part = part.trim();
        if let Some(rest) = part.strip_prefix(SESSION_COOKIE_NAME) {
            let rest = rest.trim_start();
            if let Some(value) = rest.strip_prefix('=') {
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use queueforge_core::UserTag;

    #[test]
    fn create_get_remove_session() {
        let store = SessionStore::new();
        let token = store.create("admin", vec![UserTag::Administrator]);
        let s = store.get(&token).expect("session");
        assert_eq!(s.username, "admin");
        assert!(store.remove(&token));
        assert!(store.get(&token).is_none());
    }

    #[test]
    fn login_replaces_prior_session_for_same_user() {
        let store = SessionStore::new();
        let t1 = store.create("admin", vec![UserTag::Administrator]);
        let t2 = store.create("admin", vec![UserTag::Administrator]);
        assert!(
            store.get(&t1).is_none(),
            "prior session must be invalidated"
        );
        assert!(store.get(&t2).is_some());
        assert_eq!(store.session_count(), 1);
    }

    #[test]
    fn cookie_dev_defaults_not_secure() {
        let c = build_session_cookie("abc", false);
        assert!(c.contains("HttpOnly"));
        assert!(c.contains("SameSite=Lax") || c.contains("SameSite=lax"));
        assert!(c.contains("Path=/"));
        assert!(!c.to_ascii_lowercase().contains("secure"));
        assert!(c.contains(SESSION_COOKIE_NAME));
    }

    #[test]
    fn cookie_secure_when_requested() {
        let c = build_session_cookie("abc", true);
        assert!(c.to_ascii_lowercase().contains("secure"));
    }

    #[test]
    fn rate_limit_after_max_failures() {
        let store = SessionStore::new();
        let ip = "10.0.0.1";
        for _ in 0..LOGIN_FAIL_MAX {
            assert!(!store.is_login_rate_limited(ip));
            store.record_login_failure(ip);
        }
        assert!(store.is_login_rate_limited(ip));
        store.clear_login_failures(ip);
        assert!(!store.is_login_rate_limited(ip));
    }

    #[test]
    fn rate_limit_is_per_ip() {
        let store = SessionStore::new();
        let a = "10.0.0.1";
        let b = "10.0.0.2";
        for _ in 0..LOGIN_FAIL_MAX {
            store.record_login_failure(a);
        }
        assert!(store.is_login_rate_limited(a));
        assert!(!store.is_login_rate_limited(b));
        // Failures on B do not clear A's limit.
        store.record_login_failure(b);
        assert!(store.is_login_rate_limited(a));
        assert!(!store.is_login_rate_limited(b));
    }

    #[test]
    fn extract_token_parses_cookie_header() {
        let h = format!("other=1; {SESSION_COOKIE_NAME}=deadbeef; foo=bar");
        assert_eq!(
            extract_token_from_cookie_header(&h).as_deref(),
            Some("deadbeef")
        );
    }
}
