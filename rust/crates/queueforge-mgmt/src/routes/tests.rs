//! Management route tests for login, queues, exchanges, and bindings.

use std::sync::Arc;

use super::*;
use crate::connections::ConnectionTracker;
use crate::session::{LOGIN_FAIL_MAX, SESSION_COOKIE_NAME};
use crate::state::MgmtConfig;
use axum::body::Body;
use axum::extract::connect_info::MockConnectInfo;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use queueforge_auth::{AuthService, BootstrapMode};
use queueforge_core::{
    MemoryTracker, QueueDeclareOpts, QueueMetaStore, QueueRegistry, DEFAULT_VHOST,
};
use queueforge_metrics::ReadyFlag;
use queueforge_store::MetadataStore;
use tempfile::TempDir;
use tower::ServiceExt;

async fn test_state() -> (TempDir, MgmtState) {
    let dir = TempDir::new().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .unwrap());
    let store = Arc::new(store);
    let queues = QueueRegistry::shared(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    );
    let ready = ReadyFlag::new();
    ready.set_ready(true);
    let router = std::sync::Arc::new(store.bootstrap_router().expect("bootstrap router"));
    let state = MgmtState::new(
        store,
        queues,
        router,
        ConnectionTracker::shared(),
        ready,
        MgmtConfig::default(),
    );
    (dir, state)
}

/// Router with mocked peer IP for ConnectInfo extraction in oneshot tests.
fn test_app(state: MgmtState, peer: SocketAddr) -> Router {
    router(state).layer(MockConnectInfo(peer))
}

fn peer(ip: [u8; 4], port: u16) -> SocketAddr {
    SocketAddr::from((ip, port))
}

async fn body_json(res: Response) -> serde_json::Value {
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&bytes).into()))
}

fn cookie_from(res: &Response) -> Option<String> {
    res.headers()
        .get(header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(';').next().map(|p| p.trim().to_string()))
}

fn login_body(user: &str, pass: &str) -> Body {
    Body::from(format!(r#"{{"username":"{user}","password":"{pass}"}}"#))
}

async fn post_login(app: Router, body: Body) -> Response {
    app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/api/login")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .unwrap(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn login_sets_session_cookie_and_whoami() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 10001));

    let res = post_login(app.clone(), login_body("admin", "devpassword12")).await;
    assert_eq!(res.status(), StatusCode::OK);
    let cookie = cookie_from(&res).expect("Set-Cookie");
    assert!(cookie.starts_with(&format!("{SESSION_COOKIE_NAME}=")));
    // Dev defaults: HttpOnly + SameSite=Lax, no Secure flag.
    let set_cookie = res
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.contains("HttpOnly"));
    assert!(
        !set_cookie
            .split(';')
            .any(|p| p.trim().eq_ignore_ascii_case("Secure")),
        "dev cookie must not be Secure: {set_cookie}"
    );

    let json = body_json(res).await;
    assert_eq!(json["name"], "admin");

    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/whoami")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    assert_eq!(json["name"], "admin");
}

#[tokio::test]
async fn session_cookie_from_one_host_port_is_rejected_by_the_other() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 10001));

    async fn login_as(app: &Router, host: &str) -> String {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/login")
                    .header(header::HOST, host)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(login_body("admin", "devpassword12"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK, "{host} login");
        cookie_from(&res).expect("Set-Cookie")
    }

    async fn whoami(app: &Router, host: &str, cookie: &str) -> StatusCode {
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/whoami")
                    .header(header::HOST, host)
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    let rust_host = "127.0.0.1:36673";
    let bun_host = "127.0.0.1:36674";
    let rust_cookie = login_as(&app, rust_host).await;
    assert!(rust_cookie.starts_with("queueforge_session_36673="));
    assert_eq!(whoami(&app, rust_host, &rust_cookie).await, StatusCode::OK);
    assert_eq!(
        whoami(&app, bun_host, &rust_cookie).await,
        StatusCode::UNAUTHORIZED
    );

    let bun_cookie = login_as(&app, bun_host).await;
    assert!(bun_cookie.starts_with("queueforge_session_36674="));
    assert_ne!(rust_cookie, bun_cookie);
    assert_eq!(
        whoami(&app, rust_host, &bun_cookie).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(whoami(&app, bun_host, &bun_cookie).await, StatusCode::OK);

    let logout = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/logout")
                .header(header::HOST, bun_host)
                .header(header::COOKIE, &bun_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    let cleared = logout
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cleared.starts_with("queueforge_session_36674="));
    assert!(!cleared.contains("queueforge_session_36673"));
    assert_eq!(
        whoami(&app, bun_host, &bun_cookie).await,
        StatusCode::UNAUTHORIZED
    );
}

/// When TLS is enabled, `MgmtConfig.cookie_secure=true` must mark Set-Cookie Secure.
#[tokio::test]
async fn login_sets_secure_cookie_when_cookie_secure_true() {
    let dir = TempDir::new().unwrap();
    let store = MetadataStore::open(dir.path()).unwrap();
    let auth = AuthService::new(&store);
    assert!(auth
        .bootstrap_admin_if_empty(BootstrapMode::DevFallback)
        .unwrap());
    let store = Arc::new(store);
    let queues = QueueRegistry::shared(
        Arc::clone(&store) as Arc<dyn QueueMetaStore>,
        MemoryTracker::shared(),
    );
    let ready = ReadyFlag::new();
    ready.set_ready(true);
    let router = std::sync::Arc::new(store.bootstrap_router().expect("bootstrap router"));
    let state = MgmtState::new(
        store,
        queues,
        router,
        ConnectionTracker::shared(),
        ready,
        MgmtConfig {
            cookie_secure: true,
            product_version: "0.1.0-test".into(),
            trusted_proxy_cidrs: Vec::new(),
            ..MgmtConfig::default()
        },
    );
    let app = test_app(state, peer([127, 0, 0, 1], 10011));

    let res = post_login(app, login_body("admin", "devpassword12")).await;
    assert_eq!(res.status(), StatusCode::OK);
    let set_cookie = res
        .headers()
        .get(header::SET_COOKIE)
        .expect("Set-Cookie")
        .to_str()
        .unwrap();
    assert!(
        set_cookie
            .split(';')
            .any(|p| p.trim().eq_ignore_ascii_case("Secure")),
        "TLS cookie must include Secure: {set_cookie}"
    );
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("SameSite=Lax") || set_cookie.contains("SameSite=lax"));
}

#[tokio::test]
async fn login_bad_password_unauthorized() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 10002));
    let res = post_login(app, login_body("admin", "wrong-password")).await;
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_rate_limit_uses_peer_ip_not_xff() {
    let (_dir, state) = test_state().await;
    let peer_a = peer([10, 0, 0, 1], 40000);
    let app = test_app(state.clone(), peer_a);

    // Exhaust the rate limit for peer A (LOGIN_FAIL_MAX failures).
    for i in 0..LOGIN_FAIL_MAX {
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/login")
                    .header(header::CONTENT_TYPE, "application/json")
                    // Spoofed XFF must NOT bypass peer-IP rate limiting.
                    .header("x-forwarded-for", format!("1.2.3.{i}"))
                    .header("x-real-ip", format!("9.9.9.{i}"))
                    .body(login_body("admin", "wrong-password"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::UNAUTHORIZED,
            "failure {i} should be 401"
        );
    }

    // N+1 without spoofable-header bypass → 429, still with unique XFF.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/login")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-forwarded-for", "203.0.113.99")
                .body(login_body("admin", "wrong-password"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "peer IP must be rate-limited regardless of XFF"
    );

    // Distinct peer B still allowed (not globally locked by "unknown").
    let app_b = test_app(state, peer([10, 0, 0, 2], 40001));
    let res = post_login(app_b, login_body("admin", "wrong-password")).await;
    assert_eq!(
        res.status(),
        StatusCode::UNAUTHORIZED,
        "other peer must not share rate-limit bucket"
    );
}

#[tokio::test]
async fn list_queues_requires_auth_and_returns_declared() {
    let (_dir, state) = test_state().await;

    // Declare a queue via registry.
    state
        .queues
        .declare(
            DEFAULT_VHOST,
            "orders",
            QueueDeclareOpts {
                durable: false,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let app = test_app(state.clone(), peer([127, 0, 0, 1], 10003));

    // Unauthenticated → 401.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/queues/%2F")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Login.
    let res = post_login(app.clone(), login_body("admin", "devpassword12")).await;
    assert_eq!(res.status(), StatusCode::OK);
    let cookie = cookie_from(&res).unwrap();

    // List queues for default vhost (%2F).
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/queues/%2F")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "list queues status");
    let json = body_json(res).await;
    let items = json["items"].as_array().expect("items array");
    assert!(
        items
            .iter()
            .any(|q| q["name"] == "orders" && q["vhost"] == "/"),
        "expected orders queue in {json}"
    );
    assert!(json["total_count"].as_u64().unwrap() >= 1);

    // Overview works.
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/overview")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    assert_eq!(json["product_name"], "QueueForge");
    assert!(json["object_totals"]["queues"].as_u64().unwrap() >= 1);
    // Builtin exchanges on default vhost.
    assert!(json["object_totals"]["exchanges"].as_u64().unwrap() >= 4);
}

#[tokio::test]
async fn list_exchanges_default_vhost() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 10004));

    let res = post_login(app.clone(), login_body("admin", "devpassword12")).await;
    let cookie = cookie_from(&res).unwrap();

    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/exchanges/%2F")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    let items = json["items"].as_array().unwrap();
    // default + amq.direct + amq.fanout + amq.topic
    assert!(items.len() >= 4, "exchanges: {json}");
    assert!(items.iter().any(|e| e["name"] == ""
        || e["name"] == serde_json::Value::String(String::new())
        || e["name"].as_str() == Some("")));
    assert!(items.iter().any(|e| e["name"] == "amq.topic"));
}

#[tokio::test]
async fn logout_clears_session() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 10005));

    let res = post_login(app.clone(), login_body("admin", "devpassword12")).await;
    let cookie = cookie_from(&res).unwrap();

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/logout")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/whoami")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn decode_vhost_slash() {
    assert_eq!(decode_vhost("/").unwrap(), "/");
    assert_eq!(decode_vhost("%2F").unwrap(), "/");
    assert_eq!(decode_vhost("%2f").unwrap(), "/");
}

async fn auth_cookie(app: Router) -> String {
    let res = post_login(app, login_body("admin", "devpassword12")).await;
    assert_eq!(res.status(), StatusCode::OK);
    cookie_from(&res).expect("session cookie")
}

#[tokio::test]
async fn crud_queue_exchange_binding_publish_get() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 20001));
    let cookie = auth_cookie(app.clone()).await;

    // Create exchange
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/exchanges/%2F/mgmt.ex")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"type":"direct","durable":false}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED, "create exchange");

    // Create queue
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/queues/%2F/mgmt.q")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"durable":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED, "create queue");

    // Bind
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/bindings/%2F")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"source":"mgmt.ex","destination":"mgmt.q","routing_key":"rk1"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED, "bind");

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/exchanges/%2F")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "list exchanges");
    let json = body_json(res).await;
    assert!(
        json["items"]
            .as_array()
            .expect("exchange items")
            .iter()
            .any(|ex| ex["name"] == "mgmt.ex"),
        "created exchange missing from list: {json}"
    );

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/bindings/%2F")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "list bindings");
    let json = body_json(res).await;
    assert!(
        json["items"]
            .as_array()
            .expect("binding items")
            .iter()
            .any(|b| {
                b["source"] == "mgmt.ex"
                    && b["destination"] == "mgmt.q"
                    && b["routing_key"] == "rk1"
            }),
        "created binding missing from list: {json}"
    );

    // Publish
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/exchanges/%2F/mgmt.ex/publish")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"routing_key":"rk1","payload":"hello-mgmt","payload_encoding":"string"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "publish");
    let json = body_json(res).await;
    assert_eq!(json["routed"], true);

    // Get (consume)
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/queues/%2F/mgmt.q/get")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK, "get");
    let json = body_json(res).await;
    let items = json.as_array().expect("array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["payload"], "hello-mgmt");

    // Definitions export includes exchange/queue
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/definitions")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let defs = body_json(res).await;
    assert!(defs["queues"]
        .as_array()
        .unwrap()
        .iter()
        .any(|q| q["name"] == "mgmt.q"));
    assert!(defs["exchanges"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["name"] == "mgmt.ex"));

    // Unbind + delete
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/bindings/%2F/mgmt.ex/mgmt.q/rk1")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/queues/%2F/mgmt.q")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let res = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/exchanges/%2F/mgmt.ex")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn users_and_permissions_admin_only() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 20002));
    let cookie = auth_cookie(app.clone()).await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/users/alice")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"password":"alicepassword1","tags":["management"]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/permissions/alice/%2F")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"configure":".*","write":".*","read":".*"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/users")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    assert!(json
        .as_array()
        .unwrap()
        .iter()
        .any(|u| u["name"] == "alice"));

    let res = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/users/alice")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn mutation_requires_auth() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 20003));
    let res = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/queues/%2F/noauth")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unknown_api_route_is_json_404_not_spa() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 10006));

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/no-such-route")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let ct = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("application/json"),
        "expected JSON 404, got content-type {ct}"
    );
    let json = body_json(res).await;
    assert_eq!(json["error"], "not found");

    // POST also must not fall through to SPA shell.
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let ct = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("application/json"), "POST ct={ct}");
}

#[tokio::test]
async fn missing_static_asset_is_404_not_spa() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 10007));

    let res = app
        .oneshot(
            Request::builder()
                .uri("/assets/missing.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let ct = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        !ct.contains("text/html"),
        "missing asset must not be SPA shell, ct={ct}"
    );
}

#[tokio::test]
async fn spa_index_still_served_and_overview_still_json() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 10008));

    // SPA root.
    let res = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let ct = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("text/html"));
    assert_eq!(
        res.headers()
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("nosniff")
    );

    // Registered API still JSON (unauthenticated → 401).
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/overview")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let json = body_json(res).await;
    assert_eq!(json["error"], "unauthorized");
}

#[tokio::test]
async fn force_close_connection_and_404() {
    let (_dir, state) = test_state().await;
    let (id, mut rx) = state
        .connections
        .register("127.0.0.1:9".parse().unwrap(), "admin", "/");
    let app = test_app(state.clone(), peer([127, 0, 0, 1], 20010));
    let cookie = auth_cookie(app.clone()).await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/connections/{id}"))
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert!(*rx.borrow_and_update());

    let res = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/connections/conn-does-not-exist")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    state.connections.unregister(&id);
}

#[tokio::test]
async fn put_queue_accepts_arguments_and_rejects_unknown() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 20011));
    let cookie = auth_cookie(app.clone()).await;

    let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/queues/%2F/args.q")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"durable":true,"arguments":{"x-message-ttl":5000,"x-max-length":10,"x-max-priority":5}}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED, "declare with args");

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/queues/%2F/bad.args")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"arguments":{"x-unknown":1}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        StatusCode::CREATED,
        "unknown x- argument is ignored"
    );

    let res = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/queues/%2F/bad.plain")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"arguments":{"not-an-x-arg":1}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::PRECONDITION_FAILED);
}

#[tokio::test]
async fn delete_binding_uses_properties_key_for_header_args() {
    let (_dir, state) = test_state().await;
    let app = test_app(state, peer([127, 0, 0, 1], 20012));
    let cookie = auth_cookie(app.clone()).await;

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/exchanges/%2F/hdr")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"type":"headers","durable":false}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/api/queues/%2F/hq")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"durable":false}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);

    for (name, value) in [("color", "blue"), ("size", "l")] {
        let body = format!(
            r#"{{"source":"hdr","destination":"hq","routing_key":"","arguments":{{"{name}":"{value}"}}}}"#
        );
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/bindings/%2F")
                    .header(header::COOKIE, &cookie)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED, "{name}");
    }

    let color = queueforge_core::binding_properties_key(
        "",
        &[(
            compact_str::CompactString::from("color"),
            queueforge_core::HeaderArg::Str("blue".into()),
        )],
    );
    let mut encoded = String::new();
    for b in color.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            encoded.push(b as char);
        } else {
            encoded.push_str(&format!("%{b:02X}"));
        }
    }
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/bindings/%2F/hdr/hq/{encoded}"))
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/bindings/%2F")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let json = body_json(res).await;
    let left: Vec<_> = json["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["source"] == "hdr")
        .collect();
    assert_eq!(left.len(), 1);
    let size = queueforge_core::binding_properties_key(
        "",
        &[(
            compact_str::CompactString::from("size"),
            queueforge_core::HeaderArg::Str("l".into()),
        )],
    );
    assert_eq!(left[0]["properties_key"], size);
}

async fn authed(
    app: &Router,
    method: &str,
    uri: &str,
    cookie: &str,
    body: Option<&str>,
) -> Response {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::COOKIE, cookie);
    let owned;
    if body.is_some() {
        req = req.header(header::CONTENT_TYPE, "application/json");
        owned = Body::from(body.unwrap().to_string());
    } else {
        owned = Body::empty();
    }
    app.clone().oneshot(req.body(owned).unwrap()).await.unwrap()
}

#[tokio::test]
async fn console_operator_actions() {
    let (_dir, state) = test_state().await;
    let app = test_app(state.clone(), peer([127, 0, 0, 1], 21001));
    let cookie = auth_cookie(app.clone()).await;

    let res = authed(&app, "PUT", "/api/vhosts/ops", &cookie, Some("{}")).await;
    assert_eq!(res.status(), StatusCode::CREATED);
    let listed = body_json(authed(&app, "GET", "/api/vhosts", &cookie, None).await).await;
    assert!(listed["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["name"] == "ops"));

    let perm = r#"{"configure":".*","write":".*","read":".*"}"#;
    let res = authed(
        &app,
        "PUT",
        "/api/permissions/admin/%2F",
        &cookie,
        Some(perm),
    )
    .await;
    assert!(res.status() == StatusCode::CREATED || res.status() == StatusCode::NO_CONTENT);
    let perms = body_json(authed(&app, "GET", "/api/permissions", &cookie, None).await).await;
    assert!(perms
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["user"] == "admin" && p["vhost"] == "/"));

    let topic = r#"{"exchange":"amq.topic","write":"^ok\\..*","read":"^ok\\..*"}"#;
    let res = authed(
        &app,
        "PUT",
        "/api/topic-permissions/admin/%2F",
        &cookie,
        Some(topic),
    )
    .await;
    assert_eq!(res.status(), StatusCode::CREATED);
    let topics =
        body_json(authed(&app, "GET", "/api/topic-permissions", &cookie, None).await).await;
    assert_eq!(topics["items"][0]["exchange"], "amq.topic");
    assert!(!state
        .connections
        .topic_write_allowed("admin", "/", "amq.topic", "nope"));
    assert!(state
        .connections
        .topic_write_allowed("admin", "/", "amq.topic", "ok.1"));

    let definition = r#"{
            "pattern":"^pol\\..*",
            "apply-to":"queues",
            "priority":5,
            "definition":{
                "message-ttl":1000,
                "dead-letter-exchange":"amq.direct",
                "dead-letter-routing-key":"dead",
                "max-length":9,
                "max-length-bytes":99,
                "expires":60000,
                "overflow":"reject-publish",
                "delivery-limit":3,
                "alternate-exchange":"amq.fanout"
            }
        }"#;
    let res = authed(
        &app,
        "PUT",
        "/api/policies/%2F/pol.main",
        &cookie,
        Some(definition),
    )
    .await;
    assert_eq!(res.status(), StatusCode::CREATED);
    let policies = body_json(authed(&app, "GET", "/api/policies/%2F", &cookie, None).await).await;
    let def = &policies["items"][0]["definition"];
    for key in [
        "message-ttl",
        "dead-letter-exchange",
        "dead-letter-routing-key",
        "max-length",
        "max-length-bytes",
        "expires",
        "overflow",
        "delivery-limit",
        "alternate-exchange",
    ] {
        assert!(def.get(key).is_some(), "missing {key}");
    }
    let op = definition
        .replace("pol\\\\..*", "pol\\\\.op.*")
        .replace("1000", "2500");
    let res = authed(
        &app,
        "PUT",
        "/api/operator-policies/%2F/pol.op",
        &cookie,
        Some(&op),
    )
    .await;
    assert_eq!(
        res.status(),
        StatusCode::CREATED,
        "{}",
        body_json(res).await
    );
    let res = authed(
        &app,
        "PUT",
        "/api/queues/%2F/pol.op.q",
        &cookie,
        Some(r#"{"durable":true}"#),
    )
    .await;
    assert_eq!(res.status(), StatusCode::CREATED);
    let queues = body_json(authed(&app, "GET", "/api/queues/%2F", &cookie, None).await).await;
    let q = queues["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|q| q["name"] == "pol.op.q")
        .unwrap();
    assert_eq!(
        q["arguments"]["message_ttl_ms"], 2500,
        "args={}",
        q["arguments"]
    );
    assert_eq!(q["arguments"]["dead_letter_exchange"], "amq.direct");
    assert_eq!(q["arguments"]["max_length"], 9);
    let detail =
        body_json(authed(&app, "GET", "/api/queues/%2F/pol.op.q", &cookie, None).await).await;
    assert_eq!(detail["operator_policy"], "pol.op");
    assert_eq!(detail["policy"], "pol.main");
    assert_eq!(detail["type"], "classic");
    let res = authed(
        &app,
        "PUT",
        "/api/queues/%2F/pol.quorum.q",
        &cookie,
        Some(r#"{"durable":true,"arguments":{"x-queue-type":"quorum"}}"#),
    )
    .await;
    assert_eq!(
        res.status(),
        StatusCode::CREATED,
        "{}",
        body_json(res).await
    );
    let quorum =
        body_json(authed(&app, "GET", "/api/queues/%2F/pol.quorum.q", &cookie, None).await).await;
    assert_eq!(quorum["type"], "quorum");

    let res = authed(
        &app,
        "PUT",
        "/api/queues/%2F/transient.q",
        &cookie,
        Some(r#"{"durable":false}"#),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let res = authed(
        &app,
        "POST",
        "/api/feature-flags/transient_nonexcl_queues/enable",
        &cookie,
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let flags = body_json(authed(&app, "GET", "/api/feature-flags", &cookie, None).await).await;
    assert!(flags["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f["name"] == "transient_nonexcl_queues" && f["state"] == "enabled"));
    let res = authed(
        &app,
        "PUT",
        "/api/queues/%2F/transient.q",
        &cookie,
        Some(r#"{"durable":false}"#),
    )
    .await;
    assert_eq!(res.status(), StatusCode::CREATED);
    let res = authed(
        &app,
        "POST",
        "/api/feature-flags/transient_nonexcl_queues/disable",
        &cookie,
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = authed(
        &app,
        "POST",
        "/api/feature-flags/quorum_queues/disable",
        &cookie,
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let res = authed(
        &app,
        "DELETE",
        "/api/deprecated-features/transient_nonexcl_queues",
        &cookie,
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    assert!(state.connections.transient_nonexcl_permitted());

    let res = authed(
        &app,
        "PUT",
        "/api/user-limits/admin/max-connections",
        &cookie,
        Some(r#"{"value":2}"#),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = authed(
        &app,
        "PUT",
        "/api/user-limits/admin/max-channels",
        &cookie,
        Some(r#"{"value":4}"#),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = authed(
        &app,
        "PUT",
        "/api/vhost-limits/%2F/max-connections",
        &cookie,
        Some(r#"{"value":3}"#),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = authed(
        &app,
        "PUT",
        "/api/vhost-limits/%2F/max-queues",
        &cookie,
        Some(r#"{"value":50}"#),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let limits = body_json(authed(&app, "GET", "/api/limits", &cookie, None).await).await;
    assert_eq!(limits["user_limits"][0]["max-connections"], 2);
    assert_eq!(limits["user_limits"][0]["max-channels"], 4);
    assert_eq!(limits["vhost_limits"][0]["max-queues"], 50);
    let (first, _) = state
        .connections
        .register("127.0.0.1:44001".parse().unwrap(), "admin", "/");
    let (second, _) = state
        .connections
        .register("127.0.0.1:44002".parse().unwrap(), "admin", "/");
    assert!(
        !state.connections.connection_allowed("admin", "/"),
        "user max-connections is 2"
    );
    state.connections.unregister(&first);
    state.connections.unregister(&second);
    assert!(state.connections.connection_allowed("admin", "/"));

    let (id, _rx) = state
        .connections
        .register("127.0.0.1:44000".parse().unwrap(), "admin", "/");
    state
        .connections
        .sync_channels(&id, "admin", "/", "127.0.0.1:44000".parse().unwrap(), &[1]);
    state.connections.sync_consumers(
        &id,
        vec![crate::connections::ConsumerInfo {
            consumer_tag: "ctag".into(),
            connection: id.clone(),
            channel: 1,
            queue: "pol.op.q".into(),
            vhost: "/".into(),
        }],
    );
    let conns = body_json(authed(&app, "GET", "/api/connections", &cookie, None).await).await;
    assert!(conns["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["name"] == id));
    let channels = body_json(authed(&app, "GET", "/api/channels", &cookie, None).await).await;
    assert_eq!(channels["items"][0]["name"], format!("{id}:1"));
    let one =
        body_json(authed(&app, "GET", &format!("/api/channels/{id}:1"), &cookie, None).await).await;
    assert_eq!(one["number"], 1);
    let consumers = body_json(authed(&app, "GET", "/api/consumers/%2F", &cookie, None).await).await;
    assert_eq!(consumers["items"][0]["consumer_tag"], "ctag");
    state.connections.unregister(&id);
    let gone = body_json(authed(&app, "GET", "/api/connections", &cookie, None).await).await;
    assert!(gone["items"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["name"] != id));

    let nodes = body_json(authed(&app, "GET", "/api/nodes", &cookie, None).await).await;
    let node = &nodes["items"][0];
    assert!(node["name"].is_string());
    assert_eq!(node["running"], true);
    assert!(node["uptime"].is_number());
    assert!(node["mem_used"].is_number());
    assert!(node["disk_free"].is_number());
    assert!(node["mem_alarm"].is_boolean());
    assert!(node["disk_free_alarm"].is_boolean());
    assert!(node["listeners"].is_array());
    assert!(node["peers"].is_array());

    let res = authed(
        &app,
        "DELETE",
        "/api/operator-policies/%2F/pol.op",
        &cookie,
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = authed(&app, "DELETE", "/api/policies/%2F/pol.main", &cookie, None).await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = authed(
        &app,
        "DELETE",
        "/api/topic-permissions/admin/%2F/amq.topic",
        &cookie,
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = authed(
        &app,
        "DELETE",
        "/api/user-limits/admin/max-connections",
        &cookie,
        None,
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    let res = authed(&app, "DELETE", "/api/vhosts/ops", &cookie, None).await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
}
